use std::future::Future;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::pin::Pin;
use std::sync::Arc;

use url::Url;

use crate::error::{AppError, AppResult};

/// Injectable hostname-to-SocketAddr resolver used by the outbound HTTP fetcher.
/// Production uses [`SystemDnsResolver`], which resolves hostnames via
/// `tokio::net::lookup_host` and rejects any restricted/private IP before
/// pinning the verified `SocketAddr` into `reqwest`.
pub trait HostResolver: Send + Sync {
    fn resolve<'a>(
        &'a self,
        host: &'a str,
        port: u16,
    ) -> Pin<Box<dyn Future<Output = AppResult<Vec<SocketAddr>>> + Send + 'a>>;
}

#[derive(Debug, Default, Clone)]
pub struct SystemDnsResolver;

impl HostResolver for SystemDnsResolver {
    fn resolve<'a>(
        &'a self,
        host: &'a str,
        port: u16,
    ) -> Pin<Box<dyn Future<Output = AppResult<Vec<SocketAddr>>> + Send + 'a>> {
        Box::pin(async move {
            let addrs: Vec<SocketAddr> = tokio::net::lookup_host((host, port))
                .await
                .map_err(|e| {
                    AppError::Validation(format!("DNS resolution failed for host '{host}': {e}"))
                })?
                .collect();

            if addrs.is_empty() {
                return Err(AppError::Validation(format!(
                    "DNS resolution returned no addresses for host '{host}'."
                )));
            }

            for addr in &addrs {
                if is_disallowed_ip(&addr.ip()) {
                    return Err(AppError::Validation(format!(
                        "Host '{host}' resolved to restricted network address '{}'.",
                        addr.ip()
                    )));
                }
            }

            Ok(addrs)
        })
    }
}

pub fn default_resolver() -> Arc<dyn HostResolver> {
    Arc::new(SystemDnsResolver)
}

/// Validate and normalize an HTTP/HTTPS URL, blocking loopback, link-local,
/// unspecified, multicast, metadata, and RFC1918 private IP literals as an
/// SSRF safeguard.
pub fn validate_http_url(raw: &str, field_label: &str) -> AppResult<String> {
    let parsed = parse_and_validate_http_url(raw, field_label)?;
    Ok(parsed.to_string())
}

pub fn parse_and_validate_http_url(raw: &str, field_label: &str) -> AppResult<Url> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(AppError::Validation(format!(
            "{field_label} is required and cannot be empty."
        )));
    }
    if trimmed.len() > 2048 {
        return Err(AppError::Validation(format!(
            "{field_label} must not exceed 2048 characters."
        )));
    }

    let parsed = Url::parse(trimmed).map_err(|_| {
        AppError::Validation(format!(
            "{field_label} must be a valid absolute URL (https://... or http://...)."
        ))
    })?;

    match parsed.scheme() {
        "http" | "https" => {}
        other => {
            return Err(AppError::Validation(format!(
                "{field_label} must use http or https scheme (got '{other}')."
            )));
        }
    }

    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err(AppError::Validation(format!(
            "{field_label} must not embed credentials in the URL."
        )));
    }

    let Some(host_str) = parsed.host_str() else {
        return Err(AppError::Validation(format!(
            "{field_label} must include a valid host."
        )));
    };

    let lower_host = host_str.trim_end_matches('.').to_ascii_lowercase();
    if lower_host.is_empty()
        || lower_host == "localhost"
        || lower_host.ends_with(".localhost")
        || lower_host.ends_with(".local")
        || lower_host.ends_with(".internal")
        || lower_host == "metadata.google.internal"
        || lower_host == "instance-data"
    {
        return Err(AppError::Validation(format!(
            "{field_label} cannot target localhost or internal hostnames."
        )));
    }

    let bare_host = lower_host
        .strip_prefix('[')
        .and_then(|s| s.strip_suffix(']'))
        .unwrap_or(&lower_host);

    if let Ok(ip) = bare_host.parse::<IpAddr>()
        && is_disallowed_ip(&ip)
    {
        return Err(AppError::Validation(format!(
            "{field_label} cannot target private, loopback, or link-local IP addresses."
        )));
    }

    Ok(parsed)
}

/// Check whether an IPv4 or IPv6 address belongs to any non-public, loopback,
/// private, link-local, multicast, CGNAT, documentation, benchmarking, or
/// cloud metadata range.
pub fn is_disallowed_ip(ip: &IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => is_disallowed_ipv4(v4),
        IpAddr::V6(v6) => is_disallowed_ipv6(v6),
    }
}

fn is_disallowed_ipv4(v4: &Ipv4Addr) -> bool {
    let octets = v4.octets();
    v4.is_loopback()
        || v4.is_private()
        || v4.is_link_local()
        || v4.is_broadcast()
        || v4.is_unspecified()
        || v4.is_documentation()
        || v4.is_multicast()
        // 0.0.0.0/8 ("this network")
        || octets[0] == 0
        // 100.64.0.0/10 (Carrier-Grade NAT / RFC 6598, also Alibaba Cloud metadata 100.100.100.200)
        || (octets[0] == 100 && (octets[1] & 0b1100_0000) == 0b0100_0000)
        // 169.254.0.0/16 (Link-local & cloud instance metadata 169.254.169.254)
        || (octets[0] == 169 && octets[1] == 254)
        // 192.0.0.0/24 (IETF Protocol Assignments)
        || (octets[0] == 192 && octets[1] == 0 && octets[2] == 0)
        // 198.18.0.0/15 (Benchmarking RFC 2544)
        || (octets[0] == 198 && (octets[1] == 18 || octets[1] == 19))
        // 240.0.0.0/4 (Reserved for future use)
        || (octets[0] & 0b1111_0000) == 0b1111_0000
}

fn is_disallowed_ipv6(v6: &Ipv6Addr) -> bool {
    if v6.is_loopback()
        || v6.is_unspecified()
        || v6.is_multicast()
        || v6.is_unique_local()
        || v6.is_unicast_link_local()
    {
        return true;
    }

    if let Some(mapped_v4) = v6.to_ipv4_mapped() {
        return is_disallowed_ipv4(&mapped_v4);
    }
    if let Some(compat_v4) = v6.to_ipv4() {
        return is_disallowed_ipv4(&compat_v4);
    }

    let segments = v6.segments();
    // 2001:db8::/32 documentation range
    if segments[0] == 0x2001 && segments[1] == 0x0db8 {
        return true;
    }
    // AWS IPv6 IMDS fd00:ec2::254 is covered by is_unique_local (fc00::/7)

    false
}
