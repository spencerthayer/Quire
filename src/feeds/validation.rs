use std::future::Future;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::pin::Pin;
use std::sync::Arc;

use url::{Host, Url};

use crate::error::{AppError, AppResult};

/// Injectable hostname-to-SocketAddr resolver used by the outbound HTTP fetcher.
/// Production uses [`SystemDnsResolver`], which resolves hostnames via
/// `tokio::net::lookup_host` and rejects any non-public IP via [`is_public_unicast`]
/// before pinning the verified `SocketAddr` into `reqwest`.
pub trait HostResolver: Send + Sync {
    fn resolve<'a>(
        &'a self,
        host: &'a str,
        port: u16,
    ) -> Pin<Box<dyn Future<Output = AppResult<Vec<SocketAddr>>> + Send + 'a>>;

    /// Controlled test hook: returns `true` ONLY when a local test fixture
    /// explicitly permits connecting to a specific local test `SocketAddr`
    /// (`127.0.0.1:<ephemeral_port>`). Defaults to `false` in production and
    /// in untrusted resolver doubles.
    fn allows_test_fixture_addr(&self, _addr: &SocketAddr) -> bool {
        false
    }
}

#[derive(Debug, Default, Clone)]
pub struct SystemDnsResolver;

impl HostResolver for SystemDnsResolver {
    fn resolve<'a>(
        &'a self,
        host: &'a str,
        port: u16,
    ) -> Pin<Box<dyn Future<Output = AppResult<Vec<SocketAddr>>> + Send + 'a>> {
        let trimmed_host = host
            .trim()
            .strip_prefix('[')
            .and_then(|s| s.strip_suffix(']'))
            .unwrap_or(host.trim())
            .to_string();

        Box::pin(async move {
            // If `trimmed_host` is already an IPv4 or IPv6 literal, validate it directly
            // without invoking `lookup_host` on bracketed strings (fixes QUA-013).
            if let Ok(ip) = trimmed_host.parse::<IpAddr>() {
                if !is_public_unicast(&ip) {
                    return Err(AppError::Validation(format!(
                        "Host '{trimmed_host}' resolved to restricted network address '{ip}'."
                    )));
                }
                return Ok(vec![SocketAddr::new(ip, port)]);
            }

            let addrs: Vec<SocketAddr> = tokio::net::lookup_host((trimmed_host.as_str(), port))
                .await
                .map_err(|e| {
                    AppError::Validation(format!(
                        "DNS resolution failed for host '{trimmed_host}': {e}"
                    ))
                })?
                .collect();

            if addrs.is_empty() {
                return Err(AppError::Validation(format!(
                    "DNS resolution returned no addresses for host '{trimmed_host}'."
                )));
            }

            for addr in &addrs {
                if !is_public_unicast(&addr.ip()) {
                    return Err(AppError::Validation(format!(
                        "Host '{trimmed_host}' resolved to restricted network address '{}'.",
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

/// Validate and normalize an HTTP/HTTPS URL, enforcing the public-unicast SSRF
/// allow-list policy.
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

    let Some(host_enum) = parsed.host() else {
        return Err(AppError::Validation(format!(
            "{field_label} must include a valid host."
        )));
    };

    match host_enum {
        Host::Ipv4(v4) => {
            if !is_public_unicast(&IpAddr::V4(v4)) {
                return Err(AppError::Validation(format!(
                    "{field_label} cannot target private, loopback, link-local, or reserved IP addresses."
                )));
            }
        }
        Host::Ipv6(v6) => {
            if !is_public_unicast(&IpAddr::V6(v6)) {
                return Err(AppError::Validation(format!(
                    "{field_label} cannot target private, loopback, link-local, or reserved IP addresses."
                )));
            }
        }
        Host::Domain(domain) => {
            let lower_host = domain.trim_end_matches('.').to_ascii_lowercase();
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
            if let Ok(ip) = lower_host.parse::<IpAddr>()
                && !is_public_unicast(&ip)
            {
                return Err(AppError::Validation(format!(
                    "{field_label} cannot target private, loopback, link-local, or reserved IP addresses."
                )));
            }
        }
    }

    Ok(parsed)
}

/// Public-unicast allow-list classifier (Revision 2 §5.4).
/// Returns `true` if and only if `ip` is a globally routable public unicast IPv4
/// or IPv6 address.
pub fn is_public_unicast(ip: &IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => is_public_unicast_ipv4(v4),
        IpAddr::V6(v6) => is_public_unicast_ipv6(v6),
    }
}

/// Inverse helper kept for call-site compatibility.
pub fn is_disallowed_ip(ip: &IpAddr) -> bool {
    !is_public_unicast(ip)
}

fn is_public_unicast_ipv4(v4: &Ipv4Addr) -> bool {
    let octets = v4.octets();
    // 0.0.0.0/8 ("this network")
    if octets[0] == 0 {
        return false;
    }
    // 10.0.0.0/8 (RFC 1918 private)
    if octets[0] == 10 {
        return false;
    }
    // 100.64.0.0/10 (Carrier-Grade NAT RFC 6598 / Alibaba metadata)
    if octets[0] == 100 && (octets[1] & 0b1100_0000) == 0b0100_0000 {
        return false;
    }
    // 127.0.0.0/8 (Loopback)
    if octets[0] == 127 {
        return false;
    }
    // 169.254.0.0/16 (Link-local / Cloud instance metadata)
    if octets[0] == 169 && octets[1] == 254 {
        return false;
    }
    // 172.16.0.0/12 (RFC 1918 private)
    if octets[0] == 172 && (16..=31).contains(&octets[1]) {
        return false;
    }
    // 192.0.0.0/24 (IETF Protocol Assignments)
    if octets[0] == 192 && octets[1] == 0 && octets[2] == 0 {
        return false;
    }
    // 192.0.2.0/24 (TEST-NET-1)
    if octets[0] == 192 && octets[1] == 0 && octets[2] == 2 {
        return false;
    }
    // 192.88.99.0/24 (6to4 Relay Anycast, deprecated RFC 7526)
    if octets[0] == 192 && octets[1] == 88 && octets[2] == 99 {
        return false;
    }
    // 192.168.0.0/16 (RFC 1918 private)
    if octets[0] == 192 && octets[1] == 168 {
        return false;
    }
    // 198.18.0.0/15 (Benchmarking RFC 2544)
    if octets[0] == 198 && (octets[1] == 18 || octets[1] == 19) {
        return false;
    }
    // 198.51.100.0/24 (TEST-NET-2)
    if octets[0] == 198 && octets[1] == 51 && octets[2] == 100 {
        return false;
    }
    // 203.0.113.0/24 (TEST-NET-3)
    if octets[0] == 203 && octets[1] == 0 && octets[2] == 113 {
        return false;
    }
    // 224.0.0.0/4 (Multicast) and 240.0.0.0/4 (Reserved + Broadcast 255.255.255.255)
    if octets[0] >= 224 {
        return false;
    }

    true
}

fn is_public_unicast_ipv6(v6: &Ipv6Addr) -> bool {
    let segs = v6.segments();

    // Must fall inside Global Unicast `2000::/3` (top 3 bits `001`, i.e. 0x2000..=0x3fff).
    // Everything outside `2000::/3` (loopback `::1`, unspecified `::`, IPv4-mapped `::ffff:0:0/96`,
    // NAT64 `64:ff9b::/96` and `64:ff9b:1::/48`, ULA `fc00::/7`, link-local `fe80::/10`,
    // site-local `fec0::/10`, multicast `ff00::/8`) is denied by construction.
    if (segs[0] & 0xe000) != 0x2000 {
        return false;
    }

    // Deny `2001::/23` (IETF Protocol Assignments, including Teredo `2001:0000::/32` and benchmarking `2001:0002::/48`)
    if segs[0] == 0x2001 && segs[1] <= 0x01ff {
        return false;
    }

    // Deny `2001:db8::/32` (Documentation)
    if segs[0] == 0x2001 && segs[1] == 0x0db8 {
        return false;
    }

    // Deny `2002::/16` (6to4 transition prefix)
    if segs[0] == 0x2002 {
        return false;
    }

    // Deny `3fff::/20` (Documentation, RFC 9637)
    if segs[0] == 0x3fff && (segs[1] & 0xf000) == 0x0000 {
        return false;
    }

    true
}
