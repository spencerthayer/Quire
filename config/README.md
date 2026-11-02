# Quire Configuration Overview
#
# Quire runs without requiring a configuration file on disk. Configuration is
# resolved in three tiers:
#
#   1. Environment variables (QUIRE_*) — highest precedence. Used for container
#      bind addresses, storage paths, logging, and secrets.
#   2. SQLite `settings` table — editable through the web interface (/settings)
#      for nonsecret runtime settings that are not locked by an environment
#      variable.
#   3. Built-in application defaults — lowest precedence.
#
# Copy `.env.example` to `.env` for local overrides, or set environment
# variables in `compose.yaml` / Portainer Stack settings.
