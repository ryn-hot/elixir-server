use std::net::IpAddr;

use anyhow::{Context, Result, bail};
use reqwest::Url;
use tokio::net::lookup_host;

pub(crate) async fn validate_source_url_dns(
    url: &Url,
    allow_private_hosts: bool,
    context: &str,
) -> Result<Vec<IpAddr>> {
    if allow_private_hosts {
        return Ok(Vec::new());
    }
    let host = url
        .host_str()
        .map(str::trim)
        .filter(|host| !host.is_empty())
        .ok_or_else(|| anyhow::anyhow!("{context} URL host is required"))?;
    if let Ok(ip) = host.trim_matches('[').trim_matches(']').parse::<IpAddr>() {
        validate_public_source_ip(ip, context)?;
        return Ok(vec![ip]);
    }
    let port = url.port_or_known_default().unwrap_or(443);
    let mut resolved = Vec::new();
    for address in lookup_host((host, port))
        .await
        .with_context(|| format!("resolving {context} host {host}"))?
    {
        let ip = address.ip();
        validate_public_source_ip(ip, context)
            .with_context(|| format!("{context} host {host} resolved to blocked IP {ip}"))?;
        if !resolved.contains(&ip) {
            resolved.push(ip);
        }
    }
    if resolved.is_empty() {
        bail!("{context} host {host} did not resolve to any IP addresses");
    }
    Ok(resolved)
}

pub(crate) fn validate_public_source_ip(ip: IpAddr, context: &str) -> Result<()> {
    let blocked = match ip {
        IpAddr::V4(ip) => {
            ip.is_private()
                || ip.is_loopback()
                || ip.is_link_local()
                || ip.is_broadcast()
                || ip.is_documentation()
                || ip.is_multicast()
                || ip.is_unspecified()
                || ip.octets()[0] == 0
        }
        IpAddr::V6(ip) => {
            if let Some(mapped_ipv4) = ip.to_ipv4_mapped() {
                return validate_public_source_ip(IpAddr::V4(mapped_ipv4), context);
            }
            ip.is_loopback()
                || ip.is_unique_local()
                || ip.is_unicast_link_local()
                || ip.is_unspecified()
                || ip.is_multicast()
                || (ip.segments()[0] & 0xffc0) == 0xfe80
        }
    };
    if blocked {
        bail!(
            "{context} private, local, link-local, multicast, documentation, and unspecified IPs are not allowed: {ip}"
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_ip_policy_rejects_private_and_ipv4_mapped_private_ips() {
        let private_v4 = "192.168.1.10".parse::<IpAddr>().unwrap();
        let mapped_private = "::ffff:127.0.0.1".parse::<IpAddr>().unwrap();

        assert!(validate_public_source_ip(private_v4, "source registry").is_err());
        assert!(validate_public_source_ip(mapped_private, "source registry").is_err());
    }

    #[test]
    fn source_ip_policy_accepts_public_ips() {
        let public_v4 = "8.8.8.8".parse::<IpAddr>().unwrap();
        let public_v6 = "2606:4700:4700::1111".parse::<IpAddr>().unwrap();

        assert!(validate_public_source_ip(public_v4, "source registry").is_ok());
        assert!(validate_public_source_ip(public_v6, "source registry").is_ok());
    }

    #[tokio::test]
    async fn source_dns_policy_rejects_literal_private_hosts() {
        let url = Url::parse("https://127.0.0.1/manifest.json").unwrap();

        let err = validate_source_url_dns(&url, false, "source registry")
            .await
            .expect_err("private literal host should be rejected");
        assert!(err.to_string().contains("not allowed"));
    }

    #[tokio::test]
    async fn source_dns_policy_skips_private_checks_when_explicitly_allowed() -> Result<()> {
        let url = Url::parse("https://127.0.0.1/manifest.json").unwrap();

        let resolved = validate_source_url_dns(&url, true, "source registry").await?;
        assert!(resolved.is_empty());
        Ok(())
    }
}
