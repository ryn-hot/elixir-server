use std::net::IpAddr;

pub(crate) fn is_public_egress_ip(address: IpAddr) -> bool {
    match address {
        IpAddr::V4(address) => {
            let octets = address.octets();
            let shared = octets[0] == 100 && (octets[1] & 0b1100_0000) == 64;
            let protocol_assignments = octets[0] == 192 && octets[1] == 0 && octets[2] == 0;
            let deprecated_relay = octets[0] == 192 && octets[1] == 88 && octets[2] == 99;
            let benchmarking = octets[0] == 198 && matches!(octets[1], 18 | 19);
            !(address.is_private()
                || address.is_loopback()
                || address.is_link_local()
                || address.is_broadcast()
                || address.is_documentation()
                || address.is_unspecified()
                || address.is_multicast()
                || octets[0] == 0
                || octets[0] >= 224
                || shared
                || protocol_assignments
                || deprecated_relay
                || benchmarking)
        }
        IpAddr::V6(address) => {
            if let Some(mapped) = address.to_ipv4_mapped() {
                return is_public_egress_ip(IpAddr::V4(mapped));
            }
            let segments = address.segments();
            let documentation = segments[0] == 0x2001 && segments[1] == 0x0db8;
            let discard_only =
                segments[0] == 0x0100 && segments[1..].iter().all(|segment| *segment == 0);
            let site_local = (segments[0] & 0xffc0) == 0xfec0;
            !(address.is_loopback()
                || address.is_unspecified()
                || address.is_multicast()
                || address.is_unique_local()
                || address.is_unicast_link_local()
                || documentation
                || discard_only
                || site_local)
        }
    }
}
