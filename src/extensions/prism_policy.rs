pub const PRISM_CERTIFICATION_POLICY_VERSION: &str = "prism-enable-time-cert-v1";
pub const PRISM_SANDBOX_PROFILE_VERSION: &str = "prism-sandbox-v1";
pub const PRISM_EGRESS_POLICY_VERSION: &str = "prism-direct-public-block-private-v1";

pub fn prism_certification_policy_version() -> String {
    format!(
        "{PRISM_CERTIFICATION_POLICY_VERSION}+{PRISM_SANDBOX_PROFILE_VERSION}+{PRISM_EGRESS_POLICY_VERSION}"
    )
}
