-- Local VPN configuration, not peer/catalog state. Keep old identity purposes
-- through rotation so a still-running old gateway cannot be reused elsewhere.
CREATE TABLE network_vpn_identity_purposes (
    identity_sha256 TEXT PRIMARY KEY CHECK (LENGTH(identity_sha256)=71),
    purpose TEXT NOT NULL CHECK (purpose IN ('downloads','marketplace_network','marketplace_http'))
);
CREATE TABLE network_vpn_identity_lock (
    singleton INTEGER PRIMARY KEY CHECK (singleton=1)
);
INSERT INTO network_vpn_identity_lock (singleton) VALUES (1);
-- One explicit extension-HTTP configuration, never an active downloader profile.
CREATE UNIQUE INDEX download_network_marketplace_http_profile
    ON download_network_profiles(scope) WHERE scope='marketplace_http';
