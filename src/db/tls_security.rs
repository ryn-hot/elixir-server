//! Explicit isolated loopback PostgreSQL TLS qualification.
//! No ambient DATABASE_URL, production DB writes or remote TLS endpoints.
use anyhow::{Context, Result, ensure};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use rustls::{CertificateError, Error as TlsError, SignatureAlgorithm, SignatureScheme};
use sqlx::{
    Connection, PgConnection,
    postgres::{PgConnectOptions, PgSslMode},
};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

struct Fixture {
    url: reqwest::Url,
    directory: PathBuf,
    expected_protocol: String,
}
fn tls_error<'a>(source: &'a (dyn std::error::Error + 'static)) -> Option<&'a TlsError> {
    source.downcast_ref::<TlsError>().or_else(|| {
        source
            .downcast_ref::<std::io::Error>()?
            .get_ref()?
            .downcast_ref::<TlsError>()
    })
}
fn validate_url(value: &str) -> Result<reqwest::Url> {
    let url = reqwest::Url::parse(value)?;
    ensure!(
        matches!(url.scheme(), "postgres" | "postgresql")
            && matches!(url.host_str(), Some("localhost" | "127.0.0.1"))
            && url.port().is_some_and(|port| port >= 1024)
            && url.username() == "postgres"
            && url.password().is_none()
            && url.query().is_none()
            && url.fragment().is_none(),
        "explicit password-free loopback PostgreSQL fixture URL required"
    );
    let name = url
        .path()
        .strip_prefix('/')
        .context("fixture database name")?;
    ensure!(
        name.starts_with("elixir_tls_")
            && name.len() <= 63
            && name
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_'),
        "isolated qualification database required"
    );
    Ok(url)
}
impl Fixture {
    fn load() -> Result<Self> {
        let url = validate_url(
            &std::env::var("ELIXIR_TEST_POSTGRES_TLS_DATABASE_URL")
                .context("explicit isolated TLS database URL missing")?,
        )?;
        let directory = PathBuf::from(std::env::var("ELIXIR_TEST_POSTGRES_TLS_FIXTURE_DIR")?);
        ensure!(
            directory.is_absolute() && directory.canonicalize()? == directory && directory.is_dir(),
            "canonical private fixture directory required"
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            let metadata = directory.metadata()?;
            ensure!(
                metadata.uid() == unsafe { libc::geteuid() } && metadata.mode() & 0o077 == 0,
                "TLS scratch directory must be owned/private"
            );
        }
        let expected_protocol = std::env::var("ELIXIR_TEST_POSTGRES_TLS_VERSION")?;
        ensure!(
            matches!(expected_protocol.as_str(), "TLSv1.2" | "TLSv1.3"),
            "explicit actual server TLS version required"
        );
        Ok(Self {
            url,
            directory,
            expected_protocol,
        })
    }
    fn file(&self, name: &str) -> Result<PathBuf> {
        let path = self.directory.join(name);
        let metadata = std::fs::symlink_metadata(&path)?;
        ensure!(
            metadata.is_file()
                && !metadata.file_type().is_symlink()
                && metadata.len() > 0
                && metadata.len() <= 16 * 1024,
            "bounded regular TLS fixture required"
        );
        Ok(path)
    }
    fn url(
        &self,
        host: &str,
        mode: &str,
        ca: &Path,
        client: Option<(&Path, &Path)>,
    ) -> Result<String> {
        let mut url = self.url.clone();
        url.set_host(Some(host))
            .map_err(|_| anyhow::anyhow!("fixture host"))?;
        {
            let mut query = url.query_pairs_mut();
            query
                .append_pair("sslmode", mode)
                .append_pair("sslrootcert", ca.to_str().context("CA fixture path")?);
            if let Some((cert, key)) = client {
                query
                    .append_pair("sslcert", cert.to_str().context("client cert fixture")?)
                    .append_pair("sslkey", key.to_str().context("client key fixture")?);
            }
        }
        Ok(url.to_string())
    }
    async fn good(&self, url: String, has_client: bool) -> Result<()> {
        let database = crate::db::Database::connect(&crate::config::DatabaseConfig {
            url,
            max_connections: 2,
            connect_timeout_seconds: 5,
        })
        .await?;
        let result: Result<()> = async {
            let row: (i64, String, String) = sqlx::query_as(
                "SELECT CAST(CASE WHEN ssl THEN 1 ELSE 0 END AS BIGINT),version,COALESCE(client_dn,'') FROM pg_stat_ssl WHERE pid=pg_backend_pid()")
                .fetch_one(&database.pool).await?;
            ensure!(row.0 == 1 && row.1 == self.expected_protocol,
                "actual database connection did not use required TLS version");
            ensure!(!has_client || row.2.trim_start_matches('/') == "CN=qualification-client",
                "actual PostgreSQL server did not receive the client certificate");
            let answer: i64 = sqlx::query_scalar("SELECT CAST($1 AS BIGINT)")
                .bind(42_i64).fetch_one(&database.pool).await?;
            ensure!(answer == 42, "actual Any/PostgreSQL bind path changed");
            let scalars: (bool, Option<i64>, Option<String>) = sqlx::query_as(
                "SELECT CAST($1 AS BOOLEAN),CAST($2 AS BIGINT),CAST($3 AS TEXT)")
                .bind(true).bind(Option::<i64>::None).bind(Option::<String>::None)
                .fetch_one(&database.pool).await?;
            ensure!(scalars == (true, None, None),
                "existing Any boolean/nullable compatibility changed over TLS");
            Ok(())
        }.await;
        database.pool.close().await;
        result
    }
    async fn bad_certificate(&self, url: String, name_error: bool) -> Result<()> {
        let result = crate::db::Database::connect(&crate::config::DatabaseConfig {
            url,
            max_connections: 2,
            connect_timeout_seconds: 5,
        })
        .await;
        let error = match result {
            Ok(database) => {
                database.pool.close().await;
                anyhow::bail!("invalid certificate accepted");
            }
            Err(error) => error,
        };
        ensure!(error.chain().any(|source| matches!(tls_error(source),
            Some(TlsError::InvalidCertificate(reason)) if if name_error {
                matches!(reason, CertificateError::NotValidForName | CertificateError::NotValidForNameContext {..})
            } else {matches!(reason, CertificateError::UnknownIssuer)})),
            "expected certificate rejection, not an unrelated connectivity error: {error:#}");
        Ok(())
    }
}

#[test]
fn certificate_rejection_observation_preserves_io_wrapped_tls_causes() {
    let error = anyhow::Error::new(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        TlsError::InvalidCertificate(CertificateError::UnknownIssuer),
    ))
    .context("original connection failure");
    assert!(error.chain().any(|source| matches!(
        tls_error(source),
        Some(TlsError::InvalidCertificate(
            CertificateError::UnknownIssuer
        ))
    )));
}

#[test]
fn qualification_urls_cannot_use_ambient_or_remote_databases() -> Result<()> {
    validate_url("postgresql://postgres@localhost:55443/elixir_tls_example")?;
    for bad in [
        "postgresql://postgres@db.example.com:55443/elixir_tls_example",
        "postgresql://postgres@localhost:55443/production",
        "postgresql://postgres:secret@localhost:55443/elixir_tls_example",
        "postgresql://postgres@localhost:55443/elixir_tls_example?host=db.example.com",
        "postgresql://postgres@localhost/elixir_tls_example",
    ] {
        ensure!(validate_url(bad).is_err(), "unsafe fixture URL accepted");
    }
    Ok(())
}

#[tokio::test]
#[ignore = "explicit isolated local TLS PostgreSQL fixture, generated CA/server/client files; no network outside loopback"]
async fn actual_postgres_tls_modes_and_all_supported_client_key_encodings() -> Result<()> {
    let fixture = Fixture::load()?;
    let ca = fixture.file("ca.pem")?;
    let untrusted = fixture.file("untrusted-ca.pem")?;
    let default_cert = fixture.file("client-rsa.pem")?;
    let default_key = fixture.file("client-rsa-pkcs8.key")?;
    let client = Some((default_cert.as_path(), default_key.as_path()));
    fixture
        .good(fixture.url("localhost", "verify-full", &ca, client)?, true)
        .await?;
    fixture
        .bad_certificate(fixture.url("127.0.0.1", "verify-full", &ca, client)?, true)
        .await?;
    fixture
        .bad_certificate(
            fixture.url("localhost", "verify-full", &untrusted, client)?,
            false,
        )
        .await?;
    // Explicit hostname-only exception must still validate the chain.
    fixture
        .good(fixture.url("127.0.0.1", "verify-ca", &ca, client)?, true)
        .await?;
    fixture
        .bad_certificate(
            fixture.url("127.0.0.1", "verify-ca", &untrusted, client)?,
            false,
        )
        .await?;
    // Preserve the existing explicit encryption-only mode, including proof of
    // handshake-key possession; adversarial signatures are checked separately.
    fixture
        .good(
            fixture.url("127.0.0.1", "require", &untrusted, client)?,
            true,
        )
        .await?;
    for (cert, key) in [
        ("client-rsa.pem", "client-rsa-pkcs8.key"),
        ("client-rsa.pem", "client-rsa-pkcs1.key"),
        ("client-ec.pem", "client-ec-sec1.key"),
    ] {
        let cert = fixture.file(cert)?;
        let key = fixture.file(key)?;
        fixture
            .good(
                fixture.url("localhost", "verify-full", &ca, Some((&cert, &key)))?,
                true,
            )
            .await?;
    }
    // Positive control after all negative cases: rejection cannot be explained
    // by a dead fixture, and none of the failures poisoned subsequent TLS.
    fixture
        .good(fixture.url("localhost", "verify-full", &ca, client)?, true)
        .await?;
    // No password-free localhost superuser service is exposed: the private
    // fixture requires this generated client certificate. A PostgreSQL auth
    // error also proves TLS completed through the no-client-auth branch.
    let missing = crate::db::Database::connect(&crate::config::DatabaseConfig {
        url: fixture.url("localhost", "verify-full", &ca, None)?,
        max_connections: 2,
        connect_timeout_seconds: 5,
    })
    .await;
    let error = match missing {
        Ok(database) => {
            database.pool.close().await;
            anyhow::bail!("missing client certificate authenticated");
        }
        Err(error) => error,
    };
    ensure!(
        error
            .chain()
            .any(|source| matches!(source.downcast_ref::<sqlx::Error>(),
        Some(sqlx::Error::Database(error)) if error.code().as_deref() == Some("28000")
            && error.message().contains("client certificate"))),
        "expected PostgreSQL client-certificate auth rejection, not a TLS failure: {error:#}"
    );
    Ok(())
}

#[derive(Debug)]
struct InvalidSigningKey;
#[derive(Debug)]
struct InvalidSigner(SignatureScheme);
impl rustls::sign::SigningKey for InvalidSigningKey {
    fn choose_scheme(&self, offered: &[SignatureScheme]) -> Option<Box<dyn rustls::sign::Signer>> {
        offered
            .iter()
            .copied()
            .find(|scheme| {
                matches!(
                    scheme,
                    SignatureScheme::RSA_PSS_SHA256 | SignatureScheme::RSA_PKCS1_SHA256
                )
            })
            .map(|scheme| Box::new(InvalidSigner(scheme)) as Box<dyn rustls::sign::Signer>)
    }
    fn algorithm(&self) -> SignatureAlgorithm {
        SignatureAlgorithm::RSA
    }
}
impl rustls::sign::Signer for InvalidSigner {
    fn sign(&self, _: &[u8]) -> std::result::Result<Vec<u8>, TlsError> {
        Ok(vec![0; 256])
    }
    fn scheme(&self) -> SignatureScheme {
        self.0
    }
}
#[derive(Debug)]
struct InvalidResolver(Arc<rustls::sign::CertifiedKey>);
impl rustls::server::ResolvesServerCert for InvalidResolver {
    fn resolve(
        &self,
        _: rustls::server::ClientHello<'_>,
    ) -> Option<Arc<rustls::sign::CertifiedKey>> {
        Some(self.0.clone())
    }
}
fn leaf_certificate(path: &Path) -> Result<rustls::pki_types::CertificateDer<'static>> {
    let pem = std::fs::read_to_string(path)?;
    ensure!(
        pem.matches("-----BEGIN CERTIFICATE-----").count() == 1,
        "one public leaf fixture certificate required"
    );
    let base64 = pem
        .lines()
        .filter(|line| !line.starts_with("-----"))
        .collect::<String>();
    Ok(rustls::pki_types::CertificateDer::from(
        STANDARD.decode(base64)?,
    ))
}

#[tokio::test]
#[ignore = "explicit private public-certificate fixture; adversarial loopback PostgreSQL SSLRequest/TLS server only"]
async fn encryption_only_and_hostname_exception_still_reject_bad_tls12_and_tls13_signatures()
-> Result<()> {
    let fixture = Fixture::load()?;
    for version in [&rustls::version::TLS12, &rustls::version::TLS13] {
        for mode in [PgSslMode::Require, PgSslMode::VerifyCa] {
            let key = Arc::new(rustls::sign::CertifiedKey::new(
                vec![leaf_certificate(&fixture.file("server.pem")?)?],
                Arc::new(InvalidSigningKey),
            ));
            let config = rustls::ServerConfig::builder_with_provider(Arc::new(
                rustls::crypto::ring::default_provider(),
            ))
            .with_protocol_versions(&[version])?
            .with_no_client_auth()
            .with_cert_resolver(Arc::new(InvalidResolver(key)));
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
            let port = listener.local_addr()?.port();
            let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(config));
            let mut server = tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await?;
                let mut request = [0; 8];
                socket.read_exact(&mut request).await?;
                ensure!(
                    request == [0, 0, 0, 8, 4, 210, 22, 47],
                    "PostgreSQL SSLRequest expected"
                );
                socket.write_all(b"S").await?;
                Ok::<bool, anyhow::Error>(acceptor.accept(socket).await.is_err())
            });
            let options = PgConnectOptions::new()
                .host("127.0.0.1")
                .port(port)
                .username("postgres")
                .database("elixir_tls_fake")
                .ssl_mode(mode)
                .ssl_root_cert(fixture.file("ca.pem")?);
            let client = tokio::time::timeout(
                Duration::from_secs(10),
                PgConnection::connect_with(&options),
            )
            .await;
            let joined = tokio::time::timeout(Duration::from_secs(10), &mut server).await;
            let server_rejected = match joined {
                Ok(result) => result??,
                Err(error) => {
                    server.abort();
                    let _ = server.await;
                    return Err(error.into());
                }
            };
            let error = match client.context("original adversarial TLS client deadline")? {
                Ok(connection) => {
                    connection.close().await?;
                    anyhow::bail!("forged handshake accepted");
                }
                Err(error) => anyhow::Error::new(error),
            };
            ensure!(
                server_rejected,
                "server completed a forged-signature handshake"
            );
            ensure!(
                error.chain().any(|source| matches!(
                    tls_error(source),
                    Some(TlsError::InvalidCertificate(CertificateError::BadSignature))
                )),
                "wrong rejection cause instead of invalid handshake signature: {error:#}"
            );
        }
    }
    Ok(())
}
