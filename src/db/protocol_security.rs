//! SQLx protocol-limit regressions without giant allocations or ambient DBs.
use anyhow::{Result, ensure};
use sqlx::{Arguments, Connection, Row, postgres::PgArguments};

struct OversizedHint;

impl sqlx::Type<sqlx::Postgres> for OversizedHint {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        <String as sqlx::Type<sqlx::Postgres>>::type_info()
    }
}

impl<'q> sqlx::Encode<'q, sqlx::Postgres> for OversizedHint {
    fn encode_by_ref(
        &self,
        _: &mut sqlx::postgres::PgArgumentBuffer,
    ) -> std::result::Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        panic!("oversized value must fail before encoding or allocation")
    }

    fn size_hint(&self) -> usize {
        i32::MAX as usize + 1
    }
}

#[test]
fn postgres_rejects_oversized_encoding_before_allocation_and_keeps_arguments_usable() {
    let mut arguments = PgArguments::default();
    let error = Arguments::add(&mut arguments, OversizedHint).unwrap_err();
    assert!(error.to_string().contains("overflow"));
    assert_eq!(Arguments::len(&arguments), 0);
    Arguments::add(&mut arguments, "ordinary value").expect("normal encoding after rejected value");
    assert_eq!(Arguments::len(&arguments), 1);
}

// Called only by the explicitly configured private PostgreSQL TLS matrix. That
// caller validates the loopback URL, private CA/client files and actual TLS
// version. Never read DATABASE_URL or connect from the ordinary test cohort.
pub(super) async fn postgres_target_inference_and_parameter_limit(url: String) -> Result<()> {
    let database = super::Database::connect(&crate::config::DatabaseConfig {
        url: url.clone(),
        max_connections: 1,
        connect_timeout_seconds: 5,
    })
    .await?;
    let values: Result<()> = async {
        let mut connection = database.pool.acquire().await?;
        sqlx::query("CREATE TEMP TABLE protocol_compatibility (enabled BOOLEAN, stamp TIMESTAMP, wide BIGINT, nullable TEXT, payload BYTEA)")
            .execute(&mut *connection).await?;
        let bytes = vec![0_u8, 1, 127, 128, 255];
        sqlx::query("INSERT INTO protocol_compatibility VALUES ($1, $2, $3, $4, $5)")
            .bind(true)
            .bind("2026-11-10 12:00:00")
            .bind(i64::MAX - 17)
            .bind(Option::<String>::None)
            .bind(&bytes)
            .execute(&mut *connection)
            .await?;
        let row = sqlx::query("SELECT enabled, stamp::text AS stamp_text, wide, nullable, payload FROM protocol_compatibility")
            .fetch_one(&mut *connection).await?;
        ensure!(row.try_get::<bool, _>("enabled")?, "Any Boolean changed");
        ensure!(row.try_get::<String, _>("stamp_text")? == "2026-11-10 12:00:00", "Any target timestamp inference changed");
        ensure!(row.try_get::<i64, _>("wide")? == i64::MAX - 17, "Any wide integer changed");
        ensure!(row.try_get::<Option<String>, _>("nullable")?.is_none(), "Any null changed");
        ensure!(row.try_get::<Vec<u8>, _>("payload")? == bytes, "Any binary text encoding changed");
        Ok(())
    }.await;
    database.pool.close().await;
    values?;

    // Exercise the actual native PostgreSQL protocol boundary with many small
    // values, not a multi-gigabyte allocation. A rejected query must not write
    // a malformed message or poison the connection for subsequent queries.
    let mut native = sqlx::PgConnection::connect(&url).await?;
    let result: Result<()> = async {
        let mut query = sqlx::query("SELECT 1");
        for _ in 0..65_536 {
            query = query.bind(1_i32);
        }
        let error = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            query.execute(&mut native),
        )
        .await?
        .expect_err("oversized parameter count must not reach PostgreSQL");
        ensure!(
            error.to_string().contains("too many arguments"),
            "wrong parameter-limit rejection: {error}"
        );
        let answer: i32 = sqlx::query_scalar("SELECT 1")
            .fetch_one(&mut native)
            .await?;
        ensure!(answer == 1, "parameter rejection poisoned the connection");
        Ok(())
    }
    .await;
    native.close().await?;
    result
}
