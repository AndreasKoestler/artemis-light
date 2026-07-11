//! Opt-in, read-only HTTP serving layer over the persisted event tables.
//!
//! [`ServingLayer`] reads the same database the
//! [`Persisted`](crate::persistence::Persisted) writer fills, through a backend
//! selected by URL scheme: a `sqlite:` URL (or bare path) opens the layer's
//! **own** read-only SQLite pool, a `postgres://` URL (under the `postgres`
//! feature) opens a read-only PostgreSQL pool, and
//! [`from_pg_pool`](ServingLayer::from_pg_pool) borrows a caller-owned pool
//! instead. It never reuses the writer's pool and never extends the
//! [`Store`](crate::persistence::Store) trait. The whole module is compiled only
//! under the `serving` cargo feature, so consumers who never serve pay no cost.

mod backend;
mod catalog;
mod error;
mod json;
mod pool;
mod routes;
mod rows;

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Context;
use tokio_util::sync::CancellationToken;

use backend::{ServingBackend, SqliteBackend};
pub use error::ServingError;

/// Default size of the read-only connection pool.
const DEFAULT_MAX_CONNECTIONS: u32 = 4;
// The default and maximum page sizes below are chosen conventions, not hard
// requirements; change them here (or per-instance via
// [`ServingLayer::with_default_limit`] / [`ServingLayer::with_max_limit`]).
/// Default page size for row queries when `limit` is not supplied.
const DEFAULT_PAGE_LIMIT: u64 = 100;
/// Default upper bound a requested `limit` is clamped to.
const DEFAULT_MAX_LIMIT: u64 = 1000;

/// Where a [`ServingLayer`] reads from. Kept private so the URL-less injected
/// state (a Postgres pool with no URL) is unrepresentable through the public
/// API; `build_backend` matches on it to pick a backend.
enum BackendSource {
    /// A database URL the layer opens its own read-only pool from, selected by
    /// scheme (`postgres://` / `postgresql://` / `sqlite:` / bare path).
    Url(String),
    /// A caller-owned Postgres pool the layer borrows as-is and never closes or
    /// reconfigures.
    #[cfg(feature = "postgres")]
    InjectedPg(sqlx::PgPool),
}

/// Builder and entry point for the read-only HTTP serving layer.
///
/// Construct with [`ServingLayer::new`], optionally tune with the `with_*`
/// setters, then run with [`ServingLayer::serve`].
pub struct ServingLayer {
    source: BackendSource,
    addr: SocketAddr,
    max_connections: u32,
    default_limit: u64,
    max_limit: u64,
}

impl ServingLayer {
    /// Create a serving layer for the database at `database_url`, bound to
    /// `addr`. The backend is selected by URL scheme: pass the same URL the
    /// writer store uses —
    /// [`SqliteStore::connect`](crate::persistence::SqliteStore) for `sqlite:`
    /// URLs (or a bare path), or `PostgresStore::connect` for `postgres://` /
    /// `postgresql://` URLs under the `postgres` feature.
    pub fn new(database_url: impl Into<String>, addr: SocketAddr) -> Self {
        Self::with_source(BackendSource::Url(database_url.into()), addr)
    }

    /// Shared field initialiser: bind `source` to `addr` with the default
    /// connection/limit knobs, so [`new`](Self::new) and `from_pg_pool` start
    /// from identical defaults. The `max_connections` default sizes the
    /// read-only pool the URL path opens; it is ignored on the injected path,
    /// where the caller already sized the pool.
    fn with_source(source: BackendSource, addr: SocketAddr) -> Self {
        Self {
            source,
            addr,
            max_connections: DEFAULT_MAX_CONNECTIONS,
            default_limit: DEFAULT_PAGE_LIMIT,
            max_limit: DEFAULT_MAX_LIMIT,
        }
    }

    /// Build a serving layer that reads through a caller-supplied
    /// [`sqlx::PgPool`], bound to `addr` — the serving twin of
    /// [`PostgresStore::with_pool`](crate::persistence::PostgresStore). It
    /// serves the same rows and watermarks as a URL-constructed Postgres backend
    /// over the same data, using the same default limits as [`new`](Self::new).
    /// Compiled only under the `postgres` feature.
    ///
    /// # Pool ownership
    ///
    /// The pool is *borrowed*: the layer reads through it but never closes it
    /// and never reconfigures it. Unlike the URL path — which installs `SET
    /// default_transaction_read_only = on` on every pooled connection via an
    /// `after_connect` hook — this constructor installs **no** session setting,
    /// no `after_connect` hook, and no pool-option mutation, because doing so
    /// would reconfigure sessions the caller's own writers share. The
    /// backend's SQL is SELECT-only by
    /// construction, so hard read-only *enforcement* on an injected pool is the
    /// caller's choice, not something the layer imposes.
    ///
    /// # Builder setters
    ///
    /// [`with_default_limit`](Self::with_default_limit) and
    /// [`with_max_limit`](Self::with_max_limit) apply unchanged on this path.
    /// [`with_max_connections`](Self::with_max_connections) is accepted but
    /// **ignored** here: the injected pool is already sized by the caller, and
    /// the layer neither inspects nor caps its connection count.
    #[cfg(feature = "postgres")]
    pub fn from_pg_pool(pool: sqlx::PgPool, addr: SocketAddr) -> Self {
        Self::with_source(BackendSource::InjectedPg(pool), addr)
    }

    /// Set the read-only connection-pool size (default 4).
    pub fn with_max_connections(mut self, n: u32) -> Self {
        self.max_connections = n;
        self
    }

    /// Set the default row-query page size used when `limit` is absent (default 100).
    pub fn with_default_limit(mut self, n: u64) -> Self {
        self.default_limit = n;
        self
    }

    /// Set the maximum row-query page size a requested `limit` is clamped to (default 1000).
    pub fn with_max_limit(mut self, n: u64) -> Self {
        self.max_limit = n;
        self
    }

    /// Open the read-only pool and build the axum [`Router`](axum::Router) for
    /// the serving layer's routes.
    ///
    /// Lets callers mount the serving routes into their own axum application
    /// (e.g. behind their own middleware); it is also the seam the integration
    /// tests drive via `oneshot`. [`serve`](Self::serve) uses it internally.
    pub async fn into_router(self) -> anyhow::Result<axum::Router> {
        let backend = self.build_backend().await?;
        let state = routes::AppState::new(backend, self.default_limit, self.max_limit);
        Ok(routes::router(state))
    }

    /// Build the read-only storage backend from the layer's `BackendSource`.
    ///
    /// An injected Postgres pool (from `from_pg_pool`) is wrapped as-is via
    /// `PgBackend::with_pool` (see the pool-ownership notes on
    /// [`from_pg_pool`](Self::from_pg_pool)). A URL source selects a backend by
    /// scheme: `postgres://` / `postgresql://` opens a PostgreSQL backend
    /// (under the `postgres` feature), `sqlite:` (or a bare path) opens a
    /// SQLite backend. An unrecognised scheme is an error rather than a panic.
    async fn build_backend(&self) -> anyhow::Result<Arc<dyn ServingBackend>> {
        match &self.source {
            #[cfg(feature = "postgres")]
            BackendSource::InjectedPg(pool) => {
                Ok(Arc::new(backend::PgBackend::with_pool(pool.clone())))
            }
            BackendSource::Url(database_url) => {
                let url = database_url.as_str();
                if url.starts_with("postgres://") || url.starts_with("postgresql://") {
                    #[cfg(feature = "postgres")]
                    {
                        let backend = backend::PgBackend::connect(url, self.max_connections)
                            .await
                            .context("cannot start serving layer")?;
                        return Ok(Arc::new(backend));
                    }
                    #[cfg(not(feature = "postgres"))]
                    anyhow::bail!(
                        "PostgreSQL serving requires the `postgres` feature to be enabled"
                    );
                }
                if url.starts_with("sqlite:") || !url.contains("://") {
                    let pool = pool::open_read_only_pool(url, self.max_connections)
                        .await
                        .context("cannot start serving layer")?;
                    return Ok(Arc::new(SqliteBackend::new(pool)));
                }
                anyhow::bail!("unsupported database URL scheme: {url}")
            }
        }
    }

    /// Serve the read-only HTTP API on the configured address until `shutdown`
    /// is cancelled, then drain in-flight requests and release the address.
    pub async fn serve(self, shutdown: CancellationToken) -> anyhow::Result<()> {
        let addr = self.addr;
        let app = self.into_router().await?;
        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .context("cannot start serving layer")?;
        axum::serve(listener, app)
            .with_graceful_shutdown(async move { shutdown.cancelled().await })
            .await
            // Unlike the startup failures above, this one occurs mid-operation.
            .context("serving layer failed while serving")?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn any_addr() -> SocketAddr {
        "127.0.0.1:0".parse().unwrap()
    }

    /// An unrecognised URL scheme is rejected by backend selection rather than
    /// panicking. The positive sqlite-scheme path is covered end-to-end by the
    /// serving integration suite (`tests/serving.rs`), which drives
    /// `into_router` over a sqlite file.
    #[tokio::test]
    async fn into_router_rejects_unknown_scheme() {
        let result = ServingLayer::new("mysql://localhost/db", any_addr())
            .into_router()
            .await;
        assert!(result.is_err(), "an unknown URL scheme must error");
    }

    /// With the `postgres` feature off, a `postgres://` URL is rejected rather
    /// than panicking or failing to link. Runs under `cargo test --features
    /// serving` (serving on, postgres off).
    #[cfg(not(feature = "postgres"))]
    #[tokio::test]
    async fn into_router_rejects_postgres_url_when_feature_off() {
        let result = ServingLayer::new("postgres://localhost/db", any_addr())
            .into_router()
            .await;
        assert!(
            result.is_err(),
            "a postgres URL must error when the postgres feature is off"
        );
    }

    /// The paging and pool-size setters mutate their fields and hand the
    /// builder back, so a full configuration is one chained expression.
    #[test]
    fn builder_setters_apply() {
        let layer = ServingLayer::new("sqlite::memory:", any_addr())
            .with_max_connections(8)
            .with_default_limit(50)
            .with_max_limit(500);
        assert_eq!(layer.max_connections, 8);
        assert_eq!(layer.default_limit, 50);
        assert_eq!(layer.max_limit, 500);
    }
}
