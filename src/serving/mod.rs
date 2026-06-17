//! Opt-in, read-only HTTP serving layer over the persisted SQLite tables.
//!
//! [`ServingLayer`] opens its **own** read-only [`sqlx::SqlitePool`] to the same
//! database file the [`Persisted`](crate::persistence::Persisted) writer uses —
//! it never reuses the writer's single-connection pool and never extends the
//! [`Store`](crate::persistence::Store) trait. The whole module is compiled only
//! under the `serving` cargo feature, so consumers who never serve pay no cost
//! (serving-layer.OPTIN.1/.2).

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
// Open question OQ-4 (unresolved with maintainer): the default and maximum page
// sizes below are standing assumptions; change them here (or per-instance via
// [`ServingLayer::with_default_limit`] / [`ServingLayer::with_max_limit`]).
/// Default page size for row queries when `limit` is not supplied.
const DEFAULT_PAGE_LIMIT: u64 = 100;
/// Default upper bound a requested `limit` is clamped to.
const DEFAULT_MAX_LIMIT: u64 = 1000;

/// Builder and entry point for the read-only HTTP serving layer.
///
/// Construct with [`ServingLayer::new`], optionally tune with the `with_*`
/// setters, then run with [`ServingLayer::serve`].
pub struct ServingLayer {
    database_url: String,
    addr: SocketAddr,
    max_connections: u32,
    default_limit: u64,
    max_limit: u64,
}

impl ServingLayer {
    /// Create a serving layer for the database at `database_url` (the same URL
    /// passed to [`SqliteStore::connect`](crate::persistence::SqliteStore)),
    /// bound to `addr`.
    pub fn new(database_url: impl Into<String>, addr: SocketAddr) -> Self {
        Self {
            database_url: database_url.into(),
            addr,
            max_connections: DEFAULT_MAX_CONNECTIONS,
            default_limit: DEFAULT_PAGE_LIMIT,
            max_limit: DEFAULT_MAX_LIMIT,
        }
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
    /// This is an intentional part of the public API (beyond the original
    /// builder/`serve` surface): it lets callers mount the serving routes into
    /// their own axum application (e.g. behind their own middleware), and is the
    /// seam the integration tests drive via `oneshot`. [`serve`](Self::serve)
    /// uses it internally.
    pub async fn into_router(self) -> anyhow::Result<axum::Router> {
        let backend = self.build_backend().await?;
        let state = routes::AppState::new(backend, self.default_limit, self.max_limit);
        Ok(routes::router(state))
    }

    /// Select and open the read-only storage backend from the database URL
    /// scheme (postgres-store.SERVE.2): `sqlite:` (or a bare path) opens a
    /// SQLite backend. An unrecognised scheme is an error rather than a panic
    /// (postgres-store.SERVE.4-adjacent UnknownSchemeOrFeatureOff). The
    /// `postgres://` scheme is wired to a PostgreSQL backend in a later phase.
    async fn build_backend(&self) -> anyhow::Result<Arc<dyn ServingBackend>> {
        let url = self.database_url.as_str();
        if url.starts_with("postgres://") || url.starts_with("postgresql://") {
            anyhow::bail!(
                "PostgreSQL serving backend is not available in this build \
                 (the `postgres` serving backend is not yet wired)"
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

    /// Serve the read-only HTTP API on the configured address until `shutdown`
    /// is cancelled, then drain in-flight requests and release the address
    /// (serving-layer.SERVER.1/.2/.3). Realises the `Unbound to Bound` and
    /// `Bound to Serving` transitions; cancellation drives `Serving to Draining`
    /// and `Draining to Stopped`.
    pub async fn serve(self, shutdown: CancellationToken) -> anyhow::Result<()> {
        let addr = self.addr;
        let app = self.into_router().await?;
        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .context("cannot start serving layer")?;
        axum::serve(listener, app)
            .with_graceful_shutdown(async move { shutdown.cancelled().await })
            .await
            .context("cannot start serving layer")?;
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
    /// panicking (UnknownSchemeOrFeatureOff). The positive sqlite-scheme path is
    /// covered end-to-end by the serving integration suite (`tests/serving.rs`),
    /// which drives `into_router` over a sqlite file through the new backend.
    #[tokio::test]
    async fn into_router_rejects_unknown_scheme() {
        let result = ServingLayer::new("mysql://localhost/db", any_addr())
            .into_router()
            .await;
        assert!(result.is_err(), "an unknown URL scheme must error");
    }
}
