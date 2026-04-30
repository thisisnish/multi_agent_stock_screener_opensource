"""
screener/lib/storage/__init__.py — Public API for the storage package.

Factory function::

    from screener.lib.storage import get_storage_dao
    from screener.lib.config_loader import AppConfig

    dao = get_storage_dao(cfg)          # returns a StorageDAO
    doc = await dao.get("tickers", "AAPL")

Currently only ``"firestore"`` is supported.  Attempting to instantiate any
other provider raises :class:`StorageConfigError` immediately at startup,
giving a clear failure message rather than a cryptic AttributeError later.
"""

from __future__ import annotations

from screener.lib.config_loader import AppConfig
from screener.lib.storage.base import StorageDAO


class StorageConfigError(RuntimeError):
    """Raised when the storage provider in config is unsupported or misconfigured."""


def get_storage_dao(config: AppConfig) -> StorageDAO:
    """Instantiate and return a :class:`StorageDAO` for the configured provider.

    Args:
        config: Fully-validated :class:`AppConfig` instance.

    Returns:
        A concrete :class:`StorageDAO` ready for use.

    Raises:
        StorageConfigError: If ``config.storage.provider`` is not ``"firestore"``.
    """
    provider = config.storage.provider

    if provider == "firestore":
        # Import deferred so the google-cloud-firestore dependency is only
        # required when Firestore is actually the configured backend.
        from screener.lib.storage.firestore import FirestoreDAO

        fs_cfg = config.storage.firestore
        return FirestoreDAO(
            project_id=fs_cfg.project_id,
            database=fs_cfg.database,
        )

    raise StorageConfigError(
        f"Unsupported storage provider: '{provider}'. "
        f"Only 'firestore' is currently implemented.  "
        f"To use a different backend, implement StorageDAO and register it here."
    )
