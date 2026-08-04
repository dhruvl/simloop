"""Throwaway certificates for the TLS tests, minted in memory.

Nothing reaches the disk and no key material is committed: one authority is
created per session and issues leaves for whatever sim hostnames a test
invents. ECDSA keys, because RSA keygen would be paid on every use.
"""

from __future__ import annotations

import functools
import ssl
from typing import Any


@functools.lru_cache(maxsize=1)
def _authority() -> Any:
    import trustme

    return trustme.CA(key_type=trustme.KeyType.ECDSA)


def forget() -> None:
    """Drop the cached authority so the next context mints a fresh one."""
    _authority.cache_clear()


def server_context(*names: str) -> ssl.SSLContext:
    """A listener's context, presenting a leaf issued for ``names``."""
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    _authority().issue_cert(*names).configure_cert(context)
    return context


def client_context() -> ssl.SSLContext:
    """A connector's context: real verification against this session's CA.

    It trusts that authority and nothing else, so a test can never pass
    because the machine it runs on happens to trust something.
    """
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    context.verify_mode = ssl.CERT_REQUIRED
    context.check_hostname = True
    _authority().configure_trust(context)
    return context
