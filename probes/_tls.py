"""Throwaway certificates for the TLS probes, minted in memory.

A simulation's hostnames — ``web``, ``ws`` — are names no public authority
would ever sign, so the probes issue their own and trust nothing else.

trustme is imported inside the functions on purpose: probe modules are
imported with the ``probes`` dependency group absent, so nothing reachable
from one may import a third-party package at module scope.
"""

from __future__ import annotations

import functools
import ssl
from typing import Any


@functools.lru_cache(maxsize=1)
def _authority() -> Any:
    import trustme

    return trustme.CA(key_type=trustme.KeyType.ECDSA)


def server_context(*names: str) -> ssl.SSLContext:
    """A listener's context, presenting a leaf issued for ``names``."""
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    _authority().issue_cert(*names).configure_cert(context)
    return context


def client_context() -> ssl.SSLContext:
    """A client's context, verifying against this session's authority alone."""
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    context.verify_mode = ssl.CERT_REQUIRED
    context.check_hostname = True
    _authority().configure_trust(context)
    return context
