"""Real TLS over the simulated stream transports, with no descriptor anywhere.

The handshake is the standard library's: ``asyncio.sslproto.SSLProtocol``
drives an ``ssl.SSLObject`` over a pair of memory BIOs, and each flight it
produces leaves as one ordinary simulated packet, paying the same seeded
latency every other packet pays. Certificate verification is the real thing,
and the handshake and shutdown deadlines are ordinary loop timers, so they
fire in virtual time.

This is the only module that imports ``ssl``. ``_loop`` imports it lazily, so
``import simloop`` and every run that never asks for TLS stay free of it.
"""

from __future__ import annotations

import ssl
from asyncio import sslproto
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from simloop._loop import SimLoop

# typeshed types the waiter as required and the handshake timeout as an int;
# sslproto accepts no waiter at all (it returns from _wakeup_waiter before
# touching a None one) and treats both timeouts as floats. Constructing
# through one untyped alias keeps that disagreement in a single place instead
# of scattering ignores over every call.
_SSLProtocol: Any = sslproto.SSLProtocol


def context(value: object, *, server_side: bool) -> ssl.SSLContext | None:
    """The context an ``ssl=`` argument asks for, or None for the stdlib default.

    ``ssl=True`` means whatever context sslproto would build, which exists
    only for a client: a server has no default certificate to present, so
    that combination is refused when the listener is created rather than once
    per connection it accepts.
    """
    if isinstance(value, bool):
        if server_side:
            raise ValueError("Server side SSL needs a valid SSLContext")
        return None
    if isinstance(value, ssl.SSLContext):
        return value
    # Named here rather than left to surface as an AttributeError from inside
    # the OpenSSL glue several steps later.
    raise TypeError(
        "ssl argument must be True or an instance of ssl.SSLContext, "
        f"not {type(value).__name__!r}"
    )


async def connect(
    loop: SimLoop,
    protocol_factory: Any,
    host: Any,
    port: Any,
    sslcontext: ssl.SSLContext | None,
    server_hostname: str | None,
    handshake_timeout: float | None,
    shutdown_timeout: float | None,
) -> tuple[Any, Any]:
    """Open a simulated connection and hand it to a TLS client handshake."""
    waiter: Any = loop.create_future()
    upgrade: dict[str, Any] = {}

    def factory() -> Any:
        app = protocol_factory()
        protocol = _SSLProtocol(
            loop,
            app,
            sslcontext,
            waiter,
            False,
            server_hostname,
            ssl_handshake_timeout=handshake_timeout,
            ssl_shutdown_timeout=shutdown_timeout,
        )
        # Captured at construction because SSLProtocol.connection_lost drops
        # both references before it wakes the waiter: a connection torn down
        # during the handshake must still hand back the transport the standard
        # library would have handed back, not None.
        upgrade["transport"] = protocol._app_transport
        upgrade["protocol"] = app
        return protocol

    try:
        # The application protocol is built inside the factory, so a refused
        # connection never constructs one — the plaintext path's rule too.
        raw, _ = await loop._net._open_connection(factory, host, port)
    except BaseException:
        # Nothing was ever built, so nothing will complete this future.
        waiter.cancel()
        raise
    try:
        await waiter
    except BaseException:
        raw.close()
        raise
    return upgrade["transport"], upgrade["protocol"]


def server_factory(
    loop: SimLoop,
    protocol_factory: Any,
    sslcontext: ssl.SSLContext | None,
    handshake_timeout: float | None,
    shutdown_timeout: float | None,
) -> Callable[[], Any]:
    """Wrap a listener's factory so every connection it accepts speaks TLS."""

    def factory() -> Any:
        # The waiter is None on purpose. Accepting is synchronous, so there is
        # no coroutine to hand a handshake failure to, and a future carrying an
        # exception nobody retrieves would reach Future.__del__ and from there
        # the loop's unhandled-error list — failing an otherwise green run
        # because some client presented a bad certificate. A server-side
        # failure reaches _fatal_error instead, which only debug-logs an
        # OSError, and ssl.SSLError is one: the connection is reset and the run
        # stays green.
        return _SSLProtocol(
            loop,
            protocol_factory(),
            sslcontext,
            None,
            True,
            None,
            ssl_handshake_timeout=handshake_timeout,
            ssl_shutdown_timeout=shutdown_timeout,
        )

    return factory


def _migrate_buffered(protocol: Any, ssl_protocol: Any, *, server_side: bool) -> None:
    """Hand a stream reader's already-read bytes to the TLS engine.

    A server that read a plaintext marker off the wire can easily have read
    the ClientHello sitting behind it into the reader's own buffer, where the
    handshake would never see it and would stall until its deadline.
    """
    if not server_side:
        return
    from asyncio.streams import StreamReaderProtocol

    if not isinstance(protocol, StreamReaderProtocol):
        return
    reader = getattr(protocol, "_stream_reader", None)
    if reader is None:
        return
    buffer = reader._buffer
    if buffer:
        ssl_protocol._incoming.write(buffer)
        buffer.clear()


async def upgrade(
    loop: SimLoop,
    transport: Any,
    protocol: Any,
    sslcontext: Any,
    *,
    server_side: bool,
    server_hostname: str | None,
    handshake_timeout: float | None,
    shutdown_timeout: float | None,
) -> Any:
    """Turn an established plaintext connection into a TLS one, in place."""
    if not isinstance(sslcontext, ssl.SSLContext):
        raise TypeError(
            "sslcontext is expected to be an instance of ssl.SSLContext, "
            f"got {sslcontext!r}"
        )
    if not getattr(transport, "_start_tls_compatible", False):
        raise TypeError(f"transport {transport!r} is not supported by start_tls()")

    waiter: Any = loop.create_future()
    ssl_protocol = _SSLProtocol(
        loop,
        protocol,
        sslcontext,
        waiter,
        server_side,
        server_hostname,
        call_connection_made=False,
        ssl_handshake_timeout=handshake_timeout,
        ssl_shutdown_timeout=shutdown_timeout,
    )
    # Paused before anything is swapped, so no packet can reach the new
    # protocol ahead of its own connection_made.
    transport.pause_reading()
    _migrate_buffered(protocol, ssl_protocol, server_side=server_side)
    transport.set_protocol(ssl_protocol)
    made = loop.call_soon(ssl_protocol.connection_made, transport)
    resumed = loop.call_soon(transport.resume_reading)
    try:
        await waiter
    except BaseException:
        transport.close()
        made.cancel()
        resumed.cancel()
        raise
    app_transport: Any = ssl_protocol._app_transport
    return app_transport
