"""An exactly-once job scheduler in plain asyncio, tested with simloop.

Plain-stdlib demo application: a single broker leases jobs to stateless
workers under time-based leases with fencing tokens; clients submit with
idempotency keys. Nothing in this package imports simloop — the harness
lives entirely in the test suite.
"""

from __future__ import annotations
