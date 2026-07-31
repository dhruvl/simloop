"""Compatibility probes: dev-only, never packaged.

Each ``probe_<library>.py`` module drives one library's happy path under a
SimLoop and reports what happened; ``report.py`` runs them all and prints the
table published in docs/compatibility.md.
"""
