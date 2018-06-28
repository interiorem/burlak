"""Metrics retrieval submodule.

TODO(metrics):
  - Merge with sys_metrics.py, mostly duplicated functionality.
  - For some metrics zero values returned on error, but this values could
    lead to wrong scheduling, it should be better to raise exception.
"""
from .fetcher import MetricsFetcher
from .system import SystemMetrics

__all__ = ['MetricsFetcher', 'SystemMetrics']
