# Notebook: common_utils | Language: python | Commands: 2

# ===== CMD 1 =====
"""
common_utils.py — Shared utilities used across the entire framework.

Provides:
  - retry_with_backoff   : exponential-backoff retry decorator
  - Timer                : context-manager performance timer
  - generate_run_id      : UUID-based execution ID
  - safe_cast            : type-safe column casting
  - flatten_dict         : for logging nested configs
  - deep_merge_dicts     : merge two dicts recursively
  - truncate_string      : safe string truncation for error messages
  - parse_json_safely    : JSON parse with fallback
  - normalize_name       : snake_case normalization
  - chunked_list         : split list into N-sized batches

Usage:
    %run ../02_Utilities/common_utils
"""

import time
import uuid
import json
import re
import functools
import traceback
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple, TypeVar

T = TypeVar("T")


# ---------------------------------------------------------------------------
# Retry with exponential backoff
# ---------------------------------------------------------------------------

def retry_with_backoff(
    max_attempts: int = 3,
    base_delay_sec: float = 60.0,
    max_delay_sec: float = 600.0,
    backoff_factor: float = 2.0,
    retryable_exceptions: tuple = (Exception,),
    on_retry: Optional[Callable] = None
):
    """
    Decorator: retry a function with exponential backoff.

    Args:
        max_attempts     : Total attempts including the first (default 3).
        base_delay_sec   : Initial wait between retries (default 60s).
        max_delay_sec    : Cap on delay (default 600s = 10 min).
        backoff_factor   : Multiplier per retry (default 2x).
        retryable_exceptions : Tuple of exception types that trigger retry.
        on_retry         : Optional callback(attempt, exception) for logging.

    Example:
        @retry_with_backoff(max_attempts=3, base_delay_sec=30)
        def load_table(config):
            ...
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            delay = base_delay_sec
            last_exc = None
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except retryable_exceptions as exc:
                    last_exc = exc
                    if attempt == max_attempts:
                        raise
                    if on_retry:
                        on_retry(attempt, exc)
                    actual_delay = min(delay, max_delay_sec)
                    print(
                        f"[retry] {func.__name__} attempt {attempt}/{max_attempts} failed: "
                        f"{type(exc).__name__}: {str(exc)[:200]}. "
                        f"Retrying in {actual_delay:.0f}s..."
                    )
                    time.sleep(actual_delay)
                    delay *= backoff_factor
            raise last_exc  # never reached; satisfies type checker
        return wrapper
    return decorator


# ---------------------------------------------------------------------------
# Timer context manager
# ---------------------------------------------------------------------------

class Timer:
    """
    Context manager for timing code blocks.

    Example:
        with Timer("read phase") as t:
            df = spark.read...
        print(t.elapsed_sec)  # seconds as float
    """
    def __init__(self, label: str = ""):
        self.label = label
        self.start_time: float = 0.0
        self.end_time:   float = 0.0
        self.elapsed_sec: float = 0.0
        self.start_dt: Optional[datetime] = None
        self.end_dt:   Optional[datetime] = None

    def __enter__(self):
        self.start_time = time.perf_counter()
        self.start_dt   = datetime.now(timezone.utc)
        return self

    def __exit__(self, *args):
        self.end_time   = time.perf_counter()
        self.end_dt     = datetime.now(timezone.utc)
        self.elapsed_sec = self.end_time - self.start_time
        if self.label:
            print(f"[timer] {self.label}: {self.elapsed_sec:.2f}s")


# ---------------------------------------------------------------------------
# Run / Execution ID generation
# ---------------------------------------------------------------------------

def generate_run_id() -> str:
    """Generate a UUID4 string for use as a run/execution ID."""
    return str(uuid.uuid4())


def generate_correlation_id(pipeline_name: str, batch_ts: Optional[str] = None) -> str:
    """
    Generate a deterministic-style correlation ID for batch grouping.
    Format: <pipeline_name_slug>_<yyyymmddHHMMSS>
    """
    slug = normalize_name(pipeline_name)[:30]
    ts   = batch_ts or datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    return f"{slug}_{ts}"


# ---------------------------------------------------------------------------
# String helpers
# ---------------------------------------------------------------------------

def normalize_name(name: str) -> str:
    """Convert any string to snake_case (strip special chars, lowercase)."""
    s = re.sub(r"[^a-zA-Z0-9_]", "_", name or "")
    s = re.sub(r"_+", "_", s)
    return s.strip("_").lower()


def truncate_string(s: str, max_length: int = 500) -> str:
    """Safely truncate a string for storage in audit tables."""
    if not s:
        return ""
    text = str(s)
    if len(text) <= max_length:
        return text
    return text[:max_length - 3] + "..."


def parse_json_safely(s: str, default: Any = None) -> Any:
    """Parse JSON string, returning `default` on any error."""
    if not s:
        return default
    try:
        return json.loads(s)
    except (json.JSONDecodeError, TypeError):
        return default


def to_json(obj: Any) -> str:
    """Serialize to compact JSON string; returns '{}' on error."""
    try:
        return json.dumps(obj, default=str)
    except Exception:
        return "{}"


# ---------------------------------------------------------------------------
# Collection helpers
# ---------------------------------------------------------------------------

def chunked_list(lst: List[Any], chunk_size: int) -> List[List[Any]]:
    """Split a list into sublists of `chunk_size`."""
    return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]


def flatten_dict(d: Dict, parent_key: str = "", sep: str = ".") -> Dict:
    """Flatten nested dict for log serialisation."""
    items: List[Tuple] = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


def deep_merge_dicts(base: Dict, override: Dict) -> Dict:
    """Recursively merge `override` into `base`; `override` wins on conflict."""
    result = dict(base)
    for k, v in override.items():
        if k in result and isinstance(result[k], dict) and isinstance(v, dict):
            result[k] = deep_merge_dicts(result[k], v)
        else:
            result[k] = v
    return result


# ---------------------------------------------------------------------------
# Error helpers
# ---------------------------------------------------------------------------

def get_error_category(exc: Exception) -> str:
    """
    Classify an exception into an ErrorCategory string.
    Maps common exception types to the audit error_category values.
    """
    msg = str(exc).lower()
    tp  = type(exc).__name__.lower()

    if any(x in msg for x in ["connection refused", "timed out", "cannot connect", "unreachable"]):
        return "CONNECTION"
    if any(x in msg for x in ["permission denied", "access denied", "unauthorized", "403", "401"]):
        return "PERMISSION"
    if any(x in msg for x in ["schema", "column", "field", "struct"]):
        return "SCHEMA_DRIFT"
    if any(x in msg for x in ["timeout", "socket timeout", "read timed out"]):
        return "TIMEOUT"
    if "transform" in msg or "cast" in msg or "type mismatch" in msg:
        return "TRANSFORMATION"
    if any(x in msg for x in ["network", "socket", "ssl", "certificate"]):
        return "NETWORK"
    return "UNKNOWN"


def format_exception(exc: Exception) -> Tuple[str, str]:
    """
    Returns (short_message, full_stack_trace) for audit logging.
    short_message is truncated to 500 chars.
    """
    short = truncate_string(str(exc), 500)
    stack = truncate_string(traceback.format_exc(), 5000)
    return short, stack


# ---------------------------------------------------------------------------
# Spark helpers
# ---------------------------------------------------------------------------

def count_rows_safely(df) -> int:
    """Count rows without triggering full scan on very large DataFrames (uses LIMIT trick)."""
    try:
        return df.count()
    except Exception:
        return -1


def repartition_for_write(df, target_partitions: int = 8):
    """
    Repartition a DataFrame before Delta write.
    Reduces small-file problem without expensive shuffle:
      - If already at target: no-op
      - If > 2x target:      coalesce (no shuffle)
      - Otherwise:           repartition (with shuffle)
    """
    try:
        current = df.rdd.getNumPartitions()
    except Exception:
        return df.repartition(target_partitions)

    if current == target_partitions:
        return df
    if current > target_partitions * 2:
        return df.coalesce(target_partitions)
    return df.repartition(target_partitions)


print("[common_utils] Loaded — retry_with_backoff, Timer, generate_run_id, helpers ready.")

# ===== CMD 2 =====


