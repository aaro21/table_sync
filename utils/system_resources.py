import os
import psutil


def get_optimal_worker_count(min_mem_per_worker_gb: float = 1.5, cpu_util_fraction: float = 0.75) -> int:
    """Return a recommended number of workers based on system CPU and memory limits."""
    total_cores = os.cpu_count() or 1
    reserved_system_mem_gb = float(os.environ.get("RESERVED_MEMORY_GB", 4.0))
    available_mem_gb = max(0, (psutil.virtual_memory().available / (1024 ** 3)) - reserved_system_mem_gb)

    max_by_cpu = max(1, int(total_cores * cpu_util_fraction))
    max_by_mem = max(1, int(available_mem_gb / min_mem_per_worker_gb))

    max_workers_cap = int(os.environ.get("MAX_WORKERS_CAP", 32))
    env_override = os.environ.get("WORKER_COUNT")

    if env_override and env_override.isdigit():
        return int(env_override)

    return min(max_by_cpu, max_by_mem, max_workers_cap)
