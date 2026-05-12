import os
import multiprocessing

def load_slurm_dask_config():
    """
    Detect SLURM resources and return safe Dask configuration.
    """

    # -------------------------
    # CPUs (SLURM-aware)
    # -------------------------
    slurm_cpus = os.environ.get("SLURM_CPUS_PER_TASK")

    if slurm_cpus and slurm_cpus.strip():
        ncores = int(slurm_cpus)
    else:
        # fallback (only if SLURM did not define CPUs)
        ncores = 4

    # -------------------------
    # Memory (SLURM-aware)
    # -------------------------
    slurm_mem = os.environ.get("SLURM_MEM_PER_NODE")

    if slurm_mem is not None:
        # SLURM gives memory in MB (most clusters)
        total_mem = int(slurm_mem) * 1024**2
    else:
        raise EnvironmentError("SLURM_MEM_PER_NODE not found in environment variables. Cannot determine memory limit for Dask.")

    # safety margin (HPC best practice)
    usable_ram = int(total_mem * 0.70)

    # -------------------------
    # Threads strategy
    # -------------------------
    threads = min(16, max(4, ncores // 2))

    return {
        "ncores": ncores,
        "threads": threads,
        "memory_limit": usable_ram,
    }