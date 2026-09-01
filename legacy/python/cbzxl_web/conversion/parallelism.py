def plan_parallelism(
    cpu_budget: int,
    page_count: int,
    encoder_threads: int | None = None,
) -> tuple[int, int]:
    """Allocate a fixed CPU budget across page workers and encoder threads."""
    budget = max(1, int(cpu_budget or 1))
    workers = max(1, min(int(page_count or 1), budget, 4))
    threads = max(1, int(encoder_threads or budget // workers))
    while workers * threads > budget and workers > 1:
        workers -= 1
    return workers, min(threads, budget)

