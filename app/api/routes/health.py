import asyncio
import time
from typing import Annotated, Literal

from fastapi import APIRouter, Depends, Response, status
from pydantic import BaseModel

from app.api.deps import Probe, get_health_probes

router = APIRouter()

# Hard upper bound per probe, independent of client-level timeouts: a hung
# dependency must not hang /health.
PROBE_TIMEOUT_S = 3.0


class CheckResult(BaseModel):
    status: Literal["up", "down"]
    latency_ms: float
    error: str | None = None


class HealthResponse(BaseModel):
    status: Literal["ok", "degraded"]
    checks: dict[str, CheckResult]


async def _run_probe(probe: Probe) -> CheckResult:
    started = time.perf_counter()
    try:
        await asyncio.wait_for(probe(), timeout=PROBE_TIMEOUT_S)
    except Exception as exc:  # any failure means "down"; keep the reason
        return CheckResult(
            status="down",
            latency_ms=round((time.perf_counter() - started) * 1000, 1),
            error=f"{type(exc).__name__}: {exc}" if str(exc) else type(exc).__name__,
        )
    return CheckResult(status="up", latency_ms=round((time.perf_counter() - started) * 1000, 1))


@router.get("/health", response_model=HealthResponse)
async def health(
    response: Response,
    probes: Annotated[dict[str, Probe], Depends(get_health_probes)],
) -> HealthResponse:
    # Probes run concurrently: total latency ~= the slowest probe, not the sum.
    results = await asyncio.gather(*(_run_probe(p) for p in probes.values()))
    checks = dict(zip(probes.keys(), results, strict=True))

    healthy = all(c.status == "up" for c in checks.values())
    if not healthy:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    return HealthResponse(status="ok" if healthy else "degraded", checks=checks)
