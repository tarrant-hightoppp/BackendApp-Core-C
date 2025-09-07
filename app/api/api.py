from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from typing import Optional

from app.api.deps import get_db
from app.auditalgo.service import run_audit, S3Adapter
from app.auditalgo.schemas import AuditRunResponse, AuditAccountResult

router = APIRouter(prefix="/api/audit", tags=["audit"])

@router.post("/run", response_model=AuditRunResponse)
def run_audit_route(
    coverage: float = Query(0.80, ge=0.05, le=0.99),
    coverage_by_abs: bool = Query(False, description="Ако True, покрива 80% от Σ|amount|"),
    max_full: int = Query(30, ge=1, le=1000),
    prefix_mode: bool = Query(False, description="True: match по префикс (напр. 401*), False: точно съвпадение"),
    sides: str = Query("debit,credit", description="Запетайно: 'debit', 'credit' или и двете"),
    make_presigned_urls: bool = Query(True),
    db: Session = Depends(get_db),
):
    process_sides = [s.strip() for s in sides.split(',') if s.strip() in {"debit","credit"}] or ["debit","credit"]
    results = run_audit(
        db,
        S3Adapter(),
        coverage=coverage,
        coverage_by_abs=coverage_by_abs,
        max_full=max_full,
        prefix_mode=prefix_mode,
        process_sides=process_sides,
        make_presigned_urls=make_presigned_urls,
    )
    return AuditRunResponse(
        generated=len(results),
        results=[AuditAccountResult(**r) for r in results]
    )