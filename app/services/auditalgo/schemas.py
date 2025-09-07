from pydantic import BaseModel, Field
from typing import List, Optional

class AuditAccountResult(BaseModel):
    side: str            # "debit" или "credit"
    account: str
    canonical_account: str
    match_mode: str      # "exact" или "prefix"
    total_rows: int
    exported_rows: int
    total_amount: float
    coverage_target: float
    coverage_by_abs: bool
    s3_key: Optional[str] = None
    s3_url: Optional[str] = None
    export_type: str     # "full" или "top80"

class AuditRunResponse(BaseModel):
    generated: int = Field(..., description="Общ брой генерирани файлове")
    results: List[AuditAccountResult]