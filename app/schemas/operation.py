from datetime import date, datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel


# Shared properties
class OperationBase(BaseModel):
    operation_date: date
    document_type: Optional[str] = None
    document_number: Optional[str] = None
    debit_account: Optional[str] = None
    credit_account: Optional[str] = None
    amount: float
    description: Optional[str] = None
    partner_name: Optional[str] = None
    analytical_debit: Optional[str] = None
    analytical_credit: Optional[str] = None
    account_name: Optional[str] = None
    # Audit-related fields
    sequence_number: Optional[int] = None  # № по ред
    verified_amount: Optional[float] = None  # Установена сума при одита
    deviation_amount: Optional[float] = None  # Отклонение
    control_action: Optional[str] = None  # Установено контролно действие при одита
    deviation_note: Optional[str] = None  # Отклонение (second deviation field)


# Properties to receive via API on creation
class OperationCreate(OperationBase):
    file_id: int
    template_type: str
    raw_data: Optional[Dict[str, Any]] = None


# Properties to receive via API on update
class OperationUpdate(OperationBase):
    pass


# Properties shared by models stored in DB
class OperationInDBBase(OperationBase):
    id: int
    file_id: int
    template_type: str
    created_at: datetime
    
    class Config:
        orm_mode = True


# Properties to return via API
class Operation(OperationInDBBase):
    pass


# Properties for pagination
class OperationList(BaseModel):
    items: List[Operation]
    total: int


# Properties for filters
class OperationFilter(BaseModel):
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    document_type: Optional[str] = None
    debit_account: Optional[str] = None
    credit_account: Optional[str] = None
    min_amount: Optional[float] = None
    max_amount: Optional[float] = None
    description_contains: Optional[str] = None
    template_type: Optional[str] = None
    # New audit filter fields
    sequence_number: Optional[int] = None
    has_verified_amount: Optional[bool] = None
    has_deviation: Optional[bool] = None
    has_control_action: Optional[bool] = None