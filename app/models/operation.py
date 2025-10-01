from sqlalchemy import Column, Integer, String, Date, Numeric, Text, ForeignKey, DateTime
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship

from app.db.base_class import Base


class AccountingOperation(Base):
    id = Column(Integer, primary_key=True, index=True)
    file_id = Column(Integer, ForeignKey("uploadedfile.id"))
    
    # Common fields across all templates
    operation_date = Column(Date, nullable=False)
    document_type = Column(String)
    document_number = Column(String)
    debit_account = Column(String)
    credit_account = Column(String)
    amount = Column(Numeric(15, 2), nullable=False)
    description = Column(Text)
    
    # Additional fields that might be specific to some templates
    partner_name = Column(String)
    analytical_debit = Column(String)
    analytical_credit = Column(String)
    account_name = Column(String)  # For Business Navigator
    
    # Audit fields - new fields added for audit functionality
    sequence_number = Column(Integer)  # № по ред
    verified_amount = Column(Numeric(15, 2))  # Установена сума при одита
    deviation_amount = Column(Numeric(15, 2))  # Отклонение
    control_action = Column(Text)  # Установено контролно действие при одита
    deviation_note = Column(Text)  # Отклонение (second deviation field)
    
    # Metadata
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    template_type = Column(String, nullable=False)  # Which template this came from
    raw_data = Column(JSONB)  # Store the raw data from the Excel for reference
    import_uuid = Column(String, nullable=False)  # UUID of the import batch
    
    # Relationships
    file = relationship("UploadedFile", back_populates="operations")