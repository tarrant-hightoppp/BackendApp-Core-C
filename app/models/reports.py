from sqlalchemy import Boolean, Column, Integer, String, DateTime
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship

from app.db.base_class import Base


class Reports(Base):
    """
    This is the reports table
    """
    id = Column(Integer, primary_key=True, index=True)
    # TODO  : Да се допишат останалите колони 
    
 