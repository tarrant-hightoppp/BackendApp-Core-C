from typing import Generator
from sqlalchemy.orm import Session

from app.db.session import SessionLocal


def get_db() -> Generator:
    """
    Dependency for getting the database session
    """
    try:
        db = SessionLocal()
        yield db
    finally:
        db.close()


def get_db_session() -> Session:
    """
    Create and return a new database session (not a dependency)
    
    This function is useful when you need to create a fresh session
    outside of the FastAPI dependency injection system, particularly
    to ensure database transactions are properly committed.
    
    Note: The caller must close this session when done with it.
    """
    return SessionLocal()