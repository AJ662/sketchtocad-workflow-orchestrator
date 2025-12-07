from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import os

from .saga_models import Base

DATABASE_URL = os.getenv(
    'DATABASE_URL',
    'postgresql://postgres:password@postgres-saga:5432/sagadb'
)

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def init_database():    
    enum_values = [
        'STARTED',
        'IMAGE_PROCESSING',
        'AWAITING_ENHANCEMENT_SELECTION',
        'GENERATING_ENHANCED_COLORS',
        'AWAITING_CLUSTERING',
        'PROCESSING_CLUSTERING',
        'AWAITING_EXPORT',
        'DXF_EXPORT',
        'COMPLETED',
        'FAILED',
        'COMPENSATING',
        'COMPENSATED',
    ]
    
    with engine.connect() as conn:
        # Check if enum exists
        result = conn.execute(text(
            "SELECT 1 FROM pg_type WHERE typname = 'sagastatus'"
        ))
        enum_exists = result.fetchone() is not None
        
        if not enum_exists:
            # Create enum with all values
            values_str = ", ".join(f"'{v}'" for v in enum_values)
            conn.execute(text(f"CREATE TYPE sagastatus AS ENUM ({values_str})"))
            conn.commit()
        else:
            # Add any missing values
            for value in enum_values:
                try:
                    conn.execute(text(
                        f"ALTER TYPE sagastatus ADD VALUE IF NOT EXISTS '{value}'"
                    ))
                    conn.commit()
                except Exception:
                    pass  # Value already exists
    
    # Create tables
    Base.metadata.create_all(bind=engine)
    print("Database tables created successfully!")


def get_db():
    """Get database session"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


if __name__ == "__main__":
    init_database()