import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

load_dotenv() # Load environment variables from .env file

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+psycopg2://sentinel_user:sentinel_password@localhost:5432/sentinel_db")

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close() 