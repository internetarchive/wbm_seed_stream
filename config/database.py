import os
import sys
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func
from sqlalchemy.exc import ProgrammingError

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from models import Base 
from db import engine

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+psycopg2://sentinel_user:sentinel_password@localhost:5432/sentinel_db")

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def check_timescaledb_extension():
    try:
        with engine.connect() as connection:
            result = connection.execute(
                text("SELECT 1 FROM pg_extension WHERE extname = 'timescaledb';")
            ).fetchone()
            
            if result is not None:
                func_result = connection.execute(
                    text("SELECT COUNT(*) FROM pg_proc WHERE proname = 'create_hypertable';")
                ).fetchone()
                return func_result[0] > 0
            else:
                return False
                
    except Exception as e:
        return False

def create_tables():
    timescaledb_available = check_timescaledb_extension()
    
    if not timescaledb_available:
        Base.metadata.create_all(bind=engine)
        return

    Base.metadata.create_all(bind=engine)

    with engine.connect() as connection:
        try:
            result = connection.execute(
                text("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'urls');")
            ).fetchone()
            
            if not result[0]:
                try:
                    connection.execute(
                        text("SELECT create_hypertable('urls', 'received_at', if_not_exists => TRUE);")
                    )
                except Exception as e:
                    if "already a hypertable" not in str(e) and "unique index" not in str(e):
                        pass
            else:
                hypertable_result = connection.execute(
                    text("SELECT count(*) FROM _timescaledb_catalog.hypertable WHERE table_name = 'urls';")
                ).fetchone()
                
                if hypertable_result[0] == 0:
                    try:
                        connection.execute(
                            text("SELECT create_hypertable('urls', 'received_at', if_not_exists => TRUE);")
                        )
                    except Exception as e:
                        pass

            try:
                hypertable_result = connection.execute(
                    text("SELECT count(*) FROM _timescaledb_catalog.hypertable WHERE table_name = 'urls';")
                ).fetchone()
                
                if hypertable_result[0] > 0:
                    policy_result = connection.execute(
                        text("""SELECT count(*) FROM _timescaledb_config.bgw_job j
                           JOIN _timescaledb_catalog.hypertable h ON j.hypertable_id = h.id
                           WHERE j.proc_name = 'policy_compression' AND h.table_name = 'urls';""")
                    ).fetchone()

                    if policy_result[0] == 0:
                        connection.execute(
                            text("SELECT add_compression_policy('urls', INTERVAL '7 days');")
                        )
                    
            except Exception as e:
                pass

        except ProgrammingError as e:
            Base.metadata.create_all(bind=engine)

if __name__ == "__main__":
    create_tables()