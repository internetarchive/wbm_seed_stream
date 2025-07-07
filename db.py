import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, Column, String, Float, Integer, Text, Boolean, DateTime
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func
from sqlalchemy.exc import ProgrammingError

load_dotenv()

DATABASE_URL = os.getenv('DATABASE_URL')
if DATABASE_URL:
    if DATABASE_URL.startswith('postgresql://'):
        DATABASE_URL = DATABASE_URL.replace('postgresql://', 'postgresql+psycopg2://')
else:
    from config.settings import settings
    DATABASE_URL = f"postgresql+psycopg2://{settings.POSTGRES_USER}:{settings.POSTGRES_PASSWORD}@{settings.POSTGRES_HOST}:{settings.POSTGRES_PORT}/{settings.POSTGRES_DB}"

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()

class URL(Base):
    __tablename__ = "urls"

    id = Column(Integer, primary_key=True, index=True)
    url = Column(String, unique=True, index=True, nullable=False)
    timestamp = Column(String, nullable=False) # Store as string for now, convert to timestamp in Spark
    domain = Column(String, index=True, nullable=False)
    priority_score = Column(Float, nullable=False, default=0.0)
    reasons = Column(Text, nullable=True) # Stored as JSON string
    domain_frequency = Column(Integer, nullable=False, default=0)
    domain_frequency_pct = Column(Float, nullable=False, default=0.0)
    is_active = Column(Boolean, default=True) # New column
    received_at = Column(DateTime(timezone=True), server_default=func.now()) # For TimescaleDB partitioning

    def __repr__(self):
        return f"<URL(url='{self.url}', priority_score={self.priority_score})>"

def check_timescaledb_extension():
    """Check if TimescaleDB extension is available and enabled."""
    try:
        with engine.connect() as connection:
            # Check if TimescaleDB extension is installed
            result = connection.execute(
                "SELECT 1 FROM pg_extension WHERE extname = 'timescaledb';"
            ).fetchone()
            
            if result is not None:
                print(f"TimescaleDB extension found in database")
                # Double-check by testing if create_hypertable function exists
                func_result = connection.execute(
                    "SELECT COUNT(*) FROM pg_proc WHERE proname = 'create_hypertable';"
                ).fetchone()
                return func_result[0] > 0
            else:
                print("TimescaleDB extension not found in pg_extension table")
                return False
                
    except Exception as e:
        print(f"Error checking TimescaleDB extension: {e}")
        import traceback
        traceback.print_exc()
        return False

def create_urls_table():
    """Create the urls table and configure TimescaleDB features if available."""
    
    # Check if TimescaleDB is available first
    timescaledb_available = check_timescaledb_extension()
    
    if not timescaledb_available:
        print("TimescaleDB extension not found. Creating regular PostgreSQL table...")
        Base.metadata.create_all(bind=engine)
        print("'urls' table created as regular PostgreSQL table.")
        return

    print("TimescaleDB extension is available. Creating table with hypertable support...")

    # For TimescaleDB, we need to handle the table creation differently
    with engine.connect() as connection:
        try:
            # Check if table already exists
            result = connection.execute(
                "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'urls');"
            ).fetchone()
            
            if not result[0]:
                # Create the table first
                Base.metadata.create_all(bind=engine)
                print("'urls' table created.")
                
                # Now convert to hypertable
                try:
                    connection.execute(
                        "SELECT create_hypertable('urls', 'received_at', if_not_exists => TRUE);"
                    )
                    print("'urls' table converted to hypertable.")
                except Exception as e:
                    if "already a hypertable" in str(e):
                        print("'urls' table is already a hypertable.")
                    elif "unique index" in str(e):
                        print("⚠️  Cannot create hypertable due to primary key constraint.")
                        print("   The table will work as a regular PostgreSQL table.")
                        print("   To use hypertable features, consider using a composite primary key.")
                    else:
                        print(f"Error converting 'urls' to hypertable: {e}")
            else:
                print("'urls' table already exists.")
                
                # Check if it's already a hypertable
                hypertable_result = connection.execute(
                    "SELECT count(*) FROM _timescaledb_catalog.hypertable WHERE table_name = 'urls';"
                ).fetchone()
                
                if hypertable_result[0] == 0:
                    print("Table exists but is not a hypertable. Attempting conversion...")
                    try:
                        connection.execute(
                            "SELECT create_hypertable('urls', 'received_at', if_not_exists => TRUE);"
                        )
                        print("'urls' table converted to hypertable.")
                    except Exception as e:
                        if "unique index" in str(e):
                            print("⚠️  Cannot convert existing table to hypertable due to primary key constraint.")
                            print("   The table will continue to work as a regular PostgreSQL table.")
                        else:
                            print(f"Error converting existing table to hypertable: {e}")
                else:
                    print("'urls' table is already a hypertable.")

            # Try to add compression policy (only if it's a hypertable)
            try:
                # Check if table is a hypertable first
                hypertable_result = connection.execute(
                    "SELECT count(*) FROM _timescaledb_catalog.hypertable WHERE table_name = 'urls';"
                ).fetchone()
                
                if hypertable_result[0] > 0:
                    # Check for existing compression policy using correct column names
                    policy_result = connection.execute(
                        """SELECT count(*) FROM _timescaledb_config.bgw_job j
                           JOIN _timescaledb_catalog.hypertable h ON j.hypertable_id = h.id
                           WHERE j.proc_name = 'policy_compression' AND h.table_name = 'urls';"""
                    ).fetchone()

                    if policy_result[0] == 0:
                        connection.execute(
                            "SELECT add_compression_policy('urls', INTERVAL '7 days');"
                        )
                        print("Compression policy added to 'urls' table.")
                    else:
                        print("Compression policy already exists for 'urls' table.")
                else:
                    print("Skipping compression policy (table is not a hypertable).")
                    
            except Exception as e:
                print(f"Note: Could not set up compression policy: {e}")
                print("This is optional and the table will work fine without it.")

        except ProgrammingError as e:
            print(f"TimescaleDB catalog tables not accessible: {e}")
            print("Creating table as regular PostgreSQL table...")
            Base.metadata.create_all(bind=engine)
            print("'urls' table created as regular PostgreSQL table.")

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

if __name__ == "__main__":
    create_urls_table()