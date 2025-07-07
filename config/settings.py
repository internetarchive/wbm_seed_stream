import os
from dotenv import load_dotenv

load_dotenv()

class Settings:
    APP_NAME: str = "Sentinel Spark Processor"
    SPARK_MASTER: str = os.getenv("SPARK_MASTER", "local[*]")

    POSTGRES_DB: str = os.getenv("POSTGRES_DB", "sentinel_db")
    POSTGRES_USER: str = os.getenv("POSTGRES_USER", "sentinel_user")
    POSTGRES_PASSWORD: str = os.getenv("POSTGRES_PASSWORD", "sentinel_password")
    POSTGRES_HOST: str = os.getenv("POSTGRES_HOST", "localhost")
    POSTGRES_PORT: str = os.getenv("POSTGRES_PORT", "5432")

    JDBC_URL: str = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

    URLS_TABLE: str = "urls"
    URL_FEATURES_TABLE: str = "url_features" 
    DOMAIN_REPUTATION_TABLE: str = "domain_reputation"

settings = Settings() 