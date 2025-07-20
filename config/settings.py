import os
from dotenv import load_dotenv
load_dotenv()

class Settings:
    APP_NAME: str = "Sentinel Spark Processor"
    SPARK_MASTER: str = os.getenv("SPARK_MASTER", "local[*]")
    
    POSTGRES_USER: str = os.getenv("POSTGRES_USER", "postgres")
    POSTGRES_PASSWORD: str = os.getenv("POSTGRES_PASSWORD", "")
    POSTGRES_HOST: str = os.getenv("POSTGRES_HOST", "localhost")
    POSTGRES_PORT: str = os.getenv("POSTGRES_PORT", "5434")
    POSTGRES_DB: str = os.getenv("POSTGRES_DB", "sentinel")
    
    @property
    def JDBC_URL(self) -> str:
        return f"jdbc:postgresql://{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
    
    @property
    def DATABASE_URL(self) -> str:
        return f"postgresql+psycopg2://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"

settings = Settings()