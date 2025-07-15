import threading
from psycopg2 import pool
import os
from dotenv import load_dotenv

dotenv_path = os.path.join(os.path.dirname(__file__), '..', '.env')
load_dotenv(dotenv_path)

class DatabaseConnectionPool:
    def __init__(self):
        self._pool = None
        self._lock = threading.Lock()
        self._initialize_pool()
    
    def _initialize_pool(self):
        try:
            db_config = {
                'host': os.getenv('POSTGRES_HOST'),
                'port': os.getenv('POSTGRES_PORT'),
                'database': os.getenv('POSTGRES_DB'),
                'user': os.getenv('POSTGRES_USER'),
                'password': os.getenv('POSTGRES_PASSWORD'),
            }
            
            self._pool = pool.ThreadedConnectionPool(
                minconn=1,
                maxconn=20,
                **db_config
            )
        except Exception as e:
            print(f"Error initializing database connection pool: {e}")
            self._pool = None
    
    def get_connection(self):
        if self._pool is None:
            self._initialize_pool()
        
        if self._pool is None:
            raise Exception("Database connection pool not available")
        
        with self._lock:
            return self._pool.getconn()
    
    def return_connection(self, conn):
        if self._pool is not None and conn is not None:
            with self._lock:
                self._pool.putconn(conn)
    
    def close_all_connections(self):
        if self._pool is not None:
            with self._lock:
                self._pool.closeall()
                self._pool = None

_connection_pool = DatabaseConnectionPool()

def get_connection():
    return _connection_pool.get_connection()

def return_connection(conn):
    _connection_pool.return_connection(conn)

def close_all_connections():
    _connection_pool.close_all_connections()

class DatabaseConnection:
    def __init__(self):
        self.conn = None
    
    def __enter__(self):
        self.conn = get_connection()
        return self.conn
    
    def __exit__(self):
        if self.conn:
            return_connection(self.conn)