from logging.config import fileConfig

from sqlalchemy import engine_from_config
from sqlalchemy import pool

from alembic import context

import os # ADD THIS LINE
from dotenv import load_dotenv # ADD THIS LINE
from models import Base # ADD THIS LINE - make sure 'models' is the correct module for your SQLAlchemy Base

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

print("DEBUG: alembic/env.py is being executed.") # DEBUG PRINT

# Interpret the config file for Python logging.
# This line sets up loggers basically.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

load_dotenv() # ADD THIS LINE: Load environment variables from .env file

# Retrieve the database URL from environment variables and set it in Alembic config
database_url = os.getenv('DATABASE_URL') # ADD THIS BLOCK
print(f"DEBUG: DATABASE_URL from env: {database_url}") # DEBUG PRINT
if database_url:
    config.set_main_option('sqlalchemy.url', database_url) # ADD THIS BLOCK

# add your model's MetaData object here
# for 'autogenerate' support
# from myapp import mymodel
# target_metadata = mymodel.Base.metadata
target_metadata = Base.metadata # CRITICAL: CHANGE THIS LINE FROM None TO Base.metadata

print(f"DEBUG: target_metadata is set: {target_metadata is not None}") # DEBUG PRINT
print(f"DEBUG: target_metadata type: {type(target_metadata)}") # DEBUG PRINT
# If you have models defined, you can also inspect tables
# print(f"DEBUG: target_metadata tables (if any): {target_metadata.tables.keys()}") # OPTIONAL DEBUG PRINT

# other values from the config, defined by the needs of env.py,
# can be acquired:
# my_important_option = config.get_main_option("my_important_option")
# ... etc.


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection, target_metadata=target_metadata
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()