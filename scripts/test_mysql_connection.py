from sqlalchemy import text

from db_config import get_engine


engine = get_engine()

try:
    with engine.connect() as connection:
        version = connection.execute(
            text("SELECT VERSION()")
        ).scalar()

        database = connection.execute(
            text("SELECT DATABASE()")
        ).scalar()

        print("MySQL connection successful!")
        print(f"MySQL version: {version}")
        print(f"Database: {database}")

except Exception as error:
    print("MySQL connection failed!")
    print(f"Error: {error}")