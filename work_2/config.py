import os

from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('POSTGRES_DB')
DB_PASS = os.getenv('POSTGRES_PASSWORD')
DB_USER = os.getenv('POSTGRES_USER')
