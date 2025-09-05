from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Настройки приложения, загружаемые из .env файла."""

    DB_HOST: str
    DB_PORT: int
    POSTGRES_DB: str
    POSTGRES_PASSWORD: str
    POSTGRES_USER: str
    REDIS_HOST: str
    REDIS_PORT: int
    REDIS_DB: int
    REDIS_PASSWORD: str

    class Config:
        env_file = '.env'
        env_file_encoding = 'utf-8'


settings = Settings()
