import json
import datetime as dt
from typing import Any

import redis.asyncio as redis

from core.config import settings


class DateEncoder(json.JSONEncoder):
    """Кастомный JSON encoder для обработки datetime объектов"""
    def default(self, obj: Any) -> Any:
        if isinstance(obj, dt.date):
            return obj.isoformat()
        return super().default(obj)


class RedisManager:
    """
    Менеджер для работы с Redis, обеспечивающий ленивую инициализацию клиента
    и управление кэшированием данных.
    """

    def __init__(self):
        self._client = None

    def get_date(self) -> int:
        """
        Вычисляет время жизни кэша до следующего указанного времени.

        Returns:
            int: Количество секунд до 14:11 следующего дня
        """
        now = dt.datetime.now()
        tomorrow = now + dt.timedelta(days=1)
        target_time = tomorrow.replace(
            hour=14,
            minute=11,
            second=0,
            microsecond=0
        )

        cache_time = target_time - now
        return int(cache_time.total_seconds())

    async def get_client(self) -> redis.Redis:
        """
        Возвращает клиент Redis, инициализируя его при первом вызове.

        Returns:
            redis.Redis: Клиент Redis с подключением к серверу

        Raises:
            redis.ConnectionError: Если не удалось подключиться к Redis
        """
        if self._client is None:
            self._client = redis.Redis(
                host=settings.REDIS_HOST,
                port=settings.REDIS_PORT,
                db=settings.REDIS_DB,
                password=settings.REDIS_PASSWORD,
                decode_responses=True
            )
        return self._client
    
    async def close(self) -> None:
        """Корректно закрывает соединение с Redis"""
        if self._client:
            await self._client.close()
            self._client = None

    async def get_cached_data(self, key: str) -> Any:
        """
        Получает и десериализует данные из кэша Redis.

        Args:
            cached_key: Ключ для поиска в кэше

        Returns:
            Any: Десериализованные данные или None,
            если ключ не найден
        """
        client = await self.get_client()
        data = await client.get(key)
        return data

    async def set_cached_data(self, key: str, data: Any) -> None:
        """
        Сохраняет данные в кэш с автоматическим вычислением времени жизни.

        Args:
            key: Ключ для сохранения данных
            data: Данные для кэширования (должны быть сериализуемы в JSON)
        """
        client = await self.get_client()
        await client.setex(key, self.get_date(), data)


redis_manager = RedisManager()


async def get_cache(cached_key: str) -> Any:
    """
    Получает и десериализует данные из кэша Redis.

    Args:
        cached_key: Ключ для поиска в кэше

    Returns:
        Any: Десериализованные данные или None, если ключ не найден
    """
    cached_data = await redis_manager.get_cached_data(cached_key)

    if cached_data:
        return json.loads(cached_data)

    return None


async def set_cache(cached_key: str, data: Any, to_dict: bool = False) -> None:
    """
    Сохраняет данные в кэш Redis с опцией сериализации объектов.

    Args:
        cached_key: Ключ для сохранения данных
        data: Данные для кэширования
        to_dict: Если True, преобразует элементы данных в словари
                 с помощью метода to_dict()
    """
    if to_dict:
        await redis_manager.set_cached_data(
            cached_key, json.dumps(
                [item.to_dict() for item in data],
                cls=DateEncoder)
            )
    else:
        await redis_manager.set_cached_data(
            cached_key, json.dumps([item for item in data], cls=DateEncoder))
