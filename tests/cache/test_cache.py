import datetime as dt
import json

import pytest
from redis.asyncio.client import Redis

from core.cache import (redis_manager, DateEncoder,
                        RedisManager, get_cache, set_cache)
from models.spimex import SpimexTradingResults


class TestCache:
    """Тесты для модуля кэширования."""

    def test_date_encoder(self):
        """Тест кодировщика дат в JSON."""

        date_encoder = DateEncoder()
        result = date_encoder.default(dt.date(2025, 10, 10))
        assert isinstance(result, str)

    @pytest.mark.asyncio
    async def test_get_date(self):
        """Тест расчета времени до следующего обновления кэша."""

        redis_manager = RedisManager()
        seconds = redis_manager.get_date()

        now = dt.datetime.now()
        tomorrow = now + dt.timedelta(days=1)
        target_time = tomorrow.replace(hour=14,
                                       minute=11,
                                       second=0,
                                       microsecond=0)
        time_after_seconds = now + dt.timedelta(seconds=seconds)

        time_difference = abs(
            (time_after_seconds - target_time).total_seconds())
        assert time_difference < 1, f"Difference: {time_difference} seconds"

    @pytest.mark.asyncio
    async def test_get_redis_client(self):
        """Тест получения Redis клиента."""

        redis_client = await redis_manager.get_client()
        assert isinstance(redis_client, Redis)

    @pytest.mark.asyncio
    async def test_close(self):
        """Тест закрытия соединения с Redis."""

        redis = RedisManager()
        await redis.get_client()
        await redis.close()
        assert redis._client is None

    @pytest.mark.asyncio
    async def test_set_and_get_cached_data(self):
        """Тест установки и получения данных из кэша."""

        redis_manager = RedisManager()
        await redis_manager.set_cached_data('key', 'value')
        result = await redis_manager.get_cached_data('key')
        assert result == 'value'

    @pytest.mark.asyncio
    async def test_get_json_cache(self):
        """Тест получения JSON данных из кэша."""

        redis_manager = RedisManager()

        test_data = {'key': 'value', 'number': 42}
        await redis_manager.set_cached_data('test_key', json.dumps(test_data))

        cache = await get_cache('test_key')
        assert cache == test_data

    @pytest.mark.parametrize(
        ('key', 'value'),
        (
            ('key1', 'value'),
            ('key2', SpimexTradingResults(
                oil_id='OIL004',
                delivery_basis_id='BASIS005',
                delivery_type_id='TYPE005',
                id=5,
                exchange_product_id='TEST005',
                exchange_product_name='Economy Oil',
                delivery_basis_name='Economy Basis',
                volume=2500,
                total='80000.50',
                count=18,
                date=dt.date(2025, 1, 19),
                created_on=dt.datetime(2025, 1, 19, 9, 5, 26),
                updated_on=dt.datetime(2025, 1, 19, 9, 5, 26)
            ))
        )
    )
    @pytest.mark.asyncio
    async def test_set_json_cache(self, key, value):
        """Тест установки JSON данных в кэш."""

        if isinstance(value, str):
            await set_cache(key, (value,))
            cache = await get_cache(key)
            assert cache == [value]
        else:
            await set_cache(key, (value,), to_dict=True)
            cache = await get_cache(key)
            assert cache == [
                {
                    'id': 5, 'exchange_product_id': 'TEST005',
                    'exchange_product_name': 'Economy Oil',
                    'oil_id': 'OIL004',
                    'delivery_basis_id': 'BASIS005',
                    'delivery_basis_name': 'Economy Basis',
                    'delivery_type_id': 'TYPE005', 'volume': 2500,
                    'total': '80000.50', 'count': 18,
                    'date': '2025-01-19',
                    'created_on': '2025-01-19T09:05:26',
                    'updated_on': '2025-01-19T09:05:26'
                    }
                ]
