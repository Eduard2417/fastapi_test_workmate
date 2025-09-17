import datetime as dt
import json

import pytest

from core.cache import redis_manager


class TestSpimexCreation:
    """Тесты для создания данных Spimex."""

    @pytest.mark.asyncio
    async def test_create_spimex(self,
                                 async_client,
                                 create_spimex_date
                                 ):
        """Тест создания записей Spimex через API."""

        response = await async_client.post('/create_spimex',
                                           json=create_spimex_date)
        assert response.status_code == 201


@pytest.mark.usefixtures('pull_spimex')
class TestSpimexQueries:
    """Тесты для запросов данных Spimex."""

    @pytest.mark.asyncio
    async def test_get_all(self,
                           async_client,
                           spimex_data):
        """Тест получения всех записей Spimex."""

        response = await async_client.get('/all')
        assert response.status_code == 200
        assert response.json() == spimex_data

    @pytest.mark.parametrize(('limit', 'date'), ((1, ['2025-01-19']),
                             (2, ['2025-01-19', '2025-01-18'])))
    @pytest.mark.asyncio
    async def test_get_last_trading_dates(self,
                                          async_client,
                                          limit,
                                          date):
        """Тест получения последних торговых дат."""

        params = {'limit': limit}
        response = await async_client.get('/get_last_trading_dates',
                                          params=params)
        assert response.json() == date

    @pytest.mark.parametrize('params_fixture', [
        'dynamics_spimex_params',
        'dynamics_spimex_params_minimal'
    ])
    @pytest.mark.asyncio
    async def test_get_dynamics(self,
                                async_client,
                                spimex_data,
                                request,
                                params_fixture):
        """Тест получения динамики торгов."""

        params = request.getfixturevalue(params_fixture)

        expected_dates = ['2025-01-15', '2025-01-16']
        response = await async_client.get('/get_dynamics', params=params)
        results = response.json()

        assert len(results) == len(expected_dates)

        spimex_dict = [sorted(item) for item in spimex_data[:2]]
        result_dict = [sorted(item) for item in response.json()]

        assert spimex_dict == result_dict

    @pytest.mark.parametrize('params_fixture', [
        'get_trading_result_params',
        'get_trading_result_params_minimal'
    ])
    @pytest.mark.asyncio
    async def test_get_trading_results(self,
                                       async_client,
                                       spimex_data,
                                       request,
                                       params_fixture
                                       ):
        """Тест получения торговых результатов."""

        params = request.getfixturevalue(params_fixture)
        response = await async_client.get('/get_trading_results',
                                          params=params)
        spimex_dict = [sorted(item) for item in spimex_data[:2]]
        result_dict = [sorted(item) for item in response.json()]
        assert spimex_dict == result_dict

    @pytest.mark.parametrize(
        ('url', 'params', 'cache_key'),
        (
            ('/all', None, 'all'),
            ('/get_last_trading_dates', {'limit': 3}, 'last_trading_dates:3'),
            ('/get_dynamics', {
                'oil_id': 'OIL001',
                'delivery_basis_id': 'BASIS001',
                'delivery_type_id': 'TYPE001',
                'start_date': dt.date(2025, 1, 15),
                'end_date': dt.date(2025, 1, 16)
            }, "get_dynamics:OIL001_BASIS001_TYPE001_2025-01-15_2025-01-16"),
            ('/get_trading_results', {
                    'oil_id': 'OIL001',
                    'delivery_basis_id': 'BASIS001',
                    'delivery_type_id': 'TYPE001',
                    'limit': 3
                }, "trading_results:OIL001_BASIS001_TYPE001_3")
        )
    )
    @pytest.mark.asyncio
    async def test_cache_behavior(self,
                                  async_client,
                                  url,
                                  params,
                                  cache_key
                                  ):
        """Тест поведения кэширования для различных эндпоинтов."""

        response1 = await async_client.get(url, params=params)
        assert response1.status_code == 200

        cached_data = await redis_manager._client.get(cache_key)
        assert cached_data is not None

        redis_data = json.loads(cached_data)

        response2 = await async_client.get(url, params=params)
        response_data = response2.json()

        response_dicts = [sorted(item) for item in response_data]
        redis_dicts = [sorted(item) for item in redis_data]

        assert response_dicts == redis_dicts
