import pytest

from services.spimex import (get_filters, get_all_spimex, get_last_spimex,
                             get_dynamics_spimex, get_trading_results_spimex)
from schemas.spimex import SpimexModel


class TestSpimexModel:
    """Тесты для утилитных функций Spimex."""

    def test_get_filters(self):
        """Тест функции получения фильтров."""

        name = 'name'
        surname = 'surname'
        age = 13

        filters = get_filters(name=name, surname=surname, age=age)
        assert filters == {'name': name, 'surname': surname, 'age': age}


@pytest.mark.usefixtures('pull_spimex')
class TestSpimexDatabaseQueries:
    """Тесты для запросов к базе данных Spimex."""

    @pytest.mark.asyncio
    async def test_get_all_spimex(self, db_session, spimex_data):
        """Тест получения всех записей Spimex."""

        stmt = get_all_spimex()
        result = await db_session.execute(stmt)
        spimex_objects = result.scalars().all()
        validated_data = [
            SpimexModel.model_validate(obj).model_dump(mode='json')
            for obj in spimex_objects
        ]
        assert validated_data == spimex_data

    @pytest.mark.asyncio
    async def test_get_last_spimex(self, db_session, last_spimex_data):
        """Тест получения последних дат Spimex."""
        stmt = get_last_spimex(limit=3)
        result = await db_session.execute(stmt)
        spimex_objects = result.scalars().all()
        assert spimex_objects == last_spimex_data

    @pytest.mark.asyncio
    async def test_get_dynamics_spimex(self, db_session, spimex_data,
                                       dynamics_spimex_params):
        """Тест получения динамики Spimex с фильтрами."""

        stmt = get_dynamics_spimex(**dynamics_spimex_params)
        result = await db_session.execute(stmt)
        spimex_objects = result.scalars().all()
        validated_data = [
            SpimexModel.model_validate(obj).model_dump(mode='json')
            for obj in spimex_objects
        ]
        assert validated_data == spimex_data[:2]

    @pytest.mark.asyncio
    async def test_get_trading_results_spimex(self, db_session,
                                              get_trading_result_params,
                                              spimex_data):
        """Тест получения торговых результатов Spimex."""

        stmt = get_trading_results_spimex(**get_trading_result_params)
        result = await db_session.execute(stmt)
        spimex_objects = result.scalars().all()
        validated_data = [
            SpimexModel.model_validate(obj).model_dump(mode='json')
            for obj in spimex_objects
        ]
        sorted_validated = sorted(validated_data, key=lambda x: x['id'])
        assert sorted_validated == spimex_data[:2]
