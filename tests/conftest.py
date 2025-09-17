from decimal import Decimal
import datetime as dt

import pytest
import pytest_asyncio
from sqlalchemy import text
from httpx import AsyncClient, ASGITransport

from api.main import app
from core.database import (create_engine_with_config,
                           async_sessionmaker,
                           Base,
                           get_session)
from models.spimex import SpimexTradingResults
from core.cache import redis_manager


@pytest_asyncio.fixture(scope="function")
async def test_app():
    """Фикстура тестового приложения с переопределенной сессией БД."""

    test_engine = create_engine_with_config()
    test_async_session = async_sessionmaker(bind=test_engine,
                                            expire_on_commit=False)

    async def override_get_session():
        async with test_async_session() as session:
            yield session

    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    app.dependency_overrides[get_session] = override_get_session

    yield app

    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    await test_engine.dispose()
    app.dependency_overrides.clear()


@pytest_asyncio.fixture
async def db_session(test_app):
    """Фикстура асинхронной сессии БД для тестов."""

    get_session_func = app.dependency_overrides[get_session]
    async for session in get_session_func():
        yield session


@pytest.fixture
def spimex_data():
    """Тестовые данные для SpimexTradingResults."""

    return [
        {
            'oil_id': 'OIL001',
            'delivery_basis_id': 'BASIS001',
            'delivery_type_id': 'TYPE001',
            'id': 1,
            'exchange_product_id': 'TEST001',
            'exchange_product_name': 'Test Oil',
            'delivery_basis_name': 'Test Basis',
            'volume': 1000,
            'total': '50000.50',
            'count': 10,
            'date': '2025-01-15',
            'created_on': '2025-01-15T09:05:26',
            'updated_on': '2025-01-15T09:05:26'
        },
        {
            'oil_id': 'OIL001',
            'delivery_basis_id': 'BASIS001',
            'delivery_type_id': 'TYPE001',
            'id': 2,
            'exchange_product_id': 'TEST001',
            'exchange_product_name': 'Another Oil',
            'delivery_basis_name': 'Another Basis',
            'volume': 2000,
            'total': '75000.75',
            'count': 15,
            'date': '2025-01-16',
            'created_on': '2025-01-16T09:05:26',
            'updated_on': '2025-01-16T09:05:26'
        },
        {
            'oil_id': 'OIL002',
            'delivery_basis_id': 'BASIS003',
            'delivery_type_id': 'TYPE003',
            'id': 3,
            'exchange_product_id': 'TEST003',
            'exchange_product_name': 'Premium Oil',
            'delivery_basis_name': 'Premium Basis',
            'volume': 1500,
            'total': '90000.25',
            'count': 12,
            'date': '2025-01-17',
            'created_on': '2025-01-17T09:05:26',
            'updated_on': '2025-01-17T09:05:26'
        },
        {
            'oil_id': 'OIL003',
            'delivery_basis_id': 'BASIS004',
            'delivery_type_id': 'TYPE004',
            'id': 4,
            'exchange_product_id': 'TEST004',
            'exchange_product_name': 'Standard Oil',
            'delivery_basis_name': 'Standard Basis',
            'volume': 3000,
            'total': '120000.00',
            'count': 20,
            'date': '2025-01-18',
            'created_on': '2025-01-18T09:05:26',
            'updated_on': '2025-01-18T09:05:26'
        },
        {
            'oil_id': 'OIL004',
            'delivery_basis_id': 'BASIS005',
            'delivery_type_id': 'TYPE005',
            'id': 5,
            'exchange_product_id': 'TEST005',
            'exchange_product_name': 'Economy Oil',
            'delivery_basis_name': 'Economy Basis',
            'volume': 2500,
            'total': '80000.50',
            'count': 18,
            'date': '2025-01-19',
            'created_on': '2025-01-19T09:05:26',
            'updated_on': '2025-01-19T09:05:26'
        }
    ]


@pytest_asyncio.fixture
async def pull_spimex(db_session, spimex_data):
    """Заполняет БД тестовыми данными Spimex."""

    tables = ["spimex_trading_results"]

    for table in tables:
        await db_session.execute(text(f"DELETE FROM {table}"))

    await db_session.commit()

    spimex_list = []
    for spimex in spimex_data:
        data = spimex.copy()
        data['date'] = dt.date.fromisoformat(data['date'])
        data['created_on'] = dt.datetime.fromisoformat(data['created_on'])
        data['updated_on'] = dt.datetime.fromisoformat(data['updated_on'])
        data['total'] = Decimal(data['total'])
        spimex_list.append(SpimexTradingResults(**data))

    db_session.add_all(spimex_list)
    await db_session.commit()
    return spimex_data


@pytest_asyncio.fixture
async def async_client(test_app):
    """Асинхронный HTTP клиент для тестов."""

    transport = ASGITransport(app=test_app)
    async with AsyncClient(transport=transport,
                           base_url="http://test") as client:
        yield client
    await transport.aclose()


@pytest_asyncio.fixture(scope="function", autouse=True)
async def redis_manager_fixture():
    """Фикстура для управления Redis соединением."""

    if not redis_manager._client:
        await redis_manager.get_client()

    await redis_manager._client.flushall()

    yield

    if redis_manager._client:
        await redis_manager._client.aclose()
        redis_manager._client = None


@pytest.fixture
def last_spimex_data():
    """Данные последних дат для тестов."""

    return [dt.date(2025, 1, 19), dt.date(2025, 1, 18), dt.date(2025, 1, 17)]


@pytest.fixture
def dynamics_spimex_params(filter_data):
    """Параметры для тестирования динамики с фильтрами."""
    return {
        'oil_id': filter_data['oil_id'],
        'delivery_basis_id': filter_data['delivery_basis_id'],
        'delivery_type_id': filter_data['delivery_type_id'],
        'start_date': dt.date(2025, 1, 15),
        'end_date': dt.date(2025, 1, 16)
    }


@pytest.fixture
def dynamics_spimex_params_minimal():
    """Минимальные параметры для тестирования динамики."""

    return {
        'start_date': dt.date(2025, 1, 15),
        'end_date': dt.date(2025, 1, 16)
    }


@pytest.fixture
def get_trading_result_params(filter_data):
    """Параметры для получения торговых результатов с фильтрами."""

    return {
        'oil_id': filter_data['oil_id'],
        'delivery_basis_id': filter_data['delivery_basis_id'],
        'delivery_type_id': filter_data['delivery_type_id'],
        'limit': 3
    }


@pytest.fixture
def get_trading_result_params_minimal(filter_data):
    """Минимальные параметры для получения торговых результатов."""

    return {
        'limit': 2
    }


@pytest.fixture
def create_spimex_date():
    """Данные дат для создания Spimex записей."""

    return {'date': ('12.09.2025', '11.09.2025', '10.09.2025')}


@pytest.fixture
def filter_data():
    """Данные фильтров для тестов."""

    return {
            'oil_id': 'OIL001',
            'delivery_basis_id': 'BASIS001',
            'delivery_type_id': 'TYPE001'
            }
