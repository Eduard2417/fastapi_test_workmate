import pytest


@pytest.fixture
def get_time():
    return '12.09.2025'


@pytest.fixture
def get_row_spimex():
    """Фикстура возвращает тестовые сырые данные Spimex."""

    return [[
        {
            'Форма СЭТ-БТ': 'A001B02C',
            'Unnamed: 2': 'Product 1',
            'Unnamed: 3': 'Basis 1',
            'Unnamed: 4': '100',
            'Unnamed: 5': '5000.50',
            'Unnamed: 14': '5'
        }
    ], [
        {
            'Форма СЭТ-БТ': 'A002B03D',
            'Unnamed: 2': 'Product 2',
            'Unnamed: 3': 'Basis 2',
            'Unnamed: 4': '200',
            'Unnamed: 5': '6000.75',
            'Unnamed: 14': '3'
        }
    ]]
