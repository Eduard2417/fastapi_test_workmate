import datetime as dt
from typing import Dict, Any

from sqlalchemy import select, desc, between, Select

from models.spimex import SpimexTradingResults


def get_filters(**kargs) -> Dict[str, Any]:
    return {key: value for key, value in kargs.items() if value and value != ''}


def get_all_spimex() -> Select:
    return select(SpimexTradingResults)


def get_last_spimex(limit: int) -> Select:
    """
    Создает запрос для получения последних уникальных дат торговых результатов.

    Args:
        limit (int): Количество последних дат для возврата
    """
    return (
        select(SpimexTradingResults.date)
        .distinct()
        .order_by(desc(SpimexTradingResults.date))
        .limit(limit)
        )


def get_dynamics_spimex(
        start_date: dt.date,
        end_date: dt.date,
        oil_id: str | None = None,
        delivery_type_id: str | None = None,
        delivery_basis_id: str | None = None
        ) -> Select:
    """
    Создает запрос для получения динамики торговых результатов за период.

    Args:
        start_date (dt.date): Начальная дата периода
        end_date (dt.date): Конечная дата периода
        oil_id (Optional[str]): ID нефтепродукта для фильтрации
        delivery_type_id (Optional[str]): ID типа поставки для фильтрации
        delivery_basis_id (Optional[str]): ID базиса поставки для фильтрации
    """
    filters = get_filters(
        oil_id=oil_id,
        delivery_type_id=delivery_type_id,
        delivery_basis_id=delivery_basis_id
    )
    return select(SpimexTradingResults).filter(
        between(SpimexTradingResults.date, start_date, end_date)
    ).filter_by(**filters)


def get_trading_results_spimex(
        limit: int = 5,
        oil_id: str | None = None,
        delivery_type_id: str | None = None,
        delivery_basis_id: str | None = None
        ) -> Select:
    """
    Создает запрос для получения последних торговых результатов с фильтрацией.

    Args:
        limit (int): Количество последних записей для возврата. По умолчанию 5
        oil_id (Optional[str]): ID нефтепродукта для фильтрации
        delivery_type_id (Optional[str]): ID типа поставки для фильтрации
        delivery_basis_id (Optional[str]): ID базиса поставки для фильтрации
    """
    filters = get_filters(
        oil_id=oil_id,
        delivery_type_id=delivery_type_id,
        delivery_basis_id=delivery_basis_id
    )

    return select(SpimexTradingResults).filter_by(**filters).order_by(
        desc(SpimexTradingResults.date)).limit(limit)
