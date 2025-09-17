from typing import List
import datetime as dt

from fastapi import status, Query, APIRouter

from services.spimex import (get_all_spimex, get_last_spimex,
                             get_dynamics_spimex,
                             get_trading_results_spimex)
from services.parse import get_spimex
from core.dependencies import session_depend
from core.cache import get_cache, set_cache
from schemas.spimex import (SpimexDateModel,
                            SpimexModel)


router = APIRouter()


@router.post('/create_spimex', status_code=201)
async def create_spimex(session: session_depend, date: SpimexDateModel):
    spimexes = await get_spimex(date.date)
    session.add_all(spimexes)
    await session.commit()
    return {'ok': status.HTTP_201_CREATED}


@router.get('/all',
            status_code=status.HTTP_200_OK,
            response_model=List[SpimexModel])
async def get_all(session: session_depend):
    cached_key = 'all'
    cached_data = await get_cache(cached_key)
    if cached_data:
        return cached_data
    stmt = get_all_spimex()
    result = await session.execute(stmt)
    data = result.scalars().all()
    if data:
        await set_cache(cached_key, data, to_dict=True)
    return data


@router.get('/get_last_trading_dates')
async def get_last_trading_dates(
        session: session_depend,
        limit: int = Query(5, ge=1, le=100)
        ):
    cached_key = f'last_trading_dates:{limit}'
    cached_data = await get_cache(cached_key)
    if cached_data:
        return cached_data

    stmt = get_last_spimex(limit=limit)
    result = await session.execute(stmt)
    data = result.scalars().all()

    if data:
        await set_cache(cached_key, data)

    return data


@router.get('/get_dynamics')
async def get_dynamics(
        session: session_depend,
        oil_id: str = Query(None, max_length=25),
        delivery_basis_id: str = Query(None, max_length=25),
        delivery_type_id: str = Query(None, max_length=25),
        start_date: dt.date = Query(),
        end_date: dt.date = Query()

        ):
    cached_key = (f'get_dynamics:{oil_id}_{delivery_basis_id}_'
                  f'{delivery_type_id}_{start_date}_{end_date}')
    cached_data = await get_cache(cached_key)
    if cached_data:
        return cached_data

    stmt = get_dynamics_spimex(
        oil_id=oil_id,
        delivery_basis_id=delivery_basis_id,
        delivery_type_id=delivery_type_id,
        start_date=start_date,
        end_date=end_date,
    )
    result = await session.execute(stmt)
    data = result.scalars().all()

    if data:
        await set_cache(cached_key, data, to_dict=True)

    return data


@router.get('/get_trading_results', status_code=status.HTTP_200_OK)
async def get_trading_results(
        session: session_depend,
        oil_id: str = Query(None, max_length=25),
        delivery_basis_id: str = Query(None, max_length=25),
        delivery_type_id: str = Query(None, max_length=25),
        limit: int = Query(5, ge=1, le=100)
        ):
    cached_key = (f'trading_results:{oil_id}_{delivery_basis_id}_'
                  f'{delivery_type_id}_{limit}')
    cached_data = await get_cache(cached_key)
    if cached_data:
        return cached_data
    stmt = get_trading_results_spimex(
        oil_id=oil_id,
        delivery_basis_id=delivery_basis_id,
        delivery_type_id=delivery_type_id,
        limit=limit)
    result = await session.execute(stmt)
    data = result.scalars().all()

    if data:
        await set_cache(cached_key, data, to_dict=True)

    return data
