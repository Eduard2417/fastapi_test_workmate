from typing import List

from fastapi import FastAPI, status
from contextlib import asynccontextmanager

from services.spimex import (get_all_spimex, get_last_spimex,
                             get_dynamics_spimex,
                             get_trading_results_spimex)
from services.parse import get_spimex
from core.database import create_database, engine
from core.dependencies import session_depend
from core.cache import get_cache, set_cache
from schemas.spimex import (SpimexDateModel, SpimexLimitModel,
                            SpimexModel, SpimexBetweenModel,
                            SpimexLastModel)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await create_database()
    yield
    await engine.dispose()


app = FastAPI(lifespan=lifespan)


@app.post('/create_spimex')
async def create_spimex(session: session_depend, date: SpimexDateModel):
    spimexes = await get_spimex(date.date)
    session.add_all(spimexes)
    await session.commit()
    return {'ok': status.HTTP_201_CREATED}


@app.get('/all',
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


@app.get('/get_last_trading_dates')
async def get_last_trading_dates(
        session: session_depend,
        limit: SpimexLimitModel
        ):
    cached_key = f'last_trading_dates:{limit.limit}'
    cached_data = await get_cache(cached_key)
    if cached_data:
        return cached_data

    stmt = get_last_spimex(limit=limit.limit)
    result = await session.execute(stmt)
    data = result.scalars().all()

    if data:
        await set_cache(cached_key, data)

    return data


@app.get('/get_dynamics')
async def get_dynamics(
        session: session_depend,
        dynamics_spimex: SpimexBetweenModel
        ):
    cached_key = f'get_dynamics:{
        dynamics_spimex.oil_id,
        dynamics_spimex.delivery_basis_id,
        dynamics_spimex.delivery_type_id,
        dynamics_spimex.start_date,
        dynamics_spimex.end_date}'
    cached_data = await get_cache(cached_key)
    if cached_data:
        return cached_data

    stmt = get_dynamics_spimex(
        oil_id=dynamics_spimex.oil_id,
        delivery_basis_id=dynamics_spimex.delivery_basis_id,
        delivery_type_id=dynamics_spimex.delivery_type_id,
        start_date=dynamics_spimex.start_date,
        end_date=dynamics_spimex.end_date,
    )
    result = await session.execute(stmt)
    data = result.scalars().all()

    if data:
        await set_cache(cached_key, data, to_dict=True)

    return data


@app.get('/get_trading_results', status_code=status.HTTP_200_OK)
async def get_trading_results(
        session: session_depend,
        last_limit: SpimexLastModel
        ):
    cached_key = f'trading_results:{
        last_limit.oil_id,
        last_limit.delivery_basis_id,
        last_limit.delivery_type_id,
        last_limit.limit}'
    cached_data = await get_cache(cached_key)
    if cached_data:
        return cached_data
    stmt = get_trading_results_spimex(
        oil_id=last_limit.oil_id,
        delivery_basis_id=last_limit.delivery_basis_id,
        delivery_type_id=last_limit.delivery_type_id,
        limit=last_limit.limit)
    result = await session.execute(stmt)
    data = result.scalars().all()

    if data:
        await set_cache(cached_key, data, to_dict=True)

    return data
