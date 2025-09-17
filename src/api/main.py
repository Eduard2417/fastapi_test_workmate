from fastapi import FastAPI
from contextlib import asynccontextmanager

from core.database import create_database, engine
from api.routes import router


@asynccontextmanager
async def lifespan(app: FastAPI):
    await create_database()
    yield
    await engine.dispose()


app = FastAPI(lifespan=lifespan)


app.include_router(router)
