import asyncio
from datetime import datetime
from decimal import Decimal

from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
import pandas as pd

from parse_utils import parse_href, download_file, save_filtered_csv
from models import Base, SpimexTradingResults
from config import DB_USER, DB_PASS, DB_HOST, DB_PORT, DB_NAME

times = ('25.08.2025',)
url = 'https://spimex.com/markets/oil_products/trades/results/'
DATABASE_URL = (
    f"postgresql+asyncpg://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

engine = create_async_engine(url=DATABASE_URL, echo=True)
async_session = async_sessionmaker(bind=engine)


async def create_database():
    async with engine.connect() as conn:
        await conn.run_sync(Base.metadata.create_all)
        await conn.commit()


async def process_time(time: str):
    href = await parse_href(url, time)
    if not href:
        raise ValueError(f"Не найден файл для времени {time}")
    file = await download_file(href)
    file_name = await save_filtered_csv(file, time)
    df = await asyncio.to_thread(pd.read_excel, file_name, engine='openpyxl')
    return df.to_dict(orient='records')


async def save_to_db(time: str, data: dict) -> None:
    spimex_objects = []

    for row in data:
        if row['Форма СЭТ-БТ'] == 'Итого:':
            break

        if row['Unnamed: 14'] != '-':

            date_obj = datetime.strptime(time, '%d.%m.%Y').date()

            spimex = SpimexTradingResults(
                exchange_product_id=row['Форма СЭТ-БТ'],
                exchange_product_name=row['Unnamed: 2'],
                oil_id=row['Форма СЭТ-БТ'][:4],
                delivery_basis_id=row['Форма СЭТ-БТ'][4:7],
                delivery_basis_name=row['Unnamed: 3'],
                delivery_type_id=row['Форма СЭТ-БТ'][-1],
                volume=int(row['Unnamed: 4']) if row['Unnamed: 4'] else None,
                total=Decimal(
                    str(row['Unnamed: 5'])) if row['Unnamed: 5'] else None,
                count=int(row['Unnamed: 14']) if row['Unnamed: 14'] else None,
                date=date_obj
            )
            spimex_objects.append(spimex)

    async with async_session() as session:
        session.add_all(spimex_objects)
        await session.commit()


async def main(times: tuple):
    await create_database()
    tasks = [process_time(time) for time in times]
    all_data = await asyncio.gather(*tasks)
    for time, data in zip(times, all_data):
        await save_to_db(time, data)

if __name__ == '__main__':
    asyncio.run(main(times))
