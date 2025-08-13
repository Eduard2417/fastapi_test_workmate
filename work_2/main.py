import asyncio
from datetime import datetime
import os

from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
import pandas as pd

from parse_utils import parse_href, download_file, save_filtered_csv
from models import Base, SpimexTradingResults

time = '13.08.2025'
url = 'https://spimex.com/markets/oil_products/trades/results/'
DATABASE_URL = "sqlite+aiosqlite:///database.sqlite"

engine = create_async_engine(url=DATABASE_URL, echo=True)
async_session = async_sessionmaker(bind=engine)


async def create_database():
    async with engine.connect() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def main(clear: bool = True) -> None:
    """
    Основная асинхронная функция для обработки и сохранения данных с Spimex.

    Выполняет последовательно:
    1. Создает структуру БД
    2. Получает данные по указанному URL и времени
    3. Фильтрует и сохраняет данные во временный файл
    4. Загружает данные в базу данных
    5. Удаляет excel файл (по умолчанию)

    Args:
        clear (bool): Флаг для удаления excel файла после обработки.
                     По умолчанию True (файл удаляется).

    Exceptions:
        ValueError: Если не удалось найти файл для указанного времени

    Логика обработки данных:
        - Пропускает строки с 'Итого:' в колонке 'Форма СЭТ-БТ'
        - Пропускает строки с '-' в колонке 'Unnamed: 14'
        - Преобразует дату из строки в объект date
        - Сохраняет отфильтрованные данные в БД

    Пример использования:
        >>> asyncio.run(main(clear=True))
    """
    await create_database()

    href = await parse_href(url, time)
    if not href:
        raise ValueError(f"Не найден файл для времени {time}")
    file = await download_file(href)
    file_name = await save_filtered_csv(file, time)

    df = pd.read_excel(file_name)
    data = df.to_dict(orient='records')

    async with async_session() as session:
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
                    volume=row['Unnamed: 4'],
                    total=row['Unnamed: 5'],
                    count=row['Unnamed: 14'],
                    date=date_obj
                )
                session.add(spimex)
        await session.commit()

    if clear:
        os.remove(file_name)


if __name__ == '__main__':
    asyncio.run(main())
