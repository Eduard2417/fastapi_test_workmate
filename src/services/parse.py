
import asyncio
from datetime import datetime
from decimal import Decimal
from io import BytesIO
from typing import Any, List, Dict, Tuple

import aiohttp
from bs4 import BeautifulSoup
import pandas as pd

from models.spimex import SpimexTradingResults

url = 'https://spimex.com/markets/oil_products/trades/results/'


async def parse_href(url: str, time: str) -> str | None:
    """
    Извлекает URL для скачивания файла с сайта Spimex по указанному времени.

    Параметры:
        url (str): URL страницы для парсинга
        time (str): Временная метка для поиска на странице

    Возвращает:
        str: Очищенный URL для скачивания, если найден, иначе None

    Пример:
        >>> url = "https://spimex.com/markets/oil_products/trades/results/"
        >>> download_url = parse_href(url, "10:30")
    """

    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            html = await response.text()

    soup = BeautifulSoup(html, 'html.parser')
    div = soup.find('div', id='comp_d609bce6ada86eff0b6f7e49e6bae904')
    for div_item in div.find_all('div', class_='accordeon-inner__wrap-item'):
        if div_item.find('span').text.strip() == time:
            a = div_item.find('a',
                              class_='accordeon-inner__item-title link xls')
            return f"https://spimex.com{a['href'].split('?')[0]}"
    else:
        return None


async def download_file(file_url: str) -> pd.DataFrame:
    """
    Скачивает Excel-файл по URL и возвращает его содержимое в виде DataFrame.

    Args:
        file_url (str): Ссылка на Excel-файл для скачивания

    Returns:
        pd.DataFrame: Данные из Excel-файла в виде таблицы pandas

    Пример:
        >>> df = download_file("https://example.com/data.xlsx")
    """

    async with aiohttp.ClientSession() as session:
        async with session.get(file_url) as response:
            file_content = await response.read()
            return pd.read_excel(BytesIO(file_content))


async def save_filtered_csv(df: pd.DataFrame, time: str) -> str | None:
    """
    Фильтрует DataFrame и сохраняет данные в Excel-файл.

    Ищет строку с указанной единицей измерения, сохраняет все данные после нее,
    начиная с 3 строк ниже найденной строки.

    Args:
        df (pd.DataFrame): Исходный DataFrame с данными для обработки
        time (str): Временная метка для использования в имени файла

    Returns:
        str | None: Имя сохраненного файла или None,
        если искомая строка не найдена

    Пример:
        >>> filename = save_filtered_csv(df, "12.05.2023")
        >>> print(f"Сохранено в {filename}")
    """

    csv_filename = f"spimex_{time.replace('.', '_')}.xls"
    mask = df.apply(
        lambda row: row.astype(str).str.contains(
            'Единица измерения: Метрическая тонна').any(), axis=1)
    if not mask.any():
        print("Не найдена строка с 'Единица измерения: Метрическая тонна'")
        return None
    start_idx = mask.idxmax() + 3
    filtered_df = df.iloc[start_idx:]
    await asyncio.to_thread(
        filtered_df.to_excel,
        csv_filename,
        index=False,
        engine='openpyxl'
    )
    return csv_filename


async def process_time(time: str) -> List[Dict[str, Any]]:
    """
    Обрабатывает данные для указанного времени: парсит URL, скачивает файл,
    сохраняет отфильтрованные данные и преобразует в словарь.

    Args:
        time (str): Временная метка в формате 'dd.mm.YYYY'

    Returns:
        List[Dict[str, Any]]: Список словарей с данными из Excel файла

    Raises:
        ValueError: Если не найден файл для указанного времени

    Пример:
        >>> data = await process_time("12.05.2023")
        >>> print(f"Получено {len(data)} записей")
    """

    href = await parse_href(url, time)
    if not href:
        raise ValueError(f"Не найден файл для времени {time}")
    file = await download_file(href)
    file_name = await save_filtered_csv(file, time)
    df = await asyncio.to_thread(pd.read_excel, file_name, engine='openpyxl')
    return df.to_dict(orient='records')


def get_objects(
        time: str,
        data: List[Dict[str, Any]]
        ) -> List[SpimexTradingResults]:
    """
    Преобразует сырые данные в список объектов SpimexTradingResults.

    Обрабатывает данные до встречи строки 'Итого:' и пропускает записи
    с значением '-' в колонке 'Unnamed: 14'.

    Args:
        time (str): Временная метка в формате 'dd.mm.YYYY'
        data (List[Dict[str, Any]]): Список словарей с сырыми данными

    Returns:
        List[SpimexTradingResults]: Список объектов торговых результатов Spimex

    Пример:
        >>> objects = get_objects("12.05.2023", raw_data)
        >>> print(f"Создано {len(objects)} объектов")
    """

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
    return spimex_objects


async def get_spimex(times: Tuple[str, ...]) -> List[SpimexTradingResults]:
    """
    Получает и обрабатывает данные Spimex для нескольких временных меток.

    Параллельно обрабатывает все указанные временные периоды и объединяет
    результаты в единый список объектов.

    Args:
        times (Tuple[str, ...]): Кортеж временных меток в формате 'dd.mm.YYYY'

    Returns:
        List[SpimexTradingResults]: Объединенный список объектов
        торговых результатов

    Пример:
        >>> times = ("12.05.2023", "13.05.2023")
        >>> results = await get_spimex(times)
        >>> print(f"Всего обработано {len(results)} записей")
    """

    tasks = [process_time(time) for time in times]
    all_data = await asyncio.gather(*tasks)
    result = []
    for time, data in zip(times, all_data):
        result.extend(get_objects(time, data))
    return result
