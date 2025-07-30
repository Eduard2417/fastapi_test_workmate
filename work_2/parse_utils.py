import requests
from io import BytesIO

import pandas as pd
from bs4 import BeautifulSoup


def parse_href(url: str, time: str) -> str | None:
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
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    div = soup.find('div', id='comp_d609bce6ada86eff0b6f7e49e6bae904')
    for div_item in div.find_all('div', class_='accordeon-inner__wrap-item'):
        if div_item.find('span').text.strip() == time:
            a = div_item.find('a',
                              class_='accordeon-inner__item-title link xls')
            return f"https://spimex.com{a['href'].split('?')[0]}"
    else:
        return None


def download_file(file_url: str) -> pd.DataFrame:
    """
    Скачивает Excel-файл по URL и возвращает его содержимое в виде DataFrame.

    Args:
        file_url (str): Ссылка на Excel-файл для скачивания

    Returns:
        pd.DataFrame: Данные из Excel-файла в виде таблицы pandas

    Пример:
        >>> df = download_file("https://example.com/data.xlsx")
    """
    file_content = requests.get(file_url).content
    return pd.read_excel(BytesIO(file_content))


def save_filtered_csv(df: pd.DataFrame, time: str) -> str | None:
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
    filtered_df.to_excel(csv_filename, index=False, engine='openpyxl')
    return csv_filename
