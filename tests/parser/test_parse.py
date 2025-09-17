from decimal import Decimal
import unittest
from unittest.mock import patch
from io import BytesIO

import pytest
import pandas as pd
from aioresponses import aioresponses

from services.parse import (url, parse_href, download_file,
                            save_filtered_csv, process_time,
                            get_objects, get_spimex)


class TestParse:
    """Тесты для модуля парсинга данных Spimex."""

    @pytest.mark.asyncio
    async def test_parse_href(self, get_time):
        """Тест парсинга ссылки из HTML контента."""

        time_to_find = get_time
        html_content = f"""
            <div id="comp_d609bce6ada86eff0b6f7e49e6bae904">
                <div class="accordeon-inner__wrap-item">
                    <span>{time_to_find}</span>
                    <a class="accordeon-inner__item-title link xls"
                    href="/some/path/file.xls?param=value">Скачать</a>
                </div>
            </div>
            """

        with aioresponses() as m:
            m.get(url, body=html_content, status=200)
            result = await parse_href(url, time_to_find)
            assert result == "https://spimex.com/some/path/file.xls"

    @pytest.mark.asyncio
    async def test_download_file(self):
        """Тест загрузки файла с данными."""

        test_data = pd.DataFrame({'Column1': [1, 2, 3],
                                  'Column2': ['A', 'B', 'C']})

        buffer = BytesIO()
        test_data.to_excel(buffer, index=False, engine='openpyxl')

        with aioresponses() as m:
            m.get(url, body=buffer.getvalue(), status=200)
            result = await download_file(url)
            pd.testing.assert_frame_equal(result, test_data)

    @pytest.mark.asyncio
    async def test_save_filtered_csv(self):
        """Тест сохранения отфильтрованного CSV файла."""

        test_df = pd.DataFrame({
            'Column1': ['Единица измерения: Метрическая тонна', 'data1'],
            'Column2': ['extra info', 'data2']
        })

        with patch('builtins.open', unittest.mock.mock_open()), \
             patch('pandas.DataFrame.to_excel') as mock_to_excel:

            result = await save_filtered_csv(test_df, '12.09.2025')
            assert result == "spimex_12_09_2025.xls"
            mock_to_excel.assert_called_once()

    @pytest.mark.asyncio
    async def test_process_time(self, get_time):
        """Тест обработки данных по времени."""

        test_time = get_time
        test_df = pd.DataFrame({'col1': [1, 2], 'col2': ['a', 'b']})

        with patch('services.parse.parse_href', return_value="test_url"), \
             patch('services.parse.download_file', return_value=test_df), \
             patch('services.parse.save_filtered_csv',
                   return_value="test.xls"), \
             patch('pandas.read_excel', return_value=test_df):

            result = await process_time(test_time)
            assert result == test_df.to_dict('records')

    def test_get_objects(self, get_row_spimex):
        """Тест преобразования сырых данных в объекты Spimex."""

        time = "12.09.2025"

        result = get_objects(time, get_row_spimex[0])

        assert len(result) == 1
        assert result[0].exchange_product_id == 'A001B02C'
        assert result[0].volume == 100
        assert result[0].total == Decimal('5000.50')
        assert result[0].count == 5

    @pytest.mark.asyncio
    async def test_get_spimex(self, get_row_spimex):
        """Тест основного процесса получения данных Spimex."""

        test_times = ("12.09.2025", "13.09.2025")

        with patch('services.parse.process_time', side_effect=get_row_spimex):
            result = await get_spimex(test_times)

            assert len(result) == 2
            assert result[0].exchange_product_id == 'A001B02C'
            assert result[1].exchange_product_id == 'A002B03D'
