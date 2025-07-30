## Описание
Скрипт для автоматического сбора и обработки данных о торгах нефтепродуктами с официального сайта Spimex. 

# Стек технологий

* aiosqlite==0.21.0
* bs4==0.0.2
* pandas==2.3.1
* requests==2.32.4
* SQLAlchemy==2.0.42

# Запуск

Для запуска необходимо:

1. Создайте или активируйте виртуальное окружение (Не обязательный пункт)
Windows
```
source venv/script/activate
```
MacOS Linux
```
source /venv/bin/activate
```

2. Обновите pip и установите зависимости
```
python -m pip install --upgrade pip
```
```
pip install -r requirements.txt
```

3. Запуск сприпта 

Пример использования:

```
python main.py
```