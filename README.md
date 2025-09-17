# Описание
Скрипт для автоматического сбора и обработки данных о торгах нефтепродуктами с официального сайта Spimex. С поддержкой api

## Стек технологий

* aiosqlite==0.21.0
* bs4==0.0.2
* pandas==2.3.1
* requests==2.32.4
* SQLAlchemy==2.0.42
* FastApi==0.116.1
* Pyadntic==2.11.7
* Redis==6.4.0
* pytest==8.4.2

## Запуск

Для запуска необходимо запустить docker compose:

1. Пропишите команду 

```
docker compose -f docker-compose.yml
```

## Запуск тестов

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

3. Припишите команду 

```
docker compose -f docker-compose-test.yml
```

4. Запустите тесты командой в терминале - pytest