from decimal import Decimal
import datetime as dt

from sqlalchemy.orm import declarative_base, Mapped, mapped_column
from sqlalchemy import text

Base = declarative_base()


class SpimexTradingResults(Base):
    __tablename__ = 'spimex_trading_results'

    id: Mapped[int] = mapped_column(primary_key=True)
    exchange_product_id: Mapped[str]
    exchange_product_name: Mapped[str]
    oil_id: Mapped[str]
    delivery_basis_id: Mapped[str]
    delivery_basis_name: Mapped[str]
    delivery_type_id: Mapped[str]
    volume: Mapped[int | None]
    total: Mapped[Decimal | None]
    count: Mapped[int | None]
    date: Mapped[dt.datetime]
    created_on: Mapped[dt.datetime] = mapped_column(
        server_default=text("(datetime('now', 'localtime'))"))

    updated_on: Mapped[dt.datetime] = mapped_column(
        server_default=text("(datetime('now', 'localtime'))"),
        onupdate=text("(datetime('now', 'localtime'))"))
