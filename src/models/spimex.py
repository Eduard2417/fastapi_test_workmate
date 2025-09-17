from decimal import Decimal
import datetime as dt

from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy import text, Numeric

from core.database import Base


class SpimexTradingResults(Base):
    __tablename__ = 'spimex_trading_results'
    __table_args__ = {'extend_existing': True}

    id: Mapped[int] = mapped_column(primary_key=True)
    exchange_product_id: Mapped[str]
    exchange_product_name: Mapped[str]
    oil_id: Mapped[str]
    delivery_basis_id: Mapped[str]
    delivery_basis_name: Mapped[str]
    delivery_type_id: Mapped[str]
    volume: Mapped[int | None]
    total: Mapped[Decimal | None] = mapped_column(Numeric(10, 2))
    count: Mapped[int | None]
    date: Mapped[dt.date]
    created_on: Mapped[dt.datetime] = mapped_column(
        server_default=text("CURRENT_TIMESTAMP"))

    updated_on: Mapped[dt.datetime] = mapped_column(
        server_default=text("CURRENT_TIMESTAMP"),
        onupdate=text("CURRENT_TIMESTAMP"))

    def to_dict(self):
        return {
            'id': self.id,
            'exchange_product_id': self.exchange_product_id,
            'exchange_product_name': self.exchange_product_name,
            'oil_id': self.oil_id,
            'delivery_basis_id': self.delivery_basis_id,
            'delivery_basis_name': self.delivery_basis_name,
            'delivery_type_id': self.delivery_type_id,
            'volume': self.volume,
            'total': str(self.total),
            'count': self.count,
            'date': self.date.isoformat(),
            'created_on': self.created_on.isoformat(),
            'updated_on': self.updated_on.isoformat()
        }
