from typing import Tuple
from decimal import Decimal
import datetime as dt

from pydantic import BaseModel, constr, Field, ConfigDict


class SpimexDateModel(BaseModel):
    date: Tuple[constr(min_length=10, max_length=10), ...]


class SpimexLimitModel(BaseModel):
    limit: int = Field(ge=1, le=100)


class SpimexBaseModel(BaseModel):
    oil_id: str | None = None
    delivery_basis_id: str | None = None
    delivery_type_id: str | None = None


class SpimexBetweenModel(SpimexBaseModel):
    start_date: dt.date
    end_date: dt.date

    model_config = ConfigDict(extra='forbid')


class SpimexLastModel(SpimexBaseModel):
    limit: int = 10

    model_config = ConfigDict(extra='forbid')


class SpimexModel(SpimexBaseModel):
    id: int
    exchange_product_id: str
    exchange_product_name: str
    delivery_basis_name: str
    volume: int | None
    total: Decimal | None
    count: int | None
    date: dt.date
    created_on: dt.datetime
    updated_on: dt.datetime

    model_config = ConfigDict(from_attributes=True,
                              arbitrary_types_allowed=True)
