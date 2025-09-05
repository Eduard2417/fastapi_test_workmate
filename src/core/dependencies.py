from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Annotated

from core.database import get_session

session_depend = Annotated[AsyncSession, Depends(get_session)]
