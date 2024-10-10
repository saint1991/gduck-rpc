from datetime import date, datetime, time
from decimal import Decimal
from typing import TypeAlias

from dateutil.relativedelta import relativedelta

Value: TypeAlias = bool | int | float | Decimal | str | datetime | date | time | relativedelta | None
