from pandas import Timestamp
from enum import Enum
import pandas as pd
from typing import List


class T_GRANU(Enum):
    HOUR = 1
    DAY = 2
    MONTH = 3
    QUARTER = 4
    YEAR = 5


scale_dict = {
    T_GRANU.HOUR: "hour",
    T_GRANU.DAY: "day",
    T_GRANU.MONTH: "month",
    T_GRANU.QUARTER: "quarter",
    T_GRANU.YEAR: "year",
}


class Datetime:
    # def __init__(self, dt: Timestamp, chain: List[T_GRANU]):
    #     self.chain = chain
    #     self.full_resolution = []
    #     dt_dict = {
    #         "hour": str(dt.hour).zfill(2),
    #         "day": str(dt.day).zfill(2),
    #         "month": str(dt.month).zfill(2),
    #         "quarter": str(dt.quarter).zfill(1),
    #         "year": str(dt.year).zfill(4),
    #     }
    #     for t_scale in chain:
    #         scale = scale_dict[t_scale]
    #         self.full_resolution.append(dt_dict[scale])

    def __init__(self, dt: Timestamp) -> None:
        # self.dt = dt
        self.hour = str(dt.hour).zfill(2)
        self.day = str(dt.day).zfill(2)
        self.month = str(dt.month).zfill(2)
        self.quarter = str(dt.quarter).zfill(1)
        self.year = str(dt.year).zfill(4)
        # self.full_resolution = [
        #     str(self.hour).zfill(2),
        #     str(self.day).zfill(2),
        #     str(self.month).zfill(2),
        #     str(self.quarter).zfill(1),
        #     str(self.year).zfill(4),
        # ]

    def transform(self, granu: T_GRANU):
        if granu == T_GRANU.HOUR:
            return [self.year, self.month, self.day, self.hour]
        elif granu == T_GRANU.DAY:
            return [self.year, self.month, self.day]
        elif granu == T_GRANU.MONTH:
            return [self.year, self.month]
        elif granu == T_GRANU.QUARTER:
            return [self.year, self.quarter]
        elif granu == T_GRANU.YEAR:
            return [self.year]

    def __transform(self, granu: T_GRANU):
        idx = granu - self.chain[0].value
        return list(reversed(self.full_resolution[idx:]))

    def to_str(self, repr):
        return "-".join([x for x in repr])

    def to_int(self, repr):
        return int("".join([x for x in repr]))

    def transform_to_key(self, granu: T_GRANU):
        repr = self.full_resolution[granu - 1 :]
        return str(repr)


def parse_datetime(dt: Timestamp):
    if pd.isnull(dt):
        return None
    return Datetime(dt)


def transform(dt: Datetime, granu: T_GRANU):
    return dt.full_resolution[granu - 1 :]


def set_temporal_granu(dt: Datetime, granu: T_GRANU):
    if dt is None:
        return None
    return dt.to_str(dt.transform(granu))


def dt_to_str(dt):
    resolution = dt[::-1]
    res = ""
    for i, token in enumerate(resolution):
        res += str(token)
        if i + 1 != len(resolution):
            res += str("-")
    return res