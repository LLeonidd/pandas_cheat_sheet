"""
Скорретировать датафрейм так, что будет  1 поле даты, которое
соответствует вромени t_start или t_end события.

Пример.
Исходный датафрейм:
      store   t_start     d_start     t_end       d_end  program
0  Avangard  23:00:00  01-12-2022  23:59:00  01-12-2022        1
1  Avangard  23:00:01  01-12-2022  02:00:02  02-12-2022        1


Выходной датафрейм:
      store        date   t_start     t_end  program
0  Avangard  01-12-2022  23:00:00  23:59:00        1
1  Avangard  01-12-2022  23:00:01       NaN        1
2  Avangard  02-12-2022       NaN  02:00:02        1
"""
import typing
import numpy as np
import pandas as pd
from collections import namedtuple


DfContainer = namedtuple('DfContainer', ['store', 'date', 't_start', 't_end', 'program'])


def make_simple_df() -> pd.DataFrame:
    """Создать исходный датафрейм."""
    df = pd.DataFrame({
        'store': ['Avangard', 'Avangard'],
        't_start': ['23:00:00', '23:00:01' ],
        'd_start': ['01-12-2022', '01-12-2022'],
        't_end': ['23:59:00', '02:00:02'],
        'd_end': ['01-12-2022', '02-12-2022'],
        'program': [1, 1],
    })
    return df


def merge_date_df(df: pd.DataFrame) -> typing.Generator[DfContainer, None, None]:
    """Сгенерирвоать данные для нового дотафрейма с фиксированной датой."""
    for _, row in df.iterrows():
        if row['d_start'] != row['d_end']:
            yield DfContainer(row.store, row.d_start, row.t_start, np.nan, row.program)
            yield DfContainer(row.store, row.d_end, np.nan, row.t_end, row.program)
        else:
            yield DfContainer(row.store, row.d_start, row.t_start, row.t_end, row.program)


if __name__ == '__main__':
    df_ = make_simple_df()
    result_df = pd.DataFrame(merge_date_df(df_), columns=DfContainer._fields)
    print(df_, "\n" * 2)
    print(result_df)
