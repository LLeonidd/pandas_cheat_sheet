"""
Иногда необходимо пометить ячейки датафрейма определенной меткой, чтобы в дальнейшем обработать ее. 
Например при экспорте в html обернуть такие ячейки в тег <span> и подсветить их цветом. 
"""
import pandas as pd
from dataclasses import dataclass



def make_pivot() -> pd.DataFrame:
    """
    Создание моковой сводной таблицы
    """
    df = pd.DataFrame(columns=['date', 'store', 'time_start', 'time_end', 'program'])
    df['date'] = [f'{day}-11-2022' for day in range(10, 15)]
    df['store'] = [f'store_{store}' for store in range(1, 6)]
    df['time_start'] = [f'{hour}:00:00' for hour in range(10, 15)]
    df['time_end'] = [f'{hour}:00:00' for hour in range(16, 21)]
    df['program'] = [program for program in range(1, 6)]
    return df.pivot_table(
        index=('store', 'date'),
        columns='program',
        aggfunc={
            'program': 'count',
            'time_start': 'min',
            'time_end': 'max'
        }
    )


@dataclass
class ErrorLabel:
    val: any
    
    def to_html_format(self):
        ...


def check_error(cell):
    if isinstance(cell, ErrorLabel):
        return cell.to_html_format()
    else:
        return cell


if __name__ == '__main__':
    df = make_pivot()
    for index in df.index:
        for column in df.columns:
            if column == ('time_start', 1):
                val = 10 if index[0] == 'store_3' else 2
                df.at[index, column] = ErrorLabel(val=val)

    df = df.applymap(check_error)
    print(df['time_start'])
