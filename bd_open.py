import sqlite3
import pandas as pd

pd.set_option('display.max_columns', None)  # Все столбцы
pd.set_option('display.max_rows', None)     # Все строки
pd.set_option('display.width', None)        # Вся ширина
pd.set_option('display.max_colwidth', None) # Полный текст в ячейках

conn = sqlite3.connect('hh_vacancies.db')
df = pd.read_sql("SELECT * FROM vacancies", conn)
print(df)

conn.close()