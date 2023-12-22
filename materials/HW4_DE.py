from sqlalchemy import create_engine
import pandas as pd
import numpy as np

# подключение к серверу с БД
DIAL = "postgresql"
DRV = "psycopg2"
HOST = "rc1b-o3ezvcgz5072sgar.mdb.yandexcloud.net"
PORT = "6432"
BD = "db"
LOGIN = "hse"
PAS = "hsepassword"

url_req = f"{DIAL}+{DRV}://{LOGIN}:{PAS}@{HOST}:{PORT}/{BD}"
print(url_req)
print("-------------------------")

eng = create_engine(url_req)
connect = eng.connect()
print(connect)
print("-------------------------")

# таблица med_an_name
df_med_an_name = pd.read_sql_table('med_an_name', connect, schema='de')
df_med_an_name['id'] = df_med_an_name['id'].astype(str)
print("df_med_an_name")
print(df_med_an_name.head())
print("-------------------------")

# таблица med_name
df_med_name = pd.read_sql_table('med_name', connect, schema='de')
print("df_med_name")
print(df_med_name.head())
print("-------------------------")
connect.close()

# считываем эксел файл с диагнозами
df_medicine = pd.read_excel('medicine.xlsx', sheet_name='hard')
df_medicine['Анализ'] = df_medicine['Анализ'].astype(str)
print("df_medicine")
print(df_medicine.head())
print("-------------------------")

# осуществим слияние таблиц df_medicine и df_med_an_name по столбцам Анализ и id,
# так как это один и тотже столбец
df = pd.merge(df_medicine, df_med_an_name, right_on='id', left_on='Анализ', how='left')
df['is_simple'] = df['is_simple'].astype(str)

# создадим столбец num, где отразим только те показатели, что оцениваются цифрой.
# те значения, что являются качественными заменятся на Nan.
df.loc[df['is_simple'] == "N", 'num'] = pd.to_numeric(df['Значение'], errors='coerce')
df = df.drop('id', axis= 1)

# создадим столбец word, где отразим только те показатели, что оцениваются словами или символами.
# те значения, что являются цифровыми заменятся на Nan.
df.loc[df['is_simple'] == "Y", 'word'] = df['Значение']

# тут представлена однотипная операция заполнения столбца 'Заключение' с кучей условий
df.loc[((df['is_simple'] == "N") & (df['num'] > df['max_value'])), 'Заключение'] = "Повышен"
df.loc[((df['is_simple'] == "N") & (df['num'] < df['min_value'])), 'Заключение'] = "Понижен"
df.loc[((df['is_simple'] == "N") & (df['num'] >= df['min_value']) & \
        (df['num'] <= df['max_value'])), 'Заключение'] = "Норма"
df.loc[((df['is_simple'] == "Y") & ((df['word'].str.find("Пол") != -1) | \
        (df['word'].str.find("+") != -1))), 'Заключение'] = "Положительный"
df.loc[((df['is_simple'] == "Y") & ((df['word'].str.find("Отр") != -1) | \
        (df['word'].str.find("-") != -1))), 'Заключение'] = "Норма"
print("df")
print(df.head())
print("-------------------------")

# Если заключение не норма, то заполним этими пациентами df_lech и выведем тех,
# у которых больше двух отклонение в показателях
df_lech = df[df['Заключение'] != 'Норма']
df_lech = df_lech[df_lech.groupby('Код пациента')['Код пациента'].transform('size') >= 2]
print("df_lech")
print(df_lech.head())
print("-------------------------")

# формируем результат вывода
alex_med_results = pd.merge(df_med_name[['phone', 'name', 'id']], \
        df_lech[['name','Заключение','Код пациента' ]], left_on='id', right_on='Код пациента')
alex_med_results = alex_med_results.drop(['Код пациента','id'], axis=1)
alex_med_results.rename(columns = {'phone':' Телефон', 'name_x':'ФИО', \
        'name_y':'Анализ'}, inplace = True)
print("alex_med_results")
print(alex_med_results)
print("-------------------------")

# отправить результат в public и сохраняем .xlsx
eng = create_engine(url_req)
connect = eng.connect()
print(connect)
alex_med_results.to_sql(name='alex_med_results', con=eng, schema='public', \
        if_exists='replace', index=False)
with pd.ExcelWriter('alex_med_results.xlsx') as writer:
    alex_med_results.to_excel(writer, index= False)
connect.close()