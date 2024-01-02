import pandas as pd

class PassportBlackList:
    """База данных по паспортам из черных списсков: формирование и сохранение в public"""

    def __init__(self, path_table: str):

        source = pd.read_excel(path_table)
        self.data = source.values.tolist()

    def insert_date_in_table(self, cursor_db, conn_db):

        cursor_db.execute(""" delete from public.alex_DWH_FACT_passport_blacklist; """)
        conn_db.commit()

        cursor_db.executemany(""" INSERT INTO public.alex_DWH_FACT_passport_blacklist(
                                entry_dt,
                                passport_num
                                ) VALUES(%s, %s) """,
                          self.data)
        conn_db.commit()