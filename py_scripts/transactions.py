import pandas as pd

class Transactions:

    """База данных по транзакциям: формирование и сохранение в public"""

    def __init__(self, path_table: str):

        data = pd.read_csv(path_table, sep=";")
        self.data = data.values.tolist()
        for i in range(len(self.data)):
            self.data[i][2] = self.data[i][2].replace(",", ".")

    def insert_date_in_table(self, cursor_db, conn_db):
        cursor_db.executemany(""" INSERT INTO public.alex_DWH_FACT_transactions(
                                    trans_id,
                                    trans_date,
                                    amt,
                                    card_num,
                                    oper_type,
                                    oper_result,
                                    terminal
                                ) VALUES(%s, %s, %s, %s, %s, %s, %s) """,
                          self.data)
        conn_db.commit()