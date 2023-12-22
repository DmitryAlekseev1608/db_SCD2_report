import pandas as pd
import tqdm

class Transactions:

    """База данных по транзакциям: формирование и сохранение в public"""

    def __init__(self, path_table: str):
        print("Create transactions")
        data = pd.read_csv(path_table, sep=";")
        self.data = data.values.tolist()
        for i in tqdm.tqdm(range(len(self.data))):
            self.data[i][2] = self.data[i][2].replace(",", ".")

        self.insert_table = """ INSERT INTO public.alex_DWH_FACT_transactions(
                                trans_id,
                                trans_date,
                                amt,
                                card_num,
                                oper_type,
                                oper_result,
                                terminal
                            ) VALUES(%s, %s, %s, %s, %s, %s, %s) """


