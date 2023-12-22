import pandas as pd
import tqdm

class Terminals:

    """База данных по терминалам: формирование и сохранение в public"""

    def __init__(self, path_table: str):

        data = pd.read_excel(path_table)
        self.data = data.values.tolist()
        for i in tqdm.tqdm(range(len(self.data))):
            self.data[i].append(path_table[6:16])

        self.del_stage = """ delete from public.alex_STG_terminals;
                            delete from public.alex_STG_terminals_del;
                        """
        self.copy_source = """ insert into public.alex_STG_terminals(
                                terminal_id,
                                terminal_type,
                                terminal_city,
                                terminal_address,
                                update_dt
                                ) VALUES(%s, %s, %s, %s, %s)
                            where update_dt > ( select max_update_dt
                                                from table public.alex_META_meta
                                                where schema_name='public'
                                                and table_name='public.alex_DWH_FACT_transactions' 
                                                );
                            """
        
