import os
import shutil
from hydra import compose, initialize
import psycopg2

def backup():

    name_files_dir = os.listdir('archive')
    
    for name in name_files_dir:
        if name != '.txt':
            shutil.move(f"archive/{name}", f"input/{name[:-7]}")

    # загружаем конфигурации
    initialize(version_base=None, config_path="../configs", job_name="app")
    cfg = compose(config_name="configs")

    # подключение к серверу с БД
    conn_db = psycopg2.connect(database=cfg.db.BD,
                        host=cfg.db.HOST,
                        user=cfg.db.LOGIN,
                        password=cfg.db.PAS,
                        port=cfg.db.PORT)
    
    # отключение автокоммита
    conn_db.autocommit = False

    # создание курсора
    cursor_db = conn_db.cursor()
    
    cursor_db.execute("""DROP TABLE public.alex_META_meta;
                      DROP TABLE public.alex_DWH_FACT_transactions;
                      DROP TABLE public.alex_DWH_DIM_terminals_HIST;    
                      DROP TABLE public.alex_STG_terminals;
                      DROP TABLE public.alex_STG_terminals_del;
                      DROP TABLE public.alex_REP_FRAUD;
                      DROP TABLE public.alex_DWH_FACT_passport_blacklist;
                      DROP TABLE public.alex_STG_passport_blacklist;    
                      DROP TABLE public.alex_STG_passport_blacklist_del;   
                      DROP TABLE public.alex_DWH_DIM_cards_HIST;
                      DROP TABLE public.alex_STG_cards;    
                      DROP TABLE public.alex_STG_cards_del;  
                      DROP TABLE public.alex_DWH_DIM_account_HIST;  
                      DROP TABLE public.alex_STG_account;  
                      DROP TABLE public.alex_STG_account_del;                                                                                    
                        """)   

    conn_db.commit()
    cursor_db.close()
    conn_db.close()

if __name__ == "__main__":
    backup()