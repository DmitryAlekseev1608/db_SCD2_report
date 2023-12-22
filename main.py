import psycopg2
from hydra import compose, initialize
from omegaconf import OmegaConf
from py_scripts.transactions import Transactions
from py_scripts.terminals import Terminals

def main():

    # загружаем конфигурации
    initialize(version_base=None, config_path="configs", job_name="app")
    cfg = compose(config_name="configs")

    # подключение к серверу с БД
    conn_db = psycopg2.connect(database=cfg.db.BD,
                        host=cfg.db.HOST,
                        user=cfg.db.LOGIN,
                        password=cfg.db.PAS,
                        port=cfg.db.PORT)
    
    # Отключение автокоммита
    conn_db.autocommit = False

    # Создание курсора
    cursor_db = conn_db.cursor()
    PATH = {"2021-03-01": ["input/2021-03-01/transactions_01032021.txt",
                           "input/2021-03-01/terminals_01032021.xlsx"]}

    # Получить результат
    cursor_db.execute("SELECT version();")
    record = cursor_db.fetchone()
    print("Вы подключены к - ", record, "\n")

    # print("Вставка данных в таблицу alex_DWH_FACT_transactions")
    # alex_DWH_FACT_transactions = Transactions(PATH["2021-03-01"][0])
    # cursor_db.executemany(alex_DWH_FACT_transactions.insert_table,
    #                       alex_DWH_FACT_transactions.data)
    
    print("Вставка данных в таблицу public.alex_DWH_DIM_terminals_HIST")
    alex_DWH_DIM_terminals_HIST = Terminals(PATH["2021-03-01"][1])
    
    print("1. Очистка стейджинговых таблиц")
    cursor_db.execute(alex_DWH_DIM_terminals_HIST.del_stage)

    print("2. Захват данных из источника (измененных с момента последней загрузки) в стейджинг")   
    cursor_db.executemany(alex_DWH_DIM_terminals_HIST.copy_source,
                           alex_DWH_DIM_terminals_HIST.data)


    conn_db.commit()

if __name__ == "__main__":
    main()