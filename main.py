import psycopg2
from hydra import compose, initialize
from omegaconf import OmegaConf
from py_scripts.transactions import Transactions

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
    PATH = ["input/2021-03-01/transactions_01032021.txt"]

    # Получить результат
    cursor_db.execute("SELECT version();")
    record = cursor_db.fetchone()
    print("Вы подключены к - ", record, "\n")

    # # Вставка данных в таблицу alex_DWH_FACT_transactions:
    # alex_DWH_FACT_transactions = Transactions(PATH[0])
    # print("Transfering in db alex_DWH_FACT_transactions")
    # cursor_db.executemany(alex_DWH_FACT_transactions.insert_table,
    #                       alex_DWH_FACT_transactions.data)
    




    conn_db.commit()



if __name__ == "__main__":
    main()