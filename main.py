import psycopg2
from hydra import compose, initialize
from py_scripts.transactions import Transactions
from py_scripts.terminals import Terminals
from py_scripts.files_operations import save_name_file, trasfer_file
from py_scripts.fraud import Fraud

def main():

    # считываем имена файлов
    name_files_dir_blacklist, name_files_dir_terminals, name_files_dir_transactions = save_name_file()

    # загружаем конфигурации
    initialize(version_base=None, config_path="configs", job_name="app")
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

    # Получить результат
    cursor_db.execute("SELECT version();")
    record = cursor_db.fetchone()
    print("Вы подключены к - ", record, "\n")

    # работа с alex_DWH_FACT_transactions
    alex_DWH_FACT_transactions = Transactions(f"input/{name_files_dir_transactions}")
    alex_DWH_FACT_transactions.insert_date_in_table(cursor_db, conn_db)

    # работа с public.alex_DWH_DIM_terminals_HIST
    alex_DWH_DIM_terminals_HIST = Terminals(f"input/{name_files_dir_terminals}")
    alex_DWH_DIM_terminals_HIST.insert_date_in_table(cursor_db, conn_db)

    # работа с public.alex_REP_FRAUD
    alex_REP_FRAUD = Fraud(name_files_dir_transactions)
    alex_REP_FRAUD.insert_date_in_table_type_1(cursor_db, conn_db)

    trasfer_file(name_files_dir_blacklist, name_files_dir_terminals, name_files_dir_transactions)

if __name__ == "__main__":
    main()