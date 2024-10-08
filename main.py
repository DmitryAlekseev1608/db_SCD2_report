import psycopg2
from hydra import compose, initialize
from py_scripts.transactions import Transactions
from py_scripts.terminals import Terminals
from py_scripts.passport_blacklist import PassportBlackList
from py_scripts.cards import Cards
from py_scripts.account import Account
from py_scripts.clients import Clients
from py_scripts.files_operations import save_name_file, trasfer_file
from py_scripts.fraud import Fraud

def main():

    # загружаем конфигурации
    initialize(version_base=None, config_path="configs", job_name="app")
    cfg = compose(config_name="configs")

    # считываем имена файлов
    name_files_dir_blacklist, name_files_dir_terminals, name_files_dir_transactions = save_name_file(cfg)

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
    alex_DWH_FACT_transactions = Transactions(f"{cfg.files.path_to_input}/{name_files_dir_transactions}")
    alex_DWH_FACT_transactions.insert_date_in_table(cursor_db, conn_db)

    # работа с public.alex_DWH_DIM_terminals_HIST
    alex_DWH_DIM_terminals_HIST = Terminals(f"{cfg.files.path_to_input}/{name_files_dir_terminals}")
    alex_DWH_DIM_terminals_HIST.insert_date_in_table(cursor_db, conn_db)

    # работа с public.alex_DWH_FACT_passport_blacklist
    alex_DWH_FACT_passport_blacklist = PassportBlackList(f"{cfg.files.path_to_input}/{name_files_dir_blacklist}")
    alex_DWH_FACT_passport_blacklist.insert_date_in_table(cursor_db, conn_db)

    # работа с public.alex_DWH_DIM_cards_HIST
    alex_DWH_DIM_cards_HIST = Cards(name_files_dir_terminals)
    alex_DWH_DIM_cards_HIST.insert_date_in_table(cursor_db, conn_db)

    # работа с public.alex_DWH_DIM_account_HIST
    alex_DWH_DIM_account_HIST = Account(name_files_dir_terminals)
    alex_DWH_DIM_account_HIST.insert_date_in_table(cursor_db, conn_db)

    # работа с public.alex_DWH_DIM_clients_HIST
    alex_DWH_DIM_clients_HIST = Clients(name_files_dir_terminals)
    alex_DWH_DIM_clients_HIST.insert_date_in_table(cursor_db, conn_db)   

    # # работа с public.alex_REP_FRAUD
    alex_REP_FRAUD = Fraud(name_files_dir_transactions)
    # # операции 1-го типа мошеничества
    alex_REP_FRAUD.insert_date_in_table_type_1(cursor_db, conn_db)
    # # операции 2-го типа мошеничества
    alex_REP_FRAUD.insert_date_in_table_type_2(cursor_db, conn_db)
    # # операции 3-го типа мошеничества
    alex_REP_FRAUD.insert_date_in_table_type_3(cursor_db, conn_db)
    # # операция 4-го типа мошеничества
    alex_REP_FRAUD.insert_date_in_table_type_4(cursor_db, conn_db, alex_DWH_FACT_transactions)

    # перемещение файлов с изменением формата
    trasfer_file(cfg, name_files_dir_blacklist, name_files_dir_terminals, name_files_dir_transactions)

    cursor_db.close()
    conn_db.close()

if __name__ == "__main__":
    main()