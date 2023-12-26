-- Подготовка самих таблиц

-- Создание таблицы с метаданными

create table public.alex_META_meta (
    schema_name varchar(50),
    table_name varchar(50),
    max_update_dt DATE
);

insert into public.alex_META_meta( schema_name, table_name, max_update_dt )
values ('public',
        'public.alex_DWH_DIM_terminals_HIST',
        DATE('1900-01-01')),
        ('public',
        'public.alex_DWH_FACT_passport_blacklist',
        DATE('1900-01-01'));

-----------------------------------------------------------------
-- Работа с таблицей alex_DWH_DIM_terminals_HIST

CREATE TABLE public.alex_DWH_DIM_terminals_HIST(
                                terminal_id VARCHAR(30),
                                terminal_type VARCHAR(30), 
                                terminal_city VARCHAR(30), 
                                terminal_address VARCHAR(300),
                                effective_from DATE,
	                            effective_to DATE,
	                            deleted_flg bool,
	                            PRIMARY KEY (terminal_id, effective_from)
                                );

CREATE TABLE public.alex_STG_terminals(
                                terminal_id VARCHAR(30),
                                terminal_type VARCHAR(30), 
                                terminal_city VARCHAR(30), 
                                terminal_address VARCHAR(300),
                                update_dt DATE
                                );

CREATE TABLE public.alex_STG_terminals_del( 
	terminal_id VARCHAR(30)
);

-----------------------------------------------------------------
-- Работа с таблицей public.alex_REP_FRAUD

CREATE TABLE public.alex_REP_FRAUD(
                                event_dt TIME(0),
                                passport VARCHAR(30), 
                                fio VARCHAR(100), 
                                phone BPCHAR(16),
                                event_type NUMERIC,
                                report_dt DATE
                                );

-----------------------------------------------------------------
-- Работа с таблицей public.alex_DWH_FACT_transactions

CREATE TABLE public.alex_DWH_FACT_transactions(
                                trans_id VARCHAR(20),
                                trans_date TIMESTAMP(0),
                                amt DECIMAL(30, 2),
                                card_num VARCHAR(30),
                                oper_type VARCHAR(30),
                                oper_result VARCHAR(30),
                                terminal VARCHAR(30)                       
                                );

-----------------------------------------------------------------
-- Работа с таблицей public.alex_DWH_FACT_passport_blacklist

CREATE TABLE public.alex_DWH_FACT_passport_blacklist(
                                passport_num VARCHAR(50),
                                entry_dt DATE,
                                effective_from DATE,
	                            effective_to DATE,
	                            deleted_flg bool,
	                            PRIMARY KEY (passport_num, effective_from)
                                );

CREATE TABLE public.alex_STG_passport_blacklist(
                                passport_num VARCHAR(50),
                                entry_dt DATE,
                                update_dt DATE,
	                            PRIMARY KEY (passport_num)                                
                                );

CREATE TABLE public.alex_STG_passport_blacklist_del( 
	                            passport_num VARCHAR(30)
                                );