-- Подготовка самих таблиц

-- Создание таблицы с метаданными

create table public.alex_META_meta (
    schema_name varchar(30),
    table_name varchar(30),
    max_update_dt timestamp(0)
);

insert into public.alex_META_meta( schema_name, table_name, max_update_dt )
values( 'public.alex_DWH_FACT_transactions',
        'alex_source',
        to_timestamp('1900-01-01','YYYY-MM-DD')
        );

-----------------------------------------------------------------
-- Работа с таблицей public.alex_DWH_FACT_transactions

CREATE TABLE public.alex_DWH_FACT_transactions(
                                trans_id VARCHAR(20),
                                trans_date DATE,
                                amt DECIMAL(30, 2),
                                card_num VARCHAR(30),
                                oper_type VARCHAR(30),
                                oper_result VARCHAR(30),
                                terminal VARCHAR(30)
                                );

-----------------------------------------------------------------
-- Работа с таблицей alex_DWH_DIM_terminals_HIST

CREATE TABLE public.alex_DWH_DIM_terminals_HIST(
                                terminal_id VARCHAR(10),
                                terminal_type VARCHAR(10), 
                                terminal_city VARCHAR(10), 
                                terminal_address VARCHAR(100),
                                effective_from timestamp(0),
	                            effective_to timestamp(0),
	                            deleted_flg bool
                                );

CREATE TABLE public.alex_STG_terminals(
                                terminal_id VARCHAR(10),
                                terminal_type VARCHAR(10), 
                                terminal_city VARCHAR(10), 
                                terminal_address VARCHAR(100),
                                update_dt timestamp(0)
                                );

CREATE TABLE public.alex_STG_terminals_del( 
	id integer
);





