class Clients:
    """База данных по клиентам: формирование и сохранение в public"""

    def __init__(self, path_table: str):

        self.update_dt = f"{path_table[-9:-5]}-{path_table[-11:-9]}-{path_table[-13:-11]}"

    def insert_date_in_table(self, cursor_db, conn_db):

       
        # 1. Очистка стейджинговых таблиц
        cursor_db.execute(""" delete from public.alex_STG_clients;
                            delete from public.alex_STG_clients_del;
                        """)
        conn_db.commit()

        # 2. Захват данных из источника (измененных с момента последней загрузки) в стейджинг
        cursor_db.execute(f""" insert into public.alex_STG_clients(
                          card_num,
                          account_num,
                          update_dt )
                        select
                          card_num,
                          account,
                          TO_DATE('{self.update_dt}', 'YYYY-MM-DD')
                        from info.cards
                        where update_dt >
                        (select 
                          max_update_dt
                        from public.alex_META_meta
                        where schema_name='public'
                        and table_name='public.alex_DWH_DIM_clients_HIST')
                        or update_dt is null;
                        """)
        conn_db.commit()
        
        # 3. Захват в стейджинг ключей из источника полным срезом для вычисления удалений:
        cursor_db.execute(""" insert into public.alex_STG_clients_del( card_num )
                        select card_num from info.cards;
                       """)
        conn_db.commit()
        
        # 4. Загрузка в target новых строчек из источника, если они есть (формат SCD2):
        cursor_db.execute(f"""insert into public.alex_DWH_DIM_clients_HIST(
                                card_num,
                                account_num,
                                effective_from,
	                            effective_to,
	                            deleted_flg
                          ) 
                          select
                                stg.card_num,
                                stg.account_num,
                                update_dt,
                                TO_DATE('2999-01-01', 'YYYY-MM-DD'),
                                false
                            from public.alex_STG_clients stg
                            left join public.alex_DWH_DIM_clients_HIST tgt
                            on stg.card_num = tgt.card_num
                            where tgt.card_num is null;""")
        conn_db.commit()

    # 5. Обновление в target измененных строчек на источнике в два этапа (формат SCD2).
    # 5.1. Обновим имеющуюся в target строчку и закроем ее по дате.
        cursor_db.execute(f"""update public.alex_DWH_DIM_clients_HIST
                        set
                            effective_to = TO_DATE('{self.update_dt}', 'YYYY-MM-DD') - interval '1 day'
                        from (
                            select 
                                stg.card_num,
                                stg.account_num,
                                stg.update_dt,
                                null 
                            from public.alex_STG_clients stg
                            inner join public.alex_DWH_DIM_clients_HIST tgt
                            on stg.card_num = tgt.card_num
                            where (stg.account_num <> tgt.account_num or ( stg.account_num is null and tgt.account_num is not null ) or ( stg.account_num is not null and tgt.account_num is null ))
                          ) tmp
                        where alex_DWH_DIM_clients_HIST.card_num = tmp.card_num
                        and alex_DWH_DIM_clients_HIST.effective_to = TO_DATE('2999-01-01', 'YYYY-MM-DD')
                        """)
        conn_db.commit()      
        
    # 5.2. Добавление новой строчки с изменными данными в новой редакции (формат SCD2)
        cursor_db.execute(f"""insert into public.alex_DWH_DIM_clients_HIST(
                                card_num,
                                account_num, 
                                effective_from,
	                            effective_to,
	                            deleted_flg)
                            select distinct
                                stg.card_num,
                                stg.account_num,
                                stg.update_dt,
                                TO_DATE('2999-01-01', 'YYYY-MM-DD'),
                                false
                            from public.alex_STG_clients stg
                            inner join public.alex_DWH_DIM_clients_HIST tgt
                            on stg.card_num = tgt.card_num
                            where stg.account_num <> tgt.account_num or ( stg.account_num is null and tgt.account_num is not null ) or ( stg.account_num is not null and tgt.account_num is null )
                            """)
        conn_db.commit()

    # 6. Обновление той строчки, что удалили в источнике (формат SCD2)
        cursor_db.execute(f"""update public.alex_DWH_DIM_clients_HIST
                        set 
                            effective_to = TO_DATE('{self.update_dt}', 'YYYY-MM-DD') - interval '1 day',
                            deleted_flg = true
                        where card_num in (
                            select tgt.card_num
                            from public.alex_DWH_DIM_clients_HIST tgt
                            left join public.alex_STG_clients_del stg
                            on stg.card_num = tgt.card_num
                            where stg.card_num is null
                        );
                        """)
        conn_db.commit()    

    # 7. Обновление метаданных
        cursor_db.execute("""update public.alex_META_meta
                        set max_update_dt = coalesce( (select max( update_dt ) from public.alex_STG_clients ), ( select max_update_dt
                            from public.alex_META_meta where schema_name='public' and table_name='public.alex_DWH_DIM_clients_HIST' ) )
                        where schema_name='public' and table_name = 'public.alex_DWH_DIM_clients_HIST';
                        """)
        conn_db.commit()