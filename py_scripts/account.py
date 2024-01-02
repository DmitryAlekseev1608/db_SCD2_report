class Account:
    """База данных по аккаунтам: формирование и сохранение в public"""

    def __init__(self, path_table: str):

        self.update_dt = f"{path_table[-9:-5]}-{path_table[-11:-9]}-{path_table[-13:-11]}"

    def insert_date_in_table(self, cursor_db, conn_db):
       
        # 1. Очистка стейджинговых таблиц
        cursor_db.execute(""" delete from public.alex_STG_accounts;
                            delete from public.alex_STG_accounts_del;
                        """)
        conn_db.commit()

        # 2. Захват данных из источника (измененных с момента последней загрузки) в стейджинг
        cursor_db.execute(f""" insert into public.alex_STG_accounts(
                          account_num,
                          valid_to,
                          client,
                          update_dt )
                        select
                          account,
                          valid_to,
                          client,
                          TO_DATE('{self.update_dt}', 'YYYY-MM-DD')
                        from info.accounts
                        where update_dt >
                        (select 
                          max_update_dt
                        from public.alex_META_meta
                        where schema_name='public'
                        and table_name='public.alex_DWH_DIM_accounts_HIST')
                        or update_dt is null;
                        """)
        conn_db.commit()
        
        # 3. Захват в стейджинг ключей из источника полным срезом для вычисления удалений:
        cursor_db.execute(""" insert into public.alex_STG_accounts_del( account_num )
                        select account from info.accounts;
                       """)
        conn_db.commit()
        
        # 4. Загрузка в target новых строчек из источника, если они есть (формат SCD2):
        cursor_db.execute(f"""insert into public.alex_DWH_DIM_accounts_HIST(
                                account_num,
                                valid_to,
                                client,
                                effective_from,
	                            effective_to,
	                            deleted_flg
                          ) 
                          select
                                stg.account_num,
                                stg.valid_to,
                                stg.client,
                                update_dt,
                                TO_DATE('2999-01-01', 'YYYY-MM-DD'),
                                false
                            from public.alex_STG_accounts stg
                            left join public.alex_DWH_DIM_accounts_HIST tgt
                            on stg.account_num = tgt.account_num
                            where tgt.account_num is null;""")
        conn_db.commit()

    # 5. Обновление в target измененных строчек на источнике в два этапа (формат SCD2).
    # 5.1. Обновим имеющуюся в target строчку и закроем ее по дате.
        cursor_db.execute(f"""update public.alex_DWH_DIM_accounts_HIST
                        set
                            effective_to = TO_DATE('{self.update_dt}', 'YYYY-MM-DD') - interval '1 day'
                        from (
                            select 
                                stg.account_num,
                                stg.valid_to,
                                stg.client,
                                stg.update_dt,
                                null 
                            from public.alex_STG_accounts stg
                            inner join public.alex_DWH_DIM_accounts_HIST tgt
                            on stg.account_num = tgt.account_num
                            where (stg.valid_to <> tgt.valid_to or ( stg.valid_to is null and tgt.valid_to is not null ) or ( stg.valid_to is not null and tgt.valid_to is null ))
                            or (stg.client <> tgt.client or ( stg.client is null and tgt.client is not null ) or ( stg.client is not null and tgt.client is null ))
                          ) tmp
                        where alex_DWH_DIM_accounts_HIST.account_num = tmp.account_num
                        and alex_DWH_DIM_accounts_HIST.effective_to = TO_DATE('2999-01-01', 'YYYY-MM-DD')
                        """)
        conn_db.commit()      
        
    # 5.2. Добавление новой строчки с изменными данными в новой редакции (формат SCD2)
        cursor_db.execute(f"""insert into public.alex_DWH_DIM_accounts_HIST(
                                account_num,
                                valid_to,
                                client,
                                effective_from,
	                            effective_to,
	                            deleted_flg)
                            select distinct
                                stg.account_num,
                                stg.valid_to,
                                stg.client,
                                stg.update_dt,
                                TO_DATE('2999-01-01', 'YYYY-MM-DD'),
                                false
                            from public.alex_STG_accounts stg
                            inner join public.alex_DWH_DIM_accounts_HIST tgt
                            on stg.account_num = tgt.account_num
                            where (stg.valid_to <> tgt.valid_to or ( stg.valid_to is null and tgt.valid_to is not null ) or ( stg.valid_to is not null and tgt.valid_to is null ))
                            or (stg.client <> tgt.client or ( stg.client is null and tgt.client is not null ) or ( stg.client is not null and tgt.client is null ))
                            """)
        conn_db.commit()

    # 6. Обновление той строчки, что удалили в источнике (формат SCD2)
        cursor_db.execute(f"""update public.alex_DWH_DIM_accounts_HIST
                        set 
                            effective_to = TO_DATE('{self.update_dt}', 'YYYY-MM-DD') - interval '1 day',
                            deleted_flg = true
                        where account_num in (
                            select tgt.account_num
                            from public.alex_DWH_DIM_accounts_HIST tgt
                            left join public.alex_STG_accounts_del stg
                            on stg.account_num = tgt.account_num
                            where stg.account_num is null
                        );
                        """)
        conn_db.commit()    

    # 7. Обновление метаданных
        cursor_db.execute("""update public.alex_META_meta
                        set max_update_dt = coalesce( (select max( update_dt ) from public.alex_STG_accounts ), ( select max_update_dt
                            from public.alex_META_meta where schema_name='public' and table_name='public.alex_DWH_DIM_accounts_HIST' ) )
                        where schema_name='public' and table_name = 'public.alex_DWH_DIM_accounts_HIST';
                        """)
        conn_db.commit()