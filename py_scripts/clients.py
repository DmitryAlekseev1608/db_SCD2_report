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
                          client_id,
                          fio,
                          date_of_birth,
                          passport_num,
                          passport_valid_to,
                          phone,
                          update_dt )
                        select
                          client_id,
                          last_name || ' ' || first_name || ' ' || patronymic as fio,
                          date_of_birth,
                          passport_num,
                          passport_valid_to,
                          phone,
                          TO_DATE('{self.update_dt}', 'YYYY-MM-DD')
                        from info.clients
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
        cursor_db.execute(""" insert into public.alex_STG_clients_del( client_id )
                        select client_id from info.clients;
                       """)
        conn_db.commit()
        
        # 4. Загрузка в target новых строчек из источника, если они есть (формат SCD2):
        cursor_db.execute(f"""insert into public.alex_DWH_DIM_clients_HIST(
                                client_id,
                                fio,
                                date_of_birth,
                                passport_num,
                                passport_valid_to,
                                phone,
                                effective_from,
	                            effective_to,
	                            deleted_flg
                          ) 
                          select
                                stg.client_id,
                                stg.fio,
                                stg.date_of_birth,
                                stg.passport_num,
                                stg.passport_valid_to,
                                stg.phone,
                                update_dt,
                                TO_DATE('2999-01-01', 'YYYY-MM-DD'),
                                false
                            from public.alex_STG_clients stg
                            left join public.alex_DWH_DIM_clients_HIST tgt
                            on stg.client_id = tgt.client_id
                            where tgt.client_id is null;""")
        conn_db.commit()

    # 5. Обновление в target измененных строчек на источнике в два этапа (формат SCD2).
    # 5.1. Обновим имеющуюся в target строчку и закроем ее по дате.
        cursor_db.execute(f"""update public.alex_DWH_DIM_clients_HIST
                        set
                            effective_to = TO_DATE('{self.update_dt}', 'YYYY-MM-DD') - interval '1 day'
                        from (
                            select 
                                stg.client_id,
                                stg.fio,
                                stg.date_of_birth,
                                stg.passport_num,
                                stg.passport_valid_to,
                                stg.phone,                                
                                stg.update_dt,
                                null
                            from public.alex_STG_clients stg
                            inner join public.alex_DWH_DIM_clients_HIST tgt
                            on stg.client_id = tgt.client_id
                            where (stg.fio <> tgt.fio or ( stg.fio is null and tgt.fio is not null ) or ( stg.fio is not null and tgt.fio is null ))
                            or (stg.date_of_birth <> tgt.date_of_birth or ( stg.date_of_birth is null and tgt.date_of_birth is not null ) or ( stg.date_of_birth is not null and tgt.date_of_birth is null ))                          
                            or (stg.passport_num <> tgt.passport_num or ( stg.passport_num is null and tgt.passport_num is not null ) or ( stg.passport_num is not null and tgt.passport_num is null ))                          
                            or (stg.passport_valid_to <> tgt.passport_valid_to or ( stg.passport_valid_to is null and tgt.passport_valid_to is not null ) or ( stg.passport_valid_to is not null and tgt.passport_valid_to is null ))                          
                            or (stg.phone <> tgt.phone or ( stg.phone is null and tgt.phone is not null ) or ( stg.phone is not null and tgt.phone is null ))                          
                          ) tmp
                        where alex_DWH_DIM_clients_HIST.client_id = tmp.client_id
                        and alex_DWH_DIM_clients_HIST.effective_to = TO_DATE('2999-01-01', 'YYYY-MM-DD')
                        """)
        conn_db.commit()      
        
    # 5.2. Добавление новой строчки с изменными данными в новой редакции (формат SCD2)
        cursor_db.execute(f"""insert into public.alex_DWH_DIM_clients_HIST(
                                client_id,
                                fio,
                                date_of_birth,
                                passport_num,
                                passport_valid_to,
                                phone,
                                effective_from,
	                            effective_to,
	                            deleted_flg)
                            select distinct
                                stg.client_id,
                                stg.fio,
                                stg.date_of_birth,
                                stg.passport_num,
                                stg.passport_valid_to,
                                stg.phone,                                
                                stg.update_dt,
                                TO_DATE('2999-01-01', 'YYYY-MM-DD'),
                                false
                            from public.alex_STG_clients stg
                            inner join public.alex_DWH_DIM_clients_HIST tgt
                            on stg.client_id = tgt.client_id
                            where (stg.fio <> tgt.fio or ( stg.fio is null and tgt.fio is not null ) or ( stg.fio is not null and tgt.fio is null ))
                            or (stg.date_of_birth <> tgt.date_of_birth or ( stg.date_of_birth is null and tgt.date_of_birth is not null ) or ( stg.date_of_birth is not null and tgt.date_of_birth is null ))                          
                            or (stg.passport_num <> tgt.passport_num or ( stg.passport_num is null and tgt.passport_num is not null ) or ( stg.passport_num is not null and tgt.passport_num is null ))                          
                            or (stg.passport_valid_to <> tgt.passport_valid_to or ( stg.passport_valid_to is null and tgt.passport_valid_to is not null ) or ( stg.passport_valid_to is not null and tgt.passport_valid_to is null ))                          
                            or (stg.phone <> tgt.phone or ( stg.phone is null and tgt.phone is not null ) or ( stg.phone is not null and tgt.phone is null ))
                            """)
        conn_db.commit()

    # 6. Обновление той строчки, что удалили в источнике (формат SCD2)
        cursor_db.execute(f"""update public.alex_DWH_DIM_clients_HIST
                        set 
                            effective_to = TO_DATE('{self.update_dt}', 'YYYY-MM-DD') - interval '1 day',
                            deleted_flg = true
                        where client_id in (
                            select tgt.client_id
                            from public.alex_DWH_DIM_clients_HIST tgt
                            left join public.alex_STG_clients_del stg
                            on stg.client_id = tgt.client_id
                            where stg.client_id is null);
                        """)
        conn_db.commit()    

    # 7. Обновление метаданных
        cursor_db.execute("""update public.alex_META_meta
                        set max_update_dt = coalesce( (select max( update_dt ) from public.alex_STG_clients ), ( select max_update_dt
                            from public.alex_META_meta where schema_name='public' and table_name='public.alex_DWH_DIM_clients_HIST' ) )
                        where schema_name='public' and table_name = 'public.alex_DWH_DIM_clients_HIST';
                        """)
        conn_db.commit()