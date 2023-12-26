import pandas as pd

class Terminals:
    """База данных по терминалам: формирование и сохранение в public"""

    def __init__(self, path_table: str):

        self.source = pd.read_excel(path_table)
        self.update_dt = f"{path_table[-9:-5]}-{path_table[-11:-9]}-{path_table[-13:-11]}"

    def insert_date_in_table(self, cursor_db, conn_db):

        # 0. Поиск обновленных строк
        cursor_db.execute("""select
                                terminal_id,
                                terminal_type,
                                terminal_city,
                                terminal_address,
                                TO_CHAR(effective_from, 'yyyy-mm-dd')
                        from public.alex_DWH_DIM_terminals_HIST
                        where deleted_flg = false
                        and effective_to = TO_DATE('2999-01-01', 'YYYY-MM-DD')
                        """)
        records = cursor_db.fetchall()
        names = [x[0] for x in cursor_db.description]
        old_source = pd.DataFrame(records, columns=names)
        self.source = self.source.merge(old_source, on=['terminal_id', 'terminal_type', 'terminal_city', 'terminal_address'], how='left')
        self.source.columns = self.source.columns.str.replace('to_char', 'update_dt')
        self.source.loc[self.source['update_dt'].isnull(), 'update_dt'] = self.update_dt

        # 1. Очистка стейджинговых таблиц
        cursor_db.execute(""" delete from public.alex_STG_terminals;
                            delete from public.alex_STG_terminals_del;
                        """)
        conn_db.commit()

        # 2. Захват данных из источника (измененных с момента последней загрузки) в стейджинг
        cursor_db.execute("""select max_update_dt
                        from public.alex_META_meta
                        where schema_name='public'
                        and table_name='public.alex_DWH_DIM_terminals_HIST'
                        """)
        max_update_dt = cursor_db.fetchall()[0][0].strftime('20%y-%m-%d')
        cursor_db.executemany(""" insert into public.alex_STG_terminals(
                                terminal_id,
                                terminal_type,
                                terminal_city,
                                terminal_address,
                                update_dt
                                ) VALUES(%s, %s, %s, %s, %s);
                            """, self.source[self.source['update_dt']>max_update_dt].values.tolist())
        conn_db.commit()
        
        # 3. Захват в стейджинг ключей из источника полным срезом для вычисления удалений:
        tmp_source_terminal_id = [[i] for i in self.source['terminal_id'].values.tolist()]
        cursor_db.executemany("""insert into public.alex_STG_terminals_del( terminal_id
                            ) VALUES(%s);
                            """, tmp_source_terminal_id)
        conn_db.commit()
        
        # 4. Загрузка в target новых строчек из источника, если они есть (формат SCD2):
        cursor_db.execute(f"""insert into public.alex_DWH_DIM_terminals_HIST(
                                terminal_id,
                                terminal_type, 
                                terminal_city, 
                                terminal_address,
                                effective_from,
	                            effective_to,
	                            deleted_flg
                          ) 
                          select
                                stg.terminal_id,
                                stg.terminal_type,
                                stg.terminal_city,
                                stg.terminal_address,
                                TO_DATE('{self.update_dt}', 'YYYY-MM-DD'),
                                TO_DATE('2999-01-01', 'YYYY-MM-DD'),
                                false
                            from public.alex_STG_terminals stg
                            left join public.alex_DWH_DIM_terminals_HIST tgt
                            on stg.terminal_id = tgt.terminal_id
                            where tgt.terminal_id is null;""")
        conn_db.commit()

    # 5. Обновление в target измененных строчек на источнике в два этапа (формат SCD2).
    # 5.1. Обновим имеющуюся в target строчку и закроем ее по дате.
        cursor_db.execute(f"""update public.alex_DWH_DIM_terminals_HIST
                        set
                            effective_to = TO_DATE('{self.update_dt}', 'YYYY-MM-DD') - interval '1 day'
                        from (
                            select 
                                stg.terminal_id,
                                stg.terminal_type,
                                stg.terminal_city,
                                stg.terminal_address,
                                stg.update_dt,
                                null 
                            from public.alex_STG_terminals stg
                            inner join public.alex_DWH_DIM_terminals_HIST tgt
                            on stg.terminal_id = tgt.terminal_id
                            where (stg.terminal_type <> tgt.terminal_type or ( stg.terminal_type is null and tgt.terminal_type is not null ) or ( stg.terminal_type is not null and tgt.terminal_type is null ))
                            or (stg.terminal_city <> tgt.terminal_city or ( stg.terminal_city is null and tgt.terminal_city is not null ) or ( stg.terminal_city is not null and tgt.terminal_city is null ))
                            or (stg.terminal_address <> tgt.terminal_address or ( stg.terminal_address is null and tgt.terminal_address is not null ) or ( stg.terminal_address is not null and tgt.terminal_address is null ))                     
                          ) tmp
                        where alex_DWH_DIM_terminals_HIST.terminal_id = tmp.terminal_id
                        and alex_DWH_DIM_terminals_HIST.effective_to = TO_DATE('2999-01-01', 'YYYY-MM-DD')
                        """) 
        conn_db.commit()      
        
    # 5.2. Добавление новой строчки с изменными данными в новой редакции (формат SCD2)
        cursor_db.execute(f"""insert into public.alex_DWH_DIM_terminals_HIST(
                                terminal_id,
                                terminal_type, 
                                terminal_city, 
                                terminal_address,
                                effective_from,
	                            effective_to,
	                            deleted_flg)
                            select distinct
                                stg.terminal_id,
                                stg.terminal_type,
                                stg.terminal_city,
                                stg.terminal_address,
                                TO_DATE('{self.update_dt}', 'YYYY-MM-DD'),
                                TO_DATE('2999-01-01', 'YYYY-MM-DD'),
                                false
                            from public.alex_STG_terminals stg
                            inner join public.alex_DWH_DIM_terminals_HIST tgt
                            on stg.terminal_id = tgt.terminal_id
                            where (stg.terminal_type <> tgt.terminal_type or ( stg.terminal_type is null and tgt.terminal_type is not null ) or ( stg.terminal_type is not null and tgt.terminal_type is null ))
                            or (stg.terminal_city <> tgt.terminal_city or ( stg.terminal_city is null and tgt.terminal_city is not null ) or ( stg.terminal_city is not null and tgt.terminal_city is null ))
                            or (stg.terminal_address <> tgt.terminal_address or ( stg.terminal_address is null and tgt.terminal_address is not null ) or ( stg.terminal_address is not null and tgt.terminal_address is null ))                     
                            """)
        conn_db.commit()

    # 6. Обновление той строчки, что удалили в источнике (формат SCD2)
        cursor_db.execute(f"""update public.alex_DWH_DIM_terminals_HIST
                        set 
                            effective_to = TO_DATE('{self.update_dt}', 'YYYY-MM-DD') - interval '1 day',
                            deleted_flg = true
                        where terminal_id in (
                            select tgt.terminal_id
                            from public.alex_DWH_DIM_terminals_HIST tgt
                            left join public.alex_STG_terminals_del stg
                            on stg.terminal_id = tgt.terminal_id
                            where stg.terminal_id is null
                        );
                        """)
        conn_db.commit()    

    # 7. Обновление метаданных
        cursor_db.execute("""update public.alex_META_meta
                        set max_update_dt = coalesce( (select max( update_dt ) from public.alex_STG_terminals ), ( select max_update_dt
                            from public.alex_META_meta where schema_name='public' and table_name='public.alex_DWH_DIM_terminals_HIST' ) )
                        where schema_name='public' and table_name = 'public.alex_DWH_DIM_terminals_HIST';
                        """)
        conn_db.commit()