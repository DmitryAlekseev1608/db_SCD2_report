import pandas as pd

class Cards:
    """База данных по картам: формирование и сохранение в public"""

    def insert_date_in_table(self, cursor_db, conn_db):

        # 1. Очистка стейджинговых таблиц
        cursor_db.execute(""" delete from public.alex_STG_cards;
                            delete from public.alex_STG_cards_del;
                        """)
        conn_db.commit()

        # 2. Захват данных из источника (измененных с момента последней загрузки) в стейджинг
        cursor_db.execute(""" insert into public.alex_STG_cards(
                          card_num,
                          account_num,
                          update_dt )
                        select
                          card_num,
                          account,
                          update_dt
                        from info.cards
                        where update_dt >
                        (select 
                          max_update_dt
                        from public.alex_META_meta
                        where schema_name='public'
                        and table_name='public.alex_DWH_DIM_cards_HIST);
                        """)
        conn_db.commit()
        
        # 3. Захват в стейджинг ключей из источника полным срезом для вычисления удалений:
        cursor_db.execute(""" insert into public.alex_STG_cards_del( card_num )
                        select card_num from info.cards;
                       """)
        conn_db.commit()
        
        # 4. Загрузка в target новых строчек из источника, если они есть (формат SCD2):
        cursor_db.execute(f"""insert into public.alex_DWH_DIM_cards_HIST(
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
                            from public.alex_STG_cards stg
                            left join public.alex_DWH_DIM_cards_HIST tgt
                            on stg.card_num = tgt.card_num
                            where tgt.card_num is null;""")
        conn_db.commit()

    # 5. Обновление в target измененных строчек на источнике в два этапа (формат SCD2).
    # 5.1. Обновим имеющуюся в target строчку и закроем ее по дате.
        cursor_db.execute(f"""update public.alex_DWH_DIM_cards_HIST
                        set
                            effective_to = stg.update_dt - interval '1 day'
                        from (
                            select 
                                stg.card_num,
                                stg.account_num,
                                stg.update_dt,
                                null 
                            from public.alex_STG_cards stg
                            inner join public.alex_DWH_DIM_cards_HIST tgt
                            on stg.card_num = tgt.card_num
                            where (stg.account_num <> tgt.account_num or ( stg.account_num is null and tgt.terminal_type is not null ) or ( stg.terminal_type is not null and tgt.terminal_type is null ))
                          ) tmp
                        where alex_DWH_DIM_cards_HIST.terminal_id = tmp.terminal_id
                        and alex_DWH_DIM_cards_HIST.effective_to = TO_DATE('2999-01-01', 'YYYY-MM-DD')
                        """) 
        conn_db.commit()      
        
    # 5.2. Добавление новой строчки с изменными данными в новой редакции (формат SCD2)
        cursor_db.execute(f"""insert into public.alex_DWH_DIM_cards_HIST(
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
                            from public.alex_STG_cards stg
                            inner join public.alex_DWH_DIM_cards_HIST tgt
                            on stg.terminal_id = tgt.terminal_id
                            where (stg.terminal_type <> tgt.terminal_type or ( stg.terminal_type is null and tgt.terminal_type is not null ) or ( stg.terminal_type is not null and tgt.terminal_type is null ))
                            or (stg.terminal_city <> tgt.terminal_city or ( stg.terminal_city is null and tgt.terminal_city is not null ) or ( stg.terminal_city is not null and tgt.terminal_city is null ))
                            or (stg.terminal_address <> tgt.terminal_address or ( stg.terminal_address is null and tgt.terminal_address is not null ) or ( stg.terminal_address is not null and tgt.terminal_address is null ))                     
                            """)
        conn_db.commit()

    # 6. Обновление той строчки, что удалили в источнике (формат SCD2)
        cursor_db.execute(f"""update public.alex_DWH_DIM_cards_HIST
                        set 
                            effective_to = TO_DATE('{self.update_dt}', 'YYYY-MM-DD') - interval '1 day',
                            deleted_flg = true
                        where terminal_id in (
                            select tgt.terminal_id
                            from public.alex_DWH_DIM_cards_HIST tgt
                            left join public.alex_STG_terminals_del stg
                            on stg.terminal_id = tgt.terminal_id
                            where stg.terminal_id is null
                        );
                        """)
        conn_db.commit()    

    # 7. Обновление метаданных
        cursor_db.execute("""update public.alex_META_meta
                        set max_update_dt = coalesce( (select max( update_dt ) from public.alex_STG_cards ), ( select max_update_dt
                            from public.alex_META_meta where schema_name='public' and table_name='public.alex_DWH_DIM_cards_HIST' ) )
                        where schema_name='public' and table_name = 'public.alex_DWH_DIM_cards_HIST';
                        """)
        conn_db.commit()