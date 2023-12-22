import pandas as pd
import tqdm

class Terminals:

    """База данных по терминалам: формирование и сохранение в public"""

    def __init__(self, path_table: str):
        print("Create terminals")

        data = pd.read_excel(path_table)
        self.data = data.values.tolist()
        for i in tqdm.tqdm(range(len(self.data))):
            self.data[i].append(path_table[6:16])

        self.del_stage = """ delete from public.alex_STG_terminals;
                            delete from public.alex_STG_terminals_del;
                        """
        
        self.copy_source = """ insert into public.alex_STG_terminals(
                                terminal_id,
                                terminal_type,
                                terminal_city,
                                terminal_address,
                                update_dt
                                ) VALUES(%s, %s, %s, %s, %s)

                                

                            where update_dt > ( select max_update_dt from public.alex_meta where schema_name='dwh' and table_name='alex_source' );
                        """
        

        """ INSERT INTO public.alex_DWH_FACT_transactions(
                                trans_id,
                                trans_date,
                                amt,
                                card_num,
                                oper_type,
                                oper_result,
                                terminal
                            ) VALUES(%s, %s, %s, %s, %s, %s, %s) """
                            
                            
                            -- 3. Захват в стейджинг ключей из источника полным срезом для вычисления удалений:
                            insert into public.alex_stg_del( id )
                            select id from public.alex_source;

                            -- 4. Загрузка в target новых строчек из источника, если они есть (формат SCD2):
                            insert into public.alex_target( id, val, start_dt, end_dt, is_deleted )
                            select 
                                stg.id,
                                stg.val,
                                now(),
                                to_timestamp('2999-01-01','YYYY-MM-DD'),
                                false
                            from public.alex_stg stg
                            left join public.alex_target tgt
                            on stg.id = tgt.id
                            where tgt.id is null;

                            -- 5. Обновление в target измененных строчек на источнике в два этапа (формат SCD2).
                            -- 5.1. Обновим имеющуюся в target строчку и закроем ее по дате.
                            update public.alex_target
                            set 
                                end_dt = now() - interval '1 second'
                            from (
                                select 
                                    stg.id,
                                    stg.val, 
                                    stg.update_dt, 
                                    null 
                                from public.alex_stg stg
                                inner join public.alex_target tgt
                                on stg.id = tgt.id
                                where stg.val <> tgt.val or ( stg.val is null and tgt.val is not null ) or ( stg.val is not null and tgt.val is null )
                            ) tmp
                            where alex_target.id = tmp.id; 

                            -- 5.2. Добавление новой строчки с изменными данными в новой редакции (формат SCD2).
                            insert into public.alex_target( id, val, start_dt, end_dt, is_deleted )
                            select 
                                stg.id, 
                                stg.val, 
                                now(),
                                to_timestamp('2999-01-01','YYYY-MM-DD'),
                                false
                            from public.alex_stg stg
                            inner join public.alex_target tgt
                            on stg.id = tgt.id
                            where stg.val <> tgt.val or ( stg.val is null and tgt.val is not null ) or ( stg.val is not null and tgt.val is null );

                            -- 6. Обновление той строчки, что удалили в источнике (формат SCD2).
                            update public.alex_target
                            set 
                                end_dt = now() - interval '1 second',
                                is_deleted = true
                            where id in (
                                select tgt.id
                                from public.alex_target tgt
                                left join public.alex_stg_del stg
                                on stg.id = tgt.id
                                where stg.id is null
                            );

                            -- 7. Обновление метаданных.
                            update public.alex_meta
                            set max_update_dt = coalesce( (select max( update_dt ) from public.alex_stg ), ( select max_update_dt
                                from public.alex_meta where schema_name='dwh' and table_name='alex_source' ) )
                            where schema_name='dwh' and table_name = 'alex_source';



