import pandas as pd
import datetime

class Fraud:
    """База данных по отчету: формирование и сохранение в public"""

    def __init__(self, path_table):

        self.path_table = path_table
        self.update_dt = f"{path_table[-8:-4]}-{path_table[-10:-8]}-{path_table[-12:-10]}"

    def insert_date_in_table_type_1(self, cursor_db, conn_db):
        """Работа с мошенической операций 1-го типа: 
        Совершение операции при просроченном или заблокированном паспорте"""

        cursor_db.execute(f"""insert into public.alex_REP_FRAUD(
                                event_dt,
                                passport, 
                                fio, 
                                phone,
                                event_type,
                                report_dt
                                )
                            select
                                tmp_p.event_dt,	
                                tmp_p.passport,
                                tmp_p.fio,
                                tmp_p.phone,
                                tmp_p.event_type,
                                tmp_p.report_dt
                            from
                            (select 
                                tmp_a.trans_date::time as event_dt,	
                                cln.passport_num as passport,
                                cln.fio,
                                cln.phone as phone,
                                1 as event_type,
                                tmp_a.trans_date::date as report_dt,
                                cln.passport_valid_to as passport_valid_to,
                                tmp_a.trans_date::date as trans_date
                            from
                            (select
                                tmp.trans_date as trans_date,
                                acn.client as client
                            from
                            (select
                                trn.trans_date as trans_date,
                                crd.account_num as account
                            from public.alex_DWH_FACT_transactions trn
                            left join public.alex_DWH_DIM_cards_HIST crd
                            on crd.card_num = trn.card_num
                            where trn.trans_date::date = TO_DATE('{self.update_dt}', 'YYYY-MM-DD')
                            and crd.deleted_flg is false
                            and crd.effective_to = TO_DATE('2999-01-01', 'YYYY-MM-DD') ) tmp
                            left join public.alex_DWH_DIM_account_HIST acn
                            on tmp.account = acn.account_num
                            where acn.deleted_flg is false
                            and acn.effective_to = TO_DATE('2999-01-01', 'YYYY-MM-DD') ) tmp_a
                            left join public.alex_DWH_DIM_clients_HIST cln
                            on tmp_a.client = cln.client_id
                            where cln.deleted_flg is false
                            and cln.effective_to = TO_DATE('2999-01-01', 'YYYY-MM-DD')) tmp_p
                            left join public.alex_dwh_fact_passport_blacklist pbl
                            on tmp_p.passport = pbl.passport_num
                            where (pbl.passport_num = tmp_p.passport
                            or tmp_p.passport_valid_to < tmp_p.trans_date
                            or tmp_p.passport_valid_to = tmp_p.trans_date)
                            and (pbl.deleted_flg = false and pbl.effective_to = TO_DATE('2999-01-01', 'YYYY-MM-DD'))
                            """)
        conn_db.commit()

    def insert_date_in_table_type_2(self, cursor_db, conn_db):
        """Работа с мошенической операций 2-го типа: 
        cовершение операции при недействующем договоре."""

        cursor_db.execute(f"""insert into public.alex_REP_FRAUD(
                                event_dt,
                                passport, 
                                fio, 
                                phone,
                                event_type,
                                report_dt
                                )
                            select
                                tmp_a.event_dt,
                                cln.passport_num as passport,
                                cln.fio,
                                cln.phone,
                                2,
                                TO_DATE('{self.update_dt}', 'YYYY-MM-DD')                                
                            from
                            (select
                                tmp.trans_date::time as event_dt,
                                acn.account_num as account_num,
                                acn.client as client
                            from
                            (select
                                trn.trans_date as trans_date,
                                crd.account_num as account
                            from public.alex_DWH_FACT_transactions trn
                            left join public.alex_DWH_DIM_cards_HIST crd
                            on crd.card_num = trn.card_num
                            where trn.trans_date::date = TO_DATE('{self.update_dt}', 'YYYY-MM-DD')
                            and crd.deleted_flg is false
                            and crd.effective_to = TO_DATE('2999-01-01', 'YYYY-MM-DD') ) tmp
                            left join public.alex_DWH_DIM_account_HIST acn
                            on tmp.account = acn.account_num
                            where acn.deleted_flg is false
                            and acn.effective_to = TO_DATE('2999-01-01', 'YYYY-MM-DD')
                            and acn.valid_to < tmp.trans_date) tmp_a
                            left join public.alex_DWH_DIM_clients_HIST cln
                            on tmp_a.client = cln.client_id
                            where cln.deleted_flg is false
                            and cln.effective_to = TO_DATE('2999-01-01', 'YYYY-MM-DD')
                            """)
        conn_db.commit()

    def insert_date_in_table_type_3(self, cursor_db, conn_db):
        """Работа с мошенической операций 3-го типа: 
        cовершение операций в разных городах в течение одного часа."""

        cursor_db.execute(f"""insert into public.alex_REP_FRAUD(
                                event_dt,
                                passport, 
                                fio, 
                                phone,
                                event_type,
                                report_dt
                                )
                            select
                                tmp_d.event_dt,
                                cln.passport_num as passport,
                                cln.fio,
                                cln.phone,
                                3 as event_type,
                                TO_DATE('{self.update_dt}', 'YYYY-MM-DD')
                            from
                            (select
                                tmp_c.event_dt,
                                acn.client
                            from
                            (select
                                tmp_b.event_dt,
                                crd.account_num
                            from
                            (select distinct
                                MAX(tmp_a.trans_date) OVER (PARTITION BY tmp_a.card_num) as event_dt,
                                tmp_a.card_num
                            from
                            (select 
                                tmp.trans_date,
                                tmp.terminal_city,
                                tmp.card_num,
                                LEAD(tmp.terminal_city) OVER (PARTITION BY tmp.card_num order by card_num, trans_date) as next_city
                            from
                            (select
                                trn.trans_date::time as trans_date,
                                ter.terminal_city,
                                trn.card_num,
                                LEAD(trn.trans_date::time) OVER (PARTITION BY trn.card_num order by card_num, trans_date) - trn.trans_date::time as diff
                            from public.alex_DWH_FACT_transactions trn
                            left join public.alex_DWH_DIM_terminals_HIST ter
                            on ter.terminal_id  = trn.terminal
                            where trn.trans_date::date = TO_DATE('{self.update_dt}', 'YYYY-MM-DD')
                            and ter.deleted_flg is false
                            and ter.effective_to = TO_DATE('2999-01-01', 'YYYY-MM-DD') ) tmp
                            where tmp.diff < to_timestamp('01:00:00', 'HH24:MI:SS')::time ) tmp_a
                            where tmp_a.terminal_city <> tmp_a.next_city) tmp_b
                            left join public.alex_DWH_DIM_cards_HIST crd
                            on tmp_b.card_num = crd.card_num
                            where crd.deleted_flg is false
                            and crd.effective_to = TO_DATE('2999-01-01', 'YYYY-MM-DD') ) tmp_c
                            left join public.alex_DWH_DIM_account_HIST acn
                            on tmp_c.account_num = acn.account_num
                            where acn.deleted_flg is false
                            and acn.effective_to = TO_DATE('2999-01-01', 'YYYY-MM-DD') ) tmp_d
                            left join public.alex_DWH_DIM_clients_HIST cln
                            on tmp_d.client = cln.client_id
                            where cln.deleted_flg is false
                            and cln.effective_to = TO_DATE('2999-01-01', 'YYYY-MM-DD')
                           """)
        conn_db.commit()

    def insert_date_in_table_type_4(self, cursor_db, conn_db, alex_DWH_FACT_transactions):
        """Работа с мошенической операций 4-го типа: 
        попытка подбора суммы."""

        df = alex_DWH_FACT_transactions.pd_data.sort_values(by=['card_num', 'transaction_date'])
        card_num = df['card_num'].unique()
        df['amount'] = df['amount'].str.replace(',','.').astype(float)
        list_result = []

        for card in card_num:

            if len(df[df['card_num'] == card]) >= 4:
                
                for i in range(len(df[df['card_num'] == card])-3):

                    if df[df['card_num'] == card].iloc[i]['oper_result'] == 'REJECT' and \
                        df[df['card_num'] == card].iloc[i+1]['oper_result'] == 'REJECT' and \
                        df[df['card_num'] == card].iloc[i+2]['oper_result'] == 'REJECT' and \
                        df[df['card_num'] == card].iloc[i+3]['oper_result'] == 'SUCCESS' and \
                        df[df['card_num'] == card].iloc[i]['amount'] >= df[df['card_num'] == card].iloc[i+1]['amount'] and\
                        df[df['card_num'] == card].iloc[i+1]['amount'] >= df[df['card_num'] == card].iloc[i+2]['amount'] and \
                        df[df['card_num'] == card].iloc[i+2]['amount'] >= df[df['card_num'] == card].iloc[i+3]['amount'] and \
                        pd.to_datetime(df[df['card_num'] == card].iloc[i+3]['transaction_date']) - pd.to_datetime(df[df['card_num'] == card].iloc[i]['transaction_date']) <= \
                        pd.to_datetime("2021-03-01 00:20:00.000") - pd.to_datetime("2021-03-01 00:00:00.000"):
                        list_result.append([datetime.datetime.strptime(df[df['card_num'] == card].iloc[i+3].values.tolist()[1], '%Y-%m-%d %H:%M:%S').time().strftime("%H:%M:%S"), \
                                                df[df['card_num'] == card].iloc[i+3].values.tolist()[3]])

        cursor_db.executemany(""" INSERT INTO public.alex_STG_REP_FRAUD(
                    event_dt,
                    card_num
                ) VALUES(%s, %s) """,
            list_result)
        conn_db.commit()