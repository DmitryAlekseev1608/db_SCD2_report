import pandas as pd

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
                                cln.last_name || ' ' || cln.first_name || ' ' || cln.patronymic as fio,
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
                                crd.account as account
                            from public.alex_DWH_FACT_transactions trn
                            left join info.cards crd
                            on crd.card_num = trn.card_num
                            where trn.trans_date::date = TO_DATE('{self.update_dt}', 'YYYY-MM-DD')) tmp
                            left join info.accounts acn
                            on tmp.account = acn.account) tmp_a
                            left join info.clients cln
                            on tmp_a.client = cln.client_id) tmp_p
                            left join public.alex_dwh_fact_passport_blacklist pbl
                            on tmp_p.passport = pbl.passport_num
                            where (pbl.passport_num = tmp_p.passport and pbl.deleted_flg = false and pbl.effective_to = TO_DATE('2999-01-01', 'YYYY-MM-DD'))
                            or (tmp_p.passport_valid_to < tmp_p.trans_date)
                            or (tmp_p.passport_valid_to = tmp_p.trans_date)
                            """)
        conn_db.commit()