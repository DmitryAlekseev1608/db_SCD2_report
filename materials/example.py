import os

import pandas as pd
import psycopg2


# Создание подключения к PostgreSQL
conn_src = psycopg2.connect(database="source_db",  # Замените на имя вашей базы данных
                        host="your_host",  # Замените на ваш хост
                        user="your_user",  # Замените на ваше имя пользователя
                        password="your_password",  # Замените на ваш пароль
                        port="5432")

conn_tgt = psycopg2.connect(database="target_db",  # Замените на имя вашей базы данных
                        host="your_host",  # Замените на ваш хост
                        user="your_user",  # Замените на ваше имя пользователя
                        password="your_password",  # Замените на ваш пароль
                        port="5432")


# Отключение автокоммита
conn_src.autocommit = False
conn_tgt.autocommit = False

# Создание курсора
cursor_src = conn_src.cursor()
cursor_tgt = conn_tgt.cursor()

#####################################################################################################
### Загрузка в stg

# DDL 
# -- FACT TABLES
# create table your_schema.fact_rides (
# 	ride_id int,
# 	from_text varchar(200),
# 	to_text varchar(200),
# 	distance numeric(5, 2),
# 	price numeric(7, 2),
# 	client_phone char(18),
# 	driver_id int,
# 	car_id char(9),
# 	arrival_time timestamp(0),
# 	start_time timestamp(0),
# 	end_time timestamp(0),
# 	processed_time timestamp(0)
# );
#
# create table your_schema.stg_rides (
# 	ride_id int,
# 	from_text varchar(200),
# 	to_text varchar(200),
# 	distance numeric(5, 2),
# 	price numeric(7, 2),
# 	client_phone char(18),
# 	car_id char(9),
# 	arrival_time timestamp(0),
# 	start_time timestamp(0),
# 	end_time timestamp(0)
# );
#
# create table your_schema.dim_drivers(
# 	personnel_num serial,
# 	license_num char(12),
# 	first_name varchar(20),
# 	last_name varchar(20),
# 	middle_name varchar(20),
# 	license_date date,
# 	card_num char(19),
# 	update_time timestamp(0),
# 	birth_date date,
# 	create_time timestamp(0),
# 	processed_time timestamp(0)
# );
#
# create table your_schema.fact_waybills(
# 	waybill_id char(6),
# 	driver_id int,
# 	car_id char(9),
# 	work_start_time timestamp(0),
# 	work_end_time timestamp(0),
# 	issue_time timestamp(0),
# 	processed_time timestamp(0)
# );
#
# create table your_schema.stg_waybills(
# 	waybill_id char(6),
# 	driver_id int,
# 	car_id char(9),
# 	work_start_time timestamp(0),
# 	work_end_time timestamp(0),
# 	issue_time timestamp(0)
# );
#
# -- STAGING TABLES
# create table your_schema.stg_movement(
# 	movement_id int,
# 	car_id char(9),
# 	ride_id int,
# 	event varchar(6),
# 	event_time timestamp(0)
# );
#
# create table your_schema.stg_drivers(
# 	license_num char(12),
# 	first_name varchar(20),
# 	last_name varchar(20),
# 	middle_name varchar(20),
# 	license_valid_to date,
# 	card_num char(19),
# 	update_time timestamp(0),
# 	birth_date date
# );
#
# -- REPORTS
# create table your_schema.rep_driver_payments (
# 	personnel_num int,
# 	last_name varchar(20),
# 	first_name varchar(20),
# 	middle_name varchar(20),
# 	card_num char(19),
# 	amount numeric(11, 2),
# 	report_date date
# );


# stg_drivers

cursor_src.execute(""" SELECT
                           driver_license,
                           first_name,
                           last_name,
                           middle_name,
                           driver_valid_to,
                           card_num,
                           update_dt,
                           birth_dt
                       FROM main.drivers """)
records = cursor_src.fetchall()
names = [x[0] for x in cursor_src.description]
df = pd.DataFrame(records, columns=names)

cursor_tgt.executemany(""" INSERT INTO your_schema.stg_drivers(
                               license_num,
                               first_name,
                               last_name,
                               middle_name,
                               license_valid_to,
                               card_num,
                               update_time,
                               birth_date
                           ) VALUES(%s, %s, %s, %s, %s, %s, %s, %s) """, df.values.tolist())


cursor_src.execute(""" SELECT
                           driver_license,
                           first_name,
                           last_name,
                           middle_name,
                           driver_valid_to,
                           card_num,
                           update_dt,
                           birth_dt
                       FROM main.drivers """)
records = cursor_src.fetchall()
names = [x[0] for x in cursor_src.description]
df = pd.DataFrame(records, columns=names)

cursor_tgt.executemany(""" INSERT INTO your_schema.stg_drivers(
                               license_num,
                               first_name,
                               last_name,
                               middle_name,
                               license_valid_to,
                               card_num,
                               update_time,
                               birth_date
                           ) VALUES(%s, %s, %s, %s, %s, %s, %s, %s) """, df.values.tolist())


cursor_src.execute(""" SELECT
                           driver_license,
                           first_name,
                           last_name,
                           middle_name,
                           driver_valid_to,
                           card_num,
                           update_dt,
                           birth_dt
                       FROM main.drivers """)
records = cursor_src.fetchall()
names = [x[0] for x in cursor_src.description]
df = pd.DataFrame(records, columns=names)

cursor_tgt.executemany(""" INSERT INTO your_schema.stg_drivers(
                               license_num,
                               first_name,
                               last_name,
                               middle_name,
                               license_valid_to,
                               card_num,
                               update_time,
                               birth_date
                           ) VALUES(%s, %s, %s, %s, %s, %s, %s, %s) """, df.values.tolist())



# stg_waybills

WAYBILLS_FILES_PREFIX = '/mnt/files/waybills'

# Assuming connection to target database is already established and named conn_tgt
cursor_tgt = conn_tgt.cursor()

# Get the list of files in the directory
files = os.listdir(WAYBILLS_FILES_PREFIX)

if files:
    # Read the first file
    first_file = files[0]
    df = pd.read_xml(f'{WAYBILLS_FILES_PREFIX}/{first_file}',
                     xpath='//waybills/waybill|//waybills/waybill/driver|//waybills/waybill/period')

    # Assuming the structure of XML and required columns are as follows:
    # <waybill number="...">
    #   <driver license="..."/>
    #   <car plate_num="..."/>
    #   <period start="..." stop="..." issuedt="..."/>
    # </waybill>
    df['waybill_id'] = df['number'][0]
    df = df[['waybill_id', 'license', 'plate_num', 'start', 'stop', 'issuedt']]

    cursor_tgt.executemany(""" INSERT INTO your_schema.stg_waybills(
                                    waybill_id,
                                    driver_id,
                                    car_id,
                                    work_start_time,
                                    work_end_time,
                                    issue_time
                              ) VALUES(%s, %s, %s, %s, %s, %s) """, df.values.tolist())
    conn_tgt.commit()


#####################################################################################################
### Загрузка dim

# dim_drivers
cursor_tgt.execute(""" 
    INSERT INTO your_schema.dim_drivers (
        license_num,        
        first_name,
        last_name,
        middle_name,
        license_date,     
        card_num,
        update_time,       
        birth_date,         
        create_time,        
        processed_time      
    )
    SELECT
        stg.license_num,    
        stg.first_name,
        stg.last_name,
        stg.middle_name,
        stg.license_valid_to, 
        stg.card_num,
        stg.update_time,    
        stg.birth_date,     
        NOW(),               
        NOW()                
    FROM your_schema.stg_drivers stg
    LEFT JOIN your_schema.dim_drivers tgt
    ON stg.license_num = tgt.license_num
    WHERE tgt.license_num IS NULL        
""")



#####################################################################################################
### Загрузка fact

# fact_rides

cursor_tgt.execute(""" 
    INSERT INTO your_schema.stg_fact_rides (
        ride_id,
        point_from_txt,
        point_to_txt,
        distance_val,
        price_amt,
        client_phone_num,
        car_plate_num,               --
        ride_arrival_dt,
        ride_start_dt,
        ride_end_dt
    )
    SELECT *
    FROM (
        SELECT
            r.ride_id,
            MAX(r.point_from) AS point_from_txt,
            MAX(r.point_to) AS point_to_txt,
            MAX(r.distance) AS distance_val,
            MAX(r.price) AS price_amt,
            MAX(r.client_phone) AS client_phone_num,
            MAX(m.car_plate_num) AS car_plate_num, -- Assuming car_plate_num comes from the movement table
            MAX(CASE WHEN m.event = 'READY' THEN m.dt END) AS ride_arrival_dt,
            MAX(CASE WHEN m.event = 'BEGIN' THEN m.dt END) AS ride_start_dt,
            MAX(CASE WHEN (m.event = 'END' OR m.event = 'CANCEL') THEN m.dt END) AS ride_end_dt
        FROM your_schema.stg_rides r
        LEFT JOIN your_schema.stg_movement m ON r.ride_id = m.ride_id 
        GROUP BY r.ride_id
    ) grp
    LEFT JOIN your_schema.stg_fact_rides tgt ON grp.ride_id = tgt.ride_id
    WHERE grp.ride_end_dt IS NOT NULL AND tgt.ride_id IS NULL
""")





conn_tgt.commit()

cursor_src.close()
cursor_tgt.close()
conn_src.close()
conn_tgt.close()
