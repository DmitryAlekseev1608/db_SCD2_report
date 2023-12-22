----------------------------------------------------------------------------
-- Выборки (этот код приведен для удобства просмотра содержимого таблиц)
select * from public.alex_source;
select * from public.alex_stg;
select * from public.alex_target;
select * from public.alex_meta;
select * from public.alex_stg_del;

----------------------------------------------------------------------------
-- Подготовка самих таблиц
create table public.alex_source( 
	id integer,
	val varchar(50),
	update_dt timestamp(0)
);

create table public.alex_stg( 
	id integer,
	val varchar(50),
	update_dt timestamp(0)
);

create table public.alex_target (
	id integer,
	val varchar(50),
	start_dt timestamp(0),
	end_dt timestamp(0),
	is_deleted bool	
);

create table public.alex_meta (
    schema_name varchar(30),
    table_name varchar(30),
    max_update_dt timestamp(0)
);

create table public.alex_stg_del( 
	id integer
);

insert into public.alex_meta( schema_name, table_name, max_update_dt )
values( 'dwh','alex_source', to_timestamp('1900-01-01','YYYY-MM-DD') );

----------------------------------------------------------------------------
-- ЭКСПЕРИМЕНТЫ
-- После каждого из приведенных эксперименов запускается скрипт с SCD2 ниже.
-- Потом проверяется результат между alex_source и alex_target.

-- 1 эксперимент. Добавление базового набора данных:
insert into public.alex_source ( id, val, update_dt ) values ( 1, 'A', now() );
insert into public.alex_source ( id, val, update_dt ) values ( 2, 'B', now() );
insert into public.alex_source ( id, val, update_dt ) values ( 3, 'C', now() );
insert into public.alex_source ( id, val, update_dt ) values ( 4, 'D', now() );
-- запускаем скрипт с SCD2 ниже и проверяем таблицу alex_target.

-- 2 эксперимент. Добавление новой строчки:
insert into public.alex_source ( id, val, update_dt ) values ( 5, 'E', now() );
-- запускаем скрипт с SCD2 ниже и проверяем таблицу alex_target.

-- 3 эксперимент. Удаление строчки:
delete from public.alex_source where id = 2;
-- запускаем скрипт с SCD2 ниже и проверяем таблицу alex_target.

-- 4 эксперимент. Обновление строчки:
update public.alex_source set val = 'X', update_dt = now() where id = 3;
-- запускаем скрипт с SCD2 ниже и проверяем таблицу alex_target.

-- 5 эксперимент. Чистим содержимое таблиц, чтобы проще проверить alex_target
-- со строками A, B, C и D и заново формируем исходные условия.
truncate table public.alex_source;
truncate table public.alex_stg;
truncate table public.alex_target;
truncate table public.alex_meta;
truncate table public.alex_stg_del;
insert into public.alex_meta( schema_name, table_name, max_update_dt )
values( 'dwh','alex_source', to_timestamp('1900-01-01','YYYY-MM-DD') );
-- повторяем эксперимент номер 1 полностью. Потом одновременно запускаем эксперименты 2-4.
-- запускаем скрипт с SCD2 ниже и проверяем таблицу alex_target.

----------------------------------------------------------------------------
-- СКРИПТ SCD2, что запускается по каждому эксперименту.
-- 1. Очистка стейджинговых таблиц:
delete from public.alex_stg;
delete from public.alex_stg_del;

-- 2. Захват данных из источника (измененных с момента последней загрузки) в стейджинг:
insert into public.alex_stg( id, val, update_dt )
select id, val, update_dt from public.alex_source
where update_dt > ( select max_update_dt from public.alex_meta where schema_name='dwh' and table_name='alex_source' );

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

-- 8. Фиксация транзакции.
commit;
