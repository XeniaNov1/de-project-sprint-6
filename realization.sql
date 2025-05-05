--файл загружен дагом dags/sprint_6_files_dag.py
--командой в терминале забрала файл из контейнера в локальную папку
--2. создадим таблицу group_log

drop table if exists STV2025021827__STAGING.groups_log

create table STV2025021827__STAGING.groups_log
(
	group_id int PRIMARY KEY,
	user_id int,
	user_id_from int,
	event varchar(10),
	datetime datetime
)
order by group_id
 	SEGMENTED BY HASH(group_id) ALL NODES
 	PARTITION BY datetime::date
	GROUP BY calendar_hierarchy_day(datetime::date, 3, 2);

--3. копируем данные из файла
copy STV2025021827__STAGING.groups_log (
     group_id, user_id, user_id_from, event, datetime) 
     from 
     local 'C:\Users\Ethan\Gold_recovery_Project\project_de\sprint6\s6-lessons\group_log.csv'
     delimiter ',';

--4 создадим таблицу связей
    
drop table if exists STV2025021827__DWH.l_user_group_activity;

create table STV2025021827__DWH.l_user_group_activity
(
	hk_l_user_group_activity int PRIMARY KEY,
	hk_user_id bigint not null CONSTRAINT fk_user_socdem_user REFERENCES STV2025021827__DWH.h_users (hk_user_id),
	hk_group_id bigint not null CONSTRAINT fk_s_group_name_l_groups_dialogs REFERENCES STV2025021827__DWH.h_groups (hk_group_id),
	load_dt datetime,
	load_src varchar(20)
)
order by load_dt
 	SEGMENTED BY hk_l_user_group_activity ALL NODES
 	PARTITION BY load_dt::date
	GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

--5 Создать скрипты миграции в таблицу связи

INSERT INTO STV2025021827__DWH.l_user_group_activity(hk_l_user_group_activity, hk_user_id,hk_group_id,load_dt,load_src)

select hash(hu.hk_user_id, hg.hk_group_id),
	hu.hk_user_id,
	hg.hk_group_id,
	now() as load_dt,
	's3' as load_src
from STV2025021827__STAGING.groups_log as gl
left join STV2025021827__DWH.h_users hu on gl.user_id = hu.user_id 
left join STV2025021827__DWH.h_groups hg on gl.group_id = hg.group_id 
;

-- 6 Создать и наоплнить сателлит

drop table if exists STV2025021827__DWH.s_auth_history;

create table STV2025021827__DWH.s_auth_history
(
	hk_l_user_group_activity bigint not null CONSTRAINT fk_auth_history_user REFERENCES STV2025021827__DWH.l_user_group_activity (hk_l_user_group_activity),
	user_id_from int,
	event varchar(10),
	event_dt datetime,
	load_dt datetime,
	load_src varchar(20)
)
order by load_dt
 	SEGMENTED BY hk_l_user_group_activity ALL NODES
 	PARTITION BY load_dt::date
	GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);
	
INSERT INTO STV2025021827__DWH.s_auth_history(hk_l_user_group_activity, user_id_from,event,event_dt,load_dt,load_src)

select
	luga.hk_l_user_group_activity,
	gl.user_id_from,
	gl.event,
	gl.datetime,
	now() as load_dt,
	's3' as load_src

from STV2025021827__STAGING.groups_log as gl
left join STV2025021827__DWH.h_groups as hg on gl.group_id = hg.group_id
left join STV2025021827__DWH.h_users as hu on gl.user_id = hu.user_id
left join STV2025021827__DWH.l_user_group_activity as luga on hg.hk_group_id = luga.hk_group_id and hu.hk_user_id = luga.hk_user_id
;

-- 7 ответ бизнесу
-- CTE 1

with user_group_messages as (
    select 
            lgd.hk_group_id, count(lum.hk_user_id) as cnt_users_in_group_with_messages
            from STV2025021827__DWH.l_groups_dialogs lgd 
            join STV2025021827__DWH.l_user_message lum on lgd.hk_message_id = lum.hk_message_id
            group by lgd.hk_group_id having count(lum.hk_message_id) > 0 
)

select hk_group_id,
            cnt_users_in_group_with_messages
from user_group_messages
order by cnt_users_in_group_with_messages
limit 10
;

-- CTE 2

with user_group_log as (
    select hg.hk_group_id, count(distinct gl.user_id) as cnt_added_users
    from STV2025021827__STAGING.groups_log gl
    left join STV2025021827__DWH.h_groups hg on gl.group_id = hg.group_id
    where gl.event = 'add'
    group by hg.hk_group_id
)

select hk_group_id
            ,cnt_added_users
from user_group_log
order by cnt_added_users
limit 10
;

--ответ бизнесу
with user_group_messages as (
    select 
            lgd.hk_group_id, count(lum.hk_user_id) as cnt_users_in_group_with_messages
            from STV2025021827__DWH.l_groups_dialogs lgd 
            join STV2025021827__DWH.l_user_message lum on lgd.hk_message_id = lum.hk_message_id
            group by lgd.hk_group_id 
            having count(lum.hk_message_id) > 0 
),
user_group_log as (
    select hg.hk_group_id, count(distinct gl.user_id) as cnt_added_users
    from STV2025021827__STAGING.groups_log gl
    left join STV2025021827__DWH.h_groups hg on gl.group_id = hg.group_id
    where gl.event = 'add'
    group by hg.hk_group_id
)



select
	ugm.hk_group_id, 
	ugl.cnt_added_users, 
	ugm.cnt_users_in_group_with_messages, 
	cnt_added_users/cnt_users_in_group_with_messages as group_conversion
from user_group_messages ugm
left join user_group_log ugl on ugm.hk_group_id = ugl.hk_group_id
order by cnt_added_users/cnt_users_in_group_with_messages desc;