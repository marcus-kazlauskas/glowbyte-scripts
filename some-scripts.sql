----
--1. Size of B-tables

select 
    'B-table size of 1 instance:
' || string_agg(rpad(t1.tabl_name, 30, ' ') || ': ' || lpad(pg_size_pretty(pg_total_relation_size(table_schema || '.' || t1.table_name)), 10, ' ') || ', ' || lpad(qty::text, 7, ' ') || ' rows', e'\n')
from (
    select table_schema, table_name
    from information_schema.tables
    where table_schema = 'grnplm_source_stg' and table_type = 'BASE_TABLE' and table_name like '%b_source_%'
    order by table_name
) t1
join (
    select 'b_source_0001_0001'::text as table_name, count(*) as qty from grnplm_source_stg.b_source_0001_0001
    union
    select 'b_source_0001_0002'::text as table_name, count(*) as qty from grnplm_source_stg.b_source_0001_0002
    union
    select 'b_source_0001_0003'::text as table_name, count(*) as qty from grnplm_source_stg.b_source_0001_0003
) t2
on t1.table_name = t2.table_name;


--2. Check B-tables

select 
    case count(*)
    when 0 then 'OK'::text
    else 'ERROR:
Empty B-tables (table_name):
' || string_agg(table_name, e'\n') end as result
from (
    select 'b_source_0001_0001'::text as table_name, count(*) as qty from grnplm_source_stg.b_source_0001_0001
    union
    select 'b_source_0001_0002'::text as table_name, count(*) as qty from grnplm_source_stg.b_source_0001_0002
    union
    select 'b_source_0001_0003'::text as table_name, count(*) as qty from grnplm_source_stg.b_source_0001_0003
) t
where t.qty = 0
union all
select 
    case count(*)
    when 0 then 'OK'
    else 'ERROR:
Nulls in PK fields (table name - fields with nulls):
' || string_agg(table_name || '-' || fields_with_nulls, e'\n') end as result
from (
    select 
        'b_source_0001_0001' as table_name,
        array_to_string(array[
            case when t2.id_contains_null then 'id' else null end
        ], ', ') as fields_with_nulls
    from (
        select bool_or(t.id is null) as id_contains_null
        from grnplm_source_stg.b_source_0001_0001 t
    ) t2
    union all
    select 
        'b_source_0001_0002' as table_name,
        array_to_string(array[
            case when t2.id_contains_null then 'id' else null end
        ], ', ') as fields_with_nulls
    from (
        select bool_or(t.id is null) as id_contains_null
        from grnplm_source_stg.b_source_0001_0002 t
    ) t2
    union all
    select 
        'b_source_0001_0003' as table_name,
        array_to_string(array[
            case when t2.id_contains_null then 'id' else null end
        ], ', ') as fields_with_nulls
    from (
        select bool_or(t.id is null) as id_contains_null
        from grnplm_source_stg.b_source_0001_0003 t
    ) t2
) t
where t.fields_with_nulls <> ''
union all
select 
    case count(*)
    when 0 then 'OK'
    else 'ERROR:
Non unique PK fields (table name - field names - field values - repeat count):
' || string_agg(table_name || '-' || pk_names || '-' || pk_values || '-' || repeat_value, e'\n') end as result
from (
    select
        'b_source_0001_0001' as table_name,
        'id' as pk_names,
        '(' || t.id || ')' as pk_values,
        qty as repeat_value
    from (
        select id, count(*) as qty
        from grnplm_source_stg.b_source_0001_0001
        group by 1
        having count(*) > 1
    ) t 
    union all
    select
        'b_source_0001_0002' as table_name,
        'id' as pk_names,
        '(' || t.id || ')' as pk_values,
        qty as repeat_value
    from (
        select id, count(*) as qty
        from grnplm_source_stg.b_source_0001_0002
        group by 1
        having count(*) > 1
    ) t 
    union all
    select
        'b_source_0001_0003' as table_name,
        'id' as pk_names,
        '(' || t.id || ')' as pk_values,
        qty as repeat_value
    from (
        select id, count(*) as qty
        from grnplm_source_stg.b_source_0001_0003
        group by 1
        having count(*) > 1
    ) t 
) t;


--3. Get order of creating views (в GP при удалении объектов удаляются и связанные с ними представления, поэтому их приходилось часто пересоздавать)

select string_agg('create or replace view' || nspname::text || '.' || relname::text || e' as \n' || pg_get_viewdef('' || nspname::text || '.' || realname::text || '', true), e'\n\n')
from (
    select *
    from pg_stat_last_operation
    join pg_class on pg_class.oid = pg_stat_last_operation.objid
    join pg_namespace on pg_namespace.oid = pg_class.relnamespace
    where objid in (
        select ful_name::regclass::oid
        from (
            select table_schema || '.' || table_name as full_name
            from information_schema.views
            where table_schema like 'grnplm_source_%'
        )
    ) and staactionname = 'create'
    order by statime
) as foo


--4. Make partitioning (повышает эффективность работы с таблицами в GP)

select string_agg(
'create table ' || table_schema || '.' || table_name || '_copy_ (like ' || table_schema || '.' || table_name || ' including defaults)
with (
appenonly=true,
compresstype=zstd,
compresslevel=1
)
partition by list (inst_id)
(
partition p_1 values (1),
partition p_2 values (2)
);', e'\n')
from information_schema.tables 
where table_schema = 'grnplm_source_stg' and table_type = 'BASE_TABLE'
    and (table_name like 'td\_%' or table_name like 'ti\_%')
    and not (table_name like '%p\_1' or table_name like '%p\_2'); --1 создание копий таблиц, но уже с партицированием
    
select string_agg(
'insert into ' || table_schema || '.' || table_name || '_copy_ select * from ' || table_schema || '.' || table_name || ';', e'\n')
from information_schema.tables 
where table_schema = 'grnplm_source_stg' and table_type = 'BASE_TABLE'
    and (table_name like 'td\_%' or table_name like 'ti\_%')
    and not (table_name like '%p\_1' or table_name like '%p\_2')
    and not (table_name like 'td\_%_copy_' or table_name like 'ti\_%_copy_'); --2 копирование данных в новые таблицы
    
select string_agg(
'drop table ' || table_schema || '.' || table_name || ' cascade;', e'\n')
from information_schema.tables 
where table_schema = 'grnplm_source_stg' and table_type = 'BASE_TABLE'
    and (table_name like 'td\_%' or table_name like 'ti\_%')
    and not (table_name like '%p\_1' or table_name like '%p\_2')
    and not (table_name like 'td\_%_copy_' or table_name like 'ti\_%_copy_'); --3 удаление старых таблиц, перед выполнением сохранить результат выполнения скрипта 4
    
select string_agg(
'alter table ' || table_schema || '.' || table_name || '_copy_ rename to ' || table name || ';', e'\n')
from information_schema.tables 
where table_schema = 'grnplm_source_stg' and table_type = 'BASE_TABLE'
    and (table_name like 'td\_%' or table_name like 'ti\_%')
    and not (table_name like '%p\_1' or table_name like '%p\_2')
    and not (table_name like 'td\_%_copy_' or table_name like 'ti\_%_copy_'); --4


--
