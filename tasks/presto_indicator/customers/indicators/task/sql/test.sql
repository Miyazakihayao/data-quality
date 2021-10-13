select
'presto0820' as db_type,
'sf10820' as db_name,
'lineitem' as table_name,
null as col_name,
'$YYYY$mm$dd$HH' as task_time,
'cus_cnt1' as rule_name,
sum(5) as value,
cast(map_agg(custkey,
cast(MAP_FROM_ENTRIES(array[('name',
address),
('address',
address),
('nationkey',
address)]) as JSON)) as JSON)
from
sf1.customer
where
custkey<10