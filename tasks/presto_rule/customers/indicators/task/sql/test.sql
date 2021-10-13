select
'prestos' as db_type,
'sf1' as db_name,
'lineitem' as table_name,
'linestatus' as col_name,
'$YYYY$mm$dd' as task_time,
'cus_cnt' as rule_name,
sum(5) as value
from  sf1.lineitem where 1=1 and (linestatus not in ( 'a','b','c' ) or linestatus is null)
union all
select
'prestos' as db_type,
'sf1' as db_name,
'customer' as table_name,
'nationkey' as col_name,
'$YYYY$mm$dd' as task_time,
'cus_cnt' as rule_name,
sum(7) as value
from  sf1.customer where 1=1 and (nationkey not in ( 1,2,3 ) or nationkey is null)