SET mapred.job.name='dashboard_result_shop_rootcid';
SET hive.exec.compress.output=true;
set hive.cbo.enable=true;
SET mapred.max.split.size=512000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET mapred.output.compression.type=BLOCK;
SET mapreduce.map.output.compress=true;
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
SET hive.exec.compress.output.codec=org.apache.hadoop.io.compress.SnappyCodec;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.dynamic.partition=true;
SET mapreduce.reduce.shuffle.input.buffer.percent =0.6;
SET hive.exec.max.created.files=655350;
SET hive.exec.max.dynamic.partitions=10000000;
SET hive.exec.max.dynamic.partitions.pernode=10000000;
set hive.stats.autogather=false;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task = 512000000;
set hive.support.concurrency=false;


set MODIFIED = substr(current_timestamp,1,19);

----------------------------
----3.3.店铺类目数据计算
----new_dashboard.dashboard_result_shop_rootcid
----------------------------


drop table if exists new_dashboard.dashboard_result_shop_rootcid;
create table if not exists new_dashboard.dashboard_result_shop_rootcid(
	shop_id string,
	root_cid string,
	root_cid_name string,
	modified string,
	stat_date string
)ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

insert overwrite table new_dashboard.dashboard_result_shop_rootcid
select 
    dp_id as shop_id,
	cid  as root_cid,
	name as root_cid_name,
    ${hiveconf:MODIFIED} as modified,
    '${stat_date}' as stat_date
from 
	(
	select 
		dp_id,
		cid,
		name
	from dw_base.b_top_shop_item_cats  -- dw_base.b_std_cat
	where part = '${stat_date}'
		and dp_id is not NULL and cid is not NULL and name is not NULL
		and is_parent = 'true' and parent_cid = '0' and status = 'normal'
	group by 
		dp_id,
		cid,
		name
	)a;
	



