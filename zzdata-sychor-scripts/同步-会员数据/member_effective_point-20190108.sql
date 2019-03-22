SET mapred.job.name='b_member_effective_point-会员有效积分信息';
--set hive.execution.engine=mr;
set hive.tez.container.size=6144;
set hive.cbo.enable=true;
SET hive.exec.compress.output=true;
SET mapred.max.split.size=512000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET mapred.output.compression.type=BLOCK;
SET mapreduce.map.output.compress=true;
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
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

add jar hdfs://standard-cluster/user/hive/jar/plat-hive-udf-1.0.0.jar;
create temporary function cardid_hash as 'com.shuyun.plat.hive.udf.CardPlanIdHashUDF';

-- 创建每日会员积分增量数据
CREATE EXTERNAL TABLE IF NOT EXISTS dw_source.`s_member_efffect_point`(
	`card_plan_id` string,
    `member_id` string,
	`point` int,
    `effective_date` string,
	`occur_date` string,
	`create_date` string,
	`overdue_date` string,
	`valid` int,
	`modify_time` string,
	`tenant` string
)
partitioned by(`dt` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/hive/source_data/std_source/member/effectivepoint';

-- 批量重设分区
msck repair TABLE dw_source.`s_member_efffect_point`;

-- 创建会员积分信息基表
CREATE TABLE IF NOT EXISTS dw_business.`b_member_efffect_point`(
	`card_plan_id` string,
    `member_id` string,
	`point` int,
    `effective_date` string,
	`occur_date` string,
	`create_date` string,
	`overdue_date` string,
	`valid` int,
	`modify_time` string,
	`tenant` string
)
partitioned by(`part` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' lines terminated by '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY",'orc.bloom.filter.columns'='card_plan_id');

-- 合并去重取最新数据
insert overwrite table dw_business.b_member_efffect_point partition(part)
select a.card_plan_id,a.member_id,a.point,a.effective_date,a.occur_date,a.create_date,a.overdue_date,a.valid,a.modify_time,a.tenant,
	   cardid_hash(a.card_plan_id) as part
from (
	select re.card_plan_id,re.member_id,re.point,re.effective_date,re.occur_date,re.create_date,re.overdue_date,re.valid,re.modify_time,re.tenant,
		row_number() over (partition by re.card_plan_id,re.member_id order by re.modify_time desc) as num 
	from(
		select t.card_plan_id,t.member_id,t.point,t.effective_date,t.occur_date,t.create_date,t.overdue_date,t.valid,t.modify_time,t.tenant
		from dw_business.b_member_efffect_point t
		union all
		select t2.card_plan_id,t2.member_id,t2.point,t2.effective_date,t2.occur_date,t2.create_date,t2.overdue_date,t2.valid,
		if(t2.modify_time='',t2.occur_date,t2.modify_time) as modify_time,t2.tenant
		from dw_source.s_member_efffect_point t2 where t2.dt='${stat_date}'
	)re
) a 
where a.num = 1
distribute by cardid_hash(a.card_plan_id);





