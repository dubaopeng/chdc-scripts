SET mapred.job.name='b_member_point-会员积分信息';
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
CREATE EXTERNAL TABLE IF NOT EXISTS dw_source.`s_member_point`(
	`card_plan_id` string,
    `member_id` string,
    `available_point` bigint,
	`total_point` bigint,
	`consumed_point` bigint,
	`expired_point` bigint,
	`create_time` string,
	`update_time` string,
	`tenant` string
)
partitioned by(`dt` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/hive/source_data/std_source/member/point';

-- 批量重设分区
msck repair TABLE dw_source.`s_member_point`;

-- 创建会员积分信息基表
CREATE TABLE IF NOT EXISTS dw_business.`b_member_point`(
	`card_plan_id` string,
    `member_id` string,
    `available_point` bigint,
	`total_point` bigint,
	`consumed_point` bigint,
	`expired_point` bigint,
	`create_time` string,
	`update_time` string,
	`tenant` string
)
partitioned by(`part` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' lines terminated by '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY",'orc.bloom.filter.columns'='card_plan_id');

-- 合并去重取最新数据
insert overwrite table dw_business.b_member_point partition(part)
select t.card_plan_id,t.member_id,t.available_point,t.total_point,t.consumed_point,t.expired_point,t.create_time,t.update_time,t.tenant,
		cardid_hash(t.card_plan_id) as part
 from (
	select re.card_plan_id,re.member_id,re.available_point,re.total_point,re.consumed_point,re.expired_point,re.create_time,re.update_time,re.tenant,
		row_number() over(distribute by re.card_plan_id,re.member_id sort by re.update_time desc) as num
	from (
		select t1.card_plan_id,t1.member_id,t1.available_point,t1.total_point,t1.consumed_point,t1.expired_point,t1.create_time,t1.update_time,t1.tenant
			from dw_business.b_member_point t1
		union all 
		select t2.card_plan_id,t2.member_id,t2.available_point,t2.total_point,t2.consumed_point,t2.expired_point,t2.create_time,
			if(t2.update_time=='',t2.create_time,t2.update_time) as update_time,t2.tenant
			from dw_source.s_member_point t2 where t2.dt='${stat_date}'
	) re
) t
where t.num=1
distribute by cardid_hash(t.card_plan_id);





