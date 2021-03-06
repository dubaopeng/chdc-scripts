SET mapred.job.name='b_std_member_base_info-会员基础信息表';
set hive.tez.auto.reducer.parallelism=true;
set hive.tez.container.size=16384;
set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.noconditionaltask.size=4915;
set tez.runtime.unordered.output.buffer.size-mb=1640;
set tez.runtime.io.sort.mb=6553;
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

-- 创建会员卡信息表
CREATE TABLE IF NOT EXISTS dw_business.`b_std_member_base_info`(
	`tenant` string,
	`card_plan_id` string,
    `member_id` string,
	`plat_code` string,
    `uni_shop_id` string,
    `card_number` string,
	`card_name` string,
	`bind_mobile` string,
	`name` string,
	`birthday` bigint,
	`gender` string,
	`shop_id` string,
	`guide_id` string,
	`grade` int,
	`grade_period` string,
	`available_point` bigint,
	`total_point` bigint,
	`consumed_point` bigint,
	`expired_point` bigint,
	`created` string,
	`modified` string
)
partitioned by(`part` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' lines terminated by '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");

-- 在每天会员的相关数据同步完成后，对会员基础信息进行合并
insert overwrite table dw_business.`b_std_member_base_info` partition(part)
select t.tenant,t.card_plan_id,t.member_id,t.plat_code,t.uni_shop_id,t.card_number,t.card_name,t.bind_mobile,t.name,t.birthday,t.gender,t.shop_id,t.guide_id,
		t.grade,t.grade_period,
		t.available_point,t.total_point,t.consumed_point,t.expired_point,
		t.created,t.modified,
		cardid_hash(t.card_plan_id) as part
from (
	select a.tenant,a.card_plan_id,a.member_id,a.plat_code,a.uni_shop_id,a.card_number,a.card_name,a.bind_mobile,a.name,a.birthday,a.gender,a.shop_id,a.guide_id,
		b.grade,b.grade_period,
		c.available_point,c.total_point,c.consumed_point,c.expired_point,
		a.created,a.modified
	from dw_business.b_std_member a
	left join dw_business.b_member_grade b
	on a.card_plan_id=b.card_plan_id and a.member_id=b.member_id and a.tenant=b.tenant
	left join dw_business.b_member_point c
	on a.card_plan_id=c.card_plan_id and a.member_id=c.member_id and a.tenant=c.tenant
)t
distribute by cardid_hash(t.card_plan_id);






