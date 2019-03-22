SET mapred.job.name='b_std_member-会员基础信息';
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

-- 创建每日会员卡增量数据
CREATE EXTERNAL TABLE IF NOT EXISTS dw_source.`s_std_member`(
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
	`created` string,
	`modified` string,
	`tenant` string
)
partitioned by(`dt` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/hive/source_data/std_source/member/info';

-- 批量重设分区
msck repair table dw_source.`s_std_member`;

-- 创建会员卡信息表
CREATE TABLE IF NOT EXISTS dw_business.`b_std_member`(
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
	`created` string,
	`modified` string,
	`tenant` string
)
partitioned by(`part` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' lines terminated by '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY",'orc.bloom.filter.columns'='card_plan_id');


-- 合并并去重插入结果
insert overwrite table dw_business.b_std_member partition(part)
select t.card_plan_id,t.member_id,t.plat_code,t.uni_shop_id,t.card_number,t.card_name,t.bind_mobile,t.name,t.birthday,t.gender,t.shop_id,t.guide_id,t.created,t.modified,t.tenant,cardid_hash(t.card_plan_id) as part
 from (
	select re.card_plan_id,re.member_id,re.plat_code,re.uni_shop_id,re.card_number,re.card_name,re.bind_mobile,re.name,re.birthday,re.gender,re.shop_id,re.guide_id,re.created,re.modified,re.tenant,
		row_number() over(distribute by re.card_plan_id,re.member_id,re.card_number sort by re.modified desc) as num
	from (
		select t1.card_plan_id,t1.member_id,t1.plat_code,t1.uni_shop_id,t1.card_number,t1.card_name,t1.bind_mobile,t1.name,t1.birthday,t1.gender,t1.shop_id,t1.guide_id,t1.created,t1.modified,t1.tenant
			from dw_business.`b_std_member` t1
		union all 
		select t2.card_plan_id,t2.member_id,t2.plat_code,t2.uni_shop_id,t2.card_number,t2.card_name,t2.bind_mobile,t2.name,t2.birthday,t2.gender,t2.shop_id,t2.guide_id,t2.created,t2.modified,t2.tenant
			from dw_source.`s_std_member` t2 where t2.dt='${stat_date}'
	) re
) t
where t.num=1
distribute by cardid_hash(t.card_plan_id);





