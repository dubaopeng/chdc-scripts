SET mapred.job.name='b_card_grade_info-卡等级信息表同步';
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


-- 创建卡等级信息同步表
CREATE EXTERNAL TABLE IF NOT EXISTS dw_source.`s_card_grade_info`(
	`card_plan_id` string,
    `grade` int,
	`grade_name` string,
	`ordar` int,
	`tenant` string
)
partitioned by(`dt` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/hive/source_data/std_source/member/card_grade';

-- 批量重设分区
msck repair TABLE dw_source.`s_card_grade_info`;

-- 创建卡等级信息表
CREATE TABLE IF NOT EXISTS dw_business.`b_card_grade_info`(
	`card_plan_id` string,
    `grade` int,
	`grade_name` string,
	`ordar` int,
	`tenant` string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' lines terminated by '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY",'orc.bloom.filter.columns'='card_plan_id');

-- 由于等级存在变更，所以全量覆盖
insert overwrite table dw_business.`b_card_grade_info`
select a.card_plan_id,a.grade,a.grade_name,a.ordar,a.tenant
from (
	select re.card_plan_id,re.grade,re.grade_name,re.ordar,re.tenant,
		row_number() over (partition by re.card_plan_id,re.grade) as num
	from(
		select card_plan_id,grade,grade_name,ordar,tenant from dw_business.b_card_grade_info
		union all
		select card_plan_id,grade,grade_name,ordar,tenant
		from dw_source.s_card_grade_info where dt='${stat_date}'
	)re
) a 
where a.num = 1;


