SET mapred.job.name='b_preferential_send_history-权益发送数据';
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

-- 创建每日权益发送数据增量表
-- DROP TABLE IF EXISTS dw_source.`s_preferential_send_history`;
CREATE EXTERNAL TABLE IF NOT EXISTS dw_source.`s_preferential_send_history`(
	`uni_id` string,
    `plat_code` string,
    `plat_account` string,
	`partner` string,
    `shop_id` string,
	`send_time` string, 
	`send_type` string, 
	`preferential_id` string,
	`preferential_name` string,
	`preferential_type` string,
	`preferential_content` string,
	`is_verification` int
)
partitioned by(`dt` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS TEXTFILE 
LOCATION '/user/hive/source_data/business_source/preferential';

-- 批量重设分区
msck repair table dw_source.`s_preferential_send_history`;

-- 创建权益发送数据历史记录表
CREATE TABLE IF NOT EXISTS dw_business.`b_preferential_send_history`(
	`uni_id` string,
    `plat_code` string,
    `plat_account` string,
	`partner` string,
    `shop_id` string,
	`send_time` string, 
	`send_type` string, 
	`preferential_id` string,
	`preferential_name` string,
	`preferential_type` string,
	`preferential_content` string,
	`is_verification` int
)
partitioned by(`plat` string,`day` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' lines terminated by '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");


-- 增量导入权益发送历史记录表中
insert into dw_business.`b_preferential_send_history` partition(plat,day)
select t.uni_id,t.plat_code,t.plat_account,t.partner,t.shop_id,t.send_time,t.send_type,t.preferential_id,
t.preferential_name,t.preferential_type,t.preferential_content,t.is_verification,t.plat_code as plat,t.dt as day
from(
    select *,row_number() over (partition by uni_id,shop_id,send_time,preferential_id) as num 
    from dw_source.`s_preferential_send_history` where dt='${stat_date}'
) t 
where t.num=1
distribute by t.plat_code;






