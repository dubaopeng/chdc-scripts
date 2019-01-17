SET mapred.job.name='b_customer_member_relation-全渠道客户与会员关系表';
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


--add jar hdfs://master01.bigdata.shuyun.com:8020/user/hive/jar/plat-hive-udf-1.0.0.jar;
add jar hdfs://standard-cluster/user/hive/jar/plat-hive-udf-1.0.0.jar;
create temporary function cardid_hash as 'com.shuyun.plat.hive.udf.CardPlanIdHashUDF';

-- 创建客户与会员关系数据同步表
CREATE EXTERNAL TABLE IF NOT EXISTS dw_source.`s_customer_member_relation`(
	`uni_id` string,
	`card_plan_id` string,
    `member_id` string
)
partitioned by(`dt` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/hive/source_data/std_source/member/relation';

-- 批量重设分区
msck repair TABLE dw_source.`s_customer_member_relation`;

-- 创建客户与会员关系基表
CREATE TABLE IF NOT EXISTS dw_business.`b_customer_member_relation`(
	`uni_id` string,
	`card_plan_id` string,
    `member_id` string
)
partitioned by(`part` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' lines terminated by '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");

-- 由于关系从在解绑的情况，计算增量是不对的，所以全量覆盖
insert overwrite table dw_business.`b_customer_member_relation` partition(part)
select a.uni_id,a.card_plan_id,a.member_id,cardid_hash(a.card_plan_id) as part
from (
	select *,row_number() over (partition by uni_id,card_plan_id,member_id) as num 
	from dw_source.s_customer_member_relation where dt='${stat_date}'
) a 
where a.num = 1
distribute by cardid_hash(a.card_plan_id);







