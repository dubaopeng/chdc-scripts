SET mapred.job.name='b_member_grade-会员等级信息';
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

-- 创建每日会员等级增量数据
CREATE EXTERNAL TABLE IF NOT EXISTS dw_source.`s_member_grade`(
	`card_plan_id` string,
    `member_id` string,
    `grade` int,
	`grade_period` string
)
partitioned by(`dt` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/hive/source_data/std_source/member/grade';

-- 批量重设分区
msck repair TABLE dw_source.`s_member_grade`;

-- 创建会员等级信息基表
CREATE TABLE IF NOT EXISTS dw_business.`b_member_grade`(
	`card_plan_id` string,
    `member_id` string,
    `grade` int,
	`grade_period` string
)
partitioned by(`part` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' lines terminated by '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");

-- 合并并去重插入结果
insert overwrite table dw_business.`b_member_grade` partition(part)
select t.card_plan_id,t.member_id,t.grade,t.grade_period,cardid_hash(t.card_plan_id) as part
 from (
	select *,row_number() over (partition by card_plan_id,member_id) as num 
    from dw_source.`s_member_grade` where dt='${stat_date}'
) t
where t.num=1
distribute by cardid_hash(t.card_plan_id);





