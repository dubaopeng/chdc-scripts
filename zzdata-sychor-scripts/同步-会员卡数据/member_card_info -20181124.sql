SET mapred.job.name='b_std_membercard-会员卡数据';
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

-- add jar ${nameNode}/user/hive/jar/plat-hive-udf-1.0.0.jar;
add jar hdfs://master01.bigdata.shuyun.com:8020/user/hive/jar/plat-hive-udf-1.0.0.jar;
create temporary function cardid_hash as 'com.shuyun.plat.hive.udf.CardPlanIdHashUDF';

-- 创建每日会员卡增量数据
-- DROP TABLE IF EXISTS dw_source.`s_std_membercard`;
CREATE EXTERNAL TABLE IF NOT EXISTS dw_source.`s_std_membercard`(
	`card_plan_id` string,
    `member_id` string,
    `card_number` string,
	`card_name` string,
    `grade` string,
	`grade_name` string,
	`grade_period` string, 
	`available_point` bigint, 
	`upcoming_expired_point` bigint,
	`total_point` bigint,
	`consumed_point` bigint,
	`expired_point` bigint,
	`bind_mobile` string,
	`name` string,
	`birthday` bigint
)
partitioned by(`dt` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/hive/source_data/business_source/membercard';

-- 批量重设分区
msck repair table dw_source.`s_std_membercard`;

-- 创建会员卡信息表
CREATE TABLE IF NOT EXISTS dw_base.`b_std_membercard`(
	`card_plan_id` string,
    `member_id` string,
    `card_number` string,
	`card_name` string,
    `grade` string,
	`grade_name` string,
	`grade_period` string, 
	`available_point` bigint, 
	`upcoming_expired_point` bigint,
	`total_point` bigint,
	`consumed_point` bigint,
	`expired_point` bigint,
	`bind_mobile` string,
	`name` string,
	`birthday` bigint
)
partitioned by(`part` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' lines terminated by '\n'
STORED AS RCFILE;

-- 创建会员卡临时表,存放去重数据
DROP TABLE IF EXISTS dw_source.`s_std_membercard_temp`;
CREATE TABLE IF NOT EXISTS dw_source.`s_std_membercard_temp`(
	`card_plan_id` string,
    `member_id` string,
    `card_number` string,
	`card_name` string,
    `grade` string,
	`grade_name` string,
	`grade_period` string, 
	`available_point` bigint, 
	`upcoming_expired_point` bigint,
	`total_point` bigint,
	`consumed_point` bigint,
	`expired_point` bigint,
	`bind_mobile` string,
	`name` string,
	`birthday` bigint
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' lines terminated by '\n'
STORED AS RCFILE;

-- 增量会员信息插入临时表
insert into dw_source.`s_std_membercard_temp`
select a.card_plan_id,a.member_id,a.card_number,a.card_name,a.grade,a.grade_name,a.grade_period,
a.available_point,a.upcoming_expired_point,a.total_point,a.consumed_point,a.expired_point,
a.bind_mobile,a.name,a.birthday from (
	select *,row_number() over (partition by card_plan_id,member_id,card_number) as num 
	from dw_source.s_std_membercard where dt='${stat_date}'
) a where num = 1;

-- 临时表与全量数据进行比对，对新增的个更新的进行合并入库
insert overwrite table dw_base.b_std_membercard partition(part)  
SELECT  
    t.card_plan_id,
	t.member_id,
    t.card_number,
    t.card_name,
    t.grade,
	t.grade_name,
    t.grade_period,
	t.available_point,
    t.upcoming_expired_point,
    t.total_point,
    t.consumed_point,
    t.expired_point,
	t.bind_mobile,
	t.name,
	t.birthday,
    cardid_hash(t.card_plan_id) as part
FROM
    (SELECT 
        t1.card_plan_id,
    	t1.member_id,
        t1.card_number,
        t1.card_name,
        t1.grade,
		t1.grade_name,
        t1.grade_period,
    	t1.available_point,
        t1.upcoming_expired_point,
        t1.total_point,
        t1.consumed_point,
        t1.expired_point,
		t1.bind_mobile,
		t1.name,
		t1.birthday
    FROM dw_base.b_std_membercard t1
    LEFT OUTER JOIN 
        dw_source.s_std_membercard_temp t2
    ON t1.card_plan_id = t2.card_plan_id
        AND t1.member_id = t2.member_id
        AND t1.card_number = t2.card_number
    WHERE t2.card_plan_id IS NULL
            AND t2.member_id IS NULL
            AND t2.card_number IS NULL) t 
UNION ALL SELECT 
    tmp.card_plan_id,
	tmp.member_id,
    tmp.card_number,
    tmp.card_name,
    tmp.grade,
	tmp.grade_name,
    tmp.grade_period,
	tmp.available_point,
    tmp.upcoming_expired_point,
    tmp.total_point,
    tmp.consumed_point,
    tmp.expired_point,
	tmp.bind_mobile,
	tmp.name,
	tmp.birthday,
    cardid_hash(tmp.card_plan_id) as part
FROM
    dw_source.s_std_membercard_temp tmp;






