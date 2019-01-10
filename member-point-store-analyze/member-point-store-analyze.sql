SET mapred.job.name='member-point-store-analyze 积分存量分析';
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

-- 会员基本信息：dw_business.b_std_member_base_info
-- 有效积分：dw_business.b_member_efffect_point
-- 等级变更表：dw_business.b_member_grade_change
-- 积分变更表: dw_business.b_member_point_change

set submitTime=from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss');

-- 设置积分间隔值
set pointv1=100;
set pointv2=200;
set pointv3=500;


CREATE TABLE IF NOT EXISTS dw_rfm.`b_effect_point_base_temp`(
	`card_plan_id` string,
	`member_id` string,
	`point` bigint,
	`mp1` int,
	`mp2` int,
	`mp3` int
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");

-- 初始化有效积分的区间分隔
insert overwrite table dw_rfm.b_effect_point_base_temp
select t.card_plan_id,t.member_id,t.point,
case when t.point<=0 then 1
	when t.point>0 and t.point <= ${hiveconf:pointv1} then 2
	when t.point> ${hiveconf:pointv1} and t.point <= (${hiveconf:pointv1}*2) then 3
	when t.point>(${hiveconf:pointv1}*2) and t.point <= (${hiveconf:pointv1}*3) then 4
	when t.point>(${hiveconf:pointv1}*3) and t.point <= (${hiveconf:pointv1}*4) then 5
	when t.point>(${hiveconf:pointv1}*4) and t.point <= (${hiveconf:pointv1}*5) then 6
	when t.point>(${hiveconf:pointv1}*5) and t.point <= (${hiveconf:pointv1}*6) then 7
	when t.point>(${hiveconf:pointv1}*6) and t.point <= (${hiveconf:pointv1}*7) then 8
	when t.point>(${hiveconf:pointv1}*7) and t.point <= (${hiveconf:pointv1}*8) then 9
	when t.point>(${hiveconf:pointv1}*8) and t.point <= (${hiveconf:pointv1}*9) then 10
	when t.point>(${hiveconf:pointv1}*9) and t.point <= (${hiveconf:pointv1}*10) then 11
	when t.point>(${hiveconf:pointv1}*10) then 12 end as mp1,
case when t.point<=0 then 1
	when t.point>0 and t.point <= ${hiveconf:pointv2} then 2
	when t.point> ${hiveconf:pointv2} and t.point <= (${hiveconf:pointv2}*2) then 3
	when t.point>(${hiveconf:pointv2}*2) and t.point <= (${hiveconf:pointv2}*3) then 4
	when t.point>(${hiveconf:pointv2}*3) and t.point <= (${hiveconf:pointv2}*4) then 5
	when t.point>(${hiveconf:pointv2}*4) and t.point <= (${hiveconf:pointv2}*5) then 6
	when t.point>(${hiveconf:pointv2}*5) and t.point <= (${hiveconf:pointv2}*6) then 7
	when t.point>(${hiveconf:pointv2}*6) and t.point <= (${hiveconf:pointv2}*7) then 8
	when t.point>(${hiveconf:pointv2}*7) and t.point <= (${hiveconf:pointv2}*8) then 9
	when t.point>(${hiveconf:pointv2}*8) and t.point <= (${hiveconf:pointv2}*9) then 10
	when t.point>(${hiveconf:pointv2}*9) and t.point <= (${hiveconf:pointv2}*10) then 11
	when t.point>(${hiveconf:pointv2}*10) then 12 end as mp2,
case when t.point<=0 then 1
	when t.point>0 and t.point <= ${hiveconf:pointv3} then 2
	when t.point> ${hiveconf:pointv3} and t.point <= (${hiveconf:pointv3}*2) then 3
	when t.point>(${hiveconf:pointv3}*2) and t.point <= (${hiveconf:pointv3}*3) then 4
	when t.point>(${hiveconf:pointv3}*3) and t.point <= (${hiveconf:pointv3}*4) then 5
	when t.point>(${hiveconf:pointv3}*4) and t.point <= (${hiveconf:pointv3}*5) then 6
	when t.point>(${hiveconf:pointv3}*5) and t.point <= (${hiveconf:pointv3}*6) then 7
	when t.point>(${hiveconf:pointv3}*6) and t.point <= (${hiveconf:pointv3}*7) then 8
	when t.point>(${hiveconf:pointv3}*7) and t.point <= (${hiveconf:pointv3}*8) then 9
	when t.point>(${hiveconf:pointv3}*8) and t.point <= (${hiveconf:pointv3}*9) then 10
	when t.point>(${hiveconf:pointv3}*9) and t.point <= (${hiveconf:pointv3}*10) then 11
	when t.point>(${hiveconf:pointv3}*10) then 12 end as mp3
from dw_business.b_member_efffect_point t
where substr(t.effective_date,1,10) <= '${stat_date}'
	  and substr(t.overdue_date,1,10) > '${stat_date}'
	  and t.valid=1;

-- 积分存量MP分析结果，需要同步给业务
CREATE TABLE IF NOT EXISTS dw_rfm.`cix_online_card_store_point`(
	`card_plan_id` string,
	`mptype` int,
	`interval_type` int,
	`members` bigint,
	`member_rate` double,
	`stat_date` string,
	`modified` string
)
partitioned by(`part` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

insert overwrite table dw_rfm.cix_online_card_store_point partition(part='${stat_date}')
select r1.card_plan_id,r1.mptype,r1.interval_type,r1.members,
		(r1.members/r.totalmembers) as member_rate,
		'${stat_date}' as stat_date,
		${hiveconf:submitTime} as modified
from
(
	select t.card_plan_id,t.mp1 as mptype,100 as interval_type,count(t.member_id) members
		from dw_rfm.b_effect_point_base_temp t
		group by t.card_plan_id,t.mp1
	union all
	select t1.card_plan_id,t1.mp2 as mptype,200 as interval_type,count(t1.member_id) members
		from dw_rfm.b_effect_point_base_temp t1
		group by t1.card_plan_id,t1.mp2
	union all
	select t2.card_plan_id,t2.mp3 as mptype,500 as interval_type,count(t2.member_id) members
		from dw_rfm.b_effect_point_base_temp t2
		group by t2.card_plan_id,t2.mp3
) r1
left join 
(
	select tmp.card_plan_id,count(tmp.member_id) as totalmembers
	from dw_rfm.b_effect_point_base_temp tmp
) r
on r1.card_plan_id=r.card_plan_id;


-- 计算积分变化趋势








