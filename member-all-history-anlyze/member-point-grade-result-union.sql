SET mapred.job.name='member-point-grade-result-union-合并所有历史月底数据同步到业务库';

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

set pre1MonthEnd=date_sub(concat(substr('${stat_date}',0,7),'-01'),1);
set pre2MonthEnd=add_months(${hiveconf:pre1MonthEnd},-1);
set pre3MonthEnd=add_months(${hiveconf:pre1MonthEnd},-2);
set pre4MonthEnd=add_months(${hiveconf:pre1MonthEnd},-3);
set pre5MonthEnd=add_months(${hiveconf:pre1MonthEnd},-4);
set pre6MonthEnd=add_months(${hiveconf:pre1MonthEnd},-5);
set pre7MonthEnd=add_months(${hiveconf:pre1MonthEnd},-6);
set pre8MonthEnd=add_months(${hiveconf:pre1MonthEnd},-7);
set pre9MonthEnd=add_months(${hiveconf:pre1MonthEnd},-8);
set pre10MonthEnd=add_months(${hiveconf:pre1MonthEnd},-9);
set pre11MonthEnd=add_months(${hiveconf:pre1MonthEnd},-10);
set pre12MonthEnd=add_months(${hiveconf:pre1MonthEnd},-11);
set pre13MonthEnd=add_months(${hiveconf:pre1MonthEnd},-12);

CREATE TABLE IF NOT EXISTS dw_rfm.`cix_online_member_point_grade_change_pre13month`(
	`card_plan_id` string,
	`rangetype` int,
	`effect_points` bigint,
	`point_change_nums` bigint,
	`send_points` bigint,
	`consume_points` bigint,
	`overdue_points` bigint,
	`newmembers` bigint,
	`upnums` bigint,
	`downnums` bigint,
	`stat_date` string,
	`modified` string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

insert overwrite table dw_rfm.cix_online_member_point_grade_change_pre13month
select t.card_plan_id,t.rangetype,t.effect_points,t.point_change_nums,t.send_points,t.consume_points,
		t.overdue_points,t.newmembers,t.upnums,t.downnums,t.stat_date,t.modified
from dw_rfm.cix_online_member_point_grade_change t
where t.part in(${hiveconf:pre1MonthEnd},${hiveconf:pre2MonthEnd},${hiveconf:pre3MonthEnd},${hiveconf:pre4MonthEnd},
			${hiveconf:pre5MonthEnd},${hiveconf:pre6MonthEnd},${hiveconf:pre7MonthEnd},${hiveconf:pre8MonthEnd},${hiveconf:pre9MonthEnd},
			${hiveconf:pre10MonthEnd},${hiveconf:pre11MonthEnd},${hiveconf:pre12MonthEnd},${hiveconf:pre13MonthEnd});


CREATE TABLE IF NOT EXISTS dw_rfm.`cix_online_member_grade_transform_pre13month`(
	`card_plan_id` string,
	`nowgrade` int, 
	`oldgrade` int, 
	`members` bigint,
	`rangetype` int,
	`stat_date` string,
	`modified` string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

insert overwrite table dw_rfm.cix_online_member_grade_transform_pre13month
select t.card_plan_id,t.nowgrade,t.oldgrade,t.members,t.rangetype,t.stat_date,t.modified
from dw_rfm.cix_online_member_grade_transform t
where t.part in(${hiveconf:pre1MonthEnd},${hiveconf:pre2MonthEnd},${hiveconf:pre3MonthEnd},${hiveconf:pre4MonthEnd},
			${hiveconf:pre5MonthEnd},${hiveconf:pre6MonthEnd},${hiveconf:pre7MonthEnd},${hiveconf:pre8MonthEnd},${hiveconf:pre9MonthEnd},
			${hiveconf:pre10MonthEnd},${hiveconf:pre11MonthEnd},${hiveconf:pre12MonthEnd},${hiveconf:pre13MonthEnd});
	
-- 需要将cix_online_member_point_grade_change_pre13month通过sqoop同步到业务库中
-- 需要将cix_online_member_grade_transform_pre13month通过sqoop同步到业务库中

