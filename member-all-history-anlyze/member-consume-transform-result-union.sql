SET mapred.job.name='member-consume-transform-result-union-合并所有历史月底数据同步到业务库';

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

CREATE TABLE IF NOT EXISTS dw_rfm.`cix_online_member_consume_transform_pre13month`(
	`tenant` string,
	`plat_code` string,
    `uni_shop_id` string,
	`card_plan_id` string,
	`join_month` string, --会员加入月份 2018-12, all合计行标识
    `members` bigint,  --入会会员数
    `payments` double, --近一年消费金额
	`f0` bigint,
    `f1` bigint,
	`f2` bigint,
	`f3` bigint,
    `f4` bigint,
	`f5` bigint,
    `type` int, -- 1:租户级 2:平台级 3:店铺级
	`end_month` int,
	`stat_date` string,
	`modified` string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

insert overwrite table dw_rfm.cix_online_member_consume_transform_pre13month
select t.tenant,t.plat_code,t.uni_shop_id,t.card_plan_id,t.join_month,t.members,t.payments,
	   t.f0,t.f1,t.f2,t.f3,t.f4,t.f5,
	   t.type,t.end_month,t.stat_date,t.modified
from dw_rfm.cix_online_member_consume_transform t
where t.part in(${hiveconf:pre1MonthEnd},${hiveconf:pre2MonthEnd},${hiveconf:pre3MonthEnd},${hiveconf:pre4MonthEnd},
			${hiveconf:pre5MonthEnd},${hiveconf:pre6MonthEnd},${hiveconf:pre7MonthEnd},${hiveconf:pre8MonthEnd},${hiveconf:pre9MonthEnd},
			${hiveconf:pre10MonthEnd},${hiveconf:pre11MonthEnd},${hiveconf:pre12MonthEnd},${hiveconf:pre13MonthEnd});

-- 需要将cix_online_member_consume_transform_pre13month通过sqoop同步到业务库中

