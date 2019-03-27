SET mapred.job.name='union-offline-member-all-pre13monthend-result';
set hive.tez.auto.reducer.parallelism=true;
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

-- 将线下会员相关表的历史月底数据同步到指定目录，同步给业务方
insert overwrite table dw_rfm.cix_offline_member_consume_statistic partition(part='2100-01-01')
select t.card_plan_id,t.uni_shop_id,t.shop_name,t.date_type,t.grade,t.store_members,t.shop_members,shop_payment,
	   t.shop_avg_price,t.shop_avg_times,t.shop_single_price,t.all_members,t.all_payment,t.all_avg_price,t.all_avg_times,
	   t.all_single_price,t.payment_rate,t.stat_date,t.modified
from dw_rfm.cix_offline_member_consume_statistic t
where t.part in(${hiveconf:pre1MonthEnd},${hiveconf:pre2MonthEnd},${hiveconf:pre3MonthEnd},${hiveconf:pre4MonthEnd},
			${hiveconf:pre5MonthEnd},${hiveconf:pre6MonthEnd},${hiveconf:pre7MonthEnd},${hiveconf:pre8MonthEnd},${hiveconf:pre9MonthEnd},
			${hiveconf:pre10MonthEnd},${hiveconf:pre11MonthEnd},${hiveconf:pre12MonthEnd},${hiveconf:pre13MonthEnd});

insert overwrite table dw_rfm.cix_offline_plat_member_sale_rate partition(part='2100-01-01')
select t.card_plan_id,t.plat_code,t.date_type,t.total_payment,t.mtotal_payment,t.sale_rate,total_times,
	   t.mtotal_times,t.times_rate,t.grade,t.stat_date,t.modified
from dw_rfm.cix_offline_plat_member_sale_rate t
where t.part in(${hiveconf:pre1MonthEnd},${hiveconf:pre2MonthEnd},${hiveconf:pre3MonthEnd},${hiveconf:pre4MonthEnd},
			${hiveconf:pre5MonthEnd},${hiveconf:pre6MonthEnd},${hiveconf:pre7MonthEnd},${hiveconf:pre8MonthEnd},${hiveconf:pre9MonthEnd},
			${hiveconf:pre10MonthEnd},${hiveconf:pre11MonthEnd},${hiveconf:pre12MonthEnd},${hiveconf:pre13MonthEnd});

insert overwrite table dw_rfm.cix_offline_sale_shops_of_card partition(part='2100-01-01')
select t.card_plan_id,t.plat_code,uni_shop_id,shop_name,t.date_type,t.grade,
		t.total_payment,t.mtotal_payment,t.sale_rate,total_times,t.mtotal_times,t.times_rate,
		t.stat_date,t.modified
from dw_rfm.cix_offline_sale_shops_of_card t
where t.part in(${hiveconf:pre1MonthEnd},${hiveconf:pre2MonthEnd},${hiveconf:pre3MonthEnd},${hiveconf:pre4MonthEnd},
			${hiveconf:pre5MonthEnd},${hiveconf:pre6MonthEnd},${hiveconf:pre7MonthEnd},${hiveconf:pre8MonthEnd},${hiveconf:pre9MonthEnd},
			${hiveconf:pre10MonthEnd},${hiveconf:pre11MonthEnd},${hiveconf:pre12MonthEnd},${hiveconf:pre13MonthEnd});

insert overwrite table dw_rfm.cix_offline_shop_member_change_statistic partition(part='2100-01-01')
select t.card_plan_id,t.plat_code,uni_shop_id,shop_name,t.members,
		t.yestoday,t.thisweek,t.thismonth,thisyear,t.last7day,t.last30day,t.lastyear,
		t.stat_date,t.modified
from dw_rfm.cix_offline_shop_member_change_statistic t
where t.part in(${hiveconf:pre1MonthEnd},${hiveconf:pre2MonthEnd},${hiveconf:pre3MonthEnd},${hiveconf:pre4MonthEnd},
			${hiveconf:pre5MonthEnd},${hiveconf:pre6MonthEnd},${hiveconf:pre7MonthEnd},${hiveconf:pre8MonthEnd},${hiveconf:pre9MonthEnd},
			${hiveconf:pre10MonthEnd},${hiveconf:pre11MonthEnd},${hiveconf:pre12MonthEnd},${hiveconf:pre13MonthEnd});
			
-- 删除临时数据表
drop table if exists dw_rfm.b_offline_last30day_trade_temp;
drop table if exists dw_rfm.b_offline_last30day_member_trade_temp;		
drop table if exists dw_rfm.b_shop_memeber_created_reconized_temp;
drop table if exists dw_rfm.b_shop_memeber_increm_staticits_temp;
drop table if exists dw_rfm.b_plat_sale_total_statics_temp;
drop table if exists dw_rfm.b_shop_member_total_statics_temp;
drop table if exists dw_rfm.b_plat_member_total_statics_temp;
drop table if exists dw_rfm.b_shop_grade_total_statics_temp;
drop table if exists dw_rfm.b_plat_grade_total_statics_temp;
drop table if exists dw_rfm.b_shop_sale_total_statics_temp;
drop table if exists dw_rfm.b_offline_shop_last30day_trade;
drop table if exists dw_rfm.b_opencard_shop_member_consume_statics;
drop table if exists dw_rfm.b_shop_all_member_consume_statics;
drop table if exists dw_rfm.b_offline_shop_members_count_temp;


