SET mapred.job.name='member-life-cycle-analyze 会员生命周期分析';
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

-- 历史订单类目统计

-- 会员基本信息：dw_business.b_std_member_base_info
-- 会员卡和平台店铺关系表：dw_business.b_card_shop_rel
-- 会员和客户的关系表：dw_business.b_customer_member_relation
-- 会员卡等级信息：dw_business.b_card_grade_info
-- 全渠道店铺客户RFM: dw_rfm.b_qqd_shop_rfm

set submitTime=from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss');

-- 1、以客户信息关联会员信息，如果客户有会员信息，那么客户就是会员，否则是非会员
-- 计算出会员和店铺客户的信息

-- 今天店铺级的所有客户RFM数据与当天的会员信息进行关联，算出RFM是会员的数据
-- 给数据进行打标，识别出潜客，沉默客，流失客等
drop table if exists dw_rfm.b_customer_member_rfm_temp;
create table dw_rfm.b_customer_member_rfm_temp as
select t.tenant,t.plat_code,t.uni_shop_id,t.uni_id,t.ismember,t.card_plan_id,t.member_id,t.grade,
case when (t.earliest_time <= '${stat_date}' and t.first_buy_time is null) or t.first_buy_time > '${stat_date}' then 'qianke'
when t.year_buy_times = 1 and t.first_buy_time > add_months('${stat_date}',-12) then 'active_new'
when t.year_buy_times >= 2 and t.first_buy_time > add_months('${stat_date}',-12) then 'phurce_new'
when t.year_buy_times = 1 and t.first_buy_time <= add_months('${stat_date}',-12) then 'active_old'
when t.year_buy_times >= 2 and t.first_buy_time <= add_months('${stat_date}',-12) then 'phurce_old'
when t.year_buy_times=0 and t.tyear_buy_times>=1 then 'silent'
when t.year_buy_times=0 and t.tyear_buy_times=0 and t.btyear_buy_times>=1 then 'loss' else '0' end as custype
from
(
	select rfm.tenant,rfm.plat_code,rfm.uni_shop_id,rfm.uni_id,
		   rfm.earliest_time,rfm.first_buy_time,rfm.year_buy_times,
		   rfm.tyear_buy_times,rfm.btyear_buy_times
		   case when m.card_plan_id is null then -1 else 0 end as ismember,
		   m.card_plan_id,m.member_id,m.grade
	from(
		select * from dw_rfm.b_qqd_shop_rfm where part='${stat_date}'
	) rfm
	left join 
	(
		select r.card_plan_id,r.member_id,r.grade,r.plat_code,r.shop_id,r1.uni_id,
			concat(r.plat_code,'|',r.shop_id) as uni_shop_id 
		from(
			select t.card_plan_id,t.member_id,t.grade,t1.plat_code,t1.shop_id
			from (
				select a.card_plan_id,a.member_id,a.grade
				from  dw_business.b_std_member_base_info a
				where substr(a.created,1,10) <= '${stat_date}' 
			) t
			join 
			dw_business.b_card_shop_rel t1
			on t.card_plan_id = t1.card_plan_id
		) r
		join dw_business.b_customer_member_relation r1
		on r.card_plan_id = r1.card_plan_id and r.member_id=r1.member_id
	) m
	on rfm.plat_code = m.plat_code and rfm.uni_shop_id = m.uni_shop_id and rfm.uni_id = m.uni_id
);

-- 基于上面的数据进行各类数据与等级的分析，然后合并各个数据维度的统计结果即可

-- grade + custype
-- grade 
-- custype 







