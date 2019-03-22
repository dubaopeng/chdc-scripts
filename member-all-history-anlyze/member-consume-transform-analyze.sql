SET mapred.job.name='member-consume-transform-monthend-analyze-历史月底会员消费转化分析';

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

-- 会员基本信息：dw_business.b_std_member_base_info
-- 会员卡和平台店铺关系表：dw_business.b_card_shop_rel
-- 会员和客户的关系表：dw_business.b_customer_member_relation
-- 会员卡等级信息：dw_business.b_card_grade_info
-- 租户和店铺的关系：dw_base.b_std_tenant_shop
-- 全渠道店铺客户RFM: dw_rfm.b_qqd_shop_rfm

set preMonthEnd=date_sub(concat(substr('${stat_date}',0,7),'-01'),1);
set thisMonthEnd=add_months(${hiveconf:preMonthEnd},-${monthNum});

set isMonthEnd=1;
set submitTime=from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss');

-- 1、从会员基础信息表中分析近14个月入会的会员，并给会员打上入会月份标记
-- 2、以近14个月的会员与今日店铺级RFM宽表进行左联，获取近一年的payments,times
-- 会员消费转化表定义
CREATE TABLE IF NOT EXISTS dw_rfm.`b_last14month_member_monthend_rfm`(
	`card_plan_id` string,
	`member_id` string,
	`joinmonth` string,
	`uni_id` string,
	`plat_code` string,
	`uni_shop_id` string,
	`tenant` string,
	`payment` double,
	`frequency` int
)
PARTITIONED BY(part string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");

insert overwrite table dw_rfm.`b_last14month_member_monthend_rfm` partition(part)
select tmp.card_plan_id,tmp.member_id,tmp.joinmonth,tmp.uni_id,
	   tmp.plat_code,tmp.uni_shop_id,
	   rfm.tenant,
	 if(rfm.year_payment is null,0,rfm.year_payment) as payment,
	 case when rfm.year_buy_times is null or rfm.year_buy_times=0 then 0 
		  when rfm.year_buy_times=1 then 1
		  when rfm.year_buy_times=2 then 2
		  when rfm.year_buy_times=3 then 3
		  when rfm.year_buy_times=4 then 4
		  when rfm.year_buy_times>=5 then 5 end as frequency,
	${hiveconf:thisMonthEnd} as part
from 
(
	select r.card_plan_id,r.member_id,r.joinmonth,r.plat_code,
		concat(r.plat_code,'|',r.shop_id) as uni_shop_id,
		r1.uni_id
	from(		
		select t1.card_plan_id,t1.plat_code,t1.shop_id,t.member_id,t.joinmonth
			from dw_business.b_card_shop_rel t1
		left join (
			select a.card_plan_id,a.member_id,substr(a.created,1,7) as joinmonth
			from  dw_business.b_std_member_base_info a
			where substr(a.created,1,10) > add_months(${hiveconf:thisMonthEnd},-14) 
				  and substr(a.created,1,10) <= ${hiveconf:thisMonthEnd} 
		)t
		on t1.card_plan_id = t.card_plan_id
		where t.member_id is not null
	) r
	join dw_business.b_customer_member_relation r1
	on r.card_plan_id = r1.card_plan_id and r.member_id=r1.member_id
) tmp
left outer join (
	select tenant,plat_code,uni_shop_id,uni_id,year_payment,year_buy_times 
	from dw_rfm.b_qqd_shop_rfm where part=${hiveconf:thisMonthEnd}
)rfm
on tmp.plat_code = rfm.plat_code and tmp.uni_shop_id=rfm.uni_shop_id and tmp.uni_id=rfm.uni_id
where rfm.tenant is not null;


-- 会员消费转化表定义
CREATE TABLE IF NOT EXISTS dw_rfm.`cix_online_member_consume_transform`(
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
partitioned by(`part` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

insert overwrite table dw_rfm.`cix_online_member_consume_transform` partition(part)
select r1.tenant,r1.plat_code,r1.uni_shop_id,r1.card_plan_id,
	r1.joinmonth as join_month,
	r1.totalmembers as members,
	r1.payments,
	if(r2.f0 is null,0,r2.f0) as f0,
	if(r2.f1 is null,0,r2.f1) as f1,
	if(r2.f2 is null,0,r2.f2) as f2,
	if(r2.f3 is null,0,r2.f3) as f3,
	if(r2.f4 is null,0,r2.f4) as f4,
	if(r2.f5 is null,0,r2.f5) as f5,
	3 as type,
	${hiveconf:isMonthEnd} as end_month,
	${hiveconf:thisMonthEnd} as stat_date,
	${hiveconf:submitTime} as modified,
	${hiveconf:thisMonthEnd} as part
from 
(
	--以上面rfm临时表统计,入会时间对应的入会人数，会员消费金额
	select a.tenant,a.plat_code,a.uni_shop_id,a.card_plan_id,a.joinmonth,
		   count(distinct a.member_id) totalmembers,
		   sum(a.payment) payments
	from 
	dw_rfm.b_last14month_member_monthend_rfm a where a.part=${hiveconf:thisMonthEnd}
	group by a.tenant,a.plat_code,a.uni_shop_id,a.card_plan_id,a.joinmonth
) r1
left join
(
	-- 计算frequency对应人数
	select t.tenant,t.plat_code,t.uni_shop_id,t.card_plan_id,t.joinmonth,
			cast(concat_ws('',collect_set(if(t.frequency=0,t.fmembers,''))) as bigint) f0, 
			cast(concat_ws('',collect_set(if(t.frequency=1,t.fmembers,''))) as bigint) f1,
			cast(concat_ws('',collect_set(if(t.frequency=2,t.fmembers,''))) as bigint) f2,
			cast(concat_ws('',collect_set(if(t.frequency=3,t.fmembers,''))) as bigint) f3,
			cast(concat_ws('',collect_set(if(t.frequency=4,t.fmembers,''))) as bigint) f4,
			cast(concat_ws('',collect_set(if(t.frequency=5,t.fmembers,''))) as bigint) f5
	from
	(
		select a.tenant,a.plat_code,a.uni_shop_id,a.card_plan_id,a.joinmonth,a.frequency,
			   count(distinct a.member_id) fmembers
		from 
		dw_rfm.b_last14month_member_monthend_rfm a where part=${hiveconf:thisMonthEnd}
		group by a.tenant,a.plat_code,a.uni_shop_id,a.card_plan_id,a.joinmonth,a.frequency
	) t
	group by t.tenant,t.plat_code,t.uni_shop_id,t.card_plan_id,t.joinmonth
) r2
on r1.tenant=r2.tenant and r1.plat_code=r2.plat_code and r1.uni_shop_id=r2.uni_shop_id
   and r1.card_plan_id=r2.card_plan_id and r1.joinmonth=r2.joinmonth;

--计算店铺+卡的合计行数据
insert into table dw_rfm.cix_online_member_consume_transform partition(part)
select t.tenant,t.plat_code,t.uni_shop_id,t.card_plan_id,'all' as join_month,
	sum(t.members) as members,
	sum(t.payments) as payments,
	sum(t.f0) as f0,
	sum(t.f1) as f1,
	sum(t.f2) as f2,
	sum(t.f3) as f3,
	sum(t.f4) as f4,
	sum(t.f5) as f5,
	3 as type,
	${hiveconf:isMonthEnd} as end_month,
	${hiveconf:thisMonthEnd} as stat_date,
	${hiveconf:submitTime} as modified,
	${hiveconf:thisMonthEnd} as part
from dw_rfm.cix_online_member_consume_transform t
where t.part=${hiveconf:thisMonthEnd}
group by t.tenant,t.plat_code,t.uni_shop_id,t.card_plan_id;


-- 基于店铺级的数据，计算平台级数据 和 租户级数据
insert into table dw_rfm.cix_online_member_consume_transform partition(part)
select t.tenant,t.plat_code,null as uni_shop_id,t.card_plan_id,t.join_month,
	sum(t.members) as members,
	sum(t.payments) as payments,
	sum(t.f0) as f0,
	sum(t.f1) as f1,
	sum(t.f2) as f2,
	sum(t.f3) as f3,
	sum(t.f4) as f4,
	sum(t.f5) as f5,
	2 as type,
	${hiveconf:isMonthEnd} as end_month,
	${hiveconf:thisMonthEnd} as stat_date,
	${hiveconf:submitTime} as modified,
	${hiveconf:thisMonthEnd} as part
from dw_rfm.cix_online_member_consume_transform t 
where t.part=${hiveconf:thisMonthEnd}
group by t.tenant,t.plat_code,t.card_plan_id,t.join_month;


insert into table dw_rfm.cix_online_member_consume_transform partition(part)
select t1.tenant,null as plat_code,null as uni_shop_id,t1.card_plan_id,t1.join_month,
	sum(t1.members) as members,
	sum(t1.payments) as payments,
	sum(t1.f0) as f0,
	sum(t1.f1) as f1,
	sum(t1.f2) as f2,
	sum(t1.f3) as f3,
	sum(t1.f4) as f4,
	sum(t1.f5) as f5,
	1 as type,
	${hiveconf:isMonthEnd} as end_month,
	${hiveconf:thisMonthEnd} as stat_date,
	${hiveconf:submitTime} as modified,
	${hiveconf:thisMonthEnd} as part
from dw_rfm.cix_online_member_consume_transform t1 
where t1.part=${hiveconf:thisMonthEnd}
group by t1.tenant,t1.card_plan_id,t1.join_month;


--删除临时表分区
--alter table dw_rfm.b_last14month_member_monthend_rfm drop partition (part=${hiveconf:thisMonthEnd});

-- 统计完成后需要将表 dw_rfm.cix_online_member_consume_transform各个分区下的数据合并到同一个表中同步给业务库




