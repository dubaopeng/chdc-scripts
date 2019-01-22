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

-- 会员基本信息：dw_business.b_std_member_base_info
-- 会员卡和平台店铺关系表：dw_business.b_card_shop_rel
-- 会员和客户的关系表：dw_business.b_customer_member_relation
-- 会员卡等级信息：dw_business.b_card_grade_info
-- 全渠道店铺客户RFM: dw_rfm.b_qqd_shop_rfm

-- 设置变量记录统计当天是否为月末的那天
set isMonthEnd=if(date_sub(concat(substr(add_months('${stat_date}',1),0,7),'-01'),1)='${stat_date}',1,0);
-- 设置任务提交时间
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
		   rfm.tyear_buy_times,rfm.btyear_buy_times,
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
) t;


-- 会员生命周期类型表定义
CREATE TABLE IF NOT EXISTS dw_rfm.`cix_online_member_life_cycle`(
	`tenant` string,
	`plat_code` string,
    `uni_shop_id` string,
	`card_plan_id` string,
	`grade` int, -- 会员等级 -1:非会员行 98:会员合计行 99:合计 其他都是等级号
    `prospective` bigint, 
    `active_new` bigint,
	`phurce_new` bigint,
    `active_old` bigint,
	`phurce_old` bigint,
	`silent` bigint,
    `loss` bigint,
	`whole` bigint,
    `type` int, -- 1:租户级 2:平台级 3:店铺级
	`end_month` int,
	`stat_date` string,
	`modified` string
)
partitioned by(`part` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

-- 基于上面的数据进行各类数据与等级的分析，然后合并各个数据维度的统计结果即可
-- grade + custype
-- grade 
-- custype 

-- 计算会员的各等级对应的客户类型的数量
insert overwrite table dw_rfm.cix_online_member_life_cycle partition(part='${stat_date}')
select c.tenant,c.plat_code,c.uni_shop_id,c.card_plan_id,c.grade,
	if(c.prospective is null,0,c.prospective) as prospective,
	if(c.active_new is null,0,c.active_new) active_new,
	if(c.phurce_new is null,0,c.phurce_new) phurce_new,
	if(c.active_old is null,0,c.active_old) active_old,
	if(c.phurce_old is null,0,c.phurce_old) phurce_old,
	if(c.silent is null,0,c.silent) silent,
	if(c.loss is null,0,c.loss) loss,
	c.whole,
	3 as type,
	${hiveconf:isMonthEnd} as end_month,
	'${stat_date}' as stat_date,
	${hiveconf:submitTime} as modified
from (
	select b.tenant,b.plat_code,b.uni_shop_id,b.card_plan_id,b.grade,
			cast(concat_ws('',collect_set(if(b.custype='qianke',b.num,''))) as bigint) prospective, --潜客数量
			cast(concat_ws('',collect_set(if(b.custype='active_new',b.num,''))) as bigint) active_new, -- 新客数
			cast(concat_ws('',collect_set(if(b.custype='phurce_new',b.num,''))) as bigint) phurce_new, -- 新客复购数
			cast(concat_ws('',collect_set(if(b.custype='active_old',b.num,''))) as bigint) active_old, -- 老客数
			cast(concat_ws('',collect_set(if(b.custype='phurce_old',b.num,''))) as bigint) phurce_old, -- 老客复购数
			cast(concat_ws('',collect_set(if(b.custype='silent',b.num,''))) as bigint) silent,  --沉默客户数
			cast(concat_ws('',collect_set(if(b.custype='loss',b.num,''))) as bigint) loss,   --流失客户数
			sum(if(b.custype='0',0,b.num)) as whole -- 全体客户
	from(
		select 
			t.tenant,t.plat_code,t.uni_shop_id,t.card_plan_id,t.grade,t.custype,count(distinct t.uni_id) num
		from 
			dw_rfm.b_customer_member_rfm_temp t
		where t.ismember=0
		group by t.tenant,t.plat_code,t.uni_shop_id,t.card_plan_id,t.grade,t.custype
	) b
	group by b.tenant,b.plat_code,b.uni_shop_id,b.card_plan_id,b.grade
	union all
	select b.tenant,b.plat_code,b.uni_shop_id,b.card_plan_id,-1 as grade,
			cast(concat_ws('',collect_set(if(b.custype='qianke',b.num,''))) as bigint) prospective, --潜客数量
			cast(concat_ws('',collect_set(if(b.custype='active_new',b.num,''))) as bigint) active_new, -- 新客数
			cast(concat_ws('',collect_set(if(b.custype='phurce_new',b.num,''))) as bigint) phurce_new, -- 新客复购数
			cast(concat_ws('',collect_set(if(b.custype='active_old',b.num,''))) as bigint) active_old, -- 老客数
			cast(concat_ws('',collect_set(if(b.custype='phurce_old',b.num,''))) as bigint) phurce_old, -- 老客复购数
			cast(concat_ws('',collect_set(if(b.custype='silent',b.num,''))) as bigint) silent,  --沉默客户数
			cast(concat_ws('',collect_set(if(b.custype='loss',b.num,''))) as bigint) loss,   --流失客户数
			sum(if(b.custype='0',0,b.num)) whole  -- 全体客户
	from(
		select 
			t.tenant,t.plat_code,t.uni_shop_id,t.card_plan_id,t.custype,count(distinct t.uni_id) num
		from 
			dw_rfm.b_customer_member_rfm_temp t
		where t.ismember=-1
		group by t.tenant,t.plat_code,t.uni_shop_id,t.card_plan_id,t.custype
	) b
	group by b.tenant,b.plat_code,b.uni_shop_id,b.card_plan_id
) c;

-- 计算会员的合计行和所有客户的合计行数据
insert into table dw_rfm.cix_online_member_life_cycle partition(part='${stat_date}')
select t.tenant,t.plat_code,t.uni_shop_id,t.card_plan_id,98 as grade,--会员行数据
	  sum(t.prospective)  as prospective,
	  sum(t.active_new)  as active_new,
	  sum(t.phurce_new)  as phurce_new,
	  sum(t.active_old)  as active_old,
	  sum(t.phurce_old)  as phurce_old,
	  sum(t.silent)  as silent,
	  sum(t.loss)  as loss,
	  sum(t.whole) as whole,
	  3 as type,
	  ${hiveconf:isMonthEnd} as end_month,
	  '${stat_date}' as stat_date,
	  ${hiveconf:submitTime} as modified
from dw_rfm.cix_online_member_life_cycle t
where t.part='${stat_date}' and t.grade > -1 
group by t.tenant,t.plat_code,t.uni_shop_id,t.card_plan_id
union all
select t1.tenant,t1.plat_code,t1.uni_shop_id,t1.card_plan_id,99 as grade,
	  sum(t1.prospective)  as prospective,
	  sum(t1.active_new)  as active_new,
	  sum(t1.phurce_new)  as phurce_new,
	  sum(t1.active_old)  as active_old,
	  sum(t1.phurce_old)  as phurce_old,
	  sum(t1.silent)  as silent,
	  sum(t1.loss)  as loss,
	  sum(t1.whole) as whole,
	  3 as type,
	  ${hiveconf:isMonthEnd} as end_month,
	  '${stat_date}' as stat_date,
	  ${hiveconf:submitTime} as modified
from dw_rfm.cix_online_member_life_cycle t1
	where t1.part='${stat_date}' --合计行数据
group by t1.tenant,t1.plat_code,t1.uni_shop_id,t1.card_plan_id;


-- 基于店铺级的数据，计算平台级数据 和 租户级数据
insert into table dw_rfm.cix_online_member_life_cycle partition(part='${stat_date}')
select t.tenant,t.plat_code,null as uni_shop_id,t.card_plan_id,t.grade,
	  sum(t.prospective)  as prospective,
	  sum(t.active_new)  as active_new,
	  sum(t.phurce_new)  as phurce_new,
	  sum(t.active_old)  as active_old,
	  sum(t.phurce_old)  as phurce_old,
	  sum(t.silent)  as silent,
	  sum(t.loss)  as loss,
	  sum(t.whole) as whole,
	  2 as type,
	  ${hiveconf:isMonthEnd} as end_month,
	  '${stat_date}' as stat_date,
	  ${hiveconf:submitTime} as modified
from dw_rfm.cix_online_member_life_cycle t 
where t.part='${stat_date}'
group by t.tenant,t.plat_code,t.card_plan_id,t.grade
union all
select t1.tenant,null as plat_code,null as uni_shop_id,t1.card_plan_id,t1.grade,
	  sum(t1.prospective)  as prospective,
	  sum(t1.active_new)  as active_new,
	  sum(t1.phurce_new)  as phurce_new,
	  sum(t1.active_old)  as active_old,
	  sum(t1.phurce_old)  as phurce_old,
	  sum(t1.silent)  as silent,
	  sum(t1.loss)  as loss,
	  sum(t1.whole) as whole,
	  1 as type,
	  ${hiveconf:isMonthEnd} as end_month,
	  '${stat_date}' as stat_date,
	  ${hiveconf:submitTime} as modified
from dw_rfm.cix_online_member_life_cycle t1 
where t1.part='${stat_date}'
group by t1.tenant,t1.card_plan_id,t1.grade;

-- 删除中间临时表
drop table if exists dw_rfm.b_customer_member_rfm_temp;

-- 统计完成后需要将表 dw_rfm.cix_online_member_life_cycle 的分区${stat_date}下的数据同步给业务库




