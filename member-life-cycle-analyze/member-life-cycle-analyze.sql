SET mapred.job.name='member-life-cycle-analyze-会员生命周期分析';
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
-- 全渠道店铺客户RFM: dw_rfm.b_qqd_shop_rfm

-- 设置变量记录统计当天是否为月末的那天
set isMonthEnd=if(date_sub(concat(substr(add_months('${stat_date}',1),0,7),'-01'),1)='${stat_date}',1,0);
-- 设置任务提交时间
set submitTime=from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss');

-- 1、以客户信息关联会员信息，如果客户有会员信息，那么客户就是会员，否则是非会员
-- 计算出会员和店铺客户的信息

-- 给数据进行打标，识别出潜客，沉默客，流失客等
drop table if exists dw_rfm.b_shop_card_customer_temp;
create table dw_rfm.b_shop_card_customer_temp as
select t.tenant,t.plat_code,t.uni_shop_id,t.uni_id,m.card_plan_id,m.member_id,m.grade,
	case when (t.earliest_time <= '${stat_date}' and t.first_buy_time is null) or t.first_buy_time > '${stat_date}' then 'qianke'
	when t.year_buy_times = 1 and t.first_buy_time > add_months('${stat_date}',-12) then 'active_new'
	when t.year_buy_times >= 2 and t.first_buy_time > add_months('${stat_date}',-12) then 'phurce_new'
	when t.year_buy_times = 1 and t.first_buy_time <= add_months('${stat_date}',-12) then 'active_old'
	when t.year_buy_times >= 2 and t.first_buy_time <= add_months('${stat_date}',-12) then 'phurce_old'
	when t.year_buy_times=0 and t.tyear_buy_times>=1 then 'silent'
	when t.year_buy_times=0 and t.tyear_buy_times=0 and t.btyear_buy_times>=1 then 'loss' else 'qianke' end as custype
from(
	select a.card_plan_id,a.plat_code,b.tenant,b.uni_shop_id,b.uni_id,b.earliest_time,
			b.first_buy_time,b.year_buy_times,b.tyear_buy_times,b.btyear_buy_times
	from dw_business.b_card_shop_rel a
	left join(
		select tenant,plat_code,uni_shop_id,uni_id,earliest_time,first_buy_time,year_buy_times,
			tyear_buy_times,btyear_buy_times
		from dw_rfm.b_qqd_shop_rfm where part='${stat_date}'
	) b
	on a.plat_code=b.plat_code and concat(a.plat_code,'|',a.shop_id)=b.uni_shop_id
	where b.tenant is not null
) t
left join (
	select r.card_plan_id,r.member_id,r.grade,r.plat_code,r.shop_id,r1.uni_id,
		concat(r.plat_code,'|',r.shop_id) as uni_shop_id 
	from(
		select t.card_plan_id,t.member_id,t.grade,t1.plat_code,t1.shop_id
		from (
			select a.card_plan_id,a.member_id,a.grade
			from  dw_business.b_std_member_base_info a
			where substr(a.created,1,10) <= '${stat_date}' 
			and a.card_plan_id is not null 
			and a.member_id is not null
			and a.grade is not null
		) t
		join dw_business.b_card_shop_rel t1
		on t.card_plan_id = t1.card_plan_id
		where t1.plat_code is not null
	) r
	join dw_business.b_customer_member_relation r1
	on r.card_plan_id = r1.card_plan_id and r.member_id=r1.member_id
) m
on t.plat_code = m.plat_code and t.uni_shop_id = m.uni_shop_id and t.uni_id = m.uni_id;


--将每个店铺所有客户的生命周期统计出来
drop table if exists dw_rfm.b_shop_all_customer_life_count;
create table dw_rfm.b_shop_all_customer_life_count as
select c.tenant,c.plat_code,c.uni_shop_id,
	if(c.prospective is null,0,c.prospective) as prospective,
	if(c.active_new is null,0,c.active_new) active_new,
	if(c.phurce_new is null,0,c.phurce_new) phurce_new,
	if(c.active_old is null,0,c.active_old) active_old,
	if(c.phurce_old is null,0,c.phurce_old) phurce_old,
	if(c.silent is null,0,c.silent) silent,
	if(c.loss is null,0,c.loss) loss,
	c.whole
from (
	select b.tenant,b.plat_code,b.uni_shop_id,
		cast(concat_ws('',collect_set(if(b.custype='qianke',b.num,''))) as bigint) prospective, 
		cast(concat_ws('',collect_set(if(b.custype='active_new',b.num,''))) as bigint) active_new, 
		cast(concat_ws('',collect_set(if(b.custype='phurce_new',b.num,''))) as bigint) phurce_new, 
		cast(concat_ws('',collect_set(if(b.custype='active_old',b.num,''))) as bigint) active_old, 
		cast(concat_ws('',collect_set(if(b.custype='phurce_old',b.num,''))) as bigint) phurce_old, 
		cast(concat_ws('',collect_set(if(b.custype='silent',b.num,''))) as bigint) silent, 
		cast(concat_ws('',collect_set(if(b.custype='loss',b.num,''))) as bigint) loss,  
		sum(b.num) whole  
	from(
		select 
			t.tenant,t.plat_code,t.uni_shop_id,t.custype,count(t.uni_id) num
		from 
			dw_rfm.b_shop_card_customer_temp t
		group by t.tenant,t.plat_code,t.uni_shop_id,t.custype
	) b
	group by b.tenant,b.plat_code,b.uni_shop_id
)c;

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

-- 计算每个店铺会员卡中各等级对应的客户类型的数量
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
			cast(concat_ws('',collect_set(if(b.custype='qianke',b.num,''))) as bigint) prospective, 
			cast(concat_ws('',collect_set(if(b.custype='active_new',b.num,''))) as bigint) active_new,
			cast(concat_ws('',collect_set(if(b.custype='phurce_new',b.num,''))) as bigint) phurce_new, 
			cast(concat_ws('',collect_set(if(b.custype='active_old',b.num,''))) as bigint) active_old,
			cast(concat_ws('',collect_set(if(b.custype='phurce_old',b.num,''))) as bigint) phurce_old,
			cast(concat_ws('',collect_set(if(b.custype='silent',b.num,''))) as bigint) silent, 
			cast(concat_ws('',collect_set(if(b.custype='loss',b.num,''))) as bigint) loss,
			sum(b.num) as whole
	from(
		select 
			t.tenant,t.plat_code,t.uni_shop_id,t.card_plan_id,t.grade,t.custype,count(distinct t.uni_id) num
		from 
			dw_rfm.b_shop_card_customer_temp t
		where t.grade is not null
		group by t.tenant,t.plat_code,t.uni_shop_id,t.card_plan_id,t.grade,t.custype
	) b
	group by b.tenant,b.plat_code,b.uni_shop_id,b.card_plan_id,b.grade	
) c;

-- 计算会员卡下会员的合计行
drop table if exists dw_rfm.b_shop_member_count_temp;
create table dw_rfm.b_shop_member_count_temp as
select t.tenant,t.plat_code,t.uni_shop_id,t.card_plan_id,98 as grade,
	  sum(t.prospective)  as prospective,
	  sum(t.active_new)  as active_new,
	  sum(t.phurce_new)  as phurce_new,
	  sum(t.active_old)  as active_old,
	  sum(t.phurce_old)  as phurce_old,
	  sum(t.silent)  as silent,
	  sum(t.loss)  as loss,
	  sum(t.whole) as whole
from dw_rfm.cix_online_member_life_cycle t
where t.part='${stat_date}' 
	and t.card_plan_id is not null
group by t.tenant,t.plat_code,t.uni_shop_id,t.card_plan_id;

-- 计算店铺中各会员卡的非会员数量，店铺客户总指标-店铺会员的指标
drop table if exists dw_rfm.b_shop_member_total_temp;
create table dw_rfm.b_shop_member_total_temp as
select a.tenant,a.plat_code,a.uni_shop_id,b.card_plan_id,
	   a.prospective,a.active_new,a.phurce_new,a.active_old,a.phurce_old,a.silent,a.loss,a.whole,
	    (a.prospective-b.prospective) as ng_prospective, 
	    (a.active_new-b.active_new) as ng_active_new, 
		(a.phurce_new-b.phurce_new) as ng_phurce_new, 
		(a.active_old-b.active_old) as ng_active_old, 
		(a.phurce_old-b.phurce_old) as ng_phurce_old,
		(a.silent-b.silent) as ng_silent,
		(a.loss-b.loss) as ng_loss,
		(a.whole-b.whole) as ng_whole
from dw_rfm.b_shop_all_customer_life_count a
left join dw_rfm.b_shop_member_count_temp b
on a.tenant=b.tenant and a.plat_code=b.plat_code and a.uni_shop_id=b.uni_shop_id;

--店铺级的会员行和店铺级的非会员行
insert into table dw_rfm.cix_online_member_life_cycle partition(part='${stat_date}')
select a.tenant,a.plat_code,a.uni_shop_id,a.card_plan_id,99 as grade,
	   a.prospective,a.active_new,a.phurce_new,a.active_old,a.phurce_old,a.silent,a.loss,a.whole,
	   3 as type,
	   ${hiveconf:isMonthEnd} as end_month,
	   '${stat_date}' as stat_date,
	   ${hiveconf:submitTime} as modified
	   from dw_rfm.b_shop_member_total_temp a
union all
select b.tenant,b.plat_code,b.uni_shop_id,b.card_plan_id,-1 as grade,
	   b.ng_prospective,b.ng_active_new,b.ng_phurce_new,b.ng_active_old,b.ng_phurce_old,b.ng_silent,b.ng_loss,b.ng_whole,
	   3 as type,
	   ${hiveconf:isMonthEnd} as end_month,
	   '${stat_date}' as stat_date,
	   ${hiveconf:submitTime} as modified
	   from dw_rfm.b_shop_member_total_temp b
union all
select c.tenant,c.plat_code,c.uni_shop_id,c.card_plan_id,c.grade,
	   c.prospective,c.active_new,c.phurce_new,c.active_old,c.phurce_old,c.silent,c.loss,c.whole,
	   3 as type,
	   ${hiveconf:isMonthEnd} as end_month,
	   '${stat_date}' as stat_date,
	   ${hiveconf:submitTime} as modified
	   from dw_rfm.b_shop_member_count_temp c;


-- 基于店铺级卡客户关系数据，计算平台级数据和租户级数据
drop table if exists dw_rfm.b_plat_card_customer_temp;
create table dw_rfm.b_plat_card_customer_temp as
select t.tenant,t.plat_code,t.uni_id,m.card_plan_id,m.member_id,m.grade,
	case when (t.earliest_time <= '${stat_date}' and t.first_buy_time is null) or t.first_buy_time > '${stat_date}' then 'qianke'
	when t.year_buy_times = 1 and t.first_buy_time > add_months('${stat_date}',-12) then 'active_new'
	when t.year_buy_times >= 2 and t.first_buy_time > add_months('${stat_date}',-12) then 'phurce_new'
	when t.year_buy_times = 1 and t.first_buy_time <= add_months('${stat_date}',-12) then 'active_old'
	when t.year_buy_times >= 2 and t.first_buy_time <= add_months('${stat_date}',-12) then 'phurce_old'
	when t.year_buy_times=0 and t.tyear_buy_times>=1 then 'silent'
	when t.year_buy_times=0 and t.tyear_buy_times=0 and t.btyear_buy_times>=1 then 'loss' else 'qianke' end as custype
from(
	select tenant,plat_code,uni_id,earliest_time,first_buy_time,year_buy_times,
			tyear_buy_times,btyear_buy_times
	from dw_rfm.b_qqd_plat_rfm where part='${stat_date}'
) t
left join dw_rfm.b_shop_card_customer_temp m
on t.tenant=m.tenant and t.plat_code = m.plat_code and t.uni_id = m.uni_id;


--将每个平台所有客户的生命周期统计出来
drop table if exists dw_rfm.b_plat_all_customer_life_count;
create table dw_rfm.b_plat_all_customer_life_count as
select c.tenant,c.plat_code,
	if(c.prospective is null,0,c.prospective) as prospective,
	if(c.active_new is null,0,c.active_new) active_new,
	if(c.phurce_new is null,0,c.phurce_new) phurce_new,
	if(c.active_old is null,0,c.active_old) active_old,
	if(c.phurce_old is null,0,c.phurce_old) phurce_old,
	if(c.silent is null,0,c.silent) silent,
	if(c.loss is null,0,c.loss) loss,
	c.whole
from (
	select b.tenant,b.plat_code,
		cast(concat_ws('',collect_set(if(b.custype='qianke',b.num,''))) as bigint) prospective, 
		cast(concat_ws('',collect_set(if(b.custype='active_new',b.num,''))) as bigint) active_new, 
		cast(concat_ws('',collect_set(if(b.custype='phurce_new',b.num,''))) as bigint) phurce_new, 
		cast(concat_ws('',collect_set(if(b.custype='active_old',b.num,''))) as bigint) active_old, 
		cast(concat_ws('',collect_set(if(b.custype='phurce_old',b.num,''))) as bigint) phurce_old, 
		cast(concat_ws('',collect_set(if(b.custype='silent',b.num,''))) as bigint) silent, 
		cast(concat_ws('',collect_set(if(b.custype='loss',b.num,''))) as bigint) loss,  
		sum(b.num) whole  
	from(
		select 
			t.tenant,t.plat_code,t.custype,count(distinct t.uni_id) num
		from 
			dw_rfm.b_plat_card_customer_temp t
		group by t.tenant,t.plat_code,t.custype
	) b
	group by b.tenant,b.plat_code
)c;

-- 创建平台级的会员生命周期
CREATE TABLE IF NOT EXISTS dw_rfm.`b_plat_member_life_cycle_temp`(
	`tenant` string,
	`plat_code` string,
	`card_plan_id` string,
	`grade` int, 
    `prospective` bigint, 
    `active_new` bigint,
	`phurce_new` bigint,
    `active_old` bigint,
	`phurce_old` bigint,
	`silent` bigint,
    `loss` bigint,
	`whole` bigint
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");

-- 计算每个平台会员卡中各等级对应的客户类型的数量
insert overwrite table dw_rfm.b_plat_member_life_cycle_temp
select c.tenant,c.plat_code,c.card_plan_id,c.grade,
	if(c.prospective is null,0,c.prospective) as prospective,
	if(c.active_new is null,0,c.active_new) active_new,
	if(c.phurce_new is null,0,c.phurce_new) phurce_new,
	if(c.active_old is null,0,c.active_old) active_old,
	if(c.phurce_old is null,0,c.phurce_old) phurce_old,
	if(c.silent is null,0,c.silent) silent,
	if(c.loss is null,0,c.loss) loss,
	c.whole
from (
	select b.tenant,b.plat_code,b.card_plan_id,b.grade,
			cast(concat_ws('',collect_set(if(b.custype='qianke',b.num,''))) as bigint) prospective, 
			cast(concat_ws('',collect_set(if(b.custype='active_new',b.num,''))) as bigint) active_new,
			cast(concat_ws('',collect_set(if(b.custype='phurce_new',b.num,''))) as bigint) phurce_new, 
			cast(concat_ws('',collect_set(if(b.custype='active_old',b.num,''))) as bigint) active_old,
			cast(concat_ws('',collect_set(if(b.custype='phurce_old',b.num,''))) as bigint) phurce_old,
			cast(concat_ws('',collect_set(if(b.custype='silent',b.num,''))) as bigint) silent, 
			cast(concat_ws('',collect_set(if(b.custype='loss',b.num,''))) as bigint) loss,
			sum(b.num) as whole
	from(
		select 
			t.tenant,t.plat_code,t.card_plan_id,t.grade,t.custype,count(distinct t.uni_id) num
		from 
			dw_rfm.b_plat_card_customer_temp t
		where t.grade is not null
		group by t.tenant,t.plat_code,t.card_plan_id,t.grade,t.custype
	) b
	group by b.tenant,b.plat_code,b.card_plan_id,b.grade	
) c;

-- 计算平台+会员卡下会员的合计行
drop table if exists dw_rfm.b_plat_member_count_temp;
create table dw_rfm.b_plat_member_count_temp as
select t.tenant,t.plat_code,t.card_plan_id,98 as grade,
	  sum(t.prospective)  as prospective,
	  sum(t.active_new)  as active_new,
	  sum(t.phurce_new)  as phurce_new,
	  sum(t.active_old)  as active_old,
	  sum(t.phurce_old)  as phurce_old,
	  sum(t.silent)  as silent,
	  sum(t.loss)  as loss,
	  sum(t.whole) as whole
from dw_rfm.b_plat_member_life_cycle_temp t
where t.card_plan_id is not null
group by t.tenant,t.plat_code,t.card_plan_id;

-- 计算平台中各会员卡的非会员数量，平台客户总指标-平台会员的指标
drop table if exists dw_rfm.b_plat_member_total_temp;
create table dw_rfm.b_plat_member_total_temp as
select a.tenant,a.plat_code,b.card_plan_id,
	   a.prospective,a.active_new,a.phurce_new,a.active_old,a.phurce_old,a.silent,a.loss,a.whole,
	    (a.prospective-b.prospective) as ng_prospective, 
	    (a.active_new-b.active_new) as ng_active_new, 
		(a.phurce_new-b.phurce_new) as ng_phurce_new, 
		(a.active_old-b.active_old) as ng_active_old, 
		(a.phurce_old-b.phurce_old) as ng_phurce_old,
		(a.silent-b.silent) as ng_silent,
		(a.loss-b.loss) as ng_loss,
		(a.whole-b.whole) as ng_whole
from dw_rfm.b_plat_all_customer_life_count a
left join dw_rfm.b_plat_member_count_temp b
on a.tenant=b.tenant and a.plat_code=b.plat_code;

--平台级的会员行和平台级的非会员行
insert into table dw_rfm.b_plat_member_life_cycle_temp
select a.tenant,a.plat_code,a.card_plan_id,99 as grade,
	   a.prospective,a.active_new,a.phurce_new,a.active_old,a.phurce_old,a.silent,a.loss,a.whole
	   from dw_rfm.b_plat_member_total_temp a
union all
select b.tenant,b.plat_code,b.card_plan_id,-1 as grade,
	   b.ng_prospective,b.ng_active_new,b.ng_phurce_new,b.ng_active_old,b.ng_phurce_old,b.ng_silent,b.ng_loss,b.ng_whole
	   from dw_rfm.b_plat_member_total_temp b
union all
select c.tenant,c.plat_code,c.card_plan_id,c.grade,
	   c.prospective,c.active_new,c.phurce_new,c.active_old,c.phurce_old,c.silent,c.loss,c.whole
	   from dw_rfm.b_plat_member_count_temp c;
	   
	   
-- 基于平台级卡客户关系数据，计算租户级数据和租户级数据
drop table if exists dw_rfm.b_tenant_card_customer_temp;
create table dw_rfm.b_tenant_card_customer_temp as
select t.tenant,t.uni_id,m.card_plan_id,m.member_id,m.grade,
	case when (t.earliest_time <= '${stat_date}' and t.first_buy_time is null) or t.first_buy_time > '${stat_date}' then 'qianke'
	when t.year_buy_times = 1 and t.first_buy_time > add_months('${stat_date}',-12) then 'active_new'
	when t.year_buy_times >= 2 and t.first_buy_time > add_months('${stat_date}',-12) then 'phurce_new'
	when t.year_buy_times = 1 and t.first_buy_time <= add_months('${stat_date}',-12) then 'active_old'
	when t.year_buy_times >= 2 and t.first_buy_time <= add_months('${stat_date}',-12) then 'phurce_old'
	when t.year_buy_times=0 and t.tyear_buy_times>=1 then 'silent'
	when t.year_buy_times=0 and t.tyear_buy_times=0 and t.btyear_buy_times>=1 then 'loss' else 'qianke' end as custype
from(
	select tenant,uni_id,earliest_time,first_buy_time,year_buy_times,
			tyear_buy_times,btyear_buy_times
	from dw_rfm.b_qqd_tenant_rfm where part='${stat_date}'
) t
left join dw_rfm.b_plat_card_customer_temp m
on t.tenant=m.tenant and t.uni_id = m.uni_id;


--将每个租户所有客户的生命周期统计出来
drop table if exists dw_rfm.b_tenant_all_customer_life_count;
create table dw_rfm.b_tenant_all_customer_life_count as
select c.tenant,
	if(c.prospective is null,0,c.prospective) as prospective,
	if(c.active_new is null,0,c.active_new) active_new,
	if(c.phurce_new is null,0,c.phurce_new) phurce_new,
	if(c.active_old is null,0,c.active_old) active_old,
	if(c.phurce_old is null,0,c.phurce_old) phurce_old,
	if(c.silent is null,0,c.silent) silent,
	if(c.loss is null,0,c.loss) loss,
	c.whole
from (
	select b.tenant,
		cast(concat_ws('',collect_set(if(b.custype='qianke',b.num,''))) as bigint) prospective, 
		cast(concat_ws('',collect_set(if(b.custype='active_new',b.num,''))) as bigint) active_new, 
		cast(concat_ws('',collect_set(if(b.custype='phurce_new',b.num,''))) as bigint) phurce_new, 
		cast(concat_ws('',collect_set(if(b.custype='active_old',b.num,''))) as bigint) active_old, 
		cast(concat_ws('',collect_set(if(b.custype='phurce_old',b.num,''))) as bigint) phurce_old, 
		cast(concat_ws('',collect_set(if(b.custype='silent',b.num,''))) as bigint) silent, 
		cast(concat_ws('',collect_set(if(b.custype='loss',b.num,''))) as bigint) loss,  
		sum(b.num) whole  
	from(
		select 
			t.tenant,t.custype,count(distinct t.uni_id) num
		from 
			dw_rfm.b_tenant_card_customer_temp t
		group by t.tenant,t.custype
	) b
	group by b.tenant
)c;

-- 创建租户级的会员生命周期
CREATE TABLE IF NOT EXISTS dw_rfm.`b_tenant_member_life_cycle_temp`(
	`tenant` string,
	`card_plan_id` string,
	`grade` int, 
    `prospective` bigint, 
    `active_new` bigint,
	`phurce_new` bigint,
    `active_old` bigint,
	`phurce_old` bigint,
	`silent` bigint,
    `loss` bigint,
	`whole` bigint
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");

-- 计算每个平台会员卡中各等级对应的客户类型的数量
insert overwrite table dw_rfm.b_tenant_member_life_cycle_temp
select c.tenant,c.card_plan_id,c.grade,
	if(c.prospective is null,0,c.prospective) as prospective,
	if(c.active_new is null,0,c.active_new) active_new,
	if(c.phurce_new is null,0,c.phurce_new) phurce_new,
	if(c.active_old is null,0,c.active_old) active_old,
	if(c.phurce_old is null,0,c.phurce_old) phurce_old,
	if(c.silent is null,0,c.silent) silent,
	if(c.loss is null,0,c.loss) loss,
	c.whole
from (
	select b.tenant,b.card_plan_id,b.grade,
			cast(concat_ws('',collect_set(if(b.custype='qianke',b.num,''))) as bigint) prospective, 
			cast(concat_ws('',collect_set(if(b.custype='active_new',b.num,''))) as bigint) active_new,
			cast(concat_ws('',collect_set(if(b.custype='phurce_new',b.num,''))) as bigint) phurce_new, 
			cast(concat_ws('',collect_set(if(b.custype='active_old',b.num,''))) as bigint) active_old,
			cast(concat_ws('',collect_set(if(b.custype='phurce_old',b.num,''))) as bigint) phurce_old,
			cast(concat_ws('',collect_set(if(b.custype='silent',b.num,''))) as bigint) silent, 
			cast(concat_ws('',collect_set(if(b.custype='loss',b.num,''))) as bigint) loss,
			sum(b.num) as whole
	from(
		select 
			t.tenant,t.card_plan_id,t.grade,t.custype,count(distinct t.uni_id) num
		from 
			dw_rfm.b_tenant_card_customer_temp t
		where t.grade is not null
		group by t.tenant,t.card_plan_id,t.grade,t.custype
	) b
	group by b.tenant,b.card_plan_id,b.grade	
) c;

-- 计算租户+会员卡下会员的合计行
drop table if exists dw_rfm.b_tenant_member_count_temp;
create table dw_rfm.b_tenant_member_count_temp as
select t.tenant,t.card_plan_id,98 as grade,
	  sum(t.prospective)  as prospective,
	  sum(t.active_new)  as active_new,
	  sum(t.phurce_new)  as phurce_new,
	  sum(t.active_old)  as active_old,
	  sum(t.phurce_old)  as phurce_old,
	  sum(t.silent)  as silent,
	  sum(t.loss)  as loss,
	  sum(t.whole) as whole
from dw_rfm.b_tenant_member_life_cycle_temp t
where t.card_plan_id is not null
group by t.tenant,t.card_plan_id;

-- 计算租户中各会员卡的非会员数量，租户客户总指标-平台会员的指标
drop table if exists dw_rfm.b_tenant_member_total_temp;
create table dw_rfm.b_tenant_member_total_temp as
select a.tenant,b.card_plan_id,
	   a.prospective,a.active_new,a.phurce_new,a.active_old,a.phurce_old,a.silent,a.loss,a.whole,
	    (a.prospective-b.prospective) as ng_prospective, 
	    (a.active_new-b.active_new) as ng_active_new, 
		(a.phurce_new-b.phurce_new) as ng_phurce_new, 
		(a.active_old-b.active_old) as ng_active_old, 
		(a.phurce_old-b.phurce_old) as ng_phurce_old,
		(a.silent-b.silent) as ng_silent,
		(a.loss-b.loss) as ng_loss,
		(a.whole-b.whole) as ng_whole
from dw_rfm.b_tenant_all_customer_life_count a
left join dw_rfm.b_tenant_member_count_temp b
on a.tenant=b.tenant;

--租户级的会员行和平台级的非会员行
insert into table dw_rfm.b_tenant_member_life_cycle_temp
select a.tenant,a.card_plan_id,99 as grade,
	   a.prospective,a.active_new,a.phurce_new,a.active_old,a.phurce_old,a.silent,a.loss,a.whole
	   from dw_rfm.b_tenant_member_total_temp a
union all
select b.tenant,b.card_plan_id,-1 as grade,
	   b.ng_prospective,b.ng_active_new,b.ng_phurce_new,b.ng_active_old,b.ng_phurce_old,b.ng_silent,b.ng_loss,b.ng_whole
	   from dw_rfm.b_tenant_member_total_temp b
union all
select c.tenant,c.card_plan_id,c.grade,
	   c.prospective,c.active_new,c.phurce_new,c.active_old,c.phurce_old,c.silent,c.loss,c.whole
	   from dw_rfm.b_tenant_member_count_temp c;

--将平台级和租户级数据合入结果表
insert into table dw_rfm.cix_online_member_life_cycle partition(part='${stat_date}')
select t.tenant,t.plat_code,null as uni_shop_id,t.card_plan_id,t.grade,
	  t.prospective,
	  t.active_new,
	  t.phurce_new,
	  t.active_old,
	  t.phurce_old,
	  t.silent,
	  t.loss,
	  t.whole,
	  2 as type,
	  ${hiveconf:isMonthEnd} as end_month,
	  '${stat_date}' as stat_date,
	  ${hiveconf:submitTime} as modified
from dw_rfm.b_plat_member_life_cycle_temp t 
where t.card_plan_id is not null
union all
select t1.tenant,null as plat_code,null as uni_shop_id,t1.card_plan_id,t1.grade,
	  t1.prospective,
	  t1.active_new,
	  t1.phurce_new,
	  t1.active_old,
	  t1.phurce_old,
	  t1.silent,
	  t1.loss,
	  t1.whole,
	  1 as type,
	  ${hiveconf:isMonthEnd} as end_month,
	  '${stat_date}' as stat_date,
	  ${hiveconf:submitTime} as modified
from dw_rfm.b_tenant_member_life_cycle_temp t1 
where t1.card_plan_id is not null;

-- 避免union all产生文件目录无法导入问题
drop table if exists dw_rfm.cix_online_member_life_cycle_temp;
create table if not exists dw_rfm.cix_online_member_life_cycle_temp as
select tenant,plat_code,uni_shop_id,card_plan_id,grade,
	  prospective,active_new,phurce_new,active_old,phurce_old,silent,
	  loss,whole,type,end_month,stat_date,modified
from dw_rfm.cix_online_member_life_cycle
where part='${stat_date}' and card_plan_id is not null;
	
insert overwrite table dw_rfm.cix_online_member_life_cycle partition(part='${stat_date}')
select tenant,plat_code,uni_shop_id,card_plan_id,grade,
	  prospective,active_new,phurce_new,active_old,phurce_old,silent,
	  loss,whole,type,end_month,stat_date,modified 
from dw_rfm.cix_online_member_life_cycle_temp;
	  
drop table if exists dw_rfm.cix_online_member_life_cycle_temp;

-- 删除中间临时表
drop table if exists dw_rfm.b_shop_card_customer_temp;
drop table if exists dw_rfm.b_shop_member_total_temp;
drop table if exists dw_rfm.b_shop_member_count_temp;
drop table if exists dw_rfm.b_shop_all_customer_life_count;

drop table if exists dw_rfm.b_plat_card_customer_temp;
drop table if exists dw_rfm.b_plat_member_total_temp;
drop table if exists dw_rfm.b_plat_member_count_temp;
drop table if exists dw_rfm.b_plat_all_customer_life_count;
drop table if exists dw_rfm.b_plat_member_life_cycle_temp;

drop table if exists dw_rfm.b_tenant_card_customer_temp;
drop table if exists dw_rfm.b_tenant_member_total_temp;
drop table if exists dw_rfm.b_tenant_member_count_temp;
drop table if exists dw_rfm.b_tenant_all_customer_life_count;
drop table if exists dw_rfm.b_tenant_member_life_cycle_temp;

-- 统计完成后需要将表 dw_rfm.cix_online_member_life_cycle 的分区${stat_date}下的数据同步给业务库




