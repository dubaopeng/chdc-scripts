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


-- 1、获取会员信息及其关联的客户Id

select c.card_plan_id,c.plat_code,c.uni_shop_id,c.uni_id,c.grade,
	d.earliest_time,d.first_buy_time,d.year_buy_times,d.tyear_buy_times,d.btyear_buy_times
from (
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
) c
right join dw_rfm.b_qqd_shop_rfm d
on c.plat_code=d.plat_code and c.uni_shop_id=d.uni_shop_id and c.uni_id=d.uni_id;


-- 2、与RFM宽表进行关联


select * from dw_rfm.b_qqd_shop_rfm
left join 





--创建积分变更临时表
CREATE TABLE IF NOT EXISTS dw_rfm.`b_point_change_analyze_temp`(
	`card_plan_id` string,
	`member_id` string,
	`change_value` bigint,
	`type` int,
	`yestoday` int,
	`thisweek` int,
	`thismonth` int,
	`last7day` int,
	`last30day` int
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");

-- 近30天有积分变化的数据，中间包含昨天，本周、本月、近7天
insert overwrite table dw_rfm.b_point_change_analyze_temp
select card_plan_id,member_id,change_value,
	case when change_value >= 0 then 1 else -1 end as type,
	case when substr(change_time,1,10) >= ${hiveconf:yestoday} then 1 else 0 end as yestoday,
	case when substr(change_time,1,10) >= ${hiveconf:weekfirst} then 1 else 0 end as thisweek,
	case when substr(change_time,1,10) >= ${hiveconf:monthfirst} then 1 else 0 end as thismonth,
	case when substr(change_time,1,10) >= ${hiveconf:last7day} then 1 else 0 end as last7day,
	case when substr(change_time,1,10) >= ${hiveconf:last30day} then 1 else 0 end as last30day
from dw_business.b_member_point_change
where part >= ${hiveconf:last30day} and part <= '${stat_date}'
and substr(change_time,1,10) >= ${hiveconf:last30day} and substr(change_time,1,10)<='${stat_date}';

-- 计算每个时间段内有积分变动的会员数
drop table if exists dw_rfm.b_point_change_members_temp;
create table dw_rfm.b_point_change_members_temp as
select card_plan_id,1 as rangetype,count(distinct member_id) as members
from dw_rfm.b_point_change_analyze_temp
where yestoday=1 group by card_plan_id
union all
select card_plan_id,2 as rangetype,count(distinct member_id) as members
from dw_rfm.b_point_change_analyze_temp
where thisweek=1 group by card_plan_id
union all
select card_plan_id,3 as rangetype,count(distinct member_id) as members
from dw_rfm.b_point_change_analyze_temp
where thismonth=1
group by card_plan_id
union all
select card_plan_id,4 as rangetype,count(distinct member_id) as members
from dw_rfm.b_point_change_analyze_temp
where last7day=1
group by card_plan_id
union all
select card_plan_id,5 as rangetype,count(distinct member_id) as members
from dw_rfm.b_point_change_analyze_temp
where last30day=1
group by card_plan_id;


-- 计算发放和消耗的积分 type=1 发放 type=-1 消耗
drop table if exists dw_rfm.b_point_change_total_temp;
create table dw_rfm.b_point_change_total_temp as
	select card_plan_id,type,1 as rangetype,
	case when type=1 then sum(change_value) else abs(sum(change_value)) end as totalpoint
	from dw_rfm.b_point_change_analyze_temp
	where yestoday=1 group by card_plan_id,type
union all
	select card_plan_id,type,2 as rangetype,
	case when type=1 then sum(change_value) else abs(sum(change_value)) end as totalpoint
	from dw_rfm.b_point_change_analyze_temp
	where thisweek=1 group by card_plan_id,type
union all
	select card_plan_id,type,3 as rangetype,
	case when type=1 then sum(change_value) else abs(sum(change_value)) end as totalpoint
	from dw_rfm.b_point_change_analyze_temp
	where thismonth=1 group by card_plan_id,type
union all
	select card_plan_id,type,4 as rangetype,
	case when type=1 then sum(change_value) else abs(sum(change_value)) end as totalpoint
	from dw_rfm.b_point_change_analyze_temp
	where last7day=1 group by card_plan_id,type
union all
	select card_plan_id,type,5 as rangetype,
	case when type=1 then sum(change_value) else abs(sum(change_value)) end as totalpoint
	from dw_rfm.b_point_change_analyze_temp
	where last30day=1 group by card_plan_id,type;


-- 计算积分失效和当前有效积分
-- 积分失效：积分到期日期在统计周期内的积分
-- 有效积分存量：截至“数据截至日期”时有效积分

-- 近30天失效的积分计算
drop table if exists dw_rfm.b_invalid_points_last30day;
create table dw_rfm.b_invalid_points_last30day as
select card_plan_id,point,
	case when substr(overdue_date,1,10) >= ${hiveconf:yestoday} then 1 else 0 end as yestoday,
	case when substr(overdue_date,1,10) >= ${hiveconf:weekfirst} then 1 else 0 end as thisweek,
	case when substr(overdue_date,1,10) >= ${hiveconf:monthfirst} then 1 else 0 end as thismonth,
	case when substr(overdue_date,1,10) >= ${hiveconf:last7day} then 1 else 0 end as last7day,
	case when substr(overdue_date,1,10) >= ${hiveconf:last30day} then 1 else 0 end as last30day
from dw_business.b_member_efffect_point
where substr(overdue_date,1,10) <= '${stat_date}'
and substr(overdue_date,1,10) >= ${hiveconf:last30day}
and valid=0;

-- 几个时间段的过期积分总数
drop table if exists dw_rfm.b_invalid_points_statics_temp;
create table dw_rfm.b_invalid_points_statics_temp as 
select card_plan_id,1 as rangetype,sum(point) as overdue_points
	from dw_rfm.b_invalid_points_last30day
	where yestoday=1 group by card_plan_id
union all
	select card_plan_id,2 as rangetype,sum(point) as overdue_points
	from dw_rfm.b_invalid_points_last30day
	where thisweek=1 group by card_plan_id
union all
	select card_plan_id,3 as rangetype,sum(point) as overdue_points
	from dw_rfm.b_invalid_points_last30day
	where thismonth=1 group by card_plan_id
union all
	select card_plan_id,4 as rangetype,sum(point) as overdue_points
	from dw_rfm.b_invalid_points_last30day
	where last7day=1 group by card_plan_id
union all
	select card_plan_id,5 as rangetype,sum(point) as overdue_points
	from dw_rfm.b_invalid_points_last30day
	where last30day=1 group by card_plan_id;

-- 删除掉近30天的过期积分的统计
drop table if exists dw_rfm.b_invalid_points_last30day;


-- 积分存量存在疑问
select
from dw_business.b_member_efffect_point
where substr(effective_date,1,10) >= '${stat_date}'
and valid=1;


-- 从会员基本信息中查询新增的会员
drop table if exists dw_rfm.b_new_member_last30day_temp;
create table dw_rfm.b_new_member_last30day_temp as
select card_plan_id,member_id,
	case when substr(created,1,10) >= ${hiveconf:yestoday} then 1 else 0 end as yestoday,
	case when substr(created,1,10) >= ${hiveconf:weekfirst} then 1 else 0 end as thisweek,
	case when substr(created,1,10) >= ${hiveconf:monthfirst} then 1 else 0 end as thismonth,
	case when substr(created,1,10) >= ${hiveconf:last7day} then 1 else 0 end as last7day,
	case when substr(created,1,10) >= ${hiveconf:last30day} then 1 else 0 end as last30day
from dw_business.b_std_member_base_info
where substr(created,1,10) >= ${hiveconf:last30day};

-- 各时间段新增会员的统计结果
drop table if exists dw_rfm.b_new_member_statics_temp;
create table dw_rfm.b_new_member_statics_temp as 
select card_plan_id,1 as rangetype,count(member_id) as newmembers
	from dw_rfm.b_new_member_last30day_temp
	where yestoday=1 group by card_plan_id
union all
	select card_plan_id,2 as rangetype,count(member_id) as newmembers
	from dw_rfm.b_new_member_last30day_temp
	where thisweek=1 group by card_plan_id
union all
	select card_plan_id,3 as rangetype,count(member_id) as newmembers
	from dw_rfm.b_new_member_last30day_temp
	where thismonth=1 group by card_plan_id
union all
	select card_plan_id,4 as rangetype,count(member_id) as newmembers
	from dw_rfm.b_new_member_last30day_temp
	where last7day=1 group by card_plan_id
union all
	select card_plan_id,5 as rangetype,count(member_id) as newmembers
	from dw_rfm.b_new_member_last30day_temp
	where last30day=1 group by card_plan_id;

-- 删除临时近30天的会员临时表
drop table if exists dw_rfm.b_new_member_last30day_temp;


-- 计算会员等级变更数量

select
* 
from dw_business.b_member_grade_change t
where t.part = '${stat_date}'



