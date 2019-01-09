SET mapred.job.name='member-point-grade-change 会员积分等级变更分析';
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
-- 有效积分：dw_business.b_member_efffect_point
-- 等级变更表：dw_business.b_member_grade_change
-- 积分变更表: dw_business.b_member_point_change

set submitTime=from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss');

--昨天
set yestoday=date_sub('${stat_date}',1);
--本周的第一天
set weekfirst=date_sub('${stat_date}',pmod(datediff('${stat_date}', concat(year('${stat_date}'),'-01-01'))-6,7));
-- 本月的第一天
set monthfirst=date_sub('${stat_date}',dayofmonth('${stat_date}')-1);
-- 近七天
set last7day=date_sub('${stat_date}',7);
-- 近30天
set last30day=date_sub('${stat_date}',30);


-- 首先计算昨天的数据

-- 1、有积分变动的会员人数
select card_plan_id,member_id,change_value,change_time,action_type,source
from dw_business.b_member_point_change

-- 2、会员等级变化表
select card_plan_id,member_id,grade_before_change,grade_after_change,grade_period,change_time,change_type,change_source
from dw_business.b_member_grade_change

-- 3、会员有效积分
select card_plan_id,member_id,point,effective_date,occur_date,create_date,overdue_date,valid,modify_time
from dw_business.b_member_efffect_point


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


-- 当前有效积分存量,不随时间变化,只有最新当前一份数据
drop table if exists dw_rfm.b_current_effect_points_temp;
create table dw_rfm.b_current_effect_points_temp as
select card_plan_id,sum(point) effect_points
from dw_business.b_member_efffect_point
where substr(effective_date,1,10) <= '${stat_date}'
and substr(overdue_date,1,10) > '${stat_date}'
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
-- 昨日等级变动会员
drop table if exists dw_rfm.b_member_grade_statics_temp;
create table dw_rfm.b_member_grade_statics_temp as
select re.card_plan_id,re.rangetype,sum(re.upnums) as upnums,sum(re.downnums) as downnums
from(
	select t1.card_plan_id,t1.rangetype,
	case when t1.change_type='UP' then t1.members else 0 end as upnums,
	case when t1.change_type='DOWN' then t1.members else 0 end as downnums
	from
	(
		select re.card_plan_id,re.change_type,1 as rangetype,count(re.member_id) members
		from(
			select *,row_number() over (partition by r.card_plan_id,r.member_id order by r.change_time) as num
			from(
				select t.card_plan_id,t.member_id,t.change_type,t.change_time
					from dw_business.b_member_grade_change t
				where t.part = ${hiveconf:yestoday}
					and substr(t.change_time,1,10) = ${hiveconf:yestoday}
					and (t.change_type = 'UP' or t.change_type = 'DOWN')
			) r
		) re
		where re.num=1
		group by re.card_plan_id,re.change_type

		union all
		-- 本周指标计算
		select re.card_plan_id,re.change_type,2 as rangetype,count(re.member_id) members
		from(
			select *,row_number() over (partition by r.card_plan_id,r.member_id order by r.change_time) as num
			from(
				select t.card_plan_id,t.member_id,t.change_type,t.change_time
					from dw_business.b_member_grade_change t
				where t.part >= ${hiveconf:weekfirst}
					and substr(t.change_time,1,10) >= ${hiveconf:weekfirst}
					and (t.change_type = 'UP' or t.change_type = 'DOWN')
			) r
		) re
		where re.num=1
		group by re.card_plan_id,re.change_type

		union all
		-- 本周指标计算
		select re.card_plan_id,re.change_type,3 as rangetype,count(re.member_id) members
		from(
			select *,row_number() over (partition by r.card_plan_id,r.member_id order by r.change_time) as num
			from(
				select t.card_plan_id,t.member_id,t.change_type,t.change_time
					from dw_business.b_member_grade_change t
				where t.part >= ${hiveconf:monthfirst}
					and substr(t.change_time,1,10) >= ${hiveconf:monthfirst}
					and (t.change_type = 'UP' or t.change_type = 'DOWN')
			) r
		) re
		where re.num=1
		group by re.card_plan_id,re.change_type

		union all
		-- 近7天指标计算
		select re.card_plan_id,re.change_type,4 as rangetype,count(re.member_id) members
		from(
			select *,row_number() over (partition by r.card_plan_id,r.member_id order by r.change_time) as num
			from(
				select t.card_plan_id,t.member_id,t.change_type,t.change_time
					from dw_business.b_member_grade_change t
				where t.part >= ${hiveconf:last7day}
					and substr(t.change_time,1,10) >= ${hiveconf:last7day}
					and (t.change_type = 'UP' or t.change_type = 'DOWN')
			) r
		) re
		where re.num=1
		group by re.card_plan_id,re.change_type

		union all
		-- 近30天指标计算
		select re.card_plan_id,re.change_type,5 as rangetype,count(re.member_id) members
		from(
			select *,row_number() over (partition by r.card_plan_id,r.member_id order by r.change_time) as num
			from(
				select t.card_plan_id,t.member_id,t.change_type,t.change_time
					from dw_business.b_member_grade_change t
				where t.part >= ${hiveconf:last30day}
					and substr(t.change_time,1,10) >= ${hiveconf:last30day}
					and (t.change_type = 'UP' or t.change_type = 'DOWN')
			) r
		) re
		where re.num=1
		group by re.card_plan_id,re.change_type
	) t1
) re
group by re.card_plan_id,re.rangetype;


-- 将上面几类统计数据进行合并，整理成结果表输出给业务







