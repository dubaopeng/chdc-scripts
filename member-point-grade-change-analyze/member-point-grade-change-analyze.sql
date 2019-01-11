SET mapred.job.name='member-point-grade-change-会员积分等级变更分析';
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

--创建积分变更临时表
CREATE TABLE IF NOT EXISTS dw_rfm.`b_point_change_analyze_temp`(
	`card_plan_id` string,
	`member_id` string,
	`change_value` bigint,
	`udtype` int,
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
	case when change_value >= 0 then 1 else -1 end as udtype,
	case when substr(change_time,1,10) >= ${hiveconf:yestoday} then 1 else 0 end as yestoday,
	case when substr(change_time,1,10) >= ${hiveconf:weekfirst} then 1 else 0 end as thisweek,
	case when substr(change_time,1,10) >= ${hiveconf:monthfirst} then 1 else 0 end as thismonth,
	case when substr(change_time,1,10) >= ${hiveconf:last7day} then 1 else 0 end as last7day,
	case when substr(change_time,1,10) >= ${hiveconf:last30day} then 1 else 0 end as last30day
from dw_business.b_member_point_change
where part >= ${hiveconf:last30day} and part <= '${stat_date}'
and substr(change_time,1,10) >= ${hiveconf:last30day} and substr(change_time,1,10)<='${stat_date}';

-- 计算每个时间段内有积分变动的会员数
drop table if exists dw_rfm.b_point_change_members_statics_temp;
create table dw_rfm.b_point_change_members_statics_temp as
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


-- 计算发放和消耗的积分 udtype=1 发放 udtype=-1 消耗
drop table if exists dw_rfm.b_point_change_statics_temp;
create table dw_rfm.b_point_change_statics_temp as
select re.card_plan_id,re.rangetype,sum(re.send_points) send_points,sum(re.consume_points) consume_points
from (
	select t.card_plan_id,t.rangetype,
		   case when t.udtype=1 then t.totalpoint else 0 end as send_points,
		   case when t.udtype=-1 then t.totalpoint else 0 end as consume_points
	from
	(
		select card_plan_id,udtype,1 as rangetype,
			case when udtype=1 then sum(change_value) else abs(sum(change_value)) end as totalpoint
			from dw_rfm.b_point_change_analyze_temp
			where yestoday=1 group by card_plan_id,udtype
		union all
			select card_plan_id,udtype,2 as rangetype,
			case when udtype=1 then sum(change_value) else abs(sum(change_value)) end as totalpoint
			from dw_rfm.b_point_change_analyze_temp
			where thisweek=1 group by card_plan_id,udtype
		union all
			select card_plan_id,udtype,3 as rangetype,
			case when udtype=1 then sum(change_value) else abs(sum(change_value)) end as totalpoint
			from dw_rfm.b_point_change_analyze_temp
			where thismonth=1 group by card_plan_id,udtype
		union all
			select card_plan_id,udtype,4 as rangetype,
			case when udtype=1 then sum(change_value) else abs(sum(change_value)) end as totalpoint
			from dw_rfm.b_point_change_analyze_temp
			where last7day=1 group by card_plan_id,udtype
		union all
			select card_plan_id,udtype,5 as rangetype,
			case when udtype=1 then sum(change_value) else abs(sum(change_value)) end as totalpoint
			from dw_rfm.b_point_change_analyze_temp
			where last30day=1 group by card_plan_id,udtype
	) t
) re
group by re.card_plan_id,re.rangetype;


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
	and valid=1
	group by card_plan_id;


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
			select *,row_number() over (partition by r.card_plan_id,r.member_id order by r.change_time desc) as num
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
			select *,row_number() over (partition by r.card_plan_id,r.member_id order by r.change_time desc) as num
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
			select *,row_number() over (partition by r.card_plan_id,r.member_id order by r.change_time desc) as num
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
			select *,row_number() over (partition by r.card_plan_id,r.member_id order by r.change_time desc) as num
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
			select *,row_number() over (partition by r.card_plan_id,r.member_id order by r.change_time desc) as num
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
-- 获取所有的卡计划ID后进行左联即可
CREATE TABLE IF NOT EXISTS dw_rfm.`cix_online_member_point_grade_change`(
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
partitioned by(`part` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

insert overwrite table dw_rfm.`cix_online_member_point_grade_change` partition(part='${stat_date}')
select t.card_plan_id,t.rangetype,t.effect_points,
	if(d.members is null,0,d.members) as point_change_nums,
	if(e.send_points is null,0,e.send_points) as send_points,
	if(e.consume_points is null,0,e.consume_points) as consume_points,
	if(f.overdue_points is null,0,f.overdue_points) as overdue_points,
	if(h.newmembers is null,0,h.newmembers) as newmembers,
	if(g.upnums is null,0,g.upnums) as upnums,
	if(g.downnums is null,0,g.downnums) as downnums,
	'${stat_date}' as stat_date,
	${hiveconf:submitTime} as modified
from 
(
	select r.card_plan_id,r.effect_points,rangetype
	from (
		-- 列变行，补全时间类型
		select a.card_plan_id,if(b.effect_points is null,0,b.effect_points) as effect_points,split('1,2,3,4,5', ',') as timetype
		from 
		(	
			--获取所有卡
			select card_plan_id from dw_business.b_std_member_base_info group by card_plan_id
		) a
		left join dw_rfm.b_current_effect_points_temp b
		on a.card_plan_id=b.card_plan_id
	) r
	lateral view explode(r.timetype) adtable as rangetype
) t
left join dw_rfm.b_point_change_members_statics_temp d
on t.card_plan_id = d.card_plan_id and t.rangetype = d.rangetype
left join dw_rfm.b_point_change_statics_temp e
on t.card_plan_id = e.card_plan_id and t.rangetype=e.rangetype
left join dw_rfm.b_invalid_points_statics_temp f
on t.card_plan_id = f.card_plan_id and t.rangetype=f.rangetype
left join dw_rfm.b_new_member_statics_temp h
on t.card_plan_id = h.card_plan_id and t.rangetype=h.rangetype
left join dw_rfm.b_member_grade_statics_temp g
on t.card_plan_id = g.card_plan_id and t.rangetype=g.rangetype;


-- 分析等级变化矩阵，前等级--现等级 的二维矩阵

-- 等级变更表中计算各个时间段的等级变化情况
-- 需要从等级变更明细表中，计算出指定日期的会员现有等级和日期前一天的之前的最新等级对比

-- 1、计算昨天 oldgrade=-1标识非会员
drop table if exists dw_rfm.b_yestoday_member_grade_change_temp;
create table dw_rfm.b_yestoday_member_grade_change_temp as
select re.card_plan_id,re.nowgrade,re.oldgrade,count(re.member_id) members
from (
	select t.card_plan_id,t.member_id,t.grade as nowgrade,
		case when t.member_type=-1 then -1 --非会员
		when t1.oldgrade is null then t.grade --如果没有变动记录,使用当前等级
		else t1.oldgrade end as oldgrade --使用变动前原等级 
	from (
		select card_plan_id,member_id,grade,
		case when substr(created,1,10) >= ${hiveconf:yestoday} then -1 else 0 end as member_type --非会员标识
		from dw_business.b_std_member_base_info
	) t
	left join 
	(
		select r1.card_plan_id,r1.member_id,r1.grade_before_change as oldgrade from (
			select card_plan_id,member_id,grade_before_change,row_number() over (partition by card_plan_id,member_id order by change_time asc) as rank 
			from dw_business.b_member_grade_change
			where part = ${hiveconf:yestoday} and substr(change_time,1,10) = ${hiveconf:yestoday}
		) r1
		where r1.rank=1
	) t1
	on t.card_plan_id=t1.card_plan_id and t.member_id=t1.member_id
) re
group by re.card_plan_id,re.nowgrade,re.oldgrade;

-- 增加列合计计算
insert overwrite table dw_rfm.b_yestoday_member_grade_change_temp
select r.card_plan_id,r.nowgrade,r.oldgrade,r.members from dw_rfm.b_yestoday_member_grade_change_temp r
union all
select r1.card_plan_id,r1.nowgrade,99 as oldgrade,sum(r1.members) members from dw_rfm.b_yestoday_member_grade_change_temp r1
group by r1.card_plan_id,r1.nowgrade;

-- 增加行合计
insert overwrite table dw_rfm.b_yestoday_member_grade_change_temp
select r.card_plan_id,r.nowgrade,r.oldgrade,r.members from dw_rfm.b_yestoday_member_grade_change_temp r
union all
select r1.card_plan_id,99 as nowgrade,r1.oldgrade,sum(r1.members) members from dw_rfm.b_yestoday_member_grade_change_temp r1
group by r1.card_plan_id,r1.oldgrade;


-- 本周等级变化
drop table if exists dw_rfm.b_thisweek_member_grade_change_temp;
create table dw_rfm.b_thisweek_member_grade_change_temp as
select re.card_plan_id,re.nowgrade,re.oldgrade,count(re.member_id) members
from (
	select t.card_plan_id,t.member_id,t.grade as nowgrade,
		case when t.member_type=-1 then -1 --非会员
		when t1.oldgrade is null then t.grade --如果没有变动记录,使用当前等级
		else t1.oldgrade end as oldgrade --使用变动前原等级 
	from (
		select card_plan_id,member_id,grade,
		case when substr(created,1,10) >= ${hiveconf:weekfirst} then -1 else 0 end as member_type --非会员标识
		from dw_business.b_std_member_base_info
	) t
	left join 
	(
		select r1.card_plan_id,r1.member_id,r1.grade_before_change as oldgrade from (
			select card_plan_id,member_id,grade_before_change,row_number() over (partition by card_plan_id,member_id order by change_time asc) as rank 
			from dw_business.b_member_grade_change
			where part >= ${hiveconf:weekfirst} and substr(change_time,1,10) >= ${hiveconf:weekfirst}
		) r1
		where r1.rank=1
	) t1
	on t.card_plan_id=t1.card_plan_id and t.member_id=t1.member_id
) re
group by re.card_plan_id,re.nowgrade,re.oldgrade;

-- 增加列合计计算
insert overwrite table dw_rfm.b_thisweek_member_grade_change_temp
select r.card_plan_id,r.nowgrade,r.oldgrade,r.members from dw_rfm.b_thisweek_member_grade_change_temp r
union all
select r1.card_plan_id,r1.nowgrade,99 as oldgrade,sum(r1.members) members from dw_rfm.b_thisweek_member_grade_change_temp r1
group by r1.card_plan_id,r1.nowgrade;

-- 增加行合计
insert overwrite table dw_rfm.b_thisweek_member_grade_change_temp
select r.card_plan_id,r.nowgrade,r.oldgrade,r.members from dw_rfm.b_thisweek_member_grade_change_temp r
union all
select r1.card_plan_id,99 as nowgrade,r1.oldgrade,sum(r1.members) members from dw_rfm.b_thisweek_member_grade_change_temp r1
group by r1.card_plan_id,r1.oldgrade;

-- 本月等级变化
drop table if exists dw_rfm.b_thismonth_member_grade_change_temp;
create table dw_rfm.b_thismonth_member_grade_change_temp as
select re.card_plan_id,re.nowgrade,re.oldgrade,count(re.member_id) members
from (
	select t.card_plan_id,t.member_id,t.grade as nowgrade,
		case when t.member_type=-1 then -1 --非会员
		when t1.oldgrade is null then t.grade --如果没有变动记录,使用当前等级
		else t1.oldgrade end as oldgrade --使用变动前原等级 
	from (
		select card_plan_id,member_id,grade,
		case when substr(created,1,10) >= ${hiveconf:monthfirst} then -1 else 0 end as member_type --非会员标识
		from dw_business.b_std_member_base_info
	) t
	left join 
	(
		select r1.card_plan_id,r1.member_id,r1.grade_before_change as oldgrade 
		from (
			select card_plan_id,member_id,grade_before_change,row_number() over (partition by card_plan_id,member_id order by change_time asc) as rank 
			from dw_business.b_member_grade_change
			where part >= ${hiveconf:monthfirst} and substr(change_time,1,10) >= ${hiveconf:monthfirst}
		) r1
		where r1.rank=1
	) t1
	on t.card_plan_id=t1.card_plan_id and t.member_id=t1.member_id
) re
group by re.card_plan_id,re.nowgrade,re.oldgrade;

-- 增加列合计计算
insert overwrite table dw_rfm.b_thismonth_member_grade_change_temp
select r.card_plan_id,r.nowgrade,r.oldgrade,r.members from dw_rfm.b_thismonth_member_grade_change_temp r
union all
select r1.card_plan_id,r1.nowgrade,99 as oldgrade,sum(r1.members) members from dw_rfm.b_thismonth_member_grade_change_temp r1
group by r1.card_plan_id,r1.nowgrade;

-- 增加行合计
insert overwrite table dw_rfm.b_thismonth_member_grade_change_temp
select r.card_plan_id,r.nowgrade,r.oldgrade,r.members from dw_rfm.b_thismonth_member_grade_change_temp r
union all
select r1.card_plan_id,99 as nowgrade,r1.oldgrade,sum(r1.members) members from dw_rfm.b_thismonth_member_grade_change_temp r1
group by r1.card_plan_id,r1.oldgrade;

-- 近7天等级变化
drop table if exists dw_rfm.b_last7day_member_grade_change_temp;
create table dw_rfm.b_last7day_member_grade_change_temp as
select re.card_plan_id,re.nowgrade,re.oldgrade,count(re.member_id) members
from (
	select t.card_plan_id,t.member_id,t.grade as nowgrade,
		case when t.member_type=-1 then -1 --非会员
		when t1.oldgrade is null then t.grade --如果没有变动记录,使用当前等级
		else t1.oldgrade end as oldgrade --使用变动前原等级 
	from (
		select card_plan_id,member_id,grade,
		case when substr(created,1,10) >= ${hiveconf:last7day} then -1 else 0 end as member_type --非会员标识
		from dw_business.b_std_member_base_info
	) t
	left join 
	(
		select r1.card_plan_id,r1.member_id,r1.grade_before_change as oldgrade 
		from (
			select card_plan_id,member_id,grade_before_change,row_number() over (partition by card_plan_id,member_id order by change_time asc) as rank 
			from dw_business.b_member_grade_change
			where part >= ${hiveconf:last7day} and substr(change_time,1,10) >= ${hiveconf:last7day}
		) r1
		where r1.rank=1
	) t1
	on t.card_plan_id=t1.card_plan_id and t.member_id=t1.member_id
) re
group by re.card_plan_id,re.nowgrade,re.oldgrade;

-- 增加列合计计算
insert overwrite table dw_rfm.b_last7day_member_grade_change_temp
select r.card_plan_id,r.nowgrade,r.oldgrade,r.members from dw_rfm.b_last7day_member_grade_change_temp r
union all
select r1.card_plan_id,r1.nowgrade,99 as oldgrade,sum(r1.members) members from dw_rfm.b_last7day_member_grade_change_temp r1
group by r1.card_plan_id,r1.nowgrade;

-- 增加行合计
insert overwrite table dw_rfm.b_last7day_member_grade_change_temp
select r.card_plan_id,r.nowgrade,r.oldgrade,r.members from dw_rfm.b_last7day_member_grade_change_temp r
union all
select r1.card_plan_id,99 as nowgrade,r1.oldgrade,sum(r1.members) members from dw_rfm.b_last7day_member_grade_change_temp r1
group by r1.card_plan_id,r1.oldgrade;


-- 近30天等级变化
drop table if exists dw_rfm.b_last30day_member_grade_change_temp;
create table dw_rfm.b_last30day_member_grade_change_temp as
select re.card_plan_id,re.nowgrade,re.oldgrade,count(re.member_id) members
from (
	select t.card_plan_id,t.member_id,t.grade as nowgrade,
		case when t.member_type=-1 then -1 --非会员
		when t1.oldgrade is null then t.grade --如果没有变动记录,使用当前等级
		else t1.oldgrade end as oldgrade --使用变动前原等级 
	from (
		select card_plan_id,member_id,grade,
		case when substr(created,1,10) >= ${hiveconf:last30day} then -1 else 0 end as member_type --非会员标识
		from dw_business.b_std_member_base_info
	) t
	left join 
	(
		select r1.card_plan_id,r1.member_id,r1.grade_before_change as oldgrade 
		from (
			select card_plan_id,member_id,grade_before_change,row_number() over (partition by card_plan_id,member_id order by change_time asc) as rank 
			from dw_business.b_member_grade_change
			where part >= ${hiveconf:last30day} and substr(change_time,1,10) >= ${hiveconf:last30day}
		) r1
		where r1.rank=1
	) t1
	on t.card_plan_id=t1.card_plan_id and t.member_id=t1.member_id
) re
group by re.card_plan_id,re.nowgrade,re.oldgrade;

-- 增加列合计计算
insert overwrite table dw_rfm.b_last30day_member_grade_change_temp
select r.card_plan_id,r.nowgrade,r.oldgrade,r.members from dw_rfm.b_last30day_member_grade_change_temp r
union all
select r1.card_plan_id,r1.nowgrade,99 as oldgrade,sum(r1.members) members from dw_rfm.b_last30day_member_grade_change_temp r1
group by r1.card_plan_id,r1.nowgrade;

-- 增加行合计
insert overwrite table dw_rfm.b_last30day_member_grade_change_temp
select r.card_plan_id,r.nowgrade,r.oldgrade,r.members from dw_rfm.b_last30day_member_grade_change_temp r
union all
select r1.card_plan_id,99 as nowgrade,r1.oldgrade,sum(r1.members) members from dw_rfm.b_last30day_member_grade_change_temp r1
group by r1.card_plan_id,r1.oldgrade;


-- 对上面几个时间段的数据进行合并，生成最终的等级变化表,输出给业务端
CREATE TABLE IF NOT EXISTS dw_rfm.`cix_online_member_grade_transform`(
	`card_plan_id` string,
	`nowgrade` int, -- 现会员等级(99:行合计列,其他为等级数字)
	`oldgrade` int, -- 前会员等级(-1:非会员 99:列合计,其他为等级数字)
	`members` bigint, -- 会员人数
	`rangetype` int, -- 时间区间类型 1:昨日 2:本周 3:本月 4:近7天 5:近30天
	`stat_date` string,
	`modified` string
)
partitioned by(`part` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

insert overwrite table dw_rfm.cix_online_member_grade_transform partition(part='${stat_date}')
select r.card_plan_id,r.nowgrade,r.oldgrade,r.members,1 as rangetype,'${stat_date}' as stat_date,${hiveconf:submitTime} as modified 
from dw_rfm.b_yestoday_member_grade_change_temp r
union all
select r1.card_plan_id,r1.nowgrade,r1.oldgrade,r1.members,2 as rangetype,'${stat_date}' as stat_date,${hiveconf:submitTime} as modified 
from dw_rfm.b_thisweek_member_grade_change_temp r1
union all
select r2.card_plan_id,r2.nowgrade,r2.oldgrade,r2.members,3 as rangetype,'${stat_date}' as stat_date,${hiveconf:submitTime} as modified 
from dw_rfm.b_thismonth_member_grade_change_temp r2
union all
select r3.card_plan_id,r3.nowgrade,r3.oldgrade,r3.members,4 as rangetype,'${stat_date}' as stat_date,${hiveconf:submitTime} as modified 
from dw_rfm.b_last7day_member_grade_change_temp r3
union all
select r4.card_plan_id,r4.nowgrade,r4.oldgrade,r4.members,5 as rangetype,'${stat_date}' as stat_date,${hiveconf:submitTime} as modified 
from dw_rfm.b_last30day_member_grade_change_temp r4;

-- 删除中间临时表
drop table if exists dw_rfm.b_yestoday_member_grade_change_temp;
drop table if exists dw_rfm.b_thisweek_member_grade_change_temp;
drop table if exists dw_rfm.b_thismonth_member_grade_change_temp;
drop table if exists dw_rfm.b_last7day_member_grade_change_temp;
drop table if exists dw_rfm.b_last30day_member_grade_change_temp;

-- 需要对两个结果数据进行数据同步
-- cix_online_member_point_grade_change
-- cix_online_member_grade_transform










