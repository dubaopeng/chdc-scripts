SET mapred.job.name='tenant-member-point-grade-change-租户会员积分等级变更分析';

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
-- 有效积分：dw_business.b_member_efffect_point
-- 等级变更表：dw_business.b_member_grade_change
-- 积分变更表: dw_business.b_member_point_change

set preMonthEnd=date_sub(concat(substr('${stat_date}',0,7),'-01'),1);
set thisMonthEnd=add_months(${hiveconf:preMonthEnd},-${monthNum});

set submitTime=from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss');

--昨天
set yestoday=date_sub(${hiveconf:thisMonthEnd},1);
--本周的第一天
set weekfirst=date_sub(${hiveconf:thisMonthEnd},pmod(datediff(${hiveconf:thisMonthEnd}, concat(year(${hiveconf:thisMonthEnd}),'-01-01'))-6,7));
-- 本月的第一天
set monthfirst=date_sub(${hiveconf:thisMonthEnd},dayofmonth(${hiveconf:thisMonthEnd})-1);
-- 近七天
set last7day=date_sub(${hiveconf:thisMonthEnd},7);
-- 近30天
set last30day=date_sub(${hiveconf:thisMonthEnd},30);

--创建积分变更临时表
CREATE TABLE IF NOT EXISTS dw_rfm.`b_point_change_analyze_tenants`(
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
PARTITIONED BY(part string,stat_date string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");

-- 近30天有积分变化的数据，中间包含昨天，本周、本月、近7天
insert overwrite table dw_rfm.b_point_change_analyze_tenants partition(part='${tenant}',stat_date)
select a.card_plan_id,b.member_id,b.change_value,b.udtype,
	   b.yestoday,b.thisweek,b.thismonth,b.last7day,b.last30day,
	   ${hiveconf:thisMonthEnd} as stat_date
from (
	select card_plan_id from dw_rfm.b_tenant_card_plan_relation where tenant='${tenant}'
) a
left join(
	select card_plan_id,member_id,change_value,
		case when change_value >= 0 then 1 else -1 end as udtype,
		case when substr(change_time,1,10) >= ${hiveconf:yestoday} then 1 else 0 end as yestoday,
		case when substr(change_time,1,10) >= ${hiveconf:weekfirst} then 1 else 0 end as thisweek,
		case when substr(change_time,1,10) >= ${hiveconf:monthfirst} then 1 else 0 end as thismonth,
		case when substr(change_time,1,10) >= ${hiveconf:last7day} then 1 else 0 end as last7day,
		case when substr(change_time,1,10) >= ${hiveconf:last30day} then 1 else 0 end as last30day
	from dw_business.b_member_point_change
	where part >= ${hiveconf:last30day} and part <= ${hiveconf:thisMonthEnd}
	and substr(change_time,1,10) >= ${hiveconf:last30day} and substr(change_time,1,10)<=${hiveconf:thisMonthEnd}
) b
on a.card_plan_id=b.card_plan_id;

-- 计算每个时间段内有积分变动的会员数
CREATE TABLE IF NOT EXISTS dw_rfm.`b_point_change_members_statics_tenants`(
	`card_plan_id` string,
	`rangetype` int,
	`members` bigint
)
PARTITIONED BY(part string,stat_date string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");

insert overwrite table dw_rfm.`b_point_change_members_statics_tenants` partition(part='${tenant}',stat_date)
select r.card_plan_id,r.rangetype,r.members,${hiveconf:thisMonthEnd} as stat_date
from(
	select card_plan_id,1 as rangetype,count(distinct member_id) as members
	from dw_rfm.b_point_change_analyze_tenants
	where part='${tenant}' and stat_date=${hiveconf:thisMonthEnd} and yestoday=1 group by card_plan_id
	union all
	select card_plan_id,2 as rangetype,count(distinct member_id) as members
	from dw_rfm.b_point_change_analyze_tenants
	where part='${tenant}' and stat_date=${hiveconf:thisMonthEnd} and thisweek=1 group by card_plan_id
	union all
	select card_plan_id,3 as rangetype,count(distinct member_id) as members
	from dw_rfm.b_point_change_analyze_tenants
	where part='${tenant}' and stat_date=${hiveconf:thisMonthEnd} and thismonth=1
	group by card_plan_id
	union all
	select card_plan_id,4 as rangetype,count(distinct member_id) as members
	from dw_rfm.b_point_change_analyze_tenants
	where part='${tenant}' and stat_date=${hiveconf:thisMonthEnd} and last7day=1
	group by card_plan_id
	union all
	select card_plan_id,5 as rangetype,count(distinct member_id) as members
	from dw_rfm.b_point_change_analyze_tenants
	where part='${tenant}' and stat_date=${hiveconf:thisMonthEnd} and last30day=1
	group by card_plan_id
)r;

-- 计算发放和消耗的积分 udtype=1 发放 udtype=-1 消耗
CREATE TABLE IF NOT EXISTS dw_rfm.`b_point_change_statics_tenants`(
	`card_plan_id` string,
	`rangetype` int,
	`send_points` bigint,
	`consume_points` bigint
)
PARTITIONED BY(part string,stat_date string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");

insert overwrite table dw_rfm.`b_point_change_statics_tenants` partition(part='${tenant}',stat_date)
select re.card_plan_id,re.rangetype,sum(re.send_points) send_points,sum(re.consume_points) consume_points,
	  ${hiveconf:thisMonthEnd} as stat_date
from (
	select t.card_plan_id,t.rangetype,
		   case when t.udtype=1 then t.totalpoint else 0 end as send_points,
		   case when t.udtype=-1 then t.totalpoint else 0 end as consume_points
	from
	(
		select card_plan_id,udtype,1 as rangetype,
			case when udtype=1 then sum(change_value) else abs(sum(change_value)) end as totalpoint
			from dw_rfm.b_point_change_analyze_tenants
			where part='${tenant}' and stat_date=${hiveconf:thisMonthEnd} and yestoday=1 group by card_plan_id,udtype
		union all
			select card_plan_id,udtype,2 as rangetype,
			case when udtype=1 then sum(change_value) else abs(sum(change_value)) end as totalpoint
			from dw_rfm.b_point_change_analyze_tenants
			where part='${tenant}' and stat_date=${hiveconf:thisMonthEnd} and thisweek=1 group by card_plan_id,udtype
		union all
			select card_plan_id,udtype,3 as rangetype,
			case when udtype=1 then sum(change_value) else abs(sum(change_value)) end as totalpoint
			from dw_rfm.b_point_change_analyze_tenants
			where part='${tenant}' and stat_date=${hiveconf:thisMonthEnd} and thismonth=1 group by card_plan_id,udtype
		union all
			select card_plan_id,udtype,4 as rangetype,
			case when udtype=1 then sum(change_value) else abs(sum(change_value)) end as totalpoint
			from dw_rfm.b_point_change_analyze_tenants
			where part='${tenant}' and stat_date=${hiveconf:thisMonthEnd} and last7day=1 group by card_plan_id,udtype
		union all
			select card_plan_id,udtype,5 as rangetype,
			case when udtype=1 then sum(change_value) else abs(sum(change_value)) end as totalpoint
			from dw_rfm.b_point_change_analyze_tenants
			where part='${tenant}' and stat_date=${hiveconf:thisMonthEnd} and last30day=1 group by card_plan_id,udtype
	) t
) re
group by re.card_plan_id,re.rangetype;

-- 计算积分失效和当前有效积分
-- 积分失效：积分到期日期在统计周期内的积分
-- 有效积分存量：截至“数据截至日期”时有效积分

-- 近30天失效的积分计算
CREATE TABLE IF NOT EXISTS dw_rfm.`b_invalid_points_last30day_tenants`(
	`card_plan_id` string,
	`point` bigint,
	`yestoday` int,
	`thisweek` int,
	`thismonth` int,
	`last7day` int,
	`last30day` int
)
PARTITIONED BY(part string,stat_date string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");

insert overwrite table dw_rfm.b_invalid_points_last30day_tenants partition(part='${tenant}',stat_date)
select a.card_plan_id,b.point,b.yestoday,b.thisweek,b.thismonth,b.last7day,b.last30day,
	   ${hiveconf:thisMonthEnd} as stat_date
from (
	select card_plan_id from dw_rfm.b_tenant_card_plan_relation where tenant='${tenant}'
) a
left join(
	select card_plan_id,point,
		case when substr(overdue_date,1,10) >= ${hiveconf:yestoday} then 1 else 0 end as yestoday,
		case when substr(overdue_date,1,10) >= ${hiveconf:weekfirst} then 1 else 0 end as thisweek,
		case when substr(overdue_date,1,10) >= ${hiveconf:monthfirst} then 1 else 0 end as thismonth,
		case when substr(overdue_date,1,10) >= ${hiveconf:last7day} then 1 else 0 end as last7day,
		case when substr(overdue_date,1,10) >= ${hiveconf:last30day} then 1 else 0 end as last30day
	from dw_business.b_member_efffect_point
	where substr(overdue_date,1,10) <= ${hiveconf:thisMonthEnd}
	and substr(overdue_date,1,10) >= ${hiveconf:last30day}
	and valid=0
) b
on a.card_plan_id=b.card_plan_id;

-- 几个时间段的过期积分总数
CREATE TABLE IF NOT EXISTS dw_rfm.`b_invalid_points_statics_tenants`(
	`card_plan_id` string,
	`rangetype` int,
	`overdue_points` bigint
)
PARTITIONED BY(part string,stat_date string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");

insert overwrite table dw_rfm.`b_invalid_points_statics_tenants` partition(part='${tenant}',stat_date)
select r.card_plan_id,r.rangetype,r.overdue_points,${hiveconf:thisMonthEnd} as stat_date
from (
	select card_plan_id,1 as rangetype,sum(point) as overdue_points
		from dw_rfm.b_invalid_points_last30day_tenants
		where part='${tenant}' and stat_date=${hiveconf:thisMonthEnd} and yestoday=1 group by card_plan_id
	union all
		select card_plan_id,2 as rangetype,sum(point) as overdue_points
		from dw_rfm.b_invalid_points_last30day_tenants
		where part='${tenant}' and stat_date=${hiveconf:thisMonthEnd} and thisweek=1 group by card_plan_id
	union all
		select card_plan_id,3 as rangetype,sum(point) as overdue_points
		from dw_rfm.b_invalid_points_last30day_tenants
		where part='${tenant}' and stat_date=${hiveconf:thisMonthEnd} and thismonth=1 group by card_plan_id
	union all
		select card_plan_id,4 as rangetype,sum(point) as overdue_points
		from dw_rfm.b_invalid_points_last30day_tenants
		where part='${tenant}' and stat_date=${hiveconf:thisMonthEnd} and last7day=1 group by card_plan_id
	union all
		select card_plan_id,5 as rangetype,sum(point) as overdue_points
		from dw_rfm.b_invalid_points_last30day_tenants
		where part='${tenant}' and stat_date=${hiveconf:thisMonthEnd} and last30day=1 group by card_plan_id
)r;

-- 当前有效积分存量,不随时间变化,只有最新当前一份数据
CREATE TABLE IF NOT EXISTS dw_rfm.`b_current_effect_points_tenants`(
	`card_plan_id` string,
	`effect_points` bigint
)
PARTITIONED BY(part string,stat_date string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");

insert overwrite table dw_rfm.`b_current_effect_points_tenants` partition(part='${tenant}',stat_date)
select t.card_plan_id,sum(t.point) effect_points,${hiveconf:thisMonthEnd} as stat_date
from(
	select a.card_plan_id,b.point
	from (
		select card_plan_id from dw_rfm.b_tenant_card_plan_relation where tenant='${tenant}'
	) a
	left join 
	(
		select card_plan_id,point
		from dw_business.b_member_efffect_point
		where substr(effective_date,1,10) <= ${hiveconf:thisMonthEnd}
		and substr(overdue_date,1,10) > ${hiveconf:thisMonthEnd}
		and valid=1
	) b
	on a.card_plan_id=b.card_plan_id
) t
group by t.card_plan_id;
	

-- 从会员基本信息中查询新增的会员
CREATE TABLE IF NOT EXISTS dw_rfm.`b_new_member_last30day_tenants`(
	`card_plan_id` string,
	`member_id` string,
	`yestoday` int,
	`thisweek` int,
	`thismonth` int,
	`last7day` int,
	`last30day` int
)
PARTITIONED BY(part string,stat_date string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");

insert overwrite table dw_rfm.`b_new_member_last30day_tenants` partition(part='${tenant}',stat_date)
select a.card_plan_id,b.member_id,b.yestoday,b.thisweek,b.thismonth,b.last7day,b.last30day,
	   ${hiveconf:thisMonthEnd} as stat_date
from (
	select card_plan_id from dw_rfm.b_tenant_card_plan_relation where tenant='${tenant}'
) a
left join(
	select card_plan_id,member_id,
		case when substr(created,1,10) >= ${hiveconf:yestoday} then 1 else 0 end as yestoday,
		case when substr(created,1,10) >= ${hiveconf:weekfirst} then 1 else 0 end as thisweek,
		case when substr(created,1,10) >= ${hiveconf:monthfirst} then 1 else 0 end as thismonth,
		case when substr(created,1,10) >= ${hiveconf:last7day} then 1 else 0 end as last7day,
		case when substr(created,1,10) >= ${hiveconf:last30day} then 1 else 0 end as last30day
	from dw_business.b_std_member_base_info
	where substr(created,1,10) >= ${hiveconf:last30day}
)b
on a.card_plan_id=b.card_plan_id;

-- 各时间段新增会员的统计结果
CREATE TABLE IF NOT EXISTS dw_rfm.`b_new_member_statics_tenants`(
	`card_plan_id` string,
	`rangetype` int,
	`newmembers` bigint
)
PARTITIONED BY(part string,stat_date string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");

insert overwrite table dw_rfm.`b_new_member_statics_tenants` partition(part='${tenant}',stat_date)
select r.card_plan_id,r.rangetype,r.newmembers,${hiveconf:thisMonthEnd} as stat_date
from (
	select card_plan_id,1 as rangetype,count(member_id) as newmembers
		from dw_rfm.b_new_member_last30day_tenants
		where part='${tenant}' and stat_date=${hiveconf:thisMonthEnd} and yestoday=1 group by card_plan_id
	union all
		select card_plan_id,2 as rangetype,count(member_id) as newmembers
		from dw_rfm.b_new_member_last30day_tenants
		where part='${tenant}' and stat_date=${hiveconf:thisMonthEnd} and thisweek=1 group by card_plan_id
	union all
		select card_plan_id,3 as rangetype,count(member_id) as newmembers
		from dw_rfm.b_new_member_last30day_tenants
		where part='${tenant}' and stat_date=${hiveconf:thisMonthEnd} and thismonth=1 group by card_plan_id
	union all
		select card_plan_id,4 as rangetype,count(member_id) as newmembers
		from dw_rfm.b_new_member_last30day_tenants
		where part='${tenant}' and stat_date=${hiveconf:thisMonthEnd} and last7day=1 group by card_plan_id
	union all
		select card_plan_id,5 as rangetype,count(member_id) as newmembers
		from dw_rfm.b_new_member_last30day_tenants
		where part='${tenant}' and stat_date=${hiveconf:thisMonthEnd} and last30day=1 group by card_plan_id
) r;

-- 计算会员等级变更数量
CREATE TABLE IF NOT EXISTS dw_rfm.`b_member_grade_statics_tenants`(
	`card_plan_id` string,
	`rangetype` int,
	`upnums` bigint,
	`downnums` bigint
)
PARTITIONED BY(part string,stat_date string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");

insert overwrite table dw_rfm.`b_member_grade_statics_tenants` partition(part='${tenant}',stat_date)
select re.card_plan_id,re.rangetype,sum(re.upnums) as upnums,sum(re.downnums) as downnums,${hiveconf:thisMonthEnd} as stat_date
from(
	select t1.card_plan_id,t1.rangetype,
	case when lower(t1.change_type)='up' then t1.members else 0 end as upnums,
	case when lower(t1.change_type)='down' then t1.members else 0 end as downnums
	from
	(
		select re.card_plan_id,re.change_type,1 as rangetype,count(re.member_id) members
		from(
			select *,row_number() over (partition by r.card_plan_id,r.member_id order by r.change_time desc) as num
			from(
				select a.card_plan_id,b.member_id,b.change_type,b.change_time
				from (
					select card_plan_id from dw_rfm.b_tenant_card_plan_relation where tenant='${tenant}'
				) a
				left join(
					select t.card_plan_id,t.member_id,t.change_type,t.change_time
						from dw_business.b_member_grade_change t
					where t.part = ${hiveconf:yestoday}
						and substr(t.change_time,1,10) = ${hiveconf:yestoday}
						and (lower(t.change_type) = 'up' or lower(t.change_type) = 'down')
				)b
				on a.card_plan_id=b.card_plan_id
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
				select a.card_plan_id,b.member_id,b.change_type,b.change_time
				from (
					select card_plan_id from dw_rfm.b_tenant_card_plan_relation where tenant='${tenant}'
				) a
				left join(
					select t.card_plan_id,t.member_id,t.change_type,t.change_time
						from dw_business.b_member_grade_change t
					where t.part >= ${hiveconf:weekfirst}
						and substr(t.change_time,1,10) >= ${hiveconf:weekfirst}
						and (lower(t.change_type) = 'up' or lower(t.change_type) = 'down')
				)b
				on a.card_plan_id=b.card_plan_id
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
				select a.card_plan_id,b.member_id,b.change_type,b.change_time
				from (
					select card_plan_id from dw_rfm.b_tenant_card_plan_relation where tenant='${tenant}'
				) a
				left join(
					select t.card_plan_id,t.member_id,t.change_type,t.change_time
						from dw_business.b_member_grade_change t
					where t.part >= ${hiveconf:monthfirst}
						and substr(t.change_time,1,10) >= ${hiveconf:monthfirst}
						and (lower(t.change_type) = 'up' or lower(t.change_type) = 'down')
				)b
				on a.card_plan_id=b.card_plan_id
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
				select a.card_plan_id,b.member_id,b.change_type,b.change_time
				from (
					select card_plan_id from dw_rfm.b_tenant_card_plan_relation where tenant='${tenant}'
				) a
				left join(
					select t.card_plan_id,t.member_id,t.change_type,t.change_time
						from dw_business.b_member_grade_change t
					where t.part >= ${hiveconf:last7day}
						and substr(t.change_time,1,10) >= ${hiveconf:last7day}
						and (lower(t.change_type) = 'up' or lower(t.change_type) = 'down')
				)b
				on a.card_plan_id=b.card_plan_id
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
				select a.card_plan_id,b.member_id,b.change_type,b.change_time
				from (
					select card_plan_id from dw_rfm.b_tenant_card_plan_relation where tenant='${tenant}'
				) a
				left join(
					select t.card_plan_id,t.member_id,t.change_type,t.change_time
						from dw_business.b_member_grade_change t
					where t.part >= ${hiveconf:last30day}
						and substr(t.change_time,1,10) >= ${hiveconf:last30day}
						and (lower(t.change_type) = 'up' or lower(t.change_type) = 'down')
				)b
				on a.card_plan_id=b.card_plan_id
			) r
		) re
		where re.num=1
		group by re.card_plan_id,re.change_type
	) t1
) re
group by re.card_plan_id,re.rangetype;


-- 将上面几类统计数据进行合并，整理成结果表输出给业务
-- 获取所有的卡计划ID后进行左联即可
CREATE TABLE IF NOT EXISTS dw_rfm.`b_member_point_grade_change_tenants`(
	`card_plan_id` string,
	`rangetype` int,
	`effect_points` bigint,
	`point_change_nums` bigint,
	`send_points` bigint,
	`consume_points` bigint,
	`overdue_points` bigint,
	`newmembers` bigint,
	`upnums` bigint,
	`downnums` bigint
)
PARTITIONED BY(part string,stat_date string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");

insert overwrite table dw_rfm.`b_member_point_grade_change_tenants` partition(part='${tenant}',stat_date)
select t.card_plan_id,t.rangetype,t.effect_points,
	if(d.members is null,0,d.members) as point_change_nums,
	if(e.send_points is null,0,e.send_points) as send_points,
	if(e.consume_points is null,0,e.consume_points) as consume_points,
	if(f.overdue_points is null,0,f.overdue_points) as overdue_points,
	if(h.newmembers is null,0,h.newmembers) as newmembers,
	if(g.upnums is null,0,g.upnums) as upnums,
	if(g.downnums is null,0,g.downnums) as downnums,
	${hiveconf:thisMonthEnd} as stat_date
from 
(
	select r.card_plan_id,r.effect_points,rangetype
	from (
		select a.card_plan_id,if(b.effect_points is null,0,b.effect_points) as effect_points,split('1,2,3,4,5', ',') as timetype
		from 
		(	
			select card_plan_id from dw_rfm.b_tenant_card_plan_relation where tenant='${tenant}'
		) a
		left join 
		(select * from dw_rfm.b_current_effect_points_tenants where part='${tenant}' and stat_date=${hiveconf:thisMonthEnd}) b
		on a.card_plan_id=b.card_plan_id
	) r
	lateral view explode(r.timetype) adtable as rangetype
) t
left join (select * from dw_rfm.b_point_change_members_statics_tenants where part='${tenant}' and stat_date=${hiveconf:thisMonthEnd}) d
on t.card_plan_id = d.card_plan_id and t.rangetype = d.rangetype
left join (select * from dw_rfm.b_point_change_statics_tenants where part='${tenant}' and stat_date=${hiveconf:thisMonthEnd}) e
on t.card_plan_id = e.card_plan_id and t.rangetype=e.rangetype
left join (select * from dw_rfm.b_invalid_points_statics_tenants where part='${tenant}' and stat_date=${hiveconf:thisMonthEnd}) f
on t.card_plan_id = f.card_plan_id and t.rangetype=f.rangetype
left join (select * from dw_rfm.b_new_member_statics_tenants where part='${tenant}' and stat_date=${hiveconf:thisMonthEnd}) h
on t.card_plan_id = h.card_plan_id and t.rangetype=h.rangetype
left join (select * from dw_rfm.b_member_grade_statics_tenants where part='${tenant}' and stat_date=${hiveconf:thisMonthEnd}) g
on t.card_plan_id = g.card_plan_id and t.rangetype=g.rangetype;

-- 分析等级变化矩阵，前等级--现等级 的二维矩阵
-- 等级变更表中计算各个时间段的等级变化情况
-- 需要从等级变更明细表中，计算出指定日期的会员现有等级和日期前一天的之前的最新等级对比

-- 1、计算昨天 oldgrade=-1标识非会员
CREATE TABLE IF NOT EXISTS dw_rfm.`b_yestoday_member_grade_change_tenants`(
	`card_plan_id` string,
	`nowgrade` int,
	`oldgrade` int,
	`members` bigint
)
PARTITIONED BY(part string,stat_date string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");

insert overwrite table dw_rfm.`b_yestoday_member_grade_change_tenants` partition(part='${tenant}',stat_date)
select re.card_plan_id,re.nowgrade,re.oldgrade,count(re.member_id) members,${hiveconf:thisMonthEnd} as stat_date
from (
	select t.card_plan_id,t.member_id,t.grade as nowgrade,
		case when t.member_type=-1 then -1 
		when t1.oldgrade is null then t.grade 
		else t1.oldgrade end as oldgrade 
	from (
		select a.card_plan_id,b.member_id,if(b.grade is null,-1,b.grade) as grade,b.member_type
		from 
		(	
			select card_plan_id from dw_rfm.b_tenant_card_plan_relation where tenant='${tenant}'
		) a
		left join(
			select card_plan_id,member_id,grade,
			case when substr(created,1,10) >= ${hiveconf:yestoday} then -1 else 0 end as member_type 
			from dw_business.b_std_member_base_info
		) b
		on a.card_plan_id=b.card_plan_id
	) t
	left join 
	(
		select r1.card_plan_id,r1.member_id,if(r1.grade_before_change is null,-1,r1.grade_before_change) as oldgrade 
		from (
			select a.card_plan_id,b.member_id,b.grade_before_change,row_number() over (partition by a.card_plan_id,b.member_id order by b.change_time asc) as rank 
			from (
				select card_plan_id from dw_rfm.b_tenant_card_plan_relation where tenant='${tenant}'
			) a
			left join(
				select card_plan_id,member_id,grade_before_change,change_time
				from dw_business.b_member_grade_change
				where part = ${hiveconf:yestoday} and substr(change_time,1,10) = ${hiveconf:yestoday}
			) b
			on a.card_plan_id=b.card_plan_id
		) r1
		where r1.rank=1
	) t1
	on t.card_plan_id=t1.card_plan_id and t.member_id=t1.member_id
) re
group by re.card_plan_id,re.nowgrade,re.oldgrade;

-- 增加列合计计算
insert overwrite table dw_rfm.b_yestoday_member_grade_change_tenants partition(part='${tenant}',stat_date)
select re.card_plan_id,re.nowgrade,re.oldgrade,re.members,${hiveconf:thisMonthEnd} as stat_date
from (
	select r.card_plan_id,r.nowgrade,r.oldgrade,r.members 
	from dw_rfm.b_yestoday_member_grade_change_tenants r where r.part='${tenant}' and r.stat_date=${hiveconf:thisMonthEnd}
	union all
	select r1.card_plan_id,r1.nowgrade,99 as oldgrade,sum(r1.members) members 
	from dw_rfm.b_yestoday_member_grade_change_tenants r1 where r1.part='${tenant}' and r1.stat_date=${hiveconf:thisMonthEnd}
	group by r1.card_plan_id,r1.nowgrade
)re;

-- 增加行合计
insert overwrite table dw_rfm.b_yestoday_member_grade_change_tenants partition(part='${tenant}',stat_date)
select re.card_plan_id,re.nowgrade,re.oldgrade,re.members,${hiveconf:thisMonthEnd} as stat_date
from(
	select r.card_plan_id,r.nowgrade,r.oldgrade,r.members 
	from dw_rfm.b_yestoday_member_grade_change_tenants r where r.part='${tenant}' and r.stat_date=${hiveconf:thisMonthEnd}
	union all
	select r1.card_plan_id,99 as nowgrade,r1.oldgrade,sum(r1.members) members 
	from dw_rfm.b_yestoday_member_grade_change_tenants r1 where r1.part='${tenant}' and r1.stat_date=${hiveconf:thisMonthEnd}
	group by r1.card_plan_id,r1.oldgrade
)re;

-- 本周等级变化
CREATE TABLE IF NOT EXISTS dw_rfm.`b_thisweek_member_grade_change_tenants`(
	`card_plan_id` string,
	`nowgrade` int,
	`oldgrade` int,
	`members` bigint
)
PARTITIONED BY(part string,stat_date string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");

insert overwrite table dw_rfm.b_thisweek_member_grade_change_tenants partition(part='${tenant}',stat_date)
select re.card_plan_id,re.nowgrade,re.oldgrade,count(re.member_id) members,${hiveconf:thisMonthEnd} as stat_date
from (
	select t.card_plan_id,t.member_id,t.grade as nowgrade,
		case when t.member_type=-1 then -1 
		when t1.oldgrade is null then t.grade 
		else t1.oldgrade end as oldgrade 
	from (
		select a.card_plan_id,b.member_id,if(b.grade is null,-1,b.grade) as grade,b.member_type
		from 
		(	
			select card_plan_id from dw_rfm.b_tenant_card_plan_relation where tenant='${tenant}'
		) a
		left join(
			select card_plan_id,member_id,grade,
			case when substr(created,1,10) >= ${hiveconf:weekfirst} then -1 else 0 end as member_type 
			from dw_business.b_std_member_base_info
		) b
		on a.card_plan_id=b.card_plan_id
	) t
	left join 
	(
		select r1.card_plan_id,r1.member_id,if(r1.grade_before_change is null,-1,r1.grade_before_change) as oldgrade 
		from (
			select a.card_plan_id,b.member_id,b.grade_before_change,row_number() over (partition by a.card_plan_id,b.member_id order by b.change_time asc) as rank 
			from (
				select card_plan_id from dw_rfm.b_tenant_card_plan_relation where tenant='${tenant}'
			) a
			left join(
				select card_plan_id,member_id,grade_before_change,change_time
				from dw_business.b_member_grade_change
				where part >= ${hiveconf:weekfirst} and substr(change_time,1,10) >= ${hiveconf:weekfirst}
			) b
			on a.card_plan_id=b.card_plan_id
		) r1
		where r1.rank=1
	) t1
	on t.card_plan_id=t1.card_plan_id and t.member_id=t1.member_id
) re
group by re.card_plan_id,re.nowgrade,re.oldgrade;

-- 增加列合计计算
insert overwrite table dw_rfm.b_thisweek_member_grade_change_tenants partition(part='${tenant}',stat_date)
select re.card_plan_id,re.nowgrade,re.oldgrade,re.members,${hiveconf:thisMonthEnd} as stat_date
from(
	select r.card_plan_id,r.nowgrade,r.oldgrade,r.members 
	from dw_rfm.b_thisweek_member_grade_change_tenants r where r.part='${tenant}' and r.stat_date=${hiveconf:thisMonthEnd}
	union all
	select r1.card_plan_id,r1.nowgrade,99 as oldgrade,sum(r1.members) members 
	from dw_rfm.b_thisweek_member_grade_change_tenants r1 where r1.part='${tenant}' and r1.stat_date=${hiveconf:thisMonthEnd}
	group by r1.card_plan_id,r1.nowgrade
)re;

-- 增加行合计
insert overwrite table dw_rfm.b_thisweek_member_grade_change_tenants partition(part='${tenant}',stat_date)
select re.card_plan_id,re.nowgrade,re.oldgrade,re.members,${hiveconf:thisMonthEnd} as stat_date 
from (
	select r.card_plan_id,r.nowgrade,r.oldgrade,r.members 
		from dw_rfm.b_thisweek_member_grade_change_tenants r where r.part='${tenant}' and r.stat_date=${hiveconf:thisMonthEnd}
	union all
	select r1.card_plan_id,99 as nowgrade,r1.oldgrade,sum(r1.members) members 
		from dw_rfm.b_thisweek_member_grade_change_tenants r1 where r1.part='${tenant}' and r1.stat_date=${hiveconf:thisMonthEnd}
	group by r1.card_plan_id,r1.oldgrade
)re;

-- 本月等级变化
CREATE TABLE IF NOT EXISTS dw_rfm.`b_thismonth_member_grade_change_tenants`(
	`card_plan_id` string,
	`nowgrade` int,
	`oldgrade` int,
	`members` bigint
)
PARTITIONED BY(part string,stat_date string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");

insert overwrite table dw_rfm.b_thismonth_member_grade_change_tenants partition(part='${tenant}',stat_date)
select re.card_plan_id,re.nowgrade,re.oldgrade,count(re.member_id) members,${hiveconf:thisMonthEnd} as stat_date
from (
	select t.card_plan_id,t.member_id,t.grade as nowgrade,
		case when t.member_type=-1 then -1 
		when t1.oldgrade is null then t.grade 
		else t1.oldgrade end as oldgrade 
	from (
		select a.card_plan_id,b.member_id,if(b.grade is null,-1,b.grade) as grade,b.member_type
		from 
		(	
			select card_plan_id from dw_rfm.b_tenant_card_plan_relation where tenant='${tenant}'
		) a
		left join(
			select card_plan_id,member_id,grade,
			case when substr(created,1,10) >= ${hiveconf:monthfirst} then -1 else 0 end as member_type 
			from dw_business.b_std_member_base_info
		) b
		on a.card_plan_id=b.card_plan_id
	) t
	left join 
	(
		select r1.card_plan_id,r1.member_id,if(r1.grade_before_change is null,-1,r1.grade_before_change) as oldgrade
		from (
			select a.card_plan_id,b.member_id,b.grade_before_change,row_number() over (partition by a.card_plan_id,b.member_id order by b.change_time asc) as rank 
			from (
				select card_plan_id from dw_rfm.b_tenant_card_plan_relation where tenant='${tenant}'
			) a
			left join(
				select card_plan_id,member_id,grade_before_change,change_time
				from dw_business.b_member_grade_change
				where part >= ${hiveconf:monthfirst} and substr(change_time,1,10) >= ${hiveconf:monthfirst}
			) b
			on a.card_plan_id=b.card_plan_id
		) r1
		where r1.rank=1
	) t1
	on t.card_plan_id=t1.card_plan_id and t.member_id=t1.member_id
) re
group by re.card_plan_id,re.nowgrade,re.oldgrade;

-- 增加列合计计算
insert overwrite table dw_rfm.b_thismonth_member_grade_change_tenants partition(part='${tenant}',stat_date)
select re.card_plan_id,re.nowgrade,re.oldgrade,re.members,${hiveconf:thisMonthEnd} as stat_date
from(
	select r.card_plan_id,r.nowgrade,r.oldgrade,r.members 
	from dw_rfm.b_thismonth_member_grade_change_tenants r where r.part='${tenant}' and r.stat_date=${hiveconf:thisMonthEnd}
	union all
	select r1.card_plan_id,r1.nowgrade,99 as oldgrade,sum(r1.members) members 
	from dw_rfm.b_thismonth_member_grade_change_tenants r1 where r1.part='${tenant}' and r1.stat_date=${hiveconf:thisMonthEnd}
	group by r1.card_plan_id,r1.nowgrade
)re;

-- 增加行合计
insert overwrite table dw_rfm.b_thismonth_member_grade_change_tenants partition(part='${tenant}',stat_date)
select re.card_plan_id,re.nowgrade,re.oldgrade,re.members,${hiveconf:thisMonthEnd} as stat_date
from(
	select r.card_plan_id,r.nowgrade,r.oldgrade,r.members 
	from dw_rfm.b_thismonth_member_grade_change_tenants r where r.part='${tenant}' and r.stat_date=${hiveconf:thisMonthEnd}
	union all
	select r1.card_plan_id,99 as nowgrade,r1.oldgrade,sum(r1.members) members 
	from dw_rfm.b_thismonth_member_grade_change_tenants r1 where r1.part='${tenant}' and r1.stat_date=${hiveconf:thisMonthEnd}
	group by r1.card_plan_id,r1.oldgrade
)re;

-- 近7天等级变化
CREATE TABLE IF NOT EXISTS dw_rfm.`b_last7day_member_grade_change_tenants`(
	`card_plan_id` string,
	`nowgrade` int,
	`oldgrade` int,
	`members` bigint
)
PARTITIONED BY(part string,stat_date string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");

insert overwrite table dw_rfm.b_last7day_member_grade_change_tenants partition(part='${tenant}',stat_date)
select re.card_plan_id,re.nowgrade,re.oldgrade,count(re.member_id) members,${hiveconf:thisMonthEnd} as stat_date
from (
	select t.card_plan_id,t.member_id,t.grade as nowgrade,
		case when t.member_type=-1 then -1 
		when t1.oldgrade is null then t.grade 
		else t1.oldgrade end as oldgrade  
	from (
		select a.card_plan_id,b.member_id,if(b.grade is null,-1,b.grade) as grade,b.member_type
		from 
		(	
			select card_plan_id from dw_rfm.b_tenant_card_plan_relation where tenant='${tenant}'
		) a
		left join(
			select card_plan_id,member_id,grade,
			case when substr(created,1,10) >= ${hiveconf:last7day} then -1 else 0 end as member_type
			from dw_business.b_std_member_base_info
		) b
		on a.card_plan_id=b.card_plan_id
	) t
	left join 
	(
		select r1.card_plan_id,r1.member_id,if(r1.grade_before_change is null,-1,r1.grade_before_change) as oldgrade
		from (
			select a.card_plan_id,b.member_id,b.grade_before_change,row_number() over (partition by a.card_plan_id,b.member_id order by b.change_time asc) as rank 
			from (
				select card_plan_id from dw_rfm.b_tenant_card_plan_relation where tenant='${tenant}'
			) a
			left join(
				select card_plan_id,member_id,grade_before_change,change_time
				from dw_business.b_member_grade_change
				where part >= ${hiveconf:last7day} and substr(change_time,1,10) >= ${hiveconf:last7day}
			) b
			on a.card_plan_id=b.card_plan_id			
		) r1
		where r1.rank=1
	) t1
	on t.card_plan_id=t1.card_plan_id and t.member_id=t1.member_id
) re
group by re.card_plan_id,re.nowgrade,re.oldgrade;

-- 增加列合计计算
insert overwrite table dw_rfm.b_last7day_member_grade_change_tenants partition(part='${tenant}',stat_date)
select re.card_plan_id,re.nowgrade,re.oldgrade,re.members,${hiveconf:thisMonthEnd} as stat_date
from(
	select r.card_plan_id,r.nowgrade,r.oldgrade,r.members 
	from dw_rfm.b_last7day_member_grade_change_tenants r where r.part='${tenant}' and r.stat_date=${hiveconf:thisMonthEnd}
	union all
	select r1.card_plan_id,r1.nowgrade,99 as oldgrade,sum(r1.members) members 
	from dw_rfm.b_last7day_member_grade_change_tenants r1 where r1.part='${tenant}' and r1.stat_date=${hiveconf:thisMonthEnd}
	group by r1.card_plan_id,r1.nowgrade
)re;

-- 增加行合计
insert overwrite table dw_rfm.b_last7day_member_grade_change_tenants partition(part='${tenant}',stat_date)
select re.card_plan_id,re.nowgrade,re.oldgrade,re.members,${hiveconf:thisMonthEnd} as stat_date
from(
	select r.card_plan_id,r.nowgrade,r.oldgrade,r.members 
	from dw_rfm.b_last7day_member_grade_change_tenants r where r.part='${tenant}' and r.stat_date=${hiveconf:thisMonthEnd}
	union all
	select r1.card_plan_id,99 as nowgrade,r1.oldgrade,sum(r1.members) members 
	from dw_rfm.b_last7day_member_grade_change_tenants r1 where r1.part='${tenant}' and r1.stat_date=${hiveconf:thisMonthEnd}
	group by r1.card_plan_id,r1.oldgrade
)re;

-- 近30天等级变化
CREATE TABLE IF NOT EXISTS dw_rfm.`b_last30day_member_grade_change_tenants`(
	`card_plan_id` string,
	`nowgrade` int,
	`oldgrade` int,
	`members` bigint
)
PARTITIONED BY(part string,stat_date string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");

insert overwrite table dw_rfm.b_last30day_member_grade_change_tenants partition(part='${tenant}',stat_date)
select re.card_plan_id,re.nowgrade,re.oldgrade,count(re.member_id) members,${hiveconf:thisMonthEnd} as stat_date
from (
	select t.card_plan_id,t.member_id,t.grade as nowgrade,
		case when t.member_type=-1 then -1
		when t1.oldgrade is null then t.grade 
		else t1.oldgrade end as oldgrade 
	from (
		select a.card_plan_id,b.member_id,if(b.grade is null,-1,b.grade) as grade,b.member_type
		from 
		(	
			select card_plan_id from dw_rfm.b_tenant_card_plan_relation where tenant='${tenant}'
		) a
		left join(
			select card_plan_id,member_id,grade,
			case when substr(created,1,10) >= ${hiveconf:last30day} then -1 else 0 end as member_type 
			from dw_business.b_std_member_base_info
		) b
		on a.card_plan_id=b.card_plan_id
	) t
	left join 
	(
		select r1.card_plan_id,r1.member_id,if(r1.grade_before_change is null,-1,r1.grade_before_change) as oldgrade
		from (
			select a.card_plan_id,b.member_id,b.grade_before_change,row_number() over (partition by a.card_plan_id,b.member_id order by b.change_time asc) as rank 
			from (
				select card_plan_id from dw_rfm.b_tenant_card_plan_relation where tenant='${tenant}'
			) a
			left join(
				select card_plan_id,member_id,grade_before_change,change_time
				from dw_business.b_member_grade_change
				where part >= ${hiveconf:last30day} and substr(change_time,1,10) >= ${hiveconf:last30day}
			) b
			on a.card_plan_id=b.card_plan_id			
		) r1
		where r1.rank=1
	) t1
	on t.card_plan_id=t1.card_plan_id and t.member_id=t1.member_id
) re
group by re.card_plan_id,re.nowgrade,re.oldgrade;

-- 增加列合计计算
insert overwrite table dw_rfm.b_last30day_member_grade_change_tenants partition(part='${tenant}',stat_date)
select re.card_plan_id,re.nowgrade,re.oldgrade,re.members,${hiveconf:thisMonthEnd} as stat_date
from(
	select r.card_plan_id,r.nowgrade,r.oldgrade,r.members 
	from dw_rfm.b_last30day_member_grade_change_tenants r where r.part='${tenant}' and r.stat_date=${hiveconf:thisMonthEnd}
	union all
	select r1.card_plan_id,r1.nowgrade,99 as oldgrade,sum(r1.members) members 
	from dw_rfm.b_last30day_member_grade_change_tenants r1 where r1.part='${tenant}' and r1.stat_date=${hiveconf:thisMonthEnd}
	group by r1.card_plan_id,r1.nowgrade
)re;

-- 增加行合计
insert overwrite table dw_rfm.b_last30day_member_grade_change_tenants partition(part='${tenant}',stat_date)
select re.card_plan_id,re.nowgrade,re.oldgrade,re.members,${hiveconf:thisMonthEnd} as part
from(
	select r.card_plan_id,r.nowgrade,r.oldgrade,r.members 
	from dw_rfm.b_last30day_member_grade_change_tenants r where r.part='${tenant}' and r.stat_date=${hiveconf:thisMonthEnd}
	union all
	select r1.card_plan_id,99 as nowgrade,r1.oldgrade,sum(r1.members) members 
	from dw_rfm.b_last30day_member_grade_change_tenants r1 where r1.part='${tenant}' and r1.stat_date=${hiveconf:thisMonthEnd}
	group by r1.card_plan_id,r1.oldgrade
)re;

-- 对上面几个时间段的数据进行合并，生成最终的等级变化表,输出给业务端
CREATE TABLE IF NOT EXISTS dw_rfm.`b_member_grade_transform_tenants`(
	`card_plan_id` string,
	`nowgrade` int, 
	`oldgrade` int, 
	`members` bigint,
	`rangetype` int 
)
partitioned by(`part` string,stat_date string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");

insert overwrite table dw_rfm.b_member_grade_transform_tenants partition(part='${tenant}',stat_date)
select r.card_plan_id,r.nowgrade,r.oldgrade,r.members,1 as rangetype,${hiveconf:thisMonthEnd} as stat_date
from dw_rfm.b_yestoday_member_grade_change_tenants r where r.part='${tenant}' and r.stat_date=${hiveconf:thisMonthEnd}
union all
select r1.card_plan_id,r1.nowgrade,r1.oldgrade,r1.members,2 as rangetype,${hiveconf:thisMonthEnd} as stat_date
from dw_rfm.b_thisweek_member_grade_change_tenants r1 where r1.part='${tenant}' and r1.stat_date=${hiveconf:thisMonthEnd}
union all
select r2.card_plan_id,r2.nowgrade,r2.oldgrade,r2.members,3 as rangetype,${hiveconf:thisMonthEnd} as stat_date
from dw_rfm.b_thismonth_member_grade_change_tenants r2 where r2.part='${tenant}' and r2.stat_date=${hiveconf:thisMonthEnd}
union all
select r3.card_plan_id,r3.nowgrade,r3.oldgrade,r3.members,4 as rangetype,${hiveconf:thisMonthEnd} as stat_date
from dw_rfm.b_last7day_member_grade_change_tenants r3 where r3.part='${tenant}' and r3.stat_date=${hiveconf:thisMonthEnd}
union all
select r4.card_plan_id,r4.nowgrade,r4.oldgrade,r4.members,5 as rangetype,${hiveconf:thisMonthEnd} as stat_date
from dw_rfm.b_last30day_member_grade_change_tenants r4 where r4.part='${tenant}' and r4.stat_date=${hiveconf:thisMonthEnd};

-- 删除临时近30天的会员临时表、过期积分的统计
--alter table dw_rfm.b_invalid_points_last30day_tenants drop partition (part=${hiveconf:thisMonthEnd});
--alter table dw_rfm.b_new_member_last30day_tenants drop partition (part=${hiveconf:thisMonthEnd});
-- 删除中间临时表
--alter table dw_rfm.b_point_change_analyze_tenants drop partition (part=${hiveconf:thisMonthEnd});
--alter table dw_rfm.b_yestoday_member_grade_change_tenants drop partition (part=${hiveconf:thisMonthEnd});
--alter table dw_rfm.b_thisweek_member_grade_change_tenants drop partition (part=${hiveconf:thisMonthEnd});
--alter table dw_rfm.b_thismonth_member_grade_change_tenants drop partition (part=${hiveconf:thisMonthEnd});
--alter table dw_rfm.b_last7day_member_grade_change_tenants drop partition (part=${hiveconf:thisMonthEnd});
--alter table dw_rfm.b_last30day_member_grade_change_tenants drop partition (part=${hiveconf:thisMonthEnd});

-- 需要对两个结果数据进行数据同步
-- b_member_point_grade_change_tenants
-- b_member_grade_transform_tenants










