SET mapred.job.name='wsj-shop-customer-label-analyze';
set hive.tez.container.size=16144;
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


-- 计算租户级的首次认知时间
-- 从客户行为数据中获取最早记录的时间，作为首次认知时间,首次认知来源
-- 计算店铺级的影响力、活跃度、传播力
-- 影响力计算 每周日计算近12周的数据

-- 计算传播力 分享次数、分享内容数、平均分享次数、最高分享次数
drop table if exists dw_wsj.b_last12week_share_actions;
create table dw_wsj.b_last12week_share_actions(
	`user_id` string,
	`subject_id` string,
	`sub_id` string,
	`action_time` string,
    `business_id` string,
	`business_action` string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");

insert overwrite table dw_wsj.b_last12week_share_actions
select user_id,subject_id,sub_id,action_time,business_id,business_action
from dw_wsj.b_wsj_actions where part >= date_sub('${stat_date}',12 * 7) and action='SHARE';


-- 计算用户的分享次数、最高分享次数, 公式计算客户传播力
drop table if exists dw_wsj.b_customer_communication;
create table dw_wsj.b_customer_communication(
	`subject_id` string,
	`sub_id` string,
	`user_id` string,
	`share_total_times` bigint,
    `share_content_num` bigint,
	`avg_share_times` double,
	`max_share_times` bigint,
	`communication` double
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");

insert overwrite table dw_wsj.b_customer_communication
select a.subject_id,a.sub_id,a.user_id,a.share_total_times,
	   a.share_content_num,a.avg_share_times,b.max_share_times,
	   pow(ln(a.share_total_times+1)*0.4+ln(a.share_content_num+1)*0.4+ln(a.avg_share_times+1)*0.1+ln(b.max_share_times+1)*0.1,2)*10 as communication
from (
	select t.subject_id,t.sub_id,t.user_id,t.share_total_times,t.share_content_num,
	   (t.share_total_times/t.share_content_num) as avg_share_times
	from(
		select subject_id,sub_id,user_id,count(action_time) as share_total_times,
			   count(distinct business_id,business_action) as share_content_num
		from dw_wsj.b_last12week_share_actions
		group by subject_id,sub_id,user_id
	) t
) a
left join (
	select t.subject_id,t.sub_id,t.user_id,max(t.business_share_time) as max_share_times
	from(
		select subject_id,sub_id,user_id,business_id,business_action,
			count(action_time) as business_share_time
		from dw_wsj.b_last12week_share_actions
		group by subject_id,sub_id,user_id,business_id,business_action
	)t
	group by t.subject_id,t.sub_id,t.user_id
) b
on a.subject_id=b.subject_id and a.sub_id=b.sub_id and a.user_id=b.user_id;


-- 计算客户影响力 分享次数、分享内容数、平均分享次数、最高分享次数
drop table if exists dw_wsj.b_last12week_open_share_actions;
create table dw_wsj.b_last12week_open_share_actions(
	`user_id` string,
	`subject_id` string,
	`sub_id` string,
	`relate_userid` string,
	`action_time` string,
    `business_id` string,
	`business_action` string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");

insert overwrite table dw_wsj.b_last12week_open_share_actions
select user_id,subject_id,sub_id,relate_userid,action_time,business_id,business_action
from dw_wsj.b_wsj_actions 
where part >= date_sub('${stat_date}',12 * 7) 
	and action='OPEN_SHARE' 
	and relate_userid is not null
	and user_id != relate_userid;

	
-- 计算用户的分享打开次数、分享打开人数, 平均打开次数,最高打开次数
drop table if exists dw_wsj.b_customer_influence;
create table dw_wsj.b_customer_influence(
	`subject_id` string,
	`sub_id` string,
	`user_id` string,
	`open_times` bigint,
    `open_users` bigint,
	`avg_times` double,
	`max_open_times` bigint,
	`influence` double
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");

insert overwrite table dw_wsj.b_customer_influence
select a.subject_id,a.sub_id,a.relate_userid as user_id,a.open_times,a.open_users,a.avg_times,b.max_open_times,
	(0.95*(ln(a.open_times+1)*0.4 + ln(a.open_users+1)*0.4 + ln(a.avg_times+1)*0.1 + ln(b.max_open_times+1)*0.1)) as influence
from (
	select t.subject_id,t.sub_id,t.relate_userid,t.open_times,
		   t.open_users,t.open_times/t.open_users as avg_times
	from (
		select subject_id,sub_id,relate_userid,count(action_time) open_times,
				count(distinct user_id) open_users
		from dw_wsj.b_last12week_open_share_actions
		group by subject_id,sub_id,relate_userid
	) t
) a
left join (
	select t.subject_id,t.sub_id,t.relate_userid,max(t.open_share_times) as max_open_times
	from(
		select subject_id,sub_id,relate_userid,business_id,business_action,
			count(action_time) as open_share_times
		from dw_wsj.b_last12week_open_share_actions
		group by subject_id,sub_id,relate_userid,business_id,business_action
	)t
	group by t.subject_id,t.sub_id,t.relate_userid
) b
on a.subject_id=b.subject_id and a.sub_id=b.sub_id and a.relate_userid=b.relate_userid;

insert overwrite table dw_wsj.b_customer_influence
select t.subject_id,t.sub_id,t.user_id,t.open_times,t.open_users,t.avg_times,t.max_open_times,
	   pow(t.influence + 0.05*(ln(t.share_total_times+1)*0.4+ln(t.share_content_num+1)*0.4+
	   ln(t.avg_share_times+1)*0.1+ln(t.max_share_times+1)*0.1),2)*10 as influence
from (
	select 
	   if(a.subject_id is not null,a.subject_id,b.subject_id) as subject_id,
	   if(a.sub_id is not null,a.sub_id,b.sub_id) as sub_id,
	   if(a.user_id is not null,a.user_id,b.user_id) as user_id,
	   a.open_times,a.open_users,a.avg_times,a.max_open_times,
	   if(a.influence is null,0,a.influence) influence,
	   if(b.share_total_times is null,0,b.share_total_times) as share_total_times,
	   if(b.share_content_num is null,0,b.share_content_num) as share_content_num,
	   if(b.avg_share_times is null,0,b.avg_share_times) as avg_share_times,
	   if(b.max_share_times is null,0,b.max_share_times) as max_share_times
	from dw_wsj.b_customer_influence a
	full join dw_wsj.b_customer_communication b
	on a.subject_id=b.subject_id and a.sub_id=b.sub_id and a.user_id=b.user_id
) t;

-- 获取近12周所有的行为记录
drop table if exists dw_wsj.b_last12week_all_actions;
create table dw_wsj.b_last12week_all_actions(
	`user_id` string,
	`subject_id` string,
	`sub_id` string,
	`action_day` string,
	`action_time` string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");

insert overwrite table dw_wsj.b_last12week_all_actions
select user_id,subject_id,sub_id,substr(action_time,0,10) as action_day,action_time
from dw_wsj.b_wsj_actions 
where part >= date_sub('${stat_date}',12 * 7);

-- 计算活跃度,用户的活跃天数、活跃次数、平均活跃次数、最大活跃天数
drop table if exists dw_wsj.b_customer_activity;
create table dw_wsj.b_customer_activity(
	`subject_id` string,
	`sub_id` string,
	`user_id` string,
	`active_days` bigint,
	`active_times` bigint,
	`avg_active_times` double,
	`max_active_times` bigint,
	`activity` double
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");

insert overwrite table dw_wsj.b_customer_activity
select a.subject_id,a.sub_id,a.user_id,a.active_days,a.active_times,(a.active_times/a.active_days) as avg_active_times,
	   b.max_active_times,
	   pow(ln(a.active_days+1)*0.4 + ln(a.active_times+1)*0.4 + ln((a.active_times/a.active_days)+1)*0.1 + ln(b.max_active_times+1)*0.1,2)*10 as activity
from (
	select subject_id,sub_id,user_id,count(distinct action_day) active_days,count(action_time) active_times
	from dw_wsj.b_last12week_all_actions
	group by subject_id,sub_id,user_id
) a
left join 
(
	select t.subject_id,t.sub_id,t.user_id,t.active_times as max_active_times
	from (
		select re.subject_id,re.sub_id,re.user_id,re.active_times,
			row_number() over(distribute by re.subject_id,re.sub_id,re.user_id sort by re.active_times desc) as num
		from (
			select subject_id,sub_id,user_id,action_day,count(action_time) active_times
			from dw_wsj.b_last12week_all_actions
			group by subject_id,sub_id,user_id,action_day
		) re
	)t
	where t.num=1
)b
on a.subject_id=b.subject_id and a.sub_id=b.sub_id and a.user_id=b.user_id;

	
-- 客户类型识别表   1:潜客  2：认知客，3：兴趣客 4：粉丝 5：KOL
drop table if exists dw_wsj.b_customer_type_reconized;
create table dw_wsj.b_customer_type_reconized(
	`subject_id` string,
	`sub_id` string,
	`user_id` string,
	`customer_type` string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");

insert overwrite table dw_wsj.b_customer_type_reconized
select t.subject_id,t.sub_id,t.user_id,
	   case when t.readnums==1 or t.visitnums==1 then 2 
	   when t.readnums > 1 or t.visitnums>1 or t.sharenums>=1 then 3
	   else 1 end as customer_type
from (
	select a.subject_id,a.sub_id,a.user_id,
		sum(a.readnum) as readnums,
		sum(a.visitnum) visitnums,
		sum(a.sharenum) sharenums
	from (
		select user_id,subject_id,sub_id,
			   if(source=='mp' and action=='READ',1,0) as readnum,
			   if(source=='mini' and action=='VISIT',1,0) as visitnum,
			   if(action=='SHARE',1,0) as sharenum
		from dw_wsj.b_wsj_actions 
		where part >= date_sub('${stat_date}',30) 
			and action in('READ','VISIT','SHARE')
	) a
	group by a.subject_id,a.sub_id,a.user_id
) t;

-- 合并店铺级客户标签数据
drop table if exists dw_wsj.b_shop_customer_label_info;
create table dw_wsj.b_shop_customer_label_info(
	`company` string,
	`uni_customer_id` string,
	`shop_id` string,
	`appid` string,
	`openid` string,
	`first_cognitive_time` string, 
	`first_cognitive_scene` string, 
    `last_active_time` string, 
	`last_active_scene` string,
	`subscribe` int, 
	`last_subscribe_time` string, 
    `last_subscribe_scene` string,
	`first_subsribe` int,
	`customer_type` int,
	`influence` double,
	`activity` double,
	`communication` double
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");


insert overwrite table dw_wsj.b_shop_customer_label_info
select a.company,a.uni_customer_id,a.shop_id,a.appid,a.openid,
	f.first_cognitive_time,f.first_cognitive_scene,f.last_active_time,f.last_active_scene,
	f.subscribe,f.last_subscribe_time,f.last_subscribe_scene,f.first_subsribe,
	case when (a.auth_mobile is not null and length(a.auth_mobile)>0) or f.subscribe=1 or f.placed=1 then 4
		when b.customer_type is not null then b.customer_type 
		else 1 end as customer_type,
	if(d.influence is null,0,round(d.influence,3)) as influence,
	if(e.activity is null,0,round(e.activity,3)) as activity,
	if(c.communication is null,0,round(c.communication,3)) as communication
from dw_wsj.b_wsj_member a
left join dw_wsj.b_wsj_member_action f
on a.company=f.company and a.shop_id=f.shop_id and a.openid=f.openid
left join dw_wsj.b_customer_type_reconized b
on a.company=b.subject_id and a.shop_id=b.sub_id and a.openid=b.user_id
left join dw_wsj.b_customer_influence d
on a.company=d.subject_id and a.shop_id=d.sub_id and a.openid=d.user_id
left join dw_wsj.b_customer_activity e
on a.company=e.subject_id and a.shop_id=e.sub_id and a.openid=e.user_id
left join dw_wsj.b_customer_communication c
on a.company=c.subject_id and a.shop_id=c.sub_id and a.openid=c.user_id;











