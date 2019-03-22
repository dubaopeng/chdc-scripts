SET mapred.job.name='s_wsj_actions-微世界行为数据分析';
--set hive.execution.engine=mr;
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

-- 创建行为数据每天增量同步表
CREATE EXTERNAL TABLE IF NOT EXISTS dw_source.`s_wsj_actions`(
	`id` string,
	`user_id` string,
    `source` string,
	`relate_userid` string,
	`subject_id` string,
    `sub_id` string, 
    `action_time` string,
	`plat_code` string,
	`action` string,
    `business_id` string, 
	`business_title` string,
	`business_action` string,
	`action_source` string,
    `source_detail` string, 
    `target` string,
	`content` string,
	`start_time` string,
    `end_time` string,
	`url` string,
	`push_msg_id` string,
	`visit_duration` bigint, 
    `referrer_info` string,
	`scene` string,
	`order_id` string,
    `marketing_type` string,
	`feedback` string,
	`feedback_time` string
)
partitioned by(`dt` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/hive/source_data/wsjscrm/actions';

-- 批量重设分区
msck repair table dw_source.`s_wsj_actions`;

-- 创建微世界行为数据基础表(以日期和行为类型分区)
CREATE TABLE IF NOT EXISTS dw_wsj.`b_wsj_actions`(
	`id` string,
	`user_id` string,
    `source` string,
	`relate_userid` string,
	`subject_id` string,
    `sub_id` string, 
    `action_time` string,
	`plat_code` string,
    `business_id` string, 
	`business_title` string,
	`business_action` string,
	`action_source` string,
    `source_detail` string, 
    `target` string,
	`content` string,
	`start_time` string,
    `end_time` string,
	`url` string,
	`push_msg_id` string,
	`visit_duration` bigint, 
    `referrer_info` string,
	`scene` string,
	`order_id` string,
    `marketing_type` string,
	`feedback` string,
	`feedback_time` string
)
partitioned by(part string,action string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' lines terminated by '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");


-- 数据去重，插入基础表数据分区
insert into dw_wsj.b_wsj_actions partition(part='${stat_date}',action)
select  t.id,
		t.user_id,
		t.source,
		t.relate_userid,
		t.subject_id,
		t.sub_id,
		t.action_time,
		t.plat_code,
		t.business_id,
		t.business_title,
		t.business_action,
		t.action_source,
		t.source_detail,
		t.target,
		t.content,
		t.start_time,
		t.end_time,
		t.url,
		t.push_msg_id,
		t.visit_duration,
		t.referrer_info,
		t.scene,
		t.order_id,
		t.marketing_type,
		t.feedback,
		t.feedback_time,
		t.action as action
from(
    select id,user_id,source,relate_userid,subject_id,sub_id,action_time,plat_code,action,
		business_id,business_title,business_action,action_source,source_detail,target,content,
		start_time,end_time,url,push_msg_id,visit_duration,referrer_info,
		scene,order_id,marketing_type,feedback,feedback_time,
		row_number() over (partition by user_id,source,action_time) as num 
    from dw_source.s_wsj_actions where dt='${stat_date}'
) t 
where t.num=1
distribute by t.action;








