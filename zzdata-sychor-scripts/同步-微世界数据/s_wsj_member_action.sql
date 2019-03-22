SET mapred.job.name='s_wsj_member_action-微世界用户表状态同步';
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

-- 微世界店铺用户增量表
CREATE EXTERNAL TABLE IF NOT EXISTS dw_source.`s_wsj_member_action`(
	`company_id` string,
	`uni_customer_id` string,
    `appid` string,
	`shop_id` string,
	`openid` string,
    `unionid` string,
	`type` int,   --粉丝所属 0:小程序 1:公众号 2:个人号
    `subscribe` int, --是否关注公众号,0-未关注 1-已关注 2-已取关
	`first_cognitive_time` string, --首次认知时间
	`first_cognitive_scene` string, --首次认知来源
	`first_subscribe_time` string,  --首次关注时间
	`last_subscribe_time` string,  --最后关注时间
    `first_subscribe_scene` string, --首次关注来源
    `last_subscribe_scene` string,  --最后关注来源
    `last_active_time` string, --最后活跃时间
	`last_active_scene` string, --最后活跃来源
	`first_subsribe` int,      -- 是否首次关注， 0-否 1-是
	`placed` int,    --是否下单 0-否 1-是
	`qr_scene` string,   		-- 二维码扫码场景
	`create_time` string,
	`update_time` string
)
partitioned by(`dt` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/hive/source_data/wsjscrm/member_action';

-- 批量重设分区
msck repair table dw_source.`s_wsj_member_action`;

-- 创建店铺用户行为状态表
CREATE TABLE IF NOT EXISTS dw_wsj.`b_wsj_member_action`(
	`uni_customer_id` string,
    `appid` string,
	`shop_id` string,
	`openid` string,
    `unionid` string,
	`type` int,  
    `subscribe` int, 
	`first_cognitive_time` string, 
	`first_cognitive_scene` string,
	`first_subscribe_time` string, 
	`last_subscribe_time` string, 
    `first_subscribe_scene` string, 
    `last_subscribe_scene` string, 
    `last_active_time` string, 
	`last_active_scene` string,
	`first_subsribe` int, 
	`placed` int,  
	`qr_scene` string, 
	`create_time` string,
	`update_time` string
)
partitioned by(`company` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' lines terminated by '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");

-- 合并并去重插入结果表
insert overwrite table dw_wsj.b_wsj_member_action partition(company)
select t.uni_customer_id,t.appid,t.shop_id,t.openid,t.unionid,t.type,t.subscribe,t.first_cognitive_time,
			t.first_cognitive_scene,t.first_subscribe_time,t.last_subscribe_time,t.first_subscribe_scene,
			t.last_subscribe_scene,t.last_active_time,t.last_active_scene,t.first_subsribe,t.placed,
			t.qr_scene,t.create_time,t.update_time,t.company_id as company
from (
	select re.company_id,re.uni_customer_id,re.appid,re.shop_id,re.openid,re.unionid,re.type,re.subscribe,re.first_cognitive_time,
			re.first_cognitive_scene,re.first_subscribe_time,re.last_subscribe_time,re.first_subscribe_scene,
			re.last_subscribe_scene,re.last_active_time,re.last_active_scene,re.first_subsribe,re.placed,
			re.qr_scene,re.create_time,re.update_time,
		row_number() over(distribute by re.company_id,re.appid,re.openid sort by re.update_time desc) as num
	from (
		select t1.company as company_id,t1.uni_customer_id,t1.appid,t1.shop_id,t1.openid,t1.unionid,t1.type,t1.subscribe,t1.first_cognitive_time,
				t1.first_cognitive_scene,t1.first_subscribe_time,t1.last_subscribe_time,t1.first_subscribe_scene,
				t1.last_subscribe_scene,t1.last_active_time,t1.last_active_scene,t1.first_subsribe,
				t1.placed,t1.qr_scene,t1.create_time,t1.update_time
			from dw_wsj.b_wsj_member_action t1
		union all 
		select 
			t2.company_id,t2.uni_customer_id,t2.appid,t2.shop_id,t2.openid,t2.unionid,t2.type,t2.subscribe,t2.first_cognitive_time,
			t2.first_cognitive_scene,t2.first_subscribe_time,t2.last_subscribe_time,t2.first_subscribe_scene,
			t2.last_subscribe_scene,t2.last_active_time,t2.last_active_scene,t2.first_subsribe,t2.placed,
			t2.qr_scene,t2.create_time,t2.update_time
			from dw_source.s_wsj_member_action t2
			where t2.dt='${stat_date}'
	)re
) t 
where t.num =1
distribute by company;





