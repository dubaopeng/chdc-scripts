SET mapred.job.name='offline-shop-member-change-analyze';
set hive.tez.auto.reducer.parallelism=true;
set hive.tez.container.size=8192;
set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.noconditionaltask.size=2250;
set tez.runtime.unordered.output.buffer.size-mb=820;
set tez.runtime.io.sort.mb=3276;
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


set submitTime=from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss');
set yestoday=date_sub('${stat_date}',0);
set weekfirst=date_sub('${stat_date}',pmod(datediff('${stat_date}', concat(year('${stat_date}'),'-01-01'))-6,7));
set monthfirst=date_sub('${stat_date}',dayofmonth('${stat_date}')-1);
set yearfirst=concat(substr('${stat_date}',0,4),'-01-01');
set last7day=date_sub('${stat_date}',7);
set last30day=date_sub('${stat_date}',30);
set lastyear=add_months('${stat_date}',-12);

-- 识别会员开卡时间段
create table if not exists dw_rfm.b_shop_memeber_created_reconized_temp(
	card_plan_id string,
	plat_code string,
	uni_shop_id string,
	shop_id string,
	member_id string,
	yestoday int,
	thisweek int,
	thismonth int,
	thisyear int,
	last7day int,
	last30day int,
	lastyear int
)
PARTITIONED BY(part string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");

--从会员基础信息表中就可以获取所有所需数据
insert overwrite table dw_rfm.b_shop_memeber_created_reconized_temp partition(part='${stat_date}')
select card_plan_id,plat_code,uni_shop_id,shop_id,member_id,
   case when substr(created,1,10) >= ${hiveconf:yestoday} then 1 else 0 end as yestoday,
   case when substr(created,1,10) >= ${hiveconf:weekfirst} then 1 else 0 end as thisweek,
   case when substr(created,1,10) >= ${hiveconf:monthfirst} then 1 else 0 end as thismonth,
   case when substr(created,1,10) >= ${hiveconf:yearfirst} then 1 else 0 end as thisyear,
   case when substr(created,1,10) > ${hiveconf:last7day} then 1 else 0 end as last7day,
   case when substr(created,1,10) > ${hiveconf:last30day} then 1 else 0 end as last30day,
   case when substr(created,1,10) > ${hiveconf:lastyear} then 1 else 0 end as lastyear   
from dw_business.b_std_member_base_info 
where (plat_code='TAOBAO' or plat_code='OFFLINE') and substr(created,1,10) <= '${stat_date}';

-- 各时间段内会员增量数据 
create table if not exists dw_rfm.b_shop_memeber_increm_staticits_temp(
	card_plan_id string,
	plat_code string,
	uni_shop_id string,
	shop_id string,
	members bigint,
	yestoday bigint,
	thisweek bigint,
	thismonth bigint,
	thisyear bigint,
	last7day bigint,
	last30day bigint,
	lastyear bigint
)
PARTITIONED BY(part string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");

insert overwrite table dw_rfm.b_shop_memeber_increm_staticits_temp partition(part='${stat_date}')
select card_plan_id,plat_code,uni_shop_id,shop_id,
		count(member_id) as members,
		sum(yestoday) as yestoday,
		sum(thisweek) as thisweek,
		sum(thismonth) as thismonth,
		sum(thisyear) as thisyear,
		sum(last7day) as last7day,
		sum(last30day) as last30day,
		sum(lastyear) as lastyear
from dw_rfm.b_shop_memeber_created_reconized_temp
where part='${stat_date}'
group by card_plan_id,plat_code,uni_shop_id,shop_id;


-- 店铺会员变化分析结果
create table if not exists dw_rfm.cix_offline_shop_member_change_statistic(
	card_plan_id string,
	plat_code string,
	uni_shop_id string,
	shop_name string,
	members bigint,
	yestoday bigint,
	thisweek bigint,
	thismonth bigint,
	thisyear bigint,
	last7day bigint,
	last30day bigint,
	lastyear bigint,
	stat_date string,
	modified string
)
PARTITIONED BY(part string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

insert overwrite table dw_rfm.cix_offline_shop_member_change_statistic partition(part='${stat_date}')
select t.card_plan_id,t.plat_code,t.uni_shop_id,b.shop_name,
	   t.members,t.yestoday,t.thisweek,t.thismonth,t.thisyear,t.last7day,t.last30day,t.lastyear,
	   '${stat_date}' as stat_date,
	   ${hiveconf:submitTime} as modified
from (
	select a.card_plan_id,a.plat_code,a.uni_shop_id,a.shop_id,
		   a.members,a.yestoday,a.thisweek,a.thismonth,a.thisyear,a.last7day,a.last30day,a.lastyear
	from dw_rfm.b_shop_memeber_increm_staticits_temp a
	where a.part='${stat_date}'
) t
left join(
	select plat_code,shop_id,shop_name 
	from dw_base.b_std_tenant_shop where plat_code='OFFLINE' or plat_code='TAOBAO'
	group by plat_code,shop_id,shop_name
) b
on t.plat_code=b.plat_code and t.shop_id=b.shop_id;

-- 刪除临时表的分区
ALTER TABLE dw_rfm.b_shop_memeber_created_reconized_temp DROP IF EXISTS PARTITION (part='${stat_date}');
ALTER TABLE dw_rfm.b_shop_memeber_increm_staticits_temp DROP IF EXISTS PARTITION (part='${stat_date}');


--需要将 cix_offline_shop_member_change_statistic同步到业务库

