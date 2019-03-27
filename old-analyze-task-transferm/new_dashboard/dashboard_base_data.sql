SET mapred.job.name='dashboard_base_data';
SET hive.exec.compress.output=true;
set hive.cbo.enable=true;
SET mapred.max.split.size=512000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET mapred.output.compression.type=BLOCK;
SET mapreduce.map.output.compress=true;
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
SET hive.exec.compress.output.codec=org.apache.hadoop.io.compress.SnappyCodec;
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


-- 昨日订单基础数据
-- 为昨日计算和订单漏斗图计算使用
drop table if exists new_dashboard.dashboard_base_data_trade_1;
create table if not exists new_dashboard.dashboard_base_data_trade_1(
	dp_id         string,
	tid           string,
	status        string,
	created       string,
	buyer_nick    string,
	pay_time      string,
	payment       string,
	consign_time  string,
	end_time      string
)ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");

insert overwrite table new_dashboard.dashboard_base_data_trade_1
select
    dp_id,
    tid,
    status,
    created,
    buyer_nick,
    pay_time,
    payment,
    consign_time,
    end_time
from dw_base.b_top_trade
where (part >= substr('${stat_date}',1,7) or part = 'active')
    and (substr(created,1,10) = '${stat_date}' or substr(pay_time,1,10) = '${stat_date}');



-- 近30天订单基础数据
-- 为订单漏斗图计算使用
drop table if exists new_dashboard.dashboard_base_data_trade_30;
create table if not exists new_dashboard.dashboard_base_data_trade_30(
	dp_id         string,
	tid           string,
	status        string,
	created       string,
	buyer_nick    string,
	pay_time      string,
	payment       string,
	consign_time  string,
	end_time      string
)ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");

insert overwrite table new_dashboard.dashboard_base_data_trade_30
select
    dp_id,
    tid,
    status,
    created,
    buyer_nick,
    pay_time,
    payment,
    consign_time,
    end_time
from dw_base.b_top_trade
where (part >= substr(date_sub('${stat_date}',29),1,7) or part = 'active')
    and substr(created,1,10) >= substr(date_sub('${stat_date}',29),1,10)
    and substr(created,1,10) <= substr('${stat_date}',1,10);
   -- or substr(pay_time,1,10) >= substr(date_sub('${stat_date}',29),1,10));


