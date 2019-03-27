SET mapred.job.name='dashboard_result_shop_dsr';
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


set MODIFIED = substr(current_timestamp,1,19);

----------------------------
----5.首页店铺dsr数据计算
----new_dashboard.dashboard_result_shop_dsr
----------------------------

drop table if exists new_dashboard.dashboard_result_shop_dsr_1;
create table if not exists new_dashboard.dashboard_result_shop_dsr_1(
	dp_id         string,
	last_sync        string,
	item_score       double,
	service_score    double,
	delivery_score      double
)ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");

insert overwrite table new_dashboard.dashboard_result_shop_dsr_1
select
    dp_id,
	substr(last_sync,1,10) as last_sync ,
	item_score ,
	service_score ,
	delivery_score
from dw_base.b_top_shop_dsr
where part = '${stat_date}'
    and dp_id is not NULL;


drop table if exists new_dashboard.dashboard_result_shop_dsr_7;
create table if not exists new_dashboard.dashboard_result_shop_dsr_7(
	dp_id         string,
	item_score       double,
	service_score    double,
	delivery_score      double
)ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");

insert overwrite table new_dashboard.dashboard_result_shop_dsr_7
select
    dp_id,
    avg(item_score) as item_score,
    avg(service_score) as service_score,
    avg(delivery_score) as delivery_score
from
    (
    select
        dp_id,
        dp_nick ,
        substr(last_sync,1,10) as last_sync ,
        item_score ,
        service_score ,
        delivery_score
    from dw_base.b_top_shop_dsr
    where part >= substr(date_sub('${stat_date}',6),1,7)
        and dp_id is not NULL
    )a
where dp_id is not null
group by
    dp_id;



drop table if exists new_dashboard.dashboard_result_shop_dsr_30;
create table if not exists new_dashboard.dashboard_result_shop_dsr_30(
	dp_id         string,
	item_score       double,
	service_score    double,
	delivery_score      double
)ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");

insert overwrite table new_dashboard.dashboard_result_shop_dsr_30
select
    dp_id,
    avg(item_score) as item_score,
    avg(service_score) as service_score,
    avg(delivery_score) as delivery_score
from
    (
    select
        dp_id,
        dp_nick ,
        substr(last_sync,1,10) as last_sync ,
        item_score ,
        service_score ,
        delivery_score
    from dw_base.b_top_shop_dsr
    where part >= substr(date_sub('${stat_date}',29),1,7)
        and dp_id is not NULL
    )a
where dp_id is not null
group by
    dp_id;




--结果
drop table if exists new_dashboard.dashboard_result_shop_dsr;
create table if not exists new_dashboard.dashboard_result_shop_dsr(
	shop_id         string,
	period           bigint,
	desc_rate        double,
	service_rate       double,
	shipping_rate    double,
	modified      string,
	stat_date     string
)ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS textfile;

insert overwrite table new_dashboard.dashboard_result_shop_dsr
select
    dp_id as shop_id,
    '1' as period,
    case when item_score is NULL or item_score = 'NULL' then 0 else round(item_score,6) end as desc_rate,
    case when service_score is NULL or service_score = 'NULL' then 0 else round(service_score,6) end as service_rate,
    case when delivery_score is NULL or delivery_score = 'NULL' then 0 else round(delivery_score,6) end as shipping_rate,
    ${hiveconf:MODIFIED} as modified,
    '${stat_date}' as stat_date
from new_dashboard.dashboard_result_shop_dsr_1
where dp_id is not null

union all

select
    dp_id as shop_id,
    '7' as period,
    case when item_score is NULL or item_score = 'NULL' then 0 else round(item_score,6) end as desc_rate,
    case when service_score is NULL or service_score = 'NULL' then 0 else round(service_score,6) end as service_rate,
    case when delivery_score is NULL or delivery_score = 'NULL' then 0 else round(delivery_score,6) end as shipping_rate,
    ${hiveconf:MODIFIED} as modified,
    '${stat_date}' as stat_date
from new_dashboard.dashboard_result_shop_dsr_7
where dp_id is not null

union all

select
    dp_id as shop_id,
    '30' as period,
    case when item_score is NULL or item_score = 'NULL' then 0 else round(item_score,6) end as desc_rate,
    case when service_score is NULL or service_score = 'NULL' then 0 else round(service_score,6) end as service_rate,
    case when delivery_score is NULL or delivery_score = 'NULL' then 0 else round(delivery_score,6) end as shipping_rate,
     ${hiveconf:MODIFIED} as modified,
    '${stat_date}' as stat_date
from new_dashboard.dashboard_result_shop_dsr_30
where dp_id is not null;




