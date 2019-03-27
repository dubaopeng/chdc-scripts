SET mapred.job.name='dashboard_result_trade_status_analysis';
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
----2.首页订单漏斗图数据计算
----new_dashboard.dashboard_result_trade_status_analysis
----------------------------

--昨日订单，流转耗时，流转率计算
drop table if exists new_dashboard.dashboard_result_trade_status_analysis_1;
create table if not exists new_dashboard.dashboard_result_trade_status_analysis_1(
	dp_id                 string,
	tid_count_created     bigint,
	tid_count_paytime     bigint,
	tid_count_sends       bigint,
	tid_count_finished    bigint,
	rate_shiping_time_paytime  double,
	rate_shiping_time_sends    double,
	rate_shiping_time_finished double
)ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");

insert overwrite table new_dashboard.dashboard_result_trade_status_analysis_1
select
    a.dp_id,
    a.tid_count_created,
    b.tid_count_paytime,
    c.tid_count_sends,
    d.tid_count_finished,
    b.rate_shiping_time_paytime,
    c.rate_shiping_time_sends,
    d.rate_shiping_time_finished
from
    --下单订单
    (
    select
        dp_id,
        count(tid) as tid_count_created
    from
        (
        select
            dp_id,
            tid
        from new_dashboard.dashboard_base_data_trade_1
        where substr(created,1,10) = '${stat_date}'
        group by dp_id,tid
        )a
    group by
        dp_id
    )a
left join
    --付款订单
    (
    select
        dp_id,
        count(tid) as tid_count_paytime,
        avg(case when pay_time = 'NULL' or pay_time is NULL or created >= pay_time then 0 else ((unix_timestamp(pay_time, 'yyyy-MM-dd HH:mm:ss') - unix_timestamp(created, 'yyyy-MM-dd HH:mm:ss'))/60) end) as rate_shiping_time_paytime
    from
        (
        select
            dp_id,
            tid,
            created,
            pay_time
        from new_dashboard.dashboard_base_data_trade_1
        where substr(created,1,10) = '${stat_date}'
            and status in ('SELLER_CONSIGNED_PART','WAIT_SELLER_SEND_GOODS','WAIT_BUYER_CONFIRM_GOODS','TRADE_BUYER_SIGNED','TRADE_FINISHED')
        group by dp_id,tid,created,pay_time
        )a
    group by
        dp_id
    )b
on a.dp_id = b.dp_id
left join
    --发货订单
    (
    select
        dp_id,
        count(tid) as tid_count_sends,
        avg(case when consign_time = 'NULL' or consign_time is NULL or pay_time = 'NULL' or pay_time is NULL or pay_time >= consign_time then 0 else ((unix_timestamp(consign_time, 'yyyy-MM-dd HH:mm:ss') - unix_timestamp(pay_time, 'yyyy-MM-dd HH:mm:ss'))/3600) end) as rate_shiping_time_sends
    from
        (
        select
            dp_id,
            tid,
            pay_time,
            consign_time
        from new_dashboard.dashboard_base_data_trade_1
        where substr(created,1,10) = '${stat_date}'
            --and status in ('SELLER_CONSIGNED_PART','WAIT_SELLER_SEND_GOODS','WAIT_BUYER_CONFIRM_GOODS','TRADE_BUYER_SIGNED')
              and status in ('SELLER_CONSIGNED_PART','WAIT_BUYER_CONFIRM_GOODS','TRADE_BUYER_SIGNED','TRADE_FINISHED')
        group by dp_id,tid,pay_time,consign_time
        )a
    group by
        dp_id
    )c
on a.dp_id = c.dp_id
left join
    --完成订单
    (
    select
        dp_id,
        count(tid) as tid_count_finished,
        avg(case when end_time = 'NULL' or end_time is NULL or consign_time = 'NULL' or consign_time is NULL or consign_time >= end_time then 0 else ((unix_timestamp(end_time, 'yyyy-MM-dd HH:mm:ss') - unix_timestamp(consign_time, 'yyyy-MM-dd HH:mm:ss'))/(3600*24)) end) as rate_shiping_time_finished
    from
        (
        select
            dp_id,
            tid,
            consign_time,
            end_time
        from new_dashboard.dashboard_base_data_trade_1
        where substr(created,1,10) = '${stat_date}'
            and status in ('TRADE_FINISHED')
        group by dp_id,tid,consign_time,end_time
        )a
    group by
        dp_id
    )d
on a.dp_id = d.dp_id;


--近7天订单，流转耗时，流转率计算
drop table if exists new_dashboard.dashboard_result_trade_status_analysis_7;
create table if not exists new_dashboard.dashboard_result_trade_status_analysis_7(
	dp_id                 string,
	tid_count_created     bigint,
	tid_count_paytime     bigint,
	tid_count_sends       bigint,
	tid_count_finished    bigint,
	rate_shiping_time_paytime  double,
	rate_shiping_time_sends    double,
	rate_shiping_time_finished double
)ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");

insert overwrite table new_dashboard.dashboard_result_trade_status_analysis_7
select
    a.dp_id,
    a.tid_count_created,
    b.tid_count_paytime,
    c.tid_count_sends,
    d.tid_count_finished,
    b.rate_shiping_time_paytime,
    c.rate_shiping_time_sends,
    d.rate_shiping_time_finished
from
    --下单订单
    (
    select
        dp_id,
        count(tid) as tid_count_created
    from
        (
        select
            dp_id,
            tid
        from new_dashboard.dashboard_base_data_trade_30
        where substr(created,1,10) >= substr(date_sub('${stat_date}',6),1,10)
			and substr(created,1,10) <= substr('${stat_date}',1,10)
        group by dp_id,tid
        )a
    group by
        dp_id
    )a
left join
    --付款订单
    (
    select
        dp_id,
        count(tid) as tid_count_paytime,
        avg(case when pay_time = 'NULL' or pay_time is NULL or created >= pay_time then 0 else ((unix_timestamp(pay_time, 'yyyy-MM-dd HH:mm:ss') - unix_timestamp(created, 'yyyy-MM-dd HH:mm:ss'))/60) end) as rate_shiping_time_paytime
    from
        (
        select
            dp_id,
            tid,
            created,
            pay_time
        from new_dashboard.dashboard_base_data_trade_30
        where substr(created,1,10) >= substr(date_sub('${stat_date}',6),1,10)
			and substr(created,1,10) <= substr('${stat_date}',1,10)
            and status in ('SELLER_CONSIGNED_PART','WAIT_SELLER_SEND_GOODS','WAIT_BUYER_CONFIRM_GOODS','TRADE_BUYER_SIGNED','TRADE_FINISHED')
        group by dp_id,tid,created,pay_time
        )a
    group by
        dp_id
    )b
on a.dp_id = b.dp_id
left join
    --发货订单
    (
    select
        dp_id,
        count(tid) as tid_count_sends,
        avg(case when consign_time = 'NULL' or consign_time is NULL or pay_time = 'NULL' or pay_time is NULL or pay_time >= consign_time then 0 else ((unix_timestamp(consign_time, 'yyyy-MM-dd HH:mm:ss') - unix_timestamp(pay_time, 'yyyy-MM-dd HH:mm:ss'))/3600) end) as rate_shiping_time_sends
    from
        (
        select
            dp_id,
            tid,
            pay_time,
            consign_time
        from new_dashboard.dashboard_base_data_trade_30
        where substr(created,1,10) >= substr(date_sub('${stat_date}',6),1,10)
			and substr(created,1,10) <= substr('${stat_date}',1,10)
            --and status in ('SELLER_CONSIGNED_PART','WAIT_SELLER_SEND_GOODS','WAIT_BUYER_CONFIRM_GOODS','TRADE_BUYER_SIGNED')
              and status in ('SELLER_CONSIGNED_PART','WAIT_BUYER_CONFIRM_GOODS','TRADE_BUYER_SIGNED','TRADE_FINISHED')
        group by dp_id,tid,pay_time,consign_time
        )a
    group by
        dp_id
    )c
on a.dp_id = c.dp_id
left join
    --完成订单
    (
    select
        dp_id,
        count(tid) as tid_count_finished,
        avg(case when end_time = 'NULL' or end_time is NULL or consign_time = 'NULL' or consign_time is NULL or consign_time >= end_time then 0 else ((unix_timestamp(end_time, 'yyyy-MM-dd HH:mm:ss') - unix_timestamp(consign_time, 'yyyy-MM-dd HH:mm:ss'))/(3600*24)) end) as rate_shiping_time_finished
    from
        (
        select
            dp_id,
            tid,
            consign_time,
            end_time
        from new_dashboard.dashboard_base_data_trade_30
        where substr(created,1,10) >= substr(date_sub('${stat_date}',6),1,10)
			and substr(created,1,10) <= substr('${stat_date}',1,10)
            and status in ('TRADE_FINISHED')
        group by dp_id,tid,consign_time,end_time
        )a
    group by
        dp_id
    )d
on a.dp_id = d.dp_id;


--近30天订单，流转耗时，流转率计算
drop table if exists new_dashboard.dashboard_result_trade_status_analysis_30;
create table if not exists new_dashboard.dashboard_result_trade_status_analysis_30(
	dp_id                 string,
	tid_count_created     bigint,
	tid_count_paytime     bigint,
	tid_count_sends       bigint,
	tid_count_finished    bigint,
	rate_shiping_time_paytime  double,
	rate_shiping_time_sends    double,
	rate_shiping_time_finished double
)ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");

insert overwrite table new_dashboard.dashboard_result_trade_status_analysis_30
select
    a.dp_id,
    a.tid_count_created,
    b.tid_count_paytime,
    c.tid_count_sends,
    d.tid_count_finished,
    b.rate_shiping_time_paytime,
    c.rate_shiping_time_sends,
    d.rate_shiping_time_finished
from
    --下单订单
    (
    select
        dp_id,
        count(tid) as tid_count_created
    from
        (
        select
            dp_id,
            tid
        from new_dashboard.dashboard_base_data_trade_30
        where substr(created,1,10) >= substr(date_sub('${stat_date}',29),1,10)
        group by dp_id,tid
        )a
    group by
        dp_id
    )a
left join
    --付款订单
    (
    select
        dp_id,
        count(tid) as tid_count_paytime,
        avg(case when pay_time = 'NULL' or pay_time is NULL or created >= pay_time then 0 else ((unix_timestamp(pay_time, 'yyyy-MM-dd HH:mm:ss') - unix_timestamp(created, 'yyyy-MM-dd HH:mm:ss'))/60) end) as rate_shiping_time_paytime
    from
        (
        select
            dp_id,
            tid,
            created,
            pay_time
        from new_dashboard.dashboard_base_data_trade_30
        where substr(created,1,10) >= substr(date_sub('${stat_date}',29),1,10)
            and status in ('SELLER_CONSIGNED_PART','WAIT_SELLER_SEND_GOODS','WAIT_BUYER_CONFIRM_GOODS','TRADE_BUYER_SIGNED','TRADE_FINISHED')
        group by dp_id,tid,created,pay_time
        )a
    group by
        dp_id
    )b
on a.dp_id = b.dp_id
left join
    --发货订单
    (
    select
        dp_id,
        count(tid) as tid_count_sends,
        avg(case when consign_time = 'NULL' or consign_time is NULL or pay_time = 'NULL' or pay_time is NULL or pay_time >= consign_time then 0 else ((unix_timestamp(consign_time, 'yyyy-MM-dd HH:mm:ss') - unix_timestamp(pay_time, 'yyyy-MM-dd HH:mm:ss'))/3600) end) as rate_shiping_time_sends
    from
        (
        select
            dp_id,
            tid,
            pay_time,
            consign_time
        from new_dashboard.dashboard_base_data_trade_30
        where substr(created,1,10) >= substr(date_sub('${stat_date}',29),1,10)
            and status in ('SELLER_CONSIGNED_PART','WAIT_BUYER_CONFIRM_GOODS','TRADE_BUYER_SIGNED','TRADE_FINISHED')
        group by dp_id,tid,pay_time,consign_time
        )a
    group by
        dp_id
    )c
on a.dp_id = c.dp_id
left join
    --完成订单
    (
    select
        dp_id,
        count(tid) as tid_count_finished,
        avg(case when end_time = 'NULL' or end_time is NULL or consign_time = 'NULL' or consign_time is NULL or consign_time >= end_time then 0 else ((unix_timestamp(end_time, 'yyyy-MM-dd HH:mm:ss') - unix_timestamp(consign_time, 'yyyy-MM-dd HH:mm:ss'))/(3600*24)) end) as rate_shiping_time_finished
    from
        (
        select
            dp_id,
            tid,
            consign_time,
            end_time
        from new_dashboard.dashboard_base_data_trade_30
        where substr(created,1,10) >= substr(date_sub('${stat_date}',29),1,10)
            and status in ('TRADE_FINISHED')
        group by dp_id,tid,consign_time,end_time
        )a
    group by
        dp_id
    )d
on a.dp_id = d.dp_id;


--结果
drop table if exists new_dashboard.dashboard_result_trade_status_analysis;
create table if not exists new_dashboard.dashboard_result_trade_status_analysis(
	shop_id                     string,
	period                      bigint,
	tid_count_created           bigint,
	tid_count_paytime           bigint,
	tid_count_sends             bigint,
	tid_count_finished          bigint,
    rate_shiping_paytime        double,
    rate_shiping_sends          double,
    rate_shiping_finished       double,
    rate_shiping_time_paytime   double,
    rate_shiping_time_sends     double,
    rate_shiping_time_finished  double,
    modified                    string,
    stat_date                  string
)ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS textfile;

insert overwrite table new_dashboard.dashboard_result_trade_status_analysis
select
    dp_id as shop_id,
    '1' as period,
    case when tid_count_created is NULL or tid_count_created = 'NULL' then 0 else tid_count_created end as tid_count_created,
    case when tid_count_paytime is NULL or tid_count_paytime = 'NULL' then 0 else tid_count_paytime end as tid_count_paytime,
    case when tid_count_sends is NULL or tid_count_sends = 'NULL' then 0 else tid_count_sends end as tid_count_sends,
    case when tid_count_finished is NULL or tid_count_finished = 'NULL' then 0 else tid_count_finished end as tid_count_finished,

    --流转率
    case when tid_count_created is NULL or tid_count_created = 0 or tid_count_created = 'NULL' or tid_count_paytime is NULL or tid_count_paytime = 'NULL' then 0 else round((tid_count_paytime / tid_count_created),6) end as rate_shiping_paytime,
    case when tid_count_paytime is NULL or tid_count_paytime = 0 or tid_count_paytime = 'NULL' or tid_count_sends is NULL or tid_count_sends = 'NULL' then 0 else round((tid_count_sends / tid_count_paytime),6) end as rate_shiping_sends,
    case when tid_count_paytime is NULL or tid_count_paytime = 0 or tid_count_paytime = 'NULL' or tid_count_finished is NULL or tid_count_finished = 'NULL' then 0 else round((tid_count_finished / tid_count_sends),6) end as rate_shiping_finished,

    --流转耗时
    case when rate_shiping_time_paytime is NULL or rate_shiping_time_paytime = 'NULL' then 0 else round(rate_shiping_time_paytime) end as rate_shiping_time_paytime,
    case when rate_shiping_time_sends is NULL or rate_shiping_time_sends = 'NULL' then 0 else round(rate_shiping_time_sends,6) end as rate_shiping_time_sends,
    case when rate_shiping_time_finished is NULL or rate_shiping_time_finished = 'NULL' then 0 else round(rate_shiping_time_finished,6) end as rate_shiping_time_finished,
    ${hiveconf:MODIFIED} as modified,
    '${stat_date}' as stat_date
from new_dashboard.dashboard_result_trade_status_analysis_1
where dp_id is not NULL

union all

select
    dp_id as shop_id,
    '7' as period,
    case when tid_count_created is NULL or tid_count_created = 'NULL' then 0 else tid_count_created end as tid_count_created,
    case when tid_count_paytime is NULL or tid_count_paytime = 'NULL' then 0 else tid_count_paytime end as tid_count_paytime,
    case when tid_count_sends is NULL or tid_count_sends = 'NULL' then 0 else tid_count_sends end as tid_count_sends,
    case when tid_count_finished is NULL or tid_count_finished = 'NULL' then 0 else tid_count_finished end as tid_count_finished,

    --流转率
    case when tid_count_created is NULL or tid_count_created = 0 or tid_count_created = 'NULL' or tid_count_paytime is NULL or tid_count_paytime = 'NULL' then 0 else round((tid_count_paytime / tid_count_created),6) end as rate_shiping_paytime,
    case when tid_count_paytime is NULL or tid_count_paytime = 0 or tid_count_paytime = 'NULL' or tid_count_sends is NULL or tid_count_sends = 'NULL' then 0 else round((tid_count_sends / tid_count_paytime),6) end as rate_shiping_sends,
    case when tid_count_paytime is NULL or tid_count_paytime = 0 or tid_count_paytime = 'NULL' or tid_count_finished is NULL or tid_count_finished = 'NULL' then 0 else round((tid_count_finished / tid_count_sends),6) end as rate_shiping_finished,

    --流转耗时
    case when rate_shiping_time_paytime is NULL or rate_shiping_time_paytime = 'NULL' then 0 else round(rate_shiping_time_paytime) end as rate_shiping_time_paytime,
    case when rate_shiping_time_sends is NULL or rate_shiping_time_sends = 'NULL' then 0 else round(rate_shiping_time_sends,6) end as rate_shiping_time_sends,
    case when rate_shiping_time_finished is NULL or rate_shiping_time_finished = 'NULL' then 0 else round(rate_shiping_time_finished,6) end as rate_shiping_time_finished,
    ${hiveconf:MODIFIED} as modified,
    '${stat_date}' as stat_date
from new_dashboard.dashboard_result_trade_status_analysis_7
where dp_id is not NULL

union all

select
    dp_id as shop_id,
    '30' as period,
    case when tid_count_created is NULL or tid_count_created = 'NULL' then 0 else tid_count_created end as tid_count_created,
    case when tid_count_paytime is NULL or tid_count_paytime = 'NULL' then 0 else tid_count_paytime end as tid_count_paytime,
    case when tid_count_sends is NULL or tid_count_sends = 'NULL' then 0 else tid_count_sends end as tid_count_sends,
    case when tid_count_finished is NULL or tid_count_finished = 'NULL' then 0 else tid_count_finished end as tid_count_finished,

    --流转率
    case when tid_count_created is NULL or tid_count_created = 0 or tid_count_created = 'NULL' or tid_count_paytime is NULL or tid_count_paytime = 'NULL' then 0 else round((tid_count_paytime / tid_count_created),6) end as rate_shiping_paytime,
    case when tid_count_paytime is NULL or tid_count_paytime = 0 or tid_count_paytime = 'NULL' or tid_count_sends is NULL or tid_count_sends = 'NULL' then 0 else round((tid_count_sends / tid_count_paytime),6) end as rate_shiping_sends,
    case when tid_count_paytime is NULL or tid_count_paytime = 0 or tid_count_paytime = 'NULL' or tid_count_finished is NULL or tid_count_finished = 'NULL' then 0 else round((tid_count_finished / tid_count_sends),6) end as rate_shiping_finished,

    --流转耗时
    case when rate_shiping_time_paytime is NULL or rate_shiping_time_paytime = 'NULL' then 0 else round(rate_shiping_time_paytime) end as rate_shiping_time_paytime,
    case when rate_shiping_time_sends is NULL or rate_shiping_time_sends = 'NULL' then 0 else round(rate_shiping_time_sends,6) end as rate_shiping_time_sends,
    case when rate_shiping_time_finished is NULL or rate_shiping_time_finished = 'NULL' then 0 else round(rate_shiping_time_finished,6) end as rate_shiping_time_finished,
    ${hiveconf:MODIFIED} as modified,
    '${stat_date}' as stat_date
from new_dashboard.dashboard_result_trade_status_analysis_30
where dp_id is not NULL;


