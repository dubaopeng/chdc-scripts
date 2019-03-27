SET mapred.job.name='dashboard_result_trade_count_analysis';
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
----1.首页T+1数据计算
----new_dashboard.dashboard_result_trade_count_analysis
----------------------------

--T+1订单数据计算
drop table if exists new_dashboard.dashboard_trade_count_analysis;
create table if not exists new_dashboard.dashboard_trade_count_analysis(
	dp_id                       string,
	payment                     double,
	buyernick_count_created     bigint,
	buyernick_count_paytime     bigint,
	refund_payment              double
)ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");

insert overwrite table new_dashboard.dashboard_trade_count_analysis
select
    a.dp_id,
    b.payment,
    a.buyernick_count_created,
    c.buyernick_count_paytime,
    d.refund_payment
from
    (
    --下单人数
    select
        dp_id,
        count(buyer_nick) as buyernick_count_created
    --from new_dashboard.dashboard_base_data_trade_1
    from
        (
        select
            dp_id,
            buyer_nick
        from new_dashboard.dashboard_base_data_trade_1
        where substr(created,1,10) = '${stat_date}'
        group by
            dp_id,
            buyer_nick
        )a
    group by dp_id
    )a
left join
    (
    --付款金额
    select
        dp_id,
        sum(payment) as payment
    from new_dashboard.dashboard_base_data_trade_1
    where substr(pay_time,1,10) = '${stat_date}' and payment >0 and payment is not null
    group by dp_id
    )b
    on a.dp_id = b.dp_id
left join
    (
    --付款人数
    select
        dp_id,
        count(buyer_nick) as buyernick_count_paytime
    from
        (
        select
            dp_id,
            buyer_nick
        from new_dashboard.dashboard_base_data_trade_1
        where substr(created,1,10) = '${stat_date}'
            and substr(pay_time,1,10) = '${stat_date}'
            and payment > 0
        group by
            dp_id,
            buyer_nick
        )a
    group by dp_id
    )c
    on a.dp_id = c.dp_id
left join
    (
    --退款金额
    select
          dp_id,
          sum(refund_fee) as refund_payment
    from dw_base.b_top_refund
    where substr(created,1,10) = '${stat_date}'
      and status='SUCCESS'
      and refund_fee>0 and  refund_fee is not null
    group by dp_id
    )d
    on a.dp_id = d.dp_id;


--昨日数据计算结果
drop table if exists new_dashboard.dashboard_result_trade_count_analysis;
create table if not exists new_dashboard.dashboard_result_trade_count_analysis(
	dp_id                       string,
	payment                     double,
	buyernick_count_created     bigint,
	buyernick_count_paytime     bigint,
	payment_rate                double,
	refund_payment              double,
	modified                    string,
	stat_date                   string
)ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS textfile;

insert overwrite table new_dashboard.dashboard_result_trade_count_analysis
select
    dp_id,
    case when payment is NULL or payment = 'NULL' then 0 else round(payment,6) end as payment,
    case when buyernick_count_created is NULL or buyernick_count_created = 'NULL' then 0 else round(buyernick_count_created,6) end as buyernick_count_created,
    case when buyernick_count_paytime is NULL or buyernick_count_paytime = 'NULL' then 0 else round(buyernick_count_paytime,6) end as buyernick_count_paytime,
    case when buyernick_count_created is NULL or buyernick_count_created = 0 or buyernick_count_created = 'NULL' or buyernick_count_paytime is NULL or buyernick_count_paytime = 'NULL' then 0 else round((buyernick_count_paytime / buyernick_count_created),6) end as payment_rate,
    case when refund_payment is NULL or refund_payment = 'NULL' then 0 else round(refund_payment,6) end as refund_payment,
    ${hiveconf:MODIFIED} as modified,
    '${stat_date}' as stat_date
from new_dashboard.dashboard_trade_count_analysis
where dp_id is not NULL;


