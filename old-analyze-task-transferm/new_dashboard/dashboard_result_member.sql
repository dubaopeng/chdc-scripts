SET mapred.job.name='dashboard_result_member';
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
----2.首页会员等级数据计算
----new_dashboard.dashboard_result_member
----------------------------

drop table if exists new_dashboard.dashboard_result_member_1;
create table if not exists new_dashboard.dashboard_result_member_1(
   dp_id string,
   grade bigint,
   buyernick_count bigint
)ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");

insert overwrite table new_dashboard.dashboard_result_member_1
select
    dp_id,
    grade,
    count(buyer_nick) as buyernick_count
from dw_base.b_top_member
where dp_id is not NULL and grade is not NULL
group by
    dp_id,
    grade;

--会员等级结果
drop table if exists new_dashboard.dashboard_result_member;
create table if not exists new_dashboard.dashboard_result_member(
   shop_id string,
   grade bigint,
   buyernick_count bigint,
   buyernick_rate double,
   modified string,
   stat_date string
)ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS textfile;

insert overwrite table new_dashboard.dashboard_result_member
select
    a.dp_id as shop_id,
    a.grade,
    case when a.buyernick_count = 'NULL' or a.buyernick_count is null then 0 else a.buyernick_count end as buyernick_count,
    case when b.buyernick_count_all = 'NULL' or b.buyernick_count_all is null or b.buyernick_count_all = 0 or a.buyernick_count = 'NULL' or a.buyernick_count is null then 0 else round((a.buyernick_count/b.buyernick_count_all),6) end as buyernick_rate,
    ${hiveconf:MODIFIED} as modified,
    '${stat_date}' as stat_date
from new_dashboard.dashboard_result_member_1 a
left join
    (
    select
        dp_id,
        sum(buyernick_count) as buyernick_count_all
    from new_dashboard.dashboard_result_member_1
    group by dp_id
    )b
on a.dp_id = b.dp_id;

