SET mapred.job.name='b_std_plat_customer-平台与客户数据同步作业';
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


-- 全渠道客户与平台数据每日增量表
CREATE EXTERNAL TABLE IF NOT EXISTS dw_source.`s_std_plat_customer`( 
    `uni_id` string, 
    `plat_code` string, 
    `plat_account` string,
    `plat_nick` string,
    `partner` string,
    `tenant` string,
    `plat_avatar` string
)
partitioned by(`dt` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS TEXTFILE 
LOCATION '/user/hive/source_data/std_source/plat_customer';

-- 批量重设分区
msck repair table dw_source.`s_std_plat_customer`;


-- 全渠道平台客户表，以租户和平台进行分区
CREATE TABLE IF NOT EXISTS dw_base.`b_std_plat_customer`( 
    `uni_id` string, 
    `plat_code` string, 
    `plat_account` string,
    `plat_nick` string,
    `partner` string,
    `tenant` string,
    `plat_avatar` string
)
partitioned by(tenantid string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS RCFILE;


-- 第一步，创建增量数据中去重后的数据表
DROP TABLE IF EXISTS dw_source.`s_std_plat_customer_temp`;
CREATE TABLE IF NOT EXISTS dw_source.`s_std_plat_customer_temp`( 
    `uni_id` string, 
    `plat_code` string, 
    `plat_account` string,
    `plat_nick` string,
    `partner` string,
    `tenant` string,
    `plat_avatar` string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS RCFILE;

-- 第二步：对今日的数据进行去重，插入临时表
insert into dw_source.s_std_plat_customer_temp 
select t.uni_id,t.plat_code,t.plat_account,t.plat_nick,t.partner,t.tenant,t.plat_avatar from (
select *,row_number() over (partition by plat_code,plat_account,partner,tenant) as num 
from dw_source.`s_std_plat_customer` where dt='${today}') t where t.num=1;


-- 第三步：查询出全量表与本次增量数据的差集，然后和增量数据合并，就完成了会员数据的增加和更新
insert overwrite table dw_base.b_std_plat_customer partition(tenantid)  
SELECT  
    t.uni_id,
	t.plat_code,
    t.plat_account,
    t.plat_nick,
    t.partner,
	t.tenant,
    t.plat_avatar,
    t.tenant as tenantid
FROM
    (SELECT 
        bqp.uni_id,
		bqp.plat_code,
		bqp.plat_account,
		bqp.plat_nick,
		bqp.partner,
		bqp.tenant,
		bqp.plat_avatar
    FROM
        (SELECT 
			t2.uni_id,
            t2.plat_code,
            t2.plat_account,
            t2.plat_nick,
            t2.partner,
            t2.tenant,
            t2.plat_avatar,
            CASE
                WHEN t2.plat_code = 'OFFLINE' THEN t2.partner
                ELSE t2.plat_code
            END AS defaupartner
    FROM
        dw_base.b_std_plat_customer t2) bqp
    LEFT OUTER JOIN (SELECT 
        t1.uni_id,
            t1.plat_code,
            t1.plat_account,
            t1.plat_nick,
            t1.partner,
            t1.tenant,
            t1.plat_avatar,
            CASE
                WHEN t1.plat_code = 'OFFLINE' THEN t1.partner
                ELSE t1.plat_code
            END AS defaupartner
    FROM
        dw_source.s_std_plat_customer_temp t1) bqpt 
    ON (bqp.plat_code = bqpt.plat_code
        AND bqp.plat_account = bqpt.plat_account
        AND bqp.defaupartner = bqpt.defaupartner)
    WHERE
        bqpt.plat_code IS NULL
            AND bqpt.plat_account IS NULL
            AND bqpt.defaupartner IS NULL) t 
UNION ALL SELECT 
    tmp.uni_id,
    tmp.plat_code,
    tmp.plat_account,
    tmp.plat_nick,
    tmp.partner,
	tmp.tenant,
    tmp.plat_avatar,
    tmp.tenant as tenantid
FROM
    dw_source.s_std_plat_customer_temp tmp;
	
DROP TABLE IF EXISTS dw_source.`s_std_plat_customer_temp`;
