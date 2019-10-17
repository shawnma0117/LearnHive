------------------------------------------------------------------------------------------
--###                               Part0：命令行工具                                 ###--
-----------------------------------------------------------------------------------------
use ads_inno;
show tables;
show tables like 'shawn_*'; -- 模糊搜索表
show partitions ad.ad_base_impression partition(hour='00',pb='rtb');
desc formatted tablename; -- 查看建表信息
hive -e "xxxx" > ~/shawnma/nuts/nuts_ord.txt
quit; --退出session
dfs -ls path;  -- 查看hdfs路径
dfs -lsr path; -- 递归查看
dfs -du hdfs://BJYZH3-HD-JRJT-4137.jd.com:54310/user/jrjt/warehouse/stage.db/s_h02_click_log; --查看表文件大小
dfs -get /user/jrjt/warehouse/ods.db/o_h02_click_log_i_new/dt=2014-01-21/000212_0 /home/jrjt/testan/; --下载文件到某个目录

------------------------------------------------------------------------------------------
--###                               Part0：常用配置项                                 ###--
-----------------------------------------------------------------------------------------
SET hive.exec.parallel
SET hive.groupby.orderby.position.alias=false;






------------------------------------------------------------------------------------------
--###                           Part1：建表,DDL,数据导入                               ###--
-----------------------------------------------------------------------------------------

--【从hdfs导入】
create table if not exists ads_inno.temp_nuts_pin
(
  user_log_acct  string  comment 'jd pin' 
)row format delimited fields terminated by '\t' lines terminated by '\n';

load data inpath 'hdfs://ns3/user/jd_ad/ads_inno/shawnma/nuts/ordpin.txt' into table temp_nuts_pin;
--【导入本地数据】
create table if not exists tmp_imp_sample
(
  pt string,
  pos_id int,
  ad_plan_id bigint,
  advertise_pin string,
  advertiser_id bigint,
  advertiser_type int,
  user_pin string,
  user_ip string,
  impress_time timestamp
)row format delimited fields terminated by '\t' lines terminated by '\n';

load data local inpath '/home/ads_inno/shawnma/heinz/impression_smpl.txt' into table tmp_imp_sample; 

--【insert into】
create table if not exists tmp_imp_sample_rtb
(
  pos_id int,
  ad_plan_id bigint,
  advertise_pin string,
  advertiser_id bigint,
  advertiser_type int,
  user_pin string,
  user_ip string,
  impress_time timestamp,
  mobile_type int,
  device_id string,
  device_type int
)row format delimited fields terminated by '\t' lines terminated by '\n';

insert into table tmp_imp_sample_rtb
select pos_id,ad_plan_id,advertise_pin,advertiser_id, advertiser_type, user_pin,user_ip,from_unixtime(impress_time), mobile_type, device_id,device_type 
from ad.ad_base_impression TABLESAMPLE(0.01 PERCENT) s where business_type=64 and dt='2017-08-16' and pt='rtb';

-- 【create table as select】
CREATE TABLE IF NOT EXISTS blift_impress AS 
    select
            user_pin   ,
            user_id    ,
            device_id  ,
            device_type,
            from_unixtime(impress_time) as imp_ymdhms
    from
            ad.ad_waidan_impression
    where
            advertiser_id = 629155
            and ad_plan_id in(105070068, 105092551) 
            and dmp_id in (436183,436184,498928,531667,523542)
            and dt >= '2017-08-15'
            and dt <= '2017-10-02';
			
--【创建外表，指定location为hdfs地址】
create external table if not exists shawn_milka_clk_withpin
(
  campaignid              bigint ,                                     
  user_id                 string ,                                     
  device_id               string ,                                     
  click_id                  string ,                                    
  click_time            bigint ,                                     
  mobile_type             int ,                                       
  media_name              string ,                                    
  trade_type              string ,                                   
  ad_date                 string ,                                     
  pin                     string ,                                     
  update_time             string )
row format delimited fields terminated by '\t' lines terminated by '\n'
LOCATION 'hdfs://ns1018/user/jd_ad/ads_inno/ads_inno.db/shawn_milka_clk_withpin';

--【创建分区表，并使用动态分区插入】
create table if not exists shawn_buffalo_test
(
  user_pin string,
  impress_time bigint,
  impress_dt string,
  user_log_acct string,
  sale_ord_dt string ,
  item_sku_id string
  )
partitioned by (dt string)
row format delimited fields terminated by '\t' 
lines terminated by '\n';

set hive.exec.dynamic.partition=true; -- 需要打开动态分区配置
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table shawn_buffalo_test partition (dt)
select ..., dt from  xxx   -- 分区字段必须放在最后，动态分区是按照位置读取的

--【创建ORC压缩表】
-- 相比textfile, orc表能实现70%的空间压缩，且因为是列式存储，读取效率更高
CREATE EXTERNAL TABLE IF NOT EXISTS ads_inno.sony_mz_click_data_orc
(
    ad_plan_id bigint COMMENT '计划id',
    advertise_pin string COMMENT '广告主pin',
    advertiser_id bigint COMMENT '广告主id',
    advertiser_type int COMMENT '广告主类型',
    ad_spread_type int COMMENT '站内外类型',
    merchant_id bigint,
    k string COMMENT '秒针活动ID',
    p string COMMENT '秒针点位ID',
    click_time bigint COMMENT '点击时间',
    device_type string COMMENT '设备类型',
    device_id string COMMENT '设备id',
    click_id string COMMENT '唯一点击标识'
)
PARTITIONED BY (dt string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
 WITH SERDEPROPERTIES (
 'field.delim'='\t','escape.delim'='\n',
 'serialization.null.format'=''
 ) STORED AS orc;


-- 【copy schema from existing table】
create table ads_inno.tmp_lux_idfamd5 like ads_inno.tmp_3c_idfamd5

                                                                              
--------------------------------------------------------------------------------------
--###                                  Part2：DML                                ###--
--------------------------------------------------------------------------------------

-- 【修改列名】只能一列一列来
ALTER TABLE table_name CHANGE [COLUMN] col_old_name col_new_name column_type [COMMENT col_comment] [FIRST|AFTER column_name]
alter table shawn_mars CHANGE COLUMN idfa_md5 device_type INT;

-- 【删除分区】
alter table tmp_h02_click_log_baitiao drop partition(dt='2014-03-01'); 

-- 【增加列】
alter table shawn_qm_phone2pin_ext add columns(
is_female smallint,
is_jiaju smallint,
is_muyin smallint,
is_related_brd smallint);


-- 【覆盖原来的结果】
insert overwrite table a
select * from b distribute by user_pin,device_type sort by device_type asc,updatetime desc;

-- 【删除符合某些条件的数据】用insert overwrite实现
insert overwrite table shawn_milka_imp_freq 
select * from shawn_milka_imp_freq where pin !='';

-- 【不退出hive环境，导出数据到本地文件系统】
-- 但只能指定一个路径，无法指定名称。
insert overwrite local directory '/home/wyp/wyp'
ROW FORMAT DELIMITED FIELDS TERMINATED BY','
select * from wyp;

-- 【map side join】
set hive.auto.convert.join = true ;

select campaign,user_pin,count(distinct imp_id) as imp_cnt from 
(select /*+MAPJOIN(b)*/ b.pin as user_pin,a.user_id,a.imp_id,a.campaign from (select * from ads_inno.shawn_intel_global_imp where user_pin in ('','nobody')) a JOIN (select uuid,pin from app.app_uuid_pin_mapping where dt='2017-12-20') b on a.user_id=b.uuid
union all 
select user_pin,user_id,imp_id,campaign from ads_inno.shawn_intel_global_imp where user_pin not in ('','nobody')
) c group by campaign,user_pin

-- 【multi-insert】
----适用于：从同一个数据源，根据不同条件，插入到同个结果表的不同分区，或是不同表中。
use ads_inno;
create table if not EXISTS shawn_intel_global_imp (
    user_pin string,
    user_id string,
    device_id string,
    device_type int ,
    mobile_type int,
    imp_id string,
    impress_time int,
    ad_plan_id string,
    dt string
)
partitioned by (campaign string)
row format delimited 
fields terminated by '\t' 
lines terminated by '\n';

from ad.ad_waidan_impression
insert overwrite table ads_inno.shawn_intel_global_imp partition (campaign='bts')
  select user_pin,user_id,device_id,device_type,mobile_type,imp_id,impress_time,ad_plan_id,dt 
  where advertise_pin='OMD-Beijing' and dt>='2017-08-20' and dt<='2017-09-05' and (user_id !='' OR user_pin !='') and ad_plan_id in (105569750,105780504) and is_bill != '1'
insert overwrite table ads_inno.shawn_intel_global_imp partition (campaign='nov')
  select user_pin,user_id,device_id,device_type,mobile_type,imp_id,impress_time,ad_plan_id,dt 
  where advertise_pin='OMD-Beijing' and dt>='2017-11-01' and dt<='2017-11-20' and (user_id !='' OR user_pin !='') and ad_plan_id in (108364794,108527561) and is_bill != '1'

-- 【REGEX Column Specification: 用正则表达式筛选列】
set hive.support.quoted.identifiers=none;
SELECT `(ds|hr)?+.+` FROM sales     -- 除了ds, hr其他列都要

-- 【指定某几个字段分组，并在组内按照1个或多个值排序】
select key1, key2, value1, value2, value3 from my_table distribute by key1,key2 sort by key1,key2,value2
-- 注意：如果想要相同key被归到一起，必须把他们也写在sort by中
									      
-- 【COMMON TABLE EXPRESSION】
-- 如果有多个不同query要从同一份中间数据计算，可以用CTE先把中间结果定义出来，后续多次引用，省代码量。
with q1 as ( select key from src where key = '5')
select *
from q1;
 
-- from style
with q1 as (select * from src where key= '5')
from q1
select *;
  
-- chaining CTEs
with q1 as ( select key from q2 where key = '5'),
q2 as ( select key from src where key = '5')
select * from (select key from q1) a;
  
-- union example
with q1 as (select * from src where key= '5'),
q2 as (select * from src s2 where key = '4')
select * from q1 union all select * from q2;

-- insert example
create table s1 like src;
with q1 as ( select key, value from src where key = '5')
from q1
insert overwrite table s1
select *;
 
-- ctas example
create table s2 as
with q1 as ( select key from src where key = '4')
select * from q1;
 
-- view example
create view v1 as
with q1 as ( select key from src where key = '5')
select * from q1;
select * from v1;									      
																			  
--------------------------------------------------------------------------------------
--###                                 Part3：函数                                 ###--
--------------------------------------------------------------------------------------

--#.  添加UDF
add jar /software/udf/UDFUnionAll.jar;
create temporary function sysdate as 'com.jd.bi.hive.udf.SysDate';

--#.  时间戳处理
  
from_unixtime(bigint unixtime[, string format]) -- 时间戳转格式日期 

unix_timestamp(string date) -- 日期转时间戳  unix_timestamp('2009-03-20 11:30:01') = 1237573801
unix_timestamp(string date, string pattern)

--#.  数学函数 															  
-- 随机抽样数据
rand()
select * from (select var, rand(123) as rd from table_a ) table_b where rd between 0.1 and 0.2;

--#.  文本处理函数
-- 包含字符串中是否包含某些子字符串														  
key_word rlike '(精华|精华液|面部精华|小黑瓶|青春密码|青春密码酵素|欧莱雅面部精华|蔡徐坤青春密码|欧莱雅蔡徐坤|蔡徐坤小黑瓶)' 	

-- 判断字段是否为空
length(user_Log_acct) > 0 -- 比较巧妙的方法。相当于 user_log_acct is not null and user_log_acct != ''												  
																			  

--#.  窗口分析函数
注意：over()不会做任何aggregation，行数与原始table一致。
function() over(partition by col order by col_val)  -- 指定分组字段
function() over(order by col_val desc);   -- 省略partition，就不做分组

-- ROW_NUMBER: 每一个行打一个递增行号，如1，2，3，4
select
	item_third_cate_cd,
	item_third_cate_name,
	sales,
	row_number() over(partition by item_third_cate_cd order by sales desc) as rank -- 每个三级品类下按照销量倒序排列
from
	gdm.gdm_m03_item_sku_da;

select *, row_number() over(partition by 1) from shawn_learnhive -- 给所有数据加行号

-- RANK(): 遇到并列的时候，留空档序号，如1，2，2，4
rank() over(partition by item_third_cate_cd order by sales desc) rank;

-- DENSE_RANK: 遇到并列的时候，不留空档序号，如1，2，2，3																			  
dense_rank() over(partition by item_third_cate_cd order by sales desc) rank;

-- NTILE(n): 将指定分区内的数据分成n份。作用是非常方便得提出符合某一tile的记录
SELECT cookieid,createtime,pv,NTILE(3) OVER(PARTITION BY cookieid ORDER BY pv DESC) AS rn FROM lxw1234;

-- FIRST_VALUE(col,false), LAST_VALUE：找出分区中的第一个/最后一个值。第二个参数表示是否忽略null
select distinct a.* from (select cookieid,first_value(createtime) over(partition by cookieid order by pv) from shawn_learnhive) a
-- 窗口函数返回的结果与原表行数相同，如果想去掉多余数据，需要加一个subquery来group by，不可在原始query上执行，否则会报错													  

-- SUM(), AVG(),COUNT(),MIN(),MAX() 与窗口分析结合
SUM(a) OVER(PARTITION BY b ORDER BY c desc) -- 排序后，到当前行的running total. 如果有并列行，返回相同结果。使用场景：计算累计和。
SUM(a) OVER(PARTITION BY b ORDER BY c desc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) --和前一种写法等效。
SUM(a) OVER(PARTITION BY b)                 -- 无排序的情况下，结果是计算每个分组的总和，同个分组内每行结果相同。使用场景：计算每个种类的占比。
SUM(pv) OVER(PARTITION BY cookieid ORDER BY createtime ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS pv4, --当前行+往前3行
SUM(pv) OVER(PARTITION BY cookieid ORDER BY createtime ROWS BETWEEN 3 PRECEDING AND 1 FOLLOWING) AS pv5, --当前行+往前3行+往后1行
SUM(pv) OVER(PARTITION BY cookieid ORDER BY createtime ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS pv6 ---当前行+往后所有行

																			  
-- WINDOW 子句语法
select cookieid,createtime, pv,sum(pv) over w from shawn_learnhive WINDOW w as (PARTITION BY cookieid ORDER BY createtime ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
-- 计算到当前行，近3天的pv总和

-- ROLLUP: 维度由粗到细聚合，可以做上卷分析
select dim_1, dim_2, count(distinct value), GROUPING__ID from table_a group by dim_1,dim_2 with ROLLUP
-- GROUPING_SETS: 按照指定维度组合进行聚合，
select dim_1, dim_2, count(distinct value), GROUPING__ID from table_a group by dim_1,dim_2 GROUPING SETS(dim_1,dim_2,(dim_1,dim_2))
-- CUBE: 根据group by的所有维度的组合进行聚合
select dim_1, dim_2, count(distinct value), GROUPING__ID from table_a group by dim_1,dim_2 with CUBE ORDER BY GROUPING__ID


--#.  行列转换

-- 将一列多行转为一行多列. aka：long format --> wide format
-- collect_set
select id_var,value_var[0],value_var[1] 
from (
select id_var,collect_set(value_var) as value_var from table_a group by id_var
) a;
