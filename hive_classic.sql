-----------------------------------------0、命令行工具-----------------------------------------
use ads_inno;
show tables;
show partitions ad.ad_base_impression partition(hour='00',pb='rtb');
desc formatted tablename; -- 查看建表信息
hive -e "xxxx" > ~/shawnma/nuts/nuts_ord.txt
quit; --退出session
dfs -ls path;  -- 查看hdfs路径
dfs -lsr path; -- 递归查看
dfs -du hdfs://BJYZH3-HD-JRJT-4137.jd.com:54310/user/jrjt/warehouse/stage.db/s_h02_click_log; --查看表文件大小
dfs -get /user/jrjt/warehouse/ods.db/o_h02_click_log_i_new/dt=2014-01-21/000212_0 /home/jrjt/testan/; --下载文件到某个目录

-----------------------------------------1、建表,DDL,数据导入-------------------------------------------
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

--【创建分区表】
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


--【创建ORC压缩表】
 CREATE TABLE if not exists daysOnHandTableWarehouse_sample (
            dim_subd_num                   string      COMMENT     '分公司维编号',
            dim_subd_name                  string      COMMENT     '分公司维名称',
            dim_delv_center_name           string      COMMENT     '配送中心维名称',
            delv_center_num                string      COMMENT     '配送中心编号',
            item_sku_id                    string      COMMENT     '商品SKU编号'
        )
COMMENT '联合利华库存大表sample'
PARTITIONED BY ( dt string )
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
stored as orc
LOCATION 'hdfs://ns3/user/jd_ad/ads_polaris/daysOnHandTableWarehouse_sample';


-- 【copy schema from existing table】
create table ads_inno.tmp_lux_idfamd5 like ads_inno.tmp_3c_idfamd5

                                                                              
-----------------------------------------2、Data Manipulation-------------------------------------------

-- 【修改列名】只能一列一列来
ALTER TABLE table_name CHANGE [COLUMN] col_old_name col_new_name column_type [COMMENT col_comment] [FIRST|AFTER column_name]
alter table shawn_mars CHANGE COLUMN idfa_md5 device_type INT;

-- 【增加列】
alter table shawn_qm_phone2pin_ext add columns(
is_female smallint,
is_jiaju smallint,
is_muyin smallint,
is_related_brd smallint);


-- 【覆盖原来的结果】
insert overwrite table a
select * from b distribute by user_pin,device_type sort by device_type asc,updatetime desc;

-- 删除符合某些条件的数据,用insert overwrite实现
insert overwrite table shawn_milka_imp_freq 
select * from shawn_milka_imp_freq where pin !='';

-- 不退出hive环境，导出数据到本地文件系统 
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

--【multi-insert】
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

--REGEX Column Specification: 用正则表达式筛选列
SELECT `(ds|hr)?+.+` FROM sales     -- 除了ds, hr其他列都要
																			  
-------------------------------------------3、函数-------------------------------------------
-- 添加UDF
add jar /software/udf/UDFUnionAll.jar;
create temporary function sysdate as 'com.jd.bi.hive.udf.SysDate';

-----------------------------------------3.1.数学 -------------------------------------------																			  
-- 随机抽样数据
select * from (select var, rand(123) as rd from table_a ) table_b where rd between 0.1 and 0.2;

																			  
																			  
																			  

----------------------------------------3.2.窗口分析-------------------------------------------
作用是能够做移动窗口的分析，比如三天内下单金额的平均值。 
注意：over()不会做任何aggregation，行数与原始table一致。
function() over(partition by col order by col_val)  -- 指定分组字段
function() over(order by col_val desc);   -- 省略partition，就不做分组
-- ROW_NUMBER: 每一个行打一个递增行号，如1，2，3，4
select
	item_third_cate_cd,
	item_third_cate_name,
	sales,
	row_number() over(partition by item_third_cate_cd order by sales desc) as rank
from
	gdm.gdm_m03_item_sku_da;

-- RANK: 遇到并列的时候，留空档序号，如1，2，2，4
rank() over(partition by item_third_cate_cd order by sales desc) rank;

-- DENSE_RANK: 遇到并列的时候，不留空档序号，如1，2，2，3																			  
dense_rank() over(partition by item_third_cate_cd order by sales desc) rank;

-- NTILE(n): 将指定分区内的数据分成n份。作用是非常方便得提出符合某一tile的记录
SELECT cookieid,createtime,pv,NTILE(3) OVER(PARTITION BY cookieid ORDER BY pv DESC) AS rn FROM lxw1234;

-- FIRST_VALUE(col,false), LAST_VALUE：找出分区中的第一个/最后一个值。第二个参数表示是否忽略null
																  

-- SUM() OVER(): 排序后，到第n位的running total. 如果有并列，并列的行返回相同的计算结果。
-- 同理，over() 前面还可以加AVG(),COUNT(),MIN(),MAX()
select cookieid,site_id,createtime, pv,sum(pv) over(partition by cookieid order by createtime) from shawn_learnhive
																			  
-- window 子句
select cookieid,createtime, pv,sum(pv) over w from shawn_learnhive WINDOW w as (PARTITION BY cookieid ORDER BY createtime ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
-- 计算到当前行，近3天的pv总和                                                  
----------------------------------------3.3. UDTF-------------------------------------------
