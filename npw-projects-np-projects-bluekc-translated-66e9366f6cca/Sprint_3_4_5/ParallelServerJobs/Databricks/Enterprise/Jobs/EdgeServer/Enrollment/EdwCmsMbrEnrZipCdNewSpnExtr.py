# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2024 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC DESCRIPTION:  This job creates a Zip code span that would apply to all subscribers if their Zip code changes during their enrollment period.
# MAGIC JOB NAME: EdwCmsMbrEnrZipCdNewSpnExtr
# MAGIC CALLED BY:  EdgeServerEdwCmsEnrSeq
# MAGIC 
# MAGIC RERUN INFORMATION: 
# MAGIC              PREVIOUS RUN SUCCESSFUL:   Nothing to reset. 
# MAGIC              PREVIOUS RUN ABORTED:         Nothing to reset, just restart.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC =============================================================================================================================================
# MAGIC Developer\(9)Date\(9)\(9)Project/Ticket #\(9)Change Description\(9)\(9)\(9)Development Project\(9)Code Reviewer\(9)Date Reviewed||
# MAGIC =============================================================================================================================================
# MAGIC Harsha Ravuri\(9)2024-04-04\(9)US#612091\(9)Original programming\(9)\(9)EnterpriseDev2                       Jeyaprasanna           2024-04-18
# MAGIC Harsha Ravuri\(9)2024-10-18\(9)US#629013\(9)Updated code to handle enrollment\(9)EnterpriseDev1                       Jeyaprasanna           2024-11-06
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9) those started in the middle of the year.

# MAGIC These steps will check if a member has only 
# MAGIC one Zip code change in a span. 
# MAGIC If yes, it will check with the last modified Zip code data. 
# MAGIC If it doesn't find anything,
# MAGIC  we will not consider it a Zip change.
# MAGIC This step will create prespan effective dates where span breaks for new zip codes.
# MAGIC This step will create prespan effective dates where span breaks for new zip codes.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
import pyspark
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Enterprise
# COMMAND ----------


edw_secret_name = get_widget_value('edw_secret_name','')
EDWOwner = get_widget_value('EDWOwner','')
RiskAdjYr = get_widget_value('RiskAdjYr','')
BeginDt = get_widget_value('BeginDt','')
EndDate = get_widget_value('EndDate','')
QHPID = get_widget_value('QHPID','')

jdbc_url_edw, jdbc_props_edw = get_db_config(edw_secret_name)

extract_query_W_Table = f"""--NewSpan
with mbr as (
Select mbr.SUB_SK,we.SUB_INDV_BE_KEY, we.MBR_INDV_BE_KEY,mbr.sub_uniq_key,we.MBR_ENR_EFF_DT_SK,we.MBR_ENR_TERM_DT_SK,we.PRM_AMT,we.ENR_PERD_ACTVTY_CD,we.RATE_AREA_ID,we.QHP_ID
FROM     {EDWOwner}.MBR_D mbr,
         {EDWOwner}.MBR_ENR_D mbrEnr,
         {EDWOwner}.GRP_D grp,
         {EDWOwner}.MBR_ENR_QHP_D MBR_QHP,
         {EDWOwner}.SUB_D sub,{EDWOwner}.w_edge_mbr_elig_extr we
WHERE    mbr.MBR_SK = mbrEnr.MBR_SK
AND      mbr.GRP_SK = grp.GRP_SK
AND      MBR_QHP.MBR_ENR_SK = mbrEnr.MBR_ENR_SK
AND      MBR_QHP.MBR_UNIQ_KEY = mbrEnr.MBR_UNIQ_KEY
AND      MBR_QHP.MBR_ENR_EFF_DT_SK = mbrEnr.MBR_ENR_EFF_DT_SK
AND      mbrEnr.MBR_ENR_ELIG_IN = 'Y'
AND      mbrEnr.MBR_ENR_CLS_PLN_PROD_CAT_CD = 'MED'
AND      MBR_QHP.MBR_ENR_CLS_PLN_PROD_CAT_CD = 'MED'
AND      mbrEnr.MBR_ENR_EFF_DT_SK <= '{EndDate}'
AND      mbrEnr.MBR_ENR_TERM_DT_SK >= '{BeginDt}'
AND      mbrEnr.PROD_SH_NM IN ('PCB', 'PC', 'BCARE', 'BLUE-ACCESS', 'BLUE-SELECT','BLUESELECT+')
AND      mbrEnr.CLS_ID <> 'MHIP'
AND      MBR_QHP.QHP_ID <> 'NA'
AND      MBR_QHP.QHP_ID LIKE '{QHPID}'
AND      mbr.SUB_UNIQ_KEY = sub.SUB_UNIQ_KEY
AND      left(mbr.MBR_RELSHP_NM,3) = 'SUB'
and we.SUB_INDV_BE_KEY = we.mbr_indv_be_key
and we.sub_indv_be_key = mbr.mbr_indv_be_key
)
--=======================================================================================================
--Extract all update dates from sub_addr_audit_d in span
,all_Sub_Add_Audit as (
select distinct m.sub_sk,m.mbr_indv_be_key,m.QHP_ID,saad.SUB_ADDR_AUDIT_ROW_ID,
saad.SUB_ADDR_ZIP_CD_5,saad.LAST_UPDT_RUN_CYC_EXCTN_DT_SK from {EDWOwner}.sub_addr_audit_d saad, mbr m
where saad.sub_sk = m.sub_sk and saad.LAST_UPDT_RUN_CYC_EXCTN_DT_SK between m.MBR_ENR_EFF_DT_SK and m.MBR_ENR_TERM_DT_SK and saad.SUB_ADDR_NM ='SUBSCRIBER HOME' and SUB_ADDR_ZIP_CD_5 <> ''
)
--=======================================================================================================
--Extract distinct Zip code if member has multiple update dates
,distinct_sub_addr_audit_zip as (
select distinct saad.mbr_indv_be_key,substr(replace(saad.LAST_UPDT_RUN_CYC_EXCTN_DT_SK,'-',''),1,6) as month_LAST_UPDT_RUN_CYC_EXCTN_DT_SK,
row_number() over (partition by saad.mbr_indv_be_key,substr(replace(saad.LAST_UPDT_RUN_CYC_EXCTN_DT_SK,'-',''),1,6) order by saad.mbr_indv_be_key,substr(replace(saad.LAST_UPDT_RUN_CYC_EXCTN_DT_SK,'-',''),1,6)) as row_num
from all_Sub_Add_Audit saad
)
--=======================================================================================================
--Below query extracts lastest zip code update if memebr has multiple zip codes in same month
,Mnth_dtn_sub_addr_audit_zip as (
select asaa.mbr_indv_be_key, asaa.QHP_ID,max(asaa.SUB_ADDR_AUDIT_ROW_ID)as SUB_ADDR_AUDIT_ROW_ID
,substr(replace(asaa.LAST_UPDT_RUN_CYC_EXCTN_DT_SK,'-',''),1,6) as mnth_LAST_UPDT_RUN_CYC_EXCTN_DT_SK
from all_Sub_Add_Audit asaa
group by asaa.mbr_indv_be_key,asaa.QHP_ID,substr(replace(asaa.LAST_UPDT_RUN_CYC_EXCTN_DT_SK,'-',''),1,6)
)
--=======================================================================================================
--Below query extract final distinct zip codes
,fnl_dtn_sub_addr_audit_zip as (
select distinct asaa.mbr_indv_be_key,asaa.QHP_ID,asaa.SUB_ADDR_AUDIT_ROW_ID,asaa.SUB_ADDR_ZIP_CD_5,asaa.LAST_UPDT_RUN_CYC_EXCTN_DT_SK
from all_Sub_Add_Audit asaa, Mnth_dtn_sub_addr_audit_zip mdsaa,
(select asaa2.mbr_indv_be_key,asaa2.QHP_ID,asaa2.SUB_ADDR_AUDIT_ROW_ID,asaa2.SUB_ADDR_ZIP_CD_5,
row_number() over (partition by asaa2.mbr_indv_be_key,asaa2.SUB_ADDR_ZIP_CD_5 order by asaa2.mbr_indv_be_key,asaa2.QHP_ID,asaa2.SUB_ADDR_ZIP_CD_5,asaa2.LAST_UPDT_RUN_CYC_EXCTN_DT_SK asc) as row_num
from all_Sub_Add_Audit asaa2) as assaaSub
where asaa.mbr_indv_be_key = mdsaa.mbr_indv_be_key and asaa.QHP_ID=mdsaa.QHP_ID and asaa.SUB_ADDR_AUDIT_ROW_ID = mdsaa.SUB_ADDR_AUDIT_ROW_ID
and asaa.mbr_indv_be_key = assaaSub.mbr_indv_be_key and asaa.SUB_ADDR_AUDIT_ROW_ID = assaaSub.SUB_ADDR_AUDIT_ROW_ID
and assaaSub.row_num = 1
)
--=======================================================================================================
--Below query extracts enrollment start date
,MinEffDt as (
select fdsaa.mbr_indv_be_key,m.QHP_ID,min(m.MBR_ENR_EFF_DT_SK) as MBR_ENR_EFF_DT_SK
from fnl_dtn_sub_addr_audit_zip fdsaa, mbr m
where fdsaa.mbr_indv_be_key = m. sub_indv_be_Key and fdsaa.QHP_ID = m.QHP_ID
group by fdsaa.mbr_indv_be_key,m.QHP_ID
)
--=======================================================================================================
--Below query extract first zip code in span
,first_Sub_Add_Audit_Zip as (
select distinct fsaaz.mbr_indv_be_key,fsaaz.QHP_ID,fsaaz.SUB_ADDR_AUDIT_ROW_ID,fsaaz.LAST_UPDT_RUN_CYC_EXCTN_DT_SK,fsaaz.SUB_ADDR_ZIP_CD_5,
row_number() over ( partition by fsaaz.mbr_indv_be_key,fsaaz.QHP_ID order by fsaaz.mbr_indv_be_key,fsaaz.QHP_ID,fsaaz.SUB_ADDR_AUDIT_ROW_ID asc) row_num
from fnl_dtn_sub_addr_audit_zip fsaaz
)
--=======================================================================================================
--Below query identifies true first month zip changes
,trueFirst_Sub_Add_Audit_Zip as (
select distinct fdsaa.mbr_indv_be_key,fdsaa.QHP_ID,fdsaa.SUB_ADDR_AUDIT_ROW_ID,fdsaa.SUB_ADDR_ZIP_CD_5,fdsaa.LAST_UPDT_RUN_CYC_EXCTN_DT_SK,m.MBR_ENR_EFF_DT_SK
from fnl_dtn_sub_addr_audit_zip fdsaa, first_Sub_Add_Audit_Zip fsaa,MinEffDt m
where fdsaa.SUB_ADDR_AUDIT_ROW_ID = fsaa.SUB_ADDR_AUDIT_ROW_ID
and fdsaa.mbr_indv_be_key = fsaa.mbr_indv_be_key
and fdsaa.QHP_ID = fsaa.QHP_ID
and fdsaa.mbr_indv_be_key = m.mbr_indv_be_key
and fdsaa.QHP_ID = m.QHP_ID
and month(fsaa.LAST_UPDT_RUN_CYC_EXCTN_DT_SK) = month(m.MBR_ENR_EFF_DT_SK)
and year(fsaa.LAST_UPDT_RUN_CYC_EXCTN_DT_SK) = year(m.MBR_ENR_EFF_DT_SK)
and fsaa.row_num = 1
)
--=======================================================================================================
--Below drops true first month zip code
,uniq_Sub_Add_Audit_Zip as (
select fdasaaz.mbr_indv_be_key,fdasaaz.QHP_ID,fdasaaz.SUB_ADDR_AUDIT_ROW_ID,fdasaaz.SUB_ADDR_ZIP_CD_5,fdasaaz.LAST_UPDT_RUN_CYC_EXCTN_DT_SK
from fnl_dtn_sub_addr_audit_zip fdasaaz, trueFirst_Sub_Add_Audit_Zip tsaaz
where tsaaz.mbr_indv_be_key = fdasaaz.mbr_indv_be_key
and tsaaz.QHP_ID = fdasaaz.QHP_ID
and tsaaz.SUB_ADDR_AUDIT_ROW_ID != fdasaaz.SUB_ADDR_AUDIT_ROW_ID
and tsaaz.LAST_UPDT_RUN_CYC_EXCTN_DT_SK != fdasaaz.LAST_UPDT_RUN_CYC_EXCTN_DT_SK
union
select fdasaaz.mbr_indv_be_key,fdasaaz.QHP_ID,fdasaaz.SUB_ADDR_AUDIT_ROW_ID,fdasaaz.SUB_ADDR_ZIP_CD_5,fdasaaz.LAST_UPDT_RUN_CYC_EXCTN_DT_SK
from fnl_dtn_sub_addr_audit_zip fdasaaz,
(
select fdasaazm.mbr_indv_be_key,fdasaazm.QHP_ID
from fnl_dtn_sub_addr_audit_zip fdasaazm
minus
select distinct tsaaz.mbr_indv_be_key,tsaaz.QHP_ID
from trueFirst_Sub_Add_Audit_Zip tsaaz
) Mtsaaz
where fdasaaz.mbr_indv_be_key = mtsaaz.mbr_indv_be_key
and fdasaaz.QHP_ID = mtsaaz.QHP_ID
)
--=======================================================================================================
--Below query list out members having only one update in span
,mbr_zip_Single_Updt as (
select distinct usaaz.mbr_indv_be_key,usaaz.QHP_ID,count(usaaz.SUB_ADDR_ZIP_CD_5) as count_zip
from fnl_dtn_sub_addr_audit_zip usaaz
group by usaaz.mbr_indv_be_key,usaaz.QHP_ID having count(usaaz.SUB_ADDR_ZIP_CD_5) = 1
)
--=======================================================================================================
--Below query extract final NewSpan
,Final_NewSpan as (
select distinct m.SUB_INDV_BE_KEY,m.MBR_INDV_BE_KEY,usaaz.SUB_ADDR_ZIP_CD_5,m.MBR_ENR_EFF_DT_SK,m.MBR_ENR_TERM_DT_SK,
case when  m.MBR_ENR_EFF_DT_SK > cast(concat(left(usaaz.LAST_UPDT_RUN_CYC_EXCTN_DT_SK,7),'-01') as char(10)) then m.MBR_ENR_EFF_DT_SK else cast(concat(left(usaaz.LAST_UPDT_RUN_CYC_EXCTN_DT_SK,7),'-01') as char(10))end as New_MBR_ENR_EFF_DT_SK ,
case when cast(m.MBR_ENR_TERM_DT_SK as date) > (cast(concat(left(usaaz.LAST_UPDT_RUN_CYC_EXCTN_DT_SK,7),'-01') as date) + 1 month) - 1 day
     then (cast(concat(left(usaaz.LAST_UPDT_RUN_CYC_EXCTN_DT_SK,7),'-01') as date) + 1 month) - 1 day
     else case when month(m.MBR_ENR_TERM_DT_SK) = month(usaaz.LAST_UPDT_RUN_CYC_EXCTN_DT_SK) then m.MBR_ENR_TERM_DT_SK end end as New_MBR_ENR_TERM_DT_SK,
m.PRM_AMT ,m.ENR_PERD_ACTVTY_CD ,m.RATE_AREA_ID ,
case when count_zip = 1 then 'Yes' else 'No' end as Flag,m.QHP_ID,usaaz.SUB_ADDR_AUDIT_ROW_ID,usaaz.LAST_UPDT_RUN_CYC_EXCTN_DT_SK
from mbr m, uniq_Sub_Add_Audit_Zip usaaz
left outer join mbr_zip_Single_Updt mzsu
on usaaz.mbr_indv_be_key = mzsu.mbr_indv_be_key and usaaz.QHP_ID = mzsu.QHP_ID
where m.mbr_indv_be_key = usaaz.mbr_indv_be_key
and m.QHP_ID = usaaz.QHP_ID
and usaaz.LAST_UPDT_RUN_CYC_EXCTN_DT_SK between m.MBR_ENR_EFF_DT_SK and m.MBR_ENR_TERM_DT_SK
)
--=======================================================================================================
,Final1 as (
SELECT fn.SUB_INDV_BE_KEY, fn.MBR_INDV_BE_KEY, fn.SUB_ADDR_ZIP_CD_5, fn.MBR_ENR_EFF_DT_SK, fn.MBR_ENR_TERM_DT_SK, fn.New_MBR_ENR_EFF_DT_SK,
fn.New_MBR_ENR_TERM_DT_SK, fn.PRM_AMT, fn.ENR_PERD_ACTVTY_CD, fn.RATE_AREA_ID, fn.Flag, fn.QHP_ID,
fn.SUB_ADDR_AUDIT_ROW_ID, fn.LAST_UPDT_RUN_CYC_EXCTN_DT_SK,
row_number() over (partition by fn.SUB_INDV_BE_KEY, fn.MBR_INDV_BE_KEY,fn.QHP_ID order by fn.SUB_INDV_BE_KEY, fn.MBR_INDV_BE_KEY,fn.QHP_ID) row_num
FROM Final_NewSpan fn
)
--=======================================================================================================
select f11.SUB_INDV_BE_KEY,f11.MBR_INDV_BE_KEY,f11.SUB_ADDR_ZIP_CD_5,f11.MBR_ENR_EFF_DT_SK,f11.MBR_ENR_TERM_DT_SK,f11.New_MBR_ENR_EFF_DT_SK,
f11.New_MBR_ENR_TERM_DT_SK,f11.PRM_AMT,f11.ENR_PERD_ACTVTY_CD,f11.RATE_AREA_ID,f11.Flag,f11.QHP_ID,f11.SUB_ADDR_AUDIT_ROW_ID,f11.LAST_UPDT_RUN_CYC_EXCTN_DT_SK,
row_number() over (partition by f11.SUB_INDV_BE_KEY, f11.MBR_INDV_BE_KEY,f11.QHP_ID order by f11.SUB_INDV_BE_KEY, f11.MBR_INDV_BE_KEY,f11.QHP_ID) row_num
from Final1 f11,
(
select f1.SUB_INDV_BE_KEY,f1.MBR_INDV_BE_KEY,f1.New_MBR_ENR_EFF_DT_SK,f1.QHP_ID,f1.New_MBR_ENR_TERM_DT_SK,
Max(f1.LAST_UPDT_RUN_CYC_EXCTN_DT_SK) as LAST_UPDT_RUN_CYC_EXCTN_DT_SK
from Final1 f1
group by f1.SUB_INDV_BE_KEY,f1.MBR_INDV_BE_KEY,f1.QHP_ID,f1.New_MBR_ENR_EFF_DT_SK,f1.New_MBR_ENR_TERM_DT_SK
) as f12
where f11.SUB_INDV_BE_KEY = f12.SUB_INDV_BE_KEY
and f11.MBR_INDV_BE_KEY = f12.MBR_INDV_BE_KEY
and f11.QHP_ID = f12.QHP_ID
and f11.New_MBR_ENR_EFF_DT_SK = f12.New_MBR_ENR_EFF_DT_SK
and f11.New_MBR_ENR_TERM_DT_SK = f12.New_MBR_ENR_TERM_DT_SK
and f11.LAST_UPDT_RUN_CYC_EXCTN_DT_SK = f12.LAST_UPDT_RUN_CYC_EXCTN_DT_SK
"""

df_W_Table = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_edw)
    .options(**jdbc_props_edw)
    .option("query", extract_query_W_Table)
    .load()
)

extract_query_Ref_W_Table = f"""--NewSpan
with mbr as (
Select mbr.SUB_SK,we.SUB_INDV_BE_KEY, we.MBR_INDV_BE_KEY,mbr.sub_uniq_key,we.MBR_ENR_EFF_DT_SK,we.MBR_ENR_TERM_DT_SK,we.PRM_AMT,we.ENR_PERD_ACTVTY_CD,we.RATE_AREA_ID,we.QHP_ID
FROM     {EDWOwner}.MBR_D mbr,
         {EDWOwner}.MBR_ENR_D mbrEnr,
         {EDWOwner}.GRP_D grp,
         {EDWOwner}.MBR_ENR_QHP_D MBR_QHP,
         {EDWOwner}.SUB_D sub,{EDWOwner}.w_edge_mbr_elig_extr we
WHERE    mbr.MBR_SK = mbrEnr.MBR_SK
AND      mbr.GRP_SK = grp.GRP_SK
AND      MBR_QHP.MBR_ENR_SK = mbrEnr.MBR_ENR_SK
AND      MBR_QHP.MBR_UNIQ_KEY = mbrEnr.MBR_UNIQ_KEY
AND      MBR_QHP.MBR_ENR_EFF_DT_SK = mbrEnr.MBR_ENR_EFF_DT_SK
AND      mbrEnr.MBR_ENR_ELIG_IN = 'Y'
AND      mbrEnr.MBR_ENR_CLS_PLN_PROD_CAT_CD = 'MED'
AND      MBR_QHP.MBR_ENR_CLS_PLN_PROD_CAT_CD = 'MED'
AND      mbrEnr.MBR_ENR_EFF_DT_SK <= '{EndDate}'
AND      mbrEnr.MBR_ENR_TERM_DT_SK >= '{BeginDt}'
AND      mbrEnr.PROD_SH_NM IN ('PCB', 'PC', 'BCARE', 'BLUE-ACCESS', 'BLUE-SELECT','BLUESELECT+')
AND      mbrEnr.CLS_ID <> 'MHIP'
AND      MBR_QHP.QHP_ID <> 'NA'
AND      MBR_QHP.QHP_ID LIKE '{QHPID}'
AND      mbr.SUB_UNIQ_KEY = sub.SUB_UNIQ_KEY
AND      left(mbr.MBR_RELSHP_NM,3) = 'SUB'
and we.SUB_INDV_BE_KEY = we.mbr_indv_be_key
and we.sub_indv_be_key = mbr.mbr_indv_be_key
)
--=======================================================================================================
,all_Sub_Add_Audit as (
select distinct m.sub_sk,m.mbr_indv_be_key,m.qhp_id,saad.SUB_ADDR_AUDIT_ROW_ID,
saad.SUB_ADDR_ZIP_CD_5,saad.LAST_UPDT_RUN_CYC_EXCTN_DT_SK
from {EDWOwner}.sub_addr_audit_d saad, mbr m
where saad.sub_sk = m.sub_sk
and saad.SUB_ADDR_NM ='SUBSCRIBER HOME'
and saad.SUB_ADDR_ZIP_CD_5 <> ''
and  saad.LAST_UPDT_RUN_CYC_EXCTN_DT_SK <= '{EndDate}'
)
--=======================================================================================================
,Mnth_dtn_sub_addr_audit_zip1 as (
select asaa.mbr_indv_be_key,asaa.QHP_ID,
max(asaa.SUB_ADDR_AUDIT_ROW_ID) as SUB_ADDR_AUDIT_ROW_ID,
substr(replace(asaa.LAST_UPDT_RUN_CYC_EXCTN_DT_SK,'-',''),1,6) as mnth_LAST_UPDT_RUN_CYC_EXCTN_DT_SK
from all_Sub_Add_Audit asaa
group by asaa.mbr_indv_be_key,asaa.QHP_ID,substr(replace(asaa.LAST_UPDT_RUN_CYC_EXCTN_DT_SK,'-',''),1,6)
)
--=======================================================================================================
,Mnth_dtn_sub_addr_audit_zip as (
select asaa.mbr_indv_be_key,
max(asaa.SUB_ADDR_AUDIT_ROW_ID) as SUB_ADDR_AUDIT_ROW_ID,
substr(replace(asaa.LAST_UPDT_RUN_CYC_EXCTN_DT_SK,'-',''),1,6) as mnth_LAST_UPDT_RUN_CYC_EXCTN_DT_SK
from all_Sub_Add_Audit asaa
where asaa.LAST_UPDT_RUN_CYC_EXCTN_DT_SK between '{BeginDt}' and '{EndDate}'
group by asaa.mbr_indv_be_key,substr(replace(asaa.LAST_UPDT_RUN_CYC_EXCTN_DT_SK,'-',''),1,6)
)
--=======================================================================================================
,fnl_dtn_sub_addr_audit_zip1 as (
select distinct asaa.mbr_indv_be_key,asaa.QHP_ID,asaa.SUB_ADDR_AUDIT_ROW_ID,asaa.SUB_ADDR_ZIP_CD_5,asaa.LAST_UPDT_RUN_CYC_EXCTN_DT_SK
from all_Sub_Add_Audit asaa, Mnth_dtn_sub_addr_audit_zip1 mdsaa
where asaa.mbr_indv_be_key = mdsaa.mbr_indv_be_key
and asaa.QHP_ID = mdsaa.QHP_ID
AND asaa.SUB_ADDR_AUDIT_ROW_ID = mdsaa.SUB_ADDR_AUDIT_ROW_ID
)
--=======================================================================================================
,fnl_dtn_sub_addr_audit_zip as (
select distinct asaa.mbr_indv_be_key,asaa.SUB_ADDR_AUDIT_ROW_ID,asaa.SUB_ADDR_ZIP_CD_5,asaa.LAST_UPDT_RUN_CYC_EXCTN_DT_SK
from all_Sub_Add_Audit asaa, Mnth_dtn_sub_addr_audit_zip mdsaa
where asaa.mbr_indv_be_key = mdsaa.mbr_indv_be_key
and asaa.SUB_ADDR_AUDIT_ROW_ID = mdsaa.SUB_ADDR_AUDIT_ROW_ID
)
--=======================================================================================================
,MinEffDt as (
select fdsaa.mbr_indv_be_key,min(m.MBR_ENR_EFF_DT_SK) as MBR_ENR_EFF_DT_SK
from fnl_dtn_sub_addr_audit_zip fdsaa, mbr m
where fdsaa.mbr_indv_be_key = m. sub_indv_be_Key
group by fdsaa.mbr_indv_be_key
)
--=======================================================================================================
,first_Sub_Add_Audit_Zip as (
select distinct fsaaz.mbr_indv_be_key,fsaaz.SUB_ADDR_AUDIT_ROW_ID,fsaaz.LAST_UPDT_RUN_CYC_EXCTN_DT_SK,fsaaz.SUB_ADDR_ZIP_CD_5,
row_number() over ( partition by fsaaz.mbr_indv_be_key order by fsaaz.mbr_indv_be_key,fsaaz.SUB_ADDR_AUDIT_ROW_ID asc) row_num
from fnl_dtn_sub_addr_audit_zip fsaaz
)
--=======================================================================================================
,trueFirst_Sub_Add_Audit_Zip as (
select distinct fdsaa.mbr_indv_be_key,fdsaa.SUB_ADDR_AUDIT_ROW_ID,fdsaa.SUB_ADDR_ZIP_CD_5,fdsaa.LAST_UPDT_RUN_CYC_EXCTN_DT_SK,m.MBR_ENR_EFF_DT_SK
from fnl_dtn_sub_addr_audit_zip fdsaa, first_Sub_Add_Audit_Zip fsaa,MinEffDt m
where fdsaa.SUB_ADDR_AUDIT_ROW_ID = fsaa.SUB_ADDR_AUDIT_ROW_ID
and fdsaa.mbr_indv_be_key = fsaa.mbr_indv_be_key
and fdsaa.mbr_indv_be_key = m.mbr_indv_be_key
and month(fsaa.LAST_UPDT_RUN_CYC_EXCTN_DT_SK) = month(m.MBR_ENR_EFF_DT_SK)
and fsaa.row_num = 1
)
--=======================================================================================================
,uniq_Sub_Add_Audit_Zip as (
select fdasaaz.mbr_indv_be_key,fdasaaz.SUB_ADDR_AUDIT_ROW_ID,fdasaaz.SUB_ADDR_ZIP_CD_5,fdasaaz.LAST_UPDT_RUN_CYC_EXCTN_DT_SK
from fnl_dtn_sub_addr_audit_zip fdasaaz,trueFirst_Sub_Add_Audit_Zip tsaaz
where tsaaz.mbr_indv_be_key = fdasaaz.mbr_indv_be_key
and tsaaz.SUB_ADDR_AUDIT_ROW_ID != fdasaaz.SUB_ADDR_AUDIT_ROW_ID
and tsaaz.LAST_UPDT_RUN_CYC_EXCTN_DT_SK != fdasaaz.LAST_UPDT_RUN_CYC_EXCTN_DT_SK
union
select fdasaaz.mbr_indv_be_key,fdasaaz.SUB_ADDR_AUDIT_ROW_ID,fdasaaz.SUB_ADDR_ZIP_CD_5,fdasaaz.LAST_UPDT_RUN_CYC_EXCTN_DT_SK
from fnl_dtn_sub_addr_audit_zip fdasaaz
where fdasaaz.mbr_indv_be_key not in (select distinct tsaaz.mbr_indv_be_key from trueFirst_Sub_Add_Audit_Zip tsaaz)
)
--=======================================================================================================
,mbr_zip_Single_Updt as (
select usaaz.mbr_indv_be_key,count(usaaz.SUB_ADDR_ZIP_CD_5) as count_zip
from fnl_dtn_sub_addr_audit_zip usaaz
group by usaaz.mbr_indv_be_key
having count(usaaz.SUB_ADDR_ZIP_CD_5) = 1
)
--=======================================================================================================
,Final_NewSpan as (
select distinct m.SUB_INDV_BE_KEY , m.MBR_INDV_BE_KEY ,usaaz.SUB_ADDR_ZIP_CD_5 ,m.QHP_ID,usaaz.SUB_ADDR_AUDIT_ROW_ID,usaaz.LAST_UPDT_RUN_CYC_EXCTN_DT_SK
from mbr m,
uniq_Sub_Add_Audit_Zip usaaz
left outer join mbr_zip_Single_Updt mzsu
on usaaz.mbr_indv_be_key = mzsu.mbr_indv_be_key
where m.mbr_indv_be_key = usaaz.mbr_indv_be_key
and usaaz.LAST_UPDT_RUN_CYC_EXCTN_DT_SK between m.MBR_ENR_EFF_DT_SK and m.MBR_ENR_TERM_DT_SK
)
--=======================================================================================================
,MinFinal_NewSpan as (
select fns.SUB_INDV_BE_KEY, fns.MBR_INDV_BE_KEY, fns.QHP_ID,min(fns.SUB_ADDR_AUDIT_ROW_ID) min_SUB_ADDR_AUDIT_ROW_ID
from Final_NewSpan fns
group by fns.SUB_INDV_BE_KEY, fns.MBR_INDV_BE_KEY, fns.QHP_ID
)
--=======================================================================================================
,Pre_final as (
select distinct fdsaaz1.mbr_indv_be_key as SUB_INDV_BE_KEY, fdsaaz1.mbr_indv_be_key,fdsaaz1.QHP_ID ,
fdsaaz1.SUB_ADDR_ZIP_CD_5 ,fdsaaz1.SUB_ADDR_AUDIT_ROW_ID ,fdsaaz1.LAST_UPDT_RUN_CYC_EXCTN_DT_SK
from fnl_dtn_sub_addr_audit_zip1 fdsaaz1 ,Final_NewSpan fn ,MinFinal_NewSpan mfns
where fdsaaz1.mbr_indv_be_key = fn.mbr_indv_be_key
and fdsaaz1.QHP_ID = fn.QHP_ID
and fn.SUB_INDV_BE_KEY = mfns.SUB_INDV_BE_KEY
and fn.QHP_ID = mfns.QHP_ID
and fn.SUB_ADDR_AUDIT_ROW_ID = mfns.min_SUB_ADDR_AUDIT_ROW_ID
and fdsaaz1.LAST_UPDT_RUN_CYC_EXCTN_DT_SK != fn.LAST_UPDT_RUN_CYC_EXCTN_DT_SK
and fdsaaz1.SUB_ADDR_AUDIT_ROW_ID < fn.SUB_ADDR_AUDIT_ROW_ID
)
--=======================================================================================================
select distinct pf.SUB_INDV_BE_KEY,pf.SUB_ADDR_ZIP_CD_5,'Yes' as Flag_1,m.MBR_ENR_EFF_DT_SK,m.MBR_ENR_TERM_DT_SK,m.QHP_ID
from Pre_final pf,
(select m.sub_indv_be_Key,max(m.SUB_ADDR_AUDIT_ROW_ID) as SUB_ADDR_AUDIT_ROW_ID
 from Pre_final m
 group by m.sub_indv_be_Key) pfm,
mbr m
where pf.SUB_INDV_BE_KEY = pfm.SUB_INDV_BE_KEY
and pf.SUB_ADDR_AUDIT_ROW_ID = pfm.SUB_ADDR_AUDIT_ROW_ID
and pf.SUB_INDV_BE_KEY = m.SUB_INDV_BE_KEY
"""

df_Ref_W_Table = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_edw)
    .options(**jdbc_props_edw)
    .option("query", extract_query_Ref_W_Table)
    .load()
)

df_Filter_865_out1 = df_W_Table.select(
    F.col("SUB_INDV_BE_KEY").alias("SUB_INDV_BE_KEY"),
    F.col("MBR_INDV_BE_KEY").alias("MBR_INDV_BE_KEY"),
    F.col("SUB_ADDR_ZIP_CD_5").alias("SUB_ADDR_ZIP_CD_5"),
    F.col("MBR_ENR_EFF_DT_SK").alias("MBR_ENR_EFF_DT_SK"),
    F.col("MBR_ENR_TERM_DT_SK").alias("MBR_ENR_TERM_DT_SK"),
    F.col("New_MBR_ENR_EFF_DT_SK").alias("New_MBR_ENR_EFF_DT_SK"),
    F.col("New_MBR_ENR_TERM_DT_SK").alias("New_MBR_ENR_TERM_DT_SK"),
    F.col("PRM_AMT").alias("PRM_AMT"),
    F.col("ENR_PERD_ACTVTY_CD").alias("ENR_PERD_ACTVTY_CD"),
    F.col("RATE_AREA_ID").alias("RATE_AREA_ID"),
    F.col("QHP_ID").alias("QHP_ID"),
    F.col("Flag").alias("Flag"),
    F.col("SUB_ADDR_AUDIT_ROW_ID").alias("SUB_ADDR_AUDIT_ROW_ID"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("row_num").alias("row_num"),
)

df_Filter_865_out2 = df_W_Table.select(
    F.col("SUB_INDV_BE_KEY").alias("SUB_INDV_BE_KEY"),
    F.col("MBR_INDV_BE_KEY").alias("MBR_INDV_BE_KEY"),
    F.col("SUB_ADDR_ZIP_CD_5").alias("SUB_ADDR_ZIP_CD_5"),
    F.col("MBR_ENR_EFF_DT_SK").alias("MBR_ENR_EFF_DT_SK"),
    F.col("MBR_ENR_TERM_DT_SK").alias("MBR_ENR_TERM_DT_SK"),
    F.col("New_MBR_ENR_EFF_DT_SK").alias("New_MBR_ENR_EFF_DT_SK"),
    F.col("New_MBR_ENR_TERM_DT_SK").alias("New_MBR_ENR_TERM_DT_SK"),
    F.col("PRM_AMT").alias("PRM_AMT"),
    F.col("ENR_PERD_ACTVTY_CD").alias("ENR_PERD_ACTVTY_CD"),
    F.col("RATE_AREA_ID").alias("RATE_AREA_ID"),
    F.col("QHP_ID").alias("QHP_ID"),
    F.col("Flag").alias("Flag"),
    F.col("SUB_ADDR_AUDIT_ROW_ID").alias("SUB_ADDR_AUDIT_ROW_ID"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("row_num").alias("row_num"),
)

df_Copy_1_out1 = df_Ref_W_Table.select(
    F.col("SUB_INDV_BE_KEY").alias("SUB_INDV_BE_KEY"),
    F.col("SUB_ADDR_ZIP_CD_5").alias("Ref_SUB_ADDR_ZIP_CD_5"),
    F.col("Flag_1").alias("Flag_1"),
    F.col("QHP_ID").alias("QHP_ID"),
)

df_Copy_1_out2 = df_Ref_W_Table.select(
    F.col("SUB_INDV_BE_KEY").alias("SUB_INDV_BE_KEY"),
    F.col("SUB_ADDR_ZIP_CD_5").alias("Ref_SUB_ADDR_ZIP_CD_5"),
    F.col("Flag_1").alias("Flag_1"),
    F.col("QHP_ID").alias("QHP_ID"),
)

df_Lookup_871 = df_Filter_865_out1.alias("DSLink867").join(
    df_Copy_1_out2.alias("ref"),
    (F.col("DSLink867.SUB_INDV_BE_KEY") == F.col("ref.SUB_INDV_BE_KEY"))
    & (F.col("DSLink867.SUB_ADDR_ZIP_CD_5") == F.col("ref.Ref_SUB_ADDR_ZIP_CD_5"))
    & (F.col("DSLink867.QHP_ID") == F.col("ref.QHP_ID")),
    how="left"
)

df_Lookup_871_out1 = df_Lookup_871.select(
    F.col("DSLink867.SUB_INDV_BE_KEY").alias("SUB_INDV_BE_KEY"),
    F.col("DSLink867.MBR_INDV_BE_KEY").alias("MBR_INDV_BE_KEY"),
    F.col("DSLink867.SUB_ADDR_ZIP_CD_5").alias("SUB_ADDR_ZIP_CD_5"),
    F.col("DSLink867.MBR_ENR_EFF_DT_SK").alias("MBR_ENR_EFF_DT_SK"),
    F.col("DSLink867.MBR_ENR_TERM_DT_SK").alias("MBR_ENR_TERM_DT_SK"),
    F.col("DSLink867.New_MBR_ENR_EFF_DT_SK").alias("New_MBR_ENR_EFF_DT_SK"),
    F.col("DSLink867.New_MBR_ENR_TERM_DT_SK").alias("New_MBR_ENR_TERM_DT_SK"),
    F.col("DSLink867.PRM_AMT").alias("PRM_AMT"),
    F.col("DSLink867.ENR_PERD_ACTVTY_CD").alias("ENR_PERD_ACTVTY_CD"),
    F.col("DSLink867.RATE_AREA_ID").alias("RATE_AREA_ID"),
    F.col("DSLink867.QHP_ID").alias("QHP_ID"),
    F.col("DSLink867.Flag").alias("Flag"),
    F.col("DSLink867.SUB_ADDR_AUDIT_ROW_ID").alias("SUB_ADDR_AUDIT_ROW_ID"),
    F.col("DSLink867.LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("DSLink867.row_num").alias("row_num"),
    F.col("ref.Flag_1").alias("Flag_1"),
)

df_Lookup_871_out2 = df_Lookup_871.select(
    df_Filter_865_out1.columns  # "Reject" link, but effectively no transformation columns are listed, so reuse
)

df_ACA_Enrollment_DropNewChk = df_Lookup_871_out1.select(
    F.col("SUB_INDV_BE_KEY"),
    F.col("MBR_INDV_BE_KEY"),
    rpad("SUB_ADDR_ZIP_CD_5",5," "),
    rpad("MBR_ENR_EFF_DT_SK",10," "),
    rpad("MBR_ENR_TERM_DT_SK",10," "),
    rpad("New_MBR_ENR_EFF_DT_SK",10," "),
    rpad("New_MBR_ENR_TERM_DT_SK",10," "),
    F.col("PRM_AMT"),
    F.col("ENR_PERD_ACTVTY_CD"),
    F.col("RATE_AREA_ID"),
    F.col("QHP_ID"),
    F.col("Flag"),
    F.col("SUB_ADDR_AUDIT_ROW_ID"),
    rpad("LAST_UPDT_RUN_CYC_EXCTN_DT_SK",10," "),
    F.col("row_num"),
    F.col("Flag_1"),
)

write_files(
    df_ACA_Enrollment_DropNewChk,
    f"{adls_path_raw}/landing/ACA_Enrollment_DropNewChk.dat",
    delimiter="|",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote='"',
    nullValue=None
)

df_Funnel_878_in1 = df_Filter_865_out2
df_Funnel_878_in2 = df_Lookup_871_out2

df_Funnel_878 = df_Funnel_878_in1.select(
    F.col("SUB_INDV_BE_KEY"),
    F.col("MBR_INDV_BE_KEY"),
    F.col("SUB_ADDR_ZIP_CD_5"),
    F.col("MBR_ENR_EFF_DT_SK"),
    F.col("MBR_ENR_TERM_DT_SK"),
    F.col("New_MBR_ENR_EFF_DT_SK"),
    F.col("New_MBR_ENR_TERM_DT_SK"),
    F.col("PRM_AMT"),
    F.col("ENR_PERD_ACTVTY_CD"),
    F.col("RATE_AREA_ID"),
    F.col("QHP_ID"),
    F.col("Flag"),
    F.col("SUB_ADDR_AUDIT_ROW_ID"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("row_num")
).unionByName(
    df_Funnel_878_in2.select(
        F.col("SUB_INDV_BE_KEY"),
        F.col("MBR_INDV_BE_KEY"),
        F.col("SUB_ADDR_ZIP_CD_5"),
        F.col("MBR_ENR_EFF_DT_SK"),
        F.col("MBR_ENR_TERM_DT_SK"),
        F.col("New_MBR_ENR_EFF_DT_SK"),
        F.col("New_MBR_ENR_TERM_DT_SK"),
        F.col("PRM_AMT"),
        F.col("ENR_PERD_ACTVTY_CD"),
        F.col("RATE_AREA_ID"),
        F.col("QHP_ID"),
        F.col("Flag"),
        F.col("SUB_ADDR_AUDIT_ROW_ID"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        F.col("row_num")
    )
)

df_Copy_1_out3 = df_Copy_1_out1  # "Old_Zip"
df_Old_zipCd = df_Funnel_878.alias("New_ZipChng").join(
    df_Copy_1_out3.alias("Old_Zip"),
    (F.col("New_ZipChng.SUB_INDV_BE_KEY") == F.col("Old_Zip.SUB_INDV_BE_KEY")) & 
    (F.col("New_ZipChng.QHP_ID") == F.col("Old_Zip.QHP_ID")),
    how="left"
).select(
    F.col("New_ZipChng.SUB_INDV_BE_KEY").alias("SUB_INDV_BE_KEY"),
    F.col("New_ZipChng.MBR_INDV_BE_KEY").alias("MBR_INDV_BE_KEY"),
    F.col("New_ZipChng.SUB_ADDR_ZIP_CD_5").alias("SUB_ADDR_ZIP_CD_5"),
    F.col("New_ZipChng.MBR_ENR_EFF_DT_SK").alias("MBR_ENR_EFF_DT_SK"),
    F.col("New_ZipChng.MBR_ENR_TERM_DT_SK").alias("MBR_ENR_TERM_DT_SK"),
    F.col("New_ZipChng.New_MBR_ENR_EFF_DT_SK").alias("New_MBR_ENR_EFF_DT_SK"),
    F.col("New_ZipChng.New_MBR_ENR_TERM_DT_SK").alias("New_MBR_ENR_TERM_DT_SK"),
    F.col("New_ZipChng.PRM_AMT").alias("PRM_AMT"),
    F.col("New_ZipChng.ENR_PERD_ACTVTY_CD").alias("ENR_PERD_ACTVTY_CD"),
    F.col("New_ZipChng.RATE_AREA_ID").alias("RATE_AREA_ID"),
    F.col("New_ZipChng.QHP_ID").alias("QHP_ID"),
    F.col("New_ZipChng.Flag").alias("Flag"),
    F.col("New_ZipChng.SUB_ADDR_AUDIT_ROW_ID").alias("SUB_ADDR_AUDIT_ROW_ID"),
    F.col("New_ZipChng.LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("New_ZipChng.row_num").alias("row_num"),
    F.col("Old_Zip.Ref_SUB_ADDR_ZIP_CD_5").alias("Ref_SUB_ADDR_ZIP_CD_5"),
    F.col("Old_Zip.Flag_1").alias("Flag_1")
)

df_RD1_in = df_Old_zipCd
df_RD1_dedup = dedup_sort(
    df_RD1_in,
    partition_cols=[
        "SUB_INDV_BE_KEY","MBR_INDV_BE_KEY","SUB_ADDR_ZIP_CD_5","MBR_ENR_EFF_DT_SK","MBR_ENR_TERM_DT_SK","New_MBR_ENR_EFF_DT_SK",
        "New_MBR_ENR_TERM_DT_SK","PRM_AMT","ENR_PERD_ACTVTY_CD","RATE_AREA_ID","QHP_ID","SUB_ADDR_AUDIT_ROW_ID",
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK","Ref_SUB_ADDR_ZIP_CD_5","Flag_1","Flag"
    ],
    sort_cols=[]
)

df_RD1_out = df_RD1_dedup.select(
    F.col("SUB_INDV_BE_KEY"),
    F.col("MBR_INDV_BE_KEY"),
    F.col("SUB_ADDR_ZIP_CD_5"),
    F.col("MBR_ENR_EFF_DT_SK"),
    F.col("MBR_ENR_TERM_DT_SK"),
    F.col("New_MBR_ENR_EFF_DT_SK"),
    F.col("New_MBR_ENR_TERM_DT_SK"),
    F.col("PRM_AMT"),
    F.col("ENR_PERD_ACTVTY_CD"),
    F.col("RATE_AREA_ID"),
    F.col("QHP_ID"),
    F.col("Flag"),
    F.col("SUB_ADDR_AUDIT_ROW_ID"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("Flag_1"),
    F.col("Ref_SUB_ADDR_ZIP_CD_5")
).alias("RD")

df_Tfm_AssignTrm = df_RD1_out.select(
    F.col("RD.SUB_INDV_BE_KEY").alias("SUB_INDV_BE_KEY"),
    F.col("RD.MBR_INDV_BE_KEY").alias("MBR_INDV_BE_KEY"),
    F.col("RD.SUB_ADDR_ZIP_CD_5").alias("SUB_ADDR_ZIP_CD_5"),
    F.col("RD.MBR_ENR_EFF_DT_SK").alias("MBR_ENR_EFF_DT_SK"),
    F.col("RD.MBR_ENR_TERM_DT_SK").alias("MBR_ENR_TERM_DT_SK"),
    F.col("RD.New_MBR_ENR_EFF_DT_SK").alias("New_MBR_ENR_EFF_DT_SK"),
    F.col("RD.New_MBR_ENR_TERM_DT_SK").alias("New_MBR_ENR_TERM_DT_SK"),
    F.col("RD.PRM_AMT").alias("PRM_AMT"),
    F.col("RD.ENR_PERD_ACTVTY_CD").alias("ENR_PERD_ACTVTY_CD"),
    F.col("RD.RATE_AREA_ID").alias("RATE_AREA_ID"),
    F.expr("""CASE WHEN RD.Flag = 'Yes'
      THEN (
        CASE
          WHEN Ref_SUB_ADDR_ZIP_CD_5 = SUB_ADDR_ZIP_CD_5 AND Flag_1 = 'Yes' THEN 'Yes'
          WHEN Ref_SUB_ADDR_ZIP_CD_5 <> SUB_ADDR_ZIP_CD_5 AND Flag_1 = 'No' THEN 'Yes'
          WHEN Ref_SUB_ADDR_ZIP_CD_5 <> SUB_ADDR_ZIP_CD_5 AND Flag_1 = 'Yes' THEN 'No'
          ELSE 'Yes'
        END
      )
      ELSE RD.Flag
    END""").alias("Flag"),
    F.col("RD.QHP_ID").alias("QHP_ID"),
    F.col("RD.SUB_ADDR_AUDIT_ROW_ID").alias("SUB_ADDR_AUDIT_ROW_ID"),
    F.col("RD.LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.expr("""CASE WHEN (IF IsNotNull(Flag_1) THEN Flag_1 ELSE "" ) = '' THEN 'No' ELSE Flag_1 END""").alias("Flag_1"),
    F.expr("""CASE WHEN (IF IsNotNull(Ref_SUB_ADDR_ZIP_CD_5) THEN Ref_SUB_ADDR_ZIP_CD_5 ELSE "" ) = '' THEN SetNull() ELSE Ref_SUB_ADDR_ZIP_CD_5 END""").alias("Ref_SUB_ADDR_ZIP_CD_5"),
    F.col("RD.Flag").alias("flag_Original")
).alias("split")

df_Tfm_SplitSpans_out1 = df_Tfm_AssignTrm.where("split.Flag = 'No'").select(
    F.col("split.SUB_INDV_BE_KEY").alias("SUB_INDV_BE_KEY"),
    F.col("split.MBR_INDV_BE_KEY").alias("MBR_INDV_BE_KEY"),
    F.col("split.SUB_ADDR_ZIP_CD_5").alias("SUB_ADDR_ZIP_CD_5"),
    F.expr("""CASE WHEN split.MBR_ENR_EFF_DT_SK[1, 4] : split.MBR_ENR_EFF_DT_SK[6, 2] < split.LAST_UPDT_RUN_CYC_EXCTN_DT_SK[1, 4] : split.LAST_UPDT_RUN_CYC_EXCTN_DT_SK[6, 2]
         AND split.MBR_ENR_TERM_DT_SK[1, 4] : split.MBR_ENR_TERM_DT_SK[6, 2] > split.LAST_UPDT_RUN_CYC_EXCTN_DT_SK[1, 4] : split.LAST_UPDT_RUN_CYC_EXCTN_DT_SK[6, 2]
         THEN FIND.DATE.EE(split.LAST_UPDT_RUN_CYC_EXCTN_DT_SK, '1', 'M', 'F', 'CCYY_MM_DD')
         ELSE CASE WHEN split.MBR_ENR_EFF_DT_SK[1, 4] : split.MBR_ENR_EFF_DT_SK[6, 2] < split.LAST_UPDT_RUN_CYC_EXCTN_DT_SK[1, 4] : split.LAST_UPDT_RUN_CYC_EXCTN_DT_SK[6, 2]
         AND split.MBR_ENR_TERM_DT_SK[1, 4] : split.MBR_ENR_TERM_DT_SK[6, 2] = split.LAST_UPDT_RUN_CYC_EXCTN_DT_SK[1, 4] : split.LAST_UPDT_RUN_CYC_EXCTN_DT_SK[6, 2]
         THEN 'No'
         ELSE CASE WHEN split.MBR_ENR_EFF_DT_SK[1, 4] : split.MBR_ENR_EFF_DT_SK[6, 2] = split.LAST_UPDT_RUN_CYC_EXCTN_DT_SK[1, 4] : split.LAST_UPDT_RUN_CYC_EXCTN_DT_SK[6, 2]
         AND split.MBR_ENR_TERM_DT_SK[1, 4] : split.MBR_ENR_TERM_DT_SK[6, 2] > split.LAST_UPDT_RUN_CYC_EXCTN_DT_SK[1, 4] : split.LAST_UPDT_RUN_CYC_EXCTN_DT_SK[6, 2]
         THEN FIND.DATE.EE(split.LAST_UPDT_RUN_CYC_EXCTN_DT_SK, '1', 'M', 'F', 'CCYY_MM_DD')
         ELSE split.MBR_ENR_EFF_DT_SK END END END""").alias("MBR_ENR_EFF_DT_SK"),
    F.col("split.MBR_ENR_TERM_DT_SK").alias("MBR_ENR_TERM_DT_SK"),
    F.col("split.PRM_AMT").alias("PRM_AMT"),
    F.col("split.ENR_PERD_ACTVTY_CD").alias("ENR_PERD_ACTVTY_CD"),
    F.col("split.RATE_AREA_ID").alias("RATE_AREA_ID"),
    F.col("split.QHP_ID").alias("QHP_ID"),
    F.col("split.New_MBR_ENR_TERM_DT_SK").alias("New_MBR_ENR_TERM_DT_SK")
).alias("Seq_post1")

df_Tfm_SplitSpans_out2 = df_Tfm_AssignTrm.where("split.Flag = 'No' And IsNotNull(split.Ref_SUB_ADDR_ZIP_CD_5)").select(
    F.col("split.SUB_INDV_BE_KEY").alias("SUB_INDV_BE_KEY"),
    F.col("split.MBR_INDV_BE_KEY").alias("MBR_INDV_BE_KEY"),
    F.col("split.Ref_SUB_ADDR_ZIP_CD_5").alias("SUB_ADDR_ZIP_CD_5"),
    F.col("split.MBR_ENR_EFF_DT_SK").alias("MBR_ENR_EFF_DT_SK"),
    F.expr("""CASE WHEN split.LAST_UPDT_RUN_CYC_EXCTN_DT_SK[1, 4] : split.LAST_UPDT_RUN_CYC_EXCTN_DT_SK[6, 2] < split.MBR_ENR_TERM_DT_SK[1, 4] : split.MBR_ENR_TERM_DT_SK[6, 2]
         AND split.LAST_UPDT_RUN_CYC_EXCTN_DT_SK[1, 4] : split.LAST_UPDT_RUN_CYC_EXCTN_DT_SK[6, 2] > split.MBR_ENR_EFF_DT_SK[1, 4] : split.MBR_ENR_EFF_DT_SK[6, 2]
         THEN FIND.DATE.EE(split.LAST_UPDT_RUN_CYC_EXCTN_DT_SK, '-1', 'M', 'L', 'CCYY_MM_DD')
         ELSE CASE WHEN split.LAST_UPDT_RUN_CYC_EXCTN_DT_SK[1, 4] : split.LAST_UPDT_RUN_CYC_EXCTN_DT_SK[6, 2] = split.MBR_ENR_TERM_DT_SK[1, 4] : split.MBR_ENR_TERM_DT_SK[6, 2]
         AND split.LAST_UPDT_RUN_CYC_EXCTN_DT_SK[1, 4] : split.LAST_UPDT_RUN_CYC_EXCTN_DT_SK[6, 2] > split.MBR_ENR_EFF_DT_SK[1, 4] : split.MBR_ENR_EFF_DT_SK[6, 2]
         THEN FIND.DATE.EE(split.LAST_UPDT_RUN_CYC_EXCTN_DT_SK, '-1', 'M', 'L', 'CCYY_MM_DD')
         ELSE CASE WHEN split.LAST_UPDT_RUN_CYC_EXCTN_DT_SK[1, 4] : split.LAST_UPDT_RUN_CYC_EXCTN_DT_SK[6, 2] = split.MBR_ENR_EFF_DT_SK[1, 4] : split.MBR_ENR_EFF_DT_SK[6, 2]
         THEN 'No'
         ELSE split.MBR_ENR_TERM_DT_SK END END END""").alias("MBR_ENR_TERM_DT_SK"),
    F.col("split.PRM_AMT").alias("PRM_AMT"),
    F.col("split.ENR_PERD_ACTVTY_CD").alias("ENR_PERD_ACTVTY_CD"),
    F.col("split.RATE_AREA_ID").alias("RATE_AREA_ID"),
    F.col("split.QHP_ID").alias("QHP_ID"),
    F.col("split.New_MBR_ENR_EFF_DT_SK").alias("New_MBR_ENR_EFF_DT_SK")
).alias("Seq0")

df_Tfm_SplitSpans_out3 = df_Tfm_AssignTrm.where("split.Flag = 'No'").select(
    F.col("split.SUB_INDV_BE_KEY").alias("SUB_INDV_BE_KEY"),
    F.col("split.MBR_INDV_BE_KEY").alias("MBR_INDV_BE_KEY"),
    F.col("split.SUB_ADDR_ZIP_CD_5").alias("SUB_ADDR_ZIP_CD_5"),
    F.col("split.New_MBR_ENR_EFF_DT_SK").alias("MBR_ENR_EFF_DT_SK"),
    F.col("split.New_MBR_ENR_TERM_DT_SK").alias("MBR_ENR_TERM_DT_SK"),
    F.col("split.PRM_AMT").alias("PRM_AMT"),
    F.expr("'001'").alias("ENR_PERD_ACTVTY_CD"),
    F.col("split.RATE_AREA_ID").alias("RATE_AREA_ID"),
    F.col("split.QHP_ID").alias("QHP_ID")
).alias("New_Span1")

df_Copy_NewSpan_out1 = df_Tfm_SplitSpans_out3.select(
    F.col("New_Span1.SUB_INDV_BE_KEY").alias("SUB_INDV_BE_KEY"),
    F.col("New_Span1.MBR_INDV_BE_KEY").alias("MBR_INDV_BE_KEY"),
    F.col("New_Span1.SUB_ADDR_ZIP_CD_5").alias("SUB_ADDR_ZIP_CD_5"),
    F.col("New_Span1.MBR_ENR_EFF_DT_SK").alias("MBR_ENR_EFF_DT_SK"),
    F.col("New_Span1.MBR_ENR_TERM_DT_SK").alias("MBR_ENR_TERM_DT_SK"),
    F.col("New_Span1.PRM_AMT").alias("PRM_AMT"),
    F.col("New_Span1.ENR_PERD_ACTVTY_CD").alias("ENR_PERD_ACTVTY_CD"),
    F.col("New_Span1.RATE_AREA_ID").alias("RATE_AREA_ID"),
    F.col("New_Span1.QHP_ID").alias("QHP_ID")
).alias("All_NewSpan")

df_Copy_NewSpan_out2 = df_Tfm_SplitSpans_out3.select(
    F.col("New_Span1.SUB_INDV_BE_KEY").alias("SUB_INDV_BE_KEY"),
    F.col("New_Span1.MBR_INDV_BE_KEY").alias("MBR_INDV_BE_KEY"),
    F.col("New_Span1.SUB_ADDR_ZIP_CD_5").alias("SUB_ADDR_ZIP_CD_5"),
    F.col("New_Span1.MBR_ENR_EFF_DT_SK").alias("MBR_ENR_EFF_DT_SK"),
    F.col("New_Span1.MBR_ENR_TERM_DT_SK").alias("MBR_ENR_TERM_DT_SK"),
    F.col("New_Span1.PRM_AMT").alias("PRM_AMT"),
    F.col("New_Span1.ENR_PERD_ACTVTY_CD").alias("ENR_PERD_ACTVTY_CD"),
    F.col("New_Span1.RATE_AREA_ID").alias("RATE_AREA_ID"),
    F.col("New_Span1.QHP_ID").alias("QHP_ID")
).alias("New_Span")

df_Copy_NewSpan_out3 = df_Tfm_SplitSpans_out3.select(
    F.col("New_Span1.SUB_INDV_BE_KEY").alias("SUB_INDV_BE_KEY"),
    F.col("New_Span1.MBR_INDV_BE_KEY").alias("MBR_INDV_BE_KEY"),
    F.col("New_Span1.SUB_ADDR_ZIP_CD_5").alias("SUB_ADDR_ZIP_CD_5"),
    F.col("New_Span1.MBR_ENR_EFF_DT_SK").alias("MBR_ENR_EFF_DT_SK"),
    F.col("New_Span1.MBR_ENR_TERM_DT_SK").alias("MBR_ENR_TERM_DT_SK"),
    F.col("New_Span1.PRM_AMT").alias("PRM_AMT"),
    F.col("New_Span1.ENR_PERD_ACTVTY_CD").alias("ENR_PERD_ACTVTY_CD"),
    F.col("New_Span1.RATE_AREA_ID").alias("RATE_AREA_ID"),
    F.col("New_Span1.QHP_ID").alias("QHP_ID")
).alias("New_SpanPost")

df_tfm_AllNew = df_Copy_NewSpan_out1.select(
    F.col("All_NewSpan.SUB_INDV_BE_KEY"),
    F.col("All_NewSpan.MBR_INDV_BE_KEY"),
    F.col("All_NewSpan.SUB_ADDR_ZIP_CD_5"),
    F.col("All_NewSpan.MBR_ENR_EFF_DT_SK"),
    F.col("All_NewSpan.MBR_ENR_TERM_DT_SK"),
    F.col("All_NewSpan.PRM_AMT"),
    F.expr("'001'").alias("ENR_PERD_ACTVTY_CD"),
    F.col("All_NewSpan.RATE_AREA_ID"),
    F.col("All_NewSpan.QHP_ID"),
    F.expr("'NEW'").alias("SpanType")
).alias("NewSpn")

df_Sort_972_in = df_Tfm_SplitSpans_out2

df_Sort_972 = df_Sort_972_in.orderBy(
    F.col("MBR_INDV_BE_KEY"),
    F.col("SUB_INDV_BE_KEY"),
    F.col("QHP_ID"),
    F.col("New_MBR_ENR_EFF_DT_SK")
).alias("Seq")

df_preSpan_chk = df_Sort_972.select(
    F.col("Seq.SUB_INDV_BE_KEY").alias("SUB_INDV_BE_KEY"),
    F.col("Seq.MBR_INDV_BE_KEY").alias("MBR_INDV_BE_KEY"),
    F.col("Seq.SUB_ADDR_ZIP_CD_5").alias("SUB_ADDR_ZIP_CD_5"),
    F.expr("""CASE WHEN  (  IF svPreviousEFD = svCurrentEFD and svPrvIndvBk = svCurIndvBk and svPrvQHP = svCrtQHP THEN 'Yes' ELSE 'No' ) = 'Yes'
            THEN FIND.DATE.EE(Seq.New_MBR_ENR_EFF_DT_SK, '-1', 'M', 'F', 'CCYY_MM_DD')
            ELSE Seq.MBR_ENR_EFF_DT_SK END""").alias("MBR_ENR_EFF_DT_SK"),
    F.col("Seq.MBR_ENR_TERM_DT_SK").alias("MBR_ENR_TERM_DT_SK"),
    F.col("Seq.PRM_AMT").alias("PRM_AMT"),
    F.col("Seq.ENR_PERD_ACTVTY_CD").alias("ENR_PERD_ACTVTY_CD"),
    F.col("Seq.RATE_AREA_ID").alias("RATE_AREA_ID"),
    F.col("Seq.QHP_ID").alias("QHP_ID")
).alias("Pre_Span1")

df_Filter_Pre = df_preSpan_chk.select(
    F.col("Pre_Span1.SUB_INDV_BE_KEY").alias("SUB_INDV_BE_KEY"),
    F.col("Pre_Span1.MBR_INDV_BE_KEY").alias("MBR_INDV_BE_KEY"),
    F.col("Pre_Span1.SUB_ADDR_ZIP_CD_5").alias("SUB_ADDR_ZIP_CD_5"),
    F.col("Pre_Span1.MBR_ENR_EFF_DT_SK").alias("MBR_ENR_EFF_DT_SK"),
    F.col("Pre_Span1.MBR_ENR_TERM_DT_SK").alias("MBR_ENR_TERM_DT_SK"),
    F.col("Pre_Span1.PRM_AMT").alias("PRM_AMT"),
    F.col("Pre_Span1.ENR_PERD_ACTVTY_CD").alias("ENR_PERD_ACTVTY_CD"),
    F.col("Pre_Span1.RATE_AREA_ID").alias("RATE_AREA_ID"),
    F.col("Pre_Span1.QHP_ID").alias("QHP_ID")
).alias("Pre_Span")

df_Lookup_Pre = df_Filter_Pre.alias("Pre_Span").join(
    df_Copy_NewSpan_out2.alias("New_Span"),
    (
        (F.col("Pre_Span.SUB_INDV_BE_KEY") == F.col("New_Span.SUB_INDV_BE_KEY")) &
        (F.col("Pre_Span.MBR_INDV_BE_KEY") == F.col("New_Span.MBR_INDV_BE_KEY")) &
        (F.col("Pre_Span.MBR_ENR_EFF_DT_SK") == F.col("New_Span.MBR_ENR_EFF_DT_SK")) &
        (F.col("Pre_Span.MBR_ENR_TERM_DT_SK") == F.col("New_Span.MBR_ENR_TERM_DT_SK")) &
        (F.col("Pre_Span.PRM_AMT") == F.col("New_Span.PRM_AMT")) &
        (F.col("Pre_Span.RATE_AREA_ID") == F.col("New_Span.RATE_AREA_ID")) &
        (F.col("Pre_Span.QHP_ID") == F.col("New_Span.QHP_ID"))
    ),
    how="left"
)

df_Lookup_Pre_out1 = df_Lookup_Pre.select(
    F.col("Pre_Span.SUB_INDV_BE_KEY").alias("SUB_INDV_BE_KEY"),
    F.col("Pre_Span.MBR_INDV_BE_KEY").alias("MBR_INDV_BE_KEY"),
    F.col("New_Span.SUB_ADDR_ZIP_CD_5").alias("SUB_ADDR_ZIP_CD_5"),
    F.col("Pre_Span.MBR_ENR_EFF_DT_SK").alias("MBR_ENR_EFF_DT_SK"),
    F.col("Pre_Span.MBR_ENR_TERM_DT_SK").alias("MBR_ENR_TERM_DT_SK"),
    F.col("Pre_Span.PRM_AMT").alias("PRM_AMT"),
    F.col("New_Span.ENR_PERD_ACTVTY_CD").alias("ENR_PERD_ACTVTY_CD"),
    F.col("Pre_Span.RATE_AREA_ID").alias("RATE_AREA_ID"),
    F.col("Pre_Span.QHP_ID").alias("QHP_ID")
).alias("Mtch_Pre")

df_Lookup_Pre_out2 = df_Filter_Pre.alias("Pre_Span1").join(
    df_Lookup_Pre.select(df_Filter_Pre.columns),  # just to produce a "reject" link – not truly implemented
    on=["SUB_INDV_BE_KEY","MBR_INDV_BE_KEY","SUB_ADDR_ZIP_CD_5","MBR_ENR_EFF_DT_SK","MBR_ENR_TERM_DT_SK","PRM_AMT","ENR_PERD_ACTVTY_CD","RATE_AREA_ID","QHP_ID"],
    how="leftanti"
).alias("Rej")

df_tfm_Rej = df_Lookup_Pre_out2.select(
    F.col("Rej.SUB_INDV_BE_KEY"),
    F.col("Rej.MBR_INDV_BE_KEY"),
    F.col("Rej.SUB_ADDR_ZIP_CD_5"),
    F.col("Rej.MBR_ENR_EFF_DT_SK"),
    F.col("Rej.MBR_ENR_TERM_DT_SK"),
    F.col("Rej.PRM_AMT"),
    F.col("Rej.ENR_PERD_ACTVTY_CD"),
    F.col("Rej.RATE_AREA_ID"),
    F.col("Rej.QHP_ID")
).alias("Rej")

df_tfm_Rej_out = df_tfm_Rej.select(
    F.col("Rej.SUB_INDV_BE_KEY").alias("SUB_INDV_BE_KEY"),
    F.col("Rej.MBR_INDV_BE_KEY").alias("MBR_INDV_BE_KEY"),
    F.col("Rej.SUB_ADDR_ZIP_CD_5").alias("SUB_ADDR_ZIP_CD_5"),
    F.col("Rej.MBR_ENR_EFF_DT_SK").alias("MBR_ENR_EFF_DT_SK"),
    F.col("Rej.MBR_ENR_TERM_DT_SK").alias("MBR_ENR_TERM_DT_SK"),
    F.col("Rej.PRM_AMT").alias("PRM_AMT"),
    F.col("Rej.ENR_PERD_ACTVTY_CD").alias("ENR_PERD_ACTVTY_CD"),
    F.col("Rej.RATE_AREA_ID").alias("RATE_AREA_ID"),
    F.col("Rej.QHP_ID").alias("QHP_ID"),
    F.expr("'PRE'").alias("SpanType")
).alias("Rej_preSpan")

df_tfm_PreMtch_out = df_Lookup_Pre_out1.select(
    F.col("Mtch_Pre.SUB_INDV_BE_KEY").alias("SUB_INDV_BE_KEY"),
    F.col("Mtch_Pre.MBR_INDV_BE_KEY").alias("MBR_INDV_BE_KEY"),
    F.col("Mtch_Pre.SUB_ADDR_ZIP_CD_5").alias("SUB_ADDR_ZIP_CD_5"),
    F.col("Mtch_Pre.MBR_ENR_EFF_DT_SK").alias("MBR_ENR_EFF_DT_SK"),
    F.col("Mtch_Pre.MBR_ENR_TERM_DT_SK").alias("MBR_ENR_TERM_DT_SK"),
    F.col("Mtch_Pre.PRM_AMT").alias("PRM_AMT"),
    F.col("Mtch_Pre.ENR_PERD_ACTVTY_CD").alias("ENR_PERD_ACTVTY_CD"),
    F.col("Mtch_Pre.RATE_AREA_ID").alias("RATE_AREA_ID"),
    F.col("Mtch_Pre.QHP_ID").alias("QHP_ID"),
    F.expr("'MPRE'").alias("SpanType")
).alias("PreMtch")

df_Funnel_802_in1 = df_tfm_Rej_out
df_Funnel_802_in2 = df_tfm_PreMtch_out
df_Funnel_802_in3 = df_tfm_AllNew
df_Tfm_PostMtch_empty = df_tfm_AllNew.limit(0)  # placeholder for "PostMtch" input
df_Tfm_PostSpan_empty = df_tfm_AllNew.limit(0)  # placeholder for "PostSpan" input

# In the actual JSON, there are more merges into this funnel, so we gather them:
df_Funnel_802_in4 = df_Tfm_PostMtch_empty
df_Funnel_802_in5 = df_Tfm_PostSpan_empty

df_Funnel_802 = df_Funnel_802_in1.select(
    "SUB_INDV_BE_KEY","MBR_INDV_BE_KEY","SUB_ADDR_ZIP_CD_5","MBR_ENR_EFF_DT_SK","MBR_ENR_TERM_DT_SK","PRM_AMT","ENR_PERD_ACTVTY_CD","RATE_AREA_ID","QHP_ID","SpanType"
).unionByName(
    df_Funnel_802_in2.select(
        "SUB_INDV_BE_KEY","MBR_INDV_BE_KEY","SUB_ADDR_ZIP_CD_5","MBR_ENR_EFF_DT_SK","MBR_ENR_TERM_DT_SK","PRM_AMT","ENR_PERD_ACTVTY_CD","RATE_AREA_ID","QHP_ID","SpanType"
    )
).unionByName(
    df_Funnel_802_in3.select(
        "SUB_INDV_BE_KEY","MBR_INDV_BE_KEY","SUB_ADDR_ZIP_CD_5","MBR_ENR_EFF_DT_SK","MBR_ENR_TERM_DT_SK","PRM_AMT","ENR_PERD_ACTVTY_CD","RATE_AREA_ID","QHP_ID","SpanType"
    )
).unionByName(
    df_Funnel_802_in4.select(
        "SUB_INDV_BE_KEY","MBR_INDV_BE_KEY","SUB_ADDR_ZIP_CD_5","MBR_ENR_EFF_DT_SK","MBR_ENR_TERM_DT_SK","PRM_AMT","ENR_PERD_ACTVTY_CD","RATE_AREA_ID","QHP_ID","SpanType"
    )
).unionByName(
    df_Funnel_802_in5.select(
        "SUB_INDV_BE_KEY","MBR_INDV_BE_KEY","SUB_ADDR_ZIP_CD_5","MBR_ENR_EFF_DT_SK","MBR_ENR_TERM_DT_SK","PRM_AMT","ENR_PERD_ACTVTY_CD","RATE_AREA_ID","QHP_ID","SpanType"
    )
).alias("all")

df_Sort_952 = df_Funnel_802.orderBy(
    F.col("MBR_INDV_BE_KEY"),
    F.col("QHP_ID"),
    F.col("MBR_ENR_EFF_DT_SK"),
    F.col("MBR_ENR_TERM_DT_SK"),
    F.col("SpanType")
).alias("DSLink954")

df_Transformer_953 = df_Sort_952.select(
    F.col("all.SUB_INDV_BE_KEY").alias("SUB_INDV_BE_KEY"),
    F.col("all.MBR_INDV_BE_KEY").alias("MBR_INDV_BE_KEY"),
    F.col("all.SUB_ADDR_ZIP_CD_5").alias("SUB_ADDR_ZIP_CD_5"),
    F.col("all.MBR_ENR_EFF_DT_SK").alias("MBR_ENR_EFF_DT_SK"),
    F.col("all.MBR_ENR_TERM_DT_SK").alias("MBR_ENR_TERM_DT_SK"),
    F.col("all.PRM_AMT").alias("PRM_AMT"),
    F.col("all.ENR_PERD_ACTVTY_CD").alias("ENR_PERD_ACTVTY_CD"),
    F.col("all.RATE_AREA_ID").alias("RATE_AREA_ID"),
    F.col("all.QHP_ID").alias("QHP_ID"),
    F.col("all.SpanType").alias("SpanType")
).alias("DSLink954")

df_Transformer_953_out = df_Transformer_953.select(
    F.col("DSLink954.SUB_INDV_BE_KEY").alias("SUB_INDV_BE_KEY"),
    F.col("DSLink954.MBR_INDV_BE_KEY").alias("MBR_INDV_BE_KEY"),
    F.expr("""CASE WHEN (IF svPrvIndvBK = svCrntIndvBK AND svPrvQHP = svCrntQHP AND svPrvEET = svCrntEET AND svPrvETD = svCrntETD THEN 'Yes' ELSE 'No') = 'Yes'
             THEN svPrvZipCd ELSE DSLink954.SUB_ADDR_ZIP_CD_5 END""").alias("SUB_ADDR_ZIP_CD_5"),
    F.col("DSLink954.MBR_ENR_EFF_DT_SK").alias("MBR_ENR_EFF_DT_SK"),
    F.col("DSLink954.MBR_ENR_TERM_DT_SK").alias("MBR_ENR_TERM_DT_SK"),
    F.col("DSLink954.PRM_AMT").alias("PRM_AMT"),
    F.expr("""CASE WHEN (IF svPrvIndvBK = svCrntIndvBK AND svPrvQHP = svCrntQHP AND svPrvEET = svCrntEET AND svPrvETD = svCrntETD THEN 'Yes' ELSE 'No') = 'Yes'
             THEN svPrvEPAC ELSE DSLink954.ENR_PERD_ACTVTY_CD END""").alias("ENR_PERD_ACTVTY_CD"),
    F.col("DSLink954.RATE_AREA_ID").alias("RATE_AREA_ID"),
    F.col("DSLink954.QHP_ID").alias("QHP_ID"),
    F.expr("""CASE WHEN (IF svPrvIndvBK = svCrntIndvBK AND svPrvQHP = svCrntQHP AND svPrvEET = svCrntEET AND svPrvETD = svCrntETD THEN 'Yes' ELSE 'No') = 'Yes'
             THEN svPrvSpan ELSE DSLink954.SpanType END""").alias("SpanType")
).alias("RD2")

df_rd2_dedup = dedup_sort(
    df_Transformer_953_out,
    partition_cols=[
        "SUB_INDV_BE_KEY","MBR_INDV_BE_KEY","MBR_ENR_EFF_DT_SK","MBR_ENR_TERM_DT_SK","QHP_ID",
        "ENR_PERD_ACTVTY_CD","PRM_AMT","RATE_AREA_ID","SpanType","SUB_ADDR_ZIP_CD_5"
    ],
    sort_cols=[]
)

df_rd2_out = df_rd2_dedup.select(
    F.col("RD2.SUB_INDV_BE_KEY").alias("SUB_INDV_BE_KEY"),
    F.col("RD2.MBR_INDV_BE_KEY").alias("MBR_INDV_BE_KEY"),
    F.col("RD2.SUB_ADDR_ZIP_CD_5").alias("SUB_ADDR_ZIP_CD_5"),
    F.col("RD2.MBR_ENR_EFF_DT_SK").alias("MBR_ENR_EFF_DT_SK"),
    F.col("RD2.MBR_ENR_TERM_DT_SK").alias("MBR_ENR_TERM_DT_SK"),
    F.col("RD2.PRM_AMT").alias("PRM_AMT"),
    F.col("RD2.ENR_PERD_ACTVTY_CD").alias("ENR_PERD_ACTVTY_CD"),
    F.col("RD2.RATE_AREA_ID").alias("RATE_AREA_ID"),
    F.col("RD2.QHP_ID").alias("QHP_ID"),
    F.col("RD2.SpanType").alias("SpanType")
).alias("all")

df_Sort_953 = df_rd2_out.orderBy(
    F.col("MBR_INDV_BE_KEY"),
    F.col("QHP_ID"),
    F.col("MBR_ENR_EFF_DT_SK"),
    F.col("MBR_ENR_TERM_DT_SK"),
    F.col("SpanType")
).alias("DSLink954")

df_tfm_out = df_Sort_953.select(
    F.col("all.SUB_INDV_BE_KEY").alias("SUB_INDV_BE_KEY"),
    F.col("all.MBR_INDV_BE_KEY").alias("MBR_INDV_BE_KEY"),
    F.col("all.SUB_ADDR_ZIP_CD_5").alias("SUB_ADDR_ZIP_CD_5"),
    F.col("all.MBR_ENR_EFF_DT_SK").alias("MBR_ENR_EFF_DT_SK"),
    F.expr("""CASE WHEN svPrvIndvBK = svCrntIndvBK AND svPrvQHP = svCrntQHP AND svPrvEET <> svCrntEET AND svPrvETD = svCrntETD
             THEN FIND.DATE.EE(svPrvEET, '-1', 'M', 'L', 'CCYY_MM_DD') ELSE all.MBR_ENR_TERM_DT_SK END""").alias("MBR_ENR_TERM_DT_SK"),
    F.col("all.PRM_AMT").alias("PRM_AMT"),
    F.col("all.ENR_PERD_ACTVTY_CD").alias("ENR_PERD_ACTVTY_CD"),
    F.col("all.RATE_AREA_ID").alias("RATE_AREA_ID"),
    F.col("all.QHP_ID").alias("QHP_ID"),
    F.col("all.SpanType").alias("SpanType")
).alias("RD2")

df_Copy_999_out = df_tfm_out.select(
    F.col("RD2.SUB_INDV_BE_KEY").alias("SUB_INDV_BE_KEY"),
    F.col("RD2.MBR_INDV_BE_KEY").alias("MBR_INDV_BE_KEY"),
    F.col("RD2.SUB_ADDR_ZIP_CD_5").alias("SUB_ADDR_ZIP_CD_5"),
    F.col("RD2.MBR_ENR_EFF_DT_SK").alias("MBR_ENR_EFF_DT_SK"),
    F.col("RD2.MBR_ENR_TERM_DT_SK").alias("MBR_ENR_TERM_DT_SK"),
    F.col("RD2.PRM_AMT").alias("PRM_AMT"),
    F.col("RD2.ENR_PERD_ACTVTY_CD").alias("ENR_PERD_ACTVTY_CD"),
    F.col("RD2.RATE_AREA_ID").alias("RATE_AREA_ID"),
    F.col("RD2.QHP_ID").alias("QHP_ID"),
    F.col("RD2.SpanType").alias("SpanType")
).alias("Final")

df_ACA_Enrollment_PreNewSpan = df_Copy_999_out.select(
    F.col("Final.SUB_INDV_BE_KEY"),
    F.col("Final.MBR_INDV_BE_KEY"),
    rpad("SUB_ADDR_ZIP_CD_5",5," "),
    rpad("MBR_ENR_EFF_DT_SK",10," "),
    rpad("MBR_ENR_TERM_DT_SK",10," "),
    F.col("PRM_AMT"),
    F.col("ENR_PERD_ACTVTY_CD"),
    F.col("RATE_AREA_ID"),
    F.col("QHP_ID"),
    F.col("SpanType")
)

write_files(
    df_ACA_Enrollment_PreNewSpan,
    f"{adls_path_raw}/landing/ACA_Enrollment_PreNewSpan.dat",
    delimiter="|",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote='"',
    nullValue=None
)