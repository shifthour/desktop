# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2010 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC CALLED BY: IIdsMbrLmtExtrSeq
# MAGIC 
# MAGIC 
# MAGIC PROCESSING:   Select Ids MBR_ENR rows for P table creation
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Ralph Tucker          06/15/2010      4113                      IntegrateNewDevl                                                                       IntegrateNewDevl            Steph Goddard          06/23/2010
# MAGIC Steph Goddard        07/08/2010     4113                      changed to extract current product/member and compare          RebuildIntNewDevl
# MAGIC                                                                                       with prior to only select prior records that have current membership
# MAGIC 
# MAGIC Manasa Andru         10/6/2013       TFS-1275              Changed the job to two extracts to get rid of warnings                 IntegrateNewDevl           Kalyan Neelam           2013-10-09

# MAGIC Extract for current year - extract product data for current year, only extract membership if the member is CURRENT
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


CurrRunCycle = get_widget_value("CurrRunCycle","100")
CurrDate = get_widget_value("CurrDate","2010-07-19")
IDSOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")
CurrYear = get_widget_value("CurrYear","2010")
PrevYear = get_widget_value("PrevYear","2009")
YrEndDate = get_widget_value("YrEndDate","2009-12-31")
ProdCmpntTypCdSk = get_widget_value("ProdCmpntTypCdSk","844281128")
ClsPlnProdCatCdMedSk = get_widget_value("ClsPlnProdCatCdMedSk","1949")
ClsPlnProdCatCdDntlSk = get_widget_value("ClsPlnProdCatCdDntlSk","1946")
MbrRelshpCdSk = get_widget_value("MbrRelshpCdSk","1975")

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query = f"""SELECT 
 ENR.SRC_SYS_CD_SK,
 ENR.MBR_UNIQ_KEY,
 MAP1.TRGT_CD,
 MAX(ENR.TERM_DT_SK) as term_dt_sk_for_max,
 CASE WHEN MAX(ENR.TERM_DT_SK) > '{CurrDate}' THEN '{CurrDate}' ELSE MAX(ENR.TERM_DT_SK) END as Max_End_Dt
FROM {IDSOwner}.MBR_ENR ENR,
     {IDSOwner}.CD_MPPNG MAP1
WHERE 
     ENR.EFF_DT_SK <= '{CurrDate}'
 AND ENR.TERM_DT_SK >= '{CurrYear}-01-01'
 AND ENR.ELIG_IN = 'Y'
 AND ENR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK = MAP1.CD_MPPNG_SK
 AND MAP1.TRGT_CD IN ('MED','DNTL')
GROUP BY ENR.SRC_SYS_CD_SK, ENR.MBR_UNIQ_KEY, MAP1.TRGT_CD
"""

df_IDSCurMaxEnr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_Trans2 = df_IDSCurMaxEnr.withColumn("SRC_SYS_CD_SK", col("SRC_SYS_CD_SK")) \
    .withColumn("MBR_UNIQ_KEY", col("MBR_UNIQ_KEY")) \
    .withColumn("PROD_ACCUM_ID", lit(None)) \
    .withColumn("TERM_DT_SK", col("Max_End_Dt")) \
    .withColumn("AS_OF_DT_SK", lit(None))

df_Trans2 = df_Trans2.withColumn("TERM_DT_SK", rpad(col("TERM_DT_SK"), 10, " "))
df_Trans2 = df_Trans2.withColumn("AS_OF_DT_SK", rpad(col("AS_OF_DT_SK"), 10, " "))
df_Trans2 = df_Trans2.select(
    "SRC_SYS_CD_SK",
    "MBR_UNIQ_KEY",
    "PROD_ACCUM_ID",
    "TERM_DT_SK",
    "AS_OF_DT_SK"
)

before_sql = f"CALL {IDSOwner}.BCSP_TRUNCATE('{IDSOwner}', 'W_MBR_ACCUM')"
execute_dml(before_sql, jdbc_url, jdbc_props)

drop_temp_sql = "DROP TABLE IF EXISTS STAGING.IdsPFmlyLmtTableLd2Extr_LoadWTable_temp"
execute_dml(drop_temp_sql, jdbc_url, jdbc_props)

df_Trans2.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.IdsPFmlyLmtTableLd2Extr_LoadWTable_temp") \
    .mode("overwrite") \
    .save()

merge_sql = f"""
MERGE INTO {IDSOwner}.W_MBR_ACCUM AS t
USING STAGING.IdsPFmlyLmtTableLd2Extr_LoadWTable_temp AS s
ON t.SRC_SYS_CD_SK = s.SRC_SYS_CD_SK
AND t.MBR_UNIQ_KEY = s.MBR_UNIQ_KEY
AND t.PROD_ACCUM_ID = s.PROD_ACCUM_ID
WHEN MATCHED THEN
  UPDATE SET
    t.TERM_DT_SK = s.TERM_DT_SK,
    t.AS_OF_DT_SK = s.AS_OF_DT_SK
WHEN NOT MATCHED THEN
  INSERT (
    SRC_SYS_CD_SK,
    MBR_UNIQ_KEY,
    PROD_ACCUM_ID,
    TERM_DT_SK,
    AS_OF_DT_SK
  )
  VALUES (
    s.SRC_SYS_CD_SK,
    s.MBR_UNIQ_KEY,
    s.PROD_ACCUM_ID,
    s.TERM_DT_SK,
    s.AS_OF_DT_SK
  );
"""
execute_dml(merge_sql, jdbc_url, jdbc_props)

create_table_sql = f"""
CREATE TABLE {IDSOwner}.W_BSD_ACCUM (
PROD_SK INTEGER NOT NULL,
ACCUM_NO INTEGER NOT NULL,
BNF_SUM_DTL_NTWK_TYP_CD_SK INTEGER NOT NULL,
BNF_SUM_DTL_TYP_CD_SK INTEGER NOT NULL,
BLUEKC_DPLY_IN CHAR(1) NOT NULL,
MBR_360_DPLY_IN CHAR(1) NOT NULL,
EXTRNL_DPLY_ACCUM_DESC VARCHAR(255) NOT NULL,
INTRNL_DPLY_ACCUM_DESC VARCHAR(255) NOT NULL,
LMT_AMT DECIMAL(13,2) NOT NULL,
STOPLOSS_AMT DECIMAL(13,2) NOT NULL,
PROD_ID VARCHAR(20) NOT NULL,
PROD_CMPNT_PFX_ID VARCHAR(20) NOT NULL,
PROD_CMPNT_EFF_DT_SK CHAR(10) NOT NULL,
PROD_CMPNT_TERM_DT_SK CHAR(10) NOT NULL,
PROD_CMPNT_TYP_CD_SK INTEGER NOT NULL,
PROD_CMPNT_TYP_CD VARCHAR(20) NOT NULL
, PRIMARY KEY (PROD_SK) )
"""
execute_dml(create_table_sql, jdbc_url, jdbc_props)

drop_w_mbr_accum_sql = f"DROP TABLE {IDSOwner}.W_MBR_ACCUM"
execute_dml(drop_w_mbr_accum_sql, jdbc_url, jdbc_props)

final_sql = f"{IDSOwner}.W_MBR_ACCUM"
execute_dml(final_sql, jdbc_url, jdbc_props)