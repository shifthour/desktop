# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2010 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC CALLED BY: IdsMbrLmtExtrSeq
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
# MAGIC Manasa Andru         10/6/2013       TFS-1275              Changed the job to two extracts to get rid of warnings                 IntegrateNewDevl           Kalyan Neelam             2013-10-09

# MAGIC Extract for prior year - extract product data for prior year, extract membership for prior year.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType, DecimalType, StructType, StructField
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


CurrRunCycle = get_widget_value('CurrRunCycle','100')
CurrDate = get_widget_value('CurrDate','2010-07-19')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
CurrYear = get_widget_value('CurrYear','2010')
PrevYear = get_widget_value('PrevYear','2009')
YrEndDate = get_widget_value('YrEndDate','2009-12-31')
ProdCmpntTypCdSk = get_widget_value('ProdCmpntTypCdSk','844281128')
ClsPlnProdCatCdMedSk = get_widget_value('ClsPlnProdCatCdMedSk','1949')
ClsPlnProdCatCdDntlSk = get_widget_value('ClsPlnProdCatCdDntlSk','1946')
MbrRelshpCdSk = get_widget_value('MbrRelshpCdSk','1975')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query = f"""SELECT ENR.SRC_SYS_CD_SK,  
               ENR.MBR_UNIQ_KEY,
               MAP1.TRGT_CD,
               MAX(ENR.TERM_DT_SK),
               case when MAX(ENR.TERM_DT_SK) > '{YrEndDate}'
                        then '{YrEndDate}'
                        else  MAX(ENR.TERM_DT_SK)
               end as Max_End_Dt
 FROM {IDSOwner}.MBR_ENR ENR,
           {IDSOwner}.CD_MPPNG MAP1
Where 
           ENR.EFF_DT_SK <= '{YrEndDate}' 
  AND ENR.TERM_DT_SK >= '{PrevYear}-01-01'
  AND ENR.ELIG_IN = 'Y' 
  AND ENR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK = MAP1.CD_MPPNG_SK
  AND MAP1.TRGT_CD IN ('MED','DNTL')
Group by ENR.SRC_SYS_CD_SK,ENR.MBR_UNIQ_KEY,MAP1.TRGT_CD

{IDSOwner}.MBR_ENR ENR
"""

df_IDSPrevMaxEnr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_Trans1 = df_IDSPrevMaxEnr.select(
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("PROD_ACCUM_ID").alias("PROD_ACCUM_ID"),
    F.col("TERM_DT_SK").alias("TERM_DT_SK"),
    F.col("AS_OF_DT_SK").alias("AS_OF_DT_SK")
)

df_Trans1 = df_Trans1.withColumn("TERM_DT_SK", F.rpad("TERM_DT_SK", 10, " "))
df_Trans1 = df_Trans1.withColumn("AS_OF_DT_SK", F.rpad("AS_OF_DT_SK", 10, " "))

execute_dml(
    f"EXEC {IDSOwner}.BCSP_TRUNCATE('{IDSOwner}', 'W_MBR_ACCUM')",
    jdbc_url,
    jdbc_props
)

execute_dml(
    "DROP TABLE IF EXISTS STAGING.IdsPFmlyLmtTableLd1Extr_PopulateWMaxEnrTerm_temp",
    jdbc_url,
    jdbc_props
)

df_Trans1.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.IdsPFmlyLmtTableLd1Extr_PopulateWMaxEnrTerm_temp") \
    .mode("append") \
    .save()

merge_sql = f"""
MERGE INTO {IDSOwner}.W_MBR_ACCUM AS T
USING STAGING.IdsPFmlyLmtTableLd1Extr_PopulateWMaxEnrTerm_temp AS S
ON T.SRC_SYS_CD_SK = S.SRC_SYS_CD_SK
AND T.MBR_UNIQ_KEY = S.MBR_UNIQ_KEY
AND T.PROD_ACCUM_ID = S.PROD_ACCUM_ID
WHEN MATCHED THEN
  UPDATE SET
    T.TERM_DT_SK = S.TERM_DT_SK,
    T.AS_OF_DT_SK = S.AS_OF_DT_SK
WHEN NOT MATCHED THEN
  INSERT (SRC_SYS_CD_SK, MBR_UNIQ_KEY, PROD_ACCUM_ID, TERM_DT_SK, AS_OF_DT_SK)
  VALUES (S.SRC_SYS_CD_SK, S.MBR_UNIQ_KEY, S.PROD_ACCUM_ID, S.TERM_DT_SK, S.AS_OF_DT_SK);
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)

create_sql = f"""CREATE TABLE {IDSOwner}.W_BSD_ACCUM (
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
PROD_CMPNT_TYP_CD VARCHAR(20) NOT NULL,
 PRIMARY KEY (PROD_SK)
)"""
execute_dml(create_sql, jdbc_url, jdbc_props)

drop_sql = f"DROP TABLE {IDSOwner}.W_MBR_ACCUM"
execute_dml(drop_sql, jdbc_url, jdbc_props)

final_sql = f"{IDSOwner}.W_MBR_ACCUM"
execute_dml(final_sql, jdbc_url, jdbc_props)