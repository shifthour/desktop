# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC The IDS PDX_NTWK job require that any row not represented in the new NABP.dat file have it's TERM_DT_SK set to the run date.
# MAGIC 
# MAGIC This is done by setting the term date and the last update run cycle values in every row in the table to the Current Run Cycle to the Current Run Date, respectively.  This step is done before running the extract and foreign key jobs.  The extract job will update the term date field with the default value of 9999-12-31.
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:     For all rows in PDX_NTWK, loads current run date into TERM_DT_SK field 
# MAGIC                               and current run cycle into LAST_UPDT_RUN_CYC_EXCTN_SK field
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #                               Change Description                                                Development Project      Code Reviewer          Date Reviewed       
# MAGIC --------------------            --------------------     ------------------------                              ----------------------------------------                                        --------------------------------       -------------------------------   ----------------------------       
# MAGIC Giri Mallavaram        11/04/2019      6131 PBM Replacement               Initial Programming                                                    IntegrateDevl                 Kalyan Neelam          2019-11-20
# MAGIC 
# MAGIC Velmani K                07/10/2020      6264 - PBM Phase II -                   Added condition to exclude                                       IntegrateDev5
# MAGIC                                                          Government Programs                   MedD Records while updating the TERM_DT

# MAGIC This SQL is only to initiate the routines in the Transform Stage.
# MAGIC 
# MAGIC The data row is never used.
# MAGIC For every row, sets the term date to the Current Run Date and the last update run cycle to the Current Run Cycle.
# MAGIC Optum Pharmacy Network Term Date Update
# MAGIC this job needs to run before the OptumPdxNtwkExtr.  It sets all of the term date to today for al of the Optum Pharmacy Networks.   It does not set the term dates for the Argus Pharmacys
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
CurrRunDate = get_widget_value('CurrRunDate','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
SrcSysCd = get_widget_value('SrcSysCd','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query = "SELECT IBMREQD FROM sysibm.sysdummy1"
df_SYSDUMMY1 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_X = df_SYSDUMMY1.select(
    F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(CurrRunDate).alias("TERM_DT_SK")
)

execute_dml("DROP TABLE IF EXISTS STAGING.OptumPdxNtwkTermDtUpdt_PdxNtwkUpdt_temp", jdbc_url, jdbc_props)
df_X.write.jdbc(url=jdbc_url, table="STAGING.OptumPdxNtwkTermDtUpdt_PdxNtwkUpdt_temp", mode="overwrite", properties=jdbc_props)

merge_sql = f"""
MERGE INTO {IDSOwner}.PDX_NTWK AS T
USING (
    SELECT
        LAST_UPDT_RUN_CYC_EXCTN_SK,
        TERM_DT_SK
    FROM STAGING.OptumPdxNtwkTermDtUpdt_PdxNtwkUpdt_temp
) AS S
ON (
    T.TERM_DT_SK = '9999-12-31'
    AND T.PDX_NTWK_CD_SK IN (
        SELECT
            CD_MPPNG_SK
        FROM {IDSOwner}.CD_MPPNG
        WHERE SRC_SYS_CD='OPTUMRX'
          AND TRGT_DOMAIN_NM='PHARMACY NETWORK'
          AND SRC_CD NOT LIKE '45%'
    )
)
WHEN MATCHED THEN
    UPDATE SET
        T.LAST_UPDT_RUN_CYC_EXCTN_SK = S.LAST_UPDT_RUN_CYC_EXCTN_SK,
        T.TERM_DT_SK = S.TERM_DT_SK
WHEN NOT MATCHED THEN
    DO NOTHING;
"""
execute_dml(merge_sql, jdbc_url, jdbc_props)

snapshot_query = f"SELECT SRC_SYS_CD_SK, PROV_ID, PDX_NTWK_CD_SK FROM {IDSOwner}.PDX_NTWK WHERE LAST_UPDT_RUN_CYC_EXCTN_SK = {CurrRunCycle}"
df_PdxNtwkUpdt = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", snapshot_query)
    .load()
)

write_files(
    df_PdxNtwkUpdt.select("SRC_SYS_CD_SK","PROV_ID","PDX_NTWK_CD_SK"),
    f"{adls_path}/load/B_PDX_NTWK.NABP.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)