# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #                                   Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------                                   -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Ralph Tucker          5/21/2009        Web Realign 3500                             New ETL                                                                                     devlIDSNew                    Steph Goddard           05/28/2009
# MAGIC 
# MAGIC  SAndrew               2009-08-18         Production Support                            Brought down from ids20 to testIDS for emergency prod fix         testIDS                    
# MAGIC                                                                                                                   Changed the SQLServer Update Mode from Clear table to Insert/Update
# MAGIC                                                                                                                     Added tbl to the delete process 
# MAGIC Kalyan Neelam       2010-04-14         4428                                                 Added 19 new fields at the end                                                      IntegrateWrhsDevl         Steph Goddard           04/19/2010
# MAGIC 
# MAGIC Bhoomi Dasari        2011-10-17        4673                                              Changed WAIST_CRCMFR_NO to Decimal(7,2) from Integer(10)     IntegrateWrhsDevl           SAndrew                  2011-10-21       
# MAGIC   
# MAGIC Archana Palivela             09/14/2013          5114                                 Original Programming(Server to Parallel)                                             IntegrateWrhsDevl           Jag Yelavarthi            2013-11-28

# MAGIC Code SK lookups for Denormalization
# MAGIC 
# MAGIC Lookup Keys:
# MAGIC CLM_DIAG_ORDNL_CD_SK
# MAGIC Write CLM_DM_CLM_DIAG Data into a Sequential file for Load Job IdsDmProdDmDedctCmpntLoad.
# MAGIC Read all the Data from IDS CLM_DIAG, DIAG_CD Tables;
# MAGIC Add Defaults and Null Handling.
# MAGIC Reference Code Mapping Data for Code SK lookups
# MAGIC Job Name: IdsDmClmDmClmDiagExtr
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
DataMartRunCycle = get_widget_value('DataMartRunCycle','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

df_db2_CLM_DIAG_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""
SELECT 
DIAG.SRC_SYS_CD_SK,
DIAG.CLM_ID,
CLM_DIAG_ORDNL_CD_SK,
DIAG_CD,
DIAG.DIAG_CD_SK,
DIAG_CD_DESC
FROM
{IDSOwner}.CLM_DIAG DIAG,
{IDSOwner}.W_WEBDM_ETL_DRVR EXTR,
{IDSOwner}.DIAG_CD CD,
{IDSOwner}.CD_MPPNG MAP
WHERE
DIAG.SRC_SYS_CD_SK = EXTR.SRC_SYS_CD_SK 
AND DIAG.CLM_ID = EXTR.CLM_ID
AND DIAG.DIAG_CD_SK = CD.DIAG_CD_SK
AND DIAG.CLM_DIAG_ORDNL_CD_SK = MAP.CD_MPPNG_SK
"""
    )
    .load()
)

df_DB2_CD_MPPNG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""
SELECT 
CD_MPPNG_SK,
COALESCE(SRC_CD,'UNK') SRC_CD,
COALESCE(SRC_CD_NM,'UNK') SRC_CD_NM,
COALESCE(TRGT_CD,'UNK') TRGT_CD,
COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM
FROM {IDSOwner}.CD_MPPNG
"""
    )
    .load()
)

df_copy_35_lnk_SrcSysCd_Lkp = df_DB2_CD_MPPNG.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("SRC_CD").alias("SRC_CD"),
    F.col("SRC_CD_NM").alias("SRC_CD_NM"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_copy_35_lnk_DiagCdNm_Lkp = df_DB2_CD_MPPNG.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("SRC_CD").alias("SRC_CD"),
    F.col("SRC_CD_NM").alias("SRC_CD_NM"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_lkp_Codes_1 = df_db2_CLM_DIAG_in.alias("lnk_IdsDmClmDmClmDiagExtr_InABC").join(
    df_copy_35_lnk_DiagCdNm_Lkp.alias("lnk_DiagCdNm_Lkp"),
    F.col("lnk_IdsDmClmDmClmDiagExtr_InABC.CLM_DIAG_ORDNL_CD_SK") == F.col("lnk_DiagCdNm_Lkp.CD_MPPNG_SK"),
    "left"
)

df_lkp_Codes = df_lkp_Codes_1.join(
    df_copy_35_lnk_SrcSysCd_Lkp.alias("lnk_SrcSysCd_Lkp"),
    F.col("lnk_IdsDmClmDmClmDiagExtr_InABC.SRC_SYS_CD_SK") == F.col("lnk_SrcSysCd_Lkp.CD_MPPNG_SK"),
    "left"
).select(
    F.col("lnk_IdsDmClmDmClmDiagExtr_InABC.CLM_ID").alias("CLM_ID"),
    F.col("lnk_SrcSysCd_Lkp.TRGT_CD").alias("SRC_SYS_CD"),
    F.col("lnk_DiagCdNm_Lkp.TRGT_CD").alias("CLM_DIAG_ORDNL_CD"),
    F.col("lnk_IdsDmClmDmClmDiagExtr_InABC.DIAG_CD").alias("DIAG_CD"),
    F.col("lnk_IdsDmClmDmClmDiagExtr_InABC.DIAG_CD_DESC").alias("DIAG_CD_DESC")
)

df_xfrm_BusinessLogic = (
    df_lkp_Codes
    .withColumn(
        "SRC_SYS_CD",
        F.when(trim(F.col("SRC_SYS_CD")) == "", F.lit("UNK")).otherwise(F.col("SRC_SYS_CD"))
    )
    .withColumn(
        "CLM_DIAG_ORDNL_CD",
        F.when(trim(F.col("CLM_DIAG_ORDNL_CD")) == "", F.lit("UNK")).otherwise(F.col("CLM_DIAG_ORDNL_CD"))
    )
    .withColumn("LAST_UPDT_RUN_CYC_NO", F.lit(DataMartRunCycle))
)

df_final = df_xfrm_BusinessLogic.select(
    "SRC_SYS_CD",
    "CLM_ID",
    "CLM_DIAG_ORDNL_CD",
    "DIAG_CD",
    "DIAG_CD_DESC",
    "LAST_UPDT_RUN_CYC_NO"
)

df_final = (
    df_final
    .withColumn("SRC_SYS_CD", F.rpad(F.col("SRC_SYS_CD"), <...>, " "))
    .withColumn("CLM_ID", F.rpad(F.col("CLM_ID"), <...>, " "))
    .withColumn("CLM_DIAG_ORDNL_CD", F.rpad(F.col("CLM_DIAG_ORDNL_CD"), <...>, " "))
    .withColumn("DIAG_CD", F.rpad(F.col("DIAG_CD"), <...>, " "))
    .withColumn("DIAG_CD_DESC", F.rpad(F.col("DIAG_CD_DESC"), <...>, " "))
    .withColumn("LAST_UPDT_RUN_CYC_NO", F.rpad(F.col("LAST_UPDT_RUN_CYC_NO"), <...>, " "))
)

write_files(
    df_final,
    f"{adls_path}/load/CLM_DM_CLM_DIAG.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)