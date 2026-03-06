# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2009 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #               Change Description                             Development Project      Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------     ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Raja Gummadi                  2015-04-22      5460                                Originally Programmed                            IntegrateNewDevl          Kalyan Neelam             2015-04-24
# MAGIC 
# MAGIC 
# MAGIC Manasa Andru                  2020-04-10       US - 191510                Changed the Balancing file write mode        IntegrateDev2         Jaideep Mankala         04/09/2020
# MAGIC                                                                                                                         to 'Overwrite' from 'Append'
# MAGIC 
# MAGIC 
# MAGIC Venkat M                       2021-12-16      475647                                  Added SRC_SYS_CD to the Look up Key            IntegrateDev2             Goutham K                     2021-12-16
# MAGIC                                                                                                              Modified Lookup SQL

# MAGIC JobName: CblTalnMrLnSvcCatXfrm
# MAGIC 
# MAGIC Strip field function and Tranforamtion rules are applied on the data Extracted from inboundfile
# MAGIC Strip Carriage Return, Line Feed, and Tab.
# MAGIC Transformed Data will land into a Dataset for Primary Keying job.
# MAGIC 
# MAGIC Data is partitioned on Natural Key Columns.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
SrcSysCd = get_widget_value('SrcSysCd','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
RunID = get_widget_value('RunID','')
RunIDTimeStamp = get_widget_value('RunIDTimeStamp','')
FileName = get_widget_value('FileName','')
CurrDate = get_widget_value('CurrDate','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query_db2_MR_LN_SVC_CAT_In = f"""
SELECT
    CAT.MR_LN_ID,
    CD.TRGT_CD AS SRC_SYS_CD,
    MAX(CAT.EFF_DT_SK) AS EFF_DT_SK,
    CAT.TERM_DT_SK
FROM
    {IDSOwner}.MR_LN_SVC_CAT CAT
    INNER JOIN {IDSOwner}.CD_MPPNG CD
        ON CAT.SRC_SYS_CD_SK = CD.CD_MPPNG_SK
    INNER JOIN (
        SELECT MR_LN_ID, SRC_SYS_CD_SK, MAX(EFF_DT_SK) EFF_DT_SK
        FROM BCBSKC.MR_LN_SVC_CAT B
        GROUP BY SRC_SYS_CD_SK, MR_LN_ID
    ) B
        ON CAT.MR_LN_ID = B.MR_LN_ID
        AND CAT.SRC_SYS_CD_SK = B.SRC_SYS_CD_SK
        AND CAT.EFF_DT_SK = B.EFF_DT_SK
GROUP BY
    CAT.MR_LN_ID,
    CAT.SRC_SYS_CD_SK,
    CD.TRGT_CD,
    CAT.TERM_DT_SK
"""
df_db2_MR_LN_SVC_CAT_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_MR_LN_SVC_CAT_In)
    .load()
)

df_ds_MR_LN_SVC_CAT_Extr = spark.read.parquet(
    f"{adls_path}/ds/MR_LN_SVC_CAT.{SrcSysCd}.extr.{RunID}.parquet"
)

df_StripField = df_ds_MR_LN_SVC_CAT_Extr.select(
    F.col("MR_LN_ID"),
    F.col("ACTURL_HLTH_CST_GRP_SUM_ID"),
    F.col("CST_MDL_UTILITY_ID"),
    F.col("MR_LN_DESC"),
    F.col("MR_LN_PFX_DESC"),
    F.col("MR_LN_BODY_DESC"),
    F.col("MR_LN_SFX_DESC")
)

df_Xfrm_BusinessRules = df_StripField.select(
    F.concat(F.lit(SrcSysCd), F.lit(";"), F.col("MR_LN_ID")).alias("PRI_NAT_KEY_STRING"),
    F.lit(RunIDTimeStamp).alias("FIRST_RECYC_TS"),
    F.lit(0).alias("MR_LN_SVC_CAT_SK"),
    F.upper(F.col("MR_LN_ID")).alias("MR_LN_ID"),
    F.lit(SrcSysCd).alias("SRC_SYS_CD"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("ACTURL_HLTH_CST_GRP_SUM_ID").alias("ACTURL_HLTH_CST_GRP_SUM_ID"),
    F.col("CST_MDL_UTILITY_ID").alias("CST_MDL_UTILITY_ID"),
    F.col("MR_LN_DESC").alias("MR_LN_DESC"),
    F.col("MR_LN_PFX_DESC").alias("MR_LN_PFX_DESC"),
    F.col("MR_LN_BODY_DESC").alias("MR_LN_BODY_DESC"),
    F.col("MR_LN_SFX_DESC").alias("MR_LN_SFX_DESC"),
    F.lit(CurrDate).alias("EFF_DT_SK"),
    F.lit("2199-12-31").alias("TERM_DT_SK")
)

df_Lookup_49 = (
    df_Xfrm_BusinessRules.alias("effdt")
    .join(
        df_db2_MR_LN_SVC_CAT_In.alias("Extr"),
        on=[
            F.col("effdt.MR_LN_ID") == F.col("Extr.MR_LN_ID"),
            F.col("effdt.SRC_SYS_CD") == F.col("Extr.SRC_SYS_CD"),
        ],
        how="left",
    )
    .select(
        F.col("effdt.PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
        F.col("effdt.FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
        F.col("effdt.MR_LN_SVC_CAT_SK").alias("MR_LN_SVC_CAT_SK"),
        F.col("effdt.MR_LN_ID").alias("MR_LN_ID"),
        F.col("Extr.EFF_DT_SK").alias("EFF_DT_SK"),
        F.col("effdt.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("effdt.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("effdt.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("effdt.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.col("effdt.ACTURL_HLTH_CST_GRP_SUM_ID").alias("ACTURL_HLTH_CST_GRP_SUM_ID"),
        F.col("effdt.CST_MDL_UTILITY_ID").alias("CST_MDL_UTILITY_ID"),
        F.col("effdt.MR_LN_DESC").alias("MR_LN_DESC"),
        F.col("effdt.MR_LN_PFX_DESC").alias("MR_LN_PFX_DESC"),
        F.col("effdt.MR_LN_BODY_DESC").alias("MR_LN_BODY_DESC"),
        F.col("effdt.MR_LN_SFX_DESC").alias("MR_LN_SFX_DESC"),
        F.col("Extr.TERM_DT_SK").alias("TERM_DT_SK"),
    )
)

df_xfrmdt = df_Lookup_49

df_xfrmdt_pkeyOut = (
    df_xfrmdt
    .withColumn(
        "EFF_DT_SK",
        F.when(
            (F.length(trim(F.col("EFF_DT_SK"))) == 0)
            | (F.col("TERM_DT_SK") != F.lit("2199-12-31")),
            F.lit(CurrDate)
        ).otherwise(F.col("EFF_DT_SK"))
    )
    .withColumn("TERM_DT_SK", F.lit("2199-12-31"))
    .select(
        F.col("PRI_NAT_KEY_STRING"),
        F.col("FIRST_RECYC_TS"),
        F.col("MR_LN_SVC_CAT_SK"),
        F.col("MR_LN_ID"),
        F.col("EFF_DT_SK"),
        F.col("SRC_SYS_CD"),
        F.col("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("SRC_SYS_CD_SK"),
        F.col("ACTURL_HLTH_CST_GRP_SUM_ID"),
        F.col("CST_MDL_UTILITY_ID"),
        F.col("MR_LN_DESC"),
        F.col("MR_LN_PFX_DESC"),
        F.col("MR_LN_BODY_DESC"),
        F.col("MR_LN_SFX_DESC"),
        F.col("TERM_DT_SK"),
    )
    .withColumn("EFF_DT_SK", F.rpad(F.col("EFF_DT_SK"), 10, " "))
    .withColumn("TERM_DT_SK", F.rpad(F.col("TERM_DT_SK"), 10, " "))
)

df_xfrmdt_snapshot = (
    df_xfrmdt
    .withColumn(
        "EFF_DT_SK",
        F.when(
            (F.length(trim(F.col("EFF_DT_SK"))) == 0)
            | (F.col("TERM_DT_SK") != F.lit("2199-12-31")),
            F.lit(CurrDate)
        ).otherwise(F.col("EFF_DT_SK"))
    )
    .select(
        F.col("MR_LN_ID"),
        F.col("EFF_DT_SK"),
        F.col("SRC_SYS_CD_SK"),
    )
    .withColumn("EFF_DT_SK", F.rpad(F.col("EFF_DT_SK"), 10, " "))
)

write_files(
    df_xfrmdt_pkeyOut,
    f"MR_LN_SVC_CAT.{SrcSysCd}.xfrm.{RunID}.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

write_files(
    df_xfrmdt_snapshot,
    f"{adls_path}/load/B_MR_LN_SVC_CAT.{SrcSysCd}.dat",
    ",",
    "overwrite",
    False,
    False,
    "^",
    None
)