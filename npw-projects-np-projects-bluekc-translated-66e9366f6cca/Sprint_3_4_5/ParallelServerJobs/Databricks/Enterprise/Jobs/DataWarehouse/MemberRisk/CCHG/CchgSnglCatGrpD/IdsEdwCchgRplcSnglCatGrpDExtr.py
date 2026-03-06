# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2015 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC PROCESSING:  Extract data from IDS CCHG_SNGL_CAT_GRP_D_RPLC table
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date               Project/Altiris #               Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------   -----------------------------------    ---------------------------------------------------------   ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Goutham Kalidindi             4/8/2022       US-500022                    New Job to Extrac CCHG RPLC data        EnterpriseDev2      Reddy Sanam              04/13/2022

# MAGIC Read all the Data from IDS CCHG_SNGL_CAT_GRP_RPLC Table;
# MAGIC Add Defaults and Null Handling.
# MAGIC Write CCHG_SNGL_CAT_GRP_D_RPLC Data into a Sequential file for Load Job
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
ExtractRunCycle = get_widget_value('ExtractRunCycle','')
CurrRunCycleDate = get_widget_value('CurrRunCycleDate','')
CurrRunCycle = get_widget_value('CurrRunCycle','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

sql_db2_CCHG_SNGL_CAT_GRP_RPLC_in = f"""SELECT
CSCG.CCHG_SNGL_CAT_GRP_RPLC_SK as CCHG_SNGL_CAT_GRP_SK,
CSCG.CCHG_SNGL_CAT_GRP_CD,
CSCG.SRC_SYS_CD_SK,
CSCG.CRT_RUN_CYC_EXCTN_SK,
CSCG.LAST_UPDT_RUN_CYC_EXCTN_SK,
CSCG.ICD_VRSN_CD_SK,
CSCG.CCHG_SNGL_CAT_GRP_DESC,
CSCG.ICD_DIAG_CLM_MPPNG_DESC,
COALESCE(CD1.TRGT_CD, 'UNK') as SRC_SYS_CD,
CSCG.VRSN_ID
FROM {IDSOwner}.CCHG_SNGL_CAT_GRP_RPLC AS CSCG
LEFT OUTER JOIN
{IDSOwner}.CD_MPPNG AS CD1
ON
CSCG.SRC_SYS_CD_SK = CD1.CD_MPPNG_SK
WHERE
CSCG.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtractRunCycle}
"""

df_db2_CCHG_SNGL_CAT_GRP_RPLC_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", sql_db2_CCHG_SNGL_CAT_GRP_RPLC_in)
    .load()
)

df_IdsOut = df_db2_CCHG_SNGL_CAT_GRP_RPLC_in

sql_db2_Cd_Mppng_In = f"""SELECT
CD.CD_MPPNG_SK,
COALESCE(CD.TRGT_CD, 'UNK') AS TRGT_CD,
COALESCE(CD.TRGT_CD_NM, 'UNK') AS TRGT_CD_NM
FROM {IDSOwner}.CD_MPPNG CD
"""

df_db2_Cd_Mppng_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", sql_db2_Cd_Mppng_In)
    .load()
)

df_lnk_Cd_Mppng_In = df_db2_Cd_Mppng_In

df_IcdVrsnCd_lkup = df_lnk_Cd_Mppng_In.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_rules = (
    df_IdsOut.join(
        df_IcdVrsnCd_lkup,
        on=[df_IdsOut["ICD_VRSN_CD_SK"] == df_IcdVrsnCd_lkup["CD_MPPNG_SK"]],
        how="left"
    )
    .select(
        df_IdsOut["CCHG_SNGL_CAT_GRP_SK"].alias("CCHG_SNGL_CAT_GRP_SK"),
        df_IdsOut["CCHG_SNGL_CAT_GRP_CD"].alias("CCHG_SNGL_CAT_GRP_CD"),
        df_IdsOut["SRC_SYS_CD"].alias("SRC_SYS_CD"),
        df_IcdVrsnCd_lkup["TRGT_CD"].alias("ICD_VRSN_CD"),
        df_IdsOut["CCHG_SNGL_CAT_GRP_DESC"].alias("CCHG_SNGL_CAT_GRP_DESC"),
        df_IdsOut["ICD_DIAG_CLM_MPPNG_DESC"].alias("ICD_DIAG_CLM_MPPNG_DESC"),
        df_IdsOut["LAST_UPDT_RUN_CYC_EXCTN_SK"].alias("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK"),
        df_IdsOut["ICD_VRSN_CD_SK"].alias("ICD_VRSN_CD_SK"),
        df_IdsOut["VRSN_ID"].alias("VRSN_ID")
    )
)

df_lnk_xfm_Data = (
    df_rules
    .filter(
        (F.col("CCHG_SNGL_CAT_GRP_SK") != 1)
        & (F.col("CCHG_SNGL_CAT_GRP_SK") != 0)
    )
    .withColumn(
        "ICD_VRSN_CD",
        F.when(F.col("ICD_VRSN_CD").isNull(), F.lit("UNK")).otherwise(F.col("ICD_VRSN_CD"))
    )
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.lit(CurrRunCycleDate), 10, " "))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.lit(CurrRunCycleDate), 10, " "))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(CurrRunCycle))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(CurrRunCycle))
    .select(
        "CCHG_SNGL_CAT_GRP_SK",
        "CCHG_SNGL_CAT_GRP_CD",
        "SRC_SYS_CD",
        "CRT_RUN_CYC_EXCTN_DT_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        "ICD_VRSN_CD",
        "CCHG_SNGL_CAT_GRP_DESC",
        "ICD_DIAG_CLM_MPPNG_DESC",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "IDS_LAST_UPDT_RUN_CYC_EXCTN_SK",
        "ICD_VRSN_CD_SK",
        "VRSN_ID"
    )
)

df_lnk_NA_out_temp = df_rules.limit(1)
df_lnk_NA_out = (
    df_lnk_NA_out_temp
    .withColumn("CCHG_SNGL_CAT_GRP_SK", F.lit(1))
    .withColumn("CCHG_SNGL_CAT_GRP_CD", F.lit("NA"))
    .withColumn("SRC_SYS_CD", F.lit("NA"))
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.lit("1753-01-01"), 10, " "))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.lit("1753-01-01"), 10, " "))
    .withColumn("ICD_VRSN_CD", F.lit("NA"))
    .withColumn("CCHG_SNGL_CAT_GRP_DESC", F.lit("NA"))
    .withColumn("ICD_DIAG_CLM_MPPNG_DESC", F.lit("NA"))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(100))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(100))
    .withColumn("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(100))
    .withColumn("ICD_VRSN_CD_SK", F.lit(1))
    .withColumn("VRSN_ID", F.lit("NA"))
    .select(
        "CCHG_SNGL_CAT_GRP_SK",
        "CCHG_SNGL_CAT_GRP_CD",
        "SRC_SYS_CD",
        "CRT_RUN_CYC_EXCTN_DT_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        "ICD_VRSN_CD",
        "CCHG_SNGL_CAT_GRP_DESC",
        "ICD_DIAG_CLM_MPPNG_DESC",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "IDS_LAST_UPDT_RUN_CYC_EXCTN_SK",
        "ICD_VRSN_CD_SK",
        "VRSN_ID"
    )
)

df_lnk_UNK_out_temp = df_rules.limit(1)
df_lnk_UNK_out = (
    df_lnk_UNK_out_temp
    .withColumn("CCHG_SNGL_CAT_GRP_SK", F.lit(0))
    .withColumn("CCHG_SNGL_CAT_GRP_CD", F.lit("UNK"))
    .withColumn("SRC_SYS_CD", F.lit("UNK"))
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.lit("1753-01-01"), 10, " "))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.lit("1753-01-01"), 10, " "))
    .withColumn("ICD_VRSN_CD", F.lit("UNK"))
    .withColumn("CCHG_SNGL_CAT_GRP_DESC", F.lit("UNK"))
    .withColumn("ICD_DIAG_CLM_MPPNG_DESC", F.lit("UNK"))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(100))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(100))
    .withColumn("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(100))
    .withColumn("ICD_VRSN_CD_SK", F.lit(0))
    .withColumn("VRSN_ID", F.lit("UNK"))
    .select(
        "CCHG_SNGL_CAT_GRP_SK",
        "CCHG_SNGL_CAT_GRP_CD",
        "SRC_SYS_CD",
        "CRT_RUN_CYC_EXCTN_DT_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        "ICD_VRSN_CD",
        "CCHG_SNGL_CAT_GRP_DESC",
        "ICD_DIAG_CLM_MPPNG_DESC",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "IDS_LAST_UPDT_RUN_CYC_EXCTN_SK",
        "ICD_VRSN_CD_SK",
        "VRSN_ID"
    )
)

df_fnl_Data = df_lnk_NA_out.unionByName(df_lnk_UNK_out).unionByName(df_lnk_xfm_Data)

df_fnl_Data_final = df_fnl_Data.select(
    "CCHG_SNGL_CAT_GRP_SK",
    "CCHG_SNGL_CAT_GRP_CD",
    "SRC_SYS_CD",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "ICD_VRSN_CD",
    "CCHG_SNGL_CAT_GRP_DESC",
    "ICD_DIAG_CLM_MPPNG_DESC",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "IDS_LAST_UPDT_RUN_CYC_EXCTN_SK",
    "ICD_VRSN_CD_SK",
    "VRSN_ID"
)

write_files(
    df_fnl_Data_final,
    f"{adls_path}/load/CCHG_SNGL_CAT_GRP_D_RPLC.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)