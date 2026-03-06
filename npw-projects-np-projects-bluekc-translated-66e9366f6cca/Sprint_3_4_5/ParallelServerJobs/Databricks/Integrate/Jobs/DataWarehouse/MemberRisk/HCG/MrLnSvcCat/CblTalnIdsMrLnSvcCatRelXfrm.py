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
# MAGIC 
# MAGIC 
# MAGIC Razia                                   2021-10-19        US-428909        Copied over from CblTalnClmLnHlthCstGrpCntl     IntegrateDev1         Goutham K                  11/9/2021
# MAGIC                                                                                                           to Load the new Rpcl Table

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
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType, StructType, StructField
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../../shared_containers/PrimaryKey/<shared_container_name>
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
extract_query = f"SELECT MR_LN_ID, MAX(EFF_DT_SK) AS EFF_DT_SK, TERM_DT_SK FROM {IDSOwner}.MR_LN_SVC_CAT GROUP BY MR_LN_ID, EFF_DT_SK, TERM_DT_SK"
df_db2_MR_LN_SVC_CAT_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_ds_MR_LN_SVC_CAT_Extr = spark.read.parquet(
    f"{adls_path}/ds/MR_LN_SVC_CAT.{SrcSysCd}.extr.{RunID}.parquet"
)

df_stripfield = df_ds_MR_LN_SVC_CAT_Extr.select(
    F.col("MR_LN_ID"),
    F.col("ACTURL_HLTH_CST_GRP_SUM_ID"),
    F.col("CST_MDL_UTILITY_ID"),
    F.col("MR_LN_DESC"),
    F.col("MR_LN_PFX_DESC"),
    F.col("MR_LN_BODY_DESC"),
    F.col("MR_LN_SFX_DESC")
)

df_xfrm_business_rules = df_stripfield.select(
    (F.lit(SrcSysCd) + F.lit(";") + F.col("MR_LN_ID")).alias("PRI_NAT_KEY_STRING"),
    F.lit(RunIDTimeStamp).alias("FIRST_RECYC_TS"),
    F.lit(0).alias("MR_LN_SVC_CAT_SK"),
    UpCase(F.col("MR_LN_ID")).alias("MR_LN_ID"),
    F.lit(SrcSysCd).alias("SRC_SYS_CD"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("ACTURL_HLTH_CST_GRP_SUM_ID"),
    F.col("CST_MDL_UTILITY_ID"),
    F.col("MR_LN_DESC"),
    F.col("MR_LN_PFX_DESC"),
    F.col("MR_LN_BODY_DESC"),
    F.col("MR_LN_SFX_DESC"),
    F.lit(CurrDate).alias("EFF_DT_SK"),
    F.lit("2199-12-31").alias("TERM_DT_SK")
)

df_lookup_49 = (
    df_xfrm_business_rules.alias("effdt")
    .join(
        df_db2_MR_LN_SVC_CAT_In.alias("Extr"),
        F.col("effdt.MR_LN_ID") == F.col("Extr.MR_LN_ID"),
        how="left"
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
        F.col("Extr.TERM_DT_SK").alias("TERM_DT_SK")
    )
)

df_xfrmdt_pkeyout = (
    df_lookup_49.alias("dt")
    .select(
        F.col("dt.PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
        F.col("dt.FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
        F.col("dt.MR_LN_SVC_CAT_SK").alias("MR_LN_SVC_CAT_SK"),
        F.col("dt.MR_LN_ID").alias("MR_LN_ID"),
        F.when(
            (F.length(trim(F.col("dt.EFF_DT_SK"))) == 0) |
            (F.col("dt.TERM_DT_SK") != F.lit("2199-12-31")),
            F.lit(CurrDate)
        ).otherwise(F.col("dt.EFF_DT_SK")).alias("EFF_DT_SK"),
        F.col("dt.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("dt.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("dt.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("dt.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.col("dt.ACTURL_HLTH_CST_GRP_SUM_ID").alias("ACTURL_HLTH_CST_GRP_SUM_ID"),
        F.col("dt.CST_MDL_UTILITY_ID").alias("CST_MDL_UTILITY_ID"),
        F.col("dt.MR_LN_DESC").alias("MR_LN_DESC"),
        F.col("dt.MR_LN_PFX_DESC").alias("MR_LN_PFX_DESC"),
        F.col("dt.MR_LN_BODY_DESC").alias("MR_LN_BODY_DESC"),
        F.col("dt.MR_LN_SFX_DESC").alias("MR_LN_SFX_DESC"),
        F.lit("2199-12-31").alias("TERM_DT_SK")
    )
)

df_xfrmdt_snapshot = (
    df_lookup_49.alias("dt")
    .select(
        F.col("dt.MR_LN_ID").alias("MR_LN_ID"),
        F.when(
            (F.length(trim(F.col("dt.EFF_DT_SK"))) == 0) |
            (F.col("dt.TERM_DT_SK") != F.lit("2199-12-31")),
            F.lit(CurrDate)
        ).otherwise(F.col("dt.EFF_DT_SK")).alias("EFF_DT_SK"),
        F.col("dt.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK")
    )
)

df_MR_LN_SVC_CAT_xfrm_out = df_xfrmdt_pkeyout.select(
    F.col("PRI_NAT_KEY_STRING"),
    F.col("FIRST_RECYC_TS"),
    F.col("MR_LN_SVC_CAT_SK"),
    F.col("MR_LN_ID"),
    F.rpad(F.col("EFF_DT_SK"), 10, " ").alias("EFF_DT_SK"),
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
    F.rpad(F.col("TERM_DT_SK"), 10, " ").alias("TERM_DT_SK")
)

write_files(
    df_MR_LN_SVC_CAT_xfrm_out,
    f"{adls_path}/ds/MR_LN_SVC_CAT.{SrcSysCd}.xfrm.{RunID}.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

df_B_MR_LN_SVC_CAT_out = df_xfrmdt_snapshot.select(
    F.col("MR_LN_ID"),
    F.rpad(F.col("EFF_DT_SK"), 10, " ").alias("EFF_DT_SK"),
    F.col("SRC_SYS_CD_SK")
)

write_files(
    df_B_MR_LN_SVC_CAT_out,
    f"{adls_path}/load/B_MR_LN_SVC_CAT.{SrcSysCd}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)