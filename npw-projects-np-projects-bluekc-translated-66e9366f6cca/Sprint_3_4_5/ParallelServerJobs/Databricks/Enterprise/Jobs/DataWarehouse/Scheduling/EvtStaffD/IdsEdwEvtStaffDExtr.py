# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2009 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY: EdwProductExtrSeq
# MAGIC 
# MAGIC PROCESSING: Extracts all IDS records and loads in to EDW EVT_STAFF_D table
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date               Project/Altiris #               Change Description                                    Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------   -----------------------------------    ---------------------------------------------------------              ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Bhoomi Dasari                 08/14/2009         4113                          Originally Programmed                                    devlEDWnew                Steph Goddard             08/19/2009
# MAGIC 
# MAGIC Archana Palivela             11/10/2013        5114                           Originally Programmed (In Parallel)                  EnterpriseWhseDevl     Jag Yelavarthi               2014-01-16

# MAGIC Job name: IdsEdwEvtStaffDExtr
# MAGIC 
# MAGIC 
# MAGIC Job Extract data from IDS table and apply Fkey and Code denormalization, create a load ready file.
# MAGIC Read data from source table EVT_STAFF
# MAGIC Code SK lookups for Denormalization
# MAGIC 
# MAGIC Lookup Keys:
# MAGIC EVT_STAFF_TYP_SK
# MAGIC Write EVT_STAFF_D Data into a Sequential file for Load Ready Job.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWRunCycle = get_widget_value('EDWRunCycle','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
IdsRunCycle = get_widget_value('IdsRunCycle','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

df_db2_EVT_STAFF_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""SELECT
EVT_STAFF.EVT_STAFF_SK,
EVT_STAFF.EVT_STAFF_ID,
COALESCE(CD.TRGT_CD,'UNK') SRC_SYS_CD,
EVT_STAFF.LAST_UPDT_RUN_CYC_EXCTN_SK,
EVT_STAFF.EVT_STAFF_TYP_SK,
EVT_STAFF.EVT_STAFF_ACTV_IN,
EVT_STAFF.EFF_DT_SK,
EVT_STAFF.TERM_DT_SK,
EVT_STAFF.EVT_STAFF_EMAIL_ADDR,
EVT_STAFF.EVT_STAFF_NM,
EVT_STAFF.EVT_STAFF_PHN_NO,
EVT_STAFF.LAST_UPDT_DTM,
EVT_STAFF.LAST_UPDT_USER_ID
FROM {IDSOwner}.EVT_STAFF EVT_STAFF
LEFT JOIN {IDSOwner}.CD_MPPNG CD
ON EVT_STAFF.SRC_SYS_CD_SK = CD.CD_MPPNG_SK
WHERE EVT_STAFF.LAST_UPDT_RUN_CYC_EXCTN_SK >= {IdsRunCycle}"""
    )
    .load()
)

df_db2_EVT_STAFF_TYP_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""SELECT
EVT_STAFF_TYP.EVT_STAFF_TYP_SK,
EVT_STAFF_TYP.EVT_STAFF_TYP_ID,
EVT_STAFF_TYP.EVT_STAFF_TYP_DESC
FROM {IDSOwner}.EVT_STAFF_TYP EVT_STAFF_TYP"""
    )
    .load()
)

df_lkp_Codes = (
    df_db2_EVT_STAFF_Extr.alias("Ink_IdsEdwEvtStaffDExtr_InABC")
    .join(
        df_db2_EVT_STAFF_TYP_Extr.alias("Ref_EvtStaffTyp"),
        F.col("Ink_IdsEdwEvtStaffDExtr_InABC.EVT_STAFF_TYP_SK")
        == F.col("Ref_EvtStaffTyp.EVT_STAFF_TYP_SK"),
        how="left",
    )
    .select(
        F.col("Ink_IdsEdwEvtStaffDExtr_InABC.EVT_STAFF_SK").alias("EVT_STAFF_SK"),
        F.col("Ink_IdsEdwEvtStaffDExtr_InABC.EVT_STAFF_ID").alias("EVT_STAFF_ID"),
        F.col("Ink_IdsEdwEvtStaffDExtr_InABC.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("Ink_IdsEdwEvtStaffDExtr_InABC.EVT_STAFF_ACTV_IN").alias("EVT_STAFF_ACTV_IN"),
        F.col("Ink_IdsEdwEvtStaffDExtr_InABC.EVT_STAFF_TYP_SK").alias("EVT_STAFF_TYP_SK"),
        F.col("Ink_IdsEdwEvtStaffDExtr_InABC.EFF_DT_SK").alias("EVT_STAFF_EFF_DT_SK"),
        F.col("Ink_IdsEdwEvtStaffDExtr_InABC.LAST_UPDT_DTM").alias("EVT_STAFF_LAST_UPDT_DTM"),
        F.col("Ink_IdsEdwEvtStaffDExtr_InABC.TERM_DT_SK").alias("EVT_STAFF_TERM_DT_SK"),
        F.col("Ink_IdsEdwEvtStaffDExtr_InABC.EVT_STAFF_EMAIL_ADDR").alias("EVT_STAFF_EMAIL_ADDR"),
        F.col("Ink_IdsEdwEvtStaffDExtr_InABC.LAST_UPDT_USER_ID").alias("EVT_STAFF_LAST_UPDT_USER_ID"),
        F.col("Ink_IdsEdwEvtStaffDExtr_InABC.EVT_STAFF_NM").alias("EVT_STAFF_NM"),
        F.col("Ink_IdsEdwEvtStaffDExtr_InABC.EVT_STAFF_PHN_NO").alias("EVT_STAFF_PHN_NO"),
        F.col("Ref_EvtStaffTyp.EVT_STAFF_TYP_ID").alias("EVT_STAFF_TYP_ID"),
        F.col("Ref_EvtStaffTyp.EVT_STAFF_TYP_DESC").alias("EVT_STAFF_TYP_DESC"),
        F.col("Ink_IdsEdwEvtStaffDExtr_InABC.LAST_UPDT_RUN_CYC_EXCTN_SK").alias(
            "IDS_LAST_UPDT_RUN_CYC_EXCTN_SK"
        ),
    )
)

df_Lnk_Main_OutABC = (
    df_lkp_Codes.filter((F.col("EVT_STAFF_SK") != 0) & (F.col("EVT_STAFF_SK") != 1))
    .withColumn(
        "SRC_SYS_CD",
        F.when(F.trim(F.col("SRC_SYS_CD")) == "", F.lit("UNK")).otherwise(F.col("SRC_SYS_CD")),
    )
    .withColumn(
        "EVT_STAFF_TYP_ID",
        F.when(F.trim(F.col("EVT_STAFF_TYP_ID")) == "", F.lit("UNK")).otherwise(
            F.col("EVT_STAFF_TYP_ID")
        ),
    )
    .withColumn(
        "EVT_STAFF_TYP_DESC",
        F.when(F.trim(F.col("EVT_STAFF_TYP_DESC")) == "", F.lit("UNK")).otherwise(
            F.col("EVT_STAFF_TYP_DESC")
        ),
    )
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", F.lit(EDWRunCycleDate))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.lit(EDWRunCycleDate))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(EDWRunCycle))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(EDWRunCycle))
    .select(
        F.col("EVT_STAFF_SK"),
        F.col("EVT_STAFF_ID"),
        F.col("SRC_SYS_CD"),
        F.col("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        F.col("EVT_STAFF_ACTV_IN"),
        F.col("EVT_STAFF_TYP_SK"),
        F.col("EVT_STAFF_EFF_DT_SK"),
        F.col("EVT_STAFF_LAST_UPDT_DTM"),
        F.col("EVT_STAFF_TERM_DT_SK"),
        F.col("EVT_STAFF_EMAIL_ADDR"),
        F.col("EVT_STAFF_LAST_UPDT_USER_ID"),
        F.col("EVT_STAFF_NM"),
        F.col("EVT_STAFF_PHN_NO"),
        F.col("EVT_STAFF_TYP_ID"),
        F.col("EVT_STAFF_TYP_DESC"),
        F.col("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK"),
    )
)

df_tmp_NA = df_lkp_Codes.limit(1)
df_Lnk_NA_Out = (
    df_tmp_NA.withColumn("EVT_STAFF_SK", F.lit(1))
    .withColumn("EVT_STAFF_ID", F.lit("NA"))
    .withColumn("SRC_SYS_CD", F.lit("NA"))
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", F.lit("1753-01-01"))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.lit("1753-01-01"))
    .withColumn("EVT_STAFF_ACTV_IN", F.lit("N"))
    .withColumn("EVT_STAFF_TYP_SK", F.lit(1))
    .withColumn("EVT_STAFF_EFF_DT_SK", F.lit("1753-01-01"))
    .withColumn("EVT_STAFF_LAST_UPDT_DTM", F.lit("1753-01-01 00:00:00.000"))
    .withColumn("EVT_STAFF_TERM_DT_SK", F.lit("1753-01-01"))
    .withColumn("EVT_STAFF_EMAIL_ADDR", F.lit(""))
    .withColumn("EVT_STAFF_LAST_UPDT_USER_ID", F.lit("NA"))
    .withColumn("EVT_STAFF_NM", F.lit("NA"))
    .withColumn("EVT_STAFF_PHN_NO", F.lit(""))
    .withColumn("EVT_STAFF_TYP_ID", F.lit("NA"))
    .withColumn("EVT_STAFF_TYP_DESC", F.lit(""))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(100))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(100))
    .withColumn("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(100))
    .select(
        F.col("EVT_STAFF_SK"),
        F.col("EVT_STAFF_ID"),
        F.col("SRC_SYS_CD"),
        F.col("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        F.col("EVT_STAFF_ACTV_IN"),
        F.col("EVT_STAFF_TYP_SK"),
        F.col("EVT_STAFF_EFF_DT_SK"),
        F.col("EVT_STAFF_LAST_UPDT_DTM"),
        F.col("EVT_STAFF_TERM_DT_SK"),
        F.col("EVT_STAFF_EMAIL_ADDR"),
        F.col("EVT_STAFF_LAST_UPDT_USER_ID"),
        F.col("EVT_STAFF_NM"),
        F.col("EVT_STAFF_PHN_NO"),
        F.col("EVT_STAFF_TYP_ID"),
        F.col("EVT_STAFF_TYP_DESC"),
        F.col("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK"),
    )
)

df_tmp_UNK = df_lkp_Codes.limit(1)
df_Lnk_UNK_Out = (
    df_tmp_UNK.withColumn("EVT_STAFF_SK", F.lit(0))
    .withColumn("EVT_STAFF_ID", F.lit("UNK"))
    .withColumn("SRC_SYS_CD", F.lit("UNK"))
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", F.lit("1753-01-01"))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.lit("1753-01-01"))
    .withColumn("EVT_STAFF_ACTV_IN", F.lit("N"))
    .withColumn("EVT_STAFF_TYP_SK", F.lit(0))
    .withColumn("EVT_STAFF_EFF_DT_SK", F.lit("1753-01-01"))
    .withColumn("EVT_STAFF_LAST_UPDT_DTM", F.lit("1753-01-01 00:00:00.000"))
    .withColumn("EVT_STAFF_TERM_DT_SK", F.lit("1753-01-01"))
    .withColumn("EVT_STAFF_EMAIL_ADDR", F.lit(""))
    .withColumn("EVT_STAFF_LAST_UPDT_USER_ID", F.lit("UNK"))
    .withColumn("EVT_STAFF_NM", F.lit("UNK"))
    .withColumn("EVT_STAFF_PHN_NO", F.lit(""))
    .withColumn("EVT_STAFF_TYP_ID", F.lit("UNK"))
    .withColumn("EVT_STAFF_TYP_DESC", F.lit(""))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(100))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(100))
    .withColumn("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(100))
    .select(
        F.col("EVT_STAFF_SK"),
        F.col("EVT_STAFF_ID"),
        F.col("SRC_SYS_CD"),
        F.col("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        F.col("EVT_STAFF_ACTV_IN"),
        F.col("EVT_STAFF_TYP_SK"),
        F.col("EVT_STAFF_EFF_DT_SK"),
        F.col("EVT_STAFF_LAST_UPDT_DTM"),
        F.col("EVT_STAFF_TERM_DT_SK"),
        F.col("EVT_STAFF_EMAIL_ADDR"),
        F.col("EVT_STAFF_LAST_UPDT_USER_ID"),
        F.col("EVT_STAFF_NM"),
        F.col("EVT_STAFF_PHN_NO"),
        F.col("EVT_STAFF_TYP_ID"),
        F.col("EVT_STAFF_TYP_DESC"),
        F.col("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK"),
    )
)

common_cols = [
    "EVT_STAFF_SK",
    "EVT_STAFF_ID",
    "SRC_SYS_CD",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "EVT_STAFF_ACTV_IN",
    "EVT_STAFF_TYP_SK",
    "EVT_STAFF_EFF_DT_SK",
    "EVT_STAFF_LAST_UPDT_DTM",
    "EVT_STAFF_TERM_DT_SK",
    "EVT_STAFF_EMAIL_ADDR",
    "EVT_STAFF_LAST_UPDT_USER_ID",
    "EVT_STAFF_NM",
    "EVT_STAFF_PHN_NO",
    "EVT_STAFF_TYP_ID",
    "EVT_STAFF_TYP_DESC",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "IDS_LAST_UPDT_RUN_CYC_EXCTN_SK",
]

df_Funnel_37 = (
    df_Lnk_Main_OutABC.select(common_cols)
    .unionByName(df_Lnk_UNK_Out.select(common_cols), allowMissingColumns=True)
    .unionByName(df_Lnk_NA_Out.select(common_cols), allowMissingColumns=True)
)

df_final = (
    df_Funnel_37
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
    .withColumn("EVT_STAFF_ACTV_IN", F.rpad(F.col("EVT_STAFF_ACTV_IN"), 1, " "))
    .withColumn("EVT_STAFF_EFF_DT_SK", F.rpad(F.col("EVT_STAFF_EFF_DT_SK"), 10, " "))
    .withColumn("EVT_STAFF_TERM_DT_SK", F.rpad(F.col("EVT_STAFF_TERM_DT_SK"), 10, " "))
    .select(common_cols)
)

write_files(
    df_final,
    f"{adls_path}/load/EVT_STAFF_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)