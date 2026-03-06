# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC CALLED BY: 
# MAGIC 
# MAGIC PROCESSING:  Strip field function and Tranforamtion rules are applied on the data Extracted from inbound file
# MAGIC                                                                 
# MAGIC      
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                   DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                       DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC Kalyan Neelam          2015-07-27           5460                              Initial Programming                                                                                IntegrateDev2            Bhoomi Dasari              8/30/2015
# MAGIC 
# MAGIC Krishnakanth             2017-12-06           30001                            Parameterize the variable Vendor                                                          IntegrateDev2         Kalyan Neelam            2017-12-06
# MAGIC   Manivannan

# MAGIC INDV_BE_CCHG Transform job
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
from pyspark.sql.functions import col, lit, when, length, concat, rpad
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------

IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
SrcSysCd = get_widget_value('SrcSysCd','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
RunID = get_widget_value('RunID','')
RunIDTimeStamp = get_widget_value('RunIDTimeStamp','')
CurrDate = get_widget_value('CurrDate','')
CurrYearMo = get_widget_value('CurrYearMo','')
Vendor = get_widget_value('Vendor','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query_DB2_MBR = f"SELECT distinct INDV_BE_KEY FROM {IDSOwner}.MBR"
df_DB2_MBR = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_DB2_MBR)
    .load()
)

extract_query_DB2_MPI_INDV_BE_XWALK = f"SELECT distinct PREV_INDV_BE_KEY FROM {IDSOwner}.MPI_INDV_BE_CRSWALK"
df_DB2_MPI_INDV_BE_XWALK = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_DB2_MPI_INDV_BE_XWALK)
    .load()
)

df_ds_INDV_BE_CCHG_Extr = spark.read.parquet(f"{adls_path}/ds/INDV_BE_CCHG.{SrcSysCd}.extr.{RunID}.parquet")

df_StripField = df_ds_INDV_BE_CCHG_Extr.select(
    trim(col("MBR_ID")).alias("MBR_ID"),
    trim(col("CCHG_ID")).alias("CCHG_ID"),
    trim(col("CCHG_STRT_DT")).alias("CCHG_STRT_DT"),
    trim(col("CCHG_END_DT")).alias("CCHG_END_DT"),
    trim(col("CCHG_CT")).alias("CCHG_CT"),
    trim(col("CCHG_SEC_CD")).alias("CCHG_SEC_CD"),
    trim(col("CCHG_TRTY_CD")).alias("CCHG_TRTY_CD"),
    trim(col("CCHG_4TH_CD")).alias("CCHG_4TH_CD"),
    trim(col("CCHG_5TH_CD")).alias("CCHG_5TH_CD"),
    col("MBR_ID").alias("MBR_ID_for_error_file")
)

df_MbrLookup = (
    df_StripField.alias("rules")
    .join(
        df_DB2_MBR.alias("Mbr_lkup"),
        col("rules.MBR_ID") == col("Mbr_lkup.INDV_BE_KEY"),
        "left"
    )
    .join(
        df_DB2_MPI_INDV_BE_XWALK.alias("MpiIndvBeXwalk_lkup"),
        col("rules.MBR_ID") == col("MpiIndvBeXwalk_lkup.PREV_INDV_BE_KEY"),
        "left"
    )
    .select(
        col("rules.MBR_ID").alias("MBR_ID"),
        col("rules.CCHG_ID").alias("CCHG_ID"),
        col("rules.CCHG_STRT_DT").alias("CCHG_STRT_DT"),
        col("rules.CCHG_END_DT").alias("CCHG_END_DT"),
        col("rules.CCHG_CT").alias("CCHG_CT"),
        col("rules.CCHG_SEC_CD").alias("CCHG_SEC_CD"),
        col("rules.CCHG_TRTY_CD").alias("CCHG_TRTY_CD"),
        col("rules.CCHG_4TH_CD").alias("CCHG_4TH_CD"),
        col("rules.CCHG_5TH_CD").alias("CCHG_5TH_CD"),
        col("Mbr_lkup.INDV_BE_KEY").alias("INDV_BE_KEY"),
        col("MpiIndvBeXwalk_lkup.PREV_INDV_BE_KEY").alias("PREV_INDV_BE_KEY"),
        col("rules.MBR_ID_for_error_file").alias("MBR_ID_for_error_file")
    )
)

df_BusinessRules = df_MbrLookup.withColumn(
    "ErrorInd",
    when(
        (
            (col("INDV_BE_KEY").isNull() & col("PREV_INDV_BE_KEY").isNull())
        )
        | col("CCHG_STRT_DT").isNull(),
        "Y"
    ).otherwise("N")
)

df_BusinessRules_PkeyOut = (
    df_BusinessRules
    .filter(col("ErrorInd") == "N")
    .select(
        concat(col("MBR_ID"), lit(";"), col("CCHG_STRT_DT"), lit(";"), lit(CurrYearMo), lit(";"), lit(SrcSysCd)).alias("PRI_NAT_KEY_STRING"),
        lit(RunIDTimeStamp).alias("FIRST_RECYC_TS"),
        lit("0").alias("INDV_BE_CCHG_SK"),
        col("MBR_ID").alias("INDV_BE_KEY"),
        col("CCHG_STRT_DT").alias("CCHG_STRT_YR_MO_SK"),
        lit(CurrYearMo).alias("PRCS_YR_MO_SK"),
        lit(SrcSysCd).alias("SRC_SYS_CD"),
        lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
        lit("0").alias("CRT_RUN_CYC_EXCTN_SK"),
        lit("0").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        when(col("CCHG_ID").isNull() | (length(trim(col("CCHG_ID"))) == 0), lit("NA")).otherwise(col("CCHG_ID")).alias("CCHG_MULT_CAT_GRP_PRI"),
        when(col("CCHG_SEC_CD").isNull() | (length(trim(col("CCHG_SEC_CD"))) == 0), lit("NA")).otherwise(col("CCHG_SEC_CD")).alias("CCHG_MULT_CAT_GRP_SEC"),
        when(col("CCHG_TRTY_CD").isNull() | (length(trim(col("CCHG_TRTY_CD"))) == 0), lit("NA")).otherwise(col("CCHG_TRTY_CD")).alias("CCHG_MULT_CAT_GRP_TRTY"),
        when(col("CCHG_4TH_CD").isNull() | (length(trim(col("CCHG_4TH_CD"))) == 0), lit("NA")).otherwise(col("CCHG_4TH_CD")).alias("CCHG_MULT_CAT_GRP_4TH"),
        when(col("CCHG_5TH_CD").isNull() | (length(trim(col("CCHG_5TH_CD"))) == 0), lit("NA")).otherwise(col("CCHG_5TH_CD")).alias("CCHG_MULT_CAT_GRP_5TH"),
        col("CCHG_END_DT").alias("CCHG_END_YR_MO_SK"),
        when(col("CCHG_CT") == "6", lit("5")).otherwise(col("CCHG_CT")).alias("CCHG_CT")
    )
)

df_snapshot = (
    df_BusinessRules
    .filter(col("ErrorInd") == "N")
    .select(
        col("MBR_ID").alias("INDV_BE_KEY"),
        col("CCHG_STRT_DT").alias("CCHG_STRT_YR_MO_SK"),
        lit(CurrYearMo).alias("PRCS_YR_MO_SK"),
        lit(SrcSysCdSk).alias("SRC_SYS_CD_SK")
    )
)

df_Error = (
    df_BusinessRules
    .filter(col("ErrorInd") == "Y")
    .select(
        col("MBR_ID_for_error_file").alias("MBR_ID"),
        col("CCHG_ID"),
        col("CCHG_STRT_DT"),
        col("CCHG_END_DT"),
        col("CCHG_CT"),
        col("CCHG_SEC_CD"),
        col("CCHG_TRTY_CD"),
        col("CCHG_4TH_CD"),
        col("CCHG_5TH_CD")
    )
)

df_pkeyOut_final = df_BusinessRules_PkeyOut.select(
    col("PRI_NAT_KEY_STRING"),
    col("FIRST_RECYC_TS"),
    col("INDV_BE_CCHG_SK"),
    col("INDV_BE_KEY"),
    rpad(col("CCHG_STRT_YR_MO_SK"), 6, " ").alias("CCHG_STRT_YR_MO_SK"),
    rpad(col("PRCS_YR_MO_SK"), 6, " ").alias("PRCS_YR_MO_SK"),
    col("SRC_SYS_CD"),
    col("SRC_SYS_CD_SK"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("CCHG_MULT_CAT_GRP_PRI"),
    col("CCHG_MULT_CAT_GRP_SEC"),
    col("CCHG_MULT_CAT_GRP_TRTY"),
    col("CCHG_MULT_CAT_GRP_4TH"),
    col("CCHG_MULT_CAT_GRP_5TH"),
    rpad(col("CCHG_END_YR_MO_SK"), 6, " ").alias("CCHG_END_YR_MO_SK"),
    col("CCHG_CT")
)
write_files(
    df_pkeyOut_final,
    f"{adls_path}/ds/INDV_BE_CCHG.{SrcSysCd}.xfrm.{RunID}.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

df_snapshot_final = df_snapshot.select(
    col("INDV_BE_KEY"),
    rpad(col("CCHG_STRT_YR_MO_SK"), 6, " ").alias("CCHG_STRT_YR_MO_SK"),
    rpad(col("PRCS_YR_MO_SK"), 6, " ").alias("PRCS_YR_MO_SK"),
    col("SRC_SYS_CD_SK")
)
write_files(
    df_snapshot_final,
    f"{adls_path}/load/B_INDV_BE_CCHG.{SrcSysCd}.dat",
    ",",
    "append",
    False,
    False,
    "^",
    None
)

df_Error_final = df_Error.select(
    col("MBR_ID"),
    rpad(col("CCHG_ID"), 5, " ").alias("CCHG_ID"),
    rpad(col("CCHG_STRT_DT"), 8, " ").alias("CCHG_STRT_DT"),
    rpad(col("CCHG_END_DT"), 8, " ").alias("CCHG_END_DT"),
    rpad(col("CCHG_CT"), 3, " ").alias("CCHG_CT"),
    rpad(col("CCHG_SEC_CD"), 5, " ").alias("CCHG_SEC_CD"),
    rpad(col("CCHG_TRTY_CD"), 5, " ").alias("CCHG_TRTY_CD"),
    rpad(col("CCHG_4TH_CD"), 5, " ").alias("CCHG_4TH_CD"),
    rpad(col("CCHG_5TH_CD"), 5, " ").alias("CCHG_5TH_CD")
)
write_files(
    df_Error_final,
    f"{adls_path_publish}/external/{Vendor}_INDV_BE_CCHG_ERRORS.dat",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)