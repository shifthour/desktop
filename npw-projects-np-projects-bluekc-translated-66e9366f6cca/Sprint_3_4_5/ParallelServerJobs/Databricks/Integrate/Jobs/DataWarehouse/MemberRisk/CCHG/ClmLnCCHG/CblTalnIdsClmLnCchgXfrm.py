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
# MAGIC Krishnakanth             2017-12-06           30001                            Parameterize the variable Vendor                                                          IntegrateDev2          Kalyan Neelam            2017-12-06     
# MAGIC   Manivannan

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
from pyspark.sql.types import StructType, StructField, StringType
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
CurrDate = get_widget_value('CurrDate','')
YearMo = get_widget_value('YearMo','')
Vendor = get_widget_value('Vendor','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
df_db2_K_CLM_LN = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""SELECT 
CD.TRGT_CD as SRC_SYS_CD,
CLM_LN.CLM_ID,
CLM_LN.CLM_LN_SEQ_NO,
CLM_LN.CLM_LN_SK,
CLM_LN.SRC_SYS_CD_SK as CLM_SRC_SYS_CD_SK

FROM {IDSOwner}.K_CLM_LN CLM_LN,
     {IDSOwner}.CD_MPPNG CD

WHERE
CLM_LN.SRC_SYS_CD_SK = CD.CD_MPPNG_SK"""
    )
    .load()
)

schema_ds_CLM_LN_CCHG_Extr = StructType([
    StructField("MBR_ID", StringType(), True),
    StructField("CLM_ID", StringType(), True),
    StructField("PROV_TYP_CD", StringType(), True),
    StructField("DIAG_CD", StringType(), True),
    StructField("SVC_STRT_DT", StringType(), True),
    StructField("PROC_CD", StringType(), True),
    StructField("RVNU_CD", StringType(), True),
    StructField("POS_CD", StringType(), True),
    StructField("NDC", StringType(), True),
    StructField("ALW_AMT", StringType(), True),
    StructField("PD_AMT", StringType(), True),
    StructField("CLM_LN_SEQ_NO", StringType(), True),
    StructField("UNIT_CT", StringType(), True),
    StructField("DAYS_SUPPLIED_CT", StringType(), True),
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("CLNT_ID", StringType(), True),
    StructField("CCHG_ID", StringType(), True)
])

df_ds_CLM_LN_CCHG_Extr = (
    spark.read.format("parquet")
    .schema(schema_ds_CLM_LN_CCHG_Extr)
    .load(f"{adls_path}/ds/CLM_LN_CCHG.{SrcSysCd}.extr.{RunID}.parquet")
)

df_StripField = (
    df_ds_CLM_LN_CCHG_Extr
    .withColumn("MBR_ID", Trim(F.col("MBR_ID")))
    .withColumn("CLM_ID", Trim(STRIP.FIELD.EE(F.col("CLM_ID")), ' ', 'A'))
    .withColumn("PROV_TYP_CD", Trim(F.col("PROV_TYP_CD")))
    .withColumn("DIAG_CD", Trim(F.col("DIAG_CD")))
    .withColumn("SVC_STRT_DT", Trim(F.col("SVC_STRT_DT")))
    .withColumn("PROC_CD", Trim(F.col("PROC_CD")))
    .withColumn("RVNU_CD", Trim(F.col("RVNU_CD")))
    .withColumn("POS_CD", Trim(F.col("POS_CD")))
    .withColumn("NDC", Trim(F.col("NDC")))
    .withColumn("ALW_AMT", Trim(F.col("ALW_AMT")))
    .withColumn("PD_AMT", Trim(F.col("PD_AMT")))
    .withColumn("CLM_LN_SEQ_NO", Trim(F.col("CLM_LN_SEQ_NO")))
    .withColumn("UNIT_CT", Trim(F.col("UNIT_CT")))
    .withColumn("DAYS_SUPPLIED_CT", Trim(F.col("DAYS_SUPPLIED_CT")))
    .withColumn("SRC_SYS_CD", Trim(F.col("SRC_SYS_CD"), ' ', 'A'))
    .withColumn("CLNT_ID", Trim(F.col("CLNT_ID")))
    .withColumn("CCHG_ID", Trim(F.col("CCHG_ID")))
)

df_str = df_StripField.alias("rules")
df_db2 = df_db2_K_CLM_LN.alias("ClmLnLKup")
df_join_81 = df_str.join(
    df_db2,
    [
        df_str["CLM_ID"] == df_db2["CLM_ID"],
        df_str["CLM_LN_SEQ_NO"] == df_db2["CLM_LN_SEQ_NO"],
        df_str["SRC_SYS_CD"] == df_db2["SRC_SYS_CD"],
    ],
    how="left"
)

df_Join_81 = df_join_81.select(
    df_str["MBR_ID"].alias("MBR_ID"),
    df_str["CLM_ID"].alias("CLM_ID"),
    df_str["PROV_TYP_CD"].alias("PROV_TYP_CD"),
    df_str["DIAG_CD"].alias("DIAG_CD"),
    df_str["SVC_STRT_DT"].alias("SVC_STRT_DT"),
    df_str["PROC_CD"].alias("PROC_CD"),
    df_str["RVNU_CD"].alias("RVNU_CD"),
    df_str["POS_CD"].alias("POS_CD"),
    df_str["NDC"].alias("NDC"),
    df_str["ALW_AMT"].alias("ALW_AMT"),
    df_str["PD_AMT"].alias("PD_AMT"),
    df_str["CLM_LN_SEQ_NO"].alias("CLM_LN_SEQ_NO"),
    df_str["UNIT_CT"].alias("UNIT_CT"),
    df_str["DAYS_SUPPLIED_CT"].alias("DAYS_SUPPLIED_CT"),
    df_str["SRC_SYS_CD"].alias("SRC_SYS_CD"),
    df_str["CLNT_ID"].alias("CLNT_ID"),
    df_str["CCHG_ID"].alias("CCHG_ID"),
    df_db2["CLM_SRC_SYS_CD_SK"].alias("CLM_SRC_SYS_CD_SK")
)

df_BusinessRules = df_Join_81.withColumn(
    "ErrorInd",
    F.when(F.col("CLM_SRC_SYS_CD_SK").isNull(), F.lit("Y")).otherwise(F.lit("N"))
)

df_PkeyOut = df_BusinessRules.filter(F.col("ErrorInd") == F.lit("N")).select(
    F.concat(
        F.col("CLM_ID"),
        F.lit(";"),
        F.col("CLM_LN_SEQ_NO"),
        F.lit(";"),
        F.col("SRC_SYS_CD"),
        F.lit(";"),
        F.lit(YearMo),
        F.lit(";"),
        F.lit(SrcSysCd)
    ).alias("PRI_NAT_KEY_STRING"),
    F.lit(RunIDTimeStamp).alias("FIRST_RECYC_TS"),
    F.lit(0).alias("CLM_LN_CCHG_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("SRC_SYS_CD").alias("CLM_SRC_SYS_CD"),
    F.lit(YearMo).alias("PRCS_YR_MO_SK"),
    F.lit(SrcSysCd).alias("SRC_SYS_CD"),
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CCHG_ID").alias("CCHG_MULT_CAT_GRP")
)

df_snapshot = df_BusinessRules.filter(F.col("ErrorInd") == F.lit("N")).select(
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("CLM_SRC_SYS_CD_SK").alias("CLM_SRC_SYS_CD_SK"),
    F.lit(YearMo).alias("PRCS_YR_MO_SK"),
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK")
)

df_error = df_BusinessRules.filter(F.col("ErrorInd") == F.lit("Y")).select(
    F.col("MBR_ID").alias("MBR_ID"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("PROV_TYP_CD").alias("PROV_TYP_CD"),
    F.col("DIAG_CD").alias("DIAG_CD"),
    F.col("SVC_STRT_DT").alias("SVC_STRT_DT"),
    F.col("PROC_CD").alias("PROC_CD"),
    F.col("RVNU_CD").alias("RVNU_CD"),
    F.col("POS_CD").alias("POS_CD"),
    F.col("NDC").alias("NDC"),
    F.col("ALW_AMT").alias("ALW_AMT"),
    F.col("PD_AMT").alias("PD_AMT"),
    F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("UNIT_CT").alias("UNIT_CT"),
    F.col("DAYS_SUPPLIED_CT").alias("DAYS_SUPPLIED_CT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("CLNT_ID").alias("CLNT_ID"),
    F.col("CCHG_ID").alias("CCHG_ID")
)

df_B_CLM_LN_CCHG = df_snapshot.select(
    F.col("CLM_ID"),
    F.col("CLM_LN_SEQ_NO"),
    F.col("CLM_SRC_SYS_CD_SK"),
    F.col("PRCS_YR_MO_SK"),
    F.col("SRC_SYS_CD_SK")
)

df_B_CLM_LN_CCHG = (
    df_B_CLM_LN_CCHG
    .withColumn("CLM_ID", F.rpad(F.col("CLM_ID"), 30, " "))
    .withColumn("CLM_LN_SEQ_NO", F.rpad(F.col("CLM_LN_SEQ_NO"), 30, " "))
    .withColumn("PRCS_YR_MO_SK", F.rpad(F.col("PRCS_YR_MO_SK"), 6, " "))
)

write_files(
    df_B_CLM_LN_CCHG,
    f"{adls_path}/load/B_CLM_LN_CCHG.{SrcSysCd}.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)

df_ds_CLM_LN_CCHG_Xfrm = df_PkeyOut.select(
    F.col("PRI_NAT_KEY_STRING"),
    F.col("FIRST_RECYC_TS"),
    F.col("CLM_LN_CCHG_SK"),
    F.rpad(F.col("CLM_ID"), 30, " ").alias("CLM_ID"),
    F.rpad(F.col("CLM_LN_SEQ_NO"), 30, " ").alias("CLM_LN_SEQ_NO"),
    F.rpad(F.col("CLM_SRC_SYS_CD"), 20, " ").alias("CLM_SRC_SYS_CD"),
    F.rpad(F.col("PRCS_YR_MO_SK"), 6, " ").alias("PRCS_YR_MO_SK"),
    F.rpad(F.col("SRC_SYS_CD"), 20, " ").alias("SRC_SYS_CD"),
    F.col("SRC_SYS_CD_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.rpad(F.col("CCHG_MULT_CAT_GRP"), 3, " ").alias("CCHG_MULT_CAT_GRP")
)

write_files(
    df_ds_CLM_LN_CCHG_Xfrm,
    f"{adls_path}/ds/CLM_LN_CCHG.{SrcSysCd}.xfrm.{RunID}.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

df_error_out = (
    df_error
    .withColumn("MBR_ID", F.rpad(F.col("MBR_ID"), 30, " "))
    .withColumn("CLM_ID", F.rpad(F.col("CLM_ID"), 30, " "))
    .withColumn("PROV_TYP_CD", F.rpad(F.col("PROV_TYP_CD"), 10, " "))
    .withColumn("DIAG_CD", F.rpad(F.col("DIAG_CD"), 6, " "))
    .withColumn("SVC_STRT_DT", F.rpad(F.col("SVC_STRT_DT"), 10, " "))
    .withColumn("PROC_CD", F.rpad(F.col("PROC_CD"), 6, " "))
    .withColumn("RVNU_CD", F.rpad(F.col("RVNU_CD"), 6, " "))
    .withColumn("POS_CD", F.rpad(F.col("POS_CD"), 2, " "))
    .withColumn("NDC", F.rpad(F.col("NDC"), 11, " "))
    .withColumn("ALW_AMT", F.rpad(F.col("ALW_AMT"), 10, " "))
    .withColumn("PD_AMT", F.rpad(F.col("PD_AMT"), 10, " "))
    .withColumn("CLM_LN_SEQ_NO", F.rpad(F.col("CLM_LN_SEQ_NO"), 30, " "))
    .withColumn("UNIT_CT", F.rpad(F.col("UNIT_CT"), 4, " "))
    .withColumn("DAYS_SUPPLIED_CT", F.rpad(F.col("DAYS_SUPPLIED_CT"), 6, " "))
    .withColumn("SRC_SYS_CD", F.rpad(F.col("SRC_SYS_CD"), 20, " "))
    .withColumn("CLNT_ID", F.rpad(F.col("CLNT_ID"), 3, " "))
    .withColumn("CCHG_ID", F.rpad(F.col("CCHG_ID"), 3, " "))
)

write_files(
    df_error_out,
    f"{adls_path_publish}/external/{Vendor}_CLM_LN_CCHG_ERRORS.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)