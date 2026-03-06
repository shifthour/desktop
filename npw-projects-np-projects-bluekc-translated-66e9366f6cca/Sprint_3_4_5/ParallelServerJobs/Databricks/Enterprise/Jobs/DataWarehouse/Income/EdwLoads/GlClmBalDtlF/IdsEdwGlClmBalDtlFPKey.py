# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                  DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                       ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -------------------------------------------------------------------------------               ------------------------------    ------------------------------       --------------------
# MAGIC  Bhupinder Kaur         10/29/2013           5114                          generate primary key of  GL_CLM_BAL_DTL_F                  EnterpriseWrhsDevl       Jag Yelavarthi             2013-12-09

# MAGIC Remove Duplicates stage takes care of getting rid of duplicate rows with the same Natural key get into the PKEYing step.
# MAGIC A concious effort need to be made to evaluate which one of the duplicate set row to drop is a case by case basis decision.
# MAGIC Left Outer Join Here
# MAGIC Build primary key for GL_CLM_BAL_DTL_F
# MAGIC K table will be read to see if there is an SK already available for the Natural Keys. 
# MAGIC 
# MAGIC Job Name: IdsEdwGlClmBalDtlFPkey
# MAGIC Insert new SKEY rows generated as part of this run. This is a database insert operation.
# MAGIC New PKEYs are generated in this transformer. A database sequence will be used to create next PKEY value.
# MAGIC 
# MAGIC IMPORTANT: Make sure to change the Database SEQUENCE Name to the corresponding table name.
# MAGIC if there is no FKEY needed, just land it to Seq File for the load job follows.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


EDWRunCycleDate = get_widget_value("EDWRunCycleDate","")
EDWRunCycle = get_widget_value("EDWRunCycle","")
EDWOwner = get_widget_value("EDWOwner","")
edw_secret_name = get_widget_value("edw_secret_name","")

df_ds_GL_CLM_BAL_DTL_F_Extr = (
    spark.read.parquet(f"{adls_path}/ds/GL_CLM_BAL_DTL_F.parquet")
    .select(
        "GL_CLM_BAL_DTL_SK",
        "EDW_SRC_SYS_CD",
        "EDW_CLM_ID",
        "GL_CLM_DTL_CK",
        "PCA_IN",
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        "CRT_RUN_CYC_EXCTN_DT_SK",
        "CLM_SK",
        "CLS_PLN_PROD_CAT_CD",
        "CLM_REMIT_HIST_PAYMT_OVRD_CD",
        "EDW_FNCL_LOB_CD",
        "GL_FNCL_LOB_CD",
        "RCST_FNCL_LOB_CD",
        "IN_BAL_IN",
        "CRT_DTM",
        "PD_YR_MO",
        "DIFF_AMT",
        "EDW_PD_AMT",
        "GL_PD_AMT",
        "PCA_RUNOUT_AMT",
        "GL_ACCT_NO",
        "GRP_ID",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK"
    )
)

df_lnk_CpyOut = df_ds_GL_CLM_BAL_DTL_F_Extr.select(
    F.col("EDW_SRC_SYS_CD"),
    F.col("EDW_CLM_ID"),
    F.col("GL_CLM_DTL_CK"),
    F.col("PCA_IN")
)

df_lnk_FullDataJnIn = df_ds_GL_CLM_BAL_DTL_F_Extr.select(
    F.col("EDW_SRC_SYS_CD"),
    F.col("EDW_CLM_ID"),
    F.col("GL_CLM_DTL_CK"),
    F.col("PCA_IN"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("CLM_SK"),
    F.col("CLS_PLN_PROD_CAT_CD"),
    F.col("CLM_REMIT_HIST_PAYMT_OVRD_CD"),
    F.col("EDW_FNCL_LOB_CD"),
    F.col("GL_FNCL_LOB_CD"),
    F.col("RCST_FNCL_LOB_CD"),
    F.col("IN_BAL_IN"),
    F.col("CRT_DTM"),
    F.col("PD_YR_MO"),
    F.col("DIFF_AMT"),
    F.col("EDW_PD_AMT"),
    F.col("GL_PD_AMT"),
    F.col("PCA_RUNOUT_AMT"),
    F.col("GL_ACCT_NO"),
    F.col("GRP_ID"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

df_rdp_NaturalKeys = dedup_sort(
    df_lnk_CpyOut,
    ["EDW_SRC_SYS_CD", "EDW_CLM_ID", "GL_CLM_DTL_CK", "PCA_IN"],
    [("EDW_SRC_SYS_CD","A"),("EDW_CLM_ID","A"),("GL_CLM_DTL_CK","A"),("PCA_IN","A")]
)

df_lnk_RemDupDataOut = df_rdp_NaturalKeys.select(
    F.col("EDW_SRC_SYS_CD"),
    F.col("EDW_CLM_ID"),
    F.col("GL_CLM_DTL_CK"),
    F.col("PCA_IN")
)

jdbc_url, jdbc_props = get_db_config(edw_secret_name)
df_db2_K_GL_CLM_BAL_DTL_F_Read = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"SELECT EDW_SRC_SYS_CD, EDW_CLM_ID, GL_CLM_DTL_CK, PCA_IN, CRT_RUN_CYC_EXCTN_DT_SK, GL_CLM_BAL_DTL_SK, CRT_RUN_CYC_EXCTN_SK FROM {EDWOwner}.K_GL_CLM_BAL_DTL_F")
    .load()
)

df_jn_KMbrRcstVndrCtF = df_lnk_RemDupDataOut.alias("left").join(
    df_db2_K_GL_CLM_BAL_DTL_F_Read.alias("right"),
    on=[
        F.col("left.EDW_SRC_SYS_CD") == F.col("right.EDW_SRC_SYS_CD"),
        F.col("left.EDW_CLM_ID") == F.col("right.EDW_CLM_ID"),
        F.col("left.GL_CLM_DTL_CK") == F.col("right.GL_CLM_DTL_CK"),
        F.col("left.PCA_IN") == F.col("right.PCA_IN")
    ],
    how="left"
)

df_lnk_KGLClmBalDtlFJoinOut = df_jn_KMbrRcstVndrCtF.select(
    F.col("left.EDW_SRC_SYS_CD").alias("EDW_SRC_SYS_CD"),
    F.col("left.EDW_CLM_ID").alias("EDW_CLM_ID"),
    F.col("left.GL_CLM_DTL_CK").alias("GL_CLM_DTL_CK"),
    F.col("left.PCA_IN").alias("PCA_IN"),
    F.col("right.CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("right.GL_CLM_BAL_DTL_SK").alias("GL_CLM_BAL_DTL_SK"),
    F.col("right.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK")
)

df_xfrm_PKEYgen_in = (
    df_lnk_KGLClmBalDtlFJoinOut
    .withColumn("GL_CLM_BAL_DTL_SK_original", F.col("GL_CLM_BAL_DTL_SK"))
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK_original", F.col("CRT_RUN_CYC_EXCTN_DT_SK"))
    .withColumn("CRT_RUN_CYC_EXCTN_SK_original", F.col("CRT_RUN_CYC_EXCTN_SK"))
    .withColumn("EDWRunCycleDate", F.lit(EDWRunCycleDate))
    .withColumn("EDWRunCycle", F.lit(EDWRunCycle))
)

df_enriched = SurrogateKeyGen(
    df_xfrm_PKEYgen_in,
    <DB sequence name>,
    "GL_CLM_BAL_DTL_SK",
    <schema>,
    <secret_name>
)

df_enriched = df_enriched.withColumn(
    "CRT_RUN_CYC_EXCTN_DT_SK",
    F.when(
        F.isnull(F.col("CRT_RUN_CYC_EXCTN_DT_SK_original")),
        F.col("EDWRunCycleDate")
    ).otherwise(F.col("CRT_RUN_CYC_EXCTN_DT_SK_original"))
).withColumn(
    "CRT_RUN_CYC_EXCTN_SK",
    F.when(
        F.isnull(F.col("CRT_RUN_CYC_EXCTN_SK_original")),
        F.col("EDWRunCycle")
    ).otherwise(F.col("CRT_RUN_CYC_EXCTN_SK_original"))
)

df_lnk_KGLClmBalDtlFOut = df_enriched.filter(
    F.isnull("GL_CLM_BAL_DTL_SK_original")
).select(
    F.col("EDW_SRC_SYS_CD"),
    F.col("EDW_CLM_ID"),
    F.col("GL_CLM_DTL_CK"),
    F.col("PCA_IN"),
    F.col("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("GL_CLM_BAL_DTL_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK")
)

df_lnk_PKEYxfrmOut = df_enriched.select(
    F.col("EDW_SRC_SYS_CD"),
    F.col("EDW_CLM_ID"),
    F.col("GL_CLM_DTL_CK"),
    F.col("PCA_IN"),
    F.col("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("GL_CLM_BAL_DTL_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK")
)

temp_table_for_load = "STAGING.IdsEdwGlClmBalDtlFPKey_db2_KGLClmBalDtlFLoad_temp"
drop_sql = f"DROP TABLE IF EXISTS {temp_table_for_load}"
execute_dml(drop_sql, jdbc_url, jdbc_props)

df_lnk_KGLClmBalDtlFOut.write.format("jdbc").option("url", jdbc_url).options(**jdbc_props).option("dbtable", temp_table_for_load).mode("overwrite").save()

merge_sql = f"""
MERGE INTO {EDWOwner}.K_GL_CLM_BAL_DTL_F AS main
USING {temp_table_for_load} AS t
ON main.GL_CLM_BAL_DTL_SK = t.GL_CLM_BAL_DTL_SK
WHEN MATCHED THEN
  UPDATE SET
    EDW_SRC_SYS_CD = t.EDW_SRC_SYS_CD,
    EDW_CLM_ID = t.EDW_CLM_ID,
    GL_CLM_DTL_CK = t.GL_CLM_DTL_CK,
    PCA_IN = t.PCA_IN,
    CRT_RUN_CYC_EXCTN_DT_SK = t.CRT_RUN_CYC_EXCTN_DT_SK,
    CRT_RUN_CYC_EXCTN_SK = t.CRT_RUN_CYC_EXCTN_SK
WHEN NOT MATCHED THEN
  INSERT (
    GL_CLM_BAL_DTL_SK,
    EDW_SRC_SYS_CD,
    EDW_CLM_ID,
    GL_CLM_DTL_CK,
    PCA_IN,
    CRT_RUN_CYC_EXCTN_DT_SK,
    CRT_RUN_CYC_EXCTN_SK
  )
  VALUES (
    t.GL_CLM_BAL_DTL_SK,
    t.EDW_SRC_SYS_CD,
    t.EDW_CLM_ID,
    t.GL_CLM_DTL_CK,
    t.PCA_IN,
    t.CRT_RUN_CYC_EXCTN_DT_SK,
    t.CRT_RUN_CYC_EXCTN_SK
  );
"""
execute_dml(merge_sql, jdbc_url, jdbc_props)

df_jn_PKEYs = df_lnk_FullDataJnIn.alias("left").join(
    df_lnk_PKEYxfrmOut.alias("right"),
    on=[
        F.col("left.EDW_SRC_SYS_CD") == F.col("right.EDW_SRC_SYS_CD"),
        F.col("left.EDW_CLM_ID") == F.col("right.EDW_CLM_ID"),
        F.col("left.GL_CLM_DTL_CK") == F.col("right.GL_CLM_DTL_CK"),
        F.col("left.PCA_IN") == F.col("right.PCA_IN")
    ],
    how="left"
)

df_lnk_IdsEdwGLClmBalDtlMbrFPky_Out = df_jn_PKEYs.select(
    F.col("right.GL_CLM_BAL_DTL_SK").alias("GL_CLM_BAL_DTL_SK"),
    F.col("left.EDW_SRC_SYS_CD").alias("EDW_SRC_SYS_CD"),
    F.col("left.EDW_CLM_ID").alias("EDW_CLM_ID"),
    F.col("left.GL_CLM_DTL_CK").alias("GL_CLM_DTL_CK"),
    F.col("left.PCA_IN").alias("PCA_IN"),
    F.col("left.LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("right.CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("left.CLM_SK").alias("CLM_SK"),
    F.col("left.CLS_PLN_PROD_CAT_CD").alias("CLS_PLN_PROD_CAT_CD"),
    F.col("left.CLM_REMIT_HIST_PAYMT_OVRD_CD").alias("CLM_REMIT_HIST_PAYMT_OVRD_CD"),
    F.col("left.EDW_FNCL_LOB_CD").alias("EDW_FNCL_LOB_CD"),
    F.col("left.GL_FNCL_LOB_CD").alias("GL_FNCL_LOB_CD"),
    F.col("left.RCST_FNCL_LOB_CD").alias("RCST_FNCL_LOB_CD"),
    F.col("left.IN_BAL_IN").alias("IN_BAL_IN"),
    F.col("left.CRT_DTM").alias("CRT_DTM"),
    F.col("left.PD_YR_MO").alias("PD_YR_MO"),
    F.col("left.DIFF_AMT").alias("DIFF_AMT"),
    F.col("left.EDW_PD_AMT").alias("EDW_PD_AMT"),
    F.col("left.GL_PD_AMT").alias("GL_PD_AMT"),
    F.col("left.PCA_RUNOUT_AMT").alias("PCA_RUNOUT_AMT"),
    F.col("left.GL_ACCT_NO").alias("GL_ACCT_NO"),
    F.col("left.GRP_ID").alias("GRP_ID"),
    F.col("right.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("left.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

df_final = df_lnk_IdsEdwGLClmBalDtlMbrFPky_Out
df_final = df_final.withColumn("PCA_IN", F.rpad(F.col("PCA_IN"), 1, " "))
df_final = df_final.withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
df_final = df_final.withColumn("CRT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
df_final = df_final.withColumn("PD_YR_MO", F.rpad(F.col("PD_YR_MO"), 6, " "))
df_final = df_final.withColumn("IN_BAL_IN", F.rpad(F.col("IN_BAL_IN"), 1, " "))

df_final = df_final.select(
    "GL_CLM_BAL_DTL_SK",
    "EDW_SRC_SYS_CD",
    "EDW_CLM_ID",
    "GL_CLM_DTL_CK",
    "PCA_IN",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "CLM_SK",
    "CLS_PLN_PROD_CAT_CD",
    "CLM_REMIT_HIST_PAYMT_OVRD_CD",
    "EDW_FNCL_LOB_CD",
    "GL_FNCL_LOB_CD",
    "RCST_FNCL_LOB_CD",
    "IN_BAL_IN",
    "CRT_DTM",
    "PD_YR_MO",
    "DIFF_AMT",
    "EDW_PD_AMT",
    "GL_PD_AMT",
    "PCA_RUNOUT_AMT",
    "GL_ACCT_NO",
    "GRP_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK"
)

write_files(
    df_final,
    f"{adls_path}/load/GL_CLM_BAL_DTL_F.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)