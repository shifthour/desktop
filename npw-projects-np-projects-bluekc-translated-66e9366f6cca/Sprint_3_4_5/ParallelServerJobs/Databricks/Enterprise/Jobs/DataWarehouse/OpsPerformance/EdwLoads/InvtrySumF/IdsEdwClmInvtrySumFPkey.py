# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC Pooja Sunkara          02/12/2014        5114                              Original Programming                                                                            EnterpriseWrhsDevl      Bhoomi Dasari           2/24/2014

# MAGIC Job Name: IdsEdwClmInvtrySumFPkey
# MAGIC Read CLM_INVTRY_SUM_F Dataset created in the IdsEdwClmInvtrySumFExtr Job.
# MAGIC Copy for creating two output streams
# MAGIC This job will be used when there is a need to Assign primary keys to incoming data before loading into Target table.
# MAGIC Extract dataset is created in the extract job ran prior to this.
# MAGIC Every K table will need a dedicated Database SEQUENCE to provide and track new numbers to use.
# MAGIC New KEYS generated for each run will be Inserted into the K table.
# MAGIC Read K_CLM_INVTRY_SUM_F Table to pull the Natural Keys and the Skey.
# MAGIC Left Join on Natural Keys
# MAGIC Left Join on Natural Keys
# MAGIC New PKEYs are generated in this transformer. A database sequence will be used to create next PKEY value.
# MAGIC Write CLM_INVTRY_SUM_F.dat for the IdsEdwCmnPrctNtwkCtFLoad Job.
# MAGIC Insert Only the Newly generated Pkeys into the K Table
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
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


EDWOwner = get_widget_value("EDWOwner", "")
EDWRunCycleDate = get_widget_value("EDWRunCycleDate", "")
EDWRunCycle = get_widget_value("EDWRunCycle", "")
edw_secret_name = get_widget_value("edw_secret_name", "")

df_ds_CLM_INVTRY_SUM_F_out = spark.read.parquet(f"{adls_path}/ds/CLM_INVTRY_SUM_F.parquet")

df_lnk_ClmInvtry_in = df_ds_CLM_INVTRY_SUM_F_out.select(
    "SRC_SYS_CD",
    "OPS_WORK_UNIT_ID",
    "CLM_INVTRY_SUBTYP_CD",
    "CLM_INVTRY_PEND_CAT_CD",
    "CLM_STTUS_CHG_RSN_CD",
    "INPT_DT_SK",
    "RCVD_DT_SK",
    "LAST_UPDT_DT_SK",
    "EXTR_DT_SK",
    "IN_HSE_CLM_AGE",
    "IN_LOC_CLM_AGE",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "IN_LOC_CLM_INVTRY_AGE_CAT_SK",
    "IN_HSE_CLM_INVTRY_AGE_CAT_SK",
    "OPS_WORK_UNIT_SK",
    "EXTR_DT",
    "INPT_DT",
    "LAST_UPDT_DT",
    "RCVD_DT",
    "INVTRY_CT",
    "CLM_INVTRY_PEND_CAT_NM",
    "CLM_INVTRY_SUBTYP_NM",
    "CLM_STTUS_CHG_RSN_NM",
    "OPS_WORK_UNIT_DESC",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "CLM_INVTRY_PEND_CAT_CD_SK",
    "CLM_INVTRY_SUBTYP_CD_SK",
    "CLM_STTUS_CHG_RSN_CD_SK"
)

df_lnk_ClmInvtrySumFData_in = df_ds_CLM_INVTRY_SUM_F_out.select(
    "SRC_SYS_CD",
    "OPS_WORK_UNIT_ID",
    "CLM_INVTRY_SUBTYP_CD",
    "CLM_INVTRY_PEND_CAT_CD",
    "CLM_STTUS_CHG_RSN_CD",
    "INPT_DT_SK",
    "RCVD_DT_SK",
    "LAST_UPDT_DT_SK",
    "EXTR_DT_SK",
    "IN_HSE_CLM_AGE",
    "IN_LOC_CLM_AGE"
)

jdbc_url, jdbc_props = get_db_config(edw_secret_name)
extract_query = f"""
SELECT
 SRC_SYS_CD,
 OPS_WORK_UNIT_ID,
 CLM_INVTRY_SUBTYP_CD,
 CLM_INVTRY_PEND_CAT_CD,
 CLM_STTUS_CHG_RSN_CD,
 INPT_DT_SK,
 RCVD_DT_SK,
 LAST_UPDT_DT_SK,
 EXTR_DT_SK,
 IN_HSE_CLM_AGE,
 IN_LOC_CLM_AGE,
 CLM_INVTRY_SUM_SK
FROM {EDWOwner}.K_CLM_INVTRY_SUM_F
"""
df_db2_K_CLM_INVTRY_SUM_F_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query.strip())
    .load()
)

df_lnk_ClmInvtrySumFData_in_alias = df_lnk_ClmInvtrySumFData_in.alias("lnk_ClmInvtrySumFData_in")
df_lnk_KClmInvtrySumF_out_alias = df_db2_K_CLM_INVTRY_SUM_F_in.alias("lnk_KClmInvtrySumF_out")

df_jn_ClmInvtrySumF = df_lnk_ClmInvtrySumFData_in_alias.join(
    df_lnk_KClmInvtrySumF_out_alias,
    (
        (df_lnk_ClmInvtrySumFData_in_alias["SRC_SYS_CD"] == df_lnk_KClmInvtrySumF_out_alias["SRC_SYS_CD"])
        & (df_lnk_ClmInvtrySumFData_in_alias["OPS_WORK_UNIT_ID"] == df_lnk_KClmInvtrySumF_out_alias["OPS_WORK_UNIT_ID"])
        & (df_lnk_ClmInvtrySumFData_in_alias["CLM_INVTRY_SUBTYP_CD"] == df_lnk_KClmInvtrySumF_out_alias["CLM_INVTRY_SUBTYP_CD"])
        & (df_lnk_ClmInvtrySumFData_in_alias["CLM_INVTRY_PEND_CAT_CD"] == df_lnk_KClmInvtrySumF_out_alias["CLM_INVTRY_PEND_CAT_CD"])
        & (df_lnk_ClmInvtrySumFData_in_alias["CLM_STTUS_CHG_RSN_CD"] == df_lnk_KClmInvtrySumF_out_alias["CLM_STTUS_CHG_RSN_CD"])
        & (df_lnk_ClmInvtrySumFData_in_alias["INPT_DT_SK"] == df_lnk_KClmInvtrySumF_out_alias["INPT_DT_SK"])
        & (df_lnk_ClmInvtrySumFData_in_alias["RCVD_DT_SK"] == df_lnk_KClmInvtrySumF_out_alias["RCVD_DT_SK"])
        & (df_lnk_ClmInvtrySumFData_in_alias["LAST_UPDT_DT_SK"] == df_lnk_KClmInvtrySumF_out_alias["LAST_UPDT_DT_SK"])
        & (df_lnk_ClmInvtrySumFData_in_alias["EXTR_DT_SK"] == df_lnk_KClmInvtrySumF_out_alias["EXTR_DT_SK"])
        & (df_lnk_ClmInvtrySumFData_in_alias["IN_HSE_CLM_AGE"] == df_lnk_KClmInvtrySumF_out_alias["IN_HSE_CLM_AGE"])
        & (df_lnk_ClmInvtrySumFData_in_alias["IN_LOC_CLM_AGE"] == df_lnk_KClmInvtrySumF_out_alias["IN_LOC_CLM_AGE"])
    ),
    how="left"
)

df_jn_ClmInvtrySumF_select = df_jn_ClmInvtrySumF.select(
    df_lnk_ClmInvtrySumFData_in_alias["SRC_SYS_CD"].alias("SRC_SYS_CD"),
    df_lnk_ClmInvtrySumFData_in_alias["OPS_WORK_UNIT_ID"].alias("OPS_WORK_UNIT_ID"),
    df_lnk_ClmInvtrySumFData_in_alias["CLM_INVTRY_SUBTYP_CD"].alias("CLM_INVTRY_SUBTYP_CD"),
    df_lnk_ClmInvtrySumFData_in_alias["CLM_INVTRY_PEND_CAT_CD"].alias("CLM_INVTRY_PEND_CAT_CD"),
    df_lnk_ClmInvtrySumFData_in_alias["CLM_STTUS_CHG_RSN_CD"].alias("CLM_STTUS_CHG_RSN_CD"),
    df_lnk_ClmInvtrySumFData_in_alias["INPT_DT_SK"].alias("INPT_DT_SK"),
    df_lnk_ClmInvtrySumFData_in_alias["RCVD_DT_SK"].alias("RCVD_DT_SK"),
    df_lnk_ClmInvtrySumFData_in_alias["LAST_UPDT_DT_SK"].alias("LAST_UPDT_DT_SK"),
    df_lnk_ClmInvtrySumFData_in_alias["EXTR_DT_SK"].alias("EXTR_DT_SK"),
    df_lnk_ClmInvtrySumFData_in_alias["IN_HSE_CLM_AGE"].alias("IN_HSE_CLM_AGE"),
    df_lnk_ClmInvtrySumFData_in_alias["IN_LOC_CLM_AGE"].alias("IN_LOC_CLM_AGE"),
    df_lnk_KClmInvtrySumF_out_alias["CLM_INVTRY_SUM_SK"].alias("CLM_INVTRY_SUM_SK")
)

df_xfrm_input = df_jn_ClmInvtrySumF_select.withColumn(
    "tmp_input_clm_invtry_sum_sk",
    F.col("CLM_INVTRY_SUM_SK")
).withColumn(
    "CLM_INVTRY_SUM_SK",
    F.when(F.col("CLM_INVTRY_SUM_SK").isNull(), F.lit(None)).otherwise(F.col("CLM_INVTRY_SUM_SK"))
)

df_enriched = SurrogateKeyGen(df_xfrm_input, <DB sequence name>, "CLM_INVTRY_SUM_SK", <schema>, <secret_name>)

df_lnk_Pkey_out = df_enriched.select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("OPS_WORK_UNIT_ID").alias("OPS_WORK_UNIT_ID"),
    F.col("CLM_INVTRY_SUBTYP_CD").alias("CLM_INVTRY_SUBTYP_CD"),
    F.col("CLM_INVTRY_PEND_CAT_CD").alias("CLM_INVTRY_PEND_CAT_CD"),
    F.col("CLM_STTUS_CHG_RSN_CD").alias("CLM_STTUS_CHG_RSN_CD"),
    F.col("INPT_DT_SK").alias("INPT_DT_SK"),
    F.col("RCVD_DT_SK").alias("RCVD_DT_SK"),
    F.col("LAST_UPDT_DT_SK").alias("LAST_UPDT_DT_SK"),
    F.col("EXTR_DT_SK").alias("EXTR_DT_SK"),
    F.col("IN_HSE_CLM_AGE").alias("IN_HSE_CLM_AGE"),
    F.col("IN_LOC_CLM_AGE").alias("IN_LOC_CLM_AGE"),
    F.col("CLM_INVTRY_SUM_SK").alias("CLM_INVTRY_SUM_SK")
)

df_lnk_KClmInvtrySumF_out = df_enriched.filter(
    F.col("tmp_input_clm_invtry_sum_sk").isNull()
).select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("OPS_WORK_UNIT_ID").alias("OPS_WORK_UNIT_ID"),
    F.col("CLM_INVTRY_SUBTYP_CD").alias("CLM_INVTRY_SUBTYP_CD"),
    F.col("CLM_INVTRY_PEND_CAT_CD").alias("CLM_INVTRY_PEND_CAT_CD"),
    F.col("CLM_STTUS_CHG_RSN_CD").alias("CLM_STTUS_CHG_RSN_CD"),
    F.col("INPT_DT_SK").alias("INPT_DT_SK"),
    F.col("RCVD_DT_SK").alias("RCVD_DT_SK"),
    F.col("LAST_UPDT_DT_SK").alias("LAST_UPDT_DT_SK"),
    F.col("EXTR_DT_SK").alias("EXTR_DT_SK"),
    F.col("IN_HSE_CLM_AGE").alias("IN_HSE_CLM_AGE"),
    F.col("IN_LOC_CLM_AGE").alias("IN_LOC_CLM_AGE"),
    F.lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("CLM_INVTRY_SUM_SK").alias("CLM_INVTRY_SUM_SK"),
    F.lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK")
)

temp_table_name = "STAGING.IdsEdwClmInvtrySumFPkey_db2_K_CLM_INVTRY_SUM_F_load_temp"

spark.sql(f"DROP TABLE IF EXISTS {temp_table_name}")

(
    df_lnk_KClmInvtrySumF_out.write
    .format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("dbtable", temp_table_name)
    .mode("overwrite")
    .save()
)

merge_sql = f"""
MERGE INTO {EDWOwner}.K_CLM_INVTRY_SUM_F AS T
USING {temp_table_name} AS S
ON T.CLM_INVTRY_SUM_SK = S.CLM_INVTRY_SUM_SK
WHEN MATCHED THEN UPDATE SET
  T.SRC_SYS_CD = S.SRC_SYS_CD,
  T.OPS_WORK_UNIT_ID = S.OPS_WORK_UNIT_ID,
  T.CLM_INVTRY_SUBTYP_CD = S.CLM_INVTRY_SUBTYP_CD,
  T.CLM_INVTRY_PEND_CAT_CD = S.CLM_INVTRY_PEND_CAT_CD,
  T.CLM_STTUS_CHG_RSN_CD = S.CLM_STTUS_CHG_RSN_CD,
  T.INPT_DT_SK = S.INPT_DT_SK,
  T.RCVD_DT_SK = S.RCVD_DT_SK,
  T.LAST_UPDT_DT_SK = S.LAST_UPDT_DT_SK,
  T.EXTR_DT_SK = S.EXTR_DT_SK,
  T.IN_HSE_CLM_AGE = S.IN_HSE_CLM_AGE,
  T.IN_LOC_CLM_AGE = S.IN_LOC_CLM_AGE,
  T.CRT_RUN_CYC_EXCTN_DT_SK = S.CRT_RUN_CYC_EXCTN_DT_SK,
  T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK
WHEN NOT MATCHED THEN INSERT (
  SRC_SYS_CD,
  OPS_WORK_UNIT_ID,
  CLM_INVTRY_SUBTYP_CD,
  CLM_INVTRY_PEND_CAT_CD,
  CLM_STTUS_CHG_RSN_CD,
  INPT_DT_SK,
  RCVD_DT_SK,
  LAST_UPDT_DT_SK,
  EXTR_DT_SK,
  IN_HSE_CLM_AGE,
  IN_LOC_CLM_AGE,
  CRT_RUN_CYC_EXCTN_DT_SK,
  CLM_INVTRY_SUM_SK,
  CRT_RUN_CYC_EXCTN_SK
) VALUES (
  S.SRC_SYS_CD,
  S.OPS_WORK_UNIT_ID,
  S.CLM_INVTRY_SUBTYP_CD,
  S.CLM_INVTRY_PEND_CAT_CD,
  S.CLM_STTUS_CHG_RSN_CD,
  S.INPT_DT_SK,
  S.RCVD_DT_SK,
  S.LAST_UPDT_DT_SK,
  S.EXTR_DT_SK,
  S.IN_HSE_CLM_AGE,
  S.IN_LOC_CLM_AGE,
  S.CRT_RUN_CYC_EXCTN_DT_SK,
  S.CLM_INVTRY_SUM_SK,
  S.CRT_RUN_CYC_EXCTN_SK
);
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)

df_lnk_ClmInvtry_in_alias = df_lnk_ClmInvtry_in.alias("lnk_ClmInvtry_in")
df_lnk_Pkey_out_alias = df_lnk_Pkey_out.alias("lnk_Pkey_out")

df_jn_Pkey = df_lnk_ClmInvtry_in_alias.join(
    df_lnk_Pkey_out_alias,
    (
        (df_lnk_ClmInvtry_in_alias["SRC_SYS_CD"] == df_lnk_Pkey_out_alias["SRC_SYS_CD"])
        & (df_lnk_ClmInvtry_in_alias["OPS_WORK_UNIT_ID"] == df_lnk_Pkey_out_alias["OPS_WORK_UNIT_ID"])
        & (df_lnk_ClmInvtry_in_alias["CLM_INVTRY_SUBTYP_CD"] == df_lnk_Pkey_out_alias["CLM_INVTRY_SUBTYP_CD"])
        & (df_lnk_ClmInvtry_in_alias["CLM_INVTRY_PEND_CAT_CD"] == df_lnk_Pkey_out_alias["CLM_INVTRY_PEND_CAT_CD"])
        & (df_lnk_ClmInvtry_in_alias["CLM_STTUS_CHG_RSN_CD"] == df_lnk_Pkey_out_alias["CLM_STTUS_CHG_RSN_CD"])
        & (df_lnk_ClmInvtry_in_alias["INPT_DT_SK"] == df_lnk_Pkey_out_alias["INPT_DT_SK"])
        & (df_lnk_ClmInvtry_in_alias["RCVD_DT_SK"] == df_lnk_Pkey_out_alias["RCVD_DT_SK"])
        & (df_lnk_ClmInvtry_in_alias["LAST_UPDT_DT_SK"] == df_lnk_Pkey_out_alias["LAST_UPDT_DT_SK"])
        & (df_lnk_ClmInvtry_in_alias["EXTR_DT_SK"] == df_lnk_Pkey_out_alias["EXTR_DT_SK"])
        & (df_lnk_ClmInvtry_in_alias["IN_HSE_CLM_AGE"] == df_lnk_Pkey_out_alias["IN_HSE_CLM_AGE"])
        & (df_lnk_ClmInvtry_in_alias["IN_LOC_CLM_AGE"] == df_lnk_Pkey_out_alias["IN_LOC_CLM_AGE"])
    ),
    how="left"
)

df_lnk_IdsEdwClmInvtrySumFPkey_OutABC = df_jn_Pkey.select(
    df_lnk_Pkey_out_alias["CLM_INVTRY_SUM_SK"].alias("CLM_INVTRY_SUM_SK"),
    df_lnk_ClmInvtry_in_alias["SRC_SYS_CD"].alias("SRC_SYS_CD"),
    df_lnk_ClmInvtry_in_alias["OPS_WORK_UNIT_ID"].alias("OPS_WORK_UNIT_ID"),
    df_lnk_ClmInvtry_in_alias["CLM_INVTRY_SUBTYP_CD"].alias("CLM_INVTRY_SUBTYP_CD"),
    df_lnk_ClmInvtry_in_alias["CLM_INVTRY_PEND_CAT_CD"].alias("CLM_INVTRY_PEND_CAT_CD"),
    df_lnk_ClmInvtry_in_alias["CLM_STTUS_CHG_RSN_CD"].alias("CLM_STTUS_CHG_RSN_CD"),
    df_lnk_ClmInvtry_in_alias["INPT_DT_SK"].alias("INPT_DT_SK"),
    df_lnk_ClmInvtry_in_alias["RCVD_DT_SK"].alias("RCVD_DT_SK"),
    df_lnk_ClmInvtry_in_alias["LAST_UPDT_DT_SK"].alias("LAST_UPDT_DT_SK"),
    df_lnk_ClmInvtry_in_alias["EXTR_DT_SK"].alias("EXTR_DT_SK"),
    df_lnk_ClmInvtry_in_alias["IN_HSE_CLM_AGE"].alias("IN_HSE_CLM_AGE"),
    df_lnk_ClmInvtry_in_alias["IN_LOC_CLM_AGE"].alias("IN_LOC_CLM_AGE"),
    df_lnk_ClmInvtry_in_alias["CRT_RUN_CYC_EXCTN_DT_SK"].alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    df_lnk_ClmInvtry_in_alias["LAST_UPDT_RUN_CYC_EXCTN_DT_SK"].alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    df_lnk_ClmInvtry_in_alias["IN_LOC_CLM_INVTRY_AGE_CAT_SK"].alias("IN_LOC_CLM_INVTRY_AGE_CAT_SK"),
    df_lnk_ClmInvtry_in_alias["IN_HSE_CLM_INVTRY_AGE_CAT_SK"].alias("IN_HSE_CLM_INVTRY_AGE_CAT_SK"),
    df_lnk_ClmInvtry_in_alias["OPS_WORK_UNIT_SK"].alias("OPS_WORK_UNIT_SK"),
    df_lnk_ClmInvtry_in_alias["EXTR_DT"].alias("EXTR_DT"),
    df_lnk_ClmInvtry_in_alias["INPT_DT"].alias("INPT_DT"),
    df_lnk_ClmInvtry_in_alias["LAST_UPDT_DT"].alias("LAST_UPDT_DT"),
    df_lnk_ClmInvtry_in_alias["RCVD_DT"].alias("RCVD_DT"),
    df_lnk_ClmInvtry_in_alias["INVTRY_CT"].alias("INVTRY_CT"),
    df_lnk_ClmInvtry_in_alias["CLM_INVTRY_PEND_CAT_NM"].alias("CLM_INVTRY_PEND_CAT_NM"),
    df_lnk_ClmInvtry_in_alias["CLM_INVTRY_SUBTYP_NM"].alias("CLM_INVTRY_SUBTYP_NM"),
    df_lnk_ClmInvtry_in_alias["CLM_STTUS_CHG_RSN_NM"].alias("CLM_STTUS_CHG_RSN_NM"),
    df_lnk_ClmInvtry_in_alias["OPS_WORK_UNIT_DESC"].alias("OPS_WORK_UNIT_DESC"),
    df_lnk_ClmInvtry_in_alias["CRT_RUN_CYC_EXCTN_SK"].alias("CRT_RUN_CYC_EXCTN_SK"),
    df_lnk_ClmInvtry_in_alias["LAST_UPDT_RUN_CYC_EXCTN_SK"].alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    df_lnk_ClmInvtry_in_alias["CLM_INVTRY_PEND_CAT_CD_SK"].alias("CLM_INVTRY_PEND_CAT_CD_SK"),
    df_lnk_ClmInvtry_in_alias["CLM_INVTRY_SUBTYP_CD_SK"].alias("CLM_INVTRY_SUBTYP_CD_SK"),
    df_lnk_ClmInvtry_in_alias["CLM_STTUS_CHG_RSN_CD_SK"].alias("CLM_STTUS_CHG_RSN_CD_SK")
)

df_final = df_lnk_IdsEdwClmInvtrySumFPkey_OutABC.withColumn(
    "INPT_DT_SK", F.rpad(F.col("INPT_DT_SK"), 10, " ")
).withColumn(
    "RCVD_DT_SK", F.rpad(F.col("RCVD_DT_SK"), 10, " ")
).withColumn(
    "LAST_UPDT_DT_SK", F.rpad(F.col("LAST_UPDT_DT_SK"), 10, " ")
).withColumn(
    "EXTR_DT_SK", F.rpad(F.col("EXTR_DT_SK"), 10, " ")
).withColumn(
    "CRT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ")
).withColumn(
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ")
)

write_files(
    df_final,
    f"{adls_path}/load/CLM_INVTRY_SUM_F.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)