# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC Pooja Sunkara          02/18/2014        5114                              Original Programming                                                                            EnterpriseWrhsDevl      Bhoomi Dasari          2/24/2014

# MAGIC Job Name: IdsEdwClmOnHandSumFPkey
# MAGIC Read CLM_ON_HAND_SUM_F Dataset created in the IdsEdwClmInvtrySumFExtr Job.
# MAGIC Copy for creating two output streams
# MAGIC This job will be used when there is a need to Assign primary keys to incoming data before loading into Target table.
# MAGIC Extract dataset is created in the extract job ran prior to this.
# MAGIC Every K table will need a dedicated Database SEQUENCE to provide and track new numbers to use.
# MAGIC New KEYS generated for each run will be Inserted into the K table.
# MAGIC Read K_CLM_ON_HAND_SUM_F Table to pull the Natural Keys and the Skey.
# MAGIC Left Join on Natural Keys
# MAGIC Left Join on Natural Keys
# MAGIC New PKEYs are generated in this transformer. A database sequence will be used to create next PKEY value.
# MAGIC Write CLM_ON_HAND_SUM_F.dat for the IdsEdwClmOnHandSumFLoad Job.
# MAGIC Insert Only the Newly generated Pkeys into the K Table
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, rpad, lit
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------

EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
EDWRunCycle = get_widget_value('EDWRunCycle','')
jdbc_url, jdbc_props = get_db_config(edw_secret_name)

df_ds_CLM_ON_HAND_SUM_F_out = spark.read.parquet(f"{adls_path}/ds/CLM_ON_HAND_SUM_F.parquet")

df_cpy_Multistreams_in = df_ds_CLM_ON_HAND_SUM_F_out

df_lnk_ClmInvtry_in = df_cpy_Multistreams_in.select(
    "SRC_SYS_CD",
    "OPS_WORK_UNIT_ID",
    "CLM_INVTRY_SUBTYP_CD",
    "EXTR_DT_SK",
    "INPT_DT_SK",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "OPS_WORK_UNIT_SK",
    "EXTR_DT",
    "INPT_DT",
    "NON_FIRST_PASS_SUM_CT",
    "SUM_CT",
    "CLM_INVTRY_SUBTYP_NM",
    "OPS_WORK_UNIT_DESC",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "CLM_INVTRY_SUBTYP_CD_SK"
)

df_lnk_ClmInvtrySumFData_in = df_cpy_Multistreams_in.select(
    "SRC_SYS_CD",
    "OPS_WORK_UNIT_ID",
    "CLM_INVTRY_SUBTYP_CD",
    "EXTR_DT_SK",
    "INPT_DT_SK"
)

extract_query = f"SELECT SRC_SYS_CD, OPS_WORK_UNIT_ID, CLM_INVTRY_SUBTYP_CD, EXTR_DT_SK, INPT_DT_SK, CLM_ON_HAND_SUM_SK FROM {EDWOwner}.K_CLM_ON_HAND_SUM_F"
df_db2_K_CLM_ON_HAND_SUM_F_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_jn_ClmInvtrySumF = df_lnk_ClmInvtrySumFData_in.alias("lnk_ClmInvtrySumFData_in").join(
    df_db2_K_CLM_ON_HAND_SUM_F_in.alias("lnk_KClmInvtrySumF_out"),
    on=[
        col("lnk_ClmInvtrySumFData_in.SRC_SYS_CD") == col("lnk_KClmInvtrySumF_out.SRC_SYS_CD"),
        col("lnk_ClmInvtrySumFData_in.OPS_WORK_UNIT_ID") == col("lnk_KClmInvtrySumF_out.OPS_WORK_UNIT_ID"),
        col("lnk_ClmInvtrySumFData_in.CLM_INVTRY_SUBTYP_CD") == col("lnk_KClmInvtrySumF_out.CLM_INVTRY_SUBTYP_CD"),
        col("lnk_ClmInvtrySumFData_in.EXTR_DT_SK") == col("lnk_KClmInvtrySumF_out.EXTR_DT_SK"),
        col("lnk_ClmInvtrySumFData_in.INPT_DT_SK") == col("lnk_KClmInvtrySumF_out.INPT_DT_SK"),
    ],
    how="left"
).select(
    col("lnk_ClmInvtrySumFData_in.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("lnk_ClmInvtrySumFData_in.OPS_WORK_UNIT_ID").alias("OPS_WORK_UNIT_ID"),
    col("lnk_ClmInvtrySumFData_in.CLM_INVTRY_SUBTYP_CD").alias("CLM_INVTRY_SUBTYP_CD"),
    col("lnk_ClmInvtrySumFData_in.EXTR_DT_SK").alias("EXTR_DT_SK"),
    col("lnk_ClmInvtrySumFData_in.INPT_DT_SK").alias("INPT_DT_SK"),
    col("lnk_KClmInvtrySumF_out.CLM_ON_HAND_SUM_SK").alias("CLM_ON_HAND_SUM_SK")
)

df_xfrm_PKEYgen = df_jn_ClmInvtrySumF.withColumn("orig_CLM_ON_HAND_SUM_SK", col("CLM_ON_HAND_SUM_SK"))
df_enriched = df_xfrm_PKEYgen
df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"CLM_ON_HAND_SUM_SK",<schema>,<secret_name>)

df_lnk_Pkey_out = df_enriched.select(
    col("SRC_SYS_CD"),
    col("OPS_WORK_UNIT_ID"),
    col("CLM_INVTRY_SUBTYP_CD"),
    col("EXTR_DT_SK"),
    col("INPT_DT_SK"),
    col("CLM_ON_HAND_SUM_SK")
)

df_lnk_KClmInvtrySumF_out = (
    df_enriched.filter(col("orig_CLM_ON_HAND_SUM_SK").isNull())
    .select(
        col("SRC_SYS_CD"),
        col("OPS_WORK_UNIT_ID"),
        col("CLM_INVTRY_SUBTYP_CD"),
        col("EXTR_DT_SK"),
        col("INPT_DT_SK"),
        col("CLM_ON_HAND_SUM_SK"),
        lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK")
    )
)

execute_dml(f"DROP TABLE IF EXISTS STAGING.IdsEdwClmOnHandSumFPkey_db2_K_CLM_ON_HAND_SUM_F_load_temp", jdbc_url, jdbc_props)
(
    df_lnk_KClmInvtrySumF_out.write
    .format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("dbtable", "STAGING.IdsEdwClmOnHandSumFPkey_db2_K_CLM_ON_HAND_SUM_F_load_temp")
    .mode("overwrite")
    .save()
)

merge_sql = f"""
MERGE INTO {EDWOwner}.K_CLM_ON_HAND_SUM_F AS T
USING STAGING.IdsEdwClmOnHandSumFPkey_db2_K_CLM_ON_HAND_SUM_F_load_temp AS S
ON (
    T.SRC_SYS_CD = S.SRC_SYS_CD
    AND T.OPS_WORK_UNIT_ID = S.OPS_WORK_UNIT_ID
    AND T.CLM_INVTRY_SUBTYP_CD = S.CLM_INVTRY_SUBTYP_CD
    AND T.EXTR_DT_SK = S.EXTR_DT_SK
    AND T.INPT_DT_SK = S.INPT_DT_SK
)
WHEN MATCHED THEN
  UPDATE SET
    T.CLM_ON_HAND_SUM_SK = S.CLM_ON_HAND_SUM_SK,
    T.CRT_RUN_CYC_EXCTN_DT_SK = S.CRT_RUN_CYC_EXCTN_DT_SK,
    T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK
WHEN NOT MATCHED THEN
  INSERT (
    SRC_SYS_CD,
    OPS_WORK_UNIT_ID,
    CLM_INVTRY_SUBTYP_CD,
    EXTR_DT_SK,
    INPT_DT_SK,
    CLM_ON_HAND_SUM_SK,
    CRT_RUN_CYC_EXCTN_DT_SK,
    CRT_RUN_CYC_EXCTN_SK
  )
  VALUES (
    S.SRC_SYS_CD,
    S.OPS_WORK_UNIT_ID,
    S.CLM_INVTRY_SUBTYP_CD,
    S.EXTR_DT_SK,
    S.INPT_DT_SK,
    S.CLM_ON_HAND_SUM_SK,
    S.CRT_RUN_CYC_EXCTN_DT_SK,
    S.CRT_RUN_CYC_EXCTN_SK
  );
"""
execute_dml(merge_sql, jdbc_url, jdbc_props)

df_jn_Pkey = df_lnk_ClmInvtry_in.alias("lnk_ClmInvtry_in").join(
    df_lnk_Pkey_out.alias("lnk_Pkey_out"),
    on=[
        col("lnk_ClmInvtry_in.SRC_SYS_CD") == col("lnk_Pkey_out.SRC_SYS_CD"),
        col("lnk_ClmInvtry_in.OPS_WORK_UNIT_ID") == col("lnk_Pkey_out.OPS_WORK_UNIT_ID"),
        col("lnk_ClmInvtry_in.CLM_INVTRY_SUBTYP_CD") == col("lnk_Pkey_out.CLM_INVTRY_SUBTYP_CD"),
        col("lnk_ClmInvtry_in.EXTR_DT_SK") == col("lnk_Pkey_out.EXTR_DT_SK"),
        col("lnk_ClmInvtry_in.INPT_DT_SK") == col("lnk_Pkey_out.INPT_DT_SK"),
    ],
    how="left"
).select(
    col("lnk_Pkey_out.CLM_ON_HAND_SUM_SK").alias("CLM_ON_HAND_SUM_SK"),
    col("lnk_ClmInvtry_in.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("lnk_ClmInvtry_in.OPS_WORK_UNIT_ID").alias("OPS_WORK_UNIT_ID"),
    col("lnk_ClmInvtry_in.CLM_INVTRY_SUBTYP_CD").alias("CLM_INVTRY_SUBTYP_CD"),
    rpad(col("lnk_ClmInvtry_in.EXTR_DT_SK"), 10, " ").alias("EXTR_DT_SK"),
    rpad(col("lnk_ClmInvtry_in.INPT_DT_SK"), 10, " ").alias("INPT_DT_SK"),
    rpad(col("lnk_ClmInvtry_in.CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    rpad(col("lnk_ClmInvtry_in.LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("lnk_ClmInvtry_in.OPS_WORK_UNIT_SK").alias("OPS_WORK_UNIT_SK"),
    col("lnk_ClmInvtry_in.EXTR_DT").alias("EXTR_DT"),
    col("lnk_ClmInvtry_in.INPT_DT").alias("INPT_DT"),
    col("lnk_ClmInvtry_in.NON_FIRST_PASS_SUM_CT").alias("NON_FIRST_PASS_SUM_CT"),
    col("lnk_ClmInvtry_in.SUM_CT").alias("SUM_CT"),
    col("lnk_ClmInvtry_in.CLM_INVTRY_SUBTYP_NM").alias("CLM_INVTRY_SUBTYP_NM"),
    col("lnk_ClmInvtry_in.OPS_WORK_UNIT_DESC").alias("OPS_WORK_UNIT_DESC"),
    col("lnk_ClmInvtry_in.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("lnk_ClmInvtry_in.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("lnk_ClmInvtry_in.CLM_INVTRY_SUBTYP_CD_SK").alias("CLM_INVTRY_SUBTYP_CD_SK")
)

df_seq_CLM_ON_HAND_SUM_F_csv_load = df_jn_Pkey.select(
    "CLM_ON_HAND_SUM_SK",
    "SRC_SYS_CD",
    "OPS_WORK_UNIT_ID",
    "CLM_INVTRY_SUBTYP_CD",
    "EXTR_DT_SK",
    "INPT_DT_SK",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "OPS_WORK_UNIT_SK",
    "EXTR_DT",
    "INPT_DT",
    "NON_FIRST_PASS_SUM_CT",
    "SUM_CT",
    "CLM_INVTRY_SUBTYP_NM",
    "OPS_WORK_UNIT_DESC",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "CLM_INVTRY_SUBTYP_CD_SK"
)

write_files(
    df_seq_CLM_ON_HAND_SUM_F_csv_load,
    f"{adls_path}/load/CLM_ON_HAND_SUM_F.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)