# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                           DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                                ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------             -------------------------------    ------------------------------       --------------------
# MAGIC  Balkarn Gill                 5/29/2013           5114                          generate primary key for k_prod_lmt_sum_f                                               EnterpriseWhseDevl           Pete Marshall             2013-08-08

# MAGIC if there is no FKEY needed, just land it to Seq File for the load job follows.
# MAGIC New PKEYs are generated in this transformer. A database sequence will be used to create next PKEY value.
# MAGIC 
# MAGIC IMPORTANT: Make sure to change the Database SEQUENCE Name to the corresponding table name.
# MAGIC Build primary key for  K_PROD_LMT_SUM_F.  
# MAGIC K table will be read to see if there is an SK already available for the Natural Keys. 
# MAGIC 
# MAGIC Job Name: IdsEdwProdLmtSumFPky
# MAGIC Remove Duplicates stage takes care of getting rid of duplicate rows with the same Natural key get into the PKEYing step.
# MAGIC A concious effort need to be made to evaluate which one of the duplicate set row to drop is a case by case basis decision.
# MAGIC Insert new SKEY rows generated as part of this run. This is a database insert operation.
# MAGIC Left Outer Join Here
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
EDWRunCycle = get_widget_value('EDWRunCycle','')

# --------------------------------------------------------------------------------
# Stage: db2_KProdLmtSumF_Read (DB2ConnectorPX)
# --------------------------------------------------------------------------------
jdbc_url, jdbc_props = get_db_config(edw_secret_name)
extract_query_db2_KProdLmtSumF_Read = (
    f"SELECT SRC_SYS_CD, PROD_ID, PROD_CMPNT_EFF_DT_SK, "
    f"CRT_RUN_CYC_EXCTN_DT_SK, PROD_LMT_SUM_SK, CRT_RUN_CYC_EXCTN_SK "
    f"FROM {EDWOwner}.K_PROD_LMT_SUM_F"
)
df_db2_KProdLmtSumF_Read = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_KProdLmtSumF_Read)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: ds_PROD_LMT_SUM_F_Extr (PxDataSet) - reading from .ds => read Parquet
# --------------------------------------------------------------------------------
df_ds_PROD_LMT_SUM_F_Extr = spark.read.parquet(f"{adls_path}/ds/PROD_LMT_SUM_F.parquet")

# --------------------------------------------------------------------------------
# Stage: cpy_MultiStreams (PxCopy)
# --------------------------------------------------------------------------------
df_cpy_MultiStreams_out1 = df_ds_PROD_LMT_SUM_F_Extr.select(
    F.col("SRC_SYS_CD"),
    F.col("PROD_ID"),
    F.col("PROD_CMPNT_EFF_DT_SK"),
    F.col("PROD_SK"),
    F.col("PROD_CMPNT_TERM_DT_SK"),
    F.col("DME_MAX_AMT"),
    F.col("IN_OUT_NTWK_LFTM_MAX_AMT"),
    F.col("IP_HOSP_CYM_COPAY_AMT"),
    F.col("INFRTL_LFTM_MAX_AMT"),
    F.col("KS_KS_MNTL_HLTH_IP_MAX_VST_AMT"),
    F.col("KS_KS_MNTL_HLTH_MAX_VST_AMT"),
    F.col("MHLTH_IP_IN_NTWK_CYM_COPAY_AMT"),
    F.col("OP_HOSP_CYM_COPAY_AMT"),
    F.col("OP_SPCH_THER_SVC_MAX_AMT"),
    F.col("PPO_KS_DIR_ENR_MHLTH_MAX_AMT"),
    F.col("RTN_MAX_AMT"),
    F.col("SBSTNC_ABUSE_IP_MAX_AMT"),
    F.col("SKILL_NURSE_CYM_AMT"),
    F.col("CHIRO_CYM_VST_CT"),
    F.col("COMBND_PT_OT_CYM_VST_CT"),
    F.col("DETOX_DAYS_MAX_VST_CT"),
    F.col("HOME_HLTH_CYM_VST_CT"),
    F.col("IP_HSPC_LFTM_MAX_VST_CT"),
    F.col("OP_REHAB_MAX_VST_CT"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

df_cpy_MultiStreams_out2 = df_ds_PROD_LMT_SUM_F_Extr.select(
    F.col("SRC_SYS_CD"),
    F.col("PROD_ID"),
    F.col("PROD_CMPNT_EFF_DT_SK")
)

# --------------------------------------------------------------------------------
# Stage: rdp_NaturalKeys (PxRemDup)
# --------------------------------------------------------------------------------
df_rdp_NaturalKeys = dedup_sort(
    df_cpy_MultiStreams_out2,
    ["SRC_SYS_CD", "PROD_ID", "PROD_CMPNT_EFF_DT_SK"],
    []
)

# --------------------------------------------------------------------------------
# Stage: jn_ProdLmtSumF (PxJoin) - leftouterjoin
# --------------------------------------------------------------------------------
df_jn_ProdLmtSumF_pre = (
    df_rdp_NaturalKeys.alias("lnkRemDupDataOut")
    .join(
        df_db2_KProdLmtSumF_Read.alias("lnkKProdLmtSumFIn"),
        [
            F.col("lnkRemDupDataOut.SRC_SYS_CD") == F.col("lnkKProdLmtSumFIn.SRC_SYS_CD"),
            F.col("lnkRemDupDataOut.PROD_ID") == F.col("lnkKProdLmtSumFIn.PROD_ID"),
            F.col("lnkRemDupDataOut.PROD_CMPNT_EFF_DT_SK") == F.col("lnkKProdLmtSumFIn.PROD_CMPNT_EFF_DT_SK"),
        ],
        "left"
    )
)
df_jn_ProdLmtSumF = df_jn_ProdLmtSumF_pre.select(
    F.col("lnkRemDupDataOut.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnkRemDupDataOut.PROD_ID").alias("PROD_ID"),
    F.col("lnkRemDupDataOut.PROD_CMPNT_EFF_DT_SK").alias("PROD_CMPNT_EFF_DT_SK"),
    F.col("lnkKProdLmtSumFIn.CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("lnkKProdLmtSumFIn.PROD_LMT_SUM_SK").alias("PROD_LMT_SUM_SK"),
    F.col("lnkKProdLmtSumFIn.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK")
)

# --------------------------------------------------------------------------------
# Stage: xfm_PKEYgen (CTransformerStage)
# --------------------------------------------------------------------------------
df_enriched = df_jn_ProdLmtSumF.withColumn(
    "svProdLmtSumFSK",
    F.when(F.col("PROD_LMT_SUM_SK").isNull(), F.lit(None)).otherwise(F.col("PROD_LMT_SUM_SK"))
)

df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"svProdLmtSumFSK",<schema>,<secret_name>)

df_xfm_PKEYgen_out_insert = df_enriched.filter(
    F.col("PROD_LMT_SUM_SK").isNull()
).select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PROD_ID").alias("PROD_ID"),
    F.col("PROD_CMPNT_EFF_DT_SK").alias("PROD_CMPNT_EFF_DT_SK"),
    F.lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("svProdLmtSumFSK").alias("PROD_LMT_SUM_SK"),
    F.lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK")
)

df_xfm_PKEYgen_out_upsert = df_enriched.select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PROD_ID").alias("PROD_ID"),
    F.col("PROD_CMPNT_EFF_DT_SK").alias("PROD_CMPNT_EFF_DT_SK"),
    F.when(
        F.col("PROD_LMT_SUM_SK").isNull(),
        F.lit(EDWRunCycleDate)
    ).otherwise(
        F.col("CRT_RUN_CYC_EXCTN_DT_SK")
    ).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("svProdLmtSumFSK").alias("PROD_LMT_SUM_SK"),
    F.when(
        F.col("PROD_LMT_SUM_SK").isNull(),
        F.lit(EDWRunCycle)
    ).otherwise(
        F.col("CRT_RUN_CYC_EXCTN_SK")
    ).alias("CRT_RUN_CYC_EXCTN_SK")
)

# --------------------------------------------------------------------------------
# Stage: db2_KProdLmtSumF_Load (DB2ConnectorPX) -- Merge/Upsert
# --------------------------------------------------------------------------------
temp_table_db2_KProdLmtSumF_Load = "STAGING.IdsEdwProdLmtSumFPky_db2_KProdLmtSumF_Load_temp"

execute_dml(
    f"DROP TABLE IF EXISTS {temp_table_db2_KProdLmtSumF_Load}",
    jdbc_url,
    jdbc_props
)

df_xfm_PKEYgen_out_insert.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", temp_table_db2_KProdLmtSumF_Load) \
    .mode("overwrite") \
    .save()

merge_sql_db2_KProdLmtSumF_Load = f"""
MERGE {EDWOwner}.K_PROD_LMT_SUM_F as T
USING {temp_table_db2_KProdLmtSumF_Load} as S
ON (
  T.SRC_SYS_CD = S.SRC_SYS_CD
  AND T.PROD_ID = S.PROD_ID
  AND T.PROD_CMPNT_EFF_DT_SK = S.PROD_CMPNT_EFF_DT_SK
)
WHEN MATCHED THEN
  UPDATE SET
    T.CRT_RUN_CYC_EXCTN_DT_SK = S.CRT_RUN_CYC_EXCTN_DT_SK,
    T.PROD_LMT_SUM_SK = S.PROD_LMT_SUM_SK,
    T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK
WHEN NOT MATCHED THEN
  INSERT (
    SRC_SYS_CD,
    PROD_ID,
    PROD_CMPNT_EFF_DT_SK,
    CRT_RUN_CYC_EXCTN_DT_SK,
    PROD_LMT_SUM_SK,
    CRT_RUN_CYC_EXCTN_SK
  )
  VALUES (
    S.SRC_SYS_CD,
    S.PROD_ID,
    S.PROD_CMPNT_EFF_DT_SK,
    S.CRT_RUN_CYC_EXCTN_DT_SK,
    S.PROD_LMT_SUM_SK,
    S.CRT_RUN_CYC_EXCTN_SK
  );
"""
execute_dml(merge_sql_db2_KProdLmtSumF_Load, jdbc_url, jdbc_props)

# --------------------------------------------------------------------------------
# Stage: jn_PKEYs (PxJoin) - leftouterjoin
# --------------------------------------------------------------------------------
df_jn_PKEYs_pre = (
    df_cpy_MultiStreams_out1.alias("lnkFullDataJnIn")
    .join(
        df_xfm_PKEYgen_out_upsert.alias("lnkPKEYxfmOut"),
        [
            F.col("lnkFullDataJnIn.SRC_SYS_CD") == F.col("lnkPKEYxfmOut.SRC_SYS_CD"),
            F.col("lnkFullDataJnIn.PROD_ID") == F.col("lnkPKEYxfmOut.PROD_ID"),
            F.col("lnkFullDataJnIn.PROD_CMPNT_EFF_DT_SK") == F.col("lnkPKEYxfmOut.PROD_CMPNT_EFF_DT_SK"),
        ],
        "left"
    )
)
df_jn_PKEYs = df_jn_PKEYs_pre.select(
    F.col("lnkPKEYxfmOut.PROD_LMT_SUM_SK").alias("PROD_LMT_SUM_SK"),
    F.col("lnkFullDataJnIn.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnkFullDataJnIn.PROD_ID").alias("PROD_ID"),
    F.col("lnkFullDataJnIn.PROD_CMPNT_EFF_DT_SK").alias("PROD_CMPNT_EFF_DT_SK"),
    F.col("lnkPKEYxfmOut.CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("lnkFullDataJnIn.LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("lnkFullDataJnIn.PROD_SK").alias("PROD_SK"),
    F.col("lnkFullDataJnIn.PROD_CMPNT_TERM_DT_SK").alias("PROD_CMPNT_TERM_DT_SK"),
    F.col("lnkFullDataJnIn.DME_MAX_AMT").alias("DME_MAX_AMT"),
    F.col("lnkFullDataJnIn.IN_OUT_NTWK_LFTM_MAX_AMT").alias("IN_OUT_NTWK_LFTM_MAX_AMT"),
    F.col("lnkFullDataJnIn.IP_HOSP_CYM_COPAY_AMT").alias("IP_HOSP_CYM_COPAY_AMT"),
    F.col("lnkFullDataJnIn.INFRTL_LFTM_MAX_AMT").alias("INFRTL_LFTM_MAX_AMT"),
    F.col("lnkFullDataJnIn.KS_KS_MNTL_HLTH_IP_MAX_VST_AMT").alias("KS_KS_MNTL_HLTH_IP_MAX_VST_AMT"),
    F.col("lnkFullDataJnIn.KS_KS_MNTL_HLTH_MAX_VST_AMT").alias("KS_KS_MNTL_HLTH_MAX_VST_AMT"),
    F.col("lnkFullDataJnIn.MHLTH_IP_IN_NTWK_CYM_COPAY_AMT").alias("MHLTH_IP_IN_NTWK_CYM_COPAY_AMT"),
    F.col("lnkFullDataJnIn.OP_HOSP_CYM_COPAY_AMT").alias("OP_HOSP_CYM_COPAY_AMT"),
    F.col("lnkFullDataJnIn.OP_SPCH_THER_SVC_MAX_AMT").alias("OP_SPCH_THER_SVC_MAX_AMT"),
    F.col("lnkFullDataJnIn.PPO_KS_DIR_ENR_MHLTH_MAX_AMT").alias("PPO_KS_DIR_ENR_MHLTH_MAX_AMT"),
    F.col("lnkFullDataJnIn.RTN_MAX_AMT").alias("RTN_MAX_AMT"),
    F.col("lnkFullDataJnIn.SBSTNC_ABUSE_IP_MAX_AMT").alias("SBSTNC_ABUSE_IP_MAX_AMT"),
    F.col("lnkFullDataJnIn.SKILL_NURSE_CYM_AMT").alias("SKILL_NURSE_CYM_AMT"),
    F.col("lnkFullDataJnIn.CHIRO_CYM_VST_CT").alias("CHIRO_CYM_VST_CT"),
    F.col("lnkFullDataJnIn.COMBND_PT_OT_CYM_VST_CT").alias("COMBND_PT_OT_CYM_VST_CT"),
    F.col("lnkFullDataJnIn.DETOX_DAYS_MAX_VST_CT").alias("DETOX_DAYS_MAX_VST_CT"),
    F.col("lnkFullDataJnIn.HOME_HLTH_CYM_VST_CT").alias("HOME_HLTH_CYM_VST_CT"),
    F.col("lnkFullDataJnIn.IP_HSPC_LFTM_MAX_VST_CT").alias("IP_HSPC_LFTM_MAX_VST_CT"),
    F.col("lnkFullDataJnIn.OP_REHAB_MAX_VST_CT").alias("OP_REHAB_MAX_VST_CT"),
    F.col("lnkPKEYxfmOut.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("lnkFullDataJnIn.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

df_jn_PKEYs_final = (
    df_jn_PKEYs
    .withColumn("PROD_CMPNT_EFF_DT_SK", F.rpad(F.col("PROD_CMPNT_EFF_DT_SK"), 10, " "))
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
    .withColumn("PROD_CMPNT_TERM_DT_SK", F.rpad(F.col("PROD_CMPNT_TERM_DT_SK"), 10, " "))
    .select(
        "PROD_LMT_SUM_SK",
        "SRC_SYS_CD",
        "PROD_ID",
        "PROD_CMPNT_EFF_DT_SK",
        "CRT_RUN_CYC_EXCTN_DT_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        "PROD_SK",
        "PROD_CMPNT_TERM_DT_SK",
        "DME_MAX_AMT",
        "IN_OUT_NTWK_LFTM_MAX_AMT",
        "IP_HOSP_CYM_COPAY_AMT",
        "INFRTL_LFTM_MAX_AMT",
        "KS_KS_MNTL_HLTH_IP_MAX_VST_AMT",
        "KS_KS_MNTL_HLTH_MAX_VST_AMT",
        "MHLTH_IP_IN_NTWK_CYM_COPAY_AMT",
        "OP_HOSP_CYM_COPAY_AMT",
        "OP_SPCH_THER_SVC_MAX_AMT",
        "PPO_KS_DIR_ENR_MHLTH_MAX_AMT",
        "RTN_MAX_AMT",
        "SBSTNC_ABUSE_IP_MAX_AMT",
        "SKILL_NURSE_CYM_AMT",
        "CHIRO_CYM_VST_CT",
        "COMBND_PT_OT_CYM_VST_CT",
        "DETOX_DAYS_MAX_VST_CT",
        "HOME_HLTH_CYM_VST_CT",
        "IP_HSPC_LFTM_MAX_VST_CT",
        "OP_REHAB_MAX_VST_CT",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK"
    )
)

# --------------------------------------------------------------------------------
# Stage: seq_PROD_LMT_SUM_F_PKey (PxSequentialFile)
# --------------------------------------------------------------------------------
write_files(
    df_jn_PKEYs_final,
    f"{adls_path}/load/PROD_LMT_SUM_F.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)