# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                         DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                             ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        ----------------------------------------------                        -------------------------------    ------------------------------       --------------------
# MAGIC Raj Mangalampally    08/08/2013             P5114                       Original Programming                                   EnterpriseWrhsDevl       Jag Yelavarthi                2013-10-18
# MAGIC                                                                                                     (Server to Parallel Conversion)

# MAGIC Write SUM_AUDIT_D.dat for the EdwEdwSumAuditDLoad Job
# MAGIC New PKEYs are generated in this transformer. A database sequence will be used to create next PKEY value.
# MAGIC This job will be used when there is a need to Assign primary keys to incoming data before loading into Target table.
# MAGIC 
# MAGIC Extract dataset is created in the extract job ran prior to this.
# MAGIC 
# MAGIC Every K table will need a dedicated Database SEQUENCE to provide and track new numbers to use.
# MAGIC 
# MAGIC New KEYS generated for each run will be Inserted into the K table.
# MAGIC 
# MAGIC Job Name: EdwEdwSumAuditDPkey
# MAGIC Remove Duplicates stage takes care of getting rid of duplicate rows with the same Natural key get into the PKEYing step.
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
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


# Parameters
EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
EDWRunCycle = get_widget_value('EDWRunCycle','')

# Read from EDW: db2_K_SUM_AUDIT_D_in
jdbc_url, jdbc_props = get_db_config(edw_secret_name)
extract_query_db2_K_SUM_AUDIT_D_in = (
    "SELECT ISNULL(SUB_SK,0) AS SUB_SK, "
    "MBR_SFX_NO, "
    "SRC_SYS_CRT_DT_SK, "
    "SRC_SYS_CRT_USER_ID, "
    "CRT_RUN_CYC_EXCTN_DT_SK, "
    "SUM_AUDIT_SK, "
    "CRT_RUN_CYC_EXCTN_SK "
    f"FROM {EDWOwner}.K_SUM_AUDIT_D"
)
df_db2_K_SUM_AUDIT_D_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_K_SUM_AUDIT_D_in)
    .load()
)

# Read from PxDataSet: ds_SUM_AUDIT_D_out (SUM_AUDIT_D.ds => SUM_AUDIT_D.parquet)
df_ds_SUM_AUDIT_D_out = spark.read.parquet(f"{adls_path}/ds/SUM_AUDIT_D.parquet")
df_ds_SUM_AUDIT_D_out = df_ds_SUM_AUDIT_D_out.select(
    "SUB_SK",
    "MBR_SFX_NO",
    "SRC_SYS_CRT_DT_SK",
    "SRC_SYS_CRT_USER_ID",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "GRP_SK",
    "MBR_SK",
    "DP_RATE_AUDIT_IN",
    "GRP_ID",
    "MBR_AUDIT_IN",
    "MBR_COB_AUDIT_IN",
    "MBR_ELIG_AUDIT_IN",
    "MBR_PCP_AUDIT_IN",
    "SUB_ADDR_AUDIT_IN",
    "SUB_AUDIT_IN",
    "SUB_CLS_AUDIT_IN",
    "SUB_ELIG_AUDIT_IN",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK"
)

# cpy_MultiStreams
df_cpy_MultiStreams_lnk_SumAuditData_L_in = df_ds_SUM_AUDIT_D_out.select(
    "SUB_SK",
    "MBR_SFX_NO",
    "SRC_SYS_CRT_DT_SK",
    "SRC_SYS_CRT_USER_ID",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "GRP_SK",
    "MBR_SK",
    "DP_RATE_AUDIT_IN",
    "GRP_ID",
    "MBR_AUDIT_IN",
    "MBR_COB_AUDIT_IN",
    "MBR_ELIG_AUDIT_IN",
    "MBR_PCP_AUDIT_IN",
    "SUB_ADDR_AUDIT_IN",
    "SUB_AUDIT_IN",
    "SUB_CLS_AUDIT_IN",
    "SUB_ELIG_AUDIT_IN",
    "LAST_UPDT_RUN_CYC_EXCTN_SK"
)
df_cpy_MultiStreams_lnk_cpy_NKEY = df_ds_SUM_AUDIT_D_out.select(
    "SUB_SK",
    "MBR_SFX_NO",
    "SRC_SYS_CRT_DT_SK",
    "SRC_SYS_CRT_USER_ID"
)

# rdp_NaturalKeys (PxRemDup) on df_cpy_MultiStreams_lnk_cpy_NKEY
df_rdp_NaturalKeys_out = dedup_sort(
    df_cpy_MultiStreams_lnk_cpy_NKEY,
    ["SUB_SK", "MBR_SFX_NO", "SRC_SYS_CRT_DT_SK", "SRC_SYS_CRT_USER_ID"],
    [("SUB_SK","A"), ("MBR_SFX_NO","A"), ("SRC_SYS_CRT_DT_SK","A"), ("SRC_SYS_CRT_USER_ID","A")]
)
# This corresponds to link lnk_SumAuditDData_L_in
df_rdp_NaturalKeys_lnk_SumAuditDData_L_in = df_rdp_NaturalKeys_out.select(
    "SUB_SK",
    "MBR_SFX_NO",
    "SRC_SYS_CRT_DT_SK",
    "SRC_SYS_CRT_USER_ID"
)

# jn_SumAuditF (left outer join)
df_jn_SumAuditF_out_temp = df_rdp_NaturalKeys_lnk_SumAuditDData_L_in.alias("A").join(
    df_db2_K_SUM_AUDIT_D_in.alias("B"),
    (
        (F.col("A.SUB_SK") == F.col("B.SUB_SK"))
        & (F.col("A.MBR_SFX_NO") == F.col("B.MBR_SFX_NO"))
        & (F.col("A.SRC_SYS_CRT_DT_SK") == F.col("B.SRC_SYS_CRT_DT_SK"))
        & (F.col("A.SRC_SYS_CRT_USER_ID") == F.col("B.SRC_SYS_CRT_USER_ID"))
    ),
    "left"
)
df_jn_SumAuditF_out = df_jn_SumAuditF_out_temp.select(
    F.col("A.SUB_SK").alias("SUB_SK"),
    F.col("A.MBR_SFX_NO").alias("MBR_SFX_NO"),
    F.col("A.SRC_SYS_CRT_DT_SK").alias("SRC_SYS_CRT_DT_SK"),
    F.col("A.SRC_SYS_CRT_USER_ID").alias("SRC_SYS_CRT_USER_ID"),
    F.col("B.CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("B.SUM_AUDIT_SK").alias("SUM_AUDIT_SK"),
    F.col("B.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK")
)

# xfm_PKEYgen
df_xfm_PKEYgen_in = df_jn_SumAuditF_out

# Capture original SUM_AUDIT_SK for conditional logic
df_enriched = df_xfm_PKEYgen_in.withColumn("ORIG_SUM_AUDIT_SK", F.col("SUM_AUDIT_SK")) \
                               .withColumn("ORIG_CRT_RUN_CYC_EXCTN_DT_SK", F.col("CRT_RUN_CYC_EXCTN_DT_SK"))

# Surrogate key generation for new SUM_AUDIT_SK if ORIG_SUM_AUDIT_SK is null
df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"SUM_AUDIT_SK",<schema>,<secret_name>)

# lnk_KSumAuditD_out (Constraint: IsNull(INCOMING.SUM_AUDIT_SK))
df_lnk_KSumAuditD_out_temp = df_enriched.filter(F.col("ORIG_SUM_AUDIT_SK").isNull())
df_lnk_KSumAuditD_out_temp = df_lnk_KSumAuditD_out_temp.withColumn(
    "MBR_SFX_NO",
    F.when(F.col("MBR_SFX_NO").isNull(), F.lit("00")).otherwise(F.col("MBR_SFX_NO"))
).withColumn(
    "CRT_RUN_CYC_EXCTN_DT_SK",
    F.lit(EDWRunCycleDate)
).withColumn(
    "CRT_RUN_CYC_EXCTN_SK",
    F.lit(EDWRunCycle)
)
df_lnk_KSumAuditD_out = df_lnk_KSumAuditD_out_temp.select(
    "SUB_SK",
    "MBR_SFX_NO",
    "SRC_SYS_CRT_DT_SK",
    "SRC_SYS_CRT_USER_ID",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "SUM_AUDIT_SK",
    "CRT_RUN_CYC_EXCTN_SK"
)

# lnk_Pkey_Out
df_lnk_Pkey_Out_temp = df_enriched.withColumn(
    "CRT_RUN_CYC_EXCTN_DT_SK",
    F.when(F.col("ORIG_SUM_AUDIT_SK").isNotNull(), F.col("ORIG_CRT_RUN_CYC_EXCTN_DT_SK")).otherwise(F.lit(EDWRunCycleDate))
).withColumn(
    "CRT_RUN_CYC_EXCTN_SK",
    F.when(F.col("ORIG_SUM_AUDIT_SK").isNull(), F.lit(EDWRunCycle)).otherwise(F.col("CRT_RUN_CYC_EXCTN_SK"))
)
df_lnk_Pkey_Out = df_lnk_Pkey_Out_temp.select(
    "SUB_SK",
    "MBR_SFX_NO",
    "SRC_SYS_CRT_DT_SK",
    "SRC_SYS_CRT_USER_ID",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "SUM_AUDIT_SK",
    "CRT_RUN_CYC_EXCTN_SK"
)

# db2_K_SUM_AUDIT_D_Load (Insert => replicate logic as a merge, with do-nothing update)
# Write df_lnk_KSumAuditD_out to STAGING temporary table, then merge
temp_table_name = "STAGING.EdwEdwSumAuditDPkey_db2_K_SUM_AUDIT_D_Load_temp"
drop_temp_sql = f"DROP TABLE IF EXISTS {temp_table_name}"
execute_dml(drop_temp_sql, jdbc_url, jdbc_props)

(
    df_lnk_KSumAuditD_out.write
    .format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("dbtable", temp_table_name)
    .mode("overwrite")
    .save()
)

merge_sql = f"""
MERGE INTO {EDWOwner}.K_SUM_AUDIT_D AS T
USING {temp_table_name} AS S
ON 
    T.SUB_SK = S.SUB_SK
    AND T.MBR_SFX_NO = S.MBR_SFX_NO
    AND T.SRC_SYS_CRT_DT_SK = S.SRC_SYS_CRT_DT_SK
    AND T.SRC_SYS_CRT_USER_ID = S.SRC_SYS_CRT_USER_ID
WHEN MATCHED THEN
  UPDATE SET 
    T.SUB_SK = T.SUB_SK,
    T.MBR_SFX_NO = T.MBR_SFX_NO,
    T.SRC_SYS_CRT_DT_SK = T.SRC_SYS_CRT_DT_SK,
    T.SRC_SYS_CRT_USER_ID = T.SRC_SYS_CRT_USER_ID,
    T.CRT_RUN_CYC_EXCTN_DT_SK = T.CRT_RUN_CYC_EXCTN_DT_SK,
    T.SUM_AUDIT_SK = T.SUM_AUDIT_SK,
    T.CRT_RUN_CYC_EXCTN_SK = T.CRT_RUN_CYC_EXCTN_SK
WHEN NOT MATCHED THEN
  INSERT (
    SUB_SK,
    MBR_SFX_NO,
    SRC_SYS_CRT_DT_SK,
    SRC_SYS_CRT_USER_ID,
    CRT_RUN_CYC_EXCTN_DT_SK,
    SUM_AUDIT_SK,
    CRT_RUN_CYC_EXCTN_SK
  )
  VALUES (
    S.SUB_SK,
    S.MBR_SFX_NO,
    S.SRC_SYS_CRT_DT_SK,
    S.SRC_SYS_CRT_USER_ID,
    S.CRT_RUN_CYC_EXCTN_DT_SK,
    S.SUM_AUDIT_SK,
    S.CRT_RUN_CYC_EXCTN_SK
  );
"""
execute_dml(merge_sql, jdbc_url, jdbc_props)

# jn_Pkey (left outer join)
df_jn_Pkey_out_temp = df_cpy_MultiStreams_lnk_SumAuditData_L_in.alias("A").join(
    df_lnk_Pkey_Out.alias("B"),
    (
        (F.col("A.SUB_SK") == F.col("B.SUB_SK"))
        & (F.col("A.MBR_SFX_NO") == F.col("B.MBR_SFX_NO"))
        & (F.col("A.SRC_SYS_CRT_DT_SK") == F.col("B.SRC_SYS_CRT_DT_SK"))
        & (F.col("A.SRC_SYS_CRT_USER_ID") == F.col("B.SRC_SYS_CRT_USER_ID"))
    ),
    "left"
)
df_jn_Pkey_out = df_jn_Pkey_out_temp.select(
    F.col("B.SUM_AUDIT_SK").alias("SUM_AUDIT_SK"),
    F.col("A.SUB_SK").alias("SUB_SK"),
    F.col("A.MBR_SFX_NO").alias("MBR_SFX_NO"),
    F.col("A.SRC_SYS_CRT_DT_SK").alias("SRC_SYS_CRT_DT_SK"),
    F.col("A.SRC_SYS_CRT_USER_ID").alias("SRC_SYS_CRT_USER_ID"),
    F.col("B.CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("A.LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("A.GRP_SK").alias("GRP_SK"),
    F.col("A.MBR_SK").alias("MBR_SK"),
    F.col("A.DP_RATE_AUDIT_IN").alias("DP_RATE_AUDIT_IN"),
    F.col("A.GRP_ID").alias("GRP_ID"),
    F.col("A.MBR_AUDIT_IN").alias("MBR_AUDIT_IN"),
    F.col("A.MBR_COB_AUDIT_IN").alias("MBR_COB_AUDIT_IN"),
    F.col("A.MBR_ELIG_AUDIT_IN").alias("MBR_ELIG_AUDIT_IN"),
    F.col("A.MBR_PCP_AUDIT_IN").alias("MBR_PCP_AUDIT_IN"),
    F.col("A.SUB_ADDR_AUDIT_IN").alias("SUB_ADDR_AUDIT_IN"),
    F.col("A.SUB_AUDIT_IN").alias("SUB_AUDIT_IN"),
    F.col("A.SUB_CLS_AUDIT_IN").alias("SUB_CLS_AUDIT_IN"),
    F.col("A.SUB_ELIG_AUDIT_IN").alias("SUB_ELIG_AUDIT_IN"),
    F.col("B.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("A.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

# seq_SUM_AUDIT_D_csv_out (final write to SUM_AUDIT_D.dat)
# Apply rpad for char columns before final select
df_final = df_jn_Pkey_out.withColumn(
    "SRC_SYS_CRT_DT_SK",
    F.rpad(F.col("SRC_SYS_CRT_DT_SK"), 10, " ")
).withColumn(
    "CRT_RUN_CYC_EXCTN_DT_SK",
    F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ")
).withColumn(
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ")
).withColumn(
    "DP_RATE_AUDIT_IN",
    F.rpad(F.col("DP_RATE_AUDIT_IN"), 1, " ")
).withColumn(
    "MBR_AUDIT_IN",
    F.rpad(F.col("MBR_AUDIT_IN"), 1, " ")
).withColumn(
    "MBR_COB_AUDIT_IN",
    F.rpad(F.col("MBR_COB_AUDIT_IN"), 1, " ")
).withColumn(
    "MBR_ELIG_AUDIT_IN",
    F.rpad(F.col("MBR_ELIG_AUDIT_IN"), 1, " ")
).withColumn(
    "MBR_PCP_AUDIT_IN",
    F.rpad(F.col("MBR_PCP_AUDIT_IN"), 1, " ")
).withColumn(
    "SUB_ADDR_AUDIT_IN",
    F.rpad(F.col("SUB_ADDR_AUDIT_IN"), 1, " ")
).withColumn(
    "SUB_AUDIT_IN",
    F.rpad(F.col("SUB_AUDIT_IN"), 1, " ")
).withColumn(
    "SUB_CLS_AUDIT_IN",
    F.rpad(F.col("SUB_CLS_AUDIT_IN"), 1, " ")
).withColumn(
    "SUB_ELIG_AUDIT_IN",
    F.rpad(F.col("SUB_ELIG_AUDIT_IN"), 1, " ")
)

df_final = df_final.select(
    "SUM_AUDIT_SK",
    "SUB_SK",
    "MBR_SFX_NO",
    "SRC_SYS_CRT_DT_SK",
    "SRC_SYS_CRT_USER_ID",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "GRP_SK",
    "MBR_SK",
    "DP_RATE_AUDIT_IN",
    "GRP_ID",
    "MBR_AUDIT_IN",
    "MBR_COB_AUDIT_IN",
    "MBR_ELIG_AUDIT_IN",
    "MBR_PCP_AUDIT_IN",
    "SUB_ADDR_AUDIT_IN",
    "SUB_AUDIT_IN",
    "SUB_CLS_AUDIT_IN",
    "SUB_ELIG_AUDIT_IN",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK"
)

write_files(
    df_final,
    f"{adls_path}/load/SUM_AUDIT_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)