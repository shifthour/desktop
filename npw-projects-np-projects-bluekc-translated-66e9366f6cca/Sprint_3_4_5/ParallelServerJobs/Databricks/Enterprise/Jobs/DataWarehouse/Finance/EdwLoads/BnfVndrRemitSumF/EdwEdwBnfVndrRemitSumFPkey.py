# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                         DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                             ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        ----------------------------------------------                        -------------------------------    ------------------------------       --------------------
# MAGIC Raj Mangalampally    08/08/2013             P5114                       Original Programming                                   EnterpriseWrhsDevl    Peter Marshall               12/10/2013
# MAGIC                                                                                                     (Server to Parallel Conversion)

# MAGIC Write BNF_VNDR_REMIT_SUM_F.dat for the EdwEdwBnfVndrRemitSumFLoad Job
# MAGIC New PKEYs are generated in this transformer. A database sequence will be used to create next PKEY value.
# MAGIC This job will be used when there is a need to Assign primary keys to incoming data before loading into Target table.
# MAGIC 
# MAGIC Extract dataset is created in the extract job ran prior to this.
# MAGIC 
# MAGIC New KEYS generated for each run will be Inserted into the K table.
# MAGIC 
# MAGIC Job Name: EdwEdwBnfSumRemitSumFPkey
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
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType, StructType, StructField
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


# MAGIC %run ../../../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

# Retrieve required parameter values
EDWOwner = get_widget_value("EDWOwner","")
EDWRunCycleDate = get_widget_value("EDWRunCycleDate","")
CurrRunCycle = get_widget_value("CurrRunCycle","")
edw_secret_name = get_widget_value("edw_secret_name","")

# --------------------------------------------------------------------------------
# Stage: db2_K_BNF_VNDR_REMIT_SUM_F_in  (DB2ConnectorPX)
# --------------------------------------------------------------------------------
jdbc_url_db2_K_BNF_VNDR_REMIT_SUM_F_in, jdbc_props_db2_K_BNF_VNDR_REMIT_SUM_F_in = get_db_config(edw_secret_name)
extract_query_db2_K_BNF_VNDR_REMIT_SUM_F_in = """SELECT 
SRC_SYS_CD, 
BNF_VNDR_ID,
BNF_SUM_DTL_TYP_CD,
BNF_VNDR_REMIT_COV_YR_MO,
BNF_VNDR_REMIT_SUM_SK
FROM
{}.".K_BNF_VNDR_REMIT_SUM_F";""".format(EDWOwner)
df_db2_K_BNF_VNDR_REMIT_SUM_F_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_K_BNF_VNDR_REMIT_SUM_F_in)
    .options(**jdbc_props_db2_K_BNF_VNDR_REMIT_SUM_F_in)
    .option("query", extract_query_db2_K_BNF_VNDR_REMIT_SUM_F_in)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: ds_BNF_VNDR_REMIT_SUM_F_out (PxDataSet) - reading from .ds as Parquet
# --------------------------------------------------------------------------------
df_ds_BNF_VNDR_REMIT_SUM_F_out = spark.read.parquet(f"{adls_path}/ds/BNF_VNDR_REMIT_SUM_F.parquet")

# --------------------------------------------------------------------------------
# Stage: cpy_MultiStreams (PxCopy)
# --------------------------------------------------------------------------------
# First output link: lnk_BnfVndrRemitSumFData_L_in
df_cpy_MultiStreams_lnk_BnfVndrRemitSumFData_L_in = df_ds_BNF_VNDR_REMIT_SUM_F_out.select(
    F.col("BNF_VNDR_REMIT_SUM_SK"),
    F.col("SRC_SYS_CD"),
    F.col("BNF_VNDR_ID"),
    F.col("BNF_SUM_DTL_TYP_CD"),
    F.col("BNF_VNDR_REMIT_COV_YR_MO").alias("BNF_VNDR_REMIT_COV_YR_MO"),
    F.col("CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("BNF_VNDR_SK"),
    F.col("BNF_VNDR_REMIT_AMT"),
    F.col("BNF_VNDR_REMIT_RATE"),
    F.col("BNF_VNDR_MBR_CT"),
    F.col("BNF_VNDR_NM"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("BNF_SUM_DTL_TYP_CD_SK")
)

# Second output link: lnk_cpy_NKEY
df_cpy_MultiStreams_lnk_cpy_NKEY = df_ds_BNF_VNDR_REMIT_SUM_F_out.select(
    F.col("SRC_SYS_CD"),
    F.col("BNF_VNDR_ID"),
    F.col("BNF_SUM_DTL_TYP_CD"),
    F.col("BNF_VNDR_REMIT_COV_YR_MO").alias("BNF_VNDR_REMIT_COV_YR_MO")
)

# --------------------------------------------------------------------------------
# Stage: rdp_NaturalKeys (PxRemDup)
# --------------------------------------------------------------------------------
df_rdp_NaturalKeys = dedup_sort(
    df_cpy_MultiStreams_lnk_cpy_NKEY,
    partition_cols=["SRC_SYS_CD", "BNF_VNDR_ID", "BNF_SUM_DTL_TYP_CD", "BNF_VNDR_REMIT_COV_YR_MO"],
    sort_cols=[]
)

# --------------------------------------------------------------------------------
# Stage: jn_BnfVndrRemitSumF (PxJoin) - Left Outer Join
# --------------------------------------------------------------------------------
df_jn_BnfVndrRemitSumF = (
    df_rdp_NaturalKeys.alias("lnk_BnfVndrRemitSumFData_L_in")
    .join(
        df_db2_K_BNF_VNDR_REMIT_SUM_F_in.alias("lnk_KBnfVndrRemitSumFPkey_out"),
        on=[
            F.col("lnk_BnfVndrRemitSumFData_L_in.SRC_SYS_CD") == F.col("lnk_KBnfVndrRemitSumFPkey_out.SRC_SYS_CD"),
            F.col("lnk_BnfVndrRemitSumFData_L_in.BNF_VNDR_ID") == F.col("lnk_KBnfVndrRemitSumFPkey_out.BNF_VNDR_ID"),
            F.col("lnk_BnfVndrRemitSumFData_L_in.BNF_SUM_DTL_TYP_CD") == F.col("lnk_KBnfVndrRemitSumFPkey_out.BNF_SUM_DTL_TYP_CD"),
            F.col("lnk_BnfVndrRemitSumFData_L_in.BNF_VNDR_REMIT_COV_YR_MO") == F.col("lnk_KBnfVndrRemitSumFPkey_out.BNF_VNDR_REMIT_COV_YR_MO")
        ],
        how="left"
    )
)

df_jn_BnfVndrRemitSumF = df_jn_BnfVndrRemitSumF.select(
    F.col("lnk_BnfVndrRemitSumFData_L_in.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnk_BnfVndrRemitSumFData_L_in.BNF_VNDR_ID").alias("BNF_VNDR_ID"),
    F.col("lnk_BnfVndrRemitSumFData_L_in.BNF_SUM_DTL_TYP_CD").alias("BNF_SUM_DTL_TYP_CD"),
    F.col("lnk_BnfVndrRemitSumFData_L_in.BNF_VNDR_REMIT_COV_YR_MO").alias("BNF_VNDR_REMIT_COV_YR_MO"),
    F.col("lnk_KBnfVndrRemitSumFPkey_out.BNF_VNDR_REMIT_SUM_SK").alias("BNF_VNDR_REMIT_SUM_SK")
)

# --------------------------------------------------------------------------------
# Stage: xfm_PKEYgen (CTransformerStage) - Surrogate Key assignment and link splitting
# --------------------------------------------------------------------------------
# Prepare for SurrogateKeyGen by preserving original Surrogate Key column
df_temp_xfm_PKEYgen = df_jn_BnfVndrRemitSumF.withColumn("BNF_VNDR_REMIT_SUM_SK_orig", F.col("BNF_VNDR_REMIT_SUM_SK"))

# Apply SurrogateKeyGen to fill missing BNF_VNDR_REMIT_SUM_SK
df_enriched = df_temp_xfm_PKEYgen
df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"BNF_VNDR_REMIT_SUM_SK",<schema>,<secret_name>)

# lnk_KBnfVndrRemitSumF_out (rows where original key was null)
df_lnk_KBnfVndrRemitSumF_out = df_enriched.filter(
    F.col("BNF_VNDR_REMIT_SUM_SK_orig").isNull()
).select(
    F.col("SRC_SYS_CD"),
    F.col("BNF_VNDR_ID"),
    F.col("BNF_SUM_DTL_TYP_CD"),
    F.rpad(F.col("BNF_VNDR_REMIT_COV_YR_MO"), 6, " ").alias("BNF_VNDR_REMIT_COV_YR_MO"),
    F.rpad(F.lit(EDWRunCycleDate), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("BNF_VNDR_REMIT_SUM_SK").alias("BNF_VNDR_REMIT_SUM_SK"),
    F.lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK")
)

# lnk_Pkey_Out (all rows, with possibly updated Surrogate Key)
df_lnk_Pkey_Out = df_enriched.select(
    F.col("SRC_SYS_CD"),
    F.col("BNF_VNDR_ID"),
    F.col("BNF_SUM_DTL_TYP_CD"),
    F.rpad(F.col("BNF_VNDR_REMIT_COV_YR_MO"), 6, " ").alias("BNF_VNDR_REMIT_COV_YR_MO"),
    F.col("BNF_VNDR_REMIT_SUM_SK")
)

# --------------------------------------------------------------------------------
# Stage: db2_K_BNF_VNDR_REMIT_SUM_F_Load (DB2ConnectorPX) - MERGE logic
# --------------------------------------------------------------------------------
jdbc_url_db2_K_BNF_VNDR_REMIT_SUM_F_Load, jdbc_props_db2_K_BNF_VNDR_REMIT_SUM_F_Load = get_db_config(edw_secret_name)
execute_dml(
    "DROP TABLE IF EXISTS STAGING.EdwEdwBnfVndrRemitSumFPkey_db2_K_BNF_VNDR_REMIT_SUM_F_Load_temp",
    jdbc_url_db2_K_BNF_VNDR_REMIT_SUM_F_Load,
    jdbc_props_db2_K_BNF_VNDR_REMIT_SUM_F_Load
)
df_lnk_KBnfVndrRemitSumF_out.write \
    .format("jdbc") \
    .option("url", jdbc_url_db2_K_BNF_VNDR_REMIT_SUM_F_Load) \
    .options(**jdbc_props_db2_K_BNF_VNDR_REMIT_SUM_F_Load) \
    .option("dbtable", "STAGING.EdwEdwBnfVndrRemitSumFPkey_db2_K_BNF_VNDR_REMIT_SUM_F_Load_temp") \
    .mode("overwrite") \
    .save()

merge_sql_db2_K_BNF_VNDR_REMIT_SUM_F_Load = f"""
MERGE INTO {EDWOwner}.K_BNF_VNDR_REMIT_SUM_F AS T
USING STAGING.EdwEdwBnfVndrRemitSumFPkey_db2_K_BNF_VNDR_REMIT_SUM_F_Load_temp AS S
ON 
  T.SRC_SYS_CD = S.SRC_SYS_CD
  AND T.BNF_VNDR_ID = S.BNF_VNDR_ID
  AND T.BNF_SUM_DTL_TYP_CD = S.BNF_SUM_DTL_TYP_CD
  AND T.BNF_VNDR_REMIT_COV_YR_MO = S.BNF_VNDR_REMIT_COV_YR_MO
WHEN MATCHED THEN UPDATE SET
  T.CRT_RUN_CYC_EXCTN_DT_SK = S.CRT_RUN_CYC_EXCTN_DT_SK,
  T.BNF_VNDR_REMIT_SUM_SK = S.BNF_VNDR_REMIT_SUM_SK,
  T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK
WHEN NOT MATCHED THEN INSERT
(
  SRC_SYS_CD,
  BNF_VNDR_ID,
  BNF_SUM_DTL_TYP_CD,
  BNF_VNDR_REMIT_COV_YR_MO,
  CRT_RUN_CYC_EXCTN_DT_SK,
  BNF_VNDR_REMIT_SUM_SK,
  CRT_RUN_CYC_EXCTN_SK
)
VALUES
(
  S.SRC_SYS_CD,
  S.BNF_VNDR_ID,
  S.BNF_SUM_DTL_TYP_CD,
  S.BNF_VNDR_REMIT_COV_YR_MO,
  S.CRT_RUN_CYC_EXCTN_DT_SK,
  S.BNF_VNDR_REMIT_SUM_SK,
  S.CRT_RUN_CYC_EXCTN_SK
);
"""
execute_dml(merge_sql_db2_K_BNF_VNDR_REMIT_SUM_F_Load, jdbc_url_db2_K_BNF_VNDR_REMIT_SUM_F_Load, jdbc_props_db2_K_BNF_VNDR_REMIT_SUM_F_Load)

# --------------------------------------------------------------------------------
# Stage: jn_Pkey (PxJoin) - Left Outer Join
# --------------------------------------------------------------------------------
df_jn_Pkey = (
    df_cpy_MultiStreams_lnk_BnfVndrRemitSumFData_L_in.alias("lnk_BnfVndrRemitSumFData_L_in")
    .join(
        df_lnk_Pkey_Out.alias("lnk_Pkey_Out"),
        on=[
            F.col("lnk_BnfVndrRemitSumFData_L_in.SRC_SYS_CD") == F.col("lnk_Pkey_Out.SRC_SYS_CD"),
            F.col("lnk_BnfVndrRemitSumFData_L_in.BNF_VNDR_ID") == F.col("lnk_Pkey_Out.BNF_VNDR_ID"),
            F.col("lnk_BnfVndrRemitSumFData_L_in.BNF_SUM_DTL_TYP_CD") == F.col("lnk_Pkey_Out.BNF_SUM_DTL_TYP_CD"),
            F.col("lnk_BnfVndrRemitSumFData_L_in.BNF_VNDR_REMIT_COV_YR_MO") == F.col("lnk_Pkey_Out.BNF_VNDR_REMIT_COV_YR_MO")
        ],
        how="left"
    )
)

df_jn_Pkey = df_jn_Pkey.select(
    F.col("lnk_Pkey_Out.BNF_VNDR_REMIT_SUM_SK").alias("BNF_VNDR_REMIT_SUM_SK"),
    F.col("lnk_BnfVndrRemitSumFData_L_in.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnk_BnfVndrRemitSumFData_L_in.BNF_VNDR_ID").alias("BNF_VNDR_ID"),
    F.col("lnk_BnfVndrRemitSumFData_L_in.BNF_SUM_DTL_TYP_CD").alias("BNF_SUM_DTL_TYP_CD"),
    F.col("lnk_BnfVndrRemitSumFData_L_in.BNF_VNDR_REMIT_COV_YR_MO").alias("BNF_VNDR_REMIT_COV_YR_MO"),
    F.col("lnk_BnfVndrRemitSumFData_L_in.CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("lnk_BnfVndrRemitSumFData_L_in.LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("lnk_BnfVndrRemitSumFData_L_in.BNF_VNDR_SK").alias("BNF_VNDR_SK"),
    F.col("lnk_BnfVndrRemitSumFData_L_in.BNF_VNDR_REMIT_AMT").alias("BNF_VNDR_REMIT_AMT"),
    F.col("lnk_BnfVndrRemitSumFData_L_in.BNF_VNDR_REMIT_RATE").alias("BNF_VNDR_REMIT_RATE"),
    F.col("lnk_BnfVndrRemitSumFData_L_in.BNF_VNDR_MBR_CT").alias("BNF_VNDR_MBR_CT"),
    F.col("lnk_BnfVndrRemitSumFData_L_in.BNF_VNDR_NM").alias("BNF_VNDR_NM"),
    F.col("lnk_BnfVndrRemitSumFData_L_in.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("lnk_BnfVndrRemitSumFData_L_in.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("lnk_BnfVndrRemitSumFData_L_in.BNF_SUM_DTL_TYP_CD_SK").alias("BNF_SUM_DTL_TYP_CD_SK")
)

# --------------------------------------------------------------------------------
# Stage: seq_BNF_VNDR_REMIT_SUM_F_csv_out (PxSequentialFile)
# --------------------------------------------------------------------------------
df_seq_BNF_VNDR_REMIT_SUM_F_csv_out = df_jn_Pkey.select(
    F.col("BNF_VNDR_REMIT_SUM_SK"),
    F.col("SRC_SYS_CD"),
    F.col("BNF_VNDR_ID"),
    F.col("BNF_SUM_DTL_TYP_CD"),
    F.rpad(F.col("BNF_VNDR_REMIT_COV_YR_MO"), 6, " ").alias("BNF_VNDR_REMIT_COV_YR_MO"),
    F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("BNF_VNDR_SK"),
    F.col("BNF_VNDR_REMIT_AMT"),
    F.col("BNF_VNDR_REMIT_RATE"),
    F.col("BNF_VNDR_MBR_CT"),
    F.col("BNF_VNDR_NM"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("BNF_SUM_DTL_TYP_CD_SK")
)

write_files(
    df_seq_BNF_VNDR_REMIT_SUM_F_csv_out,
    f"{adls_path}/load/BNF_VNDR_REMIT_SUM_F.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)