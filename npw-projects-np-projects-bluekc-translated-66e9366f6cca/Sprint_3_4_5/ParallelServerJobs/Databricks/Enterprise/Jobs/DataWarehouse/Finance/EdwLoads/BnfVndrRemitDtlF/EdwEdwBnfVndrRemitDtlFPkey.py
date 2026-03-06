# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                         DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                             ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        ----------------------------------------------                        -------------------------------    ------------------------------       --------------------
# MAGIC Raj Mangalampally    08/26/2013             P5114                       Original Programming                                   EnterpriseWrhsDevl    Peter Marshall               12/10/2013
# MAGIC                                                                                                     (Server to Parallel Conversion)

# MAGIC Write BNF_VNDR_REMIT_DTL_F.dat for the EdwEdwBnfVndrRemitDtlFLoad Job
# MAGIC New PKEYs are generated in this transformer. A database sequence will be used to create next PKEY value.
# MAGIC This job will be used when there is a need to Assign primary keys to incoming data before loading into Target table.
# MAGIC 
# MAGIC Extract dataset is created in the extract job ran prior to this.
# MAGIC 
# MAGIC New KEYS generated for each run will be Inserted into the K table.
# MAGIC 
# MAGIC Job Name: EdwEdwBnfSumRemitDtlFPkey
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
from pyspark.sql.functions import col, lit, when, rpad
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


# Retrieve parameter values
EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
CurrRunCycle = get_widget_value('CurrRunCycle','')

# Read from EDW database (db2_K_BNF_VNDR_REMIT_DTL_F_in)
jdbc_url, jdbc_props = get_db_config(edw_secret_name)
extract_query_db2_K_BNF_VNDR_REMIT_DTL_F_in = (
    f"SELECT SRC_SYS_CD, BNF_VNDR_ID, BNF_SUM_DTL_TYP_CD, BNF_VNDR_REMIT_COV_YR_MO, MBR_UNIQ_KEY, BNF_VNDR_REMIT_DTL_SK "
    f"FROM {EDWOwner}.K_BNF_VNDR_REMIT_DTL_F"
)
df_db2_K_BNF_VNDR_REMIT_DTL_F_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_K_BNF_VNDR_REMIT_DTL_F_in)
    .load()
)

# Read dataset as parquet (ds_BNF_VNDR_REMIT_DTL_F_out)
df_ds_BNF_VNDR_REMIT_DTL_F_out = spark.read.parquet(f"{adls_path}/ds/BNF_VNDR_REMIT_DTL_F.parquet")
df_ds_BNF_VNDR_REMIT_DTL_F_out = df_ds_BNF_VNDR_REMIT_DTL_F_out.select(
    "BNF_VNDR_REMIT_DTL_SK",
    "SRC_SYS_CD",
    "BNF_VNDR_ID",
    "BNF_SUM_DTL_TYP_CD",
    "BNF_VNDR_REMIT_COV_YR_MO",
    "MBR_UNIQ_KEY",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "BNF_VNDR_REMIT_SUM_SK",
    "MBR_SK",
    "MBR_ORIG_CT_SK",
    "MBR_HOME_ADDR_ZIP_CD_5",
    "PROD_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "BNF_SUM_DTL_TYP_CD_SK"
)

# cpy_MultiStreams: create two outputs
df_cpy_MultiStreams_out1 = df_ds_BNF_VNDR_REMIT_DTL_F_out.select(
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("BNF_VNDR_ID").alias("BNF_VNDR_ID"),
    col("BNF_SUM_DTL_TYP_CD").alias("BNF_SUM_DTL_TYP_CD"),
    col("BNF_VNDR_REMIT_COV_YR_MO").alias("BNF_VNDR_REMIT_COV_YR_MO"),
    col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    col("CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("BNF_VNDR_REMIT_SUM_SK").alias("BNF_VNDR_REMIT_SUM_SK"),
    col("MBR_SK").alias("MBR_SK"),
    col("MBR_ORIG_CT_SK").alias("MBR_ORIG_CT_SK"),
    col("MBR_HOME_ADDR_ZIP_CD_5").alias("MBR_HOME_ADDR_ZIP_CD_5"),
    col("PROD_ID").alias("PROD_ID"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("BNF_SUM_DTL_TYP_CD_SK").alias("BNF_SUM_DTL_TYP_CD_SK")
)

df_cpy_MultiStreams_out2 = df_ds_BNF_VNDR_REMIT_DTL_F_out.select(
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("BNF_VNDR_ID").alias("BNF_VNDR_ID"),
    col("BNF_SUM_DTL_TYP_CD").alias("BNF_SUM_DTL_TYP_CD"),
    col("BNF_VNDR_REMIT_COV_YR_MO").alias("BNF_VNDR_REMIT_COV_YR_MO"),
    col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY")
)

# rdp_NaturalKeys (PxRemDup)
df_rdp_NaturalKeys = dedup_sort(
    df_cpy_MultiStreams_out2,
    partition_cols=["SRC_SYS_CD","BNF_VNDR_ID","BNF_SUM_DTL_TYP_CD","BNF_VNDR_REMIT_COV_YR_MO","MBR_UNIQ_KEY"],
    sort_cols=[]
)
df_rdp_NaturalKeys = df_rdp_NaturalKeys.select(
    "SRC_SYS_CD",
    "BNF_VNDR_ID",
    "BNF_SUM_DTL_TYP_CD",
    "BNF_VNDR_REMIT_COV_YR_MO",
    "MBR_UNIQ_KEY"
)

# jn_BnfVndrRemitDtlF (left outer join)
df_jn_BnfVndrRemitDtlF = df_rdp_NaturalKeys.alias("l").join(
    df_db2_K_BNF_VNDR_REMIT_DTL_F_in.alias("r"),
    on=[
        col("l.SRC_SYS_CD")==col("r.SRC_SYS_CD"),
        col("l.BNF_VNDR_ID")==col("r.BNF_VNDR_ID"),
        col("l.BNF_SUM_DTL_TYP_CD")==col("r.BNF_SUM_DTL_TYP_CD"),
        col("l.BNF_VNDR_REMIT_COV_YR_MO")==col("r.BNF_VNDR_REMIT_COV_YR_MO"),
        col("l.MBR_UNIQ_KEY")==col("r.MBR_UNIQ_KEY")
    ],
    how="left"
).select(
    col("l.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("l.BNF_VNDR_ID").alias("BNF_VNDR_ID"),
    col("l.BNF_SUM_DTL_TYP_CD").alias("BNF_SUM_DTL_TYP_CD"),
    col("l.BNF_VNDR_REMIT_COV_YR_MO").alias("BNF_VNDR_REMIT_COV_YR_MO"),
    col("l.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    col("r.BNF_VNDR_REMIT_DTL_SK").alias("BNF_VNDR_REMIT_DTL_SK")
)

# xfm_PKEYgen
df_xfm_PKEYgen_insert = df_jn_BnfVndrRemitDtlF.filter(col("BNF_VNDR_REMIT_DTL_SK").isNull())
df_xfm_PKEYgen_insert = SurrogateKeyGen(df_xfm_PKEYgen_insert,<DB sequence name>,"BNF_VNDR_REMIT_DTL_SK",<schema>,<secret_name>)
df_xfm_PKEYgen_insert = df_xfm_PKEYgen_insert.withColumn("CRT_RUN_CYC_EXCTN_DT_SK", lit(EDWRunCycleDate))
df_xfm_PKEYgen_insert = df_xfm_PKEYgen_insert.withColumn("CRT_RUN_CYC_EXCTN_SK", lit(CurrRunCycle))

df_xfm_PKEYgen_else = df_jn_BnfVndrRemitDtlF.filter(col("BNF_VNDR_REMIT_DTL_SK").isNotNull())

df_xfm_PKEYgen_all = df_xfm_PKEYgen_insert.unionByName(df_xfm_PKEYgen_else, allowMissingColumns=True)

# lnk_Pkey_Out columns
df_xfm_PKEYgen_out = df_xfm_PKEYgen_all.select(
    "SRC_SYS_CD",
    "BNF_VNDR_ID",
    "BNF_SUM_DTL_TYP_CD",
    "BNF_VNDR_REMIT_COV_YR_MO",
    "MBR_UNIQ_KEY",
    "BNF_VNDR_REMIT_DTL_SK"
)

# Prepare dataframe for DB load link (only newly inserted)
df_xfm_PKEYgen_toDB = df_xfm_PKEYgen_insert.select(
    "SRC_SYS_CD",
    "BNF_VNDR_ID",
    "BNF_SUM_DTL_TYP_CD",
    "BNF_VNDR_REMIT_COV_YR_MO",
    "MBR_UNIQ_KEY",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "BNF_VNDR_REMIT_DTL_SK",
    "CRT_RUN_CYC_EXCTN_SK"
)

# db2_K_BNF_VNDR_REMIT_DTL_F_Load (merge to EDW)
temp_table_name = "STAGING.EdwEdwBnfVndrRemitDtlFPkey_db2_K_BNF_VNDR_REMIT_DTL_F_Load_temp"
execute_dml(f"DROP TABLE IF EXISTS {temp_table_name}", jdbc_url, jdbc_props)
df_xfm_PKEYgen_toDB.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", temp_table_name) \
    .mode("overwrite") \
    .save()

merge_sql_db2_K_BNF_VNDR_REMIT_DTL_F_Load = f"""
MERGE INTO {EDWOwner}.K_BNF_VNDR_REMIT_DTL_F AS T
USING {temp_table_name} AS S
ON
    T.SRC_SYS_CD = S.SRC_SYS_CD
    AND T.BNF_VNDR_ID = S.BNF_VNDR_ID
    AND T.BNF_SUM_DTL_TYP_CD = S.BNF_SUM_DTL_TYP_CD
    AND T.BNF_VNDR_REMIT_COV_YR_MO = S.BNF_VNDR_REMIT_COV_YR_MO
    AND T.MBR_UNIQ_KEY = S.MBR_UNIQ_KEY
WHEN MATCHED THEN UPDATE SET
    T.SRC_SYS_CD = S.SRC_SYS_CD,
    T.BNF_VNDR_ID = S.BNF_VNDR_ID,
    T.BNF_SUM_DTL_TYP_CD = S.BNF_SUM_DTL_TYP_CD,
    T.BNF_VNDR_REMIT_COV_YR_MO = S.BNF_VNDR_REMIT_COV_YR_MO,
    T.MBR_UNIQ_KEY = S.MBR_UNIQ_KEY,
    T.CRT_RUN_CYC_EXCTN_DT_SK = S.CRT_RUN_CYC_EXCTN_DT_SK,
    T.BNF_VNDR_REMIT_DTL_SK = S.BNF_VNDR_REMIT_DTL_SK,
    T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK
WHEN NOT MATCHED THEN INSERT
(
    SRC_SYS_CD,
    BNF_VNDR_ID,
    BNF_SUM_DTL_TYP_CD,
    BNF_VNDR_REMIT_COV_YR_MO,
    MBR_UNIQ_KEY,
    CRT_RUN_CYC_EXCTN_DT_SK,
    BNF_VNDR_REMIT_DTL_SK,
    CRT_RUN_CYC_EXCTN_SK
)
VALUES
(
    S.SRC_SYS_CD,
    S.BNF_VNDR_ID,
    S.BNF_SUM_DTL_TYP_CD,
    S.BNF_VNDR_REMIT_COV_YR_MO,
    S.MBR_UNIQ_KEY,
    S.CRT_RUN_CYC_EXCTN_DT_SK,
    S.BNF_VNDR_REMIT_DTL_SK,
    S.CRT_RUN_CYC_EXCTN_SK
);
"""
execute_dml(merge_sql_db2_K_BNF_VNDR_REMIT_DTL_F_Load, jdbc_url, jdbc_props)

# jn_Pkey (left outer join of cpy_MultiStreams_out1 and xfm_PKEYgen_out)
df_jn_pkey = df_cpy_MultiStreams_out1.alias("l").join(
    df_xfm_PKEYgen_out.alias("r"),
    on=[
        col("l.SRC_SYS_CD")==col("r.SRC_SYS_CD"),
        col("l.BNF_VNDR_ID")==col("r.BNF_VNDR_ID"),
        col("l.BNF_SUM_DTL_TYP_CD")==col("r.BNF_SUM_DTL_TYP_CD"),
        col("l.BNF_VNDR_REMIT_COV_YR_MO")==col("r.BNF_VNDR_REMIT_COV_YR_MO"),
        col("l.MBR_UNIQ_KEY")==col("r.MBR_UNIQ_KEY")
    ],
    how="left"
).select(
    col("r.BNF_VNDR_REMIT_DTL_SK").alias("BNF_VNDR_REMIT_DTL_SK"),
    col("l.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("l.BNF_VNDR_ID").alias("BNF_VNDR_ID"),
    col("l.BNF_SUM_DTL_TYP_CD").alias("BNF_SUM_DTL_TYP_CD"),
    col("l.BNF_VNDR_REMIT_COV_YR_MO").alias("BNF_VNDR_REMIT_COV_YR_MO"),
    col("l.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    col("l.CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    col("l.LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("l.BNF_VNDR_REMIT_SUM_SK").alias("BNF_VNDR_REMIT_SUM_SK"),
    col("l.MBR_SK").alias("MBR_SK"),
    col("l.MBR_ORIG_CT_SK").alias("MBR_ORIG_CT_SK"),
    col("l.MBR_HOME_ADDR_ZIP_CD_5").alias("MBR_HOME_ADDR_ZIP_CD_5"),
    col("l.PROD_ID").alias("PROD_ID"),
    col("l.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("l.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("l.BNF_SUM_DTL_TYP_CD_SK").alias("BNF_SUM_DTL_TYP_CD_SK")
)

# seq_BNF_VNDR_REMIT_DTL_F_csv_out (write to .dat)
df_seq_BNF_VNDR_REMIT_DTL_F_csv_out = df_jn_pkey
df_seq_BNF_VNDR_REMIT_DTL_F_csv_out = df_seq_BNF_VNDR_REMIT_DTL_F_csv_out.withColumn(
    "BNF_VNDR_REMIT_COV_YR_MO", rpad(col("BNF_VNDR_REMIT_COV_YR_MO"), 6, " ")
).withColumn(
    "CRT_RUN_CYC_EXCTN_DT_SK", rpad(col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ")
).withColumn(
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK", rpad(col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ")
).withColumn(
    "MBR_HOME_ADDR_ZIP_CD_5", rpad(col("MBR_HOME_ADDR_ZIP_CD_5"), 5, " ")
)

df_seq_BNF_VNDR_REMIT_DTL_F_csv_out = df_seq_BNF_VNDR_REMIT_DTL_F_csv_out.select(
    "BNF_VNDR_REMIT_DTL_SK",
    "SRC_SYS_CD",
    "BNF_VNDR_ID",
    "BNF_SUM_DTL_TYP_CD",
    "BNF_VNDR_REMIT_COV_YR_MO",
    "MBR_UNIQ_KEY",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "BNF_VNDR_REMIT_SUM_SK",
    "MBR_SK",
    "MBR_ORIG_CT_SK",
    "MBR_HOME_ADDR_ZIP_CD_5",
    "PROD_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "BNF_SUM_DTL_TYP_CD_SK"
)

write_files(
    df_seq_BNF_VNDR_REMIT_DTL_F_csv_out,
    f"{adls_path}/load/BNF_VNDR_REMIT_DTL_F.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)