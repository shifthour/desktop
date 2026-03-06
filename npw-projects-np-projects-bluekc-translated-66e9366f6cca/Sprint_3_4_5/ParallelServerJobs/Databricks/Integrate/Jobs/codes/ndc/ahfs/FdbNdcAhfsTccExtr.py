# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 01/28/08 08:28:47 Batch  14638_30532 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 12/26/07 11:16:35 Batch  14605_40601 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 11/21/07 14:41:36 Batch  14570_52900 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_2 10/10/07 13:50:33 Batch  14528_49851 PROMOTE bckcetl ids20 dsadm bls for bl
# MAGIC ^1_2 10/10/07 13:38:24 Batch  14528_49120 INIT bckcett testIDS30 dsadm bls for bl
# MAGIC ^1_1 10/08/07 17:37:28 Batch  14526_63461 PROMOTE bckcett testIDS30 u08717 Brent
# MAGIC ^1_1 10/08/07 17:30:51 Batch  14526_63055 INIT bckcett devlIDS30 u08717 Brent
# MAGIC ^1_1 05/23/06 11:51:34 Batch  14023_42698 INIT bckcett devlIDS30 u05779 bj
# MAGIC ^1_10 12/13/05 13:55:16 Batch  13862_50122 INIT bckcett devlIDS30 u08717 Brent
# MAGIC ^1_9 10/05/05 10:21:04 Batch  13793_37270 INIT bckcett devlIDS30 u08717 Brent
# MAGIC ^1_8 09/13/05 13:35:31 Batch  13771_48935 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_7 09/12/05 16:01:51 Batch  13770_57714 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_6 09/12/05 15:45:16 Batch  13770_56719 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_5 09/12/05 15:38:28 Batch  13770_56313 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_4 09/09/05 16:34:40 Batch  13767_59682 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_3 09/09/05 16:10:39 Batch  13767_58243 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_2 08/15/05 09:23:22 Batch  13742_33806 INIT bckcett devlIDS30 u05779 bj
# MAGIC ^1_1 08/12/05 14:08:33 Batch  13739_50918 INIT bckcett devlIDS30 dsadm Gina Parr
# MAGIC ^1_1 08/05/05 14:03:16 Batch  13732_50601 INIT bckcett devlIDS30 u05779 bj
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2005 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC       
# MAGIC 
# MAGIC INPUTS:
# MAGIC            The file named RHIC3D3_HIC_THERAP_CLASS_DESC is received from First Data Bank and staged to #FilePath#/landing/ahfs.dat
# MAGIC   
# MAGIC HASH FILES:  hf_ahfs_tcc
# MAGIC 
# MAGIC CONTAINERS:  None
# MAGIC 
# MAGIC TRANSFORMS:  
# MAGIC                   no description fields so no STRIP.FIELD
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC                    Output file is created with a temp. name.                 
# MAGIC 
# MAGIC OUTPUTS: 
# MAGIC                    Sequential file name is created in the job control ( TmpOutFile Parameter )
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC                   Hugh Sisson -     06/2005 -          Original programming
# MAGIC                   BJ Luce             04/25/2006        use Environment parameters. Name output file FdbNdcAhfsTcc.dat
# MAGIC 
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                         Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                    ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada              4/16/2007          3264                              Added Balancing process to the overall                 devlIDS30               Steph Goddard            09/27/2007
# MAGIC                                                                                                       job that takes a snapshot of the source data   
# MAGIC  
# MAGIC Hari Krishna Raoi Yadav  05/18/2021     US- 382322                  updated the derivaion logic in Stage BusinessRules  IntegrateSITF          Jeyaprasanna              2021-05-23
# MAGIC                                                                                                       for Column AHFS_TCC_DESC

# MAGIC Transforming American Hosptial Formulary System codes to CRF
# MAGIC Apply business logic
# MAGIC Assign primary surrogate key
# MAGIC Writing Sequential File to ../key
# MAGIC Balancing snapshot of source table
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import col, lit, when, rpad
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


CurrRunCycle = get_widget_value("CurrRunCycle","")
RunID = get_widget_value("RunID","")
CurrDate = get_widget_value("CurrDate","")
ids_secret_name = get_widget_value("ids_secret_name","")

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

# --------------------------------------------------------------------------------
# Read from dummy table replacing the hashed file "hf_ahfs_tcc" (Scenario B)
# --------------------------------------------------------------------------------
df_hf_ahfs_tcc_lkup = (
    spark.read.format("jdbc")
        .option("url", jdbc_url)
        .options(**jdbc_props)
        .option("query", "SELECT AHFS_TCC, CRT_RUN_CYC_EXCTN_SK, AHFS_TCC_SK FROM IDS.dummy_hf_ahfs_tcc")
        .load()
)

# --------------------------------------------------------------------------------
# PullAHFS (CSeqFileStage) reading from landing/ahfs.dat
# --------------------------------------------------------------------------------
schema_PullAHFS = StructType([
    StructField("AHFS_TCC", IntegerType(), nullable=False),
    StructField("AHFS_TCC_DESC", StringType(), nullable=False)
])
df_PullAHFS = (
    spark.read.format("csv")
        .schema(schema_PullAHFS)
        .option("header", "false")
        .option("quote", '"')
        .option("sep", ",")
        .load(f"{adls_path_raw}/landing/ahfs.dat")
)

# --------------------------------------------------------------------------------
# BusinessRules (CTransformerStage)
# --------------------------------------------------------------------------------
df_BusinessRules = (
    df_PullAHFS
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", F.lit(0))
    .withColumn("INSRT_UPDT_CD", F.lit("I"))
    .withColumn("DISCARD_IN", F.lit("N"))
    .withColumn("PASS_THRU_IN", F.lit("Y"))
    .withColumn("FIRST_RECYC_DT", F.lit(CurrDate))
    .withColumn("ERR_CT", F.lit(0))
    .withColumn("RECYCLE_CT", F.lit(0))
    .withColumn("SRC_SYS_CD", F.lit("FDB"))
    .withColumn("PRI_KEY_STRING", F.concat(F.lit("FDB"), F.lit(";"), col("AHFS_TCC")))
    .withColumn("AHFS_TCC_SK", F.lit(0))
    .withColumn("AHFS_TCC", trim(col("AHFS_TCC")))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(0))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(0))
    .withColumn("AHFS_TCC_DESC", Convert("CHAR(10):CHAR(13):CHAR(9)", "", trim(col("AHFS_TCC_DESC"))))
)

# --------------------------------------------------------------------------------
# PrimaryKey (CTransformerStage) merging lookup from df_hf_ahfs_tcc_lkup
# --------------------------------------------------------------------------------
df_join = (
    df_BusinessRules.alias("t")
    .join(
        df_hf_ahfs_tcc_lkup.alias("l"),
        F.col("t.AHFS_TCC") == F.col("l.AHFS_TCC"),
        how="left"
    )
    .select(
        F.col("t.*"),
        F.col("l.AHFS_TCC_SK").alias("lkup_AHFS_TCC_SK"),
        F.col("l.CRT_RUN_CYC_EXCTN_SK").alias("lkup_CRT_RUN_CYC_EXCTN_SK")
    )
)

df_temp = (
    df_join
    .withColumn(
        "AHFS_TCC_SK",
        when(col("lkup_AHFS_TCC_SK").isNull(), F.lit(None)).otherwise(col("lkup_AHFS_TCC_SK"))
    )
    .withColumn(
        "CRT_RUN_CYC_EXCTN_SK",
        when(col("lkup_AHFS_TCC_SK").isNull(), lit(CurrRunCycle)).otherwise(col("lkup_CRT_RUN_CYC_EXCTN_SK"))
    )
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", lit(CurrRunCycle))
)

df_enriched = df_temp
df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"AHFS_TCC_SK",<schema>,<secret_name>)

# --------------------------------------------------------------------------------
# Output link "Key" to AhfsTccExtr (CSeqFileStage)
# --------------------------------------------------------------------------------
df_key = df_enriched.select(
    col("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    rpad(col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    rpad(col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    col("FIRST_RECYC_DT"),
    col("ERR_CT"),
    col("RECYCLE_CT"),
    col("SRC_SYS_CD"),
    col("PRI_KEY_STRING"),
    col("AHFS_TCC_SK"),
    col("AHFS_TCC"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("AHFS_TCC_DESC")
)

write_files(
    df_key,
    f"{adls_path}/key/FdbNdcAhfsTccExtr.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote='"',
    nullValue=None
)

# --------------------------------------------------------------------------------
# Output link "updt" to hf_ahfs_tcc (CHashedFileStage) - Scenario B dummy DB table insert
# Only rows where lkup_AHFS_TCC_SK was null
# --------------------------------------------------------------------------------
df_updt = df_enriched.filter(col("lkup_AHFS_TCC_SK").isNull()).select(
    col("AHFS_TCC"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("AHFS_TCC_SK")
)

execute_dml("DROP TABLE IF EXISTS STAGING.FdbNdcAhfsTccExtr_hf_ahfs_tcc_temp", jdbc_url, jdbc_props)

(
    df_updt.write
    .format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("dbtable", "STAGING.FdbNdcAhfsTccExtr_hf_ahfs_tcc_temp")
    .mode("error")
    .save()
)

merge_sql = """
MERGE INTO IDS.dummy_hf_ahfs_tcc AS T
USING STAGING.FdbNdcAhfsTccExtr_hf_ahfs_tcc_temp AS S
ON T.AHFS_TCC = S.AHFS_TCC
WHEN NOT MATCHED THEN
  INSERT (AHFS_TCC, CRT_RUN_CYC_EXCTN_SK, AHFS_TCC_SK)
  VALUES (S.AHFS_TCC, S.CRT_RUN_CYC_EXCTN_SK, S.AHFS_TCC_SK);
"""
execute_dml(merge_sql, jdbc_url, jdbc_props)

# --------------------------------------------------------------------------------
# FDB_Source (CSeqFileStage) reading from landing/ahfs.dat again
# --------------------------------------------------------------------------------
schema_FDB_Source = StructType([
    StructField("AHFS_TCC", IntegerType(), nullable=False)
])
df_FDB_Source = (
    spark.read.format("csv")
        .schema(schema_FDB_Source)
        .option("header", "false")
        .option("quote", '"')
        .option("sep", ",")
        .load(f"{adls_path_raw}/landing/ahfs.dat")
)

# --------------------------------------------------------------------------------
# Transform (CTransformerStage) -> output to Snapshot_File
# --------------------------------------------------------------------------------
df_Transform = df_FDB_Source.select(
    trim(col("AHFS_TCC")).alias("AHFS_TCC")
)

# --------------------------------------------------------------------------------
# Snapshot_File (CSeqFileStage) writing to load/B_AHFS_TCC.dat
# --------------------------------------------------------------------------------
write_files(
    df_Transform.select(rpad(col("AHFS_TCC"), 1, " ").alias("AHFS_TCC")),
    f"{adls_path}/load/B_AHFS_TCC.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote='"',
    nullValue=None
)