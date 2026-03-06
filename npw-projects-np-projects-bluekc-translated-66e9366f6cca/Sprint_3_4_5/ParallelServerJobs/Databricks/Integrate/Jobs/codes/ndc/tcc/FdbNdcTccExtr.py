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
# MAGIC            #FilePath#/landing/tcc.dat
# MAGIC   
# MAGIC HASH FILES:  none
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
# MAGIC                    #$FilePath#/key/FdbNdcTccExtr.dat
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC                   Hugh Sisson -    06/2005 -           Original programming
# MAGIC                   BJ Luce             4/2006             use environment variables and hard code outfile
# MAGIC 
# MAGIC 
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                         Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                    ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada              4/17/2007          3264                              Added Balancing process to the overall                 devlIDS30               Steph Goddard            09/27/2007
# MAGIC                                                                                                       job that takes a snapshot of the source data

# MAGIC Transforming Theraputic Class codes to CRF
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
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


CurrRunCycle = get_widget_value('CurrRunCycle', '1')
RunID = get_widget_value('RunID', '20070424')
CurrDate = get_widget_value('CurrDate', '')
ids_secret_name = get_widget_value('ids_secret_name','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

execute_dml("DROP TABLE IF EXISTS STAGING.FdbNdcTccExtr_hf_tcc_temp", jdbc_url, jdbc_props)

df_hf_tcc_lkup = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", "SELECT TCC, CRT_RUN_CYC_EXCTN_SK, TCC_SK FROM dummy_hf_tcc")
    .load()
)

schema_PullTcc = StructType([
    StructField("HIC3_SEQN", IntegerType(), nullable=False),
    StructField("HIC3", StringType(), nullable=False),
    StructField("HIC3_DESC", StringType(), nullable=False),
    StructField("HIC3_GRPN", IntegerType(), nullable=False),
    StructField("HIC3_ROOT", IntegerType(), nullable=False)
])

df_PullTcc = (
    spark.read
    .option("sep", ",")
    .option("quote", "\"")
    .option("header", "false")
    .schema(schema_PullTcc)
    .csv(f"{adls_path_raw}/landing/tcc.dat")
)

df_BusinessRules = (
    df_PullTcc
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", F.lit(0))
    .withColumn("INSRT_UPDT_CD", F.lit("I"))
    .withColumn("DISCARD_IN", F.lit("N"))
    .withColumn("PASS_THRU_IN", F.lit("Y"))
    .withColumn("FIRST_RECYC_DT", F.lit(CurrDate))
    .withColumn("ERR_CT", F.lit(0))
    .withColumn("RECYCLE_CT", F.lit(0))
    .withColumn("SRC_SYS_CD", F.lit("FDB"))
    .withColumn("PRI_KEY_STRING", F.concat(F.lit("FDB"), F.lit(";"), F.col("HIC3")))
    .withColumn("TCC_SK", F.lit(0))
    .withColumn("TCC", F.col("HIC3"))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(0))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(0))
    .withColumn("TCC_DESC", F.col("HIC3_DESC"))
)

df_PrimaryKey_full = (
    df_BusinessRules.alias("Transform")
    .join(
        df_hf_tcc_lkup.alias("lkup"),
        F.col("Transform.TCC") == F.col("lkup.TCC"),
        "left"
    )
)

df_PrimaryKey_temp = (
    df_PrimaryKey_full
    .withColumn(
        "SK",
        F.when(F.col("lkup.TCC_SK").isNull(), F.lit(None)).otherwise(F.col("lkup.TCC_SK"))
    )
    .withColumn(
        "NewCrtRunCcyExctnSK",
        F.when(F.col("lkup.TCC_SK").isNull(), F.lit(CurrRunCycle).cast(IntegerType()))
         .otherwise(F.col("lkup.CRT_RUN_CYC_EXCTN_SK").cast(IntegerType()))
    )
)

df_enriched = df_PrimaryKey_temp
df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,'SK',<schema>,<secret_name>)

df_Key = df_enriched.select(
    F.col("Transform.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("Transform.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("Transform.DISCARD_IN").alias("DISCARD_IN"),
    F.col("Transform.PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("Transform.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("Transform.ERR_CT").alias("ERR_CT"),
    F.col("Transform.RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("Transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Transform.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("SK").alias("TCC_SK"),
    F.col("Transform.TCC").alias("TCC"),
    F.col("NewCrtRunCcyExctnSK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(CurrRunCycle).cast(IntegerType()).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("Transform.TCC_DESC").alias("TCC_DESC")
)

df_updt = (
    df_enriched
    .filter(F.col("lkup.TCC_SK").isNull())
    .select(
        F.col("Transform.TCC").alias("TCC"),
        F.col("NewCrtRunCcyExctnSK").cast(IntegerType()).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("SK").cast(IntegerType()).alias("TCC_SK")
    )
)

df_updt.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.FdbNdcTccExtr_hf_tcc_temp") \
    .mode("overwrite") \
    .save()

merge_sql = """
MERGE INTO dummy_hf_tcc AS target
USING STAGING.FdbNdcTccExtr_hf_tcc_temp AS source
ON target.TCC = source.TCC
WHEN NOT MATCHED THEN
INSERT (TCC, CRT_RUN_CYC_EXCTN_SK, TCC_SK)
VALUES (source.TCC, source.CRT_RUN_CYC_EXCTN_SK, source.TCC_SK);
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)

df_FdbNdcTccExtr_final = df_Key.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("TCC_SK"),
    F.rpad(F.col("TCC"), 3, " ").alias("TCC"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.rpad(F.col("TCC_DESC"), 50, " ").alias("TCC_DESC")
)

write_files(
    df_FdbNdcTccExtr_final,
    f"{adls_path}/key/FdbNdcTccExtr.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

schema_FDB_Source = StructType([
    StructField("HIC3_SEQN", IntegerType(), nullable=False),
    StructField("HIC3", StringType(), nullable=False),
    StructField("HIC3_DESC", StringType(), nullable=False),
    StructField("HIC3_GRPN", IntegerType(), nullable=False),
    StructField("HIC3_ROOT", IntegerType(), nullable=False)
])

df_FDB_Source = (
    spark.read
    .option("sep", ",")
    .option("quote", "\"")
    .option("header", "false")
    .schema(schema_FDB_Source)
    .csv(f"{adls_path_raw}/landing/tcc.dat")
)

df_Transform = df_FDB_Source.withColumn("TCC", trim(strip_field(F.col("HIC3")))).select("TCC")

df_Snapshot_File = df_Transform.select(
    F.rpad(F.col("TCC"), 3, " ").alias("TCC")
)

write_files(
    df_Snapshot_File,
    f"{adls_path}/load/B_TCC.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)