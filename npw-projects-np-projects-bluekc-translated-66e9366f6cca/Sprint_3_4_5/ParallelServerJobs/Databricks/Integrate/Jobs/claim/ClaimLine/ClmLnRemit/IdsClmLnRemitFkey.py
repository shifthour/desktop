# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_2 03/10/09 09:25:21 Batch  15045_33943 INIT bckcetl ids20 dcg01 sa Bringing ALL Claim code down to devlIDS
# MAGIC ^1_2 02/10/09 11:16:45 Batch  15017_40611 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 08/22/08 10:09:03 Batch  14845_36590 PROMOTE bckcetl ids20 dsadm bls for sa
# MAGIC ^1_1 08/22/08 09:55:32 Batch  14845_35734 INIT bckcett testIDS dsadm bls for sa
# MAGIC ^1_1 08/19/08 10:43:19 Batch  14842_38609 PROMOTE bckcett testIDS u03651 steph for Sharon 3057
# MAGIC ^1_1 08/19/08 10:38:02 Batch  14842_38285 INIT bckcett devlIDSnew u03651 steffy
# MAGIC 
# MAGIC 
# MAGIC ************************************************************************************
# MAGIC COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC ************************************************************************************
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  
# MAGIC 
# MAGIC DESCRIPTION:  Get foreign key as stated above and create file to load to CLM_LN_REMIT
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------    
# MAGIC Parik                       2008-07-30        3057(Web Remit)   Original Programming                                                                   devlIDSnew                   Steph Goddard          08/11/2008

# MAGIC Read common record format file from extract job.
# MAGIC Set all foreign surragote keys
# MAGIC Writing Sequential File to /load
# MAGIC Merge source data with default rows
# MAGIC Recycle records with ErrCount > 0
# MAGIC This hash file is written to by all claim foriegn key jobs.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType, TimestampType
from pyspark.sql.functions import col, lit, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


Source = get_widget_value('Source','')
InFile = get_widget_value('InFile','BcbsClmLnRemitExtr.ClmLnRemit.dat.20080730')
Logging = get_widget_value('Logging','Y')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')

schema_ClmLnRemitExtr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False),
    StructField("INSRT_UPDT_CD", StringType(), False),
    StructField("DISCARD_IN", StringType(), False),
    StructField("PASS_THRU_IN", StringType(), False),
    StructField("FIRST_RECYC_DT", TimestampType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", DecimalType(38,10), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PRI_KEY_STRING", StringType(), False),
    StructField("CLM_LN_SK", IntegerType(), False),
    StructField("CLM_ID", StringType(), False),
    StructField("CLM_LN_SEQ_NO", IntegerType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("REMIT_PATN_RESP_AMT", DecimalType(38,10), False),
    StructField("REMIT_PROV_WRT_OFF_AMT", DecimalType(38,10), False),
    StructField("REMIT_MBR_OTHR_LIAB_AMT", DecimalType(38,10), False),
    StructField("REMIT_NO_RESP_AMT", DecimalType(38,10), False)
])

file_path_ClmLnRemitExtr = f"{adls_path}/key/{InFile}"

df_ClmLnRemitExtr = (
    spark.read.format("csv")
    .schema(schema_ClmLnRemitExtr)
    .option("header","false")
    .option("quote","\"")
    .option("delimiter",",")
    .load(file_path_ClmLnRemitExtr)
)

df_foreignkey = df_ClmLnRemitExtr.withColumn("ErrCount", GetFkeyErrorCnt(col("CLM_LN_SK"))).withColumn("PassThru", col("PASS_THRU_IN"))

df_recycle = df_foreignkey.filter("ErrCount > 0").select(
    GetRecycleKey(col("CLM_LN_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
    col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    col("DISCARD_IN").alias("DISCARD_IN"),
    col("PASS_THRU_IN").alias("PASS_THRU_IN"),
    col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    col("ERR_CT").alias("ERR_CT"),
    (col("RECYCLE_CT") + lit(1)).alias("RECYCLE_CT"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    col("CLM_LN_SK").alias("CLM_LN_SK"),
    col("CLM_ID").alias("CLM_ID"),
    col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("REMIT_PATN_RESP_AMT").alias("REMIT_PATN_RESP_AMT"),
    col("REMIT_PROV_WRT_OFF_AMT").alias("REMIT_PROV_WRT_OFF_AMT"),
    col("REMIT_MBR_OTHR_LIAB_AMT").alias("REMIT_MBR_OTHR_LIAB_AMT"),
    col("REMIT_NO_RESP_AMT").alias("REMIT_NO_RESP_AMT")
)

df_fkey = df_foreignkey.filter((col("ErrCount") == 0) | (col("PassThru") == "Y")).select(
    col("CLM_LN_SK").alias("CLM_LN_SK"),
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    col("CLM_ID").alias("CLM_ID"),
    col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("REMIT_PATN_RESP_AMT").alias("REMIT_PATN_RESP_AMT"),
    col("REMIT_PROV_WRT_OFF_AMT").alias("REMIT_PROV_WRT_OFF_AMT"),
    col("REMIT_MBR_OTHR_LIAB_AMT").alias("REMIT_MBR_OTHR_LIAB_AMT"),
    col("REMIT_NO_RESP_AMT").alias("REMIT_NO_RESP_AMT")
)

df_defaultUNK = df_foreignkey.limit(1).select(
    lit(0).alias("CLM_LN_SK"),
    lit(0).alias("SRC_SYS_CD_SK"),
    lit("UNK").alias("CLM_ID"),
    lit(0).alias("CLM_LN_SEQ_NO"),
    lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit(0.00).alias("REMIT_PATN_RESP_AMT"),
    lit(0.00).alias("REMIT_PROV_WRT_OFF_AMT"),
    lit(0.00).alias("REMIT_MBR_OTHR_LIAB_AMT"),
    lit(0.00).alias("REMIT_NO_RESP_AMT")
)

df_defaultNA = df_foreignkey.limit(1).select(
    lit(1).alias("CLM_LN_SK"),
    lit(1).alias("SRC_SYS_CD_SK"),
    lit("NA").alias("CLM_ID"),
    lit(1).alias("CLM_LN_SEQ_NO"),
    lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit(0.00).alias("REMIT_PATN_RESP_AMT"),
    lit(0.00).alias("REMIT_MBR_OTHR_LIAB_AMT"),
    lit(0.00).alias("REMIT_NO_RESP_AMT"),
    lit(0.00).alias("REMIT_PROV_WRT_OFF_AMT")
)

df_recycle_clms = df_foreignkey.filter("ErrCount > 0").select(
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("CLM_ID").alias("CLM_ID")
)

df_recycle_wr = df_recycle.withColumn(
    "INSRT_UPDT_CD", rpad(col("INSRT_UPDT_CD"), 10, " ")
).withColumn(
    "DISCARD_IN", rpad(col("DISCARD_IN"), 1, " ")
).withColumn(
    "PASS_THRU_IN", rpad(col("PASS_THRU_IN"), 1, " ")
).select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "CLM_LN_SK",
    "CLM_ID",
    "CLM_LN_SEQ_NO",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "REMIT_PATN_RESP_AMT",
    "REMIT_PROV_WRT_OFF_AMT",
    "REMIT_MBR_OTHR_LIAB_AMT",
    "REMIT_NO_RESP_AMT"
)

write_files(
    df_recycle_wr,
    "hf_recycle.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

df_recycle_clms_wr = df_recycle_clms.select("SRC_SYS_CD","CLM_ID")

write_files(
    df_recycle_clms_wr,
    "hf_claim_recycle_keys.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

df_defaultUNK_2 = df_defaultUNK.select(
    "CLM_LN_SK",
    "SRC_SYS_CD_SK",
    "CLM_ID",
    "CLM_LN_SEQ_NO",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "REMIT_PATN_RESP_AMT",
    "REMIT_PROV_WRT_OFF_AMT",
    "REMIT_MBR_OTHR_LIAB_AMT",
    "REMIT_NO_RESP_AMT"
)

df_defaultNA_2 = df_defaultNA.select(
    "CLM_LN_SK",
    "SRC_SYS_CD_SK",
    "CLM_ID",
    "CLM_LN_SEQ_NO",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "REMIT_PATN_RESP_AMT",
    "REMIT_PROV_WRT_OFF_AMT",
    "REMIT_MBR_OTHR_LIAB_AMT",
    "REMIT_NO_RESP_AMT"
)

df_fkey_2 = df_fkey.select(
    "CLM_LN_SK",
    "SRC_SYS_CD_SK",
    "CLM_ID",
    "CLM_LN_SEQ_NO",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "REMIT_PATN_RESP_AMT",
    "REMIT_PROV_WRT_OFF_AMT",
    "REMIT_MBR_OTHR_LIAB_AMT",
    "REMIT_NO_RESP_AMT"
)

df_collector = df_defaultUNK_2.unionByName(df_defaultNA_2).unionByName(df_fkey_2)

final_columns = [
    "CLM_LN_SK",
    "SRC_SYS_CD_SK",
    "CLM_ID",
    "CLM_LN_SEQ_NO",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "REMIT_PATN_RESP_AMT",
    "REMIT_PROV_WRT_OFF_AMT",
    "REMIT_MBR_OTHR_LIAB_AMT",
    "REMIT_NO_RESP_AMT"
]

df_final = df_collector.select(final_columns)

file_path_clm_ln_remit = f"{adls_path}/load/CLM_LN_REMIT.{Source}.dat"

write_files(
    df_final,
    file_path_clm_ln_remit,
    ",",
    "overwrite",
    False,
    True,
    "\"",
    None
)