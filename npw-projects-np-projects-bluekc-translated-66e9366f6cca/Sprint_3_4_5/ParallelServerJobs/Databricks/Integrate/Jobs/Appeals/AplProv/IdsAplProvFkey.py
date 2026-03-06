# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_2 10/26/07 13:38:47 Batch  14544_49159 PROMOTE bckcetl ids20 dsadm bls for hs
# MAGIC ^1_2 10/26/07 13:19:46 Batch  14544_47992 INIT bckcett testIDS30 dsadm bls for hs
# MAGIC ^1_1 09/28/07 14:58:08 Batch  14516_53893 INIT bckcett testIDS30 dsadm bls for hs
# MAGIC ^1_3 09/13/07 17:24:15 Batch  14501_62661 PROMOTE bckcett testIDS30 u11141 Hugh Sisson
# MAGIC ^1_3 09/13/07 17:10:39 Batch  14501_61844 INIT bckcett devlIDS30 u11141 Hugh Sisson
# MAGIC ^1_2 09/12/07 11:51:28 Batch  14500_42704 INIT bckcett devlIDS30 u03651 deploy to test steffy
# MAGIC ^1_1 09/05/07 12:58:39 Batch  14493_46724 INIT bckcett devlIDS30 u11141 Hugh Sisson
# MAGIC 
# MAGIC ***************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:   Takes the primary key file and applies the foreign keys.   
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer             Date                         Change Description                                                                           Project #          Development Project          Code Reviewer          Date Reviewed  
# MAGIC ------------------           ----------------------------     -----------------------------------------------------------------------                                   ----------------         ------------------------------------       ----------------------------      ----------------
# MAGIC Bhoomi Dasari     07/11/2007               Initial program                                                                                     3028                  devlIDS30                         Steph Goddard           8/23/07

# MAGIC Set all foreign surrogate keys
# MAGIC Merge source data with default rows
# MAGIC Writing Sequential File to /load
# MAGIC Read common record format file from extract job.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, DecimalType
from pyspark.sql.functions import col, lit, rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


Logging = get_widget_value("Logging","X")
InFile = get_widget_value("InFile","FctsAplProvExtr.AplProv.dat.2007010167")

schema_IdsAplProvExtr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False),
    StructField("INSRT_UPDT_CD", StringType(), False),
    StructField("DISCARD_IN", StringType(), False),
    StructField("PASS_THRU_IN", StringType(), False),
    StructField("FIRST_RECYC_DT", TimestampType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", DecimalType(10,0), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PRI_KEY_STRING", StringType(), False),
    StructField("APL_PROV_SK", IntegerType(), False),
    StructField("APL_ID", StringType(), False),
    StructField("SEQ_NO", IntegerType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("APL_SK", IntegerType(), False),
    StructField("PROV_SK", IntegerType(), False),
    StructField("PROV_GRP_PROV_SK", IntegerType(), False),
    StructField("APL_PROV_RELSHP_RSN_CD_SK", IntegerType(), False)
])

df_IdsAplProvExtr = (
    spark.read
    .option("delimiter", ",")
    .option("quote", "\"")
    .schema(schema_IdsAplProvExtr)
    .csv(f"{adls_path}/key/{InFile}")
)

df_foreignKey = df_IdsAplProvExtr.withColumn(
    "SrcSysCdSk", GetFkeyCodes("IDS", col("APL_PROV_SK"), "SOURCE SYSTEM", col("SRC_SYS_CD"), Logging)
).withColumn(
    "PassThru", col("PASS_THRU_IN")
).withColumn(
    "svAplSk", GetFkeyApl(col("SRC_SYS_CD"), col("APL_PROV_SK"), col("APL_SK"), Logging)
).withColumn(
    "svProvSk", GetFkeyProv(col("SRC_SYS_CD"), col("APL_PROV_SK"), col("PROV_SK"), Logging)
).withColumn(
    "svProvGrpProvSk", GetFkeyProv(col("SRC_SYS_CD"), col("APL_PROV_SK"), col("PROV_GRP_PROV_SK"), Logging)
).withColumn(
    "svAplProvCdSk", GetFkeyCodes(col("SRC_SYS_CD"), col("APL_PROV_SK"), "APPEAL PROVIDER RELATIONSHIP", col("APL_PROV_RELSHP_RSN_CD_SK"), Logging)
).withColumn(
    "ErrCount", GetFkeyErrorCnt(col("APL_PROV_SK"))
)

df_Fkey = df_foreignKey.filter((col("ErrCount") == 0) | (col("PassThru") == "Y")).select(
    col("APL_PROV_SK").alias("APL_PROV_SK"),
    col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
    col("APL_ID").alias("APL_ID"),
    col("SEQ_NO").alias("SEQ_NO"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("svAplSk").alias("APL_SK"),
    col("svProvSk").alias("PROV_SK"),
    col("svProvGrpProvSk").alias("PROV_GRP_PROV_SK"),
    col("svAplProvCdSk").alias("APL_PROV_RELSHP_RSN_CD_SK")
)

df_recycle = df_foreignKey.filter(col("ErrCount") > 0).select(
    GetRecycleKey(col("APL_PROV_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
    col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    col("DISCARD_IN").alias("DISCARD_IN"),
    col("PASS_THRU_IN").alias("PASS_THRU_IN"),
    col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    col("ErrCount").alias("ERR_CT"),
    (col("RECYCLE_CT") + lit(1)).alias("RECYCLE_CT"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    col("APL_PROV_SK").alias("APL_PROV_SK"),
    col("APL_ID").alias("APL_ID"),
    col("SEQ_NO").alias("SEQ_NO"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("APL_SK").alias("APL_SK"),
    col("PROV_SK").alias("PROV_SK"),
    col("PROV_GRP_PROV_SK").alias("PROV_GRP_PROV_SK"),
    col("APL_PROV_RELSHP_RSN_CD_SK").alias("APL_PROV_RELSHP_RSN_CD_SK")
)

write_files(
    df_recycle,
    f"{adls_path}/hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

df_defaultUNK = spark.createDataFrame(
    [(0, 0, "UNK", 0, 0, 0, 0, 0, 0, 0)],
    [
        "APL_PROV_SK",
        "SRC_SYS_CD_SK",
        "APL_ID",
        "SEQ_NO",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "APL_SK",
        "PROV_SK",
        "PROV_GRP_PROV_SK",
        "APL_PROV_RELSHP_RSN_CD_SK"
    ]
)

df_defaultNA = spark.createDataFrame(
    [(1, 1, "NA", 1, 1, 1, 1, 1, 1, 1)],
    [
        "APL_PROV_SK",
        "SRC_SYS_CD_SK",
        "APL_ID",
        "SEQ_NO",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "APL_SK",
        "PROV_SK",
        "PROV_GRP_PROV_SK",
        "APL_PROV_RELSHP_RSN_CD_SK"
    ]
)

df_collector = df_Fkey.unionByName(df_defaultUNK).unionByName(df_defaultNA)

df_final = df_collector.select(
    col("APL_PROV_SK"),
    col("SRC_SYS_CD_SK"),
    rpad(col("APL_ID"), <...>, " ").alias("APL_ID"),
    col("SEQ_NO"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("APL_SK"),
    col("PROV_SK"),
    col("PROV_GRP_PROV_SK"),
    col("APL_PROV_RELSHP_RSN_CD_SK")
)

write_files(
    df_final,
    f"{adls_path}/load/APL_PROV.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)