# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_2 06/02/09 10:11:31 Batch  15129_36694 PROMOTE bckcetl ids20 dsadm bls for sa
# MAGIC ^1_2 06/02/09 10:05:13 Batch  15129_36315 INIT bckcett:31540 testIDS dsadm bls for sa
# MAGIC ^1_1 05/22/09 13:25:41 Batch  15118_48362 PROMOTE bckcett:31540 testIDS u150906 TTR229_Sharon_testIds       Maddy
# MAGIC ^1_1 05/22/09 13:17:26 Batch  15118_47880 INIT bckcett:31540 devlIDS u150906 maddy
# MAGIC ^1_1 12/06/07 10:30:05 Batch  14585_37808 INIT bckcetl ids20 dsadm dadm
# MAGIC ^1_1 10/03/07 10:51:00 Batch  14521_39063 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 03/07/07 16:30:43 Batch  14311_59451 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 10/09/06 14:25:59 Batch  14162_51974 PROMOTE bckcetl ids20 dsadm Keith for Sharon
# MAGIC ^1_1 10/09/06 14:14:37 Batch  14162_51294 INIT bckcett testIDS30 dsadm Keith for Sharon
# MAGIC ^1_7 10/05/06 14:35:41 Batch  14158_52545 PROMOTE bckcett testIDS30 u10157 sa
# MAGIC ^1_7 10/05/06 14:34:04 Batch  14158_52446 INIT bckcett devlIDS30 u10157 SA
# MAGIC ^1_6 10/05/06 14:31:17 Batch  14158_52278 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_5 09/28/06 18:10:23 Batch  14151_65432 PROMOTE bckcett devlIDS30 u10157 sa
# MAGIC ^1_5 09/28/06 18:00:51 Batch  14151_64855 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_4 09/22/06 12:01:15 Batch  14145_43279 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_3 09/21/06 16:08:28 Batch  14144_58111 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_2 09/20/06 23:19:38 Batch  14143_83979 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_1 09/20/06 22:15:19 Batch  14143_80124 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_1 09/19/06 09:53:29 Batch  14142_35614 INIT bckcett devlIDS30 u03651 steffy
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2005 BLUE CROSS/BLUE SHIELD OF KANSAS CITY
# MAGIC COPYRIGHT 2006 BLUE CROSS/BLUE SHIELD OF KANSAS CITY  
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     IdsAgntMktngTerrFkey
# MAGIC 
# MAGIC DESCRIPTION:     Takes the sorted file and applies the primary key.   Preps the file for the foreign key lookup process that follows.
# MAGIC       
# MAGIC 
# MAGIC INPUTS:  Primary Key output file
# MAGIC 	
# MAGIC   
# MAGIC HASH FILES:     hf_recycle
# MAGIC 
# MAGIC 
# MAGIC TRANSFORMS:  GetFkeyCodes
# MAGIC                             GetFkeyDates
# MAGIC                             GetFkeyErrorCnt
# MAGIC 
# MAGIC PROCESSING:  Natural keys are converted to surrogate keys
# MAGIC 
# MAGIC OUTPUTS:   AGNT_MKTNG_TERR
# MAGIC                      hf_recycle - If recycling was used for this job records would be written to this hash file that had errors.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC            Sharon Andrew   -  08/08/2006  -  Originally programmed

# MAGIC Set all foreign surragote keys
# MAGIC Merge source data with default rows
# MAGIC Writing Sequential File to /load
# MAGIC Read common record format file from extract job.
# MAGIC Builds table records with source system BBBAGNT
# MAGIC End of IDS Agent Marketing Territory foreign keying.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType
import pyspark.sql.functions as F
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


Logging = get_widget_value("Logging","")
InFile = get_widget_value("InFile","")

schema_IdsAgntMktngTerrExtr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), True),
    StructField("INSRT_UPDT_CD", StringType(), True),
    StructField("DISCARD_IN", StringType(), True),
    StructField("PASS_THRU_IN", StringType(), True),
    StructField("FIRST_RECYC_DT", TimestampType(), True),
    StructField("ERR_CT", IntegerType(), True),
    StructField("RECYCLE_CT", DecimalType(38,10), True),
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("PRI_KEY_STRING", StringType(), True),
    StructField("AGNT_TERR_SK", IntegerType(), True),
    StructField("SRC_SYS_CD_SK", IntegerType(), True),
    StructField("AGNT_ID", StringType(), True),
    StructField("TERR_CD", StringType(), True),
    StructField("AGNT_TERR_LVL_CD", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True)
])

df_IdsAgntMktngTerrExtr = (
    spark.read
    .option("header", False)
    .option("sep", ",")
    .option("quote", "\"")
    .schema(schema_IdsAgntMktngTerrExtr)
    .csv(f"{adls_path}/key/{InFile}")
)

df_ForeignKey = (
    df_IdsAgntMktngTerrExtr
    .withColumn(
        "SrcSysCdSk",
        GetFkeyCodes(
            F.lit("IDS"),
            F.col("AGNT_TERR_SK"),
            F.lit("SOURCE SYSTEM"),
            F.col("SRC_SYS_CD"),
            Logging
        )
    )
    .withColumn("PassThru", F.col("PASS_THRU_IN"))
    .withColumn(
        "AgentTerrirotyLevel",
        GetFkeyCodes(
            F.lit("BBBAGNT"),
            F.col("AGNT_TERR_SK"),
            F.lit("TERRITORY LEVEL"),
            trim(F.col("AGNT_TERR_LVL_CD")),
            Logging
        )
    )
    .withColumn(
        "TerritorySK",
        GetFkeyMktngTerritory(
            F.lit("BBBAGNT"),
            F.col("AGNT_TERR_SK"),
            F.col("TERR_CD"),
            F.col("AGNT_TERR_LVL_CD"),
            Logging
        )
    )
    .withColumn(
        "svAgntSK",
        GetFkeyAgnt(
            F.lit("FACETS"),
            F.col("AGNT_TERR_SK"),
            F.col("AGNT_ID"),
            Logging
        )
    )
    .withColumn("ErrCount", GetFkeyErrorCnt(F.col("AGNT_TERR_SK")))
)

df_ForeignKey_Fkey = (
    df_ForeignKey
    .filter((F.col("ErrCount") == 0) | (F.col("PassThru") == F.lit("Y")))
    .select(
        F.col("AGNT_TERR_SK").alias("AGNT_TERR_SK"),
        F.col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
        F.col("AGNT_ID").alias("AGNT_ID"),
        F.col("TERR_CD").alias("MKTNG_TERR_ID"),
        F.col("AgentTerrirotyLevel").alias("MKTNG_TERR_CAT_CD_SK"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("svAgntSK").alias("AGNT_SK"),
        F.col("TerritorySK").alias("MKTNG_TERR_SK")
    )
)

df_ForeignKey_Recycle = (
    df_ForeignKey
    .filter(F.col("ErrCount") > 0)
    .select(
        F.col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        F.col("DISCARD_IN").alias("DISCARD_IN"),
        F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
        F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        F.col("ERR_CT").alias("ERR_CT"),
        F.col("RECYCLE_CT").alias("RECYCLE_CT"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        F.col("AGNT_TERR_SK").alias("AGNT_TERR_SK"),
        F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.col("AGNT_ID").alias("AGNT_ID"),
        F.col("TERR_CD").alias("MKTNG_TERR_ID"),
        F.col("AGNT_TERR_LVL_CD").alias("AGNT_TERR_LVL_CD"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
    )
)

schema_Collector = StructType([
    StructField("AGNT_TERR_SK", IntegerType(), True),
    StructField("SRC_SYS_CD_SK", IntegerType(), True),
    StructField("AGNT_ID", StringType(), True),
    StructField("MKTNG_TERR_ID", StringType(), True),
    StructField("MKTNG_TERR_CAT_CD_SK", IntegerType(), True),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("AGNT_SK", IntegerType(), True),
    StructField("MKTNG_TERR_SK", IntegerType(), True)
])

df_DefaultUNK = spark.createDataFrame(
    [(0, 0, "UNK", "UNK", 0, 0, 0, 0, 0)],
    schema_Collector
)

df_DefaultNA = spark.createDataFrame(
    [(1, 1, "NA", "NA", 1, 1, 1, 1, 1)],
    schema_Collector
)

df_Collector = (
    df_ForeignKey_Fkey
    .union(df_DefaultUNK)
    .union(df_DefaultNA)
)

df_ForeignKey_Recycle_final = (
    df_ForeignKey_Recycle
    .withColumn("INSRT_UPDT_CD", F.rpad(F.col("INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", F.rpad(F.col("DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", F.rpad(F.col("PASS_THRU_IN"), 1, " "))
    .withColumn("MKTNG_TERR_ID", F.rpad(F.col("MKTNG_TERR_ID"), 3, " "))
)

write_files(
    df_ForeignKey_Recycle_final.select(
        "JOB_EXCTN_RCRD_ERR_SK",
        "INSRT_UPDT_CD",
        "DISCARD_IN",
        "PASS_THRU_IN",
        "FIRST_RECYC_DT",
        "ERR_CT",
        "RECYCLE_CT",
        "SRC_SYS_CD",
        "PRI_KEY_STRING",
        "AGNT_TERR_SK",
        "SRC_SYS_CD_SK",
        "AGNT_ID",
        "MKTNG_TERR_ID",
        "AGNT_TERR_LVL_CD",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK"
    ),
    "hf_recycle.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

df_Collector_final = df_Collector.withColumn("MKTNG_TERR_ID", F.rpad(F.col("MKTNG_TERR_ID"), 3, " "))

write_files(
    df_Collector_final.select(
        "AGNT_TERR_SK",
        "SRC_SYS_CD_SK",
        "AGNT_ID",
        "MKTNG_TERR_ID",
        "MKTNG_TERR_CAT_CD_SK",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "AGNT_SK",
        "MKTNG_TERR_SK"
    ),
    f"{adls_path}/load/AGNT_MKTNG_TERR.dat",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)