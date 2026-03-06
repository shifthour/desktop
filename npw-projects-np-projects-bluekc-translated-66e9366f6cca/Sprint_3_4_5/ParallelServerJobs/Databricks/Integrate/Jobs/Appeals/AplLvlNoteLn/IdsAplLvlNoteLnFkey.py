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
# MAGIC Â© Copyright 2007 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC Called by:
# MAGIC                     FctsAplLoadSeq
# MAGIC 
# MAGIC Processing:
# MAGIC                     *
# MAGIC 
# MAGIC Control Job Rerun Information: 
# MAGIC              Previous Run Successful:    Restore InFile, if necessary
# MAGIC              Previous Run Aborted:         Restart, no other steps necessary
# MAGIC 
# MAGIC Modifications:                        
# MAGIC                                               Project/                                                                                                 Code                  Date
# MAGIC Developer        Date              Altiris #           Change Description                                                        Reviewer            Reviewed
# MAGIC ----------------------  -------------------   -------------------   ------------------------------------------------------------------------------------   ----------------------     -------------------   
# MAGIC Hugh Sisson    08/09/2007   3028              Original program                                                              Steph Goddard   9/6/07

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
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    TimestampType,
    DecimalType
)
from pyspark.sql.functions import (
    col,
    lit,
    row_number,
    rpad
)
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


Logging = get_widget_value("Logging","X")
InFile = get_widget_value("InFile","FctsAplLvlNoteLnExtr.AplLvlNoteLn.dat.200708151024")

schema_IdsAplLvlNoteLnExtr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), nullable=False),
    StructField("INSRT_UPDT_CD", StringType(), nullable=False),
    StructField("DISCARD_IN", StringType(), nullable=False),
    StructField("PASS_THRU_IN", StringType(), nullable=False),
    StructField("FIRST_RECYC_DT", TimestampType(), nullable=False),
    StructField("ERR_CT", IntegerType(), nullable=False),
    StructField("RECYCLE_CT", DecimalType(38,10), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("PRI_KEY_STRING", StringType(), nullable=False),
    StructField("APL_LVL_NOTE_LN_SK", IntegerType(), nullable=False),
    StructField("APL_ID", StringType(), nullable=False),
    StructField("LVL_SEQ_NO", IntegerType(), nullable=False),
    StructField("NOTE_SEQ_NO", IntegerType(), nullable=False),
    StructField("NOTE_DEST_ID", StringType(), nullable=False),
    StructField("LN_SEQ_NO", IntegerType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("APL_LVL_NOTE_SK", IntegerType(), nullable=False),
    StructField("LN_TX", StringType(), nullable=True)
])

df_IdsAplLvlNoteLnExtr = (
    spark.read
    .option("sep", ",")
    .option("quote", "\"")
    .option("header", False)
    .schema(schema_IdsAplLvlNoteLnExtr)
    .csv(f"{adls_path}/key/{InFile}")
)

df_foreignKey = (
    df_IdsAplLvlNoteLnExtr
    .withColumn(
        "SrcSysCdSk",
        GetFkeyCodes(
            lit("IDS"),
            col("APL_LVL_NOTE_LN_SK"),
            lit("SOURCE SYSTEM"),
            col("SRC_SYS_CD"),
            Logging
        )
    )
    .withColumn("PassThru", col("PASS_THRU_IN"))
    .withColumn(
        "svAplLvlNoteSk",
        GetFkeyAplLvlNote(
            col("SRC_SYS_CD"),
            col("APL_LVL_NOTE_LN_SK"),
            col("APL_ID"),
            col("LVL_SEQ_NO"),
            col("NOTE_SEQ_NO"),
            col("NOTE_DEST_ID"),
            Logging
        )
    )
    .withColumn("ErrCount", GetFkeyErrorCnt(col("APL_LVL_NOTE_LN_SK")))
)

df_Fkey = (
    df_foreignKey
    .filter((col("ErrCount") == 0) | (col("PassThru") == "Y"))
    .select(
        col("APL_LVL_NOTE_LN_SK").alias("APL_LVL_NOTE_LN_SK"),
        col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
        col("APL_ID").alias("APL_ID"),
        col("LVL_SEQ_NO").alias("LVL_SEQ_NO"),
        col("NOTE_SEQ_NO").alias("NOTE_SEQ_NO"),
        col("NOTE_DEST_ID").alias("NOTE_DEST_ID"),
        col("LN_SEQ_NO").alias("LN_SEQ_NO"),
        col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("svAplLvlNoteSk").alias("APL_LVL_NOTE_SK"),
        col("LN_TX").alias("LN_TX")
    )
)

windowRowNum = Window.orderBy(lit(1))
df_with_rownum = df_foreignKey.withColumn("row_num", row_number().over(windowRowNum))

df_defaultUNK = (
    df_with_rownum
    .filter(col("row_num") == 1)
    .select(
        lit(0).alias("APL_LVL_NOTE_LN_SK"),
        lit(0).alias("SRC_SYS_CD_SK"),
        lit("UNK").alias("APL_ID"),
        lit(0).alias("LVL_SEQ_NO"),
        lit(0).alias("NOTE_SEQ_NO"),
        lit("UNK").alias("NOTE_DEST_ID"),
        lit(0).alias("LN_SEQ_NO"),
        lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        lit(0).alias("APL_LVL_NOTE_SK"),
        lit("UNK").alias("LN_TX")
    )
)

df_defaultNA = (
    df_with_rownum
    .filter(col("row_num") == 1)
    .select(
        lit(1).alias("APL_LVL_NOTE_LN_SK"),
        lit(1).alias("SRC_SYS_CD_SK"),
        lit("NA").alias("APL_ID"),
        lit(1).alias("LVL_SEQ_NO"),
        lit(1).alias("NOTE_SEQ_NO"),
        lit("NA").alias("NOTE_DEST_ID"),
        lit(1).alias("LN_SEQ_NO"),
        lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        lit(1).alias("APL_LVL_NOTE_SK"),
        lit("NA").alias("LN_TX")
    )
)

df_collector = (
    df_Fkey.select(
        "APL_LVL_NOTE_LN_SK",
        "SRC_SYS_CD_SK",
        "APL_ID",
        "LVL_SEQ_NO",
        "NOTE_SEQ_NO",
        "NOTE_DEST_ID",
        "LN_SEQ_NO",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "APL_LVL_NOTE_SK",
        "LN_TX"
    )
    .union(
        df_defaultUNK.select(
            "APL_LVL_NOTE_LN_SK",
            "SRC_SYS_CD_SK",
            "APL_ID",
            "LVL_SEQ_NO",
            "NOTE_SEQ_NO",
            "NOTE_DEST_ID",
            "LN_SEQ_NO",
            "CRT_RUN_CYC_EXCTN_SK",
            "LAST_UPDT_RUN_CYC_EXCTN_SK",
            "APL_LVL_NOTE_SK",
            "LN_TX"
        )
    )
    .union(
        df_defaultNA.select(
            "APL_LVL_NOTE_LN_SK",
            "SRC_SYS_CD_SK",
            "APL_ID",
            "LVL_SEQ_NO",
            "NOTE_SEQ_NO",
            "NOTE_DEST_ID",
            "LN_SEQ_NO",
            "CRT_RUN_CYC_EXCTN_SK",
            "LAST_UPDT_RUN_CYC_EXCTN_SK",
            "APL_LVL_NOTE_SK",
            "LN_TX"
        )
    )
)

df_Recycle = (
    df_foreignKey
    .filter(col("ErrCount") > 0)
    .select(
        GetRecycleKey(col("APL_LVL_NOTE_LN_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
        rpad(col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
        rpad(col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
        rpad(col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
        col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        col("ErrCount").alias("ERR_CT"),
        (col("RECYCLE_CT") + 1).alias("RECYCLE_CT"),
        rpad(col("SRC_SYS_CD"), lit(<...>), " ").alias("SRC_SYS_CD"),
        rpad(col("PRI_KEY_STRING"), lit(<...>), " ").alias("PRI_KEY_STRING"),
        col("APL_LVL_NOTE_LN_SK").alias("APL_LVL_NOTE_LN_SK"),
        rpad(col("APL_ID"), lit(<...>), " ").alias("APL_ID"),
        col("LVL_SEQ_NO").alias("LVL_SEQ_NO"),
        col("NOTE_SEQ_NO").alias("NOTE_SEQ_NO"),
        rpad(col("NOTE_DEST_ID"), lit(<...>), " ").alias("NOTE_DEST_ID"),
        col("LN_SEQ_NO").alias("LN_SEQ_NO"),
        col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("APL_LVL_NOTE_SK").alias("APL_LVL_NOTE_SK"),
        rpad(col("LN_TX"), lit(<...>), " ").alias("LN_TX")
    )
)

write_files(
    df_Recycle,
    f"{adls_path}/hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

df_collector_final = (
    df_collector
    .withColumn("APL_ID", rpad(col("APL_ID"), lit(<...>), " "))
    .withColumn("NOTE_DEST_ID", rpad(col("NOTE_DEST_ID"), lit(<...>), " "))
    .withColumn("LN_TX", rpad(col("LN_TX"), lit(<...>), " "))
)

write_files(
    df_collector_final,
    f"{adls_path}/load/APL_LVL_NOTE_LN.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)