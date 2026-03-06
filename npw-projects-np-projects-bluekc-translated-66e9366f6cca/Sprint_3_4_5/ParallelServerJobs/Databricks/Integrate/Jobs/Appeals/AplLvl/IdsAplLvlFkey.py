# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
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
# MAGIC Bhoomi Dasari    07/11/2007                Initial program                                                                                   3028                  devlIDS30                          Steph Goddard           8/23/07
# MAGIC 
# MAGIC Ravi Singh          2018-10- 10                Updated stage variables for 3rd party Vendor                                   MTM-5841       IntegrateDev2	           Abhiram Dasarathy      2018-10-30      
# MAGIC                                                               Evicore(MEDSLTNS) data to load to IDS
# MAGIC 
# MAGIC Ravi Singh          2018-10- 25                Updated stage variables for 3rd party Vendor                                   MTM-5841       IntegrateDev2	                 Kalyan Neelam           2018-11-12
# MAGIC                                                               TELLIGEN data to load to IDS
# MAGIC 
# MAGIC Ravi Singh          2018-11- 30                Updated stage variables for 3rd party Vendor                                   MTM-5841       IntegrateDev2                        Kalyan Neelam             2018-12-10   
# MAGIC                                                               New Direction data to load to IDS

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
    DateType,
    DecimalType
)
from pyspark.sql.functions import (
    col,
    lit,
    when,
    row_number,
    to_timestamp,
    rpad
)
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


Logging = get_widget_value("Logging", "X")
InFile = get_widget_value("InFile", "")

schema = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), nullable=False),
    StructField("INSRT_UPDT_CD", StringType(), nullable=False),
    StructField("DISCARD_IN", StringType(), nullable=False),
    StructField("PASS_THRU_IN", StringType(), nullable=False),
    StructField("FIRST_RECYC_DT", TimestampType(), nullable=False),
    StructField("ERR_CT", IntegerType(), nullable=False),
    StructField("RECYCLE_CT", DecimalType(38,10), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("PRI_KEY_STRING", StringType(), nullable=False),
    StructField("APL_LVL_SK", IntegerType(), nullable=False),
    StructField("APL_ID", StringType(), nullable=False),
    StructField("SEQ_NO", IntegerType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("APL_SK", IntegerType(), nullable=False),
    StructField("CRT_USER_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_USER_SK", IntegerType(), nullable=False),
    StructField("PRI_USER_SK", IntegerType(), nullable=False),
    StructField("SEC_USER_SK", IntegerType(), nullable=False),
    StructField("TRTY_USER_SK", IntegerType(), nullable=False),
    StructField("APL_LVL_CD_SK", IntegerType(), nullable=False),
    StructField("APL_LVL_CUR_STTUS_CD_SK", IntegerType(), nullable=False),
    StructField("APL_LVL_DCSN_CD_SK", IntegerType(), nullable=False),
    StructField("APL_LVL_DCSN_RSN_CD_SK", IntegerType(), nullable=False),
    StructField("APL_LVL_DSPT_RSLTN_TYP_CD_SK", IntegerType(), nullable=False),
    StructField("APL_LVL_INITN_METH_CD_SK", IntegerType(), nullable=False),
    StructField("APL_LVL_LATE_DCSN_RSN_CD_SK", IntegerType(), nullable=False),
    StructField("APL_LVL_NTFCTN_CAT_CD_SK", IntegerType(), nullable=False),
    StructField("APL_LVL_NTFCTN_METH_CD_SK", IntegerType(), nullable=False),
    StructField("EXPDTD_IN", StringType(), nullable=False),
    StructField("HRNG_IN", StringType(), nullable=False),
    StructField("INITN_DT_SK", StringType(), nullable=False),
    StructField("CRT_DTM", TimestampType(), nullable=False),
    StructField("CUR_STTUS_DTM", DateType(), nullable=False),
    StructField("DCSN_DT_SK", StringType(), nullable=False),
    StructField("HRNG_DT_SK", StringType(), nullable=False),
    StructField("LAST_UPDT_DTM", TimestampType(), nullable=False),
    StructField("NTFCTN_DT_SK", StringType(), nullable=False),
    StructField("CUR_STTUS_SEQ_NO", IntegerType(), nullable=False),
    StructField("LVL_DESC", StringType(), nullable=False)
])

df_input = (
    spark.read.format("csv")
    .option("header", "false")
    .option("quote", "\"")
    .option("delimiter", ",")
    .schema(schema)
    .load(f"{adls_path}/key/{InFile}")
)

df_transformed = (
    df_input
    .withColumn("SrcSysCdSk", GetFkeyCodes("IDS", col("APL_LVL_SK"), "SOURCE SYSTEM", col("SRC_SYS_CD"), Logging))
    .withColumn("PassThru", col("PASS_THRU_IN"))
    .withColumn("svAplSk", GetFkeyApl(col("SRC_SYS_CD"), col("APL_LVL_SK"), col("APL_SK"), Logging))
    .withColumn(
        "svCrtUsrSk",
        when(
            (col("SRC_SYS_CD") == "MEDSLTNS") | (col("SRC_SYS_CD") == "NDBH"),
            col("CRT_USER_SK")
        ).otherwise(
            GetFkeyAppUsr(col("SRC_SYS_CD"), col("APL_LVL_SK"), col("LAST_UPDT_USER_SK"), Logging)
        )
    )
    .withColumn(
        "svLstUpdtUsrSk",
        when(
            (col("SRC_SYS_CD") == "MEDSLTNS") | (col("SRC_SYS_CD") == "NDBH"),
            col("CRT_USER_SK")
        ).otherwise(
            GetFkeyAppUsr(col("SRC_SYS_CD"), col("APL_LVL_SK"), col("LAST_UPDT_USER_SK"), Logging)
        )
    )
    .withColumn(
        "svPriUsrSk",
        when(
            col("SRC_SYS_CD") == "MEDSLTNS",
            col("PRI_USER_SK")
        ).otherwise(
            GetFkeyAppUsr(col("SRC_SYS_CD"), col("APL_LVL_SK"), col("PRI_USER_SK"), Logging)
        )
    )
    .withColumn("svSecUsrSk", GetFkeyAppUsr(col("SRC_SYS_CD"), col("APL_LVL_SK"), col("SEC_USER_SK"), Logging))
    .withColumn("svTrtyUsrSk", GetFkeyAppUsr(col("SRC_SYS_CD"), col("APL_LVL_SK"), col("TRTY_USER_SK"), Logging))
    .withColumn(
        "svAplLvlCdSk",
        when(
            col("SRC_SYS_CD") == "TELLIGEN",
            col("APL_LVL_CD_SK")
        ).otherwise(
            GetFkeyCodes(col("SRC_SYS_CD"), col("APL_LVL_SK"), "APPEAL LEVEL CODE", col("APL_LVL_CD_SK"), Logging)
        )
    )
    .withColumn(
        "svAplLvlCurSttsCdSk",
        GetFkeyCodes(col("SRC_SYS_CD"), col("APL_LVL_SK"), "APPEAL STATUS", col("APL_LVL_CUR_STTUS_CD_SK"), Logging)
    )
    .withColumn(
        "svAplLvlDcsnCdSk",
        GetFkeyCodes(col("SRC_SYS_CD"), col("APL_LVL_SK"), "APPEAL LEVEL DECISION", col("APL_LVL_DCSN_CD_SK"), Logging)
    )
    .withColumn(
        "svAplLvlDcsnRsnCdSk",
        when(
            (col("SRC_SYS_CD") == "MEDSLTNS") | (col("SRC_SYS_CD") == "TELLIGEN") | (col("SRC_SYS_CD") == "NDBH"),
            col("APL_LVL_DCSN_RSN_CD_SK")
        ).otherwise(
            GetFkeyCodes(
                col("SRC_SYS_CD"), col("APL_LVL_SK"),
                "APPEAL LEVEL DECISION REASON",
                col("APL_LVL_DCSN_RSN_CD_SK"),
                Logging
            )
        )
    )
    .withColumn(
        "svAplLvlDsptRsltTypCdSk",
        when(
            (col("SRC_SYS_CD") == "MEDSLTNS") | (col("SRC_SYS_CD") == "TELLIGEN") | (col("SRC_SYS_CD") == "NDBH"),
            col("APL_LVL_DSPT_RSLTN_TYP_CD_SK")
        ).otherwise(
            GetFkeyCodes(
                col("SRC_SYS_CD"), col("APL_LVL_SK"),
                "APPEAL LEVEL DISPUTE RESOLUTION TYPE",
                col("APL_LVL_DSPT_RSLTN_TYP_CD_SK"),
                Logging
            )
        )
    )
    .withColumn(
        "svAplLvlInitMethCdSk",
        when(
            (col("SRC_SYS_CD") == "TELLIGEN") | (col("SRC_SYS_CD") == "NDBH"),
            col("APL_LVL_INITN_METH_CD_SK")
        ).otherwise(
            GetFkeyCodes(
                col("SRC_SYS_CD"), col("APL_LVL_SK"),
                "APPEAL INITIATION METHOD",
                col("APL_LVL_INITN_METH_CD_SK"),
                Logging
            )
        )
    )
    .withColumn(
        "svAplLvlLtDcsnRsnCdSk",
        when(
            (col("SRC_SYS_CD") == "MEDSLTNS") | (col("SRC_SYS_CD") == "TELLIGEN") | (col("SRC_SYS_CD") == "NDBH"),
            col("APL_LVL_LATE_DCSN_RSN_CD_SK")
        ).otherwise(
            GetFkeyCodes(
                col("SRC_SYS_CD"), col("APL_LVL_SK"),
                "APPEAL LEVEL LATE DECISION REASON",
                col("APL_LVL_LATE_DCSN_RSN_CD_SK"),
                Logging
            )
        )
    )
    .withColumn(
        "svAplLvlntfcCtCdSk",
        when(
            (col("SRC_SYS_CD") == "TELLIGEN") | (col("SRC_SYS_CD") == "NDBH"),
            col("APL_LVL_NTFCTN_CAT_CD_SK")
        ).otherwise(
            GetFkeyCodes(
                col("SRC_SYS_CD"), col("APL_LVL_SK"),
                "APPEAL LEVEL NOTIFICATION CATEGORY",
                col("APL_LVL_NTFCTN_CAT_CD_SK"),
                Logging
            )
        )
    )
    .withColumn(
        "svAplLvlntfctnMethCdSk",
        when(
            (col("SRC_SYS_CD") == "TELLIGEN") | (col("SRC_SYS_CD") == "NDBH"),
            col("APL_LVL_NTFCTN_METH_CD_SK")
        ).otherwise(
            GetFkeyCodes(
                col("SRC_SYS_CD"), col("APL_LVL_SK"),
                "APPEAL LEVEL NOTIFICATION METHOD",
                col("APL_LVL_NTFCTN_METH_CD_SK"),
                Logging
            )
        )
    )
    .withColumn("svInitnDtSk", GetFkeyDate("IDS", col("APL_LVL_SK"), col("INITN_DT_SK"), Logging))
    .withColumn("svDcsnDtSk", GetFkeyDate("IDS", col("APL_LVL_SK"), col("DCSN_DT_SK"), Logging))
    .withColumn("svHrgnDtSk", GetFkeyDate("IDS", col("APL_LVL_SK"), col("HRNG_DT_SK"), Logging))
    .withColumn("svNtfctnDtSk", GetFkeyDate("IDS", col("APL_LVL_SK"), col("NTFCTN_DT_SK"), Logging))
    .withColumn("ErrCount", GetFkeyErrorCnt(col("APL_LVL_SK")))
)

df_fkey = df_transformed.filter((col("ErrCount") == 0) | (col("PassThru") == "Y"))
df_recycle = df_transformed.filter(col("ErrCount") > 0)

df_recycle_out = df_recycle.select(
    GetRecycleKey(col("APL_LVL_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
    col("INSRT_UPDT_CD"),
    col("DISCARD_IN"),
    col("PASS_THRU_IN"),
    col("FIRST_RECYC_DT"),
    col("ErrCount").alias("ERR_CT"),
    (col("RECYCLE_CT") + lit(1)).alias("RECYCLE_CT"),
    col("SRC_SYS_CD"),
    col("PRI_KEY_STRING"),
    col("APL_LVL_SK"),
    col("APL_ID"),
    col("SEQ_NO"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("APL_SK"),
    col("CRT_USER_SK"),
    col("LAST_UPDT_USER_SK"),
    col("PRI_USER_SK"),
    col("SEC_USER_SK"),
    col("TRTY_USER_SK"),
    col("APL_LVL_CD_SK"),
    col("APL_LVL_CUR_STTUS_CD_SK"),
    col("APL_LVL_DCSN_CD_SK"),
    col("APL_LVL_DCSN_RSN_CD_SK"),
    col("APL_LVL_DSPT_RSLTN_TYP_CD_SK"),
    col("APL_LVL_INITN_METH_CD_SK"),
    col("APL_LVL_LATE_DCSN_RSN_CD_SK"),
    col("APL_LVL_NTFCTN_CAT_CD_SK"),
    col("APL_LVL_NTFCTN_METH_CD_SK"),
    col("EXPDTD_IN"),
    col("HRNG_IN"),
    col("INITN_DT_SK"),
    col("CRT_DTM"),
    col("CUR_STTUS_DTM"),
    col("DCSN_DT_SK"),
    col("HRNG_DT_SK"),
    col("LAST_UPDT_DTM"),
    col("NTFCTN_DT_SK"),
    col("CUR_STTUS_SEQ_NO"),
    col("LVL_DESC")
)

write_files(
    df_recycle_out,
    "hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

w = Window.orderBy(lit(1))
df_with_inrownum = df_transformed.withColumn("_rownum", row_number().over(w)).cache()

df_defaultunk = df_with_inrownum.filter(col("_rownum") == 1).select(
    lit(0).alias("APL_LVL_SK"),
    lit(0).alias("SRC_SYS_CD_SK"),
    lit("UNK").alias("APL_ID"),
    lit(0).alias("SEQ_NO"),
    lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("APL_SK"),
    lit(0).alias("CRT_USER_SK"),
    lit(0).alias("LAST_UPDT_USER_SK"),
    lit(0).alias("PRI_USER_SK"),
    lit(0).alias("SEC_USER_SK"),
    lit(0).alias("TRTY_USER_SK"),
    lit(0).alias("APL_LVL_CD_SK"),
    lit(0).alias("APL_LVL_CUR_STTUS_CD_SK"),
    lit(0).alias("APL_LVL_DCSN_CD_SK"),
    lit(0).alias("APL_LVL_DCSN_RSN_CD_SK"),
    lit(0).alias("APL_LVL_DSPT_RSLTN_TYP_CD_SK"),
    lit(0).alias("APL_LVL_INITN_METH_CD_SK"),
    lit(0).alias("APL_LVL_LATE_DCSN_RSN_CD_SK"),
    lit(0).alias("APL_LVL_NTFCTN_CAT_CD_SK"),
    lit(0).alias("APL_LVL_NTFCTN_METH_CD_SK"),
    lit("U").alias("EXPDTD_IN"),
    lit("U").alias("HRNG_IN"),
    lit("UNK").alias("INITN_DT_SK"),
    to_timestamp(lit("1753-01-01"), "yyyy-MM-dd").alias("CRT_DTM"),
    lit("1753-01-01").alias("CUR_STTUS_DTM"),
    lit("UNK").alias("DCSN_DT_SK"),
    lit("UNK").alias("HRNG_DT_SK"),
    to_timestamp(lit("1753-01-01"), "yyyy-MM-dd").alias("LAST_UPDT_DTM"),
    lit("UNK").alias("NTFCTN_DT_SK"),
    lit(0).alias("CUR_STTUS_SEQ_NO"),
    lit("UNK").alias("LVL_DESC")
)

df_defaultna = df_with_inrownum.filter(col("_rownum") == 1).select(
    lit(1).alias("APL_LVL_SK"),
    lit(1).alias("SRC_SYS_CD_SK"),
    lit("NA").alias("APL_ID"),
    lit(1).alias("SEQ_NO"),
    lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit(1).alias("APL_SK"),
    lit(1).alias("CRT_USER_SK"),
    lit(1).alias("LAST_UPDT_USER_SK"),
    lit(1).alias("PRI_USER_SK"),
    lit(1).alias("SEC_USER_SK"),
    lit(1).alias("TRTY_USER_SK"),
    lit(1).alias("APL_LVL_CD_SK"),
    lit(1).alias("APL_LVL_CUR_STTUS_CD_SK"),
    lit(1).alias("APL_LVL_DCSN_CD_SK"),
    lit(1).alias("APL_LVL_DCSN_RSN_CD_SK"),
    lit(1).alias("APL_LVL_DSPT_RSLTN_TYP_CD_SK"),
    lit(1).alias("APL_LVL_INITN_METH_CD_SK"),
    lit(1).alias("APL_LVL_LATE_DCSN_RSN_CD_SK"),
    lit(1).alias("APL_LVL_NTFCTN_CAT_CD_SK"),
    lit(1).alias("APL_LVL_NTFCTN_METH_CD_SK"),
    lit("X").alias("EXPDTD_IN"),
    lit("X").alias("HRNG_IN"),
    lit("NA").alias("INITN_DT_SK"),
    to_timestamp(lit("1753-01-01"), "yyyy-MM-dd").alias("CRT_DTM"),
    lit("1753-01-01").alias("CUR_STTUS_DTM"),
    lit("NA").alias("DCSN_DT_SK"),
    lit("NA").alias("HRNG_DT_SK"),
    to_timestamp(lit("1753-01-01"), "yyyy-MM-dd").alias("LAST_UPDT_DTM"),
    lit("NA").alias("NTFCTN_DT_SK"),
    lit(1).alias("CUR_STTUS_SEQ_NO"),
    lit("NA").alias("LVL_DESC")
)

df_fkey_out = df_fkey.select(
    col("APL_LVL_SK"),
    col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
    col("APL_ID"),
    col("SEQ_NO"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("svAplSk").alias("APL_SK"),
    col("svCrtUsrSk").alias("CRT_USER_SK"),
    col("svLstUpdtUsrSk").alias("LAST_UPDT_USER_SK"),
    col("svPriUsrSk").alias("PRI_USER_SK"),
    col("svSecUsrSk").alias("SEC_USER_SK"),
    col("svTrtyUsrSk").alias("TRTY_USER_SK"),
    col("svAplLvlCdSk").alias("APL_LVL_CD_SK"),
    col("svAplLvlCurSttsCdSk").alias("APL_LVL_CUR_STTUS_CD_SK"),
    col("svAplLvlDcsnCdSk").alias("APL_LVL_DCSN_CD_SK"),
    col("svAplLvlDcsnRsnCdSk").alias("APL_LVL_DCSN_RSN_CD_SK"),
    col("svAplLvlDsptRsltTypCdSk").alias("APL_LVL_DSPT_RSLTN_TYP_CD_SK"),
    col("svAplLvlInitMethCdSk").alias("APL_LVL_INITN_METH_CD_SK"),
    col("svAplLvlLtDcsnRsnCdSk").alias("APL_LVL_LATE_DCSN_RSN_CD_SK"),
    col("svAplLvlntfcCtCdSk").alias("APL_LVL_NTFCTN_CAT_CD_SK"),
    col("svAplLvlntfctnMethCdSk").alias("APL_LVL_NTFCTN_METH_CD_SK"),
    col("EXPDTD_IN"),
    col("HRNG_IN"),
    col("svInitnDtSk").alias("INITN_DT_SK"),
    col("CRT_DTM"),
    col("CUR_STTUS_DTM"),
    col("svDcsnDtSk").alias("DCSN_DT_SK"),
    col("svHrgnDtSk").alias("HRNG_DT_SK"),
    col("LAST_UPDT_DTM"),
    col("svNtfctnDtSk").alias("NTFCTN_DT_SK"),
    col("CUR_STTUS_SEQ_NO"),
    col("LVL_DESC")
)

df_collector = (
    df_fkey_out
    .unionByName(df_defaultunk)
    .unionByName(df_defaultna)
)

df_final = df_collector.select(
    rpad(col("APL_LVL_SK").cast(StringType()), 0, " ").cast(IntegerType()).alias("APL_LVL_SK"),
    rpad(col("SRC_SYS_CD_SK").cast(StringType()), 0, " ").cast(IntegerType()).alias("SRC_SYS_CD_SK"),
    col("APL_ID"),  
    col("SEQ_NO"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("APL_SK"),
    col("CRT_USER_SK"),
    col("LAST_UPDT_USER_SK"),
    col("PRI_USER_SK"),
    col("SEC_USER_SK"),
    col("TRTY_USER_SK"),
    col("APL_LVL_CD_SK"),
    col("APL_LVL_CUR_STTUS_CD_SK"),
    col("APL_LVL_DCSN_CD_SK"),
    col("APL_LVL_DCSN_RSN_CD_SK"),
    col("APL_LVL_DSPT_RSLTN_TYP_CD_SK"),
    col("APL_LVL_INITN_METH_CD_SK"),
    col("APL_LVL_LATE_DCSN_RSN_CD_SK"),
    col("APL_LVL_NTFCTN_CAT_CD_SK"),
    col("APL_LVL_NTFCTN_METH_CD_SK"),
    rpad(col("EXPDTD_IN"), 1, " ").alias("EXPDTD_IN"),
    rpad(col("HRNG_IN"), 1, " ").alias("HRNG_IN"),
    rpad(col("INITN_DT_SK"), 10, " ").alias("INITN_DT_SK"),
    col("CRT_DTM"),
    col("CUR_STTUS_DTM"),
    rpad(col("DCSN_DT_SK"), 10, " ").alias("DCSN_DT_SK"),
    rpad(col("HRNG_DT_SK"), 10, " ").alias("HRNG_DT_SK"),
    col("LAST_UPDT_DTM"),
    rpad(col("NTFCTN_DT_SK"), 10, " ").alias("NTFCTN_DT_SK"),
    col("CUR_STTUS_SEQ_NO"),
    col("LVL_DESC")
)

write_files(
    df_final,
    f"{adls_path}/load/APL_LVL.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)