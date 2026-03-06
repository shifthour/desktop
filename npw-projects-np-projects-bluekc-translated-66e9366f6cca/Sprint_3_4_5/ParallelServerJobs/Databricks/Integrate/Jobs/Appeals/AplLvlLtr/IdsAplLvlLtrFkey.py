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
# MAGIC 
# MAGIC ***************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2006 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:   Takes the primary key file and applies the foreign keys.   
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer             Date                         Change Description                                                                           Project #          Development Project          Code Reviewer          Date Reviewed  
# MAGIC ------------------           ----------------------------     -----------------------------------------------------------------------                                   ----------------         ------------------------------------       ----------------------------      ----------------
# MAGIC Steph Goddard     07/11/2007              Initial program                                                                                                             devlIDS30                           Brent Leland              08-20-2007

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
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    TimestampType,
    DecimalType
)
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


Logging = get_widget_value("Logging", "X")
InFile = get_widget_value("InFile", "FctsAplLvlLtrExtr.AplLvlLtr.dat.2007010105")

schema_IdsAplLvlLtrExtr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False),
    StructField("INSRT_UPDT_CD", StringType(), False),
    StructField("DISCARD_IN", StringType(), False),
    StructField("PASS_THRU_IN", StringType(), False),
    StructField("FIRST_RECYC_DT", TimestampType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", DecimalType(38, 10), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PRI_KEY_STRING", StringType(), False),
    StructField("APL_LVL_LTR_SK", IntegerType(), False),
    StructField("SRC_SYS_CD_SK", IntegerType(), False),
    StructField("APL_ID", StringType(), False),
    StructField("SEQ_NO", IntegerType(), False),
    StructField("APL_LVL_LTR_STYLE_CD", StringType(), False),
    StructField("LTR_SEQ_NO", IntegerType(), False),
    StructField("LTR_DEST_ID", StringType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("APL_LVL_SK", IntegerType(), False),
    StructField("APL_LVL_LTR_TYP_CD", StringType(), False),
    StructField("RQST_DT_SK", StringType(), False),
    StructField("ADDREE_NM", StringType(), True),
    StructField("ADDREE_ADDR_LN_1", StringType(), True),
    StructField("ADDREE_ADDR_LN_2", StringType(), True),
    StructField("ADDREE_CITY_NM", StringType(), True),
    StructField("APL_LVL_ADDREE_ST_CD", StringType(), False),
    StructField("ADDREE_POSTAL_CD", StringType(), True),
    StructField("ADDREE_CNTY_NM", StringType(), True),
    StructField("ADDREE_PHN_NO", StringType(), True),
    StructField("ADDREE_FAX_NO", StringType(), True),
    StructField("EXPL_TX", StringType(), True),
    StructField("LTR_TX_1", StringType(), True),
    StructField("LTR_TX_2", StringType(), True),
    StructField("LTR_TX_3", StringType(), True),
    StructField("LTR_TX_4", StringType(), True),
    StructField("SUBJ_TX", StringType(), True)
])

df_IdsAplLvlLtrExtr = (
    spark.read
    .option("header", False)
    .option("inferSchema", False)
    .option("quote", '"')
    .option("sep", ",")
    .schema(schema_IdsAplLvlLtrExtr)
    .csv(f"{adls_path}/key/{InFile}")
)

df_ForeignKeyVars = (
    df_IdsAplLvlLtrExtr
    .withColumn("SrcSysCdSk", GetFkeyCodes(F.lit("IDS"), F.col("APL_LVL_LTR_SK"), F.lit("SOURCE SYSTEM"), F.col("SRC_SYS_CD"), Logging))
    .withColumn("PassThru", F.col("PASS_THRU_IN"))
    .withColumn("svRqstDtSk", GetFkeyDate(F.lit("IDS"), F.col("APL_LVL_LTR_SK"), F.col("RQST_DT_SK"), Logging))
    .withColumn("svAplStyleCdSk", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("APL_LVL_LTR_SK"), F.lit("ATTACHMENT TYPE"), F.col("APL_LVL_LTR_STYLE_CD"), Logging))
    .withColumn("svAplLtrTypCdSk", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("APL_LVL_LTR_SK"), F.lit("CLAIM LETTER TYPE"), F.col("APL_LVL_LTR_TYP_CD"), Logging))
    .withColumn("svAplLvlSk", GetFkeyAplLvl(F.col("SRC_SYS_CD"), F.col("APL_LVL_LTR_SK"), F.col("APL_ID"), F.col("SEQ_NO"), Logging))
    .withColumn("svStCdSk", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("APL_LVL_LTR_SK"), F.lit("STATE"), F.col("APL_LVL_ADDREE_ST_CD"), Logging))
    .withColumn("ErrCount", GetFkeyErrorCnt(F.col("APL_LVL_LTR_SK")))
)

df_fkey = (
    df_ForeignKeyVars
    .filter((F.col("ErrCount") == 0) | (F.col("PassThru") == "Y"))
    .select(
        F.col("APL_LVL_LTR_SK").alias("APL_LVL_LTR_SK"),
        F.col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
        F.col("APL_ID").alias("APL_ID"),
        F.col("SEQ_NO").alias("SEQ_NO"),
        F.col("svAplStyleCdSk").alias("APL_LVL_LTR_STYLE_CD_SK"),
        F.col("LTR_SEQ_NO").alias("LTR_SEQ_NO"),
        F.col("LTR_DEST_ID").alias("LTR_DEST_ID"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("svAplLvlSk").alias("APL_LVL_SK"),
        F.col("svAplLtrTypCdSk").alias("APL_LVL_LTR_TYP_CD_SK"),
        F.col("svRqstDtSk").alias("RQST_DT_SK"),
        F.col("ADDREE_NM").alias("ADDREE_NM"),
        F.col("ADDREE_ADDR_LN_1").alias("ADDREE_ADDR_LN_1"),
        F.col("ADDREE_ADDR_LN_2").alias("ADDREE_ADDR_LN_2"),
        F.col("ADDREE_CITY_NM").alias("ADDREE_CITY_NM"),
        F.col("svStCdSk").alias("APL_LVL_ADDREE_ST_CD_SK"),
        F.col("ADDREE_POSTAL_CD").alias("ADDREE_POSTAL_CD"),
        F.col("ADDREE_CNTY_NM").alias("ADDREE_CNTY_NM"),
        F.col("ADDREE_PHN_NO").alias("ADDREE_PHN_NO"),
        F.col("ADDREE_FAX_NO").alias("ADDREE_FAX_NO"),
        F.col("EXPL_TX").alias("EXPL_TX"),
        F.col("LTR_TX_1").alias("LTR_TX_1"),
        F.col("LTR_TX_2").alias("LTR_TX_2"),
        F.col("LTR_TX_3").alias("LTR_TX_3"),
        F.col("LTR_TX_4").alias("LTR_TX_4"),
        F.col("SUBJ_TX").alias("SUBJ_TX")
    )
)

df_recycle_prep = df_ForeignKeyVars.filter(F.col("ErrCount") > 0)

df_recycle = (
    df_recycle_prep
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", GetRecycleKey(F.col("APL_LVL_LTR_SK")))
    .withColumn("ERR_CT", F.col("ErrCount"))
    .withColumn("RECYCLE_CT", F.col("RECYCLE_CT") + F.lit(1))
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
        F.col("APL_LVL_LTR_SK").alias("APL_LVL_LTR_SK"),
        F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.col("APL_ID").alias("APL_ID"),
        F.col("SEQ_NO").alias("SEQ_NO"),
        F.col("APL_LVL_LTR_STYLE_CD").alias("APL_LVL_LTR_STYLE_CD"),
        F.col("LTR_SEQ_NO").alias("LTR_SEQ_NO"),
        F.col("LTR_DEST_ID").alias("LTR_DEST_ID"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("APL_LVL_SK").alias("APL_LVL_SK"),
        F.col("APL_LVL_LTR_TYP_CD").alias("APL_LVL_LTR_TYP_CD"),
        F.col("RQST_DT_SK").alias("RQST_DT_SK"),
        F.col("ADDREE_NM").alias("ADDREE_NM"),
        F.col("ADDREE_ADDR_LN_1").alias("ADDREE_ADDR_LN_1"),
        F.col("ADDREE_ADDR_LN_2").alias("ADDREE_ADDR_LN_2"),
        F.col("ADDREE_CITY_NM").alias("ADDREE_CITY_NM"),
        F.col("APL_LVL_ADDREE_ST_CD").alias("APL_LVL_ADDREE_ST_CD"),
        F.col("ADDREE_POSTAL_CD").alias("ADDREE_POSTAL_CD"),
        F.col("ADDREE_CNTY_NM").alias("ADDREE_CNTY_NM"),
        F.col("ADDREE_PHN_NO").alias("ADDREE_PHN_NO"),
        F.col("ADDREE_FAX_NO").alias("ADDREE_FAX_NO"),
        F.col("EXPL_TX").alias("EXPL_TX"),
        F.col("LTR_TX_1").alias("LTR_TX_1"),
        F.col("LTR_TX_2").alias("LTR_TX_2"),
        F.col("LTR_TX_3").alias("LTR_TX_3"),
        F.col("LTR_TX_4").alias("LTR_TX_4"),
        F.col("SUBJ_TX").alias("SUBJ_TX")
    )
)

write_files(
    df_recycle.select(
        "JOB_EXCTN_RCRD_ERR_SK",
        "INSRT_UPDT_CD",
        "DISCARD_IN",
        "PASS_THRU_IN",
        "FIRST_RECYC_DT",
        "ERR_CT",
        "RECYCLE_CT",
        "SRC_SYS_CD",
        "PRI_KEY_STRING",
        "APL_LVL_LTR_SK",
        "SRC_SYS_CD_SK",
        "APL_ID",
        "SEQ_NO",
        "APL_LVL_LTR_STYLE_CD",
        "LTR_SEQ_NO",
        "LTR_DEST_ID",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "APL_LVL_SK",
        "APL_LVL_LTR_TYP_CD",
        "RQST_DT_SK",
        "ADDREE_NM",
        "ADDREE_ADDR_LN_1",
        "ADDREE_ADDR_LN_2",
        "ADDREE_CITY_NM",
        "APL_LVL_ADDREE_ST_CD",
        "ADDREE_POSTAL_CD",
        "ADDREE_CNTY_NM",
        "ADDREE_PHN_NO",
        "ADDREE_FAX_NO",
        "EXPL_TX",
        "LTR_TX_1",
        "LTR_TX_2",
        "LTR_TX_3",
        "LTR_TX_4",
        "SUBJ_TX"
    ),
    f"{adls_path}/hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote='"',
    nullValue=None
)

df_defaultUNK = spark.createDataFrame(
    [
        (
            0, 0, "UNK", 0, 0, 0, "UNK", 0, 0, 0, 0, "UNK", "UNK", "UNK", "UNK", "UNK",
            0, "UNK", "UNK", "UNK", "UNK", "UNK", "UNK", "UNK", "UNK", "UNK", "UNK"
        )
    ],
    [
        "APL_LVL_LTR_SK",
        "SRC_SYS_CD_SK",
        "APL_ID",
        "SEQ_NO",
        "APL_LVL_LTR_STYLE_CD_SK",
        "LTR_SEQ_NO",
        "LTR_DEST_ID",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "APL_LVL_SK",
        "APL_LVL_LTR_TYP_CD_SK",
        "RQST_DT_SK",
        "ADDREE_NM",
        "ADDREE_ADDR_LN_1",
        "ADDREE_ADDR_LN_2",
        "ADDREE_CITY_NM",
        "APL_LVL_ADDREE_ST_CD_SK",
        "ADDREE_POSTAL_CD",
        "ADDREE_CNTY_NM",
        "ADDREE_PHN_NO",
        "ADDREE_FAX_NO",
        "EXPL_TX",
        "LTR_TX_1",
        "LTR_TX_2",
        "LTR_TX_3",
        "LTR_TX_4",
        "SUBJ_TX"
    ]
)

df_defaultNA = spark.createDataFrame(
    [
        (
            1, 1, "NA", 0, 1, 0, "NA", 1, 1, 1, 1, "NA", "NA", "NA", "NA", "NA",
            1, "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA", "NA"
        )
    ],
    [
        "APL_LVL_LTR_SK",
        "SRC_SYS_CD_SK",
        "APL_ID",
        "SEQ_NO",
        "APL_LVL_LTR_STYLE_CD_SK",
        "LTR_SEQ_NO",
        "LTR_DEST_ID",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "APL_LVL_SK",
        "APL_LVL_LTR_TYP_CD_SK",
        "RQST_DT_SK",
        "ADDREE_NM",
        "ADDREE_ADDR_LN_1",
        "ADDREE_ADDR_LN_2",
        "ADDREE_CITY_NM",
        "APL_LVL_ADDREE_ST_CD_SK",
        "ADDREE_POSTAL_CD",
        "ADDREE_CNTY_NM",
        "ADDREE_PHN_NO",
        "ADDREE_FAX_NO",
        "EXPL_TX",
        "LTR_TX_1",
        "LTR_TX_2",
        "LTR_TX_3",
        "LTR_TX_4",
        "SUBJ_TX"
    ]
)

df_collector = (
    df_fkey
    .unionByName(df_defaultUNK)
    .unionByName(df_defaultNA)
)

df_final = (
    df_collector
    .withColumn("APL_ID", F.rpad(F.col("APL_ID"), <...>, " "))
    .withColumn("LTR_DEST_ID", F.rpad(F.col("LTR_DEST_ID"), <...>, " "))
    .withColumn("RQST_DT_SK", F.rpad(F.col("RQST_DT_SK"), 10, " "))
    .withColumn("ADDREE_NM", F.rpad(F.col("ADDREE_NM"), <...>, " "))
    .withColumn("ADDREE_ADDR_LN_1", F.rpad(F.col("ADDREE_ADDR_LN_1"), <...>, " "))
    .withColumn("ADDREE_ADDR_LN_2", F.rpad(F.col("ADDREE_ADDR_LN_2"), <...>, " "))
    .withColumn("ADDREE_CITY_NM", F.rpad(F.col("ADDREE_CITY_NM"), <...>, " "))
    .withColumn("ADDREE_POSTAL_CD", F.rpad(F.col("ADDREE_POSTAL_CD"), <...>, " "))
    .withColumn("ADDREE_CNTY_NM", F.rpad(F.col("ADDREE_CNTY_NM"), <...>, " "))
    .withColumn("ADDREE_PHN_NO", F.rpad(F.col("ADDREE_PHN_NO"), <...>, " "))
    .withColumn("ADDREE_FAX_NO", F.rpad(F.col("ADDREE_FAX_NO"), <...>, " "))
    .withColumn("EXPL_TX", F.rpad(F.col("EXPL_TX"), <...>, " "))
    .withColumn("LTR_TX_1", F.rpad(F.col("LTR_TX_1"), <...>, " "))
    .withColumn("LTR_TX_2", F.rpad(F.col("LTR_TX_2"), <...>, " "))
    .withColumn("LTR_TX_3", F.rpad(F.col("LTR_TX_3"), <...>, " "))
    .withColumn("LTR_TX_4", F.rpad(F.col("LTR_TX_4"), <...>, " "))
    .withColumn("SUBJ_TX", F.rpad(F.col("SUBJ_TX"), <...>, " "))
)

df_final_select = df_final.select(
    "APL_LVL_LTR_SK",
    "SRC_SYS_CD_SK",
    "APL_ID",
    "SEQ_NO",
    "APL_LVL_LTR_STYLE_CD_SK",
    "LTR_SEQ_NO",
    "LTR_DEST_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "APL_LVL_SK",
    "APL_LVL_LTR_TYP_CD_SK",
    "RQST_DT_SK",
    "ADDREE_NM",
    "ADDREE_ADDR_LN_1",
    "ADDREE_ADDR_LN_2",
    "ADDREE_CITY_NM",
    "APL_LVL_ADDREE_ST_CD_SK",
    "ADDREE_POSTAL_CD",
    "ADDREE_CNTY_NM",
    "ADDREE_PHN_NO",
    "ADDREE_FAX_NO",
    "EXPL_TX",
    "LTR_TX_1",
    "LTR_TX_2",
    "LTR_TX_3",
    "LTR_TX_4",
    "SUBJ_TX"
)

write_files(
    df_final_select,
    f"{adls_path}/load/APL_LVL_LTR.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)