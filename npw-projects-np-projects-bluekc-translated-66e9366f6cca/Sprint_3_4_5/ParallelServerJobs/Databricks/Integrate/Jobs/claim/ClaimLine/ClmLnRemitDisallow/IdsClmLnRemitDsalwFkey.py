# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_3 03/10/09 09:25:21 Batch  15045_33943 INIT bckcetl ids20 dcg01 sa Bringing ALL Claim code down to devlIDS
# MAGIC ^1_3 02/10/09 11:16:45 Batch  15017_40611 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_2 02/02/09 09:52:49 Batch  15009_35578 PROMOTE bckcetl ids20 dsadm bls for brent
# MAGIC ^1_2 01/26/09 13:03:15 Batch  15002_46999 INIT bckcett testIDS dsadm bls for bl
# MAGIC ^1_2 12/16/08 10:00:18 Batch  14961_36031 PROMOTE bckcett testIDS u03651 steph for Brent - claim primary keying
# MAGIC ^1_2 12/16/08 09:15:37 Batch  14961_33343 INIT bckcett devlIDS u03651 steffy
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
# MAGIC DESCRIPTION:  Get foreign key as stated above and create file to load to CLM_LN_REMIT_DSALW
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
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


Source = get_widget_value("Source", "")
InFile = get_widget_value("InFile", "BcbsClmLnRemitDsalwExtr.ClmLnRemitDsalw.dat.20080730")
Logging = get_widget_value("Logging", "Y")
SrcSysCdSk = get_widget_value("SrcSysCdSk", "")

schema_ClmLnRemitDsalwExtr = T.StructType([
    T.StructField("JOB_EXCTN_RCRD_ERR_SK", T.IntegerType(), nullable=False),
    T.StructField("INSRT_UPDT_CD", T.StringType(), nullable=False),
    T.StructField("DISCARD_IN", T.StringType(), nullable=False),
    T.StructField("PASS_THRU_IN", T.StringType(), nullable=False),
    T.StructField("FIRST_RECYC_DT", T.TimestampType(), nullable=False),
    T.StructField("ERR_CT", T.IntegerType(), nullable=False),
    T.StructField("RECYCLE_CT", T.DecimalType(38, 10), nullable=False),
    T.StructField("SRC_SYS_CD", T.StringType(), nullable=False),
    T.StructField("PRI_KEY_STRING", T.StringType(), nullable=False),
    T.StructField("CLM_LN_REMIT_DSALW_SK", T.IntegerType(), nullable=False),
    T.StructField("CLM_ID", T.StringType(), nullable=False),
    T.StructField("CLM_LN_SEQ_NO", T.IntegerType(), nullable=False),
    T.StructField("CLM_LN_DSALW_TYP_CD", T.StringType(), nullable=False),
    T.StructField("BYPS_IN", T.StringType(), nullable=False),
    T.StructField("CRT_RUN_CYC_EXCTN_SK", T.IntegerType(), nullable=False),
    T.StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", T.IntegerType(), nullable=False),
    T.StructField("CLM_LN_SK", T.IntegerType(), nullable=False),
    T.StructField("CLM_LN_REMIT_DSALW_EXCD", T.StringType(), nullable=True),
    T.StructField("CLM_LN_RMT_DSW_EXCD_RESP_CD", T.StringType(), nullable=False),
    T.StructField("CLM_LN_RMT_DSALW_TYP_CAT_CD", T.StringType(), nullable=False),
    T.StructField("REMIT_DSALW_AMT", T.DecimalType(38, 10), nullable=False)
])

df_ClmLnRemitDsalwExtr = (
    spark.read
    .option("header", False)
    .option("quote", "\"")
    .option("delimiter", ",")
    .schema(schema_ClmLnRemitDsalwExtr)
    .csv(f"{adls_path}/key/{InFile}")
)

df_ForeignKey_vars = (
    df_ClmLnRemitDsalwExtr
    .withColumn("PassThru", F.col("PASS_THRU_IN"))
    .withColumn(
        "svClmLnDsalwTypCdSk",
        GetFkeyCodes(
            F.col("SRC_SYS_CD"),
            F.col("CLM_LN_REMIT_DSALW_SK"),
            F.lit("CLAIM LINE DISALLOW TYPE"),
            F.col("CLM_LN_DSALW_TYP_CD"),
            F.lit(Logging)
        )
    )
    .withColumn(
        "svClmLnDsalwTypCatCdSk",
        GetFkeyCodes(
            F.col("SRC_SYS_CD"),
            F.col("CLM_LN_REMIT_DSALW_SK"),
            F.lit("DISALLOW TYPE CATEGORY"),
            F.col("CLM_LN_DSALW_TYP_CD"),
            F.lit(Logging)
        )
    )
    .withColumn(
        "svClmLnSk",
        GetFkeyClmLn(
            F.col("SRC_SYS_CD"),
            F.col("CLM_LN_REMIT_DSALW_SK"),
            F.col("CLM_ID"),
            F.col("CLM_LN_SEQ_NO"),
            F.lit(Logging)
        )
    )
    .withColumn(
        "svClmLnRemitDsalwExcdSk",
        GetFkeyExcd(
            F.col("SRC_SYS_CD"),
            F.col("CLM_LN_REMIT_DSALW_SK"),
            F.col("CLM_LN_REMIT_DSALW_EXCD"),
            F.lit(Logging)
        )
    )
    .withColumn(
        "svClmLnRmtDswExcdRespCdSk",
        GetFkeyCodes(
            F.col("SRC_SYS_CD"),
            F.col("CLM_LN_REMIT_DSALW_SK"),
            F.lit("EXPLANATION CODE LIABILITY"),
            F.col("CLM_LN_RMT_DSW_EXCD_RESP_CD"),
            F.lit(Logging)
        )
    )
    .withColumn("ErrCount", GetFkeyErrorCnt(F.col("CLM_LN_REMIT_DSALW_SK")))
)

df_recycle = (
    df_ForeignKey_vars
    .filter(F.col("ErrCount") > 0)
    .select(
        F.expr("GetRecycleKey(CLM_LN_REMIT_DSALW_SK)").alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.col("INSRT_UPDT_CD"),
        F.col("DISCARD_IN"),
        F.col("PASS_THRU_IN"),
        F.col("FIRST_RECYC_DT"),
        F.col("ERR_CT"),
        (F.col("RECYCLE_CT") + F.lit(1)).alias("RECYCLE_CT"),
        F.col("SRC_SYS_CD"),
        F.col("PRI_KEY_STRING"),
        F.col("CLM_LN_REMIT_DSALW_SK"),
        F.col("CLM_ID"),
        F.col("CLM_LN_SEQ_NO"),
        F.col("CLM_LN_DSALW_TYP_CD"),
        F.col("BYPS_IN"),
        F.col("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("CLM_LN_SK"),
        F.col("CLM_LN_REMIT_DSALW_EXCD"),
        F.col("CLM_LN_RMT_DSW_EXCD_RESP_CD"),
        F.col("CLM_LN_RMT_DSALW_TYP_CAT_CD"),
        F.col("REMIT_DSALW_AMT")
    )
)

df_recycle_clms = (
    df_ForeignKey_vars
    .filter(F.col("ErrCount") > 0)
    .select(
        F.col("SRC_SYS_CD"),
        F.col("CLM_ID")
    )
)

df_Fkey = (
    df_ForeignKey_vars
    .filter((F.col("ErrCount") == 0) | (F.col("PassThru") == F.lit("Y")))
    .select(
        F.col("CLM_LN_REMIT_DSALW_SK"),
        F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
        F.col("CLM_ID"),
        F.col("CLM_LN_SEQ_NO"),
        F.col("svClmLnDsalwTypCdSk").alias("CLM_LN_DSALW_TYP_CD_SK"),
        F.col("BYPS_IN"),
        F.col("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("svClmLnSk").alias("CLM_LN_SK"),
        F.col("svClmLnRemitDsalwExcdSk").alias("CLM_LN_REMIT_DSALW_EXCD_SK"),
        F.col("svClmLnRmtDswExcdRespCdSk").alias("CLM_LN_RMT_DSW_EXCD_RESP_CD_SK"),
        F.col("svClmLnDsalwTypCatCdSk").alias("CLM_LN_RMT_DSALW_TYP_CAT_CD_SK"),
        F.col("REMIT_DSALW_AMT")
    )
)

w = Window.orderBy(F.lit(1))
df_numbered = df_ForeignKey_vars.withColumn("_row_num", F.row_number().over(w))

df_DefaultUNK = (
    df_numbered
    .filter(F.col("_row_num") == 1)
    .select(
        F.lit(0).alias("CLM_LN_REMIT_DSALW_SK"),
        F.lit(0).alias("SRC_SYS_CD_SK"),
        F.lit("UNK").alias("CLM_ID"),
        F.lit(0).alias("CLM_LN_SEQ_NO"),
        F.lit(0).alias("CLM_LN_DSALW_TYP_CD_SK"),
        F.lit("U").alias("BYPS_IN"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("CLM_LN_SK"),
        F.lit(0).alias("CLM_LN_REMIT_DSALW_EXCD_SK"),
        F.lit(0).alias("CLM_LN_RMT_DSW_EXCD_RESP_CD_SK"),
        F.lit(0).alias("CLM_LN_RMT_DSALW_TYP_CAT_CD_SK"),
        F.lit(0.00).alias("REMIT_DSALW_AMT")
    )
)

df_DefaultNA = (
    df_numbered
    .filter(F.col("_row_num") == 1)
    .select(
        F.lit(1).alias("CLM_LN_REMIT_DSALW_SK"),
        F.lit(1).alias("SRC_SYS_CD_SK"),
        F.lit("NA").alias("CLM_ID"),
        F.lit(1).alias("CLM_LN_SEQ_NO"),
        F.lit(1).alias("CLM_LN_DSALW_TYP_CD_SK"),
        F.lit("X").alias("BYPS_IN"),
        F.lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(1).alias("CLM_LN_SK"),
        F.lit(1).alias("CLM_LN_REMIT_DSALW_EXCD_SK"),
        F.lit(1).alias("CLM_LN_RMT_DSW_EXCD_RESP_CD_SK"),
        F.lit(1).alias("CLM_LN_RMT_DSALW_TYP_CAT_CD_SK"),
        F.lit(0.00).alias("REMIT_DSALW_AMT")
    )
)

df_recycle_final = (
    df_recycle
    .withColumn("INSRT_UPDT_CD", F.rpad(F.col("INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", F.rpad(F.col("DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", F.rpad(F.col("PASS_THRU_IN"), 1, " "))
    .withColumn("CLM_LN_DSALW_TYP_CD", F.rpad(F.col("CLM_LN_DSALW_TYP_CD"), 10, " "))
    .withColumn("BYPS_IN", F.rpad(F.col("BYPS_IN"), 1, " "))
    .withColumn("CLM_LN_REMIT_DSALW_EXCD", F.rpad(F.col("CLM_LN_REMIT_DSALW_EXCD"), 10, " "))
    .withColumn("CLM_LN_RMT_DSW_EXCD_RESP_CD", F.rpad(F.col("CLM_LN_RMT_DSW_EXCD_RESP_CD"), 10, " "))
    .withColumn("CLM_LN_RMT_DSALW_TYP_CAT_CD", F.rpad(F.col("CLM_LN_RMT_DSALW_TYP_CAT_CD"), 10, " "))
)

write_files(
    df_recycle_final.select(
        "JOB_EXCTN_RCRD_ERR_SK",
        "INSRT_UPDT_CD",
        "DISCARD_IN",
        "PASS_THRU_IN",
        "FIRST_RECYC_DT",
        "ERR_CT",
        "RECYCLE_CT",
        "SRC_SYS_CD",
        "PRI_KEY_STRING",
        "CLM_LN_REMIT_DSALW_SK",
        "CLM_ID",
        "CLM_LN_SEQ_NO",
        "CLM_LN_DSALW_TYP_CD",
        "BYPS_IN",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "CLM_LN_SK",
        "CLM_LN_REMIT_DSALW_EXCD",
        "CLM_LN_RMT_DSW_EXCD_RESP_CD",
        "CLM_LN_RMT_DSALW_TYP_CAT_CD",
        "REMIT_DSALW_AMT"
    ),
    f"{adls_path}/hf_recycle.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

write_files(
    df_recycle_clms,
    f"{adls_path}/hf_claim_recycle_keys.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

df_collector = df_DefaultUNK.unionByName(df_DefaultNA).unionByName(df_Fkey)

df_collector_final = df_collector.withColumn("BYPS_IN", F.rpad(F.col("BYPS_IN"), 1, " "))

write_files(
    df_collector_final.select(
        "CLM_LN_REMIT_DSALW_SK",
        "SRC_SYS_CD_SK",
        "CLM_ID",
        "CLM_LN_SEQ_NO",
        "CLM_LN_DSALW_TYP_CD_SK",
        "BYPS_IN",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "CLM_LN_SK",
        "CLM_LN_REMIT_DSALW_EXCD_SK",
        "CLM_LN_RMT_DSW_EXCD_RESP_CD_SK",
        "CLM_LN_RMT_DSALW_TYP_CAT_CD_SK",
        "REMIT_DSALW_AMT"
    ),
    f"{adls_path}/load/CLM_LN_REMIT_DSALW.{Source}.dat",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)