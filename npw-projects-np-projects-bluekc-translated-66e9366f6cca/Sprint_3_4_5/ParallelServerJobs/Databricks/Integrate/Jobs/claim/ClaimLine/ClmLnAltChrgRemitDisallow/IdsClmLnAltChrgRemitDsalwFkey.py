# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_2 09/22/09 15:32:09 Batch  15241_56279 PROMOTE bckcetl:31540 ids20 dsadm bls for sa
# MAGIC ^1_2 09/22/09 15:22:02 Batch  15241_55325 INIT bckcett:31540 testIDSnew dsadm bls for sa
# MAGIC ^1_1 09/22/09 09:19:44 Batch  15241_33586 INIT bckcett:31540 testIDSnew dsadm bls for sa
# MAGIC ^1_2 07/06/09 11:52:09 Batch  15163_42760 PROMOTE bckcett:31540 testIDSnew u150906 3833-RemitAlternateCharge_Sharon_testIDSnew             Maddy
# MAGIC ^1_2 07/06/09 11:37:10 Batch  15163_41931 INIT bckcett:31540 devlIDSnew u150906 3833-RemitAlternateCharge_Sharon_devlIDSnew                  Maddy
# MAGIC 
# MAGIC 
# MAGIC ************************************************************************************
# MAGIC COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC ************************************************************************************
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  BCBSClmLnRemitLoadSeq
# MAGIC 
# MAGIC DESCRIPTION:  This job fkeys and builds load file for CLM_LN_ALT_CHRG
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer           Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------         --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------    
# MAGIC SAndrew             2009-06-10      3833 Remit             new program                                                                                  devlIDSnew                 Steph Goddard            07/02/2009                                    
# MAGIC                                                    Alternate Chrg

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
from pyspark.sql.functions import col, lit, row_number, monotonically_increasing_id, rpad
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, DecimalType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


Logging = get_widget_value('Logging','Y')
Source = get_widget_value('Source','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
InFile = get_widget_value('InFile','BcbsClmLnRemitDsalwExtr.ClmLnRemitDsalw.dat.20080730')

schema_ClmLnAltChrgRemitDsalwExtr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), nullable=False),
    StructField("INSRT_UPDT_CD", StringType(), nullable=False),
    StructField("DISCARD_IN", StringType(), nullable=False),
    StructField("PASS_THRU_IN", StringType(), nullable=False),
    StructField("FIRST_RECYC_DT", TimestampType(), nullable=False),
    StructField("ERR_CT", IntegerType(), nullable=False),
    StructField("RECYCLE_CT", DecimalType(), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("PRI_KEY_STRING", StringType(), nullable=False),
    StructField("CLM_LN_REMIT_DSALW_SK", IntegerType(), nullable=False),
    StructField("CLM_ID", StringType(), nullable=False),
    StructField("CLM_LN_SEQ_NO", IntegerType(), nullable=False),
    StructField("CLM_LN_DSALW_TYP_CD", StringType(), nullable=False),
    StructField("BYPS_IN", StringType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("CLM_LN_SK", IntegerType(), nullable=False),
    StructField("CLM_LN_REMIT_DSALW_EXCD", StringType(), nullable=True),
    StructField("CLM_LN_RMT_DSW_EXCD_RESP_CD", StringType(), nullable=False),
    StructField("CLM_LN_RMT_DSALW_TYP_CAT_CD", StringType(), nullable=False),
    StructField("REMIT_DSALW_AMT", DecimalType(), nullable=False)
])

df_ClmLnAltChrgRemitDsalwExtr = (
    spark.read
    .option("header", False)
    .option("sep", ",")
    .option("quote", "\"")
    .schema(schema_ClmLnAltChrgRemitDsalwExtr)
    .csv(f"{adls_path}/key/{InFile}")
)

df_with_stagevars = (
    df_ClmLnAltChrgRemitDsalwExtr
    .withColumn("PassThru", col("PASS_THRU_IN"))
    .withColumn("svClmLnDsalwTypCdSk", GetFkeyCodes(col("SRC_SYS_CD"), col("CLM_LN_REMIT_DSALW_SK"), lit("CLAIM LINE DISALLOW TYPE"), col("CLM_LN_DSALW_TYP_CD"), Logging))
    .withColumn("svClmLnDsalwTypCatCdSk", GetFkeyCodes(col("SRC_SYS_CD"), col("CLM_LN_REMIT_DSALW_SK"), lit("DISALLOW TYPE CATEGORY"), col("CLM_LN_DSALW_TYP_CD"), Logging))
    .withColumn("svClmLnSk", GetFkeyClmLn(col("SRC_SYS_CD"), col("CLM_LN_REMIT_DSALW_SK"), col("CLM_ID"), col("CLM_LN_SEQ_NO"), Logging))
    .withColumn("svClmLnRemitDsalwExcdSk", GetFkeyExcd(col("SRC_SYS_CD"), col("CLM_LN_REMIT_DSALW_SK"), col("CLM_LN_REMIT_DSALW_EXCD"), Logging))
    .withColumn("svClmLnRmtDswExcdRespCdSk", GetFkeyCodes(col("SRC_SYS_CD"), col("CLM_LN_REMIT_DSALW_SK"), lit("EXPLANATION CODE LIABILITY"), col("CLM_LN_RMT_DSW_EXCD_RESP_CD"), Logging))
    .withColumn("ErrCount", GetFkeyErrorCnt(col("CLM_LN_REMIT_DSALW_SK")))
    .withColumn("row_num", row_number().over(Window.orderBy(monotonically_increasing_id())))
)

df_recycle = (
    df_with_stagevars
    .filter(col("ErrCount") > 0)
    .select(
        GetRecycleKey(col("CLM_LN_REMIT_DSALW_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
        col("INSRT_UPDT_CD"),
        col("DISCARD_IN"),
        col("PASS_THRU_IN"),
        col("FIRST_RECYC_DT"),
        col("ERR_CT"),
        (col("RECYCLE_CT") + lit(1)).alias("RECYCLE_CT"),
        col("SRC_SYS_CD"),
        col("PRI_KEY_STRING"),
        col("CLM_LN_REMIT_DSALW_SK"),
        col("CLM_ID"),
        col("CLM_LN_SEQ_NO"),
        col("CLM_LN_DSALW_TYP_CD"),
        col("BYPS_IN"),
        col("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("CLM_LN_SK"),
        col("CLM_LN_REMIT_DSALW_EXCD"),
        col("CLM_LN_RMT_DSW_EXCD_RESP_CD"),
        col("CLM_LN_RMT_DSALW_TYP_CAT_CD"),
        col("REMIT_DSALW_AMT")
    )
)

df_recycle_for_write = (
    df_recycle
    .withColumn("INSRT_UPDT_CD", rpad(col("INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", rpad(col("DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", rpad(col("PASS_THRU_IN"), 1, " "))
    .withColumn("CLM_LN_DSALW_TYP_CD", rpad(col("CLM_LN_DSALW_TYP_CD"), 10, " "))
    .withColumn("BYPS_IN", rpad(col("BYPS_IN"), 1, " "))
    .withColumn("CLM_LN_REMIT_DSALW_EXCD", rpad(col("CLM_LN_REMIT_DSALW_EXCD"), 10, " "))
    .withColumn("CLM_LN_RMT_DSW_EXCD_RESP_CD", rpad(col("CLM_LN_RMT_DSW_EXCD_RESP_CD"), 10, " "))
    .withColumn("CLM_LN_RMT_DSALW_TYP_CAT_CD", rpad(col("CLM_LN_RMT_DSALW_TYP_CAT_CD"), 10, " "))
    .select(
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
    )
)

write_files(
    df_recycle_for_write,
    f"{adls_path}/hf_recycle.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

df_claim_recycle = (
    df_with_stagevars
    .filter(col("ErrCount") > 0)
    .select(
        col("SRC_SYS_CD"),
        col("CLM_ID")
    )
)

df_claim_recycle_for_write = (
    df_claim_recycle
    .withColumn("SRC_SYS_CD", rpad(col("SRC_SYS_CD"), <...>, " "))
    .withColumn("CLM_ID", rpad(col("CLM_ID"), <...>, " "))
    .select("SRC_SYS_CD", "CLM_ID")
)

write_files(
    df_claim_recycle_for_write,
    f"{adls_path}/hf_claim_recycle_keys.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

df_defaultUNK = (
    df_with_stagevars
    .filter(col("row_num") == 1)
    .select(
        lit(0).alias("CLM_LN_REMIT_DSALW_SK"),
        lit(0).alias("SRC_SYS_CD_SK"),
        lit("UNK").alias("CLM_ID"),
        lit(0).alias("CLM_LN_SEQ_NO"),
        lit(0).alias("CLM_LN_DSALW_TYP_CD_SK"),
        lit("U").alias("BYPS_IN"),
        lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        lit(0).alias("CLM_LN_SK"),
        lit(0).alias("CLM_LN_REMIT_DSALW_EXCD_SK"),
        lit(0).alias("CLM_LN_RMT_DSW_EXCD_RESP_CD_SK"),
        lit(0).alias("CLM_LN_RMT_DSALW_TYP_CAT_CD_SK"),
        lit(0.00).alias("REMIT_DSALW_AMT")
    )
)

df_defaultNA = (
    df_with_stagevars
    .filter(col("row_num") == 1)
    .select(
        lit(1).alias("CLM_LN_REMIT_DSALW_SK"),
        lit(1).alias("SRC_SYS_CD_SK"),
        lit("NA").alias("CLM_ID"),
        lit(1).alias("CLM_LN_SEQ_NO"),
        lit(1).alias("CLM_LN_DSALW_TYP_CD_SK"),
        lit("X").alias("BYPS_IN"),
        lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        lit(1).alias("CLM_LN_SK"),
        lit(1).alias("CLM_LN_REMIT_DSALW_EXCD_SK"),
        lit(1).alias("CLM_LN_RMT_DSW_EXCD_RESP_CD_SK"),
        lit(1).alias("CLM_LN_RMT_DSALW_TYP_CAT_CD_SK"),
        lit(0.00).alias("REMIT_DSALW_AMT")
    )
)

df_fkey = (
    df_with_stagevars
    .filter((col("ErrCount") == 0) | (col("PassThru") == 'Y'))
    .select(
        col("CLM_LN_REMIT_DSALW_SK"),
        lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
        col("CLM_ID"),
        col("CLM_LN_SEQ_NO"),
        col("svClmLnDsalwTypCdSk").alias("CLM_LN_DSALW_TYP_CD_SK"),
        col("BYPS_IN"),
        col("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("svClmLnSk").alias("CLM_LN_SK"),
        col("svClmLnRemitDsalwExcdSk").alias("CLM_LN_REMIT_DSALW_EXCD_SK"),
        col("svClmLnRmtDswExcdRespCdSk").alias("CLM_LN_RMT_DSW_EXCD_RESP_CD_SK"),
        col("svClmLnDsalwTypCatCdSk").alias("CLM_LN_RMT_DSALW_TYP_CAT_CD_SK"),
        col("REMIT_DSALW_AMT")
    )
)

df_collector = df_defaultUNK.union(df_defaultNA).union(df_fkey)

df_collector_for_write = (
    df_collector
    .withColumn("BYPS_IN", rpad(col("BYPS_IN"), 1, " "))
    .withColumn("CLM_ID", rpad(col("CLM_ID"), <...>, " "))
    .select(
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
    )
)

write_files(
    df_collector_for_write,
    f"{adls_path}/load/CLM_LN_ALT_CHRG_REMIT_DSALW.{Source}.dat",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)