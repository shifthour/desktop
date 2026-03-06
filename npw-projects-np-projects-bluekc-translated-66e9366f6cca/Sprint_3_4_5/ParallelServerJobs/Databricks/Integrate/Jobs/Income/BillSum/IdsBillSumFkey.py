# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2010 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC Called by:
# MAGIC                     FctsIncomeLoadSeq
# MAGIC 
# MAGIC Processing:
# MAGIC                     *
# MAGIC 
# MAGIC Control Job Rerun Information: 
# MAGIC                     Previous Run Successful:    If necessary, restore key file
# MAGIC                     Previous Run Aborted:         Restart, no other steps necessary
# MAGIC 
# MAGIC Modifications:                        
# MAGIC                                                  Project/                                                                                                                       Code                   Date
# MAGIC Developer           Date              Altiris #     Change Description                                                                                     Reviewer            Reviewed
# MAGIC -------------------------  -------------------   -------------   -----------------------------------------------------------------------------------------------------------------   -------------------------  -------------------
# MAGIC Hugh Sisson        2010-09-17   3346        Original program                                                                                           Steph Goddard   09/21/2010
# MAGIC Raja Gummadi     2014-02-19   5127        Added 3 new columns to the Job                                                                   Kalyan Neelam    2014-02-19
# MAGIC                                                                  BILL_SUM_APTC_PD_STTUS_CD_SK
# MAGIC                                                                  APTC_DLQNCY_DT_SK
# MAGIC                                                                  SBSDY_AMT
# MAGIC 
# MAGIC Goutham Kalidindi  2021-03-24 358186    Changed Datatype length for field BLBL_BLIV_ID_LAST                             Reddy Sanam      2021-04-01
# MAGIC                                                                   char(12) to Varchar(15)

# MAGIC Read common record format file.
# MAGIC Writing Sequential File to /load
# MAGIC Assign all foreign keys and create default rows for UNK and NA.
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
    StringType,
    IntegerType,
    DecimalType,
    TimestampType
)
from pyspark.sql.functions import (
    col,
    lit,
    when,
    trim,
    row_number,
    monotonically_increasing_id,
    rpad
)
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# Parameters
InFile = get_widget_value("InFile", "IdsBillSumPkey.BillSumTmp.dat")
Logging = get_widget_value("Logging", "N")
SrcSysCdSk = get_widget_value("SrcSysCdSk", "1581")

# Schema for IdsBillSumCrf (CSeqFileStage)
schema_IdsBillSumCrf = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), nullable=False),
    StructField("INSRT_UPDT_CD", StringType(), nullable=False),
    StructField("DISCARD_IN", StringType(), nullable=False),
    StructField("PASS_THRU_IN", StringType(), nullable=False),
    StructField("FIRST_RECYC_DT", TimestampType(), nullable=False),
    StructField("ERR_CT", IntegerType(), nullable=False),
    StructField("RECYCLE_CT", DecimalType(38,10), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("PRI_KEY_STRING", StringType(), nullable=False),
    StructField("BILL_SUM_SK", IntegerType(), nullable=False),
    StructField("BILL_ENTY_UNIQ_KEY", IntegerType(), nullable=False),
    StructField("BILL_DUE_DT_SK", StringType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("BILL_ENTY", IntegerType(), nullable=False),
    StructField("LAST_BILL_INVC", StringType(), nullable=False),
    StructField("BILL_SUM_BILL_TYP_CD", StringType(), nullable=False),
    StructField("BILL_SUM_BILL_PD_STTUS_CD", StringType(), nullable=False),
    StructField("BILL_SUM_SPCL_BILL_CD", StringType(), nullable=False),
    StructField("PRORT_IN", StringType(), nullable=False),
    StructField("CRT_DTM", TimestampType(), nullable=False),
    StructField("DLQNCY_DT_SK", StringType(), nullable=False),
    StructField("END_DT_SK", StringType(), nullable=False),
    StructField("LAST_ALLOC_DT_SK", StringType(), nullable=False),
    StructField("PRM_UPDT_DTM", TimestampType(), nullable=False),
    StructField("RECON_DT_SK", StringType(), nullable=False),
    StructField("BILL_AMT", DecimalType(38,10), nullable=False),
    StructField("RCVD_AMT", DecimalType(38,10), nullable=False),
    StructField("ACTV_SUBS_NO", IntegerType(), nullable=False),
    StructField("BILL_DAYS_NO", IntegerType(), nullable=False),
    StructField("PRORT_FCTR", IntegerType(), nullable=False),
    StructField("LAST_BILL_INVC_ID", StringType(), nullable=False),
    StructField("BLBL_SUBSIDY_AMT_NVL", DecimalType(38,10), nullable=True),
    StructField("BLBL_APTC_PAID_STS_NVL", StringType(), nullable=True),
    StructField("BLBL_APTC_DLNQ_DT_NVL", StringType(), nullable=True)
])

# Read input file for IdsBillSumCrf
df_IdsBillSumCrf = (
    spark.read.format("csv")
    .option("header", "false")
    .option("quote", "\"")
    .option("escape", "\"")
    .schema(schema_IdsBillSumCrf)
    .load(f"{adls_path}/key/{InFile}")
)

# Derive columns in PurgeTrn (CTransformerStage)
df_with_vars = (
    df_IdsBillSumCrf
    .withColumn("PassThru", col("PASS_THRU_IN"))
    .withColumn("svBillEntySk", GetFkeyBillEnty(col("SRC_SYS_CD"), col("BILL_SUM_SK"), col("BILL_ENTY"), Logging))
    .withColumn("svLastBillInvcSk", GetFkeyInvc(col("SRC_SYS_CD"), col("BILL_SUM_SK"), col("LAST_BILL_INVC"), Logging))
    .withColumn("svBillSumBillTypCdSk", GetFkeyCodes(col("SRC_SYS_CD"), col("BILL_SUM_SK"), lit("BILLING SUMMARY BILL TYPE"), col("BILL_SUM_BILL_TYP_CD"), Logging))
    .withColumn("svBillSumBillPdSttusCdSk", GetFkeyCodes(col("SRC_SYS_CD"), col("BILL_SUM_SK"), lit("BILLING PAID STATUS"), col("BILL_SUM_BILL_PD_STTUS_CD"), Logging))
    .withColumn("svBillSumSpclBillCdSk", GetFkeyCodes(col("SRC_SYS_CD"), col("BILL_SUM_SK"), lit("BILLING SUMMARY SPECIAL BILL"), col("BILL_SUM_SPCL_BILL_CD"), Logging))
    .withColumn("svBillSumAptcPaidStsCdSk", GetFkeyClctnDomainCodes(col("SRC_SYS_CD"), col("BILL_SUM_SK"), lit("APTC PAID STATUS"), lit("FACETS DBO"), lit("APTC PAID STATUS"), lit("IDS"), col("BLBL_APTC_PAID_STS_NVL"), Logging))
    .withColumn("svBillSumAptcDlnqDtSk", GetFkeyDate(lit("IDS"), col("BILL_SUM_SK"), col("BLBL_APTC_DLNQ_DT_NVL"), Logging))
    .withColumn("ErrCount", GetFkeyErrorCnt(col("BILL_SUM_SK")))
)

# lnkRecycle data (ErrCount > 0)
df_lnkRecycle = (
    df_with_vars.filter(col("ErrCount") > 0)
    .select(
        GetRecycleKey(col("BILL_SUM_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
        rpad(col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
        rpad(col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
        rpad(col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
        col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        col("ErrCount").alias("ERR_CT"),
        (col("RECYCLE_CT") + lit(1)).alias("RECYCLE_CT"),
        col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        col("BILL_SUM_SK").alias("BILL_SUM_SK"),
        col("BILL_ENTY_UNIQ_KEY").alias("BILL_ENTY_UNIQ_KEY"),
        rpad(col("BILL_DUE_DT_SK"), 10, " ").alias("BILL_DUE_DT_SK"),
        col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("BILL_ENTY").alias("BILL_ENTY"),
        col("LAST_BILL_INVC").alias("LAST_BILL_INVC"),
        rpad(col("BILL_SUM_BILL_TYP_CD"), 2, " ").alias("BILL_SUM_BILL_TYP_CD"),
        rpad(col("BILL_SUM_BILL_PD_STTUS_CD"), 2, " ").alias("BILL_SUM_BILL_PD_STTUS_CD"),
        rpad(col("BILL_SUM_SPCL_BILL_CD"), 2, " ").alias("BILL_SUM_SPCL_BILL_CD"),
        rpad(col("PRORT_IN"), 1, " ").alias("PRORT_IN"),
        col("CRT_DTM").alias("CRT_DTM"),
        rpad(col("DLQNCY_DT_SK"), 10, " ").alias("DLQNCY_DT_SK"),
        rpad(col("END_DT_SK"), 10, " ").alias("END_DT_SK"),
        rpad(col("LAST_ALLOC_DT_SK"), 10, " ").alias("LAST_ALLOC_DT_SK"),
        col("PRM_UPDT_DTM").alias("PRM_UPDT_DTM"),
        rpad(col("RECON_DT_SK"), 10, " ").alias("RECON_DT_SK"),
        col("BILL_AMT").alias("BILL_AMT"),
        col("RCVD_AMT").alias("RCVD_AMT"),
        col("ACTV_SUBS_NO").alias("ACTV_SUBS_NO"),
        col("BILL_DAYS_NO").alias("BILL_DAYS_NO"),
        col("PRORT_FCTR").alias("PRORT_FCTR"),
        col("LAST_BILL_INVC_ID").alias("LAST_BILL_INVC_ID"),
        col("BLBL_SUBSIDY_AMT_NVL").alias("BLBL_SUBSIDY_AMT_NVL"),
        rpad(col("BLBL_APTC_PAID_STS_NVL"), 1, " ").alias("BLBL_APTC_PAID_STS_NVL"),
        col("BLBL_APTC_DLNQ_DT_NVL").alias("BLBL_APTC_DLNQ_DT_NVL")
    )
)

# Write the hashed file (CHashedFileStage) as parquet (Scenario C)
write_files(
    df_lnkRecycle,
    f"hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

# df_Fkey => (ErrCount=0 or PassThru='Y')
df_Fkey = (
    df_with_vars
    .filter((col("ErrCount") == 0) | (col("PassThru") == 'Y'))
    .select(
        col("BILL_SUM_SK").alias("BILL_SUM_SK"),
        col("BILL_ENTY_UNIQ_KEY").alias("BILL_ENTY_UNIQ_KEY"),
        col("BILL_DUE_DT_SK").alias("BILL_DUE_DT_SK"),
        lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
        col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("svBillEntySk").alias("BILL_ENTY_SK"),
        col("svLastBillInvcSk").alias("LAST_BILL_INVC_SK"),
        col("svBillSumBillTypCdSk").alias("BILL_SUM_BILL_TYP_CD_SK"),
        col("svBillSumBillPdSttusCdSk").alias("BILL_SUM_BILL_PD_STTUS_CD_SK"),
        col("svBillSumSpclBillCdSk").alias("BILL_SUM_SPCL_BILL_CD_SK"),
        col("PRORT_IN").alias("PRORT_IN"),
        col("CRT_DTM").alias("CRT_DTM"),
        col("DLQNCY_DT_SK").alias("DLQNCY_DT_SK"),
        col("END_DT_SK").alias("END_DT_SK"),
        col("LAST_ALLOC_DT_SK").alias("LAST_ALLOC_DT_SK"),
        col("PRM_UPDT_DTM").alias("PRM_UPDT_DTM"),
        col("RECON_DT_SK").alias("RECON_DT_SK"),
        col("BILL_AMT").alias("BILL_AMT"),
        col("RCVD_AMT").alias("RCVD_AMT"),
        col("ACTV_SUBS_NO").alias("ACTV_SUBS_NO"),
        col("BILL_DAYS_NO").alias("BILL_DAYS_NO"),
        col("PRORT_FCTR").alias("PRORT_FCTR"),
        col("LAST_BILL_INVC_ID").alias("LAST_BILL_INVC_ID"),
        when(col("svBillSumAptcPaidStsCdSk") == 0, lit(1)).otherwise(col("svBillSumAptcPaidStsCdSk")).alias("BILL_SUM_APTC_PD_STTUS_CD_SK"),
        when(trim(col("svBillSumAptcDlnqDtSk")) == 'UNK', lit("1753-01-01")).otherwise(col("svBillSumAptcDlnqDtSk")).alias("APTC_DLQNCY_DT_SK"),
        col("BLBL_SUBSIDY_AMT_NVL").alias("SBSDY_AMT")
    )
)

# Prepare to generate exactly one row for DefaultNA, all literal columns
windowSpec = Window.orderBy(monotonically_increasing_id())
df_oneRow = df_with_vars.withColumn("ROW_NUM", row_number().over(windowSpec))
df_DefaultNA = (
    df_oneRow
    .filter(col("ROW_NUM") == 1)
    .select(
        lit(1).alias("BILL_SUM_SK"),
        lit(1).alias("BILL_ENTY_UNIQ_KEY"),
        lit("1753-01-01").alias("BILL_DUE_DT_SK"),
        lit(1).alias("SRC_SYS_CD_SK"),
        lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        lit(1).alias("BILL_ENTY_SK"),
        lit(1).alias("LAST_BILL_INVC_SK"),
        lit(1).alias("BILL_SUM_BILL_TYP_CD_SK"),
        lit(1).alias("BILL_SUM_BILL_PD_STTUS_CD_SK"),
        lit(1).alias("BILL_SUM_SPCL_BILL_CD_SK"),
        lit("X").alias("PRORT_IN"),
        lit("1753-01-01-00.00.00.000000").alias("CRT_DTM"),
        lit("1753-01-01").alias("DLQNCY_DT_SK"),
        lit("1753-01-01").alias("END_DT_SK"),
        lit("1753-01-01").alias("LAST_ALLOC_DT_SK"),
        lit("1753-01-01-00.00.00.000000").alias("PRM_UPDT_DTM"),
        lit("1753-01-01").alias("RECON_DT_SK"),
        lit(1).alias("BILL_AMT"),
        lit(1).alias("RCVD_AMT"),
        lit(1).alias("ACTV_SUBS_NO"),
        lit(1).alias("BILL_DAYS_NO"),
        lit(1).alias("PRORT_FCTR"),
        lit("NA").alias("LAST_BILL_INVC_ID"),
        lit(1).alias("BILL_SUM_APTC_PD_STTUS_CD_SK"),
        lit("1753-01-01").alias("APTC_DLQNCY_DT_SK"),
        lit(0.00).alias("SBSDY_AMT")
    )
)

# Prepare to generate exactly one row for DefaultUNK, all literal columns
df_DefaultUNK = (
    df_oneRow
    .filter(col("ROW_NUM") == 1)
    .select(
        lit(0).alias("BILL_SUM_SK"),
        lit(0).alias("BILL_ENTY_UNIQ_KEY"),
        lit("1753-01-01").alias("BILL_DUE_DT_SK"),
        lit(0).alias("SRC_SYS_CD_SK"),
        lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        lit(0).alias("BILL_ENTY_SK"),
        lit(0).alias("LAST_BILL_INVC_SK"),
        lit(0).alias("BILL_SUM_BILL_TYP_CD_SK"),
        lit(0).alias("BILL_SUM_BILL_PD_STTUS_CD_SK"),
        lit(0).alias("BILL_SUM_SPCL_BILL_CD_SK"),
        lit("U").alias("PRORT_IN"),
        lit("1753-01-01-00.00.00.000000").alias("CRT_DTM"),
        lit("1753-01-01").alias("DLQNCY_DT_SK"),
        lit("1753-01-01").alias("END_DT_SK"),
        lit("1753-01-01").alias("LAST_ALLOC_DT_SK"),
        lit("1753-01-01-00.00.00.000000").alias("PRM_UPDT_DTM"),
        lit("1753-01-01").alias("RECON_DT_SK"),
        lit(0).alias("BILL_AMT"),
        lit(0).alias("RCVD_AMT"),
        lit(0).alias("ACTV_SUBS_NO"),
        lit(0).alias("BILL_DAYS_NO"),
        lit(0).alias("PRORT_FCTR"),
        lit("UNK").alias("LAST_BILL_INVC_ID"),
        lit(0).alias("BILL_SUM_APTC_PD_STTUS_CD_SK"),
        lit("1753-01-01").alias("APTC_DLQNCY_DT_SK"),
        lit(0.00).alias("SBSDY_AMT")
    )
)

# Collector: union the three inputs (Fkey, DefaultUNK, DefaultNA)
df_collector = (
    df_Fkey
    .unionByName(df_DefaultUNK)
    .unionByName(df_DefaultNA)
)

# Final rpad for char columns before writing out to BILL_SUM.dat
df_final = (
    df_collector
    .withColumn("BILL_DUE_DT_SK", rpad(col("BILL_DUE_DT_SK"), 10, " "))
    .withColumn("PRORT_IN", rpad(col("PRORT_IN"), 1, " "))
    .withColumn("DLQNCY_DT_SK", rpad(col("DLQNCY_DT_SK"), 10, " "))
    .withColumn("END_DT_SK", rpad(col("END_DT_SK"), 10, " "))
    .withColumn("LAST_ALLOC_DT_SK", rpad(col("LAST_ALLOC_DT_SK"), 10, " "))
    .withColumn("RECON_DT_SK", rpad(col("RECON_DT_SK"), 10, " "))
    .withColumn("APTC_DLQNCY_DT_SK", rpad(col("APTC_DLQNCY_DT_SK"), 10, " "))
    .select(
        "BILL_SUM_SK",
        "BILL_ENTY_UNIQ_KEY",
        "BILL_DUE_DT_SK",
        "SRC_SYS_CD_SK",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "BILL_ENTY_SK",
        "LAST_BILL_INVC_SK",
        "BILL_SUM_BILL_TYP_CD_SK",
        "BILL_SUM_BILL_PD_STTUS_CD_SK",
        "BILL_SUM_SPCL_BILL_CD_SK",
        "PRORT_IN",
        "CRT_DTM",
        "DLQNCY_DT_SK",
        "END_DT_SK",
        "LAST_ALLOC_DT_SK",
        "PRM_UPDT_DTM",
        "RECON_DT_SK",
        "BILL_AMT",
        "RCVD_AMT",
        "ACTV_SUBS_NO",
        "BILL_DAYS_NO",
        "PRORT_FCTR",
        "LAST_BILL_INVC_ID",
        "BILL_SUM_APTC_PD_STTUS_CD_SK",
        "APTC_DLQNCY_DT_SK",
        "SBSDY_AMT"
    )
)

# Write final data to BILL_SUM.dat (CSeqFileStage)
write_files(
    df_final,
    f"{adls_path}/load/BILL_SUM.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)