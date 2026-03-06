# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2020 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     IdsITSClmFkey
# MAGIC 
# MAGIC DESCRIPTION:     Foreign Key Building for load file
# MAGIC       
# MAGIC 
# MAGIC INPUTS:  IdsITSClmPkey.ItsClm.RUNID - output from PKey job
# MAGIC 	
# MAGIC   
# MAGIC HASH FILES:     None
# MAGIC 
# MAGIC 
# MAGIC TRANSFORMS:  
# MAGIC                            
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                Massive quantities of Stage Variables with CDMA lookups for surrogate key assignement.
# MAGIC 
# MAGIC OUTPUTS: 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC             We think this program was written in 8/2005
# MAGIC            10/18/2005 BJ Luce use Key.ITS_CLM_SK for recycle key instead of ClmSk
# MAGIC            02/15/2006 Steph Goddard   changes for sequencer
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer             Date                         Change Description                                                     Project #          Development Project          Code Reviewer          Date Reviewed  
# MAGIC ------------------           ----------------------------     ----------------------------------------------------------------------------       ----------------         ------------------------------------       ----------------------------      ----------------
# MAGIC Bhoomi D              04/01/2008            New Job                                                                       3255                 devlIDScur                          Steph Goddard          04/01/2008
# MAGIC Bhoomi D              08/07/2008            Added new parameter SrcSysCdSk                              3567                devlIDS                                Steph Goddard          08/15/2008
# MAGIC 
# MAGIC Manasa Andru      10/17/2014            Added SUPLMT_DSCNT_AMT field at the end.          TFS - 9580       IntegrateCurDevl                Kalyan Neelam          2014-10-22
# MAGIC 
# MAGIC Manasa Andru      2014-11-17       Added scale of 2 to the SUPLMT_DSCNT_AMT field.    TFS-9580 PostProd Fix   IdsCurDevl              Kalyan Neelam         2014-11-20

# MAGIC This hash file is written to by all claim foriegn key jobs.
# MAGIC Read common record format file from extract job.
# MAGIC Set all foreign surrogate keys
# MAGIC Writing Sequential File to /load
# MAGIC Merge source data with default rows
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DecimalType
from pyspark.sql.functions import col, lit, when, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


Source = get_widget_value("Source", "")
InFile = get_widget_value("InFile", "")
Logging = get_widget_value("Logging", "")
SrcSysCdSk = get_widget_value("SrcSysCdSk", "")

df_ClmCrf_schema = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False),
    StructField("INSRT_UPDT_CD", StringType(), False),
    StructField("DISCARD_IN", StringType(), False),
    StructField("PASS_THRU_IN", StringType(), False),
    StructField("FIRST_RECYC_DT", TimestampType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", DecimalType(38,10), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PRI_KEY_STRING", StringType(), False),
    StructField("ITS_CLM_SK", IntegerType(), False),
    StructField("CLM_ID", StringType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("TRNSMSN_SRC_CD", StringType(), False),
    StructField("CFA_DISP_PD_DT", StringType(), False),
    StructField("DISP_FMT_PD_DT_SK", StringType(), False),
    StructField("ACES_FEE_AMT", DecimalType(38,10), False),
    StructField("ADM_FEE_AMT", DecimalType(38,10), False),
    StructField("DRG_AMT", DecimalType(38,10), False),
    StructField("SRCHRG_AMT", DecimalType(38,10), False),
    StructField("SRPLS_AMT", DecimalType(38,10), False),
    StructField("SCCF_NO", StringType(), False),
    StructField("CLMI_INVEST_IND", StringType(), False),
    StructField("CLMI_INVEST_DAYS", IntegerType(), False),
    StructField("CLMI_INVEST_BEG_DT", StringType(), False),
    StructField("CLMI_INVEST_END_DT", StringType(), False),
    StructField("SUPLMT_DSCNT_AMT", DecimalType(38,10), False)
])

df_ClmCrf = (
    spark.read.format("csv")
    .schema(df_ClmCrf_schema)
    .option("header", "false")
    .option("quote", "\"")
    .load(f"{adls_path}/key/{InFile}")
)

df_ForeignKey = (
    df_ClmCrf
    .withColumn("ClmSK", GetFkeyClm(col("SRC_SYS_CD"), col("ITS_CLM_SK"), col("CLM_ID"), Logging))
    .withColumn("svItsClmInvCdSk", GetFkeyCodes("FACETS", col("ITS_CLM_SK"), "ITS CLAIM INVESTIGATION", col("CLMI_INVEST_IND"), Logging))
    .withColumn("TrnsmsnSrcCdSk", GetFkeyCodes("FACETS", col("ITS_CLM_SK"), "TRANSMISSION STATUS", col("TRNSMSN_SRC_CD"), Logging))
    .withColumn("CfaDispPdDtSk", GetFkeyDate("IDS", col("ITS_CLM_SK"), col("CFA_DISP_PD_DT"), Logging))
    .withColumn("DispFmtDtSk", GetFkeyDate("IDS", col("ITS_CLM_SK"), col("DISP_FMT_PD_DT_SK"), Logging))
    .withColumn("svInvBegDtSk", GetFkeyDate("IDS", col("ITS_CLM_SK"), col("CLMI_INVEST_BEG_DT"), Logging))
    .withColumn("svInvEndDtSk", GetFkeyDate("IDS", col("ITS_CLM_SK"), col("CLMI_INVEST_END_DT"), Logging))
    .withColumn("PassThru", col("PASS_THRU_IN"))
    .withColumn("ErrCount", GetFkeyErrorCnt(col("ITS_CLM_SK")))
)

df_ForeignKey_fkey = (
    df_ForeignKey
    .filter((col("ErrCount") == 0) | (col("PassThru") == "Y"))
    .select(
        col("ITS_CLM_SK").alias("ITS_CLM_SK"),
        lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
        col("CLM_ID").alias("CLM_ID"),
        col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("ClmSK").alias("CLM_SK"),
        col("svItsClmInvCdSk").alias("ITS_CLM_INVSTGT_CD_SK"),
        col("TrnsmsnSrcCdSk").alias("TRNSMSN_SRC_CD_SK"),
        col("CfaDispPdDtSk").alias("CFA_DISP_PD_DT_SK"),
        col("DISP_FMT_PD_DT_SK").alias("DISP_FMT_PD_DT_SK"),
        col("svInvBegDtSk").alias("INVSTGT_BEG_DT_SK"),
        col("svInvEndDtSk").alias("INVSTGT_END_DT_SK"),
        col("ACES_FEE_AMT").alias("ACES_FEE_AMT"),
        col("ADM_FEE_AMT").alias("ADM_FEE_AMT"),
        col("DRG_AMT").alias("DRG_AMT"),
        col("SRCHRG_AMT").alias("SRCHRG_AMT"),
        col("SRPLS_AMT").alias("SRPLS_AMT"),
        col("CLMI_INVEST_DAYS").alias("INVSTGT_DAYS_NO"),
        col("SCCF_NO").alias("SCCF_NO"),
        col("SUPLMT_DSCNT_AMT").alias("SUPLMT_DSCNT_AMT")
    )
)

df_ForeignKey_defaultUNK = (
    df_ForeignKey
    .limit(1)
    .select(
        lit(0).alias("ITS_CLM_SK"),
        lit(0).alias("SRC_SYS_CD_SK"),
        lit("UNK").alias("CLM_ID"),
        lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        lit(0).alias("CLM_SK"),
        lit(0).alias("ITS_CLM_INVSTGT_CD_SK"),
        lit(0).alias("TRNSMSN_SRC_CD_SK"),
        lit(0).alias("CFA_DISP_PD_DT_SK"),
        lit(0).alias("DISP_FMT_PD_DT_SK"),
        lit(0).alias("INVSTGT_BEG_DT_SK"),
        lit(0).alias("INVSTGT_END_DT_SK"),
        lit(0).alias("ACES_FEE_AMT"),
        lit(0).alias("ADM_FEE_AMT"),
        lit(0).alias("DRG_AMT"),
        lit(0.00).alias("SRCHRG_AMT"),
        lit(0.00).alias("SRPLS_AMT"),
        lit(0).alias("INVSTGT_DAYS_NO"),
        lit("UNK").alias("SCCF_NO"),
        lit(0).alias("SUPLMT_DSCNT_AMT")
    )
)

df_ForeignKey_defaultNA = (
    df_ForeignKey
    .limit(1)
    .select(
        lit(1).alias("ITS_CLM_SK"),
        lit(1).alias("SRC_SYS_CD_SK"),
        lit("NA").alias("CLM_ID"),
        lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        lit(1).alias("CLM_SK"),
        lit(1).alias("ITS_CLM_INVSTGT_CD_SK"),
        lit(1).alias("TRNSMSN_SRC_CD_SK"),
        lit(1).alias("CFA_DISP_PD_DT_SK"),
        lit(1).alias("DISP_FMT_PD_DT_SK"),
        lit(1).alias("INVSTGT_BEG_DT_SK"),
        lit(1).alias("INVSTGT_END_DT_SK"),
        lit(0).alias("ACES_FEE_AMT"),
        lit(0).alias("ADM_FEE_AMT"),
        lit(0).alias("DRG_AMT"),
        lit(0.00).alias("SRCHRG_AMT"),
        lit(0.00).alias("SRPLS_AMT"),
        lit(0).alias("INVSTGT_DAYS_NO"),
        lit("NA").alias("SCCF_NO"),
        lit(0).alias("SUPLMT_DSCNT_AMT")
    )
)

df_ForeignKey_recycle = (
    df_ForeignKey
    .filter(col("ErrCount") > 0)
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", when(col("ErrCount") > 0, GetRecycleKey(col("ITS_CLM_SK"))).otherwise(lit(0)))
    .withColumn("RECYCLE_CT", col("RECYCLE_CT") + lit(1))
    .select(
        col("JOB_EXCTN_RCRD_ERR_SK"),
        col("INSRT_UPDT_CD"),
        col("DISCARD_IN"),
        col("PASS_THRU_IN"),
        col("FIRST_RECYC_DT"),
        col("ErrCount").alias("ERR_CT"),
        col("RECYCLE_CT"),
        col("SRC_SYS_CD"),
        col("PRI_KEY_STRING"),
        col("ITS_CLM_SK"),
        col("CLM_ID"),
        col("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("CLMI_INVEST_IND").alias("ITS_CLM_INVSTGT_CD_SK"),
        col("TRNSMSN_SRC_CD"),
        col("CFA_DISP_PD_DT"),
        col("DISP_FMT_PD_DT_SK"),
        col("CLMI_INVEST_BEG_DT").alias("INVSTGT_BEG_DT_SK"),
        col("CLMI_INVEST_END_DT").alias("INVSTGT_END_DT_SK"),
        col("ACES_FEE_AMT"),
        col("ADM_FEE_AMT"),
        col("DRG_AMT"),
        col("SRCHRG_AMT"),
        col("SRPLS_AMT"),
        col("CLMI_INVEST_DAYS").alias("INVSTGT_DAYS_NO"),
        col("SCCF_NO"),
        col("SUPLMT_DSCNT_AMT")
    )
)

df_ForeignKey_recycleClms = (
    df_ForeignKey
    .filter(col("ErrCount") > 0)
    .select(
        col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("CLM_ID").alias("CLM_ID")
    )
)

df_ForeignKey_recycle_write = df_ForeignKey_recycle.select(
    col("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    rpad(col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    rpad(col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    col("FIRST_RECYC_DT"),
    col("ERR_CT"),
    col("RECYCLE_CT"),
    col("SRC_SYS_CD"),
    col("PRI_KEY_STRING"),
    col("ITS_CLM_SK"),
    col("CLM_ID"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("ITS_CLM_INVSTGT_CD_SK"),
    rpad(col("TRNSMSN_SRC_CD"), 2, " ").alias("TRNSMSN_SRC_CD"),
    rpad(col("CFA_DISP_PD_DT"), 10, " ").alias("CFA_DISP_PD_DT"),
    rpad(col("DISP_FMT_PD_DT_SK"), 10, " ").alias("DISP_FMT_PD_DT_SK"),
    rpad(col("INVSTGT_BEG_DT_SK"), 10, " ").alias("INVSTGT_BEG_DT_SK"),
    rpad(col("INVSTGT_END_DT_SK"), 10, " ").alias("INVSTGT_END_DT_SK"),
    col("ACES_FEE_AMT"),
    col("ADM_FEE_AMT"),
    col("DRG_AMT"),
    col("SRCHRG_AMT"),
    col("SRPLS_AMT"),
    col("INVSTGT_DAYS_NO"),
    rpad(col("SCCF_NO"), 17, " ").alias("SCCF_NO"),
    col("SUPLMT_DSCNT_AMT")
)

write_files(
    df_ForeignKey_recycle_write,
    "hf_recycle.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

df_ForeignKey_recycleClms_write = df_ForeignKey_recycleClms.select(
    col("SRC_SYS_CD"),
    col("CLM_ID")
)

write_files(
    df_ForeignKey_recycleClms_write,
    "hf_claim_recycle_keys.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

df_Collector = df_ForeignKey_fkey.unionByName(df_ForeignKey_defaultUNK).unionByName(df_ForeignKey_defaultNA)

df_Collector_selected = df_Collector.select(
    "ITS_CLM_SK",
    "SRC_SYS_CD_SK",
    "CLM_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "CLM_SK",
    "ITS_CLM_INVSTGT_CD_SK",
    "TRNSMSN_SRC_CD_SK",
    "CFA_DISP_PD_DT_SK",
    "DISP_FMT_PD_DT_SK",
    "INVSTGT_BEG_DT_SK",
    "INVSTGT_END_DT_SK",
    "ACES_FEE_AMT",
    "ADM_FEE_AMT",
    "DRG_AMT",
    "SRCHRG_AMT",
    "SRPLS_AMT",
    "INVSTGT_DAYS_NO",
    "SCCF_NO",
    "SUPLMT_DSCNT_AMT"
)

df_Collector_out = (
    df_Collector_selected
    .withColumn("CFA_DISP_PD_DT_SK", rpad(col("CFA_DISP_PD_DT_SK"), 10, " "))
    .withColumn("DISP_FMT_PD_DT_SK", rpad(col("DISP_FMT_PD_DT_SK"), 10, " "))
    .withColumn("INVSTGT_BEG_DT_SK", rpad(col("INVSTGT_BEG_DT_SK"), 10, " "))
    .withColumn("INVSTGT_END_DT_SK", rpad(col("INVSTGT_END_DT_SK"), 10, " "))
    .withColumn("SCCF_NO", rpad(col("SCCF_NO"), 20, " "))
)

write_files(
    df_Collector_out,
    f"{adls_path}/load/ITS_CLM.{Source}.dat",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)