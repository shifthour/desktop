# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *Â© Copyright 2007 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  <Sequencer Name>
# MAGIC                
# MAGIC 
# MAGIC PROCESSING:   Filter list of claims with foreign key error by claim status appending the output to the Facets hit list file.
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Brent Leland            2007-08-15        IAD Prod. Supp.   Original Programming.                                                                   devlIDS30                      Steph Goddard           8/30/07
# MAGIC 
# MAGIC Manasa Andru         2015-03-11         TFS - 10616        Changed the FctsClmHitlist file location from Update                       IntegrateNewDevl        Kalyan Neelam            2015-03-12
# MAGIC                                                                                          to landing directory

# MAGIC Keep only completed claims
# MAGIC Loaded in FctsClmDriverBuild
# MAGIC Loaded by every Fkey job
# MAGIC Write claim to hit list only if corrrect status
# MAGIC Filter Facets claims with errors to hit list
# MAGIC File emailed with error count
# MAGIC List error by job name
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

df_hf_claim_recycle_keys = (
    spark.read.parquet(f"{adls_path}/hf_claim_recycle_keys.parquet")
    .select("SRC_SYS_CD", "CLM_ID")
)

df_hf_clm_recycle_sttus = (
    spark.read.parquet(f"{adls_path}/hf_clm_recycle_sttus.parquet")
    .select("SRC_SYS_CD", "CLM_ID", "CLM_STTUS")
)

df_Trans1_filter = df_hf_clm_recycle_sttus.filter(
    (trim("CLM_STTUS") == "02") | (trim("CLM_STTUS") == "91")
)
df_Trans1_out_Clm_Status = df_Trans1_filter.select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("CLM_ID").alias("CLM_ID")
)

df_hf_claim_recycle_status = dedup_sort(
    df_Trans1_out_Clm_Status,
    ["SRC_SYS_CD", "CLM_ID"],
    []
)

df_Trans2_join = df_hf_claim_recycle_keys.alias("Recycle_Clms").join(
    df_hf_claim_recycle_status.alias("Status"),
    (F.col("Recycle_Clms.SRC_SYS_CD") == F.col("Status.SRC_SYS_CD"))
    & (F.col("Recycle_Clms.CLM_ID") == F.col("Status.CLM_ID")),
    how="left"
)

df_Trans2_filtered = df_Trans2_join.filter(F.col("Status.CLM_ID").isNotNull())

df_Trans2_out_Clm = df_Trans2_filtered.select(
    F.col("Status.CLM_ID").alias("CLCL_ID")
)

df_Trans2_out_Clm = df_Trans2_out_Clm.withColumn(
    "CLCL_ID",
    rpad("CLCL_ID", <...>, " ")
)

write_files(
    df_Trans2_out_Clm,
    f"{adls_path_raw}/landing/FctsClmHitList.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
df_SYSDUMMY1 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", "SELECT 1 AS IBMREQD")
    .load()
)

Clm = JobLinkCount('IdsClmFkey', 'Trns1', 'Recycle_Clms')
ClmLn = JobLinkCount('IdsClmLnFkey', 'Trns1', 'Recycle_Clms')
ClmLnClnclEdit = JobLinkCount('IdsClmLnClnclEditFkey', 'Trns1', 'Recycle_Clms')
ClmLnCOB = JobLinkCount('IdsClmLnCOBFkey', 'ForeignKey', 'Recycle_Clms')
DntlClmLn = JobLinkCount('IdsDntlClmLineFkey', 'ForeignKey', 'Recycle_Clms')
ClmLnDiag = JobLinkCount('IdsClmLnDiagFkey', 'ForeignKey', 'Recycle_Clms')
ClmLnDsalw = JobLinkCount('IdsClmLnDsalwFkey', 'ForeignKey', 'Recycle_Clms')
ClmLnOvrd = JobLinkCount('IdsClmLnOvrdFkey', 'ForeignKey', 'Recycle_Clms')
ClmLnPCA = JobLinkCount('FctsClmLnPcaFkey', 'ForeignKey', 'Recycle_Clms')
ClmLnProcCdMod = JobLinkCount('IdsClmLnProcCdModFkey', 'ForeignKey', 'Recycle_Clms')
ClmLnSav = JobLinkCount('IdsClmLnSavFkey', 'ForeignKey', 'Recycle_Clms')
ClmCOB = JobLinkCount('IdsClmCOBFkey', 'ForeignKey', 'Recycle_Clms')
ClmDiag = JobLinkCount('IdsClmDiagFkey', 'ForeignKey', 'Recycle_Clms')
ClmExtrnlMbr = JobLinkCount('IdsClmExtrnlMbrshFkey', 'ForeignKey', 'Recycle_Clms')
ClmExtrnlRefData = JobLinkCount('IdsClmExtrnlRefDataFkey', 'ForeignKey', 'Recycle_Clms')
ClmFcltyCond = JobLinkCount('IdsClmFcltyCondFkey', 'ForeignKey', 'Recycle_Clms')
ClmFclty = JobLinkCount('IdsClmFcltyFkey', 'ForeignKey', 'Recycle_Clms')
ClmFcltyOccr = JobLinkCount('IdsClmFcltyOccrFkey', 'ForeignKey', 'Recycle_Clms')
ClmFcltyProc = JobLinkCount('IdsClmFcltyProcFkey', 'ForeignKey', 'Recycle_Clms')
ClmFcltyVal = JobLinkCount('IdsClmFcltyValFkey', 'ForeignKey', 'Recycle_Clms')
ClmITS = JobLinkCount('IdsITSClmFkey', 'ForeignKey', 'Recycle_Clms')
ClmITSMsg = JobLinkCount('IdsITSClmMsgFkey', 'ForeignKey', 'Recycle_Clms')
ClmLetter = JobLinkCount('IdsClmLetterFkey', 'ForeignKey', 'Recycle_Clms')
ClmNote = JobLinkCount('IdsClmNoteFkey', 'ForeignKey', 'Recycle_Clms')
ClmOvrPay = JobLinkCount('IdsClmOvrPayFkey', 'ForeignKey', 'Recycle_Clms')
ClmOvrd = JobLinkCount('IdsClmOvrdFkey', 'ForeignKey', 'Recycle_Clms')
ClmProv = JobLinkCount('IdsClmProvFkey', 'ForeignKey', 'Recycle_Clms')
CLmExtrnlProv = JobLinkCount('IdsClmExtrnlProvFkey', 'ForeignKey', 'Recycle_Clms')
ClmReduction = JobLinkCount('IdsClmPayRductnFkey', 'ForeignKey', 'recycle')
ClmReductionActvty = JobLinkCount('IdsClmPayRductnActvtyFkey', 'ForeignKey', 'recycle')
ClmRemitHist = JobLinkCount('IdsClmRemitHistFkey', 'ForeignKey', 'Recycle_Clms')
ClmSttusAudit = JobLinkCount('IdsClmSttusAuditFkey', 'ForeignKey', 'Recycle_Clms')

df_Trans3_out_err1 = spark.createDataFrame(
    [("FctsClm -  " + str(Clm),)],
    ["ERR_CNT"]
)
df_Trans3_out_err2 = spark.createDataFrame(
    [("FctsClmLn -  " + str(ClmLn),)],
    ["ERR_CNT"]
)
df_Trans3_out_err3 = spark.createDataFrame(
    [("FctsClmLnClnclEdit  -   " + str(ClmLnClnclEdit),)],
    ["ERR_CNT"]
)
df_Trans3_out_err4 = spark.createDataFrame(
    [("FctsClmLnCOB  -   " + str(ClmLnCOB),)],
    ["ERR_CNT"]
)
df_Trans3_out_err5 = spark.createDataFrame(
    [("FctsDntlClmLn  -   " + str(DntlClmLn),)],
    ["ERR_CNT"]
)
df_Trans3_out_err6 = spark.createDataFrame(
    [("FctsClmLnDiag  -   " + str(ClmLnDiag),)],
    ["ERR_CNT"]
)
df_Trans3_out_err7 = spark.createDataFrame(
    [("FctsClmLnDsalw  -   " + str(ClmLnDsalw),)],
    ["ERR_CNT"]
)
df_Trans3_out_err8 = spark.createDataFrame(
    [("FctsClmLnOvrd  -   " + str(ClmLnOvrd),)],
    ["ERR_CNT"]
)
df_Trans3_out_err9 = spark.createDataFrame(
    [("FctsClmLnPCA  -   " + str(ClmLnPCA),)],
    ["ERR_CNT"]
)
df_Trans3_out_err10 = spark.createDataFrame(
    [("FctsClmLnProcCdMod  -   " + str(ClmLnProcCdMod),)],
    ["ERR_CNT"]
)
df_Trans3_out_err11 = spark.createDataFrame(
    [("FctsClmLnSav  -   " + str(ClmLnSav),)],
    ["ERR_CNT"]
)
df_Trans3_out_err12 = spark.createDataFrame(
    [("FctsClmCOB  -   " + str(ClmCOB),)],
    ["ERR_CNT"]
)
df_Trans3_out_err13 = spark.createDataFrame(
    [("FctsClmDiag  -   " + str(ClmDiag),)],
    ["ERR_CNT"]
)
df_Trans3_out_err14 = spark.createDataFrame(
    [("FctsClmExtrnlMbr  -   " + str(ClmExtrnlMbr),)],
    ["ERR_CNT"]
)
df_Trans3_out_err15 = spark.createDataFrame(
    [("FctsClmExtrnlRefData  -   " + str(ClmExtrnlRefData),)],
    ["ERR_CNT"]
)
df_Trans3_out_err16 = spark.createDataFrame(
    [("FctsClmFcltyCond  -   " + str(ClmFcltyCond),)],
    ["ERR_CNT"]
)
df_Trans3_out_err17 = spark.createDataFrame(
    [("FctsClmFclty  -   " + str(ClmFclty),)],
    ["ERR_CNT"]
)
df_Trans3_out_err18 = spark.createDataFrame(
    [("FctsClmFcltyOccr  -   " + str(ClmFcltyOccr),)],
    ["ERR_CNT"]
)
df_Trans3_out_err19 = spark.createDataFrame(
    [("FctsClmFcltyProc  -   " + str(ClmFcltyProc),)],
    ["ERR_CNT"]
)
df_Trans3_out_err20 = spark.createDataFrame(
    [("FctsClmFcltyVal  -   " + str(ClmFcltyVal),)],
    ["ERR_CNT"]
)
df_Trans3_out_err21 = spark.createDataFrame(
    [("FctsClmITS  -   " + str(ClmITS),)],
    ["ERR_CNT"]
)
df_Trans3_out_err22 = spark.createDataFrame(
    [("FctsClmITSMsg  -   " + str(ClmITSMsg),)],
    ["ERR_CNT"]
)
df_Trans3_out_err23 = spark.createDataFrame(
    [("FctsClmLetter  -   " + str(ClmLetter),)],
    ["ERR_CNT"]
)
df_Trans3_out_err24 = spark.createDataFrame(
    [("FctsClmNote  -   " + str(ClmNote),)],
    ["ERR_CNT"]
)
df_Trans3_out_err25 = spark.createDataFrame(
    [("FctsClmOvrPay  -   " + str(ClmOvrPay),)],
    ["ERR_CNT"]
)
df_Trans3_out_err26 = spark.createDataFrame(
    [("FctsClmOvrd  -   " + str(ClmOvrd),)],
    ["ERR_CNT"]
)
df_Trans3_out_err27 = spark.createDataFrame(
    [("FctsClmProv  -   " + str(ClmProv),)],
    ["ERR_CNT"]
)
df_Trans3_out_err28 = spark.createDataFrame(
    [("FctsClmExtrnlProv  -   " + str(CLmExtrnlProv),)],
    ["ERR_CNT"]
)
df_Trans3_out_err29 = spark.createDataFrame(
    [("FctsClmReduction  -   " + str(ClmReduction),)],
    ["ERR_CNT"]
)
df_Trans3_out_err30 = spark.createDataFrame(
    [("FctsClmReductionActvty  -   " + str(ClmReductionActvty),)],
    ["ERR_CNT"]
)
df_Trans3_out_err31 = spark.createDataFrame(
    [("FctsClmRemitHist  -   " + str(ClmRemitHist),)],
    ["ERR_CNT"]
)
df_Trans3_out_err32 = spark.createDataFrame(
    [("FctsClmSttusAdudit  -   " + str(ClmSttusAudit),)],
    ["ERR_CNT"]
)

df_collector = (
    df_Trans3_out_err1
    .union(df_Trans3_out_err2)
    .union(df_Trans3_out_err3)
    .union(df_Trans3_out_err4)
    .union(df_Trans3_out_err5)
    .union(df_Trans3_out_err6)
    .union(df_Trans3_out_err7)
    .union(df_Trans3_out_err8)
    .union(df_Trans3_out_err9)
    .union(df_Trans3_out_err10)
    .union(df_Trans3_out_err11)
    .union(df_Trans3_out_err12)
    .union(df_Trans3_out_err13)
    .union(df_Trans3_out_err14)
    .union(df_Trans3_out_err15)
    .union(df_Trans3_out_err16)
    .union(df_Trans3_out_err17)
    .union(df_Trans3_out_err18)
    .union(df_Trans3_out_err19)
    .union(df_Trans3_out_err20)
    .union(df_Trans3_out_err21)
    .union(df_Trans3_out_err22)
    .union(df_Trans3_out_err23)
    .union(df_Trans3_out_err24)
    .union(df_Trans3_out_err25)
    .union(df_Trans3_out_err26)
    .union(df_Trans3_out_err27)
    .union(df_Trans3_out_err28)
    .union(df_Trans3_out_err29)
    .union(df_Trans3_out_err30)
    .union(df_Trans3_out_err31)
    .union(df_Trans3_out_err32)
)

df_collector = df_collector.withColumn(
    "ERR_CNT",
    rpad("ERR_CNT", <...>, " ")
)

write_files(
    df_collector,
    f"{adls_path}/scripts/log/FctsClmJobErrCnt.txt",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)