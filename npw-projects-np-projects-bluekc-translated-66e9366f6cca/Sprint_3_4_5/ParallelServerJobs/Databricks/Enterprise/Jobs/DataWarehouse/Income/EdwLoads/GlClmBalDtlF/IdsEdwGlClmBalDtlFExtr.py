# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date               Project/Altiris #               Change Description                                                                                        Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------   -----------------------------------    ---------------------------------------------------------                                                                  ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Steph Goddard                 5/23/2007       GL Balancing/3264        Originally Programmed                                                                                 devlEDW10                         Brent Leland                 06-08-2007      
# MAGIC Steph Goddard                 7/5/07             GL Balancing/3264        Changed criteria for PCA indicator                                                               testEDW10
# MAGIC 
# MAGIC Bhupinder Kaur                10/23/2013    EDWefficiencies(5114)     To create a loadfile for EDW target table GL_CLM_BAL_DTL_F                 EnterpriseWrhsDevl         Jag Yelavarthi                2013-12-09

# MAGIC These remove duplicates ie.
# MAGIC RmdupClm, RmdupAdjClm,RmdupGl,Rmdup_GlAdj are necessary to get rid of duplicates coming to these lookups and  to get a clear run
# MAGIC JobName: IdsEdwGlClmBalDtlFExtr
# MAGIC 
# MAGIC This will pull claim and journal entry transaction data from IDS to create a balancing detail table in EDW - GL_CLM_BAL_DTL_F
# MAGIC xfrm_Create_Adj stage splits incoming claims into adjusted and original claims for processing
# MAGIC CLMLogic will create records for claims only on CLM and on both CLM and JRNL_ENTRY_TRANS - for original claims only
# MAGIC This transform will create records for claims only on JRNL_ENTRY_TRANS - if the record exists on CLM, it's created below
# MAGIC Extract CLM and JRNL_ENTRY_TRNS data according to mapping rules.
# MAGIC AdjClmLogic will create records for claims on both CLM and JRNL_ENTRY_TRANS - for adjusted claims only
# MAGIC Business Rules - calculate any difference between JRNL_ENTRY and CLM and set difference indicator  and 
# MAGIC Creating UNK and NA  default rows
# MAGIC Combine the records
# MAGIC This ds file will be used in PKey job of GL_CLM_BAL_DTL_F table.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType, TimestampType
from pyspark.sql.functions import col, lit, when, substring, rpad
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------

EDWRunCycle = get_widget_value('EDWRunCycle','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
CurTimestamp = get_widget_value('CurTimestamp','')
StartDt = get_widget_value('StartDt','')
EndDt = get_widget_value('EndDt','')

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

# ----------------------------------------------------------------------------------------------------
# STAGE: db2_JRNL_ENTRY_TRANS_Extr (DB2ConnectorPX) => Reads from IDS
# ----------------------------------------------------------------------------------------------------
q_db2_JRNL_ENTRY_TRANS_Extr = f"""
SELECT DISTINCT 
       JE.TRANS_TBL_SK,
       DRCR.TRGT_CD,
       JE.ACCTG_DT_SK,
       JE.FNCL_LOB_SK,
       JE.JRNL_ENTRY_TRANS_AMT,
       JE.JRNL_LN_REF_NO,
       LOB.FNCL_LOB_CD,
       JE.ACCT_NO,
       JE.SRC_TRANS_CK
FROM {IDSOwner}.JRNL_ENTRY_TRANS JE,
     {IDSOwner}.FNCL_LOB LOB,
     {IDSOwner}.CLNDR_DT DT,
     {IDSOwner}.CD_MPPNG DRCR
WHERE JE.FNCL_LOB_SK = LOB.FNCL_LOB_SK 
  AND JE.ACCTG_DT_SK = DT.CLNDR_DT_SK
  AND JE.JRNL_ENTRY_TRANS_DR_CR_CD_SK = DRCR.CD_MPPNG_SK
  AND JE.TRANS_LN_NO = 230
  AND JE.ACCTG_DT_SK >= '{StartDt}'
  AND JE.ACCTG_DT_SK <= '{EndDt}'
  AND JE.DIST_GL_IN = 'Y'
  AND JE.ACCT_NO in ('1312','1345','1415','1419','5440','5442','5444','5446')
"""

df_db2_JRNL_ENTRY_TRANS_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", q_db2_JRNL_ENTRY_TRANS_Extr)
    .load()
)

# ----------------------------------------------------------------------------------------------------
# STAGE: xfrm_TRGTCD_logic (CTransformerStage) => 3 output links
# ----------------------------------------------------------------------------------------------------
df_xfrm_TRGTCD_logic_base = (
    df_db2_JRNL_ENTRY_TRANS_Extr
    .withColumn(
        "PCA_CLM_IN", 
        when(trim(col("ACCT_NO")) == "1419", lit("Y")).otherwise(lit("N")))
    .withColumn(
        "FNCL_LOB_CD",
        when(trim(col("FNCL_LOB_CD")) == "NA", substring(col("JRNL_LN_REF_NO"), -4, 4)).otherwise(col("FNCL_LOB_CD")))
)

# Output link lnk_GL_in (all rows)
df_lnk_GL_in = df_xfrm_TRGTCD_logic_base.select(
    col("TRANS_TBL_SK"),
    col("TRGT_CD"),
    col("PCA_CLM_IN"),
    col("ACCTG_DT_SK"),
    col("FNCL_LOB_SK"),
    col("JRNL_ENTRY_TRANS_AMT"),
    col("FNCL_LOB_CD"),
    col("ACCT_NO"),
    col("SRC_TRANS_CK")
)

# Output link lnk_Rmdup_Gl (TRGT_CD='DR')
df_lnk_Rmdup_Gl = df_xfrm_TRGTCD_logic_base.filter(col("TRGT_CD") == "DR").select(
    col("TRANS_TBL_SK"),
    col("TRGT_CD"),
    col("PCA_CLM_IN"),
    col("ACCTG_DT_SK"),
    col("FNCL_LOB_SK"),
    col("JRNL_ENTRY_TRANS_AMT"),
    col("FNCL_LOB_CD"),
    col("ACCT_NO"),
    col("SRC_TRANS_CK")
)

# Output link lnk_Rmdup_Gl_Adj (TRGT_CD='CR')
df_lnk_Rmdup_Gl_Adj = df_xfrm_TRGTCD_logic_base.filter(col("TRGT_CD") == "CR").select(
    col("TRANS_TBL_SK"),
    col("TRGT_CD"),
    col("PCA_CLM_IN"),
    col("ACCTG_DT_SK"),
    col("FNCL_LOB_SK"),
    col("JRNL_ENTRY_TRANS_AMT"),
    col("FNCL_LOB_CD"),
    col("ACCT_NO"),
    col("SRC_TRANS_CK")
)

# ----------------------------------------------------------------------------------------------------
# STAGE: RmDup_Gl (PxRemDup) => Deduplicate by (TRANS_TBL_SK, TRGT_CD, PCA_CLM_IN), keep first
# ----------------------------------------------------------------------------------------------------
df_RmDup_Gl_temp = dedup_sort(
    df_lnk_Rmdup_Gl,
    ["TRANS_TBL_SK", "TRGT_CD", "PCA_CLM_IN"],
    [("TRANS_TBL_SK","A"), ("TRGT_CD","A"), ("PCA_CLM_IN","A")]
)
df_RmDup_Gl = df_RmDup_Gl_temp.select(
    col("TRANS_TBL_SK"),
    col("TRGT_CD"),
    col("PCA_CLM_IN"),
    col("ACCTG_DT_SK"),
    col("FNCL_LOB_SK"),
    col("JRNL_ENTRY_TRANS_AMT"),
    col("FNCL_LOB_CD"),
    col("ACCT_NO"),
    col("SRC_TRANS_CK")
)

# ----------------------------------------------------------------------------------------------------
# STAGE: RmDup_GlAdj (PxRemDup) => Deduplicate by (TRANS_TBL_SK, TRGT_CD, PCA_CLM_IN), keep first
# ----------------------------------------------------------------------------------------------------
df_RmDup_GlAdj_temp = dedup_sort(
    df_lnk_Rmdup_Gl_Adj,
    ["TRANS_TBL_SK", "TRGT_CD", "PCA_CLM_IN"],
    [("TRANS_TBL_SK","A"), ("TRGT_CD","A"), ("PCA_CLM_IN","A")]
)
df_RmDup_GlAdj = df_RmDup_GlAdj_temp.select(
    col("TRANS_TBL_SK"),
    col("TRGT_CD"),
    col("PCA_CLM_IN"),
    col("ACCTG_DT_SK"),
    col("FNCL_LOB_SK"),
    col("JRNL_ENTRY_TRANS_AMT"),
    col("FNCL_LOB_CD"),
    col("ACCT_NO"),
    col("SRC_TRANS_CK")
)

# ----------------------------------------------------------------------------------------------------
# STAGE: db2_CLM_in (DB2ConnectorPX) => Reads from IDS
# ----------------------------------------------------------------------------------------------------
q_db2_CLM_in = f"""
SELECT  CLM.CLM_SK,
        CLM.SRC_SYS_CD_SK,
        CLM.CLM_ID,
        CLM.FNCL_LOB_SK,
        GRP.GRP_ID,
        CLM.PROD_SK,
        CLM.CLM_TYP_CD_SK,
        TYP.TRGT_CD AS CLM_TYP_CD,
        CHK.PCA_CHK_IN,
        CHK.CLM_REMIT_HIST_PAYMTOVRD_CD_SK,
        CLM.PCA_TYP_CD_SK,
        CLM.PD_DT_SK,
        CLM.ACTL_PD_AMT,
        CLM.PAYBL_AMT,
        LOB.FNCL_LOB_CD,
        RCST.FNCL_LOB_CD AS RCST_LOB_CD,
        PCA.TRGT_CD AS PCA_TYP_CD,
        CLM.ADJ_FROM_CLM_SK,
        STAT.TRGT_CD AS STAT_CD,
        PAYOVR.TRGT_CD AS PAYOVR_CD,
        CLMPCA.TOT_PD_AMT AS PCA_PD_AMT
FROM {IDSOwner}.CLM CLM 
LEFT JOIN {IDSOwner}.CLM_CHK CHK
       ON CLM.CLM_SK = CHK.CLM_SK
LEFT JOIN {IDSOwner}.CLM_PCA CLMPCA
       ON CLM.CLM_SK = CLMPCA.CLM_SK,
     {IDSOwner}.FNCL_LOB LOB,
     {IDSOwner}.FNCL_LOB RCST,
     {IDSOwner}.PROD PROD,
     {IDSOwner}.GRP GRP,
     {IDSOwner}.CD_MPPNG SRC,
     {IDSOwner}.CD_MPPNG STAT,
     {IDSOwner}.CD_MPPNG TYP,
     {IDSOwner}.CD_MPPNG DISP,
     {IDSOwner}.CD_MPPNG CAT,
     {IDSOwner}.CD_MPPNG PCA,
     {IDSOwner}.CD_MPPNG PAYOVR
WHERE CLM.FNCL_LOB_SK = LOB.FNCL_LOB_SK
  AND CLM.PROD_SK = PROD.PROD_SK
  AND PROD.FNCL_LOB_SK = RCST.FNCL_LOB_SK
  AND CLM.GRP_SK = GRP.GRP_SK
  AND CLM.PCA_TYP_CD_SK = PCA.CD_MPPNG_SK
  AND CLM.SRC_SYS_CD_SK = SRC.CD_MPPNG_SK
  AND SRC.TRGT_CD = 'FACETS'
  AND CLM.PD_DT_SK >= '{StartDt}'
  AND CLM.PD_DT_SK <= '{EndDt}'
  AND CLM.CLM_STTUS_CD_SK = STAT.CD_MPPNG_SK
  AND STAT.TRGT_CD in ('A02','A08','A09')
  AND CLM.CLM_TYP_CD_SK = TYP.CD_MPPNG_SK
  AND TYP.TRGT_CD in ('MED','DNTL')
  AND CLM.CLM_FINL_DISP_CD_SK = DISP.CD_MPPNG_SK
  AND DISP.TRGT_CD in ('ACPTD','DENIEDREJ','SUSP')
  AND CLM.CLM_CAT_CD_SK = CAT.CD_MPPNG_SK
  AND CAT.TRGT_CD = 'STD'
  AND CHK.CLM_REMIT_HIST_PAYMTOVRD_CD_SK = PAYOVR.CD_MPPNG_SK
"""

df_db2_CLM_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", q_db2_CLM_in)
    .load()
)

# ----------------------------------------------------------------------------------------------------
# STAGE: xfrm_Create_Adj (CTransformerStage) => 2 output links
# ----------------------------------------------------------------------------------------------------
# We need the 'STAT_CD' in order to filter on A08 vs not A08. We rename PCA_CHK_IN => PCA_CLM_IN.
df_xfrm_Create_Adj_base = df_db2_CLM_in.withColumnRenamed("PCA_CHK_IN","PCA_CLM_IN")

df_lnk_Adj_Out = df_xfrm_Create_Adj_base.filter(col("STAT_CD") == "A08").select(
    col("ADJ_FROM_CLM_SK").alias("ADJ_FROM_CLM_SK"),
    col("PCA_CLM_IN").alias("PCA_CLM_IN"),
    col("CLM_SK").alias("CLM_SK"),
    col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    col("CLM_ID").alias("CLM_ID"),
    col("FNCL_LOB_SK").alias("FNCL_LOB_SK"),
    col("GRP_ID").alias("GRP_ID"),
    col("PROD_SK").alias("PROD_SK"),
    col("CLM_TYP_CD_SK").alias("CLM_TYP_CD_SK"),
    col("CLM_TYP_CD").alias("CLM_TYP_CD"),
    col("PCA_CLM_IN").alias("PCA_CHK_IN"),
    col("CLM_REMIT_HIST_PAYMTOVRD_CD_SK").alias("CLM_REMIT_HIST_PAYMTOVRD_CD_SK"),
    col("PCA_TYP_CD_SK").alias("PCA_TYP_CD_SK"),
    col("PD_DT_SK").alias("PD_DT_SK"),
    col("ACTL_PD_AMT").alias("ACTL_PD_AMT"),
    col("PAYBL_AMT").alias("PAYBL_AMT"),
    col("FNCL_LOB_CD").alias("FNCL_LOB_CD"),
    col("RCST_LOB_CD").alias("RCST_LOB_CD"),
    col("PCA_TYP_CD").alias("PCA_TYP_CD"),
    col("PAYOVR_CD").alias("PAYOVR_CD"),
    col("PCA_PD_AMT").alias("PCA_PD_AMT"),
    lit("A").alias("Dummy")
)

df_lnk_Clm_Out = df_xfrm_Create_Adj_base.filter(col("STAT_CD") != "A08").select(
    col("CLM_SK").alias("CLM_SK"),
    col("PCA_CLM_IN").alias("PCA_CLM_IN"),
    col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    col("CLM_ID").alias("CLM_ID"),
    col("FNCL_LOB_SK").alias("FNCL_LOB_SK"),
    col("GRP_ID").alias("GRP_ID"),
    col("PROD_SK").alias("PROD_SK"),
    col("CLM_TYP_CD_SK").alias("CLM_TYP_CD_SK"),
    col("CLM_TYP_CD").alias("CLM_TYP_CD"),
    col("PCA_CLM_IN").alias("PCA_CHK_IN"),
    col("CLM_REMIT_HIST_PAYMTOVRD_CD_SK").alias("CLM_REMIT_HIST_PAYMTOVRD_CD_SK"),
    col("PCA_TYP_CD_SK").alias("PCA_TYP_CD_SK"),
    col("PD_DT_SK").alias("PD_DT_SK"),
    col("ACTL_PD_AMT").alias("ACTL_PD_AMT"),
    col("PAYBL_AMT").alias("PAYBL_AMT"),
    col("FNCL_LOB_CD").alias("FNCL_LOB_CD"),
    col("RCST_LOB_CD").alias("RCST_LOB_CD"),
    col("PCA_TYP_CD").alias("PCA_TYP_CD"),
    col("ADJ_FROM_CLM_SK").alias("ADJ_FROM_CLM_SK"),
    col("PAYOVR_CD").alias("PAYOVR_CD"),
    col("PCA_PD_AMT").alias("PCA_PD_AMT"),
    lit("C").alias("Dummy")
)

# ----------------------------------------------------------------------------------------------------
# STAGE: Rmdup_Clm (PxRemDup) => deduplicate by (CLM_SK, PCA_CLM_IN), keep first
# ----------------------------------------------------------------------------------------------------
df_Rmdup_Clm_temp = dedup_sort(
    df_lnk_Clm_Out,
    ["CLM_SK","PCA_CLM_IN"],
    [("CLM_SK","A"),("PCA_CLM_IN","A")]
)
df_Rmdup_Clm = df_Rmdup_Clm_temp.select(
    col("CLM_SK"),
    col("PCA_CLM_IN"),
    col("SRC_SYS_CD_SK"),
    col("CLM_ID"),
    col("FNCL_LOB_SK"),
    col("GRP_ID"),
    col("PROD_SK"),
    col("CLM_TYP_CD_SK"),
    col("CLM_TYP_CD"),
    col("PCA_CHK_IN"),
    col("CLM_REMIT_HIST_PAYMTOVRD_CD_SK"),
    col("PCA_TYP_CD_SK"),
    col("PD_DT_SK"),
    col("ACTL_PD_AMT"),
    col("PAYBL_AMT"),
    col("FNCL_LOB_CD"),
    col("RCST_LOB_CD"),
    col("PCA_TYP_CD"),
    col("ADJ_FROM_CLM_SK"),
    col("PAYOVR_CD"),
    col("PCA_PD_AMT"),
    col("Dummy")
)

# ----------------------------------------------------------------------------------------------------
# STAGE: cpy_Clm (PxCopy) => 2 outputs
# ----------------------------------------------------------------------------------------------------
df_cpy_Clm = df_Rmdup_Clm

df_lnk_Clm_in = df_cpy_Clm.select(
    col("CLM_SK"),
    col("PCA_CLM_IN"),
    col("SRC_SYS_CD_SK"),
    col("CLM_ID"),
    col("FNCL_LOB_SK"),
    col("GRP_ID"),
    col("PROD_SK"),
    col("CLM_TYP_CD_SK"),
    col("CLM_TYP_CD"),
    col("PCA_CHK_IN"),
    col("CLM_REMIT_HIST_PAYMTOVRD_CD_SK"),
    col("PCA_TYP_CD_SK"),
    col("PD_DT_SK"),
    col("ACTL_PD_AMT"),
    col("PAYBL_AMT"),
    col("FNCL_LOB_CD"),
    col("RCST_LOB_CD"),
    col("PCA_TYP_CD"),
    col("ADJ_FROM_CLM_SK"),
    col("PAYOVR_CD"),
    col("PCA_PD_AMT")
)

df_ref_Clm = df_cpy_Clm.select(
    col("CLM_SK"),
    col("PCA_CLM_IN"),
    col("SRC_SYS_CD_SK"),
    col("CLM_ID"),
    col("FNCL_LOB_SK"),
    col("GRP_ID"),
    col("PROD_SK"),
    col("CLM_TYP_CD_SK"),
    col("CLM_TYP_CD"),
    col("PCA_CHK_IN"),
    col("CLM_REMIT_HIST_PAYMTOVRD_CD_SK"),
    col("PCA_TYP_CD_SK"),
    col("PD_DT_SK"),
    col("ACTL_PD_AMT"),
    col("PAYBL_AMT"),
    col("FNCL_LOB_CD"),
    col("RCST_LOB_CD"),
    col("PCA_TYP_CD"),
    col("ADJ_FROM_CLM_SK"),
    col("PAYOVR_CD"),
    col("PCA_PD_AMT"),
    col("Dummy")
)

# ----------------------------------------------------------------------------------------------------
# STAGE: lkp_Clm (PxLookup) => primary link = lnk_Clm_in, lookup link = ref_Gl (left join)
# ----------------------------------------------------------------------------------------------------
# ref_Gl is df_RmDup_Gl
df_lkp_Clm = df_lnk_Clm_in.alias("lnk_Clm_in").join(
    df_RmDup_Gl.alias("ref_Gl"),
    on=[
        col("lnk_Clm_in.CLM_SK") == col("ref_Gl.TRANS_TBL_SK"),
        col("lnk_Clm_in.PCA_CLM_IN") == col("ref_Gl.PCA_CLM_IN")
    ],
    how="left"
).select(
    col("lnk_Clm_in.CLM_SK").alias("CLM_SK"),
    col("lnk_Clm_in.PCA_CLM_IN").alias("PCA_CLM_IN"),
    col("lnk_Clm_in.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    col("lnk_Clm_in.CLM_ID").alias("CLM_ID"),
    col("lnk_Clm_in.FNCL_LOB_SK").alias("FNCL_LOB_SK"),
    col("lnk_Clm_in.GRP_ID").alias("GRP_ID"),
    col("lnk_Clm_in.PROD_SK").alias("PROD_SK"),
    col("lnk_Clm_in.CLM_TYP_CD_SK").alias("CLM_TYP_CD_SK"),
    col("lnk_Clm_in.CLM_TYP_CD").alias("CLM_TYP_CD"),
    col("lnk_Clm_in.PCA_CHK_IN").alias("PCA_CHK_IN"),
    col("lnk_Clm_in.CLM_REMIT_HIST_PAYMTOVRD_CD_SK").alias("CLM_REMIT_HIST_PAYMTOVRD_CD_SK"),
    col("lnk_Clm_in.PCA_TYP_CD_SK").alias("PCA_TYP_CD_SK"),
    col("lnk_Clm_in.PD_DT_SK").alias("PD_DT_SK"),
    col("lnk_Clm_in.ACTL_PD_AMT").alias("ACTL_PD_AMT"),
    col("lnk_Clm_in.PAYBL_AMT").alias("PAYBL_AMT"),
    col("lnk_Clm_in.FNCL_LOB_CD").alias("FNCL_LOB_CD"),
    col("lnk_Clm_in.RCST_LOB_CD").alias("RCST_LOB_CD"),
    col("lnk_Clm_in.PCA_TYP_CD").alias("PCA_TYP_CD"),
    col("lnk_Clm_in.ADJ_FROM_CLM_SK").alias("ADJ_FROM_CLM_SK"),
    col("lnk_Clm_in.PAYOVR_CD").alias("PAYOVR_CD"),
    col("lnk_Clm_in.PCA_PD_AMT").alias("PCA_PD_AMT"),
    col("ref_Gl.TRANS_TBL_SK").alias("TRANS_TBL_SK"),
    col("ref_Gl.TRGT_CD").alias("TRGT_CD"),
    col("ref_Gl.JRNL_ENTRY_TRANS_AMT").alias("JRNL_ENTRY_TRANS_AMT"),
    col("ref_Gl.FNCL_LOB_CD").alias("FNCL_LOB_CD_1"),
    col("ref_Gl.ACCT_NO").alias("ACCT_NO"),
    col("ref_Gl.SRC_TRANS_CK").alias("SRC_TRANS_CK")
)

# ----------------------------------------------------------------------------------------------------
# STAGE: xfrm_CLMLogic (CTransformerStage) => one output link
# ----------------------------------------------------------------------------------------------------
df_xfrm_CLMLogic_base = df_lkp_Clm

df_lnk_xfrmClmlogic_in = df_xfrm_CLMLogic_base.select(
    lit(0).alias("GL_CLM_BAL_DTL_SK"),
    lit("FACETS").alias("EDW_SRC_SYS_CD"),
    col("CLM_ID").alias("EDW_CLM_ID"),
    when(col("TRANS_TBL_SK").isNull(), lit(0)).otherwise(col("SRC_TRANS_CK")).alias("GL_CLM_DTL_CK"),
    col("PCA_CLM_IN").alias("PCA_IN"),
    lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    col("CLM_SK").alias("CLM_SK"),
    col("CLM_TYP_CD").alias("CLS_PLN_PROD_CAT_CD"),
    when((col("PCA_CHK_IN").isNull()) | (col("PCA_CHK_IN") == 'N'), lit("NA")).otherwise(col("PAYOVR_CD")).alias("CLM_REMIT_HIST_PAYMT_OVRD_CD"),
    col("FNCL_LOB_CD").alias("EDW_FNCL_LOB_CD"),
    when((col("FNCL_LOB_CD_1").isNull()) | (trim(col("FNCL_LOB_CD_1")) == ''), lit("UNK")).otherwise(col("FNCL_LOB_CD_1")).alias("GL_FNCL_LOB_CD"),
    col("RCST_LOB_CD").alias("RCST_FNCL_LOB_CD"),
    lit("N").alias("IN_BAL_IN"),
    lit(current_timestamp()).alias("CRT_DTM"),
    (substring(col("PD_DT_SK"),1,4) + substring(col("PD_DT_SK"),6,2)).alias("PD_YR_MO"),
    lit(0).alias("DIFF_AMT"),
    when(
        (col("PCA_TYP_CD").isin("RUNOUT","PCA","EMPWBNF")), col("PAYBL_AMT")
    ).otherwise(
        when(col("PCA_CLM_IN") == 'Y', col("PCA_PD_AMT")).otherwise(col("ACTL_PD_AMT"))
    ).alias("EDW_CLM_ACTL_PD_AMT"),
    when((col("TRGT_CD").isNull()) | (trim(col("TRGT_CD")) == ''), lit(0.00)).otherwise(col("JRNL_ENTRY_TRANS_AMT")).alias("GL_PD_AMT"),
    when(
        col("PCA_TYP_CD").isin("RUNOUT","PCA","EMPWBNF"), col("PAYBL_AMT")
    ).otherwise(lit(0)).alias("PCA_RUNOUT_AMT"),
    when((col("ACCT_NO").isNull()) | (trim(col("ACCT_NO"))==''), lit("NA")).otherwise(col("ACCT_NO")).alias("GL_ACCT_NO"),
    col("GRP_ID").alias("GRP_ID"),
    lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

# ----------------------------------------------------------------------------------------------------
# STAGE: Rmdup_AdjClm (PxRemDup) => deduplicate by (ADJ_FROM_CLM_SK, PCA_CLM_IN)
# ----------------------------------------------------------------------------------------------------
df_Rmdup_AdjClm_temp = dedup_sort(
    df_lnk_Adj_Out,
    ["ADJ_FROM_CLM_SK","PCA_CLM_IN"],
    [("ADJ_FROM_CLM_SK","A"),("PCA_CLM_IN","A")]
)
df_Rmdup_AdjClm = df_Rmdup_AdjClm_temp.select(
    col("ADJ_FROM_CLM_SK"),
    col("PCA_CLM_IN"),
    col("CLM_SK"),
    col("SRC_SYS_CD_SK"),
    col("CLM_ID"),
    col("FNCL_LOB_SK"),
    col("GRP_ID"),
    col("PROD_SK"),
    col("CLM_TYP_CD_SK"),
    col("CLM_TYP_CD"),
    col("PCA_CHK_IN"),
    col("CLM_REMIT_HIST_PAYMTOVRD_CD_SK"),
    col("PCA_TYP_CD_SK"),
    col("PD_DT_SK"),
    col("ACTL_PD_AMT"),
    col("PAYBL_AMT"),
    col("FNCL_LOB_CD"),
    col("RCST_LOB_CD"),
    col("PCA_TYP_CD"),
    col("PAYOVR_CD"),
    col("PCA_PD_AMT"),
    col("Dummy")
)

# ----------------------------------------------------------------------------------------------------
# STAGE: cpy_Adj (PxCopy) => 2 outputs
# ----------------------------------------------------------------------------------------------------
df_cpy_Adj = df_Rmdup_AdjClm

df_lnk_Adj_in = df_cpy_Adj.select(
    col("ADJ_FROM_CLM_SK"),
    col("PCA_CLM_IN"),
    col("CLM_SK"),
    col("SRC_SYS_CD_SK"),
    col("CLM_ID"),
    col("FNCL_LOB_SK"),
    col("GRP_ID"),
    col("PROD_SK"),
    col("CLM_TYP_CD_SK"),
    col("CLM_TYP_CD"),
    col("PCA_CHK_IN"),
    col("CLM_REMIT_HIST_PAYMTOVRD_CD_SK"),
    col("PCA_TYP_CD_SK"),
    col("PD_DT_SK"),
    col("ACTL_PD_AMT"),
    col("PAYBL_AMT"),
    col("FNCL_LOB_CD"),
    col("RCST_LOB_CD"),
    col("PCA_TYP_CD"),
    col("PAYOVR_CD"),
    col("PCA_PD_AMT")
)

df_ref_Adj = df_cpy_Adj.select(
    col("ADJ_FROM_CLM_SK"),
    col("PCA_CLM_IN"),
    col("CLM_SK"),
    col("SRC_SYS_CD_SK"),
    col("CLM_ID"),
    col("FNCL_LOB_SK"),
    col("GRP_ID"),
    col("PROD_SK"),
    col("CLM_TYP_CD_SK"),
    col("CLM_TYP_CD"),
    col("PCA_CHK_IN"),
    col("CLM_REMIT_HIST_PAYMTOVRD_CD_SK"),
    col("PCA_TYP_CD_SK"),
    col("PD_DT_SK"),
    col("ACTL_PD_AMT"),
    col("PAYBL_AMT"),
    col("FNCL_LOB_CD"),
    col("RCST_LOB_CD"),
    col("PCA_TYP_CD"),
    col("PAYOVR_CD"),
    col("PCA_PD_AMT"),
    col("Dummy")
)

# ----------------------------------------------------------------------------------------------------
# STAGE: lkp_GL (PxLookup) => primary link = lnk_GL_in, 2 lookup links = ref_Clm (left), ref_Adj (left)
# ----------------------------------------------------------------------------------------------------

df_lkp_GL = (
    df_lnk_GL_in.alias("lnk_GL_in")
    .join(
        df_ref_Clm.alias("ref_Clm"),
        on=[
            col("lnk_GL_in.TRANS_TBL_SK") == col("ref_Clm.CLM_SK"),
            col("lnk_GL_in.PCA_CLM_IN") == col("ref_Clm.PCA_CLM_IN")
        ],
        how="left"
    )
    .join(
        df_ref_Adj.alias("ref_Adj"),
        on=[
            col("lnk_GL_in.TRANS_TBL_SK") == col("ref_Adj.ADJ_FROM_CLM_SK"),
            col("lnk_GL_in.PCA_CLM_IN") == col("ref_Adj.PCA_CLM_IN")
        ],
        how="left"
    )
    .select(
        col("lnk_GL_in.TRANS_TBL_SK").alias("TRANS_TBL_SK"),
        col("lnk_GL_in.TRGT_CD").alias("TRGT_CD"),
        col("lnk_GL_in.PCA_CLM_IN").alias("PCA_CLM_IN"),
        col("lnk_GL_in.ACCTG_DT_SK").alias("ACCTG_DT_SK"),
        col("lnk_GL_in.FNCL_LOB_SK").alias("FNCL_LOB_SK"),
        col("lnk_GL_in.JRNL_ENTRY_TRANS_AMT").alias("JRNL_ENTRY_TRANS_AMT"),
        col("lnk_GL_in.FNCL_LOB_CD").alias("FNCL_LOB_CD"),
        col("lnk_GL_in.ACCT_NO").alias("ACCT_NO"),
        col("lnk_GL_in.SRC_TRANS_CK").alias("SRC_TRANS_CK"),
        col("ref_Clm.CLM_SK").alias("CLM_CLM_SK"),
        col("ref_Clm.PCA_CLM_IN").alias("PCA_CLM_IN_1"),
        col("ref_Clm.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        col("ref_Clm.CLM_ID").alias("CLM_ID"),
        col("ref_Clm.FNCL_LOB_SK").alias("FNCL_LOB_SK_1"),
        col("ref_Clm.GRP_ID").alias("GRP_ID"),
        col("ref_Clm.PROD_SK").alias("PROD_SK"),
        col("ref_Clm.CLM_TYP_CD_SK").alias("CLM_TYP_CD_SK"),
        col("ref_Clm.CLM_TYP_CD").alias("CLM_TYP_CD"),
        col("ref_Clm.PCA_CHK_IN").alias("PCA_CHK_IN"),
        col("ref_Clm.CLM_REMIT_HIST_PAYMTOVRD_CD_SK").alias("CLM_REMIT_HIST_PAYMTOVRD_CD_SK"),
        col("ref_Clm.PCA_TYP_CD_SK").alias("PCA_TYP_CD_SK"),
        col("ref_Clm.PD_DT_SK").alias("PD_DT_SK"),
        col("ref_Clm.ACTL_PD_AMT").alias("ACTL_PD_AMT"),
        col("ref_Clm.PAYBL_AMT").alias("PAYBL_AMT"),
        col("ref_Clm.FNCL_LOB_CD").alias("FNCL_LOB_CD_1"),
        col("ref_Clm.RCST_LOB_CD").alias("RCST_LOB_CD"),
        col("ref_Clm.PCA_TYP_CD").alias("PCA_TYP_CD"),
        col("ref_Clm.ADJ_FROM_CLM_SK").alias("ADJ_FROM_CLM_SK"),
        col("ref_Clm.PAYOVR_CD").alias("PAYOVR_CD"),
        col("ref_Clm.PCA_PD_AMT").alias("PCA_PD_AMT"),
        col("ref_Adj.ADJ_FROM_CLM_SK").alias("ADJ_FROM_CLM_SK_1"),
        col("ref_Adj.PCA_CLM_IN").alias("PCA_CLM_IN_2"),
        col("ref_Adj.CLM_SK").alias("ADJ_CLM_SK"),
        col("ref_Adj.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK_1"),
        col("ref_Adj.CLM_ID").alias("CLM_ID_1"),
        col("ref_Adj.FNCL_LOB_SK").alias("FNCL_LOB_SK_2"),
        col("ref_Adj.GRP_ID").alias("GRP_ID_1"),
        col("ref_Adj.PROD_SK").alias("PROD_SK_1"),
        col("ref_Adj.CLM_TYP_CD_SK").alias("CLM_TYP_CD_SK_1"),
        col("ref_Adj.CLM_TYP_CD").alias("CLM_TYP_CD_1"),
        col("ref_Adj.PCA_CHK_IN").alias("PCA_CHK_IN_1"),
        col("ref_Adj.CLM_REMIT_HIST_PAYMTOVRD_CD_SK").alias("CLM_REMIT_HIST_PAYMTOVRD_CD_SK_1"),
        col("ref_Adj.PCA_TYP_CD_SK").alias("PCA_TYP_CD_SK_1"),
        col("ref_Adj.PD_DT_SK").alias("PD_DT_SK_1"),
        col("ref_Adj.ACTL_PD_AMT").alias("ACTL_PD_AMT_1"),
        col("ref_Adj.PAYBL_AMT").alias("PAYBL_AMT_1"),
        col("ref_Adj.FNCL_LOB_CD").alias("FNCL_LOB_CD_2"),
        col("ref_Adj.RCST_LOB_CD").alias("RCST_LOB_CD_1"),
        col("ref_Adj.PCA_TYP_CD").alias("PCA_TYP_CD_1"),
        col("ref_Adj.PAYOVR_CD").alias("PAYOVR_CD_1"),
        col("ref_Adj.PCA_PD_AMT").alias("PCA_PD_AMT_1"),
        col("ref_Clm.Dummy").alias("Dummy_Clm"),
        col("ref_Adj.Dummy").alias("Dummy_Adj")
    )
)

# ----------------------------------------------------------------------------------------------------
# STAGE: xfrm_GLLogic (CTransformerStage) => 2 output links
# ----------------------------------------------------------------------------------------------------
df_xfrm_GLLogic_base = df_lkp_GL

# Stage Variables:
# svClmRefMatch = If IsNull(Dummy_Clm) then 'NO' else  if TRGT_CD='CR' then 'NO' else 'YES'
# svAdjRefMatch = If IsNull(Dummy_Adj) then 'NO' else  if TRGT_CD='DR' then 'NO' else 'YES'
# Then constraints:
# lnk_XfrmGLlogic_in => (svAdjRefMatch='NO' and svClmRefMatch='NO')
# All columns with sets, but here we produce only that link in code.

df_xfrm_GLLogic_sv = df_xfrm_GLLogic_base.select(
    when(col("Dummy_Clm").isNull(), lit("NO"))
    .otherwise(
        when(col("TRGT_CD")=="CR", lit("NO")).otherwise(lit("YES"))
    ).alias("svClmRefMatch"),
    when(col("Dummy_Adj").isNull(), lit("NO"))
    .otherwise(
        when(col("TRGT_CD")=="DR", lit("NO")).otherwise(lit("YES"))
    ).alias("svAdjRefMatch"),
    col("*")
)

df_lnk_XfrmGLlogic_in = df_xfrm_GLLogic_sv.filter(
    (col("svAdjRefMatch")=="NO") & (col("svClmRefMatch")=="NO")
).select(
    lit(0).alias("GL_CLM_BAL_DTL_SK"),
    lit("UNK").alias("EDW_SRC_SYS_CD"),
    lit("UNK").alias("EDW_CLM_ID"),
    col("SRC_TRANS_CK").alias("GL_CLM_DTL_CK"),
    col("PCA_CLM_IN").alias("PCA_IN"),
    lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    lit(0).alias("CLM_SK"),
    when(trim(col("ACCT_NO"))=="5446", lit("DNTL")).otherwise(lit("MED")).alias("CLS_PLN_PROD_CAT_CD"),
    lit("NA").alias("CLM_REMIT_HIST_PAYMT_OVRD_CD"),
    lit("0").alias("EDW_FNCL_LOB_CD"),
    col("FNCL_LOB_CD").alias("GL_FNCL_LOB_CD"),
    lit("UNK").alias("RCST_FNCL_LOB_CD"),
    lit("N").alias("IN_BAL_IN"),
    lit(current_timestamp()).alias("CRT_DTM"),
    (substring(col("ACCTG_DT_SK"),1,4) + substring(col("ACCTG_DT_SK"),6,2)).alias("PD_YR_MO"),
    lit(0.00).alias("DIFF_AMT"),
    lit(0.00).alias("EDW_CLM_ACTL_PD_AMT"),
    when(col("TRGT_CD")=="CR", col("JRNL_ENTRY_TRANS_AMT")*-1).otherwise(col("JRNL_ENTRY_TRANS_AMT")).alias("GL_PD_AMT"),
    lit(0.00).alias("PCA_RUNOUT_AMT"),
    col("ACCT_NO").alias("GL_ACCT_NO"),
    lit("UNK").alias("GRP_ID"),
    lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

# ----------------------------------------------------------------------------------------------------
# STAGE: lkp_AdjClm (PxLookup) => primary link=lnk_Adj_in, lookup=ref_Gl_Adj (left)
# ----------------------------------------------------------------------------------------------------
df_lkp_AdjClm = df_lnk_Adj_in.alias("lnk_Adj_in").join(
    df_RmDup_GlAdj.alias("ref_Gl_Adj"),
    on=[
        col("lnk_Adj_in.ADJ_FROM_CLM_SK") == col("ref_Gl_Adj.TRANS_TBL_SK"),
        col("lnk_Adj_in.PCA_CLM_IN") == col("ref_Gl_Adj.PCA_CLM_IN")
    ],
    how="left"
).select(
    col("lnk_Adj_in.ADJ_FROM_CLM_SK").alias("ADJ_FROM_CLM_SK"),
    col("lnk_Adj_in.PCA_CLM_IN").alias("PCA_CLM_IN"),
    col("lnk_Adj_in.CLM_SK").alias("CLM_SK"),
    col("lnk_Adj_in.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    col("lnk_Adj_in.CLM_ID").alias("CLM_ID"),
    col("lnk_Adj_in.FNCL_LOB_SK").alias("FNCL_LOB_SK"),
    col("lnk_Adj_in.GRP_ID").alias("GRP_ID"),
    col("lnk_Adj_in.PROD_SK").alias("PROD_SK"),
    col("lnk_Adj_in.CLM_TYP_CD_SK").alias("CLM_TYP_CD_SK"),
    col("lnk_Adj_in.CLM_TYP_CD").alias("CLM_TYP_CD"),
    col("lnk_Adj_in.PCA_CHK_IN").alias("PCA_CHK_IN"),
    col("lnk_Adj_in.CLM_REMIT_HIST_PAYMTOVRD_CD_SK").alias("CLM_REMIT_HIST_PAYMTOVRD_CD_SK"),
    col("lnk_Adj_in.PCA_TYP_CD_SK").alias("PCA_TYP_CD_SK"),
    col("lnk_Adj_in.PD_DT_SK").alias("PD_DT_SK"),
    col("lnk_Adj_in.ACTL_PD_AMT").alias("ACTL_PD_AMT"),
    col("lnk_Adj_in.PAYBL_AMT").alias("PAYBL_AMT"),
    col("lnk_Adj_in.FNCL_LOB_CD").alias("FNCL_LOB_CD"),
    col("lnk_Adj_in.RCST_LOB_CD").alias("RCST_LOB_CD"),
    col("lnk_Adj_in.PCA_TYP_CD").alias("PCA_TYP_CD"),
    col("lnk_Adj_in.PAYOVR_CD").alias("PAYOVR_CD"),
    col("lnk_Adj_in.PCA_PD_AMT").alias("PCA_PD_AMT"),
    col("ref_Gl_Adj.JRNL_ENTRY_TRANS_AMT").alias("JRNL_ENTRY_TRANS_AMT"),
    col("ref_Gl_Adj.FNCL_LOB_CD").alias("FNCL_LOB_CD_1"),
    col("ref_Gl_Adj.ACCT_NO").alias("ACCT_NO"),
    col("ref_Gl_Adj.SRC_TRANS_CK").alias("SRC_TRANS_CK")
)

# ----------------------------------------------------------------------------------------------------
# STAGE: xfrm_AdjClmLogic (CTransformerStage) => one output link
# ----------------------------------------------------------------------------------------------------
df_xfrm_AdjClmLogic_base = df_lkp_AdjClm

df_lnk_xfrmAdjClmlogic_in = df_xfrm_AdjClmLogic_base.select(
    lit(0).alias("GL_CLM_BAL_DTL_SK"),
    lit("FACETS").alias("EDW_SRC_SYS_CD"),
    col("CLM_ID").alias("EDW_CLM_ID"),
    when(col("SRC_TRANS_CK").isNull(), lit(0)).otherwise(col("SRC_TRANS_CK")).alias("GL_CLM_DTL_CK"),
    col("PCA_CLM_IN").alias("PCA_IN"),
    lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    col("CLM_SK").alias("CLM_SK"),
    col("CLM_TYP_CD").alias("CLS_PLN_PROD_CAT_CD"),
    when((col("PCA_CHK_IN").isNull()) | (col("PCA_CHK_IN")=='N'), lit("NA")).otherwise(col("PAYOVR_CD")).alias("CLM_REMIT_HIST_PAYMT_OVRD_CD"),
    col("FNCL_LOB_CD").alias("EDW_FNCL_LOB_CD"),
    when((col("FNCL_LOB_CD_1").isNull()) | (trim(col("FNCL_LOB_CD_1"))==''), lit("UNK")).otherwise(col("FNCL_LOB_CD_1")).alias("GL_FNCL_LOB_CD"),
    col("RCST_LOB_CD").alias("RCST_FNCL_LOB_CD"),
    lit("N").alias("IN_BAL_IN"),
    lit(current_timestamp()).alias("CRT_DTM"),
    (substring(col("PD_DT_SK"),1,4) + substring(col("PD_DT_SK"),6,2)).alias("PD_YR_MO"),
    lit(0).alias("DIFF_AMT"),
    when(
        col("PCA_TYP_CD").isin("RUNOUT","PCA","EMPWBNF"), col("PAYBL_AMT")
    ).otherwise(
        when(col("PCA_CLM_IN")=='Y', col("PCA_PD_AMT")).otherwise(col("ACTL_PD_AMT"))
    ).alias("EDW_CLM_ACTL_PD_AMT"),
    when(col("JRNL_ENTRY_TRANS_AMT").isNull(), lit(0)).otherwise(col("JRNL_ENTRY_TRANS_AMT") * -1).alias("GL_PD_AMT"),
    when(
        col("PCA_TYP_CD").isin("RUNOUT","PCA","EMPWBNF"), col("PAYBL_AMT")
    ).otherwise(lit(0)).alias("PCA_RUNOUT_AMT"),
    when((col("ACCT_NO").isNull()) | (trim(col("ACCT_NO"))==''), lit("NA")).otherwise(col("ACCT_NO")).alias("GL_ACCT_NO"),
    col("GRP_ID").alias("GRP_ID"),
    lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

# ----------------------------------------------------------------------------------------------------
# STAGE: Fnl_GLClmBalDtlF (PxFunnel) => merges 3 input links
# ----------------------------------------------------------------------------------------------------
df_Fnl_GLClmBalDtlF = df_lnk_XfrmGLlogic_in.unionByName(df_lnk_xfrmClmlogic_in, allowMissingColumns=True).unionByName(df_lnk_xfrmAdjClmlogic_in, allowMissingColumns=True)

# ----------------------------------------------------------------------------------------------------
# STAGE: xfrm_Business_rules (CTransformerStage) => has 3 output links, but effectively we gather full set
#               We produce lnk_fnlclmdata_out, plus "NA" row, plus "UNK" row. 
#               For code, we'll handle them as separate filters + union, then final funnel merges them.
# ----------------------------------------------------------------------------------------------------
# We compute svAmtDiff = GL_PD_AMT - EDW_CLM_ACTL_PD_AMT
df_xfrm_Business_rules_base = df_Fnl_GLClmBalDtlF.select(
    col("GL_CLM_BAL_DTL_SK"),
    col("EDW_SRC_SYS_CD"),
    col("EDW_CLM_ID"),
    col("GL_CLM_DTL_CK"),
    col("PCA_IN"),
    col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("CRT_RUN_CYC_EXCTN_DT_SK"),
    col("CLM_SK"),
    col("CLS_PLN_PROD_CAT_CD"),
    col("CLM_REMIT_HIST_PAYMT_OVRD_CD"),
    col("EDW_FNCL_LOB_CD"),
    col("GL_FNCL_LOB_CD"),
    col("RCST_FNCL_LOB_CD"),
    col("IN_BAL_IN"),
    col("CRT_DTM"),
    col("PD_YR_MO"),
    col("DIFF_AMT"),
    col("EDW_CLM_ACTL_PD_AMT"),
    col("GL_PD_AMT"),
    col("PCA_RUNOUT_AMT"),
    col("GL_ACCT_NO"),
    col("GRP_ID"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK")
).withColumn(
    "svAmtDiff", col("GL_PD_AMT") - col("EDW_CLM_ACTL_PD_AMT")
)

# lnk_Full_Data_Out
df_lnk_Full_Data_Out = df_xfrm_Business_rules_base.select(
    col("GL_CLM_BAL_DTL_SK").alias("GL_CLM_BAL_DTL_SK"),
    col("EDW_SRC_SYS_CD").alias("EDW_SRC_SYS_CD"),
    col("EDW_CLM_ID").alias("EDW_CLM_ID"),
    col("GL_CLM_DTL_CK").alias("GL_CLM_DTL_CK"),
    col("PCA_IN").alias("PCA_IN"),
    col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    col("CLM_SK").alias("CLM_SK"),
    col("CLS_PLN_PROD_CAT_CD").alias("CLS_PLN_PROD_CAT_CD"),
    col("CLM_REMIT_HIST_PAYMT_OVRD_CD").alias("CLM_REMIT_HIST_PAYMT_OVRD_CD"),
    col("EDW_FNCL_LOB_CD").alias("EDW_FNCL_LOB_CD"),
    col("GL_FNCL_LOB_CD").alias("GL_FNCL_LOB_CD"),
    col("RCST_FNCL_LOB_CD").alias("RCST_FNCL_LOB_CD"),
    when(col("svAmtDiff")==0, lit("Y")).otherwise(lit("N")).alias("IN_BAL_IN"),
    col("CRT_DTM").alias("CRT_DTM"),
    col("PD_YR_MO").alias("PD_YR_MO"),
    col("svAmtDiff").alias("DIFF_AMT"),
    col("EDW_CLM_ACTL_PD_AMT").alias("EDW_PD_AMT"),
    col("GL_PD_AMT").alias("GL_PD_AMT"),
    col("PCA_RUNOUT_AMT").alias("PCA_RUNOUT_AMT"),
    col("GL_ACCT_NO").alias("GL_ACCT_NO"),
    col("GRP_ID").alias("GRP_ID"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

# lnk_NA_Out => one synthetic row if @INROWNUM=1, but in code we just build a single-row DF
df_lnk_NA_Out = df_xfrm_Business_rules_base.limit(1).select(
    lit(1).alias("GL_CLM_BAL_DTL_SK"),
    lit("NA").alias("EDW_SRC_SYS_CD"),
    lit("NA").alias("EDW_CLM_ID"),
    lit(0).alias("GL_CLM_DTL_CK"),
    lit("N").alias("PCA_IN"),
    lit("1753-01-01").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    lit(1).alias("CLM_SK"),
    lit("NA").alias("CLS_PLN_PROD_CAT_CD"),
    lit("NA").alias("CLM_REMIT_HIST_PAYMT_OVRD_CD"),
    lit("NA").alias("EDW_FNCL_LOB_CD"),
    lit("NA").alias("GL_FNCL_LOB_CD"),
    lit("NA").alias("RCST_FNCL_LOB_CD"),
    lit("N").alias("IN_BAL_IN"),
    lit("1753-01-01 00:00:00").alias("CRT_DTM"),
    lit("175301").alias("PD_YR_MO"),
    lit(0).alias("DIFF_AMT"),
    lit(None).alias("EDW_PD_AMT"),
    lit(None).alias("GL_PD_AMT"),
    lit(0).alias("PCA_RUNOUT_AMT"),
    lit("NA").alias("GL_ACCT_NO"),
    lit("NA").alias("GRP_ID"),
    lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

# lnk_UNK_Out => one synthetic row if @INROWNUM=1
df_lnk_UNK_Out = df_xfrm_Business_rules_base.limit(1).select(
    lit(0).alias("GL_CLM_BAL_DTL_SK"),
    lit("UNK").alias("EDW_SRC_SYS_CD"),
    lit("UNK").alias("EDW_CLM_ID"),
    lit(0).alias("GL_CLM_DTL_CK"),
    lit("N").alias("PCA_IN"),
    lit("1753-01-01").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    lit(0).alias("CLM_SK"),
    lit("UNK").alias("CLS_PLN_PROD_CAT_CD"),
    lit("UNK").alias("CLM_REMIT_HIST_PAYMT_OVRD_CD"),
    lit("UNK").alias("EDW_FNCL_LOB_CD"),
    lit("UNK").alias("GL_FNCL_LOB_CD"),
    lit("UNK").alias("RCST_FNCL_LOB_CD"),
    lit("N").alias("IN_BAL_IN"),
    lit("1753-01-01 00:00:00").alias("CRT_DTM"),
    lit("175301").alias("PD_YR_MO"),
    lit(0).alias("DIFF_AMT"),
    lit(None).alias("EDW_PD_AMT"),
    lit(None).alias("GL_PD_AMT"),
    lit(0).alias("PCA_RUNOUT_AMT"),
    lit("UNK").alias("GL_ACCT_NO"),
    lit("UNK").alias("GRP_ID"),
    lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

# ----------------------------------------------------------------------------------------------------
# STAGE: Fnl_UNK_NA_data (PxFunnel) => merges lnk_NA_Out, lnk_UNK_Out, lnk_Full_Data_Out
# ----------------------------------------------------------------------------------------------------
df_Fnl_UNK_NA_data = df_lnk_NA_Out.unionByName(df_lnk_UNK_Out, allowMissingColumns=True).unionByName(df_lnk_Full_Data_Out, allowMissingColumns=True)

# ----------------------------------------------------------------------------------------------------
# STAGE: ds_GL_CLM_BAL_DTL_F_Extr (PxDataSet) => must translate to a parquet file
# ----------------------------------------------------------------------------------------------------
df_ds_GL_CLM_BAL_DTL_F_Extr = df_Fnl_UNK_NA_data.select(
    col("GL_CLM_BAL_DTL_SK"),
    col("EDW_SRC_SYS_CD"),
    col("EDW_CLM_ID"),
    col("GL_CLM_DTL_CK"),
    col("PCA_IN"),
    col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("CRT_RUN_CYC_EXCTN_DT_SK"),
    col("CLM_SK"),
    col("CLS_PLN_PROD_CAT_CD"),
    col("CLM_REMIT_HIST_PAYMT_OVRD_CD"),
    col("EDW_FNCL_LOB_CD"),
    col("GL_FNCL_LOB_CD"),
    col("RCST_FNCL_LOB_CD"),
    col("IN_BAL_IN"),
    col("CRT_DTM"),
    col("PD_YR_MO"),
    col("DIFF_AMT"),
    col("EDW_PD_AMT"),
    col("GL_PD_AMT"),
    col("PCA_RUNOUT_AMT"),
    col("GL_ACCT_NO"),
    col("GRP_ID"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

# For final output, apply rpad on columns that are char:
df_final_output = (
    df_ds_GL_CLM_BAL_DTL_F_Extr
    .withColumn("PCA_IN", rpad(col("PCA_IN"), 1, " "))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", rpad(col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", rpad(col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
    .withColumn("IN_BAL_IN", rpad(col("IN_BAL_IN"), 1, " "))
    .withColumn("PD_YR_MO", rpad(col("PD_YR_MO"), 6, " "))
)

write_files(
    df_final_output,
    "GL_CLM_BAL_DTL_F.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)