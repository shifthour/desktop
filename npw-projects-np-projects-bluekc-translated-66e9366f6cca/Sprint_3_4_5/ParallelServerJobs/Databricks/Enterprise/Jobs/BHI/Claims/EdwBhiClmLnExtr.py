# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2013 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC JOB NAME:  EdwBhiClmLnrExtr
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:  Extract Claim Lines data to be sent to BCBSA for BHI (Blue Health Intelligence)
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  EdwBhiClmExtrSeq
# MAGIC 
# MAGIC HASH FILES:  
# MAGIC 
# MAGIC 
# MAGIC RERUN INFORMATION: 
# MAGIC 
# MAGIC              PREVIOUS RUN SUCCESSFUL:   Nothing to reset. 
# MAGIC              PREVIOUS RUN ABORTED:         Nothing to reset, just restart.
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                          Date                    Project/Ticket #\(9)           Change Description\(9)\(9)\(9)              Development Project\(9)      Code Reviewer\(9)        Date Reviewed       
# MAGIC ------------------                    -------------------            ---------------------------\(9)           -----------------------------------------------------------------------            --------------------------------\(9)      -------------------------         -------------------------    
# MAGIC Rajitha Vadlamudi            2013-08-14                    5115 BHI                             Original programming                                                                      EnterpriseNewDevl          Kalyan Neelam          2013-10-31                 
# MAGIC 
# MAGIC Bhoomi Dasari                 2014-01-24                    5115 BHI                               Additinal requirements                                                                       EnterpriseNewDevl          Kalyan Neelam           2014-01-27
# MAGIC 
# MAGIC Praveen Annam               2014-02-27                    5115 BHI                              Additional Requirements                                                                   EnterpriseCurDevl           Bhoomi Dasari            2/27/2014
# MAGIC 
# MAGIC Praveen Annam               2014-05-07                    5115 BHI                             Modified code with Delta SQL                                                            EnterpriseNewDevl        Kalyan Neelam          2014-05-12
# MAGIC                                                                                                                            and assigned Zero values to Amount fields  
# MAGIC                                                                                                                           for reversals
# MAGIC 
# MAGIC Praveen Annam            2014-07-14               5115 BHI                                       FTP stage added to transfer in binary mode                                      EnterpriseNewDevl          Bhoomi Dasari           7/15/2014
# MAGIC 
# MAGIC Bhoomi Dasari               8/1/2014                 5115/BHI                                    Added new logic to extract logic based on PROD_ID                          EnterpriseNewDevl           Kalyan Neelam          2014-08-04
# MAGIC  
# MAGIC Aishwarya                      2016-06-14                    5604 BHI                               Source query modified to pull CLM_F.CLM_SVC_STRT_YR_MO_SK  EnterpriseDevl                  Kalyan Neelam        2016-07-07
# MAGIC                                                                                                                           >= Current Month-3 years
# MAGIC Akhila M                        2016-07-21                    BHI                Changed main extract join with P_BCBSA_BHI_EXCD_XREF.BHI_NCOV_RSN_CD  EnterpriseDevl                  Jag Yelavarthi          2016-07-22
# MAGIC                                                                                                   to Left outer from inner and removed the join condition  BHI_NCOV_RSN_CD
# MAGIC                                                                                                   from NonCovRsn_Line stage.
# MAGIC 
# MAGIC Akhila M                        2016-07-26                   BHI               update default to reason code as '99' in Transformer Stage                                              EnterpriseDevl                Jag Yelavarthi           2016-07-27
# MAGIC 
# MAGIC Mohan Karnati             2020-07-24               US-252543                                    Including High Performance n/w  in product short  
# MAGIC                                                                                                                               name while  extracting in the source query                                        EnterpriseDev1               Hugh Sisson              2020-08-18
# MAGIC 
# MAGIC Tamannakumari           2024-02-29               US- 612200                          Include additional BMADVH and BMADVP in source query
# MAGIC                                                                                                                   (where PROD_SH_NM = 'BMADVH', 'BMADVP')                                          EnterpriseDev1              Jeyaprasanna            2024-03-07

# MAGIC Control File is created with file name and respective counts, write mode is set as append to capture information for all BHI files
# MAGIC Dataset generated in EdwBhiMbrEnrDsLd job
# MAGIC BHI Claim Lines Extract 
# MAGIC Create fixed length data and provide monthly file to BHI
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F, types as T
# COMMAND ----------
# MAGIC %run ../../../../Utility_Enterprise
# COMMAND ----------


edw_secret_name = get_widget_value('edw_secret_name','')
EDWOwner = get_widget_value('EDWOwner','')
BeginYrMo = get_widget_value('BeginYrMo','')
EndYrMo = get_widget_value('EndYrMo','')
PaidEndYrMo = get_widget_value('PaidEndYrMo','')
CurrDate = get_widget_value('CurrDate','')
StartDate = get_widget_value('StartDate','')
EndDate = get_widget_value('EndDate','')
LastRunCycDt = get_widget_value('LastRunCycDt','')
ProdIn = get_widget_value('ProdIn','')
YrMoMinus3 = get_widget_value('YrMoMinus3','')

jdbc_url_edw, jdbc_props_edw = get_db_config(edw_secret_name)

df_NonCovRsn_Line = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_edw)
    .options(**jdbc_props_edw)
    .option(
        "query",
        f"""
SELECT DISTINCT
   clm.CLM_ID,
  clmln.CLM_LN_FINL_DISP_CD,
  xref.BHI_NCOV_RSN_CD
FROM {EDWOwner}.CLM_F clm,
     {EDWOwner}.CLM_LN_F clmln,
     {EDWOwner}.EXCD_D excdd,
     {EDWOwner}.P_BCBSA_BHI_EXCD_XREF xref
where
clm.CLM_SK = clmln.CLM_SK
AND excdd.EXCD_ID = xref.EXCD_ID
AND clmln.CLM_LN_DSALW_EXCD_SK = excdd.EXCD_SK
AND clm.CLM_SVC_STRT_YR_MO_SK >= '{YrMoMinus3}'
AND clm.CLM_PD_YR_MO_SK BETWEEN '{BeginYrMo}' AND '{PaidEndYrMo}'
AND clmln.CLM_LN_FINL_DISP_CD = 'DENIEDREJ'
AND xref.BHI_NCOV_RSN_CD = '60'
AND clm.SRC_SYS_CD = 'FACETS'
AND clm.CLM_HOST_IN = 'N'
AND clm.CLM_SUBTYP_CD IN ('IP','OP')
AND clm.CLM_FINL_DISP_CD NOT IN ('CLSDDEL','SUSP')
AND clm.CLM_TYP_CD = 'MED'
AND clm.PROD_SH_NM IN ('PCB', 'PC', 'BCARE', 'BLUE-ACCESS', 'BLUE-SELECT','BLUESELECT+','HP','BMADVH', 'BMADVP')
AND clm.GRP_ID NOT IN ('10023000', '10024000')
"""
    )
    .load()
)

df_W_BHI_MBR_ENR_ds = spark.read.parquet(f"{adls_path}/ds/W_BHI_MBR_ENR.parquet")

df_Claim_Line = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_edw)
    .options(**jdbc_props_edw)
    .option(
        "query",
        f"""
SELECT
  clm.CLM_ID,
  clm.CLM_NTWK_STTUS_CD,
  clm.CLM_COB_IN,
  clmln.CLM_LN_SEQ_NO,
  clmln.CLM_LN_FINL_DISP_CD,
  clmln.CLM_LN_SVC_STRT_DT_SK,
  CASE WHEN clm.CLM_STTUS_CD = 'A08' THEN '0.00' ELSE clmln.CLM_LN_ALW_AMT END AS CLM_LN_ALW_AMT,
  CASE WHEN clm.CLM_STTUS_CD = 'A08' THEN '0.00' ELSE clmln.CLM_LN_CHRG_AMT END AS CLM_LN_CHRG_AMT,
  CASE WHEN clm.CLM_STTUS_CD = 'A08' THEN '0.00' ELSE clmln.CLM_LN_COINS_AMT END AS CLM_LN_COINS_AMT,
  CASE WHEN clm.CLM_STTUS_CD = 'A08' THEN '0.00' ELSE clmln.CLM_LN_COPAY_AMT END AS CLM_LN_COPAY_AMT,
  CASE WHEN clm.CLM_STTUS_CD = 'A08' THEN '0.00' ELSE clmln.CLM_LN_DEDCT_AMT END AS CLM_LN_DEDCT_AMT,
  CASE WHEN clm.CLM_STTUS_CD = 'A08' THEN '0.00' ELSE clmln.CLM_LN_DSALW_AMT END AS CLM_LN_DSALW_AMT,
  CASE WHEN clm.CLM_STTUS_CD = 'A08' THEN '0.00' ELSE clmln.CLM_LN_PAYBL_AMT END AS CLM_LN_PAYBL_AMT,
  CASE WHEN clm.CLM_STTUS_CD = 'A08' THEN '0.00' ELSE clmln.CLM_LN_UNIT_CT END AS CLM_LN_UNIT_CT,
  CASE WHEN clm.CLM_STTUS_CD = 'A08' THEN '0.00' ELSE (clmln.CLM_LN_ALW_AMT - clmln.CLM_LN_PAYBL_AMT - clmln.CLM_LN_COINS_AMT - clmln.CLM_LN_COPAY_AMT - clmln.CLM_LN_DEDCT_AMT )  END AS COB_TPL_AMT,
  clmln.CLM_LN_PRI_PROC_CD_MOD_TX,
  proc.PROC_CD,
  rvnu.RVNU_CD,
  clm.MBR_SK,
  mbr.MBR_UNIQ_KEY,
  clm.CLM_STTUS_CD,
  prodd.PROD_ID,
  excdd.EXCD_ID
FROM {EDWOwner}.CLM_F clm,
     {EDWOwner}.CLM_LN_F clmln,
     {EDWOwner}.PROC_CD_D proc,
     {EDWOwner}.RVNU_CD_D rvnu,
     {EDWOwner}.GRP_D grp,
     {EDWOwner}.CLS_D cls,
     {EDWOwner}.EXCD_D excdd,
     {EDWOwner}.MBR_D mbr,
     {EDWOwner}.PROD_D prodd,
(
SELECT DISTINCT clmf.CLM_SK
FROM {EDWOwner}.CLM_F clmf,
     {EDWOwner}.CLM_LN_F clmln,
     {EDWOwner}.CLM_F2 clmf2,
     {EDWOwner}.CLS_D cls,
     {EDWOwner}.GRP_D grpd
WHERE clmf.GRP_SK = grpd.GRP_SK
AND clmf.CLS_SK = cls.CLS_SK
AND clmf.CLM_SK = clmln.CLM_SK
AND clmf.CLS_SK = cls.CLS_SK
AND clmf.SRC_SYS_CD = 'FACETS'
AND clmf.CLM_HOST_IN = 'N'
AND clmf.CLM_SVC_STRT_YR_MO_SK >= '{YrMoMinus3}'
AND clmf.CLM_PD_YR_MO_SK BETWEEN '{BeginYrMo}' AND '{EndYrMo}'
AND clmf.CLM_SUBTYP_CD IN ('IP','OP')
AND clmf.CLM_STTUS_CD in ('A02')
AND clmf.CLM_FINL_DISP_CD NOT IN ('CLSDDEL','SUSP')
AND clmf.CLM_TYP_CD = 'MED'
AND clmf.PROD_SH_NM IN ('PCB', 'PC', 'BCARE', 'BLUE-ACCESS', 'BLUE-SELECT','BLUESELECT+','HP','BMADVH', 'BMADVP')
AND clmf.GRP_ID NOT IN ('10023000', '10024000')
AND grpd.PRNT_GRP_ID <> '650600000'
AND cls.CLS_ID <> 'MHIP'
UNION
SELECT DISTINCT clmf.CLM_SK
FROM {EDWOwner}.CLM_F clmf,
     {EDWOwner}.CLM_LN_F clmln,
     {EDWOwner}.CLM_F2 clmf2,
     {EDWOwner}.CLM_F clmf3,
     {EDWOwner}.CLS_D cls,
     {EDWOwner}.GRP_D grpd
WHERE clmf.GRP_SK = grpd.GRP_SK
AND clmf.CLS_SK = cls.CLS_SK
AND clmf.CLM_SK = clmln.CLM_SK
AND clmf.CLS_SK = cls.CLS_SK
AND clmf.SRC_SYS_CD = 'FACETS'
AND clmf.CLM_HOST_IN = 'N'
AND clmf.CLM_SVC_STRT_YR_MO_SK >= '{YrMoMinus3}'
AND clmf.CLM_PD_YR_MO_SK BETWEEN '{BeginYrMo}' AND '{EndYrMo}'
AND clmf.CLM_SUBTYP_CD IN ('IP','OP')
AND clmf.CLM_STTUS_CD in ('A08')
AND clmf.CLM_FINL_DISP_CD NOT IN ('CLSDDEL','SUSP')
AND clmf.CLM_TYP_CD = 'MED'
AND clmf.PROD_SH_NM IN ('PCB', 'PC', 'BCARE', 'BLUE-ACCESS', 'BLUE-SELECT','BLUESELECT+','HP','BMADVH', 'BMADVP')
AND clmf.GRP_ID NOT IN ('10023000', '10024000')
AND grpd.PRNT_GRP_ID <> '650600000'
AND cls.CLS_ID <> 'MHIP'
AND clmf.LAST_UPDT_RUN_CYC_EXCTN_DT_SK > '{LastRunCycDt}'
and clmf.CLM_ADJ_FROM_CLM_SK  = clmf3.CLM_SK
and clmf.CLM_PD_YR_MO_SK <> clmf3.CLM_PD_YR_MO_SK
and clmf3.CLM_PD_YR_MO_SK < '{BeginYrMo}'
) A
where
clm.CLM_SK = clmln.CLM_SK
AND clm.CLM_SK = A.CLM_SK
AND clm.MBR_SK = mbr.MBR_SK
AND clmln.CLM_LN_PROC_CD_SK = proc.PROC_CD_SK
AND clmln.CLM_LN_RVNU_CD_SK = rvnu.RVNU_CD_SK
AND clmln.CLM_LN_DSALW_EXCD_SK = excdd.EXCD_SK
AND clm.GRP_SK = grp.GRP_SK
AND clm.CLS_SK = cls.CLS_SK
AND clm.PROD_SK = prodd.PROD_SK
AND clm.SRC_SYS_CD = 'FACETS'
AND clm.CLM_HOST_IN = 'N'
AND clm.CLM_SVC_STRT_YR_MO_SK >= '{YrMoMinus3}'
AND clm.CLM_PD_YR_MO_SK BETWEEN '{BeginYrMo}' AND '{PaidEndYrMo}'
AND clm.CLM_SUBTYP_CD IN ('IP','OP')
AND clm.CLM_STTUS_CD in ('A02','A08')
AND clm.CLM_FINL_DISP_CD NOT IN ('CLSDDEL','SUSP')
AND clm.CLM_TYP_CD = 'MED'
AND clm.PROD_SH_NM IN ('PCB', 'PC', 'BCARE', 'BLUE-ACCESS', 'BLUE-SELECT','BLUESELECT+','HP','BMADVH', 'BMADVP')
AND clm.GRP_ID NOT IN ('10023000', '10024000')
AND grp.PRNT_GRP_ID <> '650600000'
AND cls.CLS_ID <> 'MHIP'
"""
    )
    .load()
)

df_Edw_EXCD_XREF = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_edw)
    .options(**jdbc_props_edw)
    .option(
        "query",
        f"""
SELECT DISTINCT
 EXCD_ID,
 BHI_NCOV_RSN_CD
 FROM {EDWOwner}.P_BCBSA_BHI_EXCD_XREF
"""
    )
    .load()
)

df_Lkp_EXCD_XREF = (
    df_Claim_Line.alias("Lnk_EXCD_XREF")
    .join(
        df_Edw_EXCD_XREF.alias("LkFrom_EXCD_XREF"),
        F.col("Lnk_EXCD_XREF.EXCD_ID") == F.col("LkFrom_EXCD_XREF.EXCD_ID"),
        "left",
    )
    .select(
        F.col("Lnk_EXCD_XREF.CLM_ID").alias("CLM_ID"),
        F.col("Lnk_EXCD_XREF.CLM_NTWK_STTUS_CD").alias("CLM_NTWK_STTUS_CD"),
        F.col("Lnk_EXCD_XREF.CLM_COB_IN").alias("CLM_COB_IN"),
        F.col("Lnk_EXCD_XREF.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
        F.col("Lnk_EXCD_XREF.CLM_LN_FINL_DISP_CD").alias("CLM_LN_FINL_DISP_CD"),
        F.col("Lnk_EXCD_XREF.CLM_LN_SVC_STRT_DT_SK").alias("CLM_LN_SVC_STRT_DT_SK"),
        F.col("Lnk_EXCD_XREF.CLM_LN_ALW_AMT").alias("CLM_LN_ALW_AMT"),
        F.col("Lnk_EXCD_XREF.CLM_LN_CHRG_AMT").alias("CLM_LN_CHRG_AMT"),
        F.col("Lnk_EXCD_XREF.CLM_LN_COINS_AMT").alias("CLM_LN_COINS_AMT"),
        F.col("Lnk_EXCD_XREF.CLM_LN_COPAY_AMT").alias("CLM_LN_COPAY_AMT"),
        F.col("Lnk_EXCD_XREF.CLM_LN_DEDCT_AMT").alias("CLM_LN_DEDCT_AMT"),
        F.col("Lnk_EXCD_XREF.CLM_LN_DSALW_AMT").alias("CLM_LN_DSALW_AMT"),
        F.col("Lnk_EXCD_XREF.CLM_LN_PAYBL_AMT").alias("CLM_LN_PAYBL_AMT"),
        F.col("Lnk_EXCD_XREF.CLM_LN_UNIT_CT").alias("CLM_LN_UNIT_CT"),
        F.col("Lnk_EXCD_XREF.COB_TPL_AMT").alias("COB_TPL_AMT"),
        F.col("Lnk_EXCD_XREF.CLM_LN_PRI_PROC_CD_MOD_TX").alias("CLM_LN_PRI_PROC_CD_MOD_TX"),
        F.col("Lnk_EXCD_XREF.PROC_CD").alias("PROC_CD"),
        F.col("Lnk_EXCD_XREF.RVNU_CD").alias("RVNU_CD"),
        F.col("LkFrom_EXCD_XREF.BHI_NCOV_RSN_CD").alias("BHI_NCOV_RSN_CD"),
        F.col("Lnk_EXCD_XREF.MBR_SK").alias("MBR_SK"),
        F.col("Lnk_EXCD_XREF.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        F.col("Lnk_EXCD_XREF.CLM_STTUS_CD").alias("CLM_STTUS_CD"),
        F.col("Lnk_EXCD_XREF.PROD_ID").alias("PROD_ID"),
        F.col("Lnk_EXCD_XREF.EXCD_ID").alias("EXCD_ID"),
    )
)

df_Transformer = (
    df_Lkp_EXCD_XREF
    .select(
        F.col("CLM_ID").alias("CLM_ID"),
        F.col("CLM_NTWK_STTUS_CD").alias("CLM_NTWK_STTUS_CD"),
        F.col("CLM_COB_IN").alias("CLM_COB_IN"),
        F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
        F.col("CLM_LN_FINL_DISP_CD").alias("CLM_LN_FINL_DISP_CD"),
        F.col("CLM_LN_SVC_STRT_DT_SK").alias("CLM_LN_SVC_STRT_DT_SK"),
        F.col("CLM_LN_ALW_AMT").alias("CLM_LN_ALW_AMT"),
        F.col("CLM_LN_CHRG_AMT").alias("CLM_LN_CHRG_AMT"),
        F.col("CLM_LN_COINS_AMT").alias("CLM_LN_COINS_AMT"),
        F.col("CLM_LN_COPAY_AMT").alias("CLM_LN_COPAY_AMT"),
        F.col("CLM_LN_DEDCT_AMT").alias("CLM_LN_DEDCT_AMT"),
        F.col("CLM_LN_DSALW_AMT").alias("CLM_LN_DSALW_AMT"),
        F.col("CLM_LN_PAYBL_AMT").alias("CLM_LN_PAYBL_AMT"),
        F.col("CLM_LN_UNIT_CT").alias("CLM_LN_UNIT_CT"),
        F.col("COB_TPL_AMT").alias("COB_TPL_AMT"),
        F.col("CLM_LN_PRI_PROC_CD_MOD_TX").alias("CLM_LN_PRI_PROC_CD_MOD_TX"),
        F.col("PROC_CD").alias("PROC_CD"),
        F.col("RVNU_CD").alias("RVNU_CD"),
        F.when(F.col("BHI_NCOV_RSN_CD").isNull(), F.lit("99")).otherwise(F.col("BHI_NCOV_RSN_CD")).alias("BHI_NCOV_RSN_CD"),
        F.col("MBR_SK").alias("MBR_SK"),
        F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        F.col("CLM_STTUS_CD").alias("CLM_STTUS_CD"),
        F.col("PROD_ID").alias("PROD_ID"),
    )
)

df_Join_MUK = (
    df_Transformer.alias("join_mb")
    .join(
        df_W_BHI_MBR_ENR_ds.alias("Mbr_MUK"),
        (F.col("join_mb.MBR_UNIQ_KEY") == F.col("Mbr_MUK.MBR_UNIQ_KEY"))
        & (F.col("join_mb.PROD_ID") == F.col("Mbr_MUK.PROD_ID")),
        "inner",
    )
    .select(
        F.col("join_mb.CLM_ID").alias("CLM_ID"),
        F.col("join_mb.CLM_NTWK_STTUS_CD").alias("CLM_NTWK_STTUS_CD"),
        F.col("join_mb.CLM_COB_IN").alias("CLM_COB_IN"),
        F.col("join_mb.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
        F.col("join_mb.CLM_LN_FINL_DISP_CD").alias("CLM_LN_FINL_DISP_CD"),
        F.col("join_mb.CLM_LN_SVC_STRT_DT_SK").alias("CLM_LN_SVC_STRT_DT_SK"),
        F.col("join_mb.CLM_LN_ALW_AMT").alias("CLM_LN_ALW_AMT"),
        F.col("join_mb.CLM_LN_CHRG_AMT").alias("CLM_LN_CHRG_AMT"),
        F.col("join_mb.CLM_LN_COINS_AMT").alias("CLM_LN_COINS_AMT"),
        F.col("join_mb.CLM_LN_COPAY_AMT").alias("CLM_LN_COPAY_AMT"),
        F.col("join_mb.CLM_LN_DEDCT_AMT").alias("CLM_LN_DEDCT_AMT"),
        F.col("join_mb.CLM_LN_DSALW_AMT").alias("CLM_LN_DSALW_AMT"),
        F.col("join_mb.CLM_LN_PAYBL_AMT").alias("CLM_LN_PAYBL_AMT"),
        F.col("join_mb.CLM_LN_UNIT_CT").alias("CLM_LN_UNIT_CT"),
        F.col("join_mb.COB_TPL_AMT").alias("COB_TPL_AMT"),
        F.col("join_mb.CLM_LN_PRI_PROC_CD_MOD_TX").alias("CLM_LN_PRI_PROC_CD_MOD_TX"),
        F.col("join_mb.PROC_CD").alias("PROC_CD"),
        F.col("join_mb.RVNU_CD").alias("RVNU_CD"),
        F.col("join_mb.BHI_NCOV_RSN_CD").alias("BHI_NCOV_RSN_CD"),
        F.col("join_mb.MBR_SK").alias("MBR_SK"),
        F.col("join_mb.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        F.col("join_mb.CLM_STTUS_CD").alias("CLM_STTUS_CD"),
    )
)

df_Lookup = (
    df_Join_MUK.alias("trnss")
    .join(
        df_NonCovRsn_Line.alias("join_rsn"),
        (
            (F.col("trnss.CLM_ID") == F.col("join_rsn.CLM_ID"))
            & (F.col("trnss.BHI_NCOV_RSN_CD") == F.col("join_rsn.BHI_NCOV_RSN_CD"))
        ),
        "left",
    )
    .select(
        F.col("trnss.CLM_ID").alias("CLM_ID"),
        F.col("trnss.CLM_NTWK_STTUS_CD").alias("CLM_NTWK_STTUS_CD"),
        F.col("trnss.CLM_COB_IN").alias("CLM_COB_IN"),
        F.col("trnss.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
        F.col("trnss.CLM_LN_FINL_DISP_CD").alias("CLM_LN_FINL_DISP_CD"),
        F.col("trnss.CLM_LN_SVC_STRT_DT_SK").alias("CLM_LN_SVC_STRT_DT_SK"),
        F.col("trnss.CLM_LN_ALW_AMT").alias("CLM_LN_ALW_AMT"),
        F.col("trnss.CLM_LN_CHRG_AMT").alias("CLM_LN_CHRG_AMT"),
        F.col("trnss.CLM_LN_COINS_AMT").alias("CLM_LN_COINS_AMT"),
        F.col("trnss.CLM_LN_COPAY_AMT").alias("CLM_LN_COPAY_AMT"),
        F.col("trnss.CLM_LN_DEDCT_AMT").alias("CLM_LN_DEDCT_AMT"),
        F.col("trnss.CLM_LN_DSALW_AMT").alias("CLM_LN_DSALW_AMT"),
        F.col("trnss.CLM_LN_PAYBL_AMT").alias("CLM_LN_PAYBL_AMT"),
        F.col("trnss.CLM_LN_UNIT_CT").alias("CLM_LN_UNIT_CT"),
        F.col("trnss.COB_TPL_AMT").alias("COB_TPL_AMT"),
        F.col("trnss.CLM_LN_PRI_PROC_CD_MOD_TX").alias("CLM_LN_PRI_PROC_CD_MOD_TX"),
        F.col("trnss.PROC_CD").alias("PROC_CD"),
        F.col("trnss.RVNU_CD").alias("RVNU_CD"),
        F.col("trnss.BHI_NCOV_RSN_CD").alias("BHI_NCOV_RSN_CD"),
        F.col("trnss.MBR_SK").alias("MBR_SK"),
        F.col("trnss.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        F.col("join_rsn.CLM_ID").alias("CLM_ID_RSN_CD"),
        F.col("trnss.CLM_STTUS_CD").alias("CLM_STTUS_CD"),
    )
)

df_BusinessLogic_raw = df_Lookup

df_BusinessLogic = (
    df_BusinessLogic_raw
    .filter(
        (F.substring(F.col("PROC_CD"), 1, 1) != F.lit("V"))
        & (F.col("CLM_ID_RSN_CD") != F.col("CLM_ID"))
    )
    .withColumn(
        "SvCobtplamt1",
        F.when(
            (F.substring(F.col("COB_TPL_AMT"), 1, 1) == "-")
            | (F.col("COB_TPL_AMT").cast("decimal(20,4)") < 0)
            | (F.substring(F.col("COB_TPL_AMT"), 1, 1) == "-"),
            F.lit("0"),
        ).otherwise(F.col("COB_TPL_AMT")),
    )
    .withColumn(
        "svCobtplamt",
        F.when(F.col("CLM_COB_IN") == "Y", F.col("SvCobtplamt1")).otherwise(F.lit("0")),
    )
    .withColumn(
        "svNoncov",
        F.when(F.col("CLM_LN_FINL_DISP_CD") == "DENIEDREJ", F.col("CLM_LN_DSALW_AMT")).otherwise(F.lit("0")),
    )
    .withColumn(
        "svNoncovamt",
        F.when(F.substring(F.col("svNoncov"), 1, 1) == "-", F.lit("0")).otherwise(F.col("svNoncov")),
    )
    .withColumn(
        "svClmId",
        F.when(
            F.substring(F.trim(F.col("CLM_ID")), -1, 1) == "R",
            F.substring(
                F.trim(F.col("CLM_ID")),
                1,
                F.length(F.trim(F.col("CLM_ID"))) - 1,
            ),
        ).otherwise(F.trim(F.col("CLM_ID"))),
    )
    .select(
        F.lit("240").alias("BHI_HOME_PLN_ID"),
        F.expr("Padstring(svClmId, ' ', 25)").alias("CLM_ID"),
        F.expr("Str('0', 3 - LEN(CLM_LN_SEQ_NO)) : CLM_LN_SEQ_NO").alias("CLM_LN_NO"),
        F.lit("FCTS ").alias("TRACEABILITY_FLD"),
        F.expr(
            "If (CLM_STTUS_CD = 'A02') Then (Str('0', 6) : Right(CLM_ID, 2)) Else (Str('0', 7) : (Right(Left(CLM_ID, 12), 2) + 1))"
        ).alias("ADJ_SEQ_NO"),
        F.expr(
            "If (IsNull(PROC_CD) = @TRUE Or Len(PROC_CD) = '' Or PROC_CD = 'NA' Or PROC_CD = 'UNK' Or CLM_STTUS_CD = 'A08') Then Space(6) Else PadString(PROC_CD, ' ', 20 - Len(PROC_CD))"
        ).alias("CPT_AND_HCPCS_CD"),
        F.expr(
            "If (IsNull(CLM_LN_PRI_PROC_CD_MOD_TX) = @TRUE Or Len(CLM_LN_PRI_PROC_CD_MOD_TX) = '' Or CLM_LN_PRI_PROC_CD_MOD_TX = 'NA' Or CLM_LN_PRI_PROC_CD_MOD_TX = 'UNK') Then Space(2) Else CLM_LN_PRI_PROC_CD_MOD_TX"
        ).alias("PROC_MOD"),
        F.expr(
            "If CLM_NTWK_STTUS_CD = 'I' Then 'Y' Else If CLM_NTWK_STTUS_CD = 'P' Then 'O' Else 'N'"
        ).alias("BNF_PAYMT_STTUS_CD"),
        F.expr("If CLM_LN_FINL_DISP_CD = 'DENIEDREJ' Then 'D' Else 'P'").alias("CLM_PAYMT_STTUS"),
        F.expr(
            "If (CLM_LN_FINL_DISP_CD = 'DENIEDREJ' and CLM_LN_DSALW_AMT > '0') Then BHI_NCOV_RSN_CD Else '00'"
        ).alias("NON_COV_RSN_CD_PRI"),
        F.lit("00").alias("NON_COV_RSN_CD_2"),
        F.lit("00").alias("NON_COV_RSN_CD_3"),
        F.lit("00").alias("NON_COV_RSN_CD_4"),
        F.lit("UN   ").alias("RMBRMT_TYP_CD"),
        F.expr("If CLM_STTUS_CD = 'A08' Then Str(' ', 4) Else RVNU_CD").alias("RVNU_CD"),
        F.expr(
            "If (Left(CLM_LN_UNIT_CT, 1) <> '-') then ('+' : Str('0', 8 - Len(padstring(CLM_LN_UNIT_CT, '0', 2))) : padstring(CLM_LN_UNIT_CT, '0', 2)) Else ('-' : Str('0', 8 - Len(Trim(padstring(CLM_LN_UNIT_CT, '0', 2), '-', 'A'))) : Trim(padstring(CLM_LN_UNIT_CT, '0', 2), '-', 'A'))"
        ).alias("TOT_UNIT"),
        F.lit("00010101").alias("SVC_POST_DT"),
        F.expr("Trim(CLM_LN_SVC_STRT_DT_SK, '-', 'A')").alias("SVC_FROM_DT"),
        F.expr(
            "If (Left(CLM_LN_CHRG_AMT, 1) <> '-') Then ('+' : Str('0', 9 - Len(DecimalToString(Trim(CLM_LN_CHRG_AMT, '.', 'A'), \"fix_zero,suppress_zero\"))) : DecimalToString(Trim(CLM_LN_CHRG_AMT, '.', 'A'), \"fix_zero,suppress_zero\")) Else ('-' : Str('0', 9 - Len(DecimalToString(Trim(Trim(CLM_LN_CHRG_AMT, '.', 'A'), '-', 'A'), \"fix_zero,suppress_zero\")))) : DecimalToString(Trim(Trim(CLM_LN_CHRG_AMT, '.', 'A'), '-', 'A'), \"fix_zero,suppress_zero\")"
        ).alias("SUBMT_AMT"),
        F.expr(
            "If (Left(svNoncovamt, 1) <> '-') Then ('+' : Str('0', 9 - Len(DecimalToString(Trim(svNoncovamt, '.', 'A'), \"fix_zero,suppress_zero\"))) : DecimalToString(Trim(svNoncovamt, '.', 'A'), \"fix_zero,suppress_zero\")) Else ('-' : Str('0', 9 - Len(DecimalToString(Trim(Trim(svNoncovamt, '.', 'A'), '-', 'A'), \"fix_zero,suppress_zero\")))) : DecimalToString(Trim(Trim(svNoncovamt, '.', 'A'), '-', 'A'), \"fix_zero,suppress_zero\")"
        ).alias("NON_COV_AMT"),
        F.expr(
            "If (Left(CLM_LN_ALW_AMT, 1) <> '-') Then ('+' : Str('0', 9 - Len(DecimalToString(Trim(CLM_LN_ALW_AMT, '.', 'A'), \"fix_zero,suppress_zero\"))) : DecimalToString(Trim(CLM_LN_ALW_AMT, '.', 'A'), \"fix_zero,suppress_zero\")) Else ('-' : Str('0', 9 - Len(DecimalToString(Trim(Trim(CLM_LN_ALW_AMT, '.', 'A'), '-', 'A'), \"fix_zero,suppress_zero\")))) : DecimalToString(Trim(Trim(CLM_LN_ALW_AMT, '.', 'A'), '-', 'A'), \"fix_zero,suppress_zero\")"
        ).alias("ALW_AMT"),
        F.expr(
            "If (Left(CLM_LN_PAYBL_AMT, 1) <> '-') Then ('+' : Str('0', 9 - Len(DecimalToString(Trim(CLM_LN_PAYBL_AMT, '.', 'A'), \"fix_zero,suppress_zero\"))) : DecimalToString(Trim(CLM_LN_PAYBL_AMT, '.', 'A'), \"fix_zero,suppress_zero\")) Else ('-' : Str('0', 9 - Len(DecimalToString(Trim(Trim(CLM_LN_PAYBL_AMT, '.', 'A'), '-', 'A'), \"fix_zero,suppress_zero\")))) : DecimalToString(Trim(Trim(CLM_LN_PAYBL_AMT, '.', 'A'), '-', 'A'), \"fix_zero,suppress_zero\")"
        ).alias("PAYMT_AMT"),
        F.expr(
            "If (Left(svCobtplamt, 1) <> '-') Then ('+' : Str('0', 9 - Len(DecimalToString(Trim(svCobtplamt, '.', 'A'), \"fix_zero,suppress_zero\"))) : DecimalToString(Trim(svCobtplamt, '.', 'A'), \"fix_zero,suppress_zero\")) Else ('-' : Str('0', 9 - Len(DecimalToString(Trim(Trim(svCobtplamt, '.', 'A'), '-', 'A'), \"fix_zero,suppress_zero\")))) : DecimalToString(Trim(Trim(svCobtplamt, '.', 'A'), '-', 'A'), \"fix_zero,suppress_zero\")"
        ).alias("COB_TPL_AMT"),
        F.expr(
            "If (Left(CLM_LN_COINS_AMT, 1) <> '-') Then ('+' : Str('0', 9 - Len(DecimalToString(Trim(CLM_LN_COINS_AMT, '.', 'A'), \"fix_zero,suppress_zero\"))) : DecimalToString(Trim(CLM_LN_COINS_AMT, '.', 'A'), \"fix_zero,suppress_zero\")) Else ('-' : Str('0', 9 - Len(DecimalToString(Trim(Trim(CLM_LN_COINS_AMT, '.', 'A'), '-', 'A'), \"fix_zero,suppress_zero\")))) : DecimalToString(Trim(Trim(CLM_LN_COINS_AMT, '.', 'A'), '-', 'A'), \"fix_zero,suppress_zero\")"
        ).alias("COINS"),
        F.expr(
            "If (Left(CLM_LN_COPAY_AMT, 1) <> '-') Then ('+' : Str('0', 9 - Len(DecimalToString(Trim(CLM_LN_COPAY_AMT, '.', 'A'), \"fix_zero,suppress_zero\"))) : DecimalToString(Trim(CLM_LN_COPAY_AMT, '.', 'A'), \"fix_zero,suppress_zero\")) Else ('-' : Str('0', 9 - Len(DecimalToString(Trim(Trim(CLM_LN_COPAY_AMT, '.', 'A'), '-', 'A'), \"fix_zero,suppress_zero\")))) : DecimalToString(Trim(Trim(CLM_LN_COPAY_AMT, '.', 'A'), '-', 'A'), \"fix_zero,suppress_zero\")"
        ).alias("COPAY"),
        F.expr(
            "If (Left(CLM_LN_DEDCT_AMT, 1) <> '-') Then ('+' : Str('0', 9 - Len(DecimalToString(Trim(CLM_LN_DEDCT_AMT, '.', 'A'), \"fix_zero,suppress_zero\"))) : DecimalToString(Trim(CLM_LN_DEDCT_AMT, '.', 'A'), \"fix_zero,suppress_zero\")) Else ('-' : Str('0', 9 - Len(DecimalToString(Trim(Trim(CLM_LN_DEDCT_AMT, '.', 'A'), '-', 'A'), \"fix_zero,suppress_zero\")))) : DecimalToString(Trim(Trim(CLM_LN_DEDCT_AMT, '.', 'A'), '-', 'A'), \"fix_zero,suppress_zero\")"
        ).alias("DEDCT"),
    )
)

df_Trans_lnk_agg_Sum = df_BusinessLogic.select(
    F.col("BHI_HOME_PLN_ID").alias("BHI_HOME_PLN_ID"),
    F.col("SUBMT_AMT").alias("SUBMT_AMT"),
    F.col("NON_COV_AMT").alias("NON_COV_AMT"),
    F.col("ALW_AMT").alias("ALW_AMT"),
    F.col("PAYMT_AMT").alias("PAYMT_AMT"),
    F.col("COB_TPL_AMT").alias("COB_TPL_AMT"),
    F.col("COINS").alias("COINS"),
    F.col("COPAY").alias("COPAY"),
    F.col("DEDCT").alias("DEDCT"),
)

df_Trans_Extract = df_BusinessLogic.select(
    F.col("BHI_HOME_PLN_ID").alias("BHI_HOME_PLN_ID"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_LN_NO").alias("CLM_LN_NO"),
    F.col("TRACEABILITY_FLD").alias("TRACEABILITY_FLD"),
    F.col("ADJ_SEQ_NO").alias("ADJ_SEQ_NO"),
    F.col("CPT_AND_HCPCS_CD").alias("CPT_AND_HCPCS_CD"),
    F.col("PROC_MOD").alias("PROC_MOD"),
    F.col("BNF_PAYMT_STTUS_CD").alias("BNF_PAYMT_STTUS_CD"),
    F.col("CLM_PAYMT_STTUS").alias("CLM_PAYMT_STTUS"),
    F.col("NON_COV_RSN_CD_PRI").alias("NON_COV_RSN_CD_PRI"),
    F.col("NON_COV_RSN_CD_2").alias("NON_COV_RSN_CD_2"),
    F.col("NON_COV_RSN_CD_3").alias("NON_COV_RSN_CD_3"),
    F.col("NON_COV_RSN_CD_4").alias("NON_COV_RSN_CD_4"),
    F.col("RMBRMT_TYP_CD").alias("RMBRMT_TYP_CD"),
    F.expr("IF trim(RVNU_CD) = 'NA' Or IsNull(RVNU_CD) Then str(' ', 4) Else RVNU_CD").alias("RVNU_CD"),
    F.col("TOT_UNIT").alias("TOT_UNIT"),
    F.col("SVC_POST_DT").alias("SVC_POST_DT"),
    F.col("SVC_FROM_DT").alias("SVC_FROM_DT"),
    F.col("SUBMT_AMT").alias("SUBMT_AMT"),
    F.col("NON_COV_AMT").alias("NON_COV_AMT"),
    F.col("ALW_AMT").alias("ALW_AMT"),
    F.col("PAYMT_AMT").alias("PAYMT_AMT"),
    F.col("COB_TPL_AMT").alias("COB_TPL_AMT"),
    F.col("COINS").alias("COINS"),
    F.col("COPAY").alias("COPAY"),
    F.col("DEDCT").alias("DEDCT"),
)

df_Trans_Count = df_BusinessLogic.select(
    F.col("BHI_HOME_PLN_ID").alias("BHI_HOME_PLN_ID")
)

write_files(
    df_Trans_Extract,
    f"{adls_path_publish}/external/std_inp_claim_lines",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)

df_Aggregator_sum = (
    df_Trans_lnk_agg_Sum.groupBy("BHI_HOME_PLN_ID")
    .agg(
        F.sum("SUBMT_AMT").alias("TOT_SUBMT_AMT"),
        F.sum("NON_COV_AMT").alias("TOT_NON_COV_AMT"),
        F.sum("ALW_AMT").alias("TOT_ALW_AMT"),
        F.sum("PAYMT_AMT").alias("TOT_PD_AMT"),
        F.sum("COB_TPL_AMT").alias("TOT_COB_TPL_AMT"),
        F.sum("COINS").alias("TOT_COINS_AMT"),
        F.sum("COPAY").alias("TOT_COPAY_AMT"),
        F.sum("DEDCT").alias("TOT_DEDCT_AMT"),
    )
)

df_Aggregator_count = (
    df_Trans_Count.groupBy("BHI_HOME_PLN_ID")
    .agg(F.count("*").alias("COUNT"))
)

df_Join = (
    df_Aggregator_sum.alias("Join_Sum")
    .join(
        df_Aggregator_count.alias("Control_Count"),
        F.col("Join_Sum.BHI_HOME_PLN_ID") == F.col("Control_Count.BHI_HOME_PLN_ID"),
        "inner",
    )
    .select(
        F.col("Join_Sum.BHI_HOME_PLN_ID").alias("BHI_HOME_PLN_ID"),
        F.col("Control_Count.COUNT").alias("COUNT"),
        F.col("Join_Sum.TOT_SUBMT_AMT").alias("TOT_SUBMT_AMT"),
        F.col("Join_Sum.TOT_NON_COV_AMT").alias("TOT_NON_COV_AMT"),
        F.col("Join_Sum.TOT_ALW_AMT").alias("TOT_ALW_AMT"),
        F.col("Join_Sum.TOT_PD_AMT").alias("TOT_PD_AMT"),
        F.col("Join_Sum.TOT_COB_TPL_AMT").alias("TOT_COB_TPL_AMT"),
        F.col("Join_Sum.TOT_COINS_AMT").alias("TOT_COINS_AMT"),
        F.col("Join_Sum.TOT_COPAY_AMT").alias("TOT_COPAY_AMT"),
        F.col("Join_Sum.TOT_DEDCT_AMT").alias("TOT_DEDCT_AMT"),
    )
)

df_Trns_cntrl = (
    df_Join
    .withColumn("svCount", F.col("COUNT"))
    .select(
        F.col("BHI_HOME_PLN_ID").alias("BHI_HOME_PLN_ID"),
        F.expr("PadString('STD_INP_CLAIM_LINES', ' ', 30)").alias("EXTR_NM"),
        F.expr("Trim(StartDate, '-', 'A')").alias("MIN_CLM_PROCESSED_DT"),
        F.expr("Trim(EndDate, '-', 'A')").alias("MAX_CLM_PRCS_DT"),
        F.expr("Trim(CurrDate, '-', 'A')").alias("SUBMSN_DT"),
        F.expr("Str('0', 10 - LEN(svCount)) : svCount").alias("RCRD_CT"),
        F.expr(
            "('+' : Str('0', 14 - Len(DecimalToString(TOT_SUBMT_AMT, \"suppress_zero\")))) : DecimalToString(TOT_SUBMT_AMT, \"suppress_zero\")"
        ).alias("TOT_SUBMT_AMT"),
        F.expr(
            "('+' : Str('0', 14 - Len(DecimalToString(TOT_NON_COV_AMT, \"suppress_zero\")))) : DecimalToString(TOT_NON_COV_AMT, \"suppress_zero\")"
        ).alias("TOT_NONCOV_AMT"),
        F.expr(
            "('+' : Str('0', 14 - Len(DecimalToString(TOT_ALW_AMT, \"suppress_zero\")))) : DecimalToString(TOT_ALW_AMT, \"suppress_zero\")"
        ).alias("TOT_ALW_AMT"),
        F.expr(
            "('+' : Str('0', 14 - Len(DecimalToString(TOT_PD_AMT, \"suppress_zero\")))) : DecimalToString(TOT_PD_AMT, \"suppress_zero\")"
        ).alias("TOT_PD_AMT"),
        F.expr(
            "('+' : Str('0', 14 - Len(DecimalToString(TOT_COB_TPL_AMT, \"suppress_zero\")))) : DecimalToString(TOT_COB_TPL_AMT, \"suppress_zero\")"
        ).alias("TOT_COB_TPL_AMT"),
        F.expr(
            "('+' : Str('0', 14 - Len(DecimalToString(TOT_COINS_AMT, \"suppress_zero\")))) : DecimalToString(TOT_COINS_AMT, \"suppress_zero\")"
        ).alias("TOT_COINS_AMT"),
        F.expr(
            "('+' : Str('0', 14 - Len(DecimalToString(TOT_COPAY_AMT, \"suppress_zero\")))) : DecimalToString(TOT_COPAY_AMT, \"suppress_zero\")"
        ).alias("TOT_COPAY_AMT"),
        F.expr(
            "('+' : Str('0', 14 - Len(DecimalToString(TOT_DEDCT_AMT, \"suppress_zero\")))) : DecimalToString(TOT_DEDCT_AMT, \"suppress_zero\")"
        ).alias("TOT_DEDCT_AMT"),
        F.expr(
            "If (Left('00000000000000', 1) <> '-') then ('+' : '00000000000000') else '000000000000000'"
        ).alias("TOT_FFS_EQVLNT_AMT"),
    )
)

write_files(
    df_Trns_cntrl,
    f"{adls_path_publish}/external/submission_control",
    delimiter=",",
    mode="append",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)