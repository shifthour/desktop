# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_2 11/04/09 07:53:48 Batch  15284_28434 PROMOTE bckcetl:31540 updt dsadm dsadm
# MAGIC ^1_2 10/19/09 16:31:27 Batch  15268_59491 PROMOTE bckcetl:31540 ids20 dsadm bls for sa
# MAGIC ^1_2 10/19/09 10:39:41 Batch  15268_38385 INIT bckcett:31540 testIDSnew dsadm bls for sa
# MAGIC ^1_2 10/05/09 18:22:38 Batch  15254_66186 PROMOTE bckcett:31540 testIDSnew u150906 4113-MBR360DM_Sharon_testIDSnew             Maddy
# MAGIC ^1_2 10/05/09 18:12:10 Batch  15254_65619 INIT bckcett:31540 devlIDSnew u150906 4113-Mbr360DM_Sharon_devlIDSnew             Maddy
# MAGIC ^1_1 09/25/09 17:45:02 Batch  15244_63926 INIT bckcett:31540 devlIDSnew u150906 4113-Mbr360_Sharon_devlIDSnew          Maddy
# MAGIC ^1_1 08/28/08 11:14:23 Batch  14851_40472 PROMOTE bckcetl ids20 dsadm bls for sa
# MAGIC ^1_1 08/28/08 11:06:31 Batch  14851_39994 INIT bckcett testIDS dsadm BLS FOR SA
# MAGIC ^1_1 08/22/08 07:08:02 Batch  14845_25699 PROMOTE bckcett testIDS u03651 steph for sharon 3057
# MAGIC ^1_1 08/22/08 07:06:03 Batch  14845_25567 INIT bckcett devlIDSnew u03651 steffy
# MAGIC ^1_1 01/29/08 14:29:13 Batch  14639_52158 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 01/09/08 13:32:13 Batch  14619_48738 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 11/24/07 09:35:50 Batch  14573_34558 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_8 07/18/07 14:21:56 Batch  14444_51727 PROMOTE bckcetl ids20 dsadm bls for hs
# MAGIC ^1_8 07/18/07 14:18:50 Batch  14444_51533 INIT bckcett testIDSnew dsadm bls for hs
# MAGIC ^1_2 07/11/07 15:03:17 Batch  14437_54205 PROMOTE bckcett testIDSnew u11141 Hugh Sisson
# MAGIC ^1_2 07/11/07 15:00:27 Batch  14437_54033 INIT bckcett devlIDS30 u11141 Hugh Sisson
# MAGIC ^1_1 07/11/07 10:52:00 Batch  14437_39130 INIT bckcett devlIDS30 u11141 Hugh Sisson
# MAGIC ^1_2 04/18/07 14:50:09 Batch  14353_53412 INIT bckcetl ids20 dcg01 sa
# MAGIC ^1_3 03/28/07 06:21:15 Batch  14332_22879 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 03/07/07 16:30:43 Batch  14311_59451 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_7 12/18/06 15:24:00 Batch  14232_55459 PROMOTE bckcetl ids20 dsadm Keith for Ralph install 12/18/2006
# MAGIC ^1_7 12/18/06 14:39:27 Batch  14232_52797 INIT bckcett testIDS30 dsadm Keith for Ralph CDHP Install 12182006
# MAGIC ^1_1 11/17/06 16:01:03 Batch  14201_57667 PROMOTE bckcett testIDS30 u11141 CDHP system testing - Hugh Sisson
# MAGIC ^1_1 11/17/06 15:57:58 Batch  14201_57500 INIT bckcett devlIDS30 u11141 CDHP system testing - Hugh Sisson
# MAGIC ^1_5 08/10/06 07:54:51 Batch  14102_28496 INIT bckcett devlIDS30 u08717 Brent
# MAGIC ^1_4 07/21/06 15:13:25 Batch  14082_54812 INIT bckcett devlIDS30 u11141 Hugh Sisson
# MAGIC ^1_3 07/18/06 15:24:59 Batch  14079_55507 PROMOTE bckcett devlIDS30 u11141 Hugh Sisson
# MAGIC ^1_5 07/14/06 14:47:09 Batch  14075_53239 PROMOTE bckcetl ids20 dsadm Jim Hart
# MAGIC ^1_5 07/14/06 14:42:55 Batch  14075_52993 INIT bckcett testIDS30 dsadm Jim Hart
# MAGIC ^1_2 07/14/06 12:15:25 Batch  14075_44129 PROMOTE bckcett testIDS30 u11141 Hugh Sisson
# MAGIC ^1_2 07/14/06 12:13:43 Batch  14075_44082 INIT bckcett devlIDS30 u11141 Hugh Sisson
# MAGIC ^1_1 07/07/06 14:45:43 Batch  14068_53151 PROMOTE bckcett devlIDS30 u11141 Hugh Sisson
# MAGIC ^1_4 06/05/06 10:28:06 Batch  14036_37701 PROMOTE bckcetl ids20 dsadm J. Mahaffey for O. Nielsen
# MAGIC ^1_4 06/05/06 10:23:52 Batch  14036_37451 INIT bckcett testIDS30 dsadm J. Mahaffey for O. Nielsen
# MAGIC ^1_7 05/19/06 09:50:58 Batch  14019_35469 PROMOTE bckcett testIDS30 u10913 Ollie moving KCREE to test
# MAGIC ^1_7 05/19/06 09:43:19 Batch  14019_35008 INIT bckcett devlIDS30 u10913 Ollie move KCREE from devl to test
# MAGIC ^1_6 05/01/06 10:02:53 Batch  14001_36182 INIT bckcett devlIDS30 u11141 Hugh Sisson
# MAGIC ^1_5 04/28/06 14:03:05 Batch  13998_50597 INIT bckcett devlIDS30 u11141 Hugh Sisson
# MAGIC ^1_3 04/07/06 12:18:30 Batch  13977_44314 INIT bckcett devlIDS30 u08717 Brent
# MAGIC 
# MAGIC 
# MAGIC  Copyright 2007 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC Called by:
# MAGIC                     IdsClmMartExtrLoadSeq
# MAGIC 
# MAGIC Processing:
# MAGIC                     Extract and load claim line data based on driver table W_WEBDM_ETL_DRVR
# MAGIC 
# MAGIC Control Job Rerun Information: 
# MAGIC              Previous Run Successful:    Verify contents of driver table - W_WEBDM_ETL_DRVR
# MAGIC              Previous Run Aborted:         Restart
# MAGIC 
# MAGIC Modifications:                        
# MAGIC                                               Project/                                                                                                                                                                              Development              Code                     Date
# MAGIC Developer           Date              Altiris #           Change Description                                                                                                                                  Project                       Reviewer               Reviewed
# MAGIC ----------------------     -------------------   -------------------   -------------------------------------------------------------------------------------------------------------------------------------------------------------    -------------------------     -------------------             ---------------------
# MAGIC Steph Goddard   03/22/2005                         Originally Programmed
# MAGIC Ralph Tucker     04/19/2005                          Added Proc_Cd_Desc field; populated by hit of hf_clmmart_proc_cd_xref hash file.
# MAGIC Steph Goddard   04/29/2005                         Changed room type code to room price method code - name and source changed, 
# MAGIC                                                                            not position in output record
# MAGIC Steph Goddard   05/06/2005                         Check from/to dates for UNK and NA
# MAGIC BJ Luce              08/29/2005                         Change SQL parameters to ClmMart for job sequencer
# MAGIC Brent Leland       12/29/2005                         Added extract and load for balancing
# MAGIC Brent Leland       04/03/2006                         Changed to use evironment parameters
# MAGIC                                                                        Changed name of driver table
# MAGIC Brent Leland       04/06/2006                         Added run cycle parameter for new table field LAST_UPDT_RUN_CYC_NO
# MAGIC Hugh Sisson       04/24/2006                         Added 23 new columns to support Care Management Implementation (Project #1762)
# MAGIC                                                                        Included Procedure Code lookup into the SQL and dropped the hf_clmmart_proc_cd_xref hash file
# MAGIC Hugh Sisson       07/12/2006                         Made diagnosis codes extraction into a separate SQL to ensure that claim lines that do not 
# MAGIC                                                                           have a primary diagnosis code (e.g. drug and dental claims) will be pulled into the Mart.              
# MAGIC Brent Leland       07/15/2006                         Changed Claim Diag SQL
# MAGIC Ralph Tucker     10/25/2006                         Added  new PCA fields to the end of the table.
# MAGIC Hugh Sisson       05/22/2007  CDHP Drug    Removed reference to PCA in CLM_LN_PCA_LOB_CD, CLM_LN_PCA_LOB_NM, 
# MAGIC                                                                           CLM_LN_PCA_PRCS_CD, CLM_LN_PCA_PRCS_NM, CLM_LN_PCA_CNSD_AMT, 
# MAGIC                                                                           CLM_LN_PCA_PD_AMT, and CLM_LN_PCA_SUB_PD_AMT
# MAGIC                                                                        Added HASH.CLEAR call
# MAGIC 
# MAGIC Parik                  2008-08-01  3057(web claim)   Added the new Claim Line Remit lookup logic                                                                                       devlIDSnew             Steph Goddard        08/11/2008
# MAGIC 
# MAGIC SAndrew            2008-08-15  3057(web claim)   Adding more detailed description 
# MAGIC \(9)                                                          Added 4 new fields to end of DM table CLM_LN_REMIT_PATN_RESP_AMT,  
# MAGIC                                                                           CLM_LN_REMIT_PROV_WRT_OFF_AMT,  CLM_LN_REMIT_MBR_OTHR_LIAB_AMT, CLM_LN_REMIT_NO_RESP_AMT.                                                                                                                                                        
# MAGIC                                                                           Incorporated lookup to new ids table #$IDSOwner#.CLM_LN_REMIT REMIT.    added hash file hf_cmcl_clmln_remit
# MAGIC 
# MAGIC Terri O'Bryan    2008-09-01  4113 -Member360   Added 2 new fields to end of DM table CLM_DM_CLM_LN:                                                               devlIDSnew              Steph Goddard        09/22/2009
# MAGIC                                                                             CLM_LN_FINL_DISP_CD, CLM_LN_FINL_DISP_NM

# MAGIC Use SKs to perform all mappings with any Lookup MappingType.
# MAGIC Extract list of claim lines to be used in balancing
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


RunCycle = get_widget_value("RunCycle","901")
IDSOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")
ClmMartOwner = get_widget_value("ClmMartOwner","")
clmmart_secret_name = get_widget_value("clmmart_secret_name","")

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
jdbc_url_clmmart, jdbc_props_clmmart = get_db_config(clmmart_secret_name)

# ------------------------------------------------------------------------------
# Read from IDSClm_Ln (DB2Connector with multiple output links)
# ------------------------------------------------------------------------------

extract_query = f"""SELECT DISTINCT
               LN.CLM_LN_SK,
               LN.CLM_SK,
               LN.SRC_SYS_CD_SK,
               LN.CLM_ID,
               LN.CLM_LN_SEQ_NO,
               LN.CLM_LN_ROOM_TYP_CD_SK,
               LN.CLM_LN_TOS_CD_SK,
               LN.CAP_LN_IN,
               LN.SVC_END_DT_SK,
               LN.SVC_STRT_DT_SK,
               LN.CHRG_AMT,
               LN.PAYBL_TO_PROV_AMT,
               LN.SVC_ID,
               LN.UNIT_CT,
               PROC.PROC_CD,
               PROC.PROC_CD_DESC,
               RVNU.RVNU_CD,
               RVNU.RVNU_CD_DESC,
               LN.CLM_LN_ROOM_PRICE_METH_CD_SK,
               LN.SVC_PROV_SK,
               LN.CLM_LN_DSALW_EXCD_SK,
               LN.CLM_LN_EOB_EXCD_SK,
               LN.CLM_LN_POS_CD_SK,
               LN.CLM_LN_PREAUTH_CD_SK,
               LN.CLM_LN_RFRL_CD_SK,
               LN.ALW_AMT,
               LN.COINS_AMT,
               LN.CNSD_CHRG_AMT,
               LN.COPAY_AMT,
               LN.DEDCT_AMT,
               LN.DSALW_AMT,
               LN.PAYBL_AMT,
               LN.ALW_PRICE_UNIT_CT,
               PROV.PROV_ID,
               LN.CLM_LN_FINL_DISP_CD_SK
FROM {IDSOwner}.CLM C,
     {IDSOwner}.CLM_LN LN,
     {IDSOwner}.W_WEBDM_ETL_DRVR EXTR,
     {IDSOwner}.RVNU_CD RVNU,
     {IDSOwner}.PROC_CD PROC,
     {IDSOwner}.PROV PROV
WHERE EXTR.SRC_SYS_CD_SK = C.SRC_SYS_CD_SK
  AND EXTR.CLM_ID = C.CLM_ID
  AND C.CLM_SK = LN.CLM_SK
  AND LN.CLM_LN_RVNU_CD_SK = RVNU.RVNU_CD_SK
  AND LN.PROC_CD_SK = PROC.PROC_CD_SK
  AND PROV.PROV_SK = LN.SVC_PROV_SK
"""
df_Extract = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query)
    .load()
)

lnkExcdXref_query = f"""SELECT EXCD_SK,EXCD_ID,EXCD_SH_TX FROM {IDSOwner}.EXCD"""
df_lnkExcdXref = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", lnkExcdXref_query)
    .load()
)

diagCdExtract_query = f"""SELECT
     LN.SRC_SYS_CD_SK,
     LN.CLM_ID,
     LN.CLM_LN_SEQ_NO,
     DIAG.DIAG_CD,
     DIAG.DIAG_CD_DESC,
     CLD.CLM_LN_DIAG_ORDNL_CD_SK
FROM
     {IDSOwner}.CLM C,
     {IDSOwner}.CLM_LN LN,
     {IDSOwner}.W_WEBDM_ETL_DRVR EXTR,
     {IDSOwner}.DIAG_CD DIAG,
     {IDSOwner}.CLM_LN_DIAG CLD
WHERE
     EXTR.SRC_SYS_CD_SK = C.SRC_SYS_CD_SK
  AND EXTR.CLM_ID = C.CLM_ID
  AND C.CLM_SK = LN.CLM_SK
  AND LN.CLM_LN_SK = CLD.CLM_LN_SK
  AND CLD.DIAG_CD_SK = DIAG.DIAG_CD_SK
"""
df_DiagCdExtract = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", diagCdExtract_query)
    .load()
)

codeMapping_query = f"""SELECT CD_MPPNG_SK
   FROM {IDSOwner}.CD_MPPNG CM
WHERE CM.SRC_DOMAIN_NM = 'DIAGNOSIS ORDINAL'
  AND CM.SRC_CD = '1'
"""
df_CodeMapping = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", codeMapping_query)
    .load()
)

clmLnPcaExtract_query = f"""SELECT PCA.CLM_LN_SK as CLM_LN_SK,
PCA.DSALW_EXCD_SK as DSALW_EXCD_SK,
PCA.CLM_LN_PCA_LOB_CD_SK as CLM_LN_PCA_LOB_CD_SK,
PCA.CLM_LN_PCA_PRCS_CD_SK as CLM_LN_PCA_PRCS_CD_SK,
PCA.CNSD_AMT as CNSD_AMT,
PCA.DSALW_AMT as DSALW_AMT,
PCA.NONCNSD_AMT as NONCNSD_AMT,
PCA.PROV_PD_AMT as PROV_PD_AMT,
PCA.SUB_PD_AMT as SUB_PD_AMT,
PCA.PD_AMT as PD_AMT,
EXCD.EXCD_SH_TX as EXCD_SH_TX,
EXCD.EXCD_ID as EXCD_ID
FROM {IDSOwner}.CLM_LN LN,
     {IDSOwner}.CLM_LN_PCA PCA,
     {IDSOwner}.W_WEBDM_ETL_DRVR EXTR,
     {IDSOwner}.EXCD EXCD
WHERE EXTR.SRC_SYS_CD_SK = LN.SRC_SYS_CD_SK
  AND EXTR.CLM_ID = LN.CLM_ID
  AND LN.CLM_LN_SK = PCA.CLM_LN_SK
  AND PCA.DSALW_EXCD_SK = EXCD.EXCD_SK
"""
df_ClmLnPcaExtract = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", clmLnPcaExtract_query)
    .load()
)

clmLnPcaLobCdExtract_query = f"""SELECT C.CLM_SK as CLM_SK,
MPPNG2.TRGT_CD as CLM_LN_PCA_LOB_CD,
MPPNG2.TRGT_CD_NM as CLM_LN_PCA_LOB_NM
FROM {IDSOwner}.CLM C,
     {IDSOwner}.CLM_CHK CLMCK,
     {IDSOwner}.W_WEBDM_ETL_DRVR EXTR,
     {IDSOwner}.CD_MPPNG MPPNG,
     {IDSOwner}.CD_MPPNG MPPNG2
WHERE EXTR.SRC_SYS_CD_SK = C.SRC_SYS_CD_SK
  AND EXTR.CLM_ID = C.CLM_ID
  AND MPPNG.CD_MPPNG_SK = C.PCA_TYP_CD_SK
  AND MPPNG.TRGT_CD in ('EMPWBNF','RUNOUT','PCA')
  AND C.CLM_SK = CLMCK.CLM_SK
  AND MPPNG2.CD_MPPNG_SK = CLMCK.CLM_CHK_LOB_CD_SK
"""
df_ClmLnPcaLobCdExtract = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", clmLnPcaLobCdExtract_query)
    .load()
)

pcaPrcsCdExtract_query = f"""SELECT CLM_SK,
MPPNG2.TRGT_CD,
MPPNG2.TRGT_CD_NM
FROM {IDSOwner}.CLM C,
     {IDSOwner}.W_WEBDM_ETL_DRVR EXTR,
     {IDSOwner}.CD_MPPNG MPPNG,
     {IDSOwner}.CD_MPPNG MPPNG2
WHERE EXTR.CLM_ID = C.CLM_ID
  AND MPPNG.CD_MPPNG_SK = C.PCA_TYP_CD_SK
  AND MPPNG.TRGT_CD in ('EMPWBNF','RUNOUT','PCA')
  AND MPPNG2.TRGT_DOMAIN_NM = 'PERSONAL CARE ACCOUNT PROCESSING'
  AND MPPNG2.SRC_CD = 'H'
"""
df_PcaPrcsCdExtract = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", pcaPrcsCdExtract_query)
    .load()
)

pcaAmtExtract_query = f"""SELECT C.CLM_SK,
               CLN.CHRG_AMT,
               CLN.PAYBL_AMT
FROM {IDSOwner}.CLM C,
     {IDSOwner}.CLM_LN CLN,
     {IDSOwner}.W_WEBDM_ETL_DRVR EXTR,
     {IDSOwner}.CD_MPPNG MPPNG
WHERE EXTR.CLM_ID = C.CLM_ID
  AND C.CLM_SK = CLN.CLM_SK
  AND MPPNG.CD_MPPNG_SK = C.PCA_TYP_CD_SK
  AND MPPNG.TRGT_CD in ('EMPWBNF','RUNOUT','PCA')
"""
df_PcaAmtExtract = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", pcaAmtExtract_query)
    .load()
)

clmLnRemit_query = f"""SELECT REMIT.CLM_LN_SK as CLM_LN_SK,
REMIT.REMIT_PATN_RESP_AMT as REMIT_PATN_RESP_AMT,
REMIT.REMIT_PROV_WRT_OFF_AMT as REMIT_PROV_WRT_OFF_AMT,
REMIT.REMIT_MBR_OTHR_LIAB_AMT as REMIT_MBR_OTHR_LIAB_AMT,
REMIT.REMIT_NO_RESP_AMT as REMIT_NO_RESP_AMT
FROM {IDSOwner}.CLM_LN_REMIT REMIT,
     {IDSOwner}.W_WEBDM_ETL_DRVR EXTR
WHERE EXTR.SRC_SYS_CD_SK = REMIT.SRC_SYS_CD_SK
  AND EXTR.CLM_ID = REMIT.CLM_ID
"""
df_ClmLnRemit = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", clmLnRemit_query)
    .load()
)

# ------------------------------------------------------------------------------
# CHashedFileStage scenario A: "AnyStage → CHashedFileStage → AnyStage"
# Deduplicate on primary key columns
# ------------------------------------------------------------------------------

df_hf_clmmart_excd_xref = dedup_sort(df_lnkExcdXref, ["EXCD_SK"], [("EXCD_SK","A")])

df_hf_cmcl_clmln_lob = dedup_sort(df_ClmLnPcaLobCdExtract, ["CLM_SK"], [("CLM_SK","A")])

df_hf_cmcl_pca_prcs_cd = dedup_sort(df_PcaPrcsCdExtract, ["CLM_SK"], [("CLM_SK","A")])

df_hf_cmcl_pca_cnsd_amt = dedup_sort(df_PcaAmtExtract, ["CLM_SK"], [("CLM_SK","A")])

df_hf_cmcl_clmln_remit = dedup_sort(df_ClmLnRemit, ["CLM_LN_SK"], [("CLM_LN_SK","A")])

# Trans4 output to "hf_clmmart_clmln_pca" will also be scenario A deduplicate:
# We will do that after we build df_ClmLnPca in Trans4.

df_hf_cmcl_diag_ord = dedup_sort(df_CodeMapping, ["CD_MPPNG_SK"], [("CD_MPPNG_SK","A")])

# Trans3 output to "hf_cmcl_diag_cd" also scenario A deduplicate afterward

# ------------------------------------------------------------------------------
# CHashedFileStage scenario C: read hashed files as source from parquet
# ------------------------------------------------------------------------------

df_hf_etrnl_cd_mppng = spark.read.parquet(f"{adls_path}/hf_etrnl_cd_mppng.parquet")

# We create separate DataFrames for each "OutputPin" because each has same columns, 
# but in DataStage they are distinct references:

df_refClmLnRoomPricMeth = df_hf_etrnl_cd_mppng.select(
    F.col("CD_MPPNG_SK"),
    F.col("TRGT_CD"),
    F.col("TRGT_CD_NM"),
    F.col("SRC_CD"),
    F.col("SRC_CD_NM"),
)

df_refSrcSys = df_hf_etrnl_cd_mppng.select(
    F.col("CD_MPPNG_SK"),
    F.col("TRGT_CD"),
    F.col("TRGT_CD_NM"),
    F.col("SRC_CD"),
    F.col("SRC_CD_NM"),
)

df_refClmLnTos = df_hf_etrnl_cd_mppng.select(
    F.col("CD_MPPNG_SK"),
    F.col("TRGT_CD"),
    F.col("TRGT_CD_NM"),
    F.col("SRC_CD"),
    F.col("SRC_CD_NM"),
)

df_refClmLnPos = df_hf_etrnl_cd_mppng.select(
    F.col("CD_MPPNG_SK"),
    F.col("TRGT_CD"),
    F.col("TRGT_CD_NM"),
    F.col("SRC_CD"),
    F.col("SRC_CD_NM"),
)

df_refClmLnPreauth = df_hf_etrnl_cd_mppng.select(
    F.col("CD_MPPNG_SK"),
    F.col("TRGT_CD"),
    F.col("TRGT_CD_NM"),
    F.col("SRC_CD"),
    F.col("SRC_CD_NM"),
)

df_refClmLnReferral = df_hf_etrnl_cd_mppng.select(
    F.col("CD_MPPNG_SK"),
    F.col("TRGT_CD"),
    F.col("TRGT_CD_NM"),
    F.col("SRC_CD"),
    F.col("SRC_CD_NM"),
)

df_refClmLnRoomTyp = df_hf_etrnl_cd_mppng.select(
    F.col("CD_MPPNG_SK"),
    F.col("TRGT_CD"),
    F.col("TRGT_CD_NM"),
    F.col("SRC_CD"),
    F.col("SRC_CD_NM"),
)

df_refClmLnFinlDisp = df_hf_etrnl_cd_mppng.select(
    F.col("CD_MPPNG_SK"),
    F.col("TRGT_CD"),
    F.col("TRGT_CD_NM"),
    F.col("SRC_CD"),
    F.col("SRC_CD_NM"),
)

# ------------------------------------------------------------------------------
# Another CHashedFileStage "cd_mppng" also scenario C: read from parquet
# ------------------------------------------------------------------------------

df_cd_mppng = spark.read.parquet(f"{adls_path}/cd_mppng.parquet")

df_SrcSysCd = df_cd_mppng.select(
    F.col("CD_MPPNG_SK"),
    F.col("TRGT_CD"),
    F.col("TRGT_CD_NM"),
    F.col("SRC_CD"),
    F.col("SRC_CD_NM"),
)

# ------------------------------------------------------------------------------
# hf_clmmart_excd_xref (already deduped -> df_hf_clmmart_excd_xref)
# We'll create separate df for refDsalwExcd and refEobExcd if needed
# but they have the same columns, so we can reuse the same df in Trans1 with alias
# ------------------------------------------------------------------------------

# ------------------------------------------------------------------------------
# hf_cmcl_clmln_lob (df_hf_cmcl_clmln_lob)
# ------------------------------------------------------------------------------
# hf_cmcl_pca_prcs_cd (df_hf_cmcl_pca_prcs_cd)
# ------------------------------------------------------------------------------
# hf_cmcl_pca_cnsd_amt (df_hf_cmcl_pca_cnsd_amt)
# ------------------------------------------------------------------------------
# hf_cmcl_clmln_remit (df_hf_cmcl_clmln_remit)
# ------------------------------------------------------------------------------

# ------------------------------------------------------------------------------
# Trans4: merges df_ClmLnPcaExtract, left join df_hf_cmcl_clmln_lob, df_hf_cmcl_pca_prcs_cd
# ------------------------------------------------------------------------------

df_Trans4_step = df_ClmLnPcaExtract.alias("ClmLnPcaExtract") \
    .join(
        df_hf_cmcl_clmln_lob.alias("refClmLnLob"),
        F.col("ClmLnPcaExtract.CLM_LN_PCA_LOB_CD_SK") == F.col("refClmLnLob.CLM_SK"),
        "left"
    ) \
    .join(
        df_hf_cmcl_pca_prcs_cd.alias("refClmLnPrcs"),
        F.col("ClmLnPcaExtract.CLM_LN_PCA_PRCS_CD_SK") == F.col("refClmLnPrcs.CLM_SK"),
        "left"
    )

df_ClmLnPca = df_Trans4_step.select(
    F.when(
        (F.trim(F.col("ClmLnPcaExtract.CLM_LN_SK"))=="NA") | (F.trim(F.col("ClmLnPcaExtract.CLM_LN_SK"))=="UNK"),
        F.lit(None)
    ).otherwise(F.col("ClmLnPcaExtract.CLM_LN_SK")).alias("CLM_LN_SK"),
    F.when(F.isnull(F.col("refClmLnLob.TRGT_CD")),F.lit(" ")).otherwise(F.col("refClmLnLob.TRGT_CD")).alias("CLM_LN_PCA_LOB_CD"),
    F.when(
        (F.isnull(F.col("refClmLnLob.TRGT_CD_NM"))) | (F.length(F.trim(F.col("refClmLnLob.TRGT_CD_NM")))==0),
        F.lit(" ")
    ).otherwise(F.col("refClmLnLob.TRGT_CD_NM")).alias("CLM_LN_PCA_LOB_NM"),
    F.when(F.isnull(F.col("refClmLnPrcs.TRGT_CD")),F.lit(" ")).otherwise(F.col("refClmLnPrcs.TRGT_CD")).alias("CLM_LN_PCA_PRCS_CD"),
    F.when(
        (F.isnull(F.col("refClmLnPrcs.TRGT_CD_NM"))) | (F.length(F.trim(F.col("refClmLnPrcs.TRGT_CD_NM")))==0),
        F.lit(" ")
    ).otherwise(F.col("refClmLnPrcs.TRGT_CD_NM")).alias("CLM_LN_PCA_PRCS_NM"),
    F.col("ClmLnPcaExtract.CNSD_AMT").alias("CNSD_AMT"),
    F.col("ClmLnPcaExtract.DSALW_AMT").alias("DSALW_AMT"),
    F.col("ClmLnPcaExtract.NONCNSD_AMT").alias("NONCNSD_AMT"),
    F.col("ClmLnPcaExtract.PD_AMT").alias("PD_AMT"),
    F.col("ClmLnPcaExtract.PROV_PD_AMT").alias("PROV_PD_AMT"),
    F.col("ClmLnPcaExtract.SUB_PD_AMT").alias("SUB_PD_AMT"),
    F.col("ClmLnPcaExtract.EXCD_ID").alias("EXCD_ID"),
    F.col("ClmLnPcaExtract.EXCD_SH_TX").alias("EXCD_SH_TX")
)

df_hf_clmmart_clmln_pca = dedup_sort(
    df_ClmLnPca,
    ["CLM_LN_SK"],
    [("CLM_LN_SK","A")]
)

# ------------------------------------------------------------------------------
# Trans3: merges df_DiagCdExtract (primary) with df_hf_cmcl_diag_ord
# outputs "DiagCd" to next hashed file
# ------------------------------------------------------------------------------
df_Trans3_step = df_DiagCdExtract.alias("DiagCdExtract").join(
    df_hf_cmcl_diag_ord.alias("CdMppng"),
    F.col("DiagCdExtract.CLM_LN_DIAG_ORDNL_CD_SK") == F.col("CdMppng.CD_MPPNG_SK"),
    "left"
)

df_DiagCd = df_Trans3_step.filter(~F.isnull(F.col("CdMppng.CD_MPPNG_SK"))).select(
    F.col("DiagCdExtract.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("DiagCdExtract.CLM_ID").alias("CLM_ID"),
    F.col("DiagCdExtract.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("DiagCdExtract.DIAG_CD").alias("DIAG_CD"),
    F.col("DiagCdExtract.DIAG_CD_DESC").alias("DIAG_CD_DESC"),
    F.col("DiagCdExtract.CLM_LN_DIAG_ORDNL_CD_SK").alias("CLM_LN_DIAG_ORDNL_CD_SK")
)

df_hf_cmcl_diag_cd = dedup_sort(
    df_DiagCd,
    ["SRC_SYS_CD_SK","CLM_ID","CLM_LN_SEQ_NO"],
    [("SRC_SYS_CD_SK","A"),("CLM_ID","A"),("CLM_LN_SEQ_NO","A")]
)

# ------------------------------------------------------------------------------
# Trans1: big transform merging df_Extract with many lookups
# ------------------------------------------------------------------------------
df_Trans1_stagevar = df_Extract.withColumn(
    "startdate",
    F.when(
        (F.trim(F.col("SVC_STRT_DT_SK"))=="NA")|(F.trim(F.col("SVC_STRT_DT_SK"))=="UNK"),
        F.lit(None)
    ).otherwise(F.col("SVC_STRT_DT_SK"))
).withColumn(
    "enddate",
    F.when(
        (F.trim(F.col("SVC_END_DT_SK"))=="NA")|(F.trim(F.col("SVC_END_DT_SK"))=="UNK"),
        F.lit(None)
    ).otherwise(F.col("SVC_END_DT_SK"))
)

tmp_Trans1 = (
    df_Trans1_stagevar.alias("Extract")
    .join(df_refClmLnRoomPricMeth.alias("refClmLnRoomPricMeth"),
          F.col("Extract.CLM_LN_ROOM_PRICE_METH_CD_SK")==F.col("refClmLnRoomPricMeth.CD_MPPNG_SK"),
          "left")
    .join(df_refSrcSys.alias("refSrcSys"),
          F.col("Extract.SRC_SYS_CD_SK")==F.col("refSrcSys.CD_MPPNG_SK"),
          "left")
    .join(df_refClmLnTos.alias("refClmLnTos"),
          F.col("Extract.CLM_LN_TOS_CD_SK")==F.col("refClmLnTos.CD_MPPNG_SK"),
          "left")
    .join(df_refClmLnPos.alias("refClmLnPos"),
          F.col("Extract.CLM_LN_POS_CD_SK")==F.col("refClmLnPos.CD_MPPNG_SK"),
          "left")
    .join(df_refClmLnPreauth.alias("refClmLnPreauth"),
          F.col("Extract.CLM_LN_PREAUTH_CD_SK")==F.col("refClmLnPreauth.CD_MPPNG_SK"),
          "left")
    .join(df_refClmLnReferral.alias("refClmLnReferral"),
          F.col("Extract.CLM_LN_RFRL_CD_SK")==F.col("refClmLnReferral.CD_MPPNG_SK"),
          "left")
    .join(df_refClmLnRoomTyp.alias("refClmLnRoomTyp"),
          F.col("Extract.CLM_LN_ROOM_TYP_CD_SK")==F.col("refClmLnRoomTyp.CD_MPPNG_SK"),
          "left")
    .join(df_hf_clmmart_excd_xref.alias("refDsalwExcd"),
          F.col("Extract.CLM_LN_DSALW_EXCD_SK")==F.col("refDsalwExcd.EXCD_SK"),
          "left")
    .join(df_hf_clmmart_excd_xref.alias("refEobExcd"),
          F.col("Extract.CLM_LN_EOB_EXCD_SK")==F.col("refEobExcd.EXCD_SK"),
          "left")
    .join(df_hf_cmcl_diag_cd.alias("refDiagCd"),
          [
              F.col("Extract.SRC_SYS_CD_SK")==F.col("refDiagCd.SRC_SYS_CD_SK"),
              F.col("Extract.CLM_ID")==F.col("refDiagCd.CLM_ID"),
              F.col("Extract.CLM_LN_SEQ_NO")==F.col("refDiagCd.CLM_LN_SEQ_NO")
          ],
          "left")
    .join(df_hf_clmmart_clmln_pca.alias("refClmLnPca"),
          F.col("Extract.CLM_LN_SK")==F.col("refClmLnPca.CLM_LN_SK"),
          "left")
    .join(df_hf_cmcl_clmln_lob.alias("refClmTypCd"),
          F.col("Extract.CLM_SK")==F.col("refClmTypCd.CLM_SK"),
          "left")
    .join(df_hf_cmcl_pca_prcs_cd.alias("refPcaPrcs"),
          F.col("Extract.CLM_SK")==F.col("refPcaPrcs.CLM_SK"),
          "left")
    .join(df_hf_cmcl_pca_cnsd_amt.alias("refPcaAmt"),
          F.col("Extract.CLM_SK")==F.col("refPcaAmt.CLM_SK"),
          "left")
    .join(df_hf_cmcl_clmln_remit.alias("ClmLnRemitCd"),
          F.col("Extract.CLM_LN_SK")==F.col("ClmLnRemitCd.CLM_LN_SK"),
          "left")
    .join(df_refClmLnFinlDisp.alias("refClmLnFinlDisp"),
          F.col("Extract.CLM_LN_FINL_DISP_CD_SK")==F.col("refClmLnFinlDisp.CD_MPPNG_SK"),
          "left")
)

df_OutFile = tmp_Trans1.select(
    F.when(F.isnull(F.col("refSrcSys.TRGT_CD")),F.lit(" ")).otherwise(F.col("refSrcSys.TRGT_CD")).alias("SRC_SYS_CD"),
    F.col("Extract.CLM_ID").alias("CLM_ID"),
    F.col("Extract.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("Extract.RVNU_CD").alias("CLM_LN_RVNU_CD"),
    F.when(F.isnull(F.col("refClmLnRoomPricMeth.TRGT_CD")),F.lit(" ")).otherwise(F.col("refClmLnRoomPricMeth.TRGT_CD")).alias("CLM_LN_ROOM_PRICE_METH_CD"),
    F.when(F.isnull(F.col("refClmLnTos.TRGT_CD")),F.lit(" ")).otherwise(F.col("refClmLnTos.TRGT_CD")).alias("CLM_LN_TOS_CD"),
    F.col("Extract.PROC_CD").alias("PROC_CD"),
    F.col("Extract.CAP_LN_IN").alias("CLM_LN_CAP_LN_IN"),
    F.col("startdate").alias("CLM_LN_SVC_STRT_DT"),
    F.col("enddate").alias("CLM_LN_SVC_END_DT"),
    F.col("Extract.CHRG_AMT").alias("CLM_LN_CHRG_AMT"),
    F.col("Extract.PAYBL_TO_PROV_AMT").alias("CLM_LN_PAYBL_TO_PROV_AMT"),
    F.col("Extract.UNIT_CT").alias("CLM_LN_UNIT_CT"),
    F.col("Extract.SVC_ID").alias("CLM_LN_SVC_ID"),
    F.col("Extract.PROC_CD_DESC").alias("PROC_CD_DESC"),
    F.when(
        (F.isnull(F.col("refClmLnPos.TRGT_CD")))|(F.length(F.trim(F.col("refClmLnPos.TRGT_CD")))==0),
        F.lit(" ")
    ).otherwise(F.col("refClmLnPos.TRGT_CD")).alias("CLM_LN_POS_CD"),
    F.when(
        (F.isnull(F.col("refClmLnPos.TRGT_CD_NM")))|(F.length(F.trim(F.col("refClmLnPos.TRGT_CD_NM")))==0),
        F.lit(" ")
    ).otherwise(F.col("refClmLnPos.TRGT_CD_NM")).alias("CLM_LN_POS_NM"),
    F.when(
        (F.isnull(F.col("refClmLnPreauth.TRGT_CD")))|(F.length(F.trim(F.col("refClmLnPreauth.TRGT_CD")))==0),
        F.lit(" ")
    ).otherwise(F.col("refClmLnPreauth.TRGT_CD")).alias("CLM_LN_PREAUTH_CD"),
    F.when(
        (F.isnull(F.col("refClmLnPreauth.TRGT_CD_NM")))|(F.length(F.trim(F.col("refClmLnPreauth.TRGT_CD_NM")))==0),
        F.lit(" ")
    ).otherwise(F.col("refClmLnPreauth.TRGT_CD_NM")).alias("CLM_LN_PREAUTH_NM"),
    F.when(
        (F.isnull(F.col("refClmLnReferral.TRGT_CD")))|(F.length(F.trim(F.col("refClmLnReferral.TRGT_CD")))==0),
        F.lit(" ")
    ).otherwise(F.col("refClmLnReferral.TRGT_CD")).alias("CLM_LN_RFRL_CD"),
    F.when(
        (F.isnull(F.col("refClmLnReferral.TRGT_CD_NM")))|(F.length(F.trim(F.col("refClmLnReferral.TRGT_CD_NM")))==0),
        F.lit(" ")
    ).otherwise(F.col("refClmLnReferral.TRGT_CD_NM")).alias("CLM_LN_RFRL_NM"),
    F.col("Extract.RVNU_CD_DESC").alias("CLM_LN_RVNU_NM"),
    F.when(
        (F.isnull(F.col("refClmLnRoomTyp.TRGT_CD")))|(F.length(F.trim(F.col("refClmLnRoomTyp.TRGT_CD")))==0),
        F.lit(" ")
    ).otherwise(F.col("refClmLnRoomTyp.TRGT_CD")).alias("CLM_LN_ROOM_TYP_CD"),
    F.when(
        (F.isnull(F.col("refClmLnRoomTyp.TRGT_CD_NM")))|(F.length(F.trim(F.col("refClmLnRoomTyp.TRGT_CD_NM")))==0),
        F.lit(" ")
    ).otherwise(F.col("refClmLnRoomTyp.TRGT_CD_NM")).alias("CLM_LN_ROOM_TYP_NM"),
    F.col("Extract.ALW_AMT").alias("CLM_LN_ALW_AMT"),
    F.col("Extract.COINS_AMT").alias("CLM_LN_COINS_AMT"),
    F.col("Extract.CNSD_CHRG_AMT").alias("CLM_LN_CNSD_CHRG_AMT"),
    F.col("Extract.COPAY_AMT").alias("CLM_LN_COPAY_AMT"),
    F.col("Extract.DEDCT_AMT").alias("CLM_LN_DEDCT_AMT"),
    F.col("Extract.DSALW_AMT").alias("CLM_LN_DSALW_AMT"),
    F.col("Extract.PAYBL_AMT").alias("CLM_LN_PAYBL_AMT"),
    F.col("Extract.ALW_PRICE_UNIT_CT").alias("CLM_LN_ALW_PRICE_UNIT_CT"),
    F.when(
        (F.length(F.trim(F.col("refDsalwExcd.EXCD_ID")))==0),
        F.lit(" ")
    ).otherwise(
        F.when(
            (F.trim(F.col("refDsalwExcd.EXCD_ID"))=="NA")|(F.trim(F.col("refDsalwExcd.EXCD_ID"))=="UNK"),
            F.lit(" ")
        ).otherwise(F.col("refDsalwExcd.EXCD_ID"))
    ).alias("DSALW_EXCD_ID"),
    F.when(F.isnull(F.col("refDsalwExcd.EXCD_SH_TX")),F.lit(" ")).otherwise(F.col("refDsalwExcd.EXCD_SH_TX")).alias("DSALW_EXCD_DESC"),
    F.when(
        (F.length(F.trim(F.col("refEobExcd.EXCD_ID")))==0),
        F.lit(" ")
    ).otherwise(
        F.when(
            (F.trim(F.col("refEobExcd.EXCD_ID"))=="NA")|(F.trim(F.col("refEobExcd.EXCD_ID"))=="UNK"),
            F.lit(" ")
        ).otherwise(F.col("refEobExcd.EXCD_ID"))
    ).alias("EOB_EXCD_ID"),
    F.when(
        (F.isnull(F.col("refDiagCd.DIAG_CD")))|(F.length(F.col("refDiagCd.DIAG_CD"))==0),
        F.lit(" ")
    ).otherwise(F.trim(F.col("refDiagCd.DIAG_CD"))).alias("PRI_DIAG_CD"),
    F.when(
        (F.isnull(F.col("refDiagCd.DIAG_CD_DESC")))|(F.length(F.col("refDiagCd.DIAG_CD_DESC"))==0),
        F.lit(" ")
    ).otherwise(F.trim(F.col("refDiagCd.DIAG_CD_DESC"))).alias("PRI_DIAG_DESC"),
    F.col("Extract.PROV_ID").alias("SVC_PROV_ID"),
    F.lit(RunCycle).alias("LAST_UPDT_RUN_CYC_NO"),
    F.when(
        F.length(F.trim(F.col("refClmLnPca.CLM_LN_PCA_LOB_CD")))==0,
        F.when(F.isnull(F.col("refClmTypCd.CLM_LN_PCA_LOB_CD")),F.lit(" ")).otherwise(F.col("refClmTypCd.CLM_LN_PCA_LOB_CD"))
    ).otherwise(F.col("refClmLnPca.CLM_LN_PCA_LOB_CD")).alias("CLM_LN_PCA_LOB_CD"),
    F.when(
        F.length(F.trim(F.col("refClmLnPca.CLM_LN_PCA_LOB_NM")))==0,
        F.when(F.isnull(F.col("refClmTypCd.CLM_LN_PCA_LOB_NM")),F.lit(" ")).otherwise(F.col("refClmTypCd.CLM_LN_PCA_LOB_NM"))
    ).otherwise(F.col("refClmLnPca.CLM_LN_PCA_LOB_NM")).alias("CLM_LN_PCA_LOB_NM"),
    F.when(
        F.length(F.trim(F.col("refClmLnPca.CLM_LN_PCA_PRCS_CD")))==0,
        F.when(F.isnull(F.col("refPcaPrcs.TRGT_CD")),F.lit(" ")).otherwise(F.col("refPcaPrcs.TRGT_CD"))
    ).otherwise(F.col("refClmLnPca.CLM_LN_PCA_PRCS_CD")).alias("CLM_LN_PCA_PRCS_CD"),
    F.when(
        F.length(F.trim(F.col("refClmLnPca.CLM_LN_PCA_PRCS_NM")))==0,
        F.when(F.isnull(F.col("refPcaPrcs.TRGT_CD_NM")),F.lit(" ")).otherwise(F.col("refPcaPrcs.TRGT_CD_NM"))
    ).otherwise(F.col("refClmLnPca.CLM_LN_PCA_PRCS_NM")).alias("CLM_LN_PCA_PRCS_NM"),
    F.when(
        F.isnull(F.col("refClmLnPca.CNSD_AMT")),
        F.when(F.isnull(F.col("refPcaAmt.CHRG_AMT")),F.lit(0)).otherwise(F.col("refPcaAmt.CHRG_AMT"))
    ).otherwise(F.col("refClmLnPca.CNSD_AMT")).alias("CLM_LN_PCA_CNSD_AMT"),
    F.when(
        F.isnull(F.col("refClmLnPca.DSALW_AMT")),
        F.lit(0)
    ).otherwise(F.col("refClmLnPca.DSALW_AMT")).alias("CLM_LN_PCA_DSALW_AMT"),
    F.when(
        F.isnull(F.col("refClmLnPca.NONCNSD_AMT")),
        F.lit(0)
    ).otherwise(F.col("refClmLnPca.NONCNSD_AMT")).alias("CLM_LN_PCA_NONCNSD_AMT"),
    F.when(
        F.isnull(F.col("refClmLnPca.PD_AMT")),
        F.when(F.isnull(F.col("refPcaAmt.PAYBL_AMT")),F.lit(0)).otherwise(F.col("refPcaAmt.PAYBL_AMT"))
    ).otherwise(F.col("refClmLnPca.PD_AMT")).alias("CLM_LN_PCA_PD_AMT"),
    F.when(
        F.isnull(F.col("refClmLnPca.PROV_PD_AMT")),
        F.lit(0)
    ).otherwise(F.col("refClmLnPca.PROV_PD_AMT")).alias("CLM_LN_PCA_PROV_PD_AMT"),
    F.when(
        F.isnull(F.col("refClmLnPca.SUB_PD_AMT")),
        F.when(F.isnull(F.col("refPcaAmt.PAYBL_AMT")),F.lit(0)).otherwise(F.col("refPcaAmt.PAYBL_AMT"))
    ).otherwise(F.col("refClmLnPca.SUB_PD_AMT")).alias("CLM_LN_PCA_SUB_PD_AMT"),
    F.when(
        F.length(F.trim(F.col("refClmLnPca.EXCD_ID")))==0,
        F.lit(" ")
    ).otherwise(
        F.when(
            (F.trim(F.col("refClmLnPca.EXCD_ID"))=="NA")|(F.trim(F.col("refClmLnPca.EXCD_ID"))=="UNK"),
            F.lit(" ")
        ).otherwise(F.col("refClmLnPca.EXCD_ID"))
    ).alias("PCA_DSALW_EXCD_ID"),
    F.when(
        F.length(F.trim(F.col("refClmLnPca.EXCD_SH_TX")))==0,
        F.lit(" ")
    ).otherwise(
        F.when(
            (F.col("refClmLnPca.EXCD_SH_TX")=="NA")|(F.col("refClmLnPca.EXCD_SH_TX")=="UNK"),
            F.lit(" ")
        ).otherwise(F.col("refClmLnPca.EXCD_SH_TX"))
    ).alias("PCA_DSALW_EXCD_DESC"),
    F.when(F.isnull(F.col("ClmLnRemitCd.REMIT_PATN_RESP_AMT")),F.lit(0)).otherwise(F.col("ClmLnRemitCd.REMIT_PATN_RESP_AMT")).alias("CLM_LN_REMIT_PATN_RESP_AMT"),
    F.when(F.isnull(F.col("ClmLnRemitCd.REMIT_PROV_WRT_OFF_AMT")),F.lit(0)).otherwise(F.col("ClmLnRemitCd.REMIT_PROV_WRT_OFF_AMT")).alias("CLM_LN_REMIT_PROV_WRT_OFF_AMT"),
    F.when(F.isnull(F.col("ClmLnRemitCd.REMIT_MBR_OTHR_LIAB_AMT")),F.lit(0)).otherwise(F.col("ClmLnRemitCd.REMIT_MBR_OTHR_LIAB_AMT")).alias("CLM_LN_REMIT_MBR_OTHR_LIAB_AMT"),
    F.when(F.isnull(F.col("ClmLnRemitCd.REMIT_NO_RESP_AMT")),F.lit(0)).otherwise(F.col("ClmLnRemitCd.REMIT_NO_RESP_AMT")).alias("CLM_LN_REMIT_NO_RESP_AMT"),
    F.when(
        (F.isnull(F.col("refClmLnFinlDisp.TRGT_CD")))|(F.length(F.trim(F.col("refClmLnFinlDisp.TRGT_CD")))==0),
        F.lit(" ")
    ).otherwise(F.col("refClmLnFinlDisp.TRGT_CD")).alias("CLM_LN_FINL_DISP_CD"),
    F.when(
        (F.isnull(F.col("refClmLnFinlDisp.TRGT_CD_NM")))|(F.length(F.trim(F.col("refClmLnFinlDisp.TRGT_CD_NM")))==0),
        F.lit(" ")
    ).otherwise(F.col("refClmLnFinlDisp.TRGT_CD_NM")).alias("CLM_LN_FINL_DISP_NM")
)

# For the final writing to the CODBCStage, we must do a merge.
# Also, for char/varchar columns, we rpad to the specified length if known.

# Identifying char or varchar columns from the JSON with known lengths:
#  CLM_LN_CAP_LN_IN (char length=1)
#  EXCD_ID (char length=4) => used in DSALW_EXCD_ID, EOB_EXCD_ID, PCA_DSALW_EXCD_ID
# The job also has many columns typed as varchar but no explicit length given except for above. 
# We will rpad only the known ones.

df_OutFile_final = (
    df_OutFile
    .withColumn("CLM_LN_CAP_LN_IN", F.rpad(F.col("CLM_LN_CAP_LN_IN"), 1, " "))
    .withColumn("DSALW_EXCD_ID", F.rpad(F.col("DSALW_EXCD_ID"), 4, " "))
    .withColumn("EOB_EXCD_ID", F.rpad(F.col("EOB_EXCD_ID"), 4, " "))
    .withColumn("PCA_DSALW_EXCD_ID", F.rpad(F.col("PCA_DSALW_EXCD_ID"), 4, " "))
)

# Now merge into #$ClmMartOwner#.CLM_DM_CLM_LN
# We create a staging table STAGING.IdsClmMartClmLnExtr_CLM_DM_CLM_LN_temp

temp_table_cdm = "STAGING.IdsClmMartClmLnExtr_CLM_DM_CLM_LN_temp"
execute_dml(f"DROP TABLE IF EXISTS {temp_table_cdm}", jdbc_url_clmmart, jdbc_props_clmmart)

df_OutFile_final.write \
    .format("jdbc") \
    .option("url", jdbc_url_clmmart) \
    .options(**jdbc_props_clmmart) \
    .option("dbtable", temp_table_cdm) \
    .mode("overwrite") \
    .save()

merge_sql_cdm = f"""
MERGE INTO {ClmMartOwner}.CLM_DM_CLM_LN AS T
USING {temp_table_cdm} AS S
ON
  T.SRC_SYS_CD = S.SRC_SYS_CD
  AND T.CLM_ID = S.CLM_ID
  AND T.CLM_LN_SEQ_NO = S.CLM_LN_SEQ_NO
WHEN MATCHED THEN
  UPDATE SET
    CLM_LN_RVNU_CD = S.CLM_LN_RVNU_CD,
    CLM_LN_ROOM_PRICE_METH_CD = S.CLM_LN_ROOM_PRICE_METH_CD,
    CLM_LN_TOS_CD = S.CLM_LN_TOS_CD,
    PROC_CD = S.PROC_CD,
    CLM_LN_CAP_LN_IN = S.CLM_LN_CAP_LN_IN,
    CLM_LN_SVC_STRT_DT = S.CLM_LN_SVC_STRT_DT,
    CLM_LN_SVC_END_DT = S.CLM_LN_SVC_END_DT,
    CLM_LN_CHRG_AMT = S.CLM_LN_CHRG_AMT,
    CLM_LN_PAYBL_TO_PROV_AMT = S.CLM_LN_PAYBL_TO_PROV_AMT,
    CLM_LN_UNIT_CT = S.CLM_LN_UNIT_CT,
    CLM_LN_SVC_ID = S.CLM_LN_SVC_ID,
    PROC_CD_DESC = S.PROC_CD_DESC,
    CLM_LN_POS_CD = S.CLM_LN_POS_CD,
    CLM_LN_POS_NM = S.CLM_LN_POS_NM,
    CLM_LN_PREAUTH_CD = S.CLM_LN_PREAUTH_CD,
    CLM_LN_PREAUTH_NM = S.CLM_LN_PREAUTH_NM,
    CLM_LN_RFRL_CD = S.CLM_LN_RFRL_CD,
    CLM_LN_RFRL_NM = S.CLM_LN_RFRL_NM,
    CLM_LN_RVNU_NM = S.CLM_LN_RVNU_NM,
    CLM_LN_ROOM_TYP_CD = S.CLM_LN_ROOM_TYP_CD,
    CLM_LN_ROOM_TYP_NM = S.CLM_LN_ROOM_TYP_NM,
    CLM_LN_ALW_AMT = S.CLM_LN_ALW_AMT,
    CLM_LN_COINS_AMT = S.CLM_LN_COINS_AMT,
    CLM_LN_CNSD_CHRG_AMT = S.CLM_LN_CNSD_CHRG_AMT,
    CLM_LN_COPAY_AMT = S.CLM_LN_COPAY_AMT,
    CLM_LN_DEDCT_AMT = S.CLM_LN_DEDCT_AMT,
    CLM_LN_DSALW_AMT = S.CLM_LN_DSALW_AMT,
    CLM_LN_PAYBL_AMT = S.CLM_LN_PAYBL_AMT,
    CLM_LN_ALW_PRICE_UNIT_CT = S.CLM_LN_ALW_PRICE_UNIT_CT,
    DSALW_EXCD_ID = S.DSALW_EXCD_ID,
    DSALW_EXCD_DESC = S.DSALW_EXCD_DESC,
    EOB_EXCD_ID = S.EOB_EXCD_ID,
    PRI_DIAG_CD = S.PRI_DIAG_CD,
    PRI_DIAG_DESC = S.PRI_DIAG_DESC,
    SVC_PROV_ID = S.SVC_PROV_ID,
    LAST_UPDT_RUN_CYC_NO = S.LAST_UPDT_RUN_CYC_NO,
    CLM_LN_PCA_LOB_CD = S.CLM_LN_PCA_LOB_CD,
    CLM_LN_PCA_LOB_NM = S.CLM_LN_PCA_LOB_NM,
    CLM_LN_PCA_PRCS_CD = S.CLM_LN_PCA_PRCS_CD,
    CLM_LN_PCA_PRCS_NM = S.CLM_LN_PCA_PRCS_NM,
    CLM_LN_PCA_CNSD_AMT = S.CLM_LN_PCA_CNSD_AMT,
    CLM_LN_PCA_DSALW_AMT = S.CLM_LN_PCA_DSALW_AMT,
    CLM_LN_PCA_NONCNSD_AMT = S.CLM_LN_PCA_NONCNSD_AMT,
    CLM_LN_PCA_PD_AMT = S.CLM_LN_PCA_PD_AMT,
    CLM_LN_PCA_PROV_PD_AMT = S.CLM_LN_PCA_PROV_PD_AMT,
    CLM_LN_PCA_SUB_PD_AMT = S.CLM_LN_PCA_SUB_PD_AMT,
    PCA_DSALW_EXCD_ID = S.PCA_DSALW_EXCD_ID,
    PCA_DSALW_EXCD_DESC = S.PCA_DSALW_EXCD_DESC,
    CLM_LN_REMIT_PATN_RESP_AMT = S.CLM_LN_REMIT_PATN_RESP_AMT,
    CLM_LN_REMIT_PROV_WRT_OFF_AMT = S.CLM_LN_REMIT_PROV_WRT_OFF_AMT,
    CLM_LN_REMIT_MBR_OTHR_LIAB_AMT = S.CLM_LN_REMIT_MBR_OTHR_LIAB_AMT,
    CLM_LN_REMIT_NO_RESP_AMT = S.CLM_LN_REMIT_NO_RESP_AMT,
    CLM_LN_FINL_DISP_CD = S.CLM_LN_FINL_DISP_CD,
    CLM_LN_FINL_DISP_NM = S.CLM_LN_FINL_DISP_NM
WHEN NOT MATCHED THEN
  INSERT (
    SRC_SYS_CD,
    CLM_ID,
    CLM_LN_SEQ_NO,
    CLM_LN_RVNU_CD,
    CLM_LN_ROOM_PRICE_METH_CD,
    CLM_LN_TOS_CD,
    PROC_CD,
    CLM_LN_CAP_LN_IN,
    CLM_LN_SVC_STRT_DT,
    CLM_LN_SVC_END_DT,
    CLM_LN_CHRG_AMT,
    CLM_LN_PAYBL_TO_PROV_AMT,
    CLM_LN_UNIT_CT,
    CLM_LN_SVC_ID,
    PROC_CD_DESC,
    CLM_LN_POS_CD,
    CLM_LN_POS_NM,
    CLM_LN_PREAUTH_CD,
    CLM_LN_PREAUTH_NM,
    CLM_LN_RFRL_CD,
    CLM_LN_RFRL_NM,
    CLM_LN_RVNU_NM,
    CLM_LN_ROOM_TYP_CD,
    CLM_LN_ROOM_TYP_NM,
    CLM_LN_ALW_AMT,
    CLM_LN_COINS_AMT,
    CLM_LN_CNSD_CHRG_AMT,
    CLM_LN_COPAY_AMT,
    CLM_LN_DEDCT_AMT,
    CLM_LN_DSALW_AMT,
    CLM_LN_PAYBL_AMT,
    CLM_LN_ALW_PRICE_UNIT_CT,
    DSALW_EXCD_ID,
    DSALW_EXCD_DESC,
    EOB_EXCD_ID,
    PRI_DIAG_CD,
    PRI_DIAG_DESC,
    SVC_PROV_ID,
    LAST_UPDT_RUN_CYC_NO,
    CLM_LN_PCA_LOB_CD,
    CLM_LN_PCA_LOB_NM,
    CLM_LN_PCA_PRCS_CD,
    CLM_LN_PCA_PRCS_NM,
    CLM_LN_PCA_CNSD_AMT,
    CLM_LN_PCA_DSALW_AMT,
    CLM_LN_PCA_NONCNSD_AMT,
    CLM_LN_PCA_PD_AMT,
    CLM_LN_PCA_PROV_PD_AMT,
    CLM_LN_PCA_SUB_PD_AMT,
    PCA_DSALW_EXCD_ID,
    PCA_DSALW_EXCD_DESC,
    CLM_LN_REMIT_PATN_RESP_AMT,
    CLM_LN_REMIT_PROV_WRT_OFF_AMT,
    CLM_LN_REMIT_MBR_OTHR_LIAB_AMT,
    CLM_LN_REMIT_NO_RESP_AMT,
    CLM_LN_FINL_DISP_CD,
    CLM_LN_FINL_DISP_NM
  )
  VALUES (
    S.SRC_SYS_CD,
    S.CLM_ID,
    S.CLM_LN_SEQ_NO,
    S.CLM_LN_RVNU_CD,
    S.CLM_LN_ROOM_PRICE_METH_CD,
    S.CLM_LN_TOS_CD,
    S.PROC_CD,
    S.CLM_LN_CAP_LN_IN,
    S.CLM_LN_SVC_STRT_DT,
    S.CLM_LN_SVC_END_DT,
    S.CLM_LN_CHRG_AMT,
    S.CLM_LN_PAYBL_TO_PROV_AMT,
    S.CLM_LN_UNIT_CT,
    S.CLM_LN_SVC_ID,
    S.PROC_CD_DESC,
    S.CLM_LN_POS_CD,
    S.CLM_LN_POS_NM,
    S.CLM_LN_PREAUTH_CD,
    S.CLM_LN_PREAUTH_NM,
    S.CLM_LN_RFRL_CD,
    S.CLM_LN_RFRL_NM,
    S.CLM_LN_RVNU_NM,
    S.CLM_LN_ROOM_TYP_CD,
    S.CLM_LN_ROOM_TYP_NM,
    S.CLM_LN_ALW_AMT,
    S.CLM_LN_COINS_AMT,
    S.CLM_LN_CNSD_CHRG_AMT,
    S.CLM_LN_COPAY_AMT,
    S.CLM_LN_DEDCT_AMT,
    S.CLM_LN_DSALW_AMT,
    S.CLM_LN_PAYBL_AMT,
    S.CLM_LN_ALW_PRICE_UNIT_CT,
    S.DSALW_EXCD_ID,
    S.DSALW_EXCD_DESC,
    S.EOB_EXCD_ID,
    S.PRI_DIAG_CD,
    S.PRI_DIAG_DESC,
    S.SVC_PROV_ID,
    S.LAST_UPDT_RUN_CYC_NO,
    S.CLM_LN_PCA_LOB_CD,
    S.CLM_LN_PCA_LOB_NM,
    S.CLM_LN_PCA_PRCS_CD,
    S.CLM_LN_PCA_PRCS_NM,
    S.CLM_LN_PCA_CNSD_AMT,
    S.CLM_LN_PCA_DSALW_AMT,
    S.CLM_LN_PCA_NONCNSD_AMT,
    S.CLM_LN_PCA_PD_AMT,
    S.CLM_LN_PCA_PROV_PD_AMT,
    S.CLM_LN_PCA_SUB_PD_AMT,
    S.PCA_DSALW_EXCD_ID,
    S.PCA_DSALW_EXCD_DESC,
    S.CLM_LN_REMIT_PATN_RESP_AMT,
    S.CLM_LN_REMIT_PROV_WRT_OFF_AMT,
    S.CLM_LN_REMIT_MBR_OTHR_LIAB_AMT,
    S.CLM_LN_REMIT_NO_RESP_AMT,
    S.CLM_LN_FINL_DISP_CD,
    S.CLM_LN_FINL_DISP_NM
  )
;
"""
execute_dml(merge_sql_cdm, jdbc_url_clmmart, jdbc_props_clmmart)

# ------------------------------------------------------------------------------
# DB2Connector "IDS_ClmLn" -> out "ClmLn" -> Trans2 -> "W_BAL_DM_CLM_LN"
# ------------------------------------------------------------------------------
ids_clmln_query = f"""SELECT LN.SRC_SYS_CD_SK as SRC_SYS_CD_SK,
LN.CLM_ID as CLM_ID,
LN.CLM_LN_SEQ_NO as CLM_LN_SEQ_NO
FROM {IDSOwner}.CLM_LN LN,
     {IDSOwner}.W_WEBDM_ETL_DRVR EXTR
WHERE LN.SRC_SYS_CD_SK = EXTR.SRC_SYS_CD_SK
  and LN.CLM_ID = EXTR.CLM_ID
"""
df_ClmLn = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", ids_clmln_query)
    .load()
)

# ------------------------------------------------------------------------------
# cd_mppng hashed file used as source in Trans2:
# We already loaded df_cd_mppng as df_SrcSysCd
# ------------------------------------------------------------------------------

df_Trans2 = df_ClmLn.alias("ClmLn").join(
    df_SrcSysCd.alias("SrcSysCd"),
    F.col("ClmLn.SRC_SYS_CD_SK")==F.col("SrcSysCd.CD_MPPNG_SK"),
    "left"
)

df_Original = df_Trans2.select(
    F.when(F.isnull(F.col("SrcSysCd.TRGT_CD")),F.lit(" ")).otherwise(F.col("SrcSysCd.TRGT_CD")).alias("SRC_SYS_CD"),
    F.col("ClmLn.CLM_ID").alias("CLM_ID"),
    F.col("ClmLn.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.lit(RunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

# No char length specified for these columns in the JSON, so no rpad needed here.

temp_table_wbal = "STAGING.IdsClmMartClmLnExtr_W_BAL_DM_CLM_LN_temp"
execute_dml(f"DROP TABLE IF EXISTS {temp_table_wbal}", jdbc_url_clmmart, jdbc_props_clmmart)

df_Original.write \
    .format("jdbc") \
    .option("url", jdbc_url_clmmart) \
    .options(**jdbc_props_clmmart) \
    .option("dbtable", temp_table_wbal) \
    .mode("overwrite") \
    .save()

merge_sql_wbal = f"""
MERGE INTO {ClmMartOwner}.W_BAL_DM_CLM_LN AS T
USING {temp_table_wbal} AS S
ON
  T.SRC_SYS_CD = S.SRC_SYS_CD
  AND T.CLM_ID = S.CLM_ID
  AND T.CLM_LN_SEQ_NO = S.CLM_LN_SEQ_NO
WHEN MATCHED THEN
  UPDATE SET
    LAST_UPDT_RUN_CYC_EXCTN_SK = S.LAST_UPDT_RUN_CYC_EXCTN_SK
WHEN NOT MATCHED THEN
  INSERT (
    SRC_SYS_CD,
    CLM_ID,
    CLM_LN_SEQ_NO,
    LAST_UPDT_RUN_CYC_EXCTN_SK
  )
  VALUES (
    S.SRC_SYS_CD,
    S.CLM_ID,
    S.CLM_LN_SEQ_NO,
    S.LAST_UPDT_RUN_CYC_EXCTN_SK
  )
;
"""
execute_dml(merge_sql_wbal, jdbc_url_clmmart, jdbc_props_clmmart)