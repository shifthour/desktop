# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC Â© Copyright 2007 Blue Cross and Blue Shield of Kansas City
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
# MAGIC 
# MAGIC 
# MAGIC   
# MAGIC Nagesh Bandi    2013-09-17              5114             Original Programming(Server to Parallel)                                                                                           IntegrateWrhsDevl      Jag Yelavarthi        2013-12-01 
# MAGIC Brent Leland       10-18-2022                              Moved DIAG_CD lookup to join stage for faster performance.                                                              IntegrateDev1\(9)Ken Bradmon\(9)2022-11-18
# MAGIC                                                                                                    
# MAGIC 
# MAGIC Goutham Kalidindi 3-27-2024     US-614877      Split the SQL in the DIAG_CD look to multiple joins for better performace                                             IntegrateDev2         Reddy Sanam         03-27-2024

# MAGIC PCA process filter:  PCA_TYP_CD in  PERSONAL CARE ACCOUNT PROCESSING code set with values of  EMPWBNF, RUNOUT, or PCA
# MAGIC for source code 'H'
# MAGIC PCA_TYP_CD in  PERSONAL CARE ACCOUNT PROCESSING code set with values of  EMPWBNF, RUNOUT, or PCA
# MAGIC Firstr diagnosis code used:  DIAGNOSIS ORDINAL = 1
# MAGIC Write CLM_DM_CLM_LN Data into a Sequential file for Load Job IdsDmClmDmClmLnLoad.
# MAGIC Read all the Data from IDS CLM_LN_ALT_CHRG_REMIT Table;
# MAGIC Add Defaults and Null Handling.
# MAGIC Job Name: 
# MAGIC 
# MAGIC IdsDmClmMartClmLnExtr
# MAGIC Code SK lookups for Denormalization
# MAGIC 
# MAGIC Lookup Keys:
# MAGIC CLM_LN_SK
# MAGIC CLM_ID
# MAGIC CLM_LN_SEQ_NO
# MAGIC SRC_SYS_CD_SK
# MAGIC CLM_LN_DSALW_EXCD_SK
# MAGIC CLM_LN_EOB_EXCD_SK
# MAGIC CLM_LN_TOS_CD_SK
# MAGIC CLM_LN_REFRL_CD_SK
# MAGIC CLM_LN_FINL_DISP_CD_SK
# MAGIC CLM_LN_PREAUTH_CD_SK
# MAGIC CLM_LN_ROOM_PRICE_METH_CD_SK
# MAGIC CLM_LN_ROOM_TYP_CD_SK
# MAGIC CLM_LN_POS_CD_SK
# MAGIC CLM_SK
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import StringType, DecimalType, TimestampType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
ids_secret_name = get_widget_value('ids_secret_name','')

# --------------------------------------------------------------------------------
# DB2ConnectorPX: db2_CLM_LN_Extr
# --------------------------------------------------------------------------------
jdbc_url_db2_CLM_LN_Extr, jdbc_props_db2_CLM_LN_Extr = get_db_config(ids_secret_name)
extract_query_db2_CLM_LN_Extr = """
SELECT DISTINCT 
              LN.CLM_LN_SK,
              LN.CLM_ID,
              LN.CLM_LN_SEQ_NO,
              LN.CLM_SK,
              LN.SVC_PROV_SK,
              LN.CLM_LN_DSALW_EXCD_SK, 
              LN.CLM_LN_EOB_EXCD_SK,
              COALESCE(CD.TRGT_CD, 'UNK') AS  SRC_SYS_CD , 
              LN.CLM_LN_FINL_DISP_CD_SK,
              LN.CAP_LN_IN,
              LN.CLM_LN_POS_CD_SK,            
              LN.CLM_LN_PREAUTH_CD_SK, 
              LN.SVC_END_DT_SK,
              LN.SVC_STRT_DT_SK,
              LN.ALW_AMT,
              LN.CLM_LN_RFRL_CD_SK, 
              LN.CHRG_AMT,   
              LN.CLM_LN_ROOM_PRICE_METH_CD_SK,            
              LN.COINS_AMT,
              LN.CLM_LN_ROOM_TYP_CD_SK,  
              LN.CNSD_CHRG_AMT, 
              LN.CLM_LN_TOS_CD_SK,  
              LN.COPAY_AMT, 
              LN.DEDCT_AMT, 
              LN.DSALW_AMT, 
              LN.PAYBL_AMT, 
              LN.PAYBL_TO_PROV_AMT, 
              LN.ALW_PRICE_UNIT_CT,
              LN.UNIT_CT,
              LN.SVC_ID,             
              PROC.PROC_CD, 
              PROC.PROC_CD_DESC, 
              RVNU.RVNU_CD, 
              RVNU.RVNU_CD_DESC,
              PROV.PROV_ID,
              LN.SRC_SYS_CD_SK
FROM      
       #$IDSOwner#.CLM C
  JOIN  #$IDSOwner#.CLM_LN LN 
          ON     C.CLM_SK  =  LN.CLM_SK 
  JOIN  #$IDSOwner#.W_WEBDM_ETL_DRVR EXTR 
          ON EXTR.SRC_SYS_CD_SK =  C.SRC_SYS_CD_SK
         AND EXTR.CLM_ID =  C.CLM_ID 
  JOIN  #$IDSOwner#.RVNU_CD RVNU
          ON LN.CLM_LN_RVNU_CD_SK =   RVNU.RVNU_CD_SK
  JOIN  #$IDSOwner#.PROC_CD PROC
          ON LN.PROC_CD_SK =   PROC.PROC_CD_SK 
  JOIN  #$IDSOwner#.PROV PROV
          ON PROV.PROV_SK = LN.SVC_PROV_SK
  LEFT JOIN #$IDSOwner#.CD_MPPNG CD
          ON LN.SRC_SYS_CD_SK = CD.CD_MPPNG_SK
"""
df_db2_CLM_LN_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_CLM_LN_Extr)
    .options(**jdbc_props_db2_CLM_LN_Extr)
    .option("query", extract_query_db2_CLM_LN_Extr)
    .load()
)

# --------------------------------------------------------------------------------
# DB2ConnectorPX: db2_CLM_LN_PCA_Extr
# --------------------------------------------------------------------------------
jdbc_url_db2_CLM_LN_PCA_Extr, jdbc_props_db2_CLM_LN_PCA_Extr = get_db_config(ids_secret_name)
extract_query_db2_CLM_LN_PCA_Extr = """
SELECT 

PCA.CLM_LN_SK,
PCA.DSALW_EXCD_SK,
PCA.CLM_LN_PCA_LOB_CD_SK,
PCA.CLM_LN_PCA_PRCS_CD_SK,
PCA.CNSD_AMT,
PCA.DSALW_AMT,
PCA.NONCNSD_AMT,
PCA.PROV_PD_AMT,
PCA.SUB_PD_AMT,
PCA.PD_AMT,
EXCD.EXCD_SH_TX,
EXCD.EXCD_ID ,
CD1.TRGT_CD CLM_LN_PCA_LOB_CD,
CD1.TRGT_CD_NM CLM_LN_PCA_LOB_NM,
CD2.TRGT_CD CLM_LN_PCA_PRCS_CD,
CD2.TRGT_CD_NM CLM_LN_PCA_PRCS_NM

FROM 

       #$IDSOwner#.CLM_LN LN
JOIN   #$IDSOwner#.CLM_LN_PCA PCA 
       ON LN.CLM_LN_SK = PCA.CLM_LN_SK
JOIN   #$IDSOwner#.W_WEBDM_ETL_DRVR EXTR 
       ON EXTR.SRC_SYS_CD_SK = LN.SRC_SYS_CD_SK  
       AND EXTR.CLM_ID = LN.CLM_ID 
JOIN   #$IDSOwner#.EXCD EXCD 
       ON PCA.DSALW_EXCD_SK = EXCD.EXCD_SK
LEFT JOIN   #$IDSOwner#.CD_MPPNG CD1 
       ON PCA.CLM_LN_PCA_LOB_CD_SK = CD1.CD_MPPNG_SK
LEFT JOIN   #$IDSOwner#.CD_MPPNG CD2 
       ON PCA.CLM_LN_PCA_PRCS_CD_SK = CD2.CD_MPPNG_SK
"""
df_db2_CLM_LN_PCA_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_CLM_LN_PCA_Extr)
    .options(**jdbc_props_db2_CLM_LN_PCA_Extr)
    .option("query", extract_query_db2_CLM_LN_PCA_Extr)
    .load()
)

# --------------------------------------------------------------------------------
# DB2ConnectorPX: db2_PCA_PRCS_Extr
# --------------------------------------------------------------------------------
jdbc_url_db2_PCA_PRCS_Extr, jdbc_props_db2_PCA_PRCS_Extr = get_db_config(ids_secret_name)
extract_query_db2_PCA_PRCS_Extr = """
SELECT 

     CLM_SK,
    MPPNG2.TRGT_CD,
    MPPNG2.TRGT_CD_NM 

  FROM 

      #$IDSOwner#.CLM C, 
       #$IDSOwner#.W_WEBDM_ETL_DRVR EXTR, 
      #$IDSOwner#.CD_MPPNG MPPNG, 
       #$IDSOwner#.CD_MPPNG MPPNG2 
 
WHERE 

    EXTR.CLM_ID = C.CLM_ID  
AND MPPNG.CD_MPPNG_SK = C.PCA_TYP_CD_SK 
AND MPPNG.TRGT_CD in ('EMPWBNF','RUNOUT','PCA')
AND MPPNG2.TRGT_DOMAIN_NM = 'PERSONAL CARE ACCOUNT PROCESSING'
AND MPPNG2.SRC_CD = 'H'
"""
df_db2_PCA_PRCS_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_PCA_PRCS_Extr)
    .options(**jdbc_props_db2_PCA_PRCS_Extr)
    .option("query", extract_query_db2_PCA_PRCS_Extr)
    .load()
)

# --------------------------------------------------------------------------------
# DB2ConnectorPX: db2_PCA_AMNT_Extr
# --------------------------------------------------------------------------------
jdbc_url_db2_PCA_AMNT_Extr, jdbc_props_db2_PCA_AMNT_Extr = get_db_config(ids_secret_name)
extract_query_db2_PCA_AMNT_Extr = """
SELECT           
               C.CLM_SK,
               CLN.CHRG_AMT,
               CLN.PAYBL_AMT

  FROM 

       #$IDSOwner#.CLM C, 
       #$IDSOwner#.CLM_LN CLN,
       #$IDSOwner#.W_WEBDM_ETL_DRVR EXTR, 
       #$IDSOwner#.CD_MPPNG MPPNG 
 
WHERE 

            EXTR.CLM_ID = C.CLM_ID
   AND C.CLM_SK = CLN.CLM_SK  
   AND MPPNG.CD_MPPNG_SK = C.PCA_TYP_CD_SK 
   AND MPPNG.TRGT_CD in ('EMPWBNF','RUNOUT','PCA')
"""
df_db2_PCA_AMNT_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_PCA_AMNT_Extr)
    .options(**jdbc_props_db2_PCA_AMNT_Extr)
    .option("query", extract_query_db2_PCA_AMNT_Extr)
    .load()
)

# --------------------------------------------------------------------------------
# DB2ConnectorPX: db2_CLM_TYP_Extr
# --------------------------------------------------------------------------------
jdbc_url_db2_CLM_TYP_Extr, jdbc_props_db2_CLM_TYP_Extr = get_db_config(ids_secret_name)
extract_query_db2_CLM_TYP_Extr = """
SELECT 

   C.CLM_SK,
   MPPNG2.TRGT_CD,
   MPPNG2.TRGT_CD_NM 

FROM 

  #$IDSOwner#.CLM C, 
  #$IDSOwner#.CLM_CHK CLMCK,
  #$IDSOwner#.W_WEBDM_ETL_DRVR EXTR, 
  #$IDSOwner#.CD_MPPNG MPPNG, 
  #$IDSOwner#.CD_MPPNG MPPNG2 

WHERE  

    EXTR.SRC_SYS_CD_SK = C.SRC_SYS_CD_SK  
AND EXTR.CLM_ID = C.CLM_ID  
AND MPPNG.CD_MPPNG_SK = C.PCA_TYP_CD_SK 
AND MPPNG.TRGT_CD in ('EMPWBNF','RUNOUT','PCA')
AND C.CLM_SK = CLMCK.CLM_SK
AND MPPNG2.CD_MPPNG_SK = CLMCK.CLM_CHK_LOB_CD_SK
;
"""
df_db2_CLM_TYP_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_CLM_TYP_Extr)
    .options(**jdbc_props_db2_CLM_TYP_Extr)
    .option("query", extract_query_db2_CLM_TYP_Extr)
    .load()
)

# --------------------------------------------------------------------------------
# DB2ConnectorPX: db2_CLM_LN_REMIT_Extr
# --------------------------------------------------------------------------------
jdbc_url_db2_CLM_LN_REMIT_Extr, jdbc_props_db2_CLM_LN_REMIT_Extr = get_db_config(ids_secret_name)
extract_query_db2_CLM_LN_REMIT_Extr = """
SELECT 

   REMIT.CLM_LN_SK,
   REMIT.REMIT_PATN_RESP_AMT,
   REMIT.REMIT_PROV_WRT_OFF_AMT,
   REMIT.REMIT_MBR_OTHR_LIAB_AMT,
   REMIT.REMIT_NO_RESP_AMT 

FROM 

   #$IDSOwner#.CLM_LN_REMIT REMIT,
   #$IDSOwner#.W_WEBDM_ETL_DRVR EXTR 
   
WHERE 

    EXTR.SRC_SYS_CD_SK = REMIT.SRC_SYS_CD_SK  AND
    EXTR.CLM_ID = REMIT.CLM_ID
"""
df_db2_CLM_LN_REMIT_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_CLM_LN_REMIT_Extr)
    .options(**jdbc_props_db2_CLM_LN_REMIT_Extr)
    .option("query", extract_query_db2_CLM_LN_REMIT_Extr)
    .load()
)

# --------------------------------------------------------------------------------
# DB2ConnectorPX: DB2_CD_MPPNG_DIAG_ORDNL
# --------------------------------------------------------------------------------
jdbc_url_DB2_CD_MPPNG_DIAG_ORDNL, jdbc_props_DB2_CD_MPPNG_DIAG_ORDNL = get_db_config(ids_secret_name)
extract_query_DB2_CD_MPPNG_DIAG_ORDNL = """
SELECT CD_MPPNG_SK FROM     #$IDSOwner#.CD_MPPNG          CM 
WHERE CM.SRC_DOMAIN_NM = 'DIAGNOSIS ORDINAL'        AND CM.SRC_CD = '1'
"""
df_DB2_CD_MPPNG_DIAG_ORDNL = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_DB2_CD_MPPNG_DIAG_ORDNL)
    .options(**jdbc_props_DB2_CD_MPPNG_DIAG_ORDNL)
    .option("query", extract_query_DB2_CD_MPPNG_DIAG_ORDNL)
    .load()
)

# --------------------------------------------------------------------------------
# DB2ConnectorPX: db2_CLM_LN_DRVR
# --------------------------------------------------------------------------------
jdbc_url_db2_CLM_LN_DRVR, jdbc_props_db2_CLM_LN_DRVR = get_db_config(ids_secret_name)
extract_query_db2_CLM_LN_DRVR = """
SELECT 

     LN.CLM_ID, 
     LN.CLM_LN_SEQ_NO,  
     LN.SRC_SYS_CD_SK,
     LN.CLM_LN_SK  

FROM
      #$IDSOwner#.CLM               C,
      #$IDSOwner#.CLM_LN            LN,  
      #$IDSOwner#.W_WEBDM_ETL_DRVR  EXTR
WHERE     C.CLM_SK = LN.CLM_SK 
  AND EXTR.CLM_ID = C.CLM_ID  
  AND EXTR.SRC_SYS_CD_SK = C.SRC_SYS_CD_SK
"""
df_db2_CLM_LN_DRVR = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_CLM_LN_DRVR)
    .options(**jdbc_props_db2_CLM_LN_DRVR)
    .option("query", extract_query_db2_CLM_LN_DRVR)
    .load()
)

# --------------------------------------------------------------------------------
# DB2ConnectorPX: db2_CD_MPPNG_in
# --------------------------------------------------------------------------------
jdbc_url_db2_CD_MPPNG_in, jdbc_props_db2_CD_MPPNG_in = get_db_config(ids_secret_name)
extract_query_db2_CD_MPPNG_in = """
SELECT 

CD_MPPNG_SK,
COALESCE(TRGT_CD,'UNK') TRGT_CD,
COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM 

FROM #$IDSOwner#.CD_MPPNG  ;
"""
df_db2_CD_MPPNG_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_CD_MPPNG_in)
    .options(**jdbc_props_db2_CD_MPPNG_in)
    .option("query", extract_query_db2_CD_MPPNG_in)
    .load()
)

# --------------------------------------------------------------------------------
# PxCopy: Cpy_Cd_Mppng (simply copying from db2_CD_MPPNG_in)
# --------------------------------------------------------------------------------
df_Cpy_Cd_Mppng = df_db2_CD_MPPNG_in

# --------------------------------------------------------------------------------
# DB2ConnectorPX: db2_ExcdExtr
# --------------------------------------------------------------------------------
jdbc_url_db2_ExcdExtr, jdbc_props_db2_ExcdExtr = get_db_config(ids_secret_name)
extract_query_db2_ExcdExtr = """
SELECT 

EXCD.EXCD_SK,
EXCD.EXCD_ID,
EXCD.EXCD_SH_TX

FROM #$IDSOwner#.EXCD EXCD;
"""
df_db2_ExcdExtr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_ExcdExtr)
    .options(**jdbc_props_db2_ExcdExtr)
    .option("query", extract_query_db2_ExcdExtr)
    .load()
)

# --------------------------------------------------------------------------------
# PxCopy: Cpy_Excd (simply copying from db2_ExcdExtr)
# --------------------------------------------------------------------------------
df_Cpy_Excd = df_db2_ExcdExtr

# --------------------------------------------------------------------------------
# PxLookup: lkp_Codes
# Primary link: df_db2_CLM_LN_Extr
# Lookup links (all "left" joins):
#   df_db2_CLM_LN_PCA_Extr,
#   df_Cpy_Excd (twice, alias refDsalwExcd and refEobExcd),
#   df_Cpy_Cd_Mppng (many times, with various aliases),
#   df_db2_PCA_PRCS_Extr,
#   df_db2_PCA_AMNT_Extr,
#   df_db2_CLM_TYP_Extr,
#   df_db2_CLM_LN_REMIT_Extr
# --------------------------------------------------------------------------------
df_lkp_Codes_temp = (
    df_db2_CLM_LN_Extr.alias("lnk_IdsDmClmMartClmLnExtr_InABC")
    .join(
        df_db2_CLM_LN_PCA_Extr.alias("refClmLnPca_lkp"),
        F.col("lnk_IdsDmClmMartClmLnExtr_InABC.CLM_LN_SK") 
         == F.col("refClmLnPca_lkp.CLM_LN_SK"),
        "left"
    )
    .join(
        df_Cpy_Excd.alias("refDsalwExcd"),
        F.col("lnk_IdsDmClmMartClmLnExtr_InABC.CLM_LN_DSALW_EXCD_SK")
         == F.col("refDsalwExcd.EXCD_SK"),
        "left"
    )
    .join(
        df_Cpy_Excd.alias("refEobExcd"),
        F.col("lnk_IdsDmClmMartClmLnExtr_InABC.CLM_LN_EOB_EXCD_SK")
         == F.col("refEobExcd.EXCD_SK"),
        "left"
    )
    .join(
        df_Cpy_Cd_Mppng.alias("refClmLnTos"),
        F.col("lnk_IdsDmClmMartClmLnExtr_InABC.CLM_LN_TOS_CD_SK")
         == F.col("refClmLnTos.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_Cpy_Cd_Mppng.alias("refClmLnReferral"),
        F.col("lnk_IdsDmClmMartClmLnExtr_InABC.CLM_LN_RFRL_CD_SK")
         == F.col("refClmLnReferral.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_Cpy_Cd_Mppng.alias("refClmLnFinlDisp"),
        F.col("lnk_IdsDmClmMartClmLnExtr_InABC.CLM_LN_FINL_DISP_CD_SK")
         == F.col("refClmLnFinlDisp.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_Cpy_Cd_Mppng.alias("refClmLnPreauth"),
        F.col("lnk_IdsDmClmMartClmLnExtr_InABC.CLM_LN_PREAUTH_CD_SK")
         == F.col("refClmLnPreauth.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_Cpy_Cd_Mppng.alias("refClmLnRoomPricMeth"),
        F.col("lnk_IdsDmClmMartClmLnExtr_InABC.CLM_LN_ROOM_PRICE_METH_CD_SK")
         == F.col("refClmLnRoomPricMeth.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_Cpy_Cd_Mppng.alias("refClmLnRoomTyp"),
        F.col("lnk_IdsDmClmMartClmLnExtr_InABC.CLM_LN_ROOM_TYP_CD_SK")
         == F.col("refClmLnRoomTyp.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_Cpy_Cd_Mppng.alias("refClmLnPos"),
        F.col("lnk_IdsDmClmMartClmLnExtr_InABC.CLM_LN_POS_CD_SK")
         == F.col("refClmLnPos.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_db2_PCA_PRCS_Extr.alias("refPcaPrcs_lkp"),
        F.col("lnk_IdsDmClmMartClmLnExtr_InABC.CLM_SK") 
         == F.col("refPcaPrcs_lkp.CLM_SK"),
        "left"
    )
    .join(
        df_db2_PCA_AMNT_Extr.alias("refPcaAmt_lkp"),
        F.col("lnk_IdsDmClmMartClmLnExtr_InABC.CLM_SK") 
         == F.col("refPcaAmt_lkp.CLM_SK"),
        "left"
    )
    .join(
        df_db2_CLM_TYP_Extr.alias("refClmTypCd_lkp"),
        F.col("lnk_IdsDmClmMartClmLnExtr_InABC.CLM_SK")
         == F.col("refClmTypCd_lkp.CLM_SK"),
        "left"
    )
    .join(
        df_db2_CLM_LN_REMIT_Extr.alias("refClmLnRemitCd_lkp"),
        F.col("lnk_IdsDmClmMartClmLnExtr_InABC.CLM_LN_SK")
         == F.col("refClmLnRemitCd_lkp.CLM_LN_SK"),
        "left"
    )
)

df_lkp_Codes = df_lkp_Codes_temp.select(
    F.col("lnk_IdsDmClmMartClmLnExtr_InABC.CLM_LN_SK").alias("CLM_LN_SK"),
    F.col("lnk_IdsDmClmMartClmLnExtr_InABC.CLM_ID").alias("CLM_ID"),
    F.col("lnk_IdsDmClmMartClmLnExtr_InABC.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("lnk_IdsDmClmMartClmLnExtr_InABC.CLM_SK").alias("CLM_SK"),
    F.col("lnk_IdsDmClmMartClmLnExtr_InABC.SVC_PROV_SK").alias("SVC_PROV_SK"),
    F.col("lnk_IdsDmClmMartClmLnExtr_InABC.CLM_LN_DSALW_EXCD_SK").alias("CLM_LN_DSALW_EXCD_SK"),
    F.col("lnk_IdsDmClmMartClmLnExtr_InABC.CLM_LN_EOB_EXCD_SK").alias("CLM_LN_EOB_EXCD_SK"),
    F.col("lnk_IdsDmClmMartClmLnExtr_InABC.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnk_IdsDmClmMartClmLnExtr_InABC.CLM_LN_FINL_DISP_CD_SK").alias("CLM_LN_FINL_DISP_CD_SK"),
    F.col("lnk_IdsDmClmMartClmLnExtr_InABC.CAP_LN_IN").alias("CAP_LN_IN"),
    F.col("lnk_IdsDmClmMartClmLnExtr_InABC.CLM_LN_POS_CD_SK").alias("CLM_LN_POS_CD_SK"),
    F.col("lnk_IdsDmClmMartClmLnExtr_InABC.CLM_LN_PREAUTH_CD_SK").alias("CLM_LN_PREAUTH_CD_SK"),
    F.col("lnk_IdsDmClmMartClmLnExtr_InABC.SVC_END_DT_SK").alias("SVC_END_DT_SK"),
    F.col("lnk_IdsDmClmMartClmLnExtr_InABC.SVC_STRT_DT_SK").alias("SVC_STRT_DT_SK"),
    F.col("lnk_IdsDmClmMartClmLnExtr_InABC.ALW_AMT").alias("ALW_AMT"),
    F.col("lnk_IdsDmClmMartClmLnExtr_InABC.CLM_LN_RFRL_CD_SK").alias("CLM_LN_RFRL_CD_SK"),
    F.col("lnk_IdsDmClmMartClmLnExtr_InABC.CHRG_AMT").alias("CLM_LN_CHRG_AMT"),
    F.col("lnk_IdsDmClmMartClmLnExtr_InABC.CLM_LN_ROOM_PRICE_METH_CD_SK").alias("CLM_LN_ROOM_PRICE_METH_CD_SK"),
    F.col("lnk_IdsDmClmMartClmLnExtr_InABC.COINS_AMT").alias("COINS_AMT"),
    F.col("lnk_IdsDmClmMartClmLnExtr_InABC.CLM_LN_ROOM_TYP_CD_SK").alias("CLM_LN_ROOM_TYP_CD_SK"),
    F.col("lnk_IdsDmClmMartClmLnExtr_InABC.CNSD_CHRG_AMT").alias("CNSD_CHRG_AMT"),
    F.col("lnk_IdsDmClmMartClmLnExtr_InABC.CLM_LN_TOS_CD_SK").alias("CLM_LN_TOS_CD_SK"),
    F.col("lnk_IdsDmClmMartClmLnExtr_InABC.COPAY_AMT").alias("COPAY_AMT"),
    F.col("lnk_IdsDmClmMartClmLnExtr_InABC.DEDCT_AMT").alias("DEDCT_AMT"),
    F.col("lnk_IdsDmClmMartClmLnExtr_InABC.DSALW_AMT").alias("DSALW_AMT"),
    F.col("lnk_IdsDmClmMartClmLnExtr_InABC.PAYBL_AMT").alias("CLM_LN_PAYBL_AMT"),
    F.col("lnk_IdsDmClmMartClmLnExtr_InABC.PAYBL_TO_PROV_AMT").alias("PAYBL_TO_PROV_AMT"),
    F.col("lnk_IdsDmClmMartClmLnExtr_InABC.ALW_PRICE_UNIT_CT").alias("ALW_PRICE_UNIT_CT"),
    F.col("lnk_IdsDmClmMartClmLnExtr_InABC.UNIT_CT").alias("UNIT_CT"),
    F.col("lnk_IdsDmClmMartClmLnExtr_InABC.SVC_ID").alias("SVC_ID"),
    F.col("lnk_IdsDmClmMartClmLnExtr_InABC.PROC_CD").alias("PROC_CD"),
    F.col("lnk_IdsDmClmMartClmLnExtr_InABC.PROC_CD_DESC").alias("PROC_CD_DESC"),
    F.col("lnk_IdsDmClmMartClmLnExtr_InABC.RVNU_CD").alias("RVNU_CD"),
    F.col("lnk_IdsDmClmMartClmLnExtr_InABC.RVNU_CD_DESC").alias("RVNU_CD_DESC"),
    F.col("lnk_IdsDmClmMartClmLnExtr_InABC.PROV_ID").alias("PROV_ID"),
    F.col("refClmLnPca_lkp.DSALW_EXCD_SK").alias("DSALW_EXCD_SK"),
    F.col("refClmLnPca_lkp.CNSD_AMT").alias("CNSD_AMT"),
    F.col("refClmLnPca_lkp.NONCNSD_AMT").alias("NONCNSD_AMT"),
    F.col("refClmLnPca_lkp.PROV_PD_AMT").alias("PROV_PD_AMT"),
    F.col("refClmLnPca_lkp.SUB_PD_AMT").alias("SUB_PD_AMT"),
    F.col("refClmLnPca_lkp.PD_AMT").alias("PD_AMT"),
    F.col("refClmLnPca_lkp.EXCD_ID").alias("PCA_DSALW_EXCD_ID"),
    F.col("refClmLnPca_lkp.EXCD_SH_TX").alias("PCA_DSALW_EXCD_DESC"),
    F.col("refClmLnPca_lkp.CLM_LN_PCA_LOB_CD").alias("CLM_LN_PCA_LOB_CD"),
    F.col("refClmLnPca_lkp.CLM_LN_PCA_LOB_NM").alias("CLM_LN_PCA_LOB_NM"),
    F.col("refClmLnPca_lkp.CLM_LN_PCA_PRCS_CD").alias("CLM_LN_PCA_PRCS_CD"),
    F.col("refClmLnPca_lkp.CLM_LN_PCA_PRCS_NM").alias("CLM_LN_PCA_PRCS_NM"),
    F.col("refClmLnPca_lkp.DSALW_AMT").alias("CLM_LN_PCA_CNSD_AMT"),
    F.col("refDsalwExcd.EXCD_ID").alias("DSALW_EXCD_ID"),
    F.col("refDsalwExcd.EXCD_SH_TX").alias("DSALW_EXCD_DESC"),
    F.col("refEobExcd.EXCD_ID").alias("EOB_EXCD_ID"),
    F.col("refClmLnRoomPricMeth.TRGT_CD").alias("CLM_LN_ROOM_PRICE_METH_CD"),
    F.col("refClmLnTos.TRGT_CD").alias("CLM_LN_TOS_CD"),
    F.col("refClmLnPos.TRGT_CD").alias("CLM_LN_POS_CD"),
    F.col("refClmLnPos.TRGT_CD_NM").alias("CLM_LN_POS_NM"),
    F.col("refClmLnPreauth.TRGT_CD").alias("CLM_LN_PREAUTH_CD"),
    F.col("refClmLnPreauth.TRGT_CD_NM").alias("CLM_LN_PREAUTH_NM"),
    F.col("refClmLnReferral.TRGT_CD").alias("CLM_LN_RFRL_CD"),
    F.col("refClmLnReferral.TRGT_CD_NM").alias("CLM_LN_RFRL_NM"),
    F.col("refClmLnRoomTyp.TRGT_CD").alias("CLM_LN_ROOM_TYP_CD"),
    F.col("refClmLnRoomTyp.TRGT_CD_NM").alias("CLM_LN_ROOM_TYP_NM"),
    F.col("refClmLnFinlDisp.TRGT_CD").alias("CLM_LN_FINL_DISP_CD"),
    F.col("refClmLnFinlDisp.TRGT_CD_NM").alias("CLM_LN_FINL_DISP_NM"),
    F.col("refPcaPrcs_lkp.TRGT_CD").alias("PCA_PRCS_CD"),
    F.col("refPcaPrcs_lkp.TRGT_CD_NM").alias("PCA_PRCS_CD_NM"),
    F.col("refPcaAmt_lkp.CHRG_AMT").alias("CHRG_AMT"),
    F.col("refPcaAmt_lkp.PAYBL_AMT").alias("PAYBL_AMT"),
    F.col("refClmTypCd_lkp.TRGT_CD").alias("REF_CLM_LN_PCA_LOB_CD"),
    F.col("refClmTypCd_lkp.TRGT_CD_NM").alias("REF_CLM_LN_PCA_LOB_NM"),
    F.col("refClmLnRemitCd_lkp.REMIT_PATN_RESP_AMT").alias("REMIT_PATN_RESP_AMT"),
    F.col("refClmLnRemitCd_lkp.REMIT_PROV_WRT_OFF_AMT").alias("REMIT_PROV_WRT_OFF_AMT"),
    F.col("refClmLnRemitCd_lkp.REMIT_MBR_OTHR_LIAB_AMT").alias("REMIT_MBR_OTHR_LIAB_AMT"),
    F.col("refClmLnRemitCd_lkp.REMIT_NO_RESP_AMT").alias("REMIT_NO_RESP_AMT"),
    F.col("lnk_IdsDmClmMartClmLnExtr_InABC.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK")
)

# --------------------------------------------------------------------------------
# DB2ConnectorPX: db2_DIAG_CD_Extr
# --------------------------------------------------------------------------------
jdbc_url_db2_DIAG_CD_Extr, jdbc_props_db2_DIAG_CD_Extr = get_db_config(ids_secret_name)
extract_query_db2_DIAG_CD_Extr = """
SELECT
    DIAG.DIAG_CD,
    DIAG.DIAG_CD_DESC,
    CLD.CLM_LN_DIAG_ORDNL_CD_SK,
    CLD.CLM_LN_SK  
FROM
 #$IDSOwner#.DIAG_CD           DIAG, 
      #$IDSOwner#.CLM_LN_DIAG       CLD,
      #$IDSOwner#.W_WEBDM_ETL_DRVR  EXTR
 WHERE CLD.DIAG_CD_SK = DIAG.DIAG_CD_SK  
   AND EXTR.CLM_ID = CLD.CLM_ID
"""
df_db2_DIAG_CD_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_DIAG_CD_Extr)
    .options(**jdbc_props_db2_DIAG_CD_Extr)
    .option("query", extract_query_db2_DIAG_CD_Extr)
    .load()
)

# --------------------------------------------------------------------------------
# PxJoin: Join_Clm_Ln (innerjoin on CLM_LN_SK)
# --------------------------------------------------------------------------------
df_Join_Clm_Ln_temp = (
    df_db2_CLM_LN_DRVR.alias("refClm_Ln_lkp")
    .join(
        df_db2_DIAG_CD_Extr.alias("refDiagCd_lkp"),
        F.col("refClm_Ln_lkp.CLM_LN_SK") == F.col("refDiagCd_lkp.CLM_LN_SK"),
        "inner"
    )
)

df_Join_Clm_Ln = df_Join_Clm_Ln_temp.select(
    F.col("refClm_Ln_lkp.CLM_ID").alias("CLM_ID"),
    F.col("refClm_Ln_lkp.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("refClm_Ln_lkp.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("refClm_Ln_lkp.CLM_LN_SK").alias("CLM_LN_SK"),
    F.col("refDiagCd_lkp.DIAG_CD").alias("DIAG_CD"),
    F.col("refDiagCd_lkp.DIAG_CD_DESC").alias("DIAG_CD_DESC"),
    F.col("refDiagCd_lkp.CLM_LN_DIAG_ORDNL_CD_SK").alias("CLM_LN_DIAG_ORDNL_CD_SK")
)

# --------------------------------------------------------------------------------
# PxLookup: Lkp_CdMppng
# Primary link: df_Join_Clm_Ln
# Lookup link: df_DB2_CD_MPPNG_DIAG_ORDNL (inner join)
# join condition: df_Join_Clm_Ln.CLM_LN_DIAG_ORDNL_CD_SK = df_DB2_CD_MPPNG_DIAG_ORDNL.CD_MPPNG_SK
# --------------------------------------------------------------------------------
df_Lkp_CdMppng_temp = (
    df_Join_Clm_Ln.alias("DSLink108")
    .join(
        df_DB2_CD_MPPNG_DIAG_ORDNL.alias("lnk_Cd_Mppng_Ordnl"),
        F.col("DSLink108.CLM_LN_DIAG_ORDNL_CD_SK") == F.col("lnk_Cd_Mppng_Ordnl.CD_MPPNG_SK"),
        "inner"
    )
)
df_Lkp_CdMppng = df_Lkp_CdMppng_temp.select(
    F.col("DSLink108.CLM_ID").alias("CLM_ID"),
    F.col("DSLink108.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("DSLink108.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("DSLink108.DIAG_CD").alias("DIAG_CD"),
    F.col("DSLink108.DIAG_CD_DESC").alias("DIAG_CD_DESC"),
    F.col("DSLink108.CLM_LN_DIAG_ORDNL_CD_SK").alias("CLM_LN_DIAG_ORDNL_CD_SK")
)

# --------------------------------------------------------------------------------
# PxJoin: Join_101 (leftouterjoin on CLM_ID, CLM_LN_SEQ_NO, SRC_SYS_CD_SK)
# Inputs:
#   df_lkp_Codes (alias DataOut)
#   df_Lkp_CdMppng (alias Lnk_Lkp_Ordnl)
# keys: [CLM_ID, CLM_LN_SEQ_NO, SRC_SYS_CD_SK]
# --------------------------------------------------------------------------------
df_Join_101_temp = (
    df_lkp_Codes.alias("DataOut")
    .join(
        df_Lkp_CdMppng.alias("Lnk_Lkp_Ordnl"),
        [
            F.col("DataOut.CLM_ID") == F.col("Lnk_Lkp_Ordnl.CLM_ID"),
            F.col("DataOut.CLM_LN_SEQ_NO") == F.col("Lnk_Lkp_Ordnl.CLM_LN_SEQ_NO"),
            F.col("DataOut.SRC_SYS_CD_SK") == F.col("Lnk_Lkp_Ordnl.SRC_SYS_CD_SK"),
        ],
        "left"
    )
)

df_Join_101 = df_Join_101_temp.select(
    F.col("DataOut.CLM_LN_SK").alias("CLM_LN_SK"),
    F.col("DataOut.CLM_ID").alias("CLM_ID"),
    F.col("DataOut.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("DataOut.CLM_SK").alias("CLM_SK"),
    F.col("DataOut.SVC_PROV_SK").alias("SVC_PROV_SK"),
    F.col("DataOut.CLM_LN_DSALW_EXCD_SK").alias("CLM_LN_DSALW_EXCD_SK"),
    F.col("DataOut.CLM_LN_EOB_EXCD_SK").alias("CLM_LN_EOB_EXCD_SK"),
    F.col("DataOut.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("DataOut.CLM_LN_FINL_DISP_CD_SK").alias("CLM_LN_FINL_DISP_CD_SK"),
    F.col("DataOut.CAP_LN_IN").alias("CAP_LN_IN"),
    F.col("DataOut.CLM_LN_POS_CD_SK").alias("CLM_LN_POS_CD_SK"),
    F.col("DataOut.CLM_LN_PREAUTH_CD_SK").alias("CLM_LN_PREAUTH_CD_SK"),
    F.col("DataOut.SVC_END_DT_SK").alias("SVC_END_DT_SK"),
    F.col("DataOut.SVC_STRT_DT_SK").alias("SVC_STRT_DT_SK"),
    F.col("DataOut.ALW_AMT").alias("ALW_AMT"),
    F.col("DataOut.CLM_LN_RFRL_CD_SK").alias("CLM_LN_RFRL_CD_SK"),
    F.col("DataOut.CLM_LN_CHRG_AMT").alias("CLM_LN_CHRG_AMT"),
    F.col("DataOut.CLM_LN_ROOM_PRICE_METH_CD_SK").alias("CLM_LN_ROOM_PRICE_METH_CD_SK"),
    F.col("DataOut.COINS_AMT").alias("COINS_AMT"),
    F.col("DataOut.CLM_LN_ROOM_TYP_CD_SK").alias("CLM_LN_ROOM_TYP_CD_SK"),
    F.col("DataOut.CNSD_CHRG_AMT").alias("CNSD_CHRG_AMT"),
    F.col("DataOut.CLM_LN_TOS_CD_SK").alias("CLM_LN_TOS_CD_SK"),
    F.col("DataOut.COPAY_AMT").alias("COPAY_AMT"),
    F.col("DataOut.DEDCT_AMT").alias("DEDCT_AMT"),
    F.col("DataOut.DSALW_AMT").alias("DSALW_AMT"),
    F.col("DataOut.CLM_LN_PAYBL_AMT").alias("CLM_LN_PAYBL_AMT"),
    F.col("DataOut.PAYBL_TO_PROV_AMT").alias("PAYBL_TO_PROV_AMT"),
    F.col("DataOut.ALW_PRICE_UNIT_CT").alias("ALW_PRICE_UNIT_CT"),
    F.col("DataOut.UNIT_CT").alias("UNIT_CT"),
    F.col("DataOut.SVC_ID").alias("SVC_ID"),
    F.col("DataOut.PROC_CD").alias("PROC_CD"),
    F.col("DataOut.PROC_CD_DESC").alias("PROC_CD_DESC"),
    F.col("DataOut.RVNU_CD").alias("RVNU_CD"),
    F.col("DataOut.RVNU_CD_DESC").alias("RVNU_CD_DESC"),
    F.col("DataOut.PROV_ID").alias("PROV_ID"),
    F.col("DataOut.DSALW_EXCD_SK").alias("DSALW_EXCD_SK"),
    F.col("DataOut.CNSD_AMT").alias("CNSD_AMT"),
    F.col("DataOut.NONCNSD_AMT").alias("NONCNSD_AMT"),
    F.col("DataOut.PROV_PD_AMT").alias("PROV_PD_AMT"),
    F.col("DataOut.SUB_PD_AMT").alias("SUB_PD_AMT"),
    F.col("DataOut.PD_AMT").alias("PD_AMT"),
    F.col("DataOut.PCA_DSALW_EXCD_ID").alias("PCA_DSALW_EXCD_ID"),
    F.col("DataOut.PCA_DSALW_EXCD_DESC").alias("PCA_DSALW_EXCD_DESC"),
    F.col("DataOut.CLM_LN_PCA_LOB_CD").alias("CLM_LN_PCA_LOB_CD"),
    F.col("DataOut.CLM_LN_PCA_LOB_NM").alias("CLM_LN_PCA_LOB_NM"),
    F.col("DataOut.CLM_LN_PCA_PRCS_CD").alias("CLM_LN_PCA_PRCS_CD"),
    F.col("DataOut.CLM_LN_PCA_PRCS_NM").alias("CLM_LN_PCA_PRCS_NM"),
    F.col("DataOut.CLM_LN_PCA_CNSD_AMT").alias("CLM_LN_PCA_CNSD_AMT"),
    F.col("Lnk_Lkp_Ordnl.DIAG_CD").alias("DIAG_CD"),
    F.col("Lnk_Lkp_Ordnl.DIAG_CD_DESC").alias("DIAG_CD_DESC"),
    F.col("DataOut.DSALW_EXCD_ID").alias("DSALW_EXCD_ID"),
    F.col("DataOut.DSALW_EXCD_DESC").alias("DSALW_EXCD_DESC"),
    F.col("DataOut.EOB_EXCD_ID").alias("EOB_EXCD_ID"),
    F.col("DataOut.CLM_LN_ROOM_PRICE_METH_CD").alias("CLM_LN_ROOM_PRICE_METH_CD"),
    F.col("DataOut.CLM_LN_TOS_CD").alias("CLM_LN_TOS_CD"),
    F.col("DataOut.CLM_LN_POS_CD").alias("CLM_LN_POS_CD"),
    F.col("DataOut.CLM_LN_POS_NM").alias("CLM_LN_POS_NM"),
    F.col("DataOut.CLM_LN_PREAUTH_CD").alias("CLM_LN_PREAUTH_CD"),
    F.col("DataOut.CLM_LN_PREAUTH_NM").alias("CLM_LN_PREAUTH_NM"),
    F.col("DataOut.CLM_LN_RFRL_CD").alias("CLM_LN_RFRL_CD"),
    F.col("DataOut.CLM_LN_RFRL_NM").alias("CLM_LN_RFRL_NM"),
    F.col("DataOut.CLM_LN_ROOM_TYP_CD").alias("CLM_LN_ROOM_TYP_CD"),
    F.col("DataOut.CLM_LN_ROOM_TYP_NM").alias("CLM_LN_ROOM_TYP_NM"),
    F.col("DataOut.CLM_LN_FINL_DISP_CD").alias("CLM_LN_FINL_DISP_CD"),
    F.col("DataOut.CLM_LN_FINL_DISP_NM").alias("CLM_LN_FINL_DISP_NM"),
    F.col("DataOut.PCA_PRCS_CD").alias("PCA_PRCS_CD"),
    F.col("DataOut.PCA_PRCS_CD_NM").alias("PCA_PRCS_CD_NM"),
    F.col("DataOut.CHRG_AMT").alias("CHRG_AMT"),
    F.col("DataOut.PAYBL_AMT").alias("PAYBL_AMT"),
    F.col("DataOut.REF_CLM_LN_PCA_LOB_CD").alias("REF_CLM_LN_PCA_LOB_CD"),
    F.col("DataOut.REF_CLM_LN_PCA_LOB_NM").alias("REF_CLM_LN_PCA_LOB_NM"),
    F.col("DataOut.REMIT_PATN_RESP_AMT").alias("REMIT_PATN_RESP_AMT"),
    F.col("DataOut.REMIT_PROV_WRT_OFF_AMT").alias("REMIT_PROV_WRT_OFF_AMT"),
    F.col("DataOut.REMIT_MBR_OTHR_LIAB_AMT").alias("REMIT_MBR_OTHR_LIAB_AMT"),
    F.col("DataOut.REMIT_NO_RESP_AMT").alias("REMIT_NO_RESP_AMT"),
    F.col("DataOut.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("Lnk_Lkp_Ordnl.CLM_LN_DIAG_ORDNL_CD_SK").alias("CLM_LN_DIAG_ORDNL_CD_SK")
)

# --------------------------------------------------------------------------------
# CTransformerStage: xfrm_BusinessLogic
# Input: df_Join_101
# Output columns specified with the given expressions
# --------------------------------------------------------------------------------
df_xfrm_BusinessLogic_temp = df_Join_101.select(
    F.when(F.trim(F.col("SRC_SYS_CD")) == "", F.lit(" ")).otherwise(F.col("SRC_SYS_CD")).alias("SRC_SYS_CD"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("RVNU_CD").alias("CLM_LN_RVNU_CD"),
    F.when(F.trim(F.col("CLM_LN_ROOM_PRICE_METH_CD")) == "", F.lit(" ")).otherwise(F.col("CLM_LN_ROOM_PRICE_METH_CD")) \
     .alias("CLM_LN_ROOM_PRICE_METH_CD"),
    F.when(F.trim(F.col("CLM_LN_TOS_CD")) == "", F.lit(" ")).otherwise(F.col("CLM_LN_TOS_CD")) \
     .alias("CLM_LN_TOS_CD"),
    F.col("PROC_CD").alias("PROC_CD"),
    F.col("CAP_LN_IN").alias("CLM_LN_CAP_LN_IN"),
    F.when(
        (F.trim(F.col("SVC_STRT_DT_SK")) == "NA") |
        (F.trim(F.col("SVC_STRT_DT_SK")) == "UNK") |
        (F.trim(F.col("SVC_STRT_DT_SK")) == ""),
        F.lit(None)
    ).otherwise(
        F.to_timestamp(
            F.concat(F.col("SVC_STRT_DT_SK"), F.lit(" 00:00:00.000")),
            "yyyy-MM-dd HH:mm:ss.SSS"
        )
    ).alias("CLM_LN_SVC_STRT_DT"),
    F.when(
        (F.trim(F.col("SVC_END_DT_SK")) == "NA") |
        (F.trim(F.col("SVC_END_DT_SK")) == "UNK") |
        (F.trim(F.col("SVC_END_DT_SK")) == ""),
        F.lit(None)
    ).otherwise(
        F.to_timestamp(
            F.concat(F.col("SVC_END_DT_SK"), F.lit(" 00:00:00.000")),
            "yyyy-MM-dd HH:mm:ss"
        )
    ).alias("CLM_LN_SVC_END_DT"),
    F.col("CLM_LN_CHRG_AMT").alias("CLM_LN_CHRG_AMT"),
    F.col("PAYBL_TO_PROV_AMT").alias("CLM_LN_PAYBL_TO_PROV_AMT"),
    F.col("UNIT_CT").alias("CLM_LN_UNIT_CT"),
    F.col("SVC_ID").alias("CLM_LN_SVC_ID"),
    F.when(F.trim(F.col("PROC_CD_DESC")) == "", F.lit("")).otherwise(F.col("PROC_CD_DESC")) \
     .alias("PROC_CD_DESC"),
    F.when(F.trim(F.col("CLM_LN_POS_CD")) == "", F.lit("")).otherwise(F.col("CLM_LN_POS_CD")) \
     .alias("CLM_LN_POS_CD"),
    F.when(F.trim(F.col("CLM_LN_POS_NM")) == "", F.lit(" ")).otherwise(F.col("CLM_LN_POS_NM")) \
     .alias("CLM_LN_POS_NM"),
    F.when(F.trim(F.col("CLM_LN_PREAUTH_CD")) == "", F.lit("")).otherwise(F.col("CLM_LN_PREAUTH_CD")) \
     .alias("CLM_LN_PREAUTH_CD"),
    F.when(F.trim(F.col("CLM_LN_PREAUTH_NM")) == "", F.lit(" ")).otherwise(F.col("CLM_LN_PREAUTH_NM")) \
     .alias("CLM_LN_PREAUTH_NM"),
    F.when(F.trim(F.col("CLM_LN_RFRL_CD")) == "", F.lit("")).otherwise(F.col("CLM_LN_RFRL_CD")) \
     .alias("CLM_LN_RFRL_CD"),
    F.when(F.trim(F.col("CLM_LN_RFRL_NM")) == "", F.lit("")).otherwise(F.col("CLM_LN_RFRL_NM")) \
     .alias("CLM_LN_RFRL_NM"),
    F.when(F.col("RVNU_CD_DESC").isNull(), F.lit("")).otherwise(F.col("RVNU_CD_DESC")) \
     .alias("CLM_LN_RVNU_NM"),
    F.when(F.trim(F.col("CLM_LN_ROOM_TYP_CD")) == "", F.lit("")).otherwise(F.col("CLM_LN_ROOM_TYP_CD")) \
     .alias("CLM_LN_ROOM_TYP_CD"),
    F.when(F.trim(F.col("CLM_LN_ROOM_TYP_NM")) == "", F.lit(" ")).otherwise(F.col("CLM_LN_ROOM_TYP_NM")) \
     .alias("CLM_LN_ROOM_TYP_NM"),
    F.col("ALW_AMT").alias("CLM_LN_ALW_AMT"),
    F.col("COINS_AMT").alias("CLM_LN_COINS_AMT"),
    F.col("CNSD_CHRG_AMT").alias("CLM_LN_CNSD_CHRG_AMT"),
    F.col("COPAY_AMT").alias("CLM_LN_COPAY_AMT"),
    F.col("DEDCT_AMT").alias("CLM_LN_DEDCT_AMT"),
    F.col("DSALW_AMT").alias("CLM_LN_DSALW_AMT"),
    F.col("CLM_LN_PAYBL_AMT").alias("CLM_LN_PAYBL_AMT"),
    F.col("ALW_PRICE_UNIT_CT").alias("CLM_LN_ALW_PRICE_UNIT_CT"),
    F.when(
        (F.trim(F.col("DSALW_EXCD_ID")) == "") |
        (F.trim(F.col("DSALW_EXCD_ID")) == "NA") |
        (F.trim(F.col("DSALW_EXCD_ID")) == "UNK"),
        F.lit(" ")
    ).otherwise(F.col("DSALW_EXCD_ID")).alias("DSALW_EXCD_ID"),
    F.when(
        F.col("DSALW_EXCD_DESC").isNull(),
        F.lit(" ")
    ).otherwise(
        F.when(
            (F.trim(F.col("DSALW_EXCD_DESC")) == "NA") |
            (F.trim(F.col("DSALW_EXCD_DESC")) == "UNK"),
            F.lit(" ")
        ).otherwise(F.col("DSALW_EXCD_DESC"))
    ).alias("DSALW_EXCD_DESC"),
    F.when(
        (F.trim(F.col("EOB_EXCD_ID")) == "") |
        (F.trim(F.col("EOB_EXCD_ID")) == "NA") |
        (F.trim(F.col("EOB_EXCD_ID")) == "UNK"),
        F.lit(" ")
    ).otherwise(F.col("EOB_EXCD_ID")).alias("EOB_EXCD_ID"),
    F.when(F.trim(F.col("DIAG_CD")) == "", F.lit(" ")).otherwise(F.col("DIAG_CD")) \
     .alias("PRI_DIAG_CD"),
    F.when(F.trim(F.col("DIAG_CD_DESC")) == "", F.lit(" ")).otherwise(F.col("DIAG_CD_DESC")) \
     .alias("PRI_DIAG_DESC"),
    F.col("PROV_ID").alias("SVC_PROV_ID"),
    F.lit("CurrRunCycle").alias("LAST_UPDT_RUN_CYC_NO"),
    F.when(
        F.col("CLM_LN_PCA_LOB_CD").isNull(),
        F.when(F.trim(F.col("REF_CLM_LN_PCA_LOB_CD")) == "", F.lit(" ")).otherwise(F.col("REF_CLM_LN_PCA_LOB_CD"))
    ).otherwise(F.col("CLM_LN_PCA_LOB_CD")).alias("CLM_LN_PCA_LOB_CD"),
    F.when(
        F.col("CLM_LN_PCA_LOB_NM").isNull(),
        F.when(F.trim(F.col("REF_CLM_LN_PCA_LOB_NM")) == "", F.lit(" ")).otherwise(F.col("REF_CLM_LN_PCA_LOB_NM"))
    ).otherwise(F.col("CLM_LN_PCA_LOB_NM")).alias("CLM_LN_PCA_LOB_NM"),
    F.when(
        F.col("CLM_LN_PCA_PRCS_CD").isNull(),
        F.when(F.trim(F.col("PCA_PRCS_CD")) == "", F.lit(" ")).otherwise(F.col("PCA_PRCS_CD"))
    ).otherwise(F.col("CLM_LN_PCA_PRCS_CD")).alias("CLM_LN_PCA_PRCS_CD"),
    F.when(
        F.col("CLM_LN_PCA_PRCS_NM").isNull(),
        F.when(F.trim(F.col("PCA_PRCS_CD_NM")) == "", F.lit(" ")).otherwise(F.col("PCA_PRCS_CD_NM"))
    ).otherwise(F.col("CLM_LN_PCA_PRCS_NM")).alias("CLM_LN_PCA_PRCS_NM"),
    F.when(
        F.col("CNSD_AMT") == 0,
        F.when(F.col("CHRG_AMT") == 0, F.lit(0)).otherwise(F.col("CHRG_AMT"))
    ).otherwise(F.col("CNSD_AMT")).alias("CLM_LN_PCA_CNSD_AMT"),
    F.col("CLM_LN_PCA_CNSD_AMT").alias("CLM_LN_PCA_DSALW_AMT"),
    F.col("NONCNSD_AMT").alias("CLM_LN_PCA_NONCNSD_AMT"),
    F.when(
        F.col("PD_AMT") == 0,
        F.when(F.col("PAYBL_AMT") == 0, F.lit(0)).otherwise(F.col("PAYBL_AMT"))
    ).otherwise(F.col("PD_AMT")).alias("CLM_LN_PCA_PD_AMT"),
    F.col("PROV_PD_AMT").alias("CLM_LN_PCA_PROV_PD_AMT"),
    F.col("SUB_PD_AMT").alias("CLM_LN_PCA_SUB_PD_AMT"),
    F.when(
        (F.trim(F.col("PCA_DSALW_EXCD_ID")) == "") |
        (F.trim(F.col("PCA_DSALW_EXCD_ID")) == "NA") |
        (F.trim(F.col("PCA_DSALW_EXCD_ID")) == "UNK"),
        F.lit(" ")
    ).otherwise(F.col("PCA_DSALW_EXCD_ID")).alias("PCA_DSALW_EXCD_ID"),
    F.when(
        (
          F.trim(
            F.when(
              F.col("PCA_DSALW_EXCD_DESC").isNotNull(), 
              F.col("PCA_DSALW_EXCD_DESC")
            ).otherwise(F.lit("NA"))
          ) == "NA"
        ) | (
          F.trim(
            F.when(
              F.col("PCA_DSALW_EXCD_DESC").isNotNull(), 
              F.col("PCA_DSALW_EXCD_DESC")
            ).otherwise(F.lit("UNK"))
          ) == "UNK"
        ),
        F.lit(" ")
    ).otherwise(F.col("PCA_DSALW_EXCD_DESC")).alias("PCA_DSALW_EXCD_DESC"),
    F.col("REMIT_PATN_RESP_AMT").alias("CLM_LN_REMIT_PATN_RESP_AMT"),
    F.col("REMIT_PROV_WRT_OFF_AMT").alias("CLM_LN_REMIT_PROV_WRT_OFF_AMT"),
    F.col("REMIT_MBR_OTHR_LIAB_AMT").alias("CLM_LN_REMIT_MBR_OTHR_LIAB_AMT"),
    F.col("REMIT_NO_RESP_AMT").alias("CLM_LN_REMIT_NO_RESP_AMT"),
    F.when(F.trim(F.col("CLM_LN_FINL_DISP_CD")) == "", F.lit("")).otherwise(F.col("CLM_LN_FINL_DISP_CD")) \
     .alias("CLM_LN_FINL_DISP_CD"),
    F.when(F.trim(F.col("CLM_LN_FINL_DISP_NM")) == "", F.lit("")).otherwise(F.col("CLM_LN_FINL_DISP_NM")) \
     .alias("CLM_LN_FINL_DISP_NM")
)

# Replace the placeholder literal "CurrRunCycle" with the actual parameter value:
df_xfrm_BusinessLogic = df_xfrm_BusinessLogic_temp.withColumn(
    "LAST_UPDT_RUN_CYC_NO",
    F.lit(CurrRunCycle)
)

# --------------------------------------------------------------------------------
# Now apply rpad for final string columns declared as char/varchar with lengths
# (from the metadata observed in the stages).
# 1) CLM_LN_CAP_LN_IN => char(1)
# 2) DSALW_EXCD_ID => char(4)
# 3) EOB_EXCD_ID => char(4)
# 4) PCA_DSALW_EXCD_ID => char(4)
# Note: SVC_STRT_DT / SVC_END_DT are timestamps now, so no rpad needed.
# --------------------------------------------------------------------------------
df_final = (
    df_xfrm_BusinessLogic
    .withColumn("CLM_LN_CAP_LN_IN", F.rpad(F.col("CLM_LN_CAP_LN_IN"), 1, " "))
    .withColumn("DSALW_EXCD_ID", F.rpad(F.col("DSALW_EXCD_ID"), 4, " "))
    .withColumn("EOB_EXCD_ID", F.rpad(F.col("EOB_EXCD_ID"), 4, " "))
    .withColumn("PCA_DSALW_EXCD_ID", F.rpad(F.col("PCA_DSALW_EXCD_ID"), 4, " "))
)

# --------------------------------------------------------------------------------
# PxSequentialFile: seq_CLM_DM_CLM_LN_csv_load
# Write to CLM_DM_CLM_LN.dat (delimiter=",", quote="^", header=False, nullValue=None, mode="overwrite")
# Path does not contain "landing" or "external", so use f"{adls_path}/load/CLM_DM_CLM_LN.dat"
# --------------------------------------------------------------------------------
output_file_seq_CLM_DM_CLM_LN_csv_load = f"{adls_path}/load/CLM_DM_CLM_LN.dat"

write_files(
    df_final,
    output_file_seq_CLM_DM_CLM_LN_csv_load,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)