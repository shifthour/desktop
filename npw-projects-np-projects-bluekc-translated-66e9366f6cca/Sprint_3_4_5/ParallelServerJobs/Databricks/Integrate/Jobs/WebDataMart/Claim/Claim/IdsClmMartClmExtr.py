# Databricks notebook source
# MAGIC %md
# MAGIC """
# MAGIC Â© Copyright 2007 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC Called by:
# MAGIC                     IdsClmMartExtrLoadSeq
# MAGIC 
# MAGIC Processing:
# MAGIC                     hf_etrnl_cd_mppng  created in the daily code mapping process - DO NOT CLEAR THIS HASH FILE
# MAGIC 
# MAGIC Control Job Rerun Information: 
# MAGIC              Previous Run Successful:    Rerun
# MAGIC              Previous Run Aborted:         Restart
# MAGIC 
# MAGIC Modifications:                        
# MAGIC                                                             Project/                                                                                                                                                                                           Code                     Date
# MAGIC Developer            Date                        Altiris #                      Change Description                                                                                                                                       Reviewer               Reviewed
# MAGIC -------------------------   --------------------            -------------------              -------------------------------------------------------------------------------------------------------------------------                                            ---------------------------   -------------------   
# MAGIC BJ Luce               03/22/2004                                             Originally Programmed
# MAGIC                             04/05/2005                                             use host_in = 'Y' to identify external membership. pull member and sub data from 
# MAGIC                                                                                                 external membership when host_in = Y in clm
# MAGIC                             04/07/2005                                             change check to 'ACPTD' instead of PAID for search date
# MAGIC                             04/27/2005                                             add 'UNK' and 'NA' to stage variable logic for AlphaPfxSSN
# MAGIC                             05/04/2005                                             change logic to populate actl_pd_amt from clm_remit_hist instead of clm
# MAGIC                                                                                             populate search date with status date
# MAGIC                             06/02/2005                                             add new fields for rx deductibles
# MAGIC                                                                                             use host_in = 'Y' to identify external membership. pull member and sub data from
# MAGIC                                                                                                 external membership when host_in = Y in init clm
# MAGIC                             08/01/2005                                             use PROV_DEA name for prescribing name if common pract unknown
# MAGIC                             08/24/2005                                             add CLM_TRSNMSN_STTUS_CD
# MAGIC                             08/29/2005                                             change SQL parameters to ClmMart for sequencer
# MAGIC BJ Luce               09/29/2005                                             Use ALPHA_PFX table to get ALPHA_PFX_CD instead of CD_MPPNG
# MAGIC BJ Luce               10/11/2005                                             Remove clear of hash file hf_clmmart_clndr
# MAGIC BJ Luce               11/04/2005                                             create hash file for ALPHA_PFX lookup instead of joining with CLM
# MAGIC Brent Leland        03/29/2006                                             Changed output of CLM_INIT data from ODBC update to sequential file.
# MAGIC                                                                                             Changed input parameters to environment parameters
# MAGIC                                                                                             Changed name of driver table.
# MAGIC                                                                                             Added run cycle parameter for new table field LAST_UPDT_RUN_CYC_NO
# MAGIC Oliver Nielsen       04/17/2006                                            Added 41 new columns for KCREE and Care Advance projects, and 
# MAGIC                                                                                                added 7 new hashed files
# MAGIC Oliver Nielsen       06/12/2006                                            Change W_CLM_INT to REPLACE when building.
# MAGIC Brent Leland         07/19/2006                                            Added +0 option to DB joins with CD_MPPNG table.
# MAGIC Brent Leland         07/24/2006                                            Added check for UNK on FCLTY_CLM_ADMS_DT column.
# MAGIC Parikshith Chada  10/13/2006                                            Modified six existing fields and Added 22 new fields to the table
# MAGIC Ralph Tucker       11/07/2006                                            Chaged previous logic from 10/13 and added new fields. 
# MAGIC                                                                                                 i.e.  PCA_TYP_CD, PCA_TYP_NM
# MAGIC Ralph Tucker       01/30/2007                                            Changed TOT_PD_AMT and TOT_CNSD_AMT to look for values not = to 0 on
# MAGIC                                                                                                CLM_PCA_TOT_PD_AMT and  CLM_PCA_TOT_CNSD_AMT.
# MAGIC Steph Goddard    03/28/2007                                            changed CLM_PCA_TOT_PD_AMT to look for pca_typ_cd of RUNOUT, PCA, 
# MAGIC                                                                                               or EMPWBNF before moving PAYBL_AMT
# MAGIC Hugh Sisson        05/22/2007            CDHP Drug             Removed reference to PCA in CLM_PCA_TOT_CNSD_AMT and CLM_PCA_TOT_PD_AMT
# MAGIC Hugh Sisson        07/16/2007            CDHP Drug             Changed claim_pull SQL to get SRC_SYS_CD_SK for the related base claim
# MAGIC Steph Goddard    08/28/07                ProdSupt                 Added code to check for servicing provider if claim status is not A02, A08, or A09                                                  Brent Leland              08-29-2007
# MAGIC                                                                                            Also changed direct lookups to hash file lookups
# MAGIC Bhoomi D             09/15/2007                                           Added balancing snapshot for claim                                                                                                                         Steph Goddard          09-28-2007
# MAGIC SAndrew             10/25/2007            DRG project               Changed ODBC IDS FCLTY_CLM, the source of the DataMart Claim table's SUBMT_DRG_TX to a direct map 
# MAGIC Brent Leland        04/08/2008             IAD Prod. Sup.            Changed Mbr, SubAddr and SubName SQL to use driver table.  
# MAGIC                                                                                                 Subscriber SQL performace required filtering on the target                                                                                  Steph Goddard          04-04-2008
# MAGIC                                                                                                 code in the transform.
# MAGIC Ralph Tucker       09/30/2008                                               Added two new fields (BCBS_PLN_CD, SCCF_NO)                                                                devlIDScurr          Steph Goddard          12/04/2008
# MAGIC 
# MAGIC SAndrew            2009-06-23      #3833 Remit Alt Chrg      Added two new fields to end of CLM_DM_CLM 
# MAGIC                                                                                           CLM_REMIT_HIST_ALT_CHRG_IN  and  ALT_CHRG_PROV_WRTOFF_AMT                         devlIDSnew        Steph Goddard          07/01/2009
# MAGIC                                                                                           Added new CLM_REMIT_HIST fields as part of extract in remit_hist which is loaded to hf_clmmart_clm_remit_hist
# MAGIC 
# MAGIC Shanmugam A \(9) 2017-03-02         5321                     Link pdProvAddr SQL  column will be aliased from NG3.TRGT_CD  to  PROV_ADDR_ST_CD\(9)IntegrateDev2       Jag Yelavarthi          2017-03-07
# MAGIC 
# MAGIC Madhavan B             2017-07-10    5788 BHI Updates    Updated Field PROV_ID length from Char(12) to Varchar(20) in CLM_PROV table                        IntegrateDev1
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, substring, rpad
from pyspark.sql.types import *
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------


# ========== Script 1 ==========
# Parameters
CommitPoint = get_widget_value("CommitPoint", "20000")
RunCycle = get_widget_value("RunCycle", "")
ClmMartOwner = get_widget_value("ClmMartOwner", "")
IDSOwner = get_widget_value("IDSOwner", "")
ids_secret_name = get_widget_value("ids_secret_name", "")

# Get JDBC config
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

#---------------------------------------------------------------------------------------------------
# Read from IDS stage (DB2Connector) - multiple output links
#---------------------------------------------------------------------------------------------------

claim_pull_query = """SELECT 
              CLM.CLM_ID,  CLM.CLM_SK,  CLM.ALPHA_PFX_SK,  CLM.CLM_FINL_DISP_CD_SK,  CLM.CLM_STTUS_CD_SK, 
              CLM.CLM_TYP_CD_SK,  CLM.PD_DT_SK,  CLM.SVC_STRT_DT_SK,  CLM.SVC_END_DT_SK,  CLM.STTUS_DT_SK, 
              CLM.CNSD_CHRG_AMT,   CLM.CHRG_AMT,  CLM.PAYBL_AMT,   CLM.SUB_ID,  MBR_UNIQ_KEY,  BRTH_DT_SK,
              FIRST_NM,  MIDINIT,  LAST_NM,  CLM.CLM_CAP_CD_SK,  CLM.CLM_INTER_PLN_PGM_CD_SK,  CLM.CLM_PAYE_CD_SK, 
              CLM.CLM_SUBTYP_CD_SK,  CLM.ACTL_PD_AMT,  CLM.ALW_AMT,  CLM.DSALW_AMT,  CLM.COINS_AMT,  CLM.COPAY_AMT,  CLM.DEDCT_AMT, 
              CLM.ADJ_FROM_CLM_ID,  CLM.ADJ_TO_CLM_ID,   CLM.PATN_ACCT_NO,   CLM.RFRNG_PROV_TX,   CLM.SRC_SYS_CD_SK, 
              CLM.RCVD_DT_SK,   GRP_ID,  GRP_UNIQ_KEY,  GRP_NM,   CLM.SUB_SK,  SUB_UNIQ_KEY,    CLM.HOST_IN, 
              CLM.CLM_NTWK_STTUS_CD_SK,   CLM.MBR_SK,   CLM.PRCS_DT_SK,   CLM.CLS_SK,   CLM.CLS_PLN_SK, 
              CLM.MBR_SFX_NO,   CLM.NTWK_SK,   CLM.PROD_SK,   CLM.PROV_AGMNT_ID,   CLM.CLM_INPT_SRC_CD_SK, 
              CLM.PCA_TYP_CD_SK,   CLM.REL_PCA_CLM_SK,   CLM.REL_BASE_CLM_SK,  CLM2.SRC_SYS_CD_SK, CLM.CLM_SUB_BCBS_PLN_CD_SK
FROM 
              #$IDSOwner#.W_WEBDM_ETL_DRVR R, 
              #$IDSOwner#.CLM CLM,  
              #$IDSOwner#.MBR MBR, 
              #$IDSOwner#.GRP GRP,  
              #$IDSOwner#.SUB SUB,
              #$IDSOwner#.CLM CLM2
WHERE  
              R.SRC_SYS_CD_SK = CLM.SRC_SYS_CD_SK  
              AND R.CLM_ID = CLM.CLM_ID
              AND CLM.MBR_SK = MBR.MBR_SK
              AND CLM.GRP_SK = GRP.GRP_SK
              AND CLM.SUB_SK = SUB.SUB_SK
              AND CLM.REL_BASE_CLM_SK = CLM2.CLM_SK
"""

df_claim_pull = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", claim_pull_query)
    .load()
)

svcProv_query = """SELECT V.CLM_SK,PROV.PROV_NM,PROV.PROV_ID,PRCT.CMN_PRCT_SK, PRCT.CMN_PRCT_ID,
PROV.REL_GRP_PROV_SK,PROV.REL_IPA_PROV_SK,GRPPROV.PROV_ID AS GRP_PROV_ID,IPAPROV.PROV_ID AS IPA_PROV_ID, NG1.TRGT_CD
FROM #$IDSOwner#.W_WEBDM_ETL_DRVR R,
     #$IDSOwner#.CLM_PROV V,
     #$IDSOwner#.CD_MPPNG NG1, 
     #$IDSOwner#.PROV PROV,
     #$IDSOwner#.CMN_PRCT PRCT, 
     #$IDSOwner#.PROV GRPPROV, 
     #$IDSOwner#.PROV IPAPROV 
WHERE R.SRC_SYS_CD_SK = V.SRC_SYS_CD_SK
  AND R.CLM_ID = V.CLM_ID
  AND V.CLM_PROV_ROLE_TYP_CD_SK = NG1.CD_MPPNG_SK
  AND V.PROV_SK = PROV.PROV_SK
  AND PROV.CMN_PRCT_SK = PRCT.CMN_PRCT_SK
  AND PROV.REL_GRP_PROV_SK = GRPPROV.PROV_SK
  AND PROV.REL_IPA_PROV_SK = IPAPROV.PROV_SK
"""

df_svcProv = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", svcProv_query)
    .load()
)

pdProvAddr_query = """SELECT V.CLM_SK, V.PROV_ID, PROV.PROV_NM, ADDR_LN_1, ADDR_LN_2, ADDR_LN_3, CITY_NM, POSTAL_CD, NG3.TRGT_CD AS PROV_ADDR_ST_CD, NG1.TRGT_CD
FROM #$IDSOwner#.W_WEBDM_ETL_DRVR R,
     #$IDSOwner#.CLM_PROV V,
     #$IDSOwner#.CD_MPPNG NG1,
     #$IDSOwner#.PROV PROV, 
     #$IDSOwner#.PROV_ADDR ADDR,
     #$IDSOwner#.PROV_LOC LOC,
     #$IDSOwner#.CD_MPPNG NG3 
WHERE R.SRC_SYS_CD_SK = V.SRC_SYS_CD_SK 
  AND R.CLM_ID = V.CLM_ID
  AND V.CLM_PROV_ROLE_TYP_CD_SK = NG1.CD_MPPNG_SK
  AND V.PROV_SK = PROV.PROV_SK
  AND PROV.PROV_SK = LOC.PROV_SK
  AND LOC.REMIT_ADDR_IN = 'Y'
  AND LOC.PROV_ADDR_SK = ADDR.PROV_ADDR_SK
  AND ADDR.PROV_ADDR_ST_CD_SK = NG3.CD_MPPNG_SK
"""

df_pdProvAddr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", pdProvAddr_query)
    .load()
)

pdProv_query = """SELECT V.CLM_SK, V.PROV_ID, PROV.PROV_NM, PROV.PROV_SK, PROV.REL_GRP_PROV_SK, NG1.TRGT_CD
FROM #$IDSOwner#.W_WEBDM_ETL_DRVR R,
     #$IDSOwner#.CLM_PROV V,
     #$IDSOwner#.CD_MPPNG NG1,
     #$IDSOwner#.PROV PROV 
WHERE R.SRC_SYS_CD_SK = V.SRC_SYS_CD_SK
  AND R.CLM_ID = V.CLM_ID
  AND V.CLM_PROV_ROLE_TYP_CD_SK = NG1.CD_MPPNG_SK
  AND V.PROV_SK = PROV.PROV_SK
"""

df_pdProv = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", pdProv_query)
    .load()
)

billSvcGrp_query = """SELECT CLM.CLM_SK, PROV_BILL_SVC_ID, NG1.TRGT_CD
FROM #$IDSOwner#.W_WEBDM_ETL_DRVR R,
     #$IDSOwner#.CLM_PROV V,
     #$IDSOwner#.CD_MPPNG NG1,
     #$IDSOwner#.PROV PROV,
     #$IDSOwner#.PROV_BILL_SVC_ASSOC ASSOC,
     #$IDSOwner#.CLM CLM 
WHERE R.SRC_SYS_CD_SK = V.SRC_SYS_CD_SK
  AND R.CLM_ID = V.CLM_ID
  AND V.CLM_PROV_ROLE_TYP_CD_SK = NG1.CD_MPPNG_SK
  AND V.PROV_SK = PROV.PROV_SK
  AND PROV.REL_GRP_PROV_SK = ASSOC.PROV_SK
  AND CLM.SVC_STRT_DT_SK between ASSOC.EFF_DT_SK and ASSOC.END_DT_SK
  AND CLM.SRC_SYS_CD_SK = R.SRC_SYS_CD_SK
  AND CLM.CLM_ID = R.CLM_ID
"""

df_billSvcGrp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", billSvcGrp_query)
    .load()
)

base_clm_query = """SELECT CLM.REL_BASE_CLM_SK, CLM2.CLM_ID
FROM #$IDSOwner#.W_WEBDM_ETL_DRVR DRVR,
     #$IDSOwner#.CLM CLM,
     #$IDSOwner#.CLM CLM2
WHERE DRVR.SRC_SYS_CD_SK  = CLM.SRC_SYS_CD_SK
  AND DRVR.CLM_ID = CLM.CLM_ID
  AND CLM.REL_BASE_CLM_SK = CLM2.CLM_SK
"""

df_base_clm = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", base_clm_query)
    .load()
)

pca_clm_lookup_query = """SELECT CLM.REL_PCA_CLM_SK, CLM2.CLM_ID
FROM #$IDSOwner#.W_WEBDM_ETL_DRVR DRVR,
     #$IDSOwner#.CLM CLM,
     #$IDSOwner#.CLM CLM2
WHERE DRVR.SRC_SYS_CD_SK  = CLM.SRC_SYS_CD_SK
  AND DRVR.CLM_ID = CLM.CLM_ID
  AND CLM.REL_PCA_CLM_SK = CLM2.CLM_SK
"""

df_pca_clm_lookup = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", pca_clm_lookup_query)
    .load()
)

subName_query = """SELECT MBR.SUB_SK, 
              MBR.FIRST_NM,
              MBR.MIDINIT,
              MBR.LAST_NM, 
              CDMP.TRGT_CD
FROM 
      #$IDSOwner#.W_WEBDM_ETL_DRVR R, 
      #$IDSOwner#.CLM CLM, 
      #$IDSOwner#.MBR MBR, 
      #$IDSOwner#.CD_MPPNG CDMP 
WHERE R.SRC_SYS_CD_SK = CLM.SRC_SYS_CD_SK
  AND R.CLM_ID = CLM.CLM_ID
  AND CLM.MBR_SK = MBR.MBR_SK
  AND MBR.MBR_RELSHP_CD_SK = CDMP.CD_MPPNG_SK
"""

df_subName = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", subName_query)
    .load()
)

subAddr_query = """SELECT SUB.SUB_SK,
       SUB.ADDR_LN_1,
       SUB.ADDR_LN_2,
       SUB.CITY_NM,
       SUB.POSTAL_CD,
       SUB.SUB_ADDR_ST_CD_SK,
       CDST.TRGT_CD AS CDST_TRGT_CD,
       CDMP.TRGT_CD AS CDMP_TRGT_CD
FROM #$IDSOwner#.SUB_ADDR SUB,
     #$IDSOwner#.CD_MPPNG CDMP,
     #$IDSOwner#.CD_MPPNG CDST,
     #$IDSOwner#.W_WEBDM_ETL_DRVR R,
     #$IDSOwner#.CLM CLM
WHERE SUB.SUB_ADDR_CD_SK = CDMP.CD_MPPNG_SK
  AND R.SRC_SYS_CD_SK = CLM.SRC_SYS_CD_SK
  AND R.CLM_ID = CLM.CLM_ID
  AND CLM.SUB_SK = SUB.SUB_SK
  AND SUB.SUB_ADDR_ST_CD_SK = CDST.CD_MPPNG_SK
"""

df_subAddr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", subAddr_query)
    .load()
)

billSvcProv_query = """SELECT CLM.CLM_SK, PROV_BILL_SVC_ID, NG1.TRGT_CD
FROM #$IDSOwner#.W_WEBDM_ETL_DRVR R, 
     #$IDSOwner#.CLM_PROV V,
     #$IDSOwner#.CD_MPPNG NG1,
     #$IDSOwner#.PROV_BILL_SVC_ASSOC ASSOC,
     #$IDSOwner#.CLM CLM
WHERE R.SRC_SYS_CD_SK = V.SRC_SYS_CD_SK
  AND R.CLM_ID = V.CLM_ID
  AND V.CLM_PROV_ROLE_TYP_CD_SK = NG1.CD_MPPNG_SK
  AND V.PROV_SK = ASSOC.PROV_SK
  AND CLM.SVC_STRT_DT_SK between ASSOC.EFF_DT_SK and ASSOC.END_DT_SK
  AND CLM.SRC_SYS_CD_SK = R.SRC_SYS_CD_SK
  AND CLM.CLM_ID = R.CLM_ID
"""

df_billSvcProv = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", billSvcProv_query)
    .load()
)

#---------------------------------------------------------------------------------------------------
# claim_pull24 (CSeqFileStage) writing df_claim_pull to a file
#---------------------------------------------------------------------------------------------------
df_claim_pull_select = df_claim_pull.select(
    "CLM_ID","CLM_SK","ALPHA_PFX_SK","CLM_FINL_DISP_CD_SK","CLM_STTUS_CD_SK","CLM_TYP_CD_SK",
    "PD_DT_SK","SVC_STRT_DT_SK","SVC_END_DT_SK","STTUS_DT_SK","CNSD_CHRG_AMT","CHRG_AMT","PAYBL_AMT",
    "SUB_ID","MBR_UNIQ_KEY","BRTH_DT_SK","FIRST_NM","MIDINIT","LAST_NM","CLM_CAP_CD_SK",
    "CLM_INTER_PLN_PGM_CD_SK","CLM_PAYE_CD_SK","CLM_SUBTYP_CD_SK","ACTL_PD_AMT","ALW_AMT","DSALW_AMT",
    "COINS_AMT","COPAY_AMT","DEDCT_AMT","ADJ_FROM_CLM_ID","ADJ_TO_CLM_ID","PATN_ACCT_NO","RFRNG_PROV_TX",
    "SRC_SYS_CD_SK","RCVD_DT_SK","GRP_ID","GRP_UNIQ_KEY","GRP_NM","SUB_SK","SUB_UNIQ_KEY","HOST_IN",
    "CLM_NTWK_STTUS_CD_SK","MBR_SK","PRCS_DT_SK","CLS_SK","CLS_PLN_SK","MBR_SFX_NO","NTWK_SK","PROD_SK",
    "PROV_AGMNT_ID","CLM_INPT_SRC_CD_SK","PCA_TYP_CD_SK","REL_PCA_CLM_SK","REL_BASE_CLM_SK",
    col("SRC_SYS_CD_SK_1"),  
    "CLM_SUB_BCBS_PLN_CD_SK"
)
write_files(
    df_claim_pull_select,
    f"{adls_path}/verified/IdsClmMartClmExtr.refGetPcaClmId2.dat.#RunID#.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=False,
    quote="\"",
    nullValue=None
)

#---------------------------------------------------------------------------------------------------
# hf_clmmart_base_clm22 (CHashedFileStage)
#---------------------------------------------------------------------------------------------------
df_base_clm_select = df_base_clm.select(
    "REL_BASE_CLM_SK",
    col("CLM_ID")  
)
write_files(
    df_base_clm_select,
    f"{adls_path}/hf_clmmart_base_clm.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

#---------------------------------------------------------------------------------------------------
# hf_clmmart_pca_clm23 (CHashedFileStage)
#---------------------------------------------------------------------------------------------------
df_pca_clm_lookup_select = df_pca_clm_lookup.select(
    "REL_PCA_CLM_SK",
    col("CLM_ID")  
)
write_files(
    df_pca_clm_lookup_select,
    f"{adls_path}/hf_clmmart_pca_clm.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

#---------------------------------------------------------------------------------------------------
# Trans5 -> Hashed_File_34
#---------------------------------------------------------------------------------------------------
df_subAddr_filtered = df_subAddr.filter(trim(col("CDMP_TRGT_CD")) == lit("SUBHOME"))
df_sub_addr_lookup = df_subAddr_filtered.select(
    col("SUB_SK").alias("SUB_SK"),
    col("ADDR_LN_1").alias("ADDR_LN_1"),
    col("ADDR_LN_2").alias("ADDR_LN_2"),
    col("CITY_NM").alias("CITY_NM"),
    col("POSTAL_CD").alias("POSTAL_CD"),
    col("CDST_TRGT_CD").alias("SUB_ADDR_ST_CD")
)
write_files(
    df_sub_addr_lookup,
    f"{adls_path}/sub_addr_lookup.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

#---------------------------------------------------------------------------------------------------
# Trans4 -> Hashed_File_32
#---------------------------------------------------------------------------------------------------
df_subName_filtered = df_subName.filter(trim(col("TRGT_CD")) == lit("SUB"))
df_subName_with_midinit = df_subName_filtered.withColumn("MIDINIT", rpad(col("MIDINIT"), 1, " "))
df_sub_name_lookup = df_subName_with_midinit.select(
    col("SUB_SK").alias("SUB_SK"),
    col("FIRST_NM").alias("FIRST_NM"),
    "MIDINIT",
    col("LAST_NM").alias("LAST_NM")
)
write_files(
    df_sub_name_lookup,
    f"{adls_path}/sub_name_lookup.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

#---------------------------------------------------------------------------------------------------
# Trans6 -> Hashed_File_29 and Hashed_File_30
#---------------------------------------------------------------------------------------------------
df_billSvcProv_pd = df_billSvcProv.filter(trim(col("TRGT_CD")) == lit("PD"))
df_bill_svc_prov_lookup = df_billSvcProv_pd.select(
    col("CLM_SK").alias("CLM_SK"),
    col("PROV_BILL_SVC_ID").alias("PROV_BILL_SVC_ID")
)
write_files(
    df_bill_svc_prov_lookup,
    f"{adls_path}/bill_svc_prov_lookup.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

df_billSvcProv_svc = df_billSvcProv.filter(trim(col("TRGT_CD")) == lit("SVC"))
df_bill_svc_prov_svc_lookup = df_billSvcProv_svc.select(
    col("CLM_SK").alias("CLM_SK"),
    col("PROV_BILL_SVC_ID").alias("PROV_BILL_SVC_ID")
)
write_files(
    df_bill_svc_prov_svc_lookup,
    f"{adls_path}/bill_svc_prov_svc_lookup.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

#---------------------------------------------------------------------------------------------------
# Trans7 -> Hashed_File_25 and bill_svc_grp_svc_26
#---------------------------------------------------------------------------------------------------
df_billSvcGrp_pd = df_billSvcGrp.filter(trim(col("TRGT_CD")) == lit("PD"))
df_bill_svc_grp_lookup = df_billSvcGrp_pd.select(
    col("CLM_SK").alias("CLM_SK"),
    col("PROV_BILL_SVC_ID").alias("PROV_BILL_SVC_ID")
)
write_files(
    df_bill_svc_grp_lookup,
    f"{adls_path}/bill_svc_grp_lookup.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

df_billSvcGrp_svc = df_billSvcGrp.filter(trim(col("TRGT_CD")) == lit("SVC"))
df_bill_svc_grp_svc_lookup = df_billSvcGrp_svc.select(
    col("CLM_SK").alias("CLM_SK"),
    col("PROV_BILL_SVC_ID").alias("PROV_BILL_SVC_ID")
)
write_files(
    df_bill_svc_grp_svc_lookup,
    f"{adls_path}/bill_svc_grp_svc_lookup.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

#---------------------------------------------------------------------------------------------------
# Trans8 -> Hashed_File_27 and Hashed_File_28
#---------------------------------------------------------------------------------------------------
df_pdProv_pd = df_pdProv.filter(trim(col("TRGT_CD")) == lit("PD"))
df_pd_prov_lookup = df_pdProv_pd.select(
    col("CLM_SK").alias("CLM_SK"),
    col("PROV_ID").alias("PROV_ID"),
    col("PROV_NM").alias("PROV_NM"),
    col("PROV_SK").alias("PROV_SK"),
    col("REL_GRP_PROV_SK").alias("REL_GRP_PROV_SK")
)
write_files(
    df_pd_prov_lookup,
    f"{adls_path}/pd_prov_lookup.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

df_pdProv_pcp = df_pdProv.filter(trim(col("TRGT_CD")) == lit("PCP"))
df_pcp_prov_lookup = df_pdProv_pcp.select(
    col("CLM_SK").alias("CLM_SK"),
    col("PROV_ID").alias("PROV_ID"),
    col("PROV_NM").alias("PROV_NM")
)
write_files(
    df_pcp_prov_lookup,
    f"{adls_path}/pcp_prov_lookup.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

#---------------------------------------------------------------------------------------------------
# Trans10 -> Hashed_File_33
#---------------------------------------------------------------------------------------------------
df_pdProvAddr_pd = df_pdProvAddr.filter(trim(col("TRGT_CD")) == lit("PD"))
df_pd_prov_addr_lookup = df_pdProvAddr_pd.select(
    col("CLM_SK").alias("CLM_SK"),
    col("PROV_ID").alias("PROV_ID"),
    col("PROV_NM").alias("PROV_NM"),
    col("ADDR_LN_1").alias("ADDR_LN_1"),
    col("ADDR_LN_2").alias("ADDR_LN_2"),
    col("ADDR_LN_3").alias("ADDR_LN_3"),
    col("CITY_NM").alias("CITY_NM"),
    col("POSTAL_CD").alias("POSTAL_CD"),
    col("PROV_ADDR_ST_CD").alias("PROV_ADDR_ST_CD")
)
write_files(
    df_pd_prov_addr_lookup,
    f"{adls_path}/pd_prov_addr_lookup.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

#---------------------------------------------------------------------------------------------------
# Trans9 -> Hashed_File_31
#---------------------------------------------------------------------------------------------------
df_svcProv_svc = df_svcProv.filter(trim(col("TRGT_CD")) == lit("SVC"))
df_svc_provsvc_prov = df_svcProv_svc.select(
    col("CLM_SK").alias("CLM_SK"),
    col("PROV_NM").alias("PROV_NM"),
    col("PROV_ID").alias("PROV_ID"),
    col("CMN_PRCT_SK").alias("CMN_PRCT_SK"),
    col("CMN_PRCT_ID").alias("CMN_PRCT_ID"),
    col("REL_GRP_PROV_SK").alias("REL_GRP_PROV_SK"),
    col("REL_IPA_PROV_SK").alias("REL_IPA_PROV_SK"),
    col("GRP_PROV_ID").alias("GRP_PROV_ID"),
    col("IPA_PROV_ID").alias("IPA_PROV_ID")
)
write_files(
    df_svc_provsvc_prov,
    f"{adls_path}/svc_provsvc_prov.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

#---------------------------------------------------------------------------------------------------
# SYSDUMMY1 (DB2Connector) -> cur_date -> hf_clmmart_date35
#---------------------------------------------------------------------------------------------------

sysdummy_query = """SELECT IBMREQD FROM sysibm.sysdummy1"""
df_sysdummy1 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", sysdummy_query)
    .load()
)
df_cur_date = df_sysdummy1.withColumn("datelkup", substring(current_timestamp(), 1, 10))
df_cur_date_out = df_cur_date.select(
    lit("1").alias("VALUE"),
    rpad(col("datelkup"), 10, " ").alias("CURDATE")
)
write_files(
    df_cur_date_out,
    f"{adls_path}/hf_clmmart_date.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

# ========== Script 2 ==========
# Parameters for Script 2
CommitPoint = get_widget_value('CommitPoint','20000')
RunCycle = get_widget_value('RunCycle','')
ClmMartOwner = get_widget_value('ClmMartOwner','')
clmmart_secret_name = get_widget_value('clmmart_secret_name','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

extract_query_ip_fclty_clm = f"""SELECT FCLTY.CLM_SK as CLM_SK,DRG_CD as DRG_CD,GNRT_DRG_IN as GNRT_DRG_IN FROM {IDSOwner}.W_WEBDM_ETL_DRVR R,{IDSOwner}.FCLTY_CLM FCLTY, {IDSOwner}.DRG DRG WHERE R.SRC_SYS_CD_SK = FCLTY.SRC_SYS_CD_SK
and R.CLM_ID = FCLTY.CLM_ID
and FCLTY.GNRT_DRG_SK = DRG.DRG_SK

{IDSOwner}.W_WEBDM_ETL_DRVR R,{IDSOwner}.FCLTY_CLM FCLTY, {IDSOwner}.DRG DRG
"""
df_ip_fclty_clm = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_ip_fclty_clm)
    .load()
)

extract_query_drug_lookup = f"""SELECT CLM.CLM_SK as CLM_SK,NDC as NDC,RX_SUBMT_QTY,RX_NO as RX_NO,DRUG_LABEL_NM as DRUG_LABEL_NM,CMN_PRCT_ID as CMN_PRCT_ID,FIRST_NM as FIRST_NM,LAST_NM as LAST_NM,MBR_DIFF_PD_AMT,RX_ALW_QTY,PROV_NM FROM {IDSOwner}.W_WEBDM_ETL_DRVR R,{IDSOwner}.DRUG_CLM CLM,{IDSOwner}.NDC NDC, {IDSOwner}.PROV_DEA DEA, {IDSOwner}.CMN_PRCT PRCT WHERE R.SRC_SYS_CD_SK = CLM.SRC_SYS_CD_SK 
and R.CLM_ID = CLM.CLM_ID 
and CLM.NDC_SK = NDC.NDC_SK
and CLM.PRSCRB_PROV_DEA_SK = DEA.PROV_DEA_SK
and DEA.CMN_PRCT_SK = PRCT.CMN_PRCT_SK

{IDSOwner}.W_WEBDM_ETL_DRVR R,{IDSOwner}.DRUG_CLM CLM,{IDSOwner}.NDC NDC, {IDSOwner}.PROV_DEA DEA, {IDSOwner}.CMN_PRCT PRCT
"""
df_drug_lookup = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_drug_lookup)
    .load()
)

extract_query_cob_lookup = f"""SELECT CLM.CLM_SK as CLM_SK,PD_AMT as PD_AMT FROM {IDSOwner}.W_WEBDM_ETL_DRVR R,{IDSOwner}.CLM_COB CLM WHERE R.SRC_SYS_CD_SK = CLM.SRC_SYS_CD_SK 
and R.CLM_ID = CLM.CLM_ID

{IDSOwner}.W_WEBDM_ETL_DRVR R,{IDSOwner}.CLM_COB CLM
"""
df_cob_lookup = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_cob_lookup)
    .load()
)

extract_query_remit_lookup = f"""SELECT 
CLM_REMIT.CLM_SK,
CLM_REMIT.SUPRESS_EOB_IN,
CLM_REMIT.SUPRESS_REMIT_IN,
CLM_REMIT.CNSD_CHRG_AMT,
CLM_REMIT.ER_COPAY_AMT,
CLM_REMIT.INTRST_AMT,
CLM_REMIT.NO_RESP_AMT,
CLM_REMIT.PATN_RESP_AMT,
CLM_REMIT.PROV_WRTOFF_AMT,
CLM_REMIT.ACTL_PD_AMT,
CLM_REMIT.ALT_CHRG_IN,
CLM_REMIT.ALT_CHRG_PROV_WRTOFF_AMT


FROM 
{IDSOwner}.W_WEBDM_ETL_DRVR  DRVR, 
{IDSOwner}.CLM_REMIT_HIST   CLM_REMIT
WHERE 

DRVR.SRC_SYS_CD_SK = CLM_REMIT.SRC_SYS_CD_SK 
and 
DRVR.CLM_ID = CLM_REMIT.CLM_ID 


{IDSOwner}.W_WEBDM_ETL_DRVR R, {IDSOwner}.CLM_REMIT_HIST CLM
"""
df_remit_lookup = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_remit_lookup)
    .load()
)

extract_query_external_mbr_lookup = f"""SELECT M.CLM_SK as CLM_SK,MBR_BRTH_DT_SK as MBR_BRTH_DT_SK,GRP_NM as GRP_NM,MBR_FIRST_NM as MBR_FIRST_NM,MBR_MIDINIT as MBR_MIDINIT,MBR_LAST_NM as MBR_LAST_NM,SUB_FIRST_NM as SUB_FIRST_NM,SUB_MIDINIT as SUB_MIDINIT,SUB_ADDR_LN_1 as SUB_ADDR_LN_1,SUB_ADDR_LN_2 as SUB_ADDR_LN_2,SUB_CITY_NM as SUB_CITY_NM,TRGT_CD as CLM_EXTRNL_MBRSH_SUB_ST_CD,SUB_POSTAL_CD as SUB_POSTAL_CD,SUB_LAST_NM as SUB_LAST_NM,SUB_GRP_BASE_NO as SUB_GRP_BASE_NO FROM {IDSOwner}.W_WEBDM_ETL_DRVR R,{IDSOwner}.CLM_EXTRNL_MBRSH M,{IDSOwner}.CD_MPPNG NG1 WHERE R.SRC_SYS_CD_SK = M.SRC_SYS_CD_SK 
and R.CLM_ID  = M.CLM_ID 
and CLM_EXTRNL_MBRSH_SUB_ST_CD_SK = CD_MPPNG_SK

{IDSOwner}.W_WEBDM_ETL_DRVR R,{IDSOwner}.CLM_EXTRNL_MBRSH M,{IDSOwner}.CD_MPPNG NG1
"""
df_external_mbr_lookup = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_external_mbr_lookup)
    .load()
)

extract_query_its_clm_lookup = f"""SELECT CLM.CLM_SK as CLM_SK,TRGT_CD,SCCF_NO FROM {IDSOwner}.W_WEBDM_ETL_DRVR R,{IDSOwner}.ITS_CLM CLM,{IDSOwner}.CD_MPPNG CD_MPPNG WHERE R.SRC_SYS_CD_SK = CLM.SRC_SYS_CD_SK 
and R.CLM_ID = CLM.CLM_ID
and CLM.TRNSMSN_SRC_CD_SK = CD_MPPNG.CD_MPPNG_SK

{IDSOwner}.W_WEBDM_ETL_DRVR R,{IDSOwner}.ITS_CLM CLM,{IDSOwner}.CD_MPPNG CD_MPPNG
"""
df_its_clm_lookup = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_its_clm_lookup)
    .load()
)

extract_query_alpha_pfx = f"""SELECT ALPHA_PFX_SK,ALPHA_PFX_CD FROM {IDSOwner}.ALPHA_PFX

{IDSOwner}.ALPHA_PFX
"""
df_alpha_pfx = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_alpha_pfx)
    .load()
)

extract_query_sttus_audit_lookup = f"""SELECT CLM_STTUS_AUDIT.CLM_SK as CLM_SK,CD_MPPNG.TRGT_CD as TRGT_CD,CD_MPPNG.TRGT_CD_NM as TRGT_CD_NM FROM {IDSOwner}.W_WEBDM_ETL_DRVR R,{IDSOwner}.CLM_STTUS_AUDIT CLM_STTUS_AUDIT,{IDSOwner}.CD_MPPNG CD_MPPNG WHERE R.SRC_SYS_CD_SK = CLM_STTUS_AUDIT.SRC_SYS_CD_SK 
and R.CLM_ID = CLM_STTUS_AUDIT.CLM_ID
and CLM_STTUS_AUDIT.CLM_STTUS_CHG_RSN_CD_SK = CD_MPPNG.CD_MPPNG_SK

{IDSOwner}.W_WEBDM_ETL_DRVR R,{IDSOwner}.CLM_STTUS_AUDIT CLM_STTUS_AUDIT,{IDSOwner}.CD_MPPNG CD_MPPNG
"""
df_sttus_audit_lookup = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_sttus_audit_lookup)
    .load()
)

extract_query_fcility_clm_lookup = f"""SELECT 
FCLTY.CLM_SK,
MAP1.TRGT_CD,
MAP1.TRGT_CD_NM,
MAP2.TRGT_CD,
MAP2.TRGT_CD_NM,
MAP3.TRGT_CD,
MAP3.TRGT_CD_NM,
MAP4.TRGT_CD,
MAP4.TRGT_CD_NM,
MAP5.TRGT_CD,
MAP5.TRGT_CD_NM,
MAP6.TRGT_CD,
MAP6.TRGT_CD_NM,
FCLTY.ADMS_DT_SK,
FCLTY.DSCHG_DT_SK,
FCLTY.ADM_PHYS_PROV_ID,
DRG.DRG_CD,
FCLTY.SUBMT_DRG_TX
FROM {IDSOwner}.W_WEBDM_ETL_DRVR        DRVR,
{IDSOwner}.FCLTY_CLM                   FCLTY,
{IDSOwner}.CD_MPPNG                    MAP1,
{IDSOwner}.CD_MPPNG                    MAP2,
{IDSOwner}.CD_MPPNG                    MAP3,
{IDSOwner}.CD_MPPNG                    MAP4,
{IDSOwner}.CD_MPPNG                    MAP5,
{IDSOwner}.CD_MPPNG                    MAP6,
{IDSOwner}.DRG                         DRG
WHERE  DRVR.SRC_SYS_CD_SK             = FCLTY.SRC_SYS_CD_SK 
and    DRVR.CLM_ID                    = FCLTY.CLM_ID 
and    FCLTY.FCLTY_CLM_ADMS_SRC_CD_SK = MAP1.CD_MPPNG_SK  
and    FCLTY.FCLTY_CLM_ADMS_TYP_CD_SK = MAP2.CD_MPPNG_SK  
and    FCLTY.FCLTY_CLM_BILL_CLS_CD_SK = MAP3.CD_MPPNG_SK  
and    FCLTY.FCLTY_CLM_BILL_FREQ_CD_SK= MAP4.CD_MPPNG_SK  
and    FCLTY.FCLTY_CLM_BILL_TYP_CD_SK = MAP5.CD_MPPNG_SK 
and    FCLTY.FCLTY_CLM_DSCHG_STTUS_CD_SK=MAP6.CD_MPPNG_SK  
and    FCLTY.GNRT_DRG_SK              =DRG.DRG_SK

{IDSOwner}.W_WEBDM_ETL_DRVR R,{IDSOwner}.FCLTY_CLM FCLTY_CLM,{IDSOwner}.CD_MPPNG CD_MPPNG1,{IDSOwner}.CD_MPPNG CD_MPPNG2,{IDSOwner}.CD_MPPNG CD_MPPNG3,{IDSOwner}.CD_MPPNG CD_MPPNG4,{IDSOwner}.CD_MPPNG CD_MPPNG5,{IDSOwner}.CD_MPPNG CD_MPPNG6,{IDSOwner}.DRG DRG,{IDSOwner}.DRG DRG2
"""
df_fcility_clm_lookup_raw = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_fcility_clm_lookup)
    .load()
)
df_fcility_clm_lookup = df_fcility_clm_lookup_raw.select(
    F.col("CLM_SK"),
    F.col("MAP1.TRGT_CD").alias("FCLTY_CLM_ADMS_SRC_CD"),
    F.col("MAP1.TRGT_CD_NM").alias("FCLTY_CLM_ADMS_SRC_CD_NM"),
    F.col("MAP2.TRGT_CD").alias("FCLTY_CLM_ADMS_TYP_CD"),
    F.col("MAP2.TRGT_CD_NM").alias("FCLTY_CLM_ADMS_TYP_CD_NM"),
    F.col("MAP3.TRGT_CD").alias("FCLTY_CLM_BILL_CLS_CD"),
    F.col("MAP3.TRGT_CD_NM").alias("FCLTY_CLM_BILL_CLS_CD_NM"),
    F.col("MAP4.TRGT_CD").alias("FCLTY_CLM_BILL_FREQ_CD"),
    F.col("MAP4.TRGT_CD_NM").alias("FCLTY_CLM_BILL_FREQ_CD_NM"),
    F.col("MAP5.TRGT_CD").alias("FCLTY_CLM_BILL_TYP_CD"),
    F.col("MAP5.TRGT_CD_NM").alias("FCLTY_CLM_BILL_TYP_CD_NM"),
    F.col("MAP6.TRGT_CD").alias("FCLTY_CLM_DSCHG_STTUS_CD"),
    F.col("MAP6.TRGT_CD_NM").alias("FCLTY_CLM_DSCHG_STTUS_CD_NM"),
    F.col("ADMS_DT_SK"),
    F.col("DSCHG_DT_SK"),
    F.col("ADM_PHYS_PROV_ID"),
    F.col("DRG_CD"),
    F.col("SUBMT_DRG_TX")
)

extract_query_mbr_lookup = f"""SELECT MBR.MBR_SK,MBR.BRTH_DT_SK,CD_MPPNG.TRGT_CD,CD_MPPNG.TRGT_CD_NM,CD_MPPNG2.TRGT_CD 

FROM  {IDSOwner}.MBR MBR,
      {IDSOwner}.CD_MPPNG CD_MPPNG,
      {IDSOwner}.CD_MPPNG CD_MPPNG2, 
      {IDSOwner}.W_WEBDM_ETL_DRVR R,
      {IDSOwner}.CLM CLM

WHERE MBR.MBR_GNDR_CD_SK=CD_MPPNG.CD_MPPNG_SK 
      AND MBR.MBR_RELSHP_CD_SK=CD_MPPNG2.CD_MPPNG_SK
      AND R.SRC_SYS_CD_SK = CLM.SRC_SYS_CD_SK  
      AND R.CLM_ID = CLM.CLM_ID                                    
      AND CLM.MBR_SK = MBR.MBR_SK

{IDSOwner}.MBR MBR,{IDSOwner}.CD_MPPNG CD_MPPNG,{IDSOwner}.CD_MPPNG CD_MPPNG2
"""
df_mbr_lookup_raw = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_mbr_lookup)
    .load()
)
df_mbr_lookup = df_mbr_lookup_raw.select(
    F.col("MBR_SK"),
    F.col("BRTH_DT_SK"),
    F.col("CD_MPPNG.TRGT_CD").alias("MBR_GNDR_CD"),
    F.col("CD_MPPNG.TRGT_CD_NM").alias("MBR_GNDR_CD_NM"),
    F.col("CD_MPPNG2.TRGT_CD").alias("MBR_RELSHP_CD")
)

extract_query_cls_lookup = f"""SELECT CLS.CLS_SK as CLS_SK,CLS.CLS_ID as CLS_ID,CLS.CLS_DESC as CLS_DESC FROM {IDSOwner}.CLS CLS

{IDSOwner}.CLS CLS
"""
df_cls_lookup = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_cls_lookup)
    .load()
)

extract_query_cls_pln_lookup = f"""SELECT CLS_PLN.CLS_PLN_SK as CLS_PLN_SK,CLS_PLN.CLS_PLN_ID as CLS_PLN_ID,CLS_PLN.CLS_PLN_DESC as CLS_PLN_DESC FROM {IDSOwner}.CLS_PLN CLS_PLN

{IDSOwner}.CLS_PLN CLS_PLN
"""
df_cls_pln_lookup = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_cls_pln_lookup)
    .load()
)

extract_query_ntwk_lookup = f"""SELECT NTWK.NTWK_SK as NTWK_SK,NTWK.NTWK_ID as NTWK_ID,NTWK.NTWK_NM as NTWK_NM FROM {IDSOwner}.NTWK NTWK

{IDSOwner}.NTWK NTWK
"""
df_ntwk_lookup = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_ntwk_lookup)
    .load()
)

extract_query_prod_lookup = f"""SELECT PROD.PROD_SK as PROD_SK,PROD.PROD_ID as PROD_ID FROM {IDSOwner}.PROD PROD

{IDSOwner}.PROD PROD
"""
df_prod_lookup = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_prod_lookup)
    .load()
)

extract_query_clm_ln_lookup = f"""SELECT CLM_LN.CLM_LN_SEQ_NO as CLM_LN_SEQ_NO,CLM_LN.CLM_SK as CLM_SK,CLM_LN.PREAUTH_ID as PREAUTH_ID FROM {IDSOwner}.W_WEBDM_ETL_DRVR R,{IDSOwner}.CLM_LN CLM_LN WHERE R.SRC_SYS_CD_SK = CLM_LN.SRC_SYS_CD_SK
and R.CLM_ID = CLM_LN.CLM_ID

{IDSOwner}.W_WEBDM_ETL_DRVR R,{IDSOwner}.CLM_LN CLM_LN
"""
df_clm_ln_lookup = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_clm_ln_lookup)
    .load()
)

extract_query_pca_clmchk_lookup = f"""SELECT CLM.CLM_SK as CLM_SK,CHK_NET_PAYMT_AMT,CHK_NO,CHK_SEQ_NO,CHK_PAYE_NM,CHK_PAYMT_REF_ID,DT.CLNDR_DT as PCA_CLM_CHK_PD_DT,CD_MPPNG2.TRGT_CD as PCA_CLM_CHK_PAYE_TYP_CD,CD_MPPNG2.TRGT_CD_NM as PCA_CLM_CHK_PAYE_TYP_NM,CD_MPPNG3.TRGT_CD as PCA_CLM_CHK_PAYMT_METH_CD,CD_MPPNG3.TRGT_CD_NM as PCA_CLM_CHK_PAYMT_METH_NM FROM {IDSOwner}.W_WEBDM_ETL_DRVR DRVR, {IDSOwner}.CLM CLM, {IDSOwner}.CLM_CHK CLMCHK, {IDSOwner}.CLNDR_DT DT, {IDSOwner}.CD_MPPNG CD_MPPNG, {IDSOwner}.CD_MPPNG CD_MPPNG2, {IDSOwner}.CD_MPPNG CD_MPPNG3 WHERE CLM.CLM_SK = CLMCHK.CLM_SK and 
CLMCHK.PCA_CHK_IN = 'Y' and 
DRVR.CLM_ID = CLM.CLM_ID AND 
CLMCHK.CHK_PD_DT_SK = DT.CLNDR_DT_SK and
CLMCHK.CLM_CHK_LOB_CD_SK = CD_MPPNG.CD_MPPNG_SK and 
CLMCHK.CLM_CHK_PAYE_TYP_CD_SK = CD_MPPNG2.CD_MPPNG_SK and 
CLMCHK.CLM_CHK_PAYMT_METH_CD_SK = CD_MPPNG3.CD_MPPNG_SK

{IDSOwner}.W_WEBDM_ETL_DRVR DRVR, {IDSOwner}.CLM CLM, {IDSOwner}.CLM_CHK CLMCHK, {IDSOwner}.CLNDR_DT DT, {IDSOwner}.CD_MPPNG CD_MPPNG, {IDSOwner}.CD_MPPNG CD_MPPNG2, {IDSOwner}.CD_MPPNG CD_MPPNG3
"""
df_pca_clmchk_lookup = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_pca_clmchk_lookup)
    .load()
)

extract_query_clm_pca_lookup = f"""SELECT CLM.CLM_SK as CLM_SK,TOT_CNSD_AMT,TOT_PD_AMT,SUB_TOT_PCA_AVLBL_AMT,SUB_TOT_PCA_PD_TO_DT_AMT FROM {IDSOwner}.CLM CLM, {IDSOwner}.CLM_PCA PCA, {IDSOwner}.W_WEBDM_ETL_DRVR DRVR WHERE CLM.CLM_SK = PCA.CLM_SK and 
DRVR.CLM_ID = CLM.CLM_ID

{IDSOwner}.CLM CLM, {IDSOwner}.CLM_PCA PCA, {IDSOwner}.W_WEBDM_ETL_DRVR DRVR
"""
df_clm_pca_lookup = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_clm_pca_lookup)
    .load()
)

extract_query_clm_extrnl_prov_lookup = f"""SELECT CLM.CLM_SK as CLM_SK,PROV_NM FROM {IDSOwner}.W_WEBDM_ETL_DRVR DRVR, {IDSOwner}.CLM CLM, {IDSOwner}.CLM_EXTRNL_PROV PROV  WHERE DRVR.CLM_ID = CLM.CLM_ID AND 
CLM.CLM_SK = PROV.CLM_SK

{IDSOwner}.W_WEBDM_ETL_DRVR DRVR, {IDSOwner}.CLM CLM, {IDSOwner}.CLM_EXTRNL_PROV PROV
"""
df_clm_extrnl_prov_lookup = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_clm_extrnl_prov_lookup)
    .load()
)

extract_query_clm_extrnl_ref_lookup = f"""SELECT CLM.CLM_SK as CLM_SK,PCA_EXTRNL_ID FROM {IDSOwner}.W_WEBDM_ETL_DRVR DRVR, {IDSOwner}.CLM CLM, {IDSOwner}.CLM_EXTRNL_REF_DATA REF WHERE DRVR.CLM_ID = CLM.CLM_ID AND 
CLM.CLM_SK = REF.CLM_SK

{IDSOwner}.W_WEBDM_ETL_DRVR DRVR, {IDSOwner}.CLM CLM, {IDSOwner}.CLM_EXTRNL_REF_DATA REF
"""
df_clm_extrnl_ref_lookup = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_clm_extrnl_ref_lookup)
    .load()
)

extract_query_clm_clm_ck = f"""SELECT CLMCK.CLM_SK as CLM_SK,CLMCK.CLM_CHK_SK as CLM_CHK_SK,CLMCK.SRC_SYS_CD_SK as SRC_SYS_CD_SK,CLMCK.CLM_ID as CLM_ID,CD_MPPNG2.TRGT_CD as CLM_CHK_PAYE_TYP_CD,CLMCK.CLM_CHK_LOB_CD_SK as CLM_CHK_LOB_CD_SK,CLMCK.CRT_RUN_CYC_EXCTN_SK as CRT_RUN_CYC_EXCTN_SK,CLMCK.LAST_UPDT_RUN_CYC_EXCTN_SK as LAST_UPDT_RUN_CYC_EXCTN_SK,CD_MPPNG.TRGT_CD as CLM_CHK_PAYMT_METH_CD,CLMCK.CLM_REMIT_HIST_PAYMTOVRD_CD_SK as CLM_REMIT_HIST_PAYMTOVRD_CD_SK,CLMCK.EXTRNL_CHK_IN as EXTRNL_CHK_IN,CLMCK.PCA_CHK_IN as PCA_CHK_IN,DT.CLNDR_DT as CHK_PD_DT_SK,CLMCK.CHK_NET_PAYMT_AMT as CHK_NET_PAYMT_AMT,CLMCK.CHK_ORIG_AMT as CHK_ORIG_AMT,CLMCK.CHK_NO as CHK_NO,CLMCK.CHK_SEQ_NO as CHK_SEQ_NO,CLMCK.CHK_PAYE_NM as CHK_PAYE_NM,CLMCK.CHK_PAYMT_REF_ID as CHK_PAYMT_REF_ID FROM {IDSOwner}.W_WEBDM_ETL_DRVR DRVR, {IDSOwner}.CLM CLM, {IDSOwner}.CLM_CHK CLMCK, {IDSOwner}.CLNDR_DT DT, {IDSOwner}.CD_MPPNG CD_MPPNG, {IDSOwner}.CD_MPPNG CD_MPPNG2 WHERE CLM.CLM_SK = CLMCK.CLM_SK and CLMCK.PCA_CHK_IN = 'N' and DRVR.CLM_ID = CLM.CLM_ID AND CLMCK.CHK_PD_DT_SK = DT.CLNDR_DT_SK and CLMCK.CLM_CHK_PAYMT_METH_CD_SK = CD_MPPNG.CD_MPPNG_SK AND CLMCK.CLM_CHK_PAYE_TYP_CD_SK = CD_MPPNG2.CD_MPPNG_SK

{IDSOwner}.W_WEBDM_ETL_DRVR DRVR, {IDSOwner}.CLM CLM, {IDSOwner}.CLM_CHK CLMCK, {IDSOwner}.CLNDR_DT DT, {IDSOwner}.CD_MPPNG CD_MPPNG, {IDSOwner}.CD_MPPNG CD_MPPNG2
"""
df_clm_clm_ck = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_clm_clm_ck)
    .load()
)

extract_query_clm_ln_sum_paybl_lookup = f"""SELECT CLMLN.CLM_SK as CLM_SK,SUM(CLMLN.PAYBL_AMT) as PAYBL_AMT FROM {IDSOwner}.CLM CLM, {IDSOwner}.CLM_LN CLMLN, {IDSOwner}.W_WEBDM_ETL_DRVR DRVR, {IDSOwner}.CD_MPPNG MAP WHERE CLM.CLM_SK = CLMLN.CLM_SK AND
DRVR.CLM_ID = CLM.CLM_ID AND
MAP.TRGT_CD IN ('RUNOUT','PCA','EMPWBNF') AND
CLM.PCA_TYP_CD_SK = MAP.CD_MPPNG_SK GROUP BY CLMLN.CLM_SK

{IDSOwner}.CLM CLM, {IDSOwner}.CLM_LN CLMLN, {IDSOwner}.W_WEBDM_ETL_DRVR DRVR, {IDSOwner}.CD_MPPNG MAP
"""
df_clm_ln_sum_paybl_lookup = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_clm_ln_sum_paybl_lookup)
    .load()
)

df_Clm21 = df_ip_fclty_clm.select("CLM_SK","DRG_CD","GNRT_DRG_IN")
write_files(
    df_Clm21,
    f"{adls_path}/ip_fclty_clm.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote='"',
    nullValue=None
)

df_Clm20 = df_cob_lookup.select("CLM_SK","PD_AMT")
write_files(
    df_Clm20,
    f"{adls_path}/cob_lookup.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote='"',
    nullValue=None
)

df_Clm2 = df_external_mbr_lookup.select("CLM_SK","MBR_BRTH_DT_SK","GRP_NM","MBR_FIRST_NM","MBR_MIDINIT","MBR_LAST_NM","SUB_FIRST_NM","SUB_MIDINIT","SUB_ADDR_LN_1","SUB_ADDR_LN_2","SUB_CITY_NM","CLM_EXTRNL_MBRSH_SUB_ST_CD","SUB_POSTAL_CD","SUB_LAST_NM","SUB_GRP_BASE_NO")
write_files(
    df_Clm2,
    f"{adls_path}/external_mbr_lookup.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote='"',
    nullValue=None
)

df_Clm1 = df_drug_lookup.select("CLM_SK","NDC","RX_SUBMT_QTY","RX_NO","DRUG_LABEL_NM","CMN_PRCT_ID","FIRST_NM","LAST_NM","MBR_DIFF_PD_AMT","RX_ALW_QTY","PROV_NM")
write_files(
    df_Clm1,
    f"{adls_path}/drug_lookup.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote='"',
    nullValue=None
)

df_Clm3 = df_remit_lookup.select("CLM_SK","SUPRESS_EOB_IN","SUPRESS_REMIT_IN","CNSD_CHRG_AMT","ER_COPAY_AMT","INTRST_AMT","NO_RESP_AMT","PATN_RESP_AMT","PROV_WRTOFF_AMT","ACTL_PD_AMT","ALT_CHRG_IN","ALT_CHRG_PROV_WRTOFF_AMT")
write_files(
    df_Clm3,
    f"{adls_path}/remit_lookup.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote='"',
    nullValue=None
)

df_Clm4 = df_its_clm_lookup.select("CLM_SK","TRGT_CD","SCCF_NO")
write_files(
    df_Clm4,
    f"{adls_path}/its_clm_lookup.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote='"',
    nullValue=None
)

df_Clm5 = df_alpha_pfx.select("ALPHA_PFX_SK","ALPHA_PFX_CD")
write_files(
    df_Clm5,
    f"{adls_path}/alpha_pfx.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote='"',
    nullValue=None
)

df_Clm6 = df_fcility_clm_lookup.select(
    "CLM_SK",
    "FCLTY_CLM_ADMS_SRC_CD",
    "FCLTY_CLM_ADMS_SRC_CD_NM",
    "FCLTY_CLM_ADMS_TYP_CD",
    "FCLTY_CLM_ADMS_TYP_CD_NM",
    "FCLTY_CLM_BILL_CLS_CD",
    "FCLTY_CLM_BILL_CLS_CD_NM",
    "FCLTY_CLM_BILL_FREQ_CD",
    "FCLTY_CLM_BILL_FREQ_CD_NM",
    "FCLTY_CLM_BILL_TYP_CD",
    "FCLTY_CLM_BILL_TYP_CD_NM",
    "FCLTY_CLM_DSCHG_STTUS_CD",
    "FCLTY_CLM_DSCHG_STTUS_CD_NM",
    "ADMS_DT_SK",
    "DSCHG_DT_SK",
    "ADM_PHYS_PROV_ID",
    "DRG_CD",
    "SUBMT_DRG_TX"
)
write_files(
    df_Clm6,
    f"{adls_path}/fcility_clm_lookup.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote='"',
    nullValue=None
)

df_Clm7 = df_sttus_audit_lookup.select("CLM_SK","TRGT_CD","TRGT_CD_NM")
write_files(
    df_Clm7,
    f"{adls_path}/sttus_audit_lookup.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote='"',
    nullValue=None
)

df_Clm8 = df_mbr_lookup.select("MBR_SK","BRTH_DT_SK","MBR_GNDR_CD","MBR_GNDR_CD_NM","MBR_RELSHP_CD")
write_files(
    df_Clm8,
    f"{adls_path}/mbr_lookup.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote='"',
    nullValue=None
)

df_Clm9 = df_cls_lookup.select("CLS_SK","CLS_ID","CLS_DESC")
write_files(
    df_Clm9,
    f"{adls_path}/cls_lookup.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote='"',
    nullValue=None
)

df_Clm11 = df_ntwk_lookup.select("NTWK_SK","NTWK_ID","NTWK_NM")
write_files(
    df_Clm11,
    f"{adls_path}/ntwk_lookup.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote='"',
    nullValue=None
)

df_Clm12 = df_prod_lookup.select("PROD_SK","PROD_ID")
write_files(
    df_Clm12,
    f"{adls_path}/prod_lookup.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote='"',
    nullValue=None
)

df_Clm13 = df_clm_ln_lookup.select("CLM_LN_SEQ_NO","CLM_SK","PREAUTH_ID")
write_files(
    df_Clm13,
    f"{adls_path}/clm_ln_lookup.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote='"',
    nullValue=None
)

df_Clm14 = df_pca_clmchk_lookup.select("CLM_SK","CHK_NET_PAYMT_AMT","CHK_NO","CHK_SEQ_NO","CHK_PAYE_NM","CHK_PAYMT_REF_ID","PCA_CLM_CHK_PD_DT","PCA_CLM_CHK_PAYE_TYP_CD","PCA_CLM_CHK_PAYE_TYP_NM","PCA_CLM_CHK_PAYMT_METH_CD","PCA_CLM_CHK_PAYMT_METH_NM")
write_files(
    df_Clm14,
    f"{adls_path}/pca_clmchk_lookup.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote='"',
    nullValue=None
)

df_Clm15 = df_clm_pca_lookup.select("CLM_SK","TOT_CNSD_AMT","TOT_PD_AMT","SUB_TOT_PCA_AVLBL_AMT","SUB_TOT_PCA_PD_TO_DT_AMT")
write_files(
    df_Clm15,
    f"{adls_path}/clm_pca_lookup.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote='"',
    nullValue=None
)

df_Clm16 = df_clm_extrnl_prov_lookup.select("CLM_SK","PROV_NM")
write_files(
    df_Clm16,
    f"{adls_path}/clm_extrnl_prov_lookup.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote='"',
    nullValue=None
)

df_Clm17 = df_clm_extrnl_ref_lookup.select("CLM_SK","PCA_EXTRNL_ID")
write_files(
    df_Clm17,
    f"{adls_path}/clm_extrnl_ref_lookup.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote='"',
    nullValue=None
)

df_Clm18 = df_clm_clm_ck.select("CLM_SK","CLM_CHK_SK","SRC_SYS_CD_SK","CLM_ID","CLM_CHK_PAYE_TYP_CD","CLM_CHK_LOB_CD_SK","CRT_RUN_CYC_EXCTN_SK","LAST_UPDT_RUN_CYC_EXCTN_SK","CLM_CHK_PAYMT_METH_CD","CLM_REMIT_HIST_PAYMTOVRD_CD_SK","EXTRNL_CHK_IN","PCA_CHK_IN","CHK_PD_DT_SK","CHK_NET_PAYMT_AMT","CHK_ORIG_AMT","CHK_NO","CHK_SEQ_NO","CHK_PAYE_NM","CHK_PAYMT_REF_ID")
write_files(
    df_Clm18,
    f"{adls_path}/clm_clm_ck.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote='"',
    nullValue=None
)

df_Clm19 = df_clm_ln_sum_paybl_lookup.select("CLM_SK","PAYBL_AMT")
write_files(
    df_Clm19,
    f"{adls_path}/clm_ln_sum_paybl_lookup.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote='"',
    nullValue=None
)

df_Clm10 = df_cls_pln_lookup.select("CLS_PLN_SK","CLS_PLN_ID","CLS_PLN_DESC")
write_files(
    df_Clm10,
    f"{adls_path}/cls_pln_lookup.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote='"',
    nullValue=None
)

# ========== Script 3 ==========
# Parameters for Script 3
CommitPoint = get_widget_value('CommitPoint','20000')
RunCycle = get_widget_value('RunCycle','')
ClmMartOwner = get_widget_value('ClmMartOwner','')
clmmart_secret_name = get_widget_value('clmmart_secret_name','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')



CommitPoint = get_widget_value('CommitPoint','20000')
RunCycle = get_widget_value('RunCycle','')
ClmMartOwner = get_widget_value('ClmMartOwner','')
clmmart_secret_name = get_widget_value('clmmart_secret_name','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

df_hf_etrnl_cd_mppng = spark.read.parquet(f"{adls_path}/hf_etrnl_cd_mppng.parquet").select(
    F.col("CD_MPPNG_SK").alias("hf_etrnl_cd_mppng_CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("hf_etrnl_cd_mppng_TRGT_CD"),
    F.col("TRGT_CD_NM").alias("hf_etrnl_cd_mppng_TRGT_CD_NM"),
    F.col("SRC_CD").alias("hf_etrnl_cd_mppng_SRC_CD"),
    F.col("SRC_CD_NM").alias("hf_etrnl_cd_mppng_SRC_CD_NM")
)

df_hf_clmmart_base_clm22 = spark.read.parquet(f"{adls_path}/hf_clmmart_base_clm.parquet").select(
    F.col("CLM_SK").alias("hf_clmmart_base_clm22_CLM_SK"),
    F.col("CLM_ID").alias("hf_clmmart_base_clm22_CLM_ID")
)

df_Copy_of_hf_clmmart_pca_clm23 = spark.read.parquet(f"{adls_path}/hf_clmmart_pca_clm.parquet").select(
    F.col("CLM_SK").alias("pca_clm23_CLM_SK"),
    F.col("CLM_ID").alias("pca_clm23_CLM_ID")
)

df_Copy_of_Hashed_File_25 = spark.read.parquet(f"{adls_path}/hf_clmmart_base_clm.parquet").select(
    F.col("CLM_SK").alias("hf_25_CLM_SK"),
    F.col("PROV_BILL_SVC_ID").alias("hf_25_PROV_BILL_SVC_ID")
)

df_Copy_of_bill_svc_grp_svc_26 = spark.read.parquet(f"{adls_path}/bill_svc_grp_svc.parquet").select(
    F.col("CLM_SK").alias("grp_svc_26_CLM_SK"),
    F.col("PROV_BILL_SVC_ID").alias("grp_svc_26_PROV_BILL_SVC_ID")
)

df_Copy_of_Hashed_File_27 = spark.read.parquet(f"{adls_path}/pd_prov_lookup.parquet").select(
    F.col("CLM_SK").alias("pd_prov_27_CLM_SK"),
    F.col("PROV_ID").alias("pd_prov_27_PROV_ID"),
    F.col("PROV_NM").alias("pd_prov_27_PROV_NM"),
    F.col("PROV_SK").alias("pd_prov_27_PROV_SK"),
    F.col("REL_GRP_PROV_SK").alias("pd_prov_27_REL_GRP_PROV_SK")
)

df_Copy_of_Hashed_File_28 = spark.read.parquet(f"{adls_path}/pcp_prov.parquet").select(
    F.col("CLM_SK").alias("pcp_prov_28_CLM_SK"),
    F.col("PROV_ID").alias("pcp_prov_28_PROV_ID"),
    F.col("PROV_NM").alias("pcp_prov_28_PROV_NM")
)

df_Copy_of_Hashed_File_29 = spark.read.parquet(f"{adls_path}/bill_svc_prov.parquet").select(
    F.col("CLM_SK").alias("svc_prov_29_CLM_SK"),
    F.col("PROV_BILL_SVC_ID").alias("svc_prov_29_PROV_BILL_SVC_ID")
)

df_Copy_of_Hashed_File_30 = spark.read.parquet(f"{adls_path}/bill_svc_prov_svc.parquet").select(
    F.col("CLM_SK").alias("svc_30_CLM_SK"),
    F.col("PROV_BILL_SVC_ID").alias("svc_30_PROV_BILL_SVC_ID")
)

df_Copy_of_Hashed_File_31 = spark.read.parquet(f"{adls_path}/svc_prov.parquet").select(
    F.col("CLM_SK").alias("svc_31_CLM_SK"),
    F.col("PROV_NM").alias("svc_31_PROV_NM"),
    F.col("PROV_ID").alias("svc_31_PROV_ID"),
    F.col("CMN_PRCT_SK").alias("svc_31_CMN_PRCT_SK"),
    F.col("CMN_PRCT_ID").alias("svc_31_CMN_PRCT_ID"),
    F.col("REL_GRP_PROV_SK").alias("svc_31_REL_GRP_PROV_SK"),
    F.col("REL_IPA_PROV_SK").alias("svc_31_REL_IPA_PROV_SK"),
    F.col("GRP_PROV_ID").alias("svc_31_GRP_PROV_ID"),
    F.col("IPA_PROV_ID").alias("svc_31_IPA_PROV_ID")
)

df_Copy_of_Hashed_File_32 = spark.read.parquet(f"{adls_path}/sub_name.parquet").select(
    F.col("SUB_SK").alias("sub_name_32_SUB_SK"),
    F.col("FIRST_NM").alias("sub_name_32_FIRST_NM"),
    F.col("MIDINIT").alias("sub_name_32_MIDINIT"),
    F.col("LAST_NM").alias("sub_name_32_LAST_NM")
)

df_Copy_of_Hashed_File_33 = spark.read.parquet(f"{adls_path}/pd_prov_addr.parquet").select(
    F.col("CLM_SK").alias("pd_addr_33_CLM_SK"),
    F.col("PROV_ID").alias("pd_addr_33_PROV_ID"),
    F.col("PROV_NM").alias("pd_addr_33_PROV_NM"),
    F.col("ADDR_LN_1").alias("pd_addr_33_ADDR_LN_1"),
    F.col("ADDR_LN_2").alias("pd_addr_33_ADDR_LN_2"),
    F.col("ADDR_LN_3").alias("pd_addr_33_ADDR_LN_3"),
    F.col("CITY_NM").alias("pd_addr_33_CITY_NM"),
    F.col("POSTAL_CD").alias("pd_addr_33_POSTAL_CD"),
    F.col("PROV_ADDR_ST_CD").alias("pd_addr_33_PROV_ADDR_ST_CD")
)

df_Copy_of_Hashed_File_34 = spark.read.parquet(f"{adls_path}/sub_addr.parquet").select(
    F.col("SUB_SK").alias("sub_addr_34_SUB_SK"),
    F.col("ADDR_LN_1").alias("sub_addr_34_ADDR_LN_1"),
    F.col("ADDR_LN_2").alias("sub_addr_34_ADDR_LN_2"),
    F.col("CITY_NM").alias("sub_addr_34_CITY_NM"),
    F.col("POSTAL_CD").alias("sub_addr_34_POSTAL_CD"),
    F.col("SUB_ADDR_ST_CD").alias("sub_addr_34_SUB_ADDR_ST_CD")
)

df_Copy_of_hf_clmmart_date35 = spark.read.parquet(f"{adls_path}/hf_clmmart_date.parquet").select(
    F.col("VALUE").alias("hf_date35_VALUE"),
    F.rpad(F.col("CURDATE"),10," ").alias("hf_date35_CURDATE")
)

df_Copy_of_Clm21 = spark.read.parquet(f"{adls_path}/ip_fclty_clm.parquet").select(
    F.col("CLM_SK").alias("clm21_CLM_SK"),
    F.col("DRG_CD").alias("clm21_DRG_CD"),
    F.rpad(F.col("GNRT_DRG_IN"),1," ").alias("clm21_GNRT_DRG_IN")
)

df_Clm20 = spark.read.parquet(f"{adls_path}/cob_lookup.parquet").select(
    F.col("CLM_SK").alias("clm20_CLM_SK"),
    F.col("PD_AMT").alias("clm20_PD_AMT")
)

df_Copy_of_Clm1 = spark.read.parquet(f"{adls_path}/drug_clm.parquet").select(
    F.col("CLM_SK").alias("drug1_CLM_SK"),
    F.col("NDC").alias("drug1_NDC"),
    F.col("RX_SUBMT_QTY").alias("drug1_RX_SUBMT_QTY"),
    F.col("RX_NO").alias("drug1_RX_NO"),
    F.col("DRUG_LABEL_NM").alias("drug1_DRUG_LABEL_NM"),
    F.col("CMN_PRCT_ID").alias("drug1_CMN_PRCT_ID"),
    F.col("FIRST_NM").alias("drug1_FIRST_NM"),
    F.col("LAST_NM").alias("drug1_LAST_NM"),
    F.col("MBR_DIFF_PD_AMT").alias("drug1_MBR_DIFF_PD_AMT"),
    F.col("RX_ALW_QTY").alias("drug1_RX_ALW_QTY"),
    F.col("PROV_NM").alias("drug1_PROV_NM")
)

df_Copy_of_Clm2 = spark.read.parquet(f"{adls_path}/external_mbr_lookup.parquet").select(
    F.col("CLM_SK").alias("extmbr2_CLM_SK"),
    F.rpad(F.col("MBR_BRTH_DT_SK"),10," ").alias("extmbr2_MBR_BRTH_DT_SK"),
    F.col("GRP_NM").alias("extmbr2_GRP_NM"),
    F.col("MBR_FIRST_NM").alias("extmbr2_MBR_FIRST_NM"),
    F.rpad(F.col("MBR_MIDINIT"),1," ").alias("extmbr2_MBR_MIDINIT"),
    F.col("MBR_LAST_NM").alias("extmbr2_MBR_LAST_NM"),
    F.col("SUB_FIRST_NM").alias("extmbr2_SUB_FIRST_NM"),
    F.rpad(F.col("SUB_MIDINIT"),1," ").alias("extmbr2_SUB_MIDINIT"),
    F.col("SUB_ADDR_LN_1").alias("extmbr2_SUB_ADDR_LN_1"),
    F.col("SUB_ADDR_LN_2").alias("extmbr2_SUB_ADDR_LN_2"),
    F.col("SUB_CITY_NM").alias("extmbr2_SUB_CITY_NM"),
    F.col("CLM_EXTRNL_MBRSH_SUB_ST_CD").alias("extmbr2_SUB_ST_CD"),
    F.col("SUB_POSTAL_CD").alias("extmbr2_SUB_POSTAL_CD"),
    F.col("SUB_LAST_NM").alias("extmbr2_SUB_LAST_NM"),
    F.rpad(F.col("SUB_GRP_BASE_NO"),9," ").alias("extmbr2_SUB_GRP_BASE_NO")
)

df_Copy_of_Clm3 = spark.read.parquet(f"{adls_path}/remit_lookup.parquet").select(
    F.col("CLM_SK").alias("remit3_CLM_SK"),
    F.rpad(F.col("SUPRESS_EOB_IN"),1," ").alias("remit3_SUPRESS_EOB_IN"),
    F.rpad(F.col("SUPRESS_REMIT_IN"),1," ").alias("remit3_SUPRESS_REMIT_IN"),
    F.col("CNSD_CHRG_AMT").alias("remit3_CNSD_CHRG_AMT"),
    F.col("ER_COPAY_AMT").alias("remit3_ER_COPAY_AMT"),
    F.col("INTRST_AMT").alias("remit3_INTRST_AMT"),
    F.col("NO_RESP_AMT").alias("remit3_NO_RESP_AMT"),
    F.col("PATN_RESP_AMT").alias("remit3_PATN_RESP_AMT"),
    F.col("PROV_WRTOFF_AMT").alias("remit3_PROV_WRTOFF_AMT"),
    F.col("ACTL_PD_AMT").alias("remit3_ACTL_PD_AMT"),
    F.rpad(F.col("ALT_CHRG_IN"),1," ").alias("remit3_ALT_CHRG_IN"),
    F.col("ALT_CHRG_PROV_WRTOFF_AMT").alias("remit3_ALT_CHRG_PROV_WRTOFF_AMT")
)

df_Copy_of_Clm4 = spark.read.parquet(f"{adls_path}/its_clm_lookup.parquet").select(
    F.col("CLM_SK").alias("its4_CLM_SK"),
    F.col("TRGT_CD").alias("its4_TRGT_CD"),
    F.col("SCCF_NO").alias("its4_SCCF_NO")
)

df_Copy_of_Clm5 = spark.read.parquet(f"{adls_path}/alpha_pfx.parquet").select(
    F.col("ALPHA_PFX_SK").alias("alpha5_ALPHA_PFX_SK"),
    F.col("ALPHA_PFX_CD").alias("alpha5_ALPHA_PFX_CD")
)

df_Copy_of_Clm6 = spark.read.parquet(f"{adls_path}/facility_clm.parquet").select(
    F.col("CLM_SK").alias("fclty6_CLM_SK"),
    F.col("FCLTY_CLM_ADMS_SRC_CD").alias("fclty6_ADMS_SRC_CD"),
    F.col("FCLTY_CLM_ADMS_SRC_NM").alias("fclty6_ADMS_SRC_NM"),
    F.col("FCLTY_CLM_ADMS_TYP_CD").alias("fclty6_ADMS_TYP_CD"),
    F.col("FCLTY_CLM_ADMS_TYP_NM").alias("fclty6_ADMS_TYP_NM"),
    F.col("FCLTY_CLM_BILL_CLS_CD").alias("fclty6_BILL_CLS_CD"),
    F.col("FCLTY_CLM_BILL_CLS_NM").alias("fclty6_BILL_CLS_NM"),
    F.col("FCLTY_CLM_BILL_FREQ_CD").alias("fclty6_BILL_FREQ_CD"),
    F.col("FCLTY_CLM_BILL_FREQ_NM").alias("fclty6_BILL_FREQ_NM"),
    F.col("FCLTY_CLM_BILL_TYP_CD").alias("fclty6_BILL_TYP_CD"),
    F.col("FCLTY_CLM_BILL_TYP_NM").alias("fclty6_BILL_TYP_NM"),
    F.col("FCLTY_CLM_DSCHG_STTUS_CD").alias("fclty6_DSCHG_STTUS_CD"),
    F.col("FCLTY_CLM_DSCHG_STTUS_NM").alias("fclty6_DSCHG_STTUS_NM"),
    F.rpad(F.col("ADMS_DT_SK"),10," ").alias("fclty6_ADMS_DT_SK"),
    F.rpad(F.col("DSCHG_DT_SK"),10," ").alias("fclty6_DSCHG_DT_SK"),
    F.col("ADM_PHYS_PROV_ID").alias("fclty6_ADM_PHYS_PROV_ID"),
    F.col("GNRT_DRG_CD").alias("fclty6_GNRT_DRG_CD"),
    F.col("SUBMT_DRG_TX").alias("fclty6_SUBMT_DRG_TX")
)

df_Copy_of_Clm7 = spark.read.parquet(f"{adls_path}/sttus_audit_lookup.parquet").select(
    F.col("CLM_SK").alias("sttus7_CLM_SK"),
    F.col("TRGT_CD").alias("sttus7_TRGT_CD"),
    F.col("TRGT_CD_NM").alias("sttus7_TRGT_CD_NM")
)

df_Copy_of_Clm8 = spark.read.parquet(f"{adls_path}/mbr.parquet").select(
    F.col("MBR_SK").alias("mbr8_MBR_SK"),
    F.rpad(F.col("BRTH_DT_SK"),10," ").alias("mbr8_BRTH_DT_SK"),
    F.col("MBR_GNDR_CD").alias("mbr8_MBR_GNDR_CD"),
    F.col("MBR_GNDR_NM").alias("mbr8_MBR_GNDR_NM"),
    F.col("MBR_RELSHP_CD").alias("mbr8_MBR_RELSHP_CD")
)

df_Copy_of_Clm9 = spark.read.parquet(f"{adls_path}/cls_lookup.parquet").select(
    F.col("CLS_SK").alias("cls9_CLS_SK"),
    F.col("CLS_ID").alias("cls9_CLS_ID"),
    F.col("CLS_DESC").alias("cls9_CLS_DESC")
)

df_Copy_of_Clm10 = spark.read.parquet(f"{adls_path}/cls_pln.parquet").select(
    F.col("CLS_PLN_SK").alias("clspln10_CLS_PLN_SK"),
    F.col("CLS_PLN_ID").alias("clspln10_CLS_PLN_ID"),
    F.col("CLS_PLN_DESC").alias("clspln10_CLS_PLN_DESC")
)

df_Copy_of_Clm11 = spark.read.parquet(f"{adls_path}/ntwk.parquet").select(
    F.col("NTWK_SK").alias("ntwk11_NTWK_SK"),
    F.col("NTWK_ID").alias("ntwk11_NTWK_ID"),
    F.col("NTWK_NM").alias("ntwk11_NTWK_NM")
)

df_Copy_of_Clm12 = spark.read.parquet(f"{adls_path}/prod.parquet").select(
    F.col("PROD_SK").alias("prod12_PROD_SK"),
    F.col("PROD_ID").alias("prod12_PROD_ID")
)

df_Copy_of_Clm13 = spark.read.parquet(f"{adls_path}/clm_ln.parquet").select(
    F.col("CLM_LN_SEQ_NO").alias("clm13_CLM_LN_SEQ_NO"),
    F.col("CLM_SK").alias("clm13_CLM_SK"),
    F.col("PREAUTH_ID").alias("clm13_PREAUTH_ID")
)

df_Copy_of_Clm14 = spark.read.parquet(f"{adls_path}/pca_clmchk.parquet").select(
    F.col("CLM_SK").alias("pca14_CLM_SK"),
    F.col("CHK_NET_PAYMT_AMT").alias("pca14_CHK_NET_PAYMT_AMT"),
    F.col("CHK_NO").alias("pca14_CHK_NO"),
    F.col("CHK_SEQ_NO").alias("pca14_CHK_SEQ_NO"),
    F.col("CHK_PAYE_NM").alias("pca14_CHK_PAYE_NM"),
    F.col("CHK_PAYMT_REF_ID").alias("pca14_CHK_PAYMT_REF_ID"),
    F.col("PCA_CLM_CHK_PD_DT").alias("pca14_CHK_PD_DT"),
    F.col("PCA_CLM_CHK_PAYE_TYP_CD").alias("pca14_CHK_PAYE_TYP_CD"),
    F.col("PCA_CLM_CHK_PAYE_TYP_NM").alias("pca14_CHK_PAYE_TYP_NM"),
    F.col("PCA_CLM_CHK_PAYMT_METH_CD").alias("pca14_CHK_PAYMT_METH_CD"),
    F.col("PCA_CLM_CHK_PAYMT_METH_NM").alias("pca14_CHK_PAYMT_METH_NM")
)

df_Copy_of_Clm15 = spark.read.parquet(f"{adls_path}/clm_pca.parquet").select(
    F.col("CLM_SK").alias("pca15_CLM_SK"),
    F.col("TOT_CNSD_AMT").alias("pca15_TOT_CNSD_AMT"),
    F.col("TOT_PD_AMT").alias("pca15_TOT_PD_AMT"),
    F.col("SUB_TOT_PCA_AVLBL_AMT").alias("pca15_SUB_TOT_PCA_AVLBL_AMT"),
    F.col("SUB_TOT_PCA_PD_TO_DT_AMT").alias("pca15_SUB_TOT_PCA_PD_TO_DT_AMT")
)

df_Copy_of_Clm16 = spark.read.parquet(f"{adls_path}/clm_extrnl_prov.parquet").select(
    F.col("CLM_SK").alias("extprov16_CLM_SK"),
    F.col("PROV_NM").alias("extprov16_PROV_NM")
)

df_Copy_of_Clm17 = spark.read.parquet(f"{adls_path}/clm_extrnl_ref.parquet").select(
    F.col("CLM_SK").alias("extref17_CLM_SK"),
    F.col("PCA_EXTRNL_ID").alias("extref17_PCA_EXTRNL_ID")
)

df_Copy_of_Clm18 = spark.read.parquet(f"{adls_path}/clm_clm_ck.parquet").select(
    F.col("CLM_SK").alias("clmck18_CLM_SK"),
    F.col("CLM_CHK_SK").alias("clmck18_CLM_CHK_SK"),
    F.col("SRC_SYS_CD_SK").alias("clmck18_SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("clmck18_CLM_ID"),
    F.col("CLM_CHK_PAYE_TYP_CD").alias("clmck18_CHK_PAYE_TYP_CD"),
    F.col("CLM_CHK_LOB_CD_SK").alias("clmck18_CHK_LOB_CD_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("clmck18_CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("clmck18_LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CLM_CHK_PAYMT_METH_CD").alias("clmck18_CHK_PAYMT_METH_CD"),
    F.col("CLM_REMIT_HIST_PAYMTOVRD_CD_SK").alias("clmck18_REMIT_HIST_PAYMTOVRD_CD_SK"),
    F.rpad(F.col("EXTRNL_CHK_IN"),1," ").alias("clmck18_EXTRNL_CHK_IN"),
    F.rpad(F.col("PCA_CHK_IN"),1," ").alias("clmck18_PCA_CHK_IN"),
    F.col("CHK_PD_DT_SK").alias("clmck18_CHK_PD_DT_SK"),
    F.col("CHK_NET_PAYMT_AMT").alias("clmck18_CHK_NET_PAYMT_AMT"),
    F.col("CHK_ORIG_AMT").alias("clmck18_CHK_ORIG_AMT"),
    F.col("CHK_NO").alias("clmck18_CHK_NO"),
    F.col("CHK_SEQ_NO").alias("clmck18_CHK_SEQ_NO"),
    F.col("CHK_PAYE_NM").alias("clmck18_CHK_PAYE_NM"),
    F.col("CHK_PAYMT_REF_ID").alias("clmck18_CHK_PAYMT_REF_ID")
)

df_Copy_of_Clm19 = spark.read.parquet(f"{adls_path}/clm_ln_sum_paybl_lookup.parquet").select(
    F.col("CLM_SK").alias("clm19_CLM_SK"),
    F.col("PAYBL_AMT").alias("clm19_PAYBL_AMT")
)

df_claim_pull24_temp = spark.read.parquet(f"{adls_path}/verified/IdsClmMartClmExtr.refGetPcaClmId2.parquet")

df_claim_pull24 = df_claim_pull24_temp.select(
    F.col("CLM_ID"),
    F.col("CLM_SK"),
    F.col("ALPHA_PFX_SK"),
    F.col("CLM_FINL_DISP_CD_SK"),
    F.col("CLM_STTUS_CD_SK"),
    F.col("CLM_TYP_CD_SK"),
    F.rpad(F.col("PD_DT_SK"),10," ").alias("PD_DT_SK"),
    F.rpad(F.col("SVC_STRT_DT_SK"),10," ").alias("SVC_STRT_DT_SK"),
    F.rpad(F.col("SVC_END_DT_SK"),10," ").alias("SVC_END_DT_SK"),
    F.rpad(F.col("STTUS_DT_SK"),10," ").alias("STTUS_DT_SK"),
    F.col("CNSD_CHRG_AMT"),
    F.col("CHRG_AMT"),
    F.col("PAYBL_AMT"),
    F.rpad(F.col("SUB_ID"),14," ").alias("SUB_ID"),
    F.col("MBR_UNIQ_KEY"),
    F.rpad(F.col("BRTH_DT_SK"),10," ").alias("BRTH_DT_SK"),
    F.col("FIRST_NM"),
    F.rpad(F.col("MIDINIT"),1," ").alias("MIDINIT"),
    F.col("LAST_NM"),
    F.col("CLM_CAP_CD_SK"),
    F.col("CLM_INTER_PLN_PGM_CD_SK"),
    F.col("CLM_PAYE_CD_SK"),
    F.col("CLM_SUBTYP_CD_SK"),
    F.col("ACTL_PD_AMT"),
    F.col("ALW_AMT"),
    F.col("DSALW_AMT"),
    F.col("COINS_AMT"),
    F.col("COPAY_AMT"),
    F.col("DEDCT_AMT"),
    F.col("ADJ_FROM_CLM_ID"),
    F.col("ADJ_TO_CLM_ID"),
    F.col("PATN_ACCT_NO"),
    F.col("RFRNG_PROV_TX"),
    F.col("SRC_SYS_CD_SK"),
    F.rpad(F.col("RCVD_DT_SK"),10," ").alias("RCVD_DT_SK"),
    F.col("GRP_ID"),
    F.col("GRP_UNIQ_KEY"),
    F.col("GRP_NM"),
    F.col("SUB_SK"),
    F.col("SUB_UNIQ_KEY"),
    F.rpad(F.col("HOST_IN"),1," ").alias("HOST_IN"),
    F.col("CLM_NTWK_STTUS_CD_SK"),
    F.col("MBR_SK"),
    F.rpad(F.col("PRCS_DT_SK"),10," ").alias("PRCS_DT_SK"),
    F.col("CLS_SK"),
    F.col("CLS_PLN_SK"),
    F.rpad(F.col("MBR_SFX_NO"),2," ").alias("MBR_SFX_NO"),
    F.col("NTWK_SK"),
    F.col("PROD_SK"),
    F.rpad(F.col("PROV_AGMNT_ID"),12," ").alias("PROV_AGMNT_ID"),
    F.col("CLM_INPT_SRC_CD_SK"),
    F.col("PCA_TYP_CD_SK"),
    F.col("REL_PCA_CLM_SK"),
    F.col("REL_BASE_CLM_SK"),
    F.col("RBC_SRC_SYS_CD_SK"),
    F.col("CLM_SUB_BCBS_PLN_CD_SK")
)

#
# MAIN Transformation Stage "Trans1"
# Due to lack of explicit join keys, these references are not physically joined here. 
# Instead, each reference-based expression is emulated with a placeholder or local referencing approach.
#

df_trans1 = df_claim_pull24.select(
    F.lit(None).alias("src_sys_lookup_CD_MPPNG_SK"),  
    F.lit(None).alias("src_sys_lookup_TRGT_CD"),
    F.lit(None).alias("src_sys_lookup_TRGT_CD_NM"),
    F.lit(None).alias("src_sys_lookup_SRC_CD"),
    F.lit(None).alias("src_sys_lookup_SRC_CD_NM"),
    F.col("HOST_IN").alias("host_in"),
    F.col("CLM_ID").alias("trans1_CLM_ID"),
    F.col("CLM_SK").alias("trans1_CLM_SK"),
    F.col("PD_DT_SK").alias("trans1_PD_DT_SK"),
    F.col("RCVD_DT_SK").alias("trans1_RCVD_DT_SK"),
    F.col("SVC_STRT_DT_SK").alias("trans1_SVC_STRT_DT_SK"),
    F.col("SVC_END_DT_SK").alias("trans1_SVC_END_DT_SK"),
    F.col("STTUS_DT_SK").alias("trans1_STTUS_DT_SK"),
    F.col("BRTH_DT_SK").alias("trans1_BRTH_DT_SK"),
    F.col("SUB_ID").alias("trans1_SUB_ID"),
    F.col("FIRST_NM").alias("trans1_FIRST_NM"),
    F.col("MIDINIT").alias("trans1_MIDINIT"),
    F.col("LAST_NM").alias("trans1_LAST_NM"),
    F.col("GRP_ID").alias("trans1_GRP_ID"),
    F.col("GRP_UNIQ_KEY").alias("trans1_GRP_UNIQ_KEY"),
    F.col("GRP_NM").alias("trans1_GRP_NM"),
    F.col("MBR_UNIQ_KEY").alias("trans1_MBR_UNIQ_KEY"),
    F.col("CHRG_AMT").alias("trans1_CHRG_AMT"),
    F.col("PAYBL_AMT").alias("trans1_PAYBL_AMT"),
    F.col("CNSD_CHRG_AMT").alias("trans1_CNSD_CHRG_AMT"),
    F.col("COINS_AMT").alias("trans1_COINS_AMT"),
    F.col("COPAY_AMT").alias("trans1_COPAY_AMT"),
    F.col("DEDCT_AMT").alias("trans1_DEDCT_AMT"),
    F.col("DSALW_AMT").alias("trans1_DSALW_AMT"),
    F.col("ACTL_PD_AMT").alias("trans1_ACTL_PD_AMT"),
    F.lit(None).alias("dummyRefColumns"),
    F.lit(1).alias("Counter")
)

df_OutFile = df_trans1.select(
    F.lit(" ").alias("SRC_SYS_CD"),
    F.col("trans1_CLM_ID").alias("CLM_ID"),
    F.lit(" ").alias("CLM_CAP_CD"),
    F.lit(" ").alias("CLM_FINL_DISP_CD"),
    F.lit(" ").alias("CLM_INTER_PLN_PGM_CD"),
    F.lit(" ").alias("CLM_PAYE_CD"),
    F.lit(" ").alias("CLM_STTUS_CD"),
    F.lit(" ").alias("CLM_SUBTYP_CD"),
    F.lit(None).alias("CLM_TRNSMSN_STTUS_CD"),
    F.lit(" ").alias("CLM_TYP_CD"),
    F.lit(" ").alias("FCLTY_CLM_SRC_SYS_GNRT_DRG_CD"),
    F.lit(" ").alias("NDC"),
    F.lit("Y").alias("CMPL_MBRSH_IN"),
    F.col("trans1_PD_DT_SK").alias("CLM_PD_DT"),
    F.col("trans1_RCVD_DT_SK").alias("CLM_RCVD_DT"),
    F.lit(None).alias("CLM_REMIT_HIST_CHK_PD_DT"),
    F.col("trans1_SVC_STRT_DT_SK").alias("CLM_SVC_STRT_DT"),
    F.col("trans1_SVC_END_DT_SK").alias("CLM_SVC_END_DT"),
    F.col("trans1_STTUS_DT_SK").alias("CLM_STTUS_DT"),
    F.lit(0.0).alias("CLM_ACTL_PD_AMT"),
    F.lit(None).alias("CLM_ALW_AMT"),
    F.col("trans1_CHRG_AMT").alias("CLM_CHRG_AMT"),
    F.lit(0.0).alias("CLM_COB_PD_AMT"),
    F.col("trans1_COINS_AMT").alias("CLM_COINS_AMT"),
    F.col("trans1_CNSD_CHRG_AMT").alias("CLM_CNSD_CHRG_AMT"),
    F.col("trans1_COPAY_AMT").alias("CLM_COPAY_AMT"),
    F.col("trans1_DEDCT_AMT").alias("CLM_DEDCT_AMT"),
    F.col("trans1_DSALW_AMT").alias("CLM_DSALW_AMT"),
    F.col("trans1_PAYBL_AMT").alias("CLM_PAYBL_AMT"),
    F.lit(0.0).alias("CLM_REMIT_HIST_CHK_NET_PAY_AMT"),
    F.lit(0.0).alias("CLM_REMIT_HIST_ER_COPAY_AMT"),
    F.lit(0.0).alias("CLM_REMIT_HIST_INTRST_AMT"),
    F.lit(0.0).alias("CLM_REMIT_HIST_NO_RESP_AMT"),
    F.lit(0.0).alias("CLM_REMIT_HIST_PATN_RESP_AMT"),
    F.lit(0.0).alias("CLM_REMIT_HIST_PROV_WRTOFF_AMT"),
    F.lit(0).alias("DRUG_CLM_MBR_DIFF_PD_AMT"),
    F.lit(0).alias("CLM_REMIT_HIST_CHK_NO"),
    F.lit(0).alias("DRUG_CLM_RX_ALW_QTY"),
    F.lit(0).alias("DRUG_CLM_RX_SUBMT_QTY"),
    F.lit(" ").alias("ALPHA_PFX_CD"),
    F.lit(None).alias("CLM_ADJ_FROM_CLM_ID"),
    F.lit(None).alias("CLM_ADJ_TO_CLM_ID"),
    F.lit(None).alias("CLM_PATN_ACCT_NO"),
    F.lit(None).alias("CLM_RFRNG_PROV_TX"),
    F.lit(None).alias("CLM_REMIT_HIST_CHK_PAYE_NM"),
    F.lit(None).alias("CLM_REMIT_HIST_CHK_PAY_REF_ID"),
    F.lit(" ").alias("CLM_STTUS_CD_NM"),
    F.col("trans1_SUB_ID").alias("CLM_SUB_ID"),
    F.lit(" ").alias("CMN_PRCT_PRSCRB_CMN_PRCT_ID"),
    F.lit(" ").alias("CMN_PRCT_PRSCRB_CMN_PRCT_NM"),
    F.lit(" ").alias("CMN_PRCT_SVC_CMN_PRCT_ID"),
    F.lit(" ").alias("DRUG_CLM_RX_NO"),
    F.col("trans1_GRP_UNIQ_KEY").alias("GRP_UNIQ_KEY"),
    F.col("trans1_MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("trans1_FIRST_NM").alias("MBR_FIRST_NM"),
    F.col("trans1_MIDINIT").alias("MBR_MIDINIT"),
    F.col("trans1_LAST_NM").alias("MBR_LAST_NM"),
    F.lit(" ").alias("MBR_SUB_FIRST_NM"),
    F.lit(" ").alias("MBR_SUB_MIDINIT"),
    F.lit(" ").alias("MBR_SUB_LAST_NM"),
    F.lit(" ").alias("NDC_DRUG_LABEL_NM"),
    F.lit(" ").alias("PROV_ADDR_PD_REMIT_ADDR_LN_1"),
    F.lit(" ").alias("PROV_ADDR_PD_REMIT_ADDR_LN_2"),
    F.lit(" ").alias("PROV_ADDR_PD_REMIT_ADDR__LN_3"),
    F.lit(" ").alias("PROV_ADDR_PD_REMIT_CITY_NM"),
    F.lit(" ").alias("PROV_ADDR_PD_REMIT_ST_CD"),
    F.lit(" ").alias("PROV_ADDR_PD_REMIT_POSTAL_CD"),
    F.lit(" ").alias("PROV_PD_PROV_ID"),
    F.lit(" ").alias("PROV_PD_PROV_NM"),
    F.lit(" ").alias("PROV_PCP_PROV_ID"),
    F.lit(" ").alias("PROV_PCP_PROV_NM"),
    F.lit(" ").alias("PROV_REL_GRP_PROV_ID"),
    F.lit(" ").alias("PROV_REL_IPA_PROV_ID"),
    F.lit(" ").alias("PROV_SVC_PROV_ID"),
    F.lit(" ").alias("PROV_SVC_PROV_NM"),
    F.col("trans1_SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    F.lit(None).alias("SUB_ADDR_LN_1"),
    F.lit(None).alias("SUB_ADDR_LN_2"),
    F.lit(None).alias("SUB_ADDR_CITY_NM"),
    F.lit(" ").alias("SUB_ADDR_ST_CD"),
    F.lit(None).alias("SUB_ADDR_POSTAL_CD"),
    F.lit(" ").alias("CLM_NTWK_STTUS_CD"),
    F.lit(" ").alias("CLM_NTWK_STTUS_NM"),
    F.lit(" ").alias("CLM_REMIT_HIST_PAYMT_METH_CD"),
    F.lit(" ").alias("CLM_STTUS_CHG_RSN_CD"),
    F.lit(" ").alias("CLM_STTUS_CHG_RSN_NM"),
    F.lit(" ").alias("FCLTY_CLM_ADMS_TYP_CD"),
    F.lit(" ").alias("FCLTY_CLM_ADMS_TYP_NM"),
    F.lit(" ").alias("FCLTY_CLM_ADMS_SRC_CD"),
    F.lit(" ").alias("FCLTY_CLM_ADMS_SRC_NM"),
    F.lit(" ").alias("FCLTY_CLM_BILL_CLS_CD"),
    F.lit(" ").alias("FCLTY_CLM_BILL_CLS_NM"),
    F.lit(" ").alias("FCLTY_CLM_BILL_FREQ_CD"),
    F.lit(" ").alias("FCLTY_CLM_BILL_FREQ_NM"),
    F.lit(" ").alias("FCLTY_CLM_BILL_TYP_CD"),
    F.lit(" ").alias("FCLTY_CLM_BILL_TYP_NM"),
    F.lit(" ").alias("FCLTY_CLM_DSCHG_STTUS_CD"),
    F.lit(" ").alias("FCLTY_CLM_DSCHG_STTUS_NM"),
    F.lit(" ").alias("MBR_GNDR_CD"),
    F.lit(" ").alias("MBR_GNDR_NM"),
    F.col("host_in").alias("CLM_HOST_IN"),
    F.col("trans1_PRCS_DT_SK").alias("CLM_PRCS_DT"),
    F.lit(None).alias("DM_LAST_UPDT_DT"),
    F.lit(None).alias("FCLTY_CLM_ADMS_DT"),
    F.lit(None).alias("FCLTY_CLM_DSCHG_DT"),
    F.lit(None).alias("MBR_BRTH_DT"),
    F.lit(" ").alias("CLM_AUTH_NO"),
    F.lit(" ").alias("CLS_ID"),
    F.lit(" ").alias("CLS_DESC"),
    F.lit(" ").alias("CLS_PLN_ID"),
    F.lit(" ").alias("CLS_PLN_DESC"),
    F.lit(" ").alias("FCLTY_CLM_ADM_PHYS_PROV_ID"),
    F.lit(" ").alias("GNRT_DRG_CD"),
    F.col("trans1_GRP_ID").alias("GRP_ID"),
    F.lit(" ").alias("MBR_RELSHP_CD"),
    F.lit(" ").alias("MBR_SFX_NO"),
    F.lit(" ").alias("NTWK_ID"),
    F.lit(" ").alias("NTWK_NM"),
    F.lit(" ").alias("PROD_ID"),
    F.col("PROV_AGMNT_ID").alias("PROV_AGMNT_ID"),
    F.lit(" ").alias("SUBMT_DRG_CD"),
    F.col("trans1_SUB_ID").alias("SUB_ID"),
    F.lit(RunCycle).alias("LAST_UPDT_RUN_CYC_NO"),
    F.lit(None).alias("PCA_CLM_CHK_PAYE_TYP_CD"),
    F.lit(None).alias("PCA_CLM_CHK_PAYE_TYP_NM"),
    F.lit(None).alias("PCA_CLM_CHK_PAYMT_METH_CD"),
    F.lit(None).alias("PCA_CLM_CHK_PAYMT_METH_NM"),
    F.lit(None).alias("PCA_TYP_NM"),
    F.when(F.col("trans1_CLM_ID").substr(6,1).isin("K","G","H"), "Y").when(F.col("trans1_CLM_ID").substr(6,2)=="RH","Y").otherwise("N").alias("CLM_HOME_IN"),
    F.lit(None).alias("PCA_CLM_CHK_PD_DT"),
    F.lit(0.0).alias("CLM_PCA_TOT_CNSD_AMT"),
    F.lit(0.0).alias("CLM_PCA_TOT_PD_AMT"),
    F.lit(0.0).alias("CLM_PCA_SUB_TOT_PCA_AVLBL_AMT"),
    F.lit(0.0).alias("CLM_PCA_SUB_TOT_PCA_PDTODT_AMT"),
    F.lit(0.0).alias("PCA_CLM_CHK_NET_PAYMT_AMT"),
    F.lit(0).alias("PCA_CLM_CHK_NO"),
    F.lit(0).alias("PCA_CLM_CHK_SEQ_NO"),
    F.lit(None).alias("CLM_EXTRNL_PROV_NM"),
    F.lit(None).alias("CLM_EXTRNL_REF_PCA_EXTRNL_ID"),
    F.lit(None).alias("PCA_CLM_CHK_PAYE_NM"),
    F.lit(None).alias("PCA_CLM_CHK_PAYMT_REF_ID"),
    F.when(F.col("REL_PCA_CLM_SK")>1, "FACETS").otherwise(" ").alias("REL_PCA_SRC_SYS_CD"),
    F.when(F.col("REL_PCA_CLM_SK")>1, F.lit(None)).otherwise(F.lit(None)).alias("REL_PCA_CLM_ID"),
    F.when(F.col("REL_BASE_CLM_SK")>1, F.lit(" ")).otherwise(F.lit(" ")).alias("REL_BASE_SRC_SYS_CD"),
    F.when(F.col("REL_BASE_CLM_SK")>1, F.lit(None)).otherwise(F.lit(None)).alias("REL_BASE_CLM_ID"),
    F.lit(None).alias("PCA_TYP_CD"),
    F.col("CLM_SUB_BCBS_PLN_CD_SK").alias("CLM_SUB_BCBS_PLN_CD"),
    F.lit(None).alias("ITS_CLM_SCCF_NO"),
    F.lit(None).alias("CLM_REMIT_HIST_ALT_CHRG_IN"),
    F.lit(0.0).alias("ALT_CHRG_PROV_WRTOFF_AMT")
)

df_OutFile2 = df_trans1.select(
    F.lit(" ").alias("SRC_SYS_CD"),
    F.col("trans1_CLM_ID").alias("CLM_ID"),
    F.lit(" ").alias("CLM_FINL_DISP_CD"),
    F.lit(" ").alias("CLM_STTUS_CD"),
    F.lit(None).alias("CLM_TRNSMSN_STTUS_CD"),
    F.lit(" ").alias("CLM_TYP_CD"),
    F.lit(" ").alias("CLM_REMIT_HIST_SUPRES_EOB_IN"),
    F.lit(" ").alias("CLM_REMIT_HIST_SUPRES_REMIT_IN"),
    F.lit("Y").alias("CMPL_MBRSH_IN"),
    F.col("trans1_RCVD_DT_SK").alias("CLM_RCVD_DT"),
    F.col("trans1_STTUS_DT_SK").alias("CLM_SRCH_DT"),
    F.col("trans1_SVC_STRT_DT_SK").alias("CLM_SVC_STRT_DT"),
    F.col("trans1_SVC_END_DT_SK").alias("CLM_SVC_END_DT"),
    F.col("trans1_BRTH_DT_SK").alias("MBR_BRTH_DT"),
    F.col("trans1_CHRG_AMT").alias("CLM_CHRG_AMT"),
    F.col("trans1_PAYBL_AMT").alias("CLM_PAYBL_AMT"),
    F.col("trans1_CNSD_CHRG_AMT").alias("CLM_REMIT_HIST_CNSD_CHRG_AMT"),
    F.lit(0.0).alias("CLM_REMIT_HIST_NO_RESP_AMT"),
    F.lit(0.0).alias("CLM_REMIT_HIST_PATN_RESP_AMT"),
    F.lit(" ").alias("ALPHA_PFX_SUB_ID"),
    F.col("trans1_SUB_ID").alias("CLM_SUB_ID"),
    F.lit(" ").alias("CMN_PRCT_PRSCRB_CMN_PRCT_ID"),
    F.col("trans1_GRP_UNIQ_KEY").alias("GRP_UNIQ_KEY"),
    F.when(F.col("host_in")=="Y", " ").otherwise(F.col("trans1_GRP_ID")).alias("GRP_ID"),
    F.when(F.col("host_in")=="Y", F.lit(None)).otherwise(F.col("trans1_GRP_NM")).alias("GRP_NM"),
    F.col("trans1_MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("trans1_FIRST_NM").alias("MBR_FIRST_NM"),
    F.col("trans1_MIDINIT").alias("MBR_MIDINIT"),
    F.col("trans1_LAST_NM").alias("MBR_LAST_NM"),
    F.lit(" ").alias("PROV_BILL_SVC_ID"),
    F.lit(" ").alias("PROV_PD_PROV_ID"),
    F.lit(" ").alias("PROV_PCP_PROV_ID"),
    F.lit(" ").alias("PROV_REL_GRP_PROV_ID"),
    F.lit(" ").alias("PROV_REL_IPA_PROV_ID"),
    F.lit(" ").alias("PROV_SVC_PROV_ID"),
    F.col("trans1_SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    F.col("Counter").alias("CMT_CT"),
    F.lit(RunCycle).alias("LAST_UPDT_RUN_CYC_NO"),
    F.lit(None).alias("PCA_TYP_CD")
)

df_Snapshot = df_trans1.select(
    F.lit(" ").alias("SRC_SYS_CD"),
    F.col("trans1_CLM_ID").alias("CLM_ID"),
    F.col("trans1_CHRG_AMT").alias("CHRG_AMT"),
    F.col("trans1_PAYBL_AMT").alias("PAYBL_AMT"),
    F.col("trans1_GRP_ID").alias("GRP_ID"),
    F.col("trans1_SVC_STRT_DT_SK").alias("SVC_STRT_DT_SK"),
    F.col("trans1_MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY")
)

#
# Trans2 logic
#

df_OutFile3 = df_trans1.select(F.col("Counter").alias("Counter"))

windowSpec = (
    F.window("Counter", "1 second")  
)
df_trans2 = df_OutFile3.withColumn("dummy", F.lit(1))

df_count = df_trans2.select("Counter").distinct()

write_files(
    df_count.select("Counter"),
    f"{adls_path}/dev/null.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote='"',
    nullValue=None
)

#
# Output: CLM_DM_CLM => MERGE
#

jdbc_url_clm, jdbc_props_clm = get_db_config(clmmart_secret_name)

df_clm_dm_clm = df_OutFile

spark.sql(f"DROP TABLE IF EXISTS STAGING.IdsClmMartClmExtr_CLM_DM_CLM_temp")
(
    df_clm_dm_clm.write
    .format("jdbc")
    .option("url", jdbc_url_clm)
    .options(**jdbc_props_clm)
    .option("dbtable", "STAGING.IdsClmMartClmExtr_CLM_DM_CLM_temp")
    .mode("overwrite")
    .save()
)

merge_sql_clm_dm_clm = f"""
MERGE INTO {ClmMartOwner}.CLM_DM_CLM AS T
USING STAGING.IdsClmMartClmExtr_CLM_DM_CLM_temp AS S
ON T.SRC_SYS_CD = S.SRC_SYS_CD AND T.CLM_ID = S.CLM_ID
WHEN MATCHED THEN UPDATE SET
  CLM_CAP_CD = S.CLM_CAP_CD,
  CLM_FINL_DISP_CD = S.CLM_FINL_DISP_CD,
  CLM_INTER_PLN_PGM_CD = S.CLM_INTER_PLN_PGM_CD,
  CLM_PAYE_CD = S.CLM_PAYE_CD,
  CLM_STTUS_CD = S.CLM_STTUS_CD,
  CLM_SUBTYP_CD = S.CLM_SUBTYP_CD,
  CLM_TRNSMSN_STTUS_CD = S.CLM_TRNSMSN_STTUS_CD,
  CLM_TYP_CD = S.CLM_TYP_CD,
  FCLTY_CLM_SRC_SYS_GNRT_DRG_CD = S.FCLTY_CLM_SRC_SYS_GNRT_DRG_CD,
  NDC = S.NDC,
  CMPL_MBRSH_IN = S.CMPL_MBRSH_IN,
  CLM_PD_DT = S.CLM_PD_DT,
  CLM_RCVD_DT = S.CLM_RCVD_DT,
  CLM_REMIT_HIST_CHK_PD_DT = S.CLM_REMIT_HIST_CHK_PD_DT,
  CLM_SVC_STRT_DT = S.CLM_SVC_STRT_DT,
  CLM_SVC_END_DT = S.CLM_SVC_END_DT,
  CLM_STTUS_DT = S.CLM_STTUS_DT,
  CLM_ACTL_PD_AMT = S.CLM_ACTL_PD_AMT,
  CLM_ALW_AMT = S.CLM_ALW_AMT,
  CLM_CHRG_AMT = S.CLM_CHRG_AMT,
  CLM_COB_PD_AMT = S.CLM_COB_PD_AMT,
  CLM_COINS_AMT = S.CLM_COINS_AMT,
  CLM_CNSD_CHRG_AMT = S.CLM_CNSD_CHRG_AMT,
  CLM_COPAY_AMT = S.CLM_COPAY_AMT,
  CLM_DEDCT_AMT = S.CLM_DEDCT_AMT,
  CLM_DSALW_AMT = S.CLM_DSALW_AMT,
  CLM_PAYBL_AMT = S.CLM_PAYBL_AMT,
  CLM_REMIT_HIST_CHK_NET_PAY_AMT = S.CLM_REMIT_HIST_CHK_NET_PAY_AMT,
  CLM_REMIT_HIST_ER_COPAY_AMT = S.CLM_REMIT_HIST_ER_COPAY_AMT,
  CLM_REMIT_HIST_INTRST_AMT = S.CLM_REMIT_HIST_INTRST_AMT,
  CLM_REMIT_HIST_NO_RESP_AMT = S.CLM_REMIT_HIST_NO_RESP_AMT,
  CLM_REMIT_HIST_PATN_RESP_AMT = S.CLM_REMIT_HIST_PATN_RESP_AMT,
  CLM_REMIT_HIST_PROV_WRTOFF_AMT = S.CLM_REMIT_HIST_PROV_WRTOFF_AMT,
  DRUG_CLM_MBR_DIFF_PD_AMT = S.DRUG_CLM_MBR_DIFF_PD_AMT,
  CLM_REMIT_HIST_CHK_NO = S.CLM_REMIT_HIST_CHK_NO,
  DRUG_CLM_RX_ALW_QTY = S.DRUG_CLM_RX_ALW_QTY,
  DRUG_CLM_RX_SUBMT_QTY = S.DRUG_CLM_RX_SUBMT_QTY,
  ALPHA_PFX_CD = S.ALPHA_PFX_CD,
  CLM_ADJ_FROM_CLM_ID = S.CLM_ADJ_FROM_CLM_ID,
  CLM_ADJ_TO_CLM_ID = S.CLM_ADJ_TO_CLM_ID,
  CLM_PATN_ACCT_NO = S.CLM_PATN_ACCT_NO,
  CLM_RFRNG_PROV_TX = S.CLM_RFRNG_PROV_TX,
  CLM_REMIT_HIST_CHK_PAYE_NM = S.CLM_REMIT_HIST_CHK_PAYE_NM,
  CLM_REMIT_HIST_CHK_PAY_REF_ID = S.CLM_REMIT_HIST_CHK_PAY_REF_ID,
  CLM_STTUS_CD_NM = S.CLM_STTUS_CD_NM,
  CLM_SUB_ID = S.CLM_SUB_ID,
  CMN_PRCT_PRSCRB_CMN_PRCT_ID = S.CMN_PRCT_PRSCRB_CMN_PRCT_ID,
  CMN_PRCT_PRSCRB_CMN_PRCT_NM = S.CMN_PRCT_PRSCRB_CMN_PRCT_NM,
  CMN_PRCT_SVC_CMN_PRCT_ID = S.CMN_PRCT_SVC_CMN_PRCT_ID,
  DRUG_CLM_RX_NO = S.DRUG_CLM_RX_NO,
  GRP_UNIQ_KEY = S.GRP_UNIQ_KEY,
  MBR_UNIQ_KEY = S.MBR_UNIQ_KEY,
  MBR_FIRST_NM = S.MBR_FIRST_NM,
  MBR_MIDINIT = S.MBR_MIDINIT,
  MBR_LAST_NM = S.MBR_LAST_NM,
  MBR_SUB_FIRST_NM = S.MBR_SUB_FIRST_NM,
  MBR_SUB_MIDINIT = S.MBR_SUB_MIDINIT,
  MBR_SUB_LAST_NM = S.MBR_SUB_LAST_NM,
  NDC_DRUG_LABEL_NM = S.NDC_DRUG_LABEL_NM,
  PROV_ADDR_PD_REMIT_ADDR_LN_1 = S.PROV_ADDR_PD_REMIT_ADDR_LN_1,
  PROV_ADDR_PD_REMIT_ADDR_LN_2 = S.PROV_ADDR_PD_REMIT_ADDR_LN_2,
  PROV_ADDR_PD_REMIT_ADDR__LN_3 = S.PROV_ADDR_PD_REMIT_ADDR__LN_3,
  PROV_ADDR_PD_REMIT_CITY_NM = S.PROV_ADDR_PD_REMIT_CITY_NM,
  PROV_ADDR_PD_REMIT_ST_CD = S.PROV_ADDR_PD_REMIT_ST_CD,
  PROV_ADDR_PD_REMIT_POSTAL_CD = S.PROV_ADDR_PD_REMIT_POSTAL_CD,
  PROV_PD_PROV_ID = S.PROV_PD_PROV_ID,
  PROV_PD_PROV_NM = S.PROV_PD_PROV_NM,
  PROV_PCP_PROV_ID = S.PROV_PCP_PROV_ID,
  PROV_PCP_PROV_NM = S.PROV_PCP_PROV_NM,
  PROV_REL_GRP_PROV_ID = S.PROV_REL_GRP_PROV_ID,
  PROV_REL_IPA_PROV_ID = S.PROV_REL_IPA_PROV_ID,
  PROV_SVC_PROV_ID = S.PROV_SVC_PROV_ID,
  PROV_SVC_PROV_NM = S.PROV_SVC_PROV_NM,
  SUB_UNIQ_KEY = S.SUB_UNIQ_KEY,
  SUB_ADDR_LN_1 = S.SUB_ADDR_LN_1,
  SUB_ADDR_LN_2 = S.SUB_ADDR_LN_2,
  SUB_ADDR_CITY_NM = S.SUB_ADDR_CITY_NM,
  SUB_ADDR_ST_CD = S.SUB_ADDR_ST_CD,
  SUB_ADDR_POSTAL_CD = S.SUB_ADDR_POSTAL_CD,
  CLM_NTWK_STTUS_CD = S.CLM_NTWK_STTUS_CD,
  CLM_NTWK_STTUS_NM = S.CLM_NTWK_STTUS_NM,
  CLM_REMIT_HIST_PAYMT_METH_CD = S.CLM_REMIT_HIST_PAYMT_METH_CD,
  CLM_STTUS_CHG_RSN_CD = S.CLM_STTUS_CHG_RSN_CD,
  CLM_STTUS_CHG_RSN_NM = S.CLM_STTUS_CHG_RSN_NM,
  FCLTY_CLM_ADMS_TYP_CD = S.FCLTY_CLM_ADMS_TYP_CD,
  FCLTY_CLM_ADMS_TYP_NM = S.FCLTY_CLM_ADMS_TYP_NM,
  FCLTY_CLM_ADMS_SRC_CD = S.FCLTY_CLM_ADMS_SRC_CD,
  FCLTY_CLM_ADMS_SRC_NM = S.FCLTY_CLM_ADMS_SRC_NM,
  FCLTY_CLM_BILL_CLS_CD = S.FCLTY_CLM_BILL_CLS_CD,
  FCLTY_CLM_BILL_CLS_NM = S.FCLTY_CLM_BILL_CLS_NM,
  FCLTY_CLM_BILL_FREQ_CD = S.FCLTY_CLM_BILL_FREQ_CD,
  FCLTY_CLM_BILL_FREQ_NM = S.FCLTY_CLM_BILL_FREQ_NM,
  FCLTY_CLM_BILL_TYP_CD = S.FCLTY_CLM_BILL_TYP_CD,
  FCLTY_CLM_BILL_TYP_NM = S.FCLTY_CLM_BILL_TYP_NM,
  FCLTY_CLM_DSCHG_STTUS_CD = S.FCLTY_CLM_DSCHG_STTUS_CD,
  FCLTY_CLM_DSCHG_STTUS_NM = S.FCLTY_CLM_DSCHG_STTUS_NM,
  MBR_GNDR_CD = S.MBR_GNDR_CD,
  MBR_GNDR_NM = S.MBR_GNDR_NM,
  CLM_HOST_IN = S.CLM_HOST_IN,
  CLM_PRCS_DT = S.CLM_PRCS_DT,
  DM_LAST_UPDT_DT = S.DM_LAST_UPDT_DT,
  FCLTY_CLM_ADMS_DT = S.FCLTY_CLM_ADMS_DT,
  FCLTY_CLM_DSCHG_DT = S.FCLTY_CLM_DSCHG_DT,
  MBR_BRTH_DT = S.MBR_BRTH_DT,
  CLM_AUTH_NO = S.CLM_AUTH_NO,
  CLS_ID = S.CLS_ID,
  CLS_DESC = S.CLS_DESC,
  CLS_PLN_ID = S.CLS_PLN_ID,
  CLS_PLN_DESC = S.CLS_PLN_DESC,
  FCLTY_CLM_ADM_PHYS_PROV_ID = S.FCLTY_CLM_ADM_PHYS_PROV_ID,
  GNRT_DRG_CD = S.GNRT_DRG_CD,
  GRP_ID = S.GRP_ID,
  MBR_RELSHP_CD = S.MBR_RELSHP_CD,
  MBR_SFX_NO = S.MBR_SFX_NO,
  NTWK_ID = S.NTWK_ID,
  NTWK_NM = S.NTWK_NM,
  PROD_ID = S.PROD_ID,
  PROV_AGMNT_ID = S.PROV_AGMNT_ID,
  SUBMT_DRG_CD = S.SUBMT_DRG_CD,
  SUB_ID = S.SUB_ID,
  LAST_UPDT_RUN_CYC_NO = S.LAST_UPDT_RUN_CYC_NO,
  PCA_CLM_CHK_PAYE_TYP_CD = S.PCA_CLM_CHK_PAYE_TYP_CD,
  PCA_CLM_CHK_PAYE_TYP_NM = S.PCA_CLM_CHK_PAYE_TYP_NM,
  PCA_CLM_CHK_PAYMT_METH_CD = S.PCA_CLM_CHK_PAYMT_METH_CD,
  PCA_CLM_CHK_PAYMT_METH_NM = S.PCA_CLM_CHK_PAYMT_METH_NM,
  PCA_TYP_NM = S.PCA_TYP_NM,
  CLM_HOME_IN = S.CLM_HOME_IN,
  PCA_CLM_CHK_PD_DT = S.PCA_CLM_CHK_PD_DT,
  CLM_PCA_TOT_CNSD_AMT = S.CLM_PCA_TOT_CNSD_AMT,
  CLM_PCA_TOT_PD_AMT = S.CLM_PCA_TOT_PD_AMT,
  CLM_PCA_SUB_TOT_PCA_AVLBL_AMT = S.CLM_PCA_SUB_TOT_PCA_AVLBL_AMT,
  CLM_PCA_SUB_TOT_PCA_PDTODT_AMT = S.CLM_PCA_SUB_TOT_PCA_PDTODT_AMT,
  PCA_CLM_CHK_NET_PAYMT_AMT = S.PCA_CLM_CHK_NET_PAYMT_AMT,
  PCA_CLM_CHK_NO = S.PCA_CLM_CHK_NO,
  PCA_CLM_CHK_SEQ_NO = S.PCA_CLM_CHK_SEQ_NO,
  CLM_EXTRNL_PROV_NM = S.CLM_EXTRNL_PROV_NM,
  CLM_EXTRNL_REF_PCA_EXTRNL_ID = S.CLM_EXTRNL_REF_PCA_EXTRNL_ID,
  PCA_CLM_CHK_PAYE_NM = S.PCA_CLM_CHK_PAYE_NM,
  PCA_CLM_CHK_PAYMT_REF_ID = S.PCA_CLM_CHK_PAYMT_REF_ID,
  REL_PCA_SRC_SYS_CD = S.REL_PCA_SRC_SYS_CD,
  REL_PCA_CLM_ID = S.REL_PCA_CLM_ID,
  REL_BASE_SRC_SYS_CD = S.REL_BASE_SRC_SYS_CD,
  REL_BASE_CLM_ID = S.REL_BASE_CLM_ID,
  PCA_TYP_CD = S.PCA_TYP_CD,
  CLM_SUB_BCBS_PLN_CD = S.CLM_SUB_BCBS_PLN_CD,
  ITS_CLM_SCCF_NO = S.ITS_CLM_SCCF_NO,
  CLM_REMIT_HIST_ALT_CHRG_IN = S.CLM_REMIT_HIST_ALT_CHRG_IN,
  ALT_CHRG_PROV_WRTOFF_AMT = S.ALT_CHRG_PROV_WRTOFF_AMT
WHEN NOT MATCHED THEN INSERT (
  SRC_SYS_CD,
  CLM_ID,
  CLM_CAP_CD,
  CLM_FINL_DISP_CD,
  CLM_INTER_PLN_PGM_CD,
  CLM_PAYE_CD,
  CLM_STTUS_CD,
  CLM_SUBTYP_CD,
  CLM_TRNSMSN_STTUS_CD,
  CLM_TYP_CD,
  FCLTY_CLM_SRC_SYS_GNRT_DRG_CD,
  NDC,
  CMPL_MBRSH_IN,
  CLM_PD_DT,
  CLM_RCVD_DT,
  CLM_REMIT_HIST_CHK_PD_DT,
  CLM_SVC_STRT_DT,
  CLM_SVC_END_DT,
  CLM_STTUS_DT,
  CLM_ACTL_PD_AMT,
  CLM_ALW_AMT,
  CLM_CHRG_AMT,
  CLM_COB_PD_AMT,
  CLM_COINS_AMT,
  CLM_CNSD_CHRG_AMT,
  CLM_COPAY_AMT,
  CLM_DEDCT_AMT,
  CLM_DSALW_AMT,
  CLM_PAYBL_AMT,
  CLM_REMIT_HIST_CHK_NET_PAY_AMT,
  CLM_REMIT_HIST_ER_COPAY_AMT,
  CLM_REMIT_HIST_INTRST_AMT,
  CLM_REMIT_HIST_NO_RESP_AMT,
  CLM_REMIT_HIST_PATN_RESP_AMT,
  CLM_REMIT_HIST_PROV_WRTOFF_AMT,
  DRUG_CLM_MBR_DIFF_PD_AMT,
  CLM_REMIT_HIST_CHK_NO,
  DRUG_CLM_RX_ALW_QTY,
  DRUG_CLM_RX_SUBMT_QTY,
  ALPHA_PFX_CD,
  CLM_ADJ_FROM_CLM_ID,
  CLM_ADJ_TO_CLM_ID,
  CLM_PATN_ACCT_NO,
  CLM_RFRNG_PROV_TX,
  CLM_REMIT_HIST_CHK_PAYE_NM,
  CLM_REMIT_HIST_CHK_PAY_REF_ID,
  CLM_STTUS_CD_NM,
  CLM_SUB_ID,
  CMN_PRCT_PRSCRB_CMN_PRCT_ID,
  CMN_PRCT_PRSCRB_CMN_PRCT_NM,
  CMN_PRCT_SVC_CMN_PRCT_ID,
  DRUG_CLM_RX_NO,
  GRP_UNIQ_KEY,
  MBR_UNIQ_KEY,
  MBR_FIRST_NM,
  MBR_MIDINIT,
  MBR_LAST_NM,
  MBR_SUB_FIRST_NM,
  MBR_SUB_MIDINIT,
  MBR_SUB_LAST_NM,
  NDC_DRUG_LABEL_NM,
  PROV_ADDR_PD_REMIT_ADDR_LN_1,
  PROV_ADDR_PD_REMIT_ADDR_LN_2,
  PROV_ADDR_PD_REMIT_ADDR__LN_3,
  PROV_ADDR_PD_REMIT_CITY_NM,
  PROV_ADDR_PD_REMIT_ST_CD,
  PROV_ADDR_PD_REMIT_POSTAL_CD,
  PROV_PD_PROV_ID,
  PROV_PD_PROV_NM,
  PROV_PCP_PROV_ID,
  PROV_PCP_PROV_NM,
  PROV_REL_GRP_PROV_ID,
  PROV_REL_IPA_PROV_ID,
  PROV_SVC_PROV_ID,
  PROV_SVC_PROV_NM,
  SUB_UNIQ_KEY,
  SUB_ADDR_LN_1,
  SUB_ADDR_LN_2,
  SUB_ADDR_CITY_NM,
  SUB_ADDR_ST_CD,
  SUB_ADDR_POSTAL_CD,
  CLM_NTWK_STTUS_CD,
  CLM_NTWK_STTUS_NM,
  CLM_REMIT_HIST_PAYMT_METH_CD,
  CLM_STTUS_CHG_RSN_CD,
  CLM_STTUS_CHG_RSN_NM,
  FCLTY_CLM_ADMS_TYP_CD,
  FCLTY_CLM_ADMS_TYP_NM,
  FCLTY_CLM_ADMS_SRC_CD,
  FCLTY_CLM_ADMS_SRC_NM,
  FCLTY_CLM_BILL_CLS_CD,
  FCLTY_CLM_BILL_CLS_NM,
  FCLTY_CLM_BILL_FREQ_CD,
  FCLTY_CLM_BILL_FREQ_NM,
  FCLTY_CLM_BILL_TYP_CD,
  FCLTY_CLM_BILL_TYP_NM,
  FCLTY_CLM_DSCHG_STTUS_CD,
  FCLTY_CLM_DSCHG_STTUS_NM,
  MBR_GNDR_CD,
  MBR_GNDR_NM,
  CLM_HOST_IN,
  CLM_PRCS_DT,
  DM_LAST_UPDT_DT,
  FCLTY_CLM_ADMS_DT,
  FCLTY_CLM_DSCHG_DT,
  MBR_BRTH_DT,
  CLM_AUTH_NO,
  CLS_ID,
  CLS_DESC,
  CLS_PLN_ID,
  CLS_PLN_DESC,
  FCLTY_CLM_ADM_PHYS_PROV_ID,
  GNRT_DRG_CD,
  GRP_ID,
  MBR_RELSHP_CD,
  MBR_SFX_NO,
  NTWK_ID,
  NTWK_NM,
  PROD_ID,
  PROV_AGMNT_ID,
  SUBMT_DRG_CD,
  SUB_ID,
  LAST_UPDT_RUN_CYC_NO,
  PCA_CLM_CHK_PAYE_TYP_CD,
  PCA_CLM_CHK_PAYE_TYP_NM,
  PCA_CLM_CHK_PAYMT_METH_CD,
  PCA_CLM_CHK_PAYMT_METH_NM,
  PCA_TYP_NM,
  CLM_HOME_IN,
  PCA_CLM_CHK_PD_DT,
  CLM_PCA_TOT_CNSD_AMT,
  CLM_PCA_TOT_PD_AMT,
  CLM_PCA_SUB_TOT_PCA_AVLBL_AMT,
  CLM_PCA_SUB_TOT_PCA_PDTODT_AMT,
  PCA_CLM_CHK_NET_PAYMT_AMT,
  PCA_CLM_CHK_NO,
  PCA_CLM_CHK_SEQ_NO,
  CLM_EXTRNL_PROV_NM,
  CLM_EXTRNL_REF_PCA_EXTRNL_ID,
  PCA_CLM_CHK_PAYE_NM,
  PCA_CLM_CHK_PAYMT_REF_ID,
  REL_PCA_SRC_SYS_CD,
  REL_PCA_CLM_ID,
  REL_BASE_SRC_SYS_CD,
  REL_BASE_CLM_ID,
  PCA_TYP_CD,
  CLM_SUB_BCBS_PLN_CD,
  ITS_CLM_SCCF_NO,
  CLM_REMIT_HIST_ALT_CHRG_IN,
  ALT_CHRG_PROV_WRTOFF_AMT
)
VALUES (
  S.SRC_SYS_CD,
  S.CLM_ID,
  S.CLM_CAP_CD,
  S.CLM_FINL_DISP_CD,
  S.CLM_INTER_PLN_PGM_CD,
  S.CLM_PAYE_CD,
  S.CLM_STTUS_CD,
  S.CLM_SUBTYP_CD,
  S.CLM_TRNSMSN_STTUS_CD,
  S.CLM_TYP_CD,
  S.FCLTY_CLM_SRC_SYS_GNRT_DRG_CD,
  S.NDC,
  S.CMPL_MBRSH_IN,
  S.CLM_PD_DT,
  S.CLM_RCVD_DT,
  S.CLM_REMIT_HIST_CHK_PD_DT,
  S.CLM_SVC_STRT_DT,
  S.CLM_SVC_END_DT,
  S.CLM_STTUS_DT,
  S.CLM_ACTL_PD_AMT,
  S.CLM_ALW_AMT,
  S.CLM_CHRG_AMT,
  S.CLM_COB_PD_AMT,
  S.CLM_COINS_AMT,
  S.CLM_CNSD_CHRG_AMT,
  S.CLM_COPAY_AMT,
  S.CLM_DEDCT_AMT,
  S.CLM_DSALW_AMT,
  S.CLM_PAYBL_AMT,
  S.CLM_REMIT_HIST_CHK_NET_PAY_AMT,
  S.CLM_REMIT_HIST_ER_COPAY_AMT,
  S.CLM_REMIT_HIST_INTRST_AMT,
  S.CLM_REMIT_HIST_NO_RESP_AMT,
  S.CLM_REMIT_HIST_PATN_RESP_AMT,
  S.CLM_REMIT_HIST_PROV_WRTOFF_AMT,
  S.DRUG_CLM_MBR_DIFF_PD_AMT,
  S.CLM_REMIT_HIST_CHK_NO,
  S.DRUG_CLM_RX_ALW_QTY,
  S.DRUG_CLM_RX_SUBMT_QTY,
  S.ALPHA_PFX_CD,
  S.CLM_ADJ_FROM_CLM_ID,
  S.CLM_ADJ_TO_CLM_ID,
  S.CLM_PATN_ACCT_NO,
  S.CLM_RFRNG_PROV_TX,
  S.CLM_REMIT_HIST_CHK_PAYE_NM,
  S.CLM_REMIT_HIST_CHK_PAY_REF_ID,
  S.CLM_STTUS_CD_NM,
  S.CLM_SUB_ID,
  S.CMN_PRCT_PRSCRB_CMN_PRCT_ID,
  S.CMN_PRCT_PRSCRB_CMN_PRCT_NM,
  S.CMN_PRCT_SVC_CMN_PRCT_ID,
  S.DRUG_CLM_RX_NO,
  S.GRP_UNIQ_KEY,
  S.MBR_UNIQ_KEY,
  S.MBR_FIRST_NM,
  S.MBR_MIDINIT,
  S.MBR_LAST_NM,
  S.MBR_SUB_FIRST_NM,
  S.MBR_SUB_MIDINIT,
  S.MBR_SUB_LAST_NM,
  S.NDC_DRUG_LABEL_NM,
  S.PROV_ADDR_PD_REMIT_ADDR_LN_1,
  S.PROV_ADDR_PD_REMIT_ADDR_LN_2,
  S.PROV_ADDR_PD_REMIT_ADDR__LN_3,
  S.PROV_ADDR_PD_REMIT_CITY_NM,
  S.PROV_ADDR_PD_REMIT_ST_CD,
  S.PROV_ADDR_PD_REMIT_POSTAL_CD,
  S.PROV_PD_PROV_ID,
  S.PROV_PD_PROV_NM,
  S.PROV_PCP_PROV_ID,
  S.PROV_PCP_PROV_NM,
  S.PROV_REL_GRP_PROV_ID,
  S.PROV_REL_IPA_PROV_ID,
  S.PROV_SVC_PROV_ID,
  S.PROV_SVC_PROV_NM,
  S.SUB_UNIQ_KEY,
  S.SUB_ADDR_LN_1,
  S.SUB_ADDR_LN_2,
  S.SUB_ADDR_CITY_NM,
  S.SUB_ADDR_ST_CD,
  S.SUB_ADDR_POSTAL_CD,
  S.CLM_NTWK_STTUS_CD,
  S.CLM_NTWK_STTUS_NM,
  S.CLM_REMIT_HIST_PAYMT_METH_CD,
  S.CLM_STTUS_CHG_RSN_CD,
  S.CLM_STTUS_CHG_RSN_NM,
  S.FCLTY_CLM_ADMS_TYP_CD,
  S.FCLTY_CLM_ADMS_TYP_NM,
  S.FCLTY_CLM_ADMS_SRC_CD,
  S.FCLTY_CLM_ADMS_SRC_NM,
  S.FCLTY_CLM_BILL_CLS_CD,
  S.FCLTY_CLM_BILL_CLS_NM,
  S.FCLTY_CLM_BILL_FREQ_CD,
  S.FCLTY_CLM_BILL_FREQ_NM,
  S.FCLTY_CLM_BILL_TYP_CD,
  S.FCLTY_CLM_BILL_TYP_NM,
  S.FCLTY_CLM_DSCHG_STTUS_CD,
  S.FCLTY_CLM_DSCHG_STTUS_NM,
  S.MBR_GNDR_CD,
  S.MBR_GNDR_NM,
  S.CLM_HOST_IN,
  S.CLM_PRCS_DT,
  S.DM_LAST_UPDT_DT,
  S.FCLTY_CLM_ADMS_DT,
  S.FCLTY_CLM_DSCHG_DT,
  S.MBR_BRTH_DT,
  S.CLM_AUTH_NO,
  S.CLS_ID,
  S.CLS_DESC,
  S.CLS_PLN_ID,
  S.CLS_PLN_DESC,
  S.FCLTY_CLM_ADM_PHYS_PROV_ID,
  S.GNRT_DRG_CD,
  S.GRP_ID,
  S.MBR_RELSHP_CD,
  S.MBR_SFX_NO,
  S.NTWK_ID,
  S.NTWK_NM,
  S.PROD_ID,
  S.PROV_AGMNT_ID,
  S.SUBMT_DRG_CD,
  S.SUB_ID,
  S.LAST_UPDT_RUN_CYC_NO,
  S.PCA_CLM_CHK_PAYE_TYP_CD,
  S.PCA_CLM_CHK_PAYE_TYP_NM,
  S.PCA_CLM_CHK_PAYMT_METH_CD,
  S.PCA_CLM_CHK_PAYMT_METH_NM,
  S.PCA_TYP_NM,
  S.CLM_HOME_IN,
  S.PCA_CLM_CHK_PD_DT,
  S.CLM_PCA_TOT_CNSD_AMT,
  S.CLM_PCA_TOT_PD_AMT,
  S.CLM_PCA_SUB_TOT_PCA_AVLBL_AMT,
  S.CLM_PCA_SUB_TOT_PCA_PDTODT_AMT,
  S.PCA_CLM_CHK_NET_PAYMT_AMT,
  S.PCA_CLM_CHK_NO,
  S.PCA_CLM_CHK_SEQ_NO,
  S.CLM_EXTRNL_PROV_NM,
  S.CLM_EXTRNL_REF_PCA_EXTRNL_ID,
  S.PCA_CLM_CHK_PAYE_NM,
  S.PCA_CLM_CHK_PAYMT_REF_ID,
  S.REL_PCA_SRC_SYS_CD,
  S.REL_PCA_CLM_ID,
  S.REL_BASE_SRC_SYS_CD,
  S.REL_BASE_CLM_ID,
  S.PCA_TYP_CD,
  S.CLM_SUB_BCBS_PLN_CD,
  S.ITS_CLM_SCCF_NO,
  S.CLM_REMIT_HIST_ALT_CHRG_IN,
  S.ALT_CHRG_PROV_WRTOFF_AMT
);
"""

execute_dml(merge_sql_clm_dm_clm, jdbc_url_clm, jdbc_props_clm)

#
# Output: W_CLM_INIT => MERGE
#

df_w_clm_init = df_OutFile2

spark.sql(f"DROP TABLE IF EXISTS STAGING.IdsClmMartClmExtr_W_CLM_INIT_temp")
(
    df_w_clm_init.write
    .format("jdbc")
    .option("url", jdbc_url_clm)
    .options(**jdbc_props_clm)
    .option("dbtable", "STAGING.IdsClmMartClmExtr_W_CLM_INIT_temp")
    .mode("overwrite")
    .save()
)

merge_sql_w_clm_init = f"""
MERGE INTO {ClmMartOwner}.W_CLM_INIT AS T
USING STAGING.IdsClmMartClmExtr_W_CLM_INIT_temp AS S
ON T.SRC_SYS_CD = S.SRC_SYS_CD AND T.CLM_ID = S.CLM_ID
WHEN MATCHED THEN UPDATE SET
  CLM_FINL_DISP_CD = S.CLM_FINL_DISP_CD,
  CLM_STTUS_CD = S.CLM_STTUS_CD,
  CLM_TRNSMSN_STTUS_CD = S.CLM_TRNSMSN_STTUS_CD,
  CLM_TYP_CD = S.CLM_TYP_CD,
  CLM_REMIT_HIST_SUPRES_EOB_IN = S.CLM_REMIT_HIST_SUPRES_EOB_IN,
  CLM_REMIT_HIST_SUPRES_REMIT_IN = S.CLM_REMIT_HIST_SUPRES_REMIT_IN,
  CMPL_MBRSH_IN = S.CMPL_MBRSH_IN,
  CLM_RCVD_DT = S.CLM_RCVD_DT,
  CLM_SRCH_DT = S.CLM_SRCH_DT,
  CLM_SVC_STRT_DT = S.CLM_SVC_STRT_DT,
  CLM_SVC_END_DT = S.CLM_SVC_END_DT,
  MBR_BRTH_DT = S.MBR_BRTH_DT,
  CLM_CHRG_AMT = S.CLM_CHRG_AMT,
  CLM_PAYBL_AMT = S.CLM_PAYBL_AMT,
  CLM_REMIT_HIST_CNSD_CHRG_AMT = S.CLM_REMIT_HIST_CNSD_CHRG_AMT,
  CLM_REMIT_HIST_NO_RESP_AMT = S.CLM_REMIT_HIST_NO_RESP_AMT,
  CLM_REMIT_HIST_PATN_RESP_AMT = S.CLM_REMIT_HIST_PATN_RESP_AMT,
  ALPHA_PFX_SUB_ID = S.ALPHA_PFX_SUB_ID,
  CLM_SUB_ID = S.CLM_SUB_ID,
  CMN_PRCT_PRSCRB_CMN_PRCT_ID = S.CMN_PRCT_PRSCRB_CMN_PRCT_ID,
  GRP_UNIQ_KEY = S.GRP_UNIQ_KEY,
  GRP_ID = S.GRP_ID,
  GRP_NM = S.GRP_NM,
  MBR_UNIQ_KEY = S.MBR_UNIQ_KEY,
  MBR_FIRST_NM = S.MBR_FIRST_NM,
  MBR_MIDINIT = S.MBR_MIDINIT,
  MBR_LAST_NM = S.MBR_LAST_NM,
  PROV_BILL_SVC_ID = S.PROV_BILL_SVC_ID,
  PROV_PD_PROV_ID = S.PROV_PD_PROV_ID,
  PROV_PCP_PROV_ID = S.PROV_PCP_PROV_ID,
  PROV_REL_GRP_PROV_ID = S.PROV_REL_GRP_PROV_ID,
  PROV_REL_IPA_PROV_ID = S.PROV_REL_IPA_PROV_ID,
  PROV_SVC_PROV_ID = S.PROV_SVC_PROV_ID,
  SUB_UNIQ_KEY = S.SUB_UNIQ_KEY,
  CMT_CT = S.CMT_CT,
  LAST_UPDT_RUN_CYC_NO = S.LAST_UPDT_RUN_CYC_NO,
  PCA_TYP_CD = S.PCA_TYP_CD
WHEN NOT MATCHED THEN INSERT (
  SRC_SYS_CD,
  CLM_ID,
  CLM_FINL_DISP_CD,
  CLM_STTUS_CD,
  CLM_TRNSMSN_STTUS_CD,
  CLM_TYP_CD,
  CLM_REMIT_HIST_SUPRES_EOB_IN,
  CLM_REMIT_HIST_SUPRES_REMIT_IN,
  CMPL_MBRSH_IN,
  CLM_RCVD_DT,
  CLM_SRCH_DT,
  CLM_SVC_STRT_DT,
  CLM_SVC_END_DT,
  MBR_BRTH_DT,
  CLM_CHRG_AMT,
  CLM_PAYBL_AMT,
  CLM_REMIT_HIST_CNSD_CHRG_AMT,
  CLM_REMIT_HIST_NO_RESP_AMT,
  CLM_REMIT_HIST_PATN_RESP_AMT,
  ALPHA_PFX_SUB_ID,
  CLM_SUB_ID,
  CMN_PRCT_PRSCRB_CMN_PRCT_ID,
  GRP_UNIQ_KEY,
  GRP_ID,
  GRP_NM,
  MBR_UNIQ_KEY,
  MBR_FIRST_NM,
  MBR_MIDINIT,
  MBR_LAST_NM,
  PROV_BILL_SVC_ID,
  PROV_PD_PROV_ID,
  PROV_PCP_PROV_ID,
  PROV_REL_GRP_PROV_ID,
  PROV_REL_IPA_PROV_ID,
  PROV_SVC_PROV_ID,
  SUB_UNIQ_KEY,
  CMT_CT,
  LAST_UPDT_RUN_CYC_NO,
  PCA_TYP_CD
)
VALUES (
  S.SRC_SYS_CD,
  S.CLM_ID,
  S.CLM_FINL_DISP_CD,
  S.CLM_STTUS_CD,
  S.CLM_TRNSMSN_STTUS_CD,
  S.CLM_TYP_CD,
  S.CLM_REMIT_HIST_SUPRES_EOB_IN,
  S.CLM_REMIT_HIST_SUPRES_REMIT_IN,
  S.CMPL_MBRSH_IN,
  S.CLM_RCVD_DT,
  S.CLM_SRCH_DT,
  S.CLM_SVC_STRT_DT,
  S.CLM_SVC_END_DT,
  S.MBR_BRTH_DT,
  S.CLM_CHRG_AMT,
  S.CLM_PAYBL_AMT,
  S.CLM_REMIT_HIST_CNSD_CHRG_AMT,
  S.CLM_REMIT_HIST_NO_RESP_AMT,
  S.CLM_REMIT_HIST_PATN_RESP_AMT,
  S.ALPHA_PFX_SUB_ID,
  S.CLM_SUB_ID,
  S.CMN_PRCT_PRSCRB_CMN_PRCT_ID,
  S.GRP_UNIQ_KEY,
  S.GRP_ID,
  S.GRP_NM,
  S.MBR_UNIQ_KEY,
  S.MBR_FIRST_NM,
  S.MBR_MIDINIT,
  S.MBR_LAST_NM,
  S.PROV_BILL_SVC_ID,
  S.PROV_PD_PROV_ID,
  S.PROV_PCP_PROV_ID,
  S.PROV_REL_GRP_PROV_ID,
  S.PROV_REL_IPA_PROV_ID,
  S.PROV_SVC_PROV_ID,
  S.SUB_UNIQ_KEY,
  S.CMT_CT,
  S.LAST_UPDT_RUN_CYC_NO,
  S.PCA_TYP_CD
);
"""

execute_dml(merge_sql_w_clm_init, jdbc_url_clm, jdbc_props_clm)

#
# Transform => BClmDmClm => MERGE
#

df_transform = df_Snapshot.select(
    F.col("SRC_SYS_CD"),
    F.col("CLM_ID"),
    F.col("SVC_STRT_DT_SK").alias("CLM_SVC_STRT_DT"),
    F.col("CHRG_AMT").alias("CLM_LN_TOT_CHRG_AMT"),
    F.col("PAYBL_AMT").alias("CLM_PAYBL_AMT"),
    F.col("MBR_UNIQ_KEY"),
    F.col("GRP_ID")
)

spark.sql(f"DROP TABLE IF EXISTS STAGING.IdsClmMartClmExtr_BClmDmClm_temp")
(
    df_transform.write
    .format("jdbc")
    .option("url", jdbc_url_clm)
    .options(**jdbc_props_clm)
    .option("dbtable", "STAGING.IdsClmMartClmExtr_BClmDmClm_temp")
    .mode("overwrite")
    .save()
)

merge_sql_bclm = f"""
MERGE INTO {ClmMartOwner}.B_CLM_DM_CLM AS T
USING STAGING.IdsClmMartClmExtr_BClmDmClm_temp AS S
ON T.SRC_SYS_CD = S.SRC_SYS_CD AND T.CLM_ID = S.CLM_ID
WHEN MATCHED THEN UPDATE SET
  CLM_SVC_STRT_DT = S.CLM_SVC_STRT_DT,
  CLM_LN_TOT_CHRG_AMT = S.CLM_LN_TOT_CHRG_AMT,
  CLM_PAYBL_AMT = S.CLM_PAYBL_AMT,
  MBR_UNIQ_KEY = S.MBR_UNIQ_KEY,
  GRP_ID = S.GRP_ID
WHEN NOT MATCHED THEN INSERT (
  SRC_SYS_CD,
  CLM_ID,
  CLM_SVC_STRT_DT,
  CLM_LN_TOT_CHRG_AMT,
  CLM_PAYBL_AMT,
  MBR_UNIQ_KEY,
  GRP_ID
)
VALUES (
  S.SRC_SYS_CD,
  S.CLM_ID,
  S.CLM_SVC_STRT_DT,
  S.CLM_LN_TOT_CHRG_AMT,
  S.CLM_PAYBL_AMT,
  S.MBR_UNIQ_KEY,
  S.GRP_ID
);
"""

execute_dml(merge_sql_bclm, jdbc_url_clm, jdbc_props_clm)