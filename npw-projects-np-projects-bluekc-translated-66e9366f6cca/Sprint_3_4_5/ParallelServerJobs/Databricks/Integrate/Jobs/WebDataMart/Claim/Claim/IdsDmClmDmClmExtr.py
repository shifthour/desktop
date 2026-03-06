# Databricks notebook source
# MAGIC %md
# MAGIC """
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
# MAGIC Archana Palivela    2013-10-19        #5114                       Code converted to Parallel from Server job                                                                                    IntegrateWrhsDevl   Jag Yelavarthi        2013-12-30
# MAGIC 
# MAGIC Jag Yelavarthi       2014-01-13         #5114                        MBR_MID_INIT - Applied logic if the content of this column is just a comma change                  IntegrateWrhsDevl 
# MAGIC                                                                                            it to a Space
# MAGIC 
# MAGIC Madhavan B         2017-07-10    5788 BHI Updates           Updated Field PROV_ID length from Char(12) to Varchar(20) in CLM_PROV table                     IntegrateDev1
# MAGIC 
# MAGIC JohnAbraham        2021-11-04      US450557                  1) Updated Field CLM_ACTL_PD_AMT derivation else condtion from 0.00 to                                IntegrateDev2            Reddy Sanam       2021-11-11
# MAGIC                                                                                                Lnk_CLmDmCLm_Out.CLM_ACTL_PD_AMT in Trx stage
# MAGIC                                                                                          2) Added Copy stage after remit_hist table to handle null in lookup stage


# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Utility_DS_Integrate
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, DecimalType
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------

IDSOwner = get_widget_value('IDSOwner','')
DatamartRunCycle = get_widget_value('DatamartRunCycle','')
CommitPoint = get_widget_value('CommitPoint','')
ids_secret_name = get_widget_value('ids_secret_name','')
jdbc_url, jdbc_props = get_db_config(ids_secret_name)

# --------------------------
# Read from DB2_CLM_In
# --------------------------
query_DB2_CLM_In = f"""
SELECT 
CLM.CLM_SK,
CLM.CLM_ID,
CLM.SRC_SYS_CD_SK,
CLM.ALPHA_PFX_SK,
CLM.CLS_SK,
CLM.CLS_PLN_SK,
CLM.MBR_SK,
CLM.NTWK_SK,
CLM.PROD_SK,
CLM.SUB_SK,
CLM.CLM_INTER_PLN_PGM_CD_SK,
CLM.HOST_IN,
CLM.CLM_CAP_CD_SK,
CLM.CLM_FINL_DISP_CD_SK,
CLM.CLM_INPT_SRC_CD_SK,
CLM.CLM_NTWK_STTUS_CD_SK,
CLM.PD_DT_SK,
CLM.CLM_PAYE_CD_SK,
CLM.PRCS_DT_SK,
CLM.RCVD_DT_SK,
CLM.SVC_STRT_DT_SK,
CLM.SVC_END_DT_SK,
CLM.CLM_STTUS_CD_SK,
CLM.STTUS_DT_SK,
CLM.CLM_SUB_BCBS_PLN_CD_SK,
CLM.CLM_SUBTYP_CD_SK,
CLM.CLM_TYP_CD_SK,
CLM.ACTL_PD_AMT,
CLM.ALW_AMT,
CLM.DSALW_AMT,
CLM.COINS_AMT,
CLM.CNSD_CHRG_AMT,
CLM.COPAY_AMT,
CLM.CHRG_AMT,
CLM.DEDCT_AMT,
CLM.PAYBL_AMT,
CLM.ADJ_FROM_CLM_ID,
CLM.ADJ_TO_CLM_ID,
CLM.MBR_SFX_NO,
CLM.PATN_ACCT_NO,
CLM.PROV_AGMNT_ID,
CLM.RFRNG_PROV_TX,
CLM.SUB_ID,
CLM.PCA_TYP_CD_SK,
CLM.REL_PCA_CLM_SK,
CLM.REL_BASE_CLM_SK,
GRP.GRP_ID,
GRP.GRP_UNIQ_KEY,
GRP.GRP_NM,
SUB.SUB_UNIQ_KEY,
MBR.MBR_UNIQ_KEY,
MBR.BRTH_DT_SK,
MBR.FIRST_NM,
MBR.MIDINIT,
MBR.LAST_NM,
CLM2.SRC_SYS_CD_SK as RBC_SRC_SYS_CD_SK
FROM 
{IDSOwner}.W_WEBDM_ETL_DRVR R, 
{IDSOwner}.CLM CLM,
{IDSOwner}.MBR MBR,
{IDSOwner}.GRP GRP,
{IDSOwner}.SUB SUB,
{IDSOwner}.CLM CLM2
WHERE 
R.SRC_SYS_CD_SK = CLM.SRC_SYS_CD_SK  
AND R.CLM_ID = CLM.CLM_ID
AND CLM.MBR_SK = MBR.MBR_SK
AND CLM.GRP_SK = GRP.GRP_SK
AND CLM.SUB_SK = SUB.SUB_SK
AND CLM.REL_BASE_CLM_SK = CLM2.CLM_SK
"""
df_DB2_CLM_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_DB2_CLM_In)
    .load()
)

# --------------------------
# Read from DB2_External_Mbr
# --------------------------
query_DB2_External_Mbr = f"""
SELECT M.CLM_SK,
MBR_BRTH_DT_SK,
GRP_NM,
MBR_FIRST_NM,
MBR_MIDINIT,
MBR_LAST_NM,
SUB_GRP_BASE_NO,
SUB_FIRST_NM,
SUB_MIDINIT,
SUB_LAST_NM,
SUB_ADDR_LN_1,
SUB_ADDR_LN_2,
SUB_CITY_NM,
SUB_POSTAL_CD,
TRGT_CD as CLM_EXTRNL_MBRSH_SUB_ST_CD
FROM 
{IDSOwner}.W_WEBDM_ETL_DRVR R,
{IDSOwner}.CLM_EXTRNL_MBRSH M,
{IDSOwner}.CD_MPPNG NG1
WHERE R.SRC_SYS_CD_SK = M.SRC_SYS_CD_SK 
and R.CLM_ID  = M.CLM_ID 
and CLM_EXTRNL_MBRSH_SUB_ST_CD_SK = CD_MPPNG_SK
"""
df_DB2_External_Mbr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_DB2_External_Mbr)
    .load()
)

# --------------------------
# Read from DB2_FCLTY_CLM
# --------------------------
query_DB2_FCLTY_CLM = f"""
SELECT 
DRG.DRG_CD,
FCLTY.CLM_SK,
FCLTY.GNRT_DRG_IN
FROM 
{IDSOwner}.W_WEBDM_ETL_DRVR R,
{IDSOwner}.FCLTY_CLM FCLTY,
{IDSOwner}.DRG DRG
WHERE R.SRC_SYS_CD_SK = FCLTY.SRC_SYS_CD_SK
and R.CLM_ID = FCLTY.CLM_ID
and FCLTY.GNRT_DRG_SK = DRG.DRG_SK
"""
df_DB2_FCLTY_CLM = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_DB2_FCLTY_CLM)
    .load()
)

# --------------------------
# Read from DB2_PROV_ADDR
# --------------------------
query_DB2_PROV_ADDR = f"""
SELECT 
V.CLM_SK,
V.PROV_ID,
PROV.PROV_NM,
ADDR.ADDR_LN_1,
ADDR.ADDR_LN_2,
ADDR.ADDR_LN_3,
ADDR.CITY_NM,
ADDR.POSTAL_CD,
NG3.TRGT_CD as PROV_ADDR_ST_CD,
NG1.TRGT_CD
FROM {IDSOwner}.W_WEBDM_ETL_DRVR R,
{IDSOwner}.CLM_PROV V,
{IDSOwner}.CD_MPPNG NG1,
{IDSOwner}.PROV PROV,
{IDSOwner}.PROV_ADDR ADDR,
{IDSOwner}.PROV_LOC LOC,
{IDSOwner}.CD_MPPNG NG3
WHERE R.SRC_SYS_CD_SK = V.SRC_SYS_CD_SK 
and R.CLM_ID = V.CLM_ID
and V.CLM_PROV_ROLE_TYP_CD_SK = NG1.CD_MPPNG_SK
and V.PROV_SK = PROV.PROV_SK
and PROV.PROV_SK = LOC.PROV_SK
and LOC.REMIT_ADDR_IN = 'Y'
and LOC.PROV_ADDR_SK = ADDR.PROV_ADDR_SK
and ADDR.PROV_ADDR_ST_CD_SK = NG3.CD_MPPNG_SK
and NG1.TRGT_CD  = 'PD'
"""
df_DB2_PROV_ADDR = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_DB2_PROV_ADDR)
    .load()
)

# --------------------------
# Read from Db2_PROV_In
# --------------------------
query_Db2_PROV_In = f"""
SELECT 
DISTINCT V.CLM_SK,
PROV.PROV_ID,
PROV.REL_GRP_PROV_SK,
PROV.REL_IPA_PROV_SK,
PROV.PROV_NM,
GRPPROV.PROV_ID as GRP_PROV_ID,
IPAPROV.PROV_ID as IPA_PROV_ID,
PRCT.CMN_PRCT_SK,
PRCT.CMN_PRCT_ID,
NG1.TRGT_CD
FROM {IDSOwner}.W_WEBDM_ETL_DRVR R,
{IDSOwner}.CLM_PROV V,
{IDSOwner}.CD_MPPNG NG1, 
{IDSOwner}.PROV PROV,
{IDSOwner}.CMN_PRCT PRCT, 
{IDSOwner}.PROV GRPPROV, 
{IDSOwner}.PROV IPAPROV
WHERE R.SRC_SYS_CD_SK = V.SRC_SYS_CD_SK
and R.CLM_ID = V.CLM_ID
and V.CLM_PROV_ROLE_TYP_CD_SK = NG1.CD_MPPNG_SK
and V.PROV_SK = PROV.PROV_SK
and PROV.CMN_PRCT_SK = PRCT.CMN_PRCT_SK
and PROV.REL_GRP_PROV_SK = GRPPROV.PROV_SK
and PROV.REL_IPA_PROV_SK = IPAPROV.PROV_SK 
and  NG1.TRGT_CD = 'SVC'
"""
df_Db2_PROV_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_Db2_PROV_In)
    .load()
)

# --------------------------
# Read from DB2_DRUG_CLM
# --------------------------
query_DB2_DRUG_CLM = f"""
SELECT 
NDC.NDC,
NDC.DRUG_LABEL_NM,
PRCT.CMN_PRCT_ID,
PRCT.FIRST_NM,
PRCT.LAST_NM,
CLM.CLM_SK,
CLM.MBR_DIFF_PD_AMT,
CLM.RX_ALW_QTY,
CLM.RX_SUBMT_QTY,
CLM.RX_NO,
DEA.PROV_NM
FROM 
{IDSOwner}.W_WEBDM_ETL_DRVR R,
{IDSOwner}.DRUG_CLM CLM,
{IDSOwner}.NDC NDC,
{IDSOwner}.PROV_DEA DEA,
{IDSOwner}.CMN_PRCT PRCT
WHERE R.SRC_SYS_CD_SK = CLM.SRC_SYS_CD_SK
and R.CLM_ID = CLM.CLM_ID
and CLM.NDC_SK = NDC.NDC_SK
and CLM.PRSCRB_PROV_DEA_SK = DEA.PROV_DEA_SK
and DEA.CMN_PRCT_SK = PRCT.CMN_PRCT_SK
"""
df_DB2_DRUG_CLM = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_DB2_DRUG_CLM)
    .load()
)

# --------------------------
# Read from DB2_ITS_CLM
# --------------------------
query_DB2_ITS_CLM = f"""
SELECT 
CLM.CLM_SK,
SCCF_NO,
TRGT_CD
FROM 
{IDSOwner}.W_WEBDM_ETL_DRVR R,
{IDSOwner}.ITS_CLM CLM,
{IDSOwner}.CD_MPPNG CD_MPPNG
WHERE R.SRC_SYS_CD_SK = CLM.SRC_SYS_CD_SK
and R.CLM_ID = CLM.CLM_ID
and CLM.TRNSMSN_SRC_CD_SK = CD_MPPNG.CD_MPPNG_SK
"""
df_DB2_ITS_CLM = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_DB2_ITS_CLM)
    .load()
)

# --------------------------
# Read from DB2_STTUS_AUDIT
# --------------------------
query_DB2_STTUS_AUDIT = f"""
SELECT 
B.CLM_SK as CLM_SK,
B.TRGT_CD as TRGT_CD,
B.TRGT_CD_NM as TRGT_CD_NM
FROM 
(
  SELECT
  A.CLM_SK,
  A.CLM_STTUS_AUDIT_SEQ_NO,
  DENSE_RANK() OVER (PARTITION BY A.CLM_SK ORDER BY A.CLM_STTUS_AUDIT_SEQ_NO desc) AS RANK_DATA,
  CD_MPPNG.TRGT_CD,
  CD_MPPNG.TRGT_CD_NM
  FROM 
  {IDSOwner}.W_WEBDM_ETL_DRVR R,
  {IDSOwner}.CLM_STTUS_AUDIT A,
  {IDSOwner}.CD_MPPNG CD_MPPNG
  WHERE R.SRC_SYS_CD_SK = A.SRC_SYS_CD_SK
  and R.CLM_ID = A.CLM_ID
  and A.CLM_STTUS_CHG_RSN_CD_SK = CD_MPPNG.CD_MPPNG_SK
) B
WHERE B.RANK_DATA = 1
"""
df_DB2_STTUS_AUDIT = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_DB2_STTUS_AUDIT)
    .load()
)

# --------------------------
# Read from DB2_Facility_Clm
# --------------------------
query_DB2_Facility_Clm = f"""
SELECT 
FCLTY.CLM_SK,
FCLTY.ADMS_DT_SK,
FCLTY.DSCHG_DT_SK,
FCLTY.ADM_PHYS_PROV_ID,
FCLTY.SUBMT_DRG_TX,
DRG.DRG_CD as GNRT_DRG_CD,
MAP1.TRGT_CD as FCLTY_CLM_ADMS_SRC_CD,
MAP1.TRGT_CD_NM as FCLTY_CLM_ADMS_SRC_NM,
MAP2.TRGT_CD  as FCLTY_CLM_ADMS_TYP_CD,
MAP2.TRGT_CD_NM as FCLTY_CLM_ADMS_TYP_NM,
MAP3.TRGT_CD as FCLTY_CLM_BILL_CLS_CD,
MAP3.TRGT_CD_NM as FCLTY_CLM_BILL_CLS_NM,
MAP4.TRGT_CD as FCLTY_CLM_BILL_FREQ_CD,
MAP4.TRGT_CD_NM as FCLTY_CLM_BILL_FREQ_NM,
MAP5.TRGT_CD as FCLTY_CLM_BILL_TYP_CD,
MAP5.TRGT_CD_NM as FCLTY_CLM_BILL_TYP_NM,
MAP6.TRGT_CD as FCLTY_CLM_DSCHG_STTUS_CD,
MAP6.TRGT_CD_NM as FCLTY_CLM_DSCHG_STTUS_NM
FROM {IDSOwner}.W_WEBDM_ETL_DRVR DRVR,
{IDSOwner}.FCLTY_CLM FCLTY,
{IDSOwner}.CD_MPPNG MAP1,
{IDSOwner}.CD_MPPNG MAP2,
{IDSOwner}.CD_MPPNG MAP3,
{IDSOwner}.CD_MPPNG MAP4,
{IDSOwner}.CD_MPPNG MAP5,
{IDSOwner}.CD_MPPNG MAP6,
{IDSOwner}.DRG DRG
WHERE  
DRVR.SRC_SYS_CD_SK = FCLTY.SRC_SYS_CD_SK
and DRVR.CLM_ID = FCLTY.CLM_ID
and FCLTY.FCLTY_CLM_ADMS_SRC_CD_SK=MAP1.CD_MPPNG_SK
and FCLTY.FCLTY_CLM_ADMS_TYP_CD_SK=MAP2.CD_MPPNG_SK
and FCLTY.FCLTY_CLM_BILL_CLS_CD_SK=MAP3.CD_MPPNG_SK
and FCLTY.FCLTY_CLM_BILL_FREQ_CD_SK=MAP4.CD_MPPNG_SK
and FCLTY.FCLTY_CLM_BILL_TYP_CD_SK=MAP5.CD_MPPNG_SK
and FCLTY.FCLTY_CLM_DSCHG_STTUS_CD_SK=MAP6.CD_MPPNG_SK
and FCLTY.GNRT_DRG_SK=DRG.DRG_SK
"""
df_DB2_Facility_Clm = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_DB2_Facility_Clm)
    .load()
)

# --------------------------
# Read from DB2_CLM_COB
# --------------------------
query_DB2_CLM_COB = f"""
SELECT 
CLM.CLM_SK,
CLM.PD_AMT
FROM 
{IDSOwner}.W_WEBDM_ETL_DRVR R,
{IDSOwner}.CLM_COB CLM
WHERE R.SRC_SYS_CD_SK = CLM.SRC_SYS_CD_SK
and R.CLM_ID = CLM.CLM_ID
"""
df_DB2_CLM_COB = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_DB2_CLM_COB)
    .load()
)

# --------------------------
# Read from DB2_CLM_LN
# --------------------------
query_DB2_CLM_LN = f"""
SELECT 
B.CLM_LN_SEQ_NO as CLM_LN_SEQ_NO,
B.CLM_SK as CLM_SK,
B.PREAUTH_ID as PREAUTH_ID
FROM 
(
  SELECT 
  A.CLM_SK,
  A.PREAUTH_ID,
  A.CLM_LN_SEQ_NO,
  DENSE_RANK() OVER (PARTITION BY A.CLM_SK ORDER BY A.CLM_LN_SEQ_NO asc) AS RANK_DATA
  FROM 
  {IDSOwner}.W_WEBDM_ETL_DRVR R,
  {IDSOwner}.CLM_LN A
  WHERE R.SRC_SYS_CD_SK = A.SRC_SYS_CD_SK
  and R.CLM_ID = A.CLM_ID
) B
WHERE B.RANK_DATA = 1
"""
df_DB2_CLM_LN = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_DB2_CLM_LN)
    .load()
)

# --------------------------
# Read from DB2_CLM_EXTRNL_REF
# --------------------------
query_DB2_CLM_EXTRNL_REF = f"""
SELECT 
CLM.CLM_SK,
PCA_EXTRNL_ID
FROM {IDSOwner}.W_WEBDM_ETL_DRVR DRVR,
{IDSOwner}.CLM CLM,
{IDSOwner}.CLM_EXTRNL_REF_DATA REF
WHERE DRVR.CLM_ID = CLM.CLM_ID 
AND CLM.CLM_SK = REF.CLM_SK
"""
df_DB2_CLM_EXTRNL_REF = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_DB2_CLM_EXTRNL_REF)
    .load()
)

# --------------------------
# Read from DB2_CLM_PCA
# --------------------------
query_DB2_CLM_PCA = f"""
SELECT CLM.CLM_SK,
TOT_CNSD_AMT,
TOT_PD_AMT,
SUB_TOT_PCA_AVLBL_AMT,
SUB_TOT_PCA_PD_TO_DT_AMT
FROM
{IDSOwner}.CLM CLM,
{IDSOwner}.CLM_PCA PCA,
{IDSOwner}.W_WEBDM_ETL_DRVR DRVR
WHERE CLM.CLM_SK = PCA.CLM_SK 
and DRVR.CLM_ID = CLM.CLM_ID
"""
df_DB2_CLM_PCA = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_DB2_CLM_PCA)
    .load()
)

# --------------------------
# Read from DB2_PCA_CLMCHK
# --------------------------
query_DB2_PCA_CLMCHK = f"""
SELECT 
CLM.CLM_SK,
CLMCHK.CHK_NET_PAYMT_AMT,
CLMCHK.CHK_NO,
CLMCHK.CHK_SEQ_NO,
CLMCHK.CHK_PAYE_NM,
CLMCHK.CHK_PAYMT_REF_ID,
DT.CLNDR_DT as PCA_CLM_CHK_PD_DT,
CD_MPPNG2.TRGT_CD as PCA_CLM_CHK_PAYE_TYP_CD,
CD_MPPNG2.TRGT_CD_NM as PCA_CLM_CHK_PAYE_TYP_NM,
CD_MPPNG3.TRGT_CD as PCA_CLM_CHK_PAYMT_METH_CD,
CD_MPPNG3.TRGT_CD_NM as PCA_CLM_CHK_PAYMT_METH_NM
FROM
{IDSOwner}.W_WEBDM_ETL_DRVR DRVR,
{IDSOwner}.CLM CLM,
{IDSOwner}.CLM_CHK CLMCHK,
{IDSOwner}.CLNDR_DT DT,
{IDSOwner}.CD_MPPNG CD_MPPNG,
{IDSOwner}.CD_MPPNG CD_MPPNG2,
{IDSOwner}.CD_MPPNG CD_MPPNG3
WHERE CLM.CLM_SK = CLMCHK.CLM_SK 
and CLMCHK.PCA_CHK_IN = 'Y'
and DRVR.CLM_ID = CLM.CLM_ID
AND CLMCHK.CHK_PD_DT_SK = DT.CLNDR_DT_SK
and CLMCHK.CLM_CHK_LOB_CD_SK = CD_MPPNG.CD_MPPNG_SK
and CLMCHK.CLM_CHK_PAYE_TYP_CD_SK = CD_MPPNG2.CD_MPPNG_SK
and CLMCHK.CLM_CHK_PAYMT_METH_CD_SK = CD_MPPNG3.CD_MPPNG_SK
"""
df_DB2_PCA_CLMCHK = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_DB2_PCA_CLMCHK)
    .load()
)

# --------------------------
# Read from DB2_Extrnl_Prov
# --------------------------
query_DB2_Extrnl_Prov = f"""
SELECT 
CLM.CLM_SK,
PROV_NM
FROM {IDSOwner}.W_WEBDM_ETL_DRVR DRVR, 
{IDSOwner}.CLM CLM, 
{IDSOwner}.CLM_EXTRNL_PROV PROV
WHERE DRVR.CLM_ID = CLM.CLM_ID 
AND CLM.CLM_SK = PROV.CLM_SK
"""
df_DB2_Extrnl_Prov = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_DB2_Extrnl_Prov)
    .load()
)

# --------------------------
# Read from DB2_CLM_CLM_CH
# --------------------------
query_DB2_CLM_CLM_CH = f"""
SELECT 
DISTINCT
CLMCK.CLM_CHK_SK,
CLMCK.SRC_SYS_CD_SK,
CLMCK.CLM_ID,
CLMCK.CLM_CHK_LOB_CD_SK,
CLMCK.CRT_RUN_CYC_EXCTN_SK,
CLMCK.LAST_UPDT_RUN_CYC_EXCTN_SK,
CLMCK.CLM_SK,
CLMCK.CLM_REMIT_HIST_PAYMTOVRD_CD_SK,
CLMCK.EXTRNL_CHK_IN,
CLMCK.PCA_CHK_IN,
CLMCK.CHK_NET_PAYMT_AMT,
CLMCK.CHK_ORIG_AMT,
CLMCK.CHK_NO,
CLMCK.CHK_SEQ_NO,
CLMCK.CHK_PAYE_NM,
CLMCK.CHK_PAYMT_REF_ID,
CD_MPPNG2.TRGT_CD as CLM_CHK_PAYE_TYP_CD,
CD_MPPNG.TRGT_CD as  CLM_CHK_PAYMT_METH_CD,
DT.CLNDR_DT as CHK_PD_DT_SK
FROM {IDSOwner}.W_WEBDM_ETL_DRVR DRVR,
{IDSOwner}.CLM CLM,
{IDSOwner}.CLM_CHK CLMCK,
{IDSOwner}.CLNDR_DT DT,
{IDSOwner}.CD_MPPNG CD_MPPNG,
{IDSOwner}.CD_MPPNG CD_MPPNG2
WHERE CLM.CLM_SK = CLMCK.CLM_SK 
and CLMCK.PCA_CHK_IN = 'N'
and DRVR.CLM_ID = CLM.CLM_ID 
AND CLMCK.CHK_PD_DT_SK = DT.CLNDR_DT_SK
and CLMCK.CLM_CHK_PAYMT_METH_CD_SK = CD_MPPNG.CD_MPPNG_SK
AND CLMCK.CLM_CHK_PAYE_TYP_CD_SK = CD_MPPNG2.CD_MPPNG_SK
"""
df_DB2_CLM_CLM_CH = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_DB2_CLM_CLM_CH)
    .load()
)

# --------------------------
# Read from DB2_CLM_LN_SUM_PBL
# --------------------------
query_DB2_CLM_LN_SUM_PBL = f"""
SELECT 
DISTINCT 
CLMLN.CLM_SK,
SUM(CLMLN.PAYBL_AMT) as PAYBL_AMT
FROM {IDSOwner}.CLM CLM,
{IDSOwner}.CLM_LN CLMLN,
{IDSOwner}.W_WEBDM_ETL_DRVR DRVR,
{IDSOwner}.CD_MPPNG MAP
WHERE CLM.CLM_SK = CLMLN.CLM_SK
AND DRVR.CLM_ID = CLM.CLM_ID
AND MAP.TRGT_CD IN ('RUNOUT','PCA','EMPWBNF')
AND CLM.PCA_TYP_CD_SK = MAP.CD_MPPNG_SK
GROUP BY CLMLN.CLM_SK
"""
df_DB2_CLM_LN_SUM_PBL = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_DB2_CLM_LN_SUM_PBL)
    .load()
)

# --------------------------
# Read from DB2_PROV
# --------------------------
query_DB2_PROV = f"""
SELECT DISTINCT
V.CLM_SK, 
V.PROV_ID, 
PROV.PROV_SK, 
REL_GRP_PROV_SK, 
PROV.PROV_NM,
NG1.TRGT_CD
FROM {IDSOwner}.W_WEBDM_ETL_DRVR R,
{IDSOwner}.CLM_PROV V,
{IDSOwner}.CD_MPPNG NG1,
{IDSOwner}.PROV PROV
WHERE R.SRC_SYS_CD_SK = V.SRC_SYS_CD_SK 
AND R.CLM_ID = V.CLM_ID 
AND V.CLM_PROV_ROLE_TYP_CD_SK = NG1.CD_MPPNG_SK
AND V.PROV_SK = PROV.PROV_SK
"""
df_DB2_PROV = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_DB2_PROV)
    .load()
)

# --------------------------
# Flt_Prov
# --------------------------
# Condition 1 -> OutputLink=1 -> TRGT_CD='PD' -> Lnk_Pcp_Prov
df_Flt_Prov_out1 = df_DB2_PROV.filter(F.col("TRGT_CD") == 'PD')
df_Lnk_Pcp_Prov = df_Flt_Prov_out1.select(
    F.col("CLM_SK").alias("CLM_SK"),
    F.col("PROV_ID").alias("PROV_ID"),
    F.col("PROV_SK").alias("PROV_SK"),
    F.col("REL_GRP_PROV_SK").alias("REL_GRP_PROV_SK"),
    F.col("PROV_NM").alias("PROV_NM"),
    F.col("TRGT_CD").alias("TRGT_CD")
)

# Condition 2 -> OutputLink=0 -> TRGT_CD='PCP' -> Lnk_Pd_Prov
df_Flt_Prov_out0 = df_DB2_PROV.filter(F.col("TRGT_CD") == 'PCP')
df_Lnk_Pd_Prov = df_Flt_Prov_out0.select(
    F.col("CLM_SK").alias("CLM_SK"),
    F.col("PROV_ID").alias("PROV_ID"),
    F.col("PROV_SK").alias("PROV_SK"),
    F.col("REL_GRP_PROV_SK").alias("REL_GRP_PROV_SK"),
    F.col("PROV_NM").alias("PROV_NM"),
    F.col("TRGT_CD").alias("TRGT_CD")
)

# --------------------------
# Read from DB2_Bill_Svc_Grp
# --------------------------
query_DB2_Bill_Svc_Grp = f"""
SELECT 
B.CLM_SK as CLM_SK,
B.PROV_BILL_SVC_ID as PROV_BILL_SVC_ID,
B.TRGT_CD as TRGT_CD
FROM 
(
  SELECT 
  CLM.CLM_SK,
  PROV_BILL_SVC_ID,
  NG1.TRGT_CD,
  ASSOC.EFF_DT_SK,
  DENSE_RANK() OVER (PARTITION BY CLM.CLM_SK ORDER BY ASSOC.EFF_DT_SK desc) AS RANK_DATA
  FROM {IDSOwner}.W_WEBDM_ETL_DRVR R,
       {IDSOwner}.CLM_PROV V,
       {IDSOwner}.CD_MPPNG NG1,
       {IDSOwner}.PROV PROV,
       {IDSOwner}.PROV_BILL_SVC_ASSOC ASSOC,
       {IDSOwner}.CLM CLM
  WHERE R.SRC_SYS_CD_SK = V.SRC_SYS_CD_SK
      AND R.CLM_ID = V.CLM_ID
      AND V.CLM_PROV_ROLE_TYP_CD_SK = NG1.CD_MPPNG_SK
      AND V.PROV_SK = PROV.PROV_SK
      AND PROV.REL_GRP_PROV_SK = ASSOC.PROV_SK
      AND CLM.SVC_STRT_DT_SK between ASSOC.EFF_DT_SK and ASSOC.END_DT_SK
      AND CLM.SRC_SYS_CD_SK = R.SRC_SYS_CD_SK
      AND CLM.CLM_ID = R.CLM_ID
) B
WHERE B.RANK_DATA = 1
"""
df_DB2_Bill_Svc_Grp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_DB2_Bill_Svc_Grp)
    .load()
)

# --------------------------
# Flt_Bill_SVc_Grp
# --------------------------
# Condition 1 -> OutputLink=0 -> TRGT_CD='PD' -> Lnk_Bill_Svc_Grp
df_Flt_Bill_SVc_Grp_out0 = df_DB2_Bill_Svc_Grp.filter(F.col("TRGT_CD") == 'PD')
df_Lnk_Bill_Svc_Grp = df_Flt_Bill_SVc_Grp_out0.select(
    F.col("CLM_SK").alias("CLM_SK"),
    F.col("PROV_BILL_SVC_ID").alias("PROV_BILL_SVC_ID"),
    F.col("TRGT_CD").alias("TRGT_CD")
)
# Condition 2 -> OutputLink=1 -> TRGT_CD='SVC' -> Lnk_Bill_Svc_Grp_Svc
df_Flt_Bill_SVc_Grp_out1 = df_DB2_Bill_Svc_Grp.filter(F.col("TRGT_CD") == 'SVC')
df_Lnk_Bill_Svc_Grp_Svc = df_Flt_Bill_SVc_Grp_out1.select(
    F.col("CLM_SK").alias("CLM_SK"),
    F.col("PROV_BILL_SVC_ID").alias("PROV_BILL_SVC_ID"),
    F.col("TRGT_CD").alias("TRGT_CD")
)

# --------------------------
# Read from DB2_BILL_SVC_PROV
# --------------------------
query_DB2_BILL_SVC_PROV = f"""
SELECT 
B.CLM_SK as CLM_SK,
B.PROV_BILL_SVC_ID as PROV_BILL_SVC_ID,
B.TRGT_CD as TRGT_CD
FROM 
(
  SELECT 
  CLM.CLM_SK,
  PROV_BILL_SVC_ID,
  NG1.TRGT_CD,
  ASSOC.EFF_DT_SK,
  DENSE_RANK() OVER (PARTITION BY CLM.CLM_SK ORDER BY ASSOC.EFF_DT_SK desc) AS RANK_DATA
  FROM {IDSOwner}.W_WEBDM_ETL_DRVR R, 
       {IDSOwner}.CLM_PROV V,
       {IDSOwner}.CD_MPPNG NG1,
       {IDSOwner}.PROV_BILL_SVC_ASSOC ASSOC,
       {IDSOwner}.CLM CLM
  WHERE R.SRC_SYS_CD_SK = V.SRC_SYS_CD_SK
      AND R.CLM_ID = V.CLM_ID
      AND V.CLM_PROV_ROLE_TYP_CD_SK = NG1.CD_MPPNG_SK
      AND V.PROV_SK = ASSOC.PROV_SK
      AND CLM.SVC_STRT_DT_SK between ASSOC.EFF_DT_SK and ASSOC.END_DT_SK
      AND CLM.SRC_SYS_CD_SK = R.SRC_SYS_CD_SK
      AND CLM.CLM_ID = R.CLM_ID
) B
WHERE B.RANK_DATA = 1
"""
df_DB2_BILL_SVC_PROV = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_DB2_BILL_SVC_PROV)
    .load()
)

# --------------------------
# Filter_Bill_SVC_PROV
# --------------------------
# Condition 1 -> OutputLink=0 -> TRGT_CD='PD' -> Lnk_Bill_Svc_Prov
df_Filter_Bill_SVC_PROV_out0 = df_DB2_BILL_SVC_PROV.filter(F.col("TRGT_CD") == 'PD')
df_Lnk_Bill_Svc_Prov = df_Filter_Bill_SVC_PROV_out0.select(
    F.col("CLM_SK").alias("CLM_SK"),
    F.col("PROV_BILL_SVC_ID").alias("PROV_BILL_SVC_ID"),
    F.col("TRGT_CD").alias("TRGT_CD")
)
# Condition 2 -> OutputLink=1 -> TRGT_CD='SVC' -> Lnk_Bill_Svc_Prov_Svu
df_Filter_Bill_SVC_PROV_out1 = df_DB2_BILL_SVC_PROV.filter(F.col("TRGT_CD") == 'SVC')
df_Lnk_Bill_Svc_Prov_Svu = df_Filter_Bill_SVC_PROV_out1.select(
    F.col("CLM_SK").alias("CLM_SK"),
    F.col("PROV_BILL_SVC_ID").alias("PROV_BILL_SVC_ID"),
    F.col("TRGT_CD").alias("TRGT_CD")
)

# --------------------------
# Read from DB2_REMIT_HIST
# --------------------------
query_DB2_REMIT_HIST = f"""
SELECT 
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
{IDSOwner}.W_WEBDM_ETL_DRVR DRVR, 
{IDSOwner}.CLM_REMIT_HIST CLM_REMIT
WHERE 
DRVR.SRC_SYS_CD_SK = CLM_REMIT.SRC_SYS_CD_SK 
and DRVR.CLM_ID = CLM_REMIT.CLM_ID
"""
df_DB2_REMIT_HIST = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_DB2_REMIT_HIST)
    .load()
)

# --------------------------
# cp_remit_hist (PxCopy)
# --------------------------
df_cp_remit_hist = df_DB2_REMIT_HIST.select(
    F.col("CLM_SK").alias("CLM_SK"),
    F.col("SUPRESS_EOB_IN").alias("SUPRESS_EOB_IN"),
    F.col("SUPRESS_REMIT_IN").alias("SUPRESS_REMIT_IN"),
    F.col("CNSD_CHRG_AMT").alias("CNSD_CHRG_AMT"),
    F.col("ER_COPAY_AMT").alias("ER_COPAY_AMT"),
    F.col("INTRST_AMT").alias("INTRST_AMT"),
    F.col("NO_RESP_AMT").alias("NO_RESP_AMT"),
    F.col("PATN_RESP_AMT").alias("PATN_RESP_AMT"),
    F.col("PROV_WRTOFF_AMT").alias("PROV_WRTOFF_AMT"),
    F.col("ACTL_PD_AMT").alias("ACTL_PD_AMT"),
    F.col("ALT_CHRG_IN").alias("ALT_CHRG_IN"),
    F.col("ALT_CHRG_PROV_WRTOFF_AMT").alias("ALT_CHRG_PROV_WRTOFF_AMT")
)
df_Lnk_Remit_lkp = df_cp_remit_hist

# --------------------------
# Build the big join for nta
# --------------------------
df_nta_tmp = (
    df_DB2_CLM_In.alias("DSLink3")
    .join(df_Db2_PROV_In.alias("Svc_Prov_Lookup"), F.col("DSLink3.CLM_SK")==F.col("Svc_Prov_Lookup.CLM_SK"), "left")
    .join(df_DB2_FCLTY_CLM.alias("Fclty_Lookup"), F.col("DSLink3.CLM_SK")==F.col("Fclty_Lookup.CLM_SK"), "left")
    .join(df_DB2_DRUG_CLM.alias("Lnk_drug_lkp"), F.col("DSLink3.CLM_SK")==F.col("Lnk_drug_lkp.CLM_SK"), "left")
    .join(df_DB2_CLM_COB.alias("Lnk_Cob_Lookpup"), F.col("DSLink3.CLM_SK")==F.col("Lnk_Cob_Lookpup.CLM_SK"), "left")
    .join(df_Lnk_Remit_lkp.alias("Lnk_Remit_lkp"), F.col("DSLink3.CLM_SK")==F.col("Lnk_Remit_lkp.CLM_SK"), "left")
    .join(df_DB2_External_Mbr.alias("Lnk_External_mbr_lkp_in"), F.col("DSLink3.CLM_SK")==F.col("Lnk_External_mbr_lkp_in.CLM_SK"), "left")
    .join(df_Lnk_Bill_Svc_Prov.alias("Lnk_Bill_Svc_Prov"), F.col("DSLink3.CLM_SK")==F.col("Lnk_Bill_Svc_Prov.CLM_SK"), "left")
    .join(df_Lnk_Bill_Svc_Grp.alias("Lnk_Bill_Svc_Grp"), F.col("DSLink3.CLM_SK")==F.col("Lnk_Bill_Svc_Grp.CLM_SK"), "left")
    .join(df_DB2_ITS_CLM.alias("Lnk_Its_CLm_Lkp"), F.col("DSLink3.CLM_SK")==F.col("Lnk_Its_CLm_Lkp.CLM_SK"), "left")
    .join(df_DB2_CLM_CLM_CH.alias("Lnk_Clm_CLm_CK_lkp"), F.col("DSLink3.CLM_SK")==F.col("Lnk_Clm_CLm_CK_lkp.CLM_SK"), "left")
    .join(df_Lnk_Bill_Svc_Prov_Svu.alias("Lnk_Bill_Svc_Prov_Svu"), F.col("DSLink3.CLM_SK")==F.col("Lnk_Bill_Svc_Prov_Svu.CLM_SK"), "left")
    .join(df_Lnk_Bill_Svc_Grp_Svc.alias("Lnk_Bill_Svc_Grp_Svc"), F.col("DSLink3.CLM_SK")==F.col("Lnk_Bill_Svc_Grp_Svc.CLM_SK"), "left")
)

# Also include other references from the JSON that appear in the "ReferenceKeys" but not in the chain above:
df_nta_tmp2 = (
    df_nta_tmp
    .join(df_DB2_STTUS_AUDIT.alias("Lnk_Sttus_Audit_Lkp"), F.col("DSLink3.CLM_SK")==F.col("Lnk_Sttus_Audit_Lkp.CLM_SK"), "left")
    .join(df_DB2_Facility_Clm.alias("Lnk_Fclty_Clm_Lkp"), F.col("DSLink3.CLM_SK")==F.col("Lnk_Fclty_Clm_Lkp.CLM_SK"), "left")
    .join(df_DB2_CLM_LN.alias("Lnk_Clm_In_Lkp"), F.col("DSLink3.CLM_SK")==F.col("Lnk_Clm_In_Lkp.CLM_SK"), "left")
    .join(df_DB2_CLM_EXTRNL_REF.alias("Lnk_Clm_Pca_lkp"), F.col("DSLink3.CLM_SK")==F.col("Lnk_Clm_Pca_lkp.CLM_SK"), "left")
    .join(df_DB2_CLM_PCA.alias("Lnk_Clm_Extrnl_Ref_Lkp"), F.col("DSLink3.CLM_SK")==F.col("Lnk_Clm_Extrnl_Ref_Lkp.CLM_SK"), "left")
    .join(df_DB2_PCA_CLMCHK.alias("Lnk_Pca_ClmChk_Lkp"), F.col("DSLink3.CLM_SK")==F.col("Lnk_Pca_ClmChk_Lkp.CLM_SK"), "left")
    .join(df_DB2_Extrnl_Prov.alias("Lnk_Extrnl_Prov_lkp"), F.col("DSLink3.CLM_SK")==F.col("Lnk_Extrnl_Prov_lkp.CLM_SK"), "left")
    .join(df_DB2_CLM_LN_SUM_PBL.alias("Lnk_Clm_Ln_Sum_Paybl_lkp"), F.col("DSLink3.CLM_SK")==F.col("Lnk_Clm_Ln_Sum_Paybl_lkp.CLM_SK"), "left")
    .join(df_Lnk_Pd_Prov.alias("Lnk_Pd_Prov"), F.col("DSLink3.CLM_SK")==F.col("Lnk_Pd_Prov.CLM_SK"), "left")
    .join(df_Lnk_Pcp_Prov.alias("Lnk_Pcp_Prov"), F.col("DSLink3.CLM_SK")==F.col("Lnk_Pcp_Prov.CLM_SK"), "left")
)

df_nta = df_nta_tmp2.select(
    F.col("Svc_Prov_Lookup.CLM_SK").alias("CLM_SK_Svc_Prov"),
    F.col("Svc_Prov_Lookup.PROV_NM").alias("PROV_NM_Svc_Prov"),
    F.col("Svc_Prov_Lookup.PROV_ID").alias("PROV_ID_Svc_Prov"),
    F.col("Svc_Prov_Lookup.CMN_PRCT_ID").alias("CMN_PRCT_ID_Svc_Prov"),
    F.col("Svc_Prov_Lookup.GRP_PROV_ID").alias("GRP_PROV_ID"),
    F.col("Svc_Prov_Lookup.IPA_PROV_ID").alias("IPA_PROV_ID"),
    F.col("Fclty_Lookup.DRG_CD").alias("DRG_CD_Fclty"),
    F.col("Fclty_Lookup.GNRT_DRG_IN").alias("GNRT_DRG_IN"),
    F.col("Fclty_Lookup.CLM_SK").alias("CLM_SK_Fclty"),
    F.col("Lnk_drug_lkp.NDC").alias("NDC"),
    F.col("Lnk_drug_lkp.DRUG_LABEL_NM").alias("DRUG_LABEL_NM"),
    F.col("Lnk_drug_lkp.CMN_PRCT_ID").alias("CMN_PRCT_ID_Drug"),
    F.col("Lnk_drug_lkp.FIRST_NM").alias("FIRST_NM_Drug"),
    F.col("Lnk_drug_lkp.LAST_NM").alias("LAST_NM_Drug"),
    F.col("Lnk_drug_lkp.CLM_SK").alias("CLM_SK_Drug"),
    F.col("Lnk_drug_lkp.MBR_DIFF_PD_AMT").alias("MBR_DIFF_PD_AMT"),
    F.col("Lnk_drug_lkp.RX_ALW_QTY").alias("RX_ALW_QTY"),
    F.col("Lnk_drug_lkp.RX_SUBMT_QTY").alias("RX_SUBMT_QTY"),
    F.col("Lnk_drug_lkp.RX_NO").alias("RX_NO"),
    F.col("Lnk_drug_lkp.PROV_NM").alias("PROV_NM_Drug"),
    F.col("Lnk_Cob_Lookpup.CLM_SK").alias("CLM_SK_Cob"),
    F.col("Lnk_Cob_Lookpup.PD_AMT").alias("PD_AMT"),
    F.col("Lnk_Remit_lkp.CLM_SK").alias("CLM_SK_Remit"),
    F.col("Lnk_Remit_lkp.SUPRESS_EOB_IN").alias("SUPRESS_EOB_IN"),
    F.col("Lnk_Remit_lkp.SUPRESS_REMIT_IN").alias("SUPRESS_REMIT_IN"),
    F.col("Lnk_Remit_lkp.CNSD_CHRG_AMT").alias("CNSD_CHRG_AMT_Remit"),
    F.col("Lnk_Remit_lkp.ER_COPAY_AMT").alias("ER_COPAY_AMT"),
    F.col("Lnk_Remit_lkp.INTRST_AMT").alias("INTRST_AMT"),
    F.col("Lnk_Remit_lkp.NO_RESP_AMT").alias("NO_RESP_AMT"),
    F.col("Lnk_Remit_lkp.PATN_RESP_AMT").alias("PATN_RESP_AMT"),
    F.col("Lnk_Remit_lkp.PROV_WRTOFF_AMT").alias("PROV_WRTOFF_AMT"),
    F.col("Lnk_Remit_lkp.ALT_CHRG_IN").alias("ALT_CHRG_IN"),
    F.col("Lnk_Remit_lkp.ALT_CHRG_PROV_WRTOFF_AMT").alias("ALT_CHRG_PROV_WRTOFF_AMT"),
    F.col("Lnk_pd_prov_addr_lkp.ADDR_LN_1").alias("ADDR_LN_1_Pd_Prov_Addr"),
    F.col("Lnk_pd_prov_addr_lkp.ADDR_LN_2").alias("ADDR_LN_2_Pd_Prov_Addr"),
    F.col("Lnk_pd_prov_addr_lkp.ADDR_LN_3").alias("ADDR_LN_3_Pd_Prov_Addr"),
    F.col("Lnk_pd_prov_addr_lkp.CITY_NM").alias("CITY_NM_Pd_Prov_Addr"),
    F.col("Lnk_pd_prov_addr_lkp.POSTAL_CD").alias("POSTAL_CD_Pd_Prov_Addr"),
    F.col("Lnk_pd_prov_addr_lkp.PROV_ADDR_ST_CD").alias("PROV_ADDR_ST_CD_Pd_Prov_Addr"),
    F.col("Lnk_Pcp_Prov.PROV_NM").alias("PROV_NM_Pcp_Prov"),
    F.col("Lnk_Pcp_Prov.PROV_ID").alias("PROV_ID_Pcp_Prov"),
    F.col("Lnk_Pd_Prov.PROV_ID").alias("PROV_ID_Pd_Prov"),
    F.col("Lnk_Pd_Prov.PROV_NM").alias("PROV_NM_Pd_Prov"),
    F.col("Lnk_External_mbr_lkp_in.CLM_SK").alias("CLM_SK_External_Mbr"),
    F.col("Lnk_External_mbr_lkp_in.MBR_BRTH_DT_SK").alias("MBR_BRTH_DT_SK"),
    F.col("Lnk_External_mbr_lkp_in.GRP_NM").alias("GRP_NM_External_Mbr"),
    F.col("Lnk_External_mbr_lkp_in.MBR_FIRST_NM").alias("MBR_FIRST_NM"),
    F.col("Lnk_External_mbr_lkp_in.MBR_MIDINIT").alias("MBR_MIDINIT"),
    F.col("Lnk_External_mbr_lkp_in.MBR_LAST_NM").alias("MBR_LAST_NM"),
    F.col("Lnk_External_mbr_lkp_in.SUB_GRP_BASE_NO").alias("SUB_GRP_BASE_NO"),
    F.col("Lnk_External_mbr_lkp_in.SUB_FIRST_NM").alias("SUB_FIRST_NM"),
    F.col("Lnk_External_mbr_lkp_in.SUB_MIDINIT").alias("SUB_MIDINIT"),
    F.col("Lnk_External_mbr_lkp_in.SUB_LAST_NM").alias("SUB_LAST_NM"),
    F.col("Lnk_External_mbr_lkp_in.SUB_ADDR_LN_1").alias("SUB_ADDR_LN_1"),
    F.col("Lnk_External_mbr_lkp_in.SUB_ADDR_LN_2").alias("SUB_ADDR_LN_2"),
    F.col("Lnk_External_mbr_lkp_in.SUB_CITY_NM").alias("SUB_CITY_NM"),
    F.col("Lnk_External_mbr_lkp_in.SUB_POSTAL_CD").alias("SUB_POSTAL_CD"),
    F.col("Lnk_External_mbr_lkp_in.CLM_EXTRNL_MBRSH_SUB_ST_CD").alias("CLM_EXTRNL_MBRSH_SUB_ST_CD"),
    F.col("Lnk_Bill_Svc_Prov.CLM_SK").alias("CLM_SK_Bill_Svc_Prov"),
    F.col("Lnk_Bill_Svc_Prov.PROV_BILL_SVC_ID").alias("PROV_BILL_SVC_ID_Bill_Svc_Prov"),
    F.col("Lnk_Bill_Svc_Grp.CLM_SK").alias("CLM_SK_Bill_Svc_Grp"),
    F.col("Lnk_Bill_Svc_Grp.PROV_BILL_SVC_ID").alias("PROV_BILL_SVC_ID_Bill_Svc_Grp"),
    F.col("Lnk_Its_CLm_Lkp.CLM_SK").alias("CLM_SK_Its_Clm"),
    F.col("Lnk_Its_CLm_Lkp.SCCF_NO").alias("SCCF_NO_Its_Clm"),
    F.col("Lnk_Its_CLm_Lkp.TRGT_CD").alias("TRGT_CD_Its_Clm"),
    F.col("Lnk_Sttus_Audit_Lkp.TRGT_CD").alias("TRGT_CD_Sttus_Audit"),
    F.col("Lnk_Sttus_Audit_Lkp.TRGT_CD_NM").alias("TRGT_CD_NM_Sttus_Audit"),
    F.col("Lnk_Fclty_Clm_Lkp.ADMS_DT_SK").alias("ADMS_DT_SK"),
    F.col("Lnk_Fclty_Clm_Lkp.DSCHG_DT_SK").alias("DSCHG_DT_SK"),
    F.col("Lnk_Fclty_Clm_Lkp.ADM_PHYS_PROV_ID").alias("ADM_PHYS_PROV_ID"),
    F.col("Lnk_Fclty_Clm_Lkp.SUBMT_DRG_TX").alias("SUBMT_DRG_TX"),
    F.col("Lnk_Fclty_Clm_Lkp.FCLTY_CLM_ADMS_SRC_CD").alias("FCLTY_CLM_ADMS_SRC_CD"),
    F.col("Lnk_Fclty_Clm_Lkp.FCLTY_CLM_ADMS_SRC_NM").alias("FCLTY_CLM_ADMS_SRC_NM"),
    F.col("Lnk_Fclty_Clm_Lkp.FCLTY_CLM_ADms_TYP_CD").alias("FCLTY_CLM_ADMS_TYP_CD"),
    F.col("Lnk_Fclty_Clm_Lkp.FCLTY_CLM_ADMS_TYP_NM").alias("FCLTY_CLM_ADMS_TYP_NM"),
    F.col("Lnk_Fclty_Clm_Lkp.FCLTY_CLM_BILL_CLS_CD").alias("FCLTY_CLM_BILL_CLS_CD"),
    F.col("Lnk_Fclty_Clm_Lkp.FCLTY_CLM_BILL_CLS_NM").alias("FCLTY_CLM_BILL_CLS_NM"),
    F.col("Lnk_Fclty_Clm_Lkp.FCLTY_CLM_BILL_FREQ_CD").alias("FCLTY_CLM_BILL_FREQ_CD"),
    F.col("Lnk_Fclty_Clm_Lkp.FCLTY_CLM_BILL_FREQ_NM").alias("FCLTY_CLM_BILL_FREQ_NM"),
    F.col("Lnk_Fclty_Clm_Lkp.FCLTY_CLM_BILL_TYP_CD").alias("FCLTY_CLM_BILL_TYP_CD"),
    F.col("Lnk_Fclty_Clm_Lkp.FCLTY_CLM_BILL_TYP_NM").alias("FCLTY_CLM_BILL_TYP_NM"),
    F.col("Lnk_Clm_In_Lkp.PREAUTH_ID").alias("PREAUTH_ID"),
    F.col("Lnk_Pca_ClmChk_Lkp.CHK_NET_PAYMT_AMT").alias("CHK_NET_PAYMT_AMT_Pca_ClmChk"),
    F.col("Lnk_Pca_ClmChk_Lkp.CHK_NO").alias("CHK_NO_Pca_AlmChk"),
    F.col("Lnk_Pca_ClmChk_Lkp.CHK_SEQ_NO").alias("CHK_SEQ_NO"),
    F.col("Lnk_Pca_ClmChk_Lkp.CHK_PAYE_NM").alias("CHK_PAYE_NM_Pca_ClmChk"),
    F.col("Lnk_Pca_ClmChk_Lkp.CHK_PAYMT_REF_ID").alias("CHK_PAYMT_REF_ID_Pca_ClmChk"),
    F.col("Lnk_Pca_ClmChk_Lkp.PCA_CLM_CHK_PAYE_TYP_CD").alias("PCA_CLM_CHK_PAYE_TYP_CD"),
    F.col("Lnk_Pca_ClmChk_Lkp.PCA_CLM_CHK_PAYE_TYP_NM").alias("PCA_CLM_CHK_PAYE_TYP_NM"),
    F.col("Lnk_Pca_ClmChk_Lkp.PCA_CLM_CHK_PAYMT_METH_CD").alias("PCA_CLM_CHK_PAYMT_METH_CD"),
    F.col("Lnk_Pca_ClmChk_Lkp.PCA_CLM_CHK_PAYMT_METH_NM").alias("PCA_CLM_CHK_PAYMT_METH_NM"),
    F.col("Lnk_Clm_Extrnl_Ref_Lkp.TOT_CNSD_AMT").alias("TOT_CNSD_AMT"),
    F.col("Lnk_Clm_Extrnl_Ref_Lkp.TOT_PD_AMT").alias("TOT_PD_AMT"),
    F.col("Lnk_Clm_Extrnl_Ref_Lkp.SUB_TOT_PCA_AVLBL_AMT").alias("SUB_TOT_PCA_AVLBL_AMT"),
    F.col("Lnk_Clm_Extrnl_Ref_Lkp.SUB_TOT_PCA_PD_TO_DT_AMT").alias("SUB_TOT_PCA_PD_TO_DT_AMT"),
    F.col("Lnk_Extrnl_Prov_lkp.PROV_NM").alias("PROV_NM_Extrnl_Prov"),
    F.col("Lnk_Clm_CLm_CK_lkp.CLM_SK").alias("CLM_SK_Clm_CLm_Ck"),
    F.col("Lnk_Clm_CLm_CK_lkp.CLM_CHK_PAYMT_METH_CD").alias("CLM_CHK_PAYMT_METH_CD"),
    F.col("Lnk_Clm_CLm_CK_lkp.CHK_PD_DT_SK").alias("CHK_PD_DT_SK"),
    F.col("Lnk_Clm_CLm_CK_lkp.CHK_NET_PAYMT_AMT").alias("CHK_NET_PAYMT_AMT"),
    F.col("Lnk_Clm_CLm_CK_lkp.CHK_NO").alias("CHK_NO"),
    F.col("Lnk_Clm_CLm_CK_lkp.CHK_PAYE_NM").alias("CHK_PAYE_NM"),
    F.col("Lnk_Clm_CLm_CK_lkp.CHK_PAYMT_REF_ID").alias("CHK_PAYMT_REF_ID"),
    F.col("Lnk_Clm_Ln_Sum_Paybl_lkp.PAYBL_AMT").alias("PAYBL_AMT_Clm_Ln_Sum_Paybl"),
    F.col("Lnk_Bill_Svc_Prov_Svu.CLM_SK").alias("CLM_SK_Bill_Svc_Prov_Svu"),
    F.col("Lnk_Bill_Svc_Prov_Svu.PROV_BILL_SVC_ID").alias("PROV_BILL_SVC_ID_Bill_Svc_Prov_Svu"),
    F.col("Lnk_Bill_Svc_Grp_Svc.CLM_SK").alias("CLM_SK"),
    F.col("Lnk_Bill_Svc_Grp_Svc.PROV_BILL_SVC_ID").alias("PROV_BILL_SVC_ID"),
    F.col("Lnk_Clm_Pca_lkp.PCA_EXTRNL_ID").alias("PCA_EXTRNL_ID"),
    F.col("DSLink3.CLM_ID").alias("CLM_ID"),
    F.col("DSLink3.PD_DT_SK").alias("PD_DT_SK"),
    F.col("DSLink3.SVC_STRT_DT_SK").alias("SVC_STRT_DT_SK"),
    F.col("DSLink3.SVC_END_DT_SK").alias("SVC_END_DT_SK"),
    F.col("DSLink3.STTUS_DT_SK").alias("STTUS_DT_SK"),
    F.col("DSLink3.CNSD_CHRG_AMT").alias("CNSD_CHRG_AMT"),
    F.col("DSLink3.CHRG_AMT").alias("CHRG_AMT"),
    F.col("DSLink3.PAYBL_AMT").alias("PAYBL_AMT"),
    F.col("DSLink3.SUB_ID").alias("SUB_ID"),
    F.col("DSLink3.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("DSLink3.BRTH_DT_SK").alias("BRTH_DT_SK"),
    F.col("DSLink3.FIRST_NM").alias("FIRST_NM"),
    F.col("DSLink3.MIDINIT").alias("MIDINIT"),
    F.col("DSLink3.LAST_NM").alias("LAST_NM"),
    F.col("DSLink3.ALW_AMT").alias("ALW_AMT"),
    F.col("DSLink3.DSALW_AMT").alias("DSALW_AMT"),
    F.col("DSLink3.COINS_AMT").alias("COINS_AMT"),
    F.col("DSLink3.COPAY_AMT").alias("COPAY_AMT"),
    F.col("DSLink3.DEDCT_AMT").alias("DEDCT_AMT"),
    F.col("DSLink3.ADJ_FROM_CLM_ID").alias("ADJ_FROM_CLM_ID"),
    F.col("DSLink3.ADJ_TO_CLM_ID").alias("ADJ_TO_CLM_ID"),
    F.col("DSLink3.PATN_ACCT_NO").alias("PATN_ACCT_NO"),
    F.col("DSLink3.RFRNG_PROV_TX").alias("RFRNG_PROV_TX"),
    F.col("DSLink3.RCVD_DT_SK").alias("RCVD_DT_SK"),
    F.col("DSLink3.GRP_ID").alias("GRP_ID"),
    F.col("DSLink3.GRP_UNIQ_KEY").alias("GRP_UNIQ_KEY"),
    F.col("DSLink3.GRP_NM").alias("GRP_NM"),
    F.col("DSLink3.SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    F.col("DSLink3.HOST_IN").alias("HOST_IN"),
    F.col("DSLink3.PRCS_DT_SK").alias("PRCS_DT_SK"),
    F.col("DSLink3.MBR_SFX_NO").alias("MBR_SFX_NO"),
    F.col("DSLink3.PROV_AGMNT_ID").alias("PROV_AGMNT_ID"),
    F.col("DSLink3.REL_PCA_CLM_SK").alias("REL_PCA_CLM_SK"),
    F.col("DSLink3.REL_BASE_CLM_SK").alias("REL_BASE_CLM_SK"),
    F.col("DSLink3.CLM_SUB_BCBS_PLN_CD_SK").alias("CLM_SUB_BCBS_PLN_CD_SK"),
    F.col("Lnk_Remit_lkp.ACTL_PD_AMT").alias("ACTL_PD_AMT"),
    F.col("Lnk_Fclty_Clm_Lkp.FCLTY_CLM_DSCHG_STTUS_CD").alias("FCLTY_CLM_DSCHG_STTUS_CD"),
    F.col("Lnk_Fclty_Clm_Lkp.FCLTY_CLM_DSCHG_STTUS_NM").alias("FCLTY_CLM_DSCHG_STTUS_NM"),
    F.col("Lnk_Fclty_Clm_Lkp.GNRT_DRG_CD").alias("GNRT_DRG_CD"),
    F.col("Lnk_Pca_ClmChk_Lkp.PCA_CLM_CHK_PD_DT").alias("PCA_CLM_CHK_PD_DT"),
    F.col("DSLink3.ACTL_PD_AMT").alias("CLM_ACTL_PD_AMT")
)

# --------------------------
# Xref_Cldm_Dm_Clm (Transformer pass-through)
# --------------------------
df_Xref_Cldm_Dm_Clm = df_nta.select([F.col(c) for c in df_nta.columns])

# --------------------------
# Seq_CLM_DM_CLM1 (write to file)
# Apply rpad for char-type columns in final SELECT
# --------------------------
df_seq_clm_dm_clm1 = df_Xref_Cldm_Dm_Clm

df_seq_clm_dm_clm1 = df_seq_clm_dm_clm1.withColumn("GNRT_DRG_IN", F.rpad(F.col("GNRT_DRG_IN"),1," "))
df_seq_clm_dm_clm1 = df_seq_clm_dm_clm1.withColumn("SUPRESS_EOB_IN", F.rpad(F.col("SUPRESS_EOB_IN"),1," "))
df_seq_clm_dm_clm1 = df_seq_clm_dm_clm1.withColumn("SUPRESS_REMIT_IN", F.rpad(F.col("SUPRESS_REMIT_IN"),1," "))
df_seq_clm_dm_clm1 = df_seq_clm_dm_clm1.withColumn("ALT_CHRG_IN", F.rpad(F.col("ALT_CHRG_IN"),1," "))
df_seq_clm_dm_clm1 = df_seq_clm_dm_clm1.withColumn("MBR_MIDINIT", F.rpad(F.col("MBR_MIDINIT"),1," "))
df_seq_clm_dm_clm1 = df_seq_clm_dm_clm1.withColumn("SUB_GRP_BASE_NO", F.rpad(F.col("SUB_GRP_BASE_NO"),9," "))
df_seq_clm_dm_clm1 = df_seq_clm_dm_clm1.withColumn("SUB_MIDINIT", F.rpad(F.col("SUB_MIDINIT"),1," "))
df_seq_clm_dm_clm1 = df_seq_clm_dm_clm1.withColumn("SUB_POSTAL_CD", F.rpad(F.col("SUB_POSTAL_CD"),11," "))
df_seq_clm_dm_clm1 = df_seq_clm_dm_clm1.withColumn("HOST_IN", F.rpad(F.col("HOST_IN"),1," "))
df_seq_clm_dm_clm1 = df_seq_clm_dm_clm1.withColumn("MIDINIT", F.rpad(F.col("MIDINIT"),1," "))
df_seq_clm_dm_clm1 = df_seq_clm_dm_clm1.withColumn("MBR_SFX_NO", F.rpad(F.col("MBR_SFX_NO"),2," "))
df_seq_clm_dm_clm1 = df_seq_clm_dm_clm1.withColumn("PROV_AGMNT_ID", F.rpad(F.col("PROV_AGMNT_ID"),12," "))
df_seq_clm_dm_clm1 = df_seq_clm_dm_clm1.withColumn("SUB_ID", F.rpad(F.col("SUB_ID"),14," "))



from pyspark.sql.functions import col, rpad


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
jdbc_url, jdbc_props = get_db_config(ids_secret_name)

# DB2_Sub_NAME
query_DB2_Sub_NAME = f"""
SELECT DISTINCT
MBR.SUB_SK, 
MBR.FIRST_NM,
MBR.MIDINIT,
MBR.LAST_NM, 
CDMP.TRGT_CD
FROM {IDSOwner}.W_WEBDM_ETL_DRVR R, 
     {IDSOwner}.CLM CLM, 
     {IDSOwner}.MBR MBR, 
     {IDSOwner}.CD_MPPNG CDMP
WHERE R.SRC_SYS_CD_SK = CLM.SRC_SYS_CD_SK
  AND R.CLM_ID = CLM.CLM_ID
  AND CLM.MBR_SK = MBR.MBR_SK
  AND MBR.MBR_RELSHP_CD_SK = CDMP.CD_MPPNG_SK 
  AND CDMP.TRGT_CD = 'SUB'
"""
df_DB2_Sub_NAME = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_DB2_Sub_NAME)
    .load()
)

# DB2_PCA_CLM
query_DB2_PCA_CLM = f"""
SELECT 
DISTINCT
CLM2.CLM_ID,
CLM.REL_PCA_CLM_SK as CLM_SK
FROM   {IDSOwner}.W_WEBDM_ETL_DRVR DRVR,
       {IDSOwner}.CLM CLM,
       {IDSOwner}.CLM CLM2
WHERE DRVR.SRC_SYS_CD_SK  = CLM.SRC_SYS_CD_SK
  AND DRVR.CLM_ID = CLM.CLM_ID
  AND CLM.REL_PCA_CLM_SK = CLM2.CLM_SK
"""
df_DB2_PCA_CLM = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_DB2_PCA_CLM)
    .load()
)

# DB2_BASE_CLM
query_DB2_BASE_CLM = f"""
SELECT 
DISTINCT 
CLM.REL_BASE_CLM_SK as CLM_SK, 
CLM2.CLM_ID
FROM {IDSOwner}.W_WEBDM_ETL_DRVR DRVR,
     {IDSOwner}.CLM CLM,
     {IDSOwner}.CLM CLM2
WHERE DRVR.SRC_SYS_CD_SK  = CLM.SRC_SYS_CD_SK
  AND DRVR.CLM_ID = CLM.CLM_ID
  AND CLM.REL_BASE_CLM_SK = CLM2.CLM_SK
"""
df_DB2_BASE_CLM = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_DB2_BASE_CLM)
    .load()
)

# DB2_NTWK
query_DB2_NTWK = f"""
SELECT
 NTWK.NTWK_SK,
 NTWK.NTWK_ID,
 NTWK.NTWK_NM 
FROM {IDSOwner}.NTWK NTWK
"""
df_DB2_NTWK = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_DB2_NTWK)
    .load()
)

# DB2_MBR
query_DB2_MBR = f"""
SELECT 
DISTINCT 
MBR.MBR_SK,
MBR.BRTH_DT_SK,
CD_MPPNG.TRGT_CD as MBR_GNDR_CD,
CD_MPPNG.TRGT_CD_NM as MBR_GNDR_NM,
CD_MPPNG2.TRGT_CD  as MBR_RELSHP_CD
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
"""
df_DB2_MBR = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_DB2_MBR)
    .load()
)

# DB2_PROD
query_DB2_PROD = f"""
SELECT 
PROD.PROD_SK,
PROD.PROD_ID
FROM {IDSOwner}.PROD PROD
"""
df_DB2_PROD = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_DB2_PROD)
    .load()
)

# Db2_CLS_PLN
query_Db2_CLS_PLN = f"""
SELECT 
CLS_PLN.CLS_PLN_SK,
CLS_PLN.CLS_PLN_ID,
CLS_PLN.CLS_PLN_DESC
FROM {IDSOwner}.CLS_PLN CLS_PLN
"""
df_Db2_CLS_PLN = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_Db2_CLS_PLN)
    .load()
)

# DB2_CLS
query_DB2_CLS = f"""
SELECT 
CLS.CLS_SK,
CLS.CLS_ID,
CLS.CLS_DESC
FROM {IDSOwner}.CLS CLS
"""
df_DB2_CLS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_DB2_CLS)
    .load()
)

# DB2_ALPHA_PFX
query_DB2_ALPHA_PFX = f"""
SELECT 
ALPHA_PFX_SK,
ALPHA_PFX_CD
FROM {IDSOwner}.ALPHA_PFX
"""
df_DB2_ALPHA_PFX = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_DB2_ALPHA_PFX)
    .load()
)

# DB2_CLM_In
query_DB2_CLM_In = f"""
SELECT 
CLM.CLM_SK,
CLM.CLM_ID,
CLM.SRC_SYS_CD_SK,
CLM.ALPHA_PFX_SK,
CLM.CLS_SK,
CLM.CLS_PLN_SK,
CLM.MBR_SK,
CLM.NTWK_SK,
CLM.PROD_SK,
CLM.SUB_SK,
CLM.CLM_INTER_PLN_PGM_CD_SK,
CLM.HOST_IN,
CLM.CLM_CAP_CD_SK,
CLM.CLM_FINL_DISP_CD_SK,
CLM.CLM_INPT_SRC_CD_SK,
CLM.CLM_NTWK_STTUS_CD_SK,
CLM.PD_DT_SK,
CLM.CLM_PAYE_CD_SK,
CLM.PRCS_DT_SK,
CLM.RCVD_DT_SK,
CLM.SVC_STRT_DT_SK,
CLM.SVC_END_DT_SK,
CLM.CLM_STTUS_CD_SK,
CLM.STTUS_DT_SK,
CLM.CLM_SUB_BCBS_PLN_CD_SK,
CLM.CLM_SUBTYP_CD_SK,
CLM.CLM_TYP_CD_SK,
CLM.ACTL_PD_AMT,
CLM.ALW_AMT,
CLM.DSALW_AMT,
CLM.COINS_AMT,
CLM.CNSD_CHRG_AMT,
CLM.COPAY_AMT,
CLM.CHRG_AMT,
CLM.DEDCT_AMT,
CLM.PAYBL_AMT,
CLM.ADJ_FROM_CLM_ID,
CLM.ADJ_TO_CLM_ID,
CLM.MBR_SFX_NO,
CLM.PATN_ACCT_NO,
CLM.PROV_AGMNT_ID,
CLM.RFRNG_PROV_TX,
CLM.SUB_ID,
CLM.PCA_TYP_CD_SK,
CLM.REL_PCA_CLM_SK,
CLM.REL_BASE_CLM_SK,
GRP.GRP_ID,
GRP.GRP_UNIQ_KEY,
GRP.GRP_NM,
SUB.SUB_UNIQ_KEY,
MBR.MBR_UNIQ_KEY,
MBR.BRTH_DT_SK,
MBR.FIRST_NM,
MBR.MIDINIT,
MBR.LAST_NM,
CLM2.SRC_SYS_CD_SK as RBC_SRC_SYS_CD_SK
FROM 
     {IDSOwner}.W_WEBDM_ETL_DRVR R,
     {IDSOwner}.CLM CLM,
     {IDSOwner}.MBR MBR,
     {IDSOwner}.GRP GRP,
     {IDSOwner}.SUB SUB,
     {IDSOwner}.CLM CLM2
WHERE 
     R.SRC_SYS_CD_SK = CLM.SRC_SYS_CD_SK
  AND R.CLM_ID = CLM.CLM_ID
  AND CLM.MBR_SK = MBR.MBR_SK
  AND CLM.GRP_SK = GRP.GRP_SK
  AND CLM.SUB_SK = SUB.SUB_SK
  AND CLM.REL_BASE_CLM_SK = CLM2.CLM_SK
"""
df_DB2_CLM_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_DB2_CLM_In)
    .load()
)

# DB2_SUB_ADDR
query_DB2_SUB_ADDR = f"""
SELECT 
DISTINCT 
SUB.SUB_SK,
SUB.ADDR_LN_1,
SUB.ADDR_LN_2,
SUB.CITY_NM,
SUB.POSTAL_CD,
CDMP.TRGT_CD  as SUB_ADDR_ST_CD_SK,
CDST.TRGT_CD as SUB_ADDR_CD,
CDMP.TRGT_CD
FROM {IDSOwner}.SUB_ADDR SUB,
     {IDSOwner}.CD_MPPNG CDMP,
     {IDSOwner}.CD_MPPNG CDST,
     {IDSOwner}.W_WEBDM_ETL_DRVR R,
     {IDSOwner}.CLM CLM
WHERE SUB.SUB_ADDR_CD_SK = CDMP.CD_MPPNG_SK
  AND R.SRC_SYS_CD_SK = CLM.SRC_SYS_CD_SK
  AND R.CLM_ID = CLM.CLM_ID
  AND CLM.SUB_SK = SUB.SUB_SK
  AND SUB.SUB_ADDR_ST_CD_SK = CDST.CD_MPPNG_SK
"""
df_DB2_SUB_ADDR = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_DB2_SUB_ADDR)
    .load()
)

# Transformer_204
df_Transformer_204 = (
    df_DB2_SUB_ADDR
    .filter(trim(col("TRGT_CD")) == 'SUBHOME')
    .select(
        col("SUB_SK").alias("SUB_SK"),
        col("ADDR_LN_1").alias("ADDR_LN_1"),
        col("ADDR_LN_2").alias("ADDR_LN_2"),
        col("CITY_NM").alias("CITY_NM"),
        col("POSTAL_CD").alias("POSTAL_CD"),
        col("SUB_ADDR_ST_CD_SK").alias("SUB_ADDR_ST_CD_SK"),
        col("SUB_ADDR_CD").alias("SUB_ADDR_CD"),
        col("TRGT_CD").alias("TRGT_CD")
    )
)

# DB2_Connector_4
query_DB2_Connector_4 = f"""
SELECT 
CD_MPPNG_SK,
COALESCE(TRGT_CD,'UNK') SRC_CD,
COALESCE(TRGT_CD_NM,'UNK') SRC_CD_NM,
COALESCE(TRGT_CD,'UNK') TRGT_CD,
COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM
from {IDSOwner}.CD_MPPNG
"""
df_DB2_Connector_4 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_DB2_Connector_4)
    .load()
)

# Copy_29 outputs
df_Copy_29_Lnk_inl_disp_lkp = df_DB2_Connector_4.select(
    col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    col("SRC_CD").alias("SRC_CD"),
    col("SRC_CD_NM").alias("SRC_CD_NM"),
    col("TRGT_CD").alias("TRGT_CD"),
    col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_Copy_29_Lnk_Inter_pln_pgm_lkp = df_DB2_Connector_4.select(
    col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    col("SRC_CD").alias("SRC_CD"),
    col("SRC_CD_NM").alias("SRC_CD_NM"),
    col("TRGT_CD").alias("TRGT_CD"),
    col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_Copy_29_Lnk_Src_Sys_lkp = df_DB2_Connector_4.select(
    col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    col("SRC_CD").alias("SRC_CD"),
    col("SRC_CD_NM").alias("SRC_CD_NM"),
    col("TRGT_CD").alias("TRGT_CD"),
    col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_Copy_29_Lnk_Clm_Subtp_Lkp = df_DB2_Connector_4.select(
    col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    col("SRC_CD").alias("SRC_CD"),
    col("SRC_CD_NM").alias("SRC_CD_NM"),
    col("TRGT_CD").alias("TRGT_CD"),
    col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_Copy_29_Lnk_Clm_cap_loop = df_DB2_Connector_4.select(
    col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    col("SRC_CD").alias("SRC_CD"),
    col("SRC_CD_NM").alias("SRC_CD_NM"),
    col("TRGT_CD").alias("TRGT_CD"),
    col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_Copy_29_Lnk_Clm_typ_lkp = df_DB2_Connector_4.select(
    col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    col("SRC_CD").alias("SRC_CD"),
    col("SRC_CD_NM").alias("SRC_CD_NM"),
    col("TRGT_CD").alias("TRGT_CD"),
    col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_Copy_29_Lnk_Clm_Sttus_lkp = df_DB2_Connector_4.select(
    col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    col("SRC_CD").alias("SRC_CD"),
    col("SRC_CD_NM").alias("SRC_CD_NM"),
    col("TRGT_CD").alias("TRGT_CD"),
    col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_Copy_29_Lnk_Ntwk_Sttud_lkp = df_DB2_Connector_4.select(
    col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    col("SRC_CD").alias("SRC_CD"),
    col("SRC_CD_NM").alias("SRC_CD_NM"),
    col("TRGT_CD").alias("TRGT_CD"),
    col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_Copy_29_Lnk_Pca_Typ_cd_lkp = df_DB2_Connector_4.select(
    col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    col("SRC_CD").alias("SRC_CD"),
    col("SRC_CD_NM").alias("SRC_CD_NM"),
    col("TRGT_CD").alias("TRGT_CD"),
    col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_Copy_29_Lnk_Clm_Paye_lkp = df_DB2_Connector_4.select(
    col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    col("SRC_CD").alias("SRC_CD"),
    col("SRC_CD_NM").alias("SRC_CD_NM"),
    col("TRGT_CD").alias("TRGT_CD"),
    col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_Copy_29_lnk_rbc_src_sys_lookup = df_DB2_Connector_4.select(
    col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    col("SRC_CD").alias("SRC_CD"),
    col("SRC_CD_NM").alias("SRC_CD_NM"),
    col("TRGT_CD").alias("TRGT_CD"),
    col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

# nta (PxLookup) - chain of left joins
df_nta = (
    df_DB2_CLM_In.alias("DSLink3")
    .join(df_DB2_ALPHA_PFX.alias("Lnk_Alpha_pfx"), col("DSLink3.ALPHA_PFX_SK") == col("Lnk_Alpha_pfx.ALPHA_PFX_SK"), "left")
    .join(df_Copy_29_Lnk_Src_Sys_lkp.alias("Lnk_Src_Sys_lkp"), col("DSLink3.SRC_SYS_CD_SK") == col("Lnk_Src_Sys_lkp.CD_MPPNG_SK"), "left")
    .join(df_Copy_29_Lnk_Inter_pln_pgm_lkp.alias("Lnk_Inter_pln_pgm_lkp"), col("DSLink3.CLM_INTER_PLN_PGM_CD_SK") == col("Lnk_Inter_pln_pgm_lkp.CD_MPPNG_SK"), "left")
    .join(df_Copy_29_Lnk_Clm_Sttus_lkp.alias("Lnk_Clm_Sttus_lkp"), col("DSLink3.CLM_STTUS_CD_SK") == col("Lnk_Clm_Sttus_lkp.CD_MPPNG_SK"), "left")
    .join(df_Copy_29_Lnk_Clm_typ_lkp.alias("Lnk_Clm_typ_lkp"), col("DSLink3.CLM_TYP_CD_SK") == col("Lnk_Clm_typ_lkp.CD_MPPNG_SK"), "left")
    .join(df_Copy_29_Lnk_Clm_Paye_lkp.alias("Lnk_Clm_Paye_lkp"), col("DSLink3.CLM_PAYE_CD_SK") == col("Lnk_Clm_Paye_lkp.CD_MPPNG_SK"), "left")
    .join(df_Copy_29_Lnk_Clm_Subtp_Lkp.alias("Lnk_Clm_Subtp_Lkp"), col("DSLink3.CLM_SUBTYP_CD_SK") == col("Lnk_Clm_Subtp_Lkp.CD_MPPNG_SK"), "left")
    .join(df_Copy_29_lnk_rbc_src_sys_lookup.alias("lnk_rbc_src_sys_lookup"), col("DSLink3.RBC_SRC_SYS_CD_SK") == col("lnk_rbc_src_sys_lookup.CD_MPPNG_SK"), "left")
    .join(df_Copy_29_Lnk_Clm_cap_loop.alias("Lnk_Clm_cap_loop"), col("DSLink3.CLM_CAP_CD_SK") == col("Lnk_Clm_cap_loop.CD_MPPNG_SK"), "left")
    .join(df_Copy_29_Lnk_inl_disp_lkp.alias("Lnk_inl_disp_lkp"), col("DSLink3.CLM_FINL_DISP_CD_SK") == col("Lnk_inl_disp_lkp.CD_MPPNG_SK"), "left")
    .join(df_DB2_NTWK.alias("Lnk_Ntwk_Lkp"), col("DSLink3.NTWK_SK") == col("Lnk_Ntwk_Lkp.NTWK_SK"), "left")
    .join(df_DB2_MBR.alias("Lnk_Mbr_Lkp"), col("DSLink3.MBR_SK") == col("Lnk_Mbr_Lkp.MBR_SK"), "left")
    .join(df_DB2_PROD.alias("Lnk_Prod_Lkp"), col("DSLink3.PROD_SK") == col("Lnk_Prod_Lkp.PROD_SK"), "left")
    .join(df_Db2_CLS_PLN.alias("Lnk_Cls_Pln_Lkp"), col("DSLink3.CLS_PLN_SK") == col("Lnk_Cls_Pln_Lkp.CLS_PLN_SK"), "left")
    .join(df_DB2_CLS.alias("Lnk_Cls_Lkp"), col("DSLink3.CLS_SK") == col("Lnk_Cls_Lkp.CLS_SK"), "left")
    .join(df_DB2_BASE_CLM.alias("Lnk_Ref_Get_CLaim"), col("DSLink3.REL_BASE_CLM_SK") == col("Lnk_Ref_Get_CLaim.CLM_SK"), "left")
    .join(df_DB2_PCA_CLM.alias("Lnk_Ref_GetPca_Clm_Id"), col("DSLink3.REL_PCA_CLM_SK") == col("Lnk_Ref_GetPca_Clm_Id.CLM_SK"), "left")
    .join(df_DB2_Sub_NAME.alias("Lnk_Sub_Name_Out"), col("DSLink3.SUB_SK") == col("Lnk_Sub_Name_Out.SUB_SK"), "left")
    .join(df_Transformer_204.alias("DSLink82"), col("DSLink3.SUB_SK") == col("DSLink82.SUB_SK"), "left")
)

df_nta_Lnk = df_nta.select(
    col("Lnk_Alpha_pfx.ALPHA_PFX_SK").alias("ALPHA_PFX_SK"),
    col("Lnk_Alpha_pfx.ALPHA_PFX_CD").alias("ALPHA_PFX_CD"),
    col("Lnk_Src_Sys_lkp.CD_MPPNG_SK").alias("CD_MPPNG_SK_Src_Sys"),
    col("Lnk_Src_Sys_lkp.TRGT_CD").alias("TRGT_CD_Src_Sys"),
    col("Lnk_Clm_cap_loop.TRGT_CD").alias("TRGT_CD_Clm_cap"),
    col("Lnk_Inter_pln_pgm_lkp.CD_MPPNG_SK").alias("CD_MPPNG_SK_Inter_pln_pgm"),
    col("Lnk_Inter_pln_pgm_lkp.TRGT_CD").alias("TRGT_CD_Inter_pln_pgm"),
    col("Lnk_Clm_Sttus_lkp.CD_MPPNG_SK").alias("CD_MPPNG_SK_Clm_Sttus"),
    col("Lnk_Clm_Sttus_lkp.SRC_CD").alias("SRC_CD_Clm_Sttus"),
    col("Lnk_Clm_Sttus_lkp.TRGT_CD").alias("TRGT_CD_Clm_Sttus"),
    col("Lnk_Clm_Sttus_lkp.TRGT_CD_NM").alias("TRGT_CD_NM_Clm_Sttus"),
    col("Lnk_Clm_typ_lkp.CD_MPPNG_SK").alias("CD_MPPNG_SK_Clm_Typ"),
    col("Lnk_Clm_typ_lkp.TRGT_CD").alias("TRGT_CD_Clm_Typ"),
    col("Lnk_Clm_Paye_lkp.CD_MPPNG_SK").alias("CD_MPPNG_SK_Clm_Paye"),
    col("Lnk_Clm_Paye_lkp.TRGT_CD").alias("TRGT_CD_Clm_Paye"),
    col("Lnk_Clm_Subtp_Lkp.CD_MPPNG_SK").alias("CD_MPPNG_SK_Clm_Subtyp"),
    col("Lnk_Clm_Subtp_Lkp.TRGT_CD").alias("TRGT_CD_Clm_Subtyp"),
    col("Lnk_inl_disp_lkp.TRGT_CD").alias("TRGT_CD_Finl_disp"),
    col("Lnk_Sub_Name_Out.FIRST_NM").alias("FIRST_NM_Sub_Name"),
    col("Lnk_Sub_Name_Out.MIDINIT").alias("MIDINIT_Sub_Name"),
    col("Lnk_Sub_Name_Out.LAST_NM").alias("LAST_NM_Sub_Name"),
    col("DSLink82.ADDR_LN_1").alias("ADDR_LN_1_Sub_Addr"),
    col("DSLink82.ADDR_LN_2").alias("ADDR_LN_2_Sub_Addr"),
    col("DSLink82.CITY_NM").alias("CITY_NM_Sub_Addr"),
    col("DSLink82.POSTAL_CD").alias("POSTAL_CD_Sub_Addr"),
    col("DSLink82.SUB_ADDR_CD").alias("SUB_ADDR_ST_CD"),
    col("Lnk_Ntwk_Lkp.NTWK_ID").alias("TRGT_CD_Ntwk_Sttus"),
    col("Lnk_Ntwk_Lkp.NTWK_NM").alias("TRGT_CD_NM_Ntwk_Sttus"),
    col("Lnk_Mbr_Lkp.BRTH_DT_SK").alias("BRTH_DT_SK_Mbr"),
    col("Lnk_Mbr_Lkp.MBR_GNDR_CD").alias("MBR_GNDR_CD"),
    col("Lnk_Mbr_Lkp.MBR_GNDR_NM").alias("MBR_GNDR_NM"),
    col("Lnk_Mbr_Lkp.MBR_RELSHP_CD").alias("MBR_RELSHP_CD"),
    col("Lnk_Cls_Lkp.CLS_ID").alias("CLS_ID_Cls"),
    col("Lnk_Cls_Lkp.CLS_DESC").alias("CLS_DESC_Cls"),
    col("Lnk_Cls_Pln_Lkp.CLS_PLN_ID").alias("CLS_PLN_ID"),
    col("Lnk_Cls_Pln_Lkp.CLS_PLN_DESC").alias("CLS_PLN_DESC"),
    col("Lnk_Ntwk_Lkp.NTWK_ID").alias("NTWK_ID"),
    col("Lnk_Ntwk_Lkp.NTWK_NM").alias("NTWK_NM"),
    col("Lnk_Prod_Lkp.PROD_ID").alias("PROD_ID"),
    col("Lnk_Pca_Typ_cd_lkp.TRGT_CD").alias("TRGT_CD_Pca_Typ_cd"),
    col("Lnk_Pca_Typ_cd_lkp.TRGT_CD_NM").alias("TRGT_CD_NM_Pca_Typ_cd"),
    col("Lnk_Ref_Get_CLaim.CLM_ID").alias("CLM_ID_Ref_Get_Claim"),
    col("Lnk_Ref_GetPca_Clm_Id.CLM_ID").alias("CLM_ID_Ref_Get_Pca_Clm"),
    col("lnk_rbc_src_sys_lookup.CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    col("lnk_rbc_src_sys_lookup.TRGT_CD").alias("TRGT_CD_Rbc_Src_Sys"),
    col("DSLink3.CLM_ID").alias("CLM_ID"),
    col("DSLink3.PD_DT_SK").alias("PD_DT_SK"),
    col("DSLink3.SVC_STRT_DT_SK").alias("SVC_STRT_DT_SK"),
    col("DSLink3.SVC_END_DT_SK").alias("SVC_END_DT_SK"),
    col("DSLink3.STTUS_DT_SK").alias("STTUS_DT_SK"),
    col("DSLink3.CNSD_CHRG_AMT").alias("CNSD_CHRG_AMT"),
    col("DSLink3.CHRG_AMT").alias("CHRG_AMT"),
    col("DSLink3.PAYBL_AMT").alias("PAYBL_AMT"),
    col("DSLink3.SUB_ID").alias("SUB_ID"),
    col("DSLink3.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    col("DSLink3.BRTH_DT_SK").alias("BRTH_DT_SK"),
    col("DSLink3.FIRST_NM").alias("FIRST_NM"),
    col("DSLink3.MIDINIT").alias("MIDINIT"),
    col("DSLink3.LAST_NM").alias("LAST_NM"),
    col("DSLink3.ALW_AMT").alias("ALW_AMT"),
    col("DSLink3.DSALW_AMT").alias("DSALW_AMT"),
    col("DSLink3.COINS_AMT").alias("COINS_AMT"),
    col("DSLink3.COPAY_AMT").alias("COPAY_AMT"),
    col("DSLink3.DEDCT_AMT").alias("DEDCT_AMT"),
    col("DSLink3.ADJ_FROM_CLM_ID").alias("ADJ_FROM_CLM_ID"),
    col("DSLink3.ADJ_TO_CLM_ID").alias("ADJ_TO_CLM_ID"),
    col("DSLink3.PATN_ACCT_NO").alias("PATN_ACCT_NO"),
    col("DSLink3.RFRNG_PROV_TX").alias("RFRNG_PROV_TX"),
    col("DSLink3.RCVD_DT_SK").alias("RCVD_DT_SK"),
    col("DSLink3.GRP_ID").alias("GRP_ID"),
    col("DSLink3.GRP_UNIQ_KEY").alias("GRP_UNIQ_KEY"),
    col("DSLink3.GRP_NM").alias("GRP_NM"),
    col("DSLink3.SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    col("DSLink3.HOST_IN").alias("HOST_IN"),
    col("DSLink3.PRCS_DT_SK").alias("PRCS_DT_SK"),
    col("DSLink3.MBR_SFX_NO").alias("MBR_SFX_NO"),
    col("DSLink3.PROV_AGMNT_ID").alias("PROV_AGMNT_ID"),
    col("DSLink3.REL_PCA_CLM_SK").alias("REL_PCA_CLM_SK"),
    col("DSLink3.REL_BASE_CLM_SK").alias("REL_BASE_CLM_SK"),
    col("DSLink3.CLM_SUB_BCBS_PLN_CD_SK").alias("CLM_SUB_BCBS_PLN_CD_SK"),
    col("DSLink3.ACTL_PD_AMT").alias("CLM_ACTL_PD_AMT")
)

# Xref_Cldm_Dm_Clm
df_Xref_Cldm_Dm_Clm = df_nta_Lnk.select(
    col("ALPHA_PFX_SK").alias("ALPHA_PFX_SK"),
    col("ALPHA_PFX_CD").alias("ALPHA_PFX_CD"),
    col("CD_MPPNG_SK_Src_Sys").alias("CD_MPPNG_SK_Src_Sys"),
    col("TRGT_CD_Src_Sys").alias("TRGT_CD_Src_Sys"),
    col("TRGT_CD_Clm_cap").alias("TRGT_CD_Clm_cap"),
    col("CD_MPPNG_SK_Inter_pln_pgm").alias("CD_MPPNG_SK_Inter_pln_pgm"),
    col("TRGT_CD_Inter_pln_pgm").alias("TRGT_CD_Inter_pln_pgm"),
    col("CD_MPPNG_SK_Clm_Sttus").alias("CD_MPPNG_SK_Clm_Sttus"),
    col("SRC_CD_Clm_Sttus").alias("SRC_CD_Clm_Sttus"),
    col("TRGT_CD_Clm_Sttus").alias("TRGT_CD_Clm_Sttus"),
    col("TRGT_CD_NM_Clm_Sttus").alias("TRGT_CD_NM_Clm_Sttus"),
    col("CD_MPPNG_SK_Clm_Typ").alias("CD_MPPNG_SK_Clm_Typ"),
    col("TRGT_CD_Clm_Typ").alias("TRGT_CD_Clm_Typ"),
    col("CD_MPPNG_SK_Clm_Paye").alias("CD_MPPNG_SK_Clm_Paye"),
    col("TRGT_CD_Clm_Paye").alias("TRGT_CD_Clm_Paye"),
    col("CD_MPPNG_SK_Clm_Subtyp").alias("CD_MPPNG_SK_Clm_Subtyp"),
    col("TRGT_CD_Clm_Subtyp").alias("TRGT_CD_Clm_Subtyp"),
    col("TRGT_CD_Finl_disp").alias("TRGT_CD_Finl_disp"),
    col("FIRST_NM_Sub_Name").alias("FIRST_NM_Sub_Name"),
    col("MIDINIT_Sub_Name").alias("MIDINIT_Sub_Name"),
    col("LAST_NM_Sub_Name").alias("LAST_NM_Sub_Name"),
    col("ADDR_LN_1_Sub_Addr").alias("ADDR_LN_1_Sub_Addr"),
    col("ADDR_LN_2_Sub_Addr").alias("ADDR_LN_2_Sub_Addr"),
    col("CITY_NM_Sub_Addr").alias("CITY_NM_Sub_Addr"),
    col("POSTAL_CD_Sub_Addr").alias("POSTAL_CD_Sub_Addr"),
    col("SUB_ADDR_ST_CD").alias("SUB_ADDR_ST_CD"),
    col("TRGT_CD_Ntwk_Sttus").alias("TRGT_CD_Ntwk_Sttus"),
    col("TRGT_CD_NM_Ntwk_Sttus").alias("TRGT_CD_NM_Ntwk_Sttus"),
    col("BRTH_DT_SK_Mbr").alias("BRTH_DT_SK_Mbr"),
    col("MBR_GNDR_CD").alias("MBR_GNDR_CD"),
    col("MBR_GNDR_NM").alias("MBR_GNDR_NM"),
    col("MBR_RELSHP_CD").alias("MBR_RELSHP_CD"),
    col("CLS_ID_Cls").alias("CLS_ID_Cls"),
    col("CLS_DESC_Cls").alias("CLS_DESC_Cls"),
    col("CLS_PLN_ID").alias("CLS_PLN_ID"),
    col("CLS_PLN_DESC").alias("CLS_PLN_DESC"),
    col("NTWK_ID").alias("NTWK_ID"),
    col("NTWK_NM").alias("NTWK_NM"),
    col("PROD_ID").alias("PROD_ID"),
    col("TRGT_CD_Pca_Typ_cd").alias("TRGT_CD_Pca_Typ_cd"),
    col("TRGT_CD_NM_Pca_Typ_cd").alias("TRGT_CD_NM_Pca_Typ_cd"),
    col("CLM_ID_Ref_Get_Claim").alias("CLM_ID_Ref_Get_Claim"),
    col("CLM_ID_Ref_Get_Pca_Clm").alias("CLM_ID_Ref_Get_Pca_Clm"),
    col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    col("TRGT_CD_Rbc_Src_Sys").alias("TRGT_CD_Rbc_Src_Sys"),
    col("CLM_ID").alias("CLM_ID"),
    col("PD_DT_SK").alias("PD_DT_SK"),
    col("SVC_STRT_DT_SK").alias("SVC_STRT_DT_SK"),
    col("SVC_END_DT_SK").alias("SVC_END_DT_SK"),
    col("STTUS_DT_SK").alias("STTUS_DT_SK"),
    col("CNSD_CHRG_AMT").alias("CNSD_CHRG_AMT"),
    col("CHRG_AMT").alias("CHRG_AMT"),
    col("PAYBL_AMT").alias("PAYBL_AMT"),
    col("SUB_ID").alias("SUB_ID"),
    col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    col("BRTH_DT_SK").alias("BRTH_DT_SK"),
    col("FIRST_NM").alias("FIRST_NM"),
    col("MIDINIT").alias("MIDINIT"),
    col("LAST_NM").alias("LAST_NM"),
    col("ALW_AMT").alias("ALW_AMT"),
    col("DSALW_AMT").alias("DSALW_AMT"),
    col("COINS_AMT").alias("COINS_AMT"),
    col("COPAY_AMT").alias("COPAY_AMT"),
    col("DEDCT_AMT").alias("DEDCT_AMT"),
    col("ADJ_FROM_CLM_ID").alias("ADJ_FROM_CLM_ID"),
    col("ADJ_TO_CLM_ID").alias("ADJ_TO_CLM_ID"),
    col("PATN_ACCT_NO").alias("PATN_ACCT_NO"),
    col("RFRNG_PROV_TX").alias("RFRNG_PROV_TX"),
    col("RCVD_DT_SK").alias("RCVD_DT_SK"),
    col("GRP_ID").alias("GRP_ID"),
    col("GRP_UNIQ_KEY").alias("GRP_UNIQ_KEY"),
    col("GRP_NM").alias("GRP_NM"),
    col("SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    col("HOST_IN").alias("HOST_IN"),
    col("PRCS_DT_SK").alias("PRCS_DT_SK"),
    col("MBR_SFX_NO").alias("MBR_SFX_NO"),
    col("PROV_AGMNT_ID").alias("PROV_AGMNT_ID"),
    col("REL_PCA_CLM_SK").alias("REL_PCA_CLM_SK"),
    col("REL_BASE_CLM_SK").alias("REL_BASE_CLM_SK"),
    col("CLM_SUB_BCBS_PLN_CD_SK").alias("CLM_SUB_BCBS_PLN_CD_SK"),
    col("CLM_ACTL_PD_AMT").alias("CLM_ACTL_PD_AMT")
)

# Seq_CLM_DM_CLM2
df_Seq_CLM_DM_CLM2 = df_Xref_Cldm_Dm_Clm.select(
    rpad(col("ALPHA_PFX_SK"),  20, " ").alias("ALPHA_PFX_SK"),
    rpad(col("ALPHA_PFX_CD"), 20, " ").alias("ALPHA_PFX_CD"),
    rpad(col("CD_MPPNG_SK_Src_Sys"), 20, " ").alias("CD_MPPNG_SK_Src_Sys"),
    rpad(col("TRGT_CD_Src_Sys"), 20, " ").alias("TRGT_CD_Src_Sys"),
    rpad(col("TRGT_CD_Clm_cap"), 20, " ").alias("TRGT_CD_Clm_cap"),
    rpad(col("CD_MPPNG_SK_Inter_pln_pgm"), 20, " ").alias("CD_MPPNG_SK_Inter_pln_pgm"),
    rpad(col("TRGT_CD_Inter_pln_pgm"), 20, " ").alias("TRGT_CD_Inter_pln_pgm"),
    rpad(col("CD_MPPNG_SK_Clm_Sttus"), 20, " ").alias("CD_MPPNG_SK_Clm_Sttus"),
    rpad(col("SRC_CD_Clm_Sttus"), 20, " ").alias("SRC_CD_Clm_Sttus"),
    rpad(col("TRGT_CD_Clm_Sttus"), 20, " ").alias("TRGT_CD_Clm_Sttus"),
    rpad(col("TRGT_CD_NM_Clm_Sttus"), 20, " ").alias("TRGT_CD_NM_Clm_Sttus"),
    rpad(col("CD_MPPNG_SK_Clm_Typ"), 20, " ").alias("CD_MPPNG_SK_Clm_Typ"),
    rpad(col("TRGT_CD_Clm_Typ"), 20, " ").alias("TRGT_CD_Clm_Typ"),
    rpad(col("CD_MPPNG_SK_Clm_Paye"), 20, " ").alias("CD_MPPNG_SK_Clm_Paye"),
    rpad(col("TRGT_CD_Clm_Paye"), 20, " ").alias("TRGT_CD_Clm_Paye"),
    rpad(col("CD_MPPNG_SK_Clm_Subtyp"), 20, " ").alias("CD_MPPNG_SK_Clm_Subtyp"),
    rpad(col("TRGT_CD_Clm_Subtyp"), 20, " ").alias("TRGT_CD_Clm_Subtyp"),
    rpad(col("TRGT_CD_Finl_disp"), 20, " ").alias("TRGT_CD_Finl_disp"),
    rpad(col("FIRST_NM_Sub_Name"), 20, " ").alias("FIRST_NM_Sub_Name"),
    rpad(col("MIDINIT_Sub_Name"), 1, " ").alias("MIDINIT_Sub_Name"),
    rpad(col("LAST_NM_Sub_Name"), 20, " ").alias("LAST_NM_Sub_Name"),
    rpad(col("ADDR_LN_1_Sub_Addr"), 20, " ").alias("ADDR_LN_1_Sub_Addr"),
    rpad(col("ADDR_LN_2_Sub_Addr"), 20, " ").alias("ADDR_LN_2_Sub_Addr"),
    rpad(col("CITY_NM_Sub_Addr"), 20, " ").alias("CITY_NM_Sub_Addr"),
    rpad(col("POSTAL_CD_Sub_Addr"), 20, " ").alias("POSTAL_CD_Sub_Addr"),
    rpad(col("SUB_ADDR_ST_CD"), 20, " ").alias("SUB_ADDR_ST_CD"),
    rpad(col("TRGT_CD_Ntwk_Sttus"), 20, " ").alias("TRGT_CD_Ntwk_Sttus"),
    rpad(col("TRGT_CD_NM_Ntwk_Sttus"), 20, " ").alias("TRGT_CD_NM_Ntwk_Sttus"),
    rpad(col("BRTH_DT_SK_Mbr"), 20, " ").alias("BRTH_DT_SK_Mbr"),
    rpad(col("MBR_GNDR_CD"), 20, " ").alias("MBR_GNDR_CD"),
    rpad(col("MBR_GNDR_NM"), 20, " ").alias("MBR_GNDR_NM"),
    rpad(col("MBR_RELSHP_CD"), 20, " ").alias("MBR_RELSHP_CD"),
    rpad(col("CLS_ID_Cls"), 20, " ").alias("CLS_ID_Cls"),
    rpad(col("CLS_DESC_Cls"), 20, " ").alias("CLS_DESC_Cls"),
    rpad(col("CLS_PLN_ID"), 20, " ").alias("CLS_PLN_ID"),
    rpad(col("CLS_PLN_DESC"), 20, " ").alias("CLS_PLN_DESC"),
    rpad(col("NTWK_ID"), 20, " ").alias("NTWK_ID"),
    rpad(col("NTWK_NM"), 20, " ").alias("NTWK_NM"),
    rpad(col("PROD_ID"), 8, " ").alias("PROD_ID"),
    rpad(col("TRGT_CD_Pca_Typ_cd"), 20, " ").alias("TRGT_CD_Pca_Typ_cd"),
    rpad(col("TRGT_CD_NM_Pca_Typ_cd"), 20, " ").alias("TRGT_CD_NM_Pca_Typ_cd"),
    rpad(col("CLM_ID_Ref_Get_Claim"), 20, " ").alias("CLM_ID_Ref_Get_Claim"),
    rpad(col("CLM_ID_Ref_Get_Pca_Clm"), 20, " ").alias("CLM_ID_Ref_Get_Pca_Clm"),
    rpad(col("CD_MPPNG_SK"), 20, " ").alias("CD_MPPNG_SK"),
    rpad(col("TRGT_CD_Rbc_Src_Sys"), 20, " ").alias("TRGT_CD_Rbc_Src_Sys"),
    rpad(col("CLM_ID"), 20, " ").alias("CLM_ID"),
    rpad(col("PD_DT_SK"), 20, " ").alias("PD_DT_SK"),
    rpad(col("SVC_STRT_DT_SK"), 20, " ").alias("SVC_STRT_DT_SK"),
    rpad(col("SVC_END_DT_SK"), 20, " ").alias("SVC_END_DT_SK"),
    rpad(col("STTUS_DT_SK"), 20, " ").alias("STTUS_DT_SK"),
    rpad(col("CNSD_CHRG_AMT"), 20, " ").alias("CNSD_CHRG_AMT"),
    rpad(col("CHRG_AMT"), 20, " ").alias("CHRG_AMT"),
    rpad(col("PAYBL_AMT"), 20, " ").alias("PAYBL_AMT"),
    rpad(col("SUB_ID"), 14, " ").alias("SUB_ID"),
    rpad(col("MBR_UNIQ_KEY"), 20, " ").alias("MBR_UNIQ_KEY"),
    rpad(col("BRTH_DT_SK"), 20, " ").alias("BRTH_DT_SK"),
    rpad(col("FIRST_NM"), 20, " ").alias("FIRST_NM"),
    rpad(col("MIDINIT"), 1, " ").alias("MIDINIT"),
    rpad(col("LAST_NM"), 20, " ").alias("LAST_NM"),
    rpad(col("ALW_AMT"), 20, " ").alias("ALW_AMT"),
    rpad(col("DSALW_AMT"), 20, " ").alias("DSALW_AMT"),
    rpad(col("COINS_AMT"), 20, " ").alias("COINS_AMT"),
    rpad(col("COPAY_AMT"), 20, " ").alias("COPAY_AMT"),
    rpad(col("DEDCT_AMT"), 20, " ").alias("DEDCT_AMT"),
    rpad(col("ADJ_FROM_CLM_ID"), 20, " ").alias("ADJ_FROM_CLM_ID"),
    rpad(col("ADJ_TO_CLM_ID"), 20, " ").alias("ADJ_TO_CLM_ID"),
    rpad(col("PATN_ACCT_NO"), 20, " ").alias("PATN_ACCT_NO"),
    rpad(col("RFRNG_PROV_TX"), 20, " ").alias("RFRNG_PROV_TX"),
    rpad(col("RCVD_DT_SK"), 20, " ").alias("RCVD_DT_SK"),
    rpad(col("GRP_ID"), 20, " ").alias("GRP_ID"),
    rpad(col("GRP_UNIQ_KEY"), 20, " ").alias("GRP_UNIQ_KEY"),
    rpad(col("GRP_NM"), 20, " ").alias("GRP_NM"),
    rpad(col("SUB_UNIQ_KEY"), 20, " ").alias("SUB_UNIQ_KEY"),
    rpad(col("HOST_IN"), 1, " ").alias("HOST_IN"),
    rpad(col("PRCS_DT_SK"), 20, " ").alias("PRCS_DT_SK"),
    rpad(col("MBR_SFX_NO"), 2, " ").alias("MBR_SFX_NO"),
    rpad(col("PROV_AGMNT_ID"), 12, " ").alias("PROV_AGMNT_ID"),
    rpad(col("REL_PCA_CLM_SK"), 20, " ").alias("REL_PCA_CLM_SK"),
    rpad(col("REL_BASE_CLM_SK"), 20, " ").alias("REL_BASE_CLM_SK"),
    rpad(col("CLM_SUB_BCBS_PLN_CD_SK"), 20, " ").alias("CLM_SUB_BCBS_PLN_CD_SK"),
    rpad(col("CLM_ACTL_PD_AMT"), 20, " ").alias("CLM_ACTL_PD_AMT")
)


import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType

IDSOwner = get_widget_value('IDSOwner','')
DatamartRunCycle = get_widget_value('DatamartRunCycle','')
CommitPoint = get_widget_value('CommitPoint','')

df_Seq_CLM_DM_CLM1 = df_seq_clm_dm_clm1

df_Seq_CLM_DM_CLM2 = df_Seq_CLM_DM_CLM2

df_lookup_293 = df_Seq_CLM_DM_CLM1.alias("Lnk_ClmDmCLm_ABCOut1").join(
    df_Seq_CLM_DM_CLM2.alias("Lnk_ClmDmCLm_ABCOut2"),
    F.col("Lnk_ClmDmCLm_ABCOut1.CLM_ID") == F.col("Lnk_ClmDmCLm_ABCOut2.CLM_ID"),
    "left"
)

def rpad_if_char(col_name, length):
    return F.rpad(F.col(col_name), length, " ")

df_final = df_lookup_293 \
    .withColumn("ALPHA_PFX_SK", F.col("Lnk_ClmDmCLm_ABCOut2.ALPHA_PFX_SK")) \
    .withColumn("ALPHA_PFX_CD", F.col("Lnk_ClmDmCLm_ABCOut2.ALPHA_PFX_CD")) \
    .withColumn("CLM_SK_Svc_Prov", F.col("Lnk_ClmDmCLm_ABCOut1.CLM_SK_Svc_Prov")) \
    .withColumn("PROV_NM_Svc_Prov", F.col("Lnk_ClmDmCLm_ABCOut1.PROV_NM_Svc_Prov")) \
    .withColumn("PROV_ID_Svc_Prov", F.col("Lnk_ClmDmCLm_ABCOut1.PROV_ID_Svc_Prov")) \
    .withColumn("CMN_PRCT_ID_Svc_Prov", F.col("Lnk_ClmDmCLm_ABCOut1.CMN_PRCT_ID_Svc_Prov")) \
    .withColumn("GRP_PROV_ID", F.col("Lnk_ClmDmCLm_ABCOut1.GRP_PROV_ID")) \
    .withColumn("IPA_PROV_ID", F.col("Lnk_ClmDmCLm_ABCOut1.IPA_PROV_ID")) \
    .withColumn("DRG_CD_Fclty", F.col("Lnk_ClmDmCLm_ABCOut1.DRG_CD_Fclty")) \
    .withColumn("GNRT_DRG_IN", rpad_if_char("Lnk_ClmDmCLm_ABCOut1.GNRT_DRG_IN", 1)) \
    .withColumn("CLM_SK_Fclty", F.col("Lnk_ClmDmCLm_ABCOut1.CLM_SK_Fclty")) \
    .withColumn("CD_MPPNG_SK_Src_Sys", F.col("Lnk_ClmDmCLm_ABCOut2.CD_MPPNG_SK_Src_Sys")) \
    .withColumn("TRGT_CD_Src_Sys", F.col("Lnk_ClmDmCLm_ABCOut2.TRGT_CD_Src_Sys")) \
    .withColumn("TRGT_CD_Clm_cap", F.col("Lnk_ClmDmCLm_ABCOut2.TRGT_CD_Clm_cap")) \
    .withColumn("CD_MPPNG_SK_Inter_pln_pgm", F.col("Lnk_ClmDmCLm_ABCOut2.CD_MPPNG_SK_Inter_pln_pgm")) \
    .withColumn("TRGT_CD_Inter_pln_pgm", F.col("Lnk_ClmDmCLm_ABCOut2.TRGT_CD_Inter_pln_pgm")) \
    .withColumn("CD_MPPNG_SK_Clm_Sttus", F.col("Lnk_ClmDmCLm_ABCOut2.CD_MPPNG_SK_Clm_Sttus")) \
    .withColumn("SRC_CD_Clm_Sttus", F.col("Lnk_ClmDmCLm_ABCOut2.SRC_CD_Clm_Sttus")) \
    .withColumn("TRGT_CD_Clm_Sttus", F.col("Lnk_ClmDmCLm_ABCOut2.TRGT_CD_Clm_Sttus")) \
    .withColumn("TRGT_CD_NM_Clm_Sttus", F.col("Lnk_ClmDmCLm_ABCOut2.TRGT_CD_NM_Clm_Sttus")) \
    .withColumn("CD_MPPNG_SK_Clm_Typ", F.col("Lnk_ClmDmCLm_ABCOut2.CD_MPPNG_SK_Clm_Typ")) \
    .withColumn("TRGT_CD_Clm_Typ", F.col("Lnk_ClmDmCLm_ABCOut2.TRGT_CD_Clm_Typ")) \
    .withColumn("CD_MPPNG_SK_Clm_Paye", F.col("Lnk_ClmDmCLm_ABCOut2.CD_MPPNG_SK_Clm_Paye")) \
    .withColumn("TRGT_CD_Clm_Paye", F.col("Lnk_ClmDmCLm_ABCOut2.TRGT_CD_Clm_Paye")) \
    .withColumn("CD_MPPNG_SK_Clm_Subtyp", F.col("Lnk_ClmDmCLm_ABCOut2.CD_MPPNG_SK_Clm_Subtyp")) \
    .withColumn("TRGT_CD_Clm_Subtyp", F.col("Lnk_ClmDmClm_ABCOut2.TRGT_CD_Clm_Subtyp")) \
    .withColumn("TRGT_CD_Finl_disp", F.col("Lnk_ClmDmCLm_ABCOut2.TRGT_CD_Finl_disp")) \
    .withColumn("NDC", F.col("Lnk_ClmDmCLm_ABCOut1.NDC")) \
    .withColumn("DRUG_LABEL_NM", F.col("Lnk_ClmDmCLm_ABCOut1.DRUG_LABEL_NM")) \
    .withColumn("CMN_PRCT_ID_Drug", F.col("Lnk_ClmDmCLm_ABCOut1.CMN_PRCT_ID_Drug")) \
    .withColumn("FIRST_NM_Drug", F.col("Lnk_ClmDmCLm_ABCOut1.FIRST_NM_Drug")) \
    .withColumn("LAST_NM_Drug", F.col("Lnk_ClmDmCLm_ABCOut1.LAST_NM_Drug")) \
    .withColumn("CLM_SK_Drug", F.col("Lnk_ClmDmCLm_ABCOut1.CLM_SK_Drug")) \
    .withColumn("MBR_DIFF_PD_AMT", F.col("Lnk_ClmDmCLm_ABCOut1.MBR_DIFF_PD_AMT")) \
    .withColumn("RX_ALW_QTY", F.col("Lnk_ClmDmCLm_ABCOut1.RX_ALW_QTY")) \
    .withColumn("RX_SUBMT_QTY", F.col("Lnk_ClmDmCLm_ABCOut1.RX_SUBMT_QTY")) \
    .withColumn("RX_NO", F.col("Lnk_ClmDmCLm_ABCOut1.RX_NO")) \
    .withColumn("PROV_NM_Drug", F.col("Lnk_ClmDmCLm_ABCOut1.PROV_NM_Drug")) \
    .withColumn("CLM_SK_Cob", F.col("Lnk_ClmDmCLm_ABCOut1.CLM_SK_Cob")) \
    .withColumn("PD_AMT", F.col("Lnk_ClmDmCLm_ABCOut1.PD_AMT")) \
    .withColumn("CLM_SK_Remit", F.col("Lnk_ClmDmCLm_ABCOut1.CLM_SK_Remit")) \
    .withColumn("SUPRESS_EOB_IN", rpad_if_char("Lnk_ClmDmCLm_ABCOut1.SUPRESS_EOB_IN", 1)) \
    .withColumn("SUPRESS_REMIT_IN", rpad_if_char("Lnk_ClmDmCLm_ABCOut1.SUPRESS_REMIT_IN", 1)) \
    .withColumn("CNSD_CHRG_AMT_Remit", F.col("Lnk_ClmDmCLm_ABCOut1.CNSD_CHRG_AMT_Remit")) \
    .withColumn("ER_COPAY_AMT", F.col("Lnk_ClmDmCLm_ABCOut1.ER_COPAY_AMT")) \
    .withColumn("INTRST_AMT", F.col("Lnk_ClmDmCLm_ABCOut1.INTRST_AMT")) \
    .withColumn("NO_RESP_AMT", F.col("Lnk_ClmDmCLm_ABCOut1.NO_RESP_AMT")) \
    .withColumn("PATN_RESP_AMT", F.col("Lnk_ClmDmCLm_ABCOut1.PATN_RESP_AMT")) \
    .withColumn("PROV_WRTOFF_AMT", F.col("Lnk_ClmDmCLm_ABCOut1.PROV_WRTOFF_AMT")) \
    .withColumn("ALT_CHRG_IN", rpad_if_char("Lnk_ClmDmCLm_ABCOut1.ALT_CHRG_IN", 1)) \
    .withColumn("ALT_CHRG_PROV_WRTOFF_AMT", F.col("Lnk_ClmDmCLm_ABCOut1.ALT_CHRG_PROV_WRTOFF_AMT")) \
    .withColumn("FIRST_NM_Sub_Name", F.col("Lnk_ClmDmCLm_ABCOut2.FIRST_NM_Sub_Name")) \
    .withColumn("MIDINIT_Sub_Name", rpad_if_char("Lnk_ClmDmCLm_ABCOut2.MIDINIT_Sub_Name", 1)) \
    .withColumn("LAST_NM_Sub_Name", F.col("Lnk_ClmDmCLm_ABCOut2.LAST_NM_Sub_Name")) \
    .withColumn("ADDR_LN_1_Sub_Addr", F.col("Lnk_ClmDmCLm_ABCOut2.ADDR_LN_1_Sub_Addr")) \
    .withColumn("ADDR_LN_2_Sub_Addr", F.col("Lnk_ClmDmCLm_ABCOut2.ADDR_LN_2_Sub_Addr")) \
    .withColumn("CITY_NM_Sub_Addr", F.col("Lnk_ClmDmCLm_ABCOut2.CITY_NM_Sub_Addr")) \
    .withColumn("POSTAL_CD_Sub_Addr", F.col("Lnk_ClmDmCLm_ABCOut2.POSTAL_CD_Sub_Addr")) \
    .withColumn("SUB_ADDR_ST_CD", F.col("Lnk_ClmDmCLm_ABCOut2.SUB_ADDR_ST_CD")) \
    .withColumn("ADDR_LN_1_Pd_Prov_Addr", F.col("Lnk_ClmDmCLm_ABCOut1.ADDR_LN_1_Pd_Prov_Addr")) \
    .withColumn("ADDR_LN_2_Pd_Prov_Addr", F.col("Lnk_ClmDmCLm_ABCOut1.ADDR_LN_2_Pd_Prov_Addr")) \
    .withColumn("ADDR_LN_3_Pd_Prov_Addr", F.col("Lnk_ClmDmCLm_ABCOut1.ADDR_LN_3_Pd_Prov_Addr")) \
    .withColumn("CITY_NM_Pd_Prov_Addr", F.col("Lnk_ClmDmCLm_ABCOut1.CITY_NM_Pd_Prov_Addr")) \
    .withColumn("POSTAL_CD_Pd_Prov_Addr", F.col("Lnk_ClmDmCLm_ABCOut1.POSTAL_CD_Pd_Prov_Addr")) \
    .withColumn("PROV_ADDR_ST_CD_Pd_Prov_Addr", F.col("Lnk_ClmDmCLm_ABCOut1.PROV_ADDR_ST_CD_Pd_Prov_Addr")) \
    .withColumn("PROV_NM_Pcp_Prov", F.col("Lnk_ClmDmCLm_ABCOut1.PROV_NM_Pcp_Prov")) \
    .withColumn("PROV_ID_Pcp_Prov", F.col("Lnk_ClmDmCLm_ABCOut1.PROV_ID_Pcp_Prov")) \
    .withColumn("PROV_ID_Pd_Prov", F.col("Lnk_ClmDmCLm_ABCOut1.PROV_ID_Pd_Prov")) \
    .withColumn("PROV_NM_Pd_Prov", F.col("Lnk_ClmDmCLm_ABCOut1.PROV_NM_Pd_Prov")) \
    .withColumn("CLM_SK_External_Mbr", F.col("Lnk_ClmDmCLm_ABCOut1.CLM_SK_External_Mbr")) \
    .withColumn("MBR_BRTH_DT_SK", F.col("Lnk_ClmDmCLm_ABCOut1.MBR_BRTH_DT_SK")) \
    .withColumn("GRP_NM_External_Mbr", F.col("Lnk_ClmDmCLm_ABCOut1.GRP_NM_External_Mbr")) \
    .withColumn("MBR_FIRST_NM", F.col("Lnk_ClmDmCLm_ABCOut1.MBR_FIRST_NM")) \
    .withColumn("MBR_MIDINIT", rpad_if_char("Lnk_ClmDmCLm_ABCOut1.MBR_MIDINIT", 1)) \
    .withColumn("MBR_LAST_NM", F.col("Lnk_ClmDmCLm_ABCOut1.MBR_LAST_NM")) \
    .withColumn("SUB_GRP_BASE_NO", rpad_if_char("Lnk_ClmDmCLm_ABCOut1.SUB_GRP_BASE_NO", 9)) \
    .withColumn("SUB_FIRST_NM", F.col("Lnk_ClmDmCLm_ABCOut1.SUB_FIRST_NM")) \
    .withColumn("SUB_MIDINIT", rpad_if_char("Lnk_ClmDmCLm_ABCOut1.SUB_MIDINIT", 1)) \
    .withColumn("SUB_LAST_NM", F.col("Lnk_ClmDmCLm_ABCOut1.SUB_LAST_NM")) \
    .withColumn("SUB_ADDR_LN_1", F.col("Lnk_ClmDmCLm_ABCOut1.SUB_ADDR_LN_1")) \
    .withColumn("SUB_ADDR_LN_2", F.col("Lnk_ClmDmCLm_ABCOut1.SUB_ADDR_LN_2")) \
    .withColumn("SUB_CITY_NM", F.col("Lnk_ClmDmCLm_ABCOut1.SUB_CITY_NM")) \
    .withColumn("SUB_POSTAL_CD", rpad_if_char("Lnk_ClmDmCLm_ABCOut1.SUB_POSTAL_CD", 11)) \
    .withColumn("CLM_EXTRNL_MBRSH_SUB_ST_CD", F.col("Lnk_ClmDmCLm_ABCOut1.CLM_EXTRNL_MBRSH_SUB_ST_CD")) \
    .withColumn("CLM_SK_Bill_Svc_Prov", F.col("Lnk_ClmDmCLm_ABCOut1.CLM_SK_Bill_Svc_Prov")) \
    .withColumn("PROV_BILL_SVC_ID_Bill_Svc_Prov", F.col("Lnk_ClmDmCLm_ABCOut1.PROV_BILL_SVC_ID_Bill_Svc_Prov")) \
    .withColumn("CLM_SK_Bill_Svc_Grp", F.col("Lnk_ClmDmCLm_ABCOut1.CLM_SK_Bill_Svc_Grp")) \
    .withColumn("PROV_BILL_SVC_ID_Bill_Svc_Grp", F.col("Lnk_ClmDmCLm_ABCOut1.PROV_BILL_SVC_ID_Bill_Svc_Grp")) \
    .withColumn("CLM_SK_Its_Clm", F.col("Lnk_ClmDmCLm_ABCOut1.CLM_SK_Its_Clm")) \
    .withColumn("SCCF_NO_Its_Clm", F.col("Lnk_ClmDmCLm_ABCOut1.SCCF_NO_Its_Clm")) \
    .withColumn("TRGT_CD_Its_Clm", F.col("Lnk_ClmDmCLm_ABCOut1.TRGT_CD_Its_Clm")) \
    .withColumn("TRGT_CD_Ntwk_Sttus", F.col("Lnk_ClmDmCLm_ABCOut2.TRGT_CD_Ntwk_Sttus")) \
    .withColumn("TRGT_CD_NM_Ntwk_Sttus", F.col("Lnk_ClmDmCLm_ABCOut2.TRGT_CD_NM_Ntwk_Sttus")) \
    .withColumn("TRGT_CD_Sttus_Audit", F.col("Lnk_ClmDmCLm_ABCOut1.TRGT_CD_Sttus_Audit")) \
    .withColumn("TRGT_CD_NM_Sttus_Audit", F.col("Lnk_ClmDmCLm_ABCOut1.TRGT_CD_NM_Sttus_Audit")) \
    .withColumn("ADMS_DT_SK", F.col("Lnk_ClmDmCLm_ABCOut1.ADMS_DT_SK")) \
    .withColumn("DSCHG_DT_SK", F.col("Lnk_ClmDmCLm_ABCOut1.DSCHG_DT_SK")) \
    .withColumn("ADM_PHYS_PROV_ID", F.col("Lnk_ClmDmCLm_ABCOut1.ADM_PHYS_PROV_ID")) \
    .withColumn("SUBMT_DRG_TX", F.col("Lnk_ClmDmCLm_ABCOut1.SUBMT_DRG_TX")) \
    .withColumn("FCLTY_CLM_ADMS_SRC_CD", F.col("Lnk_ClmDmCLm_ABCOut1.FCLTY_CLM_ADMS_SRC_CD")) \
    .withColumn("FCLTY_CLM_ADMS_SRC_NM", F.col("Lnk_ClmDmCLm_ABCOut1.FCLTY_CLM_ADMS_SRC_NM")) \
    .withColumn("FCLTY_CLM_ADMS_TYP_CD", F.col("Lnk_ClmDmCLm_ABCOut1.FCLTY_CLM_ADMS_TYP_CD")) \
    .withColumn("FCLTY_CLM_ADMS_TYP_NM", F.col("Lnk_ClmDmCLm_ABCOut1.FCLTY_CLM_ADMS_TYP_NM")) \
    .withColumn("FCLTY_CLM_BILL_CLS_CD", F.col("Lnk_ClmDmCLm_ABCOut1.FCLTY_CLM_BILL_CLS_CD")) \
    .withColumn("FCLTY_CLM_BILL_CLS_NM", F.col("Lnk_ClmDmCLm_ABCOut1.FCLTY_CLM_BILL_CLS_NM")) \
    .withColumn("FCLTY_CLM_BILL_FREQ_CD", F.col("Lnk_ClmDmCLm_ABCOut1.FCLTY_CLM_BILL_FREQ_CD")) \
    .withColumn("FCLTY_CLM_BILL_FREQ_NM", F.col("Lnk_ClmDmCLm_ABCOut1.FCLTY_CLM_BILL_FREQ_NM")) \
    .withColumn("FCLTY_CLM_BILL_TYP_CD", F.col("Lnk_ClmDmCLm_ABCOut1.FCLTY_CLM_BILL_TYP_CD")) \
    .withColumn("FCLTY_CLM_BILL_TYP_NM", F.col("Lnk_ClmDmCLm_ABCOut1.FCLTY_CLM_BILL_TYP_NM")) \
    .withColumn("BRTH_DT_SK_Mbr", F.col("Lnk_ClmDmCLm_ABCOut2.BRTH_DT_SK_Mbr")) \
    .withColumn("MBR_GNDR_CD", F.col("Lnk_ClmDmCLm_ABCOut2.MBR_GNDR_CD")) \
    .withColumn("MBR_GNDR_NM", F.col("Lnk_ClmDmCLm_ABCOut2.MBR_GNDR_NM")) \
    .withColumn("MBR_RELSHP_CD", F.col("Lnk_ClmDmCLm_ABCOut2.MBR_RELSHP_CD")) \
    .withColumn("CLS_ID_Cls", F.col("Lnk_ClmDmCLm_ABCOut2.CLS_ID_Cls")) \
    .withColumn("CLS_DESC_Cls", F.col("Lnk_ClmDmCLm_ABCOut2.CLS_DESC_Cls")) \
    .withColumn("CLS_PLN_ID", F.col("Lnk_ClmDmCLm_ABCOut2.CLS_PLN_ID")) \
    .withColumn("CLS_PLN_DESC", F.col("Lnk_ClmDmCLm_ABCOut2.CLS_PLN_DESC")) \
    .withColumn("NTWK_ID", F.col("Lnk_ClmDmCLm_ABCOut2.NTWK_ID")) \
    .withColumn("NTWK_NM", F.col("Lnk_ClmDmCLm_ABCOut2.NTWK_NM")) \
    .withColumn("PROD_ID", rpad_if_char("Lnk_ClmDmCLm_ABCOut2.PROD_ID", 8)) \
    .withColumn("PREAUTH_ID", F.col("Lnk_ClmDmCLm_ABCOut1.PREAUTH_ID")) \
    .withColumn("CHK_NET_PAYMT_AMT_Pca_ClmChk", F.col("Lnk_ClmDmCLm_ABCOut1.CHK_NET_PAYMT_AMT_Pca_ClmChk")) \
    .withColumn("CHK_NO_Pca_AlmChk", F.col("Lnk_ClmDmCLm_ABCOut1.CHK_NO_Pca_AlmChk")) \
    .withColumn("CHK_SEQ_NO", F.col("Lnk_ClmDmCLm_ABCOut1.CHK_SEQ_NO")) \
    .withColumn("CHK_PAYE_NM_Pca_ClmChk", F.col("Lnk_ClmDmCLm_ABCOut1.CHK_PAYE_NM_Pca_ClmChk")) \
    .withColumn("CHK_PAYMT_REF_ID_Pca_ClmChk", F.col("Lnk_ClmDmCLm_ABCOut1.CHK_PAYMT_REF_ID_Pca_ClmChk")) \
    .withColumn("PCA_CLM_CHK_PAYE_TYP_CD", F.col("Lnk_ClmDmCLm_ABCOut1.PCA_CLM_CHK_PAYE_TYP_CD")) \
    .withColumn("PCA_CLM_CHK_PAYE_TYP_NM", F.col("Lnk_ClmDmCLm_ABCOut1.PCA_CLM_CHK_PAYE_TYP_NM")) \
    .withColumn("PCA_CLM_CHK_PAYMT_METH_CD", F.col("Lnk_ClmDmCLm_ABCOut1.PCA_CLM_CHK_PAYMT_METH_CD")) \
    .withColumn("PCA_CLM_CHK_PAYMT_METH_NM", F.col("Lnk_ClmDmCLm_ABCOut1.PCA_CLM_CHK_PAYMT_METH_NM")) \
    .withColumn("TOT_CNSD_AMT", F.col("Lnk_ClmDmCLm_ABCOut1.TOT_CNSD_AMT")) \
    .withColumn("TOT_PD_AMT", F.col("Lnk_ClmDmCLm_ABCOut1.TOT_PD_AMT")) \
    .withColumn("SUB_TOT_PCA_AVLBL_AMT", F.col("Lnk_ClmDmCLm_ABCOut1.SUB_TOT_PCA_AVLBL_AMT")) \
    .withColumn("SUB_TOT_PCA_PD_TO_DT_AMT", F.col("Lnk_ClmDmCLm_ABCOut1.SUB_TOT_PCA_PD_TO_DT_AMT")) \
    .withColumn("PROV_NM_Extrnl_Prov", F.col("Lnk_ClmDmCLm_ABCOut1.PROV_NM_Extrnl_Prov")) \
    .withColumn("CLM_SK_Clm_CLm_Ck", F.col("Lnk_ClmDmCLm_ABCOut1.CLM_SK_Clm_CLm_Ck")) \
    .withColumn("CLM_CHK_PAYMT_METH_CD", F.col("Lnk_ClmDmCLm_ABCOut1.CLM_CHK_PAYMT_METH_CD")) \
    .withColumn("CHK_PD_DT_SK", F.col("Lnk_ClmDmCLm_ABCOut1.CHK_PD_DT_SK")) \
    .withColumn("CHK_NET_PAYMT_AMT", F.col("Lnk_ClmDmCLm_ABCOut1.CHK_NET_PAYMT_AMT")) \
    .withColumn("CHK_NO", F.col("Lnk_ClmDmCLm_ABCOut1.CHK_NO")) \
    .withColumn("CHK_PAYE_NM", F.col("Lnk_ClmDmCLm_ABCOut1.CHK_PAYE_NM")) \
    .withColumn("CHK_PAYMT_REF_ID", F.col("Lnk_ClmDmCLm_ABCOut1.CHK_PAYMT_REF_ID")) \
    .withColumn("TRGT_CD_Pca_Typ_cd", F.col("Lnk_ClmDmCLm_ABCOut2.TRGT_CD_Pca_Typ_cd")) \
    .withColumn("TRGT_CD_NM_Pca_Typ_cd", F.col("Lnk_ClmDmCLm_ABCOut2.TRGT_CD_NM_Pca_Typ_cd")) \
    .withColumn("PAYBL_AMT_Clm_Ln_Sum_Paybl", F.col("Lnk_ClmDmCLm_ABCOut1.PAYBL_AMT_Clm_Ln_Sum_Paybl")) \
    .withColumn("CLM_ID_Ref_Get_Claim", F.col("Lnk_ClmDmCLm_ABCOut2.CLM_ID_Ref_Get_Claim")) \
    .withColumn("CLM_ID_Ref_Get_Pca_Clm", F.col("Lnk_ClmDmCLm_ABCOut2.CLM_ID_Ref_Get_Pca_Clm")) \
    .withColumn("CD_MPPNG_SK", F.col("Lnk_ClmDmCLm_ABCOut2.CD_MPPNG_SK")) \
    .withColumn("TRGT_CD_Rbc_Src_Sys", F.col("Lnk_ClmDmCLm_ABCOut2.TRGT_CD_Rbc_Src_Sys")) \
    .withColumn("CLM_SK_Bill_Svc_Prov_Svu", F.col("Lnk_ClmDmCLm_ABCOut1.CLM_SK_Bill_Svc_Prov_Svu")) \
    .withColumn("PROV_BILL_SVC_ID_Bill_Svc_Prov_Svu", F.col("Lnk_ClmDmCLm_ABCOut1.PROV_BILL_SVC_ID_Bill_Svc_Prov_Svu")) \
    .withColumn("CLM_SK", F.col("Lnk_ClmDmCLm_ABCOut1.CLM_SK")) \
    .withColumn("PROV_BILL_SVC_ID", F.col("Lnk_ClmDmCLm_ABCOut1.PROV_BILL_SVC_ID")) \
    .withColumn("PCA_EXTRNL_ID", F.col("Lnk_ClmDmCLm_ABCOut1.PCA_EXTRNL_ID")) \
    .withColumn("CLM_ID", F.col("Lnk_ClmDmCLm_ABCOut2.CLM_ID")) \
    .withColumn("PD_DT_SK", F.col("Lnk_ClmDmCLm_ABCOut2.PD_DT_SK")) \
    .withColumn("SVC_STRT_DT_SK", F.col("Lnk_ClmDmCLm_ABCOut2.SVC_STRT_DT_SK")) \
    .withColumn("SVC_END_DT_SK", F.col("Lnk_ClmDmCLm_ABCOut2.SVC_END_DT_SK")) \
    .withColumn("STTUS_DT_SK", F.col("Lnk_ClmDmCLm_ABCOut2.STTUS_DT_SK")) \
    .withColumn("CNSD_CHRG_AMT", F.col("Lnk_ClmDmCLm_ABCOut2.CNSD_CHRG_AMT")) \
    .withColumn("CHRG_AMT", F.col("Lnk_ClmDmCLm_ABCOut2.CHRG_AMT")) \
    .withColumn("PAYBL_AMT", F.col("Lnk_ClmDmCLm_ABCOut2.PAYBL_AMT")) \
    .withColumn("SUB_ID", rpad_if_char("Lnk_ClmDmCLm_ABCOut2.SUB_ID", 14)) \
    .withColumn("MBR_UNIQ_KEY", F.col("Lnk_ClmDmCLm_ABCOut2.MBR_UNIQ_KEY")) \
    .withColumn("BRTH_DT_SK", F.col("Lnk_ClmDmCLm_ABCOut2.BRTH_DT_SK")) \
    .withColumn("FIRST_NM", F.col("Lnk_ClmDmCLm_ABCOut2.FIRST_NM")) \
    .withColumn("MIDINIT", rpad_if_char("Lnk_ClmDmCLm_ABCOut2.MIDINIT", 1)) \
    .withColumn("LAST_NM", F.col("Lnk_ClmDmCLm_ABCOut2.LAST_NM")) \
    .withColumn("ALW_AMT", F.col("Lnk_ClmDmCLm_ABCOut2.ALW_AMT")) \
    .withColumn("DSALW_AMT", F.col("Lnk_ClmDmCLm_ABCOut2.DSALW_AMT")) \
    .withColumn("COINS_AMT", F.col("Lnk_ClmDmCLm_ABCOut2.COINS_AMT")) \
    .withColumn("COPAY_AMT", F.col("Lnk_ClmDmCLm_ABCOut2.COPAY_AMT")) \
    .withColumn("DEDCT_AMT", F.col("Lnk_ClmDmCLm_ABCOut2.DEDCT_AMT")) \
    .withColumn("ADJ_FROM_CLM_ID", F.col("Lnk_ClmDmCLm_ABCOut2.ADJ_FROM_CLM_ID")) \
    .withColumn("ADJ_TO_CLM_ID", F.col("Lnk_ClmDmCLm_ABCOut2.ADJ_TO_CLM_ID")) \
    .withColumn("PATN_ACCT_NO", F.col("Lnk_ClmDmCLm_ABCOut2.PATN_ACCT_NO")) \
    .withColumn("RFRNG_PROV_TX", F.col("Lnk_ClmDmCLm_ABCOut2.RFRNG_PROV_TX")) \
    .withColumn("RCVD_DT_SK", F.col("Lnk_ClmDmCLm_ABCOut2.RCVD_DT_SK")) \
    .withColumn("GRP_ID", F.col("Lnk_ClmDmCLm_ABCOut2.GRP_ID")) \
    .withColumn("GRP_UNIQ_KEY", F.col("Lnk_ClmDmCLm_ABCOut2.GRP_UNIQ_KEY")) \
    .withColumn("GRP_NM", F.col("Lnk_ClmDmCLm_ABCOut2.GRP_NM")) \
    .withColumn("SUB_UNIQ_KEY", F.col("Lnk_ClmDmCLm_ABCOut2.SUB_UNIQ_KEY")) \
    .withColumn("HOST_IN", rpad_if_char("Lnk_ClmDmCLm_ABCOut2.HOST_IN", 1)) \
    .withColumn("PRCS_DT_SK", F.col("Lnk_ClmDmCLm_ABCOut2.PRCS_DT_SK")) \
    .withColumn("MBR_SFX_NO", rpad_if_char("Lnk_ClmDmCLm_ABCOut2.MBR_SFX_NO", 2)) \
    .withColumn("PROV_AGMNT_ID", rpad_if_char("Lnk_ClmDmCLm_ABCOut2.PROV_AGMNT_ID", 12)) \
    .withColumn("REL_PCA_CLM_SK", F.col("Lnk_ClmDmCLm_ABCOut2.REL_PCA_CLM_SK")) \
    .withColumn("REL_BASE_CLM_SK", F.col("Lnk_ClmDmCLm_ABCOut2.REL_BASE_CLM_SK")) \
    .withColumn("CLM_SUB_BCBS_PLN_CD_SK", F.col("Lnk_ClmDmCLm_ABCOut2.CLM_SUB_BCBS_PLN_CD_SK")) \
    .withColumn("ACTL_PD_AMT", F.col("Lnk_ClmDmCLm_ABCOut1.ACTL_PD_AMT")) \
    .withColumn("FCLTY_CLM_DSCHG_STTUS_CD", F.col("Lnk_ClmDmCLm_ABCOut1.FCLTY_CLM_DSCHG_STTUS_CD")) \
    .withColumn("FCLTY_CLM_DSCHG_STTUS_NM", F.col("Lnk_ClmDmCLm_ABCOut1.FCLTY_CLM_DSCHG_STTUS_NM")) \
    .withColumn("GNRT_DRG_CD", F.col("Lnk_ClmDmCLm_ABCOut1.GNRT_DRG_CD")) \
    .withColumn("PCA_CLM_CHK_PD_DT", F.col("Lnk_ClmDmCLm_ABCOut1.PCA_CLM_CHK_PD_DT")) \
    .withColumn("CLM_ACTL_PD_AMT", F.col("Lnk_ClmDmCLm_ABCOut2.CLM_ACTL_PD_AMT"))

final_columns_in_order = [
    "ALPHA_PFX_SK","ALPHA_PFX_CD","CLM_SK_Svc_Prov","PROV_NM_Svc_Prov","PROV_ID_Svc_Prov","CMN_PRCT_ID_Svc_Prov",
    "GRP_PROV_ID","IPA_PROV_ID","DRG_CD_Fclty","GNRT_DRG_IN","CLM_SK_Fclty","CD_MPPNG_SK_Src_Sys","TRGT_CD_Src_Sys",
    "TRGT_CD_Clm_cap","CD_MPPNG_SK_Inter_pln_pgm","TRGT_CD_Inter_pln_pgm","CD_MPPNG_SK_Clm_Sttus","SRC_CD_Clm_Sttus",
    "TRGT_CD_Clm_Sttus","TRGT_CD_NM_Clm_Sttus","CD_MPPNG_SK_Clm_Typ","TRGT_CD_Clm_Typ","CD_MPPNG_SK_Clm_Paye",
    "TRGT_CD_Clm_Paye","CD_MPPNG_SK_Clm_Subtyp","TRGT_CD_Clm_Subtyp","TRGT_CD_Finl_disp","NDC","DRUG_LABEL_NM",
    "CMN_PRCT_ID_Drug","FIRST_NM_Drug","LAST_NM_Drug","CLM_SK_Drug","MBR_DIFF_PD_AMT","RX_ALW_QTY","RX_SUBMT_QTY",
    "RX_NO","PROV_NM_Drug","CLM_SK_Cob","PD_AMT","CLM_SK_Remit","SUPRESS_EOB_IN","SUPRESS_REMIT_IN","CNSD_CHRG_AMT_Remit",
    "ER_COPAY_AMT","INTRST_AMT","NO_RESP_AMT","PATN_RESP_AMT","PROV_WRTOFF_AMT","ALT_CHRG_IN","ALT_CHRG_PROV_WRTOFF_AMT",
    "FIRST_NM_Sub_Name","MIDINIT_Sub_Name","LAST_NM_Sub_Name","ADDR_LN_1_Sub_Addr","ADDR_LN_2_Sub_Addr","CITY_NM_Sub_Addr",
    "POSTAL_CD_Sub_Addr","SUB_ADDR_ST_CD","ADDR_LN_1_Pd_Prov_Addr","ADDR_LN_2_Pd_Prov_Addr","ADDR_LN_3_Pd_Prov_Addr",
    "CITY_NM_Pd_Prov_Addr","POSTAL_CD_Pd_Prov_Addr","PROV_ADDR_ST_CD_Pd_Prov_Addr","PROV_NM_Pcp_Prov","PROV_ID_Pcp_Prov",
    "PROV_ID_Pd_Prov","PROV_NM_Pd_Prov","CLM_SK_External_Mbr","MBR_BRTH_DT_SK","GRP_NM_External_Mbr","MBR_FIRST_NM",
    "MBR_MIDINIT","MBR_LAST_NM","SUB_GRP_BASE_NO","SUB_FIRST_NM","SUB_MIDINIT","SUB_LAST_NM","SUB_ADDR_LN_1","SUB_ADDR_LN_2",
    "SUB_CITY_NM","SUB_POSTAL_CD","CLM_EXTRNL_MBRSH_SUB_ST_CD","CLM_SK_Bill_Svc_Prov","PROV_BILL_SVC_ID_Bill_Svc_Prov",
    "CLM_SK_Bill_Svc_Grp","PROV_BILL_SVC_ID_Bill_Svc_Grp","CLM_SK_Its_Clm","SCCF_NO_Its_Clm","TRGT_CD_Its_Clm",
    "TRGT_CD_Ntwk_Sttus","TRGT_CD_NM_Ntwk_Sttus","TRGT_CD_Sttus_Audit","TRGT_CD_NM_Sttus_Audit","ADMS_DT_SK","DSCHG_DT_SK",
    "ADM_PHYS_PROV_ID","SUBMT_DRG_TX","FCLTY_CLM_ADMS_SRC_CD","FCLTY_CLM_ADMS_SRC_NM","FCLTY_CLM_ADMS_TYP_CD",
    "FCLTY_CLM_ADMS_TYP_NM","FCLTY_CLM_BILL_CLS_CD","FCLTY_CLM_BILL_CLS_NM","FCLTY_CLM_BILL_FREQ_CD","FCLTY_CLM_BILL_FREQ_NM",
    "FCLTY_CLM_BILL_TYP_CD","FCLTY_CLM_BILL_TYP_NM","BRTH_DT_SK_Mbr","MBR_GNDR_CD","MBR_GNDR_NM","MBR_RELSHP_CD",
    "CLS_ID_Cls","CLS_DESC_Cls","CLS_PLN_ID","CLS_PLN_DESC","NTWK_ID","NTWK_NM","PROD_ID","PREAUTH_ID",
    "CHK_NET_PAYMT_AMT_Pca_ClmChk","CHK_NO_Pca_AlmChk","CHK_SEQ_NO","CHK_PAYE_NM_Pca_ClmChk","CHK_PAYMT_REF_ID_Pca_ClmChk",
    "PCA_CLM_CHK_PAYE_TYP_CD","PCA_CLM_CHK_PAYE_TYP_NM","PCA_CLM_CHK_PAYMT_METH_CD","PCA_CLM_CHK_PAYMT_METH_NM",
    "TOT_CNSD_AMT","TOT_PD_AMT","SUB_TOT_PCA_AVLBL_AMT","SUB_TOT_PCA_PD_TO_DT_AMT","PROV_NM_Extrnl_Prov","CLM_SK_Clm_CLm_Ck",
    "CLM_CHK_PAYMT_METH_CD","CHK_PD_DT_SK","CHK_NET_PAYMT_AMT","CHK_NO","CHK_PAYE_NM","CHK_PAYMT_REF_ID","TRGT_CD_Pca_Typ_cd",
    "TRGT_CD_NM_Pca_Typ_cd","PAYBL_AMT_Clm_Ln_Sum_Paybl","CLM_ID_Ref_Get_Claim","CLM_ID_Ref_Get_Pca_Clm","CD_MPPNG_SK",
    "TRGT_CD_Rbc_Src_Sys","CLM_SK_Bill_Svc_Prov_Svu","PROV_BILL_SVC_ID_Bill_Svc_Prov_Svu","CLM_SK","PROV_BILL_SVC_ID",
    "PCA_EXTRNL_ID","CLM_ID","PD_DT_SK","SVC_STRT_DT_SK","SVC_END_DT_SK","STTUS_DT_SK","CNSD_CHRG_AMT","CHRG_AMT","PAYBL_AMT",
    "SUB_ID","MBR_UNIQ_KEY","BRTH_DT_SK","FIRST_NM","MIDINIT","LAST_NM","ALW_AMT","DSALW_AMT","COINS_AMT","COPAY_AMT",
    "DEDCT_AMT","ADJ_FROM_CLM_ID","ADJ_TO_CLM_ID","PATN_ACCT_NO","RFRNG_PROV_TX","RCVD_DT_SK","GRP_ID","GRP_UNIQ_KEY",
    "GRP_NM","SUB_UNIQ_KEY","HOST_IN","PRCS_DT_SK","MBR_SFX_NO","PROV_AGMNT_ID","REL_PCA_CLM_SK","REL_BASE_CLM_SK",
    "CLM_SUB_BCBS_PLN_CD_SK","ACTL_PD_AMT","FCLTY_CLM_DSCHG_STTUS_CD","FCLTY_CLM_DSCHG_STTUS_NM","GNRT_DRG_CD",
    "PCA_CLM_CHK_PD_DT","CLM_ACTL_PD_AMT"
]

df_final_sorted = df_final.select(final_columns_in_order)

from pyspark.sql.functions import col, when, lit, to_timestamp, trim, substring, rpad
from pyspark.sql.types import StringType

# Gather parameters
IDSOwner = get_widget_value('IDSOwner', '')
ids_secret_name = get_widget_value('ids_secret_name', '')
DatamartRunCycle = get_widget_value('DatamartRunCycle', '')
CommitPoint = get_widget_value('CommitPoint', '')


df_lookup_293 = df_final

# Build the enriched DataFrame (stage variables).
df_enriched = df_lookup_293

df_enriched = df_enriched.withColumn(
    "svSrcSysCd",
    when(col("CD_MPPNG_SK_Src_Sys").isNull(), lit(" ")).otherwise(col("TRGT_CD_Src_Sys"))
)

df_enriched = df_enriched.withColumn(
    "svExtrnlMbrsh",
    when(col("HOST_IN") == lit("Y"), lit("Y")).otherwise(lit("N"))
)

df_enriched = df_enriched.withColumn(
    "svMbrIn",
    when(col("svExtrnlMbrsh") == lit("Y"), lit("N")).otherwise(lit("Y"))
)

df_enriched = df_enriched.withColumn(
    "svClmFinlDispCd",
    when(col("TRGT_CD_Finl_disp").isNull(), lit(" ")).otherwise(col("TRGT_CD_Finl_disp"))
)

df_enriched = df_enriched.withColumn(
    "svClmSttusCd",
    when(col("CD_MPPNG_SK_Clm_Sttus").isNull(), lit(" ")).otherwise(col("TRGT_CD_Clm_Sttus"))
)

df_enriched = df_enriched.withColumn(
    "svClmType",
    when(col("CD_MPPNG_SK_Clm_Typ").isNull(), lit(" ")).otherwise(col("TRGT_CD_Clm_Typ"))
)

df_enriched = df_enriched.withColumn(
    "svRcvdDt",
    when(
        col("RCVD_DT_SK").isNull() |
        trim(col("RCVD_DT_SK")).isin("NA", "UNK", ""),
        lit(None)
    ).otherwise(
        to_timestamp(trim(col("RCVD_DT_SK")), "yyyy-MM-dd")
    )
)

df_enriched = df_enriched.withColumn(
    "svSvcStrtDt",
    when(
        col("SVC_STRT_DT_SK").isNull() |
        trim(col("SVC_STRT_DT_SK")).isin("NA", "UNK", ""),
        lit(None)
    ).otherwise(
        to_timestamp(trim(col("SVC_STRT_DT_SK")), "yyyy-MM-dd")
    )
)

df_enriched = df_enriched.withColumn(
    "svSvcEndDt",
    when(
        col("SVC_END_DT_SK").isNull() |
        trim(col("SVC_END_DT_SK")).isin("NA", "UNK", ""),
        lit(None)
    ).otherwise(
        to_timestamp(trim(col("SVC_END_DT_SK")), "yyyy-MM-dd")
    )
)

df_enriched = df_enriched.withColumn(
    "svSttusDt",
    when(
        col("STTUS_DT_SK").isNull() |
        trim(col("STTUS_DT_SK")).isin("NA", "UNK", ""),
        lit(None)
    ).otherwise(
        to_timestamp(trim(col("STTUS_DT_SK")), "yyyy-MM-dd")
    )
)

df_enriched = df_enriched.withColumn(
    "svBrthDtSk",
    when(
        col("svExtrnlMbrsh") == lit("Y"),
        when(col("CLM_SK_External_Mbr").isNull(), lit(None))
        .otherwise(
            when(
                trim(col("MBR_BRTH_DT_SK")).isin("NA", "UNK"),
                lit(None)
            ).otherwise(
                to_timestamp(trim(col("MBR_BRTH_DT_SK")), "yyyy-MM-dd")
            )
        )
    ).otherwise(
        when(
            trim(col("BRTH_DT_SK")).isin("NA", "UNK"),
            lit(None)
        ).otherwise(
            to_timestamp(trim(col("BRTH_DT_SK")), "yyyy-MM-dd")
        )
    )
)

df_enriched = df_enriched.withColumn(
    "svAlphaPfx",
    when(col("ALPHA_PFX_SK").isNull(), lit(" ")).otherwise(trim(col("ALPHA_PFX_CD")))
)

df_enriched = df_enriched.withColumn(
    "svAlphaPfxSSN",
    when(
        (col("svAlphaPfx") == lit(" ")) |
        (col("svAlphaPfx") == lit("NA")) |
        (col("svAlphaPfx") == lit("UNK")),
        lit(" ")
    ).otherwise(col("svAlphaPfx") + col("SUB_ID"))
)

df_enriched = df_enriched.withColumn(
    "svPrscrbID",
    when(
        col("CLM_SK_Drug").isNull(),
        lit(" ")
    ).otherwise(
        when(col("CMN_PRCT_ID_Drug") == lit("UNK"), lit(" "))
        .otherwise(col("CMN_PRCT_ID_Drug"))
    )
)

df_enriched = df_enriched.withColumn(
    "svCommitCnt",
    when(
        (( (col("@INROWNUM") - lit(1)) * col("@NUMPARTITIONS") + col("@PARTITIONNUM") + lit(1)) % col("CommitPoint") == lit(0)),
        col("svCommitCnt") + lit(1)
    ).otherwise(col("svCommitCnt"))
)

# Initialize svCommitCnt if not present
df_enriched = df_enriched.fillna({"svCommitCnt": 1})

df_enriched = df_enriched.withColumn(
    "svPcaTypCd",
    when(col("TRGT_CD_Pca_Typ_cd").isNull(), lit(None)).otherwise(col("TRGT_CD_Pca_Typ_cd"))
)

df_enriched = df_enriched.withColumn(
    "svItsClmSccfNo",
    when(col("SCCF_NO_Its_Clm").isNull(), lit(" "))
    .otherwise(
        when(col("SCCF_NO_Its_Clm") == lit("NA"), lit(" "))
        .otherwise(col("SCCF_NO_Its_Clm"))
    )
)

# Initialize stage variables needed for commit detection
df_enriched = df_enriched.fillna({"PrevCountNum": -1})

df_enriched = df_enriched.withColumn(
    "NewRow",
    when(col("svCommitCnt") == col("PrevCountNum"), lit(0)).otherwise(lit(1))
)

df_enriched = df_enriched.withColumn(
    "PrevCountNum",
    col("svCommitCnt")
)

df_enriched = df_enriched.withColumn(
    "svPrscbName",
    when(
        col("CLM_SK_Drug").isNull(),
        lit(" ")
    ).otherwise(
        when(
            col("CMN_PRCT_ID_Drug") == lit("UNK"),
            col("PROV_NM_Drug")
        ).otherwise(
            when(
                trim(col("FIRST_NM_Drug")) + lit(" ") + trim(col("LAST_NM_Drug")) == lit(" "),
                lit(" ")
            ).otherwise(trim(col("FIRST_NM_Drug")) + lit(" ") + trim(col("LAST_NM_Drug")))
        )
    )
)

# -------------- Output for Null_Count (Count_Out) --------------
df_count_out = df_enriched.filter(col("NewRow") == lit(1)).select(
    col("svCommitCnt").alias("Counter")
)

write_files(
    df_count_out,
    f"{adls_path}/load/Null.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# -------------- Output for Seq_B_CLM_DM_CLM (Snapshot) --------------
df_snapshot = df_enriched.select(
    col("svSrcSysCd").alias("SRC_SYS_CD"),
    col("CLM_ID").alias("CLM_ID"),
    col("svSvcStrtDt").alias("CLM_SVC_STRT_DT"),
    col("CHRG_AMT").alias("CLM_LN_TOT_CHRG_AMT"),
    col("PAYBL_AMT").alias("CLM_PAYBL_AMT"),
    col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    col("GRP_ID").alias("GRP_ID")
)

write_files(
    df_snapshot,
    f"{adls_path}/load/B_CLM_DM_CLM.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)

# -------------- Output for Seq_W_INT_INIT (Lnk_WClmInit_Out) --------------
df_wclm_init_out = df_enriched.select(
    col("svSrcSysCd").alias("SRC_SYS_CD"),
    col("CLM_ID").alias("CLM_ID"),
    col("svClmFinlDispCd").alias("CLM_FINL_DISP_CD"),
    col("svClmSttusCd").alias("CLM_STTUS_CD"),
    when(col("CLM_SK_Its_Clm").isNull(), lit(None)).otherwise(col("TRGT_CD_Its_Clm")).alias("CLM_TRNSMSN_STTUS_CD"),
    col("svClmType").alias("CLM_TYP_CD"),
    rpad(
        when(col("CLM_SK_Remit").isNull(), lit(" ")).otherwise(col("SUPRESS_EOB_IN")),
        1, " "
    ).alias("CLM_REMIT_HIST_SUPRES_EOB_IN"),
    rpad(
        when(col("CLM_SK_Remit").isNull(), lit(" ")).otherwise(col("SUPRESS_REMIT_IN")),
        1, " "
    ).alias("CLM_REMIT_HIST_SUPRES_REMIT_IN"),
    rpad(col("svMbrIn"), 1, " ").alias("CMPL_MBRSH_IN"),
    col("svRcvdDt").alias("CLM_RCVD_DT"),
    col("svSttusDt").alias("CLM_SRCH_DT"),
    col("svSvcStrtDt").alias("CLM_SVC_STRT_DT"),
    col("svSvcEndDt").alias("CLM_SVC_END_DT"),
    col("svBrthDtSk").alias("MBR_BRTH_DT"),
    col("CHRG_AMT").alias("CLM_CHRG_AMT"),
    col("PAYBL_AMT").alias("CLM_PAYBL_AMT"),
    when(col("CLM_SK_Remit").isNull(), lit(0.00)).otherwise(col("CNSD_CHRG_AMT_Remit")).alias("CLM_REMIT_HIST_CNSD_CHRG_AMT"),
    when(col("CLM_SK_Remit").isNull(), lit(0.00)).otherwise(col("NO_RESP_AMT")).alias("CLM_REMIT_HIST_NO_RESP_AMT"),
    when(col("CLM_SK_Remit").isNull(), lit(0.00)).otherwise(col("PATN_RESP_AMT")).alias("CLM_REMIT_HIST_PATN_RESP_AMT"),
    col("svAlphaPfxSSN").alias("ALPHA_PFX_SUB_ID"),
    col("SUB_ID").alias("CLM_SUB_ID"),
    col("svPrscrbID").alias("CMN_PRCT_PRSCRB_CMN_PRCT_ID"),
    col("GRP_UNIQ_KEY").alias("GRP_UNIQ_KEY"),
    when(
        col("svExtrnlMbrsh") == lit("Y"),
        when(col("SUB_GRP_BASE_NO").isNull(), lit(" ")).otherwise(col("SUB_GRP_BASE_NO"))
    ).otherwise(col("GRP_ID")).alias("GRP_ID"),
    when(
        col("svExtrnlMbrsh") == lit("Y"),
        when(col("GRP_NM_External_Mbr").isNull(), lit(" ")).otherwise(col("GRP_NM_External_Mbr"))
    ).otherwise(col("GRP_NM")).alias("GRP_NM"),
    col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    when(
        col("svExtrnlMbrsh") == lit("Y"),
        when(col("MBR_FIRST_NM").isNull(), lit(" ")).otherwise(col("MBR_FIRST_NM"))
    ).otherwise(
        when(col("FIRST_NM").isNull(), lit(" ")).otherwise(col("FIRST_NM"))
    ).alias("MBR_FIRST_NM"),
    rpad(
        when(
            col("svExtrnlMbrsh") == lit("Y"),
            when((col("MBR_MIDINIT").isNull()) | (trim(col("MBR_MIDINIT")) == lit(",")), lit(" ")).otherwise(trim(col("MBR_MIDINIT")))
        ).otherwise(
            when(col("MIDINIT").isNull(), lit(" ")).otherwise(trim(col("MIDINIT")))
        ),
        1, " "
    ).alias("MBR_MIDINIT"),
    when(
        col("svExtrnlMbrsh") == lit("Y"),
        when(col("MBR_LAST_NM").isNull(), lit(" ")).otherwise(col("MBR_LAST_NM"))
    ).otherwise(
        when(col("LAST_NM").isNull(), lit(" ")).otherwise(col("LAST_NM"))
    ).alias("MBR_LAST_NM"),
    when(
        col("SRC_CD_Clm_Sttus").isNull(),
        lit(" ")
    ).otherwise(
        when(
            (col("SRC_CD_Clm_Sttus").isin("A02","A08","A09")),
            when(
                col("CLM_SK_Bill_Svc_Prov").isNull(),
                when(col("CLM_SK_Bill_Svc_Grp").isNull(), lit(" ")).otherwise(col("PROV_BILL_SVC_ID_Bill_Svc_Grp"))
            ).otherwise(col("PROV_BILL_SVC_ID_Bill_Svc_Prov"))
        ).otherwise(
            when(
                col("CLM_SK_Bill_Svc_Prov_Svu").isNull(),
                when(col("CLM_SK").isNull(), lit(" ")).otherwise(col("PROV_BILL_SVC_ID"))
            ).otherwise(col("PROV_BILL_SVC_ID_Bill_Svc_Prov_Svu"))
        )
    ).alias("PROV_BILL_SVC_ID"),
    when(col("PROV_ID_Pd_Prov").isNull(), lit(" ")).otherwise(col("PROV_ID_Pd_Prov")).alias("PROV_PD_PROV_ID"),
    when(col("PROV_ID_Pcp_Prov").isNull(), lit(" ")).otherwise(col("PROV_ID_Pcp_Prov")).alias("PROV_PCP_PROV_ID"),
    when(col("GRP_PROV_ID").isNull(), lit(" ")).otherwise(col("GRP_PROV_ID")).alias("PROV_REL_GRP_PROV_ID"),
    when(col("IPA_PROV_ID").isNull(), lit(" ")).otherwise(col("IPA_PROV_ID")).alias("PROV_REL_IPA_PROV_ID"),
    when(col("PROV_ID_Svc_Prov").isNull(), lit(" ")).otherwise(col("PROV_ID_Svc_Prov")).alias("PROV_SVC_PROV_ID"),
    col("SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    col("svCommitCnt").alias("CMT_CT"),
    col("DatamartRunCycle").alias("LAST_UPDT_RUN_CYC_NO"),
    col("svPcaTypCd").alias("PCA_TYP_CD")
)

write_files(
    df_wclm_init_out,
    f"{adls_path}/load/W_CLM_INIT.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)

# -------------- Output for Seq_CLM_DM_CLM (Lnk_ClmDmCLm_ABCOut) --------------
df_clmdmclm_abc_out = df_enriched.select(
    col("svSrcSysCd").alias("SRC_SYS_CD"),
    col("CLM_ID").alias("CLM_ID"),
    when(col("TRGT_CD_Clm_cap").isNull(), lit(" ")).otherwise(col("TRGT_CD_Clm_cap")).alias("CLM_CAP_CD"),
    col("svClmFinlDispCd").alias("CLM_FINL_DISP_CD"),
    when(col("CD_MPPNG_SK_Inter_pln_pgm").isNull(), lit(" ")).otherwise(col("TRGT_CD_Inter_pln_pgm")).alias("CLM_INTER_PLN_PGM_CD"),
    when(col("CD_MPPNG_SK_Clm_Paye").isNull(), lit(" ")).otherwise(col("TRGT_CD_Clm_Paye")).alias("CLM_PAYE_CD"),
    col("svClmSttusCd").alias("CLM_STTUS_CD"),
    when(col("CD_MPPNG_SK_Clm_Subtyp").isNull(), lit(" ")).otherwise(col("TRGT_CD_Clm_Subtyp")).alias("CLM_SUBTYP_CD"),
    when(col("CLM_SK_Its_Clm").isNull(), lit(None)).otherwise(col("TRGT_CD_Its_Clm")).alias("CLM_TRNSMSN_STTUS_CD"),
    col("svClmType").alias("CLM_TYP_CD"),
    when(
        col("CLM_SK_Fclty").isNull(),
        lit(" ")
    ).otherwise(
        when(col("GNRT_DRG_IN") == lit("Y"), lit(" ")).otherwise(col("DRG_CD_Fclty"))
    ).alias("FCLTY_CLM_SRC_SYS_GNRT_DRG_CD"),
    when(col("CLM_SK_Drug").isNull(), lit(" ")).otherwise(col("NDC")).alias("NDC"),
    rpad(col("svMbrIn"), 1, " ").alias("CMPL_MBRSH_IN"),
    when(
        col("PD_DT_SK").isNull() | trim(col("PD_DT_SK")).isin("NA","UNK",""),
        lit(None)
    ).otherwise(
        to_timestamp(trim(col("PD_DT_SK")), "yyyy-MM-dd")
    ).alias("CLM_PD_DT"),
    col("svRcvdDt").alias("CLM_RCVD_DT"),
    when(
        col("CHK_PD_DT_SK").isNull(),
        lit(None)
    ).otherwise(
        to_timestamp(trim(col("CHK_PD_DT_SK")), "yyyy-MM-dd")
    ).alias("CLM_REMIT_HIST_CHK_PD_DT"),
    col("svSvcStrtDt").alias("CLM_SVC_STRT_DT"),
    col("svSvcEndDt").alias("CLM_SVC_END_DT"),
    col("svSttusDt").alias("CLM_STTUS_DT"),
    when(col("ACTL_PD_AMT") >= lit(0), col("ACTL_PD_AMT")).otherwise(col("CLM_ACTL_PD_AMT")).alias("CLM_ACTL_PD_AMT"),
    col("ALW_AMT").alias("CLM_ALW_AMT"),
    col("CHRG_AMT").alias("CLM_CHRG_AMT"),
    when(col("CLM_SK_Cob").isNull(), lit(0.00)).otherwise(col("PD_AMT")).alias("CLM_COB_PD_AMT"),
    col("COINS_AMT").alias("CLM_COINS_AMT"),
    col("CNSD_CHRG_AMT").alias("CLM_CNSD_CHRG_AMT"),
    col("COPAY_AMT").alias("CLM_COPAY_AMT"),
    col("DEDCT_AMT").alias("CLM_DEDCT_AMT"),
    col("DSALW_AMT").alias("CLM_DSALW_AMT"),
    col("PAYBL_AMT").alias("CLM_PAYBL_AMT"),
    when(col("CLM_SK_Clm_CLm_Ck").isNull(), lit(0.0)).otherwise(col("CHK_NET_PAYMT_AMT")).alias("CLM_REMIT_HIST_CHK_NET_PAY_AMT"),
    when(col("CLM_SK_Remit").isNull(), lit(0.0)).otherwise(col("ER_COPAY_AMT")).alias("CLM_REMIT_HIST_ER_COPAY_AMT"),
    when(col("INTRST_AMT").isNull(), lit(0.0)).otherwise(col("INTRST_AMT")).alias("CLM_REMIT_HIST_INTRST_AMT"),
    when(col("NO_RESP_AMT").isNull(), lit(0.0)).otherwise(col("NO_RESP_AMT")).alias("CLM_REMIT_HIST_NO_RESP_AMT"),
    when(col("PATN_RESP_AMT").isNull(), lit(0.0)).otherwise(col("PATN_RESP_AMT")).alias("CLM_REMIT_HIST_PATN_RESP_AMT"),
    when(col("PROV_WRTOFF_AMT") >= lit(0), col("PROV_WRTOFF_AMT")).otherwise(lit(0.00)).alias("CLM_REMIT_HIST_PROV_WRTOFF_AMT"),
    when(col("CLM_SK_Drug").isNull(), lit(0)).otherwise(col("MBR_DIFF_PD_AMT")).alias("DRUG_CLM_MBR_DIFF_PD_AMT"),
    when(col("CLM_SK_Clm_CLm_Ck").isNull(), lit(0)).otherwise(col("CHK_NO")).alias("CLM_REMIT_HIST_CHK_NO"),
    when(col("CLM_SK_Drug").isNull(), lit(0)).otherwise(col("RX_ALW_QTY")).alias("DRUG_CLM_RX_ALW_QTY"),
    when(col("CLM_SK_Drug").isNull(), lit(0)).otherwise(col("RX_SUBMT_QTY")).alias("DRUG_CLM_RX_SUBMT_QTY"),
    col("svAlphaPfx").alias("ALPHA_PFX_CD"),
    col("ADJ_FROM_CLM_ID").alias("CLM_ADJ_FROM_CLM_ID"),
    col("ADJ_TO_CLM_ID").alias("CLM_ADJ_TO_CLM_ID"),
    when(col("PATN_ACCT_NO").isNull(), lit(None)).otherwise(col("PATN_ACCT_NO")).alias("CLM_PATN_ACCT_NO"),
    when(col("RFRNG_PROV_TX").isNull(), lit(None)).otherwise(col("RFRNG_PROV_TX")).alias("CLM_RFRNG_PROV_TX"),
    when(col("CLM_SK_Clm_CLm_Ck").isNull(), lit(None)).otherwise(col("CHK_PAYE_NM")).alias("CLM_REMIT_HIST_CHK_PAYE_NM"),
    when(col("CLM_SK_Clm_CLm_Ck").isNull(), lit(None)).otherwise(col("CHK_PAYMT_REF_ID")).alias("CLM_REMIT_HIST_CHK_PAY_REF_ID"),
    when(col("CD_MPPNG_SK_Clm_Sttus").isNull(), lit(" ")).otherwise(col("TRGT_CD_NM_Clm_Sttus")).alias("CLM_STTUS_CD_NM"),
    col("SUB_ID").alias("CLM_SUB_ID"),
    col("svPrscrbID").alias("CMN_PRCT_PRSCRB_CMN_PRCT_ID"),
    col("svPrscbName").alias("CMN_PRCT_PRSCRB_CMN_PRCT_NM"),
    when(col("CLM_SK_Svc_Prov").isNull(), lit(" ")).otherwise(col("CMN_PRCT_ID_Svc_Prov")).alias("CMN_PRCT_SVC_CMN_PRCT_ID"),
    when(col("CLM_SK_Drug").isNull(), lit(" ")).otherwise(col("RX_NO")).alias("DRUG_CLM_RX_NO"),
    col("GRP_UNIQ_KEY").alias("GRP_UNIQ_KEY"),
    col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    when(
        col("svExtrnlMbrsh") == lit("Y"),
        when(col("MBR_FIRST_NM").isNull(), lit(" ")).otherwise(col("MBR_FIRST_NM"))
    ).otherwise(
        when(col("FIRST_NM").isNull(), lit(" ")).otherwise(col("FIRST_NM"))
    ).alias("MBR_FIRST_NM"),
    rpad(
        when(
            col("svExtrnlMbrsh") == lit("Y"),
            when(col("MBR_MIDINIT").isNull() | (trim(col("MBR_MIDINIT")) == lit(",")), lit(" ")).otherwise(trim(col("MBR_MIDINIT")))
        ).otherwise(
            when(col("MIDINIT").isNull(), lit(" ")).otherwise(trim(col("MIDINIT")))
        ),
        1, " "
    ).alias("MBR_MIDINIT"),
    when(
        col("svExtrnlMbrsh") == lit("Y"),
        when(col("MBR_LAST_NM").isNull(), lit(" ")).otherwise(col("MBR_LAST_NM"))
    ).otherwise(
        when(col("LAST_NM").isNull(), lit(" ")).otherwise(col("LAST_NM"))
    ).alias("MBR_LAST_NM"),
    when(
        col("svExtrnlMbrsh") == lit("Y"),
        when(col("SUB_FIRST_NM").isNull(), lit(" ")).otherwise(col("SUB_FIRST_NM"))
    ).otherwise(
        when(col("FIRST_NM_Sub_Name").isNull(), lit(" ")).otherwise(col("FIRST_NM_Sub_Name"))
    ).alias("MBR_SUB_FIRST_NM"),
    rpad(
        when(
            col("svExtrnlMbrsh") == lit("Y"),
            when(col("SUB_MIDINIT").isNull(), lit(" ")).otherwise(col("SUB_MIDINIT"))
        ).otherwise(
            when(col("MIDINIT_Sub_Name").isNull(), lit(" ")).otherwise(col("MIDINIT_Sub_Name"))
        ),
        1, " "
    ).alias("MBR_SUB_MIDINIT"),
    when(
        col("svExtrnlMbrsh") == lit("Y"),
        when(col("SUB_LAST_NM").isNull(), lit(" ")).otherwise(col("SUB_LAST_NM"))
    ).otherwise(
        when(col("LAST_NM_Sub_Name").isNull(), lit(" ")).otherwise(col("LAST_NM_Sub_Name"))
    ).alias("MBR_SUB_LAST_NM"),
    when(col("DRUG_LABEL_NM").isNull(), lit(" ")).otherwise(col("DRUG_LABEL_NM")).alias("NDC_DRUG_LABEL_NM"),
    when(col("ADDR_LN_1_Pd_Prov_Addr").isNull(), lit(" ")).otherwise(col("ADDR_LN_1_Pd_Prov_Addr")).alias("PROV_ADDR_PD_REMIT_ADDR_LN_1"),
    when(col("ADDR_LN_2_Pd_Prov_Addr").isNull(), lit(" ")).otherwise(col("ADDR_LN_2_Pd_Prov_Addr")).alias("PROV_ADDR_PD_REMIT_ADDR_LN_2"),
    when(col("ADDR_LN_3_Pd_Prov_Addr").isNull(), lit(" ")).otherwise(col("ADDR_LN_3_Pd_Prov_Addr")).alias("PROV_ADDR_PD_REMIT_ADDR__LN_3"),
    when(col("CITY_NM_Pd_Prov_Addr").isNull(), lit(" ")).otherwise(col("CITY_NM_Pd_Prov_Addr")).alias("PROV_ADDR_PD_REMIT_CITY_NM"),
    when(
        col("PROV_ADDR_ST_CD_Pd_Prov_Addr").isNull() | (col("PROV_ADDR_ST_CD_Pd_Prov_Addr") == lit("UNK")),
        lit(" ")
    ).otherwise(col("PROV_ADDR_ST_CD_Pd_Prov_Addr")).alias("PROV_ADDR_PD_REMIT_ST_CD"),
    rpad(
        when(col("POSTAL_CD_Pd_Prov_Addr").isNull(), lit(" ")).otherwise(col("POSTAL_CD_Pd_Prov_Addr")),
        11, " "
    ).alias("PROV_ADDR_PD_REMIT_POSTAL_CD"),
    when(col("PROV_ID_Pd_Prov").isNull(), lit(" ")).otherwise(col("PROV_ID_Pd_Prov")).alias("PROV_PD_PROV_ID"),
    when(col("PROV_NM_Pd_Prov").isNull(), lit(" ")).otherwise(col("PROV_NM_Pd_Prov")).alias("PROV_PD_PROV_NM"),
    when(col("PROV_ID_Pcp_Prov").isNull(), lit(" ")).otherwise(col("PROV_ID_Pcp_Prov")).alias("PROV_PCP_PROV_ID"),
    when(col("PROV_NM_Pcp_Prov").isNull(), lit(" ")).otherwise(col("PROV_NM_Pcp_Prov")).alias("PROV_PCP_PROV_NM"),
    when(col("GRP_PROV_ID").isNull(), lit(" ")).otherwise(col("GRP_PROV_ID")).alias("PROV_REL_GRP_PROV_ID"),
    when(col("IPA_PROV_ID").isNull(), lit(" ")).otherwise(col("IPA_PROV_ID")).alias("PROV_REL_IPA_PROV_ID"),
    when(col("PROV_ID_Svc_Prov").isNull(), lit(" ")).otherwise(col("PROV_ID_Svc_Prov")).alias("PROV_SVC_PROV_ID"),
    when(col("PROV_NM_Svc_Prov").isNull(), lit(" ")).otherwise(col("PROV_NM_Svc_Prov")).alias("PROV_SVC_PROV_NM"),
    col("SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    when(
        col("svExtrnlMbrsh") == lit("Y"),
        when(col("SUB_ADDR_LN_1").isNull(), lit(None)).otherwise(col("SUB_ADDR_LN_1"))
    ).otherwise(
        when(col("ADDR_LN_1_Sub_Addr").isNull(), lit(None)).otherwise(col("ADDR_LN_1_Sub_Addr"))
    ).alias("SUB_ADDR_LN_1"),
    when(
        col("svExtrnlMbrsh") == lit("Y"),
        when(col("CLM_SK_External_Mbr").isNull(), lit(None)).otherwise(col("SUB_ADDR_LN_2"))
    ).otherwise(
        when(col("ADDR_LN_2_Sub_Addr").isNull(), lit(None)).otherwise(col("ADDR_LN_2_Sub_Addr"))
    ).alias("SUB_ADDR_LN_2"),
    when(
        col("svExtrnlMbrsh") == lit("Y"),
        when(col("CLM_SK_External_Mbr").isNull(), lit(None)).otherwise(col("SUB_CITY_NM"))
    ).otherwise(
        when(col("CITY_NM_Sub_Addr").isNull(), lit(None)).otherwise(col("CITY_NM_Sub_Addr"))
    ).alias("SUB_ADDR_CITY_NM"),
    rpad(
        when(
            col("svExtrnlMbrsh") == lit("Y"),
            when(col("CLM_SK_External_Mbr").isNull(), lit(" "))
            .otherwise(
                when(col("CLM_EXTRNL_MBRSH_SUB_ST_CD") == lit("UNK"), lit(" ")).otherwise(substring(col("CLM_EXTRNL_MBRSH_SUB_ST_CD"), 1, 2))
            )
        ).otherwise(
            when(col("SUB_ADDR_ST_CD").isNull(), lit(" "))
            .otherwise(
                when(col("SUB_ADDR_ST_CD") == lit("UNK"), lit(" ")).otherwise(substring(col("SUB_ADDR_ST_CD"), 1, 2))
            )
        ),
        2, " "
    ).alias("SUB_ADDR_ST_CD"),
    rpad(
        when(
            col("svExtrnlMbrsh") == lit("Y"),
            when(col("CLM_SK_External_Mbr").isNull(), lit(" ")).otherwise(col("SUB_POSTAL_CD"))
        ).otherwise(
            when(col("POSTAL_CD_Sub_Addr").isNull(), lit(" ")).otherwise(col("POSTAL_CD_Sub_Addr"))
        ),
        11, " "
    ).alias("SUB_ADDR_POSTAL_CD"),
    when(col("TRGT_CD_Ntwk_Sttus").isNull(), lit(" ")).otherwise(col("TRGT_CD_Ntwk_Sttus")).alias("CLM_NTWK_STTUS_CD"),
    when(col("TRGT_CD_NM_Ntwk_Sttus").isNull(), lit(" ")).otherwise(col("TRGT_CD_NM_Ntwk_Sttus")).alias("CLM_NTWK_STTUS_NM"),
    when(
        col("CLM_CHK_PAYMT_METH_CD").isNull() | (col("CLM_CHK_PAYMT_METH_CD") == lit("NA")),
        lit(" ")
    ).otherwise(col("CLM_CHK_PAYMT_METH_CD")).alias("CLM_REMIT_HIST_PAYMT_METH_CD"),
    when(
        col("TRGT_CD_Sttus_Audit").isNull() | (trim(col("TRGT_CD_Sttus_Audit")) == lit("")),
        lit(None)
    ).otherwise(col("TRGT_CD_Sttus_Audit")).alias("CLM_STTUS_CHG_RSN_CD"),
    when(col("TRGT_CD_NM_Sttus_Audit").isNull(), lit(" ")).otherwise(col("TRGT_CD_NM_Sttus_Audit")).alias("CLM_STTUS_CHG_RSN_NM"),
    when(col("FCLTY_CLM_ADMS_TYP_CD").isNull(), lit(" ")).otherwise(col("FCLTY_CLM_ADMS_TYP_CD")).alias("FCLTY_CLM_ADMS_TYP_CD"),
    when(col("FCLTY_CLM_ADMS_TYP_NM").isNull(), lit(" ")).otherwise(col("FCLTY_CLM_ADMS_TYP_NM")).alias("FCLTY_CLM_ADMS_TYP_NM"),
    when(col("FCLTY_CLM_ADMS_SRC_CD").isNull(), lit(" ")).otherwise(col("FCLTY_CLM_ADMS_SRC_CD")).alias("FCLTY_CLM_ADMS_SRC_CD"),
    when(col("FCLTY_CLM_ADMS_SRC_NM").isNull(), lit(" ")).otherwise(col("FCLTY_CLM_ADMS_SRC_NM")).alias("FCLTY_CLM_ADMS_SRC_NM"),
    when(col("FCLTY_CLM_BILL_CLS_CD").isNull(), lit(" ")).otherwise(col("FCLTY_CLM_BILL_CLS_CD")).alias("FCLTY_CLM_BILL_CLS_CD"),
    when(col("FCLTY_CLM_BILL_CLS_NM").isNull(), lit(" ")).otherwise(col("FCLTY_CLM_BILL_CLS_NM")).alias("FCLTY_CLM_BILL_CLS_NM"),
    when(col("FCLTY_CLM_BILL_FREQ_CD").isNull(), lit(" ")).otherwise(col("FCLTY_CLM_BILL_FREQ_CD")).alias("FCLTY_CLM_BILL_FREQ_CD"),
    when(col("FCLTY_CLM_BILL_FREQ_NM").isNull(), lit(" ")).otherwise(col("FCLTY_CLM_BILL_FREQ_NM")).alias("FCLTY_CLM_BILL_FREQ_NM"),
    when(col("FCLTY_CLM_BILL_TYP_CD").isNull(), lit(" ")).otherwise(col("FCLTY_CLM_BILL_TYP_CD")).alias("FCLTY_CLM_BILL_TYP_CD"),
    when(col("FCLTY_CLM_BILL_TYP_NM").isNull(), lit(" ")).otherwise(col("FCLTY_CLM_BILL_TYP_NM")).alias("FCLTY_CLM_BILL_TYP_NM"),
    when(col("FCLTY_CLM_DSCHG_STTUS_CD").isNull(), lit(" ")).otherwise(col("FCLTY_CLM_DSCHG_STTUS_CD")).alias("FCLTY_CLM_DSCHG_STTUS_CD"),
    when(col("FCLTY_CLM_DSCHG_STTUS_NM").isNull(), lit(" ")).otherwise(col("FCLTY_CLM_DSCHG_STTUS_NM")).alias("FCLTY_CLM_DSCHG_STTUS_NM"),
    when(col("MBR_GNDR_CD").isNull(), lit(" ")).otherwise(col("MBR_GNDR_CD")).alias("MBR_GNDR_CD"),
    when(col("MBR_GNDR_NM").isNull(), lit(" ")).otherwise(col("MBR_GNDR_NM")).alias("MBR_GNDR_NM"),
    rpad(col("HOST_IN"), 1, " ").alias("CLM_HOST_IN"),
    when(
        col("PRCS_DT_SK").isNull() | (trim(col("PRCS_DT_SK")) == lit("")),
        lit(None)
    ).otherwise(
        to_timestamp(trim(col("PRCS_DT_SK")), "yyyy-MM-dd")
    ).alias("CLM_PRCS_DT"),
    lit("").alias("DM_LAST_UPDT_DT"),
    when(
        trim(col("ADMS_DT_SK")) == lit(""),
        lit(None)
    ).otherwise(
        to_timestamp(trim(col("ADMS_DT_SK")), "yyyy-MM-dd")
    ).alias("FCLTY_CLM_ADMS_DT"),
    when(
        (trim(col("DSCHG_DT_SK")) == lit("")) |
        col("DSCHG_DT_SK").isNull(),
        lit(None)
    ).otherwise(
        to_timestamp(trim(col("DSCHG_DT_SK")), "yyyy-MM-dd")
    ).alias("FCLTY_CLM_DSCHG_DT"),
    when(
        trim(col("BRTH_DT_SK_Mbr")).isin("NA","UNK","") | col("BRTH_DT_SK_Mbr").isNull(),
        lit(None)
    ).otherwise(
        to_timestamp(trim(col("BRTH_DT_SK_Mbr")), "yyyy-MM-dd")
    ).alias("MBR_BRTH_DT"),
    when(col("PREAUTH_ID").isNull(), lit(" ")).otherwise(col("PREAUTH_ID")).alias("CLM_AUTH_NO"),
    when(col("CLS_ID_Cls").isNull(), lit(" ")).otherwise(col("CLS_ID_Cls")).alias("CLS_ID"),
    when(col("CLS_DESC_Cls").isNull(), lit(" ")).otherwise(col("CLS_DESC_Cls")).alias("CLS_DESC"),
    when(col("CLS_PLN_ID").isNull(), lit(" ")).otherwise(col("CLS_PLN_ID")).alias("CLS_PLN_ID"),
    when(col("CLS_PLN_DESC").isNull(), lit(" ")).otherwise(col("CLS_PLN_DESC")).alias("CLS_PLN_DESC"),
    when(col("ADM_PHYS_PROV_ID").isNull(), lit(" ")).otherwise(col("ADM_PHYS_PROV_ID")).alias("FCLTY_CLM_ADM_PHYS_PROV_ID"),
    when(col("GNRT_DRG_CD").isNull(), lit(" ")).otherwise(col("GNRT_DRG_CD")).alias("GNRT_DRG_CD"),
    col("GRP_ID").alias("GRP_ID"),
    when(col("MBR_RELSHP_CD").isNull(), lit(" ")).otherwise(col("MBR_RELSHP_CD")).alias("MBR_RELSHP_CD"),
    when(col("MBR_SFX_NO").isNull(), lit(" ")).otherwise(col("MBR_SFX_NO")).alias("MBR_SFX_NO"),
    when(col("NTWK_ID").isNull(), lit(" ")).otherwise(col("NTWK_ID")).alias("NTWK_ID"),
    when(col("NTWK_NM").isNull(), lit(" ")).otherwise(col("NTWK_NM")).alias("NTWK_NM"),
    when(col("PROD_ID").isNull(), lit(" ")).otherwise(col("PROD_ID")).alias("PROD_ID"),
    rpad(col("PROV_AGMNT_ID"), 12, " ").alias("PROV_AGMNT_ID"),
    when(
        trim(col("SUBMT_DRG_TX")) == lit("") | col("SUBMT_DRG_TX").isNull(),
        lit(" ")
    ).otherwise(col("SUBMT_DRG_TX")).alias("SUBMT_DRG_CD"),
    col("SUB_ID").alias("SUB_ID"),
    col("DatamartRunCycle").alias("LAST_UPDT_RUN_CYC_NO"),
    when(col("PCA_CLM_CHK_PAYE_TYP_CD").isNull(), lit(" ")).otherwise(col("PCA_CLM_CHK_PAYE_TYP_CD")).alias("PCA_CLM_CHK_PAYE_TYP_CD"),
    when(col("PCA_CLM_CHK_PAYE_TYP_NM").isNull(), lit(None)).otherwise(col("PCA_CLM_CHK_PAYE_TYP_NM")).alias("PCA_CLM_CHK_PAYE_TYP_NM"),
    when(
        col("PCA_CLM_CHK_PAYMT_METH_CD").isNull() | (col("PCA_CLM_CHK_PAYMT_METH_CD") == lit("NA")),
        lit(None)
    ).otherwise(col("PCA_CLM_CHK_PAYMT_METH_CD")).alias("PCA_CLM_CHK_PAYMT_METH_CD"),
    when(
        col("PCA_CLM_CHK_PAYMT_METH_NM").isNull() | (col("PCA_CLM_CHK_PAYMT_METH_NM") == lit("NA")),
        lit(" ")
    ).otherwise(col("PCA_CLM_CHK_PAYMT_METH_NM")).alias("PCA_CLM_CHK_PAYMT_METH_NM"),
    when(
        col("TRGT_CD_NM_Pca_Typ_cd").isNull() | (col("TRGT_CD_NM_Pca_Typ_cd") == lit("NA")),
        lit(None)
    ).otherwise(col("TRGT_CD_NM_Pca_Typ_cd")).alias("PCA_TYP_NM"),
    when(
        substring(col("CLM_ID"), 6, 2) == lit("RH"),
        lit("Y")
    ).otherwise(
        when(
            substring(col("CLM_ID"), 6, 1).isin("K","G","H"),
            lit("Y")
        ).otherwise(lit("N"))
    ).alias("CLM_HOME_IN"),
    when(col("PCA_CLM_CHK_PD_DT").isNull(), lit(None)).otherwise(to_timestamp(trim(col("PCA_CLM_CHK_PD_DT")), "yyyy-MM-dd")).alias("PCA_CLM_CHK_PD_DT"),
    when(col("TOT_CNSD_AMT").isNotNull(), col("TOT_CNSD_AMT"))
    .otherwise(
        when(
            (col("TRGT_CD_Pca_Typ_cd") == lit("RUNOUT")) | (col("TRGT_CD_Pca_Typ_cd") == lit("EMPWBNF")),
            col("CHRG_AMT")
        ).otherwise(lit(0.00))
    ).alias("CLM_PCA_TOT_CNSD_AMT"),
    when(col("TOT_PD_AMT").isNotNull(), col("TOT_PD_AMT"))
    .otherwise(
        when(
            (col("TRGT_CD_Pca_Typ_cd") == lit("RUNOUT")) | (col("TRGT_CD_Pca_Typ_cd") == lit("EMPWBNF")),
            col("PAYBL_AMT_Clm_Ln_Sum_Paybl")
        ).otherwise(lit(0.00))
    ).alias("CLM_PCA_TOT_PD_AMT"),
    when(col("SUB_TOT_PCA_AVLBL_AMT").isNotNull(), col("SUB_TOT_PCA_AVLBL_AMT")).otherwise(lit(0.00)).alias("CLM_PCA_SUB_TOT_PCA_AVLBL_AMT"),
    when(col("SUB_TOT_PCA_PD_TO_DT_AMT").isNotNull(), col("SUB_TOT_PCA_PD_TO_DT_AMT")).otherwise(lit(0.00)).alias("CLM_PCA_SUB_TOT_PCA_PDTODT_AMT"),
    when(col("CHK_NET_PAYMT_AMT_Pca_ClmChk").isNotNull(), col("CHK_NET_PAYMT_AMT_Pca_ClmChk")).otherwise(lit(0.00)).alias("PCA_CLM_CHK_NET_PAYMT_AMT"),
    when(col("CHK_NO_Pca_AlmChk").isNotNull(), col("CHK_NO_Pca_AlmChk")).otherwise(lit(0)).alias("PCA_CLM_CHK_NO"),
    when(col("CHK_SEQ_NO").isNotNull(), col("CHK_SEQ_NO")).otherwise(lit(0)).alias("PCA_CLM_CHK_SEQ_NO"),
    when(col("PROV_NM_Extrnl_Prov").isNull(), lit(None)).otherwise(col("PROV_NM_Extrnl_Prov")).alias("CLM_EXTRNL_PROV_NM"),
    when(
        (col("PCA_EXTRNL_ID") == lit(" ")) | col("PCA_EXTRNL_ID").isNull(),
        lit(None)
    ).otherwise(col("PCA_EXTRNL_ID")).alias("CLM_EXTRNL_REF_PCA_EXTRNL_ID"),
    when(col("CHK_PAYE_NM_Pca_ClmChk").isNull(), lit(None)).otherwise(col("CHK_PAYE_NM_Pca_ClmChk")).alias("PCA_CLM_CHK_PAYE_NM"),
    when(col("CHK_PAYMT_REF_ID_Pca_ClmChk").isNull(), lit(None)).otherwise(col("CHK_PAYMT_REF_ID_Pca_ClmChk")).alias("PCA_CLM_CHK_PAYMT_REF_ID"),
    when(col("REL_PCA_CLM_SK") > lit(1), lit("FACETS")).otherwise(lit(" ")).alias("REL_PCA_SRC_SYS_CD"),
    when(
        col("REL_PCA_CLM_SK") > lit(1),
        when(col("CLM_ID_Ref_Get_Pca_Clm").isNotNull(), col("CLM_ID_Ref_Get_Pca_Clm")).otherwise(lit(None))
    ).otherwise(lit(None)).alias("REL_PCA_CLM_ID"),
    when(
        col("REL_BASE_CLM_SK") > lit(1),
        when(col("CD_MPPNG_SK").isNull(), lit(" ")).otherwise(col("TRGT_CD_Rbc_Src_Sys"))
    ).otherwise(lit(" ")).alias("REL_BASE_SRC_SYS_CD"),
    when(
        col("REL_BASE_CLM_SK") > lit(1),
        when(col("CLM_ID_Ref_Get_Claim").isNotNull(), col("CLM_ID_Ref_Get_Claim")).otherwise(lit(" "))
    ).otherwise(lit(" ")).alias("REL_BASE_CLM_ID"),
    when(col("svPcaTypCd") == lit("NA"), lit(None)).otherwise(col("svPcaTypCd")).alias("PCA_TYP_CD"),
    col("CLM_SUB_BCBS_PLN_CD_SK").cast("integer").alias("CLM_SUB_BCBS_PLN_CD"),
    col("svItsClmSccfNo").alias("ITS_CLM_SCCF_NO"),
    rpad(
        when(col("ALT_CHRG_IN").isNull(), lit(" ")).otherwise(col("ALT_CHRG_IN")),
        1, " "
    ).alias("CLM_REMIT_HIST_ALT_CHRG_IN"),
    when(col("ALT_CHRG_IN").isNull(), lit(0.00)).otherwise(col("ALT_CHRG_PROV_WRTOFF_AMT")).alias("ALT_CHRG_PROV_WRTOFF_AMT")
)

write_files(
    df_clmdmclm_abc_out,
    f"{adls_path}/load/CLM_DM_CLM.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)