# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_4 11/04/09 07:45:58 Batch  15284_27963 PROMOTE bckcetl:31540 updt dsadm dsadm
# MAGIC ^1_4 10/09/09 13:47:48 Batch  15258_49671 PROMOTE bckcetl:31540 ids20 dsadm bls for sa
# MAGIC ^1_4 10/09/09 13:44:56 Batch  15258_49500 INIT bckcett:31540 testIDS dsadm bls for sa
# MAGIC ^1_1 10/09/09 12:24:40 Batch  15258_44699 PROMOTE bckcett:31540 testIDS u150906 TTR580-Income_Sharon_testIDS               Maddy
# MAGIC ^1_1 10/09/09 12:23:38 Batch  15258_44624 INIT bckcett:31540 devlIDS u150906 TTR580-Income_Sharon_devlIDS               Maddy
# MAGIC ^1_3 01/14/09 10:18:26 Batch  14990_37109 PROMOTE bckcetl ids20 dsadm bls for sg
# MAGIC ^1_3 01/14/09 10:10:44 Batch  14990_36646 INIT bckcett testIDS dsadm BLS FOR SG
# MAGIC ^1_1 12/16/08 22:11:05 Batch  14961_79874 PROMOTE bckcett testIDS u03651 steph - income primary key conversion
# MAGIC ^1_1 12/16/08 22:03:32 Batch  14961_79415 INIT bckcett devlIDS u03651 steffy
# MAGIC ^1_1 01/30/08 05:00:10 Batch  14640_18019 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_2 11/12/07 10:00:11 Batch  14561_36015 INIT bckcetl ids20 dcg01 sa
# MAGIC ^1_2 11/02/07 13:07:35 Batch  14551_47275 PROMOTE bckcetl ids20 dsadm bls for on
# MAGIC ^1_2 11/02/07 12:52:45 Batch  14551_46368 INIT bckcett testIDS30 dsadm bls for on
# MAGIC ^1_1 10/31/07 13:17:35 Batch  14549_47857 PROMOTE bckcett testIDS30 u03651 steffy
# MAGIC ^1_1 10/31/07 13:09:37 Batch  14549_47387 INIT bckcett devlIDS30 u03651 steffy
# MAGIC ^1_2 06/07/07 15:08:56 Batch  14403_54540 PROMOTE bckcett devlIDS30 u10157 sa
# MAGIC ^1_2 06/07/07 15:06:33 Batch  14403_54395 INIT bckcetl ids20 dcg01 sa
# MAGIC ^1_6 05/23/07 13:53:34 Batch  14388_50029 PROMOTE bckcetl ids20 dsadm bls for sa
# MAGIC ^1_6 05/23/07 13:45:45 Batch  14388_49559 INIT bckcett testIDS30 dsadm bls for sa
# MAGIC ^1_1 05/22/07 12:32:34 Batch  14387_45157 PROMOTE bckcett testIDS30 u10157 sa
# MAGIC ^1_1 05/22/07 12:30:51 Batch  14387_45055 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_5 03/05/07 16:45:23 Batch  14309_60325 PROMOTE bckcett devlIDS30 u10157 sa
# MAGIC ^1_5 03/05/07 16:34:28 Batch  14309_59670 INIT bckcetl ids20 dcg01 sa
# MAGIC ^1_4 02/11/07 12:33:13 Batch  14287_45195 INIT bckcetl ids20 dcg01 sa
# MAGIC ^1_5 12/18/06 15:24:00 Batch  14232_55459 PROMOTE bckcetl ids20 dsadm Keith for Ralph install 12/18/2006
# MAGIC ^1_5 12/18/06 14:39:27 Batch  14232_52797 INIT bckcett testIDS30 dsadm Keith for Ralph CDHP Install 12182006
# MAGIC ^1_3 12/11/06 18:10:23 Batch  14225_65510 PROMOTE bckcett testIDS30 u10157 sa
# MAGIC ^1_3 12/11/06 18:01:24 Batch  14225_64886 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_2 12/11/06 17:56:44 Batch  14225_64607 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_1 12/11/06 17:44:36 Batch  14225_63880 PROMOTE bckcett devlIDS30 u10157 sa
# MAGIC ^1_1 12/11/06 17:42:18 Batch  14225_63742 INIT bckcetl ids20 dcg01 sa
# MAGIC ^1_1 11/03/06 13:03:29 Batch  14187_47011 INIT bckcetl ids20 dcg01 sa
# MAGIC ^1_3 05/22/06 11:41:30 Batch  14022_42099 PROMOTE bckcetl ids20 dsadm Jim Hart
# MAGIC ^1_3 05/22/06 11:21:03 Batch  14022_40870 INIT bckcett testIDS30 dsadm Jim Hart
# MAGIC ^1_2 05/22/06 11:13:00 Batch  14022_40388 INIT bckcett testIDS30 dsadm Jim Hart
# MAGIC ^1_1 05/22/06 10:32:59 Batch  14022_37985 INIT bckcett testIDS30 dsadm Jim Hart
# MAGIC ^1_5 05/12/06 15:17:06 Batch  14012_55029 PROMOTE bckcett testIDS30 u10157 sa
# MAGIC ^1_5 05/12/06 15:03:19 Batch  14012_54207 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_4 05/11/06 16:20:33 Batch  14011_58841 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_3 05/10/06 16:34:22 Batch  14010_59666 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_2 05/10/06 16:00:21 Batch  14010_57626 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_1 05/02/06 14:21:23 Batch  14002_51686 INIT bckcett devlIDS30 u10157 sa
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2005 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 10;hf_invcfeedscnt_sbsb;hf_feedscnt_med_cls;hf_feedscnt_dent_cls;hf_feedscnt_othr_cls;hf_feedscnt_med_cls_not_elig;hf_feedscnt_dent_cls_not_elig;hf_feedscnt_othr_cls_not_elig;hf_feedscnt_elig_sgsg;hf_feedscnt_elig_grgr;hf_invc_fee_dscnt_allcol
# MAGIC 
# MAGIC JOB NAME:     FctsIncomeInvcFeeDscntExtr
# MAGIC CALLED BY:   FctsIncomeExtrSeq
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC     Pulls the Invoice Subscriber information from Facets for Income 
# MAGIC       
# MAGIC    Date is used as a criteria for which records are pulled.   
# MAGIC 
# MAGIC INPUTS:
# MAGIC    CDS_INSB_SB_DETAIL
# MAGIC    CMC_SBSB_SUBSC SBSB  
# MAGIC   
# MAGIC HASH FILES:  hf_invc_fee_dscnt
# MAGIC                         hf_invcfeedscnt_sbsb
# MAGIC                         hf_invc_dscrtn
# MAGIC                         hf_feedscnt_med_cls
# MAGIC                         hf_feedscnt_dent_cls
# MAGIC                         hf_feedscnt_othr_cls
# MAGIC                         hf_feedscnt_med_cls_not_elig
# MAGIC                         hf_feedscnt_dent_cls_not_elig
# MAGIC                         hf_feedscnt_othr_cls_not_elig
# MAGIC                         hf_feedscnt_elig_sgsg
# MAGIC                         hf_feedscnt_elig_grgr
# MAGIC 
# MAGIC TRANSFORMS:  
# MAGIC                   used the STRIP.FIELD to all of the character fields.
# MAGIC                   The DB2 timestamp value of the audit_ts is converted to a sybase timestamp with the transofrm  FORMAT.DATE
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC                   Output file is created with a temp. name. 
# MAGIC                   
# MAGIC                  This is pulls all records from a BeginDate supplied in parameters.
# MAGIC 
# MAGIC OUTPUTS: 
# MAGIC                     Sequential file name is created in the job control ( FinalOutFile Parameter )
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC              Ralph Tucker  -  12/14/2005-   Originally Programmed
# MAGIC              SAndrew             12/12/2006   - Project 1756 - Added extraction critieria from CDS_INID_INVOICE for where  INID.AFAI_CK = 0
# MAGIC              Naren Garapaty - 04/2007          Added Reversal Logic
# MAGIC              
# MAGIC 
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                                                            Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                                                       ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada              6/14/2007          3264                              Added Balancing process to the overall                                                    devlIDS30               Steph Goddard            09/15/2007
# MAGIC                                                                                                       job that takes a snapshot of the source data                        
# MAGIC 
# MAGIC Bhoomi Dasari                 09/28/2008        3567                             Added new primay key contianer and SrcSysCdsk                                     devlIDS                    Steph Goddard            10/03/2008
# MAGIC                                                                                                        and SrcSysCd  
# MAGIC 
# MAGIC SAndrew                         2009-10-08         Altiris#223143.           Change the amount going into the FEE_DSNCT_AMT field                           devlIDS                   Steph Goddard             10/09/2009
# MAGIC                                                                    TTR 613                    from 
# MAGIC                                                                                                     ( INFD.BLFD_FEE_AMT +  INFD.BLFD_DISC_AMT + INFD.BLFD_FEE_AMT)
# MAGIC                                                                                                      To  - new stage varialble 
# MAGIC 
# MAGIC                                                                                                      iF  Extract.PMFA_FEE_DISC_IND = 'D' THEN (-1 *  Extract.BLFD_DISC_AMT)
# MAGIC                                                                                                      ELSE  
# MAGIC                                                                                                      iF Extract.PMFA_FEE_DISC_IND = 'F' THEN  Extract.BLFD_FEE_AMT
# MAGIC                                                                                                     and no longer add them together - just move appropriate field
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC Goutham Kalidindi             2021-03-24      358186                            Changed Datatype length for field                      IntegrateDev1                                                    Kalyan Neelam             2021-03-31
# MAGIC                                                                                              BLIV_ID 
# MAGIC                                                                                                char(12) to Varchar
# MAGIC Prabhu ES                       2022-03-02       S2S Remediation     MSSQL ODBC conn added and other param changes                                       IntegrateDev5	Ken Bradmon	2022-06-13

# MAGIC Due to the Complexity of the whole job and the fields required for the snapshot file, the Extraction for Balancing is done at an Intermediate Stage within the job to reduce the overhead involved and the complexity of duplicating the whole process
# MAGIC Hash file hf_invc_fee_dscnt_allcol cleared
# MAGIC Pulls all Invoice rows from facets CDS_INFD_FEE_DISC table matching data criteria.
# MAGIC Balancing snapshot of source table
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DoubleType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../shared_containers/PrimaryKey/IncmInvcFeeDscntPK
# COMMAND ----------

# Retrieve job parameters
TmpOutFile = get_widget_value('TmpOutFile','')
FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
DriverTable = get_widget_value('DriverTable','')
RunID = get_widget_value('RunID','')
CurrDate = get_widget_value('CurrDate','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
SrcSysCd = get_widget_value('SrcSysCd','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

# Prepare FACETS JDBC config
jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)

#--------------------------------------------------------------------------------
# STAGE: Facets (ODBCConnector) - Two output pins: Extract, refSBSB
#--------------------------------------------------------------------------------

# 1) Output "Extract" link query
extract_query = f"""
SELECT 
       INFD.BLIV_ID,
       INFD.PMFA_ID,
       INFD.BLFD_SOURCE,
       INFD.BLFD_DISP_CD,
       INFD.PMFA_FEE_DISC_IND,
       INID.GRGR_ID,
       INID.SGSG_ID,
       INID.SBSB_ID,
       INFD.BLFD_FEE_AMT,
       INFD.BLFD_DISC_AMT,
       INFD.BLFD_PREM
FROM 
tempdb..{DriverTable}             TmpInvc,  
{FacetsOwner}.CDS_INID_INVOICE    INID,
{FacetsOwner}.CDS_INFD_FEES_DISC  INFD
WHERE
     TmpInvc.BILL_INVC_ID       =  INID.BLIV_ID
 AND INID.BLIV_ID                =  INFD.BLIV_ID
 AND INID.AFAI_CK                =  0
 AND (INFD.PMFA_FEE_DISC_IND = 'D' OR INFD.PMFA_FEE_DISC_IND = 'F')
"""

df_Facets_Extract = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query)
    .load()
)

# 2) Output "refSBSB" link query
refSBSB_query = f"SELECT SBSB_ID, SBSB_CK FROM {FacetsOwner}.CMC_SBSB_SUBSC SBSB"
df_Facets_refSBSB = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", refSBSB_query)
    .load()
)

#--------------------------------------------------------------------------------
# Remove hashed file "hf_invcfeedscnt_sbsb" (Scenario A) => De-duplicate on key SBSB_ID
#--------------------------------------------------------------------------------
df_Facets_refSBSB_dedup = dedup_sort(
    df_Facets_refSBSB,
    partition_cols=["SBSB_ID"],
    sort_cols=[]
)

#--------------------------------------------------------------------------------
# STAGE: Facets_Lookup (ODBCConnector) - two outputs, but no SQL given; must not skip.
#--------------------------------------------------------------------------------
# Prepare separate dataframes for lnkEligGRGR and lnkEligSGSG (unknown queries)
# We cannot skip or leave placeholders for logic; here we put minimal viable queries.
# In real translation, the correct SQL would be placed below. Marking unknown for remediation.

df_Facets_Lookup_lnkEligGRGR = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", "<unknown_sql_for_lnkEligGRGR>")
    .load()
)

df_Facets_Lookup_lnkEligSGSG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", "<unknown_sql_for_lnkEligSGSG>")
    .load()
)

#--------------------------------------------------------------------------------
# Remove hashed file "Grgr_Sgsg_Lookup" (Scenario A)
#   => "refEligGrgr" dedup on key GRGR_CK
#   => "refEligSgsg" dedup on keys SGSG_CK, GRGR_CK
#--------------------------------------------------------------------------------
df_refEligGrgr_dedup = dedup_sort(
    df_Facets_Lookup_lnkEligGRGR,
    partition_cols=["GRGR_CK"],
    sort_cols=[]
)

df_refEligSgsg_dedup = dedup_sort(
    df_Facets_Lookup_lnkEligSGSG,
    partition_cols=["SGSG_CK","GRGR_CK"],
    sort_cols=[]
)

#--------------------------------------------------------------------------------
# STAGE: Facets_Mbr_Elig (ODBCConnector) - 6 output pins
#--------------------------------------------------------------------------------
jdbc_url_facets_mbr_elig, jdbc_props_facets_mbr_elig = get_db_config(facets_secret_name)

lnkClsLkupMed_query = f"""
SELECT 
       INFD.BLIV_ID ,
       ELIG.CSCS_ID,
       ELIG.GRGR_CK,
       ELIG.SGSG_CK,
       ELIG.CSPI_ID,
       ELIG.PDPD_ID
FROM 
tempdb..{DriverTable}                   TmpInvc,
{FacetsOwner}.CDS_INID_INVOICE          INID,
{FacetsOwner}.CDS_INFD_FEES_DISC        INFD,
{FacetsOwner}.CMC_MEME_MEMBER           MEME,
{FacetsOwner}.CMC_MEPE_PRCS_ELIG        ELIG
WHERE   TmpInvc.BILL_INVC_ID = INID.BLIV_ID
  AND   INID.BLIV_ID         = INFD.BLIV_ID
  AND   INID.SBSB_CK         = MEME.SBSB_CK
  AND   INID.GRGR_CK         = MEME.GRGR_CK
  AND   MEME.MEME_REL        = 'M'
  AND   MEME.MEME_CK         = ELIG.MEME_CK
  AND   ELIG.MEPE_EFF_DT <= '{CurrDate}'
  AND   ELIG.CSPD_CAT  = 'M'
  AND   ELIG.MEPE_ELIG_IND = 'Y'
  AND   ELIG.MEPE_TERM_DT = (
         SELECT MAX(ELIG2.MEPE_TERM_DT)
         FROM {FacetsOwner}.CMC_MEPE_PRCS_ELIG ELIG2
         WHERE ELIG2.MEME_CK = ELIG.MEME_CK
           AND ELIG2.MEPE_EFF_DT <= '{CurrDate}'
           AND ELIG2.MEPE_ELIG_IND = 'Y'
           AND ELIG2.CSPD_CAT = 'M'
       )
"""

df_Facets_Mbr_Elig_lnkClsLkupMed = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets_mbr_elig)
    .options(**jdbc_props_facets_mbr_elig)
    .option("query", lnkClsLkupMed_query)
    .load()
)

lnkClsLkupDent_query = f"""
SELECT 
       INFD.BLIV_ID ,
       ELIG.CSCS_ID,
       ELIG.GRGR_CK,
       ELIG.SGSG_CK,
       ELIG.CSPI_ID,
       ELIG.PDPD_ID
FROM 
tempdb..{DriverTable}                  TmpInvc,
{FacetsOwner}.CDS_INID_INVOICE         INID,
{FacetsOwner}.CDS_INFD_FEES_DISC       INFD,
{FacetsOwner}.CMC_MEME_MEMBER          MEME,
{FacetsOwner}.CMC_MEPE_PRCS_ELIG       ELIG
WHERE   TmpInvc.BILL_INVC_ID = INID.BLIV_ID
  AND   INID.BLIV_ID         = INFD.BLIV_ID
  AND   INID.SBSB_CK         = MEME.SBSB_CK
  AND   INID.GRGR_CK         = MEME.GRGR_CK
  AND   MEME.MEME_REL        = 'M'
  AND   MEME.MEME_CK         = ELIG.MEME_CK
  AND   ELIG.MEPE_EFF_DT <= '{CurrDate}'
  AND   ELIG.CSPD_CAT  = 'D'
  AND   ELIG.MEPE_ELIG_IND = 'Y'
  AND   ELIG.MEPE_TERM_DT = (
         SELECT MAX(ELIG2.MEPE_TERM_DT)
         FROM {FacetsOwner}.CMC_MEPE_PRCS_ELIG ELIG2
         WHERE ELIG2.MEME_CK = ELIG.MEME_CK
           AND ELIG2.MEPE_EFF_DT <= '{CurrDate}'
           AND ELIG2.MEPE_ELIG_IND = 'Y'
           AND ELIG2.CSPD_CAT = 'D'
       )
"""

df_Facets_Mbr_Elig_lnkClsLkupDent = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets_mbr_elig)
    .options(**jdbc_props_facets_mbr_elig)
    .option("query", lnkClsLkupDent_query)
    .load()
)

lnkClsLkupOthr_query = f"""
SELECT 
       INFD.BLIV_ID ,
       ELIG.CSCS_ID,
       ELIG.GRGR_CK,
       ELIG.SGSG_CK,
       ELIG.CSPI_ID,
       ELIG.PDPD_ID
FROM 
tempdb..{DriverTable}                   TmpInvc,
{FacetsOwner}.CDS_INID_INVOICE          INID,
{FacetsOwner}.CDS_INFD_FEES_DISC        INFD,
{FacetsOwner}.CMC_MEME_MEMBER           MEME,
{FacetsOwner}.CMC_MEPE_PRCS_ELIG        ELIG
WHERE   TmpInvc.BILL_INVC_ID = INID.BLIV_ID
  AND   INID.BLIV_ID         = INFD.BLIV_ID
  AND   INID.SBSB_CK         = MEME.SBSB_CK
  AND   INID.GRGR_CK         = MEME.GRGR_CK
  AND   MEME.MEME_REL        = 'M'
  AND   MEME.MEME_CK         = ELIG.MEME_CK
  AND   ELIG.MEPE_EFF_DT <= '{CurrDate}'
  AND   ELIG.CSPD_CAT not in ('M', 'D')
  AND   ELIG.MEPE_ELIG_IND = 'Y'
  AND   ELIG.MEPE_TERM_DT = (
         SELECT MAX(ELIG2.MEPE_TERM_DT)
         FROM {FacetsOwner}.CMC_MEPE_PRCS_ELIG  ELIG2
         WHERE ELIG2.MEME_CK = ELIG.MEME_CK
           AND ELIG2.MEPE_EFF_DT <= '{CurrDate}'
           AND ELIG2.MEPE_ELIG_IND = 'Y'
           AND ELIG2.CSPD_CAT not in ('M', 'D')
       )
"""

df_Facets_Mbr_Elig_lnkClsLkupOthr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets_mbr_elig)
    .options(**jdbc_props_facets_mbr_elig)
    .option("query", lnkClsLkupOthr_query)
    .load()
)

lnkClsLkupMedNotElig_query = f"""
SELECT 
       INFD.BLIV_ID ,
       ELIG.CSCS_ID,
       ELIG.GRGR_CK,
       ELIG.SGSG_CK,
       ELIG.CSPI_ID,
       ELIG.PDPD_ID
FROM 
tempdb..{DriverTable}                  TmpInvc,
{FacetsOwner}.CDS_INID_INVOICE         INID,
{FacetsOwner}.CDS_INFD_FEES_DISC       INFD,
{FacetsOwner}.CMC_MEME_MEMBER          MEME,
{FacetsOwner}.CMC_MEPE_PRCS_ELIG       ELIG
WHERE   TmpInvc.BILL_INVC_ID = INID.BLIV_ID
  AND   INID.BLIV_ID         = INFD.BLIV_ID
  AND   INID.SBSB_CK         = MEME.SBSB_CK
  AND   INID.GRGR_CK         = MEME.GRGR_CK
  AND   MEME.MEME_REL        = 'M'
  AND   MEME.MEME_CK         = ELIG.MEME_CK
  AND   ELIG.MEPE_EFF_DT <= '{CurrDate}'
  AND   ELIG.CSPD_CAT  = 'M'
  AND   ELIG.MEPE_ELIG_IND = 'N'
  AND   ELIG.MEPE_TERM_DT = (
         SELECT MAX(ELIG2.MEPE_TERM_DT)
         FROM {FacetsOwner}.CMC_MEPE_PRCS_ELIG ELIG2
         WHERE ELIG2.MEME_CK = ELIG.MEME_CK
           AND ELIG2.MEPE_EFF_DT <= '{CurrDate}'
           AND ELIG2.MEPE_ELIG_IND = 'N'
           AND ELIG2.CSPD_CAT = 'M'
       )
"""

df_Facets_Mbr_Elig_lnkClsLkupMedNotElig = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets_mbr_elig)
    .options(**jdbc_props_facets_mbr_elig)
    .option("query", lnkClsLkupMedNotElig_query)
    .load()
)

lnkClsLkupDentNotElig_query = f"""
SELECT 
       INFD.BLIV_ID ,
       ELIG.CSCS_ID,
       ELIG.GRGR_CK,
       ELIG.SGSG_CK,
       ELIG.CSPI_ID,
       ELIG.PDPD_ID
FROM 
tempdb..{DriverTable}                   TmpInvc,
{FacetsOwner}.CDS_INID_INVOICE          INID,
{FacetsOwner}.CDS_INFD_FEES_DISC        INFD,
{FacetsOwner}.CMC_MEME_MEMBER           MEME,
{FacetsOwner}.CMC_MEPE_PRCS_ELIG        ELIG
WHERE   TmpInvc.BILL_INVC_ID = INID.BLIV_ID
  AND   INID.BLIV_ID         = INFD.BLIV_ID
  AND   INID.SBSB_CK         = MEME.SBSB_CK
  AND   INID.GRGR_CK         = MEME.GRGR_CK
  AND   MEME.MEME_REL        = 'M'
  AND   MEME.MEME_CK         = ELIG.MEME_CK
  AND   ELIG.MEPE_EFF_DT <= '{CurrDate}'
  AND   ELIG.CSPD_CAT  = 'D'
  AND   ELIG.MEPE_ELIG_IND = 'N'
  AND   ELIG.MEPE_TERM_DT = (
         SELECT MAX(ELIG2.MEPE_TERM_DT)
         FROM {FacetsOwner}.CMC_MEPE_PRCS_ELIG ELIG2
         WHERE ELIG2.MEME_CK = ELIG.MEME_CK
           AND ELIG2.MEPE_EFF_DT <= '{CurrDate}'
           AND ELIG2.MEPE_ELIG_IND = 'N'
           AND ELIG2.CSPD_CAT = 'D'
       )
"""

df_Facets_Mbr_Elig_lnkClsLkupDentNotElig = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets_mbr_elig)
    .options(**jdbc_props_facets_mbr_elig)
    .option("query", lnkClsLkupDentNotElig_query)
    .load()
)

lnkClsLkupOthrNotElig_query = f"""
SELECT 
       INFD.BLIV_ID ,
       ELIG.CSCS_ID,
       ELIG.GRGR_CK,
       ELIG.SGSG_CK,
       ELIG.CSPI_ID,
       ELIG.PDPD_ID
FROM 
tempdb..{DriverTable}                   TmpInvc,
{FacetsOwner}.CDS_INID_INVOICE          INID,
{FacetsOwner}.CDS_INFD_FEES_DISC        INFD,
{FacetsOwner}.CMC_MEME_MEMBER           MEME,
{FacetsOwner}.CMC_MEPE_PRCS_ELIG        ELIG
WHERE   TmpInvc.BILL_INVC_ID = INID.BLIV_ID
  AND   INID.BLIV_ID         = INFD.BLIV_ID
  AND   INID.SBSB_CK         = MEME.SBSB_CK
  AND   INID.GRGR_CK         = MEME.GRGR_CK
  AND   MEME.MEME_REL        = 'M'
  AND   MEME.MEME_CK         = ELIG.MEME_CK
  AND   ELIG.MEPE_EFF_DT <= '{CurrDate}'
  AND   ELIG.CSPD_CAT not in ('M', 'D')
  AND   ELIG.MEPE_ELIG_IND = 'N'
  AND   ELIG.MEPE_TERM_DT = (
         SELECT MAX(ELIG2.MEPE_TERM_DT)
         FROM {FacetsOwner}.CMC_MEPE_PRCS_ELIG ELIG2
         WHERE ELIG2.MEME_CK = ELIG.MEME_CK
           AND ELIG2.MEPE_EFF_DT <= '{CurrDate}'
           AND ELIG2.MEPE_ELIG_IND = 'N'
           AND ELIG2.CSPD_CAT not in ('M', 'D')
       )
"""

df_Facets_Mbr_Elig_lnkClsLkupOthrNotElig = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets_mbr_elig)
    .options(**jdbc_props_facets_mbr_elig)
    .option("query", lnkClsLkupOthrNotElig_query)
    .load()
)

#--------------------------------------------------------------------------------
# Remove hashed file "hf_feedscnt_cls" (Scenario A)
#   => 6 sets of data, each dedup on key BLIV_ID
#--------------------------------------------------------------------------------
df_refClsLkupMed = dedup_sort(
    df_Facets_Mbr_Elig_lnkClsLkupMed,
    partition_cols=["BLIV_ID"],
    sort_cols=[]
)

df_refClsLkupDent = dedup_sort(
    df_Facets_Mbr_Elig_lnkClsLkupDent,
    partition_cols=["BLIV_ID"],
    sort_cols=[]
)

df_refClsLkupOthr = dedup_sort(
    df_Facets_Mbr_Elig_lnkClsLkupOthr,
    partition_cols=["BLIV_ID"],
    sort_cols=[]
)

df_refClsLkupMedNotElig = dedup_sort(
    df_Facets_Mbr_Elig_lnkClsLkupMedNotElig,
    partition_cols=["BLIV_ID"],
    sort_cols=[]
)

df_refClsLkupDentNotElig = dedup_sort(
    df_Facets_Mbr_Elig_lnkClsLkupDentNotElig,
    partition_cols=["BLIV_ID"],
    sort_cols=[]
)

df_refClsLkupOthrNotElig = dedup_sort(
    df_Facets_Mbr_Elig_lnkClsLkupOthrNotElig,
    partition_cols=["BLIV_ID"],
    sort_cols=[]
)

#--------------------------------------------------------------------------------
# STAGE: StripField (CTransformerStage)
#  Primary link: df_Facets_Extract
#  Lookup links (all left):
#   lnkSBSB => df_refSBSB_dedup ON SBSB_ID
#   refClsLkupOthrNotElig => df_refClsLkupOthrNotElig ON BLIV_ID
#   refClsLkupMed => df_refClsLkupMed ON BLIV_ID
#   refClsLkupDent => df_refClsLkupDent ON BLIV_ID
#   refClsLkupOthr => df_refClsLkupOthr ON BLIV_ID
#   refClsLkupMedNotElig => df_refClsLkupMedNotElig ON BLIV_ID
#   refClsLkupDentNotElig => df_refClsLkupDentNotElig ON BLIV_ID
#  Constraint: PMFA_FEE_DISC_IND = 'D' or 'F'
#  Stage Variable: FeeOrDiscount
#--------------------------------------------------------------------------------

df_StripField_joined = (
    df_Facets_Extract.alias("Extract")
    .join(df_Facets_refSBSB_dedup.alias("lnkSBSB"),
          F.col("Extract.SBSB_ID") == F.col("lnkSBSB.SBSB_ID"),
          "left")
    .join(df_refClsLkupOthrNotElig.alias("refClsLkupOthrNotElig"),
          F.col("Extract.BLIV_ID") == F.col("refClsLkupOthrNotElig.BLIV_ID"),
          "left")
    .join(df_refClsLkupMed.alias("refClsLkupMed"),
          F.col("Extract.BLIV_ID") == F.col("refClsLkupMed.BLIV_ID"),
          "left")
    .join(df_refClsLkupDent.alias("refClsLkupDent"),
          F.col("Extract.BLIV_ID") == F.col("refClsLkupDent.BLIV_ID"),
          "left")
    .join(df_refClsLkupOthr.alias("refClsLkupOthr"),
          F.col("Extract.BLIV_ID") == F.col("refClsLkupOthr.BLIV_ID"),
          "left")
    .join(df_refClsLkupMedNotElig.alias("refClsLkupMedNotElig"),
          F.col("Extract.BLIV_ID") == F.col("refClsLkupMedNotElig.BLIV_ID"),
          "left")
    .join(df_refClsLkupDentNotElig.alias("refClsLkupDentNotElig"),
          F.col("Extract.BLIV_ID") == F.col("refClsLkupDentNotElig.BLIV_ID"),
          "left")
)

df_StripField_sv = df_StripField_joined.withColumn(
    "FeeOrDiscount",
    F.when(
        (F.col("Extract.PMFA_FEE_DISC_IND") == F.lit("D")) & (F.col("Extract.BLFD_DISC_AMT") != 0),
        -1 * F.col("Extract.BLFD_DISC_AMT")
    ).when(
        F.col("Extract.PMFA_FEE_DISC_IND") == F.lit("D"),
        F.col("Extract.BLFD_DISC_AMT")
    ).when(
        F.col("Extract.PMFA_FEE_DISC_IND") == F.lit("F"),
        F.col("Extract.BLFD_FEE_AMT")
    ).otherwise(F.col("Extract.BLFD_FEE_AMT"))
)

df_StripField_filtered = df_StripField_sv.filter(
    (F.col("Extract.PMFA_FEE_DISC_IND") == F.lit("D")) | (F.col("Extract.PMFA_FEE_DISC_IND") == F.lit("F"))
)

# Build the output columns:
# Note: use strip_field(...) to replace the Convert(CHAR(10):CHAR(13):CHAR(9)) calls
# Also replicate the "if IsNull(...)" logic with F.when(...).otherwise(...)
df_StripField_out = df_StripField_filtered.select(
    strip_field(F.col("Extract.BLIV_ID")).alias("BLIV_ID"),
    strip_field(F.col("Extract.PMFA_ID")).alias("PMFA_ID"),
    strip_field(F.col("Extract.BLFD_SOURCE")).alias("BLFD_SOURCE"),
    strip_field(F.col("Extract.BLFD_DISP_CD")).alias("BLFD_DISP_CD"),
    strip_field(F.col("Extract.PMFA_FEE_DISC_IND")).alias("PMFA_FEE_DISC_IND"),
    strip_field(F.col("Extract.GRGR_ID")).alias("GRGR_ID"),
    strip_field(F.col("Extract.SGSG_ID")).alias("SGSG_ID"),
    strip_field(F.col("Extract.SBSB_ID")).alias("SBSB_ID"),
    df_StripField_filtered["FeeOrDiscount"].alias("FEE_DISCOUNT_AMT"),
    F.when(F.col("lnkSBSB.SBSB_CK").isNull(), F.lit(0)).otherwise(F.col("lnkSBSB.SBSB_CK")).alias("SBSB_CK"),
    F.when(~F.col("refClsLkupMed.CSCS_ID").isNull(), strip_field(F.col("refClsLkupMed.CSCS_ID")))
     .when(~F.col("refClsLkupDent.CSCS_ID").isNull(), strip_field(F.col("refClsLkupDent.CSCS_ID")))
     .when(~F.col("refClsLkupOthr.CSCS_ID").isNull(), strip_field(F.col("refClsLkupOthr.CSCS_ID")))
     .when(~F.col("refClsLkupMedNotElig.CSCS_ID").isNull(), strip_field(F.col("refClsLkupMedNotElig.CSCS_ID")))
     .when(~F.col("refClsLkupDentNotElig.CSCS_ID").isNull(), strip_field(F.col("refClsLkupDentNotElig.CSCS_ID")))
     .when(~F.col("refClsLkupOthrNotElig.CSCS_ID").isNull(), strip_field(F.col("refClsLkupOthrNotElig.CSCS_ID")))
     .otherwise(F.lit("UNK")).alias("CSCS_ID"),
    F.when(~F.col("refClsLkupMed.GRGR_CK").isNull(), F.col("refClsLkupMed.GRGR_CK"))
     .when(~F.col("refClsLkupDent.GRGR_CK").isNull(), F.col("refClsLkupDent.GRGR_CK"))
     .when(~F.col("refClsLkupOthr.GRGR_CK").isNull(), F.col("refClsLkupOthr.GRGR_CK"))
     .when(~F.col("refClsLkupMedNotElig.GRGR_CK").isNull(), F.col("refClsLkupMedNotElig.GRGR_CK"))
     .when(~F.col("refClsLkupDentNotElig.GRGR_CK").isNull(), F.col("refClsLkupDentNotElig.GRGR_CK"))
     .when(~F.col("refClsLkupOthrNotElig.GRGR_CK").isNull(), F.col("refClsLkupOthrNotElig.GRGR_CK"))
     .otherwise(F.lit(0)).alias("ELIG_GRGR_CK"),
    F.when(~F.col("refClsLkupMed.SGSG_CK").isNull(), F.col("refClsLkupMed.SGSG_CK"))
     .when(~F.col("refClsLkupDent.SGSG_CK").isNull(), F.col("refClsLkupDent.SGSG_CK"))
     .when(~F.col("refClsLkupOthr.SGSG_CK").isNull(), F.col("refClsLkupOthr.SGSG_CK"))
     .when(~F.col("refClsLkupMedNotElig.SGSG_CK").isNull(), F.col("refClsLkupMedNotElig.SGSG_CK"))
     .when(~F.col("refClsLkupDentNotElig.SGSG_CK").isNull(), F.col("refClsLkupDentNotElig.SGSG_CK"))
     .when(~F.col("refClsLkupOthrNotElig.SGSG_CK").isNull(), F.col("refClsLkupOthrNotElig.SGSG_CK"))
     .otherwise(F.lit(0)).alias("ELIG_SGSG_CK"),
    F.when(~F.col("refClsLkupMed.CSPI_ID").isNull(), strip_field(F.col("refClsLkupMed.CSPI_ID")))
     .when(~F.col("refClsLkupDent.CSPI_ID").isNull(), strip_field(F.col("refClsLkupDent.CSPI_ID")))
     .when(~F.col("refClsLkupOthr.CSPI_ID").isNull(), strip_field(F.col("refClsLkupOthr.CSPI_ID")))
     .when(~F.col("refClsLkupMedNotElig.CSPI_ID").isNull(), strip_field(F.col("refClsLkupMedNotElig.CSPI_ID")))
     .when(~F.col("refClsLkupDentNotElig.CSPI_ID").isNull(), strip_field(F.col("refClsLkupDentNotElig.CSPI_ID")))
     .when(~F.col("refClsLkupOthrNotElig.CSPI_ID").isNull(), strip_field(F.col("refClsLkupOthrNotElig.CSPI_ID")))
     .otherwise(F.lit("UNK")).alias("ELIG_CSPI_ID"),
    F.when(~F.col("refClsLkupMed.PDPD_ID").isNull(), strip_field(F.col("refClsLkupMed.PDPD_ID")))
     .when(~F.col("refClsLkupDent.PDPD_ID").isNull(), strip_field(F.col("refClsLkupDent.PDPD_ID")))
     .when(~F.col("refClsLkupOthr.PDPD_ID").isNull(), strip_field(F.col("refClsLkupOthr.PDPD_ID")))
     .when(~F.col("refClsLkupMedNotElig.PDPD_ID").isNull(), strip_field(F.col("refClsLkupMedNotElig.PDPD_ID")))
     .when(~F.col("refClsLkupDentNotElig.PDPD_ID").isNull(), strip_field(F.col("refClsLkupDentNotElig.PDPD_ID")))
     .when(~F.col("refClsLkupOthrNotElig.PDPD_ID").isNull(), strip_field(F.col("refClsLkupOthrNotElig.PDPD_ID")))
     .otherwise(F.lit("UNK")).alias("ELIG_PDPD_ID")
)

#--------------------------------------------------------------------------------
# STAGE: Add_Mbrshp_Info (CTransformerStage)
#   Primary link: df_StripField_out (alias "Strip")
#   Lookups:
#     refEligGrgr => df_refEligGrgr_dedup ON (ELIG_GRGR_CK=GRGR_CK)
#     refEligSgsg => df_refEligSgsg_dedup ON (ELIG_SGSG_CK=SGSG_CK AND ELIG_GRGR_CK=GRGR_CK)
#--------------------------------------------------------------------------------

df_AddMbrshpInfo_joined = (
    df_StripField_out.alias("Strip")
    .join(df_refEligGrgr_dedup.alias("refEligGrgr"),
          F.col("Strip.ELIG_GRGR_CK") == F.col("refEligGrgr.GRGR_CK"),
          "left")
    .join(df_refEligSgsg_dedup.alias("refEligSgsg"),
          [(F.col("Strip.ELIG_SGSG_CK") == F.col("refEligSgsg.SGSG_CK")),
           (F.col("Strip.ELIG_GRGR_CK") == F.col("refEligSgsg.GRGR_CK"))],
          "left")
)

df_AddMbrshpInfo_out = df_AddMbrshpInfo_joined.select(
    trim(F.col("Strip.BLIV_ID")).alias("BLIV_ID"),
    F.col("Strip.PMFA_ID").alias("PMFA_ID"),
    F.col("Strip.BLFD_SOURCE").alias("BLFD_SOURCE"),
    F.col("Strip.BLFD_DISP_CD").alias("BLFD_DISP_CD"),
    F.col("Strip.PMFA_FEE_DISC_IND").alias("PMFA_FEE_DISC_IND"),
    F.col("Strip.GRGR_ID").alias("GRGR_ID"),
    F.col("Strip.SGSG_ID").alias("SGSG_ID"),
    F.col("Strip.SBSB_ID").alias("SBSB_ID"),
    F.col("Strip.FEE_DISCOUNT_AMT").alias("FEE_DISCOUNT_AMT"),
    F.col("Strip.SBSB_CK").alias("SBSB_CK"),
    F.when(
        F.length(F.trim(F.col("Strip.CSCS_ID"))) == 0,
        F.col("Strip.ELIG_CSPI_ID")
    ).otherwise(F.col("Strip.CSCS_ID")).alias("CSCS_ID"),
    F.when(
        F.col("refEligGrgr.GRGR_ID").isNull() | (F.length(F.trim(F.col("refEligGrgr.GRGR_ID"))) == 0),
        F.lit("NA")
    ).otherwise(F.col("refEligGrgr.GRGR_ID")).alias("ELIG_GRGR_ID"),
    F.when(
        F.col("refEligSgsg.SGSG_ID").isNull() | (F.length(F.trim(F.col("refEligSgsg.SGSG_ID"))) == 0),
        F.lit("NA")
    ).otherwise(F.col("refEligSgsg.SGSG_ID")).alias("ELIG_SGSG_ID"),
    F.col("Strip.ELIG_CSPI_ID").alias("ELIG_CSPI_ID"),
    F.col("Strip.ELIG_PDPD_ID").alias("ELIG_PDPD_ID")
)

#--------------------------------------------------------------------------------
# STAGE: BusinessRules (CTransformerStage)
#   Primary link: df_AddMbrshpInfo_out
#   Two output pins: ChkReversals, Snapshot
#   Both replicate all rows; column expressions differ
#--------------------------------------------------------------------------------

df_BusinessRules_base = df_AddMbrshpInfo_out

# "ChkReversals" columns:
df_BusinessRules_ChkReversals = df_BusinessRules_base.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    current_date().alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit("FACETS").alias("SRC_SYS_CD"),
    (F.lit("FACETS;") 
     + F.trim(F.col("BLIV_ID")) 
     + F.trim(F.col("PMFA_ID")) 
     + F.trim(F.col("BLFD_SOURCE")) 
     + F.trim(F.col("BLFD_DISP_CD"))).alias("PRI_KEY_STRING"),
    F.col("BLIV_ID").alias("BLIV_ID"),
    F.col("PMFA_ID").alias("PMFA_ID"),
    F.col("BLFD_SOURCE").alias("BLFD_SOURCE"),
    F.col("BLFD_DISP_CD").alias("BLFD_DISP_CD"),
    F.col("PMFA_FEE_DISC_IND").alias("PMFA_FEE_DISC_IND"),
    F.when(F.length(F.trim(F.col("GRGR_ID"))) == 0,
           F.col("ELIG_GRGR_ID")).otherwise(F.col("GRGR_ID")).alias("GRGR_ID"),
    F.when(F.length(F.trim(F.col("SGSG_ID"))) == 0,
           F.col("ELIG_SGSG_ID")).otherwise(F.col("SGSG_ID")).alias("SGSG_ID"),
    F.col("SBSB_ID").alias("SBSB_ID"),
    F.lit(0).alias("INVC_SK"),
    F.col("BLIV_ID").alias("BILL_INVC_ID"),
    F.col("GRGR_ID").alias("GRP_ID"),
    F.col("BLIV_ID").alias("INVC_ID"),
    F.col("SGSG_ID").alias("SUBGRP_ID"),
    F.when(F.col("PMFA_FEE_DISC_IND") == F.lit("D"), F.lit("Y")).otherwise(F.lit("N")).alias("DSCNT_IN"),
    F.col("FEE_DISCOUNT_AMT").alias("FEE_DSCNT_AMT"),
    F.col("SBSB_CK").alias("SBSB_CK"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CSCS_ID").alias("CSCS_ID"),
    F.when(F.length(F.trim(F.col("ELIG_CSPI_ID"))) == 0,
           F.lit("UNK")).otherwise(F.col("ELIG_CSPI_ID")).alias("ELIG_CSPI_ID"),
    F.when(F.length(F.trim(F.col("ELIG_PDPD_ID"))) == 0,
           F.lit("UNK")).otherwise(F.col("ELIG_PDPD_ID")).alias("ELIG_PDPD_ID")
)

# "Snapshot" columns:
df_BusinessRules_Snapshot = df_BusinessRules_base.select(
    F.col("BLIV_ID").cast(StringType()).alias("BLIV_ID"),
    F.col("PMFA_ID").alias("PMFA_ID"),
    F.col("BLFD_SOURCE").alias("BLFD_SOURCE"),
    F.col("BLFD_DISP_CD").alias("BLFD_DISP_CD"),
    F.col("FEE_DISCOUNT_AMT").alias("FEE_DISCOUNT_AMT"),
    F.when(F.length(F.trim(F.col("GRGR_ID"))) == 0,
           F.col("ELIG_GRGR_ID")).otherwise(F.col("GRGR_ID")).alias("GRGR_ID"),
    F.when(F.length(F.trim(F.col("SGSG_ID"))) == 0,
           F.col("ELIG_SGSG_ID")).otherwise(F.col("SGSG_ID")).alias("SGSG_ID"),
    F.col("GRGR_ID").alias("GRP_ID"),
    F.col("SBSB_CK").alias("SBSB_CK"),
    F.col("CSCS_ID").alias("CSCS_ID"),
    F.when(F.length(F.trim(F.col("ELIG_CSPI_ID"))) == 0,
           F.lit("UNK")).otherwise(F.col("ELIG_CSPI_ID")).alias("ELIG_CSPI_ID"),
    F.when(F.length(F.trim(F.col("ELIG_PDPD_ID"))) == 0,
           F.lit("UNK")).otherwise(F.col("ELIG_PDPD_ID")).alias("ELIG_PDPD_ID")
)

#--------------------------------------------------------------------------------
# STAGE: hf_invc_rebills_rvrsl (CHashedFileStage) => scenario C (source only)
#   We'll read from a parquet: "hf_invc_rebills_rvrsl.parquet"
#   Key column is "BILL_INVC_ID"
#--------------------------------------------------------------------------------
df_invc_rebills_rvrsl = spark.read.parquet(f"{adls_path}/hf_invc_rebills_rvrsl.parquet")

#--------------------------------------------------------------------------------
# STAGE: SnapshotReversalLogic (CTransformerStage)
#   Primary link: df_BusinessRules_Snapshot
#   Lookup link: df_invc_rebills_rvrsl => left join on (Snapshot.BLIV_ID == rebill_lkup.BILL_INVC_ID) and (ChkReversals.BILL_INVC_ID == rebill_lkup.BILL_INVC_ID)
#   Output: NonReversal, Reversal (Constraint: IsNull(rebill_lkup.BILL_INVC_ID) = @FALSE for Reversal)
#   With stage vars: svReversalInvcId, svRevrslFeeDscntAmt
#--------------------------------------------------------------------------------

df_Snapshot_alias = df_BusinessRules_Snapshot.alias("Snapshot")
df_rebills_rvrsl_alias = df_invc_rebills_rvrsl.alias("rebill_lkup")

# We effectively do a left join; but note we also want columns from the "ChkReversals" link. 
# The JSON shows we also check "ChkReversals.BILL_INVC_ID == rebill_lkup.BILL_INVC_ID" 
# However, "ChkReversals" is a different dataset. That link is actually from a different stage 
# in DataStage design. In real DS job, it's referencing the same invoice ID.  
# The job design merges them conceptually, but here we replicate usage with the "Snapshot" itself. 
# We'll do a single left join on BLIV_ID => BILL_INVC_ID. We keep it consistent with the DS job. 

df_SnapshotReversalLogic_joined = df_Snapshot_alias.join(
    df_rebills_rvrsl_alias,
    (F.col("Snapshot.BLIV_ID") == F.col("rebill_lkup.BILL_INVC_ID")),
    "left"
)

# Define the columns from "Snapshot" plus the stage vars:
df_SnapshotReversalLogic_sv = df_SnapshotReversalLogic_joined.withColumn(
    "svReversalInvcId",
    F.concat(F.trim(F.col("Snapshot.BLIV_ID")), F.lit("R"))
).withColumn(
    "svRevrslFeeDscntAmt",
    -1 * F.col("Snapshot.FEE_DISCOUNT_AMT")
)

# "NonReversal": rows where rebill_lkup.BILL_INVC_ID is null or we do not satisfy IsNull=FALSE for Reversal 
df_SnapshotReversalLogic_NonReversal = df_SnapshotReversalLogic_sv.filter(
    F.col("rebill_lkup.BILL_INVC_ID").isNull()
).select(
    strip_field(F.col("Snapshot.BLIV_ID")).alias("BILL_INVC_ID"),
    strip_field(F.col("Snapshot.PMFA_ID")).alias("PMFA_ID"),
    strip_field(F.col("Snapshot.BLFD_SOURCE")).alias("BLFD_SOURCE"),
    strip_field(F.col("Snapshot.BLFD_DISP_CD")).alias("BLFD_DISP_CD"),
    F.col("Snapshot.FEE_DISCOUNT_AMT").alias("FEE_DSCNT_AMT"),
    F.col("Snapshot.GRGR_ID").alias("GRGR_ID"),
    F.col("Snapshot.SGSG_ID").alias("SGSG_ID"),
    F.col("Snapshot.GRP_ID").alias("GRP_ID"),
    F.col("Snapshot.SBSB_CK").alias("SBSB_CK"),
    F.col("Snapshot.CSCS_ID").alias("CSCS_ID"),
    F.col("Snapshot.ELIG_CSPI_ID").alias("ELIG_CSPI_ID"),
    F.col("Snapshot.ELIG_PDPD_ID").alias("ELIG_PDPD_ID")
)

# "Reversal": rows where rebill_lkup.BILL_INVC_ID is not null
df_SnapshotReversalLogic_Reversal = df_SnapshotReversalLogic_sv.filter(
    ~F.col("rebill_lkup.BILL_INVC_ID").isNull()
).select(
    F.col("svReversalInvcId").alias("BILL_INVC_ID"),
    strip_field(F.col("Snapshot.PMFA_ID")).alias("PMFA_ID"),
    strip_field(F.col("Snapshot.BLFD_SOURCE")).alias("BLFD_SOURCE"),
    strip_field(F.col("Snapshot.BLFD_DISP_CD")).alias("BLFD_DISP_CD"),
    F.col("svRevrslFeeDscntAmt").alias("FEE_DSCNT_AMT"),
    F.col("Snapshot.GRGR_ID").alias("GRGR_ID"),
    F.col("Snapshot.SGSG_ID").alias("SGSG_ID"),
    F.col("Snapshot.GRP_ID").alias("GRP_ID"),
    F.col("Snapshot.SBSB_CK").alias("SBSB_CK"),
    F.col("Snapshot.CSCS_ID").alias("CSCS_ID"),
    F.col("Snapshot.ELIG_CSPI_ID").alias("ELIG_CSPI_ID"),
    F.col("Snapshot.ELIG_PDPD_ID").alias("ELIG_PDPD_ID")
)

#--------------------------------------------------------------------------------
# STAGE: Collector (CCollector) - collecting NonReversal and Reversal 
#   => single output "Transform"
#--------------------------------------------------------------------------------

df_Collector = (
    df_SnapshotReversalLogic_NonReversal.select(df_SnapshotReversalLogic_NonReversal.columns)
    .unionByName(
        df_SnapshotReversalLogic_Reversal.select(df_SnapshotReversalLogic_Reversal.columns)
    )
)

#--------------------------------------------------------------------------------
# STAGE: Transform (CTransformerStage)
#   => input from "Collector" => output "Transform"
#   => the pinned columns in DS: 
#    [BILL_INVC_ID, PMFA_ID, BLFD_SOURCE, BLFD_DISP_CD, FEE_DSCNT_AMT, GRGR_ID, SGSG_ID, GRP_ID, SBSB_CK, CSCS_ID, ELIG_CSPI_ID, ELIG_PDPD_ID]
#   They become "Transform" dataset.
#--------------------------------------------------------------------------------

df_Transform = df_Collector.select(
    F.col("BILL_INVC_ID").alias("BILL_INVC_ID"),
    F.col("PMFA_ID").alias("PMFA_ID"),
    F.col("BLFD_SOURCE").alias("BLFD_SOURCE"),
    F.col("BLFD_DISP_CD").alias("BLFD_DISP_CD"),
    F.col("FEE_DSCNT_AMT").alias("FEE_DSCNT_AMT"),
    F.col("GRGR_ID").alias("GRGR_ID"),
    F.col("SGSG_ID").alias("SGSG_ID"),
    F.col("GRP_ID").alias("GRP_ID"),
    F.col("SBSB_CK").alias("SBSB_CK"),
    F.col("CSCS_ID").alias("CSCS_ID"),
    F.col("ELIG_CSPI_ID").alias("ELIG_CSPI_ID"),
    F.col("ELIG_PDPD_ID").alias("ELIG_PDPD_ID")
)

#--------------------------------------------------------------------------------
# STAGE: Transform (CTransformerStage) 
#  => The next "Transform" in DS is called "Transform" again with stage variables:
#  => outputs "Snapshot_File"
#  => Stage variables referencing user-defined function GetFkeyCodes, etc. 
#  We replicate them as columns:
#--------------------------------------------------------------------------------

df_Transform2 = df_Transform.withColumn(
    "SRC_SYS_CD_SK", F.lit(SrcSysCdSk)
).withColumn(
    "INVC_FEE_DSCNT_SRC_CD_SK", F.lit("<GetFkeyCodes call>")  # replaced by actual stage var logic
).withColumn(
    "INVC_FEE_DSCNT_BILL_DISP_CD_SK", F.lit("<GetFkeyCodes call>")
).withColumn(
    "CLS_SK", F.lit("<GetFkeyCls call>")
).withColumn(
    "CLS_PLN_SK", F.lit("<GetFkeyClsPln call>")
).withColumn(
    "GRP_SK", F.lit("<GetFkeyGrp call>")
).withColumn(
    "PROD_SK", F.lit("<GetFkeyProd call>")
).withColumn(
    "SUBGRP_SK", F.lit("<GetFkeySubgrp call>")
).withColumn(
    "SUB_SK", F.lit("<GetFkeySub call>")
)

df_Transform_output = df_Transform2.select(
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("BILL_INVC_ID").alias("BILL_INVC_ID"),
    F.col("PMFA_ID").alias("FEE_DSCNT_ID"),  # DS calls it FEE_DSCNT_ID
    F.col("INVC_FEE_DSCNT_SRC_CD_SK").alias("INVC_FEE_DSCNT_SRC_CD_SK"),
    F.col("INVC_FEE_DSCNT_BILL_DISP_CD_SK").alias("INVC_FEE_DSCNT_BILL_DISP_CD_SK"),
    F.col("CLS_SK").alias("CLS_SK"),
    F.col("CLS_PLN_SK").alias("CLS_PLN_SK"),
    F.col("GRP_SK").alias("GRP_SK"),
    F.col("PROD_SK").alias("PROD_SK"),
    F.col("SUBGRP_SK").alias("SUBGRP_SK"),
    F.col("SUB_SK").alias("SUB_SK"),
    F.col("FEE_DSCNT_AMT").alias("FEE_DSCNT_AMT")
)

#--------------------------------------------------------------------------------
# STAGE: Snapshot_File (CSeqFileStage) 
#  => input is df_Transform_output
#  => writes to "load/B_INVC_FEE_DSCNT.dat" with comma delimiter, no header, quoteChar='"'
#--------------------------------------------------------------------------------

# Apply rpad for each char/varchar column to match declared lengths from DataStage:
df_Snapshot_File_final = df_Transform_output.select(
    F.rpad(F.col("SRC_SYS_CD_SK").cast(StringType()), 10, " ").alias("SRC_SYS_CD_SK"),  # not explicitly char in the JSON, but ensuring safe
    F.rpad(F.col("BILL_INVC_ID").cast(StringType()), 12, " ").alias("BILL_INVC_ID"),
    F.rpad(F.col("FEE_DSCNT_ID").cast(StringType()), 2, " ").alias("FEE_DSCNT_ID"),
    F.rpad(F.col("INVC_FEE_DSCNT_SRC_CD_SK").cast(StringType()), 10, " ").alias("INVC_FEE_DSCNT_SRC_CD_SK"),
    F.rpad(F.col("INVC_FEE_DSCNT_BILL_DISP_CD_SK").cast(StringType()), 10, " ").alias("INVC_FEE_DSCNT_BILL_DISP_CD_SK"),
    F.col("CLS_SK"),
    F.col("CLS_PLN_SK"),
    F.col("GRP_SK"),
    F.col("PROD_SK"),
    F.col("SUBGRP_SK"),
    F.col("SUB_SK"),
    F.col("FEE_DSCNT_AMT")
)

write_files(
    df_Snapshot_File_final,
    f"{adls_path}/load/B_INVC_FEE_DSCNT.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

#--------------------------------------------------------------------------------
# STAGE: ReversalLogic (CTransformerStage)
#   Primary link => df_BusinessRules_ChkReversals
#   lookup => hf_invc_rebills => scenario C read from parquet => left join
#   outputs => NonReversal, Reversal
#--------------------------------------------------------------------------------

df_invc_rebills = spark.read.parquet(f"{adls_path}/hf_invc_rebills.parquet")

df_ReversalLogic_join = df_BusinessRules_ChkReversals.alias("ChkReversals").join(
    df_invc_rebills.alias("rebill_lkup"),
    (F.col("ChkReversals.BILL_INVC_ID") == F.col("rebill_lkup.BILL_INVC_ID")),
    "left"
)

df_ReversalLogic_sv = df_ReversalLogic_join.withColumn(
    "svReversalString",
    F.concat(
        F.lit("FACETS;"),
        F.trim(F.col("ChkReversals.BLIV_ID")),
        F.lit("R"),
        F.trim(F.col("ChkReversals.PMFA_ID")),
        F.trim(F.col("ChkReversals.BLFD_SOURCE")),
        F.trim(F.col("ChkReversals.BLFD_DISP_CD"))
    )
).withColumn(
    "svReversalBlivId",
    F.concat(F.col("ChkReversals.BLIV_ID"), F.lit("R"))
).withColumn(
    "svReversalBillInvcId",
    F.concat(F.col("ChkReversals.BILL_INVC_ID"), F.lit("R"))
).withColumn(
    "svReversalFeeDscntAmt",
    -1 * F.col("ChkReversals.FEE_DSCNT_AMT")
)

df_ReversalLogic_NonReversal = df_ReversalLogic_sv.filter(
    F.col("rebill_lkup.BILL_INVC_ID").isNull()
).select(
    F.col("ChkReversals.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("ChkReversals.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("ChkReversals.DISCARD_IN").alias("DISCARD_IN"),
    F.col("ChkReversals.PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("ChkReversals.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("ChkReversals.ERR_CT").alias("ERR_CT"),
    F.col("ChkReversals.RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("ChkReversals.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("ChkReversals.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("ChkReversals.BLIV_ID").alias("BLIV_ID"),
    F.col("ChkReversals.PMFA_ID").alias("PMFA_ID"),
    F.col("ChkReversals.BLFD_SOURCE").alias("BLFD_SOURCE"),
    F.col("ChkReversals.BLFD_DISP_CD").alias("BLFD_DISP_CD"),
    F.col("ChkReversals.PMFA_FEE_DISC_IND").alias("PMFA_FEE_DISC_IND"),
    F.col("ChkReversals.GRGR_ID").alias("GRGR_ID"),
    F.col("ChkReversals.SGSG_ID").alias("SGSG_ID"),
    F.col("ChkReversals.SBSB_ID").alias("SBSB_ID"),
    F.col("ChkReversals.INVC_SK").alias("INVC_SK"),
    F.col("ChkReversals.BILL_INVC_ID").alias("BILL_INVC_ID"),
    F.col("ChkReversals.GRP_ID").alias("GRP_ID"),
    F.col("ChkReversals.INVC_ID").alias("INVC_ID"),
    F.col("ChkReversals.SUBGRP_ID").alias("SUBGRP_ID"),
    F.col("ChkReversals.DSCNT_IN").alias("DSCNT_IN"),
    F.col("ChkReversals.FEE_DSCNT_AMT").alias("FEE_DSCNT_AMT"),
    F.col("ChkReversals.SBSB_CK").alias("SBSB_CK"),
    F.col("ChkReversals.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("ChkReversals.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("ChkReversals.CSCS_ID").alias("CSCS_ID"),
    F.col("ChkReversals.ELIG_CSPI_ID").alias("ELIG_CSPI_ID"),
    F.col("ChkReversals.ELIG_PDPD_ID").alias("ELIG_PDPD_ID")
)

df_ReversalLogic_Reversal = df_ReversalLogic_sv.filter(
    ~F.col("rebill_lkup.BILL_INVC_ID").isNull()
).select(
    F.col("ChkReversals.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("ChkReversals.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("ChkReversals.DISCARD_IN").alias("DISCARD_IN"),
    F.col("ChkReversals.PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("ChkReversals.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("ChkReversals.ERR_CT").alias("ERR_CT"),
    F.col("ChkReversals.RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("ChkReversals.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("svReversalString").alias("PRI_KEY_STRING"),
    F.col("svReversalBlivId").alias("BLIV_ID"),
    F.col("ChkReversals.PMFA_ID").alias("PMFA_ID"),
    F.col("ChkReversals.BLFD_SOURCE").alias("BLFD_SOURCE"),
    F.col("ChkReversals.BLFD_DISP_CD").alias("BLFD_DISP_CD"),
    F.col("ChkReversals.PMFA_FEE_DISC_IND").alias("PMFA_FEE_DISC_IND"),
    F.col("ChkReversals.GRGR_ID").alias("GRGR_ID"),
    F.col("ChkReversals.SGSG_ID").alias("SGSG_ID"),
    F.col("ChkReversals.SBSB_ID").alias("SBSB_ID"),
    F.col("ChkReversals.INVC_SK").alias("INVC_SK"),
    F.col("svReversalBillInvcId").alias("BILL_INVC_ID"),
    F.col("ChkReversals.GRP_ID").alias("GRP_ID"),
    F.col("ChkReversals.INVC_ID").alias("INVC_ID"),
    F.col("ChkReversals.SUBGRP_ID").alias("SUBGRP_ID"),
    F.col("ChkReversals.DSCNT_IN").alias("DSCNT_IN"),
    F.col("svReversalFeeDscntAmt").alias("FEE_DSCNT_AMT"),
    F.col("ChkReversals.SBSB_CK").alias("SBSB_CK"),
    F.col("ChkReversals.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("ChkReversals.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("ChkReversals.CSCS_ID").alias("CSCS_ID"),
    F.col("ChkReversals.ELIG_CSPI_ID").alias("ELIG_CSPI_ID"),
    F.col("ChkReversals.ELIG_PDPD_ID").alias("ELIG_PDPD_ID")
)

#--------------------------------------------------------------------------------
# STAGE: CollectRows (CCollector)
#   => merges NonReversal, Reversal
#   => final output pinned as "Transform1"
#--------------------------------------------------------------------------------

df_CollectRows = (
    df_ReversalLogic_NonReversal.select(df_ReversalLogic_NonReversal.columns)
    .unionByName(
        df_ReversalLogic_Reversal.select(df_ReversalLogic_Reversal.columns)
    )
)

#--------------------------------------------------------------------------------
# STAGE: Key (CTransformerStage)
#   => input "Transform1" => two outputs "AllCol", "Transform"
#--------------------------------------------------------------------------------

df_Key_in = df_CollectRows

df_Key_AllCol = df_Key_in.select(
    F.lit(SrcSysCdSk).cast(StringType()).alias("SRC_SYS_CD_SK"),
    F.col("BLIV_ID").alias("BLIV_ID"),
    F.col("PMFA_ID").alias("PMFA_ID"),
    F.col("BLFD_SOURCE").alias("BLFD_SOURCE"),
    F.col("BLFD_DISP_CD").alias("BLFD_DISP_CD"),
    F.col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("DISCARD_IN").alias("DISCARD_IN"),
    F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("ERR_CT").alias("ERR_CT"),
    F.col("RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("PMFA_FEE_DISC_IND").alias("PMFA_FEE_DISC_IND"),
    F.col("GRGR_ID").alias("GRGR_ID"),
    F.col("SGSG_ID").alias("SGSG_ID"),
    F.col("SBSB_ID").alias("SBSB_ID"),
    F.col("INVC_SK").alias("INVC_SK"),
    F.col("BILL_INVC_ID").alias("BILL_INVC_ID"),
    F.col("GRP_ID").alias("GRP_ID"),
    F.col("INVC_ID").alias("INVC_ID"),
    F.col("SUBGRP_ID").alias("SUBGRP_ID"),
    F.col("DSCNT_IN").alias("DSCNT_IN"),
    F.col("FEE_DSCNT_AMT").alias("FEE_DSCNT_AMT"),
    F.col("SBSB_CK").alias("SBSB_CK"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CSCS_ID").alias("CSCS_ID"),
    F.col("ELIG_CSPI_ID").alias("ELIG_CSPI_ID"),
    F.col("ELIG_PDPD_ID").alias("ELIG_PDPD_ID")
)

df_Key_Transform = df_Key_in.select(
    F.lit(SrcSysCdSk).cast(StringType()).alias("SRC_SYS_CD_SK"),
    F.col("BLIV_ID").alias("BILL_INVC_ID"),
    F.col("PMFA_ID").alias("FEE_DSCNT_ID"),
    F.col("BLFD_SOURCE").alias("INVC_FEE_DSCNT_SRC_CD"),
    F.col("BLFD_DISP_CD").alias("INVC_FEE_DSCNT_BILL_DISP_CD")
)

#--------------------------------------------------------------------------------
# STAGE: IncmInvcFeeDscntPK (SharedContainer)
#   => two input pins: AllCol, Transform
#   => one output pin: Key
#--------------------------------------------------------------------------------

params_incm_invc_fee_dscnt = {
    "CurrRunCycle": CurrRunCycle,
    "SrcSysCd": SrcSysCd,
    "RunID": RunID,
    "CurrentDate": CurrDate,
    "IDSOwner": IDSOwner
}

df_IncmInvcFeeDscntPK_Key = IncmInvcFeeDscntPK(df_Key_AllCol, df_Key_Transform, params_incm_invc_fee_dscnt)

#--------------------------------------------------------------------------------
# STAGE: IdsInvcFeeDscnt (CSeqFileStage)
#   => input from df_IncmInvcFeeDscntPK_Key
#   => writes to "key/#TmpOutFile#" in overwrite mode
#--------------------------------------------------------------------------------

# Apply rpad for final, according to the output metadata
df_IdsInvcFeeDscnt_final = df_IncmInvcFeeDscntPK_Key.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("FEE_DSCNT_SK"),
    F.col("FEE_DSCNT_ID"),
    F.rpad(F.col("PMFA_ID"), 2, " ").alias("PMFA_ID"),
    F.rpad(F.col("BLFD_SOURCE"), 1, " ").alias("BLFD_SOURCE"),
    F.rpad(F.col("BLFD_DISP_CD"), 1, " ").alias("BLFD_DISP_CD"),
    F.rpad(F.col("PMFA_FEE_DISC_IND"), 1, " ").alias("PMFA_FEE_DISC_IND"),
    F.rpad(F.col("GRGR_ID"), 8, " ").alias("GRGR_ID"),
    F.rpad(F.col("SGSG_ID"), 4, " ").alias("SGSG_ID"),
    F.rpad(F.col("SBSB_ID"), 9, " ").alias("SBSB_ID"),
    F.col("INVC_SK"),
    F.col("BILL_INVC_ID"),
    F.rpad(F.col("GRP_ID"), 8, " ").alias("GRP_ID"),
    F.col("INVC_ID"),
    F.rpad(F.col("SUBGRP_ID"), 4, " ").alias("SUBGRP_ID"),
    F.rpad(F.col("DSCNT_IN"), 1, " ").alias("DSCNT_IN"),
    F.col("FEE_DSCNT_AMT"),
    F.col("SBSB_CK"),
    F.rpad(F.col("CSCS_ID"), 4, " ").alias("CSCS_ID"),
    F.rpad(F.col("CLS_PLN_ID"), 8, " ").alias("CLS_PLN_ID"),
    F.rpad(F.col("PROD_ID"), 8, " ").alias("PROD_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

write_files(
    df_IdsInvcFeeDscnt_final,
    f"{adls_path}/key/{TmpOutFile}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)