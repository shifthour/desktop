# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_3 01/14/09 10:18:26 Batch  14990_37109 PROMOTE bckcetl ids20 dsadm bls for sg
# MAGIC ^1_3 01/14/09 10:10:44 Batch  14990_36646 INIT bckcett testIDS dsadm BLS FOR SG
# MAGIC ^1_1 12/16/08 22:11:05 Batch  14961_79874 PROMOTE bckcett testIDS u03651 steph - income primary key conversion
# MAGIC ^1_1 12/16/08 22:03:32 Batch  14961_79415 INIT bckcett devlIDS u03651 steffy
# MAGIC ^1_2 11/12/07 10:00:11 Batch  14561_36015 INIT bckcetl ids20 dcg01 sa
# MAGIC ^1_2 11/02/07 13:07:35 Batch  14551_47275 PROMOTE bckcetl ids20 dsadm bls for on
# MAGIC ^1_2 11/02/07 12:52:45 Batch  14551_46368 INIT bckcett testIDS30 dsadm bls for on
# MAGIC ^1_1 10/31/07 13:17:35 Batch  14549_47857 PROMOTE bckcett testIDS30 u03651 steffy
# MAGIC ^1_1 10/31/07 13:09:37 Batch  14549_47387 INIT bckcett devlIDS30 u03651 steffy
# MAGIC ^1_1 09/29/07 17:49:42 Batch  14517_64188 PROMOTE bckcett devlIDS30 u10157 sa
# MAGIC ^1_1 09/29/07 17:45:48 Batch  14517_63967 PROMOTE bckcett devlIDS30 u10157 sa
# MAGIC ^1_1 09/29/07 17:41:57 Batch  14517_63723 PROMOTE bckcett devlIDS30 u10157 sa
# MAGIC ^1_1 09/29/07 17:40:34 Batch  14517_63638 INIT bckcetl ids20 dcg01 sa
# MAGIC ^1_1 08/27/07 08:55:46 Batch  14484_32151 INIT bckcetl ids20 dsadm dsadm
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
# MAGIC COPYRIGHT 2006 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC JOB NAME:     FctsIncomeInvcDscrtnExtr
# MAGIC CALLED BY:   FctsIncomeExtrSeq
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC     Pulls the Fee Discount information from Facets for Income 
# MAGIC       
# MAGIC    Date is used as a criteria for which records are pulled.   
# MAGIC 
# MAGIC INPUTS:
# MAGIC    CDS_INID_INVOICE
# MAGIC    CDS_INDI_DISCRETN
# MAGIC   
# MAGIC HASH FILES:  hf_invc_dscrtn
# MAGIC                         hf_invcdscrtn_med_cls
# MAGIC                         hf_invcdscrtn_dent_cls
# MAGIC                         hf_invcdscrtn_othr_cls
# MAGIC                         hf_invcdscrtn_med_cls_not_elig
# MAGIC                         hf_invcdscrtn_dent_cls_not_elig
# MAGIC                         hf_invcdscrtn_othr_cls_not_elig
# MAGIC 
# MAGIC 
# MAGIC TRANSFORMS:  
# MAGIC                   used the STRIP.FIELD to all of the character fields.
# MAGIC                   The DB2 timestamp value of the audit_ts is converted to a sybase timestamp with the transofrm  FORMAT.DATE
# MAGIC 
# MAGIC                   ParseDate           -   Parse out the first date from the input string in CCYY-MM-DD format.
# MAGIC                   ParseEndDate     -   Parse out the second date from the input string in CCYY-MM-DD format.
# MAGIC                   ParseCert            -   Parse out the Certificate number from the input string.
# MAGIC                   ParseDesc          -   Parse the input string removing Cert / Dates / Months
# MAGIC                   ParseMnths         -   Parse the number of months from the input string.
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
# MAGIC              Ralph Tucker  -  10/30/2005-   Originally Programmed
# MAGIC                                      -  02/27/2006 -  Made 1 a default if MO_QTY is over 72 or < 0
# MAGIC                SAndrew             12/12/2006   - Project 1756 - Added extraction critieria from CDS_INID_INVOICE for where  INID.AFAI_CK = 0           
# MAGIC 
# MAGIC                 Naren Garapaty - 04/2007 - Added Reversal Logic
# MAGIC 
# MAGIC 
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                                 Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                            ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada              6/14/2007          3264                              Added Balancing process to the overall                    devlIDS30                    Steph Goddard             09/15/2007
# MAGIC                                                                                                       job that takes a snapshot of the source data                        
# MAGIC 
# MAGIC 
# MAGIC Bhoomi Dasari                 09/28/2008        3567                             Added new primay key contianer and SrcSysCdsk     devlIDS                         Steph Goddard            10/03/2008                        
# MAGIC                                                                                                        and SrcSysCd  
# MAGIC 
# MAGIC Kimberly Doty                  08-25-2010         TTR 551                     Added 3 new columns - DPNDT_CT, SUB_CT            RebuildIntNewDevl       Steph Goddard             10/01/2010
# MAGIC                                                                                                      and SELF_BILL_LIFE_IN; modified 
# MAGIC 
# MAGIC 
# MAGIC Goutham Kalidindi  2021-03-24                258186                        Changed Datatype length for field                                    IntegrateDev1              Kalyan Neelam             2021-03-3
# MAGIC                                                                                               BLIV_ID
# MAGIC                                                                                             char(12) to Varchar(15)
# MAGIC Prabhu ES                    2022-03-02         S2S Remediation        MSSQL ODBC conn added and other param changes      IntegrateDev5		Ken Bradmon	2022-06-15

# MAGIC Hash file hf_invc_dscrtn_allcol cleared
# MAGIC Pulls all Invoice Discretionary rows from facets CDS_INDI_DISCRETN table matching data criteria.
# MAGIC Due to the Complexity of the whole job and the fields required for the snapshot file, the Extraction for Balancing is done at an Intermediate Stage within the job to reduce the overhead involved and the complexity of duplicating the whole process
# MAGIC Balancing snapshot of source table
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../shared_containers/PrimaryKey/IncmInvcDscrtnPK
# COMMAND ----------

# Parameters
TmpOutFile = get_widget_value('TmpOutFile','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
FacetsOwner = get_widget_value('FacetsOwner','')
DriverTable = get_widget_value('DriverTable','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
SrcSysCd = get_widget_value('SrcSysCd','')
IDSOwner = get_widget_value('IDSOwner','')
RunId = get_widget_value('RunId','')
CurrDate = get_widget_value('CurrDate','')
facets_secret_name = get_widget_value('facets_secret_name','')
ids_secret_name = get_widget_value('ids_secret_name','')

# Read hashed file hf_invc_rebills_rvrsl (Scenario C => parquet)
df_hf_invc_rebills_rvrsl = spark.read.parquet(f"{adls_path}/hf_invc_rebills_rvrsl.parquet")
df_hf_invc_rebills_rvrsl = df_hf_invc_rebills_rvrsl.select("BILL_INVC_ID")

# Read hashed file hf_invc_rebills (Scenario C => parquet)
df_hf_invc_rebills = spark.read.parquet(f"{adls_path}/hf_invc_rebills.parquet")
df_hf_invc_rebills = df_hf_invc_rebills.select("BILL_INVC_ID")

# ODBCConnector: CDS_INDI_DISCRETN
jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)

extract_query_Extract = """SELECT 
INDI.BLIV_ID,
INDI.BLDI_SEQ_NO,
INDI.BLDI_DESC,
INDI.BLDI_UPDATE_DTM,
INDI.BLDI_DISP_CD,
INDI.BLDI_PREM_FEE_IND,
INDI.PMFA_ID,
INDI.BLDI_FEE_DISC_AMT,
INDI.CSPI_ID,
INDI.PDPD_ID,
INDI.PDBL_ID,
INDI.BLDI_PREM_SB,
INDI.BLDI_PREM_DEP,
INID.GRGR_ID,
INID.SGSG_ID,
INID.SBSB_CK,
INDI.BLDI_LVS_DEP DPNDT_CT,
INDI.BLDI_LVS_SB SUB_CT
FROM tempdb..#""" + DriverTable + """# TmpInvc,
""" + FacetsOwner + """.CDS_INID_INVOICE INID,
""" + FacetsOwner + """.CDS_INDI_DISCRETN INDI
WHERE
TmpInvc.BILL_INVC_ID = INID.BLIV_ID 
AND INID.BLIV_ID = INDI.BLIV_ID
AND INID.AFAI_CK = 0
"""
df_extract = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_Extract)
    .load()
)

extract_query_lnkMed = """SELECT 
INDI.BLIV_ID,
ELIG.CSCS_ID
FROM tempdb..#""" + DriverTable + """# TmpInvc,
""" + FacetsOwner + """.CDS_INID_INVOICE INID,
""" + FacetsOwner + """.CDS_INDI_DISCRETN INDI,
""" + FacetsOwner + """.CMC_MEME_MEMBER MEME,
""" + FacetsOwner + """.CMC_MEPE_PRCS_ELIG ELIG
WHERE
TmpInvc.BILL_INVC_ID = INID.BLIV_ID
AND INID.BLIV_ID = INDI.BLIV_ID
AND INID.SBSB_CK = MEME.SBSB_CK
AND INID.GRGR_CK = MEME.GRGR_CK
AND MEME.MEME_REL = 'M'
AND MEME.MEME_CK = ELIG.MEME_CK
AND ELIG.MEPE_EFF_DT <= GetDate()
AND ELIG.CSPD_CAT = 'M'
AND ELIG.MEPE_ELIG_IND = 'Y'
AND ELIG.MEPE_TERM_DT = (
   SELECT MAX(ELIG2.MEPE_TERM_DT)
   FROM """ + FacetsOwner + """.CMC_MEPE_PRCS_ELIG ELIG2
   WHERE ELIG2.MEME_CK = ELIG.MEME_CK
   AND ELIG2.MEPE_EFF_DT <= GetDate()
   AND ELIG2.MEPE_ELIG_IND = 'Y'
   AND ELIG2.CSPD_CAT = 'M'
)
"""
df_lnkClsLkupMed = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_lnkMed)
    .load()
)

extract_query_lnkDent = """SELECT 
INDI.BLIV_ID,
ELIG.CSCS_ID
FROM tempdb..#""" + DriverTable + """# TmpInvc,
""" + FacetsOwner + """.CDS_INID_INVOICE INID,
""" + FacetsOwner + """.CDS_INDI_DISCRETN INDI,
""" + FacetsOwner + """.CMC_MEME_MEMBER MEME,
""" + FacetsOwner + """.CMC_MEPE_PRCS_ELIG ELIG
WHERE
TmpInvc.BILL_INVC_ID = INID.BLIV_ID
AND INID.BLIV_ID = INDI.BLIV_ID
AND INID.SBSB_CK = MEME.SBSB_CK
AND INID.GRGR_CK = MEME.GRGR_CK
AND MEME.MEME_REL = 'M'
AND MEME.MEME_CK = ELIG.MEME_CK
AND ELIG.MEPE_EFF_DT <= GetDate()
AND ELIG.CSPD_CAT = 'D'
AND ELIG.MEPE_ELIG_IND = 'Y'
AND ELIG.MEPE_TERM_DT = (
   SELECT MAX(ELIG2.MEPE_TERM_DT)
   FROM """ + FacetsOwner + """.CMC_MEPE_PRCS_ELIG ELIG2
   WHERE ELIG2.MEME_CK = ELIG.MEME_CK
   AND ELIG2.MEPE_EFF_DT <= GetDate()
   AND ELIG2.MEPE_ELIG_IND = 'Y'
   AND ELIG2.CSPD_CAT = 'D'
)
"""
df_lnkClsLkupDent = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_lnkDent)
    .load()
)

extract_query_lnkOthr = """SELECT 
INDI.BLIV_ID,
ELIG.CSCS_ID
FROM tempdb..#""" + DriverTable + """# TmpInvc,
""" + FacetsOwner + """.CDS_INID_INVOICE INID,
""" + FacetsOwner + """.CDS_INDI_DISCRETN INDI,
""" + FacetsOwner + """.CMC_MEME_MEMBER MEME,
""" + FacetsOwner + """.CMC_MEPE_PRCS_ELIG ELIG
WHERE
TmpInvc.BILL_INVC_ID = INID.BLIV_ID
AND INID.BLIV_ID = INDI.BLIV_ID
AND INID.SBSB_CK = MEME.SBSB_CK
AND INID.GRGR_CK = MEME.GRGR_CK
AND MEME.MEME_REL = 'M'
AND MEME.MEME_CK = ELIG.MEME_CK
AND ELIG.MEPE_EFF_DT <= GetDate()
AND ELIG.CSPD_CAT not in ('M','D')
AND ELIG.MEPE_ELIG_IND = 'Y'
AND ELIG.MEPE_TERM_DT = (
   SELECT MAX(ELIG2.MEPE_TERM_DT)
   FROM """ + FacetsOwner + """.CMC_MEPE_PRCS_ELIG ELIG2
   WHERE ELIG2.MEME_CK = ELIG.MEME_CK
   AND ELIG2.MEPE_EFF_DT <= GetDate()
   AND ELIG2.MEPE_ELIG_IND = 'Y'
   AND ELIG2.CSPD_CAT not in ('M','D')
)
"""
df_lnkClsLkupOthr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_lnkOthr)
    .load()
)

extract_query_lnkMedNotElig = """SELECT 
INDI.BLIV_ID,
ELIG.CSCS_ID
FROM tempdb..#""" + DriverTable + """# TmpInvc,
""" + FacetsOwner + """.CDS_INID_INVOICE INID,
""" + FacetsOwner + """.CDS_INDI_DISCRETN INDI,
""" + FacetsOwner + """.CMC_MEME_MEMBER MEME,
""" + FacetsOwner + """.CMC_MEPE_PRCS_ELIG ELIG
WHERE
TmpInvc.BILL_INVC_ID = INID.BLIV_ID
AND INID.BLIV_ID = INDI.BLIV_ID
AND INID.SBSB_CK = MEME.SBSB_CK
AND INID.GRGR_CK = MEME.GRGR_CK
AND MEME.MEME_REL = 'M'
AND MEME.MEME_CK = ELIG.MEME_CK
AND ELIG.MEPE_EFF_DT <= GetDate()
AND ELIG.CSPD_CAT = 'M'
AND ELIG.MEPE_ELIG_IND = 'N'
AND ELIG.MEPE_TERM_DT = (
   SELECT MAX(ELIG2.MEPE_TERM_DT)
   FROM """ + FacetsOwner + """.CMC_MEPE_PRCS_ELIG ELIG2
   WHERE ELIG2.MEME_CK = ELIG.MEME_CK
   AND ELIG2.MEPE_EFF_DT <= GetDate()
   AND ELIG2.MEPE_ELIG_IND = 'N'
   AND ELIG2.CSPD_CAT = 'M'
)
"""
df_lnkClsLkupMedNotElig = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_lnkMedNotElig)
    .load()
)

extract_query_lnkDentNotElig = """SELECT 
INDI.BLIV_ID,
ELIG.CSCS_ID
FROM tempdb..#""" + DriverTable + """# TmpInvc,
""" + FacetsOwner + """.CDS_INID_INVOICE INID,
""" + FacetsOwner + """.CDS_INDI_DISCRETN INDI,
""" + FacetsOwner + """.CMC_MEME_MEMBER MEME,
""" + FacetsOwner + """.CMC_MEPE_PRCS_ELIG ELIG
WHERE
TmpInvc.BILL_INVC_ID = INID.BLIV_ID
AND INID.BLIV_ID = INDI.BLIV_ID
AND INID.SBSB_CK = MEME.SBSB_CK
AND INID.GRGR_CK = MEME.GRGR_CK
AND MEME.MEME_REL = 'M'
AND MEME.MEME_CK = ELIG.MEME_CK
AND ELIG.MEPE_EFF_DT <= GetDate()
AND ELIG.CSPD_CAT = 'D'
AND ELIG.MEPE_ELIG_IND = 'N'
AND ELIG.MEPE_TERM_DT = (
   SELECT MAX(ELIG2.MEPE_TERM_DT)
   FROM """ + FacetsOwner + """.CMC_MEPE_PRCS_ELIG ELIG2
   WHERE ELIG2.MEME_CK = ELIG.MEME_CK
   AND ELIG2.MEPE_EFF_DT <= GetDate()
   AND ELIG2.MEPE_ELIG_IND = 'N'
   AND ELIG2.CSPD_CAT = 'D'
)
"""
df_lnkClsLkupDentNotElig = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_lnkDentNotElig)
    .load()
)

extract_query_lnkOthrNotElig = """SELECT 
INDI.BLIV_ID,
ELIG.CSCS_ID
FROM tempdb..#""" + DriverTable + """# TmpInvc,
""" + FacetsOwner + """.CDS_INID_INVOICE INID,
""" + FacetsOwner + """.CDS_INDI_DISCRETN INDI,
""" + FacetsOwner + """.CMC_MEME_MEMBER MEME,
""" + FacetsOwner + """.CMC_MEPE_PRCS_ELIG ELIG
WHERE
TmpInvc.BILL_INVC_ID = INID.BLIV_ID
AND INID.BLIV_ID = INDI.BLIV_ID
AND INID.SBSB_CK = MEME.SBSB_CK
AND INID.GRGR_CK = MEME.GRGR_CK
AND MEME.MEME_REL = 'M'
AND MEME.MEME_CK = ELIG.MEME_CK
AND ELIG.MEPE_EFF_DT <= GetDate()
AND ELIG.CSPD_CAT not in ('M','D')
AND ELIG.MEPE_ELIG_IND = 'N'
AND ELIG.MEPE_TERM_DT = (
   SELECT MAX(ELIG2.MEPE_TERM_DT)
   FROM """ + FacetsOwner + """.CMC_MEPE_PRCS_ELIG ELIG2
   WHERE ELIG2.MEME_CK = ELIG.MEME_CK
   AND ELIG2.MEPE_EFF_DT <= GetDate()
   AND ELIG2.MEPE_ELIG_IND = 'N'
   AND ELIG2.CSPD_CAT not in ('M','D')
)
"""
df_lnkClsLkupOthrNotElig = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_lnkOthrNotElig)
    .load()
)

# Since "hf_InvcDscrtn_cls" is Scenario A, we remove that stage and directly deduplicate each classification DataFrame on BLIV_ID

df_lnkClsLkupMed_dedup = dedup_sort(df_lnkClsLkupMed, ["BLIV_ID"], [])
df_lnkClsLkupDent_dedup = dedup_sort(df_lnkClsLkupDent, ["BLIV_ID"], [])
df_lnkClsLkupOthr_dedup = dedup_sort(df_lnkClsLkupOthr, ["BLIV_ID"], [])
df_lnkClsLkupMedNotElig_dedup = dedup_sort(df_lnkClsLkupMedNotElig, ["BLIV_ID"], [])
df_lnkClsLkupDentNotElig_dedup = dedup_sort(df_lnkClsLkupDentNotElig, ["BLIV_ID"], [])
df_lnkClsLkupOthrNotElig_dedup = dedup_sort(df_lnkClsLkupOthrNotElig, ["BLIV_ID"], [])

# Now build the StripField equivalent DataFrame.
# First join everything. We'll alias them as indicated for clarity.

df_strip_pre = (
    df_extract.alias("Extract")
    .join(df_lnkClsLkupMed_dedup.alias("refClsLkupMed"), F.col("Extract.BLIV_ID") == F.col("refClsLkupMed.BLIV_ID"), "left")
    .join(df_lnkClsLkupDent_dedup.alias("refClsLkupDent"), F.col("Extract.BLIV_ID") == F.col("refClsLkupDent.BLIV_ID"), "left")
    .join(df_lnkClsLkupOthr_dedup.alias("refClsLkupOthr"), F.col("Extract.BLIV_ID") == F.col("refClsLkupOthr.BLIV_ID"), "left")
    .join(df_lnkClsLkupMedNotElig_dedup.alias("refClsLkupMedNotElig"), F.col("Extract.BLIV_ID") == F.col("refClsLkupMedNotElig.BLIV_ID"), "left")
    .join(df_lnkClsLkupDentNotElig_dedup.alias("refClsLkupDentNotElig"), F.col("Extract.BLIV_ID") == F.col("refClsLkupDentNotElig.BLIV_ID"), "left")
    .join(df_lnkClsLkupOthrNotElig_dedup.alias("refClsLkupOthrNotElig"), F.col("Extract.BLIV_ID") == F.col("refClsLkupOthrNotElig.BLIV_ID"), "left")
)

# Now replicate the stage variables from StripField
# We'll chain withColumns. We assume each function like ParseCert, FORMAT_DATE, etc. is already defined. 
# Field-level transformations:

df_strip_vars = (
    df_strip_pre
    .withColumn("svCertExtr", ParseCert(F.col("Extract.BLDI_DESC")))
    .withColumn("svBLDIYear", FORMAT_DATE(F.col("Extract.BLDI_UPDATE_DTM"), F.lit("SYBASE"), F.lit("Timestamp"), F.lit("CCYY-MM-DD")))
    .withColumn("svBeginDt", ParseDate(F.col("Extract.BLDI_DESC"), F.col("svBLDIYear")))
    .withColumn("svEndDt", ParseEndDate(F.col("Extract.BLDI_DESC")))
    .withColumn("svParseMths", ParseMths(F.col("Extract.BLDI_DESC")))
    .withColumn("svMths", F.when(F.col("svParseMths") == F.lit("?"), F.lit("1")).otherwise(F.col("svParseMths")))
    .withColumn("svMths2", F.when(F.col("svMths").cast("int") < 0, F.lit("1")).otherwise(F.col("svMths")))
    .withColumn("svParsedDesc", Parse4Desc(F.col("Extract.BLDI_DESC")))
    .withColumn("svBeginDt2",
        F.when(F.col("svBeginDt") == F.lit("?"), F.col("svBLDIYear"))
         .otherwise(FORMAT_DATE(F.col("svBeginDt"), F.lit("DATE"), F.lit("MM/DD/YY"), F.lit("CCYY-MM-DD")))
    )
    .withColumn("svEndDt2",
        F.when(F.col("svEndDt") == F.lit("?"),
               FIND_DATE(F.col("svBeginDt2"), (F.col("svMths").cast("int") - 1), F.lit("M"), F.lit("L"), F.lit("CCYY-MM-DD")))
         .when((F.substring(F.col("svBeginDt2"), 9, 2) == F.lit("01")) & (F.col("svParseMths") == F.lit("1")),
               FIND_DATE(F.col("svBeginDt2"), F.lit(0), F.lit("M"), F.lit("L"), F.lit("CCYY-MM-DD")))
         .when((F.substring(F.col("svBeginDt2"), 9, 2) == F.lit("01")) & (F.col("svMths").cast("int") > 1),
               FIND_DATE(F.col("svBeginDt2"), (F.col("svMths").cast("int") - 1), F.lit("M"), F.lit("L"), F.lit("CCYY-MM-DD")))
         .when(F.col("svBeginDt2") > FORMAT_DATE(F.col("svEndDt"), F.lit("DATE"), F.lit("MM/DD/YY"), F.lit("CCYY-MM-DD")),
               FIND_DATE(F.col("svBeginDt2"), (F.col("svMths").cast("int") - 1), F.lit("M"), F.lit("L"), F.lit("CCYY-MM-DD")))
         .otherwise(FORMAT_DATE(F.col("svEndDt"), F.lit("DATE"), F.lit("MM/DD/YY"), F.lit("CCYY-MM-DD")))
    )
    .withColumn("svCalcMths",
        F.when(F.col("svParseMths") == F.lit("?"), MonthDiff(F.col("svBeginDt2"), F.col("svEndDt2")))
         .otherwise(F.col("svMths"))
    )
    .withColumn("svEndDt3",
        F.when((F.col("svParseMths") != F.lit("?")) & (F.col("svMths") != F.col("svCalcMths")),
               FIND_DATE(F.col("svBeginDt2"), (F.col("svMths").cast("int") - 1), F.lit("M"), F.lit("L"), F.lit("CCYY-MM-DD")))
         .otherwise(F.col("svEndDt2"))
    )
)

# Now build final output columns for "StripField" (link name "Strip"). 
df_strip = (
    df_strip_vars
    .withColumn("BLIV_ID", ConvertNonPrintable(F.col("Extract.BLIV_ID")))
    .withColumn("BLDI_SEQ_NO", F.col("Extract.BLDI_SEQ_NO"))
    .withColumn("BLDI_DESC", ConvertNonPrintable(F.col("Extract.BLDI_DESC")))
    .withColumn("BLDI_UPDATE_DTM", F.col("Extract.BLDI_UPDATE_DTM"))
    .withColumn("BLDI_DISP_CD", ConvertNonPrintable(F.col("Extract.BLDI_DISP_CD")))
    .withColumn("BLDI_PREM_FEE_IND", ConvertNonPrintable(F.col("Extract.BLDI_PREM_FEE_IND")))
    .withColumn("PMFA_ID", ConvertNonPrintable(F.col("Extract.PMFA_ID")))
    .withColumn("BLDI_FEE_DISC_AMT", F.col("Extract.BLDI_FEE_DISC_AMT"))
    .withColumn("CSPI_ID", ConvertNonPrintable(F.col("Extract.CSPI_ID")))
    .withColumn("PDPD_ID", ConvertNonPrintable(F.col("Extract.PDPD_ID")))
    .withColumn("PDBL_ID", ConvertNonPrintable(F.col("Extract.PDBL_ID")))
    .withColumn("BLDI_PREM_SB", F.col("Extract.BLDI_PREM_SB"))
    .withColumn("BLDI_PREM_DEP", F.col("Extract.BLDI_PREM_DEP"))
    .withColumn("GRGR_ID", ConvertNonPrintable(F.col("Extract.GRGR_ID")))
    .withColumn("SGSG_ID", ConvertNonPrintable(F.col("Extract.SGSG_ID")))
    .withColumn("MO_QTY", F.col("svCalcMths"))
    .withColumn("SH_DESC", ConvertNonPrintable(F.col("svParsedDesc")))
    .withColumn("SBSB_CK", F.col("Extract.SBSB_CK"))
    .withColumn(
        "CSCS_id",
        F.when(~F.isnull(ConvertNonPrintable(F.col("refClsLkupMed.CSCS_ID"))), ConvertNonPrintable(F.col("refClsLkupMed.CSCS_ID")))
         .when(~F.isnull(ConvertNonPrintable(F.col("refClsLkupDent.CSCS_ID"))), ConvertNonPrintable(F.col("refClsLkupDent.CSCS_ID")))
         .when(~F.isnull(ConvertNonPrintable(F.col("refClsLkupOthr.CSCS_ID"))), ConvertNonPrintable(F.col("refClsLkupOthr.CSCS_ID")))
         .when(~F.isnull(ConvertNonPrintable(F.col("refClsLkupMedNotElig.CSCS_ID"))), ConvertNonPrintable(F.col("refClsLkupMedNotElig.CSCS_ID")))
         .when(~F.isnull(ConvertNonPrintable(F.col("refClsLkupDentNotElig.CSCS_ID"))), ConvertNonPrintable(F.col("refClsLkupDentNotElig.CSCS_ID")))
         .when(~F.isnull(ConvertNonPrintable(F.col("refClsLkupOthrNotElig.CSCS_ID"))), ConvertNonPrintable(F.col("refClsLkupOthrNotElig.CSCS_ID")))
         .otherwise(F.lit("UNK"))
    )
    .withColumn("INVC_DSCRTN_BEG_DT_SK", F.col("svBeginDt2"))
    .withColumn("INVC_DSCRTN_END_DT_SK", F.col("svEndDt2"))
    .withColumn("DSCRTN_PRSN_ID_TX", F.when(F.trim(F.col("svCertExtr")) == F.lit("?"), F.lit(" ")).otherwise(F.trim(F.col("svCertExtr"))))
    .withColumn("DPNDT_CT", F.col("Extract.DPNDT_CT"))
    .withColumn("SUB_CT", F.col("Extract.SUB_CT"))
)

# BusinessRules stage
# Define stage variables for df_strip
df_business_vars = (
    df_strip
    .withColumn("sv1", F.when(F.isnull(F.col("BLDI_DESC")), F.lit(0)).otherwise(CountSubstring(F.col("BLDI_DESC"), F.lit("SELF BILL"))))
    .withColumn("sv2", F.when(F.isnull(F.col("BLDI_DESC")), F.lit(0)).otherwise(CountSubstring(F.col("BLDI_DESC"), F.lit("SELF BILLED"))))
    .withColumn("sv3", F.when(F.isnull(F.col("BLDI_DESC")), F.lit(0)).otherwise(CountSubstring(F.col("BLDI_DESC"), F.lit("SELF-BILL"))))
    .withColumn("sv4", F.when(F.isnull(F.col("BLDI_DESC")), F.lit(0)).otherwise(CountSubstring(F.col("BLDI_DESC"), F.lit("SELF-BILLED"))))
    .withColumn("svSelfBillLifeIn",
        F.when(
           (
             ((F.col("PDBL_ID") == F.lit("LF01")) | (F.col("PDBL_ID") == F.lit("LF02"))) &
             ((F.col("sv1") > 0) | (F.col("sv2") > 0) | (F.col("sv3") > 0) | (F.col("sv4") > 0))
           ),
           F.lit("Y")
        ).otherwise(F.lit("N"))
    )
)

# Output from BusinessRules has two links: ChkReversals and Snapshot, both filter "Strip.MO_QTY <> '?'"

df_business_chk = df_business_vars.filter(F.col("MO_QTY") != F.lit("?"))
df_business_snapshot = df_business_vars.filter(F.col("MO_QTY") != F.lit("?"))

# Build the ChkReversals link columns
df_chkReversals = (
    df_business_chk
    .select(
        F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.lit("I").alias("INSRT_UPDT_CD"),
        F.lit("N").alias("DISCARD_IN"),
        F.lit("Y").alias("PASS_THRU_IN"),
        F.col("CurrDate").alias("FIRST_RECYC_DT"),  # The original expression is "CurrDate"
        F.lit(0).alias("ERR_CT"),
        F.lit(0).alias("RECYCLE_CT"),
        F.lit("FACETS").alias("SRC_SYS_CD"),
        F.concat(F.lit("FACETS;"), F.trim(F.col("BLIV_ID"))).alias("PRI_KEY_STRING"),
        F.lit(0).alias("INVC_SK"),
        F.trim(F.col("BLIV_ID")).alias("BILL_INVC_ID"),
        F.col("BLDI_SEQ_NO").alias("BLDI_SEQ_NO"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("CSCS_id").alias("CLS_ID"),
        F.col("CSPI_ID").alias("CLS_PLN_ID"),
        F.col("GRGR_ID").alias("GRGR_ID"),
        F.when(F.length(F.trim(F.col("PMFA_ID")))==0, F.lit("NA")).otherwise(F.col("PMFA_ID")).alias("FEE_DSCNT_ID"),
        F.when(F.length(F.trim(F.col("PDPD_ID")))==0, F.lit("NA")).otherwise(F.col("PDPD_ID")).alias("PROD_ID"),
        F.col("PDBL_ID").alias("PROD_BILL_CMPNT_ID"),
        F.col("SGSG_ID").alias("SUBGRP_ID"),
        F.when(F.length(F.trim(F.col("BLDI_DISP_CD")))==0, F.lit("NA")).otherwise(F.col("BLDI_DISP_CD")).alias("INVC_DSCRTN_BILL_DISP_CD"),
        F.when(F.length(F.trim(F.col("BLDI_PREM_FEE_IND")))==0, F.lit("NA")).otherwise(F.col("BLDI_PREM_FEE_IND")).alias("INVC_DSCRTN_PRM_FEE_CD"),
        FORMAT_DATE(F.col("BLDI_UPDATE_DTM"), F.lit("SYBASE"), F.lit("TIMESTAMP"), F.lit("CCYY-MM-DD")).alias("DUE_DT"),
        F.col("BLDI_PREM_DEP").alias("DPNDT_PRM_AMT"),
        F.col("BLDI_FEE_DISC_AMT").alias("FEE_DSCNT_AMT"),
        F.col("BLDI_PREM_SB").alias("SUB_PRM_AMT"),
        F.when(F.col("MO_QTY")==F.lit("?"), F.lit("1"))
         .when(F.col("MO_QTY")==F.lit("0"), F.lit("1"))
         .when(F.col("MO_QTY").cast("int")<0, F.lit("1"))
         .when(F.col("MO_QTY").cast("int")>72, F.lit("1"))
         .otherwise(F.col("MO_QTY")).alias("DSCRTN_MO_QTY"),
        F.col("BLDI_DESC").alias("DSCRTN_DESC"),
        F.col("SH_DESC").alias("DSCRTN_SH_DESC"),
        F.col("SBSB_CK").alias("SBSB_CK"),
        F.col("INVC_DSCRTN_BEG_DT_SK").alias("INVC_DSCRTN_BEG_DT_SK"),
        F.col("INVC_DSCRTN_END_DT_SK").alias("INVC_DSCRTN_END_DT_SK"),
        F.when(F.length(F.trim(F.col("DSCRTN_PRSN_ID_TX")))==0, F.lit(" ")).otherwise(F.trim(F.col("DSCRTN_PRSN_ID_TX"))).alias("DSCRTN_PRSN_ID_TX"),
        F.when(F.isnull(F.col("DPNDT_CT")), F.lit(0))
         .when(F.length(F.col("DPNDT_CT"))==0, F.lit(0))
         .otherwise(F.col("DPNDT_CT")).alias("DPNDT_CT"),
        F.when(F.isnull(F.col("SUB_CT")), F.lit(0))
         .when(F.length(F.col("SUB_CT"))==0, F.lit(0))
         .otherwise(F.col("SUB_CT")).alias("SUB_CT"),
        F.col("svSelfBillLifeIn").alias("SELF_BILL_LIFE_IN")
    )
)

# Build the Snapshot link columns
df_snapshot = (
    df_business_snapshot
    .select(
        F.trim(F.col("BLIV_ID")).alias("BLIV_ID"),
        F.col("BLDI_SEQ_NO").alias("BLDI_SEQ_NO"),
        F.col("GRGR_ID").alias("GRGR_ID"),
        F.col("SGSG_ID").alias("SUBGRP_ID"),
        F.when(F.length(F.trim(F.col("PMFA_ID")))==0, F.lit("NA")).otherwise(F.col("PMFA_ID")).alias("FEE_DSCNT_ID"),
        F.col("CSCS_id").alias("CLS_ID"),
        F.col("CSPI_ID").alias("CLS_PLN_ID"),
        F.when(F.length(F.trim(F.col("PDPD_ID")))==0, F.lit("NA")).otherwise(F.col("PDPD_ID")).alias("PROD_ID"),
        F.col("INVC_DSCRTN_BEG_DT_SK").alias("INVC_DSCRTN_BEG_DT_SK"),
        F.col("INVC_DSCRTN_END_DT_SK").alias("INVC_DSCRTN_END_DT_SK"),
        F.col("BLDI_PREM_SB").alias("SUB_PRM_AMT"),
        F.when(F.col("MO_QTY")==F.lit("?"), F.lit("1"))
         .when(F.col("MO_QTY")==F.lit("0"), F.lit("1"))
         .when(F.col("MO_QTY").cast("int")<0, F.lit("1"))
         .when(F.col("MO_QTY").cast("int")>72, F.lit("1"))
         .otherwise(F.col("MO_QTY")).alias("DSCRTN_MO_QTY")
    )
)

# SnapshotReversalLogic
# Left join with df_hf_invc_rebills_rvrsl => compare with "Snapshot.BLIV_ID"
df_snapshot_alias = df_snapshot.alias("Snapshot")
df_hf_invc_rebills_rvrsl_alias = df_hf_invc_rebills_rvrsl.alias("rebill_lkup")

# We create a joined DF so we can apply the constraint "IsNull(rebill_lkup.BILL_INVC_ID) = @FALSE" or not.
df_snapshot_join = df_snapshot_alias.join(
    df_hf_invc_rebills_rvrsl_alias,
    (F.col("Snapshot.BLIV_ID") == F.col("rebill_lkup.BILL_INVC_ID")),
    "left"
)

# Stage variables
df_snapshot_vars = df_snapshot_join.withColumn(
    "svReversalInvcId",
    F.concat(F.trim(F.col("Snapshot.BLIV_ID")), F.lit("R"))
).withColumn(
    "svReversalSubPrmAmt",
    -1 * F.col("Snapshot.SUB_PRM_AMT")
)

# NonReversal link (IsNull(rebill_lkup.BILL_INVC_ID) = TRUE => that means no match => we keep these)
df_nonReversal = df_snapshot_vars.filter(F.isnull(F.col("rebill_lkup.BILL_INVC_ID"))).select(
    F.col("Snapshot.BLIV_ID").alias("BILL_INVC_ID"),
    F.col("Snapshot.BLDI_SEQ_NO").alias("BLDI_SEQ_NO"),
    F.col("Snapshot.GRGR_ID").alias("GRGR_ID"),
    F.col("Snapshot.SUBGRP_ID").alias("SUBGRP_ID"),
    F.col("Snapshot.FEE_DSCNT_ID").alias("FEE_DSCNT_ID"),
    F.col("Snapshot.CLS_ID").alias("CLS_ID"),
    F.col("Snapshot.CLS_PLN_ID").alias("CLS_PLN_ID"),
    F.col("Snapshot.PROD_ID").alias("PROD_ID"),
    F.col("Snapshot.INVC_DSCRTN_BEG_DT_SK").alias("INVC_DSCRTN_BEG_DT_SK"),
    F.col("Snapshot.INVC_DSCRTN_END_DT_SK").alias("INVC_DSCRTN_END_DT_SK"),
    F.col("Snapshot.SUB_PRM_AMT").alias("SUB_PRM_AMT"),
    F.col("Snapshot.DSCRTN_MO_QTY").alias("DSCRTN_MO_QTY")
)

# Reversal link (IsNull(rebill_lkup.BILL_INVC_ID) = FALSE => matched => constraint "IsNull(...) = @FALSE"
df_reversal = df_snapshot_vars.filter(~F.isnull(F.col("rebill_lkup.BILL_INVC_ID"))).select(
    F.col("svReversalInvcId").alias("BILL_INVC_ID"),
    F.col("Snapshot.BLDI_SEQ_NO").alias("BLDI_SEQ_NO"),
    F.col("Snapshot.GRGR_ID").alias("GRGR_ID"),
    F.col("Snapshot.SUBGRP_ID").alias("SUBGRP_ID"),
    F.col("Snapshot.FEE_DSCNT_ID").alias("FEE_DSCNT_ID"),
    F.col("Snapshot.CLS_ID").alias("CLS_ID"),
    F.col("Snapshot.CLS_PLN_ID").alias("CLS_PLN_ID"),
    F.col("Snapshot.PROD_ID").alias("PROD_ID"),
    F.col("Snapshot.INVC_DSCRTN_BEG_DT_SK").alias("INVC_DSCRTN_BEG_DT_SK"),
    F.col("Snapshot.INVC_DSCRTN_END_DT_SK").alias("INVC_DSCRTN_END_DT_SK"),
    F.col("svReversalSubPrmAmt").alias("SUB_PRM_AMT"),
    F.col("Snapshot.DSCRTN_MO_QTY").alias("DSCRTN_MO_QTY")
)

# Collector => union these two
df_collector = df_nonReversal.unionByName(df_reversal)

# Next stage: Collector => Transform => "Transform" output => "Snapshot_File"

# Named here as df_transform from the collector
df_transform = df_collector.select(
    F.col("BILL_INVC_ID"),
    F.col("BLDI_SEQ_NO"),
    F.col("GRGR_ID").alias("GRGR_ID"),
    F.col("SUBGRP_ID").alias("SUBGRP_ID"),
    F.col("FEE_DSCNT_ID"),
    F.col("CLS_ID"),
    F.col("CLS_PLN_ID"),
    F.col("PROD_ID"),
    F.col("INVC_DSCRTN_BEG_DT_SK"),
    F.col("INVC_DSCRTN_END_DT_SK"),
    F.col("SUB_PRM_AMT"),
    F.col("DSCRTN_MO_QTY")
)

# Next stage: We apply the next Transformer named "Transform", which has stage variables to get fkeys.
df_transform_vars = (
    df_transform
    .withColumn("svClsSk", GetFkeyCls(F.lit("FACETS"), F.lit(104), F.col("GRGR_ID"), F.col("CLS_ID"), F.lit("X")))
    .withColumn("svGrpSk", GetFkeyGrp(F.lit("FACETS"), F.lit(106), F.col("GRGR_ID"), F.lit("X")))
    .withColumn("svSubgrpSk", GetFkeySubgrp(F.lit("FACETS"), F.lit(108), F.col("GRGR_ID"), F.col("SUBGRP_ID"), F.lit("X")))
    .withColumn("svClsPlnSk", GetFkeyClsPln(F.lit("FACETS"), F.lit(112), F.col("CLS_PLN_ID"), F.lit("X")))
    .withColumn("svProdSk", GetFkeyProd(F.lit("FACETS"), F.lit(114), F.col("PROD_ID"), F.lit("X")))
    .withColumn("svInvcSk", GetFkeyInvc(F.lit("FACETS"), F.lit(116), F.col("BILL_INVC_ID"), F.lit("X")))
    .withColumn("svInvcBegDtSk", GetFkeyDate(F.lit("IDS"), F.lit(118), F.col("INVC_DSCRTN_BEG_DT_SK"), F.lit("X")))
    .withColumn("svInvcEndDtSk", GetFkeyDate(F.lit("IDS"), F.lit(120), F.col("INVC_DSCRTN_END_DT_SK"), F.lit("X")))
)

df_transform_out = df_transform_vars.select(
    F.col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
    F.col("BILL_INVC_ID").alias("BILL_INVC_ID"),
    F.col("BLDI_SEQ_NO").alias("SEQ_NO"),
    F.col("svClsSk").alias("CLS_SK"),
    F.col("svClsPlnSk").alias("CLS_PLN_SK"),
    F.col("svGrpSk").alias("GRP_SK"),
    F.col("svInvcSk").alias("INVC_SK"),
    F.col("svProdSk").alias("PROD_SK"),
    F.col("svSubgrpSk").alias("SUBGRP_SK"),
    F.col("svInvcBegDtSk").alias("INVC_DSCRTN_BEG_DT_SK"),
    F.col("svInvcEndDtSk").alias("INVC_DSCRTN_END_DT_SK"),
    F.col("SUB_PRM_AMT").alias("SUB_PRM_AMT"),
    F.col("DSCRTN_MO_QTY").alias("DSCRTN_MO_QTY")
)

# Write to Snapshot_File => B_INVC_DSCRTN.dat
# Before writing, apply final column order and rpad for any char/varchar columns (per instructions).
df_snapshot_file = df_transform_out.select(
    F.rpad(F.col("SRC_SYS_CD_SK").cast(StringType()), 10, " ").alias("SRC_SYS_CD_SK"),
    F.rpad(F.col("BILL_INVC_ID").cast(StringType()), 50, " ").alias("BILL_INVC_ID"),  # length not specified in the job, set something large
    F.rpad(F.col("SEQ_NO").cast(StringType()), 10, " ").alias("SEQ_NO"),
    F.rpad(F.col("CLS_SK").cast(StringType()), 10, " ").alias("CLS_SK"),
    F.rpad(F.col("CLS_PLN_SK").cast(StringType()), 10, " ").alias("CLS_PLN_SK"),
    F.rpad(F.col("GRP_SK").cast(StringType()), 10, " ").alias("GRP_SK"),
    F.rpad(F.col("INVC_SK").cast(StringType()), 10, " ").alias("INVC_SK"),
    F.rpad(F.col("PROD_SK").cast(StringType()), 10, " ").alias("PROD_SK"),
    F.rpad(F.col("SUBGRP_SK").cast(StringType()), 10, " ").alias("SUBGRP_SK"),
    F.rpad(F.col("INVC_DSCRTN_BEG_DT_SK").cast(StringType()), 10, " ").alias("INVC_DSCRTN_BEG_DT_SK"),
    F.rpad(F.col("INVC_DSCRTN_END_DT_SK").cast(StringType()), 10, " ").alias("INVC_DSCRTN_END_DT_SK"),
    F.col("SUB_PRM_AMT"),
    F.col("DSCRTN_MO_QTY")
)

write_files(
    df_snapshot_file,
    f"{adls_path}/load/B_INVC_DSCRTN.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# ReversaLogic -------------------------------------
# Input: ChkReversals + lookup with df_hf_invc_rebills

df_reversa_join = df_chkReversals.alias("ChkReversals").join(
    df_hf_invc_rebills.alias("rebill_lkup"),
    (F.col("ChkReversals.BILL_INVC_ID") == F.col("rebill_lkup.BILL_INVC_ID")),
    "left"
)

df_reversa_vars = df_reversa_join.withColumn(
    "svReversalString", F.concat(F.col("ChkReversals.PRI_KEY_STRING"), F.lit("R"))
).withColumn(
    "svBillInvoiceId", F.concat(F.col("ChkReversals.BILL_INVC_ID"), F.lit("R"))
).withColumn(
    "svReversalDpndtAmt", -1 * F.col("ChkReversals.DPNDT_PRM_AMT")
).withColumn(
    "svReversalSubAmt", -1 * F.col("ChkReversals.SUB_PRM_AMT")
).withColumn(
    "svReversalFeeAmt", -1 * F.col("ChkReversals.FEE_DSCNT_AMT")
)

df_nonReversal2 = df_reversa_vars.filter(F.isnull(F.col("rebill_lkup.BILL_INVC_ID"))).select(
    F.col("ChkReversals.JOB_EXCTN_RCRD_ERR_SK"),
    F.col("ChkReversals.INSRT_UPDT_CD"),
    F.col("ChkReversals.DISCARD_IN"),
    F.col("ChkReversals.PASS_THRU_IN"),
    F.col("ChkReversals.FIRST_RECYC_DT"),
    F.col("ChkReversals.ERR_CT"),
    F.col("ChkReversals.RECYCLE_CT"),
    F.col("ChkReversals.SRC_SYS_CD"),
    F.col("ChkReversals.PRI_KEY_STRING"),
    F.col("ChkReversals.INVC_SK"),
    F.col("ChkReversals.BILL_INVC_ID"),
    F.col("ChkReversals.BLDI_SEQ_NO"),
    F.col("ChkReversals.CRT_RUN_CYC_EXCTN_SK"),
    F.col("ChkReversals.LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("ChkReversals.CLS_ID"),
    F.col("ChkReversals.CLS_PLN_ID"),
    F.col("ChkReversals.GRGR_ID"),
    F.col("ChkReversals.FEE_DSCNT_ID"),
    F.col("ChkReversals.PROD_ID"),
    F.col("ChkReversals.PROD_BILL_CMPNT_ID"),
    F.col("ChkReversals.SUBGRP_ID"),
    F.col("ChkReversals.INVC_DSCRTN_BILL_DISP_CD"),
    F.col("ChkReversals.INVC_DSCRTN_PRM_FEE_CD"),
    F.col("ChkReversals.DUE_DT"),
    F.col("ChkReversals.DPNDT_PRM_AMT"),
    F.col("ChkReversals.FEE_DSCNT_AMT"),
    F.col("ChkReversals.SUB_PRM_AMT"),
    F.col("ChkReversals.DSCRTN_MO_QTY"),
    F.col("ChkReversals.DSCRTN_DESC"),
    F.col("ChkReversals.DSCRTN_SH_DESC"),
    F.col("ChkReversals.SBSB_CK"),
    F.col("ChkReversals.INVC_DSCRTN_BEG_DT_SK"),
    F.col("ChkReversals.INVC_DSCRTN_END_DT_SK"),
    F.col("ChkReversals.DSCRTN_PRSN_ID_TX"),
    F.col("ChkReversals.DPNDT_CT"),
    F.col("ChkReversals.SUB_CT"),
    F.col("ChkReversals.SELF_BILL_LIFE_IN")
)

df_reversal2 = df_reversa_vars.filter(F.col("rebill_lkup.BILL_INVC_ID").isNotNull()).select(
    F.col("ChkReversals.JOB_EXCTN_RCRD_ERR_SK"),
    F.col("ChkReversals.INSRT_UPDT_CD"),
    F.col("ChkReversals.DISCARD_IN"),
    F.col("ChkReversals.PASS_THRU_IN"),
    F.col("ChkReversals.FIRST_RECYC_DT"),
    F.col("ChkReversals.ERR_CT"),
    F.col("ChkReversals.RECYCLE_CT"),
    F.col("ChkReversals.SRC_SYS_CD"),
    F.col("svReversalString").alias("PRI_KEY_STRING"),
    F.col("ChkReversals.INVC_SK"),
    F.col("svBillInvoiceId").alias("BILL_INVC_ID"),
    F.col("ChkReversals.BLDI_SEQ_NO"),
    F.col("ChkReversals.CRT_RUN_CYC_EXCTN_SK"),
    F.col("ChkReversals.LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("ChkReversals.CLS_ID"),
    F.col("ChkReversals.CLS_PLN_ID"),
    F.col("ChkReversals.GRGR_ID"),
    F.col("ChkReversals.FEE_DSCNT_ID"),
    F.col("ChkReversals.PROD_ID"),
    F.col("ChkReversals.PROD_BILL_CMPNT_ID"),
    F.col("ChkReversals.SUBGRP_ID"),
    F.col("ChkReversals.INVC_DSCRTN_BILL_DISP_CD"),
    F.col("ChkReversals.INVC_DSCRTN_PRM_FEE_CD"),
    F.col("ChkReversals.DUE_DT"),
    F.col("svReversalDpndtAmt").alias("DPNDT_PRM_AMT"),
    F.col("svReversalFeeAmt").alias("FEE_DSCNT_AMT"),
    F.col("svReversalSubAmt").alias("SUB_PRM_AMT"),
    F.col("ChkReversals.DSCRTN_MO_QTY"),
    F.col("ChkReversals.DSCRTN_DESC"),
    F.col("ChkReversals.DSCRTN_SH_DESC"),
    F.col("ChkReversals.SBSB_CK"),
    F.col("ChkReversals.INVC_DSCRTN_BEG_DT_SK"),
    F.col("ChkReversals.INVC_DSCRTN_END_DT_SK"),
    F.col("ChkReversals.DSCRTN_PRSN_ID_TX"),
    F.col("ChkReversals.DPNDT_CT"),
    F.col("ChkReversals.SUB_CT"),
    F.col("ChkReversals.SELF_BILL_LIFE_IN")
)

df_collectRows = df_nonReversal2.unionByName(df_reversal2)

# Then from CollectRows => Key
df_key = df_collectRows.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD"),
    F.col("DISCARD_IN"),
    F.col("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("INVC_SK"),
    F.col("BILL_INVC_ID"),
    F.col("BLDI_SEQ_NO"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CLS_ID"),
    F.col("CLS_PLN_ID"),
    F.col("GRGR_ID"),
    F.col("FEE_DSCNT_ID"),
    F.col("PROD_ID"),
    F.col("PROD_BILL_CMPNT_ID"),
    F.col("SUBGRP_ID"),
    F.col("INVC_DSCRTN_BILL_DISP_CD"),
    F.col("INVC_DSCRTN_PRM_FEE_CD"),
    F.col("DUE_DT"),
    F.col("DPNDT_PRM_AMT"),
    F.col("FEE_DSCNT_AMT"),
    F.col("SUB_PRM_AMT"),
    F.col("DSCRTN_MO_QTY"),
    F.col("DSCRTN_DESC"),
    F.col("DSCRTN_SH_DESC"),
    F.col("SBSB_CK"),
    F.col("INVC_DSCRTN_BEG_DT_SK"),
    F.col("INVC_DSCRTN_END_DT_SK"),
    F.col("DSCRTN_PRSN_ID_TX"),
    F.col("DPNDT_CT"),
    F.col("SUB_CT"),
    F.col("SELF_BILL_LIFE_IN")
)

df_key_transform = df_collectRows.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("DISCARD_IN"),
    F.col("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("INVC_SK"),
    F.col("BILL_INVC_ID"),
    F.col("BLDI_SEQ_NO").alias("SEQ_NO"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CLS_ID"),
    F.col("CLS_PLN_ID"),
    F.col("GRGR_ID"),
    F.col("FEE_DSCNT_ID"),
    F.col("PROD_ID"),
    F.col("PROD_BILL_CMPNT_ID"),
    F.col("SUBGRP_ID"),
    F.col("INVC_DSCRTN_BILL_DISP_CD"),
    F.col("INVC_DSCRTN_PRM_FEE_CD"),
    F.col("DUE_DT"),
    F.col("DPNDT_PRM_AMT"),
    F.col("FEE_DSCNT_AMT"),
    F.col("SUB_PRM_AMT"),
    F.col("DSCRTN_MO_QTY"),
    F.col("DSCRTN_DESC"),
    F.col("DSCRTN_SH_DESC"),
    F.col("SBSB_CK"),
    F.col("INVC_DSCRTN_BEG_DT_SK"),
    F.col("INVC_DSCRTN_END_DT_SK"),
    F.col("DSCRTN_PRSN_ID_TX"),
    F.col("DPNDT_CT"),
    F.col("SUB_CT"),
    F.col("SELF_BILL_LIFE_IN")
)

# Next stage: Key => IncmInvcDscrtnPK (Shared Container)
params = {
    "CurrRunCycle": CurrRunCycle,
    "SrcSysCd": SrcSysCd,
    "RunID": RunId,
    "CurrentDate": CurrDate,
    "IDSOwner": IDSOwner
}

# We call the shared container, which has two inputs and one output.
# The job design shows input pins: AllCol => C180P1, Transform => C180P2 => output => C180P3
# So we pass (df_key, df_key_transform), in that order, plus params

df_idsInvcDscrtn = IncmInvcDscrtnPK(df_key, df_key_transform, params)

# Final stage: IdsInvcDscrtn => writing to #TmpOutFile# at directory "key"
df_idsInvcDscrtn_final = df_idsInvcDscrtn.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("INVC_SK"),
    F.col("BILL_INVC_ID"),
    F.col("BLDI_SEQ_NO"),
    F.rpad(F.col("CLS_ID"), 4, " ").alias("CLS_ID"),
    F.rpad(F.col("CLS_PLN_ID"), 8, " ").alias("CLS_PLN_ID"),
    F.rpad(F.col("GRGR_ID"), 8, " ").alias("GRGR_ID"),
    F.rpad(F.col("FEE_DSCNT_ID"), 2, " ").alias("FEE_DSCNT_ID"),
    F.rpad(F.col("PROD_ID"), 8, " ").alias("PROD_ID"),
    F.rpad(F.col("PROD_BILL_CMPNT_ID"), 4, " ").alias("PROD_BILL_CMPNT_ID"),
    F.rpad(F.col("SUBGRP_ID"), 4, " ").alias("SUBGRP_ID"),
    F.rpad(F.col("INVC_DSCRTN_BILL_DISP_CD"), 1, " ").alias("INVC_DSCRTN_BILL_DISP_CD"),
    F.rpad(F.col("INVC_DSCRTN_PRM_FEE_CD"), 1, " ").alias("INVC_DSCRTN_PRM_FEE_CD"),
    F.rpad(F.col("DUE_DT"), 10, " ").alias("DUE_DT"),
    F.rpad(F.col("INVC_DSCRTN_BEG_DT_SK"), 10, " ").alias("INVC_DSCRTN_BEG_DT_SK"),
    F.rpad(F.col("INVC_DSCRTN_END_DT_SK"), 10, " ").alias("INVC_DSCRTN_END_DT_SK"),
    F.col("DPNDT_PRM_AMT"),
    F.col("FEE_DSCNT_AMT"),
    F.col("SUB_PRM_AMT"),
    F.col("DSCRTN_MO_QTY"),
    F.rpad(F.col("DSCRTN_DESC"), 70, " ").alias("DSCRTN_DESC"),
    F.col("DSCRTN_PRSN_ID_TX"),
    F.rpad(F.col("DSCRTN_SH_DESC"), 80, " ").alias("DSCRTN_SH_DESC"),
    F.col("SBSB_CK"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("DPNDT_CT"),
    F.col("SUB_CT"),
    F.rpad(F.col("SELF_BILL_LIFE_IN"), 1, " ").alias("SELF_BILL_LIFE_IN")
)

write_files(
    df_idsInvcDscrtn_final,
    f"{adls_path}/key/{TmpOutFile}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)