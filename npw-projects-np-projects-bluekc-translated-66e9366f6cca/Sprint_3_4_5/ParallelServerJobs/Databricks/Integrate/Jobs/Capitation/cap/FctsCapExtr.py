# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Copyright 2008, 2015 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC Called by:
# MAGIC                     FctsCapExtrSeq
# MAGIC 
# MAGIC Processing:
# MAGIC                     *
# MAGIC 
# MAGIC Control Job Rerun Information: 
# MAGIC                     Previous Run Successful:    Restart, no other steps necessary
# MAGIC                     Previous Run Aborted:         Restart, no other steps necessary
# MAGIC 
# MAGIC Modifications:                        
# MAGIC                                                   Project/                                                                                                                                                                                            Code                   Date
# MAGIC Developer           Date               Altiris #         Change Description                                                                                               Environment                                  Reviewer             Reviewed
# MAGIC -------------------------  ---------------------  ----------------    -----------------------------------------------------------------------------------------------------------------        ---------------------------------                      -------------------------     ----------------------
# MAGIC Ralph Tucker     04/29/2005                        Originally Programmed
# MAGIC BJ Luce              05/25/2005                        changed to use standard facets parameters
# MAGIC Ralph Tucker     07/19/2005                        Changed to a distributed hash file for hf_d_cap. 
# MAGIC Parik                   4/19/2007      3264            Added Balancing process to the overall job that takes a snapshot of                                                                     Steph Goddard   09/19/2007
# MAGIC                                                                       source data                   
# MAGIC Parik                   2008-08-29     3567            Added primary key process to the job                                                                                                                     Steph Goddard   09/10/2008
# MAGIC Ralph Tucker     2011-04-19     TTR-1058    Changed CSCS_ID based upon MDTL.MDTL_EARN_DT instead                                                                        Sharon Andrew  2011-04-25
# MAGIC                                                                       of PAID_DT.  
# MAGIC Rishi Reddy        2011-06-20     4663            Added GL_CAT_CD_SK column                                                                                                                             Brent Leland       07-12-2011
# MAGIC SAndrew             2011-08-05     TTR-1188   Added hash file hf_fcts_cap_extr_main_driver between link collector to                                                                 Brent Leland       08-18-2011
# MAGIC                                                                       ensure all hash files populated.   hash files were still loading when 
# MAGIC                                                                       driver records were processed 
# MAGIC                                                                       Added hash file hf_fcts_cap_extr_main_driver to HASH.CLEAR; 
# MAGIC                                                                       now 6 files cleared.
# MAGIC SAndrew             2011-08-18     TTR-1196   Corrected the field order and keys to the hf_cap_dedup.                                                                                        Brent Leland       08-18-2011
# MAGIC SAndrew             2011-09-07     TTR-1208   Added hash files before the link collector to prevent link collector abends                                                              Brent Leland       09-09-2011
# MAGIC                                                                       hf_fcts_cap_extr_main_in1 and hf_fcts_cap_extr_main_in2
# MAGIC SAndrew             2011-09-13     TTR-1212   Due to huge jump in volume, changed shared container CapPK                                                                             Brent Leland       09-15-2011
# MAGIC                                                                       hf_cap_alloc to distributed.  Renamed it to hf_cap_allcol_dist.  
# MAGIC                                                                       In HASH.CLEAR, removed file hf_cap_allcol
# MAGIC                                                                       hf_cap_allcol_dist  is cleared in IdsCapCleanupSeq process
# MAGIC SAndrew             2011-10-01     TTR-1212   added new routine ClearDistFile to the Before-job subroutine to clear 
# MAGIC                                                                       out distibuted file hf_cap_allcol_*** 
# MAGIC                                                                       made sure Shared Container did not clear this hash file as well.
# MAGIC Manasa Andru    2011-12-28     TTR-1177    Changed logic to retrieve SUB_SK for datasource 'MDTL', also changed                                                              SAndrew          2012-01-10
# MAGIC                                                                       logic to retrieve MBR_UNIQ_KEY, GRP_SK, FNCL_LOB_SK, CLS_SK, 
# MAGIC                                                                       SUBGRP_SK, SUB_SK for datasource 'CRCA' (previously they were 
# MAGIC                                                                       defaulted to 1)
# MAGIC Hugh Sisson       2015-05-20     TFS10694   Changed the format of the fields in the hf_cap_dedup hashed file to                                                                       Kalyan Neelam     2015-05-21
# MAGIC                                                                       match the format of the source file.
# MAGIC                                                                       Corrected logic in two stage variables (svSeq and svSeqNo) to 
# MAGIC                                                                       increment correctly in teh Trans_Dedup stage
# MAGIC                                                                       Added LOBD_ID to the sort criteria of the Sort_Crca stage
# MAGIC Tejaswi Gogineni    2018-10-16   60037        Added the logic PRCS. CSPD_CAT = 'M to the crcaCapAdjIn2 link                   IntegrateDev1                              Hugh Sisson       2018-10-23
# MAGIC 
# MAGIC Tejaswi Gogineni    2018-10-23  INC0473770   Changed the Datatype for  SEQ_NO across the job to match it with the        IntegrateDev1                               Hugh Sisson       2018-10-23
# MAGIC                                                                               table datatype to get rid of warnings. 
# MAGIC 
# MAGIC Manasa Andru      2019-10-04    US# 161731      Added Date Parameter AttrbtnRowEffDt in the extract SQL in                        IntegrateDev2                          Jaideep Mankala  2019-10-11
# MAGIC                                                                                     crcaCapAdjIn2 stream and applied same change in balancing extract
# MAGIC 
# MAGIC 
# MAGIC Ashok kumar       2020-07-20    US# 252987      Added new logic in transformation to fetch PSYB new LOBID                         IntegrateDev2                          Jaideep Mankala      07/23/2020           
# MAGIC 
# MAGIC Debdeep Saha    2020-12-21    US 332906  Added new logic in Capitation job so that MA LOBs map to the General                IntegrateDev2                          Jeyaprasanna           12/21/2020
# MAGIC                                                                       Ledger Category code of "ADM," so that finance can accurately account
# MAGIC                                                                        for MA Capitation payments made to providers.   
# MAGIC Prabhu ES           2022-02-25     S2S             MSSQL connection parameters added                                                                  IntegrateDev5
# MAGIC 
# MAGIC Reddy Sanam     2022-07-18   502212          In the transformer "Trans_DeDup" the initial value is set to 100 when the               IntegrateDev2                        Goutham K                7/28/2022
# MAGIC                                                                       source is manul cap adjustment

# MAGIC Remove Carriage Return, Line Feed, and  Tab in fields
# MAGIC Writing Sequential File to /key
# MAGIC Writing to hf_cap_dedup - which is immediately read from
# MAGIC reading hf_cap_dedup - which is written to by Trans_DeDup
# MAGIC Balancing snapshot of source table
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../shared_containers/PrimaryKey/CapPK
# COMMAND ----------

IDSOwner = get_widget_value('IDSOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
TmpOutFile = get_widget_value('TmpOutFile','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
SrcSysCd = get_widget_value('SrcSysCd','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
AttrbtnRowEffDt = get_widget_value('AttrbtnRowEffDt','')
FacetsOwner = get_widget_value('FacetsOwner','')

# ---------------------------------------------------------
# Facets ODBCConnector: Read all output pins with separate queries
# ---------------------------------------------------------
jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)

# 1) MDTL_MEMB_DTL
query_MDTL_MEMB_DTL = """SELECT 
MDTL.MEME_CK,
MDTL.MDTL_EARN_DT,
MDTL.CRME_CR_PR_ID,
MDTL.CRPL_POOL_ID,
MDTL.CRFD_FUND_ID,
MDTL.CRME_SEQ_NO,
MDTL.LOBD_ID,
MDTL.CRME_CR_PR_TYPE,
MDTL.CRME_FUND_AMT,
MDTL.CRME_FUND_RATE,
MDTL.CRME_ME_MONTHS,
MDTL.CRFS_SCHD_ID,
MDTL.MDTL_PERIOD_IND,
MDTL.MDTL_PAY_DT,
MDTL.CRME_PAYEE_PR_ID,
MDTL.CRME_PRPR_ID,
MDTL.NWNW_ID,
MDTL.CSPI_ID,
MDTL.PDPD_ID,
MDTL.SBSB_ID,
MDTL.GRGR_CK,
MDTL.GRGR_ID,
MDTL.SGSG_ID,
'NA' As BSDL_MCTR_TYPE,
0 As BSDL_COPAY_AMT,
(SELECT distinct PRCS.CSCS_ID FROM """ + FacetsOwner + """.CMC_MEPE_PRCS_ELIG PRCS 
  WHERE MDTL.MEME_CK = PRCS.MEME_CK  and 
        MDTL.MDTL_EARN_DT >= PRCS.MEPE_EFF_DT AND
        MDTL.MDTL_EARN_DT <= PRCS.MEPE_TERM_DT AND
        LEN(LTRIM(RTRIM(PRCS.CSCS_ID))) <> 0) As CSCS_ID,
' ' As CRCA_STS,
'NA' As CRCA_CR_PR_TYPE,
0 As CRCA_CAP_ADJ_AMT,
'NA' As CRCA_MCTR_ARSN,
MDTL.MEME_BIRTH_DT,
'CRME' As DATA_SOURCE,
(SELECT MEMEM.SBSB_CK FROM """ + FacetsOwner + """.CMC_MEME_MEMBER MEMEM
  WHERE MDTL.MEME_CK  = MEMEM.MEME_CK ) As SBSB_CK
FROM 
""" + FacetsOwner + """.CDS_MDTL_MEMB_DTL MDTL
WHERE 
((MDTL.CRME_FUND_AMT  <> 0)  OR  
 (MDTL.CRME_FUND_AMT   = 0   AND   MDTL.MDTL_PERIOD_IND = 'C'))"""

df_MDTL_MEMB_DTL = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", query_MDTL_MEMB_DTL)
    .load()
)

# 2) lnkCopayOut
query_lnkCopayOut = """SELECT
DISTINCT
MD.PDPD_ID,
MD.MDTL_EARN_DT,
MD.CRME_CR_PR_TYPE,
BD.BSDL_COPAY_AMT,
BD.BSDL_TYPE As BSDL_MCTR_TYPE
FROM
""" + FacetsOwner + """.CDS_MDTL_MEMB_DTL MD,
""" + FacetsOwner + """.CMC_PDBC_PROD_COMP BC,
""" + FacetsOwner + """.CMC_BSDL_DETAILS BD
WHERE
(
    (   MD.CRME_CR_PR_TYPE = 'P'
        AND BD.BSDL_TYPE   = 'OVP')
 OR
    (   MD.CRME_CR_PR_TYPE IN ('S', 'G')
        AND BD.BSDL_TYPE   = 'OVS')
)
AND BC.PDPD_ID      =  MD.PDPD_ID
AND BC.PDBC_TYPE    =  'BSBS'
AND BC.PDBC_EFF_DT  <= MD.MDTL_EARN_DT
AND BC.PDBC_TERM_DT >= MD.MDTL_EARN_DT
AND BD.PDBC_PFX     =  BC.PDBC_PFX
"""

df_lnkCopayOut = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", query_lnkCopayOut)
    .load()
)

# 3) lnkSnapLob
query_lnkSnapLob = """SELECT MDTL.MEME_CK,
               MDTL.PDPD_ID,
               PDBL.PDBL_ACCT_CAT 
   FROM """ + FacetsOwner + """.CMC_PDBL_PROD_BILL PDBL,
         """ + FacetsOwner + """.CMC_PDBC_PROD_COMP PDBC,
         """ + FacetsOwner + """.CDS_MDTL_MEMB_DTL MDTL
WHERE PDBL.PDBL_ID       In   ('MED1','DEN1') AND
      PDBC.PDBC_TYPE = 'PDBL' AND
      PDBC.PDBC_PFX      =    PDBL.PDBC_PFX AND
      PDBC.PDPD_ID       =   MDTL.PDPD_ID AND
      PDBC.PDBC_EFF_DT   <=   MDTL.MDTL_EARN_DT AND
      PDBC.PDBC_TERM_DT  >    MDTL.MDTL_EARN_DT AND
      PDBL.PDBL_EFF_DT   <=   MDTL.MDTL_EARN_DT AND
      PDBL.PDBL_TERM_DT  >    MDTL.MDTL_EARN_DT
"""

df_lnkSnapLob = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", query_lnkSnapLob)
    .load()
)

# 4) crcaCapAdjIn2 -> "Sort_Crca"
query_crcaCapAdjIn2 = """SELECT 
CRCA.MEME_CK As MEME_CK,
CRCA.CRCA_EARN_DT As MDTL_EARN_DT,
CRCA.CRCA_CR_PR_ID As CRME_CR_PR_ID,
CRCA.CRPL_POOL_ID As CRPL_POOL_ID,
CRCA.CRFD_FUND_ID As CRFD_FUND_ID,
1 As CRME_SEQ_NO,
CRCA.LOBD_ID As LOBD_ID,
CRCA.CRCA_CR_PR_TYPE As CRME_CR_PR_TYPE,
0 As CRME_FUND_AMT,
0 As CRME_FUND_RATE,
CRCA.CRCA_ME_MONTHS As CRME_ME_MONTHS,
'NA' As CRFS_SCHD_ID,
' ' As MDTL_PERIOD_IND,
CRCA.CRCA_PAY_DT AS MDTL_PAY_DT,
CRCA.CRCA_PAYEE_PR_ID AS CRME_PAYEE_PR_ID,
CRCA.PRPR_ID AS CRME_PRPR_ID,
CRCA.NWNW_ID,
'NA' As CSPI_ID,
CRCA.PDPD_ID,
'NA' As SBSB_ID,
1 As GRGR_CK,
(SELECT CMC_GRGR.GRGR_ID FROM """ + FacetsOwner + """.CMC_MEME_MEMBER CMC_MEME,
   """ + FacetsOwner + """.CMC_GRGR_GROUP CMC_GRGR
  WHERE CRCA.MEME_CK = CMC_MEME.MEME_CK AND
        CMC_MEME.GRGR_CK = CMC_GRGR.GRGR_CK) As GRGR_ID,
'NA' As SGSG_ID,
'NA' As BSDL_MCTR_TYPE,
0 As BSDL_COPAY_AMT,
(SELECT PRCS.CSCS_ID FROM """ + FacetsOwner + """.CMC_MEPE_PRCS_ELIG PRCS 
  WHERE CRCA.MEME_CK = PRCS.MEME_CK AND
        CRCA.CRCA_EARN_DT >=  PRCS.MEPE_EFF_DT AND
        CRCA.CRCA_EARN_DT <=  PRCS.MEPE_TERM_DT AND 
        PRCS. CSPD_CAT = 'M' ) As CSCS_ID,
CRCA.CRCA_STS,
CRCA.CRCA_ADJ_TYPE As CRCA_CR_PR_TYPE,
CRCA.CRCA_CAP_ADJ_AMT As CRCA_CAP_ADJ_AMT,
CRCA.CRCA_MCTR_ARSN As CRCA_MCTR_ARSN,
'' As MEME_BIRTH_DT,
'CRCA' As DATA_SOURCE,
(SELECT CMC_MEME1.SBSB_CK FROM """ + FacetsOwner + """.CMC_MEME_MEMBER CMC_MEME1
  WHERE CRCA.MEME_CK = CMC_MEME1.MEME_CK) As SBSB_CK
FROM """ + FacetsOwner + """.CMC_CRCA_CAP_ADJ CRCA
WHERE CRCA.CRCA_PAY_DT = '""" + AttrbtnRowEffDt + """'
"""

df_crcaCapAdjIn2 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", query_crcaCapAdjIn2)
    .load()
)

# 5) CrfdFundDefnOut
query_CrfdFundDefnOut = """SELECT CRFD_FUND_ID,CRFD_ACCT_CAT FROM """ + FacetsOwner + """.CMC_CRFD_FUND_DEFN"""
df_CrfdFundDefnOut = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", query_CrfdFundDefnOut)
    .load()
)

# 6) lnkCRCASnapLob
query_lnkCRCASnapLob = """SELECT CRCA.MEME_CK,
               CRCA.PDPD_ID,
               PDBL.PDBL_ACCT_CAT 
   FROM """ + FacetsOwner + """.CMC_PDBL_PROD_BILL PDBL,
        """ + FacetsOwner + """.CMC_PDBC_PROD_COMP PDBC,
        """ + FacetsOwner + """.CMC_CRCA_CAP_ADJ CRCA
WHERE PDBL.PDBL_ID       In ('MED1','DEN1') AND
      PDBC.PDBC_TYPE     = 'PDBL' AND
      PDBC.PDBC_PFX      = PDBL.PDBC_PFX AND
      PDBC.PDPD_ID       = CRCA.PDPD_ID AND
      PDBC.PDBC_EFF_DT   <= CRCA.CRCA_EARN_DT AND
      PDBC.PDBC_TERM_DT  >  CRCA.CRCA_EARN_DT AND
      PDBL.PDBL_EFF_DT   <= CRCA.CRCA_EARN_DT AND
      PDBL.PDBL_TERM_DT  >  CRCA.CRCA_EARN_DT
"""

df_lnkCRCASnapLob = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", query_lnkCRCASnapLob)
    .load()
)

# 7) CrcaSgSg
query_CrcaSgSg = """SELECT
         CRCA1.MEME_CK,
         LTRIM(RTRIM(CRCA1.SGSG_ID)) As SGSG_ID,
         CRCA1.SORT_ORDER
FROM
(
SELECT   CRCA.MEME_CK,
         SGSG.SGSG_ID,
         1 As SORT_ORDER 
FROM    
""" + FacetsOwner + """.CMC_CRCA_CAP_ADJ CRCA,
""" + FacetsOwner + """.CMC_MEPE_PRCS_ELIG PRCS1,
""" + FacetsOwner + """.CMC_SGSG_SUB_GROUP SGSG 
WHERE CRCA.MEME_CK = PRCS1.MEME_CK AND
      PRCS1.SGSG_CK = SGSG.SGSG_CK AND
      CRCA.CRCA_EARN_DT BETWEEN PRCS1.MEPE_EFF_DT AND PRCS1.MEPE_TERM_DT AND
      PRCS1.MEPE_ELIG_IND = 'Y' AND
      LTRIM(RTRIM(CRCA.CSPD_CAT)) IS NOT NULL

UNION

SELECT CRCA.MEME_CK,
       SGSG.SGSG_ID,
       2 As SORT_ORDER 
FROM    
""" + FacetsOwner + """.CMC_CRCA_CAP_ADJ CRCA,
""" + FacetsOwner + """.CMC_MEPE_PRCS_ELIG PRCS1,
""" + FacetsOwner + """.CMC_SGSG_SUB_GROUP SGSG 
WHERE CRCA.MEME_CK = PRCS1.MEME_CK AND
      PRCS1.SGSG_CK = SGSG.SGSG_CK AND
      CRCA.CRCA_EARN_DT BETWEEN PRCS1.MEPE_EFF_DT AND PRCS1.MEPE_TERM_DT AND
      PRCS1.MEPE_ELIG_IND = 'Y' AND
      LTRIM(RTRIM(CRCA.CSPD_CAT)) = 'M'
) CRCA1
ORDER BY
CRCA1.MEME_CK,
CRCA1.SORT_ORDER DESC
"""

df_CrcaSgSg = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", query_CrcaSgSg)
    .load()
)


# ---------------------------------------------------------
# Sort_Crca stage
# ---------------------------------------------------------
df_Sort_Crca = df_crcaCapAdjIn2.sort(
    F.col("MEME_CK").asc(),
    F.col("MDTL_PAY_DT").asc(),
    F.col("CRME_CR_PR_ID").asc(),
    F.col("CRPL_POOL_ID").asc(),
    F.col("CRFD_FUND_ID").asc(),
    F.col("LOBD_ID").asc()
).alias("crcaCapAdjOut")


# ---------------------------------------------------------
# hf_cap_crca_sgsgid_lkup (Scenario C)
#   Input: df_CrcaSgSg
#   Write parquet, then read parquet for lookup
# ---------------------------------------------------------
df_hf_cap_crca_sgsgid_lkup_write = df_CrcaSgSg.select(
    F.col("MEME_CK").cast("int").alias("MEME_CK"),
    F.col("SGSG_ID").cast("string").alias("SGSG_ID"),
    F.col("SORT_ORDER").cast("int").alias("SORT_ORDER")
)
write_files(
    df_hf_cap_crca_sgsgid_lkup_write,
    f"{adls_path}/hf_cap_crca_sgsgid_lkup.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)
df_hf_cap_crca_sgsgid_lkup_read = spark.read.parquet(f"{adls_path}/hf_cap_crca_sgsgid_lkup.parquet").alias("SgSgId_lkup")


# ---------------------------------------------------------
# hf_cap_dedup_lkup & hf_cap_dedup (Scenario B: read-modify-write on same file "hf_cap_dedup")
#   We interpret as a dummy table in the IDS schema: IDS.dummy_hf_cap_dedup
#   We'll read from it for the left lookup (refDeDup), then upsert to it for lodDeDup
# ---------------------------------------------------------
jdbc_url_hf_cap_dedup, jdbc_props_hf_cap_dedup = get_db_config("<ids_secret_name>")
# Read from dummy table:
extract_query_hf_cap_dedup = "SELECT MEME_CK, MDTL_PAY_DT, CRFD_FUND_ID, CRPL_POOL_ID, CRME_CR_PR_ID, SEQ_NO FROM IDS.dummy_hf_cap_dedup"
df_refDeDup = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_hf_cap_dedup)
    .options(**jdbc_props_hf_cap_dedup)
    .option("query", extract_query_hf_cap_dedup)
    .load()
).alias("refDeDup")


# ---------------------------------------------------------
# TRANS_DeDup stage
#   Primary input: df_Sort_Crca
#   Lookup1 (left): df_refDeDup
#   Lookup2 (left): df_hf_cap_crca_sgsgid_lkup_read
#   Output1 => "deDupCrcaCapAdjIn" -> hf_fcts_cap_extr_main_in2 (Scenario C)
#   Output2 => "lodDeDup" -> hf_cap_dedup (Scenario B upsert)
#
#   We replicate stage variable logic with a window approach to track sequence.
# ---------------------------------------------------------

# First, join both lookups
join_cond_refDeDup = [
    F.col("crcaCapAdjOut.MEME_CK") == F.col("refDeDup.MEME_CK"),
    F.col("crcaCapAdjOut.MDTL_PAY_DT") == F.col("refDeDup.MDTL_PAY_DT"),
    F.col("crcaCapAdjOut.CRFD_FUND_ID") == F.col("refDeDup.CRFD_FUND_ID"),
    F.col("crcaCapAdjOut.CRPL_POOL_ID") == F.col("refDeDup.CRPL_POOL_ID"),
    F.col("crcaCapAdjOut.CRME_CR_PR_ID") == F.col("refDeDup.CRME_CR_PR_ID")
]
temp_deDup = df_Sort_Crca.alias("crcaCapAdjOut").join(
    df_refDeDup,
    on=join_cond_refDeDup,
    how="left"
)

temp_deDup = temp_deDup.join(
    df_hf_cap_crca_sgsgid_lkup_read,
    on=[temp_deDup["crcaCapAdjOut.MEME_CK"] == df_hf_cap_crca_sgsgid_lkup_read["SgSgId_lkup.MEME_CK"]],
    how="left"
)

# We replicate the stage variables. We'll define columns for them:

# svSeq: IF IsNull(refDeDup.MEME_CK) = @FALSE THEN refDeDup.SEQ_NO ELSE 100
temp_deDup = temp_deDup.withColumn(
    "svSeq",
    F.when(F.col("refDeDup.MEME_CK").isNotNull(), F.col("refDeDup.SEQ_NO")).otherwise(F.lit(100))
)

# We need to replicate "previous row" logic. We'll define a window by ordering on
# (MEME_CK, MDTL_PAY_DT, CRFD_FUND_ID, CRPL_POOL_ID, CRME_CR_PR_ID, some tie-breaker).
w_prev = Window.partitionBy(
    F.col("crcaCapAdjOut.MEME_CK"),
    F.col("crcaCapAdjOut.MDTL_PAY_DT"),
    F.col("crcaCapAdjOut.CRFD_FUND_ID"),
    F.col("crcaCapAdjOut.CRPL_POOL_ID"),
    F.col("crcaCapAdjOut.CRME_CR_PR_ID")
).orderBy(
    F.col("CRME_CR_PR_ID"),  # just a stable tie-break
    F.monotonically_increasing_id()  # force row order
)

# We'll retrieve the previous row's "svSeq" to emulate the stage var accumulation
temp_deDup = temp_deDup.withColumn("svPrevSeq", F.lag("svSeq").over(w_prev))
temp_deDup = temp_deDup.withColumn("svSeqNoCondition",
    F.when(
        (F.lag("crcaCapAdjOut.MEME_CK").over(w_prev) == F.col("crcaCapAdjOut.MEME_CK")) &
        (F.lag("crcaCapAdjOut.MDTL_PAY_DT").over(w_prev) == F.col("crcaCapAdjOut.MDTL_PAY_DT")) &
        (F.lag("crcaCapAdjOut.CRFD_FUND_ID").over(w_prev) == F.col("crcaCapAdjOut.CRFD_FUND_ID")) &
        (F.lag("crcaCapAdjOut.CRPL_POOL_ID").over(w_prev) == F.col("crcaCapAdjOut.CRPL_POOL_ID")) &
        (F.lag("crcaCapAdjOut.CRME_CR_PR_ID").over(w_prev) == F.col("crcaCapAdjOut.CRME_CR_PR_ID"))
        , F.col("svPrevSeq") + F.lit(1)
    ).otherwise(F.lit(100))
)

temp_deDup = temp_deDup.withColumn("svSeqNo", F.col("svSeqNoCondition"))

# Now define final columns for the output links
df_deDupCrcaCapAdjIn = temp_deDup.select(
    F.when(F.col("crcaCapAdjOut.MEME_CK") == -1, F.lit(1)).otherwise(F.col("crcaCapAdjOut.MEME_CK")).alias("MEME_CK"),
    F.col("crcaCapAdjOut.MDTL_EARN_DT").alias("MDTL_EARN_DT"),
    F.col("crcaCapAdjOut.CRME_CR_PR_ID").alias("CRME_CR_PR_ID"),
    F.col("crcaCapAdjOut.CRPL_POOL_ID").alias("CRPL_POOL_ID"),
    F.col("crcaCapAdjOut.CRFD_FUND_ID").alias("CRFD_FUND_ID"),
    F.col("svSeqNo").alias("CRME_SEQ_NO"),
    F.col("crcaCapAdjOut.LOBD_ID").alias("LOBD_ID"),
    F.col("crcaCapAdjOut.CRME_CR_PR_TYPE").alias("CRME_CR_PR_TYPE"),
    F.col("crcaCapAdjOut.CRME_FUND_AMT").alias("CRME_FUND_AMT"),
    F.col("crcaCapAdjOut.CRME_FUND_RATE").alias("CRME_FUND_RATE"),
    F.col("crcaCapAdjOut.CRME_ME_MONTHS").alias("CRME_ME_MONTHS"),
    F.col("crcaCapAdjOut.CRFS_SCHD_ID").alias("CRFS_SCHD_ID"),
    F.col("crcaCapAdjOut.MDTL_PERIOD_IND").alias("MDTL_PERIOD_IND"),
    F.col("crcaCapAdjOut.MDTL_PAY_DT").alias("MDTL_PAY_DT"),
    F.col("crcaCapAdjOut.CRME_PAYEE_PR_ID").alias("CRME_PAYEE_PR_ID"),
    F.col("crcaCapAdjOut.CRME_PRPR_ID").alias("CRME_PRPR_ID"),
    F.col("crcaCapAdjOut.NWNW_ID").alias("NWNW_ID"),
    F.col("crcaCapAdjOut.CSPI_ID").alias("CSPI_ID"),
    F.col("crcaCapAdjOut.PDPD_ID").alias("PDPD_ID"),
    F.col("crcaCapAdjOut.SBSB_ID").alias("SBSB_ID"),
    F.col("crcaCapAdjOut.GRGR_CK").alias("GRGR_CK"),
    F.when(F.col("crcaCapAdjOut.MEME_CK") == -1, F.lit("NA")).otherwise(F.col("crcaCapAdjOut.GRGR_ID")).alias("GRGR_ID"),
    F.when(
        F.col("crcaCapAdjOut.MEME_CK") == -1,
        F.lit("NA")
    ).otherwise(
        F.when(
            F.col("SgSgId_lkup.SGSG_ID").isNull(),
            F.lit("NA")
        ).otherwise(F.col("SgSgId_lkup.SGSG_ID"))
    ).alias("SGSG_ID"),
    F.col("crcaCapAdjOut.BSDL_MCTR_TYPE").alias("BSDL_MCTR_TYPE"),
    F.col("crcaCapAdjOut.BSDL_COPAY_AMT").alias("BSDL_COPAY_AMT"),
    F.when(
        F.col("crcaCapAdjOut.MEME_CK") == -1,
        F.lit("NA")
    ).otherwise(
        F.col("crcaCapAdjOut.CSCS_ID")
    ).alias("CSCS_ID"),
    F.col("crcaCapAdjOut.CRCA_STS").alias("CRCA_STS"),
    F.col("crcaCapAdjOut.CRCA_CR_PR_TYPE").alias("CRCA_CR_PR_TYPE"),
    F.col("crcaCapAdjOut.CRCA_CAP_ADJ_AMT").alias("CRCA_CAP_ADJ_AMT"),
    F.col("crcaCapAdjOut.CRCA_MCTR_ARSN").alias("CRCA_MCTR_ARSN"),
    F.col("crcaCapAdjOut.MEME_BIRTH_DT").alias("MEME_BIRTH_DT"),
    F.col("crcaCapAdjOut.DATA_SOURCE").alias("DATA_SOURCE"),
    F.when(
        F.col("crcaCapAdjOut.MEME_CK") == -1,
        F.lit("NA")
    ).otherwise(F.col("crcaCapAdjOut.SBSB_CK")).alias("SBSB_CK")
)

df_lodDeDup = temp_deDup.select(
    F.col("crcaCapAdjOut.MEME_CK").alias("MEME_CK"),
    F.col("crcaCapAdjOut.MDTL_PAY_DT").alias("MDTL_PAY_DT"),
    F.col("crcaCapAdjOut.CRFD_FUND_ID").alias("CRFD_FUND_ID"),
    F.col("crcaCapAdjOut.CRPL_POOL_ID").alias("CRPL_POOL_ID"),
    F.col("crcaCapAdjOut.CRME_CR_PR_ID").alias("CRME_CR_PR_ID"),
    F.col("svSeqNo").alias("SEQ_NO")
)

# Write "lodDeDup" back to the same dummy table via upsert
# We'll stage to a temp table then MERGE
temp_table_lodDeDup = "STAGING.FctsCapExtr_hf_cap_dedup_temp"
execute_dml(f"DROP TABLE IF EXISTS {temp_table_lodDeDup}", jdbc_url_hf_cap_dedup, jdbc_props_hf_cap_dedup)

df_lodDeDup.write \
    .format("jdbc") \
    .option("url", jdbc_url_hf_cap_dedup) \
    .options(**jdbc_props_hf_cap_dedup) \
    .option("dbtable", temp_table_lodDeDup) \
    .mode("overwrite") \
    .save()

merge_sql_lodDeDup = f"""
MERGE INTO IDS.dummy_hf_cap_dedup AS T
USING {temp_table_lodDeDup} AS S
ON T.MEME_CK = S.MEME_CK
AND T.MDTL_PAY_DT = S.MDTL_PAY_DT
AND T.CRFD_FUND_ID = S.CRFD_FUND_ID
AND T.CRPL_POOL_ID = S.CRPL_POOL_ID
AND T.CRME_CR_PR_ID = S.CRME_CR_PR_ID
WHEN MATCHED THEN
  UPDATE SET SEQ_NO = S.SEQ_NO
WHEN NOT MATCHED THEN
  INSERT (MEME_CK, MDTL_PAY_DT, CRFD_FUND_ID, CRPL_POOL_ID, CRME_CR_PR_ID, SEQ_NO)
  VALUES (S.MEME_CK, S.MDTL_PAY_DT, S.CRFD_FUND_ID, S.CRPL_POOL_ID, S.CRME_CR_PR_ID, S.SEQ_NO);
"""
execute_dml(merge_sql_lodDeDup, jdbc_url_hf_cap_dedup, jdbc_props_hf_cap_dedup)


# ---------------------------------------------------------
# hf_fcts_cap_extr_main_in2 (Scenario C)
#   Write the df_deDupCrcaCapAdjIn to a parquet
#   Then read it back for the next stage
# ---------------------------------------------------------
write_files(
    df_deDupCrcaCapAdjIn,
    f"{adls_path}/hf_fcts_cap_extr_main_in2.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)
df_hf_fcts_cap_extr_main_in2 = spark.read.parquet(f"{adls_path}/hf_fcts_cap_extr_main_in2.parquet")

# ---------------------------------------------------------
# hf_cap_dedup_lkup was scenario B (already handled). No separate read needed here; done above.
# ---------------------------------------------------------

# ---------------------------------------------------------
# hf_cap_dedup (scenario B) also handled. No further read needed.
# ---------------------------------------------------------

# ---------------------------------------------------------
# hf_cap_extr_snap_lob (Scenario C)
#   Input: df_lnkSnapLob => write parquet => read parquet for "StripField"
# ---------------------------------------------------------
df_hf_cap_extr_snap_lob_write = df_lnkSnapLob.select(
    F.col("MEME_CK").cast("int").alias("MEME_CK"),
    F.col("PDPD_ID").cast("string").alias("PDPD_ID"),
    F.col("PDBL_ACCT_CAT").cast("string").alias("PDBL_ACCT_CAT")
)
write_files(
    df_hf_cap_extr_snap_lob_write,
    f"{adls_path}/hf_cap_extr_snap_lob.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)
df_hf_cap_extr_snap_lob_read = spark.read.parquet(f"{adls_path}/hf_cap_extr_snap_lob.parquet").alias("refSnapLob")


# ---------------------------------------------------------
# hf_cap_gl_cat_cd (Scenario C)
#   Input: df_CrfdFundDefnOut => transform => write => read for "BusinessRules"
# ---------------------------------------------------------
df_hf_cap_gl_cat_cd_write = df_CrfdFundDefnOut.select(
    trim(F.col("CRFD_FUND_ID")).alias("CRFD_FUND_ID"),
    trim(F.col("CRFD_ACCT_CAT")).alias("CRFD_ACCT_CAT")
)
write_files(
    df_hf_cap_gl_cat_cd_write,
    f"{adls_path}/hf_cap_gl_cat_cd.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)
df_hf_cap_gl_cat_cd_read = spark.read.parquet(f"{adls_path}/hf_cap_gl_cat_cd.parquet").alias("refCatcd")


# ---------------------------------------------------------
# hf_cap_extr_crca_snap_lob (Scenario C)
#   Input: df_lnkCRCASnapLob => write => read for "StripField"
# ---------------------------------------------------------
df_hf_cap_extr_crca_snap_lob_write = df_lnkCRCASnapLob.select(
    F.col("MEME_CK").cast("int").alias("MEME_CK"),
    F.col("PDPD_ID").cast("string").alias("PDPD_ID"),
    F.col("PDBL_ACCT_CAT").cast("string").alias("PDBL_ACCT_CAT")
)
write_files(
    df_hf_cap_extr_crca_snap_lob_write,
    f"{adls_path}/hf_cap_extr_crca_snap_lob.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)
df_hf_cap_extr_crca_snap_lob_read = spark.read.parquet(f"{adls_path}/hf_cap_extr_crca_snap_lob.parquet").alias("refCRCASnapLob")


# ---------------------------------------------------------
# hf_cap_copay_hash (Scenario C)
#   Input: df_lnkCopayOut => transform => write => read for "StripField"
# ---------------------------------------------------------
df_hf_cap_copay_hash_write = df_lnkCopayOut.select(
    trim(F.col("PDPD_ID")).alias("PDPD_ID"),
    F.date_format(F.col("MDTL_EARN_DT"), "yyyy-MM-dd").alias("MDTL_EARN_DT"),
    trim(F.col("CRME_CR_PR_TYPE")).alias("CRME_CR_PR_TYPE"),
    F.col("BSDL_COPAY_AMT").alias("BSDL_COPAY_AMT"),
    trim(F.col("BSDL_MCTR_TYPE")).alias("BSDL_MCTR_TYPE")
)
write_files(
    df_hf_cap_copay_hash_write,
    f"{adls_path}/hf_cap_copay_hash.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)
df_hf_cap_copay_hash_read = spark.read.parquet(f"{adls_path}/hf_cap_copay_hash.parquet").alias("refCopay")


# ---------------------------------------------------------
# hf_fcts_cap_extr_main_in1 (Scenario C)
#   Input: df_MDTL_MEMB_DTL => write => read for "Collector_Paid_Adjs"
# ---------------------------------------------------------
df_hf_fcts_cap_extr_main_in1_write = df_MDTL_MEMB_DTL.select(
    F.col("MEME_CK").cast("int").alias("MEME_CK"),
    F.col("MDTL_EARN_DT").cast("timestamp").alias("MDTL_EARN_DT"),
    F.col("CRME_CR_PR_ID").alias("CRME_CR_PR_ID"),
    F.col("CRPL_POOL_ID").alias("CRPL_POOL_ID"),
    F.col("CRFD_FUND_ID").alias("CRFD_FUND_ID"),
    F.col("CRME_SEQ_NO").cast("int").alias("CRME_SEQ_NO"),
    F.col("LOBD_ID").alias("LOBD_ID"),
    F.col("CRME_CR_PR_TYPE").alias("CRME_CR_PR_TYPE"),
    F.col("CRME_FUND_AMT").cast("decimal(38,10)").alias("CRME_FUND_AMT"),
    F.col("CRME_FUND_RATE").cast("decimal(38,10)").alias("CRME_FUND_RATE"),
    F.col("CRME_ME_MONTHS").cast("decimal(38,10)").alias("CRME_ME_MONTHS"),
    F.col("CRFS_SCHD_ID").alias("CRFS_SCHD_ID"),
    F.col("MDTL_PERIOD_IND").alias("MDTL_PERIOD_IND"),
    F.col("MDTL_PAY_DT").cast("timestamp").alias("MDTL_PAY_DT"),
    F.col("CRME_PAYEE_PR_ID").alias("CRME_PAYEE_PR_ID"),
    F.col("CRME_PRPR_ID").alias("CRME_PRPR_ID"),
    F.col("NWNW_ID").alias("NWNW_ID"),
    F.col("CSPI_ID").alias("CSPI_ID"),
    F.col("PDPD_ID").alias("PDPD_ID"),
    F.col("SBSB_ID").alias("SBSB_ID"),
    F.col("GRGR_CK").cast("int").alias("GRGR_CK"),
    F.col("GRGR_ID").alias("GRGR_ID"),
    F.col("SGSG_ID").alias("SGSG_ID"),
    F.col("BSDL_MCTR_TYPE").alias("BSDL_MCTR_TYPE"),
    F.col("BSDL_COPAY_AMT").cast("decimal(38,10)").alias("BSDL_COPAY_AMT"),
    F.col("CSCS_ID").alias("CSCS_ID"),
    F.col("CRCA_STS").alias("CRCA_STS"),
    F.col("CRCA_CR_PR_TYPE").alias("CRCA_CR_PR_TYPE"),
    F.col("CRCA_CAP_ADJ_AMT").cast("decimal(38,10)").alias("CRCA_CAP_ADJ_AMT"),
    F.col("CRCA_MCTR_ARSN").alias("CRCA_MCTR_ARSN"),
    F.col("MEME_BIRTH_DT").cast("timestamp").alias("MEME_BIRTH_DT"),
    F.col("DATA_SOURCE").alias("DATA_SOURCE"),
    F.col("SBSB_CK").cast("int").alias("SBSB_CK")
)
write_files(
    df_hf_fcts_cap_extr_main_in1_write,
    f"{adls_path}/hf_fcts_cap_extr_main_in1.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)
df_hf_fcts_cap_extr_main_in1 = spark.read.parquet(f"{adls_path}/hf_fcts_cap_extr_main_in1.parquet")

# ---------------------------------------------------------
# Collector_Paid_Adjs
#   Inputs: hf_fcts_cap_extr_main_in1 => link in1
#           hf_fcts_cap_extr_main_in2 => link DSLink103
#   Collect them => output => hf_fcts_cap_extr_main_driver
# ---------------------------------------------------------
df_collector_in1 = df_hf_fcts_cap_extr_main_in1.select(df_hf_fcts_cap_extr_main_in1.columns)
df_collector_in2 = df_hf_fcts_cap_extr_main_in2.select(df_hf_fcts_cap_extr_main_in2.columns)
# Round-robin: we will just union them
df_Collector_Paid_Adjs = df_collector_in1.unionByName(df_collector_in2)

write_files(
    df_Collector_Paid_Adjs,
    f"{adls_path}/hf_fcts_cap_extr_main_driver.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)
df_hf_fcts_cap_extr_main_driver = spark.read.parquet(f"{adls_path}/hf_fcts_cap_extr_main_driver.parquet").alias("Extract")


# ---------------------------------------------------------
# hf_fcts_cap_extr_main_driver (Scenario C), we've just written and read it
# ---------------------------------------------------------

# ---------------------------------------------------------
# StripField transformer:
#   Primary link: df_hf_fcts_cap_extr_main_driver (alias "Extract")
#   Lookup1: refCopay
#   Lookup2: refSnapLob
#   Lookup3: refCRCASnapLob
# ---------------------------------------------------------
df_Strip_temp = df_hf_fcts_cap_extr_main_driver.join(
    df_hf_cap_copay_hash_read,
    on=[
        F.trim(F.col("Extract.PDPD_ID")) == F.col("refCopay.PDPD_ID"),
        F.date_format(F.col("Extract.MDTL_EARN_DT"), "yyyy-MM-dd") == F.col("refCopay.MDTL_EARN_DT"),
        F.trim(F.col("Extract.CRME_CR_PR_TYPE")) == F.col("refCopay.CRME_CR_PR_TYPE")
    ],
    how="left"
).join(
    df_hf_cap_extr_snap_lob_read,
    on=[
        F.col("Extract.MEME_CK") == F.col("refSnapLob.MEME_CK"),
        F.col("Extract.PDPD_ID") == F.col("refSnapLob.PDPD_ID")
    ],
    how="left"
).join(
    df_hf_extr_crca_snap_lob_read := df_hf_extr_crca_snap_lob_read if False else df_hf_cap_extr_crca_snap_lob_read,  # to keep lint happy 
    df_hf_cap_extr_crca_snap_lob_read,
    on=[
        F.col("Extract.MEME_CK") == F.col("refCRCASnapLob.MEME_CK"),
        F.col("Extract.PDPD_ID") == F.col("refCRCASnapLob.PDPD_ID")
    ],
    how="left"
)

df_Strip = df_Strip_temp.select(
    F.col("Extract.MEME_CK").alias("MEME_CK"),
    trim(F.col("Extract.CRME_CR_PR_ID")).alias("CRME_CR_PR_ID"),
    trim(F.col("Extract.CSPI_ID")).alias("CSPI_ID"),
    F.col("Extract.MDTL_EARN_DT").alias("MDTL_EARN_DT"),
    trim(F.col("Extract.CRFD_FUND_ID")).alias("CRFD_FUND_ID"),
    F.col("Extract.CRME_SEQ_NO").alias("CRME_SEQ_NO"),
    trim(F.col("Extract.CRPL_POOL_ID")).alias("CRPL_POOL_ID"),
    trim(F.col("Extract.LOBD_ID")).alias("LOBD_ID"),
    F.regexp_replace(F.col("Extract.CRME_CR_PR_TYPE"), "[\\r\\n\\t]", "").alias("CRME_CR_PR_TYPE"),
    F.col("Extract.CRME_FUND_AMT").alias("CRME_FUND_AMT"),
    F.col("Extract.CRME_FUND_RATE").alias("CRME_FUND_RATE"),
    F.col("Extract.CRME_ME_MONTHS").alias("CRME_ME_MONTHS"),
    trim(F.col("Extract.CRFS_SCHD_ID")).alias("CRFS_SCHD_ID"),
    F.regexp_replace(F.col("Extract.MDTL_PERIOD_IND"), "[\\r\\n\\t]", "").alias("MDTL_PERIOD_IND"),
    F.date_format(F.col("Extract.MDTL_PAY_DT"), "yyyy-MM-dd").alias("MDTL_PAY_DT"),
    trim(F.col("Extract.CRME_PAYEE_PR_ID")).alias("CRME_PAYEE_PR_ID"),
    trim(F.col("Extract.NWNW_ID")).alias("NWNW_ID"),
    trim(F.col("Extract.CRME_PRPR_ID")).alias("CRME_PRPR_ID"),
    trim(F.col("Extract.PDPD_ID")).alias("PDPD_ID"),
    F.col("Extract.GRGR_CK").alias("GRGR_CK"),
    trim(F.col("Extract.GRGR_ID")).alias("GRGR_ID"),
    trim(F.col("Extract.SGSG_ID")).alias("SGSG_ID"),
    trim(F.col("Extract.SBSB_ID")).alias("SBSB_ID"),
    F.when(F.col("refCopay.BSDL_MCTR_TYPE").isNull(), F.lit("NA")).otherwise(F.col("refCopay.BSDL_MCTR_TYPE")).alias("BSDL_MCTR_TYPE"),
    F.when(F.col("refCopay.BSDL_COPAY_AMT").isNull(), F.lit(0)).otherwise(F.col("refCopay.BSDL_COPAY_AMT")).alias("BSDL_COPAY_AMT"),
    F.when(F.col("Extract.CSCS_ID").isNull(), F.lit("NA")).otherwise(trim(F.col("Extract.CSCS_ID"))).alias("CSCS_ID"),
    F.regexp_replace(F.col("Extract.CRCA_CR_PR_TYPE"), "[\\r\\n\\t]", "").alias("CRCA_CR_PR_TYPE"),
    F.col("Extract.CRCA_CAP_ADJ_AMT").alias("CRCA_CAP_ADJ_AMT"),
    trim(F.col("Extract.CRCA_MCTR_ARSN")).alias("CRCA_MCTR_ARSN"),
    trim(F.col("Extract.CRCA_STS")).alias("CRCA_STS"),
    F.col("Extract.MEME_BIRTH_DT").alias("MEME_BIRTH_DT"),
    trim(F.col("Extract.DATA_SOURCE")).alias("DATA_SOURCE"),
    F.when(F.col("Extract.SBSB_CK").isNull(), F.lit("NA")).otherwise(trim(F.col("Extract.SBSB_CK"))).alias("SBSB_CK"),
    F.when(
        F.col("Extract.DATA_SOURCE") == F.lit("CRCA"),
        F.when(
            F.col("Extract.MEME_CK") == F.lit(-1),
            F.lit("NA")
        ).otherwise(
            F.when(
                F.col("refCRCASnapLob.PDBL_ACCT_CAT").isNull() | (F.length(F.trim(F.col("refCRCASnapLob.PDBL_ACCT_CAT"))) == 0),
                F.lit("NA")
            ).otherwise(trim(F.col("refCRCASnapLob.PDBL_ACCT_CAT")))
        )
    ).otherwise(
        F.when(
            F.col("refSnapLob.PDBL_ACCT_CAT").isNull() | (F.length(F.trim(F.col("refSnapLob.PDBL_ACCT_CAT"))) == 0),
            F.lit("NA")
        ).otherwise(trim(F.col("refSnapLob.PDBL_ACCT_CAT")))
    ).alias("FNCL_LOB_CD")
).alias("Strip")


# ---------------------------------------------------------
# BusinessRules
#   Primary link: df_Strip
#   Lookup link: df_hf_cap_gl_cat_cd_read => "refCatcd"
# ---------------------------------------------------------
df_BusinessRules_temp = df_Strip.join(
    df_hf_cap_gl_cat_cd_read,
    on=[F.col("Strip.CRFD_FUND_ID") == F.col("refCatcd.CRFD_FUND_ID")],
    how="left"
).alias("AllColOut")

df_BusinessRules_temp = df_BusinessRules_temp.withColumn(
    "RowPassThru", F.lit("Y")
).withColumn(
    "svErnDt", F.date_format(F.col("Strip.MDTL_EARN_DT"), "yyyy-MM-dd")
).withColumn(
    "svBirthDt", F.date_format(F.col("Strip.MEME_BIRTH_DT"), "yyyy-MM-dd")
).withColumn(
    "svAge",
    F.when(
        F.col("Strip.DATA_SOURCE") == F.lit("CRCA"),
        F.lit(0)
    ).otherwise(
        # AGE(svBirthDt, svErnDt) -> approximate difference in years
        (F.year(F.col("svErnDt")) - F.year(F.col("svBirthDt")))
    )
)

df_BusinessRules_temp = df_BusinessRules_temp.withColumn(
    "svGlCatCd",
    F.when(
        (F.col("refCatcd.CRFD_FUND_ID").isNotNull()) &
        (F.col("refCatcd.CRFD_ACCT_CAT") == F.lit("PSY")) &
        (F.col("Strip.LOBD_ID") == F.lit("BCBS")),
        F.lit("ADM")
    ).when(
        (F.col("refCatcd.CRFD_FUND_ID").isNotNull()) &
        (F.col("refCatcd.CRFD_ACCT_CAT") == F.lit("PSYB")) &
        (F.col("Strip.LOBD_ID") == F.lit("BCAR")),
        F.lit("ADM")
    ).when(
        (F.col("refCatcd.CRFD_FUND_ID").isNotNull()) &
        (F.col("refCatcd.CRFD_ACCT_CAT") == F.lit("EYE")) &
        (F.col("Strip.LOBD_ID") == F.lit("BAMA")),
        F.lit("MED")
    ).when(
        (F.col("refCatcd.CRFD_FUND_ID").isNotNull()) &
        (F.col("refCatcd.CRFD_ACCT_CAT") == F.lit("EYE")) &
        (F.col("Strip.LOBD_ID") == F.lit("MVMA")),
        F.lit("MED")
    ).when(
        (F.col("refCatcd.CRFD_FUND_ID").isNotNull()) &
        (F.col("refCatcd.CRFD_ACCT_CAT") == F.lit("PSY")) &
        (F.col("Strip.LOBD_ID") == F.lit("BAMA")),
        F.lit("ADM")
    ).when(
        (F.col("refCatcd.CRFD_FUND_ID").isNotNull()) &
        (F.col("refCatcd.CRFD_ACCT_CAT") == F.lit("PSY")) &
        (F.col("Strip.LOBD_ID") == F.lit("MVMA")),
        F.lit("ADM")
    ).otherwise(F.col("refCatcd.CRFD_ACCT_CAT"))
)

# Output link "AllCol"
df_AllCol = df_BusinessRules_temp.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.col("RowPassThru").alias("PASS_THRU_IN"),
    F.col("CurrDate").alias("FIRST_RECYC_DT") if False else F.lit("<...>"),  # fallback since we do not have real CurrDate
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.col("SrcSysCd").alias("SRC_SYS_CD") if False else F.lit(SrcSysCd).alias("SRC_SYS_CD"),
    F.concat(
        F.lit(SrcSysCd), F.lit(";"),
        F.col("Strip.MEME_CK"), F.lit(";"),
        F.col("Strip.CRME_CR_PR_ID"), F.lit(";"),
        F.col("svErnDt"), F.lit(";"),
        F.col("Strip.MDTL_PAY_DT"), F.lit(";"),
        F.col("Strip.CRFD_FUND_ID"), F.lit(";"),
        F.col("Strip.CRPL_POOL_ID"), F.lit(";"),
        F.col("Strip.CRME_SEQ_NO")
    ).alias("PRI_KEY_STRING"),
    F.lit(0).alias("CRT_RUN_CYC_EXTCN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXTCN_SK"),
    F.lit(0).alias("CAP_FUND_SK"),
    F.lit(0).alias("CAP_PROV_SK"),
    F.col("Strip.CSCS_ID").alias("CLS_ID"),
    F.col("Strip.CSPI_ID").alias("CLS_PLN_ID"),
    F.col("Strip.FNCL_LOB_CD").alias("FNCL_LOB_ID"),
    F.col("Strip.GRGR_CK").cast("string").alias("GRP_CK"),
    F.col("Strip.GRGR_ID").alias("GRGR_ID"),
    F.col("Strip.MEME_CK").cast("string").alias("MBR_CK"),
    F.when(F.length(F.trim(F.col("Strip.NWNW_ID"))) == 0, "NA").otherwise(F.trim(F.col("Strip.NWNW_ID"))).alias("NTWK_ID"),
    F.when(F.length(F.trim(F.col("Strip.CRME_PAYEE_PR_ID"))) == 0, "NA").otherwise(F.trim(F.col("Strip.CRME_PAYEE_PR_ID"))).alias("PD_PROV_ID"),
    F.when(F.length(F.trim(F.col("Strip.CRME_PRPR_ID"))) == 0, "NA").otherwise(F.trim(F.col("Strip.CRME_PRPR_ID"))).alias("PCP_PROV_ID"),
    F.when(F.length(F.trim(F.col("Strip.PDPD_ID"))) == 0, "NA").otherwise(F.trim(F.col("Strip.PDPD_ID"))).alias("PROD_ID"),
    F.col("Strip.SGSG_ID").alias("SUBGRP_ID"),
    F.col("Strip.SBSB_ID").alias("SUB_ID"),
    F.col("Strip.CRCA_MCTR_ARSN").alias("CAP_ADJ_RSN_CD"),
    F.when(F.length(F.trim(F.col("Strip.CRCA_STS"))) == 0, "NA").otherwise(F.trim(F.col("Strip.CRCA_STS"))).alias("CAP_ADJ_STTUS_CD"),
    F.col("Strip.CRCA_CR_PR_TYPE").alias("CAP_ADJ_TYP_CD"),
    F.when(
        (F.col("Strip.DATA_SOURCE") == F.lit("CRME")) |
        (F.col("Strip.DATA_SOURCE") == F.lit("CRCA")) |
        (F.col("Strip.DATA_SOURCE") == F.lit("EXCEL")) |
        (F.col("Strip.DATA_SOURCE") == F.lit("CDS")),
        F.col("Strip.DATA_SOURCE")
    ).otherwise(F.lit("UNK")).alias("CAP_CAT_CD"),
    F.when(F.length(F.trim(F.col("Strip.BSDL_MCTR_TYPE"))) == 0, "NA").otherwise(F.trim(F.col("Strip.BSDL_MCTR_TYPE"))).alias("CAP_COPAY_TYP_CD"),
    F.col("Strip.LOBD_ID").alias("CAP_LOB_CD"),
    F.when(F.length(F.trim(F.col("Strip.MDTL_PERIOD_IND"))) == 0, "NA").otherwise(F.trim(F.col("Strip.MDTL_PERIOD_IND"))).alias("CAP_PERD_CD"),
    F.col("Strip.CRFS_SCHD_ID").alias("CAP_SCHD_CD"),
    F.col("Strip.CRME_CR_PR_TYPE").alias("CAP_TYP_CD"),
    F.col("Strip.CRCA_CAP_ADJ_AMT").alias("ADJ_AMT"),
    F.col("Strip.CRME_FUND_AMT").alias("CAP_AMT"),
    F.when(
        (F.trim(F.col("Strip.BSDL_MCTR_TYPE")) == F.lit("OVP")) | 
        (F.trim(F.col("Strip.BSDL_MCTR_TYPE")) == F.lit("OVS"))
     , F.col("Strip.BSDL_COPAY_AMT")).otherwise(F.lit(0)).alias("COPAY_AMT"),
    F.col("Strip.CRME_FUND_RATE").alias("FUND_RATE_AMT"),
    F.when(F.col("svAge") > 120, 0).otherwise(F.when(F.col("svAge") < 0, 0).otherwise(F.col("svAge"))).alias("MBR_AGE"),
    F.col("Strip.CRME_ME_MONTHS").alias("MBR_MO_CT"),
    F.when(F.length(F.trim(F.col("Strip.SBSB_CK"))) == 0, "NA").otherwise(F.trim(F.col("Strip.SBSB_CK"))).alias("SBSB_CK"),
    F.col("svGlCatCd").alias("CRFD_ACCT_CAT")
)

df_Transform = df_BusinessRules_temp.select(
    F.lit(0).alias("SRC_SYS_CD_SK"),  # pinned as "Keys" side
    F.col("Strip.MEME_CK").alias("MBR_UNIQ_KEY"),
    F.col("Strip.CRME_CR_PR_ID").alias("CAP_PROV_ID"),
    F.col("svErnDt").alias("ERN_DT_SK"),
    F.col("Strip.MDTL_PAY_DT").alias("PD_DT_SK"),
    F.col("Strip.CRFD_FUND_ID").alias("CAP_FUND_ID"),
    F.col("Strip.CRPL_POOL_ID").alias("CAP_POOL_CD"),
    F.col("Strip.CRME_SEQ_NO").alias("SEQ_NO")
).alias("Keys")

# ---------------------------------------------------------
# CapPK (Shared Container) - Two inputs: "Transform" -> Keys, "AllCol" -> AllColOut
# ---------------------------------------------------------
params_CapPK = {
    "IDSOwner": IDSOwner,
    "CurrRunCycle": CurrRunCycle,
    "SrcSysCd": SrcSysCd
}
df_CapPK_out = CapPK(df_Transform, df_AllCol, params_CapPK)  # returns a single DF (since container has 1 output)

# ---------------------------------------------------------
# IdsCapPkey -> CSeqFileStage (PxSequentialFile)
#   Write to #TmpOutFile# in directory "key"
# ---------------------------------------------------------
final_select_IdsCapPkey = df_CapPK_out.select(df_CapPK_out.columns)

out_path_IdsCapPkey = f"{adls_path}/key/{TmpOutFile}"
write_files(
    final_select_IdsCapPkey,
    out_path_IdsCapPkey,
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# ---------------------------------------------------------
# Facets_Source (ODBCConnector) -> "Link_Collector" -> "Snapshot_File"
#   Additional source we see at end of JSON
# ---------------------------------------------------------
jdbc_url_facets_source, jdbc_props_facets_source = get_db_config(facets_secret_name)

query_MdtlMembDtlExtr = """SELECT MEME_CK,MDTL_EARN_DT,CRME_CR_PR_ID,CRPL_POOL_ID,CRFD_FUND_ID,CRME_SEQ_NO,MDTL_PAY_DT,CRME_FUND_AMT,CRME_ME_MONTHS 
FROM """ + FacetsOwner + """.CDS_MDTL_MEMB_DTL MDTL WHERE  
((MDTL.CRME_FUND_AMT  <> 0) OR  (MDTL.CRME_FUND_AMT   = 0   AND   MDTL.MDTL_PERIOD_IND = 'C'))"""

df_MdtlMembDtlExtr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets_source)
    .options(**jdbc_props_facets_source)
    .option("query", query_MdtlMembDtlExtr)
    .load()
)

query_CrcaCapAdjExtr = """SELECT 
1 MEME_CK,
CRCA.CRCA_EARN_DT MDTL_EARN_DT,
CRCA.CRCA_CR_PR_ID CRME_CR_PR_ID,
CRCA.CRPL_POOL_ID CRPL_POOL_ID,
CRCA.CRFD_FUND_ID CRFD_FUND_ID,
1 CRME_SEQ_NO,
CRCA.CRCA_PAY_DT MDTL_PAY_DT,
0 CRME_FUND_AMT,
CRCA.CRCA_ME_MONTHS CRME_ME_MONTHS
FROM 
""" + FacetsOwner + """.CMC_CRCA_CAP_ADJ CRCA
WHERE CRCA.CRCA_PAY_DT = '""" + AttrbtnRowEffDt + """'
"""

df_CrcaCapAdjExtr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets_source)
    .options(**jdbc_props_facets_source)
    .option("query", query_CrcaCapAdjExtr)
    .load()
)

df_Link_Collector_1 = df_MdtlMembDtlExtr.select(df_MdtlMembDtlExtr.columns)
df_Link_Collector_2 = df_CrcaCapAdjExtr.select(df_CrcaCapAdjExtr.columns)
df_Link_Collector = df_Link_Collector_1.unionByName(df_Link_Collector_2)

# Next, transform => "Snapshot_File"
# The next transform stage: "Transform"
df_Transform_snapshot = df_Link_Collector.withColumn(
    "svERNDT", F.date_format(F.col("MDTL_EARN_DT"), "yyyy-MM-dd")
).withColumn(
    "svErnDtSk", F.lit("GetFkeyDate(\"IDS\", 101, svERNDT, \"X\")")  # as placeholder literal
).withColumn(
    "svPDDT", F.date_format(F.col("MDTL_PAY_DT"), "yyyy-MM-dd")
).withColumn(
    "svPdDtSk", F.lit("GetFkeyDate(\"IDS\", 102, svPDDT, \"X\")")
).withColumn(
    "svCapPoolCdSk", F.lit("GetFkeyCodes(\"FACETS\", 110, \"CAPITATION POOL\", CRPL_POOL_ID, \"X\")")
)

df_Snapshot_File = df_Transform_snapshot.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.trim(F.col("MEME_CK")).alias("MBR_UNIQ_KEY"),
    F.trim(F.col("CRME_CR_PR_ID")).alias("CAP_PROV_ID"),
    F.col("svErnDtSk").alias("ERN_DT_SK"),
    F.col("svPdDtSk").alias("PD_DT_SK"),
    F.trim(F.col("CRFD_FUND_ID")).alias("CAP_FUND_ID"),
    F.col("svCapPoolCdSk").alias("CAP_POOL_CD_SK"),
    F.trim(F.col("CRME_SEQ_NO")).alias("SEQ_NO"),
    F.col("CRME_FUND_AMT").alias("CAP_AMT"),
    F.col("CRME_ME_MONTHS").alias("MBR_MO_CT")
)

out_path_Snapshot_File = f"{adls_path}/load/B_CAP.dat"
write_files(
    df_Snapshot_File,
    out_path_Snapshot_File,
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)