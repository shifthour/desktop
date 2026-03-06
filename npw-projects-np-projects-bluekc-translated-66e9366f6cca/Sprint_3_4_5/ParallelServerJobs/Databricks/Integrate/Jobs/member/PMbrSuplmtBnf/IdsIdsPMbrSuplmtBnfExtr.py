# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Modifications:                        
# MAGIC                                                  Project/                                                                                                                       Code                   Date
# MAGIC Developer           Date              Altiris #     Change Description                                                                                     Reviewer            Reviewed
# MAGIC -------------------------  -------------------   -------------   -----------------------------------------------------------------------------------------------------------------   -------------------------  -------------------
# MAGIC Bhoomi               2009-09-02    4113        Originally Programmed                                                                                  Steph Goddard   09/11/2009
# MAGIC Kalyan Neelam   2009-09-29    4113        Changed the source from Facets to IDS                                                      Steph Goddard   10/01/2009
# MAGIC Hugh Sisson       2010-01-22    4278        Added 5 new columns to the end of the table       
# MAGIC 
# MAGIC 
# MAGIC                                                     Project/                                                                                                                                 DataStage                         Code                  Date
# MAGIC Developer        Date                    Altiris #                 Change Description                                                                                  Environment                      Reviewer            Reviewed
# MAGIC ----------------------  ----------------------    -------------------           ------------------------------------------------------------------------------------                             -----------------------------------       ----------------------     -------------------   
# MAGIC Judy Reynolds    2010-05-12    4297- Alineo PH2    Added 1 additional new column to the end of the table                           IntegrateNewDevl              Steph Goddard  05/24/2010
# MAGIC                                                                                 (OTHR_CAR_PRI_MED_IN) and populate based on 
# MAGIC                                                                                 MBR_COB MBR_UNIQ_KEYs
# MAGIC Ralph Tucker  2011-07-06       TTR-1175               Changed SQL for EapBnfLvl link to use SK fields instead                        IntegrateCurDevl                Brent Leland      7-07-2011   
# MAGIC                                                                                 of ID to ID fields.
# MAGIC 
# MAGIC Manasa Andru/     2012-05-31     4830- AHY 3.0      Changed the entire logic for  WELNS_BNF_VNDR_ID field                   IntegrateCurDevl               SAndrew            2012-06-18     
# MAGIC Kalyan Neelam
# MAGIC Kalyan Neelam      2012-12-26     4830                     Changed the entire logic for  WELNS_BNF_VNDR_ID field                   IntegrateNewDevl             Bhoomi Dasari    12/26/2012
# MAGIC Kalyan Neelam     2013-01-11      4830                     Updated SQL in IDS_MBR_ENR_4 for HF scenario 5 & 6.                    IntegrateNewDevl             Bhoomi Dasari    1/15/2013
# MAGIC                                                                                   Added logic ENR1.ELIG_IN = 'Y' AND
# MAGIC                                                                                   ENR1.EFF_DT_SK <= '#CurrDate#' AND
# MAGIC                                                                                   ENR1.TERM_DT_SK >= '#CurrDate#'  AND in STEP1 query.
# MAGIC 
# MAGIC Rama Kamjula      2013-09-20         5114                  Rewritten Server job to Parallel version                                                IntegrateWrhsDevl
# MAGIC Kalyan Neelam    2013-11-18     5234 Compass     Added new column CMPSS_COV_IN, ALNO_HOME_PG_ID               IntegrateNewDevl                Bhoomi Dasari     12/09/2013
# MAGIC                                                                                and its associated logic 
# MAGIC 
# MAGIC Karthik Chintalapani  2013-01-21    5131                   Updated the logic for column  ALNO_HOME_PG_ID                          IntegrateNewDevl               Kalyan Neelam            2014-01-28
# MAGIC                                                                                    as per the new mapping rules.
# MAGIC 
# MAGIC Karthik Chintalapani  2013-01-21    5108                    Updated the logic for column  PCMH_IN                                             IntegrateNewDevl              Kalyan Neelam            2014-03-31
# MAGIC                                                                                     as per the new mapping rules.
# MAGIC 
# MAGIC Raja Gummadi         2014-11-12      5234                  Added FirstOfNextMonth parameter. Used in CLS_PLN_DTL              IntegrateNewDevl              Kalyan Neelam            2014-11-12
# MAGIC                                                                                   lookup stage.
# MAGIC Raja Gummadi         2015-12-11      5414                  Changed logic for WELNS_VNDR_ID. Input query change                 IntegrateDev1                    Jag Yelavarthi             2015-12-11
# MAGIC Raja Gummadi         2016-01-20      5414                  Excluded ASO Groups and added HM Large Groups                           IntegrateDev2                    Kalyan Neelam            2016-01-20
# MAGIC Raja Gummadi         2016-02-18      5414                  Added HealthMineFile lookup steps. Changed logic for HM                 IntegrateDev2                    Kalyan Neelam            2016-02-18
# MAGIC Raja Gummadi         2016-03-06      5414                  Changed HM input query to MED and also AHY only plans.                 IntegrateDev2                     Kalyan Neelam           2016-06-03
# MAGIC Raja Gummadi         2016-06-22      5414                  Added HealthMineFile and added GRP_AHY_PGM lookup                 IntegrateDev1                     Jag Yelavarthi             2016-06-24
# MAGIC Abhiram Dasarathy	2016-12-06      5217                  Added DNTL_NTWRK_IN field to the end of the columns	               IntegrateDev2                     Kalyan Neelam           2016-12-06  
# MAGIC Abhiram Dasarathy	2017-03-10      5414	  Removed the exclusion for JAA groups in the MBR_ENR_4 extract    IntegrateDev2	         Kalyan Neelam            2017-03-10	
# MAGIC Raja Gummadi         2017-10-18      TFS -19992       Replaced TREO with BCBSKC in PCMH and ALNO lookup extracts   IntegrateDev1                    Kalyan Neelam            2017-10-19
# MAGIC 
# MAGIC Madhavan B            2017-12-06      5744                 Adding value 'EYEMED' to VSN_BNF_VNDR_ID field for                    IntegrateDev3                    Jag Yelavarthi               2017-12-05
# MAGIC                                                                                  Members with Vision products.(MBR_ENR_CLS_PLN_PROD_CAT_CD = 'VSN')
# MAGIC Raja Gummadi         2017-12-21      TFS-20646        Added logic in GRP_AHY_PGM stage to filter out previous year rows  IntegrateDev1                    Hugh Sisson                2017-12-21
# MAGIC Giri  Mallavaram      2019-11-13      TFS-20646        Added logic for OPtum Rx for PBM_VNDR_CD   inthe trasformer          IntegrateDev2                    Kalyan Neelam             2019-11-21
# MAGIC 
# MAGIC Sravya G                   11/30/2020     US260801    Modified SQL to extract Subgroup ID (MBR_ENR_4 stage)                           IntegrateDev1                    Jaideep Mankala         12/01/2020
# MAGIC 				and also updated logic to populate WELNS_BNF_VNDR_ID field    
# MAGIC 
# MAGIC 
# MAGIC Sravya G                   2021-01-08     US338368    Modified job to assign 'HM'  Welns Bnf Vndr Ind                                                 IntegrateDev1                    Jaideep Mankala        01/08/2021
# MAGIC 				    on 10001000 group (subgroup 0001 and 0002)

# MAGIC Extracts MBR_ENR table data from IDS
# MAGIC Extract Prod_Bill_Cmpnt_ID from IDS table
# MAGIC Separates the data for different cmpnt ID's
# MAGIC Extracts data from MBR_ENR table
# MAGIC Null Handling and Business Logic
# MAGIC Creates Seq file for P_MBR_SUPLMT_BNF load
# MAGIC Dont delete this file from /ids/prod/landing.. This is a temporary work around till we build a working table or change rules to determine HM groups.
# MAGIC Extracts Member Data from IDS
# MAGIC Job Name: IdsIdsPMbrSuplmtBnfExtr
# MAGIC 
# MAGIC Creates load file for P_MBR_SUPLMT_BNF table
# MAGIC Extracts Age related from MBR_ENR table
# MAGIC Removes duplicates for MBR_SK
# MAGIC Extracts data form PROD_CMPNT table in IDS
# MAGIC Removes duplicates for MBR_SK
# MAGIC Removes duplicates to retrieve the latest record  for ALNO_Home_PG_ID
# MAGIC Removes duplicates to retrieve the latest record  for PCMH_IN
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType, StructType, StructField
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
CurrDate = get_widget_value('CurrDate','')
CurrDateMinus30 = get_widget_value('CurrDateMinus30','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
FirstOfNextMonth = get_widget_value('FirstOfNextMonth','')

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

df_db2_MBR = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"SELECT MBR_UNIQ_KEY FROM {IDSOwner}.MBR WHERE MBR_UNIQ_KEY <> 0 AND MBR_UNIQ_KEY <> 1"
    )
    .load()
)

df_db2_MBR_ENR_3 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"""SELECT DISTINCT 
MBR_ENR.MBR_UNIQ_KEY, 
CD_MPPNG_3.TRGT_CD AS NTWK_CAP_RELSHP_CAT_CD
FROM 
{IDSOwner}.MBR_ENR AS MBR_ENR, 
{IDSOwner}.CLS_PLN_DTL AS CLS_PLN_DTL, 
{IDSOwner}.CD_MPPNG AS CD_MPPNG_1,
{IDSOwner}.CLS_PLN AS CLS_PLN, 
{IDSOwner}.GRP AS GRP, 
{IDSOwner}.CLS AS CLS,
{IDSOwner}.NTWK_SET AS NTWK_SET,
{IDSOwner}.CD_MPPNG AS CD_MPPNG_2,
{IDSOwner}.NTWK_CAP_RELSHP AS NTWK_CAP_RELSHP,
{IDSOwner}.CD_MPPNG AS CD_MPPNG_3,
{IDSOwner}.NTWK AS NTWK
WHERE 
MBR_ENR.ELIG_IN = 'Y'
AND MBR_ENR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK = CD_MPPNG_1.CD_MPPNG_SK
AND CD_MPPNG_1.TRGT_CD = 'MED' 
AND MBR_ENR.EFF_DT_SK <= '{CurrDate}' 
AND MBR_ENR.TERM_DT_SK >= '{CurrDate}' 
AND MBR_ENR.SRC_SYS_CD_SK = CLS_PLN_DTL.SRC_SYS_CD_SK
AND MBR_ENR.CLS_PLN_SK = CLS_PLN.CLS_PLN_SK
AND CLS_PLN.CLS_PLN_SK = CLS_PLN_DTL.CLS_PLN_SK
AND MBR_ENR.CLS_SK = CLS.CLS_SK
AND CLS.CLS_SK = CLS_PLN_DTL.CLS_SK 
AND MBR_ENR.GRP_SK = GRP.GRP_SK
AND GRP.GRP_SK = CLS_PLN_DTL.GRP_SK
AND CLS_PLN_DTL.EFF_DT_SK <= '{CurrDate}' 
AND CLS_PLN_DTL.TERM_DT_SK >= '{CurrDate}' 
AND CLS_PLN_DTL.CLS_PLN_DTL_NTWK_SET_PFX_CD_SK = CD_MPPNG_2.CD_MPPNG_SK
AND CD_MPPNG_2.TRGT_CD = NTWK_SET.NTWK_SET_PFX_ID
AND NTWK_SET.EFF_DT_SK <= '{CurrDate}' 
AND NTWK_SET.TERM_DT_SK >= '{CurrDate}' 
AND NTWK_SET.NTWK_CAP_RELSHP_PFX_ID = NTWK_CAP_RELSHP.NTWK_CAP_RELSHP_PFX_ID
AND NTWK_CAP_RELSHP.EFF_DT_SK <= '{CurrDate}' 
AND NTWK_CAP_RELSHP.TERM_DT_SK >= '{CurrDate}' 
AND NTWK_CAP_RELSHP.NTWK_CAP_RELSHP_CAT_CD_SK = CD_MPPNG_3.CD_MPPNG_SK
AND CD_MPPNG_3.TRGT_CD NOT IN ( 'NA', 'UNK' )"""
    )
    .load()
)

df_db2_COB_MBR = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"""SELECT Distinct  
COB.MBR_UNIQ_KEY,
'Y' as COB_IND
FROM 
{IDSOwner}.MBR_COB AS COB,
{IDSOwner}.CD_MPPNG AS PAYMT_PRTY,
{IDSOwner}.CD_MPPNG AS COB_TYP_CD,
{IDSOwner}.CD_MPPNG AS MBR_COB_OTHR_CAR_ID_CD
WHERE 
COB.MBR_COB_PAYMT_PRTY_CD_SK = PAYMT_PRTY.CD_MPPNG_SK AND
COB.MBR_COB_TYP_CD_SK = COB_TYP_CD.CD_MPPNG_SK AND
COB.MBR_COB_OTHR_CAR_ID_CD_SK = MBR_COB_OTHR_CAR_ID_CD.CD_MPPNG_SK AND
COB.EFF_DT_SK<= '{CurrDate}' AND 
COB.TERM_DT_SK >= '{CurrDate}' AND 
PAYMT_PRTY.TRGT_CD = 'PRI' AND COB_TYP_CD.TRGT_CD <> 'DNTL' AND
MBR_COB_OTHR_CAR_ID_CD.TRGT_CD <> '4'"""
    )
    .load()
)

df_db2_CLS_PLN_DTL = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"""SELECT DISTINCT
MBR.MBR_UNIQ_KEY,
CLS_PLN_DTL.CMPSS_COV_IN
FROM
{IDSOwner}.MBR MBR,
{IDSOwner}.MBR_ENR MBR_ENR,
{IDSOwner}.CLS_PLN_DTL CLS_PLN_DTL,
{IDSOwner}.CD_MPPNG CD1,
{IDSOwner}.CD_MPPNG CD2
WHERE
MBR.MBR_UNIQ_KEY = MBR_ENR.MBR_UNIQ_KEY AND
MBR_ENR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK = CD1.CD_MPPNG_SK AND
CD1.TRGT_CD = 'MED' AND
MBR_ENR.ELIG_IN = 'Y' AND
((MBR_ENR.EFF_DT_SK <= '{CurrDate}' AND
MBR_ENR.TERM_DT_SK >= '{CurrDate}') OR (MBR_ENR.EFF_DT_SK <= '{FirstOfNextMonth}' AND
MBR_ENR.TERM_DT_SK >= '{FirstOfNextMonth}')) AND
MBR_ENR.GRP_SK = CLS_PLN_DTL.GRP_SK AND
MBR_ENR.CLS_SK = CLS_PLN_DTL.CLS_SK AND
MBR_ENR.CLS_PLN_SK = CLS_PLN_DTL.CLS_PLN_SK AND
CLS_PLN_DTL.CLS_PLN_DTL_PROD_CAT_CD_SK = CD2.CD_MPPNG_SK AND
CD2.TRGT_CD = 'MED' AND
CLS_PLN_DTL.CMPSS_COV_IN = 'Y' AND
((CLS_PLN_DTL.EFF_DT_SK <= '{CurrDate}' AND
CLS_PLN_DTL.TERM_DT_SK >= '{CurrDate}') OR (CLS_PLN_DTL.EFF_DT_SK <= '{FirstOfNextMonth}' AND
CLS_PLN_DTL.TERM_DT_SK >= '{FirstOfNextMonth}'))"""
    )
    .load()
)

df_db2_MBR_ENR_5 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"""SELECT DISTINCT
ENR.MBR_UNIQ_KEY,
'Y' AS EYEMED_IND
FROM {IDSOwner}.MBR_ENR AS ENR,
     {IDSOwner}.CD_MPPNG AS CD
WHERE ENR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK = CD.CD_MPPNG_SK
 AND ENR.EFF_DT_SK <= '{CurrDate}'
 AND ENR.TERM_DT_SK >= '{CurrDate}'
 AND ENR.ELIG_IN = 'Y'
 AND CD.TRGT_CD = 'VSN'"""
    )
    .load()
)

df_db2_MBR_ENR_2 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"""SELECT  DISTINCT
ENR.MBR_UNIQ_KEY,
ENR.PROD_SK,
PC.PROD_CMPNT_PFX_ID 
FROM     
{IDSOwner}.MBR_ENR   ENR, 
{IDSOwner}.CD_MPPNG   CD, 
{IDSOwner}.PROD_CMPNT  PC
WHERE
ENR.PROD_SK = PC.PROD_SK
AND ENR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK = CD.CD_MPPNG_SK 
AND ENR.ELIG_IN = 'Y' 
AND CD.TRGT_CD = 'AHY' 
AND PC.PROD_CMPNT_PFX_ID LIKE 'HY%'
AND ENR.EFF_DT_SK <=  '{CurrDate}' 
AND ENR.TERM_DT_SK >=  '{CurrDate}' 
AND PC.PROD_CMPNT_EFF_DT_SK <=  '{CurrDate}'
AND PC.PROD_CMPNT_TERM_DT_SK >=  '{CurrDate}' 
"""
    )
    .load()
)

df_db2_Esi_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"""SELECT DISTINCT
CMPNT.PROD_SK,
CMPNT.PROD_CMPNT_PFX_ID,
'Y' AS Esi_In
FROM
{IDSOwner}.PROD_CMPNT CMPNT, 
{IDSOwner}.CD_MPPNG CD1
WHERE 
CMPNT.PROD_CMPNT_TYP_CD_SK=CD1.CD_MPPNG_SK 
AND CD1.TRGT_CD='BPL' 
AND CMPNT.PROD_CMPNT_EFF_DT_SK <= '{CurrDate}'
AND CMPNT.PROD_CMPNT_TERM_DT_SK >= '{CurrDate}'"""
    )
    .load()
)

df_db2_Argus_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"""SELECT DISTINCT
CMPNT.PROD_SK,
'Y' AS Argus_In
FROM 
{IDSOwner}.PROD_CMPNT CMPNT, 
{IDSOwner}.CD_MPPNG CD1, 
{IDSOwner}.PROD_BILL_CMPNT BILL 
WHERE 
CMPNT.PROD_CMPNT_TYP_CD_SK=CD1.CD_MPPNG_SK 
AND CD1.TRGT_CD='PDBL' 
AND CMPNT.PROD_CMPNT_EFF_DT_SK <= '{CurrDate}'
AND CMPNT.PROD_CMPNT_TERM_DT_SK >= '{CurrDate}'
AND CMPNT.PROD_CMPNT_PFX_ID = BILL.PROD_CMPNT_PFX_ID
AND BILL.PROD_BILL_CMPNT_EFF_DT_SK <= '{CurrDate}'
AND BILL.PROD_BILL_CMPNT_TERM_DT_SK >= '{CurrDate}'
AND BILL.PROD_BILL_CMPNT_ID IN ('RX01','RXO1')"""
    )
    .load()
)

df_db2_ND_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"""SELECT DISTINCT
CMPNT.PROD_SK,
'Y' AS ND_In
FROM
{IDSOwner}.PROD_CMPNT CMPNT, 
{IDSOwner}.CD_MPPNG CD1, 
{IDSOwner}.PROD_BILL_CMPNT BILL 
WHERE 
CMPNT.PROD_CMPNT_TYP_CD_SK=CD1.CD_MPPNG_SK 
AND CD1.TRGT_CD='PDBL' 
AND CMPNT.PROD_CMPNT_EFF_DT_SK <= '{CurrDate}'
AND CMPNT.PROD_CMPNT_TERM_DT_SK >= '{CurrDate}'
AND CMPNT.PROD_CMPNT_PFX_ID = BILL.PROD_CMPNT_PFX_ID
AND BILL.PROD_BILL_CMPNT_EFF_DT_SK <= '{CurrDate}'
AND BILL.PROD_BILL_CMPNT_TERM_DT_SK >= '{CurrDate}'
AND BILL.PROD_BILL_CMPNT_ID IN ('ND01','NDO1')"""
    )
    .load()
)

df_db2_Pcs_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"""SELECT DISTINCT
CMPNT.PROD_SK,
'Y' AS Pcs_In
FROM
{IDSOwner}.PROD_CMPNT CMPNT, 
{IDSOwner}.CD_MPPNG CD1, 
{IDSOwner}.PROD_BILL_CMPNT BILL 
WHERE 
CMPNT.PROD_CMPNT_TYP_CD_SK=CD1.CD_MPPNG_SK 
AND CD1.TRGT_CD='PDBL' 
AND CMPNT.PROD_CMPNT_EFF_DT_SK <= '{CurrDate}'
AND CMPNT.PROD_CMPNT_TERM_DT_SK >= '{CurrDate}'
AND CMPNT.PROD_CMPNT_PFX_ID = BILL.PROD_CMPNT_PFX_ID
AND BILL.PROD_BILL_CMPNT_EFF_DT_SK <= '{CurrDate}'
AND BILL.PROD_BILL_CMPNT_TERM_DT_SK >= '{CurrDate}'
AND BILL.PROD_BILL_CMPNT_ID IN ('RX03','RXO3')"""
    )
    .load()
)

df_db2_MBR_ENR = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"""SELECT DISTINCT
ENR.MBR_UNIQ_KEY,
ENR.PROD_SK 
FROM 
{IDSOwner}.MBR_ENR ENR, 
{IDSOwner}.CD_MPPNG CD
WHERE
ENR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK = CD.CD_MPPNG_SK 
AND ENR.EFF_DT_SK <= '{CurrDate}' 
AND ENR.TERM_DT_SK >= '{CurrDate}' 
AND ENR.ELIG_IN = 'Y' 
AND CD.TRGT_CD = 'MED'"""
    )
    .load()
)

df_db2_p_drug_prod_cmpnt_pfx_excl = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"""SELECT 
PROD_CMPNT_PFX_ID, 
'Y' as PROD_CMPNT_PFX_IND
FROM {IDSOwner}.P_DRUG_PROD_CMPNT_PFX_EXCL"""
    )
    .load()
)

df_db2_P_ALNO_HOME_PG_ATTRBTN = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"""SELECT 
MBR.MBR_UNIQ_KEY,
AHPA.ALNO_HOME_PG_ID,
MPA.ROW_EFF_DT_SK
FROM
{IDSOwner}.MBR MBR,
{IDSOwner}.MBR_ENR MBR_ENR,
{IDSOwner}.P_ALNO_HOME_PG_ATTRBTN AHPA,
{IDSOwner}.MBR_PCP_ATTRBTN MPA,
{IDSOwner}.CD_MPPNG CD1,
{IDSOwner}.CD_MPPNG CD2,
(SELECT  MAX(ROW_EFF_DT_SK) AS ROW_EFF_DT_SK  FROM {IDSOwner}.MBR_PCP_ATTRBTN) AS MPA1
WHERE
MBR.MBR_UNIQ_KEY = MBR_ENR.MBR_UNIQ_KEY AND
MBR_ENR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK = CD1.CD_MPPNG_SK AND
MPA.SRC_SYS_CD_SK = CD2.CD_MPPNG_SK AND
CD1.TRGT_CD = 'MED' AND
MBR_ENR.ELIG_IN = 'Y' AND
MBR_ENR.EFF_DT_SK <= '{CurrDate}' AND
MBR_ENR.TERM_DT_SK >= '{CurrDate}' AND
MPA.MBR_UNIQ_KEY = MBR_ENR.MBR_UNIQ_KEY AND
MPA.ROW_EFF_DT_SK = MPA1.ROW_EFF_DT_SK AND
MPA.REL_GRP_PROV_SK=AHPA.PROV_GRP_PROV_SK AND
AHPA.TERM_DT_SK >= '{CurrDate}' AND
CD2.TRGT_CD = 'BCBSKC'"""
    )
    .load()
)

df_Srt_Mbr_Uniq_Key_Alno = df_db2_P_ALNO_HOME_PG_ATTRBTN.orderBy(
    F.col("MBR_UNIQ_KEY").asc(), F.col("ROW_EFF_DT_SK").asc()
)

df_Rmd_Alno_Hm_Pg_Id = dedup_sort(
    df_Srt_Mbr_Uniq_Key_Alno,
    ["MBR_UNIQ_KEY"],
    [("ROW_EFF_DT_SK", "A")]
)

df_Rmd_Alno_Hm_Pg_Id = df_Rmd_Alno_Hm_Pg_Id.select(
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("ALNO_HOME_PG_ID").alias("ALNO_HOME_PG_ID"),
    F.col("ROW_EFF_DT_SK").alias("ROW_EFF_DT_SK")
)

df_db2_Mbr_Pcp_Attrbtn = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"""SELECT 
MBR.MBR_UNIQ_KEY,
MPA.ROW_EFF_DT_SK,
MPA.MED_HOME_ID
FROM
{IDSOwner}.MBR MBR,
{IDSOwner}.MBR_ENR MBR_ENR,
{IDSOwner}.MBR_PCP_ATTRBTN MPA,
{IDSOwner}.CD_MPPNG CD1,
{IDSOwner}.CD_MPPNG CD2,
(SELECT  MAX(ROW_EFF_DT_SK) AS ROW_EFF_DT_SK  FROM {IDSOwner}.MBR_PCP_ATTRBTN) AS MPA1
WHERE
MBR.MBR_UNIQ_KEY = MBR_ENR.MBR_UNIQ_KEY AND
MBR_ENR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK = CD1.CD_MPPNG_SK AND
CD1.TRGT_CD = 'MED' AND
MBR_ENR.ELIG_IN = 'Y' AND
MBR_ENR.EFF_DT_SK <= '{CurrDate}' AND
MBR_ENR.TERM_DT_SK >= '{CurrDate}' AND
MPA.MBR_UNIQ_KEY = MBR_ENR.MBR_UNIQ_KEY AND
MPA.ROW_EFF_DT_SK = MPA1.ROW_EFF_DT_SK AND
MPA.SRC_SYS_CD_SK = CD2.CD_MPPNG_SK AND
CD2.TRGT_CD = 'BCBSKC'"""
    )
    .load()
)

df_Srt_Mbr_Uniq_Key_Pcmh = df_db2_Mbr_Pcp_Attrbtn.orderBy(
    F.col("MBR_UNIQ_KEY").asc(), F.col("ROW_EFF_DT_SK").asc()
)

df_Rmd_Pcmh_Ind = dedup_sort(
    df_Srt_Mbr_Uniq_Key_Pcmh,
    ["MBR_UNIQ_KEY"],
    [("ROW_EFF_DT_SK", "A")]
)

df_Rmd_Pcmh_Ind = df_Rmd_Pcmh_Ind.select(
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("ROW_EFF_DT_SK").alias("ROW_EFF_DT_SK"),
    F.col("MED_HOME_ID").alias("MED_HOME_ID")
)

df_db2_Mbr_Dntl_Rwrd_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"""SELECT DISTINCT
MBR.MBR_UNIQ_KEY,
CD2.TRGT_CD
FROM
{IDSOwner}.MBR MBR,
{IDSOwner}.MBR_ENR MBR_ENR,
{IDSOwner}.PROD_CMPNT CMPNT,
{IDSOwner}.PROD PROD,
{IDSOwner}.CD_MPPNG CD1,
{IDSOwner}.CD_MPPNG CD2
WHERE
MBR.MBR_SK = MBR_ENR.MBR_SK AND
MBR_ENR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK = CD1.CD_MPPNG_SK AND
CD1.TRGT_CD = 'DNTL' AND
MBR_ENR.ELIG_IN = 'Y' AND
MBR_ENR.EFF_DT_SK <= '{CurrDate}' AND
MBR_ENR.TERM_DT_SK > '{CurrDate}' AND
MBR_ENR.PROD_SK = CMPNT.PROD_SK
AND CMPNT.PROD_CMPNT_EFF_DT_SK <= '{CurrDate}'
AND CMPNT.PROD_CMPNT_TERM_DT_SK >= '{CurrDate}'
AND CMPNT.PROD_CMPNT_TYP_CD_SK = CD2.CD_MPPNG_SK
AND CD2.TRGT_CD = 'DNRP'"""
    )
    .load()
)

df_Mbr_Uniq_Key_Dntl_Rwrd = df_db2_Mbr_Dntl_Rwrd_In.orderBy(
    F.col("MBR_UNIQ_KEY").asc(), F.col("TRGT_CD").asc()
)

df_Rmd_Dntl_Rwrd_Ind = dedup_sort(
    df_Mbr_Uniq_Key_Dntl_Rwrd,
    ["MBR_UNIQ_KEY"],
    [("TRGT_CD", "A")]
)

df_Rmd_Dntl_Rwrd_Ind = df_Rmd_Dntl_Rwrd_Ind.select(
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("TRGT_CD").alias("TRGT_CD")
)

df_db2_PROD_CMPNT = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"""SELECT DISTINCT
CMPNT.PROD_SK,
PBC.PROD_CMPNT_PFX_ID,
PBC.PROD_BILL_CMPNT_ID
FROM 
{IDSOwner}.PROD_CMPNT CMPNT, 
{IDSOwner}.PROD_BILL_CMPNT PBC, 
{IDSOwner}.CD_MPPNG CD1
WHERE 
CMPNT.PROD_CMPNT_TYP_CD_SK=CD1.CD_MPPNG_SK 
AND CD1.TRGT_CD='PDBL' 
AND CMPNT.PROD_CMPNT_PFX_ID = PBC.PROD_CMPNT_PFX_ID
AND PBC.PROD_BILL_CMPNT_ID LIKE 'HY%'
AND CMPNT.PROD_CMPNT_EFF_DT_SK <= '{CurrDate}'
AND CMPNT.PROD_CMPNT_TERM_DT_SK >= '{CurrDate}'
AND PBC.PROD_BILL_CMPNT_EFF_DT_SK <= '{CurrDate}'
AND PBC.PROD_BILL_CMPNT_TERM_DT_SK >= '{CurrDate}'"""
    )
    .load()
)

df_xfm_hyxx_in = df_db2_PROD_CMPNT

df_xfm_hyxx_lnk_HY20 = df_xfm_hyxx_in.filter(F.col("PROD_BILL_CMPNT_ID")=="HY20").select(
    F.col("PROD_SK").alias("PROD_SK"),
    F.col("PROD_CMPNT_PFX_ID").alias("PROD_CMPNT_PFX_ID"),
    F.col("PROD_BILL_CMPNT_ID").alias("PROD_BILL_CMPNT_ID")
)
df_xfm_hyxx_lnk_HY30 = df_xfm_hyxx_in.filter(F.col("PROD_BILL_CMPNT_ID")=="HY30").select(
    F.col("PROD_SK").alias("PROD_SK"),
    F.col("PROD_CMPNT_PFX_ID").alias("PROD_CMPNT_PFX_ID"),
    F.col("PROD_BILL_CMPNT_ID").alias("PROD_BILL_CMPNT_ID")
)
df_xfm_hyxx_lnk_HY40 = df_xfm_hyxx_in.filter(F.col("PROD_BILL_CMPNT_ID")=="HY40").select(
    F.col("PROD_SK").alias("PROD_SK"),
    F.col("PROD_CMPNT_PFX_ID").alias("PROD_CMPNT_PFX_ID"),
    F.col("PROD_BILL_CMPNT_ID").alias("PROD_BILL_CMPNT_ID")
)
df_xfm_hyxx_lnk_HY50 = df_xfm_hyxx_in.filter(F.col("PROD_BILL_CMPNT_ID")=="HY50").select(
    F.col("PROD_SK").alias("PROD_SK"),
    F.col("PROD_CMPNT_PFX_ID").alias("PROD_CMPNT_PFX_ID"),
    F.col("PROD_BILL_CMPNT_ID").alias("PROD_BILL_CMPNT_ID")
)
df_xfm_hyxx_lnk_HY60 = df_xfm_hyxx_in.filter(F.col("PROD_BILL_CMPNT_ID")=="HY60").select(
    F.col("PROD_SK").alias("PROD_SK"),
    F.col("PROD_CMPNT_PFX_ID").alias("PROD_CMPNT_PFX_ID"),
    F.col("PROD_BILL_CMPNT_ID").alias("PROD_BILL_CMPNT_ID")
)

df_db2_MBR_ENR_2_alias = df_db2_MBR_ENR_2.alias("lnk_Extr2")
df_xfm_hyxx_lnk_HY20_alias = df_xfm_hyxx_lnk_HY20.alias("lnk_HY20")
df_xfm_hyxx_lnk_HY30_alias = df_xfm_hyxx_lnk_HY30.alias("lnk_HY30")
df_xfm_hyxx_lnk_HY40_alias = df_xfm_hyxx_lnk_HY40.alias("lnk_HY40")
df_xfm_hyxx_lnk_HY50_alias = df_xfm_hyxx_lnk_HY50.alias("lnk_HY50")
df_xfm_hyxx_lnk_HY60_alias = df_xfm_hyxx_lnk_HY60.alias("lnk_HY60")

df_lkp_Lookup_Codes_1 = df_db2_MBR_ENR_2_alias.join(
    df_xfm_hyxx_lnk_HY20_alias,
    (F.col("lnk_Extr2.PROD_SK")==F.col("lnk_HY20.PROD_SK")) &
    (F.col("lnk_Extr2.PROD_CMPNT_PFX_ID")==F.col("lnk_HY20.PROD_CMPNT_PFX_ID")),
    "left"
).select(
    F.col("lnk_Extr2.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("lnk_HY20.PROD_BILL_CMPNT_ID").alias("HY20_PROD_BILL_CMPNT_ID"),
    F.col("lnk_Extr2.PROD_SK").alias("PROD_SK"),
    F.col("lnk_Extr2.PROD_CMPNT_PFX_ID").alias("PROD_CMPNT_PFX_ID")
)

df_lkp_Lookup_Codes_2 = df_lkp_Lookup_Codes_1.alias("lkp1").join(
    df_xfm_hyxx_lnk_HY30_alias,
    (F.col("lkp1.PROD_SK")==F.col("lnk_HY30.PROD_SK")) &
    (F.col("lkp1.PROD_CMPNT_PFX_ID")==F.col("lnk_HY30.PROD_CMPNT_PFX_ID")),
    "left"
).select(
    F.col("lkp1.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("lkp1.HY20_PROD_BILL_CMPNT_ID").alias("HY20_PROD_BILL_CMPNT_ID"),
    F.col("lnk_HY30.PROD_BILL_CMPNT_ID").alias("HY30_PROD_BILL_CMPNT_ID"),
    F.col("lkp1.PROD_SK").alias("PROD_SK"),
    F.col("lkp1.PROD_CMPNT_PFX_ID").alias("PROD_CMPNT_PFX_ID")
)

df_lkp_Lookup_Codes_3 = df_lkp_Lookup_Codes_2.alias("lkp2").join(
    df_xfm_hyxx_lnk_HY40_alias,
    (F.col("lkp2.PROD_SK")==F.col("lnk_HY40.PROD_SK")) &
    (F.col("lkp2.PROD_CMPNT_PFX_ID")==F.col("lnk_HY40.PROD_CMPNT_PFX_ID")),
    "left"
).select(
    F.col("lkp2.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("lkp2.HY20_PROD_BILL_CMPNT_ID").alias("HY20_PROD_BILL_CMPNT_ID"),
    F.col("lkp2.HY30_PROD_BILL_CMPNT_ID").alias("HY30_PROD_BILL_CMPNT_ID"),
    F.col("lnk_HY40.PROD_BILL_CMPNT_ID").alias("HY40_PROD_BILL_CMPNT_ID"),
    F.col("lkp2.PROD_SK").alias("PROD_SK"),
    F.col("lkp2.PROD_CMPNT_PFX_ID").alias("PROD_CMPNT_PFX_ID")
)

df_lkp_Lookup_Codes_4 = df_lkp_Lookup_Codes_3.alias("lkp3").join(
    df_xfm_hyxx_lnk_HY50_alias,
    (F.col("lkp3.PROD_SK")==F.col("lnk_HY50.PROD_SK")) &
    (F.col("lkp3.PROD_CMPNT_PFX_ID")==F.col("lnk_HY50.PROD_CMPNT_PFX_ID")),
    "left"
).select(
    F.col("lkp3.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("lkp3.HY20_PROD_BILL_CMPNT_ID").alias("HY20_PROD_BILL_CMPNT_ID"),
    F.col("lkp3.HY30_PROD_BILL_CMPNT_ID").alias("HY30_PROD_BILL_CMPNT_ID"),
    F.col("lkp3.HY40_PROD_BILL_CMPNT_ID").alias("HY40_PROD_BILL_CMPNT_ID"),
    F.col("lnk_HY50.PROD_BILL_CMPNT_ID").alias("HY50_PROD_BILL_CMPNT_ID"),
    F.col("lkp3.PROD_SK").alias("PROD_SK"),
    F.col("lkp3.PROD_CMPNT_PFX_ID").alias("PROD_CMPNT_PFX_ID")
)

df_lkp_Lookup_Codes_5 = df_lkp_Lookup_Codes_4.alias("lkp4").join(
    df_xfm_hyxx_lnk_HY60_alias,
    (F.col("lkp4.PROD_SK")==F.col("lnk_HY60.PROD_SK")) &
    (F.col("lkp4.PROD_CMPNT_PFX_ID")==F.col("lnk_HY60.PROD_CMPNT_PFX_ID")),
    "left"
).select(
    F.col("lkp4.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("lkp4.HY20_PROD_BILL_CMPNT_ID").alias("HY20_PROD_BILL_CMPNT_ID"),
    F.col("lkp4.HY30_PROD_BILL_CMPNT_ID").alias("HY30_PROD_BILL_CMPNT_ID"),
    F.col("lkp4.HY40_PROD_BILL_CMPNT_ID").alias("HY40_PROD_BILL_CMPNT_ID"),
    F.col("lkp4.HY50_PROD_BILL_CMPNT_ID").alias("HY50_PROD_BILL_CMPNT_ID"),
    F.col("lnk_HY60.PROD_BILL_CMPNT_ID").alias("HY60_PROD_BILL_CMPNT_ID")
)

df_lkp_Lookup_Codes = df_lkp_Lookup_Codes_5.select(
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("HY20_PROD_BILL_CMPNT_ID").alias("HY20_PROD_BILL_CMPNT_ID"),
    F.col("HY30_PROD_BILL_CMPNT_ID").alias("HY30_PROD_BILL_CMPNT_ID"),
    F.col("HY40_PROD_BILL_CMPNT_ID").alias("HY40_PROD_BILL_CMPNT_ID"),
    F.col("HY50_PROD_BILL_CMPNT_ID").alias("HY50_PROD_BILL_CMPNT_ID"),
    F.col("HY60_PROD_BILL_CMPNT_ID").alias("HY60_PROD_BILL_CMPNT_ID")
)

df_xfm_Hy = df_lkp_Lookup_Codes.select(
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.when(
        (F.col("HY20_PROD_BILL_CMPNT_ID").isNull()) | (F.trim(F.col("HY20_PROD_BILL_CMPNT_ID"))==""), 
        F.lit("N")
    ).otherwise(F.lit("Y")).alias("HY20_IND"),
    F.when(
        (F.col("HY30_PROD_BILL_CMPNT_ID").isNull()) | (F.trim(F.col("HY30_PROD_BILL_CMPNT_ID"))==""), 
        F.lit("N")
    ).otherwise(F.lit("Y")).alias("HY30_IND"),
    F.when(
        (F.col("HY40_PROD_BILL_CMPNT_ID").isNull()) | (F.trim(F.col("HY40_PROD_BILL_CMPNT_ID"))==""), 
        F.lit("N")
    ).otherwise(F.lit("Y")).alias("HY40_IND"),
    F.when(
        (F.col("HY50_PROD_BILL_CMPNT_ID").isNull()) | (F.trim(F.col("HY50_PROD_BILL_CMPNT_ID"))==""), 
        F.lit("N")
    ).otherwise(F.lit("Y")).alias("HY50_IND"),
    F.when(
        (F.col("HY60_PROD_BILL_CMPNT_ID").isNull()) | (F.trim(F.col("HY60_PROD_BILL_CMPNT_ID"))==""), 
        F.lit("N")
    ).otherwise(F.lit("Y")).alias("HY60_IND")
)

df_db2_Vsp_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"""SELECT DISTINCT
CMPNT.PROD_SK,
CD2.TRGT_CD
FROM 
{IDSOwner}.PROD_CMPNT CMPNT, 
{IDSOwner}.CD_MPPNG CD1, 
{IDSOwner}.CD_MPPNG CD2, 
{IDSOwner}.BNF_SUM_DTL BNF
WHERE 
CMPNT.PROD_CMPNT_PFX_ID = BNF.PROD_CMPNT_PFX_ID
AND CMPNT.PROD_CMPNT_TYP_CD_SK = CD1.CD_MPPNG_SK 
AND CD1.TRGT_CD='BSBS' 
AND BNF.BNF_SUM_DTL_TYP_CD_SK = CD2.CD_MPPNG_SK 
AND CD2.TRGT_CD IN ( 'VSPE', 'VSPH', 'VSPO' ) 
AND CMPNT.PROD_CMPNT_EFF_DT_SK <= '{CurrDate}'
AND CMPNT.PROD_CMPNT_TERM_DT_SK >= '{CurrDate}'
order by CMPNT.PROD_SK, CD2.TRGT_CD
"""
    )
    .load()
)

df_Transformer_46_in = df_db2_Vsp_In

df_Transformer_46_lnk_VspE = df_Transformer_46_in.filter(F.col("TRGT_CD")=="VSPE").select(
    F.col("PROD_SK").alias("PROD_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.lit("Y").alias("VSPE_Ind")
)
df_Transformer_46_lnk_VspO = df_Transformer_46_in.filter(F.col("TRGT_CD")=="VSPO").select(
    F.col("PROD_SK").alias("PROD_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.lit("Y").alias("VSPO_Ind")
)
df_Transformer_46_lnk_VspH = df_Transformer_46_in.filter(F.col("TRGT_CD")=="VSPH").select(
    F.col("PROD_SK").alias("PROD_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.lit("Y").alias("VSPH_Ind")
)

df_db2_Ahy_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"""SELECT DISTINCT
CMPNT.PROD_SK,
BNF.BNF_SUM_ID
FROM 
{IDSOwner}.PROD_CMPNT CMPNT, 
{IDSOwner}.CD_MPPNG CD1, 
{IDSOwner}.BNF_SUM BNF
WHERE 
CMPNT.PROD_CMPNT_TYP_CD_SK=CD1.CD_MPPNG_SK 
AND CD1.TRGT_CD='BSBS' 
AND CMPNT.PROD_CMPNT_PFX_ID = BNF.PROD_CMPNT_PFX_ID
AND BNF.BNF_SUM_ID IN ( 'AHYP', '5000' )
AND CMPNT.PROD_CMPNT_EFF_DT_SK <= '{CurrDate}'
AND CMPNT.PROD_CMPNT_TERM_DT_SK >= '{CurrDate}'"""
    )
    .load()
)

df_Transformer_48_in = df_db2_Ahy_In

df_Transformer_48_lnk_AhyVal = df_Transformer_48_in.filter(F.col("BNF_SUM_ID")=="5000").select(
    F.col("PROD_SK").alias("PROD_SK"),
    F.col("BNF_SUM_ID").alias("BNF_SUM_ID"),
    F.lit("Y").alias("AhyVal_In")
)
df_Transformer_48_lnk_AhyPrmr = df_Transformer_48_in.filter(F.col("BNF_SUM_ID")=="AHYP").select(
    F.col("PROD_SK").alias("PROD_SK"),
    F.col("BNF_SUM_ID").alias("BNF_SUM_ID"),
    F.lit("Y").alias("AhyPrmr_In")
)

df_db2_MBR_VBB_PLN_ENR = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"""SELECT  
PLN.MBR_UNIQ_KEY,
CD.TRGT_CD,
'Y'  AS  VBB_IN
FROM     {IDSOwner}.MBR_VBB_PLN_ENR PLN,  
{IDSOwner}.VBB_PLN VBB,  
{IDSOwner}.CD_MPPNG CD,
{IDSOwner}.GRP_VBB_PLN_ELIG ELIG
where  
PLN.VBB_PLN_UNIQ_KEY =  VBB.VBB_PLN_UNIQ_KEY   
AND  VBB.VBB_MDL_CD_SK =  CD.CD_MPPNG_SK   
AND VBB.VBB_PLN_TERM_DT_SK >= '{CurrDate}'   
AND PLN.GRP_SK =  ELIG .GRP_SK   
AND PLN.CLS_SK =  ELIG .CLS_SK  
AND PLN.CLS_PLN_SK = ELIG .CLS_PLN_SK  
AND  PLN.VBB_PLN_UNIQ_KEY=  ELIG.VBB_PLN_UNIQ_KEY   
AND ELIG.VBB_PLN_ELIG_END_DT_SK >=   '{CurrDate}'   
AND PLN.MBR_UNIQ_KEY NOT IN (0,1)
group by PLN.MBR_UNIQ_KEY, CD.TRGT_CD   
order by PLN.MBR_UNIQ_KEY, CD.TRGT_CD desc"""
    )
    .load()
)

df_Remove_Duplicates_109_in = df_db2_MBR_VBB_PLN_ENR.orderBy(
    F.col("MBR_UNIQ_KEY").asc(), F.col("TRGT_CD").desc()
)

df_Remove_Duplicates_109 = dedup_sort(
    df_Remove_Duplicates_109_in,
    ["MBR_UNIQ_KEY"],
    [("TRGT_CD","D")]
)

df_Remove_Duplicates_109 = df_Remove_Duplicates_109.select(
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("VBB_IN").alias("VBB_IN")
)

df_Lookup_29_primary = df_db2_MBR_ENR.alias("lnk_Extr")

df_Lookup_29_lnk_Vbb = df_Remove_Duplicates_109.alias("lnk_Vbb")
df_Lookup_29_lnk_Esi = df_db2_Esi_In.alias("lnk_Esi")
df_Lookup_29_lnk_Argus = df_db2_Argus_In.alias("lnk_Argus")
df_Lookup_29_lnk_ND = df_db2_ND_In.alias("lnk_ND")
df_Lookup_29_lnk_Pcs = df_db2_Pcs_In.alias("lnk_Pcs")
df_Lookup_29_lnk_VspE = df_Transformer_46_lnk_VspE.alias("lnk_VspE")
df_Lookup_29_lnk_AhyVal = df_Transformer_48_lnk_AhyVal.alias("lnk_AhyVal")
df_Lookup_29_lnk_VspO = df_Transformer_46_lnk_VspO.alias("lnk_VspO")
df_Lookup_29_lnk_VspH = df_Transformer_46_lnk_VspH.alias("lnk_VspH")
df_Lookup_29_lnk_AhyPrmr = df_Transformer_48_lnk_AhyPrmr.alias("lnk_AhyPrmr")

join_1 = df_Lookup_29_primary.join(
    df_Lookup_29_lnk_Vbb,
    F.col("lnk_Extr.MBR_UNIQ_KEY")==F.col("lnk_Vbb.MBR_UNIQ_KEY"),
    "left"
)
join_2 = join_1.join(
    df_Lookup_29_lnk_Esi,
    F.col("lnk_Extr.PROD_SK")==F.col("lnk_Esi.PROD_SK"),
    "left"
)
join_3 = join_2.join(
    df_Lookup_29_lnk_Argus,
    (F.col("lnk_Extr.PROD_SK")==F.col("lnk_Argus.PROD_SK")) & (F.col("lnk_Extr.MBR_UNIQ_KEY")==F.col("lnk_Argus.MBR_UNIQ_KEY")),
    "left"
)
join_4 = join_3.join(
    df_Lookup_29_lnk_ND,
    (F.col("lnk_Extr.PROD_SK")==F.col("lnk_ND.PROD_SK")) & (F.col("lnk_Extr.MBR_UNIQ_KEY")==F.col("lnk_ND.MBR_UNIQ_KEY")),
    "left"
)
join_5 = join_4.join(
    df_Lookup_29_lnk_Pcs,
    (F.col("lnk_Extr.PROD_SK")==F.col("lnk_Pcs.PROD_SK")) & (F.col("lnk_Extr.MBR_UNIQ_KEY")==F.col("lnk_Pcs.MBR_UNIQ_KEY")),
    "left"
)
join_6 = join_5.join(
    df_Lookup_29_lnk_VspE,
    F.col("lnk_Extr.PROD_SK")==F.col("lnk_VspE.PROD_SK"),
    "left"
)
join_7 = join_6.join(
    df_Lookup_29_lnk_AhyVal,
    (F.col("lnk_Extr.PROD_SK")==F.col("lnk_AhyVal.PROD_SK")) & (F.col("lnk_Extr.MBR_UNIQ_KEY")==F.col("lnk_AhyVal.MBR_UNIQ_KEY")),
    "left"
)
join_8 = join_7.join(
    df_Lookup_29_lnk_VspO,
    F.col("lnk_Extr.PROD_SK")==F.col("lnk_VspO.PROD_SK"),
    "left"
)
join_9 = join_8.join(
    df_Lookup_29_lnk_VspH,
    F.col("lnk_Extr.PROD_SK")==F.col("lnk_VspH.PROD_SK"),
    "left"
)
join_10 = join_9.join(
    df_Lookup_29_lnk_AhyPrmr,
    (F.col("lnk_Extr.PROD_SK")==F.col("lnk_AhyPrmr.PROD_SK")) & (F.col("lnk_Extr.MBR_UNIQ_KEY")==F.col("lnk_AhyPrmr.MBR_UNIQ_KEY")),
    "left"
)

df_Lookup_29 = join_10.select(
    F.col("lnk_Extr.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY_Esi"),
    F.col("lnk_Esi.PROD_CMPNT_PFX_ID").alias("PROD_CMPNT_PFX_ID_Esi"),
    F.col("lnk_Esi.PROD_SK").alias("PROD_SK_Esi"),
    F.col("lnk_Argus.PROD_SK").alias("PROD_SK_Argus"),
    F.col("lnk_ND.PROD_SK").alias("PROD_SK_ND"),
    F.col("lnk_Pcs.PROD_SK").alias("PROD_SK_Pcs"),
    F.col("lnk_VspE.PROD_SK").alias("PROD_SK_VspE"),
    F.col("lnk_VspE.TRGT_CD").alias("TRGT_CD_VspE"),
    F.col("lnk_AhyVal.PROD_SK").alias("PROD_SK_AhyVal"),
    F.col("lnk_AhyVal.BNF_SUM_ID").alias("BNF_SUM_ID_AhyVal"),
    F.col("lnk_VspO.PROD_SK").alias("PROD_SK_VspO"),
    F.col("lnk_VspO.TRGT_CD").alias("TRGT_CD_VspO"),
    F.col("lnk_VspH.PROD_SK").alias("PROD_SK_VspH"),
    F.col("lnk_VspH.TRGT_CD").alias("TRGT_CD_VspH"),
    F.col("lnk_AhyPrmr.PROD_SK").alias("PROD_SK_AhyPrmr"),
    F.col("lnk_AhyPrmr.BNF_SUM_ID").alias("BNF_SUM_ID_AhyPrmr"),
    F.col("lnk_Extr.PROD_SK").alias("PROD_SK"),
    F.col("lnk_Esi.Esi_In").alias("Esi_In"),
    F.col("lnk_Argus.Argus_In").alias("Argus_In"),
    F.col("lnk_ND.ND_In").alias("ND_In"),
    F.col("lnk_AhyVal.AhyVal_In").alias("AhyVal_In"),
    F.col("lnk_AhyPrmr.AhyPrmr_In").alias("AhyPrmr_In"),
    F.col("lnk_Pcs.Pcs_In").alias("Pcs_In"),
    F.col("lnk_VspO.VSPO_Ind").alias("VSPO_Ind"),
    F.col("lnk_VspH.VSPH_Ind").alias("VSPH_Ind"),
    F.col("lnk_VspE.VSPE_Ind").alias("VSPE_Ind"),
    F.col("lnk_Vbb.VBB_IN").alias("VBB_IN"),
    F.col("lnk_Vbb.TRGT_CD").alias("TRGT_CD")
)

df_Transformer_53_in = df_Lookup_29

df_Transformer_53_lnk_Esi = df_Transformer_53_in.filter(F.col("Esi_In")=="Y").select(
    F.col("MBR_UNIQ_KEY_Esi").alias("MBR_UNIQ_KEY"),
    F.when(
        (F.col("PROD_CMPNT_PFX_ID_Esi").isNull())|(F.col("PROD_CMPNT_PFX_ID_Esi")==""), 
        F.lit("")
    ).otherwise(F.trim(F.col("PROD_CMPNT_PFX_ID_Esi"))).alias("PROD_CMPNT_PFX_ID"),
    F.col("Esi_In").alias("Esi_In")
)

df_Transformer_53_lnk_AhyPrmr = df_Transformer_53_in.filter(F.col("AhyPrmr_In")=="Y").select(
    F.col("MBR_UNIQ_KEY_Esi").alias("MBR_UNIQ_KEY"),
    F.col("AhyPrmr_In").alias("AhyPrmr_In")
)
df_Transformer_53_lnk_AhyVal = df_Transformer_53_in.filter(F.col("AhyVal_In")=="Y").select(
    F.col("MBR_UNIQ_KEY_Esi").alias("MBR_UNIQ_KEY"),
    F.col("AhyVal_In").alias("AhyVal_In")
)
df_Transformer_53_lnk_Pcs = df_Transformer_53_in.filter(F.col("Pcs_In")=="Y").select(
    F.col("MBR_UNIQ_KEY_Esi").alias("MBR_UNIQ_KEY"),
    F.col("Pcs_In").alias("Pcs_In")
)
df_Transformer_53_lnk_ND = df_Transformer_53_in.filter(F.col("ND_In")=="Y").select(
    F.col("MBR_UNIQ_KEY_Esi").alias("MBR_UNIQ_KEY"),
    F.col("ND_In").alias("ND_In")
)
df_Transformer_53_lnk_Argus = df_Transformer_53_in.filter(F.col("Argus_In")=="Y").select(
    F.col("MBR_UNIQ_KEY_Esi").alias("MBR_UNIQ_KEY"),
    F.col("Argus_In").alias("Argus_In")
)
df_Transformer_53_lnk_Vsp = df_Transformer_53_in.select(
    F.col("MBR_UNIQ_KEY_Esi").alias("MBR_UNIQ_KEY"),
    F.when(
        (F.col("TRGT_CD_VspE").isNull())|(F.trim(F.col("TRGT_CD_VspE"))==""), 
        F.lit(None)
    ).otherwise(F.col("TRGT_CD_VspE")).alias("VSPE"),
    F.when(
        (F.col("TRGT_CD_VspO").isNull())|(F.trim(F.col("TRGT_CD_VspO"))==""), 
        F.lit(None)
    ).otherwise(F.col("TRGT_CD_VspO")).alias("VSPO"),
    F.when(
        (F.col("TRGT_CD_VspH").isNull())|(F.trim(F.col("TRGT_CD_VspH"))==""), 
        F.lit(None)
    ).otherwise(F.col("TRGT_CD_VspH")).alias("VSPH"),
    F.lit("Y").alias("VSP_In")
)

df_Transformer_53_lnk_Vbb_In = df_Transformer_53_in.select(
    F.col("MBR_UNIQ_KEY_Esi").alias("MBR_UNIQ_KEY_Vbb"),
    F.col("VBB_IN").alias("VBB_IN"),
    F.col("TRGT_CD").alias("VBB_MDL_CD")
)

df_Lookup_73_lnk_lodProdCmpntPfxId = df_db2_p_drug_prod_cmpnt_pfx_excl.alias("lnk_lodProdCmpntPfxId")
df_Lookup_73_lnk_Esi = df_Transformer_53_lnk_Esi.alias("lnk_Esi")

join_73 = df_Lookup_73_lnk_Esi.join(
    df_Lookup_73_lnk_lodProdCmpntPfxId,
    F.col("lnk_Esi.PROD_CMPNT_PFX_ID")==F.col("lnk_lodProdCmpntPfxId.PROD_CMPNT_PFX_ID"),
    "left"
)

df_Lookup_73 = join_73.select(
    F.col("lnk_Esi.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("lnk_Esi.Esi_In").alias("Esi_In"),
    F.col("lnk_lodProdCmpntPfxId.PROD_CMPNT_PFX_IND").alias("PROD_CMPNT_PFX_IND")
)

df_Filter_111_in = df_Lookup_73

df_Filter_111 = df_Filter_111_in.select(
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("Esi_In").alias("Esi_In"),
    F.col("PROD_CMPNT_PFX_IND").alias("PROD_CMPNT_PFX_IND")
)

df_db2_MBR_ENR_4 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"""with HF_sub_data as (
SELECT
MBR.MBR_UNIQ_KEY
,'HF' AS WELNS_BNF_VNDR_ID
,MBR.BRTH_DT_SK
,MBR.sub_Sk
,SG.SUBGRP_ID
,grp.grp_sk
,grp.grp_id
,case
 when ENR.TERM_DT_SK >= '{CurrDate}'
 then 3
 when ENR.TERM_DT_SK BETWEEN '{CurrDateMinus30}' AND '{CurrDate}'
 then 9
 end AS Order
FROM
{IDSOwner}.MBR MBR
,{IDSOwner}.MBR_ENR ENR
,{IDSOwner}.GRP GRP
,{IDSOwner}.CLS_PLN CLS
,{IDSOwner}.CD_MPPNG CD1
,{IDSOwner}.CD_MPPNG CD2
,{IDSOwner}.SUBGRP SG
WHERE
MBR.MBR_SK = ENR.MBR_SK
AND ENR.GRP_SK = GRP.GRP_SK
AND ENR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK = CD1.CD_MPPNG_SK
AND ENR.CLS_PLN_SK = CLS.CLS_PLN_SK
AND MBR.MBR_RELSHP_CD_SK = CD2.CD_MPPNG_SK
AND MBR.SUBGRP_SK = SG.SUBGRP_SK AND ENR.GRP_SK = SG.GRP_SK
AND CD2.TRGT_CD ='SUB'
AND ENR.EFF_DT_SK <= '{CurrDate}'
AND (
ENR.TERM_DT_SK >= '{CurrDate}'
 or ENR.TERM_DT_SK BETWEEN '{CurrDateMinus30}' AND '{CurrDate}'
)
AND ENR.ELIG_IN = 'Y'
AND CLS.CLS_PLN_ID IN ('AHY30000', 'AHY40000', 'AHY50000','AHY30001', 'AHY40001', 'AHY50001')
),
HF_spouce_data as (
SELECT
MBR.MBR_UNIQ_KEY
,'HF' AS WELNS_BNF_VNDR_ID
,MBR.BRTH_DT_SK
,grp.grp_id
,SG.SUBGRP_ID
,case
 when ENR.TERM_DT_SK >= '{CurrDate}'
 then 4
 when ENR.TERM_DT_SK BETWEEN '{CurrDateMinus30}' AND '{CurrDate}'
 then 10
 end AS Order
FROM
{IDSOwner}.MBR MBR
,{IDSOwner}.MBR_ENR ENR
,{IDSOwner}.GRP GRP
,{IDSOwner}.CLS_PLN CLS
,{IDSOwner}.CD_MPPNG CD1
,{IDSOwner}.CD_MPPNG CD2
,{IDSOwner}.SUBGRP SG
WHERE
MBR.MBR_SK = ENR.MBR_SK
AND ENR.GRP_SK = GRP.GRP_SK
AND ENR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK = CD1.CD_MPPNG_SK
AND ENR.CLS_PLN_SK = CLS.CLS_PLN_SK
AND MBR.MBR_RELSHP_CD_SK = CD2.CD_MPPNG_SK
AND MBR.SUBGRP_SK = SG.SUBGRP_SK AND ENR.GRP_SK = SG.GRP_SK
AND CD2.TRGT_CD = 'SPOUSE'
AND ENR.EFF_DT_SK <= '{CurrDate}'
AND (
ENR.TERM_DT_SK >= '{CurrDate}'
 or ENR.TERM_DT_SK BETWEEN '{CurrDateMinus30}' AND '{CurrDate}'
)
AND ENR.ELIG_IN = 'Y'
AND CLS.CLS_PLN_ID IN ('AHY30000', 'AHY40000', 'AHY50000')
),
HF_dpndt_data as (
SELECT
MBR.MBR_UNIQ_KEY
,'HF' AS WELNS_BNF_VNDR_ID
,MBR.BRTH_DT_SK
,SG.SUBGRP_ID
,grp.grp_id
,case
 when hfsub.order = 3
 then 5
 when hfsub.order = 9
 then 11
 end AS Order
FROM
{IDSOwner}.MBR MBR
,{IDSOwner}.MBR_ENR ENR
,{IDSOwner}.GRP GRP
,{IDSOwner}.CLS_PLN CLS
,{IDSOwner}.CD_MPPNG CD1
,{IDSOwner}.CD_MPPNG CD2
,{IDSOwner}.SUBGRP SG
,(select sub_sk,grp_sk,order from HF_sub_data) hfsub
WHERE
MBR.MBR_SK = ENR.MBR_SK
AND ENR.GRP_SK = GRP.GRP_SK
AND ENR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK = CD1.CD_MPPNG_SK
AND ENR.CLS_PLN_SK = CLS.CLS_PLN_SK
AND MBR.MBR_RELSHP_CD_SK = CD2.CD_MPPNG_SK
AND MBR.SUBGRP_SK = SG.SUBGRP_SK AND ENR.GRP_SK = SG.GRP_SK
AND CD2.TRGT_CD = 'DPNDT'
AND ENR.EFF_DT_SK <= '{CurrDate}'
AND (
ENR.TERM_DT_SK >= '{CurrDate}'
 or ENR.TERM_DT_SK BETWEEN '{CurrDateMinus30}' AND '{CurrDate}'
)
AND ENR.ELIG_IN = 'Y'
and mbr.sub_sk = hfsub.sub_sk
and grp.grp_sk = hfsub.grp_sk
),
hm_dp_data as (
SELECT
MBR.MBR_UNIQ_KEY
,'HM' AS WELNS_BNF_VNDR_ID
,MBR.BRTH_DT_SK
,SG.SUBGRP_ID
,grp.grp_id
,1 AS Order
FROM
{IDSOwner}.MBR MBR
,{IDSOwner}.MBR_ENR ENR
,{IDSOwner}.GRP GRP
,{IDSOwner}.SUB SUB
,{IDSOwner}.PROD PROD
,{IDSOwner}.CD_MPPNG CD
,{IDSOwner}.SUBGRP SG
,{IDSOwner}.PROD_SH_NM PROD_SH_NM
WHERE
MBR.MBR_SK = ENR.MBR_SK
AND MBR.SUB_SK = SUB.SUB_SK
AND SUB.GRP_SK = GRP.GRP_SK
AND ENR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK = CD.CD_MPPNG_SK
AND MBR.SUBGRP_SK = SG.SUBGRP_SK AND ENR.GRP_SK = SG.GRP_SK
AND CD.TRGT_CD ='MED'
AND ENR.ELIG_IN = 'Y'
AND GRP.GRP_ID = '10001000'
AND MBR.HOST_MBR_IN = 'N'
AND ENR.PROD_SK = PROD.PROD_SK
AND PROD.PROD_SH_NM_SK = PROD_SH_NM.PROD_SH_NM_SK
AND PROD_SH_NM.MCARE_SUPLMT_COV_IN = 'N'
AND ENR.EFF_DT_SK <= '{CurrDate}'
AND (
ENR.TERM_DT_SK >= '{CurrDate}'
 OR ENR.TERM_DT_SK BETWEEN '{CurrDateMinus30}' AND '{CurrDate}'
)
),
hm_small_Data as (
SELECT
MBR.MBR_UNIQ_KEY,
'HM' AS WELNS_BNF_VNDR_ID,
MBR.BRTH_DT_SK,
SG.SUBGRP_ID,
grp.grp_id,
2 AS Order
FROM
{IDSOwner}.MBR MBR,
{IDSOwner}.MBR_ENR ENR,
{IDSOwner}.GRP GRP,
{IDSOwner}.SUB SUB,
{IDSOwner}.CD_MPPNG CD1,
{IDSOwner}.CD_MPPNG CD2,
{IDSOwner}.SUBGRP SG
WHERE
MBR.MBR_SK = ENR.MBR_SK
AND MBR.SUB_SK = SUB.SUB_SK
AND SUB.GRP_SK = GRP.GRP_SK AND
ENR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK = CD1.CD_MPPNG_SK AND
MBR.SUBGRP_SK = SG.SUBGRP_SK AND ENR.GRP_SK = SG.GRP_SK AND
CD1.TRGT_CD = 'MED' AND
GRP.GRP_MKT_SIZE_CAT_CD_SK = CD2.CD_MPPNG_SK AND
CD2.TRGT_CD IN ('SMGRP1','SMGRP2','MDGRP1','MDGRP2') AND
MBR.HOST_MBR_IN = 'N' AND
ENR.ELIG_IN = 'Y' AND
((ENR.EFF_DT_SK <= '{CurrDate}' AND ENR.TERM_DT_SK >= '{CurrDate}') OR (ENR.EFF_DT_SK <= '{CurrDate}' AND ENR.TERM_DT_SK BETWEEN '{CurrDateMinus30}' AND '{CurrDate}'))
),
HM_raw_data as (
SELECT
MBR.MBR_UNIQ_KEY
,'HM' AS WELNS_BNF_VNDR_ID
,MBR.BRTH_DT_SK
,SG.SUBGRP_ID
,CD2.TRGT_CD
,mbr.sub_sk
,grp.grp_sk
,grp.grp_id
,case
 when CD2.TRGT_CD ='SUB' AND ENR.TERM_DT_SK >= '{CurrDate}'
 then 6
 when CD2.TRGT_CD ='SPOUSE' AND ENR.TERM_DT_SK >= '{CurrDate}'
 then 7
 when CD2.TRGT_CD ='DPNDT' AND ENR.TERM_DT_SK >= '{CurrDate}'
 then 8
 when CD2.TRGT_CD ='SUB' AND (ENR.TERM_DT_SK BETWEEN '{CurrDateMinus30}' AND '{CurrDate}')
 then 12
 when CD2.TRGT_CD ='SPOUSE' AND (ENR.TERM_DT_SK BETWEEN '{CurrDateMinus30}' AND '{CurrDate}')
 then 13
 when CD2.TRGT_CD ='DPNDT' AND (ENR.TERM_DT_SK BETWEEN '{CurrDateMinus30}' AND '{CurrDate}')
 then 14
 end as Order
FROM
{IDSOwner}.MBR MBR
,{IDSOwner}.MBR_ENR ENR
,{IDSOwner}.GRP GRP
,{IDSOwner}.SUB SUB
,{IDSOwner}.CD_MPPNG CD1
,{IDSOwner}.CD_MPPNG CD2
,{IDSOwner}.SUBGRP SG
,{IDSOwner}.PRNT_GRP PRNT
,{IDSOwner}.CLS_PLN CLS
WHERE
MBR.MBR_SK = ENR.MBR_SK
AND MBR.SUB_SK = SUB.SUB_SK
AND SUB.GRP_SK = GRP.GRP_SK
AND GRP.PRNT_GRP_SK = PRNT.PRNT_GRP_SK
AND ENR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK = CD1.CD_MPPNG_SK
AND MBR.SUBGRP_SK = SG.SUBGRP_SK AND ENR.GRP_SK = SG.GRP_SK
AND ENR.CLS_PLN_SK = CLS.CLS_PLN_SK
AND (CD1.TRGT_CD = 'MED' OR CLS.CLS_PLN_ID IN ('AHY30000', 'AHY40000', 'AHY50000','AHY30001', 'AHY40001', 'AHY50001'))
AND MBR.MBR_RELSHP_CD_SK = CD2.CD_MPPNG_SK
AND MBR.HOST_MBR_IN = 'N'
AND ENR.ELIG_IN = 'Y'
AND PRNT.PRNT_GRP_ID <> '650600000'
AND GRP.GRP_ID <> '10023000'
and ENR.EFF_DT_SK <= '{CurrDate}'
AND (
ENR.TERM_DT_SK >= '{CurrDate}'
 OR ENR.TERM_DT_SK BETWEEN '{CurrDateMinus30}' AND '{CurrDate}'
)
),
hm_data as (
select
MBR_UNIQ_KEY
,WELNS_BNF_VNDR_ID
,BRTH_DT_SK
,SUBGRP_ID
,grp_id
,order
from
HM_raw_data a
where
TRGT_CD in ('SUB', 'SPOUSE')
or (
TRGT_CD = 'DPNDT'
and exists (
select
1
from
hm_raw_data b
where
a.sub_sk = b.sub_sk
and a.grp_sk = b.grp_sk
and b.trgt_Cd = 'SUB'
and b.WELNS_BNF_VNDR_ID = 'HM'
)
)
)
select
MBR_UNIQ_KEY
,WELNS_BNF_VNDR_ID
,BRTH_DT_SK
,order
,grp_id
,SUBGRP_ID
from
HF_sub_data
union
select
MBR_UNIQ_KEY
,WELNS_BNF_VNDR_ID
,BRTH_DT_SK
,order
,grp_id
,SUBGRP_ID
from
HF_spouce_data
union
select
MBR_UNIQ_KEY
,WELNS_BNF_VNDR_ID
,BRTH_DT_SK
,order
,grp_id
,SUBGRP_ID
from
HF_dpndt_data
union
select
MBR_UNIQ_KEY
,WELNS_BNF_VNDR_ID
,BRTH_DT_SK
,order
,grp_id
,SUBGRP_ID
from
hm_dp_data
union
select
MBR_UNIQ_KEY
,WELNS_BNF_VNDR_ID
,BRTH_DT_SK
,order
,grp_id
,SUBGRP_ID
from
hm_small_Data
union
select
MBR_UNIQ_KEY
,WELNS_BNF_VNDR_ID
,BRTH_DT_SK
,order
,grp_id
,SUBGRP_ID
from
hm_data
order by
MBR_UNIQ_KEY
,order desc
"""
    )
    .load()
)

df_GRP_AHY_PGM = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"""SELECT A.GRP_ID, A.GRP_AHY_PGM_STRT_DT_SK AS GRP_AHY_PGM_STRT_DT_SK
FROM
{IDSOwner}.GRP_AHY_PGM A,
(
SELECT DISTINCT G.GRP_ID, max(G.GRP_AHY_PGM_STRT_DT_SK) as GRP_AHY_PGM_STRT_DT_SK
FROM
{IDSOwner}.GRP_AHY_PGM G,
{IDSOwner}.CD_MPPNG CD1
WHERE
G.SRC_SYS_CD_SK = CD1.CD_MPPNG_SK AND
CD1.TRGT_CD = 'CRM' AND
G.GRP_ID <> '10001000' AND
G.GRP_AHY_PGM_STRT_DT_SK >= '2016-01-01' 
AND G.GRP_AHY_PGM_STRT_DT_SK <= '{CurrDate}'
group by
G.GRP_ID
) B
WHERE
A.GRP_ID = B.GRP_ID AND
A.GRP_AHY_PGM_STRT_DT_SK = B.GRP_AHY_PGM_STRT_DT_SK AND
((A.GRP_AHY_PGM_OPT_OUT_IN = '0' Or A.GRP_AHY_PGM_OPT_OUT_IN is NULL) OR A.GRP_SEL_SUB_AHY_ONLY_PGM_ID = 'YES')"""
    )
    .load()
)

df_Transformer_125_in = df_GRP_AHY_PGM

df_Transformer_125_out = df_Transformer_125_in.select(
    F.col("GRP_ID").alias("GRP_ID"),
    F.col("GRP_AHY_PGM_STRT_DT_SK").alias("Live_Date")
)

df_Lookup_126_lnk_MbrEnrOrder = df_db2_MBR_ENR_4.alias("lnk_MbrEnrOrder")
df_Lookup_126_DSLink132 = df_Transformer_125_out.alias("DSLink132")

join_126 = df_Lookup_126_lnk_MbrEnrOrder.join(
    df_Lookup_126_DSLink132,
    F.col("lnk_MbrEnrOrder.GRP_ID")==F.col("DSLink132.GRP_ID"),
    "left"
).select(
    F.col("lnk_MbrEnrOrder.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("lnk_MbrEnrOrder.WELNS_BNF_VNDR_ID").alias("WELNS_BNF_VNDR_ID"),
    F.col("lnk_MbrEnrOrder.BRTH_DT_SK").alias("BRTH_DT_SK"),
    F.col("lnk_MbrEnrOrder.Order").alias("Order"),
    F.col("DSLink132.GRP_ID").alias("GRP_ID"),
    F.col("DSLink132.Live_Date").alias("Live_Date"),
    F.col("lnk_MbrEnrOrder.GRP_ID").alias("GRP_ID_MAIN"),
    F.col("lnk_MbrEnrOrder.SUBGRP_ID").alias("SUBGRP_ID")
)

df_xfm_AgeConstraint_1 = join_126.withColumn(
    "svAgeCheck",
    F.when(
        AGE.EE(F.col("BRTH_DT_SK"), CurrDate)>=18,
        "Y"
    ).otherwise("N")
)

df_xfm_AgeConstraint_2 = df_xfm_AgeConstraint_1.withColumn(
    "svWelnsId",
    F.when(
        (F.trim(F.col("GRP_ID_MAIN"))=="10025001") |
        (F.trim(F.col("GRP_ID_MAIN"))=="24166001") |
        (F.trim(F.col("GRP_ID_MAIN"))=="32478001") |
        (F.trim(F.col("GRP_ID_MAIN"))=="27350001") |
        (F.trim(F.col("GRP_ID_MAIN"))=="10005000"),
        "NA"
    ).when(
        (F.trim(F.col("GRP_ID_MAIN"))=="10001000") & ((F.trim(F.col("SUBGRP_ID"))=="0001") | (F.trim(F.col("SUBGRP_ID"))=="0002")),
        "HM"
    ).when(
        (F.trim(F.col("GRP_ID"))==""),
        F.when(
            (F.col("Order")>2) &
            (F.col("WELNS_BNF_VNDR_ID")=="HF"),
            "HF"
        ).otherwise(
            F.when(
                F.col("Order")<=2,
                "HM"
            ).otherwise("NA")
        )
    ).otherwise(
        F.when(
            (F.col("Live_Date").isNotNull()) & (F.col("Live_Date")!="") & (CurrDate>=F.col("Live_Date")) & (F.col("WELNS_BNF_VNDR_ID")=="HM"),
            "HM"
        ).otherwise(
            F.when(
                (F.col("Live_Date").isNotNull()) & (F.col("Live_Date")!="") & (CurrDate>=F.col("Live_Date")),
                "NA"
            ).otherwise(
                F.when(
                    (F.col("WELNS_BNF_VNDR_ID")=="HM"),
                    "NA"
                ).otherwise(
                    F.col("WELNS_BNF_VNDR_ID")
                )
            )
        )
    )
)

df_xfm_AgeConstraint_out = df_xfm_AgeConstraint_2.filter(F.col("svAgeCheck")=="Y").select(
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("svWelnsId").alias("WELNS_BNF_VNDR_ID"),
    F.when(
        F.col("svWelnsId")=="NA",
        F.lit(17)
    ).otherwise(F.col("Order")).alias("Order")
)

df_dup_Mbr_Sk_in = df_xfm_AgeConstraint_out.orderBy(
    F.col("MBR_UNIQ_KEY").asc(),
    F.col("Order").desc()
)

df_dup_Mbr_Sk = dedup_sort(
    df_dup_Mbr_Sk_in,
    ["MBR_UNIQ_KEY"],
    [("Order", "D")]
)

df_dup_Mbr_Sk = df_dup_Mbr_Sk.select(
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("WELNS_BNF_VNDR_ID").alias("WELNS_BNF_VNDR_ID"),
    F.col("Order").alias("Order")
)

df_Lookup_1_primary = df_db2_MBR.alias("lnk_Extr")
df_Lookup_1_lnk_BnfVndInd = df_dup_Mbr_Sk.alias("lnk_BnfVndInd")
df_Lookup_1_lnk_EapBnfLvl = df_db2_MBR_ENR_3.alias("lnk_EapBnfLvl")
df_Lookup_1_lnk_hyXX = df_xfm_Hy.alias("lnk_hyXX")
df_Lookup_1_lnk_AhyPrmr = df_Transformer_53_lnk_AhyPrmr.alias("lnk_AhyPrmr")
df_Lookup_1_lnk_AhyVal = df_Transformer_53_lnk_AhyVal.alias("lnk_AhyVal")
df_Lookup_1_lnk_Pcs = df_Transformer_53_lnk_Pcs.alias("lnk_Pcs")
df_Lookup_1_lnk_ND = df_Transformer_53_lnk_ND.alias("lnk_ND")
df_Lookup_1_lnk_Argus = df_Transformer_53_lnk_Argus.alias("lnk_Argus")
df_Lookup_1_lnk_Vsp = df_Transformer_53_lnk_Vsp.alias("lnk_Vsp")
df_Lookup_1_lnk_ExtractCobMbrUniqKeys = df_db2_COB_MBR.alias("lnk_ExtractCobMbrUniqKeys")
df_Lookup_1_lnk_Vbb_In = df_Transformer_53_lnk_Vbb_In.alias("lnk_Vbb_In")
df_Lookup_1_lnk_OptumLnk = df_Filter_111.alias("lnk_OptumLnk")
df_Lookup_1_lnk_ClsPlnDtl = df_db2_CLS_PLN_DTL.alias("lnk_ClsPlnDtl")
df_Lookup_1_lnk_Alno_Home_Pg_Attrbtn = df_Rmd_Alno_Hm_Pg_Id.alias("lnk_Alno_Home_Pg_Attrbtn")
df_Lookup_1_lnk_Pcmh_Ind = df_Rmd_Pcmh_Ind.alias("lnk_Pcmh_Ind")
df_Lookup_1_lnk_Dntl_Rwrd_Ind = df_Rmd_Dntl_Rwrd_Ind.alias("lnk_Dntl_Rwrd_Ind")
df_Lookup_1_lnk_EyeMed = df_db2_MBR_ENR_5.alias("lnk_EyeMed")

j1 = df_Lookup_1_primary.join(
    df_Lookup_1_lnk_BnfVndInd,
    F.col("lnk_Extr.MBR_UNIQ_KEY")==F.col("lnk_BnfVndInd.MBR_UNIQ_KEY"),
    "left"
)
j2 = j1.join(
    df_Lookup_1_lnk_EapBnfLvl,
    F.col("lnk_Extr.MBR_UNIQ_KEY")==F.col("lnk_EapBnfLvl.MBR_UNIQ_KEY"),
    "left"
)
j3 = j2.join(
    df_Lookup_1_lnk_hyXX,
    F.col("lnk_Extr.MBR_UNIQ_KEY")==F.col("lnk_hyXX.MBR_UNIQ_KEY"),
    "left"
)
j4 = j3.join(
    df_Lookup_1_lnk_AhyPrmr,
    (F.col("lnk_Extr.PROD_SK")==F.col("lnk_AhyPrmr.PROD_SK")) & (F.col("lnk_Extr.MBR_UNIQ_KEY")==F.col("lnk_AhyPrmr.MBR_UNIQ_KEY")),
    "left"
)
j5 = j4.join(
    df_Lookup_1_lnk_AhyVal,
    (F.col("lnk_Extr.PROD_SK")==F.col("lnk_AhyVal.PROD_SK")) & (F.col("lnk_Extr.MBR_UNIQ_KEY")==F.col("lnk_AhyVal.MBR_UNIQ_KEY")),
    "left"
)
j6 = j5.join(
    df_Lookup_1_lnk_Pcs,
    (F.col("lnk_Extr.PROD_SK")==F.col("lnk_Pcs.PROD_SK")) & (F.col("lnk_Extr.MBR_UNIQ_KEY")==F.col("lnk_Pcs.MBR_UNIQ_KEY")),
    "left"
)
j7 = j6.join(
    df_Lookup_1_lnk_ND,
    (F.col("lnk_Extr.PROD_SK")==F.col("lnk_ND.PROD_SK")) & (F.col("lnk_Extr.MBR_UNIQ_KEY")==F.col("lnk_ND.MBR_UNIQ_KEY")),
    "left"
)
j8 = j7.join(
    df_Lookup_1_lnk_Argus,
    (F.col("lnk_Extr.PROD_SK")==F.col("lnk_Argus.PROD_SK")) & (F.col("lnk_Extr.MBR_UNIQ_KEY")==F.col("lnk_Argus.MBR_UNIQ_KEY")),
    "left"
)
j9 = j8.join(
    df_Lookup_1_lnk_Vsp,
    F.col("lnk_Extr.MBR_UNIQ_KEY")==F.col("lnk_Vsp.MBR_UNIQ_KEY"),
    "left"
)
j10 = j9.join(
    df_Lookup_1_lnk_ExtractCobMbrUniqKeys,
    F.col("lnk_Extr.MBR_UNIQ_KEY")==F.col("lnk_ExtractCobMbrUniqKeys.MBR_UNIQ_KEY"),
    "left"
)
j11 = j10.join(
    df_Lookup_1_lnk_Vbb_In,
    F.col("lnk_Extr.MBR_UNIQ_KEY")==F.col("lnk_Vbb_In.MBR_UNIQ_KEY_Vbb"),
    "left"
)
j12 = j11.join(
    df_Lookup_1_lnk_OptumLnk,
    F.col("lnk_Extr.MBR_UNIQ_KEY")==F.col("lnk_OptumLnk.MBR_UNIQ_KEY"),
    "left"
)
j13 = j12.join(
    df_Lookup_1_lnk_ClsPlnDtl,
    F.col("lnk_Extr.MBR_UNIQ_KEY")==F.col("lnk_ClsPlnDtl.MBR_UNIQ_KEY"),
    "left"
)
j14 = j13.join(
    df_Lookup_1_lnk_Alno_Home_Pg_Attrbtn,
    F.col("lnk_Extr.MBR_UNIQ_KEY")==F.col("lnk_Alno_Home_Pg_Attrbtn.MBR_UNIQ_KEY"),
    "left"
)
j15 = j14.join(
    df_Lookup_1_lnk_Pcmh_Ind,
    F.col("lnk_Extr.MBR_UNIQ_KEY")==F.col("lnk_Pcmh_Ind.MBR_UNIQ_KEY"),
    "left"
)
j16 = j15.join(
    df_Lookup_1_lnk_Dntl_Rwrd_Ind,
    F.col("lnk_Extr.MBR_UNIQ_KEY")==F.col("lnk_Dntl_Rwrd_Ind.MBR_UNIQ_KEY"),
    "left"
)
j17 = j16.join(
    df_Lookup_1_lnk_EyeMed,
    F.col("lnk_Extr.MBR_UNIQ_KEY")==F.col("lnk_EyeMed.MBR_UNIQ_KEY"),
    "left"
)

df_Lookup_1 = j17.select(
    F.col("lnk_OptumLnk.Esi_In").alias("MBR_UNIQ_KEY_Optum"),
    F.col("lnk_Argus.Argus_In").alias("MBR_UNIQ_KEY_argus"),
    F.col("lnk_Pcs.Pcs_In").alias("MBR_UNIQ_KEY_Pcs"),
    F.col("lnk_AhyPrmr.AhyPrmr_In").alias("MBR_UNIQ_KEY_AhyPrmr"),
    F.col("lnk_ND.ND_In").alias("MBR_UNIQ_KEY_Nd"),
    F.col("lnk_AhyVal.AhyVal_In").alias("MBR_UNIQ_KEY_AhyVal"),
    F.col("lnk_Vsp.VSPE").alias("VSPE"),
    F.col("lnk_Vsp.VSPO").alias("VSPO"),
    F.col("lnk_Vsp.VSPH").alias("VSPH"),
    F.col("lnk_BnfVndInd.WELNS_BNF_VNDR_ID").alias("WELNS_BNF_VNDR_ID"),
    F.col("lnk_EapBnfLvl.NTWK_CAP_RELSHP_CAT_CD").alias("NTWK_CAP_RELSHP_CAT_CD"),
    F.col("lnk_ExtractCobMbrUniqKeys.COB_IND").alias("MBR_UNIQ_KEY_ExtractCobMbrUniqKeys"),
    F.col("lnk_hyXX.HY20_IND").alias("HY20_IND"),
    F.col("lnk_hyXX.HY30_IND").alias("HY30_IND"),
    F.col("lnk_hyXX.HY40_IND").alias("HY40_IND"),
    F.col("lnk_hyXX.HY50_IND").alias("HY50_IND"),
    F.col("lnk_hyXX.HY60_IND").alias("HY60_IND"),
    F.col("lnk_Extr.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY_Extr"),
    F.col("lnk_Vbb_In.VBB_IN").alias("VBB_IN"),
    F.col("lnk_Vbb_In.VBB_MDL_CD").alias("VBB_MDL_CD"),
    F.col("lnk_ClsPlnDtl.CMPSS_COV_IN").alias("CMPSS_COV_IN"),
    F.col("lnk_Alno_Home_Pg_Attrbtn.ALNO_HOME_PG_ID").alias("ALNO_HOME_PG_ID"),
    F.col("lnk_Pcmh_Ind.MED_HOME_ID").alias("MED_HOME_ID"),
    F.col("lnk_Dntl_Rwrd_Ind.TRGT_CD").alias("DNTL_RWRD_CD"),
    F.col("lnk_EyeMed.EYEMED_IND").alias("EYEMED_IND")
)

df_xfm_BusinessLogic_1 = df_Lookup_1.withColumn(
    "svWellNessBenefitCd",
    F.when(
        F.col("HY20_IND")=="Y","AHYVAL"
    ).otherwise(
        F.when(
            F.col("HY30_IND")=="Y","AHY"
        ).otherwise(
            F.when(
                F.col("HY50_IND")=="Y","AHYPLTNM"
            ).otherwise(
                F.when(
                    F.col("HY40_IND")=="Y","AHYCSTM"
                ).otherwise(
                    F.when(
                        F.col("HY60_IND")=="Y","AHYHRAO"
                    ).otherwise(
                        F.when(
                            F.col("MBR_UNIQ_KEY_AhyPrmr").isNotNull(),"AHYPRMR"
                        ).otherwise(
                            F.when(
                                F.col("MBR_UNIQ_KEY_AhyVal").isNotNull(),"AHYVAL"
                            ).otherwise("NA")
                        )
                    )
                )
            )
        )
    )
)

df_xfm_BusinessLogic_2 = df_xfm_BusinessLogic_1.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("MBR_UNIQ_KEY_Extr").alias("MBR_UNIQ_KEY"),
    F.when(
        F.col("MBR_UNIQ_KEY_Nd").isNotNull(),"NDBH"
    ).otherwise("NA").alias("MNTL_HLTH_VNDR_ID"),
    F.when(
        (F.col("MBR_UNIQ_KEY_Optum").isNull() | (F.trim(F.col("MBR_UNIQ_KEY_Optum"))=="")) &
        (F.col("MBR_UNIQ_KEY_argus").isNull() | (F.trim(F.col("MBR_UNIQ_KEY_argus"))=="")) &
        (F.col("MBR_UNIQ_KEY_Pcs").isNull() | (F.trim(F.col("MBR_UNIQ_KEY_Pcs"))=="")),
        "NA"
    ).otherwise(
        F.when(
            F.col("MBR_UNIQ_KEY_argus").isNotNull(),
            "ARGUS"
        ).otherwise(
            F.when(
                F.col("MBR_UNIQ_KEY_Pcs").isNotNull(),
                "PCS"
            ).otherwise(
                F.when(
                    (F.col("MBR_UNIQ_KEY_Optum").isNotNull()) & (F.lit(CurrDate)<F.lit("2020-01-01")),
                    "ESI"
                ).otherwise(
                    F.when(
                        F.col("MBR_UNIQ_KEY_Optum").isNotNull(),
                        "OPTUMRX"
                    ).otherwise("UNK")
                )
            )
        )
    ).alias("PBM_VNDR_CD"),
    F.col("svWellNessBenefitCd").alias("WELNS_BNF_LVL_CD"),
    F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_NO"),
    F.when(
        (F.col("WELNS_BNF_VNDR_ID").isNull())|(F.trim(F.col("WELNS_BNF_VNDR_ID"))==""),
        "NA"
    ).otherwise(F.col("WELNS_BNF_VNDR_ID")).alias("WELNS_BNF_VNDR_ID"),
    F.when(
        F.col("NTWK_CAP_RELSHP_CAT_CD").isNotNull(),
        F.col("NTWK_CAP_RELSHP_CAT_CD")
    ).otherwise(F.lit("NA")).alias("EAP_BNF_LVL_CD"),
    F.when(
        (F.col("VSPE").isNotNull())|(F.col("VSPH").isNotNull())|(F.col("VSPO").isNotNull()),
        "48460021"
    ).otherwise(
        F.when(
            (F.trim(F.when(F.col("EYEMED_IND").isNotNull(),F.col("EYEMED_IND")).otherwise(""))!=""),
            "EYEMED"
        ).otherwise("NA")
    ).alias("VSN_BNF_VNDR_ID"),
    F.when(
        F.col("VSPE").isNull(),
        "N"
    ).otherwise("Y").alias("VSN_RTN_EXAM_IN"),
    F.when(
        F.col("VSPH").isNull(),
        "N"
    ).otherwise("Y").alias("VSN_HRDWR_IN"),
    F.when(
        F.col("VSPO").isNull(),
        "N"
    ).otherwise("Y").alias("VSN_OUT_OF_NTWK_EXAM_IN"),
    F.when(
        (F.col("MBR_UNIQ_KEY_ExtractCobMbrUniqKeys").isNull())|(F.col("MBR_UNIQ_KEY_ExtractCobMbrUniqKeys")==""),
        "N"
    ).otherwise("Y").alias("OTHR_CAR_PRI_MED_IN"),
    F.when(
        (F.col("VBB_IN").isNull())|(F.trim(F.col("VBB_IN"))==""),
        "N"
    ).otherwise("Y").alias("VBB_ENR_IN"),
    F.when(
        F.col("VBB_MDL_CD").isNull(),
        ""
    ).otherwise(F.trim(F.col("VBB_MDL_CD"))).alias("VBB_MDL_CD"),
    F.when(
        (F.col("CMPSS_COV_IN").isNull())|(F.trim(F.col("CMPSS_COV_IN"))==""),
        "N"
    ).otherwise("Y").alias("CMPSS_COV_IN"),
    F.when(
        F.col("ALNO_HOME_PG_ID").isNull(),
        ""
    ).otherwise(
        F.when(
            F.length(F.trim(F.col("ALNO_HOME_PG_ID")))==0,
            ""
        ).otherwise(F.col("ALNO_HOME_PG_ID"))
    ).alias("ALNO_HOME_PG_ID"),
    F.when(
        F.col("MED_HOME_ID").isNull(),
        "N"
    ).otherwise(
        F.when(
            F.trim(F.col("MED_HOME_ID"))=="",
            "N"
        ).otherwise(
            F.when(
                (F.length(F.trim(F.col("MED_HOME_ID")))==0) | (F.trim(F.col("MED_HOME_ID"))=="0") | (F.trim(F.col("MED_HOME_ID"))=="NA"),
                "N"
            ).otherwise("Y")
        )
    ).alias("PCMH_IN"),
    F.when(
        F.col("DNTL_RWRD_CD").isNull(),
        "N"
    ).otherwise(
        F.when(
            F.trim(F.col("DNTL_RWRD_CD"))=="",
            "N"
        ).otherwise(
            F.when(
                F.length(F.trim(F.col("DNTL_RWRD_CD")))==0,
                "N"
            ).otherwise(
                F.when(
                    F.col("DNTL_RWRD_CD")=="DNRP",
                    "Y"
                ).otherwise("N")
            )
        )
    ).alias("DNTL_RWRD_IN")
)

df_xfm_BusinessLogic = df_xfm_BusinessLogic_2

df_final_select = df_xfm_BusinessLogic.select(
    F.col("SRC_SYS_CD_SK"),
    F.col("MBR_UNIQ_KEY"),
    F.col("MNTL_HLTH_VNDR_ID"),
    F.col("PBM_VNDR_CD"),
    F.col("WELNS_BNF_LVL_CD"),
    F.col("LAST_UPDT_RUN_CYC_NO"),
    F.col("WELNS_BNF_VNDR_ID"),
    F.col("EAP_BNF_LVL_CD"),
    F.col("VSN_BNF_VNDR_ID"),
    F.rpad(F.col("VSN_RTN_EXAM_IN"),1," ").alias("VSN_RTN_EXAM_IN"),
    F.rpad(F.col("VSN_HRDWR_IN"),1," ").alias("VSN_HRDWR_IN"),
    F.rpad(F.col("VSN_OUT_OF_NTWK_EXAM_IN"),1," ").alias("VSN_OUT_OF_NTWK_EXAM_IN"),
    F.rpad(F.col("OTHR_CAR_PRI_MED_IN"),1," ").alias("OTHR_CAR_PRI_MED_IN"),
    F.rpad(F.col("VBB_ENR_IN"),1," ").alias("VBB_ENR_IN"),
    F.col("VBB_MDL_CD"),
    F.rpad(F.col("CMPSS_COV_IN"),1," ").alias("CMPSS_COV_IN"),
    F.col("ALNO_HOME_PG_ID"),
    F.rpad(F.col("PCMH_IN"),1," ").alias("PCMH_IN"),
    F.rpad(F.col("DNTL_RWRD_IN"),1," ").alias("DNTL_RWRD_IN")
)

write_files(
    df_final_select,
    f"{adls_path}/load/P_MBR_SUPLMT_BNF.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)