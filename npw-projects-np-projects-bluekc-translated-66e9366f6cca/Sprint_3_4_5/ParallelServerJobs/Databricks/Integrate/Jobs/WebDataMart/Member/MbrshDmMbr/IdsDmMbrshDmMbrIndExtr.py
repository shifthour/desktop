# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                Date                 	Project/Altiris #                       	Change Description                                                                                                                                   	Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     	-------------------------------------      	----------------------------------------------------------------------                                                                      	                      --------------------------------       -------------------------------   ----------------------------       
# MAGIC Kalyan Neelam         2009-09-29       	      4113                                  Original Programming                                       	                                                                                        devlIDSnew               Steph Goddard          10/01/2009    
# MAGIC 
# MAGIC SAndrew                  2009-11-02             TTR-626                                took out lookup to DM Mbr table to see if member existed prior to updating                                                       devlIDSnew               Steph Goddard          11/03/2009
# MAGIC                                                                                                               took out constraint IsNull (   DM_MBR_exists.MBR_UNIQ_KEY) = @FALSE
# MAGIC Kalyan Neelam         2010-03-11              4278                                     Added new fields for update - MBR_WELNS_BNF_LVL_NM, MBR_EAP_CAT_CD,                                        IntegrateCurDevl
# MAGIC                                                                                                              MBR_EAP_CAT_DESC, MBR_VSN_BNF_VNDR_ID, MBR_VSN_RTN_EXAM_IN, 
# MAGIC                                                                                                              MBR_VSN_HRDWR_IN, MBR_VSN_OUT_OF_NTWK_EXAM_IN
# MAGIC Kalyan Neelam         2010-05-014            4404                                    Added new field MBR_PT_POD_DPLY_IN on end                                                                                            IntegrateCurDevl        Steph Goddard           05/17/2010
# MAGIC 
# MAGIC Judy Reynolds          2010-05-19             4297 - Alineo PH2                Modified to add OTHR_CAR_PRI_MED_IN field to P_MBR_SUPLMT_BNF table and                                     IntegrateNewDevl      Steph Goddard           05/24/2010
# MAGIC                                                                                                              use this field to update the MBR_OTHR_CAR_PRI_MED_IN field in the 
# MAGIC                                                                                                              MBRSH_DM_MBR table
# MAGIC Judy Reynolds          2010-12-13            TTR_866                               Modified to add stages to extract Group Name, Class Description, and Sub Group Name                                  IntegrateNewDevl      Steph Goddard         12/14/2010
# MAGIC                                                                                                              and update these fields on the MBRSH_DM_MBR table
# MAGIC Judy Reynolds          2011-01-28            TTR_866                               Modified MbrEnr_PodInd SQL in IDS_MBR_ENR stage to correct how the                                                       IntegrateNewDevl       Steph Goddard         02/01/2011
# MAGIC                                                                                                              MBR_PD_POP_DPLY_IN field was being populated
# MAGIC Judy Reynolds          2011-02-11             TTR_866                             Corrected name on hashfile in hashclear and added missing hashfile                                                                  IntegrateNewDevl
# MAGIC Kalyan Neelam        2011-12-27              4799                                      Added 2 new fields on end - MBR_PT_EFF_DT, MBR_PT_TERM_DT                                                             IntegrateCurDevl       SAndrew                    2011-12-31
# MAGIC SAndrew                2012-06-16               prod abend                          added check to MBRSHP_DM_MBR before trying to update:   mbrs_exists_on_MBRSHP_DM_MBR production      
# MAGIC                                                                                                               only updates of mbr exists
# MAGIC Abhiram Dasarathy 2012-06-29              4830 - AHY                            Modified the logic for  MBR_PT_POD_DPLY_IN field to reflect the changes                                                     IntegrateWrhsDevl      Brent Leland              07-06-2012
# MAGIC                                                                                                              Added hashed file lookup for MBRSH_DM_MBR instead of database reference lookup
# MAGIC                              
# MAGIC Rama Kamjula        2013-09-20             5114                                      Rewritten Server job to Parallel version                                                                                                                IntegrateWrhsDevl      Peter Marshall            10/23/2013                    
# MAGIC Kalyan Neelam        2013-11-27             5234 Compass                       Added new column CMPSS_COV_IN and its associated logic                                                                            IntegrateNewDevl      Bhoomi Dasari          11/28/2013
# MAGIC 
# MAGIC Jag Yelavarthi      2013-12-28              5114 - Daptiv#653       Added new columns VBB_ENR_IN, VBB_MDL_CD and ALNO_HOME_PG_ID and updated the logic                        IntegrateNewDevl       Bhoomi Dasari            2014-01-03                                    
# MAGIC                                                                                                             as per the new mapping rules.
# MAGIC 
# MAGIC Jag Yelavarthi      2014-02-06              5114 - Production issue           Null Check issue is corrrected for column derivation MBR_PROV_ACTV_IN                                                      IntegrateNewDevl       Bhoomi Dasari            2014-02-06
# MAGIC 
# MAGIC Karthik Chintalapani  2013-01-21   5108                                                            Added new column  PCMH_IN                                                                                                              IntegrateNewDevl         Kalyan Neelam           2014-03-31
# MAGIC                                                                                                                           as per the new mapping rules. 
# MAGIC 
# MAGIC 
# MAGIC Pooja Sunkara           2015-04-23          TFS#7916                              Derive SRC_SYS_CD field from Extract SQL to use it in the update strategy since                                           IntegrateNewDevl          Kalyan Neelam           2015-04-28
# MAGIC                                                                                                               that is part of the natural key in MBR_DM_MBR table                                                                                       
# MAGIC Abhiram Dasarathy	2016-12-06         5217 Dental Network	            Added DNTL_NTWRK_IN field to the end of the columns					         IntegrateDev2              Kalyan Neelam           2016-12-06  
# MAGIC 
# MAGIC Kamal Tangri             2019-02-28        INC0492343                           Change is in the db2_MBR_ENR stage.  Added the new condition to include Vision.                                           IntegrateDev1              Kalyan Neelam           2019-03-04

# MAGIC Job Name: IdsDmMbrshDmMbrIndExtr
# MAGIC 
# MAGIC Updates POD_DPLY_IN in MBRSH_DM_MBR DM table.
# MAGIC Extracts Data from P table from IDS.
# MAGIC Extracts Data from MBR_ENR table in IDS
# MAGIC Remove duplicates from Source on MBR_UNIQ_KEY
# MAGIC Extracts existed Members from DM table.
# MAGIC Extracts data from Cd_Mppng from IDS
# MAGIC Extracts Cls Data from IDS
# MAGIC Extracts Group data from IDS
# MAGIC Extracts SubGroup data from IDS
# MAGIC Null Handling and Business Logic
# MAGIC Logic for Pod_Dply_In
# MAGIC Extracts data from P table - P_AHY_PILOT_GRP
# MAGIC Creates load file for the existed members to update POD_DPLY_IN
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# Retrieve all parameter values
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
ClmMartOwner = get_widget_value('ClmMartOwner','')
clmmart_secret_name = get_widget_value('clmmart_secret_name','')
CurDate = get_widget_value('CurDate','')
CurrDateMinus90 = get_widget_value('CurrDateMinus90','')

# Obtain JDBC configurations
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
jdbc_url_clmmart, jdbc_props_clmmart = get_db_config(clmmart_secret_name)

# Stage: db2_P_AHY_PILOT_GRP
extract_query_db2_P_AHY_PILOT_GRP = f"""
SELECT GRP_ID
FROM {IDSOwner}.P_AHY_PILOT_GRP P_AHY_PILOT_GRP
WHERE '{CurDate}' >= P_AHY_PILOT_GRP.PT_2_BLUE_TERM_DT
"""
df_db2_P_AHY_PILOT_GRP = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_P_AHY_PILOT_GRP)
    .load()
)

# Stage: P_MBR_SUPLMT_BNF
extract_query_P_MBR_SUPLMT_BNF = f"""
SELECT 
   MBR.SRC_SYS_CD_SK,
   BNF.MBR_UNIQ_KEY,
   COALESCE(MAP.TRGT_CD,'NA') SRC_SYS_CD,
   BNF.MNTL_HLTH_VNDR_ID,
   BNF.PBM_VNDR_CD,
   BNF.WELNS_BNF_LVL_CD,
   BNF.WELNS_BNF_VNDR_ID,
   BNF.EAP_BNF_LVL_CD,
   BNF.VSN_BNF_VNDR_ID,
   BNF.VSN_RTN_EXAM_IN,
   BNF.VSN_HRDWR_IN,
   BNF.VSN_OUT_OF_NTWK_EXAM_IN,
   BNF.OTHR_CAR_PRI_MED_IN,
   BNF.CMPSS_COV_IN,
   BNF.ALNO_HOME_PG_ID,
   BNF.VBB_ENR_IN,
   BNF.VBB_MDL_CD,
   BNF.PCMH_IN,
   BNF.DNTL_RWRD_IN
FROM
   {IDSOwner}.P_MBR_SUPLMT_BNF BNF,
   {IDSOwner}.MBR MBR,
   {IDSOwner}.CD_MPPNG MAP
WHERE
   BNF.MBR_UNIQ_KEY = MBR.MBR_UNIQ_KEY
   AND MBR.SRC_SYS_CD_SK = MAP.CD_MPPNG_SK
"""
df_P_MBR_SUPLMT_BNF = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_P_MBR_SUPLMT_BNF)
    .load()
)

# Stage: db2_MBR_ENR
extract_query_db2_MBR_ENR = f"""
SELECT DISTINCT MBR_ENR.MBR_UNIQ_KEY MBR_UNIQ_KEY
FROM {IDSOwner}.MBR_ENR AS MBR_ENR,
     {IDSOwner}.CD_MPPNG AS CD_MPPNG
WHERE MBR_ENR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK = CD_MPPNG.CD_MPPNG_SK
  AND MBR_ENR.ELIG_IN = 'Y'
  AND (CD_MPPNG.TRGT_CD = 'MED' OR CD_MPPNG.TRGT_CD = 'LIFE' OR CD_MPPNG.TRGT_CD = 'DNTL' OR CD_MPPNG.TRGT_CD = 'VSN')
  AND EFF_DT_SK <= '{CurDate}'
  AND TERM_DT_SK >= '{CurDate}'
"""
df_db2_MBR_ENR = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_MBR_ENR)
    .load()
)

# Stage: odbc_MBRSH_DM_MBR
extract_query_odbc_MBRSH_DM_MBR = f"""
SELECT MBR_UNIQ_KEY, 'Y' as MBR_EXIST_MBR
FROM {ClmMartOwner}.MBRSH_DM_MBR
"""
df_odbc_MBRSH_DM_MBR = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_clmmart)
    .options(**jdbc_props_clmmart)
    .option("query", extract_query_odbc_MBRSH_DM_MBR)
    .load()
)

# Stage: db2_CLS
extract_query_db2_CLS = f"""
SELECT
  MBR.MBR_UNIQ_KEY,
  CLS.CLS_DESC
FROM
  {IDSOwner}.MBR MBR,
  {IDSOwner}.CLS CLS
WHERE
  MBR.CLS_SK = CLS.CLS_SK
"""
df_db2_CLS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_CLS)
    .load()
)

# Stage: db2_CD_MPPNG
extract_query_db2_CD_MPPNG = f"""
SELECT COALESCE(TRGT_CD,'NA') TRGT_CD,
       TRGT_CD_NM
FROM {IDSOwner}.CD_MPPNG
WHERE TRGT_DOMAIN_NM = 'NETWORK CAPITATION RELATIONSHIP CATEGORY'
GROUP BY TRGT_CD, TRGT_CD_NM
"""
df_db2_CD_MPPNG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_CD_MPPNG)
    .load()
)

# Stage: db2_GRP_SUBGRP
extract_query_db2_GRP_SUBGRP = f"""
SELECT
  MBR.MBR_UNIQ_KEY,
  GRP.GRP_NM,
  GRP.GRP_ID
FROM
  {IDSOwner}.MBR MBR,
  {IDSOwner}.SUB SUB,
  {IDSOwner}.GRP GRP
WHERE
  MBR.SUB_SK = SUB.SUB_SK
  AND SUB.GRP_SK = GRP.GRP_SK
"""
df_db2_GRP_SUBGRP = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_GRP_SUBGRP)
    .load()
)

# Stage: db2_SUBGRP
extract_query_db2_SUBGRP = f"""
SELECT
  MBR.MBR_UNIQ_KEY,
  SGRP.SUBGRP_NM
FROM
  {IDSOwner}.MBR MBR,
  {IDSOwner}.SUBGRP SGRP
WHERE
  MBR.SUBGRP_SK = SGRP.SUBGRP_SK
"""
df_db2_SUBGRP = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_SUBGRP)
    .load()
)

# Stage: db2_MbrEnrPodInd
extract_query_db2_MbrEnrPodInd = f"""
SELECT
  MBR_ENR.MBR_UNIQ_KEY
FROM
  {IDSOwner}.MBR_ENR MBR_ENR,
  {IDSOwner}.CD_MPPNG CD_MPPNG,
  {IDSOwner}.PROD_CMPNT PROD,
  {IDSOwner}.CD_MPPNG MPPNG1,
  {IDSOwner}.PROD_BILL_CMPNT BILL
WHERE
  MBR_ENR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK = CD_MPPNG.CD_MPPNG_SK
  AND CD_MPPNG.TRGT_CD = 'AHY'
  AND MBR_ENR.ELIG_IN = 'Y'
  AND MBR_ENR.EFF_DT_SK <= '{CurDate}'
  AND MBR_ENR.TERM_DT_SK >= '{CurrDateMinus90}'
  AND MBR_ENR.PROD_SK = PROD.PROD_SK
  AND PROD.PROD_CMPNT_TYP_CD_SK = MPPNG1.CD_MPPNG_SK
  AND MPPNG1.TRGT_CD = 'PDBL'
  AND PROD.PROD_CMPNT_PFX_ID = BILL.PROD_CMPNT_PFX_ID
  AND BILL.PROD_BILL_CMPNT_ID = 'HY38'
  AND PROD.PROD_CMPNT_EFF_DT_SK <= '{CurDate}'
  AND PROD.PROD_CMPNT_TERM_DT_SK >= '{CurrDateMinus90}'
  AND PROD.PROD_CMPNT_EFF_DT_SK <= MBR_ENR.TERM_DT_SK
  AND PROD.PROD_CMPNT_TERM_DT_SK > MBR_ENR.EFF_DT_SK
  AND BILL.PROD_BILL_CMPNT_EFF_DT_SK <= '{CurDate}'
  AND BILL.PROD_BILL_CMPNT_TERM_DT_SK >= '{CurrDateMinus90}'
  AND BILL.PROD_BILL_CMPNT_EFF_DT_SK <= MBR_ENR.TERM_DT_SK
  AND BILL.PROD_BILL_CMPNT_TERM_DT_SK >= MBR_ENR.EFF_DT_SK
UNION
SELECT
  MBR_ENR1.MBR_UNIQ_KEY
FROM
  {IDSOwner}.MBR_ENR MBR_ENR1,
  {IDSOwner}.PROD_CMPNT PROD1,
  {IDSOwner}.CD_MPPNG MPPNG2,
  {IDSOwner}.CD_MPPNG MPPNG3,
  {IDSOwner}.BNF_SUM BNF
WHERE
  MBR_ENR1.PROD_SK = PROD1.PROD_SK
  AND MBR_ENR1.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK = MPPNG2.CD_MPPNG_SK
  AND MPPNG2.TRGT_CD = 'MED'
  AND MBR_ENR1.ELIG_IN = 'Y'
  AND MBR_ENR1.EFF_DT_SK <= '{CurDate}'
  AND MBR_ENR1.TERM_DT_SK >= '{CurrDateMinus90}'
  AND PROD1.PROD_CMPNT_TYP_CD_SK = MPPNG3.CD_MPPNG_SK
  AND MPPNG3.TRGT_CD = 'BSBS'
  AND PROD1.PROD_CMPNT_PFX_ID = BNF.PROD_CMPNT_PFX_ID
  AND BNF.BNF_SUM_ID = 'AHYP'
  AND PROD1.PROD_CMPNT_EFF_DT_SK <= '{CurDate}'
  AND PROD1.PROD_CMPNT_TERM_DT_SK >= '{CurrDateMinus90}'
  AND PROD1.PROD_CMPNT_EFF_DT_SK <= MBR_ENR1.TERM_DT_SK
  AND PROD1.PROD_CMPNT_TERM_DT_SK >= MBR_ENR1.EFF_DT_SK
"""
df_db2_MbrEnrPodInd = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_MbrEnrPodInd)
    .load()
)

# Stage: db2_MbrEnrPtDts
extract_query_db2_MbrEnrPtDts = f"""
SELECT 
  MBR_UNIQ_KEY,
  MBR_EFF_DT_SK,
  MBR_TERM_DT_SK,
  PROD_EFF_DT_SK,
  PROD_TERM_DT_SK,
  ORDER
FROM
(
  SELECT 
    MBR_ENR.MBR_UNIQ_KEY,
    MIN(MBR_ENR.EFF_DT_SK) as MBR_EFF_DT_SK,
    MAX(MBR_ENR.TERM_DT_SK) as MBR_TERM_DT_SK,
    MIN(BILL.PROD_BILL_CMPNT_EFF_DT_SK) as PROD_EFF_DT_SK,
    MIN(BILL.PROD_BILL_CMPNT_TERM_DT_SK) as PROD_TERM_DT_SK,
    1 as ORDER
  FROM
    {IDSOwner}.MBR_ENR MBR_ENR,
    {IDSOwner}.CD_MPPNG CD_MPPNG,
    {IDSOwner}.PROD_CMPNT PROD,
    {IDSOwner}.CD_MPPNG MPPNG1,
    {IDSOwner}.PROD_BILL_CMPNT BILL
  WHERE  
    MBR_ENR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK  = CD_MPPNG.CD_MPPNG_SK
    AND CD_MPPNG.TRGT_CD = 'AHY' 
    AND MBR_ENR.ELIG_IN = 'Y' 
    AND MBR_ENR.PROD_SK = PROD.PROD_SK
    AND PROD.PROD_CMPNT_TYP_CD_SK = MPPNG1.CD_MPPNG_SK
    AND MPPNG1.TRGT_CD = 'PDBL'
    AND PROD.PROD_CMPNT_PFX_ID = BILL.PROD_CMPNT_PFX_ID
    AND BILL.PROD_BILL_CMPNT_ID = 'HY38'
  GROUP BY
    MBR_ENR.MBR_UNIQ_KEY
UNION
  SELECT 
    MBR_ENR1.MBR_UNIQ_KEY,
    MIN(MBR_ENR1.EFF_DT_SK) as MBR_EFF_DT_SK,
    MAX(MBR_ENR1.TERM_DT_SK) as MBR_TERM_DT_SK,
    MIN(PROD1.PROD_CMPNT_EFF_DT_SK) as PROD_EFF_DT_SK,
    MAX(PROD1.PROD_CMPNT_TERM_DT_SK) as PROD_TERM_DT_SK,
    2 as ORDER
  FROM      
    {IDSOwner}.MBR_ENR MBR_ENR1,
    {IDSOwner}.PROD_CMPNT PROD1,
    {IDSOwner}.CD_MPPNG MPPNG2,
    {IDSOwner}.CD_MPPNG MPPNG3,
    {IDSOwner}.BNF_SUM BNF
  WHERE
     MBR_ENR1.PROD_SK = PROD1.PROD_SK
     AND MBR_ENR1.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK = MPPNG2.CD_MPPNG_SK
     AND MPPNG2.TRGT_CD = 'MED'
     AND MBR_ENR1.ELIG_IN = 'Y'
     AND PROD1.PROD_CMPNT_TYP_CD_SK = MPPNG3.CD_MPPNG_SK
     AND MPPNG3.TRGT_CD = 'BSBS'
     AND PROD1.PROD_CMPNT_PFX_ID = BNF.PROD_CMPNT_PFX_ID
     AND BNF.BNF_SUM_ID = 'AHYP'
  GROUP BY
    MBR_ENR1.MBR_UNIQ_KEY
)
ORDER BY
  MBR_UNIQ_KEY,
  ORDER DESC
"""
df_db2_MbrEnrPtDts = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_MbrEnrPtDts)
    .load()
)

# Stage: xfm_MbrPtDts
df_xfm_MbrPtDts = df_db2_MbrEnrPtDts.select(
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.when(
        F.col("PROD_TERM_DT_SK") <= F.col("MBR_EFF_DT_SK"),
        F.lit(None)
    ).otherwise(
        F.when(
            F.col("PROD_EFF_DT_SK") <= F.col("MBR_EFF_DT_SK"),
            F.col("MBR_EFF_DT_SK")
        ).otherwise(F.col("PROD_EFF_DT_SK"))
    ).alias("EFF_DT_SK"),
    F.when(
        F.col("PROD_TERM_DT_SK") <= F.col("MBR_EFF_DT_SK"),
        F.lit(None)
    ).otherwise(
        F.when(
            F.col("PROD_TERM_DT_SK") <= F.col("MBR_TERM_DT_SK"),
            F.col("PROD_TERM_DT_SK")
        ).otherwise(F.col("MBR_TERM_DT_SK"))
    ).alias("TERM_DT_SK")
)

# Stage: rmd (PxRemDup) - keep last
df_rmd = dedup_sort(
    df_xfm_MbrPtDts,
    ["MBR_UNIQ_KEY"],
    [("MBR_UNIQ_KEY","D")]
)

# Stage: lkp_PtDts
df_lkp_PtDts = (
    df_db2_MbrEnrPodInd.alias("lnk_MbrPodInd")
    .join(
        df_rmd.alias("lnk_Rmv_Dups"),
        F.col("lnk_MbrPodInd.MBR_UNIQ_KEY") == F.col("lnk_Rmv_Dups.MBR_UNIQ_KEY"),
        "left"
    )
    .select(
        F.col("lnk_MbrPodInd.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        F.col("lnk_Rmv_Dups.EFF_DT_SK").alias("EFF_DT_SK"),
        F.col("lnk_Rmv_Dups.TERM_DT_SK").alias("TERM_DT_SK")
    )
)

# Stage: lkp_Codes
df_lkp_Codes = (
    df_P_MBR_SUPLMT_BNF.alias("lnk_Extract")
    .join(
        df_db2_MBR_ENR.alias("lnk_MbrEnr"),
        F.col("lnk_Extract.MBR_UNIQ_KEY") == F.col("lnk_MbrEnr.MBR_UNIQ_KEY"),
        "left"
    )
    .join(
        df_odbc_MBRSH_DM_MBR.alias("lnk_ExistMbr"),
        F.col("lnk_Extract.MBR_UNIQ_KEY") == F.col("lnk_ExistMbr.MBR_UNIQ_KEY"),
        "left"
    )
    .join(
        df_db2_CD_MPPNG.alias("lnk_EapCatCdDesc"),
        F.col("lnk_Extract.EAP_BNF_LVL_CD") == F.col("lnk_EapCatCdDesc.TRGT_CD"),
        "left"
    )
    .join(
        df_db2_CLS.alias("lnk_ClsDescOut"),
        F.col("lnk_Extract.MBR_UNIQ_KEY") == F.col("lnk_ClsDescOut.MBR_UNIQ_KEY"),
        "left"
    )
    .join(
        df_db2_GRP_SUBGRP.alias("lnk_GrpNameOut"),
        F.col("lnk_Extract.MBR_UNIQ_KEY") == F.col("lnk_GrpNameOut.MBR_UNIQ_KEY"),
        "left"
    )
    .join(
        df_db2_SUBGRP.alias("lnk_SubGrpNmOut"),
        F.col("lnk_Extract.MBR_UNIQ_KEY") == F.col("lnk_SubGrpNmOut.MBR_UNIQ_KEY"),
        "left"
    )
    .join(
        df_lkp_PtDts.alias("lnk_PodOut"),
        F.col("lnk_Extract.MBR_UNIQ_KEY") == F.col("lnk_PodOut.MBR_UNIQ_KEY"),
        "left"
    )
    .select(
        F.col("lnk_Extract.MBR_UNIQ_KEY").alias("Extract_MBR_UNIQ_KEY"),
        F.col("lnk_Extract.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("lnk_Extract.MNTL_HLTH_VNDR_ID").alias("MNTL_HLTH_VNDR_ID"),
        F.col("lnk_Extract.PBM_VNDR_CD").alias("PBM_VNDR_CD"),
        F.col("lnk_Extract.WELNS_BNF_LVL_CD").alias("WELNS_BNF_LVL_CD"),
        F.col("lnk_Extract.WELNS_BNF_VNDR_ID").alias("WELNS_BNF_VNDR_ID"),
        F.col("lnk_Extract.EAP_BNF_LVL_CD").alias("EAP_BNF_LVL_CD"),
        F.col("lnk_Extract.VSN_BNF_VNDR_ID").alias("VSN_BNF_VNDR_ID"),
        F.col("lnk_Extract.VSN_RTN_EXAM_IN").alias("VSN_RTN_EXAM_IN"),
        F.col("lnk_Extract.VSN_HRDWR_IN").alias("VSN_HRDWR_IN"),
        F.col("lnk_Extract.VSN_OUT_OF_NTWK_EXAM_IN").alias("VSN_OUT_OF_NTWK_EXAM_IN"),
        F.col("lnk_Extract.OTHR_CAR_PRI_MED_IN").alias("OTHR_CAR_PRI_MED_IN"),
        F.col("lnk_MbrEnr.MBR_UNIQ_KEY").alias("MbrEnr_MBR_UNIQ_KEY"),
        F.col("lnk_EapCatCdDesc.TRGT_CD_NM").alias("EapCatCd_TRGT_CD_NM"),
        F.col("lnk_PodOut.MBR_UNIQ_KEY").alias("PodOut_MBR_UNIQ_KEY"),
        F.col("lnk_PodOut.EFF_DT_SK").alias("PodOut_EFF_DT_SK"),
        F.col("lnk_PodOut.TERM_DT_SK").alias("PodOut_TERM_DT_SK"),
        F.col("lnk_ClsDescOut.CLS_DESC").alias("CLS_DESC"),
        F.col("lnk_GrpNameOut.GRP_NM").alias("GRP_NM"),
        F.col("lnk_GrpNameOut.GRP_ID").alias("GRP_ID"),
        F.col("lnk_SubGrpNmOut.SUBGRP_NM").alias("SUBGRP_NM"),
        F.col("lnk_ExistMbr.MBR_EXIST_MBR").alias("MBR_EXIST_MBR"),
        F.col("lnk_Extract.CMPSS_COV_IN").alias("CMPSS_COV_IN"),
        F.col("lnk_Extract.ALNO_HOME_PG_ID").alias("ALNO_HOME_PG_ID"),
        F.col("lnk_Extract.VBB_ENR_IN").alias("VBB_ENR_IN"),
        F.col("lnk_Extract.VBB_MDL_CD").alias("VBB_MDL_CD"),
        F.col("lnk_Extract.PCMH_IN").alias("PCMH_IN"),
        F.col("lnk_Extract.DNTL_RWRD_IN").alias("DNTL_RWRD_IN")
    )
)

# Stage: xfm_MbrExtract
df_xfm_MbrExtract_pre = df_lkp_Codes.filter(
    (F.col("MBR_EXIST_MBR").isNotNull()) &
    (trim(F.col("MBR_EXIST_MBR")) != '')
)

df_xfm_MbrExtract = df_xfm_MbrExtract_pre.select(
    F.col("Extract_MBR_UNIQ_KEY").alias("MBR_MBR_UNIQ_KEY"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("MNTL_HLTH_VNDR_ID").alias("MBR_MNTL_HLTH_VNDR_ID"),
    F.col("PBM_VNDR_CD").alias("MBR_PBM_VNDR_CD"),
    F.col("WELNS_BNF_LVL_CD").alias("MBR_WELNS_BNF_LVL_CD"),
    F.when(F.col("WELNS_BNF_LVL_CD") == F.lit("AHYPRMR"), F.lit("PREMIER"))
     .when(F.col("WELNS_BNF_LVL_CD") == F.lit("AHYVAL"), F.lit("VALUE"))
     .when(F.col("WELNS_BNF_LVL_CD") == F.lit("AHYCSTM"), F.lit("CUSTOM"))
     .when(F.col("WELNS_BNF_LVL_CD") == F.lit("AHY"), F.lit("AHY"))
     .when(F.col("WELNS_BNF_LVL_CD") == F.lit("AHYPLTNM"), F.lit("PLATINUM"))
     .when(F.col("WELNS_BNF_LVL_CD") == F.lit("AHYHRAO"), F.lit("HRAONLY"))
     .otherwise(F.lit(None)).alias("MBR_MBR_WELNS_BNF_LVL_NM"),
    F.col("WELNS_BNF_VNDR_ID").alias("MBR_WELNS_BNF_VNDR_ID"),
    F.when(
        F.col("MbrEnr_MBR_UNIQ_KEY").isNull() |
        (F.col("MbrEnr_MBR_UNIQ_KEY") == 0),
        F.lit("N")
    ).otherwise(F.lit("Y")).alias("MBR_MBR_ACTV_PROD_IN"),
    F.col("EAP_BNF_LVL_CD").alias("MBR_EAP_CAT_CD"),
    F.when(
        F.col("EapCatCd_TRGT_CD_NM").isNull(),
        F.lit("NA")
    ).otherwise(F.col("EapCatCd_TRGT_CD_NM")).alias("MBR_EAP_CAT_DESC"),
    F.col("VSN_BNF_VNDR_ID").alias("VSN_BNF_VNDR_ID"),
    F.col("VSN_RTN_EXAM_IN").alias("VSN_RTN_EXAM_IN"),
    F.col("VSN_HRDWR_IN").alias("VSN_HRDWR_IN"),
    F.col("VSN_OUT_OF_NTWK_EXAM_IN").alias("VSN_OUT_OF_NTWK_EXAM_IN"),
    F.when(
        F.col("OTHR_CAR_PRI_MED_IN").isNull() |
        (F.col("OTHR_CAR_PRI_MED_IN") == F.lit(" ")),
        F.lit("N")
    ).otherwise(F.col("OTHR_CAR_PRI_MED_IN")).alias("MBR_OTHR_CAR_PRI_MED_IN"),
    F.when(
        F.col("PodOut_MBR_UNIQ_KEY").isNull() |
        (F.col("PodOut_MBR_UNIQ_KEY") == 0),
        F.lit("N")
    ).otherwise(F.lit("Y")).alias("MBR_PT_POD_DPLY_IN"),
    F.when(F.col("CLS_DESC").isNull(), F.lit(" ")).otherwise(F.col("CLS_DESC")).alias("CLS_DESC"),
    F.when(F.col("GRP_NM").isNull(), F.lit(" ")).otherwise(F.col("GRP_NM")).alias("GRP_NM"),
    F.when(F.col("SUBGRP_NM").isNull(), F.lit(" ")).otherwise(F.col("SUBGRP_NM")).alias("SUBGRP_NM"),
    F.when(
        F.col("PodOut_MBR_UNIQ_KEY").isNull(),
        F.lit(None)
    ).otherwise(F.to_timestamp(F.col("PodOut_EFF_DT_SK"), "yyyy-MM-dd")).alias("PodOut_EFF_DT_SK"),
    F.when(
        F.col("PodOut_MBR_UNIQ_KEY").isNull(),
        F.lit(None)
    ).otherwise(F.to_timestamp(F.col("PodOut_TERM_DT_SK"), "yyyy-MM-dd")).alias("PodOut_TERM_DT_SK"),
    F.when(F.col("GRP_ID").isNull(), F.lit(" ")).otherwise(F.col("GRP_ID")).alias("GRP_ID"),
    F.col("CMPSS_COV_IN").alias("CMPSS_COV_IN"),
    F.col("ALNO_HOME_PG_ID").alias("ALNO_HOME_PG_ID"),
    F.col("VBB_ENR_IN").alias("VBB_ENR_IN"),
    F.col("VBB_MDL_CD").alias("VBB_MDL_CD"),
    F.col("PCMH_IN").alias("PCMH_IN"),
    F.col("DNTL_RWRD_IN").alias("DNTL_RWRD_IN")
)

# Stage: lkp_PodInd
df_lkp_PodInd = (
    df_xfm_MbrExtract.alias("lnk_MbrExtr")
    .join(
        df_db2_P_AHY_PILOT_GRP.alias("lnk_GrpIdOut"),
        F.col("lnk_MbrExtr.GRP_ID") == F.col("lnk_GrpIdOut.GRP_ID"),
        "left"
    )
    .select(
        F.col("lnk_MbrExtr.MBR_MBR_UNIQ_KEY").alias("MBR_MBR_UNIQ_KEY"),
        F.col("lnk_MbrExtr.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("lnk_MbrExtr.MBR_MNTL_HLTH_VNDR_ID").alias("MBR_MNTL_HLTH_VNDR_ID"),
        F.col("lnk_MbrExtr.MBR_PBM_VNDR_CD").alias("MBR_PBM_VNDR_CD"),
        F.col("lnk_MbrExtr.MBR_WELNS_BNF_LVL_CD").alias("MBR_WELNS_BNF_LVL_CD"),
        F.col("lnk_MbrExtr.MBR_MBR_WELNS_BNF_LVL_NM").alias("MBR_MBR_WELNS_BNF_LVL_NM"),
        F.col("lnk_MbrExtr.MBR_WELNS_BNF_VNDR_ID").alias("MBR_WELNS_BNF_VNDR_ID"),
        F.col("lnk_MbrExtr.MBR_MBR_ACTV_PROD_IN").alias("MBR_MBR_ACTV_PROD_IN"),
        F.col("lnk_MbrExtr.MBR_EAP_CAT_CD").alias("MBR_EAP_CAT_CD"),
        F.col("lnk_MbrExtr.MBR_EAP_CAT_DESC").alias("MBR_EAP_CAT_DESC"),
        F.col("lnk_MbrExtr.VSN_BNF_VNDR_ID").alias("VSN_BNF_VNDR_ID"),
        F.col("lnk_MbrExtr.VSN_RTN_EXAM_IN").alias("VSN_RTN_EXAM_IN"),
        F.col("lnk_MbrExtr.VSN_HRDWR_IN").alias("VSN_HRDWR_IN"),
        F.col("lnk_MbrExtr.VSN_OUT_OF_NTWK_EXAM_IN").alias("VSN_OUT_OF_NTWK_EXAM_IN"),
        F.col("lnk_MbrExtr.MBR_OTHR_CAR_PRI_MED_IN").alias("MBR_OTHR_CAR_PRI_MED_IN"),
        F.col("lnk_MbrExtr.MBR_PT_POD_DPLY_IN").alias("MBR_PT_POD_DPLY_IN"),
        F.col("lnk_MbrExtr.CLS_DESC").alias("CLS_DESC"),
        F.col("lnk_MbrExtr.GRP_NM").alias("GRP_NM"),
        F.col("lnk_MbrExtr.SUBGRP_NM").alias("SUBGRP_NM"),
        F.col("lnk_MbrExtr.PodOut_EFF_DT_SK").alias("PodOut_EFF_DT_SK"),
        F.col("lnk_MbrExtr.PodOut_TERM_DT_SK").alias("PodOut_TERM_DT_SK"),
        F.col("lnk_MbrExtr.GRP_ID").alias("Extract_GRP_ID"),
        F.col("lnk_GrpIdOut.GRP_ID").alias("Pilot_GRP_ID"),
        F.col("lnk_MbrExtr.CMPSS_COV_IN").alias("CMPSS_COV_IN"),
        F.col("lnk_MbrExtr.ALNO_HOME_PG_ID").alias("ALNO_HOME_PG_ID"),
        F.col("lnk_MbrExtr.VBB_ENR_IN").alias("VBB_ENR_IN"),
        F.col("lnk_MbrExtr.VBB_MDL_CD").alias("VBB_MDL_CD"),
        F.col("lnk_MbrExtr.PCMH_IN").alias("PCMH_IN"),
        F.col("lnk_MbrExtr.DNTL_RWRD_IN").alias("DNTL_RWRD_IN")
    )
)

# Stage: xfm_Pod_Dply_In
df_xfm_Pod_Dply_In = df_lkp_PodInd.select(
    F.col("MBR_MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("MBR_MNTL_HLTH_VNDR_ID").alias("MBR_MNTL_HLTH_VNDR_ID"),
    F.col("MBR_PBM_VNDR_CD").alias("MBR_PBM_VNDR_CD"),
    F.col("MBR_WELNS_BNF_LVL_CD").alias("MBR_WELNS_BNF_LVL_CD"),
    F.col("MBR_MBR_WELNS_BNF_LVL_NM").alias("MBR_MBR_WELNS_BNF_LVL_NM"),
    F.col("MBR_WELNS_BNF_VNDR_ID").alias("MBR_WELNS_BNF_VNDR_ID"),
    F.col("MBR_MBR_ACTV_PROD_IN").alias("MBR_MBR_ACTV_PROD_IN"),
    F.col("MBR_EAP_CAT_CD").alias("MBR_EAP_CAT_CD"),
    F.col("MBR_EAP_CAT_DESC").alias("MBR_EAP_CAT_DESC"),
    F.col("VSN_BNF_VNDR_ID").alias("MBR_VSN_BNF_VNDR_ID"),
    F.col("VSN_RTN_EXAM_IN").alias("MBR_VSN_RTN_EXAM_IN"),
    F.col("VSN_HRDWR_IN").alias("MBR_VSN_HRDWR_IN"),
    F.col("VSN_OUT_OF_NTWK_EXAM_IN").alias("MBR_VSN_OUT_OF_NTWK_EXAM_IN"),
    F.col("MBR_OTHR_CAR_PRI_MED_IN").alias("MBR_OTHR_CAR_PRI_MED_IN"),
    F.when(
        F.to_date(F.lit(CurDate), "yyyy-MM-dd") >= F.to_date(F.lit("2013-01-01"), "yyyy-MM-dd"),
        F.lit("N")
    ).otherwise(
        F.when(F.col("Pilot_GRP_ID").isNotNull(), F.lit("N"))
         .otherwise(F.col("MBR_PT_POD_DPLY_IN"))
    ).alias("MBR_PT_POD_DPLY_IN"),
    F.col("CLS_DESC").alias("CLS_DESC"),
    F.col("GRP_NM").alias("GRP_NM"),
    F.col("SUBGRP_NM").alias("SUBGRP_NM"),
    F.col("PodOut_EFF_DT_SK").alias("MBR_PT_EFF_DT"),
    F.col("PodOut_TERM_DT_SK").alias("MBR_PT_TERM_DT"),
    F.col("CMPSS_COV_IN").alias("CMPSS_COV_IN"),
    F.col("ALNO_HOME_PG_ID").alias("ALNO_HOME_PG_ID"),
    F.col("VBB_ENR_IN").alias("VBB_ENR_IN"),
    F.col("VBB_MDL_CD").alias("VBB_MDL_CD"),
    F.col("PCMH_IN").alias("PCMH_IN"),
    F.col("DNTL_RWRD_IN").alias("DNTL_RWRD_IN")
)

# Final output to seq_MBRSH_DM_MBR (MBRSH_DM_MBR_IND.dat)
# Apply rpad for char columns in final select
df_final = df_xfm_Pod_Dply_In.select(
    F.col("MBR_UNIQ_KEY"),
    F.col("SRC_SYS_CD"),
    F.col("MBR_MNTL_HLTH_VNDR_ID"),
    F.col("MBR_PBM_VNDR_CD"),
    F.col("MBR_WELNS_BNF_LVL_CD"),
    F.col("MBR_MBR_WELNS_BNF_LVL_NM"),
    F.col("MBR_WELNS_BNF_VNDR_ID"),
    F.rpad(F.col("MBR_MBR_ACTV_PROD_IN"), 1, " ").alias("MBR_MBR_ACTV_PROD_IN"),
    F.col("MBR_EAP_CAT_CD"),
    F.col("MBR_EAP_CAT_DESC"),
    F.col("MBR_VSN_BNF_VNDR_ID"),
    F.rpad(F.col("MBR_VSN_RTN_EXAM_IN"), 1, " ").alias("MBR_VSN_RTN_EXAM_IN"),
    F.rpad(F.col("MBR_VSN_HRDWR_IN"), 1, " ").alias("MBR_VSN_HRDWR_IN"),
    F.rpad(F.col("MBR_VSN_OUT_OF_NTWK_EXAM_IN"), 1, " ").alias("MBR_VSN_OUT_OF_NTWK_EXAM_IN"),
    F.rpad(F.col("MBR_OTHR_CAR_PRI_MED_IN"), 1, " ").alias("MBR_OTHR_CAR_PRI_MED_IN"),
    F.rpad(F.col("MBR_PT_POD_DPLY_IN"), 1, " ").alias("MBR_PT_POD_DPLY_IN"),
    F.col("CLS_DESC"),
    F.col("GRP_NM"),
    F.col("SUBGRP_NM"),
    F.col("MBR_PT_EFF_DT"),
    F.col("MBR_PT_TERM_DT"),
    F.rpad(F.col("CMPSS_COV_IN"), 1, " ").alias("CMPSS_COV_IN"),
    F.col("ALNO_HOME_PG_ID"),
    F.rpad(F.col("VBB_ENR_IN"), 1, " ").alias("VBB_ENR_IN"),
    F.col("VBB_MDL_CD"),
    F.rpad(F.col("PCMH_IN"), 1, " ").alias("PCMH_IN"),
    F.rpad(F.col("DNTL_RWRD_IN"), 1, " ").alias("DNTL_RWRD_IN")
)

write_files(
    df_final,
    f"{adls_path}/load/MBRSH_DM_MBR_IND.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)