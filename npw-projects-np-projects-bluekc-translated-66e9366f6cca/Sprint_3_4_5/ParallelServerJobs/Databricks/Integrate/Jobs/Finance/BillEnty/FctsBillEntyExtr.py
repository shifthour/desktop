# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2005-2011 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:      Pulls the Fee Discount information from Facets for Income 
# MAGIC       
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC 
# MAGIC  Developer                          Date                 Project/Altiris #                Change Description                                         Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                    ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Ralph Tucker                  10/30/2005-                                          Originally Programmed
# MAGIC Sharon Andrew                 5/05/2006                                           Changed the eligibility pulls to get the subscribers 
# MAGIC                                                                                                       subgroup info.   It was just returning the first 
# MAGIC                                                                                                        members for that sub-sk - which could have 
# MAGIC                                                                                                        been the subscriber.  who knows.
# MAGIC Parikshith Chada              6/4/2007          3264                              Added Balancing process to the overall                 devlIDS30                 Steph Goddard              09/18/2007
# MAGIC                                                                                                       job that takes a snapshot of the source data           
# MAGIC 
# MAGIC Bhoomi Dasari                 09/15/2008       3567                             Added new primay key contianer and SrcSysCdsk  devlIDS                     Steph Goddard             10/03/2008
# MAGIC                                                                                                       and SrcSysCd               
# MAGIC Ralph Tucker                  02/10/2011       TTR-1014                    Moved snapshot link for balancing because of    IntegrateNewDevl          Steph Goddard             02/15/2011
# MAGIC                                                                                                       timing problems that caused an out of balance  
# MAGIC Ralph Tucker                  03/09/2011       TTR-1014                    Break Fix GRP_GRGR_ID for individual              IntegrateNewDevl          
# MAGIC 
# MAGIC Manasa Andru                9/28/2013          TTR - 1070                    Bringing up to standards                                    IntegrateNewDevl          Kalyan Neelam              2013-10-30
# MAGIC 
# MAGIC Manasa Andru                2015-04-23          TFS - 10630           Updated the Extract SQL in the InkSbsbMedCov     IntegrateNewDevl         Kalyan Neelam              2015-04-23
# MAGIC                                                                                                    link and updated the rule for SUBGRP_ID field in the 
# MAGIC                                                                                                         BusinessRules transformer.
# MAGIC Prabhu ES                      2022-03-01           S2S Remediation   MSSQL connection parameters added                     IntegrateDev5

# MAGIC Hash file hf_bill_enty_allcol cleared
# MAGIC Pulls all Bill Enty rows from facets CMC_BLEI_ENTY_INFO table.
# MAGIC Balancing snapshot of source table
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import IntegerType, StringType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../shared_containers/PrimaryKey/ComsnBillEntyPK
# COMMAND ----------

FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
TmpOutFile = get_widget_value('TmpOutFile','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
RunID = get_widget_value('RunID','')
CurrDate = get_widget_value('CurrDate','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
SrcSysCd = get_widget_value('SrcSysCd','')

jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

extract_query_Extract = (
    f"SELECT BLEI_CK,BLEI_BILL_LEVEL,BLEI_BILL_LEVEL_CK "
    f"FROM {FacetsOwner}.CMC_BLEI_ENTY_INFO"
)
df_Extract = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_Extract)
    .load()
)

extract_query_lnkSGSGSub = (
    f"SELECT SB.SBSB_CK,SB.SBSB_ID,GR.GRGR_ID "
    f"FROM {FacetsOwner}.CMC_BLEI_ENTY_INFO BLEI, "
    f"{FacetsOwner}.CMC_SBSB_SUBSC SB, "
    f"{FacetsOwner}.CMC_GRGR_GROUP GR "
    f"WHERE SB.GRGR_CK = GR.GRGR_CK AND "
    f"      BLEI.BLEI_BILL_LEVEL_CK = SB.SBSB_CK AND "
    f"      BLEI.BLEI_BILL_LEVEL = 'I'"
)
df_lnkSGSGSub = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_lnkSGSGSub)
    .load()
    .select(
        F.col("SBSB_CK").alias("SGSG_CK"),
        F.col("SBSB_ID").alias("SGSG_ID"),
        F.col("GRGR_ID").alias("GRGR_ID")
    )
)
df_lnkSGSGSub = dedup_sort(df_lnkSGSGSub, ["SGSG_CK"], [])

extract_query_lnkSbsbMedCov = (
    f"SELECT SBSB.SBSB_CK, SBSB.SBSB_ID, SGSG.SGSG_ID "
    f"FROM {FacetsOwner}.CMC_BLEI_ENTY_INFO BLEI,"
    f"     {FacetsOwner}.CMC_MEME_MEMBER MEME, "
    f"     {FacetsOwner}.CMC_MEPE_PRCS_ELIG ELIG,"
    f"     {FacetsOwner}.CMC_SBSB_SUBSC SBSB,"
    f"     {FacetsOwner}.CMC_SGSG_SUB_GROUP SGSG "
    f"WHERE BLEI.BLEI_BILL_LEVEL_CK = SBSB.SBSB_CK "
    f"  AND BLEI.BLEI_BILL_LEVEL = 'I' "
    f"  AND SBSB.SBSB_CK = MEME.SBSB_CK "
    f"  AND MEME.MEME_REL = 'M' "
    f"  AND MEME.MEME_CK = ELIG.MEME_CK "
    f"  AND ELIG.MEPE_EFF_DT <= '#CurrDate#' "
    f"  AND ELIG.CSPD_CAT = 'M' "
    f"  AND ELIG.MEPE_ELIG_IND = 'Y' "
    f"  AND ELIG.SGSG_CK = SGSG.SGSG_CK "
    f"  AND ELIG.MEPE_TERM_DT = (SELECT MAX(ELIG2.MEPE_TERM_DT) "
    f"                           FROM {FacetsOwner}.CMC_MEPE_PRCS_ELIG ELIG2 "
    f"                           WHERE ELIG2.MEME_CK = ELIG.MEME_CK "
    f"                             AND ELIG2.MEPE_EFF_DT <= '#CurrDate#' "
    f"                             AND ELIG2.MEPE_ELIG_IND = 'Y' "
    f"                             AND ELIG2.CSPD_CAT = 'M')"
)
df_lnkSbsbMedCov = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_lnkSbsbMedCov)
    .load()
    .select(
        F.col("SBSB_CK").alias("SBSB_CK"),
        F.col("SBSB_ID").alias("SBSB_ID"),
        F.col("SGSG_ID").alias("SGSG_ID")
    )
)
df_lnkSbsbMedCov = dedup_sort(df_lnkSbsbMedCov, ["SBSB_CK"], [])

extract_query_lnkSbsbDentCov = (
    f"SELECT SBSB.SBSB_CK, SBSB.SBSB_ID, SGSG.SGSG_ID "
    f"FROM {FacetsOwner}.CMC_BLEI_ENTY_INFO BLEI,"
    f"     {FacetsOwner}.CMC_MEME_MEMBER MEME, "
    f"     {FacetsOwner}.CMC_MEPE_PRCS_ELIG ELIG,"
    f"     {FacetsOwner}.CMC_SBSB_SUBSC SBSB,"
    f"     {FacetsOwner}.CMC_SGSG_SUB_GROUP SGSG "
    f"WHERE BLEI.BLEI_BILL_LEVEL_CK = SBSB.SBSB_CK "
    f"  AND BLEI.BLEI_BILL_LEVEL = 'I' "
    f"  AND SBSB.SBSB_CK = MEME.SBSB_CK "
    f"  AND MEME.MEME_REL = 'M' "
    f"  AND MEME.MEME_CK = ELIG.MEME_CK "
    f"  AND ELIG.MEPE_EFF_DT <= '#CurrDate#' "
    f"  AND ELIG.CSPD_CAT = 'D' "
    f"  AND ELIG.MEPE_ELIG_IND = 'Y' "
    f"  AND ELIG.SGSG_CK = SGSG.SGSG_CK "
    f"  AND ELIG.MEPE_TERM_DT = (SELECT MAX(ELIG2.MEPE_TERM_DT) "
    f"                           FROM {FacetsOwner}.CMC_MEPE_PRCS_ELIG ELIG2 "
    f"                           WHERE ELIG2.MEME_CK = ELIG.MEME_CK "
    f"                             AND ELIG2.MEPE_EFF_DT <= '#CurrDate#' "
    f"                             AND ELIG2.MEPE_ELIG_IND = 'Y' "
    f"                             AND ELIG2.CSPD_CAT = 'D')"
)
df_lnkSbsbDentCov = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_lnkSbsbDentCov)
    .load()
    .select(
        F.col("SBSB_CK").alias("SBSB_CK"),
        F.col("SBSB_ID").alias("SBSB_ID"),
        F.col("SGSG_ID").alias("SGSG_ID")
    )
)
df_lnkSbsbDentCov = dedup_sort(df_lnkSbsbDentCov, ["SBSB_CK"], [])

extract_query_lnkSbsbOthrCov = (
    f"SELECT SBSB.SBSB_CK, SBSB.SBSB_ID, SGSG.SGSG_ID "
    f"FROM {FacetsOwner}.CMC_BLEI_ENTY_INFO BLEI,"
    f"     {FacetsOwner}.CMC_MEME_MEMBER MEME, "
    f"     {FacetsOwner}.CMC_MEPE_PRCS_ELIG ELIG,"
    f"     {FacetsOwner}.CMC_SBSB_SUBSC SBSB,"
    f"     {FacetsOwner}.CMC_SGSG_SUB_GROUP SGSG "
    f"WHERE BLEI.BLEI_BILL_LEVEL_CK = SBSB.SBSB_CK "
    f"  AND BLEI.BLEI_BILL_LEVEL = 'I' "
    f"  AND SBSB.SBSB_CK = MEME.SBSB_CK "
    f"  AND MEME.MEME_REL = 'M' "
    f"  AND MEME.MEME_CK = ELIG.MEME_CK "
    f"  AND ELIG.MEPE_EFF_DT <= '#CurrDate#' "
    f"  AND ELIG.CSPD_CAT IN ('A','F','L','S','X','Y') "
    f"  AND ELIG.MEPE_ELIG_IND = 'Y' "
    f"  AND ELIG.SGSG_CK = SGSG.SGSG_CK "
    f"  AND ELIG.MEPE_TERM_DT = (SELECT MAX(ELIG2.MEPE_TERM_DT) "
    f"                           FROM {FacetsOwner}.CMC_MEPE_PRCS_ELIG ELIG2 "
    f"                           WHERE ELIG2.MEME_CK = ELIG.MEME_CK "
    f"                             AND ELIG2.MEPE_EFF_DT <= '#CurrDate#' "
    f"                             AND ELIG2.MEPE_ELIG_IND = 'Y' "
    f"                             AND ELIG2.CSPD_CAT IN ('A','F','L','S','X','Y'))"
)
df_lnkSbsbOthrCov = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_lnkSbsbOthrCov)
    .load()
    .select(
        F.col("SBSB_CK").alias("SBSB_CK"),
        F.col("SBSB_ID").alias("SBSB_ID"),
        F.col("SGSG_ID").alias("SGSG_ID")
    )
)
df_lnkSbsbOthrCov = dedup_sort(df_lnkSbsbOthrCov, ["SBSB_CK"], [])

extract_query_lnkSbsbMedNoCov = (
    f"SELECT SBSB.SBSB_CK, SBSB.SBSB_ID, SGSG.SGSG_ID "
    f"FROM {FacetsOwner}.CMC_BLEI_ENTY_INFO BLEI,"
    f"     {FacetsOwner}.CMC_MEME_MEMBER MEME, "
    f"     {FacetsOwner}.CMC_MEPE_PRCS_ELIG ELIG,"
    f"     {FacetsOwner}.CMC_SBSB_SUBSC SBSB,"
    f"     {FacetsOwner}.CMC_SGSG_SUB_GROUP SGSG "
    f"WHERE BLEI.BLEI_BILL_LEVEL_CK = SBSB.SBSB_CK "
    f"  AND BLEI.BLEI_BILL_LEVEL = 'I' "
    f"  AND SBSB.SBSB_CK = MEME.SBSB_CK "
    f"  AND MEME.MEME_REL = 'M' "
    f"  AND MEME.MEME_CK = ELIG.MEME_CK "
    f"  AND ELIG.MEPE_EFF_DT <= '#CurrDate#' "
    f"  AND ELIG.CSPD_CAT = 'M' "
    f"  AND ELIG.MEPE_ELIG_IND = 'N' "
    f"  AND ELIG.SGSG_CK = SGSG.SGSG_CK "
    f"  AND ELIG.MEPE_TERM_DT = (SELECT MAX(ELIG2.MEPE_TERM_DT) "
    f"                           FROM {FacetsOwner}.CMC_MEPE_PRCS_ELIG ELIG2 "
    f"                           WHERE ELIG2.MEME_CK = ELIG.MEME_CK "
    f"                             AND ELIG2.MEPE_EFF_DT <= '#CurrDate#' "
    f"                             AND ELIG2.MEPE_ELIG_IND = 'N' "
    f"                             AND ELIG2.CSPD_CAT = 'M')"
)
df_lnkSbsbMedNoCov = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_lnkSbsbMedNoCov)
    .load()
    .select(
        F.col("SBSB_CK").alias("SBSB_CK"),
        F.col("SBSB_ID").alias("SBSB_ID"),
        F.col("SGSG_ID").alias("SGSG_ID")
    )
)
df_lnkSbsbMedNoCov = dedup_sort(df_lnkSbsbMedNoCov, ["SBSB_CK"], [])

extract_query_lnkSbsbDentNoCov = (
    f"SELECT SBSB.SBSB_CK, SBSB.SBSB_ID, SGSG.SGSG_ID "
    f"FROM {FacetsOwner}.CMC_BLEI_ENTY_INFO BLEI,"
    f"     {FacetsOwner}.CMC_MEME_MEMBER MEME, "
    f"     {FacetsOwner}.CMC_MEPE_PRCS_ELIG ELIG,"
    f"     {FacetsOwner}.CMC_SBSB_SUBSC SBSB,"
    f"     {FacetsOwner}.CMC_SGSG_SUB_GROUP SGSG "
    f"WHERE BLEI.BLEI_BILL_LEVEL_CK = SBSB.SBSB_CK "
    f"  AND BLEI.BLEI_BILL_LEVEL = 'I' "
    f"  AND SBSB.SBSB_CK = MEME.SBSB_CK "
    f"  AND MEME.MEME_CK = ELIG.MEME_CK "
    f"  AND MEME.MEME_REL = 'M' "
    f"  AND ELIG.MEPE_EFF_DT <= '#CurrDate#' "
    f"  AND ELIG.CSPD_CAT = 'D' "
    f"  AND ELIG.MEPE_ELIG_IND = 'N' "
    f"  AND ELIG.SGSG_CK = SGSG.SGSG_CK "
    f"  AND ELIG.MEPE_TERM_DT = (SELECT MAX(ELIG2.MEPE_TERM_DT) "
    f"                           FROM {FacetsOwner}.CMC_MEPE_PRCS_ELIG ELIG2 "
    f"                           WHERE ELIG2.MEME_CK = ELIG.MEME_CK "
    f"                             AND ELIG2.MEPE_EFF_DT <= '#CurrDate#' "
    f"                             AND ELIG2.MEPE_ELIG_IND = 'N' "
    f"                             AND ELIG2.CSPD_CAT = 'D')"
)
df_lnkSbsbDentNoCov = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_lnkSbsbDentNoCov)
    .load()
    .select(
        F.col("SBSB_CK").alias("SBSB_CK"),
        F.col("SBSB_ID").alias("SBSB_ID"),
        F.col("SGSG_ID").alias("SGSG_ID")
    )
)
df_lnkSbsbDentNoCov = dedup_sort(df_lnkSbsbDentNoCov, ["SBSB_CK"], [])

extract_query_lnkSbsbOthrNoCov = (
    f"SELECT SBSB.SBSB_CK, SBSB.SBSB_ID, SGSG.SGSG_ID "
    f"FROM {FacetsOwner}.CMC_BLEI_ENTY_INFO BLEI,"
    f"     {FacetsOwner}.CMC_MEME_MEMBER MEME, "
    f"     {FacetsOwner}.CMC_MEPE_PRCS_ELIG ELIG,"
    f"     {FacetsOwner}.CMC_SBSB_SUBSC SBSB,"
    f"     {FacetsOwner}.CMC_SGSG_SUB_GROUP SGSG "
    f"WHERE BLEI.BLEI_BILL_LEVEL_CK = SBSB.SBSB_CK "
    f"  AND BLEI.BLEI_BILL_LEVEL = 'I' "
    f"  AND SBSB.SBSB_CK = MEME.SBSB_CK "
    f"  AND MEME.MEME_CK = ELIG.MEME_CK "
    f"  AND ELIG.MEPE_EFF_DT <= '#CurrDate#' "
    f"  AND MEME.MEME_REL = 'M' "
    f"  AND ELIG.CSPD_CAT IN ('A','F','L','S','X','Y') "
    f"  AND ELIG.MEPE_ELIG_IND = 'N' "
    f"  AND ELIG.SGSG_CK = SGSG.SGSG_CK "
    f"  AND ELIG.MEPE_TERM_DT = (SELECT MAX(ELIG2.MEPE_TERM_DT) "
    f"                           FROM {FacetsOwner}.CMC_MEPE_PRCS_ELIG ELIG2 "
    f"                           WHERE ELIG2.MEME_CK = ELIG.MEME_CK "
    f"                             AND ELIG2.MEPE_EFF_DT <= '#CurrDate#' "
    f"                             AND ELIG2.MEPE_ELIG_IND = 'N' "
    f"                             AND ELIG2.CSPD_CAT IN ('A','F','L','S','X','Y'))"
)
df_lnkSbsbOthrNoCov = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_lnkSbsbOthrNoCov)
    .load()
    .select(
        F.col("SBSB_CK").alias("SBSB_CK"),
        F.col("SBSB_ID").alias("SBSB_ID"),
        F.col("SGSG_ID").alias("SGSG_ID"),
        F.lit(None).alias("GRGR_ID")  # forcing to match the hashed-file definition that mentions GRGR_ID
    )
)
df_lnkSbsbOthrNoCov = dedup_sort(df_lnkSbsbOthrNoCov, ["SBSB_CK"], [])

extract_query_lnkSGSGSubgrp = (
    f"SELECT SG.SGSG_CK, SG.SGSG_ID, GR.GRGR_ID "
    f"FROM {FacetsOwner}.CMC_BLEI_ENTY_INFO BLEI, "
    f"{FacetsOwner}.CMC_SGSG_SUB_GROUP SG, "
    f"{FacetsOwner}.CMC_GRGR_GROUP GR "
    f"WHERE SG.GRGR_CK = GR.GRGR_CK AND "
    f"      BLEI.BLEI_BILL_LEVEL_CK = SG.SGSG_CK AND "
    f"      BLEI.BLEI_BILL_LEVEL = 'S'"
)
df_lnkSGSGSubgrp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_lnkSGSGSubgrp)
    .load()
    .select(
        F.col("SGSG_CK").alias("SGSG_CK"),
        F.col("SGSG_ID").alias("SGSG_ID"),
        F.col("GRGR_ID").alias("GRGR_ID")
    )
)
df_lnkSGSGSubgrp = dedup_sort(df_lnkSGSGSubgrp, ["SGSG_CK"], [])

extract_query_lnkSBSBSub = (
    f"SELECT SBSB.SBSB_CK, SBSB.SBSB_ID, GR.GRGR_ID "
    f"FROM {FacetsOwner}.CMC_BLEI_ENTY_INFO BLEI, "
    f"{FacetsOwner}.CMC_SBSB_SUBSC SBSB, "
    f"{FacetsOwner}.CMC_GRGR_GROUP GR "
    f"WHERE SBSB.GRGR_CK = GR.GRGR_CK AND "
    f"      BLEI.BLEI_BILL_LEVEL_CK = SBSB.SBSB_CK AND "
    f"      BLEI.BLEI_BILL_LEVEL = 'I'"
)
df_lnkSBSBSub = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_lnkSBSBSub)
    .load()
    .select(
        F.col("SBSB_CK").alias("SBSB_CK"),
        F.col("SBSB_ID").alias("SBSB_ID"),
        F.col("GRGR_ID").alias("GRGR_ID")
    )
)
df_lnkSBSBSub = dedup_sort(df_lnkSBSBSub, ["SBSB_CK"], [])

df_StripField_Joined = (
    df_Extract.alias("Extract")
    .join(df_lnkSGSGSub.alias("refSgsgSub"), F.col("Extract.BLEI_BILL_LEVEL_CK") == F.col("refSgsgSub.SGSG_CK"), "left")
    .join(df_lnkSbsbMedCov.alias("refSbsbMedCov"), F.col("Extract.BLEI_BILL_LEVEL_CK") == F.col("refSbsbMedCov.SBSB_CK"), "left")
    .join(df_lnkSbsbDentCov.alias("refSbsbDentCov"), F.col("Extract.BLEI_BILL_LEVEL_CK") == F.col("refSbsbDentCov.SBSB_CK"), "left")
    .join(df_lnkSbsbOthrCov.alias("refSbsbOthrCov"), F.col("Extract.BLEI_BILL_LEVEL_CK") == F.col("refSbsbOthrCov.SBSB_CK"), "left")
    .join(df_lnkSbsbMedNoCov.alias("refSbsbMedNoCov"), F.col("Extract.BLEI_BILL_LEVEL_CK") == F.col("refSbsbMedNoCov.SBSB_CK"), "left")
    .join(df_lnkSbsbDentNoCov.alias("refSbsbDentNoCov"), F.col("Extract.BLEI_BILL_LEVEL_CK") == F.col("refSbsbDentNoCov.SBSB_CK"), "left")
    .join(df_lnkSbsbOthrNoCov.alias("refSbsbOthrNoCov"), F.col("Extract.BLEI_BILL_LEVEL_CK") == F.col("refSbsbOthrNoCov.SBSB_CK"), "left")
    .join(df_lnkSGSGSubgrp.alias("refSGSGSubgrp"), F.col("Extract.BLEI_BILL_LEVEL_CK") == F.col("refSGSGSubgrp.SGSG_CK"), "left")
    .join(df_lnkSBSBSub.alias("refSBSBSub"), F.col("Extract.BLEI_BILL_LEVEL_CK") == F.col("refSBSBSub.SBSB_CK"), "left")
)

df_Strip = df_StripField_Joined.select(
    F.col("Extract.BLEI_CK").alias("BILL_ENTY"),
    F.col("Extract.BLEI_BILL_LEVEL").alias("BILL_ENTY_LVL_CD"),
    F.col("Extract.BLEI_BILL_LEVEL_CK").alias("BILL_ENTY_LEVEL_CK"),
    F.when(
        (F.col("refSGSGSubgrp.SGSG_ID").isNull()) | (F.length(trim(F.col("refSGSGSubgrp.SGSG_ID"))) == 0),
        F.lit("NA")
    ).otherwise(F.col("refSGSGSubgrp.SGSG_ID")).alias("GRP_SGSG_ID"),
    F.when(
        F.trim(F.col("Extract.BLEI_BILL_LEVEL")) == F.lit("S"),
        F.when(F.col("refSGSGSubgrp.GRGR_ID").isNull(), F.lit("NA")).otherwise(F.col("refSGSGSubgrp.GRGR_ID"))
    ).otherwise(
        F.when(
            F.trim(F.col("Extract.BLEI_BILL_LEVEL")) == F.lit("I"),
            F.when(F.col("refSgsgSub.GRGR_ID").isNull(), F.lit("NA")).otherwise(F.col("refSgsgSub.GRGR_ID"))
        ).otherwise(F.lit("NA"))
    ).alias("GRP_GRGR_ID"),
    F.when(
        F.trim(F.col("Extract.BLEI_BILL_LEVEL")) == F.lit("I"),
        F.when(F.col("refSBSBSub.SBSB_ID").isNull(), F.lit("NA")).otherwise(F.col("refSBSBSub.SBSB_ID"))
    ).otherwise(F.lit("NA")).alias("SUB_SBSB_ID"),
    F.when(
        F.col("refSbsbMedCov.SGSG_ID").isNull(),
        F.when(
            F.col("refSbsbDentCov.SGSG_ID").isNull(),
            F.when(
                F.col("refSbsbOthrCov.SGSG_ID").isNull(),
                F.when(
                    F.col("refSbsbMedNoCov.SGSG_ID").isNull(),
                    F.when(
                        F.col("refSbsbDentNoCov.SGSG_ID").isNull(),
                        F.when(
                            F.col("refSbsbOthrNoCov.SGSG_ID").isNull(),
                            F.lit("NA")
                        ).otherwise(F.col("refSbsbOthrNoCov.SGSG_ID"))
                    ).otherwise(F.col("refSbsbDentNoCov.SGSG_ID"))
                ).otherwise(F.col("refSbsbMedNoCov.SGSG_ID"))
            ).otherwise(F.col("refSbsbOthrCov.SGSG_ID"))
        ).otherwise(F.col("refSbsbDentCov.SGSG_ID"))
    ).otherwise(F.col("refSbsbMedCov.SGSG_ID")).alias("SUB_SGSG_ID"),
    F.when(
        F.trim(F.col("Extract.BLEI_BILL_LEVEL")) == F.lit("I"),
        F.col("Extract.BLEI_BILL_LEVEL_CK")
    ).otherwise(F.lit("NA")).alias("SUB_CK")
)

df_Snapshot = df_StripField_Joined.select(
    F.col("Extract.BLEI_CK").alias("BLEI_CK")
)

df_Transform = df_Snapshot.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("BLEI_CK").alias("BILL_ENTY_UNIQ_KEY")
)

write_files(
    df_Transform,
    f"{adls_path}/load/B_BILL_ENTY.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)

df_BusinessRulesAllCol = df_Strip.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("BILL_ENTY").alias("BILL_ENTY"),
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    F.lit(CurrDate).alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit("FACETS").alias("SRC_SYS_CD"),
    F.concat(F.lit("FACETS;"), trim(F.col("BILL_ENTY"))).alias("PRI_KEY_STRING"),
    F.lit(0).alias("BILL_ENTY_SK"),
    F.when(
        F.length(trim(F.col("BILL_ENTY_LVL_CD"))) == 0,
        F.lit("NA")
    ).otherwise(F.col("BILL_ENTY_LVL_CD")).alias("BILL_ENTY_LVL_CD"),
    F.col("GRP_GRGR_ID").alias("GRP_ID"),
    F.when(
        F.col("BILL_ENTY_LVL_CD") == F.lit("S"),
        F.col("GRP_SGSG_ID")
    ).otherwise(
        F.when(
            F.col("BILL_ENTY_LVL_CD") == F.lit("I"),
            F.col("SUB_SGSG_ID")
        ).otherwise(F.lit("NA"))
    ).alias("SUBGRP_ID"),
    F.col("SUB_SBSB_ID").alias("SUB_ID"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("BILL_ENTY_LEVEL_CK").alias("BILL_ENTY_LVL_UNIQ_KEY"),
    F.col("SUB_CK").alias("SUB_CK")
)

df_BusinessRulesTransform = df_Strip.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("BILL_ENTY").alias("BILL_ENTY_UNIQ_KEY")
)

params_ComsnBillEntyPK = {
    "CurrRunCycle": CurrRunCycle,
    "SrcSysCd": SrcSysCd,
    "RunID": RunID,
    "CurrentDate": CurrDate,
    "IDSOwner": IDSOwner
}

df_ComsnBillEntyPKOutput = ComsnBillEntyPK(df_BusinessRulesAllCol, df_BusinessRulesTransform, params_ComsnBillEntyPK)

df_IdsBillEnty_pre = df_ComsnBillEntyPKOutput.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "BILL_ENTY_SK",
    "BILL_ENTY_UNIQ_KEY",
    "GRP_ID",
    "SUBGRP_ID",
    "SUB_ID",
    "BILL_ENTY_LVL_CD",
    "BILL_ENTY_LVL_UNIQ_KEY",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "SUB_CK"
)

df_IdsBillEnty = (
    df_IdsBillEnty_pre
    .withColumn("INSRT_UPDT_CD", F.rpad(F.col("INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", F.rpad(F.col("DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", F.rpad(F.col("PASS_THRU_IN"), 1, " "))
    .withColumn("BILL_ENTY_LVL_CD", F.rpad(F.col("BILL_ENTY_LVL_CD"), 1, " "))
    .withColumn("GRP_ID", F.rpad(F.col("GRP_ID"), 8, " "))
    .withColumn("SUBGRP_ID", F.rpad(F.col("SUBGRP_ID"), 9, " "))
    .withColumn("SUB_ID", F.rpad(F.col("SUB_ID"), 4, " "))
)

write_files(
    df_IdsBillEnty,
    f"{adls_path}/key/{TmpOutFile}",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)