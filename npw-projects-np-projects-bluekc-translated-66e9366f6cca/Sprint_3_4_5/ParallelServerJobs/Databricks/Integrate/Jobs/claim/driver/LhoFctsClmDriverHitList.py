# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC ***************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007, 2022 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     FctsClmDriverHitList
# MAGIC CALLED BY:   LhoFctsClmOnDmdPrereqSeq
# MAGIC 
# MAGIC                            
# MAGIC PROCESSING:  Read list of claim IDs from FctsClmHitList.dat in ../update directory.  Claim IDs in FctsClmHitList.dat are used in SQL to get all information for loading into the TMP_IDS_CLAIM table.  The file will be removed by the job control after this job completes.
# MAGIC                  
# MAGIC                     
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                                       Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                                               --------------------------------       -------------------------------   ----------------------------       
# MAGIC Brent Leland            11/16/2004-                                   Originally Programmed
# MAGIC BJ Luce                   04/19/2005                                    add Last Activity date
# MAGIC BJ Luce                   04/20/2005                                    change from bulkload to Sybase stage to handle 
# MAGIC                                                                                        duplicates. Put constraint to bypass claims not on the facets 
# MAGIC                                                                                        clcl table
# MAGIC Brent Leland            06/21/2005                                   Took space off the end of the claim mart hit list file name.
# MAGIC Brent Leland            03/01/2006                                   Changed Job to be used in sequencer.  Used environment parameters.
# MAGIC                                                                                       Changed hit list file to update directory.
# MAGIC BJ Luce                   03/17/2006                                   change format of claim mart hit list
# MAGIC Brent Leland            05/22/2006                                   Added temporay fix to append Facets driver table to claim mart hit list file.
# MAGIC                                                                                       This will become obsolete when claim mart drives off of the run cycle table.
# MAGIC                                                                                       Remove clearing of hf_hit_list_build_clm_ids.
# MAGIC Brent Leland            07/19/2006                                   Removed tempory hit list creation since claim mart implemented 
# MAGIC                                                                                       with run cycle pull.
# MAGIC                                                                                       Removed other hit list for claim mart.
# MAGIC Brent Leland           2007-02-23                                     Added logic to SQL lookup for NASCOPAR.
# MAGIC Brent Leland           2007-08-16      IAD Prod. Supp.     Changed hit list file directory from /ids/update to /ids/prod/update                    devlIDS30                          Steph Goddard             8/30/07    
# MAGIC                           
# MAGIC Rick Henry             2012-03-05      TTR-1077               Send Non Matched Hit List ClaimID's to file for email attachment                     IntegrateNewDevl
# MAGIC 
# MAGIC Manasa Andru       2015-03-11       TFS - 10616              Changed the FctsClmHitlist file location from Update                                     IntegrateNewDevl         Kalyan Neelam            2015-03-12
# MAGIC                                                                                          to landing directory
# MAGIC Abhiram D              2016-10-25        5628	   Exclusion Criteria- P_SEL_PRCS_CRITR   				IntegrateDev2               Nathan Reynolds           25-Oct-2016
# MAGIC 					   to remove workers comp GRGR_ID's       
# MAGIC 
# MAGIC Goutham K             2021-07-13    US-386524              Added new file to capure Facets calims that got adjusted from Lumeris claim      IntegrateDev2            Jeyaprasanna                  2021-07-14
# MAGIC                                                                                     To update the Paid date 
# MAGIC Prabhu ES              2022-03-17    S2S                         MSSQL ODBC conn params added                                                                     IntegrateDev5	Ken Bradmon	2022-06-12

# MAGIC Only claims found in lookup are added to the driver table.
# MAGIC /ids/prod/landing/FctsClmHitList.dat
# MAGIC *** Running against facets_archive maybe a problem with subselect tables not being fully populated. ***
# MAGIC If a ClaimID in the HitList cannot be found in CMC_CLC_CLAIM, email notify from FctsClmPrereqSeq
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# Parameters
DriverTable = get_widget_value("DriverTable","")
MonthsInMart = get_widget_value("MonthsInMart","24")
LhoFacetsStgOwner = get_widget_value("LhoFacetsStgOwner","")
lhofacetsstg_secret_name = get_widget_value("lhofacetsstg_secret_name","")
LhoFacetsStgAcct = get_widget_value("LhoFacetsStgAcct","")
LhoFacetsStgPW = get_widget_value("LhoFacetsStgPW","")
SrcSysCdSk = get_widget_value("SrcSysCdSk","")
LhoFacetsStgDSN = get_widget_value("LhoFacetsStgDSN","")

# Stage: FctsClmHitList (CSeqFileStage) - Read .dat file
schema_FctsClmHitList = StructType([
    StructField("CLCL_ID", StringType(), True)
])
df_FctsClmHitList = (
    spark.read
    .format("csv")
    .option("header", "false")
    .option("delimiter", ",")
    .option("quote", "\"")
    .schema(schema_FctsClmHitList)
    .load(f"{adls_path_raw}/landing/LhoFctsClmHitList.dat")
)

# Stage: CMC_CLCL_CLAIM (ODBCConnector) - Read from DB with NOT EXISTS filter
jdbc_url, jdbc_props = get_db_config(lhofacetsstg_secret_name)
extract_query_CMC_CLCL_CLAIM = f"""
SELECT
  c1.CLCL_ID,
  c1.CLCL_CUR_STS,
  c1.CLCL_PAID_DT,
  c1.CLCL_ID_ADJ_TO,
  CASE
    WHEN SUBSTRING(c1.CLCL_ID_ADJ_FROM,1,1) = 'L' AND SUBSTRING(c1.CLCL_ID_CRTE_FROM,1,1) = 'L'
      THEN c1.CLCL_ID_CRTE_FROM
    WHEN (c1.CLCL_ID_ADJ_FROM = '' OR LEN(RTRIM(LTRIM(c1.CLCL_ID_ADJ_FROM))) IS NULL)
      THEN c1.CLCL_ID_CRTE_FROM
    ELSE c1.CLCL_ID_ADJ_FROM
  END AS CLCL_ID_ADJ_FROM,
  c1.CLCL_LAST_ACT_DTM
FROM {LhoFacetsStgOwner}.CMC_CLCL_CLAIM c1
WHERE NOT EXISTS (
  SELECT 'Y'
  FROM {LhoFacetsStgOwner}.CMC_CLCL_CLAIM c2,
       {LhoFacetsStgOwner}.CMC_CLMI_MISC clmi,
       {LhoFacetsStgOwner}.CMC_GRGR_GROUP grgr
  WHERE c1.CLCL_ID = c2.CLCL_ID
    AND c2.CLCL_ID = clmi.CLCL_ID
    AND c2.GRGR_CK = grgr.GRGR_CK
    AND (clmi.CLMI_LOCAL_PLAN_CD = 'PAR' OR grgr.GRGR_ID = 'NASCOPAR')
)
"""
df_CMC_CLCL_CLAIM = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_CMC_CLCL_CLAIM)
    .load()
)

# Stage: TrnsSK (CTransformerStage) - Join logic
df_join = df_FctsClmHitList.alias("Clm").join(
    df_CMC_CLCL_CLAIM.alias("ClmLkup"),
    F.col("Clm.CLCL_ID") == F.col("ClmLkup.CLCL_ID"),
    "left"
)

# Output link: lnkClmsout
df_lnkClmsout = df_join.filter(
    F.col("ClmLkup.CLCL_ID").isNotNull() &
    (F.col("ClmLkup.CLCL_ID").substr(1, 1) == F.lit("L"))
)
df_lnkClmsout = df_lnkClmsout.select(
    F.rpad(F.col("Clm.CLCL_ID").substr(1, 12), 12, " ").alias("CLM_ID"),
    F.rpad(F.col("ClmLkup.CLCL_CUR_STS"), 2, " ").alias("CLM_STS"),
    F.coalesce(F.col("ClmLkup.CLCL_PAID_DT"), F.lit("1753-01-01 00:00:00.000")).alias("CLCL_PAID_DT"),
    F.rpad(F.coalesce(F.col("ClmLkup.CLCL_ID_ADJ_TO"), F.lit(" ")), 12, " ").alias("CLCL_ID_ADJ_TO"),
    F.rpad(F.coalesce(F.col("ClmLkup.CLCL_ID_ADJ_FROM"), F.lit(" ")), 12, " ").alias("CLCL_ID_ADJ_FROM"),
    F.coalesce(F.col("ClmLkup.CLCL_LAST_ACT_DTM"), F.lit("1753-01-01 00:00:00.000")).alias("CLCL_LAST_ACT_DTM")
)

# Output link: NoClaimHit
df_noClaimHit = df_join.filter(
    F.col("ClmLkup.CLCL_ID").isNull()
)
df_noClaimHit = df_noClaimHit.select(
    F.rpad(F.col("Clm.CLCL_ID"), 12, " ").alias("CLCL_ID")
)

# Output link: Lnk_PdDt_Updt
df_pdDtUpdt = df_join.filter(
    F.col("Clm.CLCL_ID").substr(1, 1) != F.lit("L")
)
df_pdDtUpdt = df_pdDtUpdt.select(
    F.rpad(F.coalesce(F.col("ClmLkup.CLCL_ID_ADJ_FROM"), F.lit(" ")), 12, " ").alias("CLM_ID"),
    F.coalesce(F.col("ClmLkup.CLCL_PAID_DT").substr(1, 10), F.lit("1753-01-01")).alias("CLCL_PAID_DT"),
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK")
)

# Stage: FctsClaimHitListNullLkp (CSeqFileStage) - Write .dat file
write_files(
    df_noClaimHit,
    f"{adls_path_raw}/landing/LhoFctsClaimHitListNullLkp.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# Stage: Seq_PaidDtUpdt (CSeqFileStage) - Write .dat file
write_files(
    df_pdDtUpdt,
    f"{adls_path}/load/LhoFctsClmPdDtUpdt.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# Stage: TMP_IDS_CLAIM (ODBCConnector) - Merge into tempdb..#DriverTable#
df_tmp_ids_claim = df_lnkClmsout

execute_dml(
    f"DROP TABLE IF EXISTS tempdb.LhoFctsClmDriverHitList_TMP_IDS_CLAIM_temp",
    jdbc_url,
    jdbc_props
)

df_tmp_ids_claim.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "tempdb.LhoFctsClmDriverHitList_TMP_IDS_CLAIM_temp") \
    .mode("overwrite") \
    .save()

merge_sql = f"""
MERGE tempdb..{DriverTable} AS target
USING tempdb.LhoFctsClmDriverHitList_TMP_IDS_CLAIM_temp AS source
ON (target.CLM_ID = source.CLM_ID)
WHEN MATCHED THEN
  UPDATE SET
    target.CLM_STS = source.CLM_STS,
    target.CLCL_PAID_DT = source.CLCL_PAID_DT,
    target.CLCL_ID_ADJ_TO = source.CLCL_ID_ADJ_TO,
    target.CLCL_ID_ADJ_FROM = source.CLCL_ID_ADJ_FROM,
    target.CLCL_LAST_ACT_DTM = source.CLCL_LAST_ACT_DTM
WHEN NOT MATCHED THEN
  INSERT (CLM_ID, CLM_STS, CLCL_PAID_DT, CLCL_ID_ADJ_TO, CLCL_ID_ADJ_FROM, CLCL_LAST_ACT_DTM)
  VALUES (source.CLM_ID, source.CLM_STS, source.CLCL_PAID_DT, source.CLCL_ID_ADJ_TO, source.CLCL_ID_ADJ_FROM, source.CLCL_LAST_ACT_DTM);
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)