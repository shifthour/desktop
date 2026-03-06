# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC ***************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     FctsClmDriverHitList
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
# MAGIC Goutham K            2021-06-21           US37256              LHO to Facets Conversion Use CRT DT for ADJ FROM                                  IntegrateDev2            Kalyan Neelam            2021-06-21
# MAGIC Prabhu ES            2022-02-26       S2S Remediation      MSSQL connection parameters added                                                              IntegrateDev5

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
from pyspark.sql.functions import col, lit, coalesce, rpad, substring
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


DriverTable = get_widget_value('DriverTable','')
MonthsInMart = get_widget_value('MonthsInMart','')
FacetsOwner = get_widget_value('FacetsOwner','')
BCBSOwner = get_widget_value('BCBSOwner','')
FacetsDB = get_widget_value('FacetsDB','')

facets_secret_name = get_widget_value('facets_secret_name','')
bcbs_secret_name = get_widget_value('bcbs_secret_name','')
facetsdb_secret_name = get_widget_value('facetsdb_secret_name','')

jdbc_url_CMC_CLCL_CLAIM, jdbc_props_CMC_CLCL_CLAIM = get_db_config(facets_secret_name)
extract_query_CMC_CLCL_CLAIM = f"""
SELECT
  CAST(Trim(CLCL_ID) as CHAR(12)) AS CLCL_ID,
  CLCL_CUR_STS,
  CLCL_PAID_DT,
  CLCL_ID_ADJ_TO,
  CASE
    WHEN SUBSTRING(CLCL_ID_ADJ_FROM,1,1) = 'L' AND SUBSTRING(CLCL_ID_CRTE_FROM,1,1) = 'L'
         THEN CLCL_ID_CRTE_FROM
    WHEN (CLCL_ID_ADJ_FROM = '' OR LEN(RTRIM(LTRIM(CLCL_ID_ADJ_FROM))) IS NULL)
         THEN CLCL_ID_CRTE_FROM
    ELSE CLCL_ID_ADJ_FROM
  END as CLCL_ID_ADJ_FROM,
  CLCL_LAST_ACT_DTM
FROM {FacetsOwner}.CMC_CLCL_CLAIM clm1
WHERE NOT EXISTS (
      SELECT 'Y'
      FROM {FacetsOwner}.CMC_CLCL_CLAIM clm2,
           {FacetsOwner}.CMC_CLMI_MISC clmi,
           {FacetsOwner}.CMC_GRGR_GROUP grgr
      WHERE clm1.CLCL_ID = clm2.CLCL_ID
        AND clm2.CLCL_ID = clmi.CLCL_ID
        AND clm2.GRGR_CK = grgr.GRGR_CK
        AND (
             clmi.CLMI_LOCAL_PLAN_CD = 'PAR'
             OR grgr.GRGR_ID = 'NASCOPAR'
            )
    )
  AND NOT EXISTS (
      SELECT 'Y'
      FROM {BCBSOwner}.P_SEL_PRCS_CRITR P_SEL_PRCS_CRITR,
           {FacetsDB}.dbo.CMC_GRGR_GROUP CMC_GRGR_GROUP
      WHERE P_SEL_PRCS_CRITR.SEL_PRCS_ID = 'WRHSINBEXCL'
        AND P_SEL_PRCS_CRITR.SEL_PRCS_ITEM_ID = 'WORKCOMP'
        AND P_SEL_PRCS_CRITR.SEL_PRCS_CRITR_CD = 'MEDPROC'
        AND CMC_GRGR_GROUP.GRGR_ID >= P_SEL_PRCS_CRITR.CRITR_VAL_FROM_TX
        AND CMC_GRGR_GROUP.GRGR_ID <= P_SEL_PRCS_CRITR.CRITR_VAL_THRU_TX
        AND clm1.GRGR_CK=CMC_GRGR_GROUP.GRGR_CK
    )
"""
df_CMC_CLCL_CLAIM = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_CMC_CLCL_CLAIM)
    .options(**jdbc_props_CMC_CLCL_CLAIM)
    .option("query", extract_query_CMC_CLCL_CLAIM)
    .load()
)

schema_FctsClmHitList = StructType([
    StructField("CLCL_ID", StringType(), True)
])
df_FctsClmHitList = (
    spark.read.format("csv")
    .option("sep", ",")
    .option("header", "false")
    .option("quote", "\"")
    .schema(schema_FctsClmHitList)
    .load(f"{adls_path_raw}/landing/FctsClmHitList.dat")
)

df_joined = df_FctsClmHitList.alias("Clm").join(
    df_CMC_CLCL_CLAIM.alias("ClmLkup"),
    on=[df_FctsClmHitList["CLCL_ID"] == df_CMC_CLCL_CLAIM["CLCL_ID"]],
    how="left"
)

df_lnkClmsout = df_joined.filter(col("ClmLkup.CLCL_ID").isNotNull()).select(
    rpad(substring(col("Clm.CLCL_ID"), 1, 12), 12, " ").alias("CLM_ID"),
    rpad(col("ClmLkup.CLCL_CUR_STS"), 2, " ").alias("CLM_STS"),
    coalesce(col("ClmLkup.CLCL_PAID_DT"), lit("1753-01-01 00:00:00.000")).alias("CLCL_PAID_DT"),
    rpad(coalesce(col("ClmLkup.CLCL_ID_ADJ_TO"), lit(" ")), 12, " ").alias("CLCL_ID_ADJ_TO"),
    rpad(coalesce(col("ClmLkup.CLCL_ID_ADJ_FROM"), lit(" ")), 12, " ").alias("CLCL_ID_ADJ_FROM"),
    coalesce(col("ClmLkup.CLCL_LAST_ACT_DTM"), lit("1753-01-01 00:00:00.000")).alias("CLCL_LAST_ACT_DTM")
)

df_NoClaimHit = df_joined.filter(col("ClmLkup.CLCL_ID").isNull()).select(
    rpad(col("Clm.CLCL_ID"), <...>, " ").alias("CLCL_ID")
)

jdbc_url_TMP_IDS_CLAIM, jdbc_props_TMP_IDS_CLAIM = get_db_config(facets_secret_name)
temp_table_name = "tempdb.FctsClmDriverHitList_TMP_IDS_CLAIM_temp"
execute_dml(f"DROP TABLE IF EXISTS {temp_table_name}", jdbc_url_TMP_IDS_CLAIM, jdbc_props_TMP_IDS_CLAIM)

df_lnkClmsout.write.format("jdbc") \
    .option("url", jdbc_url_TMP_IDS_CLAIM) \
    .options(**jdbc_props_TMP_IDS_CLAIM) \
    .option("dbtable", temp_table_name) \
    .mode("overwrite") \
    .save()

table_name = f"tempdb..{DriverTable}"
merge_sql = f"""
MERGE {table_name} AS T
USING {temp_table_name} AS S
ON T.CLM_ID = S.CLM_ID
WHEN MATCHED THEN
  UPDATE SET
    T.CLM_ID = S.CLM_ID,
    T.CLM_STS = S.CLM_STS,
    T.CLCL_PAID_DT = S.CLCL_PAID_DT,
    T.CLCL_ID_ADJ_TO = S.CLCL_ID_ADJ_TO,
    T.CLCL_ID_ADJ_FROM = S.CLCL_ID_ADJ_FROM,
    T.CLCL_LAST_ACT_DTM = S.CLCL_LAST_ACT_DTM
WHEN NOT MATCHED THEN
  INSERT (
    CLM_ID,
    CLM_STS,
    CLCL_PAID_DT,
    CLCL_ID_ADJ_TO,
    CLCL_ID_ADJ_FROM,
    CLCL_LAST_ACT_DTM
  )
  VALUES (
    S.CLM_ID,
    S.CLM_STS,
    S.CLCL_PAID_DT,
    S.CLCL_ID_ADJ_TO,
    S.CLCL_ID_ADJ_FROM,
    S.CLCL_LAST_ACT_DTM
  );
"""
execute_dml(merge_sql, jdbc_url_TMP_IDS_CLAIM, jdbc_props_TMP_IDS_CLAIM)

write_files(
    df_NoClaimHit,
    f"{adls_path_raw}/landing/FctsClaimHitListNullLkp.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)