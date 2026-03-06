# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC **************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2006 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:  Load tempdb driver table with Facets claims for processing
# MAGIC                           
# MAGIC PROCESSING:  Extract claims from Facets claim status and claim table based on input date range and load them to a driver table in tempdb.
# MAGIC   
# MAGIC MODIFICATIONS:
# MAGIC           04/14/2006-      Brent Leland  Originally Programmed
# MAGIC           04/28/2006       Brent Leland  Changed index to temp table to be clustered.
# MAGIC           10/02/2006       Hugh Sisson  Added CMC_CKCK_CHECK query so to include rows with changes affecting CDHP (CLM_CHK table)
# MAGIC 
# MAGIC NAme            DAte            Project                                            Description                                                                                                                         environment      Code Review    Date Reviewed
# MAGIC SAndrew    2008-09-09   #3057 Web Provider Resdesign    	Aded box CMC_CLCL_CLAIM_orig - get original claim when activity on adjusting             devlIDSnew       Steph Goddard   01/15/2009
# MAGIC                                                                                                Missing too many in production.
# MAGIC AbhiramD	  2016-10-25   #5628 Workers Comp		Exclusion Criteria- P_SEL_PRCS_CRITR   to remove workers comp GRGR_ID's             IntegrateDev2     Nathan Reynolds  25 Oct 2016
# MAGIC 
# MAGIC GouthamK  2021-05-26    US- 386524 Fix Reversals              Exclude When CLM.CLCL_ID starts with an 'L' And CLM.CLCL_CUR_STS = 81              IntegrateDev2      Jeyaprasanna       2021-06-02
# MAGIC 
# MAGIC GouthamK  2021-06-10     US-372568                                   LHO Conversion - modified SQL to use CLCL_ID_CRTE_FROM                                       IntegrateDev2        Jeyaprasanna       2021-06-16
# MAGIC 
# MAGIC GouthamK  2021-07-13     US-386524                                    Lho Reversals - Created new hit list file to process L claims from the LhoClmCntl Job        IntegrateDev2       Jeyaprasanna        2021-07-14
# MAGIC Prabhu ES  2022-02-26     S2S Remediation                          MSSQL connection parameters added                                                                               IntegrateDev5
# MAGIC 
# MAGIC Ravi Ranjan 2024-10-09   US-630548                                    Added with(nolock) for all the tables in queries of stages                                                   IntegrateDev1        Jeyaprasanna       2024-10-14
# MAGIC                                                                                                 CMC_CLCL_CLAIM, CMC_CLST_STATUS, 
# MAGIC                                                                                                 CMC_CKCK_CHECK, CMC_CLCL_CLAIM_orig.
# MAGIC                                                                                                 and removed  the commented lines.

# MAGIC Remove Duplicate records
# MAGIC Find changed claims from the claim and status tables.
# MAGIC Insert records into driver table
# MAGIC Find changed claims from the check table for CDHP purposes.
# MAGIC HitList for Lumeris Claims(LhoFctsClmCntl Job)
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import substring, rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


DriverTable = get_widget_value('DriverTable','')
BeginDate = get_widget_value('BeginDate','')
EndDate = get_widget_value('EndDate','')
FacetsOwner = get_widget_value('FacetsOwner','')
BCBSOwner = get_widget_value('BCBSOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
bcbs_secret_name = get_widget_value('bcbs_secret_name','')

# CMC_CLCL_CLAIM
jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)
extract_query_CMC_CLCL_CLAIM = f"""
SELECT  CLM.CLCL_ID,
        CLM.CLCL_CUR_STS AS CLM_STS,
        CLM.CLCL_PAID_DT,
        CLM.CLCL_ID_ADJ_TO,
CASE 
    WHEN SUBSTRING(CLM.CLCL_ID_ADJ_FROM ,1,1) = 'L' 
         AND SUBSTRING(CLM.CLCL_ID_CRTE_FROM ,1,1) = 'L' 
    THEN CLM.CLCL_ID_CRTE_FROM 
    WHEN (CLM.CLCL_ID_ADJ_FROM = ''  
          OR LEN(RTRIM(LTRIM(CLM.CLCL_ID_ADJ_FROM))) IS NULL) 
    THEN CLM.CLCL_ID_CRTE_FROM
    ELSE CLM.CLCL_ID_ADJ_FROM 
END AS CLCL_ID_ADJ_FROM,
       CLM.CLCL_LAST_ACT_DTM
FROM  {FacetsOwner}.CMC_CLCL_CLAIM CLM WITH(nolock)
WHERE CLM.CLCL_LAST_ACT_DTM >= '{BeginDate}'
  AND CLM.CLCL_LAST_ACT_DTM <  '{EndDate}'
  AND NOT EXISTS (
      SELECT 'Y'
      FROM {FacetsOwner}.CMC_CLCL_CLAIM clm2 WITH(nolock),
           {FacetsOwner}.CMC_CLMI_MISC clmi WITH(nolock),
           {FacetsOwner}.CMC_GRGR_GROUP grgr WITH(nolock)
      WHERE CLM.CLCL_ID = clm2.CLCL_ID
        AND clm2.CLCL_ID = clmi.CLCL_ID
        AND clm2.GRGR_CK = grgr.GRGR_CK
        AND (clmi.CLMI_LOCAL_PLAN_CD = 'PAR' 
             OR grgr.GRGR_ID = 'NASCOPAR')
  )
  AND NOT EXISTS (
      SELECT 'Y'
      FROM {BCBSOwner}.P_SEL_PRCS_CRITR P_SEL_PRCS_CRITR WITH(nolock),
           {FacetsOwner}.CMC_GRGR_GROUP CMC_GRGR_GROUP WITH(nolock)
      WHERE P_SEL_PRCS_CRITR.SEL_PRCS_ID = 'WRHSINBEXCL'
        AND P_SEL_PRCS_CRITR.SEL_PRCS_ITEM_ID = 'WORKCOMP'
        AND P_SEL_PRCS_CRITR.SEL_PRCS_CRITR_CD = 'MEDPROC'
        AND CMC_GRGR_GROUP.GRGR_ID >= P_SEL_PRCS_CRITR.CRITR_VAL_FROM_TX
        AND CMC_GRGR_GROUP.GRGR_ID <= P_SEL_PRCS_CRITR.CRITR_VAL_THRU_TX
        AND CLM.GRGR_CK=CMC_GRGR_GROUP.GRGR_CK
  )
  AND CASE 
        WHEN SUBSTRING(CLM.CLCL_ID,1,1) = 'L' 
             AND CLM.CLCL_CUR_STS  = '81' 
        THEN 1 
        ELSE 0 
      END = 0
"""
df_CMC_CLCL_CLAIM = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_CMC_CLCL_CLAIM)
    .load()
)

# CMC_CLST_STATUS
extract_query_CMC_CLST_STATUS = f"""
SELECT  CLM.CLCL_ID,
        CLM.CLCL_CUR_STS AS CLM_STS,
        CLM.CLCL_PAID_DT,
        CLM.CLCL_ID_ADJ_TO,
CASE 
    WHEN SUBSTRING(CLM.CLCL_ID_ADJ_FROM ,1,1) = 'L' 
         AND SUBSTRING(CLM.CLCL_ID_CRTE_FROM ,1,1) = 'L' 
    THEN CLM.CLCL_ID_CRTE_FROM 
    WHEN (CLM.CLCL_ID_ADJ_FROM = ''  
          OR LEN(RTRIM(LTRIM(CLM.CLCL_ID_ADJ_FROM))) IS NULL) 
    THEN CLM.CLCL_ID_CRTE_FROM
    ELSE CLM.CLCL_ID_ADJ_FROM 
END AS CLCL_ID_ADJ_FROM,
       CLM.CLCL_LAST_ACT_DTM
FROM   {FacetsOwner}.CMC_CLST_STATUS STATUS WITH(nolock),
       {FacetsOwner}.CMC_CLCL_CLAIM  CLM WITH(nolock)
WHERE  STATUS.CLST_STS_DTM >= '{BeginDate}'
  AND  STATUS.CLST_STS_DTM <  '{EndDate}'
  AND  STATUS.CLCL_ID = CLM.CLCL_ID
  AND  NOT EXISTS (
       SELECT 'Y'
       FROM {FacetsOwner}.CMC_CLST_STATUS clm2 WITH(nolock),
            {FacetsOwner}.CMC_CLMI_MISC clmi WITH(nolock),
            {FacetsOwner}.CMC_MEME_MEMBER meme WITH(nolock),
            {FacetsOwner}.CMC_GRGR_GROUP grgr WITH(nolock)
       WHERE STATUS.CLCL_ID = clm2.CLCL_ID
         AND clm2.CLCL_ID = clmi.CLCL_ID
         AND clm2.MEME_CK = meme.MEME_CK
         AND meme.GRGR_CK = grgr.GRGR_CK
         AND (clmi.CLMI_LOCAL_PLAN_CD = 'PAR'
              OR grgr.GRGR_ID = 'NASCOPAR')
  )
  AND NOT EXISTS (
       SELECT 'Y'
       FROM {BCBSOwner}.P_SEL_PRCS_CRITR P_SEL_PRCS_CRITR WITH(nolock),
            {FacetsOwner}.CMC_GRGR_GROUP CMC_GRGR_GROUP WITH(nolock)
       WHERE P_SEL_PRCS_CRITR.SEL_PRCS_ID = 'WRHSINBEXCL'
         AND P_SEL_PRCS_CRITR.SEL_PRCS_ITEM_ID = 'WORKCOMP'
         AND P_SEL_PRCS_CRITR.SEL_PRCS_CRITR_CD = 'MEDPROC'
         AND CMC_GRGR_GROUP.GRGR_ID >= P_SEL_PRCS_CRITR.CRITR_VAL_FROM_TX
         AND CMC_GRGR_GROUP.GRGR_ID <= P_SEL_PRCS_CRITR.CRITR_VAL_THRU_TX
         AND CLM.GRGR_CK=CMC_GRGR_GROUP.GRGR_CK
  )
  AND CASE 
        WHEN SUBSTRING(CLM.CLCL_ID,1,1) = 'L' 
             AND CLM.CLCL_CUR_STS = '81' 
        THEN 1 
        ELSE 0 
      END = 0
"""
df_CMC_CLST_STATUS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_CMC_CLST_STATUS)
    .load()
)

# CMC_CKCK_CHECK
extract_query_CMC_CKCK_CHECK = f"""
SELECT 
       CLM.CLCL_ID,
       CLM.CLCL_CUR_STS AS CLM_STS,
       CLM.CLCL_PAID_DT,
       CLM.CLCL_ID_ADJ_TO,
CASE 
    WHEN SUBSTRING(CLM.CLCL_ID_ADJ_FROM ,1,1) = 'L' 
         AND SUBSTRING(CLM.CLCL_ID_CRTE_FROM ,1,1) = 'L' 
    THEN CLM.CLCL_ID_CRTE_FROM 
    WHEN (CLM.CLCL_ID_ADJ_FROM = ''  
          OR LEN(RTRIM(LTRIM(CLM.CLCL_ID_ADJ_FROM))) IS NULL) 
    THEN CLM.CLCL_ID_CRTE_FROM
    ELSE CLM.CLCL_ID_ADJ_FROM 
END AS CLCL_ID_ADJ_FROM,
       CLM.CLCL_LAST_ACT_DTM
FROM {FacetsOwner}.CMC_CKCK_CHECK CHK WITH(nolock),
     {FacetsOwner}.CMC_CLCK_CLM_CHECK CLMCHECK WITH(nolock),
     {FacetsOwner}.CMC_CLCL_CLAIM CLM WITH(nolock)
WHERE CHK.CKCK_REISS_DT <> '1753-01-01'
  AND CHK.CKCK_PRINTED_DT >= '{BeginDate}'
  AND CLM.CLCL_ID = CLMCHECK.CLCL_ID
  AND CLMCHECK.CKPY_REF_ID = CHK.CKPY_REF_ID
  AND NOT EXISTS (
      SELECT 'Y'
      FROM {FacetsOwner}.CMC_CLST_STATUS clm2 WITH(nolock),
           {FacetsOwner}.CMC_CLMI_MISC clmi WITH(nolock),
           {FacetsOwner}.CMC_MEME_MEMBER meme WITH(nolock),
           {FacetsOwner}.CMC_GRGR_GROUP grgr WITH(nolock)
      WHERE CLM.CLCL_ID = clm2.CLCL_ID
        AND clm2.CLCL_ID = clmi.CLCL_ID
        AND clm2.MEME_CK = meme.MEME_CK
        AND meme.GRGR_CK = grgr.GRGR_CK
        AND (clmi.CLMI_LOCAL_PLAN_CD = 'PAR' 
             OR grgr.GRGR_ID = 'NASCOPAR')
  )
  AND NOT EXISTS (
      SELECT 'Y'
      FROM {BCBSOwner}.P_SEL_PRCS_CRITR P_SEL_PRCS_CRITR WITH(nolock),
           {FacetsOwner}.CMC_GRGR_GROUP CMC_GRGR_GROUP WITH(nolock)
      WHERE P_SEL_PRCS_CRITR.SEL_PRCS_ID = 'WRHSINBEXCL'
        AND P_SEL_PRCS_CRITR.SEL_PRCS_ITEM_ID = 'WORKCOMP'
        AND P_SEL_PRCS_CRITR.SEL_PRCS_CRITR_CD = 'MEDPROC'
        AND CMC_GRGR_GROUP.GRGR_ID >= P_SEL_PRCS_CRITR.CRITR_VAL_FROM_TX
        AND CMC_GRGR_GROUP.GRGR_ID <= P_SEL_PRCS_CRITR.CRITR_VAL_THRU_TX
        AND CLM.GRGR_CK=CMC_GRGR_GROUP.GRGR_CK
  )
  AND CASE 
        WHEN SUBSTRING(CLM.CLCL_ID,1,1) = 'L' 
             AND CLM.CLCL_CUR_STS  = '81' 
        THEN 1 
        ELSE 0 
      END = 0
"""
df_CMC_CKCK_CHECK = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_CMC_CKCK_CHECK)
    .load()
)

# CMC_CLCL_CLAIM_orig
extract_query_CMC_CLCL_CLAIM_orig = f"""
SELECT CLM_ORIG.CLCL_ID,
       CLM_ORIG.CLCL_CUR_STS AS CLM_STS,
       CLM_ORIG.CLCL_PAID_DT,
       CLM_ORIG.CLCL_ID_ADJ_TO,
CASE 
    WHEN SUBSTRING(CLM_ORIG.CLCL_ID_ADJ_FROM ,1,1) = 'L' 
         AND SUBSTRING(CLM_ORIG.CLCL_ID_CRTE_FROM ,1,1) = 'L' 
    THEN CLM_ORIG.CLCL_ID_CRTE_FROM 
    WHEN (CLM_ORIG.CLCL_ID_ADJ_FROM = ''  
          OR LEN(RTRIM(LTRIM(CLM_ORIG.CLCL_ID_ADJ_FROM))) IS NULL) 
    THEN CLM_ORIG.CLCL_ID_CRTE_FROM
    ELSE CLM_ORIG.CLCL_ID_ADJ_FROM 
END AS CLCL_ID_ADJ_FROM,
       CLM_ORIG.CLCL_LAST_ACT_DTM
FROM  {FacetsOwner}.CMC_CLCL_CLAIM CLM WITH(nolock),
      {FacetsOwner}.CMC_CLCL_CLAIM CLM_ORIG WITH(nolock)
WHERE CLM.CLCL_LAST_ACT_DTM >= '{BeginDate}'
  AND CLM.CLCL_LAST_ACT_DTM <  '{EndDate}'
  AND CASE 
        WHEN SUBSTRING(CLM.CLCL_ID_ADJ_FROM ,1,1) = 'L' 
             AND SUBSTRING(CLM.CLCL_ID_CRTE_FROM ,1,1) = 'L' 
        THEN CLM.CLCL_ID_CRTE_FROM 
        WHEN (CLM.CLCL_ID_ADJ_FROM = ''  
              OR LEN(RTRIM(LTRIM(CLM.CLCL_ID_ADJ_FROM))) IS NULL) 
        THEN CLM.CLCL_ID_CRTE_FROM
        ELSE CLM.CLCL_ID_ADJ_FROM 
      END = CLM_ORIG.CLCL_ID
  AND NOT EXISTS (
      SELECT 'Y'
      FROM {FacetsOwner}.CMC_CLCL_CLAIM clm2 WITH(nolock),
           {FacetsOwner}.CMC_CLMI_MISC clmi WITH(nolock),
           {FacetsOwner}.CMC_GRGR_GROUP grgr WITH(nolock)
      WHERE CLM.CLCL_ID = clm2.CLCL_ID
        AND clm2.CLCL_ID = clmi.CLCL_ID
        AND clm2.GRGR_CK = grgr.GRGR_CK
        AND (clmi.CLMI_LOCAL_PLAN_CD = 'PAR' 
             OR grgr.GRGR_ID = 'NASCOPAR')
  )
  AND NOT EXISTS (
      SELECT 'Y'
      FROM {BCBSOwner}.P_SEL_PRCS_CRITR P_SEL_PRCS_CRITR WITH(nolock),
           {FacetsOwner}.CMC_GRGR_GROUP CMC_GRGR_GROUP WITH(nolock)
      WHERE P_SEL_PRCS_CRITR.SEL_PRCS_ID = 'WRHSINBEXCL'
        AND P_SEL_PRCS_CRITR.SEL_PRCS_ITEM_ID = 'WORKCOMP'
        AND P_SEL_PRCS_CRITR.SEL_PRCS_CRITR_CD = 'MEDPROC'
        AND CMC_GRGR_GROUP.GRGR_ID >= P_SEL_PRCS_CRITR.CRITR_VAL_FROM_TX
        AND CMC_GRGR_GROUP.GRGR_ID <= P_SEL_PRCS_PRITR.CRITR_VAL_THRU_TX
        AND CLM.GRGR_CK=CMC_GRGR_GROUP.GRGR_CK
  )
  AND CASE 
        WHEN SUBSTRING(CLM_ORIG.CLCL_ID,1,1) = 'L' 
             AND CLM_ORIG.CLCL_CUR_STS = '81' 
        THEN 1 
        ELSE 0 
      END = 0
"""
df_CMC_CLCL_CLAIM_orig = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_CMC_CLCL_CLAIM_orig)
    .load()
)

# Collector (Round-Robin) => treat as union of all inputs
df_collector_clcl = df_CMC_CLCL_CLAIM.selectExpr(
    "CLCL_ID as CLM_ID",
    "CLM_STS",
    "CLCL_PAID_DT",
    "CLCL_ID_ADJ_TO",
    "CLCL_ID_ADJ_FROM",
    "CLCL_LAST_ACT_DTM"
)
df_collector_clst = df_CMC_CLST_STATUS.selectExpr(
    "CLCL_ID as CLM_ID",
    "CLM_STS",
    "CLCL_PAID_DT",
    "CLCL_ID_ADJ_TO",
    "CLCL_ID_ADJ_FROM",
    "CLCL_LAST_ACT_DTM"
)
df_collector_ckck = df_CMC_CKCK_CHECK.selectExpr(
    "CLCL_ID as CLM_ID",
    "CLM_STS",
    "CLCL_PAID_DT",
    "CLCL_ID_ADJ_TO",
    "CLCL_ID_ADJ_FROM",
    "CLCL_LAST_ACT_DTM"
)
df_collector_clcl_adj_org = df_CMC_CLCL_CLAIM_orig.selectExpr(
    "CLCL_ID as CLM_ID",
    "CLM_STS",
    "CLCL_PAID_DT",
    "CLCL_ID_ADJ_TO",
    "CLCL_ID_ADJ_FROM",
    "CLCL_LAST_ACT_DTM"
)
df_Collector_union = (
    df_collector_clcl
    .unionByName(df_collector_clst)
    .unionByName(df_collector_ckck)
    .unionByName(df_collector_clcl_adj_org)
)
df_collector = df_Collector_union.select(
    "CLM_ID",
    "CLM_STS",
    "CLCL_PAID_DT",
    "CLCL_ID_ADJ_TO",
    "CLCL_ID_ADJ_FROM",
    "CLCL_LAST_ACT_DTM"
)

# hf_tmp_clm_dedup (Scenario A: remove duplicates on key columns -> "CLM_ID")
df_hf_tmp_clm_dedup = df_collector.dropDuplicates(["CLM_ID"])

# Tfrm_exclude_L_89
df_HitListL89 = df_hf_tmp_clm_dedup.filter(
    (
        (substring("CLM_ID", 1, 1) == 'L') & (df_hf_tmp_clm_dedup.CLM_STS == '89')
    )
    | (
        (substring("CLCL_ID_ADJ_FROM", 1, 1) == 'L')
        & (substring("CLM_ID", 1, 1) != 'L')
        & (df_hf_tmp_clm_dedup.CLM_STS == '02')
    )
).select("CLM_ID")

df_clmID = df_hf_tmp_clm_dedup.filter(
    (substring("CLM_ID", 1, 1) != 'L') & (df_hf_tmp_clm_dedup.CLM_STS != '89')
).select(
    "CLM_ID",
    "CLM_STS",
    "CLCL_PAID_DT",
    "CLCL_ID_ADJ_TO",
    "CLCL_ID_ADJ_FROM",
    "CLCL_LAST_ACT_DTM"
)

# L89Claims_Hitlist -> CSeqFileStage
# rpad for char columns
df_HitListL89 = df_HitListL89.withColumn("CLM_ID", rpad("CLM_ID", 12, " "))
write_files(
    df_HitListL89,
    f"{adls_path_raw}/landing/LhoFctsClmHitList.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

# CDriverTable (ODBCConnector) => replicate as merge to tempdb..#DriverTable#
df_clmID = df_clmID.withColumn("CLM_ID", rpad("CLM_ID", 12, " "))
df_clmID = df_clmID.withColumn("CLM_STS", rpad("CLM_STS", 2, " "))
df_clmID = df_clmID.withColumn("CLCL_ID_ADJ_TO", rpad("CLCL_ID_ADJ_TO", 12, " "))
df_clmID = df_clmID.withColumn("CLCL_ID_ADJ_FROM", rpad("CLCL_ID_ADJ_FROM", 12, " "))

jdbc_url_tempdb, jdbc_props_tempdb = get_db_config(facets_secret_name)
execute_dml(
    f"DROP TABLE IF EXISTS tempdb.FctsTempClmLoad_CDriverTable_temp",
    jdbc_url_tempdb,
    jdbc_props_tempdb
)
df_clmID.write \
    .format("jdbc") \
    .option("url", jdbc_url_tempdb) \
    .options(**jdbc_props_tempdb) \
    .option("dbtable", "tempdb.FctsTempClmLoad_CDriverTable_temp") \
    .mode("append") \
    .save()

merge_sql = f"""
MERGE tempdb..#DriverTable# AS T
USING tempdb.FctsTempClmLoad_CDriverTable_temp AS S
ON (T.CLM_ID = S.CLM_ID)
WHEN MATCHED THEN 
  UPDATE SET
    T.CLM_STS = S.CLM_STS,
    T.CLCL_PAID_DT = S.CLCL_PAID_DT,
    T.CLCL_ID_ADJ_TO = S.CLCL_ID_ADJ_TO,
    T.CLCL_ID_ADJ_FROM = S.CLCL_ID_ADJ_FROM,
    T.CLCL_LAST_ACT_DTM = S.CLCL_LAST_ACT_DTM
WHEN NOT MATCHED THEN
  INSERT (CLM_ID, CLM_STS, CLCL_PAID_DT, CLCL_ID_ADJ_TO, CLCL_ID_ADJ_FROM, CLCL_LAST_ACT_DTM)
  VALUES (S.CLM_ID, S.CLM_STS, S.CLCL_PAID_DT, S.CLCL_ID_ADJ_TO, S.CLCL_ID_ADJ_FROM, S.CLCL_LAST_ACT_DTM);
"""
execute_dml(merge_sql, jdbc_url_tempdb, jdbc_props_tempdb)