# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2013 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC CALLED BY:         FctsIdsWorkCompClmSeq
# MAGIC 
# MAGIC PROCESSING:     Workers Compensation claims extract from FacetsDbo and load to IDS WORK_COMPNSTN_CLM table
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                    Date                 \(9)Project                                                          Change Description                                                   Development Project\(9)          Code Reviewer          Date Reviewed       
# MAGIC ------------------                --------------------     \(9)------------------------                                        -----------------------------------------------------------------------                  ------------------------------\(9)         -------------------------------   ----------------------------       
# MAGIC Akhila Manickavelu\(9)  2016-12-15\(9)5628 WORK_COMPNSTN_CLM \(9)    Original Programming\(9)\(9)\(9)          Integratedev2                          Kalyan Neelam           2017-02-27 
# MAGIC                                                                 (Facets to IDS) ETL Report 
# MAGIC Prabhu ES                 2022-03-02            S2S Remediation                                        MSSQL ODBC conn added and other param changes  IntegrateDev5

# MAGIC This job Extracts the Claims data from Facets tables to generate the load file #Env#1.Facets_Ids_WorkCompClm.dat to be send to IDS WORK_COMPNSTN_CLM table
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
BCBSOwner = get_widget_value('BCBSOwner','')
bcbs_secret_name = get_widget_value('bcbs_secret_name','')
RunCycle = get_widget_value('RunCycle','')
Env = get_widget_value('Env','')
RunID = get_widget_value('RunID','')
DrivTable = get_widget_value('DrivTable','')
BeginDate = get_widget_value('BeginDate','')
EndDate = get_widget_value('EndDate','')

jdbc_url, jdbc_props = get_db_config(facets_secret_name)

sql_CMC_CLST_STATUS = f"""SELECT  CLM.CLCL_ID,
                 CLM.CLCL_CUR_STS,
                 CLM.CLCL_LAST_ACT_DTM,
                 CLM.CLCL_PAID_DT,
                 CLM.CLCL_ID_ADJ_TO,
                 CLM.CLCL_ID_ADJ_FROM
FROM   {FacetsOwner}.CMC_CLST_STATUS   STATUS,
       {FacetsOwner}.CMC_CLCL_CLAIM    CLM
WHERE   STATUS.CLST_STS_DTM >= '{BeginDate}'
  AND   STATUS.CLST_STS_DTM < '{EndDate}'
  AND   STATUS.CLCL_ID = CLM.CLCL_ID
  AND   NOT EXISTS (
          SELECT 'Y'
          FROM {FacetsOwner}.CMC_CLST_STATUS clm2,
               {FacetsOwner}.CMC_CLMI_MISC clmi,
               {FacetsOwner}.CMC_MEME_MEMBER meme,
               {FacetsOwner}.CMC_GRGR_GROUP grgr
          WHERE STATUS.CLCL_ID = clm2.CLCL_ID 
            AND clm2.CLCL_ID = clmi.CLCL_ID
            AND clm2.MEME_CK = meme.MEME_CK 
            AND meme.GRGR_CK = grgr.GRGR_CK
            AND (clmi.CLMI_LOCAL_PLAN_CD = 'PAR' 
                 OR  grgr.GRGR_ID = 'NASCOPAR')
        )
  AND  EXISTS (
          SELECT 'Y'
          FROM {BCBSOwner}.P_SEL_PRCS_CRITR P_SEL_PRCS_CRITR,
               {FacetsOwner}.CMC_GRGR_GROUP CMC_GRGR_GROUP
          WHERE P_SEL_PRCS_CRITR.SEL_PRCS_ID = 'WRHSINBEXCL'
            AND P_SEL_PRCS_CRITR.SEL_PRCS_ITEM_ID = 'WORKCOMP'
            AND P_SEL_PRCS_CRITR.SEL_PRCS_CRITR_CD = 'MEDPROC'
            AND CMC_GRGR_GROUP.GRGR_ID >= P_SEL_PRCS_CRITR.CRITR_VAL_FROM_TX
            AND CMC_GRGR_GROUP.GRGR_ID <= P_SEL_PRCS_CRITR.CRITR_VAL_THRU_TX
            AND CLM.GRGR_CK=CMC_GRGR_GROUP.GRGR_CK
        )
"""

df_CMC_CLST_STATUS = (
    spark.read.format("jdbc")
         .option("url", jdbc_url)
         .options(**jdbc_props)
         .option("query", sql_CMC_CLST_STATUS)
         .load()
)

sql_CMC_CLCL_CLAIM_Orig = f"""SELECT CLM_ORIG.CLCL_ID,
                 CLM_ORIG.CLCL_CUR_STS,
                 CLM_ORIG.CLCL_LAST_ACT_DTM,
                 CLM_ORIG.CLCL_PAID_DT,
                 CLM_ORIG.CLCL_ID_ADJ_TO,
                 CLM_ORIG.CLCL_ID_ADJ_FROM
FROM  {FacetsOwner}.CMC_CLCL_CLAIM    CLM ,
      {FacetsOwner}.CMC_CLCL_CLAIM    CLM_ORIG
WHERE   CLM.CLCL_LAST_ACT_DTM >= '{BeginDate}'
  AND   CLM.CLCL_LAST_ACT_DTM <  '{EndDate}'
  AND   CLM.CLCL_ID_ADJ_FROM     =    CLM_ORIG.CLCL_ID
  AND   NOT EXISTS (
         SELECT 'Y'
         FROM {FacetsOwner}.CMC_CLCL_CLAIM clm2,
              {FacetsOwner}.CMC_CLMI_MISC clmi,
              {FacetsOwner}.CMC_GRGR_GROUP grgr
         WHERE CLM.CLCL_ID = clm2.CLCL_ID 
           AND clm2.CLCL_ID = clmi.CLCL_ID 
           AND clm2.GRGR_CK = grgr.GRGR_CK 
           AND (clmi.CLMI_LOCAL_PLAN_CD = 'PAR' 
                OR grgr.GRGR_ID = 'NASCOPAR')
       )
  AND   EXISTS (
         SELECT 'Y'
         FROM {BCBSOwner}.P_SEL_PRCS_CRITR P_SEL_PRCS_CRITR,
              {FacetsOwner}.CMC_GRGR_GROUP CMC_GRGR_GROUP
         WHERE P_SEL_PRCS_CRITR.SEL_PRCS_ID = 'WRHSINBEXCL'
           AND P_SEL_PRCS_CRITR.SEL_PRCS_ITEM_ID = 'WORKCOMP'
           AND P_SEL_PRCS_CRITR.SEL_PRCS_CRITR_CD = 'MEDPROC'
           AND CMC_GRGR_GROUP.GRGR_ID >= P_SEL_PRCS_CRITR.CRITR_VAL_FROM_TX
           AND CMC_GRGR_GROUP.GRGR_ID <= P_SEL_PRCS_CRITR.CRITR_VAL_THRU_TX
           AND CLM.GRGR_CK=CMC_GRGR_GROUP.GRGR_CK
       )
"""

df_CMC_CLCL_CLAIM_Orig = (
    spark.read.format("jdbc")
         .option("url", jdbc_url)
         .options(**jdbc_props)
         .option("query", sql_CMC_CLCL_CLAIM_Orig)
         .load()
)

sql_CMC_CLCL_CLAIM = f"""SELECT   CLM.CLCL_ID,
                 CLM.CLCL_CUR_STS,
                 CLM.CLCL_LAST_ACT_DTM,
                 CLM.CLCL_PAID_DT,
                 CLM.CLCL_ID_ADJ_TO,
                 CLM.CLCL_ID_ADJ_FROM
FROM  {FacetsOwner}.CMC_CLCL_CLAIM    CLM
WHERE   CLM.CLCL_LAST_ACT_DTM >= '{BeginDate}'
  AND   CLM.CLCL_LAST_ACT_DTM <  '{EndDate}'
  AND   NOT EXISTS (
         SELECT 'Y'
         FROM {FacetsOwner}.CMC_CLCL_CLAIM clm2,
              {FacetsOwner}.CMC_CLMI_MISC clmi,
              {FacetsOwner}.CMC_GRGR_GROUP grgr
         WHERE CLM.CLCL_ID = clm2.CLCL_ID 
           AND clm2.CLCL_ID = clmi.CLCL_ID 
           AND clm2.GRGR_CK = grgr.GRGR_CK 
           AND (clmi.CLMI_LOCAL_PLAN_CD = 'PAR' 
                OR grgr.GRGR_ID = 'NASCOPAR')
       )
  AND   EXISTS (
         SELECT 'Y'
         FROM {BCBSOwner}.P_SEL_PRCS_CRITR P_SEL_PRCS_CRITR,
              {FacetsOwner}.CMC_GRGR_GROUP CMC_GRGR_GROUP
         WHERE P_SEL_PRCS_CRITR.SEL_PRCS_ID = 'WRHSINBEXCL'
           AND P_SEL_PRCS_CRITR.SEL_PRCS_ITEM_ID = 'WORKCOMP'
           AND P_SEL_PRCS_CRITR.SEL_PRCS_CRITR_CD = 'MEDPROC'
           AND CMC_GRGR_GROUP.GRGR_ID >= P_SEL_PRCS_CRITR.CRITR_VAL_FROM_TX
           AND CMC_GRGR_GROUP.GRGR_ID <= P_SEL_PRCS_CRITR.CRITR_VAL_THRU_TX
           AND CLM.GRGR_CK=CMC_GRGR_GROUP.GRGR_CK
       )
"""

df_CMC_CLCL_CLAIM = (
    spark.read.format("jdbc")
         .option("url", jdbc_url)
         .options(**jdbc_props)
         .option("query", sql_CMC_CLCL_CLAIM)
         .load()
)

sql_CMC_CKCK_CHECK = f"""SELECT 
              CLM.CLCL_ID,
              CLM.CLCL_CUR_STS,
              CLM.CLCL_LAST_ACT_DTM,
              CLM.CLCL_PAID_DT,
              CLM.CLCL_ID_ADJ_TO,
              CLM.CLCL_ID_ADJ_FROM
FROM {FacetsOwner}.CMC_CKCK_CHECK      CHK,
     {FacetsOwner}.CMC_CLCK_CLM_CHECK  CLMCHECK,
     {FacetsOwner}.CMC_CLCL_CLAIM      CLM
WHERE CHK.CKCK_REISS_DT <> '1753-01-01'
  AND CHK.CKCK_PRINTED_DT  >= '{BeginDate}'
  AND CLM.CLCL_ID = CLMCHECK.CLCL_ID
  AND CLMCHECK.CKPY_REF_ID = CHK.CKPY_REF_ID
  AND NOT EXISTS (
       SELECT 'Y'
       FROM {FacetsOwner}.CMC_CLST_STATUS clm2,
            {FacetsOwner}.CMC_CLMI_MISC clmi,
            {FacetsOwner}.CMC_MEME_MEMBER meme,
            {FacetsOwner}.CMC_GRGR_GROUP grgr
       WHERE CLM.CLCL_ID = clm2.CLCL_ID 
         AND clm2.CLCL_ID = clmi.CLCL_ID
         AND clm2.MEME_CK = meme.MEME_CK 
         AND meme.GRGR_CK = grgr.GRGR_CK
         AND ( clmi.CLMI_LOCAL_PLAN_CD = 'PAR' 
               OR  grgr.GRGR_ID = 'NASCOPAR' ) 
     )
  AND EXISTS (
       SELECT 'Y'
       FROM {BCBSOwner}.P_SEL_PRCS_CRITR P_SEL_PRCS_CRITR,
            {FacetsOwner}.CMC_GRGR_GROUP CMC_GRGR_GROUP
       WHERE P_SEL_PRCS_CRITR.SEL_PRCS_ID = 'WRHSINBEXCL'
         AND P_SEL_PRCS_CRITR.SEL_PRCS_ITEM_ID = 'WORKCOMP'
         AND P_SEL_PRCS_CRITR.SEL_PRCS_CRITR_CD = 'MEDPROC'
         AND CMC_GRGR_GROUP.GRGR_ID >= P_SEL_PRCS_CRITR.CRITR_VAL_FROM_TX
         AND CMC_GRGR_GROUP.GRGR_ID <= P_SEL_PRCS_CRITR.CRITR_VAL_THRU_TX
         AND CLM.GRGR_CK=CMC_GRGR_GROUP.GRGR_CK
     )
"""

df_CMC_CKCK_CHECK = (
    spark.read.format("jdbc")
         .option("url", jdbc_url)
         .options(**jdbc_props)
         .option("query", sql_CMC_CKCK_CHECK)
         .load()
)

df_Funnel_61 = df_CMC_CLST_STATUS.unionByName(df_CMC_CLCL_CLAIM_Orig) \
                                 .unionByName(df_CMC_CKCK_CHECK) \
                                 .unionByName(df_CMC_CLCL_CLAIM)

df_RmvDups_ClmId = dedup_sort(df_Funnel_61, ["CLCL_ID"], [])

df_RmvDups_ClmId = df_RmvDups_ClmId.select(
    F.col("CLCL_ID").alias("CLM_ID"),
    F.col("CLCL_CUR_STS").alias("CLM_STS"),
    F.col("CLCL_LAST_ACT_DTM"),
    F.col("CLCL_PAID_DT"),
    F.col("CLCL_ID_ADJ_TO"),
    F.col("CLCL_ID_ADJ_FROM")
)

df_RmvDups_ClmId = df_RmvDups_ClmId.select(
    F.rpad(F.col("CLM_ID"), 12, " ").alias("CLM_ID"),
    F.rpad(F.col("CLM_STS"), 2, " ").alias("CLM_STS"),
    F.col("CLCL_LAST_ACT_DTM"),
    F.col("CLCL_PAID_DT"),
    F.rpad(F.col("CLCL_ID_ADJ_TO"), 12, " ").alias("CLCL_ID_ADJ_TO"),
    F.rpad(F.col("CLCL_ID_ADJ_FROM"), 12, " ").alias("CLCL_ID_ADJ_FROM")
)

drop_sql = f"DROP TABLE IF EXISTS tempdb..{DrivTable}"
execute_dml(drop_sql, jdbc_url, jdbc_props)

create_sql = f"""
CREATE TABLE tempdb..{DrivTable} (
    CLM_ID CHAR(12),
    CLM_STS CHAR(2),
    CLCL_LAST_ACT_DTM DATETIME,
    CLCL_PAID_DT DATETIME,
    CLCL_ID_ADJ_TO CHAR(12),
    CLCL_ID_ADJ_FROM CHAR(12)
)
"""
execute_dml(create_sql, jdbc_url, jdbc_props)

(
    df_RmvDups_ClmId
    .write
    .format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("dbtable", f"tempdb..{DrivTable}")
    .mode("append")
    .save()
)