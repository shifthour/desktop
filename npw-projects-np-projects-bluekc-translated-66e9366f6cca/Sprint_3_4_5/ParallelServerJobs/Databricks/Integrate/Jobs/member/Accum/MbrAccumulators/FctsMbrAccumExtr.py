# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2006 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     FctsMbrAccumExtr
# MAGIC CALLED BY:  
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:   Pulls data from Facets CMC_MEAC_ACCUM for loading to the IDS MBR_ACCUM table
# MAGIC       
# MAGIC 
# MAGIC INPUTS:       FACETS MEMBERSHIP Profile system
# MAGIC                       tables:  CMC_MEAC_ACCUM, CMC_GRGR_GROUP, TMP_IDS_MBR_CRVR
# MAGIC 
# MAGIC 
# MAGIC HASH FILES:   hf_mbr_accum_all, hf_mbr_accum_y1, hf_mbr_accum_y2, hf_mbr_accum_y3, hf_mbr_accum
# MAGIC 
# MAGIC TRANSFORMS:   STRIP.FIELD
# MAGIC                              FORMAT.DATE
# MAGIC                            
# MAGIC PROCESSING:  Extract, transform, and primary keying for Membership subject area.
# MAGIC   
# MAGIC 
# MAGIC OUTPUTS:   Sequential file
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC              Parikshith Chada   09/12/2006  -  Originally Programmed
# MAGIC              Parikshith Chada   10/02/2006  -  Code Changes to Extraction process
# MAGIC              Hugh Sisson          11/15/2006  -  CDHP modification to use TMP_IDS_MBR_ACCUM_DRVR driver table
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                       Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                  ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parik                                11/29/2007          3044                             Made code changes in the SQL                        devlIDS30                Steph Goddard            12/04/2007
# MAGIC 
# MAGIC 
# MAGIC Parik                               2008-08-27       3567(Primary Key)            Added primary key process to the job                 devlIDS                     Steph Goddard             09/02/2008
# MAGIC Ralph Tucker                 2008-12-16        3648(Labor Accts)          Added two fields (Carovr_amt, Cob_oop_amt)    devlIDSnew               Steph Goddard             12/23/2008
# MAGIC Ralph Tucker                 2009-11-11       3556 CDC                      Added Change Data Capture Code                    devlIDSnew                 Steph Goddard             11/23/2009
# MAGIC 
# MAGIC Bhoomi Dasari                2/22/2013        TTR-1454                        Changes "Prefetch rows" to 5000                   IntegrateNewDevl         Kalyan Neelam              2013-03-05
# MAGIC                                                                                                         from 50 in CMC_MEAC_ACCUM stage  
# MAGIC 
# MAGIC Akhila M                        09/28/2016   5628-                       Exclusion Criteria- P_SEL_PRCS_CRITR                                                        Kalyan Neelam              2016-10-14
# MAGIC                                                             WorkersComp            to remove wokers comp GRGR_ID's         
# MAGIC 
# MAGIC Karthik Chintalapani        2016-11-11               5634      Added logic for new columns PLN_YR_EFF_DT and  PLN_YR_END_DT   IntegrateDev2  Kalyan Neelam   2016-11-28
# MAGIC 
# MAGIC 
# MAGIC Karthik Chintalapani        2016-12-01               5634      Remove Workers comp groupids                                         IntegrateDev2         Kalyan Neelam                 2016-12-15
# MAGIC Jaideep Mankala    	       2016-12-21                5634     Modified SQL in CMC_MEAC_ACCUM stage and added      IntegrateDev2         Kalyan Neelam                 2016-12-23
# MAGIC 					      constraint in Xfm stage to remove null date records
# MAGIC Karthik Chintalapani        2017-01-18              5634      Added the year as part of the lookup key to get the correct results. IntegrateDev2 Kalyan Neelam            2017-01-18
# MAGIC Jaideep Mankala            2017-08-21              5630      Modified job to read data from CMC_MATX_ACCUM_TXN   IntegrateDev1
# MAGIC 						table instead of CMC_FAAC table
# MAGIC Jaideep Mankala            2017-09-26              5630      Modified job to calculate PLN_YR_END_DT from	   IntegrateDev1            Kalyan Neelam            2017-09-26
# MAGIC 						MATX_BEN_BEG_DT date
# MAGIC                                          		
# MAGIC Prabhu ES                      2022-03-03            S2S Remediation                  MSSQL ODBC conn params added     IntegrateDev5		Ken Bradmon	2022-06-09

# MAGIC Extract Current and Previous Years
# MAGIC Apply business logic
# MAGIC Writing Sequential File to ../key
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType, NumericType
from pyspark.sql.functions import (
    col, lit, when, substring, isnull, concat, to_date, datediff, lag, rpad
)
from pyspark.sql import Window
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# ----------------------------------------------------------------
# Parameter retrieval
# ----------------------------------------------------------------
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
BCBSOwner = get_widget_value('BCBSOwner','')
bcbs_secret_name = get_widget_value('bcbs_secret_name','')
CurrRunCycle = get_widget_value('CurrRunCycle','50')
RunID = get_widget_value('RunID','2006092112345')
TmpTblRunID = get_widget_value('TmpTblRunID','20170823')
CurrDate = get_widget_value('CurrDate','2017-08-22')
SrcSysCd = get_widget_value('SrcSysCd','FACETS')
SrcSysCdSk = get_widget_value('SrcSysCdSk','103764')
LastRunDateTime = get_widget_value('LastRunDateTime','')

# ----------------------------------------------------------------
# Stage: CMC_MATX_ACCUM_TXN (ODBCConnector)
# ----------------------------------------------------------------
extract_query_CMC_MATX_ACCUM_TXN = f"""
SELECT  DISTINCT
      X.MEME_CK,
      X.PDPD_ACC_SFX, 
      X.MATX_ACC_TYPE,
      X.ACAC_ACC_NO,
      X.MATX_BEN_BEG_DT,
      X.GRGR_CK,
      X.MATX_AMT1,
      X.MATX_AMT2,
      X.MATX_AMT3,
      X.GRGR_ID,
      X.MATX_SEQ_NO

FROM

(
  SELECT  DISTINCT
      cfa.MEME_CK,
      cfa.PDPD_ACC_SFX, 
      cfa.MATX_ACC_TYPE,
      cfa.ACAC_ACC_NO,
      cfa.MATX_BEN_BEG_DT,
      cfa.GRGR_CK,
      cfa.MATX_AMT1,
      cfa.MATX_AMT2,
      cfa.MATX_AMT3,
      GRP.GRGR_ID,
      cfa.MATX_SEQ_NO
  FROM {FacetsOwner}.CMC_MATX_ACCUM_TXN cfa
  INNER JOIN {FacetsOwner}.CMC_GRGR_GROUP GRP
    ON cfa.GRGR_CK = GRP.GRGR_CK
   AND cfa.PDPD_ACC_SFX IN ('MED','DEN','HSA')
   AND cfa.MATX_ACC_TYPE IN ('D','C','L')
   AND cfa.SYS_LAST_UPD_DTM >= '{LastRunDateTime}'

  AND NOT EXISTS (
    SELECT DISTINCT 'Y'
    FROM {bcbs_secret_name}.{BCBSOwner}.P_SEL_PRCS_CRITR P_SEL_PRCS_CRITR,
         {FacetsOwner}.CMC_GRGR_GROUP CMC_GRGR
    WHERE P_SEL_PRCS_CRITR.SEL_PRCS_ID = 'WRHSINBEXCL'
      AND P_SEL_PRCS_CRITR.SEL_PRCS_ITEM_ID = 'WORKCOMP'
      AND P_SEL_PRCS_CRITR.SEL_PRCS_CRITR_CD = 'MEDPROC'
      AND CMC_GRGR.GRGR_ID >= P_SEL_PRCS_CRITR.CRITR_VAL_FROM_TX
      AND CMC_GRGR.GRGR_ID <= P_SEL_PRCS_CRITR.CRITR_VAL_THRU_TX
      AND GRP.GRGR_CK=CMC_GRGR.GRGR_CK
  )

  UNION

  SELECT DISTINCT
      cfa.MEME_CK,
      cfa.PDPD_ACC_SFX, 
      cfa.MATX_ACC_TYPE,
      cfa.ACAC_ACC_NO,
      cfa.MATX_BEN_BEG_DT,
      cfa.GRGR_CK,
      cfa.MATX_AMT1,
      cfa.MATX_AMT2,
      cfa.MATX_AMT3,
      GRP.GRGR_ID,
      cfa.MATX_SEQ_NO
  FROM tempdb..TMP_PROD_CDC_MA_HITDRV DRVR
  INNER JOIN {FacetsOwner}.CMC_MATX_ACCUM_TXN cfa
    ON cfa.MEME_CK = DRVR.MEME_CK
   AND cfa.PDPD_ACC_SFX IN ('MED','DEN','HSA')
   AND cfa.MATX_ACC_TYPE IN ('D','C','L')
  INNER JOIN {FacetsOwner}.CMC_GRGR_GROUP GRP
    ON cfa.GRGR_CK = GRP.GRGR_CK
  AND NOT EXISTS (
    SELECT DISTINCT 'Y'
    FROM {bcbs_secret_name}.{BCBSOwner}.P_SEL_PRCS_CRITR P_SEL_PRCS_CRITR,
         {FacetsOwner}.CMC_GRGR_GROUP CMC_GRGR
    WHERE P_SEL_PRCS_CRITR.SEL_PRCS_ID = 'WRHSINBEXCL'
      AND P_SEL_PRCS_CRITR.SEL_PRCS_ITEM_ID = 'WORKCOMP'
      AND P_SEL_PRCS_CRITR.SEL_PRCS_CRITR_CD = 'MEDPROC'
      AND CMC_GRGR.GRGR_ID >= P_SEL_PRCS_CRITR.CRITR_VAL_FROM_TX
      AND CMC_GRGR.GRGR_ID <= P_SEL_PRCS_CRITR.CRITR_VAL_THRU_TX
      AND GRP.GRGR_CK=CMC_GRGR.GRGR_CK
  )

  UNION

  SELECT DISTINCT
      cfa.MEME_CK,
      cfa.PDPD_ACC_SFX, 
      cfa.MATX_ACC_TYPE,
      cfa.ACAC_ACC_NO,
      cfa.MATX_BEN_BEG_DT,
      cfa.GRGR_CK,
      cfa.MATX_AMT1,
      cfa.MATX_AMT2,
      cfa.MATX_AMT3,
      GRP.GRGR_ID,
      cfa.MATX_SEQ_NO
  FROM tempdb..TMP_PROD_CDC_MA_DRVR DRVR
  INNER JOIN {FacetsOwner}.CMC_MATX_ACCUM_TXN cfa
    ON cfa.MEME_CK = DRVR.MEME_CK
   AND cfa.PDPD_ACC_SFX IN ('MED','DEN','HSA')
   AND cfa.MATX_ACC_TYPE IN ('D','C','L')
  INNER JOIN {FacetsOwner}.CMC_GRGR_GROUP GRP
    ON cfa.GRGR_CK = GRP.GRGR_CK
  AND NOT EXISTS (
    SELECT DISTINCT 'Y'
    FROM {bcbs_secret_name}.{BCBSOwner}.P_SEL_PRCS_CRITR P_SEL_PRCS_CRITR,
         {FacetsOwner}.CMC_GRGR_GROUP CMC_GRGR
    WHERE P_SEL_PRCS_CRITR.SEL_PRCS_ID = 'WRHSINBEXCL'
      AND P_SEL_PRCS_CRITR.SEL_PRCS_ITEM_ID = 'WORKCOMP'
      AND P_SEL_PRCS_CRITR.SEL_PRCS_CRITR_CD = 'MEDPROC'
      AND CMC_GRGR.GRGR_ID >= P_SEL_PRCS_CRITR.CRITR_VAL_FROM_TX
      AND CMC_GRGR.GRGR_ID <= P_SEL_PRCS_CRITR.CRITR_VAL_THRU_TX
      AND GRP.GRGR_CK=CMC_GRGR.GRGR_CK
  )
) X

ORDER BY 
      X.MEME_CK,
      X.PDPD_ACC_SFX, 
      X.MATX_ACC_TYPE,
      X.ACAC_ACC_NO,
      X.MATX_BEN_BEG_DT,
      X.MATX_SEQ_NO
"""

jdbc_url_CMC_MATX_ACCUM_TXN, jdbc_props_CMC_MATX_ACCUM_TXN = get_db_config(facets_secret_name)
df_CMC_MATX_ACCUM_TXN = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_CMC_MATX_ACCUM_TXN)
    .options(**jdbc_props_CMC_MATX_ACCUM_TXN)
    .option("query", extract_query_CMC_MATX_ACCUM_TXN)
    .load()
)

# ----------------------------------------------------------------
# Stage: MAX_CMC_MATX_ACCUM_TXN (ODBCConnector)
# ----------------------------------------------------------------
extract_query_MAX_CMC_MATX_ACCUM_TXN = f"""
SELECT DISTINCT
      MEME_CK,
      PDPD_ACC_SFX, 
      MATX_ACC_TYPE,
      ACAC_ACC_NO,
      MATX_BEN_BEG_DT,
      MAX(MATX_SEQ_NO) AS MATX_SEQ_NO
FROM {FacetsOwner}.CMC_MATX_ACCUM_TXN
WHERE PDPD_ACC_SFX IN ('MED','DEN','HSA')
  AND MATX_ACC_TYPE IN ('D','C','L')
GROUP BY 
      MEME_CK,
      PDPD_ACC_SFX, 
      MATX_ACC_TYPE,
      ACAC_ACC_NO,
      MATX_BEN_BEG_DT
"""

jdbc_url_MAX, jdbc_props_MAX = get_db_config(facets_secret_name)
df_MAX_CMC_MATX_ACCUM_TXN = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_MAX)
    .options(**jdbc_props_MAX)
    .option("query", extract_query_MAX_CMC_MATX_ACCUM_TXN)
    .load()
)

# ----------------------------------------------------------------
# Stage: hf_facets_member_accum_max_seq_no (CHashedFileStage - Scenario C)
# We write the data to Parquet, then read it back as a lookup.
# ----------------------------------------------------------------
df_hf_write = df_MAX_CMC_MATX_ACCUM_TXN.select(
    rpad(col("MEME_CK").cast("string"), 10, " ").alias("MEME_CK"),  # or simply keep as int if we desire numeric
    rpad(col("PDPD_ACC_SFX").cast("string"), 4, " ").alias("PDPD_ACC_SFX"),
    rpad(col("MATX_ACC_TYPE").cast("string"), 1, " ").alias("MATX_ACC_TYPE"),
    col("ACAC_ACC_NO"),
    col("MATX_BEN_BEG_DT"),
    col("MATX_SEQ_NO")
)

write_files(
    df_hf_write,
    "hf_facets_member_accum_max_seq_no.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote='"',
    nullValue=None
)

df_hf_facets_member_accum_max_seq_no = spark.read.parquet("hf_facets_member_accum_max_seq_no.parquet")

# ----------------------------------------------------------------
# Stage: StripField (CTransformerStage)
#   Main link: ACCUM (df_CMC_MATX_ACCUM_TXN)
#   Lookup link: ACCUM_Max_lkup (df_hf_facets_member_accum_max_seq_no) - left join
#   Constraint: Pass rows where ACCUM_Max_lkup.MEME_CK is not null => effectively an inner join
#   Stage Variables: CurrentDate, CurrentYear
# ----------------------------------------------------------------
df_joined_stripfield = (
    df_CMC_MATX_ACCUM_TXN.alias("ACCUM")
    .join(
        df_hf_facets_member_accum_max_seq_no.alias("ACCUM_Max_lkup"),
        on=[
            col("ACCUM.MEME_CK") == col("ACCUM_Max_lkup.MEME_CK"),
            col("ACCUM.PDPD_ACC_SFX") == col("ACCUM_Max_lkup.PDPD_ACC_SFX"),
            col("ACCUM.MATX_ACC_TYPE") == col("ACCUM_Max_lkup.MATX_ACC_TYPE"),
            col("ACCUM.ACAC_ACC_NO") == col("ACCUM_Max_lkup.ACAC_ACC_NO"),
            col("ACCUM.MATX_BEN_BEG_DT") == col("ACCUM_Max_lkup.MATX_BEN_BEG_DT"),
            col("ACCUM.MATX_SEQ_NO") == col("ACCUM_Max_lkup.MATX_SEQ_NO")
        ],
        how="left"
    )
    .filter(col("ACCUM_Max_lkup.MEME_CK").isNotNull())
)

# Emulate stage variables:
# CurrentDate = FORMAT.DATE(ACCUM.MATX_BEN_BEG_DT,"SYBASE","TIMESTAMP","CCYY-MM-DD")
# CurrentYear = CurrentDate[1,4]
df_stripfield_vars = (
    df_joined_stripfield
    .withColumn("CurrentDate", FORMAT_DATE(col("ACCUM.MATX_BEN_BEG_DT"), "SYBASE", "TIMESTAMP", "CCYY-MM-DD"))
    .withColumn("CurrentYear", substring(col("CurrentDate"), 1, 4))
)

# Output link "Dedup" columns:
df_StripField = df_stripfield_vars.select(
    col("ACCUM.MEME_CK").alias("MEME_CK"),
    strip_field(col("ACCUM.PDPD_ACC_SFX")).alias("PDPD_ACC_SFX"),
    strip_field(col("ACCUM.MATX_ACC_TYPE")).alias("MEAC_ACC_TYPE"),
    col("ACCUM.ACAC_ACC_NO").alias("ACAC_ACC_NO"),
    col("CurrentYear").alias("CURRENT_YR"),
    col("ACCUM.GRGR_CK").alias("GRGR_CK"),
    col("ACCUM.MATX_AMT1").alias("MATX_AMT1"),
    col("ACCUM.MATX_AMT2").alias("MATX_AMT2"),
    col("ACCUM.MATX_AMT3").alias("MATX_AMT3"),
    strip_field(col("ACCUM.GRGR_ID")).alias("GRGR_ID"),
    col("CurrentDate").alias("CurrDate")
)

# ----------------------------------------------------------------
# Stage: dedup (CHashedFileStage - Scenario A)
#   Pattern: AnyStage -> CHashedFileStage -> AnyStage
#   We replace with dedup logic on key columns => [MEME_CK, PDPD_ACC_SFX, MEAC_ACC_TYPE, ACAC_ACC_NO, CURRENT_YR]
# ----------------------------------------------------------------
df_dedup_in = df_StripField

# Ensure column order before dedup:
df_dedup_cols_ordered = df_dedup_in.select(
    "MEME_CK",
    "PDPD_ACC_SFX",
    "MEAC_ACC_TYPE",
    "ACAC_ACC_NO",
    "CURRENT_YR",
    "GRGR_CK",
    "MATX_AMT1",
    "MATX_AMT2",
    "MATX_AMT3",
    "GRGR_ID",
    "CurrDate"
)

df_dedup = dedup_sort(
    df_dedup_cols_ordered,
    ["MEME_CK","PDPD_ACC_SFX","MEAC_ACC_TYPE","ACAC_ACC_NO","CURRENT_YR"],
    []
)

# This resulting df_dedup replaces the hashed file; next stage is "BusinessLogic"

# ----------------------------------------------------------------
# Stage: BusinessLogic (CTransformerStage)
#   Input: df_dedup
#   Output pins: "AllCol" (C208P2), "Transform" (C208P1)
#   Contains row-by-row logic with stage variables: we replicate it via windowing
# ----------------------------------------------------------------
# Stage variables:
#   svKey = trim(MEME_CK) : trim(PDPD_ACC_SFX) : trim(MEAC_ACC_TYPE) : trim(ACAC_ACC_NO)
#   svCurrentDate = CurrDate
#   svDateDiffernce = if same key then difference else 0
#   svCheckPlnYrEndDt = ...
#   RowPassThru = "Y"

df_buslogic_in = df_dedup.select(
    lit(SrcSysCdSk).alias("SrcSysCdSk"),
    col("MEME_CK"),
    col("PDPD_ACC_SFX"),
    col("MEAC_ACC_TYPE"),
    col("ACAC_ACC_NO"),
    col("CURRENT_YR"),
    col("GRGR_CK"),
    col("MATX_AMT1"),
    col("MATX_AMT2"),
    col("MATX_AMT3"),
    col("GRGR_ID"),
    col("CurrDate")
)

df_buslogic_svkey = df_buslogic_in.withColumn(
    "svKey",
    concat(
        trim(col("MEME_CK")),
        trim(col("PDPD_ACC_SFX")),
        trim(col("MEAC_ACC_TYPE")),
        trim(col("ACAC_ACC_NO"))
    )
).withColumn(
    "svCurrentDate",
    col("CurrDate")
)

# Convert CurrDate to date
df_buslogic_keyed = df_buslogic_svkey.withColumn(
    "svCurrentDate_dt",
    to_date(col("svCurrentDate"), "yyyy-MM-dd")
)

windowSpec = Window.partitionBy("svKey").orderBy("svCurrentDate_dt")

df_buslogic_lag = df_buslogic_keyed.withColumn(
    "svPrevKey",
    lag(col("svKey")).over(windowSpec)
).withColumn(
    "svPrevCurrentDate",
    lag(col("svCurrentDate")).over(windowSpec)
).withColumn(
    "svPrevCurrentDate_dt",
    lag(col("svCurrentDate_dt")).over(windowSpec)
)

df_buslogic_diff = df_buslogic_lag.withColumn(
    "svDateDiffernce",
    when(
        (col("svKey") == col("svPrevKey")) & col("svPrevCurrentDate_dt").isNotNull(),
        datediff(col("svCurrentDate_dt"), col("svPrevCurrentDate_dt"))
    ).otherwise(lit(0))
)

# svCheckPlnYrEndDt logic:
# if same key and svDateDiffernce > 365 then FIND.DATE(FIND.DATE(svCurrentDate, '1','Y','X','CCYY-MM-DD'), '-1','D','X','CCYY-MM-DD')
# else if same key and svDateDiffernce <= 365 then FIND.DATE(svPrevCurrentDate, '-1','D','X','CCYY-MM-DD')
# else ''
# We treat FIND.DATE as a user-defined function.
df_buslogic_plnyr = df_buslogic_diff.withColumn(
    "svCheckPlnYrEndDt",
    when(
        (col("svKey") == col("svPrevKey")) & (col("svDateDiffernce") > 365),
        FIND_DATE(
            FIND_DATE(col("svCurrentDate"), lit("1"), lit("Y"), lit("X"), lit("CCYY-MM-DD")),
            lit("-1"),
            lit("D"),
            lit("X"),
            lit("CCYY-MM-DD")
        )
    ).when(
        (col("svKey") == col("svPrevKey")) & (col("svDateDiffernce") <= 365),
        FIND_DATE(
            col("svPrevCurrentDate"),
            lit("-1"),
            lit("D"),
            lit("X"),
            lit("CCYY-MM-DD")
        )
    ).otherwise(lit(""))
).withColumn(
    "RowPassThru",
    lit("Y")
)

df_BusinessLogic = df_buslogic_plnyr

# Now generate the two output links:

# Link "AllCol"
df_AllCol = df_BusinessLogic.select(
    col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
    col("MEME_CK").alias("MBR_UNIQ_KEY"),
    col("PDPD_ACC_SFX").alias("PROD_ACCUM_ID"),
    col("MEAC_ACC_TYPE").alias("MBR_ACCUM_TYP_CD"),
    col("ACAC_ACC_NO").alias("ACCUM_NO"),
    col("CURRENT_YR").alias("YR_NO"),
    lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(lit("I"), 10, " ").alias("INSRT_UPDT_CD"),
    rpad(lit("N"), 1, " ").alias("DISCARD_IN"),
    rpad(col("RowPassThru"), 1, " ").alias("PASS_THRU_IN"),
    col("CurrDate").alias("FIRST_RECYC_DT"),
    lit(0).alias("ERR_CT"),
    lit(0).alias("RECYCLE_CT"),
    col("SrcSysCdSk").cast("string").alias("SRC_SYS_CD"),  # or you might just lit(SrcSysCd)
    concat(
        lit(SrcSysCd), lit(";"), col("MEME_CK"), lit(";"),
        col("PDPD_ACC_SFX"), lit(";"), col("MEAC_ACC_TYPE"), lit(";"),
        col("ACAC_ACC_NO"), lit(";"), col("CURRENT_YR")
    ).alias("PRI_KEY_STRING"),
    lit(0).alias("MBR_ACCUM_SK"),
    lit(0).alias("CRT_RUN_CYC_EXCTN"),
    lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN"),
    rpad(col("GRGR_ID"), 10, " ").alias("GRGR_ID"),
    col("MEME_CK").alias("MBR_SK"),
    col("MATX_AMT1").alias("ACCUM_AMT"),
    col("MATX_AMT2").alias("CAROVR_AMT"),
    col("MATX_AMT3").alias("COB_OOP_AMT"),
    col("CurrDate").alias("PLN_YR_EFF_DT"),
    col("svCheckPlnYrEndDt").alias("PLN_YR_END_DT")
)

# Link "Transform"
df_Transform = df_BusinessLogic.select(
    col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
    col("MEME_CK").alias("MBR_UNIQ_KEY"),
    col("PDPD_ACC_SFX").alias("PROD_ACCUM_ID"),
    col("MEAC_ACC_TYPE").alias("MBR_ACCUM_TYP_CD"),
    col("ACAC_ACC_NO").alias("ACCUM_NO"),
    col("CURRENT_YR").alias("YR_NO")
)

# ----------------------------------------------------------------
# Stage: MbrAccumPK (CContainerStage / Shared Container)
#   2 inputs => df_Transform, df_AllCol
#   1 output => df_Key
# ----------------------------------------------------------------
# MAGIC %run ../../../../../shared_containers/PrimaryKey/MbrAccumPK
# COMMAND ----------

params_container = {
    "IDSOwner": IDSOwner,
    "CurrRunCycle": CurrRunCycle,
    "SrcSysCd": SrcSysCd
}
df_Key = MbrAccumPK(df_Transform, df_AllCol, params_container)

# ----------------------------------------------------------------
# Stage: IdsMbrAccumExtr (CSeqFileStage)
#   Write to a delimited file "IdsMbrAccumExtr.MbrAccum.dat.#RunID#"
#   Path "key" => does not match "landing"/"external", so use adls_path
# ----------------------------------------------------------------
# Must preserve column order exactly as pinned:
df_final = df_Key.select(
    rpad(col("JOB_EXCTN_RCRD_ERR_SK").cast("string"), 1, " ").alias("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    rpad(col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    rpad(col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    col("FIRST_RECYC_DT"),
    col("ERR_CT"),
    col("RECYCLE_CT"),
    rpad(col("SRC_SYS_CD"), 1, " ").alias("SRC_SYS_CD"), 
    col("PRI_KEY_STRING"),
    col("MBR_ACCUM_SK"),
    col("MBR_UNIQ_KEY"),
    col("PROD_ACCUM_ID"),
    col("MBR_ACCUM_TYP_CD"),
    col("ACCUM_NO"),
    col("YR_NO"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    rpad(col("GRGR_ID"), 10, " ").alias("GRGR_ID"),
    col("MBR_SK"),
    col("ACCUM_AMT"),
    col("CAROVR_AMT"),
    col("COB_OOP_AMT"),
    col("PLN_YR_EFF_DT"),
    col("PLN_YR_END_DT")
)

write_files(
    df_final,
    f"{adls_path}/key/IdsMbrAccumExtr.MbrAccum.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)