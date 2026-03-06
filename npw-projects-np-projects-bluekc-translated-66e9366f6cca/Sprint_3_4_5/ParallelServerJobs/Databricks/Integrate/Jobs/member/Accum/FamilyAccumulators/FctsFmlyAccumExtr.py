# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2006 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     FctsFmlyAccumExtr
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:   Pulls data from Facets CMC_FAAC_ACCUM table for loading to the IDS FMLY_ACCUM table
# MAGIC       
# MAGIC 
# MAGIC INPUTS:       Facets 4.21 Membership Profile system
# MAGIC                       tables:  CMC_FAAC_ACCUM, CMC_GRGR_GROUP, TMP_IDS_MBR_ACCUM_DRVR
# MAGIC 
# MAGIC 
# MAGIC HASH FILES:   hf_fmly_accum_y1, hf_fmly_accum_y2, hf_fmly_accum_y3, hf_fmly_accum_all, hf_fmly_accum
# MAGIC                       
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
# MAGIC               Parikshith Chada   09/14/2006  -  Originally Programmed
# MAGIC               Parikshith Chada   10/02/2006  -  Code changes
# MAGIC               Hugh Sisson          11/15/2006  -  CDHP modification to use TMP_IDS_MBR_ACCUM_DRVR driver table
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                       Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                  ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parik                                11/30/2007          3044                         Made code changes to the SQL                           devlIDS30               Steph Goddard             12/4/07
# MAGIC 
# MAGIC Parik                                2008-08-26        3567(Primary Key)       Added primary key process to the job                     devlIDS                  Steph Goddard              09/02/2008
# MAGIC Ralph Tucker                  2008-12-16        3648(Labor Accts)      Added two field (Carovr_amt)                               devlIDSnew               Steph Goddard              12/23/2008
# MAGIC Ralph Tucker                  2009-11-11        3556 CDC                  Added Change Data Capture Code                      devlIDSnew               Steph Goddard              11/23/2009
# MAGIC 
# MAGIC Bhoomi Dasari                 2/22/2013          TTR-1454                 Changes "Prefetch rows" to 5000                       IntegrateNewDevl       Kalyan Neelam               2013-03-05
# MAGIC                                                                                                     from 50 in CMC_FAAC_ACCUM stage  
# MAGIC 
# MAGIC Akhila M                        10/13/2016          WorkersComp                Remove Workers comp groupids                      IntegrateDev2        Kalyan Neelam               2016-10-14
# MAGIC 
# MAGIC Karthik Chintalapani        2016-11-15               5634      Added new columns PLN_YR_EFF_DT and  PLN_YR_END_DT   IntegrateDev2  Kalyan Neelam           2016-11-28
# MAGIC 
# MAGIC 
# MAGIC Karthik Chintalapani        2016-12-01               5634      Remove Workers comp groupids                                         IntegrateDev2             Kalyan Neelam            2016-12-15
# MAGIC 
# MAGIC Jaideep Mankala    	       2016-12-21                5634     Modified SQL in CMC_FAAC_ACCUM stage and added      IntegrateDev2           Kalyan Neelam            2016-12-23
# MAGIC 					      constraint in Xfm stage to remove null date records.
# MAGIC 
# MAGIC Karthik Chintalapani        2017-01-18              5634      Added the year as part of the lookup key to get the correct results. IntegrateDev2 Kalyan Neelam            2017-01-18
# MAGIC 
# MAGIC Jaideep Mankala            2017-08-21              5630      Modified job to read data from CMC_FATX_ACCUM_TXN	   IntegrateDev1
# MAGIC 						table instead of CMC_FAAC table
# MAGIC Jaideep Mankala            2017-09-26              5630      Modified job to calculate PLN_YR_END_DT from	   IntegrateDev1            Kalyan Neelam            2017-09-26
# MAGIC 						FATX_BEN_BEG_DT date		
# MAGIC  
# MAGIC Prabhu ES                      2022-03-03      S2S                MSSQL ODBC conn params added                                     IntegrateDev5	Ken Bradmon	2022-06-02

# MAGIC Strip Fields
# MAGIC Extract Current and Previous Years
# MAGIC Apply business logic.
# MAGIC This container is used in:
# MAGIC FctsFmlyAccumExtr
# MAGIC 
# MAGIC These programs need to be re-compiled when logic changes
# MAGIC Writing Sequential File to ../key
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType

IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
BCBSOwner = get_widget_value('BCBSOwner','')
bcbs_secret_name = get_widget_value('bcbs_secret_name','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
RunID = get_widget_value('RunID','')
TmpTblRunID = get_widget_value('TmpTblRunID','')
CurrDate = get_widget_value('CurrDate','')
SrcSysCd = get_widget_value('SrcSysCd','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
LastRunDateTime = get_widget_value('LastRunDateTime','')

jdbc_url_CMC_FATX_ACCUM_TXN, jdbc_props_CMC_FATX_ACCUM_TXN = get_db_config(facets_secret_name)
extract_query_CMC_FATX_ACCUM_TXN = f"""
SELECT  DISTINCT
      X.SBSB_CK,
      X.PDPD_ACC_SFX, 
      X.FATX_ACC_TYPE,
      X.ACAC_ACC_NO,
      X.FATX_BEN_BEG_DT,
      X.GRGR_CK,
      X.FATX_TXN_AMT1,
      X.FATX_TXN_AMT2,
      X.GRGR_ID,
      X.FATX_SEQ_NO
FROM
(
SELECT  DISTINCT
      cfa.SBSB_CK,
      cfa.PDPD_ACC_SFX, 
      cfa.FATX_ACC_TYPE,
      cfa.ACAC_ACC_NO,
      cfa.FATX_BEN_BEG_DT,
      cfa.GRGR_CK,
      cfa.FATX_AMT1 FATX_AMT1,
      cfa.FATX_AMT2 FATX_AMT2,
      GRP.GRGR_ID,
      cfa.FATX_SEQ_NO
FROM 
{FacetsOwner}.CMC_FATX_ACCUM_TXN  cfa
INNER JOIN
{FacetsOwner}.CMC_GRGR_GROUP GRP
    ON  cfa.GRGR_CK = GRP.GRGR_CK
      AND cfa.PDPD_ACC_SFX IN ('MED','DEN','HSA')
      AND cfa.FATX_ACC_TYPE IN ('D','C','L')
      AND cfa.SYS_LAST_UPD_DTM >= '{LastRunDateTime}'
   and  NOT EXISTS (
     SELECT DISTINCT 'Y'
     FROM 
     {bcbs_secret_name}.{BCBSOwner}.P_SEL_PRCS_CRITR P_SEL_PRCS_CRITR,
     {FacetsOwner}.CMC_GRGR_GROUP CMC_GRGR 
     WHERE P_SEL_PRCS_CRITR.SEL_PRCS_ID = 'WRHSINBEXCL'
     and P_SEL_PRCS_CRITR.SEL_PRCS_ITEM_ID = 'WORKCOMP'
     and P_SEL_PRCS_CRITR.SEL_PRCS_CRITR_CD = 'MEDPROC'
     and CMC_GRGR.GRGR_ID >= P_SEL_PRCS_CRITR.CRITR_VAL_FROM_TX
     and CMC_GRGR.GRGR_ID <= P_SEL_PRCS_CRITR.CRITR_VAL_THRU_TX
     and GRP.GRGR_CK=CMC_GRGR.GRGR_CK
   )
UNION 
SELECT  DISTINCT
      cfa.SBSB_CK,
      cfa.PDPD_ACC_SFX, 
      cfa.FATX_ACC_TYPE,
      cfa.ACAC_ACC_NO,
      cfa.FATX_BEN_BEG_DT,
      cfa.GRGR_CK,
      cfa.FATX_AMT1 FATX_AMT1,
      cfa.FATX_AMT2 FATX_AMT2,
      GRP.GRGR_ID,
      cfa.FATX_SEQ_NO
FROM 
tempdb..TMP_PROD_CDC_FA_HITDRV DRVR
INNER JOIN
{FacetsOwner}.CMC_MEME_MEMBER meme
ON meme.MEME_CK = DRVR.MEME_CK
AND meme.MEME_REL='M'
INNER JOIN
{FacetsOwner}.CMC_FATX_ACCUM_TXN  cfa
ON cfa.SBSB_CK = meme.SBSB_CK
      AND cfa.PDPD_ACC_SFX IN ('MED','DEN','HSA')
      AND cfa.FATX_ACC_TYPE IN ('D','C','L')
INNER JOIN
{FacetsOwner}.CMC_GRGR_GROUP GRP
    ON  cfa.GRGR_CK = GRP.GRGR_CK
   and  NOT EXISTS (
     SELECT DISTINCT 'Y'
     FROM 
     {bcbs_secret_name}.{BCBSOwner}.P_SEL_PRCS_CRITR P_SEL_PRCS_CRITR,
     {FacetsOwner}.CMC_GRGR_GROUP CMC_GRGR 
     WHERE P_SEL_PRCS_CRITR.SEL_PRCS_ID = 'WRHSINBEXCL'
     and P_SEL_PRCS_CRITR.SEL_PRCS_ITEM_ID = 'WORKCOMP'
     and P_SEL_PRCS_CRITR.SEL_PRCS_CRITR_CD = 'MEDPROC'
     and CMC_GRGR.GRGR_ID >= P_SEL_PRCS_CRITR.CRITR_VAL_FROM_TX
     and CMC_GRGR.GRGR_ID <= P_SEL_PRCS_CRITR.CRITR_VAL_THRU_TX
     and GRP.GRGR_CK=CMC_GRGR.GRGR_CK
   )
UNION 
SELECT  DISTINCT
      cfa.SBSB_CK,
      cfa.PDPD_ACC_SFX, 
      cfa.FATX_ACC_TYPE,
      cfa.ACAC_ACC_NO,
      cfa.FATX_BEN_BEG_DT,
      cfa.GRGR_CK,
      cfa.FATX_AMT1 FATX_AMT1,
      cfa.FATX_AMT2 FATX_AMT2,
      GRP.GRGR_ID,
      cfa.FATX_SEQ_NO
FROM 
tempdb..TMP_PROD_CDC_FA_DRVR DRVR
INNER JOIN
{FacetsOwner}.CMC_MEME_MEMBER meme
ON meme.SBSB_CK = DRVR.SBSB_CK
AND meme.MEME_REL='M'
INNER JOIN
{FacetsOwner}.CMC_FATX_ACCUM_TXN  cfa
ON cfa.SBSB_CK = meme.SBSB_CK
      AND cfa.PDPD_ACC_SFX IN ('MED','DEN','HSA')
      AND cfa.FATX_ACC_TYPE IN ('D','C','L')
INNER JOIN
{FacetsOwner}.CMC_GRGR_GROUP GRP
    ON  cfa.GRGR_CK = GRP.GRGR_CK
   and  NOT EXISTS (
     SELECT DISTINCT 'Y'
     FROM 
     {bcbs_secret_name}.{BCBSOwner}.P_SEL_PRCS_CRITR P_SEL_PRCS_CRITR,
     {FacetsOwner}.CMC_GRGR_GROUP CMC_GRGR 
     WHERE P_SEL_PRCS_CRITR.SEL_PRCS_ID = 'WRHSINBEXCL'
     and P_SEL_PRCS_CRITR.SEL_PRCS_ITEM_ID = 'WORKCOMP'
     and P_SEL_PRCS_CRITR.SEL_PRCS_CRITR_CD = 'MEDPROC'
     and CMC_GRGR.GRGR_ID >= P_SEL_PRCS_CRITR.CRITR_VAL_FROM_TX
     and CMC_GRGR.GRGR_ID <= P_SEL_PRCS_CRITR.CRITR_VAL_THRU_TX
     and GRP.GRGR_CK=CMC_GRGR.GRGR_CK
   )
)  X
ORDER BY
      X.SBSB_CK,
      X.PDPD_ACC_SFX, 
      X.FATX_ACC_TYPE,
      X.ACAC_ACC_NO,
      X.FATX_BEN_BEG_DT,
      X.FATX_SEQ_NO
"""
df_CMC_FATX_ACCUM_TXN = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_CMC_FATX_ACCUM_TXN)
    .options(**jdbc_props_CMC_FATX_ACCUM_TXN)
    .option("query", extract_query_CMC_FATX_ACCUM_TXN)
    .load()
)

jdbc_url_MAX_CMC_FATX_ACCUM_TXN, jdbc_props_MAX_CMC_FATX_ACCUM_TXN = get_db_config(facets_secret_name)
extract_query_MAX_CMC_FATX_ACCUM_TXN = f"""
SELECT  DISTINCT
      SBSB_CK,
      PDPD_ACC_SFX, 
      FATX_ACC_TYPE,
      ACAC_ACC_NO,
      FATX_BEN_BEG_DT,
      MAX(FATX_SEQ_NO) FATX_SEQ_NO
FROM {FacetsOwner}.CMC_FATX_ACCUM_TXN
where
      PDPD_ACC_SFX IN ('MED','DEN','HSA')
      AND FATX_ACC_TYPE IN ('D','C','L')
GROUP BY SBSB_CK,
      PDPD_ACC_SFX, 
      FATX_ACC_TYPE,
      ACAC_ACC_NO,
      FATX_BEN_BEG_DT
"""
df_MAX_CMC_FATX_ACCUM_TXN = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_MAX_CMC_FATX_ACCUM_TXN)
    .options(**jdbc_props_MAX_CMC_FATX_ACCUM_TXN)
    .option("query", extract_query_MAX_CMC_FATX_ACCUM_TXN)
    .load()
)

df_hf_facets_family_accum_max_seq_no = df_MAX_CMC_FATX_ACCUM_TXN.dropDuplicates([
    "SBSB_CK",
    "PDPD_ACC_SFX",
    "FATX_ACC_TYPE",
    "ACAC_ACC_NO",
    "FATX_BEN_BEG_DT",
    "FATX_SEQ_NO"
])

df_join_strip = df_CMC_FATX_ACCUM_TXN.alias("ACCUM").join(
    df_hf_facets_family_accum_max_seq_no.alias("max_lkup"),
    (
        (F.col("ACCUM.SBSB_CK") == F.col("max_lkup.SBSB_CK")) &
        (F.col("ACCUM.PDPD_ACC_SFX") == F.col("max_lkup.PDPD_ACC_SFX")) &
        (F.col("ACCUM.FATX_ACC_TYPE") == F.col("max_lkup.FATX_ACC_TYPE")) &
        (F.col("ACCUM.ACAC_ACC_NO") == F.col("max_lkup.ACAC_ACC_NO")) &
        (F.col("ACCUM.FATX_BEN_BEG_DT") == F.col("max_lkup.FATX_BEN_BEG_DT")) &
        (F.col("ACCUM.FATX_SEQ_NO") == F.col("max_lkup.FATX_SEQ_NO"))
    ),
    how="left"
)
df_stripfieldfaac_filtered = df_join_strip.filter(F.col("max_lkup.SBSB_CK").isNotNull())

df_stripfieldfaac_withvars = df_stripfieldfaac_filtered.withColumn(
    "CurrentDate",
    F.date_format(F.col("ACCUM.FATX_BEN_BEG_DT"), "yyyy-MM-dd")
).withColumn(
    "CurrentYear",
    F.substring(F.col("CurrentDate"), 1, 4)
)

df_stripfieldfaac = df_stripfieldfaac_withvars.select(
    F.col("ACCUM.SBSB_CK").alias("SBSB_CK"),
    strip_field(F.col("ACCUM.PDPD_ACC_SFX")).alias("PDPD_ACC_SFX"),
    F.col("ACCUM.FATX_ACC_TYPE").alias("FATX_ACC_TYPE"),
    F.col("ACCUM.ACAC_ACC_NO").alias("ACAC_ACC_NO"),
    F.col("CurrentYear").alias("CURRENT_YR"),
    F.col("ACCUM.GRGR_CK").alias("GRGR_CK"),
    F.col("ACCUM.FATX_TXN_AMT1").alias("FATX_TXN_AMT1"),
    F.col("ACCUM.FATX_TXN_AMT2").alias("FATX_TXN_AMT2"),
    strip_field(F.col("ACCUM.GRGR_ID")).alias("GRGR_ID"),
    F.col("CurrentDate").alias("CurrDate")
)

df_dedup = df_stripfieldfaac.dropDuplicates([
    "SBSB_CK",
    "PDPD_ACC_SFX",
    "FATX_ACC_TYPE",
    "ACAC_ACC_NO",
    "CURRENT_YR"
])

df_bus_input = df_dedup.alias("BusRules").select(
    "SBSB_CK",
    "PDPD_ACC_SFX",
    "FATX_ACC_TYPE",
    "ACAC_ACC_NO",
    "CURRENT_YR",
    "GRGR_CK",
    "FATX_TXN_AMT1",
    "FATX_TXN_AMT2",
    "GRGR_ID",
    "CurrDate"
)

df_bus_input_withkey = df_bus_input.withColumn(
    "svKey",
    F.concat(
        trim(F.col("BusRules.SBSB_CK")),
        trim(F.col("BusRules.PDPD_ACC_SFX")),
        trim(F.col("BusRules.FATX_ACC_TYPE")),
        trim(F.col("BusRules.ACAC_ACC_NO"))
    )
).withColumn(
    "RowPassThru",
    F.lit("Y")
)

w = (
    F.window("dummy", "1 second")  # placeholder, not used, we'll define a real window next
)
# In PySpark, to replicate row-by-row logic with a custom "previous row" approach, we define a window:
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------

windowSpec = Window.partitionBy("svKey").orderBy("CurrDate")

df_bus = (
    df_bus_input_withkey
    .withColumn("lagKey", F.lag("svKey").over(windowSpec))
    .withColumn("lagCurrentDate", F.lag("CurrDate").over(windowSpec))
    .withColumn(
        "svDateDiffernce",
        F.when(
            F.col("svKey") == F.col("lagKey"),
            LENGTH_DAYS_TS(F.col("CurrDate"), F.col("lagCurrentDate"), F.lit("0"))
        ).otherwise(F.lit("0"))
    )
    .withColumn(
        "svCheckPlnYrEndDt",
        F.when(
            (F.col("svKey") == F.col("lagKey")) & (F.col("svDateDiffernce") > F.lit("365")),
            FIND_DATE(FIND_DATE(F.col("CurrDate"), F.lit("1"), F.lit("Y"), F.lit("X"), F.lit("CCYY-MM-DD")), F.lit("-1"), F.lit("D"), F.lit("X"), F.lit("CCYY-MM-DD"))
        ).when(
            (F.col("svKey") == F.col("lagKey")) & (F.col("svDateDiffernce") <= F.lit("365")),
            FIND_DATE(F.col("lagCurrentDate"), F.lit("-1"), F.lit("D"), F.lit("X"), F.lit("CCYY-MM-DD"))
        ).otherwise(F.lit(""))
    )
)

df_BusinessLogic_AllCol = df_bus.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("BusRules.SBSB_CK").alias("SUB_UNIQ_KEY"),
    F.col("BusRules.PDPD_ACC_SFX").alias("PROD_ACCUM_ID"),
    F.col("BusRules.FATX_ACC_TYPE").alias("FMLY_ACCUM_TYP_CD"),
    F.col("BusRules.ACAC_ACC_NO").alias("ACCUM_NO"),
    F.col("BusRules.CURRENT_YR").alias("YR_NO"),
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.lit("I"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.lit("N"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.col("RowPassThru"), 1, " ").alias("PASS_THRU_IN"),
    F.col("BusRules.CurrDate").alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit(SrcSysCd).alias("SRC_SYS_CD"),
    F.concat(
        F.lit(SrcSysCd), F.lit(";"),
        F.col("BusRules.SBSB_CK"), F.lit(";"),
        F.col("BusRules.PDPD_ACC_SFX"), F.lit(";"),
        F.col("BusRules.FATX_ACC_TYPE"), F.lit(";"),
        F.col("BusRules.ACAC_ACC_NO"), F.lit(";"),
        F.col("BusRules.CURRENT_YR")
    ).alias("PRI_KEY_STRING"),
    F.lit(0).alias("FMLY_ACCUM_SK"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN"),
    F.rpad(F.col("BusRules.GRGR_ID"), 10, " ").alias("GRGR_ID"),
    F.col("BusRules.SBSB_CK").alias("SUB_SK"),
    F.col("BusRules.FATX_TXN_AMT1").alias("ACCUM_AMT"),
    F.col("BusRules.FATX_TXN_AMT2").alias("CAROVR_AMT"),
    F.col("BusRules.CurrDate").alias("PLN_YR_EFF_DT"),
    F.col("svCheckPlnYrEndDt").alias("PLN_YR_END_DT")
)

df_BusinessLogic_Transform = df_bus.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("BusRules.SBSB_CK").alias("SUB_UNIQ_KEY"),
    F.col("BusRules.PDPD_ACC_SFX").alias("PROD_ACCUM_ID"),
    F.col("BusRules.FATX_ACC_TYPE").alias("FMLY_ACCUM_TYP_CD"),
    F.col("BusRules.ACAC_ACC_NO").alias("ACCUM_NO"),
    F.col("BusRules.CURRENT_YR").alias("YR_NO")
)

# MAGIC %run ../../../../../shared_containers/PrimaryKey/FmlyAccumPK
params_FmlyAccumPK = {
    "IDSOwner": IDSOwner,
    "CurrRunCycle": CurrRunCycle,
    "SrcSysCd": SrcSysCd
}
df_IdsFmlyAccumExtr = FmlyAccumPK(df_BusinessLogic_Transform, df_BusinessLogic_AllCol, params_FmlyAccumPK)

df_final_select = df_IdsFmlyAccumExtr.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("FMLY_ACCUM_SK"),
    F.col("SUB_UNIQ_KEY"),
    F.col("PROD_ACCUM_ID"),
    F.col("FMLY_ACCUM_TYP_CD"),
    F.col("ACCUM_NO"),
    F.col("YR_NO"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.rpad(F.col("GRGR_ID"), 10, " ").alias("GRGR_ID"),
    F.col("SUB_SK"),
    F.col("ACCUM_AMT"),
    F.col("CAROVR_AMT"),
    F.col("PLN_YR_EFF_DT"),
    F.col("PLN_YR_END_DT")
)

write_files(
    df_final_select,
    f"{adls_path}/key/IdsFmlyAccumExtr.FmlyAccum.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)