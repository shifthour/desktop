# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Copyright 2013 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC CALLED BY: IhmfConstituentVbbExtrSeq
# MAGIC  
# MAGIC PROCESSING:   Extracts data from P_GRPP_VBB_ELIG table and looks up for all the classes and class plans to be exlcuded or included and creates a key file
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Kalyan Neelam        2013-06-11    4963 VBB Phase III    Initial Programming                                                                       IntegrateNewDevl        Bhoomi Dasari            7/3/2013
# MAGIC Raja Gummadi         2013-10-16       4963                       Changed logic for VBB_PLN_ELIG_STRT_DT and                    IntegrateNewDevl          Kalyan Neelam           2013-10-25
# MAGIC                                                                                         VBB_PLN_ELIG_END_DT
# MAGIC Kalyan Neelam        2014-11-14         TFS 9558              Added balancing snapshot file                                                     IntegrateNewDevl       Bhoomi Dasari              12/09/2014

# MAGIC Processing for EXCLD_IND = 1
# MAGIC Landing file created in IdsGrpVbbPlnEligPreprocExtr, contains the current processing records
# MAGIC The file is used later in the processing for lookup
# MAGIC IDS GRP_VBB_PLN_ELIG Extract
# MAGIC Case1_1 : HIPL_ID not BLANK or NULL And HIEL_LOG_LEVEL1 not BLANK or NULL And HIEL_INC_EXC_IND = 1 And HIEL_LOG_LEVEL2 not BLANK or NULL And HIEL_LOG_LEVEL3 not BLANK or NULL
# MAGIC 
# MAGIC Case1_2 : HIPL_ID not BLANK or NULL And HIEL_LOG_LEVEL1 not BLANK or NULL And HIEL_INC_EXC_IND = 1 And HIEL_LOG_LEVEL2 not BLANK or NULL And HIEL_LOG_LEVEL3 IS BLANK or NULL
# MAGIC 
# MAGIC Case1_3 : HIPL_ID not BLANK or NULL And HIEL_LOG_LEVEL1 not BLANK or NULL And HIEL_INC_EXC_IND = 1 And HIEL_LOG_LEVEL2 IS BLANK or NULL And HIEL_LOG_LEVEL3 not BLANK or NULL
# MAGIC 
# MAGIC Case1_4 : HIPL_ID not BLANK or NULL And HIEL_LOG_LEVEL1 not BLANK or NULL And HIEL_INC_EXC_IND = 1 And HIEL_LOG_LEVEL2 IS BLANK or NULL And HIEL_LOG_LEVEL3 IS BLANK or NULL
# MAGIC Processing for EXCLD_IND = 0
# MAGIC Case2_1 : HIPL_ID not BLANK or NULL And HIEL_LOG_LEVEL1 not BLANK or NULL And HIEL_INC_EXC_IND = 0 And HIEL_LOG_LEVEL2 not BLANK or NULL And HIEL_LOG_LEVEL3 not BLANK or NULL
# MAGIC 
# MAGIC Case2_2 : HIPL_ID not BLANK or NULL And HIEL_LOG_LEVEL1 not BLANK or NULL And HIEL_INC_EXC_IND = 0 And HIEL_LOG_LEVEL2 not BLANK or NULL And HIEL_LOG_LEVEL3 IS BLANK or NULL
# MAGIC 
# MAGIC Case2_3 : HIPL_ID not BLANK or NULL And HIEL_LOG_LEVEL1 not BLANK or NULL And HIEL_INC_EXC_IND = 0 And HIEL_LOG_LEVEL2 IS BLANK or NULL And HIEL_LOG_LEVEL3 not BLANK or NULL
# MAGIC 
# MAGIC Case2_4 : HIPL_ID not BLANK or NULL And HIEL_LOG_LEVEL1 not BLANK or NULL And HIEL_INC_EXC_IND = 0 And HIEL_LOG_LEVEL2 IS BLANK or NULL And HIEL_LOG_LEVEL3 IS BLANK or NULL
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
VBBOwner = get_widget_value('VBBOwner','')
vbb_secret_name = get_widget_value('vbb_secret_name','')
SrcSysCd = get_widget_value('SrcSysCd','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
RunId = get_widget_value('RunId','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
LastRunDtm = get_widget_value('LastRunDtm','')
CurrDate = get_widget_value('CurrDate','')
Environment = get_widget_value('Environment','')

# MAGIC %run ../../../../../shared_containers/PrimaryKey/GrpVbbPlnEligPK
# COMMAND ----------

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

extract_query_EXCLD_CLASSES = """
SELECT
  EXCLD.VBB_PLN_UNIQ_KEY,
  EXCLD.GRP_ID,
  CASE WHEN EXCLD.CLS_ID IS NULL THEN INCLD.CLS_ID ELSE EXCLD.CLS_ID END AS CLS_ID,
  CASE WHEN EXCLD.CLS_PLN_ID IS NULL THEN INCLD.CLS_PLN_ID ELSE EXCLD.CLS_PLN_ID END AS CLS_PLN_ID
FROM
(
  SELECT VBB_PLN_UNIQ_KEY, GRP_ID, CLS_ID, CLS_PLN_ID
  FROM """ + IDSOwner + """.P_GRP_VBB_ELIG
  WHERE GRP_VBB_ELIG_INCLD_IN = 0
) EXCLD,
(
  SELECT VBB_PLN_UNIQ_KEY, GRP_ID, CLS_ID, CLS_PLN_ID
  FROM """ + IDSOwner + """.P_GRP_VBB_ELIG
  WHERE GRP_VBB_ELIG_INCLD_IN = 1
) INCLD
WHERE
  INCLD.VBB_PLN_UNIQ_KEY = EXCLD.VBB_PLN_UNIQ_KEY
  AND INCLD.GRP_ID = EXCLD.GRP_ID
"""

df_EXCLD_CLASSES = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_EXCLD_CLASSES)
    .load()
)
df_hf_grpvbbplnelig_excldrecords = dedup_sort(
    df_EXCLD_CLASSES,
    ["VBB_PLN_UNIQ_KEY", "GRP_ID", "CLS_ID", "CLS_PLN_ID"],
    []
)

schema_IhmfConstituent_GrpVbbPlnEligLanding = StructType([
    StructField("HIPL_ID", IntegerType(), False),
    StructField("HIEL_ID_SEQ", IntegerType(), False),
    StructField("HIEL_START_DT", TimestampType(), True),
    StructField("HIEL_END_DT", TimestampType(), True),
    StructField("HIEL_INC_EXC_IND", IntegerType(), True),
    StructField("HIEL_SEQ_TYPE", IntegerType(), True),
    StructField("HIEL_LOG_LEVEL1", StringType(), True),
    StructField("HIEL_LOG_LEVEL2", StringType(), True),
    StructField("HIEL_LOG_LEVEL3", StringType(), True),
    StructField("HIEL_PHY_LEVEL1", StringType(), True),
    StructField("HIEL_PHY_LEVEL2", StringType(), True),
    StructField("HIEL_PHY_LEVEL3", StringType(), True),
    StructField("HIEL_NAME_LEVEL1", StringType(), True),
    StructField("HIEL_NAME_LEVEL2", StringType(), True),
    StructField("HIEL_NAME_LEVEL3", StringType(), True),
    StructField("PAYR_ID", IntegerType(), False),
    StructField("HIEL_CREATE_DT", TimestampType(), False),
    StructField("HIEL_CREATE_APP_USER", StringType(), False),
    StructField("HIEL_UPDATE_DT", TimestampType(), False),
    StructField("HIEL_UPDATE_DB_USER", StringType(), False),
    StructField("HIEL_UPDATE_APP_USER", StringType(), False),
    StructField("HIEL_UPDATE_SEQ", IntegerType(), False),
    StructField("CDVL_VALUE_CATEGORY", StringType(), False),
    StructField("HIPL_YEAR", IntegerType(), True),
    StructField("HIPL_START_DT", TimestampType(), True)
])

path_IhmfConstituent_GrpVbbPlnEligLanding = f"{adls_path}/verified/IhmfConstituent_GrpVbbPlnEligLanding.dat.{RunId}"
df_IhmfConstituent_GrpVbbPlnEligLanding = (
    spark.read.format("csv")
    .option("header", "false")
    .option("quote", "\"")
    .option("delimiter", ",")
    .schema(schema_IhmfConstituent_GrpVbbPlnEligLanding)
    .load(path_IhmfConstituent_GrpVbbPlnEligLanding)
)

df_Transformer_166 = df_IhmfConstituent_GrpVbbPlnEligLanding.select(
    F.col("HIPL_ID").alias("HIPL_ID"),
    F.col("HIEL_LOG_LEVEL1").alias("HIEL_LOG_LEVEL1"),
    F.col("HIEL_ID_SEQ").alias("HIEL_ID_SEQ"),
    F.col("HIEL_START_DT").alias("HIEL_START_DT"),
    F.col("HIEL_END_DT").alias("HIEL_END_DT"),
    F.col("HIEL_INC_EXC_IND").alias("HIEL_INC_EXC_IND"),
    F.col("HIEL_SEQ_TYPE").alias("HIEL_SEQ_TYPE"),
    F.col("HIEL_LOG_LEVEL2").alias("HIEL_LOG_LEVEL2"),
    F.col("HIEL_LOG_LEVEL3").alias("HIEL_LOG_LEVEL3"),
    F.col("HIEL_PHY_LEVEL1").alias("HIEL_PHY_LEVEL1"),
    F.col("HIEL_PHY_LEVEL2").alias("HIEL_PHY_LEVEL2"),
    F.col("HIEL_PHY_LEVEL3").alias("HIEL_PHY_LEVEL3"),
    F.col("HIEL_NAME_LEVEL1").alias("HIEL_NAME_LEVEL1"),
    F.col("HIEL_NAME_LEVEL2").alias("HIEL_NAME_LEVEL2"),
    F.col("HIEL_NAME_LEVEL3").alias("HIEL_NAME_LEVEL3"),
    F.col("PAYR_ID").alias("PAYR_ID"),
    F.col("HIEL_CREATE_DT").alias("HIEL_CREATE_DT"),
    F.col("HIEL_CREATE_APP_USER").alias("HIEL_CREATE_APP_USER"),
    F.col("HIEL_UPDATE_DT").alias("HIEL_UPDATE_DT"),
    F.col("HIEL_UPDATE_DB_USER").alias("HIEL_UPDATE_DB_USER"),
    F.col("HIEL_UPDATE_APP_USER").alias("HIEL_UPDATE_APP_USER"),
    F.col("HIEL_UPDATE_SEQ").alias("HIEL_UPDATE_SEQ"),
    F.col("CDVL_VALUE_CATEGORY").alias("CDVL_VALUE_CATEGORY"),
    F.col("HIPL_YEAR").alias("HIPL_YEAR"),
    F.col("HIPL_START_DT").alias("HIPL_START_DT")
)

df_hf_grpvbbplnelig_landingdata = dedup_sort(
    df_Transformer_166,
    ["HIPL_ID","HIEL_LOG_LEVEL1"],
    []
)

extract_query_CD_MPPNG = """
SELECT DISTINCT
  CD.CD_MPPNG_SK
FROM """ + IDSOwner + """.P_GRP_VBB_ELIG P,
     """ + IDSOwner + """.CD_MPPNG CD
WHERE
  P.VBB_PLN_CAT_CD = CD.SRC_CD
  AND CD.SRC_DOMAIN_NM = 'CLASS PLAN PRODUCT CATEGORY'
  AND CD.SRC_SYS_CD = 'FACETS'
  AND CD.TRGT_DOMAIN_NM = 'CLASS PLAN PRODUCT CATEGORY'
  AND CD.TRGT_SRC_SYS_CD = 'IDS'
"""

df_CD_MPPNG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_CD_MPPNG)
    .load()
)
df_hf_grpvbbplnelig_activeclss_cdmpng1 = dedup_sort(
    df_CD_MPPNG,
    ["CD_MPPNG_SK"],
    []
)

extract_query_case1 = """
/*********** Case 1 **********/
SELECT DISTINCT
      DRVR.VBB_PLN_UNIQ_KEY,
      CLS.GRP_ID,
      CLS.CLS_ID,
      CLS.CLS_PLN_ID,
      CLS.CLS_PLN_DTL_PROD_CAT_CD_SK,
      DRVR.VBB_PLN_STRT_YR_NO,
      CLS_PLN_DTL.PLN_BEG_DT_MO_DAY,
      DRVR.VBB_PLN_ELIG_STRT_DT_SK,
      DRVR.VBB_PLN_ELIG_END_DT_SK,
      DRVR.VBB_PLN_STRT_DT_SK
FROM """ + IDSOwner + """.CLS_PLN_DTL CLS,
(
  SELECT P.VBB_PLN_UNIQ_KEY,
         P.GRP_ID,
         P.CLS_ID,
         P.CLS_PLN_ID,
         P.VBB_PLN_ELIG_STRT_DT_SK,
         P.VBB_PLN_ELIG_END_DT_SK,
         P.VBB_PLN_STRT_YR_NO,
         P.VBB_PLN_STRT_DT_SK
  FROM """ + IDSOwner + """.P_GRP_VBB_ELIG P
  WHERE
    P.VBB_PLN_UNIQ_KEY IS NOT NULL
    AND P.GRP_ID IS NOT NULL
    AND P.CLS_ID IS NOT NULL
    AND P.CLS_PLN_ID IS NOT NULL
    AND P.GRP_VBB_ELIG_INCLD_IN = 1
  UNION
  SELECT DISTINCT
         P.VBB_PLN_UNIQ_KEY,
         P.GRP_ID,
         P.CLS_ID,
         CLS.CLS_PLN_ID,
         P.VBB_PLN_ELIG_STRT_DT_SK,
         P.VBB_PLN_ELIG_END_DT_SK,
         P.VBB_PLN_STRT_YR_NO,
         P.VBB_PLN_STRT_DT_SK
  FROM """ + IDSOwner + """.P_GRP_VBB_ELIG P,
       """ + IDSOwner + """.CLS_PLN_DTL CLS
  WHERE
    P.GRP_ID = CLS.GRP_ID
    AND P.CLS_ID = CLS.CLS_ID
    AND P.VBB_PLN_UNIQ_KEY IS NOT NULL
    AND P.GRP_ID IS NOT NULL
    AND P.CLS_ID IS NOT NULL
    AND P.CLS_PLN_ID IS NULL
    AND P.GRP_VBB_ELIG_INCLD_IN = 1
  UNION
  SELECT DISTINCT
         P.VBB_PLN_UNIQ_KEY,
         P.GRP_ID,
         CLS.CLS_ID,
         P.CLS_PLN_ID,
         P.VBB_PLN_ELIG_STRT_DT_SK,
         P.VBB_PLN_ELIG_END_DT_SK,
         P.VBB_PLN_STRT_YR_NO,
         P.VBB_PLN_STRT_DT_SK
  FROM """ + IDSOwner + """.P_GRP_VBB_ELIG P,
       """ + IDSOwner + """.CLS_PLN_DTL CLS
  WHERE
    P.GRP_ID = CLS.GRP_ID
    AND P.CLS_PLN_ID = CLS.CLS_PLN_ID
    AND P.VBB_PLN_UNIQ_KEY IS NOT NULL
    AND P.GRP_ID IS NOT NULL
    AND P.CLS_ID IS NULL
    AND P.CLS_PLN_ID IS NOT NULL
    AND P.GRP_VBB_ELIG_INCLD_IN = 1
  UNION
  SELECT DISTINCT
         P.VBB_PLN_UNIQ_KEY,
         P.GRP_ID,
         CLS.CLS_ID,
         CLS.CLS_PLN_ID,
         P.VBB_PLN_ELIG_STRT_DT_SK,
         P.VBB_PLN_ELIG_END_DT_SK,
         P.VBB_PLN_STRT_YR_NO,
         P.VBB_PLN_STRT_DT_SK
  FROM """ + IDSOwner + """.P_GRP_VBB_ELIG P,
       """ + IDSOwner + """.CLS_PLN_DTL CLS
  WHERE
    P.GRP_ID = CLS.GRP_ID
    AND P.VBB_PLN_UNIQ_KEY IS NOT NULL
    AND P.GRP_ID IS NOT NULL
    AND P.CLS_ID IS NULL
    AND P.CLS_PLN_ID IS NULL
    AND P.GRP_VBB_ELIG_INCLD_IN = 1
) DRVR
LEFT OUTER JOIN """ + IDSOwner + """.CLS_PLN_DTL CLS_PLN_DTL
ON CLS_PLN_DTL.GRP_ID = DRVR.GRP_ID
AND CLS_PLN_DTL.CLS_ID = DRVR.CLS_ID
AND CLS_PLN_DTL.CLS_PLN_ID = DRVR.CLS_PLN_ID
WHERE
  CLS.GRP_ID = DRVR.GRP_ID
  AND CLS.CLS_ID = DRVR.CLS_ID
  AND CLS.CLS_PLN_ID = DRVR.CLS_PLN_ID
  AND DRVR.VBB_PLN_ELIG_STRT_DT_SK IS NOT NULL
  AND DRVR.VBB_PLN_ELIG_END_DT_SK IS NULL
  AND CLS.EFF_DT_SK <= DRVR.VBB_PLN_ELIG_STRT_DT_SK
  AND CLS.TERM_DT_SK = '""" + CurrDate + """'
  AND SUBSTR(CLS_PLN_DTL.EFF_DT_SK, 1, 4) <= DRVR.VBB_PLN_STRT_YR_NO
  AND CLS_PLN_DTL.TERM_DT_SK >= '""" + CurrDate + """'
"""

df_case1 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_case1)
    .load()
)

extract_query_case2 = """
/*********** Case 2 **********/
SELECT DISTINCT
      DRVR.VBB_PLN_UNIQ_KEY,
      CLS.GRP_ID,
      CLS.CLS_ID,
      CLS.CLS_PLN_ID, CLS.CLS_PLN_DTL_PROD_CAT_CD_SK,
      DRVR.VBB_PLN_STRT_YR_NO,
      CLS_PLN_DTL.PLN_BEG_DT_MO_DAY,
      DRVR.VBB_PLN_ELIG_STRT_DT_SK,
      DRVR.VBB_PLN_ELIG_END_DT_SK,
      DRVR.VBB_PLN_STRT_DT_SK
FROM """ + IDSOwner + """.CLS_PLN_DTL CLS,
(
  SELECT P.VBB_PLN_UNIQ_KEY,
         P.GRP_ID,
         P.CLS_ID,
         P.CLS_PLN_ID,
         P.VBB_PLN_ELIG_STRT_DT_SK,
         P.VBB_PLN_ELIG_END_DT_SK,
         P.VBB_PLN_STRT_YR_NO,
         P.VBB_PLN_STRT_DT_SK
  FROM """ + IDSOwner + """.P_GRP_VBB_ELIG P
  WHERE
    P.VBB_PLN_UNIQ_KEY IS NOT NULL
    AND P.GRP_ID IS NOT NULL
    AND P.CLS_ID IS NOT NULL
    AND P.CLS_PLN_ID IS NOT NULL
    AND P.GRP_VBB_ELIG_INCLD_IN = 1
  UNION
  SELECT DISTINCT
         P.VBB_PLN_UNIQ_KEY,
         P.GRP_ID,
         P.CLS_ID,
         CLS.CLS_PLN_ID,
         P.VBB_PLN_ELIG_STRT_DT_SK,
         P.VBB_PLN_ELIG_END_DT_SK,
         P.VBB_PLN_STRT_YR_NO,
         P.VBB_PLN_STRT_DT_SK
  FROM """ + IDSOwner + """.P_GRP_VBB_ELIG P,
       """ + IDSOwner + """.CLS_PLN_DTL CLS
  WHERE
    P.GRP_ID = CLS.GRP_ID
    AND P.CLS_ID = CLS.CLS_ID
    AND P.VBB_PLN_UNIQ_KEY IS NOT NULL
    AND P.GRP_ID IS NOT NULL
    AND P.CLS_ID IS NOT NULL
    AND P.CLS_PLN_ID IS NULL
    AND P.GRP_VBB_ELIG_INCLD_IN = 1
  UNION
  SELECT DISTINCT
         P.VBB_PLN_UNIQ_KEY,
         P.GRP_ID,
         CLS.CLS_ID,
         P.CLS_PLN_ID,
         P.VBB_PLN_ELIG_STRT_DT_SK,
         P.VBB_PLN_ELIG_END_DT_SK,
         P.VBB_PLN_STRT_YR_NO,
         P.VBB_PLN_STRT_DT_SK
  FROM """ + IDSOwner + """.P_GRP_VBB_ELIG P,
       """ + IDSOwner + """.CLS_PLN_DTL CLS
  WHERE
    P.GRP_ID = CLS.GRP_ID
    AND P.CLS_PLN_ID = CLS.CLS_PLN_ID
    AND P.VBB_PLN_UNIQ_KEY IS NOT NULL
    AND P.GRP_ID IS NOT NULL
    AND P.CLS_ID IS NULL
    AND P.CLS_PLN_ID IS NOT NULL
    AND P.GRP_VBB_ELIG_INCLD_IN = 1
  UNION
  SELECT DISTINCT
         P.VBB_PLN_UNIQ_KEY,
         P.GRP_ID,
         CLS.CLS_ID,
         CLS.CLS_PLN_ID,
         P.VBB_PLN_ELIG_STRT_DT_SK,
         P.VBB_PLN_ELIG_END_DT_SK,
         P.VBB_PLN_STRT_YR_NO,
         P.VBB_PLN_STRT_DT_SK
  FROM """ + IDSOwner + """.P_GRP_VBB_ELIG P,
       """ + IDSOwner + """.CLS_PLN_DTL CLS
  WHERE
    P.GRP_ID = CLS.GRP_ID
    AND P.VBB_PLN_UNIQ_KEY IS NOT NULL
    AND P.GRP_ID IS NOT NULL
    AND P.CLS_ID IS NULL
    AND P.CLS_PLN_ID IS NULL
    AND P.GRP_VBB_ELIG_INCLD_IN = 1
) DRVR
LEFT OUTER JOIN """ + IDSOwner + """.CLS_PLN_DTL CLS_PLN_DTL
ON CLS_PLN_DTL.GRP_ID = DRVR.GRP_ID
AND CLS_PLN_DTL.CLS_ID = DRVR.CLS_ID
AND CLS_PLN_DTL.CLS_PLN_ID = DRVR.CLS_PLN_ID
WHERE
  CLS.GRP_ID = DRVR.GRP_ID
  AND CLS.CLS_ID = DRVR.CLS_ID
  AND CLS.CLS_PLN_ID = DRVR.CLS_PLN_ID
  AND DRVR.VBB_PLN_ELIG_STRT_DT_SK IS NULL
  AND DRVR.VBB_PLN_ELIG_END_DT_SK IS NOT NULL
  AND DRVR.VBB_PLN_STRT_YR_NO IS NOT NULL
  AND SUBSTR(CLS.EFF_DT_SK, 1, 4) <= DRVR.VBB_PLN_STRT_YR_NO
  AND CLS.TERM_DT_SK >= DRVR.VBB_PLN_ELIG_END_DT_SK
  AND SUBSTR(CLS_PLN_DTL.EFF_DT_SK, 1, 4) <= DRVR.VBB_PLN_STRT_YR_NO
  AND CLS_PLN_DTL.TERM_DT_SK >= '""" + CurrDate + """'
UNION
/*********** Case 2 **********/
SELECT DISTINCT
      DRVR.VBB_PLN_UNIQ_KEY,
      CLS.GRP_ID,
      CLS.CLS_ID,
      CLS.CLS_PLN_ID, CLS.CLS_PLN_DTL_PROD_CAT_CD_SK,
      DRVR.VBB_PLN_STRT_YR_NO,
      CLS_PLN_DTL.PLN_BEG_DT_MO_DAY,
      DRVR.VBB_PLN_ELIG_STRT_DT_SK,
      DRVR.VBB_PLN_ELIG_END_DT_SK,
      DRVR.VBB_PLN_STRT_DT_SK
FROM """ + IDSOwner + """.CLS_PLN_DTL CLS,
(
  SELECT P.VBB_PLN_UNIQ_KEY,
         P.GRP_ID,
         P.CLS_ID,
         P.CLS_PLN_ID,
         P.VBB_PLN_ELIG_STRT_DT_SK,
         P.VBB_PLN_ELIG_END_DT_SK,
         P.VBB_PLN_STRT_YR_NO,
         P.VBB_PLN_STRT_DT_SK
  FROM """ + IDSOwner + """.P_GRP_VBB_ELIG P
  WHERE
    P.VBB_PLN_UNIQ_KEY IS NOT NULL
    AND P.GRP_ID IS NOT NULL
    AND P.CLS_ID IS NOT NULL
    AND P.CLS_PLN_ID IS NOT NULL
    AND P.GRP_VBB_ELIG_INCLD_IN = 1
  UNION
  SELECT DISTINCT
         P.VBB_PLN_UNIQ_KEY,
         P.GRP_ID,
         P.CLS_ID,
         CLS.CLS_PLN_ID,
         P.VBB_PLN_ELIG_STRT_DT_SK,
         P.VBB_PLN_ELIG_END_DT_SK,
         P.VBB_PLN_STRT_YR_NO,
         P.VBB_PLN_STRT_DT_SK
  FROM """ + IDSOwner + """.P_GRP_VBB_ELIG P,
       """ + IDSOwner + """.CLS_PLN_DTL CLS
  WHERE
    P.GRP_ID = CLS.GRP_ID
    AND P.CLS_ID = CLS.CLS_ID
    AND P.VBB_PLN_UNIQ_KEY IS NOT NULL
    AND P.GRP_ID IS NOT NULL
    AND P.CLS_ID IS NOT NULL
    AND P.CLS_PLN_ID IS NULL
    AND P.GRP_VBB_ELIG_INCLD_IN = 1
  UNION
  SELECT DISTINCT
         P.VBB_PLN_UNIQ_KEY,
         P.GRP_ID,
         CLS.CLS_ID,
         P.CLS_PLN_ID,
         P.VBB_PLN_ELIG_STRT_DT_SK,
         P.VBB_PLN_ELIG_END_DT_SK,
         P.VBB_PLN_STRT_YR_NO,
         P.VBB_PLN_STRT_DT_SK
  FROM """ + IDSOwner + """.P_GRP_VBB_ELIG P,
       """ + IDSOwner + """.CLS_PLN_DTL CLS
  WHERE
    P.GRP_ID = CLS.GRP_ID
    AND P.CLS_PLN_ID = CLS.CLS_PLN_ID
    AND P.VBB_PLN_UNIQ_KEY IS NOT NULL
    AND P.GRP_ID IS NOT NULL
    AND P.CLS_ID IS NULL
    AND P.CLS_PLN_ID IS NOT NULL
    AND P.GRP_VBB_ELIG_INCLD_IN = 1
  UNION
  SELECT DISTINCT
         P.VBB_PLN_UNIQ_KEY,
         P.GRP_ID,
         CLS.CLS_ID,
         CLS.CLS_PLN_ID,
         P.VBB_PLN_ELIG_STRT_DT_SK,
         P.VBB_PLN_ELIG_END_DT_SK,
         P.VBB_PLN_STRT_YR_NO,
         P.VBB_PLN_STRT_DT_SK
  FROM """ + IDSOwner + """.P_GRP_VBB_ELIG P,
       """ + IDSOwner + """.CLS_PLN_DTL CLS
  WHERE
    P.GRP_ID = CLS.GRP_ID
    AND P.VBB_PLN_UNIQ_KEY IS NOT NULL
    AND P.GRP_ID IS NOT NULL
    AND P.CLS_ID IS NULL
    AND P.CLS_PLN_ID IS NULL
    AND P.GRP_VBB_ELIG_INCLD_IN = 1
) DRVR
LEFT OUTER JOIN """ + IDSOwner + """.CLS_PLN_DTL CLS_PLN_DTL
ON CLS_PLN_DTL.GRP_ID = DRVR.GRP_ID
AND CLS_PLN_DTL.CLS_ID = DRVR.CLS_ID
AND CLS_PLN_DTL.CLS_PLN_ID = DRVR.CLS_PLN_ID
WHERE
  CLS.GRP_ID = DRVR.GRP_ID
  AND CLS.CLS_ID = DRVR.CLS_ID
  AND CLS.CLS_PLN_ID = DRVR.CLS_PLN_ID
  AND DRVR.VBB_PLN_ELIG_STRT_DT_SK IS NULL
  AND DRVR.VBB_PLN_ELIG_END_DT_SK IS NOT NULL
  AND DRVR.VBB_PLN_STRT_YR_NO IS NULL
  AND CLS.EFF_DT_SK <= DRVR.VBB_PLN_STRT_DT_SK
  AND CLS.TERM_DT_SK >= DRVR.VBB_PLN_ELIG_END_DT_SK
  AND SUBSTR(CLS_PLN_DTL.EFF_DT_SK, 1, 4) <= DRVR.VBB_PLN_STRT_YR_NO
  AND CLS_PLN_DTL.TERM_DT_SK >= '""" + CurrDate + """'
"""

df_case2 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_case2)
    .load()
)

extract_query_case3 = """
/********** Case 3 **********/
SELECT DISTINCT
      DRVR.VBB_PLN_UNIQ_KEY,
      CLS.GRP_ID,
      CLS.CLS_ID,
      CLS.CLS_PLN_ID, CLS.CLS_PLN_DTL_PROD_CAT_CD_SK,
      DRVR.VBB_PLN_STRT_YR_NO,
      CLS_PLN_DTL.PLN_BEG_DT_MO_DAY,
      DRVR.VBB_PLN_ELIG_STRT_DT_SK,
      DRVR.VBB_PLN_ELIG_END_DT_SK,
      DRVR.VBB_PLN_STRT_DT_SK
FROM """ + IDSOwner + """.CLS_PLN_DTL CLS,
(
  SELECT P.VBB_PLN_UNIQ_KEY,
         P.GRP_ID,
         P.CLS_ID,
         P.CLS_PLN_ID,
         P.VBB_PLN_ELIG_STRT_DT_SK,
         P.VBB_PLN_ELIG_END_DT_SK,
         P.VBB_PLN_STRT_YR_NO,
         P.VBB_PLN_STRT_DT_SK
  FROM """ + IDSOwner + """.P_GRP_VBB_ELIG P
  WHERE
    P.VBB_PLN_UNIQ_KEY IS NOT NULL
    AND P.GRP_ID IS NOT NULL
    AND P.CLS_ID IS NOT NULL
    AND P.CLS_PLN_ID IS NOT NULL
    AND P.GRP_VBB_ELIG_INCLD_IN = 1
  UNION
  SELECT DISTINCT
         P.VBB_PLN_UNIQ_KEY,
         P.GRP_ID,
         P.CLS_ID,
         CLS.CLS_PLN_ID,
         P.VBB_PLN_ELIG_STRT_DT_SK,
         P.VBB_PLN_ELIG_END_DT_SK,
         P.VBB_PLN_STRT_YR_NO,
         P.VBB_PLN_STRT_DT_SK
  FROM """ + IDSOwner + """.P_GRP_VBB_ELIG P,
       """ + IDSOwner + """.CLS_PLN_DTL CLS
  WHERE
    P.GRP_ID = CLS.GRP_ID
    AND P.CLS_ID = CLS.CLS_ID
    AND P.VBB_PLN_UNIQ_KEY IS NOT NULL
    AND P.GRP_ID IS NOT NULL
    AND P.CLS_ID IS NOT NULL
    AND P.CLS_PLN_ID IS NULL
    AND P.GRP_VBB_ELIG_INCLD_IN = 1
  UNION
  SELECT DISTINCT
         P.VBB_PLN_UNIQ_KEY,
         P.GRP_ID,
         CLS.CLS_ID,
         P.CLS_PLN_ID,
         P.VBB_PLN_ELIG_STRT_DT_SK,
         P.VBB_PLN_ELIG_END_DT_SK,
         P.VBB_PLN_STRT_YR_NO,
         P.VBB_PLN_STRT_DT_SK
  FROM """ + IDSOwner + """.P_GRP_VBB_ELIG P,
       """ + IDSOwner + """.CLS_PLN_DTL CLS
  WHERE
    P.GRP_ID = CLS.GRP_ID
    AND P.CLS_PLN_ID = CLS.CLS_PLN_ID
    AND P.VBB_PLN_UNIQ_KEY IS NOT NULL
    AND P.GRP_ID IS NOT NULL
    AND P.CLS_ID IS NULL
    AND P.CLS_PLN_ID IS NOT NULL
    AND P.GRP_VBB_ELIG_INCLD_IN = 1
  UNION
  SELECT DISTINCT
         P.VBB_PLN_UNIQ_KEY,
         P.GRP_ID,
         CLS.CLS_ID,
         CLS.CLS_PLN_ID,
         P.VBB_PLN_ELIG_STRT_DT_SK,
         P.VBB_PLN_ELIG_END_DT_SK,
         P.VBB_PLN_STRT_YR_NO,
         P.VBB_PLN_STRT_DT_SK
  FROM """ + IDSOwner + """.P_GRP_VBB_ELIG P,
       """ + IDSOwner + """.CLS_PLN_DTL CLS
  WHERE
    P.GRP_ID = CLS.GRP_ID
    AND P.VBB_PLN_UNIQ_KEY IS NOT NULL
    AND P.GRP_ID IS NOT NULL
    AND P.CLS_ID IS NULL
    AND P.CLS_PLN_ID IS NULL
    AND P.GRP_VBB_ELIG_INCLD_IN = 1
) DRVR
LEFT OUTER JOIN """ + IDSOwner + """.CLS_PLN_DTL CLS_PLN_DTL
ON CLS_PLN_DTL.GRP_ID = DRVR.GRP_ID
AND CLS_PLN_DTL.CLS_ID = DRVR.CLS_ID
AND CLS_PLN_DTL.CLS_PLN_ID = DRVR.CLS_PLN_ID
WHERE
  CLS.GRP_ID = DRVR.GRP_ID
  AND CLS.CLS_ID = DRVR.CLS_ID
  AND CLS.CLS_PLN_ID = DRVR.CLS_PLN_ID
  AND DRVR.VBB_PLN_ELIG_STRT_DT_SK IS NOT NULL
  AND DRVR.VBB_PLN_ELIG_END_DT_SK IS NOT NULL
  AND CLS.EFF_DT_SK <= DRVR.VBB_PLN_ELIG_STRT_DT_SK
  AND CLS.TERM_DT_SK >= DRVR.VBB_PLN_ELIG_END_DT_SK
  AND SUBSTR(CLS_PLN_DTL.EFF_DT_SK, 1, 4) <= DRVR.VBB_PLN_STRT_YR_NO
  AND CLS_PLN_DTL.TERM_DT_SK >= '""" + CurrDate + """'
"""

df_case3 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_case3)
    .load()
)

extract_query_case4 = """
/*********** Case 4 **********/
SELECT DISTINCT
      DRVR.VBB_PLN_UNIQ_KEY,
      CLS.GRP_ID,
      CLS.CLS_ID,
      CLS.CLS_PLN_ID, CLS.CLS_PLN_DTL_PROD_CAT_CD_SK,
      DRVR.VBB_PLN_STRT_YR_NO,
      CLS_PLN_DTL.PLN_BEG_DT_MO_DAY,
      DRVR.VBB_PLN_ELIG_STRT_DT_SK,
      DRVR.VBB_PLN_ELIG_END_DT_SK,
      DRVR.VBB_PLN_STRT_DT_SK
FROM """ + IDSOwner + """.CLS_PLN_DTL CLS,
(
  SELECT P.VBB_PLN_UNIQ_KEY,
         P.GRP_ID,
         P.CLS_ID,
         P.CLS_PLN_ID,
         P.VBB_PLN_ELIG_STRT_DT_SK,
         P.VBB_PLN_ELIG_END_DT_SK,
         P.VBB_PLN_STRT_YR_NO,
         P.VBB_PLN_STRT_DT_SK
  FROM """ + IDSOwner + """.P_GRP_VBB_ELIG P
  WHERE
    P.VBB_PLN_UNIQ_KEY IS NOT NULL
    AND P.GRP_ID IS NOT NULL
    AND P.CLS_ID IS NOT NULL
    AND P.CLS_PLN_ID IS NOT NULL
    AND P.GRP_VBB_ELIG_INCLD_IN = 1
  UNION
  SELECT DISTINCT
         P.VBB_PLN_UNIQ_KEY,
         P.GRP_ID,
         P.CLS_ID,
         CLS.CLS_PLN_ID,
         P.VBB_PLN_ELIG_STRT_DT_SK,
         P.VBB_PLN_ELIG_END_DT_SK,
         P.VBB_PLN_STRT_YR_NO,
         P.VBB_PLN_STRT_DT_SK
  FROM """ + IDSOwner + """.P_GRP_VBB_ELIG P,
       """ + IDSOwner + """.CLS_PLN_DTL CLS
  WHERE
    P.GRP_ID = CLS.GRP_ID
    AND P.CLS_ID = CLS.CLS_ID
    AND P.VBB_PLN_UNIQ_KEY IS NOT NULL
    AND P.GRP_ID IS NOT NULL
    AND P.CLS_ID IS NOT NULL
    AND P.CLS_PLN_ID IS NULL
    AND P.GRP_VBB_ELIG_INCLD_IN = 1
  UNION
  SELECT DISTINCT
         P.VBB_PLN_UNIQ_KEY,
         P.GRP_ID,
         CLS.CLS_ID,
         P.CLS_PLN_ID,
         P.VBB_PLN_ELIG_STRT_DT_SK,
         P.VBB_PLN_ELIG_END_DT_SK,
         P.VBB_PLN_STRT_YR_NO,
         P.VBB_PLN_STRT_DT_SK
  FROM """ + IDSOwner + """.P_GRP_VBB_ELIG P,
       """ + IDSOwner + """.CLS_PLN_DTL CLS
  WHERE
    P.GRP_ID = CLS.GRP_ID
    AND P.CLS_PLN_ID = CLS.CLS_PLN_ID
    AND P.VBB_PLN_UNIQ_KEY IS NOT NULL
    AND P.GRP_ID IS NOT NULL
    AND P.CLS_ID IS NULL
    AND P.CLS_PLN_ID IS NOT NULL
    AND P.GRP_VBB_ELIG_INCLD_IN = 1
  UNION
  SELECT DISTINCT
         P.VBB_PLN_UNIQ_KEY,
         P.GRP_ID,
         CLS.CLS_ID,
         CLS.CLS_PLN_ID,
         P.VBB_PLN_ELIG_STRT_DT_SK,
         P.VBB_PLN_ELIG_END_DT_SK,
         P.VBB_PLN_STRT_YR_NO,
         P.VBB_PLN_STRT_DT_SK
  FROM """ + IDSOwner + """.P_GRP_VBB_ELIG P,
       """ + IDSOwner + """.CLS_PLN_DTL CLS
  WHERE
    P.GRP_ID = CLS.GRP_ID
    AND P.VBB_PLN_UNIQ_KEY IS NOT NULL
    AND P.GRP_ID IS NOT NULL
    AND P.CLS_ID IS NULL
    AND P.CLS_PLN_ID IS NULL
    AND P.GRP_VBB_ELIG_INCLD_IN = 1
) DRVR
LEFT OUTER JOIN """ + IDSOwner + """.CLS_PLN_DTL CLS_PLN_DTL
ON CLS_PLN_DTL.GRP_ID = DRVR.GRP_ID
AND CLS_PLN_DTL.CLS_ID = DRVR.CLS_ID
AND CLS_PLN_DTL.CLS_PLN_ID = DRVR.CLS_PLN_ID
WHERE
  CLS.GRP_ID = DRVR.GRP_ID
  AND CLS.CLS_ID = DRVR.CLS_ID
  AND CLS.CLS_PLN_ID = DRVR.CLS_PLN_ID
  AND DRVR.VBB_PLN_ELIG_STRT_DT_SK IS NULL
  AND DRVR.VBB_PLN_ELIG_END_DT_SK IS NULL
  AND DRVR.VBB_PLN_STRT_YR_NO IS NOT NULL
  AND SUBSTR(CLS.EFF_DT_SK, 1, 4) <= DRVR.VBB_PLN_STRT_YR_NO
  AND CLS.TERM_DT_SK >= '""" + CurrDate + """'
  AND SUBSTR(CLS_PLN_DTL.EFF_DT_SK, 1, 4) <= DRVR.VBB_PLN_STRT_YR_NO
  AND CLS_PLN_DTL.TERM_DT_SK >= '""" + CurrDate + """'
UNION
SELECT DISTINCT
      DRVR.VBB_PLN_UNIQ_KEY,
      CLS.GRP_ID,
      CLS.CLS_ID,
      CLS.CLS_PLN_ID, CLS.CLS_PLN_DTL_PROD_CAT_CD_SK,
      DRVR.VBB_PLN_STRT_YR_NO,
      CLS_PLN_DTL.PLN_BEG_DT_MO_DAY,
      DRVR.VBB_PLN_ELIG_STRT_DT_SK,
      DRVR.VBB_PLN_ELIG_END_DT_SK,
      DRVR.VBB_PLN_STRT_DT_SK
FROM """ + IDSOwner + """.CLS_PLN_DTL CLS,
(
  SELECT P.VBB_PLN_UNIQ_KEY,
         P.GRP_ID,
         P.CLS_ID,
         P.CLS_PLN_ID,
         P.VBB_PLN_ELIG_STRT_DT_SK,
         P.VBB_PLN_ELIG_END_DT_SK,
         P.VBB_PLN_STRT_YR_NO,
         P.VBB_PLN_STRT_DT_SK
  FROM """ + IDSOwner + """.P_GRP_VBB_ELIG P
  WHERE
    P.VBB_PLN_UNIQ_KEY IS NOT NULL
    AND P.GRP_ID IS NOT NULL
    AND P.CLS_ID IS NOT NULL
    AND P.CLS_PLN_ID IS NOT NULL
    AND P.GRP_VBB_ELIG_INCLD_IN = 1
  UNION
  SELECT DISTINCT
         P.VBB_PLN_UNIQ_KEY,
         P.GRP_ID,
         P.CLS_ID,
         CLS.CLS_PLN_ID,
         P.VBB_PLN_ELIG_STRT_DT_SK,
         P.VBB_PLN_ELIG_END_DT_SK,
         P.VBB_PLN_STRT_YR_NO,
         P.VBB_PLN_STRT_DT_SK
  FROM """ + IDSOwner + """.P_GRP_VBB_ELIG P,
       """ + IDSOwner + """.CLS_PLN_DTL CLS
  WHERE
    P.GRP_ID = CLS.GRP_ID
    AND P.CLS_ID = CLS.CLS_ID
    AND P.VBB_PLN_UNIQ_KEY IS NOT NULL
    AND P.GRP_ID IS NOT NULL
    AND P.CLS_ID IS NOT NULL
    AND P.CLS_PLN_ID IS NULL
    AND P.GRP_VBB_ELIG_INCLD_IN = 1
  UNION
  SELECT DISTINCT
         P.VBB_PLN_UNIQ_KEY,
         P.GRP_ID,
         CLS.CLS_ID,
         P.CLS_PLN_ID,
         P.VBB_PLN_ELIG_STRT_DT_SK,
         P.VBB_PLN_ELIG_END_DT_SK,
         P.VBB_PLN_STRT_YR_NO,
         P.VBB_PLN_STRT_DT_SK
  FROM """ + IDSOwner + """.P_GRP_VBB_ELIG P,
       """ + IDSOwner + """.CLS_PLN_DTL CLS
  WHERE
    P.GRP_ID = CLS.GRP_ID
    AND P.CLS_PLN_ID = CLS.CLS_PLN_ID
    AND P.VBB_PLN_UNIQ_KEY IS NOT NULL
    AND P.GRP_ID IS NOT NULL
    AND P.CLS_ID IS NULL
    AND P.CLS_PLN_ID IS NOT NULL
    AND P.GRP_VBB_ELIG_INCLD_IN = 1
  UNION
  SELECT DISTINCT
         P.VBB_PLN_UNIQ_KEY,
         P.GRP_ID,
         CLS.CLS_ID,
         CLS.CLS_PLN_ID,
         P.VBB_PLN_ELIG_STRT_DT_SK,
         P.VBB_PLN_ELIG_END_DT_SK,
         P.VBB_PLN_STRT_YR_NO,
         P.VBB_PLN_STRT_DT_SK
  FROM """ + IDSOwner + """.P_GRP_VBB_ELIG P,
       """ + IDSOwner + """.CLS_PLN_DTL CLS
  WHERE
    P.GRP_ID = CLS.GRP_ID
    AND P.VBB_PLN_UNIQ_KEY IS NOT NULL
    AND P.GRP_ID IS NOT NULL
    AND P.CLS_ID IS NULL
    AND P.CLS_PLN_ID IS NULL
    AND P.GRP_VBB_ELIG_INCLD_IN = 1
) DRVR
LEFT OUTER JOIN """ + IDSOwner + """.CLS_PLN_DTL CLS_PLN_DTL
ON CLS_PLN_DTL.GRP_ID = DRVR.GRP_ID
AND CLS_PLN_DTL.CLS_ID = DRVR.CLS_ID
AND CLS_PLN_DTL.CLS_PLN_ID = DRVR.CLS_PLN_ID
WHERE
  CLS.GRP_ID = DRVR.GRP_ID
  AND CLS.CLS_ID = DRVR.CLS_ID
  AND CLS.CLS_PLN_ID = DRVR.CLS_PLN_ID
  AND DRVR.VBB_PLN_ELIG_STRT_DT_SK IS NULL
  AND DRVR.VBB_PLN_ELIG_END_DT_SK IS NULL
  AND DRVR.VBB_PLN_STRT_YR_NO IS NULL
  AND CLS.EFF_DT_SK <= DRVR.VBB_PLN_STRT_DT_SK
  AND SUBSTR(CLS.EFF_DT_SK, 1, 4) <= DRVR.VBB_PLN_STRT_YR_NO
  AND CLS.TERM_DT_SK >= '""" + CurrDate + """'
  AND SUBSTR(CLS_PLN_DTL.EFF_DT_SK, 1, 4) <= DRVR.VBB_PLN_STRT_YR_NO
  AND CLS_PLN_DTL.TERM_DT_SK >= '""" + CurrDate + """'
"""

df_case4 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_case4)
    .load()
)

df_LinkCollector_Scenario1 = df_case1.unionByName(df_case2).unionByName(df_case3).unionByName(df_case4)

df_IncldClasses_join = df_LinkCollector_Scenario1.alias("Scenario1").join(
    df_hf_grpvbbplnelig_activeclss_cdmpng1.alias("cdmppng"),
    on=[F.col("Scenario1.CD_MPPNG_SK") == F.col("cdmppng.CD_MPPNG_SK")],
    how="left"
)
df_IncldClasses_filtered = df_IncldClasses_join.filter(F.col("cdmppng.CD_MPPNG_SK").isNotNull())

df_IncldClasses = df_IncldClasses_filtered.select(
    F.col("Scenario1.VBB_PLN_UNIQ_KEY").alias("VBB_PLN_UNIQ_KEY"),
    F.col("Scenario1.GRP_ID").alias("GRP_ID"),
    F.col("Scenario1.CLS_ID").alias("CLS_ID"),
    F.col("Scenario1.CLS_PLN_ID").alias("CLS_PLN_ID"),
    F.col("Scenario1.VBB_PLN_STRT_YR_NO").alias("VBB_PLN_STRT_YR_NO"),
    rpad(F.col("Scenario1.PLN_BEG_DT_MO_DAY"), 4, " ").alias("PLN_BEG_DT_MO_DAY"),
    rpad(F.col("Scenario1.VBB_PLN_ELIG_STRT_DT_SK"), 10, " ").alias("VBB_PLN_ELIG_STRT_DT_SK"),
    rpad(F.col("Scenario1.VBB_PLN_ELIG_END_DT_SK"), 10, " ").alias("VBB_PLN_ELIG_END_DT_SK"),
    rpad(F.col("Scenario1.VBB_PLN_STRT_DT_SK"), 10, " ").alias("VBB_PLN_STRT_DT_SK")
)

df_hf_grpvbbplnelig_validclasses_1 = dedup_sort(
    df_IncldClasses,
    ["VBB_PLN_UNIQ_KEY","GRP_ID","CLS_ID","CLS_PLN_ID"],
    []
)

extract_query_P_GRP_VBB_ELIG0 = """
SELECT VBB_PLN_UNIQ_KEY,GRP_ID,CLS_ID,CLS_PLN_ID
FROM """ + IDSOwner + """.P_GRP_VBB_ELIG
WHERE GRP_VBB_ELIG_INCLD_IN = 0
"""  # The trailing #$IDSOwner#.P_GRP_VBB_ELIG in source text is ignored as leftover

df_P_GRP_VBB_ELIG0 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_P_GRP_VBB_ELIG0)
    .load()
)

df_Transformer_101 = df_P_GRP_VBB_ELIG0.select(
    F.col("VBB_PLN_UNIQ_KEY").alias("VBB_PLN_UNIQ_KEY"),
    F.col("GRP_ID").alias("GRP_ID"),
    F.col("CLS_ID").alias("CLS_ID"),
    F.col("CLS_PLN_ID").alias("CLS_PLN_ID")
)

df_case2_3 = df_Transformer_101.select(
    F.col("VBB_PLN_UNIQ_KEY"),
    F.col("GRP_ID"),
    F.col("CLS_ID"),
    F.col("CLS_PLN_ID")
)
df_case2_1 = df_Transformer_101.select(
    F.col("VBB_PLN_UNIQ_KEY"),
    F.col("GRP_ID"),
    F.col("CLS_ID")
)
df_case2_2 = df_Transformer_101.select(
    F.col("VBB_PLN_UNIQ_KEY"),
    F.col("GRP_ID"),
    F.col("CLS_PLN_ID")
)

df_case2_3_union = df_case2_3.unionByName(df_case2_1).unionByName(df_case2_2)
df_hf_grpvbbplnelig_excldclss = dedup_sort(
    df_case2_3_union,
    ["VBB_PLN_UNIQ_KEY","GRP_ID"],  # Merging all possible PK combos
    []
)

df_Transformer_97_Scenario2 = df_LinkCollector_Scenario1.alias("Scenario2")

df_hf_grpvbbplnelig_excldclss_case_2_3 = df_hf_grpvbbplnelig_excldclss.alias("case_2_3")
df_hf_grpvbbplnelig_excldclss_case_2_1 = df_hf_grpvbbplnelig_excldclss.alias("case_2_1")
df_hf_grpvbbplnelig_excldclss_case_2_2 = df_hfvbbpln_excldclss_case2_2 = df_hf_grpvbbplnelig_excldclss.alias("case_2_2")

df_Transformer_97_join_1 = df_Transformer_97_Scenario2.join(
    df_hf_grpvbbplnelig_excldclss_case_2_3,
    on=[
        F.col("Scenario2.VBB_PLN_UNIQ_KEY") == F.col("case_2_3.VBB_PLN_UNIQ_KEY"),
        F.col("Scenario2.GRP_ID") == F.col("case_2_3.GRP_ID"),
        F.col("Scenario2.CLS_CLS_ID") == F.col("case_2_3.CLS_ID"),
        F.col("Scenario2.CLS_CLS_PLN_ID") == F.col("case_2_3.CLS_PLN_ID")
    ],
    how="left"
).alias("j1")

df_Transformer_97_join_2 = df_Transformer_97_join_1.join(
    df_hf_grpvbbplnelig_excldclss_case_2_1,
    on=[
        F.col("j1.Scenario2.VBB_PLN_UNIQ_KEY") == F.col("case_2_1.VBB_PLN_UNIQ_KEY"),
        F.col("j1.Scenario2.GRP_ID") == F.col("case_2_1.GRP_ID"),
        F.col("j1.Scenario2.CLS_CLS_ID") == F.col("case_2_1.CLS_ID")
    ],
    how="left"
).alias("j2")

df_Transformer_97_join_3 = df_Transformer_97_join_2.join(
    df_hf_grpvbbplnelig_excldclss_case_2_2,
    on=[
        F.col("j2.Scenario2.VBB_PLN_UNIQ_KEY") == F.col("case_2_2.VBB_PLN_UNIQ_KEY"),
        F.col("j2.Scenario2.GRP_ID") == F.col("case_2_2.GRP_ID"),
        F.col("j2.Scenario2.CLS_CLS_PLN_ID") == F.col("case_2_2.CLS_PLN_ID")
    ],
    how="left"
)

cond_case2_3 = F.col("case_2_3.VBB_PLN_UNIQ_KEY").isNull() 
cond_case2_1 = F.col("case_2_1.VBB_PLN_UNIQ_KEY").isNull()
cond_case2_2 = F.col("case_2_2.VBB_PLN_UNIQ_KEY").isNull()

svRecInd_expr = F.when(
    (F.col("Scenario2.P_CLS_ID").isNotNull() & F.col("Scenario2.P_CLS_PLN_ID").isNotNull()),
    F.when(cond_case2_3, F.lit("Y")).otherwise(F.lit("N"))
).otherwise(
    F.when(
        (F.col("Scenario2.P_CLS_ID").isNotNull() & F.col("Scenario2.P_CLS_PLN_ID").isNull()),
        F.when(cond_case2_1, F.lit("Y")).otherwise(F.lit("N"))
    ).otherwise(
        F.when(
            (F.col("Scenario2.P_CLS_ID").isNull() & F.col("Scenario2.P_CLS_PLN_ID").isNotNull()),
            F.when(cond_case2_2, F.lit("Y")).otherwise(F.lit("N"))
        ).otherwise(F.lit("Y"))
    )
)

df_Transformer_97_with_svRecInd = df_Transformer_97_join_3.withColumn("svRecInd", svRecInd_expr)

df_ExcldClasses_join = df_Transformer_97_with_svRecInd.alias("DSLink99").join(
    df_hf_grpvbbplnelig_activeclss_cdmpng1.alias("cdmppng1"),
    on=[F.col("DSLink99.CD_MPPNG_SK") == F.col("cdmppng1.CD_MPPNG_SK")],
    how="left"
)

df_ExcldClasses_filtered = df_ExcldClasses_join.filter(
    (F.col("DSLink99.svRecInd") == F.lit("Y")) & (F.col("cdmppng1.CD_MPPNG_SK").isNotNull())
)

df_ExcldClasses_output = df_ExcldClasses_filtered.select(
    F.col("DSLink99.VBB_PLN_UNIQ_KEY").alias("VBB_PLN_UNIQ_KEY"),
    F.col("DSLink99.GRP_ID").alias("GRP_ID"),
    F.col("DSLink99.CLS_CLS_ID").alias("CLS_ID"),
    F.col("DSLink99.CLS_CLS_PLN_ID").alias("CLS_PLN_ID"),
    F.col("DSLink99.VBB_PLN_STRT_YR_NO").alias("VBB_PLN_STRT_YR_NO"),
    rpad(F.col("DSLink99.PLN_BEG_DT_MO_DAY"), 4, " ").alias("PLN_BEG_DT_MO_DAY"),
    rpad(F.col("DSLink99.VBB_PLN_ELIG_STRT_DT_SK"), 10, " ").alias("VBB_PLN_ELIG_STRT_DT_SK"),
    rpad(F.col("DSLink99.VBB_PLN_ELIG_END_DT_SK"), 10, " ").alias("VBB_PLN_ELIG_END_DT_SK"),
    rpad(F.col("DSLink99.VBB_PLN_STRT_DT_SK"), 10, " ").alias("VBB_PLN_STRT_DT_SK")
)

df_hf_grpvbbplnelig_validclasses_0 = dedup_sort(
    df_ExcldClasses_output,
    ["VBB_PLN_UNIQ_KEY","GRP_ID","CLS_ID","CLS_PLN_ID"],
    []
)

df_Link_Collector_combined_data = df_hf_grpvbbplnelig_validclasses_0.unionByName(df_hf_grpvbbplnelig_validclasses_1)

df_hf_grpvbbplnelig_combnd_dedupe = dedup_sort(
    df_Link_Collector_combined_data,
    ["VBB_PLN_UNIQ_KEY","GRP_ID","CLS_ID","CLS_PLN_ID"],
    []
)

df_Transformer_102_join = df_hf_grpvbbplnelig_combnd_dedupe.alias("deduped_data").join(
    df_hf_grpvbbplnelig_excldrecords.alias("Exclude_recs"),
    on=[
        F.col("deduped_data.VBB_PLN_UNIQ_KEY") == F.col("Exclude_recs.VBB_PLN_UNIQ_KEY"),
        F.col("deduped_data.GRP_ID") == F.col("Exclude_recs.GRP_ID"),
        F.col("deduped_data.CLS_ID") == F.col("Exclude_recs.CLS_ID"),
        F.col("deduped_data.CLS_PLN_ID") == F.col("Exclude_recs.CLS_PLN_ID")
    ],
    how="left"
)
df_Transformer_102_filtered = df_Transformer_102_join.filter(F.col("Exclude_recs.VBB_PLN_UNIQ_KEY").isNull())

df_Transformer_102_output = df_Transformer_102_filtered.select(
    F.col("deduped_data.VBB_PLN_UNIQ_KEY").alias("VBB_PLN_UNIQ_KEY"),
    F.col("deduped_data.GRP_ID").alias("GRP_ID"),
    F.col("deduped_data.CLS_ID").alias("CLS_ID"),
    F.col("deduped_data.CLS_PLN_ID").alias("CLS_PLN_ID"),
    F.col("deduped_data.VBB_PLN_STRT_YR_NO").alias("VBB_PLN_STRT_YR_NO"),
    F.col("deduped_data.PLN_BEG_DT_MO_DAY").alias("PLN_BEG_DT_MO_DAY"),
    F.col("deduped_data.VBB_PLN_ELIG_STRT_DT_SK").alias("VBB_PLN_ELIG_STRT_DT_SK"),
    F.col("deduped_data.VBB_PLN_ELIG_END_DT_SK").alias("VBB_PLN_ELIG_END_DT_SK"),
    F.col("deduped_data.VBB_PLN_STRT_DT_SK").alias("VBB_PLN_STRT_DT_SK")
)

df_Transformer_join_CurrentRecs = df_Transformer_102_output.alias("DSLink107").join(
    df_hf_grpvbbplnelig_landingdata.alias("CurrentRecs"),
    on=[
        F.col("DSLink107.VBB_PLN_UNIQ_KEY") == F.col("CurrentRecs.HIPL_ID"),
        F.col("DSLink107.GRP_ID") == F.col("CurrentRecs.HIEL_LOG_LEVEL1")
    ],
    how="left"
)

svEndDt_expr = F.when(
    (F.col("DSLink107.VBB_PLN_STRT_YR_NO").isNull()) | (F.length(F.col("DSLink107.VBB_PLN_STRT_YR_NO").cast(StringType())) == 0),
    F.concat_ws("-", F.expr("substring(DSLink107.VBB_PLN_STRT_DT_SK,1,4)"), F.expr("substring(DSLink107.PLN_BEG_DT_MO_DAY,1,2)"), F.expr("substring(DSLink107.PLN_BEG_DT_MO_DAY,3,2) + 1"))
).otherwise(
    F.concat_ws("-", F.col("DSLink107.VBB_PLN_STRT_YR_NO").cast(StringType()), F.expr("substring(DSLink107.PLN_BEG_DT_MO_DAY,1,2)"), F.expr("substring(DSLink107.PLN_BEG_DT_MO_DAY,3,2) + 1"))
)

svStartDt_expr = F.when(
    (F.col("DSLink107.VBB_PLN_STRT_YR_NO").isNull()) | (F.length(F.col("DSLink107.VBB_PLN_STRT_YR_NO").cast(StringType())) == 0),
    F.concat_ws("-", F.expr("substring(DSLink107.VBB_PLN_STRT_DT_SK,1,4)"), F.expr("substring(DSLink107.PLN_BEG_DT_MO_DAY,1,2)"), F.expr("substring(DSLink107.PLN_BEG_DT_MO_DAY,3,2)"))
).otherwise(
    F.concat_ws("-", F.col("DSLink107.VBB_PLN_STRT_YR_NO").cast(StringType()), F.expr("substring(DSLink107.PLN_BEG_DT_MO_DAY,1,2)"), F.expr("substring(DSLink107.PLN_BEG_DT_MO_DAY,3,2)"))
)

df_Transformer_with_svCols = (
    df_Transformer_join_CurrentRecs
    .withColumn("svEndDt", svEndDt_expr)
    .withColumn("svStartDt", svStartDt_expr)
)

df_Transformer_AllCol = df_Transformer_with_svCols.filter(F.col("CurrentRecs.HIPL_ID").isNotNull()).select(
    F.col("DSLink107.VBB_PLN_UNIQ_KEY").alias("VBB_PLN_UNIQ_KEY"),
    F.col("DSLink107.GRP_ID").alias("GRP_ID"),
    F.col("DSLink107.CLS_ID").alias("CLS_ID"),
    F.col("DSLink107.CLS_PLN_ID").alias("CLS_PLN_ID"),
    F.lit("SrcSysCdSk").alias("SRC_SYS_CD_SK"),  
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(F.lit("I"),10," ").alias("INSRT_UPDT_CD"),
    rpad(F.lit("N"),1," ").alias("DISCARD_IN"),
    rpad(F.lit("Y"),1," ").alias("PASS_THRU_IN"),
    F.lit("CurrDate").alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit("SrcSysCd").alias("SRC_SYS_CD"),
    F.concat_ws(";", 
        F.col("DSLink107.VBB_PLN_UNIQ_KEY").cast(StringType()), 
        F.col("DSLink107.GRP_ID"), 
        F.col("DSLink107.CLS_ID"), 
        F.col("DSLink107.CLS_PLN_ID"), 
        F.lit("SrcSysCd")
    ).alias("PRI_KEY_STRING"),
    F.lit(0).alias("VBB_PLN_ELIG_SK"),
    F.lit("CurrRunCycle").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit("CurrRunCycle").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    rpad(
        F.when(
            F.col("DSLink107.VBB_PLN_ELIG_STRT_DT_SK").isNull(),
            F.when(
                F.col("DSLink107.VBB_PLN_UNIQ_KEY").isNull(),
                F.lit("1753-01-01")
            ).otherwise(F.col("svStartDt"))
        ).otherwise(F.col("DSLink107.VBB_PLN_ELIG_STRT_DT_SK")),
        10, " "
    ).alias("VBB_PLN_ELIG_STRT_DT"),
    rpad(
        F.when(
            F.col("DSLink107.VBB_PLN_ELIG_END_DT_SK").isNull(),
            F.when(
                F.col("DSLink107.VBB_PLN_UNIQ_KEY").isNull(),
                F.lit("2199-12-31")
            ).otherwise(
                F.expr("FIND.DATE(svEndDt, -1, 'D', 'X', 'CCYY-MM-DD')")
            )
        ).otherwise(F.col("DSLink107.VBB_PLN_ELIG_END_DT_SK")),
        10, " "
    ).alias("VBB_PLN_ELIG_END_DT")
)

df_Transformer_Transform = df_Transformer_with_svCols.filter(F.col("CurrentRecs.HIPL_ID").isNotNull()).select(
    F.col("DSLink107.VBB_PLN_UNIQ_KEY").alias("VBB_PLN_UNIQ_KEY"),
    F.col("DSLink107.GRP_ID").alias("GRP_ID"),
    F.col("DSLink107.CLS_ID").alias("CLS_ID"),
    F.col("DSLink107.CLS_PLN_ID").alias("CLS_PLN_ID"),
    F.lit("SrcSysCdSk").alias("SRC_SYS_CD_SK")
)

df_Transformer_snapshot = df_Transformer_with_svCols.select(
    F.col("DSLink107.VBB_PLN_UNIQ_KEY").alias("VBB_PLN_UNIQ_KEY"),
    F.col("DSLink107.GRP_ID").alias("GRP_ID"),
    F.col("DSLink107.CLS_ID").alias("CLS_ID"),
    F.col("DSLink107.CLS_PLN_ID").alias("CLS_PLN_ID"),
    F.lit("SrcSysCdSk").alias("SRC_SYS_CD_SK")
)

path_B_GRP_VBB_PLN_ELIG = f"{adls_path}/load/B_GRP_VBB_PLN_ELIG.dat"
write_files(
    df_Transformer_snapshot.select(
        F.col("VBB_PLN_UNIQ_KEY"),
        F.col("GRP_ID"),
        F.col("CLS_ID"),
        F.col("CLS_PLN_ID"),
        F.col("SRC_SYS_CD_SK")
    ),
    path_B_GRP_VBB_PLN_ELIG,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

params_GrpVbbPlnEligPK = {
    "CurrRunCycle": CurrRunCycle,
    "SrcSysCd": SrcSysCd,
    "IDSOwner": IDSOwner
}
df_GrpVbbPlnElig = GrpVbbPlnEligPK(df_Transformer_AllCol, df_Transformer_Transform, params_GrpVbbPlnEligPK)

df_GrpVbbPlnElig_write = df_GrpVbbPlnElig.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(F.col("INSRT_UPDT_CD"),10," "),
    rpad(F.col("DISCARD_IN"),1," "),
    rpad(F.col("PASS_THRU_IN"),1," "),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("VBB_PLN_ELIG_SK"),
    F.col("VBB_PLN_UNIQ_KEY"),
    F.col("GRP_ID"),
    F.col("CLS_ID"),
    F.col("CLS_PLN_ID"),
    F.col("SRC_SYS_CD_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    rpad(F.col("VBB_PLN_ELIG_STRT_DT"),10," "),
    rpad(F.col("VBB_PLN_ELIG_END_DT"),10," ")
)

path_GrpVbbPlnElig = f"{adls_path}/key/IhmfConstituentGrpVbbPlnEligExtr.GrpVbbPlnElig.dat.{RunId}"
write_files(
    df_GrpVbbPlnElig_write,
    path_GrpVbbPlnElig,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)