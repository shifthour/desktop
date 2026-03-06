# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Copyright 2012 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC 
# MAGIC CALLED BY : BCBSCommonClmErrMbrRecycCntl
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION :  MedTrak Common claims recycle process
# MAGIC                                
# MAGIC 
# MAGIC MODIFICATIONS :
# MAGIC 
# MAGIC Developer                                  Date                 Project/Altiris #          Change Description                                                           Development Project         Code Reviewer          Date Reviewed       
# MAGIC ------------------                            --------------------         ------------------------          -----------------------------------------------------------------------                   --------------------------------         -------------------------------   ----------------------------      
# MAGIC Jaideep Mankala                    2018-03-18              5828                          Original Programming                                                       IntegrateDev2                 Kalyan Neelam           2018-03-19
# MAGIC Kaushik Kapoor                      2018-06-06              5828                      Adding the new Member Match process for handling
# MAGIC                                                                                                              BabyBoy scenario                                                                IntegrateDev2\(9)Abhiram Dasarathy\(9)    2018-06-13

# MAGIC Full Outer Join between the Input file and IDS to retrieve all the different Member Unique Keys for a Member per Claim
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../shared_containers/PrimaryKey/PBMClaimsStep6MemMatch
# COMMAND ----------

# Retrieve parameters
EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
SrcSysCdErr = get_widget_value('SrcSysCdErr','')
RunID = get_widget_value('RunID','123456')
RunCycle = get_widget_value('RunCycle','')
RunCycleDate = get_widget_value('RunCycleDate','')
Logging = get_widget_value('Logging','')
SrcSysCd = get_widget_value('SrcSysCd','')
SrcSysCd1 = get_widget_value('SrcSysCd1','')
CurrDate = get_widget_value('CurrDate','')

# Read ErrorFile2 (CSeqFileStage)
schema_ErrorFile2 = StructType([
    StructField("CLM_ID", StringType(), True)
])
df_ErrorFile2 = (
    spark.read
    .option("header", False)
    .option("delimiter", ",")
    .option("quote", "\"")
    .schema(schema_ErrorFile2)
    .csv(f"{adls_path_publish}/external/{SrcSysCd1}_MbrMatch_RejectRecs.dat")
)

# Read GRP_ID (DB2Connector, IDS)
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
sql_grp_id = f"""
SELECT DISTINCT
      PBM_GRP_ID, 
      GRP_ID
FROM {IDSOwner}.P_PBM_GRP_XREF XREF
WHERE
      UPPER(XREF.SRC_SYS_CD) = 'MEDTRAK'
  AND '{CurrDate}' BETWEEN DATE(XREF.EFF_DT) AND DATE(XREF.TERM_DT)
"""
df_GRP_ID = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", sql_grp_id)
    .load()
)

# Scenario A hashed file replacement for hf_medtrak_medclm_preproc_grpsklkup
df_GRP_ID_dedup = dedup_sort(df_GRP_ID, ["PBM_GRP_ID"], [])

# Read IDS_CLM (DB2Connector, IDS)
sql_ids_clm = f"""
SELECT
CLM.CLM_SK
FROM {IDSOwner}.CLM AS CLM,
     {IDSOwner}.CD_MPPNG AS CD
WHERE CLM.SRC_SYS_CD_SK = CD.CD_MPPNG_SK
  AND CD.TRGT_CD = '{SrcSysCd}'
  AND CLM.GRP_SK = 0
"""
df_IDS_CLM_raw = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", sql_ids_clm)
    .load()
)

# Scenario A hashed file replacement for hf_medtrak_rx_ids_clm_recyc
df_IDS_CLM = dedup_sort(df_IDS_CLM_raw, ["CLM_SK"], [])

# Read EDW_CLM_F (DB2Connector, EDW)
jdbc_url_edw, jdbc_props_edw = get_db_config(edw_secret_name)
sql_edw_clm = f"""
SELECT
CLM_SK
FROM {EDWOwner}.CLM_F
WHERE 
SRC_SYS_CD = '{SrcSysCd}'
  AND GRP_SK = 0
"""
df_EDW_CLM_raw = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_edw)
    .options(**jdbc_props_edw)
    .option("query", sql_edw_clm)
    .load()
)

# Scenario A hashed file replacement for hf_medtrak_rx_edw_clm_recyc
df_EDW_CLM = dedup_sort(df_EDW_CLM_raw, ["CLM_SK"], [])

# Read IDS_MBR (DB2Connector, IDS)
sql_ids_mbr = f"""
SELECT DISTINCT 
      MEMBER.MBR_UNIQ_KEY,
      REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(MEMBER.FIRST_NM,'-',''),'&',''),'.',''),' ',''),'''',''),'\"',''),',',''),
      REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(MEMBER.LAST_NM,'-',''),'&',''),'.',''),' ',''),'''',''),'\"',''),',',''),
      MEMBER.SSN,
      MEMBER.BRTH_DT_SK ,
      MEMBER.GNDR_CD,
      REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(SUBSCRIBER.FIRST_NM,'-',''),'&',''),'.',''),' ',''),'''',''),'\"',''),',',''),
      REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(SUBSCRIBER.LAST_NM,'-',''),'&',''),'.',''),' ',''),'''',''),'\"',''),',',''),
      SUBSCRIBER.SSN,
      SUBSCRIBER.BRTH_DT_SK,
      MEMBER.GRP_ID,
      MEMBER.SUB_ID,
      MEMBER.MBR_SFX_NO
FROM
(
  SELECT 
      MBR.MBR_UNIQ_KEY,
      MBR.FIRST_NM,
      MBR.LAST_NM,
      MBR.SSN,
      MBR.BRTH_DT_SK ,
      CD.TRGT_CD GNDR_CD,
      MBR.SUB_SK,
      GRP.GRP_ID,
      SUB.SUB_ID, 
      MBR.MBR_SFX_NO
  FROM 
        {IDSOwner}.MBR MBR,
        {IDSOwner}.SUB SUB,
        {IDSOwner}.GRP GRP,
        {IDSOwner}.CD_MPPNG CD,
        {IDSOwner}.MBR_ENR ENR,
        {IDSOwner}.CD_MPPNG CD1,
        {IDSOwner}.P_PBM_GRP_XREF XREF
  WHERE
        MBR.MBR_SK = ENR.MBR_SK AND
        MBR.SUB_SK = SUB.SUB_SK AND
        SUB.GRP_SK = GRP.GRP_SK AND
        MBR.MBR_GNDR_CD_SK = CD.CD_MPPNG_SK AND
        ENR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK = CD1.CD_MPPNG_SK AND
        GRP.GRP_ID = XREF.GRP_ID AND 
        UPPER(XREF.SRC_SYS_CD) = 'MEDTRAK' AND
        '{CurrDate}' BETWEEN DATE(XREF.EFF_DT) AND DATE(XREF.TERM_DT) AND
        CD1.TRGT_CD IN ('MED', 'MED1') AND
        UPPER(MBR.LAST_NM) NOT LIKE '%DO NOT USE%'
) MEMBER
LEFT OUTER JOIN
(
  SELECT 
      MBR1.MBR_UNIQ_KEY,
      MBR1.FIRST_NM,
      MBR1.LAST_NM,
      MBR1.SSN,
      MBR1.BRTH_DT_SK,
      MBR1.SUB_SK
  FROM
        {IDSOwner}.MBR MBR1,
        {IDSOwner}.CD_MPPNG CD1
  WHERE
        MBR1.MBR_RELSHP_CD_SK = CD1.CD_MPPNG_SK AND
        CD1.TRGT_CD = 'SUB' AND
        UPPER(MBR1.LAST_NM) NOT LIKE '%DO NOT USE%'
) SUBSCRIBER
ON MEMBER.SUB_SK = SUBSCRIBER.SUB_SK
"""
df_IDS_MBR = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", sql_ids_mbr)
    .load()
)

# Transformer Copy_of_Transformer_6 logic:
# We create multiple output DataFrames from df_IDS_MBR (primary link is MBR).
# Columns in df_IDS_MBR: 
#  0 MBR_UNIQ_KEY
#  1 MBR_FIRST_NM
#  2 MBR_LAST_NM
#  3 MBR_SSN
#  4 BRTH_DT_SK
#  5 GNDR_CD
#  6 SUB_FIRST_NM
#  7 SUB_LAST_NM
#  8 SUB_SSN
#  9 SUB_BRTH_DT_SK
# 10 GRP_ID
# 11 SUB_ID
# 12 MBR_SFX_NO

df_Copy_of_Transformer_6 = df_IDS_MBR.withColumnRenamed("MBR_UNIQ_KEY","MBR_MBR_UNIQ_KEY") \
                                     .withColumnRenamed("MBR_FIRST_NM","MBR_MBR_FIRST_NM") \
                                     .withColumnRenamed("MBR_LAST_NM","MBR_MBR_LAST_NM") \
                                     .withColumnRenamed("SSN","MBR_SSN") \
                                     .withColumnRenamed("BRTH_DT_SK","MBR_BRTH_DT_SK") \
                                     .withColumnRenamed("SUB_FIRST_NM","SUB_FIRST_NM") \
                                     .withColumnRenamed("SUB_LAST_NM","SUB_LAST_NM") \
                                     .withColumnRenamed("SUB_SSN","SUB_SSN") \
                                     .withColumnRenamed("SUB_BRTH_DT_SK","SUB_BRTH_DT_SK") \
                                     .withColumnRenamed("GRP_ID","MBR_GRP_ID") \
                                     .withColumnRenamed("SUB_ID","MBR_SUB_ID") \
                                     .withColumnRenamed("MBR_SFX_NO","MBR_MBR_SFX_NO") \
                                     .withColumnRenamed("GNDR_CD","MBR_GNDR_CD")

# Output link Step1
df_step1 = df_Copy_of_Transformer_6.select(
    F.col("MBR_SSN").alias("MBR_SSN"),
    F.col("SUB_SSN").alias("SUB_SSN"),
    F.col("MBR_BRTH_DT_SK").alias("MBR_BRTH_DT_SK"),
    F.expr("UpCase(trim(MBR_MBR_LAST_NM))[1,4]").alias("MBR_LAST_NM"),
    F.col("MBR_MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.expr("UpCase(trim(MBR_MBR_FIRST_NM))").alias("MBR_FIRST_NM"),
    F.col("MBR_GNDR_CD").alias("GNDR_CD"),
    F.expr("UpCase(trim(SUB_FIRST_NM))").alias("SUB_FIRST_NM"),
    F.expr("UpCase(trim(SUB_LAST_NM))").alias("SUB_LAST_NM"),
    F.col("SUB_BRTH_DT_SK").alias("SUB_BRTH_DT_SK"),
    F.col("MBR_GRP_ID").alias("GRP_ID"),
    F.col("MBR_SUB_ID").alias("SUB_ID"),
    F.col("MBR_MBR_SFX_NO").alias("MBR_SFX_NO")
)

# Output link Step2
df_step2 = df_Copy_of_Transformer_6.select(
    F.col("MBR_SSN").alias("MBR_SSN"),
    F.col("SUB_SSN").alias("SUB_SSN"),
    F.col("MBR_BRTH_DT_SK").alias("MBR_BRTH_DT_SK"),
    F.col("MBR_MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.expr("UpCase(trim(MBR_MBR_FIRST_NM))").alias("MBR_FIRST_NM"),
    F.expr("UpCase(trim(MBR_MBR_LAST_NM))").alias("MBR_LAST_NM"),
    F.col("MBR_GNDR_CD").alias("GNDR_CD"),
    F.expr("UpCase(trim(SUB_FIRST_NM))").alias("SUB_FIRST_NM"),
    F.expr("UpCase(trim(SUB_LAST_NM))").alias("SUB_LAST_NM"),
    F.col("SUB_BRTH_DT_SK").alias("SUB_BRTH_DT_SK"),
    F.col("MBR_GRP_ID").alias("GRP_ID"),
    F.col("MBR_SUB_ID").alias("SUB_ID"),
    F.col("MBR_MBR_SFX_NO").alias("MBR_SFX_NO")
)

# Output link Step3
df_step3 = df_Copy_of_Transformer_6.select(
    F.expr("UpCase(trim(MBR_MBR_FIRST_NM))[1,4]").alias("MBR_FIRST_NM"),
    F.expr("UpCase(trim(MBR_MBR_LAST_NM))[1,4]").alias("MBR_LAST_NM"),
    F.col("MBR_BRTH_DT_SK").alias("MBR_BRTH_DT_SK"),
    F.col("SUB_SSN").alias("SUB_SSN"),
    F.col("MBR_GNDR_CD").alias("GNDR_CD"),
    F.col("MBR_SSN").alias("MBR_SSN"),
    F.col("MBR_MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.expr("UpCase(trim(SUB_FIRST_NM))").alias("SUB_FIRST_NM"),
    F.expr("UpCase(trim(SUB_LAST_NM))").alias("SUB_LAST_NM"),
    F.col("SUB_BRTH_DT_SK").alias("SUB_BRTH_DT_SK"),
    F.col("MBR_GRP_ID").alias("GRP_ID"),
    F.col("MBR_SUB_ID").alias("SUB_ID"),
    F.col("MBR_MBR_SFX_NO").alias("MBR_SFX_NO")
)

# Output link Step4
df_step4 = df_Copy_of_Transformer_6.select(
    F.expr("UpCase(trim(MBR_MBR_FIRST_NM))[1,4]").alias("MBR_FIRST_NM"),
    F.expr("UpCase(trim(MBR_MBR_LAST_NM))[1,4]").alias("MBR_LAST_NM"),
    F.expr("UpCase(trim(SUB_FIRST_NM))[1,4]").alias("SUB_FIRST_NM"),
    F.expr("UpCase(trim(SUB_LAST_NM))[1,4]").alias("SUB_LAST_NM"),
    F.col("MBR_BRTH_DT_SK").alias("MBR_BRTH_DT_SK"),
    F.col("MBR_GNDR_CD").alias("GNDR_CD"),
    F.col("MBR_SSN").alias("MBR_SSN"),
    F.col("SUB_SSN").alias("SUB_SSN"),
    F.col("MBR_MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("SUB_BRTH_DT_SK").alias("SUB_BRTH_DT_SK"),
    F.col("MBR_GRP_ID").alias("GRP_ID"),
    F.col("MBR_SUB_ID").alias("SUB_ID"),
    F.col("MBR_MBR_SFX_NO").alias("MBR_SFX_NO")
)

# Output link Step5
df_step5 = df_Copy_of_Transformer_6.filter(
    F.col("MBR_SSN") != F.col("SUB_SSN")
).select(
    F.col("MBR_SSN").alias("MBR_SSN"),
    F.col("SUB_SSN").alias("SUB_SSN"),
    F.col("MBR_BRTH_DT_SK").alias("MBR_BRTH_DT_SK"),
    F.col("MBR_MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.expr("UpCase(trim(MBR_MBR_FIRST_NM))").alias("MBR_FIRST_NM"),
    F.expr("UpCase(trim(MBR_MBR_LAST_NM))").alias("MBR_LAST_NM"),
    F.col("MBR_GNDR_CD").alias("GNDR_CD"),
    F.expr("UpCase(trim(SUB_FIRST_NM))").alias("SUB_FIRST_NM"),
    F.expr("UpCase(trim(SUB_LAST_NM))").alias("SUB_LAST_NM"),
    F.col("SUB_BRTH_DT_SK").alias("SUB_BRTH_DT_SK"),
    F.col("MBR_GRP_ID").alias("GRP_ID"),
    F.col("MBR_SUB_ID").alias("SUB_ID"),
    F.col("MBR_MBR_SFX_NO").alias("MBR_SFX_NO")
)

# Output link Step6
df_step6 = df_Copy_of_Transformer_6.select(
    F.col("SUB_SSN").alias("SUB_SSN"),
    F.col("MBR_GRP_ID").alias("GRP_ID"),
    F.col("MBR_BRTH_DT_SK").alias("MBR_BRTH_DT_SK"),
    F.col("MBR_GNDR_CD").alias("GNDR_CD"),
    F.expr("UpCase(trim(MBR_MBR_FIRST_NM))").alias("MBR_FIRST_NM"),
    F.expr("UpCase(trim(MBR_MBR_LAST_NM))").alias("MBR_LAST_NM"),
    F.expr("UpCase(trim(SUB_FIRST_NM))").alias("SUB_FIRST_NM"),
    F.expr("UpCase(trim(SUB_LAST_NM))").alias("SUB_LAST_NM"),
    F.col("MBR_MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("MBR_SSN").alias("MBR_SSN"),
    F.col("SUB_BRTH_DT_SK").alias("SUB_BRTH_DT_SK"),
    F.col("MBR_SUB_ID").alias("SUB_ID"),
    F.col("MBR_MBR_SFX_NO").alias("MBR_SFX_NO"),
    F.lit("NA").alias("PBM_GRP_ID")
)

# Shared Container PBMClaimsStep6MemMatch
params_PBMClaimsStep6MemMatch = {}
df_step6_lkup = PBMClaimsStep6MemMatch(df_step6, params_PBMClaimsStep6MemMatch)

# Scenario A hashed file replacement for each StepX output:
df_step1_lkup = dedup_sort(
    df_step1,
    ["MBR_SSN","SUB_SSN","MBR_BRTH_DT_SK","MBR_LAST_NM"],
    []
)
df_step2_lkup = dedup_sort(
    df_step2,
    ["MBR_SSN","SUB_SSN","MBR_BRTH_DT_SK"],
    []
)
df_step3_lkup = dedup_sort(
    df_step3,
    ["MBR_FIRST_NM","MBR_LAST_NM","MBR_BRTH_DT_SK","SUB_SSN","GNDR_CD","MBR_SSN"],
    []
)
df_step4_lkup = dedup_sort(
    df_step4,
    ["MBR_FIRST_NM","MBR_LAST_NM","SUB_FIRST_NM","SUB_LAST_NM","MBR_BRTH_DT_SK","GNDR_CD"],
    []
)
df_step5_lkup = dedup_sort(
    df_step5,
    ["MBR_SSN","SUB_SSN"],
    []
)
# df_step6_lkup is already from the container. We deduplicate it before next usage:
df_step6_lkup_dedup = dedup_sort(
    df_step6_lkup,
    ["SUB_SSN","MBR_BRTH_DT_SK","GNDR_CD","MBR_UNIQ_KEY"],
    []
)

# Read IDS_P_CLM_ERR (DB2Connector, IDS)
sql_ids_p_clm_err = f"""
SELECT
CLM_SK,
CLM_ID,
SRC_SYS_CD,
CLM_TYP_CD,
CLM_SUBTYP_CD,
CLM_SVC_STRT_DT_SK,
SRC_SYS_GRP_PFX,
SRC_SYS_GRP_ID,
SRC_SYS_GRP_SFX,
SUB_SSN,
PATN_LAST_NM,
PATN_FIRST_NM,
PATN_GNDR_CD,
PATN_BRTH_DT_SK,
ERR_CD,
ERR_DESC,
FEP_MBR_ID,
SUB_FIRST_NM,
SUB_LAST_NM,
SRC_SYS_SUB_ID,
SRC_SYS_MBR_SFX_NO,
GRP_ID,
FILE_DT_SK,
PATN_SSN
FROM {IDSOwner}.P_CLM_MBRSH_ERR_RECYC
WHERE SRC_SYS_CD = '{SrcSysCdErr}'
"""
df_IDS_P_CLM_ERR = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", sql_ids_p_clm_err)
    .load()
)

# Scenario A hashed file replacement for hf_medtrak_ids_p_clm_err
df_hf_medtrak_ids_p_clm_err = dedup_sort(df_IDS_P_CLM_ERR, ["CLM_SK"], [])

# Transformer Trn_Mbr_Mtch
df_Trn_Mbr_Mtch_in = df_hf_medtrak_ids_p_clm_err

# Output link Next => columns
df_Trn_Mbr_Mtch_Next = df_Trn_Mbr_Mtch_in.select(
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("SUB_SSN").alias("SUB_SSN"),
    F.col("PATN_LAST_NM").alias("PATN_LAST_NM"),
    F.col("PATN_FIRST_NM").alias("PATN_FIRST_NM"),
    F.col("SUB_FIRST_NM").alias("SUB_FIRST_NM"),
    F.col("SUB_LAST_NM").alias("SUB_LAST_NM"),
    F.col("PATN_BRTH_DT_SK").alias("PATN_DOB"),
    F.expr("trim(PATN_GNDR_CD)").alias("PATN_SEX"),
    F.col("CLM_SVC_STRT_DT_SK").alias("FILL_DT_SK"),
    F.col("PATN_SSN").alias("PATN_SSN")
)

# Output link Lnk_Err_Clm_To_Hf => columns
df_Trn_Mbr_Mtch_ErrClmToHf = df_Trn_Mbr_Mtch_in.select(
    F.col("CLM_SK").alias("CLM_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("CLM_TYP_CD").alias("CLM_TYP_CD"),
    F.col("CLM_SUBTYP_CD").alias("CLM_SUBTYP_CD"),
    F.col("CLM_SVC_STRT_DT_SK").alias("CLM_SVC_STRT_DT_SK"),
    F.col("SRC_SYS_GRP_PFX").alias("SRC_SYS_GRP_PFX"),
    F.col("SRC_SYS_GRP_ID").alias("SRC_SYS_GRP_ID"),
    F.col("SRC_SYS_GRP_SFX").alias("SRC_SYS_GRP_SFX"),
    F.col("SUB_SSN").alias("SUB_SSN"),
    F.col("PATN_LAST_NM").alias("PATN_LAST_NM"),
    F.col("PATN_FIRST_NM").alias("PATN_FIRST_NM"),
    F.col("PATN_GNDR_CD").alias("PATN_GNDR_CD"),
    F.col("PATN_BRTH_DT_SK").alias("PATN_BRTH_DT_SK"),
    F.col("ERR_CD").alias("ERR_CD"),
    F.col("ERR_DESC").alias("ERR_DESC"),
    F.col("FEP_MBR_ID").alias("FEP_MBR_ID"),
    F.col("SUB_FIRST_NM").alias("SUB_FIRST_NM"),
    F.col("SUB_LAST_NM").alias("SUB_LAST_NM"),
    F.col("SRC_SYS_SUB_ID").alias("SRC_SYS_SUB_ID"),
    F.col("SRC_SYS_MBR_SFX_NO").alias("SRC_SYS_MBR_SFX_NO"),
    F.col("GRP_ID").alias("GRP_ID"),
    F.col("FILE_DT_SK").alias("FILE_DT_SK"),
    F.col("PATN_SSN").alias("PATN_SSN")
)

# Scenario A hashed file replacement for hf_medtrak_errclm_mbrmatch_land
df_hf_medtrak_errclm_mbrmatch_land = dedup_sort(df_Trn_Mbr_Mtch_ErrClmToHf, ["CLM_SK"], [])

# Convert Transformer
df_Convert_in = df_Trn_Mbr_Mtch_Next
# Stage Variables
df_Convert = df_Convert_in.withColumn("svPatnFrstNm", F.expr("Trim(Convert(char(45) : char(38) : char(46) : char(32) : char(39) : char(34) : char(44), '       ', PATN_FIRST_NM), ' ', 'A')")) \
                          .withColumn("svPatnLastNm", F.expr("Trim(Convert(char(45) : char(38) : char(46) : char(32) : char(39) : char(34) : char(44), '       ', PATN_LAST_NM), ' ', 'A')")) \
                          .withColumn("svSubFrstNm", F.expr("Trim(Convert(char(45) : char(38) : char(46) : char(32) : char(39) : char(34) : char(44), '       ', SUB_FIRST_NM), ' ', 'A')")) \
                          .withColumn("svSubLastNm", F.expr("Trim(Convert(char(45) : char(38) : char(46) : char(32) : char(39) : char(34) : char(44), '       ', SUB_LAST_NM), ' ', 'A')"))

df_Convert_out_Medtrak = df_Convert.select(
    F.expr("UpCase(trim(svPatnFrstNm))").alias("PATN_FIRST_NM"),
    F.expr("UpCase(trim(svPatnLastNm))").alias("PATN_LAST_NM"),
    F.expr("trim(PATN_DOB)").alias("DOB"),
    F.expr("trim(PATN_SEX)").alias("SEX_CD"),
    F.expr("UpCase(trim(svSubFrstNm))").alias("CARDHLDR_FIRST_NM"),
    F.expr("UpCase(trim(svSubLastNm))").alias("CARDHLDR_LAST_NM"),
    F.expr("trim(PATN_SSN)").alias("PATN_SSN"),
    F.expr("trim(SUB_SSN)").alias("CARDHLDR_SSN"),
    F.expr("trim(CLM_ID)").alias("CLM_ID"),
    F.col("FILL_DT_SK").alias("FILL_DT_SK")
)

# Scenario A hashed file replacement for "Convert => Medtrak => MemberMatch"
df_MemberMatch_in = df_Convert_out_Medtrak

# Left join lookups: Step1_lkup, Step2_lkup, Step3_lkup, Step4_lkup, Step5_lkup, Step6_lkup
# We'll sequentially join them with left joins:
df_mm_join1 = df_MemberMatch_in.alias("Medtrak").join(
    df_step1_lkup.alias("Step1_lkup"),
    on=[ F.col("Medtrak.PATN_SSN") == F.col("Step1_lkup.MBR_SSN"),
         F.col("Medtrak.CARDHLDR_SSN") == F.col("Step1_lkup.SUB_SSN"),
         F.col("Medtrak.DOB") == F.col("Step1_lkup.MBR_BRTH_DT_SK"),
         F.expr("Medtrak.PATN_LAST_NM[1,4]") == F.col("Step1_lkup.MBR_LAST_NM") ],
    how="left"
)
df_mm_join2 = df_mm_join1.alias("Medtrak").join(
    df_step2_lkup.alias("Step2_lkup"),
    on=[ F.col("Medtrak.Medtrak.PATN_SSN") == F.col("Step2_lkup.MBR_SSN"),
         F.col("Medtrak.Medtrak.CARDHLDR_SSN") == F.col("Step2_lkup.SUB_SSN"),
         F.col("Medtrak.Medtrak.DOB") == F.col("Step2_lkup.MBR_BRTH_DT_SK") ],
    how="left"
)
df_mm_join3 = df_mm_join2.alias("Medtrak").join(
    df_step3_lkup.alias("Step3_lkup"),
    on=[ F.expr("Medtrak.Medtrak.PATN_FIRST_NM[1,4]") == F.col("Step3_lkup.MBR_FIRST_NM"),
         F.expr("Medtrak.Medtrak.PATN_LAST_NM[1,4]") == F.col("Step3_lkup.MBR_LAST_NM"),
         F.col("Medtrak.Medtrak.DOB") == F.col("Step3_lkup.MBR_BRTH_DT_SK"),
         F.col("Medtrak.Medtrak.CARDHLDR_SSN") == F.col("Step3_lkup.SUB_SSN"),
         F.col("Medtrak.Medtrak.SEX_CD") == F.col("Step3_lkup.GNDR_CD"),
         F.col("Medtrak.Medtrak.PATN_SSN") == F.col("Step3_lkup.MBR_SSN") ],
    how="left"
)
df_mm_join4 = df_mm_join3.alias("Medtrak").join(
    df_step4_lkup.alias("Step4_lkup"),
    on=[ F.expr("Medtrak.Medtrak.PATN_FIRST_NM[1,4]") == F.col("Step4_lkup.MBR_FIRST_NM"),
         F.expr("Medtrak.Medtrak.PATN_LAST_NM[1,4]") == F.col("Step4_lkup.MBR_LAST_NM"),
         F.expr("Medtrak.Medtrak.CARDHLDR_FIRST_NM[1,4]") == F.col("Step4_lkup.SUB_FIRST_NM"),
         F.expr("Medtrak.Medtrak.CARDHLDR_LAST_NM[1,4]") == F.col("Step4_lkup.SUB_LAST_NM"),
         F.col("Medtrak.Medtrak.DOB") == F.col("Step4_lkup.MBR_BRTH_DT_SK"),
         F.col("Medtrak.Medtrak.SEX_CD") == F.col("Step4_lkup.GNDR_CD") ],
    how="left"
)
df_mm_join5 = df_mm_join4.alias("Medtrak").join(
    df_step5_lkup.alias("Step5_lkup"),
    on=[ F.col("Medtrak.Medtrak.PATN_SSN") == F.col("Step5_lkup.MBR_SSN"),
         F.col("Medtrak.Medtrak.CARDHLDR_SSN") == F.col("Step5_lkup.SUB_SSN") ],
    how="left"
)
df_mm_join6 = df_mm_join5.alias("Medtrak").join(
    df_step6_lkup_dedup.alias("Step6_lkup"),
    on=[ F.expr("Trim(Medtrak.Medtrak.CARDHLDR_SSN)") == F.expr("Trim(Step6_lkup.SUB_SSN)"),
         F.expr("Trim(Medtrak.Medtrak.DOB)") == F.col("Step6_lkup.MBR_BRTH_DT_SK"),
         F.expr("Upcase(Trim(Medtrak.Medtrak.SEX_CD))") == F.col("Step6_lkup.GNDR_CD") ],
    how="left"
)

# Implement StageVariables logic in MemberMatch
df_mm_stagevar = df_mm_join6.withColumn(
    "svMbrUniqKey",
    F.expr("""
CASE WHEN Step1_lkup.MBR_UNIQ_KEY IS NOT NULL THEN Step1_lkup.MBR_UNIQ_KEY
     WHEN Step2_lkup.MBR_UNIQ_KEY IS NOT NULL THEN Step2_lkup.MBR_UNIQ_KEY
     WHEN Step3_lkup.MBR_UNIQ_KEY IS NOT NULL THEN Step3_lkup.MBR_UNIQ_KEY
     WHEN Step4_lkup.MBR_UNIQ_KEY IS NOT NULL THEN Step4_lkup.MBR_UNIQ_KEY
     WHEN Step5_lkup.MBR_UNIQ_KEY IS NOT NULL THEN Step5_lkup.MBR_UNIQ_KEY
     WHEN Step6_lkup.MBR_UNIQ_KEY IS NOT NULL AND Step6_lkup.CNT = 1 THEN Step6_lkup.MBR_UNIQ_KEY
     ELSE ''
END
""")
).withColumn(
    "svSubId",
    F.expr("""
CASE WHEN Step1_lkup.SUB_ID IS NOT NULL THEN Step1_lkup.SUB_ID
     WHEN Step2_lkup.SUB_ID IS NOT NULL THEN Step2_lkup.SUB_ID
     WHEN Step3_lkup.SUB_ID IS NOT NULL THEN Step3_lkup.SUB_ID
     WHEN Step4_lkup.SUB_ID IS NOT NULL THEN Step4_lkup.SUB_ID
     WHEN Step5_lkup.SUB_ID IS NOT NULL THEN Step5_lkup.SUB_ID
     WHEN Step6_lkup.SUB_ID IS NOT NULL THEN Step6_lkup.SUB_ID
     ELSE ''
END
""")
).withColumn(
    "svGrpId",
    F.expr("""
CASE WHEN Step1_lkup.GRP_ID IS NOT NULL THEN Step1_lkup.GRP_ID
     WHEN Step2_lkup.GRP_ID IS NOT NULL THEN Step2_lkup.GRP_ID
     WHEN Step3_lkup.GRP_ID IS NOT NULL THEN Step3_lkup.GRP_ID
     WHEN Step4_lkup.GRP_ID IS NOT NULL THEN Step4_lkup.GRP_ID
     WHEN Step5_lkup.GRP_ID IS NOT NULL THEN Step5_lkup.GRP_ID
     WHEN Step6_lkup.GRP_ID IS NOT NULL THEN Step6_lkup.GRP_ID
     ELSE ''
END
""")
).withColumn(
    "svMbrSfxNo",
    F.expr("""
CASE WHEN Step1_lkup.MBR_SFX_NO IS NOT NULL THEN Step1_lkup.MBR_SFX_NO
     WHEN Step2_lkup.MBR_SFX_NO IS NOT NULL THEN Step2_lkup.MBR_SFX_NO
     WHEN Step3_lkup.MBR_SFX_NO IS NOT NULL THEN Step3_lkup.MBR_SFX_NO
     WHEN Step4_lkup.MBR_SFX_NO IS NOT NULL THEN Step4_lkup.MBR_SFX_NO
     WHEN Step5_lkup.MBR_SFX_NO IS NOT NULL THEN Step5_lkup.MBR_SFX_NO
     WHEN Step6_lkup.MBR_SFX_NO IS NOT NULL THEN Step6_lkup.MBR_SFX_NO
     ELSE ''
END
""")
)

df_mm_land = df_mm_stagevar.filter(F.expr("svMbrUniqKey <> ''")).select(
    F.col("Medtrak.CLM_ID").alias("CLM_ID"),
    F.col("Medtrak.FILL_DT_SK").alias("FILL_DT_SK"),
    F.expr("CASE WHEN (svMbrUniqKey = 'UNK' or trim(svMbrUniqKey) = '') THEN 0 ELSE svMbrUniqKey END").alias("MBR_UNIQ_KEY")
)

df_mm_reject = df_mm_stagevar.filter(F.expr("svMbrUniqKey = ''")).select(
    F.col("Medtrak.CLM_ID").alias("CLM_ID")
)

# Load_ErrorFile2 (CSeqFileStage) => append to #SrcSysCd1#_MbrMatch_RejectRecs.dat
write_files(
    df_mm_reject,
    f"{adls_path_publish}/external/{SrcSysCd1}_MbrMatch_RejectRecs.dat",
    delimiter=",",
    mode="append",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# Scenario A hashed file replacement for hf_medtrak_mbrmatch_drugenrmatch_dedupe
df_hf_medtrak_mbrmatch_drugenrmatch_dedupe = dedup_sort(df_mm_land, ["CLM_ID","FILL_DT_SK","MBR_UNIQ_KEY"], [])

# Output to W_DRUG_ENR_MATCH (DB2Connector, IDS) with "Truncate" logic
df_W_DRUG_ENR_MATCH = df_hf_medtrak_mbrmatch_drugenrmatch_dedupe
# The stage calls: CALL #$IDSOwner#.BCSP_TRUNCATE('#$IDSOwner#', 'W_DRUG_ENR_MATCH')
truncate_sql = f"CALL {IDSOwner}.BCSP_TRUNCATE('{IDSOwner}', 'W_DRUG_ENR_MATCH')"
execute_dml(truncate_sql, jdbc_url_ids, jdbc_props_ids)

# Now insert records
# We'll do a staging table approach for consistency
staging_table_w_drug = f"STAGING.MedtrakPClmMbrErrExtr_W_DRUG_ENR_MATCH_temp"
drop_staging_w_drug = f"DROP TABLE IF EXISTS {staging_table_w_drug}"
execute_dml(drop_staging_w_drug, jdbc_url_ids, jdbc_props_ids)

df_W_DRUG_ENR_MATCH.createOrReplaceTempView("temp_W_DRUG_ENR_MATCH_unused")  # We won't write it physically this way
# Instead, we will just write to the staging table via JDBC:
# Create staging:
create_staging_sql = f"CREATE TABLE {staging_table_w_drug} (CLM_ID VARCHAR(255), FILL_DT_SK CHAR(10), MBR_UNIQ_KEY INT)"
execute_dml(create_staging_sql, jdbc_url_ids, jdbc_props_ids)

# Write DF to staging
(
    df_W_DRUG_ENR_MATCH
    .write
    .format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("dbtable", staging_table_w_drug)
    .mode("append")
    .save()
)

# Insert into final
insert_sql_w_drug = f"""
INSERT INTO {IDSOwner}.W_DRUG_ENR_MATCH (CLM_ID,FILL_DT_SK,MBR_UNIQ_KEY)
SELECT CLM_ID,FILL_DT_SK,MBR_UNIQ_KEY FROM {staging_table_w_drug}
"""
execute_dml(insert_sql_w_drug, jdbc_url_ids, jdbc_props_ids)

# Read the same table's result => "W_DRUG_ENR_MATCH" next pin is a SELECT:
# The stage had an output pin that runs a query
sql_w_drug_enr_match_out = f"""
SELECT 
       CLM_ID,
       MBR_UNIQ_KEY,
       GRP_ID,
       SUB_ID, 
       MBR_SFX_NO,
       SUB_UNIQ_KEY,
       EFF_DT_SK
FROM 
(
{"""
SELECT 
      DRUG.CLM_ID,
      ENR.MBR_UNIQ_KEY,
      MAX(ENR.TERM_DT_SK) TERM_DT_SK,
      MAX(ENR.EFF_DT_SK) EFF_DT_SK,
      GRP.GRP_ID,
      SUB.SUB_ID, 
      MBR.MBR_SFX_NO,
      SUB.SUB_UNIQ_KEY,
      1 as Order
FROM
    """ + f"""{IDSOwner}.MBR_ENR ENR,
    {IDSOwner}.CD_MPPNG CD1,
    {IDSOwner}.W_DRUG_ENR_MATCH DRUG,
    {IDSOwner}.PROD PROD,
    {IDSOwner}.MBR MBR,
    {IDSOwner}.SUB SUB,
    {IDSOwner}.GRP GRP
WHERE
     MBR.MBR_SK = ENR.MBR_SK AND
     MBR.SUB_SK = SUB.SUB_SK AND
     SUB.GRP_SK = GRP.GRP_SK AND
     ENR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK = CD1.CD_MPPNG_SK AND
     CD1.TRGT_CD IN ('MED', 'DNTL') AND
     ENR.ELIG_IN = 'Y' AND
     ENR.MBR_UNIQ_KEY = DRUG.MBR_UNIQ_KEY AND
     ENR.EFF_DT_SK <= DRUG.FILL_DT_SK AND
     ENR.TERM_DT_SK >= DRUG.FILL_DT_SK AND
     ENR.PROD_SK = PROD.PROD_SK
GROUP BY
     DRUG.CLM_ID,
     ENR.MBR_UNIQ_KEY,
     GRP.GRP_ID,
     SUB.SUB_ID, 
     MBR.MBR_SFX_NO,
     SUB.SUB_UNIQ_KEY
UNION
SELECT 
      DRUG.CLM_ID,
      ENR.MBR_UNIQ_KEY,
      MAX(ENR.TERM_DT_SK) TERM_DT_SK,
      MAX(ENR.EFF_DT_SK) EFF_DT_SK,
      GRP.GRP_ID,
      SUB.SUB_ID, 
      MBR.MBR_SFX_NO,
      SUB.SUB_UNIQ_KEY,
      2 as Order
FROM
    {IDSOwner}.MBR_ENR ENR,
    {IDSOwner}.CD_MPPNG CD1,
    {IDSOwner}.W_DRUG_ENR_MATCH DRUG,
    {IDSOwner}.PROD PROD,
    {IDSOwner}.MBR MBR,
    {IDSOwner}.SUB SUB,
    {IDSOwner}.GRP GRP
WHERE
     MBR.MBR_SK = ENR.MBR_SK AND
     MBR.SUB_SK = SUB.SUB_SK AND
     SUB.GRP_SK = GRP.GRP_SK AND
     ENR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK = CD1.CD_MPPNG_SK AND
     CD1.TRGT_CD IN ('MED', 'DNTL') AND
     ENR.ELIG_IN = 'Y' AND
     ENR.MBR_UNIQ_KEY = DRUG.MBR_UNIQ_KEY AND
     ENR.PROD_SK = PROD.PROD_SK
GROUP BY
     DRUG.CLM_ID,
     ENR.MBR_UNIQ_KEY,
     GRP.GRP_ID,
     SUB.SUB_ID, 
     MBR.MBR_SFX_NO,
     SUB.SUB_UNIQ_KEY
UNION
SELECT 
      DRUG.CLM_ID,
      ENR.MBR_UNIQ_KEY,
      MAX(ENR.TERM_DT_SK) TERM_DT_SK,
      MAX(ENR.EFF_DT_SK) EFF_DT_SK,
      GRP.GRP_ID,
      SUB.SUB_ID, 
      MBR.MBR_SFX_NO,
      SUB.SUB_UNIQ_KEY,
      3 as Order
FROM
    {IDSOwner}.MBR_ENR ENR,
    {IDSOwner}.CD_MPPNG CD1,
    {IDSOwner}.W_DRUG_ENR_MATCH DRUG,
    {IDSOwner}.PROD PROD,
    {IDSOwner}.MBR MBR,
    {IDSOwner}.SUB SUB,
    {IDSOwner}.GRP GRP
WHERE
     MBR.MBR_SK = ENR.MBR_SK AND
     MBR.SUB_SK = SUB.SUB_SK AND
     SUB.GRP_SK = GRP.GRP_SK AND
     ENR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK = CD1.CD_MPPNG_SK AND
     CD1.TRGT_CD IN ('MED', 'DNTL') AND
     ENR.ELIG_IN = 'N' AND
     ENR.MBR_UNIQ_KEY = DRUG.MBR_UNIQ_KEY AND
     ENR.PROD_SK = PROD.PROD_SK
GROUP BY
     DRUG.CLM_ID,
     ENR.MBR_UNIQ_KEY,
     GRP.GRP_ID,
     SUB.SUB_ID, 
     MBR.MBR_SFX_NO,
     SUB.SUB_UNIQ_KEY
""" + f""") T
ORDER BY
CLM_ID,
Order desc,
TERM_DT_SK,
EFF_DT_SK,
MBR_UNIQ_KEY
"""

df_W_DRUG_ENR_MATCH_out = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", sql_w_drug_enr_match_out)
    .load()
)

# Scenario A hashed file replacement for hf_bcbssc_errclm_mbrmatch_mbrlkup
df_hf_bcbssc_errclm_mbrmatch_mbrlkup = dedup_sort(df_W_DRUG_ENR_MATCH_out, ["CLM_ID"], [])

# Trn_MbrMtch_Lkup transformer
# Primary link => hf_medtrak_errclm_mbrmatch_land
df_trn_mbrmtch_lkup_in = df_hf_medtrak_errclm_mbrmatch_land

# Then 4 lookups => Lnk_Mbr_Lkup, GrpBase_lkup, IDS_CLM, EDW_CLM

# We create dataframes for each lookup join:

df_Lnk_Mbr_Lkup = df_hf_bcbssc_errclm_mbrmatch_mbrlkup.alias("Lnk_Mbr_Lkup")
df_GrpBase_lkup = df_GRP_ID_dedup.alias("GrpBase_lkup")
df_IDS_CLM_lkup = df_IDS_CLM.alias("IDS_CLM")
df_EDW_CLM_lkup = df_EDW_CLM.alias("EDW_CLM")

joined_lkup_1 = df_trn_mbrmtch_lkup_in.alias("Lnk_ErrClm")\
    .join(df_Lnk_Mbr_Lkup, F.col("Lnk_ErrClm.CLM_ID") == F.col("Lnk_Mbr_Lkup.CLM_ID"), "left")\
    .join(df_GrpBase_lkup, F.col("Lnk_ErrClm.SRC_SYS_GRP_ID") == F.col("GrpBase_lkup.PBM_GRP_ID"), "left")\
    .join(df_IDS_CLM_lkup, F.col("Lnk_ErrClm.CLM_SK") == F.col("IDS_CLM.CLM_SK"), "left")\
    .join(df_EDW_CLM_lkup, F.col("Lnk_ErrClm.CLM_SK") == F.col("EDW_CLM.CLM_SK"), "left")

# Add stage variables
df_final_lkup = joined_lkup_1.withColumn(
    "svMbrUniqKey",
    F.expr("CASE WHEN Lnk_Mbr_Lkup.MBR_UNIQ_KEY IS NULL THEN 0 ELSE Lnk_Mbr_Lkup.MBR_UNIQ_KEY END")
).withColumn(
    "svGrpId",
    F.expr("""
CASE WHEN GrpBase_lkup.GRP_ID IS NOT NULL THEN GrpBase_lkup.GRP_ID
     WHEN Lnk_Mbr_Lkup.GRP_ID IS NOT NULL THEN Lnk_Mbr_Lkup.GRP_ID
     ELSE 'UNK'
END
""")
).withColumn(
    "svSubId",
    F.expr("""CASE WHEN Lnk_Mbr_Lkup.SUB_ID IS NULL THEN '0' ELSE Lnk_Mbr_Lkup.SUB_ID END""")
).withColumn(
    "svMbrSfxNo",
    F.expr("""CASE WHEN Lnk_Mbr_Lkup.MBR_SFX_NO IS NULL THEN 0 ELSE Lnk_Mbr_Lkup.MBR_SFX_NO END""")
).withColumn(
    "svMbrEnrEffDt",
    F.expr("""CASE WHEN Lnk_Mbr_Lkup.EFF_DT_SK IS NULL THEN '1753-01-01' ELSE Lnk_Mbr_Lkup.EFF_DT_SK END""")
).withColumn(
    "svSubUniqKey",
    F.expr("""CASE WHEN Lnk_Mbr_Lkup.SUB_UNIQ_KEY IS NOT NULL THEN Lnk_Mbr_Lkup.SUB_UNIQ_KEY ELSE 0 END""")
).withColumn(
    "svSrcSysCd",
    F.expr(f"""
CASE WHEN Lnk_ErrClm.SRC_SYS_CD IS NULL OR length(trim(Lnk_ErrClm.SRC_SYS_CD))=0 THEN "UNK"
     WHEN trim(Lnk_ErrClm.SRC_SYS_CD) in ('PCT','WELLDYNERX','PCS','CAREMARK','ARGUS','EDC','OT@2','ADOL','MOHSAIC','CAREADVANCE','ESI','OPTUMRX','MCSOURCE','MCAID','MEDTRAK','BCBSSC','BCA','BCBSA','SAVRX','LDI','EYEMED','CVS','LUMERIS','MEDIMPACT') THEN 'FACETS'
     ELSE Lnk_ErrClm.SRC_SYS_CD
END
""")
)

# Output link Lnk_WDrugEnr
df_Lnk_WDrugEnr = df_final_lkup.filter(F.expr("Lnk_Mbr_Lkup.MBR_UNIQ_KEY IS NOT NULL")).select(
    F.col("Lnk_ErrClm.CLM_ID").alias("CLM_ID"),
    F.col("Lnk_ErrClm.CLM_SVC_STRT_DT_SK").alias("FILL_DT_SK"),
    F.col("Lnk_Mbr_Lkup.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY")
)

# Output link Lnk_ErrClmLand
df_Lnk_ErrClmLand = df_final_lkup.filter(F.expr("Lnk_Mbr_Lkup.MBR_UNIQ_KEY IS NOT NULL")).select(
    F.col("Lnk_ErrClm.CLM_SK").alias("CLM_SK"),
    F.col("Lnk_ErrClm.CLM_ID").alias("CLM_ID"),
    F.col("Lnk_ErrClm.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Lnk_ErrClm.CLM_TYP_CD").alias("CLM_TYP_CD"),
    F.col("Lnk_ErrClm.CLM_SUBTYP_CD").alias("CLM_SUBTYP_CD"),
    F.col("Lnk_ErrClm.CLM_SVC_STRT_DT_SK").alias("CLM_SVC_STRT_DT_SK"),
    F.col("Lnk_ErrClm.SRC_SYS_GRP_PFX").alias("SRC_SYS_GRP_PFX"),
    F.col("Lnk_ErrClm.SRC_SYS_GRP_ID").alias("SRC_SYS_GRP_ID"),
    F.col("Lnk_ErrClm.SRC_SYS_GRP_SFX").alias("SRC_SYS_GRP_SFX"),
    F.col("Lnk_ErrClm.SUB_SSN").alias("SUB_SSN"),
    F.col("Lnk_ErrClm.PATN_LAST_NM").alias("PATN_LAST_NM"),
    F.col("Lnk_ErrClm.PATN_FIRST_NM").alias("PATN_FIRST_NM"),
    F.col("Lnk_ErrClm.PATN_GNDR_CD").alias("PATN_GNDR_CD"),
    F.col("Lnk_ErrClm.PATN_BRTH_DT_SK").alias("PATN_BRTH_DT_SK"),
    F.col("svMbrUniqKey").alias("MBR_UNIQ_KEY"),
    F.col("svGrpId").alias("GRP_ID"),
    F.col("svSubId").alias("SUB_ID"),
    F.col("svMbrSfxNo").alias("MBR_SFX_NO"),
    F.col("svSubUniqKey").alias("SUB_UNIQ_KEY"),
    F.col("svMbrEnrEffDt").alias("EFF_DT_SK")
)

# Output link ErrorClm
df_ErrorClm = df_final_lkup.select(
    F.col("Lnk_ErrClm.CLM_ID").alias("CLM_ID"),
    F.col("Lnk_ErrClm.CLM_SK").alias("CLM_SK"),
    F.col("Lnk_ErrClm.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Lnk_ErrClm.CLM_TYP_CD").alias("CLM_TYP_CD"),
    F.col("Lnk_ErrClm.CLM_SUBTYP_CD").alias("CLM_SUBTYP_CD"),
    F.col("Lnk_ErrClm.CLM_SVC_STRT_DT_SK").alias("CLM_SVC_STRT_DT_SK"),
    F.col("Lnk_ErrClm.SRC_SYS_GRP_PFX").alias("SRC_SYS_GRP_PFX"),
    F.col("Lnk_ErrClm.SRC_SYS_GRP_ID").alias("SRC_SYS_GRP_ID"),
    F.col("Lnk_ErrClm.SRC_SYS_GRP_SFX").alias("SRC_SYS_GRP_SFX"),
    F.col("Lnk_ErrClm.SUB_SSN").alias("SUB_SSN"),
    F.col("Lnk_ErrClm.PATN_LAST_NM").alias("PATN_LAST_NM"),
    F.col("Lnk_ErrClm.PATN_FIRST_NM").alias("PATN_FIRST_NM"),
    F.col("Lnk_ErrClm.PATN_GNDR_CD").alias("PATN_GNDR_CD"),
    F.col("Lnk_ErrClm.PATN_BRTH_DT_SK").alias("PATN_BRTH_DT_SK"),
    F.col("Lnk_ErrClm.ERR_CD").alias("ERR_CD"),
    F.col("Lnk_ErrClm.ERR_DESC").alias("ERR_DESC"),
    F.col("Lnk_ErrClm.FEP_MBR_ID").alias("FEP_MBR_ID"),
    F.col("Lnk_ErrClm.SUB_FIRST_NM").alias("SUB_FIRST_NM"),
    F.col("Lnk_ErrClm.SUB_LAST_NM").alias("SUB_LAST_NM"),
    F.col("Lnk_ErrClm.SRC_SYS_SUB_ID").alias("SRC_SYS_SUB_ID"),
    F.col("Lnk_ErrClm.SRC_SYS_MBR_SFX_NO").alias("SRC_SYS_MBR_SFX_NO"),
    F.expr("""
CASE WHEN GrpBase_lkup.GRP_ID IS NOT NULL THEN GrpBase_lkup.GRP_ID
     WHEN Lnk_Mbr_Lkup.GRP_ID IS NOT NULL THEN Lnk_Mbr_Lkup.GRP_ID
     WHEN Lnk_ErrClm.GRP_ID IS NOT NULL THEN Lnk_Mbr_Lkup.GRP_ID
     ELSE 'UNK'
END
""").alias("GRP_ID"),
    F.col("Lnk_ErrClm.FILE_DT_SK").alias("FILE_DT_SK"),
    F.col("Lnk_ErrClm.PATN_SSN").alias("PATN_SSN")
)

# Output link ids_grp_update
df_ids_grp_update = df_final_lkup.filter(F.expr("(length(trim(Lnk_ErrClm.GRP_ID))=0 OR Lnk_ErrClm.GRP_ID IS NULL) AND GrpBase_lkup.GRP_ID IS NOT NULL AND IDS_CLM.CLM_SK IS NOT NULL")).select(
    F.col("Lnk_ErrClm.CLM_SK").alias("CLM_SK"),
    F.col("RunCycle").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.expr("GetFkeyGrp(svSrcSysCd, Lnk_ErrClm.CLM_SK, svGrpId, Logging)").alias("GRP_SK")
)

# Output link edw_grp_update
df_edw_grp_update = df_final_lkup.filter(F.expr("(length(trim(Lnk_ErrClm.GRP_ID))=0 OR Lnk_ErrClm.GRP_ID IS NULL) AND GrpBase_lkup.GRP_ID IS NOT NULL AND EDW_CLM.CLM_SK IS NOT NULL")).select(
    F.col("Lnk_ErrClm.CLM_SK").alias("CLM_SK"),
    F.col("RunCycleDate").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("RunCycle").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.expr("GetFkeyGrp(svSrcSysCd, Lnk_ErrClm.CLM_SK, svGrpId, Logging)").alias("GRP_SK"),
    F.col("svGrpId").alias("GRP_ID")
)

# Stage MedtrakErrClmLand (CSeqFileStage)
write_files(
    df_Lnk_ErrClmLand.select(
        "CLM_SK","CLM_ID","SRC_SYS_CD","CLM_TYP_CD","CLM_SUBTYP_CD","CLM_SVC_STRT_DT_SK",
        "SRC_SYS_GRP_PFX","SRC_SYS_GRP_ID","SRC_SYS_GRP_SFX","SUB_SSN","PATN_LAST_NM","PATN_FIRST_NM",
        "PATN_GNDR_CD","PATN_BRTH_DT_SK","MBR_UNIQ_KEY","GRP_ID","SUB_ID","MBR_SFX_NO",
        "SUB_UNIQ_KEY","EFF_DT_SK"
    ),
    f"{adls_path}/verified/{SrcSysCd1}_ErrClm_Landing.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# Stage W_DRUG_ENR (CSeqFileStage)
write_files(
    df_Lnk_WDrugEnr.select("CLM_ID","FILL_DT_SK","MBR_UNIQ_KEY"),
    f"{adls_path}/load/W_DRUG_ENR.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# IDS_CLM_Update (DB2Connector, Database=IDS) => actual job had "Update" with an SQL snippet
# We'll do a merge that only updates matched. Key=CLM_SK
staging_table_ids_clm = f"STAGING.MedtrakPClmMbrErrExtr_IDS_CLM_Update_temp"
drop_staging_ids_clm = f"DROP TABLE IF EXISTS {staging_table_ids_clm}"
execute_dml(drop_staging_ids_clm, jdbc_url_ids, jdbc_props_ids)

create_staging_ids_clm = f"""
CREATE TABLE {staging_table_ids_clm} 
(
CLM_SK INT,
LAST_UPDT_RUN_CYC_EXCTN_SK INT,
GRP_SK INT
)
"""
execute_dml(create_staging_ids_clm, jdbc_url_ids, jdbc_props_ids)

(
    df_ids_grp_update
    .write
    .format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("dbtable", staging_table_ids_clm)
    .mode("append")
    .save()
)

merge_sql_ids_clm = f"""
MERGE INTO {IDSOwner}.CLM AS T
USING {staging_table_ids_clm} AS S
ON T.CLM_SK = S.CLM_SK
WHEN MATCHED THEN
  UPDATE SET
    T.LAST_UPDT_RUN_CYC_EXCTN_SK = S.LAST_UPDT_RUN_CYC_EXCTN_SK,
    T.GRP_SK = S.GRP_SK
WHEN NOT MATCHED THEN
  INSERT (CLM_SK, LAST_UPDT_RUN_CYC_EXCTN_SK, GRP_SK)
  VALUES (S.CLM_SK, S.LAST_UPDT_RUN_CYC_EXCTN_SK, S.GRP_SK);
"""
execute_dml(merge_sql_ids_clm, jdbc_url_ids, jdbc_props_ids)

# EDW_CLM_Update (DB2Connector, Database=EDW) => also "Update" => do merge
staging_table_edw_clm = f"STAGING.MedtrakPClmMbrErrExtr_EDW_CLM_Update_temp"
drop_staging_edw_clm = f"DROP TABLE IF EXISTS {staging_table_edw_clm}"
execute_dml(drop_staging_edw_clm, jdbc_url_edw, jdbc_props_edw)

create_staging_edw_clm = f"""
CREATE TABLE {staging_table_edw_clm}
(
CLM_SK INT,
LAST_UPDT_RUN_CYC_EXCTN_DT_SK CHAR(10),
LAST_UPDT_RUN_CYC_EXCTN_SK INT,
GRP_SK INT,
GRP_ID VARCHAR(255)
)
"""
execute_dml(create_staging_edw_clm, jdbc_url_edw, jdbc_props_edw)

(
    df_edw_grp_update
    .write
    .format("jdbc")
    .option("url", jdbc_url_edw)
    .options(**jdbc_props_edw)
    .option("dbtable", staging_table_edw_clm)
    .mode("append")
    .save()
)

merge_sql_edw_clm = f"""
MERGE INTO {EDWOwner}.CLM_F AS T
USING {staging_table_edw_clm} AS S
ON T.CLM_SK = S.CLM_SK
WHEN MATCHED THEN
  UPDATE SET
    T.LAST_UPDT_RUN_CYC_EXCTN_DT_SK = S.LAST_UPDT_RUN_CYC_EXCTN_DT_SK,
    T.LAST_UPDT_RUN_CYC_EXCTN_SK = S.LAST_UPDT_RUN_CYC_EXCTN_SK,
    T.GRP_SK = S.GRP_SK,
    T.GRP_ID = S.GRP_ID
WHEN NOT MATCHED THEN
  INSERT (CLM_SK, LAST_UPDT_RUN_CYC_EXCTN_DT_SK, LAST_UPDT_RUN_CYC_EXCTN_SK, GRP_SK, GRP_ID)
  VALUES (S.CLM_SK, S.LAST_UPDT_RUN_CYC_EXCTN_DT_SK, S.LAST_UPDT_RUN_CYC_EXCTN_SK, S.GRP_SK, S.GRP_ID);
"""
execute_dml(merge_sql_edw_clm, jdbc_url_edw, jdbc_props_edw)

# Scenario A hashed file replacement for hf_medtrak_errmbrclm_errlkup
df_hf_medtrak_errmbrclm_errlkup = dedup_sort(df_ErrorClm, ["CLM_ID"], [])

# Stage SCErrFile (Transformer)
df_SCErrFile_primary = df_ErrorFile2.alias("RejectRecs")
df_SCErrFile_lookup = df_hf_medtrak_errmbrclm_errlkup.alias("Error")

df_SCErrFile_join = df_SCErrFile_primary.join(
    df_SCErrFile_lookup,
    F.col("RejectRecs.CLM_ID") == F.col("Error.CLM_ID"),
    "left"
)

df_ErrFileUpdate = df_SCErrFile_join.select(
    F.col("Error.CLM_SK").alias("CLM_SK"),
    F.col("Error.CLM_ID").alias("CLM_ID"),
    F.col("Error.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Error.CLM_TYP_CD").alias("CLM_TYP_CD"),
    F.col("Error.CLM_SUBTYP_CD").alias("CLM_SUBTYP_CD"),
    F.col("Error.CLM_SVC_STRT_DT_SK").alias("CLM_SVC_STRT_DT_SK"),
    F.col("Error.SRC_SYS_GRP_PFX").alias("SRC_SYS_GRP_PFX"),
    F.col("Error.SRC_SYS_GRP_ID").alias("SRC_SYS_GRP_ID"),
    F.col("Error.SRC_SYS_GRP_SFX").alias("SRC_SYS_GRP_SFX"),
    F.col("Error.SUB_SSN").alias("SUB_SSN"),
    F.col("Error.PATN_LAST_NM").alias("PATN_LAST_NM"),
    F.col("Error.PATN_FIRST_NM").alias("PATN_FIRST_NM"),
    F.col("Error.PATN_GNDR_CD").alias("PATN_GNDR_CD"),
    F.col("Error.PATN_BRTH_DT_SK").alias("PATN_BRTH_DT_SK"),
    F.col("Error.ERR_CD").alias("ERR_CD"),
    F.col("Error.ERR_DESC").alias("ERR_DESC"),
    F.col("Error.FEP_MBR_ID").alias("FEP_MBR_ID"),
    F.col("Error.SUB_FIRST_NM").alias("SUB_FIRST_NM"),
    F.col("Error.SUB_LAST_NM").alias("SUB_LAST_NM"),
    F.col("Error.SRC_SYS_SUB_ID").alias("SRC_SYS_SUB_ID"),
    F.col("Error.SRC_SYS_MBR_SFX_NO").alias("SRC_SYS_MBR_SFX_NO"),
    F.col("Error.GRP_ID").alias("GRP_ID"),
    F.col("Error.FILE_DT_SK").alias("FILE_DT_SK"),
    F.col("Error.PATN_SSN").alias("PATN_SSN")
)

df_ErrFileReport = df_SCErrFile_join.select(
    F.col("Error.CLM_ID").alias("CLM_ID"),
    F.col("Error.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Error.CLM_TYP_CD").alias("CLM_TYP_CD"),
    F.col("Error.CLM_SUBTYP_CD").alias("CLM_SUBTYP_CD"),
    F.col("Error.CLM_SVC_STRT_DT_SK").alias("CLM_SVC_STRT_DT_SK"),
    F.col("Error.SRC_SYS_GRP_PFX").alias("SRC_SYS_GRP_PFX"),
    F.col("Error.SRC_SYS_GRP_ID").alias("SRC_SYS_GRP_ID"),
    F.col("Error.SRC_SYS_GRP_SFX").alias("SRC_SYS_GRP_SFX"),
    F.col("Error.SUB_SSN").alias("SUB_SSN"),
    F.col("Error.PATN_LAST_NM").alias("PATN_LAST_NM"),
    F.col("Error.PATN_FIRST_NM").alias("PATN_FIRST_NM"),
    F.col("Error.PATN_GNDR_CD").alias("PATN_GNDR_CD"),
    F.col("Error.PATN_BRTH_DT_SK").alias("PATN_BRTH_DT_SK"),
    F.col("Error.ERR_CD").alias("ERR_CD"),
    F.col("Error.ERR_DESC").alias("ERR_DESC"),
    F.col("Error.FEP_MBR_ID").alias("FEP_MBR_ID"),
    F.col("Error.SUB_FIRST_NM").alias("SUB_FIRST_NM"),
    F.col("Error.SUB_LAST_NM").alias("SUB_LAST_NM"),
    F.col("Error.SRC_SYS_SUB_ID").alias("SRC_SYS_SUB_ID"),
    F.col("Error.SRC_SYS_MBR_SFX_NO").alias("SRC_SYS_MBR_SFX_NO"),
    F.col("Error.GRP_ID").alias("GRP_ID"),
    F.col("Error.FILE_DT_SK").alias("FILE_DT_SK"),
    F.col("Error.PATN_SSN").alias("PATN_SSN")
)

# Stage ErrFileUpdate => write to "P_CLM_MBRSH_ERR_RECYC_#SrcSysCd1#.dat"
write_files(
    df_ErrFileUpdate.select(
        "CLM_SK","CLM_ID","SRC_SYS_CD","CLM_TYP_CD","CLM_SUBTYP_CD","CLM_SVC_STRT_DT_SK",
        "SRC_SYS_GRP_PFX","SRC_SYS_GRP_ID","SRC_SYS_GRP_SFX","SUB_SSN","PATN_LAST_NM","PATN_FIRST_NM",
        "PATN_GNDR_CD","PATN_BRTH_DT_SK","ERR_CD","ERR_DESC","FEP_MBR_ID","SUB_FIRST_NM","SUB_LAST_NM",
        "SRC_SYS_SUB_ID","SRC_SYS_MBR_SFX_NO","GRP_ID","FILE_DT_SK","PATN_SSN"
    ),
    f"{adls_path}/load/P_CLM_MBRSH_ERR_RECYC_{SrcSysCd1}.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# Stage ErrFileReport => write to "#SrcSysCd1#_MbrMatch_MedClm_ErrorFile_Recycle.dat" in external
write_files(
    df_ErrFileReport.select(
        "CLM_ID","SRC_SYS_CD","CLM_TYP_CD","CLM_SUBTYP_CD","CLM_SVC_STRT_DT_SK",
        "SRC_SYS_GRP_PFX","SRC_SYS_GRP_ID","SRC_SYS_GRP_SFX","SUB_SSN","PATN_LAST_NM","PATN_FIRST_NM",
        "PATN_GNDR_CD","PATN_BRTH_DT_SK","ERR_CD","ERR_DESC","FEP_MBR_ID","SUB_FIRST_NM","SUB_LAST_NM",
        "SRC_SYS_SUB_ID","SRC_SYS_MBR_SFX_NO","GRP_ID","FILE_DT_SK","PATN_SSN"
    ),
    f"{adls_path_publish}/external/{SrcSysCd1}_MbrMatch_MedClm_ErrorFile_Recycle.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=True,
    quote="\"",
    nullValue=None
)