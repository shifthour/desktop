# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2024 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC DESCRIPTION:  This job merge with remaining members in W table
# MAGIC JOB NAME: EdwCmsMbrEnrZipCdMergeExtr
# MAGIC CALLED BY:  EdgeServerEdwCmsEnrSeq
# MAGIC 
# MAGIC RERUN INFORMATION: 
# MAGIC              PREVIOUS RUN SUCCESSFUL:   Nothing to reset. 
# MAGIC              PREVIOUS RUN ABORTED:         Nothing to reset, just restart.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC =====================================================================================================================================
# MAGIC Developer\(9)Date\(9)\(9)Project/Ticket #\(9)Change Description\(9)\(9)\(9)Development Project\(9)Code Reviewer\(9)Date Reviewed||
# MAGIC =====================================================================================================================================
# MAGIC Harsha Ravuri\(9)2024-04-04\(9)US#612091\(9)Original programming\(9)\(9)EnterpriseDev2                         Jeyaprasanna         2024-04-18
# MAGIC Harsha Ravuri\(9)2024-10-18\(9)US#629013\(9)Updated code to handle enrollment\(9)EnterpriseDev1                         Jeyaprasanna         2024-11-06
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9) those started in the middle of the year.

# MAGIC This section identifies any post spans that didn't have any Zip changes.
# MAGIC This section identifies if there is any pre-span 
# MAGIC that didn't have any Zip code changes in the W table.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DecimalType
)
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../Utility_Enterprise
# COMMAND ----------


EDWOwner = get_widget_value("EDWOwner", "")
edw_secret_name = get_widget_value("edw_secret_name", "")
RiskAdjYr = get_widget_value("RiskAdjYr", "")
BeginDt = get_widget_value("BeginDt", "")
EndDate = get_widget_value("EndDate", "")
QHPID = get_widget_value("QHPID", "")

# -----------------------------------------------------------------------------
# Ref_W_Table (DB2ConnectorPX - EDW) - Reading from EDW with a SQL query
# -----------------------------------------------------------------------------
jdbc_url_Ref_W_Table, jdbc_props_Ref_W_Table = get_db_config(edw_secret_name)
extract_query_Ref_W_Table = f"""
SELECT   distinct we.SUB_INDV_BE_KEY,
         we.MBR_INDV_BE_KEY,
         we.QHP_ID,
         we.MBR_ENR_EFF_DT_SK,
         we.MBR_ENR_TERM_DT_SK,
         we.PRM_AMT,
         we.ENR_PERD_ACTVTY_CD,
         we.RATE_AREA_ID,
         case 
              when mbr.MBR_HOME_ADDR_ZIP_CD_5 is null then GRP_ZIP_CD_5 
              else mbr.MBR_HOME_ADDR_ZIP_CD_5 
         end as MBR_HOME_ADDR_ZIP_CD_5
FROM     {EDWOwner}.MBR_D mbr,
         {EDWOwner}.MBR_ENR_D mbrEnr,
         {EDWOwner}.GRP_D grp,
         {EDWOwner}.MBR_ENR_QHP_D MBR_QHP,
         {EDWOwner}.SUB_D sub,
         {EDWOwner}.w_edge_mbr_elig_extr we
WHERE    mbr.MBR_SK = mbrEnr.MBR_SK
AND      mbr.GRP_SK = grp.GRP_SK
AND      MBR_QHP.MBR_ENR_SK = mbrEnr.MBR_ENR_SK
AND      MBR_QHP.MBR_UNIQ_KEY = mbrEnr.MBR_UNIQ_KEY
AND      MBR_QHP.MBR_ENR_EFF_DT_SK = mbrEnr.MBR_ENR_EFF_DT_SK
AND      mbrEnr.MBR_ENR_ELIG_IN = 'Y'
AND      mbrEnr.MBR_ENR_CLS_PLN_PROD_CAT_CD = 'MED'
AND      MBR_QHP.MBR_ENR_CLS_PLN_PROD_CAT_CD = 'MED'
AND      mbrEnr.MBR_ENR_EFF_DT_SK <= '{EndDate}'
AND      mbrEnr.MBR_ENR_TERM_DT_SK >= '{BeginDt}'
AND      mbrEnr.PROD_SH_NM IN ('PCB', 'PC', 'BCARE', 'BLUE-ACCESS', 'BLUE-SELECT','BLUESELECT+')
AND      mbrEnr.CLS_ID <> 'MHIP'
AND      MBR_QHP.QHP_ID <> 'NA'
AND      MBR_QHP.QHP_ID LIKE '{QHPID}'
AND      mbr.SUB_UNIQ_KEY = sub.SUB_UNIQ_KEY
AND      left(mbr.MBR_RELSHP_NM,3) = 'SUB'
AND      we.SUB_INDV_BE_KEY = we.mbr_indv_be_key
AND      we.sub_indv_be_key = mbr.mbr_indv_be_key
"""
df_Ref_W_Table = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_Ref_W_Table)
    .options(**jdbc_props_Ref_W_Table)
    .option("query", extract_query_Ref_W_Table)
    .load()
)

# -----------------------------------------------------------------------------
# Copy_Wedge (PxCopy)
# -----------------------------------------------------------------------------
df_Copy_Wedge_ref_pre = df_Ref_W_Table.select(
    F.col("SUB_INDV_BE_KEY").alias("SUB_INDV_BE_KEY"),
    F.col("MBR_INDV_BE_KEY").alias("MBR_INDV_BE_KEY"),
    F.col("QHP_ID").alias("QHP_ID"),
    F.col("MBR_ENR_EFF_DT_SK").alias("MBR_ENR_EFF_DT_SK"),
    F.col("RATE_AREA_ID").alias("RATE_AREA_ID"),
    F.col("MBR_ENR_TERM_DT_SK").alias("MBR_ENR_TERM_DT_SK"),
    F.col("PRM_AMT").alias("PRM_AMT"),
    F.col("ENR_PERD_ACTVTY_CD").alias("ENR_PERD_ACTVTY_CD"),
    F.col("MBR_HOME_ADDR_ZIP_CD_5").alias("MBR_HOME_ADDR_ZIP_CD_5")
)

df_Copy_Wedge_DSLink995 = df_Ref_W_Table.select(
    F.col("SUB_INDV_BE_KEY").alias("SUB_INDV_BE_KEY"),
    F.col("MBR_INDV_BE_KEY").alias("MBR_INDV_BE_KEY"),
    F.col("QHP_ID").alias("QHP_ID"),
    F.col("MBR_ENR_EFF_DT_SK").alias("MBR_ENR_EFF_DT_SK"),
    F.col("RATE_AREA_ID").alias("RATE_AREA_ID"),
    F.col("MBR_ENR_TERM_DT_SK").alias("MBR_ENR_TERM_DT_SK"),
    F.col("PRM_AMT").alias("PRM_AMT"),
    F.col("ENR_PERD_ACTVTY_CD").alias("ENR_PERD_ACTVTY_CD"),
    F.col("MBR_HOME_ADDR_ZIP_CD_5").alias("MBR_HOME_ADDR_ZIP_CD_5")
)

# -----------------------------------------------------------------------------
# ACA_Enrollment_PreNewSpan (PxSequentialFile) - Reading delimited file
# -----------------------------------------------------------------------------
schema_ACA_Enrollment_PreNewSpan = StructType([
    StructField("SUB_INDV_BE_KEY", DecimalType(38,10), nullable=False),
    StructField("MBR_INDV_BE_KEY", DecimalType(38,10), nullable=False),
    StructField("SUB_ADDR_ZIP_CD_5", StringType(), nullable=True),
    StructField("MBR_ENR_EFF_DT_SK", StringType(), nullable=False),
    StructField("MBR_ENR_TERM_DT_SK", StringType(), nullable=False),
    StructField("PRM_AMT", DecimalType(38,10), nullable=True),
    StructField("ENR_PERD_ACTVTY_CD", StringType(), nullable=True),
    StructField("RATE_AREA_ID", StringType(), nullable=True),
    StructField("QHP_ID", StringType(), nullable=False),
    StructField("SpanType", StringType(), nullable=False)
])
df_ACA_Enrollment_PreNewSpan = (
    spark.read.format("csv")
    .option("header", True)
    .option("delimiter", "|")
    .option("quote", None)
    .option("nullValue", None)
    .schema(schema_ACA_Enrollment_PreNewSpan)
    .load(f"{adls_path_raw}/landing/ACA_Enrollment_PreNewSpan.dat")
)

# -----------------------------------------------------------------------------
# Copy_New (PxCopy)
# -----------------------------------------------------------------------------
df_Copy_New_New1 = df_ACA_Enrollment_PreNewSpan.select(
    F.col("SUB_INDV_BE_KEY").alias("SUB_INDV_BE_KEY"),
    F.col("MBR_INDV_BE_KEY").alias("MBR_INDV_BE_KEY"),
    F.col("SUB_ADDR_ZIP_CD_5").alias("SUB_ADDR_ZIP_CD_5"),
    F.col("MBR_ENR_EFF_DT_SK").alias("MBR_ENR_EFF_DT_SK"),
    F.col("MBR_ENR_TERM_DT_SK").alias("MBR_ENR_TERM_DT_SK"),
    F.col("PRM_AMT").alias("PRM_AMT"),
    F.col("ENR_PERD_ACTVTY_CD").alias("ENR_PERD_ACTVTY_CD"),
    F.col("RATE_AREA_ID").alias("RATE_AREA_ID"),
    F.col("QHP_ID").alias("QHP_ID"),
    F.col("SpanType").alias("SpanType")
)

df_Copy_New_DSLink952 = df_ACA_Enrollment_PreNewSpan.select(
    F.col("SUB_INDV_BE_KEY").alias("SUB_INDV_BE_KEY"),
    F.col("MBR_INDV_BE_KEY").alias("MBR_INDV_BE_KEY"),
    F.col("SUB_ADDR_ZIP_CD_5").alias("SUB_ADDR_ZIP_CD_5"),
    F.col("MBR_ENR_EFF_DT_SK").alias("MBR_ENR_EFF_DT_SK"),
    F.col("MBR_ENR_TERM_DT_SK").alias("MBR_ENR_TERM_DT_SK"),
    F.col("PRM_AMT").alias("PRM_AMT"),
    F.col("ENR_PERD_ACTVTY_CD").alias("ENR_PERD_ACTVTY_CD"),
    F.col("RATE_AREA_ID").alias("RATE_AREA_ID"),
    F.col("QHP_ID").alias("QHP_ID"),
    F.col("SpanType").alias("SpanType")
)

# -----------------------------------------------------------------------------
# Filter_905 (PxFilter - No conditions, OutputRejects = true)
# We assume all rows go to the first output link "pre" and none to "Post_New".
# -----------------------------------------------------------------------------
df_Filter_905_pre = df_Copy_New_New1
df_Filter_905_Post_New = spark.createDataFrame([], df_Copy_New_New1.schema)

# -----------------------------------------------------------------------------
# Filter_942 (PxFilter - No conditions, OutputRejects = true)
# We assume all rows go to the first output link "PostNew" and none to "DSLink1024".
# -----------------------------------------------------------------------------
df_Filter_942_PostNew = df_Filter_905_Post_New
df_Filter_942_DSLink1024 = spark.createDataFrame([], df_Filter_905_Post_New.schema)

# -----------------------------------------------------------------------------
# lkp_pre (PxLookup)
#   PrimaryLink = df_Copy_Wedge_ref_pre
#   LookupLink = df_Filter_905_pre (left join)
# -----------------------------------------------------------------------------
df_lkp_pre_joined = df_Copy_Wedge_ref_pre.join(
    df_Filter_905_pre,
    on=[
        df_Copy_Wedge_ref_pre["SUB_INDV_BE_KEY"] == df_Filter_905_pre["SUB_INDV_BE_KEY"],
        df_Copy_Wedge_ref_pre["MBR_INDV_BE_KEY"] == df_Filter_905_pre["MBR_INDV_BE_KEY"],
        df_Copy_Wedge_ref_pre["QHP_ID"] == df_Filter_905_pre["QHP_ID"]
    ],
    how="left"
)

# Matched rows (where lookup not null):
condition_matched = (
    df_Filter_905_pre["SUB_INDV_BE_KEY"].isNotNull() &
    df_Filter_905_pre["MBR_INDV_BE_KEY"].isNotNull() &
    df_Filter_905_pre["QHP_ID"].isNotNull()
)
df_lkp_pre_Mtch_Pre = df_lkp_pre_joined.filter(condition_matched).select(
    df_Copy_Wedge_ref_pre["SUB_INDV_BE_KEY"].alias("SUB_INDV_BE_KEY"),
    df_Copy_Wedge_ref_pre["MBR_INDV_BE_KEY"].alias("MBR_INDV_BE_KEY"),
    df_Copy_Wedge_ref_pre["QHP_ID"].alias("QHP_ID"),
    df_Copy_Wedge_ref_pre["MBR_ENR_EFF_DT_SK"].alias("MBR_ENR_EFF_DT_SK"),
    df_Copy_Wedge_ref_pre["RATE_AREA_ID"].alias("RATE_AREA_ID"),
    df_Copy_Wedge_ref_pre["MBR_ENR_TERM_DT_SK"].alias("MBR_ENR_TERM_DT_SK"),
    df_Copy_Wedge_ref_pre["PRM_AMT"].alias("PRM_AMT"),
    df_Copy_Wedge_ref_pre["ENR_PERD_ACTVTY_CD"].alias("ENR_PERD_ACTVTY_CD"),
    df_Copy_Wedge_ref_pre["MBR_HOME_ADDR_ZIP_CD_5"].alias("MBR_HOME_ADDR_ZIP_CD_5"),
    df_Filter_905_pre["SUB_ADDR_ZIP_CD_5"].alias("SUB_ADDR_ZIP_CD_5"),
    df_Filter_905_pre["MBR_ENR_EFF_DT_SK"].alias("MBR_ENR_EFF_DT_SK_1")
)

# Unmatched rows (lookup didn't find a match):
df_lkp_pre_All_rej = df_lkp_pre_joined.filter(~condition_matched).select(
    df_Copy_Wedge_ref_pre["SUB_INDV_BE_KEY"].alias("SUB_INDV_BE_KEY"),
    df_Copy_Wedge_ref_pre["MBR_INDV_BE_KEY"].alias("MBR_INDV_BE_KEY"),
    df_Copy_Wedge_ref_pre["QHP_ID"].alias("QHP_ID"),
    df_Copy_Wedge_ref_pre["MBR_ENR_EFF_DT_SK"].alias("MBR_ENR_EFF_DT_SK"),
    df_Copy_Wedge_ref_pre["RATE_AREA_ID"].alias("RATE_AREA_ID"),
    df_Copy_Wedge_ref_pre["MBR_ENR_TERM_DT_SK"].alias("MBR_ENR_TERM_DT_SK"),
    df_Copy_Wedge_ref_pre["PRM_AMT"].alias("PRM_AMT"),
    df_Copy_Wedge_ref_pre["ENR_PERD_ACTVTY_CD"].alias("ENR_PERD_ACTVTY_CD")
)

# -----------------------------------------------------------------------------
# tfm_pre (CTransformerStage)
# -----------------------------------------------------------------------------
df_tfm_pre = df_lkp_pre_Mtch_Pre.withColumn("svEnrEff", F.regexp_replace(F.col("MBR_ENR_EFF_DT_SK_1"), "-", "")) \
    .withColumn("svEnrTrmDt1", F.regexp_replace(F.col("MBR_ENR_TERM_DT_SK"), "-", ""))

df_tfm_pre = df_tfm_pre.withColumn(
    "svLogicChk",
    F.when(
        F.col("svEnrEff") > F.col("svEnrTrmDt1"),
        F.lit("Yes")
    ).otherwise(F.lit("No"))
)

df_tfm_pre_Rej = df_tfm_pre.filter(F.col("svLogicChk") == "No").select(
    F.col("SUB_INDV_BE_KEY"),
    F.col("MBR_INDV_BE_KEY"),
    F.col("QHP_ID"),
    F.col("MBR_ENR_EFF_DT_SK"),
    F.col("RATE_AREA_ID"),
    F.col("MBR_ENR_TERM_DT_SK"),
    F.col("PRM_AMT"),
    F.col("ENR_PERD_ACTVTY_CD"),
    F.col("MBR_HOME_ADDR_ZIP_CD_5")
)

df_tfm_pre_DSLink1106 = df_tfm_pre.filter(F.col("svLogicChk") == "Yes").select(
    F.col("SUB_INDV_BE_KEY"),
    F.col("MBR_INDV_BE_KEY"),
    F.col("QHP_ID"),
    F.col("MBR_ENR_EFF_DT_SK"),
    F.col("RATE_AREA_ID"),
    F.col("MBR_ENR_TERM_DT_SK"),
    F.col("PRM_AMT"),
    F.col("ENR_PERD_ACTVTY_CD"),
    F.when(
        (F.col("SUB_ADDR_ZIP_CD_5").isNull()) | (F.col("SUB_ADDR_ZIP_CD_5") == ""),
        F.col("MBR_HOME_ADDR_ZIP_CD_5")
    ).otherwise(F.col("SUB_ADDR_ZIP_CD_5")).alias("SUB_ADDR_ZIP_CD_5")
)

df_tfm_pre_DSLink1109 = df_tfm_pre.filter(F.col("svLogicChk") == "No").select(
    F.col("SUB_INDV_BE_KEY").alias("SUB_INDV_BE_KEY"),
    F.col("MBR_INDV_BE_KEY").alias("MBR_INDV_BE_KEY"),
    F.col("QHP_ID").alias("QHP_ID"),
    F.col("MBR_ENR_EFF_DT_SK").alias("MBR_ENR_EFF_DT_SK"),
    F.col("RATE_AREA_ID"),
    F.col("MBR_ENR_TERM_DT_SK").alias("MBR_ENR_TERM_DT_SK"),
    F.col("PRM_AMT"),
    F.col("ENR_PERD_ACTVTY_CD"),
    F.col("SUB_ADDR_ZIP_CD_5").alias("SUB_ADDR_ZIP_CD_5")
)

# -----------------------------------------------------------------------------
# Lookup_1110 (PxLookup)
#   PrimaryLink = DSLink1106
#   LookupLink = DSLink1109 (left join)
# -----------------------------------------------------------------------------
df_Lookup_1110_joined = df_tfm_pre_DSLink1106.alias("DSLink1106").join(
    df_tfm_pre_DSLink1109.alias("DSLink1109"),
    on=[
        F.col("DSLink1106.SUB_INDV_BE_KEY") == F.col("DSLink1109.SUB_INDV_BE_KEY"),
        F.col("DSLink1106.MBR_INDV_BE_KEY") == F.col("DSLink1109.MBR_INDV_BE_KEY"),
        F.col("DSLink1106.QHP_ID") == F.col("DSLink1109.QHP_ID"),
        F.col("DSLink1106.MBR_ENR_EFF_DT_SK") == F.col("DSLink1109.MBR_ENR_EFF_DT_SK")
    ],
    how="left"
)

condition_1110_matched = (
    F.col("DSLink1109.SUB_INDV_BE_KEY").isNotNull()
    & F.col("DSLink1109.MBR_INDV_BE_KEY").isNotNull()
    & F.col("DSLink1109.QHP_ID").isNotNull()
    & F.col("DSLink1109.MBR_ENR_EFF_DT_SK").isNotNull()
)

df_Lookup_1110_DSLink1115 = df_Lookup_1110_joined.select(
    F.col("DSLink1106.SUB_INDV_BE_KEY").alias("SUB_INDV_BE_KEY"),
    F.col("DSLink1106.MBR_INDV_BE_KEY").alias("MBR_INDV_BE_KEY"),
    F.col("DSLink1106.QHP_ID").alias("QHP_ID"),
    F.col("DSLink1106.MBR_ENR_EFF_DT_SK").alias("MBR_ENR_EFF_DT_SK"),
    F.col("DSLink1106.RATE_AREA_ID").alias("RATE_AREA_ID"),
    F.col("DSLink1106.MBR_ENR_TERM_DT_SK").alias("MBR_ENR_TERM_DT_SK"),
    F.col("DSLink1106.PRM_AMT").alias("PRM_AMT"),
    F.col("DSLink1106.ENR_PERD_ACTVTY_CD").alias("ENR_PERD_ACTVTY_CD"),
    F.col("DSLink1106.SUB_ADDR_ZIP_CD_5").alias("SUB_ADDR_ZIP_CD_5")
)

df_Lookup_1110_DSLink1114 = df_Lookup_1110_joined.select(
    "DSLink1106.*"
)

# -----------------------------------------------------------------------------
# Peek_1116 (PxPeek) - DSLink1115
#   We do not do anything except pass through. 
# -----------------------------------------------------------------------------
df_Peek_1116 = df_Lookup_1110_DSLink1115  # passes through

# -----------------------------------------------------------------------------
# Funnel_1093 (PxFunnel)
#   Inputs: All_rej (lkp_pre) + Rej (tfm_pre)
# -----------------------------------------------------------------------------
df_Funnel_1093 = df_lkp_pre_All_rej.select(
    F.col("SUB_INDV_BE_KEY"),
    F.col("MBR_INDV_BE_KEY"),
    F.col("QHP_ID"),
    F.col("MBR_ENR_EFF_DT_SK"),
    F.col("RATE_AREA_ID"),
    F.col("MBR_ENR_TERM_DT_SK"),
    F.col("PRM_AMT"),
    F.col("ENR_PERD_ACTVTY_CD")
).unionByName(
    df_tfm_pre_Rej.select(
        "SUB_INDV_BE_KEY",
        "MBR_INDV_BE_KEY",
        "QHP_ID",
        "MBR_ENR_EFF_DT_SK",
        "RATE_AREA_ID",
        "MBR_ENR_TERM_DT_SK",
        "PRM_AMT",
        "ENR_PERD_ACTVTY_CD"
    ),
    allowMissingColumns=True
)

# -----------------------------------------------------------------------------
# Filter_942 (already defined) => df_Filter_942_PostNew is main link
# -----------------------------------------------------------------------------
df_Filter_942_PostNew_out = df_Filter_942_PostNew

# -----------------------------------------------------------------------------
# lkp_PostNew (PxLookup)
#   PrimaryLink = Funnel_1093 => "Ref_PostNew"
#   LookupLink = Filter_942 => "PostNew"  (left join)
# -----------------------------------------------------------------------------
df_lkp_PostNew_joined = df_Funnel_1093.alias("Ref_PostNew").join(
    df_Filter_942_PostNew_out.alias("PostNew"),
    on=[
        F.col("Ref_PostNew.SUB_INDV_BE_KEY") == F.col("PostNew.SUB_INDV_BE_KEY"),
        F.col("Ref_PostNew.MBR_INDV_BE_KEY") == F.col("PostNew.MBR_INDV_BE_KEY")
    ],
    how="left"
)

condition_lkp_PostNew_matched = (
    F.col("PostNew.SUB_INDV_BE_KEY").isNotNull()
    & F.col("PostNew.MBR_INDV_BE_KEY").isNotNull()
)

df_lkp_PostNew_Match_PostNew = df_lkp_PostNew_joined.filter(condition_lkp_PostNew_matched).select(
    F.col("Ref_PostNew.SUB_INDV_BE_KEY").alias("SUB_INDV_BE_KEY"),
    F.col("Ref_PostNew.MBR_INDV_BE_KEY").alias("MBR_INDV_BE_KEY"),
    F.col("Ref_PostNew.QHP_ID").alias("QHP_ID"),
    F.col("Ref_PostNew.MBR_ENR_EFF_DT_SK").alias("MBR_ENR_EFF_DT_SK"),
    F.col("Ref_PostNew.RATE_AREA_ID").alias("RATE_AREA_ID"),
    F.col("Ref_PostNew.MBR_ENR_TERM_DT_SK").alias("MBR_ENR_TERM_DT_SK"),
    F.col("Ref_PostNew.PRM_AMT").alias("PRM_AMT"),
    F.col("Ref_PostNew.ENR_PERD_ACTVTY_CD").alias("ENR_PERD_ACTVTY_CD"),
    F.col("PostNew.SUB_ADDR_ZIP_CD_5").alias("SUB_ADDR_ZIP_CD_5"),
    F.col("PostNew.MBR_ENR_TERM_DT_SK").alias("MBR_ENR_TERM_DT_SK_1")
)

df_lkp_PostNew_All_Rej = df_lkp_PostNew_joined.filter(~condition_lkp_PostNew_matched)

# -----------------------------------------------------------------------------
# tfm_PostNew (CTransformerStage)
# -----------------------------------------------------------------------------
df_tfm_PostNew = df_lkp_PostNew_Match_PostNew.withColumn("svAllEffDt",
    F.regexp_replace(F.col("MBR_ENR_EFF_DT_SK"), "-", "")
).withColumn("svTrmDt",
    F.regexp_replace(F.col("MBR_ENR_TERM_DT_SK_1"), "-", "")
)

df_tfm_PostNew = df_tfm_PostNew.withColumn(
    "svLogicChk",
    F.when(F.col("svAllEffDt") > F.col("svTrmDt"), F.lit("Yes")).otherwise(F.lit("No"))
)

df_tfm_PostNew_DSLink1120 = df_tfm_PostNew.filter(F.col("svLogicChk") == "Yes").select(
    "SUB_INDV_BE_KEY",
    "MBR_INDV_BE_KEY",
    "QHP_ID",
    "MBR_ENR_EFF_DT_SK",
    "RATE_AREA_ID",
    "MBR_ENR_TERM_DT_SK",
    "PRM_AMT",
    "ENR_PERD_ACTVTY_CD",
    "SUB_ADDR_ZIP_CD_5",
    F.col("MBR_ENR_TERM_DT_SK_1").alias("MBR_ENR_TERM_DT_SK_1")
)

df_tfm_PostNew_DSLink1135 = df_tfm_PostNew.filter(F.col("svLogicChk") == "No").select(
    "SUB_INDV_BE_KEY",
    "MBR_INDV_BE_KEY",
    "QHP_ID",
    "MBR_ENR_EFF_DT_SK",
    "RATE_AREA_ID",
    "MBR_ENR_TERM_DT_SK",
    "PRM_AMT",
    "ENR_PERD_ACTVTY_CD"
)

df_tfm_PostNew_ref = df_tfm_PostNew.filter(F.col("svLogicChk") == "No").select(
    F.col("SUB_INDV_BE_KEY").alias("SUB_INDV_BE_KEY"),
    F.col("MBR_INDV_BE_KEY").alias("MBR_INDV_BE_KEY"),
    F.col("QHP_ID").alias("QHP_ID"),
    F.col("MBR_ENR_EFF_DT_SK").alias("MBR_ENR_EFF_DT_SK"),
    F.col("RATE_AREA_ID"),
    F.col("MBR_ENR_TERM_DT_SK").alias("MBR_ENR_TERM_DT_SK"),
    F.col("PRM_AMT"),
    F.col("ENR_PERD_ACTVTY_CD"),
    F.col("SUB_ADDR_ZIP_CD_5").alias("SUB_ADDR_ZIP_CD_5"),
    F.col("MBR_ENR_TERM_DT_SK_1").alias("MBR_ENR_TERM_DT_SK_1")
)

# -----------------------------------------------------------------------------
# Lookup_1119 (PxLookup)
#   PrimaryLink = DSLink1120
#   LookupLink = ref (above) (left join)
# -----------------------------------------------------------------------------
df_Lookup_1119_joined = df_tfm_PostNew_DSLink1120.alias("DSLink1120").join(
    df_tfm_PostNew_ref.alias("ref"),
    on=[
        F.col("DSLink1120.SUB_INDV_BE_KEY") == F.col("ref.SUB_INDV_BE_KEY"),
        F.col("DSLink1120.MBR_INDV_BE_KEY") == F.col("ref.MBR_INDV_BE_KEY"),
        F.col("DSLink1120.QHP_ID") == F.col("ref.QHP_ID"),
        F.col("DSLink1120.MBR_ENR_TERM_DT_SK") == F.col("ref.MBR_ENR_TERM_DT_SK")
    ],
    how="left"
)

df_Lookup_1119_DSLink1112 = df_Lookup_1119_joined.select(
    F.col("DSLink1120.SUB_INDV_BE_KEY").alias("SUB_INDV_BE_KEY"),
    F.col("DSLink1120.MBR_INDV_BE_KEY").alias("MBR_INDV_BE_KEY"),
    F.col("DSLink1120.QHP_ID").alias("QHP_ID"),
    F.col("DSLink1120.MBR_ENR_EFF_DT_SK").alias("MBR_ENR_EFF_DT_SK"),
    F.col("DSLink1120.RATE_AREA_ID").alias("RATE_AREA_ID"),
    F.col("DSLink1120.MBR_ENR_TERM_DT_SK").alias("MBR_ENR_TERM_DT_SK"),
    F.col("DSLink1120.PRM_AMT").alias("PRM_AMT"),
    F.col("DSLink1120.ENR_PERD_ACTVTY_CD").alias("ENR_PERD_ACTVTY_CD"),
    F.col("DSLink1120.SUB_ADDR_ZIP_CD_5").alias("SUB_ADDR_ZIP_CD_5"),
    F.col("DSLink1120.MBR_ENR_TERM_DT_SK_1").alias("MBR_ENR_TERM_DT_SK_1")
)

df_Lookup_1119_Mtch_Post = df_Lookup_1119_joined.select(
    F.col("DSLink1120.SUB_INDV_BE_KEY").alias("SUB_INDV_BE_KEY"),
    F.col("DSLink1120.MBR_INDV_BE_KEY").alias("MBR_INDV_BE_KEY"),
    F.col("DSLink1120.QHP_ID").alias("QHP_ID"),
    F.col("DSLink1120.MBR_ENR_EFF_DT_SK").alias("MBR_ENR_EFF_DT_SK"),
    F.col("DSLink1120.RATE_AREA_ID").alias("RATE_AREA_ID"),
    F.col("DSLink1120.MBR_ENR_TERM_DT_SK").alias("MBR_ENR_TERM_DT_SK"),
    F.col("DSLink1120.PRM_AMT").alias("PRM_AMT"),
    F.col("DSLink1120.ENR_PERD_ACTVTY_CD").alias("ENR_PERD_ACTVTY_CD"),
    F.col("DSLink1120.SUB_ADDR_ZIP_CD_5").alias("SUB_ADDR_ZIP_CD_5")
)

# -----------------------------------------------------------------------------
# Peek_1113 (PxPeek) => DSLink1112
# -----------------------------------------------------------------------------
df_Peek_1113 = df_Lookup_1119_DSLink1112

# -----------------------------------------------------------------------------
# Copy_1115 (PxCopy)
# -----------------------------------------------------------------------------
df_Copy_1115_RD_Pre = df_Lookup_1110_DSLink1114.select(
    F.col("SUB_INDV_BE_KEY"),
    F.col("MBR_INDV_BE_KEY"),
    F.col("QHP_ID"),
    F.col("MBR_ENR_EFF_DT_SK"),
    F.col("RATE_AREA_ID"),
    F.col("MBR_ENR_TERM_DT_SK"),
    F.col("PRM_AMT"),
    F.col("ENR_PERD_ACTVTY_CD"),
    F.col("SUB_ADDR_ZIP_CD_5")
)

# -----------------------------------------------------------------------------
# RD_Pre (PxRemDup)
# -----------------------------------------------------------------------------
df_RD_Pre_dedup = dedup_sort(
    df_Copy_1115_RD_Pre,
    partition_cols=["SUB_INDV_BE_KEY","MBR_INDV_BE_KEY","QHP_ID","MBR_ENR_EFF_DT_SK","MBR_ENR_TERM_DT_SK","ENR_PERD_ACTVTY_CD","PRM_AMT","RATE_AREA_ID","SUB_ADDR_ZIP_CD_5"],
    sort_cols=[]
)

# -----------------------------------------------------------------------------
# RD_Pre => output link "Pre"
# -----------------------------------------------------------------------------
df_RD_Pre_Pre = df_RD_Pre_dedup.select(
    "SUB_INDV_BE_KEY",
    "MBR_INDV_BE_KEY",
    "QHP_ID",
    "MBR_ENR_EFF_DT_SK",
    "RATE_AREA_ID",
    "MBR_ENR_TERM_DT_SK",
    "PRM_AMT",
    "ENR_PERD_ACTVTY_CD",
    "SUB_ADDR_ZIP_CD_5"
)

# -----------------------------------------------------------------------------
# RD_Post (PxRemDup)
# -----------------------------------------------------------------------------
df_RD_Post_dedup = dedup_sort(
    df_lookup_1119_Mtch_Post,
    partition_cols=["SUB_INDV_BE_KEY","MBR_INDV_BE_KEY","MBR_ENR_EFF_DT_SK","MBR_ENR_TERM_DT_SK","QHP_ID","ENR_PERD_ACTVTY_CD","PRM_AMT","RATE_AREA_ID","SUB_ADDR_ZIP_CD_5"],
    sort_cols=[]
)

df_RD_Post_Post = df_RD_Post_dedup.select(
    F.col("SUB_INDV_BE_KEY"),
    F.col("MBR_INDV_BE_KEY"),
    F.col("QHP_ID"),
    F.col("MBR_ENR_EFF_DT_SK"),
    F.col("RATE_AREA_ID"),
    F.col("MBR_ENR_TERM_DT_SK"),
    F.col("PRM_AMT"),
    F.col("ENR_PERD_ACTVTY_CD"),
    F.col("SUB_ADDR_ZIP_CD_5")
)

# -----------------------------------------------------------------------------
# Copy_950 (PxCopy) => from df_Copy_New_DSLink952 => output DSLink954
# -----------------------------------------------------------------------------
df_Copy_950_DSLink954 = df_Copy_New_DSLink952.select(
    F.col("SUB_INDV_BE_KEY"),
    F.col("MBR_INDV_BE_KEY"),
    F.col("SUB_ADDR_ZIP_CD_5"),
    F.col("MBR_ENR_EFF_DT_SK"),
    F.col("MBR_ENR_TERM_DT_SK"),
    F.col("PRM_AMT"),
    F.col("ENR_PERD_ACTVTY_CD"),
    F.col("RATE_AREA_ID"),
    F.col("QHP_ID")
)

# -----------------------------------------------------------------------------
# Copy_996 (PxCopy) => from df_Copy_Wedge_DSLink995 => output DSLink998
# -----------------------------------------------------------------------------
df_Copy_996_DSLink998 = df_Copy_Wedge_DSLink995.select(
    F.col("SUB_INDV_BE_KEY"),
    F.col("MBR_INDV_BE_KEY"),
    F.col("QHP_ID"),
    F.col("MBR_ENR_EFF_DT_SK"),
    F.col("RATE_AREA_ID"),
    F.col("MBR_ENR_TERM_DT_SK"),
    F.col("PRM_AMT"),
    F.col("ENR_PERD_ACTVTY_CD"),
    F.col("MBR_HOME_ADDR_ZIP_CD_5").alias("SUB_ADDR_ZIP_CD_5")
)

# -----------------------------------------------------------------------------
# Funnel_1137 => All_Rej(lkp_PostNew) + DSLink1135(tfm_PostNew)
# -----------------------------------------------------------------------------
df_Funnel_1137 = df_lkp_PostNew_All_Rej.select(
    F.col("Ref_PostNew.SUB_INDV_BE_KEY").alias("SUB_INDV_BE_KEY"),
    F.col("Ref_PostNew.MBR_INDV_BE_KEY").alias("MBR_INDV_BE_KEY"),
    F.col("Ref_PostNew.QHP_ID").alias("QHP_ID"),
    F.col("Ref_PostNew.MBR_ENR_EFF_DT_SK").alias("MBR_ENR_EFF_DT_SK"),
    F.col("Ref_PostNew.RATE_AREA_ID").alias("RATE_AREA_ID"),
    F.col("Ref_PostNew.MBR_ENR_TERM_DT_SK").alias("MBR_ENR_TERM_DT_SK"),
    F.col("Ref_PostNew.PRM_AMT").alias("PRM_AMT"),
    F.col("Ref_PostNew.ENR_PERD_ACTVTY_CD").alias("ENR_PERD_ACTVTY_CD")
).unionByName(
    df_tfm_PostNew_DSLink1135.select(
        "SUB_INDV_BE_KEY",
        "MBR_INDV_BE_KEY",
        "QHP_ID",
        "MBR_ENR_EFF_DT_SK",
        "RATE_AREA_ID",
        "MBR_ENR_TERM_DT_SK",
        "PRM_AMT",
        "ENR_PERD_ACTVTY_CD"
    ),
    allowMissingColumns=True
)

# -----------------------------------------------------------------------------
# Copy_1033 => from Filter_942_DSLink1024 => output DSLink1034
#   (Filter_942_DSLink1024 is empty, so result is empty)
# -----------------------------------------------------------------------------
df_Copy_1033_DSLink1034 = df_Filter_942_DSLink1024.select(
    F.col("SUB_INDV_BE_KEY"),
    F.col("MBR_INDV_BE_KEY"),
    F.col("SUB_ADDR_ZIP_CD_5"),
    F.col("MBR_ENR_EFF_DT_SK"),
    F.col("MBR_ENR_TERM_DT_SK"),
    F.col("PRM_AMT"),
    F.col("ENR_PERD_ACTVTY_CD"),
    F.col("RATE_AREA_ID"),
    F.col("QHP_ID"),
    F.col("SpanType")
)

# -----------------------------------------------------------------------------
# Lookup_1025 (PxLookup)
#   PrimaryLink = DSLink1014 (Funnel_1137)
#   LookupLink = DSLink1034 (empty) (left join)
# -----------------------------------------------------------------------------
df_Lookup_1025_joined = df_Funnel_1137.alias("DSLink1014").join(
    df_Copy_1033_DSLink1034.alias("DSLink1034"),
    on=[
        F.col("DSLink1014.SUB_INDV_BE_KEY") == F.col("DSLink1034.SUB_INDV_BE_KEY"),
        F.col("DSLink1014.MBR_INDV_BE_KEY") == F.col("DSLink1034.MBR_INDV_BE_KEY")
    ],
    how="left"
)
df_Lookup_1025_DSLink10 = df_Lookup_1025_joined.select(
    F.col("DSLink1014.SUB_INDV_BE_KEY").alias("SUB_INDV_BE_KEY"),
    F.col("DSLink1014.MBR_INDV_BE_KEY").alias("MBR_INDV_BE_KEY"),
    F.col("DSLink1014.QHP_ID").alias("QHP_ID"),
    F.col("DSLink1014.MBR_ENR_EFF_DT_SK").alias("MBR_ENR_EFF_DT_SK"),
    F.col("DSLink1014.RATE_AREA_ID").alias("RATE_AREA_ID"),
    F.col("DSLink1014.MBR_ENR_TERM_DT_SK").alias("MBR_ENR_TERM_DT_SK"),
    F.col("DSLink1014.PRM_AMT").alias("PRM_AMT"),
    F.col("DSLink1014.ENR_PERD_ACTVTY_CD").alias("ENR_PERD_ACTVTY_CD"),
    F.col("DSLink1034.MBR_ENR_EFF_DT_SK").alias("MBR_ENR_EFF_DT_SK_1"),
    F.col("DSLink1034.MBR_ENR_TERM_DT_SK").alias("MBR_ENR_TERM_DT_SK_1"),
    F.col("DSLink1034.SUB_ADDR_ZIP_CD_5").alias("SUB_ADDR_ZIP_CD_5")
)

df_Lookup_1025_DSLink1036 = df_Lookup_1025_joined.select(
    F.col("DSLink1014.*")
)

# -----------------------------------------------------------------------------
# Transformer_1053
# -----------------------------------------------------------------------------
df_Transformer_1053 = df_Lookup_1025_DSLink10.withColumn(
    "svEffDt",
    F.regexp_replace(F.col("MBR_ENR_EFF_DT_SK"), "-", "")
).withColumn(
    "svEffDt1",
    F.regexp_replace(F.col("MBR_ENR_EFF_DT_SK_1"), "-", "")
)

df_Transformer_1053 = df_Transformer_1053.withColumn(
    "svLogicChk",
    F.when(F.col("svEffDt") != F.col("svEffDt1"), F.lit("Yes")).otherwise(F.lit("No"))
)

df_Transformer_1053_DSLink1027 = df_Transformer_1053.filter(F.col("svLogicChk") == "Yes").select(
    "SUB_INDV_BE_KEY",
    "MBR_INDV_BE_KEY",
    "QHP_ID",
    "MBR_ENR_EFF_DT_SK",
    "RATE_AREA_ID",
    "MBR_ENR_TERM_DT_SK",
    "PRM_AMT",
    "ENR_PERD_ACTVTY_CD",
    "MBR_ENR_EFF_DT_SK_1",
    "MBR_ENR_TERM_DT_SK_1",
    "SUB_ADDR_ZIP_CD_5"
)

# -----------------------------------------------------------------------------
# Transformer_1028
# -----------------------------------------------------------------------------
df_Transformer_1028 = df_Transformer_1053_DSLink1027.withColumn(
    "svEffDt",
    F.regexp_replace(F.col("MBR_ENR_EFF_DT_SK"), "-", "")
).withColumn(
    "svTrmDt",
    F.regexp_replace(F.col("MBR_ENR_TERM_DT_SK"), "-", "")
).withColumn(
    "svEffDt1",
    F.regexp_replace(F.col("MBR_ENR_EFF_DT_SK_1"), "-", "")
).withColumn(
    "svTrmDt1",
    F.regexp_replace(F.col("MBR_ENR_TERM_DT_SK_1"), "-", "")
)

df_Transformer_1028 = df_Transformer_1028.withColumn(
    "svLogicChk2",
    F.when(
        (F.col("svEffDt1") < F.col("svTrmDt")) & (F.col("svTrmDt1") > F.col("svEffDt")),
        F.lit("Yes")
    ).otherwise(F.lit("No"))
).withColumn(
    "svPostLogicChk",
    F.when(
        F.col("svTrmDt1") < F.col("svEffDt"),
        F.lit("Post")
    ).otherwise(F.lit("No"))
)

df_Transformer_1028_DSLink1043 = df_Transformer_1028.filter(F.col("svLogicChk2") == "Yes").select(
    "SUB_INDV_BE_KEY",
    "MBR_INDV_BE_KEY",
    "QHP_ID",
    "MBR_ENR_EFF_DT_SK",
    "RATE_AREA_ID",
    "MBR_ENR_TERM_DT_SK",
    "PRM_AMT",
    "ENR_PERD_ACTVTY_CD"
)

df_Transformer_1028_DSLink1098 = df_Transformer_1028.filter(F.col("svLogicChk2") == "No").select(
    "SUB_INDV_BE_KEY",
    "MBR_INDV_BE_KEY",
    "QHP_ID",
    "MBR_ENR_EFF_DT_SK",
    "RATE_AREA_ID",
    "MBR_ENR_TERM_DT_SK",
    "PRM_AMT",
    "ENR_PERD_ACTVTY_CD",
    "SUB_ADDR_ZIP_CD_5"
)

# -----------------------------------------------------------------------------
# Lookup_1044 (PxLookup)
#   PrimaryLink = DSLink1043
#   LookupLink = Ref_Old_W_Table
# -----------------------------------------------------------------------------
jdbc_url_Ref_Old_W_Table, jdbc_props_Ref_Old_W_Table = get_db_config(edw_secret_name)
extract_query_Ref_Old_W_Table = f"""
--NewSpan
with mbr as (
Select mbr.SUB_SK,we.SUB_INDV_BE_KEY, we.MBR_INDV_BE_KEY,mbr.sub_uniq_key,we.MBR_ENR_EFF_DT_SK,we.MBR_ENR_TERM_DT_SK,we.PRM_AMT,we.ENR_PERD_ACTVTY_CD,we.RATE_AREA_ID,we.QHP_ID
FROM     {EDWOwner}.MBR_D mbr,
         {EDWOwner}.MBR_ENR_D mbrEnr,
         {EDWOwner}.GRP_D grp,
         {EDWOwner}.MBR_ENR_QHP_D MBR_QHP,
         {EDWOwner}.SUB_D sub,{EDWOwner}.w_edge_mbr_elig_extr we
WHERE    mbr.MBR_SK = mbrEnr.MBR_SK
AND      mbr.GRP_SK = grp.GRP_SK
AND      MBR_QHP.MBR_ENR_SK = mbrEnr.MBR_ENR_SK
AND      MBR_QHP.MBR_UNIQ_KEY = mbrEnr.MBR_UNIQ_KEY
AND      MBR_QHP.MBR_ENR_EFF_DT_SK = mbrEnr.MBR_ENR_EFF_DT_SK
AND      mbrEnr.MBR_ENR_ELIG_IN = 'Y'
AND      mbrEnr.MBR_ENR_CLS_PLN_PROD_CAT_CD = 'MED'
AND      MBR_QHP.MBR_ENR_CLS_PLN_PROD_CAT_CD = 'MED'
AND      mbrEnr.MBR_ENR_EFF_DT_SK <= '{EndDate}'
AND      mbrEnr.MBR_ENR_TERM_DT_SK >= '{BeginDt}'
AND      mbrEnr.PROD_SH_NM IN ('PCB', 'PC', 'BCARE', 'BLUE-ACCESS', 'BLUE-SELECT','BLUESELECT+')
AND      mbrEnr.CLS_ID <> 'MHIP'
AND      MBR_QHP.QHP_ID <> 'NA'
AND      MBR_QHP.QHP_ID LIKE '{QHPID}'
AND      mbr.SUB_UNIQ_KEY = sub.SUB_UNIQ_KEY
AND      left(mbr.MBR_RELSHP_NM,3) = 'SUB'
and we.SUB_INDV_BE_KEY = we.mbr_indv_be_key
and we.sub_indv_be_key = mbr.mbr_indv_be_key
)
--=======================================================================================================
--Extract all update dates from sub_addr_audit_d in span
,all_Sub_Add_Audit as (
select distinct m.sub_sk,m.mbr_indv_be_key,m.qhp_id,saad.SUB_ADDR_AUDIT_ROW_ID
,saad.SUB_ADDR_ZIP_CD_5,saad.LAST_UPDT_RUN_CYC_EXCTN_DT_SK from {EDWOwner}.sub_addr_audit_d saad, mbr m
where saad.sub_sk = m.sub_sk 
and saad.SUB_ADDR_NM ='SUBSCRIBER HOME'
and saad.LAST_UPDT_RUN_CYC_EXCTN_DT_SK <> ''
and  saad.LAST_UPDT_RUN_CYC_EXCTN_DT_SK <= DATE(M.MBR_ENR_EFF_DT_SK) - 1 DAY
)
--=======================================================================================================
--Below query extracts lastest zip code update if memebr have multiple zip codes in same month for 1st month span
,Mnth_dtn_sub_addr_audit_zip1 as (
select asaa.mbr_indv_be_key,asaa.QHP_ID
,max(asaa.SUB_ADDR_AUDIT_ROW_ID) as SUB_ADDR_AUDIT_ROW_ID
,substr(replace(asaa.LAST_UPDT_RUN_CYC_EXCTN_DT_SK,'-',''),1,6) as mnth_LAST_UPDT_RUN_CYC_EXCTN_DT_SK
from all_Sub_Add_Audit asaa
group by asaa.mbr_indv_be_key,asaa.QHP_ID,substr(replace(asaa.LAST_UPDT_RUN_CYC_EXCTN_DT_SK,'-',''),1,6)
)
--=======================================================================================================
,Mnth_dtn_sub_addr_audit_zip as (
select asaa.mbr_indv_be_key
,max(asaa.SUB_ADDR_AUDIT_ROW_ID) as SUB_ADDR_AUDIT_ROW_ID
,substr(replace(asaa.LAST_UPDT_RUN_CYC_EXCTN_DT_SK,'-',''),1,6) as mnth_LAST_UPDT_RUN_CYC_EXCTN_DT_SK
from all_Sub_Add_Audit asaa
where asaa.LAST_UPDT_RUN_CYC_EXCTN_DT_SK between DATE('{BeginDt}') - 1 YEAR AND DATE('{EndDate}') - 1 YEAR
group by asaa.mbr_indv_be_key,substr(replace(asaa.LAST_UPDT_RUN_CYC_EXCTN_DT_SK,'-',''),1,6)
)
--=======================================================================================================
,fnl_dtn_sub_addr_audit_zip1 as (
select distinct asaa.mbr_indv_be_key,asaa.QHP_ID,asaa.SUB_ADDR_AUDIT_ROW_ID,asaa.SUB_ADDR_ZIP_CD_5,asaa.LAST_UPDT_RUN_CYC_EXCTN_DT_SK from all_Sub_Add_Audit asaa, Mnth_dtn_sub_addr_audit_zip1 mdsaa ,
(
select asaa2.mbr_indv_be_key,asaa2.QHP_ID,asaa2.SUB_ADDR_AUDIT_ROW_ID,row_number() over (partition by asaa2.mbr_indv_be_key,asaa2.QHP_ID,asaa2.LAST_UPDT_RUN_CYC_EXCTN_DT_SK order by asaa2.mbr_indv_be_key,asaa2.QHP_ID,asaa2.LAST_UPDT_RUN_CYC_EXCTN_DT_SK asc ) as row_num from all_Sub_Add_Audit asaa2
) as assaaSub where asaa.mbr_indv_be_key = mdsaa.mbr_indv_be_key and asaa.QHP_ID = mdsaa.QHP_ID AND asaa.SUB_ADDR_AUDIT_ROW_ID = mdsaa.SUB_ADDR_AUDIT_ROW_ID and asaa.mbr_indv_be_key = assaaSub.mbr_indv_be_key and asaa.SUB_ADDR_AUDIT_ROW_ID = assaaSub.SUB_ADDR_AUDIT_ROW_ID and assaaSub.row_num = 1)
--=======================================================================================================
,fnl_dtn_sub_addr_audit_zip as (
select distinct asaa.mbr_indv_be_key,asaa.SUB_ADDR_AUDIT_ROW_ID,asaa.SUB_ADDR_ZIP_CD_5,asaa.LAST_UPDT_RUN_CYC_EXCTN_DT_SK 
from all_Sub_Add_Audit asaa, Mnth_dtn_sub_addr_audit_zip mdsaa,
(select asaa2.mbr_indv_be_key,asaa2.SUB_ADDR_AUDIT_ROW_ID,asaa2.SUB_ADDR_ZIP_CD_5 
,row_number() over (partition by asaa2.mbr_indv_be_key,asaa2.SUB_ADDR_ZIP_CD_5 order by asaa2.mbr_indv_be_key,asaa2.SUB_ADDR_ZIP_CD_5,asaa2.LAST_UPDT_RUN_CYC_EXCTN_DT_SK asc) as row_num
from all_Sub_Add_Audit asaa2) as assaaSub
where asaa.mbr_indv_be_key = mdsaa.mbr_indv_be_key and asaa.SUB_ADDR_AUDIT_ROW_ID = mdsaa.SUB_ADDR_AUDIT_ROW_ID and asaa.mbr_indv_be_key = assaaSub.mbr_indv_be_key and asaa.SUB_ADDR_AUDIT_ROW_ID = assaaSub.SUB_ADDR_AUDIT_ROW_ID and assaaSub.row_num = 1)
--=======================================================================================================
,MinEffDt as (
select fdsaa.mbr_indv_be_key,min(m.MBR_ENR_EFF_DT_SK) as MBR_ENR_EFF_DT_SK
from fnl_dtn_sub_addr_audit_zip fdsaa, mbr m where fdsaa.mbr_indv_be_key = m. sub_indv_be_Key group by fdsaa.mbr_indv_be_key)
--=======================================================================================================
,first_Sub_Add_Audit_Zip as (
select distinct fsaaz.mbr_indv_be_key,fsaaz.SUB_ADDR_AUDIT_ROW_ID,fsaaz.LAST_UPDT_RUN_CYC_EXCTN_DT_SK,fsaaz.SUB_ADDR_ZIP_CD_5,row_number() over ( partition by fsaaz.mbr_indv_be_key order by fsaaz.mbr_indv_be_key,fsaaz.SUB_ADDR_AUDIT_ROW_ID asc) row_num from fnl_dtn_sub_addr_audit_zip fsaaz)
--=======================================================================================================
,trueFirst_Sub_Add_Audit_Zip as (
select distinct fdsaa.mbr_indv_be_key,fdsaa.SUB_ADDR_AUDIT_ROW_ID,fdsaa.SUB_ADDR_ZIP_CD_5,fdsaa.LAST_UPDT_RUN_CYC_EXCTN_DT_SK,m.MBR_ENR_EFF_DT_SK
from fnl_dtn_sub_addr_audit_zip fdsaa, first_Sub_Add_Audit_Zip fsaa,MinEffDt m
where fdsaa.SUB_ADDR_AUDIT_ROW_ID = fsaa.SUB_ADDR_AUDIT_ROW_ID and fdsaa.mbr_indv_be_key = fsaa.mbr_indv_be_key and fdsaa.mbr_indv_be_key = m.mbr_indv_be_key and month(fsaa.LAST_UPDT_RUN_CYC_EXCTN_DT_SK) = month(m.MBR_ENR_EFF_DT_SK) and fsaa.row_num = 1 
)
--=======================================================================================================
,uniq_Sub_Add_Audit_Zip as (
select fdasaaz.mbr_indv_be_key,fdasaaz.SUB_ADDR_AUDIT_ROW_ID,fdasaaz.SUB_ADDR_ZIP_CD_5,fdasaaz.LAST_UPDT_RUN_CYC_EXCTN_DT_SK from fnl_dtn_sub_addr_audit_zip fdasaaz, trueFirst_Sub_Add_Audit_Zip tsaaz
where tsaaz.mbr_indv_be_key = fdasaaz.mbr_indv_be_key and tsaaz.SUB_ADDR_AUDIT_ROW_ID != fdasaaz.SUB_ADDR_AUDIT_ROW_ID
and tsaaz.LAST_UPDT_RUN_CYC_EXCTN_DT_SK != fdasaaz.LAST_UPDT_RUN_CYC_EXCTN_DT_SK union
select fdasaaz.mbr_indv_be_key,fdasaaz.SUB_ADDR_AUDIT_ROW_ID,fdasaaz.SUB_ADDR_ZIP_CD_5,fdasaaz.LAST_UPDT_RUN_CYC_EXCTN_DT_SK from fnl_dtn_sub_addr_audit_zip fdasaaz where fdasaaz.mbr_indv_be_key not in (select distinct tsaaz.mbr_indv_be_key from trueFirst_Sub_Add_Audit_Zip tsaaz))
--=======================================================================================================
,mbr_zip_Single_Updt as (
select usaaz.mbr_indv_be_key,count(usaaz.SUB_ADDR_ZIP_CD_5) as count_zip from fnl_dtn_sub_addr_audit_zip usaaz group by usaaz.mbr_indv_be_key having count(usaaz.SUB_ADDR_ZIP_CD_5) = 1)
--=======================================================================================================
,Final_NewSpan as ( 
select distinct m.SUB_INDV_BE_KEY , m.MBR_INDV_BE_KEY ,usaaz.SUB_ADDR_ZIP_CD_5 ,m.QHP_ID,usaaz.SUB_ADDR_AUDIT_ROW_ID,usaaz.LAST_UPDT_RUN_CYC_EXCTN_DT_SK from mbr m, uniq_Sub_Add_Audit_Zip usaaz left outer join mbr_zip_Single_Updt mzsu on usaaz.mbr_indv_be_key = mzsu.mbr_indv_be_key where m.mbr_indv_be_key = usaaz.mbr_indv_be_key and usaaz.LAST_UPDT_RUN_CYC_EXCTN_DT_SK between m.MBR_ENR_EFF_DT_SK and m.MBR_ENR_TERM_DT_SK)
--=======================================================================================================
,MinFinal_NewSpan as (
select fns.SUB_INDV_BE_KEY, fns.MBR_INDV_BE_KEY, fns.QHP_ID,min(fns.SUB_ADDR_AUDIT_ROW_ID) min_SUB_ADDR_AUDIT_ROW_ID
from Final_NewSpan fns
group by fns.SUB_INDV_BE_KEY, fns.MBR_INDV_BE_KEY, fns.QHP_ID)
--=======================================================================================================
,Pre_final as ( 
select distinct fdsaaz1.mbr_indv_be_key as SUB_INDV_BE_KEY, fdsaaz1.mbr_indv_be_key,fdsaaz1.QHP_ID ,fdsaaz1.SUB_ADDR_ZIP_CD_5 ,fdsaaz1.SUB_ADDR_AUDIT_ROW_ID ,fdsaaz1.LAST_UPDT_RUN_CYC_EXCTN_DT_SK 
from fnl_dtn_sub_addr_audit_zip1 fdsaaz1 ,Final_NewSpan fn ,MinFinal_NewSpan mfns
where fdsaaz1.mbr_indv_be_key = fn.mbr_indv_be_key and fdsaaz1.QHP_ID = fn.QHP_ID and fn.SUB_INDV_BE_KEY = mfns.SUB_INDV_BE_KEY and fn.QHP_ID = mfns.QHP_ID and fn.SUB_ADDR_AUDIT_ROW_ID = mfns.min_SUB_ADDR_AUDIT_ROW_ID and fdsaaz1.LAST_UPDT_RUN_CYC_EXCTN_DT_SK != fn.LAST_UPDT_RUN_CYC_EXCTN_DT_SK and fdsaaz1.SUB_ADDR_AUDIT_ROW_ID < fn.SUB_ADDR_AUDIT_ROW_ID
)
--=======================================================================================================
select distinct pf.SUB_INDV_BE_KEY,pf.SUB_INDV_BE_KEY as MBR_INDV_BE_KEY,pf.SUB_ADDR_ZIP_CD_5 as Ref_SUB_ADDR_ZIP_CD_5,'Yes' as Flag_1 from Pre_final pf, (select m.sub_indv_be_Key,max(m.SUB_ADDR_AUDIT_ROW_ID) as SUB_ADDR_AUDIT_ROW_ID from Pre_final m group by m.sub_indv_be_Key) pfm,mbr m where pf.SUB_INDV_BE_KEY = pfm.SUB_INDV_BE_KEY and pf.SUB_ADDR_AUDIT_ROW_ID = pfm.SUB_ADDR_AUDIT_ROW_ID and pf.SUB_INDV_BE_KEY = m.SUB_INDV_BE_KEY
"""
df_Ref_Old_W_Table = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_Ref_Old_W_Table)
    .options(**jdbc_props_Ref_Old_W_Table)
    .option("query", extract_query_Ref_Old_W_Table)
    .load()
)

df_Lookup_1044_joined = df_Transformer_1028_DSLink1043.alias("DSLink1043").join(
    df_Ref_Old_W_Table.alias("ref1"),
    on=[
        F.col("DSLink1043.SUB_INDV_BE_KEY") == F.col("ref1.SUB_INDV_BE_KEY"),
        F.col("DSLink1043.MBR_INDV_BE_KEY") == F.col("ref1.MBR_INDV_BE_KEY")
    ],
    how="inner"
)

df_Lookup_1044_DSLink1046 = df_Lookup_1044_joined.select(
    F.col("DSLink1043.SUB_INDV_BE_KEY").alias("SUB_INDV_BE_KEY"),
    F.col("DSLink1043.MBR_INDV_BE_KEY").alias("MBR_INDV_BE_KEY"),
    F.col("DSLink1043.QHP_ID").alias("QHP_ID"),
    F.col("DSLink1043.MBR_ENR_EFF_DT_SK").alias("MBR_ENR_EFF_DT_SK"),
    F.col("DSLink1043.RATE_AREA_ID").alias("RATE_AREA_ID"),
    F.col("DSLink1043.MBR_ENR_TERM_DT_SK").alias("MBR_ENR_TERM_DT_SK"),
    F.col("DSLink1043.PRM_AMT").alias("PRM_AMT"),
    F.col("DSLink1043.ENR_PERD_ACTVTY_CD").alias("ENR_PERD_ACTVTY_CD"),
    F.col("ref1.Ref_SUB_ADDR_ZIP_CD_5").alias("SUB_ADDR_ZIP_CD_5")
)

# -----------------------------------------------------------------------------
# Copy_1037 => from Lookup_1025_DSLink1036 => DSLink989
# -----------------------------------------------------------------------------
df_Copy_1037_DSLink989 = df_Lookup_1025_DSLink1036.select(
    F.col("SUB_INDV_BE_KEY"),
    F.col("MBR_INDV_BE_KEY"),
    F.col("QHP_ID"),
    F.col("MBR_ENR_EFF_DT_SK"),
    F.col("RATE_AREA_ID"),
    F.col("MBR_ENR_TERM_DT_SK"),
    F.col("PRM_AMT"),
    F.col("ENR_PERD_ACTVTY_CD")
)

# -----------------------------------------------------------------------------
# Lookup_990 (PxLookup)
#   PrimaryLink = DSLink989
#   LookupLink = DSLink998 (inner)
# -----------------------------------------------------------------------------
df_Lookup_990_joined = df_Copy_1037_DSLink989.alias("DSLink989").join(
    df_Copy_996_DSLink998.alias("DSLink998"),
    on=[
        F.col("DSLink989.SUB_INDV_BE_KEY") == F.col("DSLink998.SUB_INDV_BE_KEY"),
        F.col("DSLink989.MBR_INDV_BE_KEY") == F.col("DSLink998.MBR_INDV_BE_KEY"),
        F.col("DSLink989.QHP_ID") == F.col("DSLink998.QHP_ID"),
        F.col("DSLink989.MBR_ENR_EFF_DT_SK") == F.col("DSLink998.MBR_ENR_EFF_DT_SK"),
        F.col("DSLink989.RATE_AREA_ID") == F.col("DSLink998.RATE_AREA_ID"),
        F.col("DSLink989.MBR_ENR_TERM_DT_SK") == F.col("DSLink998.MBR_ENR_TERM_DT_SK"),
        F.col("DSLink989.PRM_AMT") == F.col("DSLink998.PRM_AMT"),
        F.col("DSLink989.ENR_PERD_ACTVTY_CD") == F.col("DSLink998.ENR_PERD_ACTVTY_CD")
    ],
    how="inner"
)
df_Lookup_990_DSLink992 = df_Lookup_990_joined.select(
    F.col("DSLink989.SUB_INDV_BE_KEY").alias("SUB_INDV_BE_KEY"),
    F.col("DSLink989.MBR_INDV_BE_KEY").alias("MBR_INDV_BE_KEY"),
    F.col("DSLink989.QHP_ID").alias("QHP_ID"),
    F.col("DSLink989.MBR_ENR_EFF_DT_SK").alias("MBR_ENR_EFF_DT_SK"),
    F.col("DSLink989.RATE_AREA_ID").alias("RATE_AREA_ID"),
    F.col("DSLink989.MBR_ENR_TERM_DT_SK").alias("MBR_ENR_TERM_DT_SK"),
    F.col("DSLink989.PRM_AMT").alias("PRM_AMT"),
    F.col("DSLink989.ENR_PERD_ACTVTY_CD").alias("ENR_PERD_ACTVTY_CD"),
    F.col("DSLink998.SUB_ADDR_ZIP_CD_5").alias("SUB_ADDR_ZIP_CD_5")
)

# -----------------------------------------------------------------------------
# Funnel_957 (PxFunnel)
#   Inputs: RD_Post_Post, RD_Pre_Pre, DSLink954, DSLink992, DSLink1098, DSLink1046
# -----------------------------------------------------------------------------
df_Funnel_957 = df_RD_Post_Post.unionByName(df_RD_Pre_Pre, allowMissingColumns=True) \
    .unionByName(df_Copy_950_DSLink954, allowMissingColumns=True) \
    .unionByName(df_Lookup_990_DSLink992, allowMissingColumns=True) \
    .unionByName(df_Transformer_1028_DSLink1098, allowMissingColumns=True) \
    .unionByName(df_Lookup_1044_DSLink1046, allowMissingColumns=True)

# -----------------------------------------------------------------------------
# Sort_1144 (PxSort)
# -----------------------------------------------------------------------------
df_Sort_1144 = df_Funnel_957.withColumn("keyChange", F.lit(None).cast(StringType()))

# -----------------------------------------------------------------------------
# Filter_1151 (PxFilter - No conditions => pass all, no rejects)
# -----------------------------------------------------------------------------
df_Filter_1151 = df_Sort_1144

# -----------------------------------------------------------------------------
# Remove_Duplicates_1001 (PxRemDup)
# -----------------------------------------------------------------------------
df_Remove_Duplicates_1001 = dedup_sort(
    df_Filter_1151,
    partition_cols=["SUB_INDV_BE_KEY","MBR_INDV_BE_KEY","MBR_ENR_EFF_DT_SK","MBR_ENR_TERM_DT_SK","QHP_ID","PRM_AMT","RATE_AREA_ID","ENR_PERD_ACTVTY_CD","SUB_ADDR_ZIP_CD_5"],
    sort_cols=[]
)

df_Remove_Duplicates_1001_out = df_Remove_Duplicates_1001.select(
    F.col("SUB_INDV_BE_KEY"),
    F.col("MBR_INDV_BE_KEY"),
    F.col("QHP_ID"),
    F.col("MBR_ENR_EFF_DT_SK").alias("W_MBR_ENR_EFF_DT_SK"),
    F.col("MBR_ENR_TERM_DT_SK").alias("W_MBR_ENR_TERM_DT_SK"),
    F.col("PRM_AMT"),
    F.col("ENR_PERD_ACTVTY_CD"),
    F.col("RATE_AREA_ID"),
    F.col("SUB_ADDR_ZIP_CD_5")
)

# -----------------------------------------------------------------------------
# Sort_1121 (PxSort)
# -----------------------------------------------------------------------------
df_Sort_1121 = df_Remove_Duplicates_1001_out  # no actual changes except sorting
df_Sort_1121_out = df_Sort_1121.select(
    F.col("SUB_INDV_BE_KEY"),
    F.col("MBR_INDV_BE_KEY"),
    F.col("QHP_ID"),
    F.col("W_MBR_ENR_EFF_DT_SK"),
    F.col("W_MBR_ENR_TERM_DT_SK"),
    F.col("PRM_AMT"),
    F.col("ENR_PERD_ACTVTY_CD"),
    F.col("RATE_AREA_ID"),
    F.col("SUB_ADDR_ZIP_CD_5")
)

# -----------------------------------------------------------------------------
# Transformer_1124 (CTransformerStage)
# -----------------------------------------------------------------------------
df_Transformer_1124 = df_Sort_1121_out.withColumn("svCrntInBeKey", F.col("MBR_INDV_BE_KEY")) \
    .withColumn("svCrntSubInBeKey", F.col("SUB_INDV_BE_KEY")) \
    .withColumn("svCrntQHP", F.col("QHP_ID")) \
    .withColumn("svCrntEED", F.col("W_MBR_ENR_EFF_DT_SK")) \
    .withColumn("svCrntETD", F.col("W_MBR_ENR_TERM_DT_SK"))

df_Transformer_1124 = df_Transformer_1124.withColumn(
    "svChk",
    F.when(
        (F.col("svCrntInBeKey") == F.col("svPrevInBeKey"))
        & (F.col("svCrntSubInBeKey") == F.col("svPrevSubInBeKey"))
        & (F.col("svCrntQHP") == F.col("svPrevQHP")),
        F.when(
            (F.col("svCrntEED") < F.col("svPrevEED")) & (F.col("svCrntETD") < F.col("svPrevEED")),
            F.col("svCrntETD")
        ).otherwise(F.lit("<...>"))  # The exact expression references a function FIND.DATE.EE not defined
    ).otherwise(F.col("svCrntETD"))
)

df_Transformer_1124 = df_Transformer_1124.withColumn("svPrevInBeKey", F.col("svCrntInBeKey"))
df_Transformer_1124 = df_Transformer_1124.withColumn("svPrevSubInBeKey", F.col("svCrntSubInBeKey"))
df_Transformer_1124 = df_Transformer_1124.withColumn("svPrevQHP", F.col("svCrntQHP"))
df_Transformer_1124 = df_Transformer_1124.withColumn("svPrevEED", F.col("svCrntEED"))
df_Transformer_1124 = df_Transformer_1124.withColumn("svPrevETD", F.col("svCrntETD"))

df_Transformer_1124_out = df_Transformer_1124.select(
    F.col("SUB_INDV_BE_KEY"),
    F.col("MBR_INDV_BE_KEY"),
    F.col("QHP_ID"),
    F.col("W_MBR_ENR_EFF_DT_SK"),
    F.col("svChk").alias("W_MBR_ENR_TERM_DT_SK"),
    F.col("PRM_AMT"),
    F.col("ENR_PERD_ACTVTY_CD"),
    F.col("RATE_AREA_ID"),
    F.col("SUB_ADDR_ZIP_CD_5")
)

# -----------------------------------------------------------------------------
# Copy_of_w_edge_mbr_elig_extr_Updt (PxSequentialFile) - Final Output
# -----------------------------------------------------------------------------
# Before writing, apply rpad for columns with SqlType char or varchar in the final schema
df_final = df_Transformer_1124_out.select(
    F.col("SUB_INDV_BE_KEY"),
    F.col("MBR_INDV_BE_KEY"),
    F.col("QHP_ID"),
    F.rpad(F.col("W_MBR_ENR_EFF_DT_SK"), 10, " ").alias("W_MBR_ENR_EFF_DT_SK"),
    F.rpad(F.col("W_MBR_ENR_TERM_DT_SK"), 10, " ").alias("W_MBR_ENR_TERM_DT_SK"),
    F.col("PRM_AMT"),
    F.col("ENR_PERD_ACTVTY_CD"),
    F.col("RATE_AREA_ID"),
    F.rpad(F.col("SUB_ADDR_ZIP_CD_5"), 5, " ").alias("SUB_ADDR_ZIP_CD_5")
)

write_files(
    df_final,
    f"{adls_path_raw}/landing/w_edge_mbr_elig_extr_updt.dat",
    delimiter="|",
    mode="overwrite",
    is_parquet=False,
    header=True,
    quote="\"",
    nullValue=None
)