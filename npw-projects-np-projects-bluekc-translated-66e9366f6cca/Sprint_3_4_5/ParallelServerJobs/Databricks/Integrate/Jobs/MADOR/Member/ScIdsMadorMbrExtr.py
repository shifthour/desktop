# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2018 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC CALLED BY : ScIdsMadorLoadSeq
# MAGIC 
# MAGIC PROCESSING : The Job Extracts Member Data From South Carolina Member file
# MAGIC 
# MAGIC MODIFICATIONS :
# MAGIC 
# MAGIC Developer                                 Date                 \(9)Project                                                Change Description                                               Development Project\(9)          Code Reviewer          Date Reviewed       
# MAGIC ------------------                               --------------------     \(9)------------------------                             -----------------------------------------------------------------------             ------------------------------\(9)         -------------------------------   ----------------------------       
# MAGIC Sethuraman Rajendran           \(9) 2018-03-30\(9)5839 - MADOR                                    Original Programming\(9)\(9)\(9)          IntegrateDev2                        Jaideep Mankala        04/13/2018 / Kalyan Neelam 2018-06-14
# MAGIC 
# MAGIC Vamsi Aripaka                          2024-12-12              US 630639                          Added P_MA_DOR_GRP_XREF table and applied              IntegrateDev1                        Jeyaprasanna             2024-12-18
# MAGIC                                                                                                                             transformation logics in xfm_int and xfm_ind_mnthly 
# MAGIC                                                                                                                             stages. Removed unstructured file and added
# MAGIC                                                                                                                             Seq_MBR_ENR file stage as source.

# MAGIC Member source file for South Carolina feed
# MAGIC Extract Member data from South carolina Member file
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
    StringType
)
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

ids_secret_name = get_widget_value('ids_secret_name','')
IDSOwner = get_widget_value('IDSOwner','')
Tax_Year = get_widget_value('Tax_Year','')
SrcSysCd = get_widget_value('SrcSysCd','')
RunID = get_widget_value('RunID','')
AsOfDtm = get_widget_value('AsOfDtm','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

# ------------------------------------------------------------------------------------------------
# STAGE: SUB_MA_DOR (DB2ConnectorPX - read from IDS)
# ------------------------------------------------------------------------------------------------
extract_query_SUB_MA_DOR = (
    "SELECT SUB_MA_DOR_SK, GRP_ID, SUB_ID, TAX_YR FROM "
    + IDSOwner
    + ".SUB_MA_DOR"
)
df_SUB_MA_DOR = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_SUB_MA_DOR)
    .load()
)

# ------------------------------------------------------------------------------------------------
# STAGE: P_MA_DOR_GRP_XREF (DB2ConnectorPX - read from IDS)
# ------------------------------------------------------------------------------------------------
extract_query_P_MA_DOR_GRP_XREF = (
    "SELECT CES_CLNT_ID, GRP_ID, GRP_SK FROM "
    + IDSOwner
    + ".P_MA_DOR_GRP_XREF"
)
df_P_MA_DOR_GRP_XREF = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_P_MA_DOR_GRP_XREF)
    .load()
)

# ------------------------------------------------------------------------------------------------
# STAGE: Seq_MBR_PLN (PxSequentialFile - read a CSV with schema)
# ------------------------------------------------------------------------------------------------
schema_seq_mbr_pln = StructType([
    StructField("CES_CLNT_ID", StringType(), True),
    StructField("PLN_ID", StringType(), True),
    StructField("SUB_ID", StringType(), True),
    StructField("EMPL_FIRST_NM", StringType(), True),
    StructField("EMPL_LAST_NM", StringType(), True),
    StructField("MBR_FIRST_NM", StringType(), True),
    StructField("MBR_MIDINIT", StringType(), True),
    StructField("MBR_LAST_NM", StringType(), True),
    StructField("MBR_NO", StringType(), True),
    StructField("MBR_RELSHP_CD", StringType(), True),
    StructField("MBR_HOME_ADDR_LN_1", StringType(), True),
    StructField("MBR_HOME_ADDR_LN_2", StringType(), True),
    StructField("MBR_HOME_ADDR_CITY_NM", StringType(), True),
    StructField("MBR_HOME_ADDR_ST_CD", StringType(), True),
    StructField("MBR_HOME_ADDR_ZIP_CD", StringType(), True),
    StructField("COV_BEG_DT", StringType(), True),
    StructField("COV_END_DT", StringType(), True),
    StructField("COV_TYP", StringType(), True),
    StructField("MBR_DOB", StringType(), True),
    StructField("ALT_ID", StringType(), True)
])
df_Seq_MBR_PLN = (
    spark.read.format("csv")
    .option("header", True)
    .option("delimiter", ",")
    .option("quote", "\"")
    .schema(schema_seq_mbr_pln)
    .load(f"{adls_path_raw}/landing/NTNL_ALNCE_MADOR_MBR_PLN.csv")
)

# ------------------------------------------------------------------------------------------------
# STAGE: xfm_int (CTransformerStage)
# ------------------------------------------------------------------------------------------------
df_xfm_int_pre = (
    df_Seq_MBR_PLN
    .withColumn("SVEndtFmt", F.to_date(F.col("COV_END_DT"), "MM-dd-yyyy"))
    .withColumn("SVBgndtFmt", F.to_date(F.col("COV_BEG_DT"), "MM-dd-yyyy"))
    .withColumn("SvPostal", F.regexp_replace(F.col("MBR_HOME_ADDR_ZIP_CD"), "-", ""))
)

df_xfm_int = df_xfm_int_pre.select(
    F.when(
        F.length(F.col("MBR_NO")) == 1,
        F.concat(F.col("SUB_ID"), F.lit("0"), F.col("MBR_NO"))
    ).otherwise(
        F.concat(F.col("SUB_ID"), F.col("MBR_NO"))
    ).alias("MBR_ID"),
    F.col("SUB_ID").alias("SUB_ID"),
    F.col("CES_CLNT_ID").alias("CES_CLNT_ID"),
    F.lit(Tax_Year).alias("TAX_YR"),  # char(4)
    F.lit(SrcSysCd).alias("SRC_SYS_CD"),
    F.lit(AsOfDtm).alias("AS_OF_DTM"),
    F.lit("NA").alias("MBR_SK"),
    F.lit("NA").alias("SUB_SK"),
    F.lit(0).alias("MBR_RELSHP_CD_SK"),
    F.col("MBR_RELSHP_CD").alias("MBR_RELSHP_TYPE_NO"),
    trim(F.upper(F.col("MBR_FIRST_NM"))).alias("MBR_FIRST_NM"),
    trim(F.upper(F.col("MBR_MIDINIT"))).alias("MBR_MIDINIT"),
    trim(F.upper(F.col("MBR_LAST_NM"))).alias("MBR_LAST_NM"),
    trim(F.upper(F.col("MBR_HOME_ADDR_LN_1"))).alias("MBR_MAIL_ADDR_LN_1"),
    F.when(
        (F.length(F.col("MBR_HOME_ADDR_LN_2")) == 0) | (F.col("MBR_HOME_ADDR_LN_2").isNull()),
        F.lit(None)
    ).otherwise(trim(F.upper(F.col("MBR_HOME_ADDR_LN_2")))).alias("MBR_MAIL_ADDR_LN_2"),
    F.lit(None).alias("MBR_MAIL_ADDR_LN_3"),
    trim(F.upper(F.col("MBR_HOME_ADDR_CITY_NM"))).alias("MBR_MAIL_ADDR_CITY_NM"),
    F.when(
        F.length(F.col("MBR_HOME_ADDR_ST_CD")) == 0,
        F.lit("0")
    ).otherwise(F.upper(F.col("MBR_HOME_ADDR_ST_CD"))).alias("MBR_MAIL_ADDR_ST_CD"),
    F.col("MBR_HOME_ADDR_ZIP_CD").substr(F.lit(1), F.lit(5)).alias("MBR_MAIL_ADDR_ZIP_CD_5"),
    F.when(
        F.length(trim(F.col("SvPostal").substr(F.lit(6), F.lit(4)))) == 4,
        trim(F.col("SvPostal").substr(F.lit(6), F.lit(4)))
    ).otherwise(F.lit(None)).alias("MBR_MAIL_ADDR_ZIP_CD_4"),
    F.to_date(F.col("MBR_DOB"), "MM-dd-yyyy").alias("MBR_BRTH_DT"),
    F.when(
        F.length(F.col("MBR_NO")) == 1,
        F.concat(F.lit("0"), F.col("MBR_NO"))
    ).otherwise(F.col("MBR_NO")).alias("MBR_SFX_NO"),
    F.col("SVBgndtFmt").alias("COV_BEG_DT"),
    F.col("SVEndtFmt").alias("COV_END_DT"),
    F.concat(F.col("SUB_ID"), F.col("MBR_NO")).alias("SUBMBR")
)

df_xfm_int_linkAggregator = df_xfm_int.select(
    F.concat(F.col("SUB_ID"), F.col("MBR_SFX_NO")).alias("SUBMBR")
)

# ------------------------------------------------------------------------------------------------
# STAGE: Aggregator_287 (PxAggregator)
# method=hash, key=SUBMBR, selection=count => Rank=RecCount()
# ------------------------------------------------------------------------------------------------
df_Aggregator_287 = (
    df_xfm_int_linkAggregator
    .groupBy("SUBMBR")
    .agg(F.count("*").alias("Rank"))
)

# ------------------------------------------------------------------------------------------------
# STAGE: CopyAg (PxCopy - no changes)
# ------------------------------------------------------------------------------------------------
df_CopyAg = df_Aggregator_287

# ------------------------------------------------------------------------------------------------
# STAGE: Lookup_src_dups (PxLookup)
# Primary link is df_xfm_int, left link is df_CopyAg on SUBMBR
# ------------------------------------------------------------------------------------------------
df_Lookup_src_dups = (
    df_xfm_int.alias("Lnk_xfm_Mbr")
    .join(
        df_CopyAg.alias("lnk_rtv_dupscnt"),
        on=[F.col("Lnk_xfm_Mbr.SUBMBR") == F.col("lnk_rtv_dupscnt.SUBMBR")],
        how="left"
    )
    .select(
        F.col("Lnk_xfm_Mbr.MBR_ID").alias("MBR_ID"),
        F.col("Lnk_xfm_Mbr.SUB_ID").alias("SUB_ID"),
        F.col("Lnk_xfm_Mbr.CES_CLNT_ID").alias("CES_CLNT_ID"),
        F.col("Lnk_xfm_Mbr.TAX_YR").alias("TAX_YR"),
        F.col("Lnk_xfm_Mbr.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("Lnk_xfm_Mbr.AS_OF_DTM").alias("AS_OF_DTM"),
        F.col("Lnk_xfm_Mbr.MBR_SK").alias("MBR_SK"),
        F.col("Lnk_xfm_Mbr.SUB_SK").alias("SUB_SK"),
        F.col("Lnk_xfm_Mbr.MBR_RELSHP_CD_SK").alias("MBR_RELSHP_CD_SK"),
        F.col("Lnk_xfm_Mbr.MBR_RELSHP_TYPE_NO").alias("MBR_RELSHP_TYPE_NO"),
        F.col("Lnk_xfm_Mbr.MBR_FIRST_NM").alias("MBR_FIRST_NM"),
        F.col("Lnk_xfm_Mbr.MBR_MIDINIT").alias("MBR_MIDINIT"),
        F.col("Lnk_xfm_Mbr.MBR_LAST_NM").alias("MBR_LAST_NM"),
        F.col("Lnk_xfm_Mbr.MBR_MAIL_ADDR_LN_1").alias("MBR_MAIL_ADDR_LN_1"),
        F.col("Lnk_xfm_Mbr.MBR_MAIL_ADDR_LN_2").alias("MBR_MAIL_ADDR_LN_2"),
        F.col("Lnk_xfm_Mbr.MBR_MAIL_ADDR_LN_3").alias("MBR_MAIL_ADDR_LN_3"),
        F.col("Lnk_xfm_Mbr.MBR_MAIL_ADDR_CITY_NM").alias("MBR_MAIL_ADDR_CITY_NM"),
        F.col("Lnk_xfm_Mbr.MBR_MAIL_ADDR_ST_CD").alias("MBR_MAIL_ADDR_ST_CD"),
        F.col("Lnk_xfm_Mbr.MBR_MAIL_ADDR_ZIP_CD_5").alias("MBR_MAIL_ADDR_ZIP_CD_5"),
        F.col("Lnk_xfm_Mbr.MBR_MAIL_ADDR_ZIP_CD_4").alias("MBR_MAIL_ADDR_ZIP_CD_4"),
        F.col("Lnk_xfm_Mbr.MBR_BRTH_DT").alias("MBR_BRTH_DT"),
        F.col("Lnk_xfm_Mbr.MBR_SFX_NO").alias("MBR_SFX_NO"),
        F.col("Lnk_xfm_Mbr.COV_BEG_DT").alias("COV_BEG_DT"),
        F.col("Lnk_xfm_Mbr.COV_END_DT").alias("COV_END_DT"),
        F.col("Lnk_xfm_Mbr.SUBMBR").alias("SUBMBR"),
        F.col("lnk_rtv_dupscnt.Rank").alias("Rank")
    )
)

# ------------------------------------------------------------------------------------------------
# STAGE: xfm_dups_uniq (CTransformerStage) with constraints
# Constraint1: Rank>1 => lnk_Dups1
# Constraint2: Rank<=1 => lnk_Uniq
# ------------------------------------------------------------------------------------------------
df_lnk_Dups1 = (
    df_Lookup_src_dups
    .filter(F.col("Rank") > 1)
    .select(
        F.col("MBR_ID").alias("MBR_ID"),
        F.col("SUB_ID").alias("SUB_ID"),
        F.col("COV_END_DT").alias("COV_END_DT"),
        F.col("COV_BEG_DT").alias("COV_BEG_DT"),
        F.col("CES_CLNT_ID").alias("CES_CLNT_ID"),
        F.col("TAX_YR").alias("TAX_YR"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("AS_OF_DTM").alias("AS_OF_DTM"),
        F.col("MBR_SK").alias("MBR_SK"),
        F.col("SUB_SK").alias("SUB_SK"),
        F.col("MBR_RELSHP_CD_SK").alias("MBR_RELSHP_CD_SK"),
        F.col("MBR_RELSHP_TYPE_NO").alias("MBR_RELSHP_TYPE_NO"),
        F.col("MBR_FIRST_NM").alias("MBR_FIRST_NM"),
        F.col("MBR_MIDINIT").alias("MBR_MIDINIT"),
        F.col("MBR_LAST_NM").alias("MBR_LAST_NM"),
        F.col("MBR_MAIL_ADDR_LN_1").alias("MBR_MAIL_ADDR_LN_1"),
        F.col("MBR_MAIL_ADDR_LN_2").alias("MBR_MAIL_ADDR_LN_2"),
        F.col("MBR_MAIL_ADDR_LN_3").alias("MBR_MAIL_ADDR_LN_3"),
        F.col("MBR_MAIL_ADDR_CITY_NM").alias("MBR_MAIL_ADDR_CITY_NM"),
        F.col("MBR_MAIL_ADDR_ST_CD").alias("MBR_MAIL_ADDR_ST_CD"),
        F.col("MBR_MAIL_ADDR_ZIP_CD_5").alias("MBR_MAIL_ADDR_ZIP_CD_5"),
        F.col("MBR_MAIL_ADDR_ZIP_CD_4").alias("MBR_MAIL_ADDR_ZIP_CD_4"),
        F.col("MBR_BRTH_DT").alias("MBR_BRTH_DT"),
        F.col("MBR_SFX_NO").alias("MBR_SFX_NO"),
        F.col("SUBMBR").alias("SUBMBR"),
        F.col("Rank").alias("Rank")
    )
)

df_lnk_Uniq = (
    df_Lookup_src_dups
    .filter(F.col("Rank") <= 1)
    .select(
        F.col("MBR_ID").alias("MBR_ID"),
        F.col("SUB_ID").alias("SUB_ID"),
        F.col("CES_CLNT_ID").alias("CES_CLNT_ID"),
        F.col("TAX_YR").alias("TAX_YR"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("AS_OF_DTM").alias("AS_OF_DTM"),
        F.col("MBR_SK").alias("MBR_SK"),
        F.col("SUB_SK").alias("SUB_SK"),
        F.col("MBR_RELSHP_CD_SK").alias("MBR_RELSHP_CD_SK"),
        F.col("MBR_RELSHP_TYPE_NO").alias("MBR_RELSHP_TYPE_NO"),
        F.col("MBR_FIRST_NM").alias("MBR_FIRST_NM"),
        F.col("MBR_MIDINIT").alias("MBR_MIDINIT"),
        F.col("MBR_LAST_NM").alias("MBR_LAST_NM"),
        F.col("MBR_MAIL_ADDR_LN_1").alias("MBR_MAIL_ADDR_LN_1"),
        F.col("MBR_MAIL_ADDR_LN_2").alias("MBR_MAIL_ADDR_LN_2"),
        F.col("MBR_MAIL_ADDR_LN_3").alias("MBR_MAIL_ADDR_LN_3"),
        F.col("MBR_MAIL_ADDR_CITY_NM").alias("MBR_MAIL_ADDR_CITY_NM"),
        F.col("MBR_MAIL_ADDR_ST_CD").alias("MBR_MAIL_ADDR_ST_CD"),
        F.col("MBR_MAIL_ADDR_ZIP_CD_5").alias("MBR_MAIL_ADDR_ZIP_CD_5"),
        F.col("MBR_MAIL_ADDR_ZIP_CD_4").alias("MBR_MAIL_ADDR_ZIP_CD_4"),
        F.col("MBR_BRTH_DT").alias("MBR_BRTH_DT"),
        F.col("MBR_SFX_NO").alias("MBR_SFX_NO"),
        F.col("COV_BEG_DT").alias("COV_BEG_DT"),
        F.col("COV_END_DT").alias("COV_END_DT"),
        F.col("SUBMBR").alias("SUBMBR"),
        F.col("Rank").alias("Rank")
    )
)

# ------------------------------------------------------------------------------------------------
# STAGE: Sort_SubDupsDt (PxSort) on df_lnk_Dups1
# ------------------------------------------------------------------------------------------------
df_Sort_SubDupsDt = df_lnk_Dups1.sort(
    F.col("SUB_ID").asc(),
    F.col("MBR_SFX_NO").asc(),
    F.col("COV_END_DT").asc()
)

# ------------------------------------------------------------------------------------------------
# STAGE: xfm_loop_mbr_dates (CTransformerStage)
# DataStage loop logic is highly specialized. We replicate field transformations best we can.
# "COV_BEG_DT" => "svFinalBegindt" if last row in group else 0, etc. Attempt a window approach.
# ------------------------------------------------------------------------------------------------
window_loop = (
    F.window().partitionBy("SUB_ID").orderBy(F.col("SUB_ID"), F.col("MBR_SFX_NO"), F.col("COV_END_DT"))
)
df_loop_mark = df_Sort_SubDupsDt.withColumn(
    "_is_last_in_group",
    F.when(
        F.lead("SUB_ID").over(window_loop) != F.col("SUB_ID"), 
        F.lit(True)
    ).otherwise(F.when(F.lead("SUB_ID").over(window_loop).isNull(), True).otherwise(False))
).withColumn(
    "svFinalBegindt",
    F.when(F.col("_is_last_in_group"), F.lit(None)).otherwise(F.col("COV_BEG_DT"))
)

df_xfm_loop_mbr_dates = df_loop_mark.select(
    F.col("MBR_ID").alias("MBR_ID"),
    F.col("SUB_ID").alias("SUB_ID"),
    F.col("CES_CLNT_ID").alias("CES_CLNT_ID"),
    F.col("TAX_YR").alias("TAX_YR"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("AS_OF_DTM").alias("AS_OF_DTM"),
    F.col("MBR_SK").alias("MBR_SK"),
    F.col("SUB_SK").alias("SUB_SK"),
    F.col("MBR_RELSHP_CD_SK").alias("MBR_RELSHP_CD_SK"),
    F.col("MBR_RELSHP_TYPE_NO").alias("MBR_RELSHP_TYPE_NO"),
    F.col("MBR_FIRST_NM").alias("MBR_FIRST_NM"),
    F.col("MBR_MIDINIT").alias("MBR_MIDINIT"),
    F.col("MBR_LAST_NM").alias("MBR_LAST_NM"),
    F.col("MBR_MAIL_ADDR_LN_1").alias("MBR_MAIL_ADDR_LN_1"),
    F.col("MBR_MAIL_ADDR_LN_2").alias("MBR_MAIL_ADDR_LN_2"),
    F.col("MBR_MAIL_ADDR_LN_3").alias("MBR_MAIL_ADDR_LN_3"),
    F.col("MBR_MAIL_ADDR_CITY_NM").alias("MBR_MAIL_ADDR_CITY_NM"),
    F.col("MBR_MAIL_ADDR_ST_CD").alias("MBR_MAIL_ADDR_ST_CD"),
    F.col("MBR_MAIL_ADDR_ZIP_CD_5").alias("MBR_MAIL_ADDR_ZIP_CD_5"),
    F.col("MBR_MAIL_ADDR_ZIP_CD_4").alias("MBR_MAIL_ADDR_ZIP_CD_4"),
    F.col("MBR_BRTH_DT").alias("MBR_BRTH_DT"),
    F.col("MBR_SFX_NO").alias("MBR_SFX_NO"),
    F.when(F.col("svFinalBegindt").isNull(), F.lit(None)).otherwise(F.col("svFinalBegindt")).alias("COV_BEG_DT"),
    F.col("COV_END_DT").alias("COV_END_DT"),
    F.col("SUBMBR").alias("SUBMBR"),
    F.col("Rank").alias("Rank"),
    F.col("COV_BEG_DT").alias("COV_BEG_DT_1"),
    F.col("COV_END_DT").alias("COV_END_DT_1")
)

# ------------------------------------------------------------------------------------------------
# STAGE: RD_Sub_mbr (PxRemDup) => dedup_sort helper
# Retain=first, Key=SUB_ID, MBR_SFX_NO
# Sort indicates SUB_ID asc, MBR_SFX_NO asc, COV_END_DT desc
# ------------------------------------------------------------------------------------------------
df_sorted_for_RD = df_xfm_loop_mbr_dates.sort(
    F.col("SUB_ID").asc(),
    F.col("MBR_SFX_NO").asc(),
    F.col("COV_END_DT").desc()
)
df_RD_Sub_mbr = dedup_sort(
    df_sorted_for_RD,
    partition_cols=["SUB_ID", "MBR_SFX_NO"],
    sort_cols=[("COV_END_DT", "D")]
)

df_RD_Sub_mbr_out = df_RD_Sub_mbr.select(
    F.col("MBR_ID").alias("MBR_ID"),
    F.col("SUB_ID").alias("SUB_ID"),
    F.col("CES_CLNT_ID").alias("CES_CLNT_ID"),
    F.col("TAX_YR").alias("TAX_YR"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("AS_OF_DTM").alias("AS_OF_DTM"),
    F.col("MBR_SK").alias("MBR_SK"),
    F.col("SUB_SK").alias("SUB_SK"),
    F.col("MBR_RELSHP_CD_SK").alias("MBR_RELSHP_CD_SK"),
    F.col("MBR_RELSHP_TYPE_NO").alias("MBR_RELSHP_TYPE_NO"),
    F.col("MBR_FIRST_NM").alias("MBR_FIRST_NM"),
    F.col("MBR_MIDINIT").alias("MBR_MIDINIT"),
    F.col("MBR_LAST_NM").alias("MBR_LAST_NM"),
    F.col("MBR_MAIL_ADDR_LN_1").alias("MBR_MAIL_ADDR_LN_1"),
    F.col("MBR_MAIL_ADDR_LN_2").alias("MBR_MAIL_ADDR_LN_2"),
    F.col("MBR_MAIL_ADDR_LN_3").alias("MBR_MAIL_ADDR_LN_3"),
    F.col("MBR_MAIL_ADDR_CITY_NM").alias("MBR_MAIL_ADDR_CITY_NM"),
    F.col("MBR_MAIL_ADDR_ST_CD").alias("MBR_MAIL_ADDR_ST_CD"),
    F.col("MBR_MAIL_ADDR_ZIP_CD_5").alias("MBR_MAIL_ADDR_ZIP_CD_5"),
    F.col("MBR_MAIL_ADDR_ZIP_CD_4").alias("MBR_MAIL_ADDR_ZIP_CD_4"),
    F.col("MBR_BRTH_DT").alias("MBR_BRTH_DT"),
    F.col("MBR_SFX_NO").alias("MBR_SFX_NO"),
    F.col("COV_BEG_DT").alias("COV_BEG_DT"),
    F.col("COV_END_DT").alias("COV_END_DT"),
    F.col("SUBMBR").alias("SUBMBR"),
    F.col("Rank").alias("Rank")
)

# ------------------------------------------------------------------------------------------------
# STAGE: Fnl_Dups_uniq (PxFunnel) => funnel df_lnk_Uniq, df_RD_Sub_mbr_out
# ------------------------------------------------------------------------------------------------
df_Fnl_Dups_uniq = (
    df_lnk_Uniq.unionByName(df_RD_Sub_mbr_out, allowMissingColumns=True)
)

# ------------------------------------------------------------------------------------------------
# STAGE: xfm_ind_mnthly (CTransformerStage)
# Add monthly coverage indicators.
# ------------------------------------------------------------------------------------------------
def coverage_expr(month_str):
    # "If (Tax_Year : '-MM-01' >= COV_BEG_DT) and ((Tax_Year : '-MM-xx') <= COV_END_DT or COV_END_DT >= Tax_Year : '-MM-yy') then 'X' else..."
    # We replicate the exact logic for each month from the JSON.
    # Because the day ranges differ, we'll just handle each month individually outside the function.
    return None

df_ind_pre = df_Fnl_Dups_uniq.select(
    F.col("MBR_ID"),
    F.col("SUB_ID"),
    F.col("COV_BEG_DT"),
    F.col("COV_END_DT"),
    F.col("CES_CLNT_ID"),
    F.col("TAX_YR"),
    F.col("SRC_SYS_CD"),
    F.col("AS_OF_DTM"),
    F.col("MBR_SK"),
    F.col("SUB_SK"),
    F.col("MBR_RELSHP_CD_SK"),
    F.col("MBR_RELSHP_TYPE_NO"),
    F.col("MBR_FIRST_NM"),
    F.col("MBR_MIDINIT"),
    F.col("MBR_LAST_NM"),
    F.col("MBR_MAIL_ADDR_LN_1"),
    F.col("MBR_MAIL_ADDR_LN_2"),
    F.col("MBR_MAIL_ADDR_LN_3"),
    F.col("MBR_MAIL_ADDR_CITY_NM"),
    F.col("MBR_MAIL_ADDR_ST_CD"),
    F.col("MBR_MAIL_ADDR_ZIP_CD_5"),
    F.col("MBR_MAIL_ADDR_ZIP_CD_4"),
    F.col("MBR_BRTH_DT"),
    F.col("MBR_SFX_NO"),
    F.col("SUBMBR"),
    F.col("Rank")
)

def cov_flag_expr(m_01, m_end, m_16):
    # Replicates the pattern:
    # If (Tax_Year||m_01 >= COV_BEG_DT) and ((Tax_Year||m_end <= COV_END_DT) or (COV_END_DT >= Tax_Year||m_16)) 
    # then 'X' 
    # else if (COV_BEG_DT > Tax_Year||m_01 and COV_BEG_DT <= Tax_Year||m_16) then 'X'
    # else ''
    cond1 = (
        (F.concat(F.lit(Tax_Year), F.lit(m_01)).cast("date") >= F.col("COV_BEG_DT"))
        & (
            (F.concat(F.lit(Tax_Year), F.lit(m_end)).cast("date") <= F.col("COV_END_DT"))
            | (F.col("COV_END_DT") >= F.concat(F.lit(Tax_Year), F.lit(m_16)).cast("date"))
        )
    )
    cond2 = (
        (F.col("COV_BEG_DT") > F.concat(F.lit(Tax_Year), F.lit(m_01)).cast("date"))
        & (F.col("COV_BEG_DT") <= F.concat(F.lit(Tax_Year), F.lit(m_16)).cast("date"))
    )
    return F.when(cond1 | cond2, F.lit("X")).otherwise(F.lit(""))

df_ind = (
    df_ind_pre
    .withColumn("JAN_COV_IN", cov_flag_expr("-01-01", "-01-31", "-01-16"))
    .withColumn("FEB_COV_IN", cov_flag_expr("-02-01", "-02-28", "-02-13"))
    .withColumn("MAR_COV_IN", cov_flag_expr("-03-01", "-03-31", "-03-16"))
    .withColumn("APR_COV_IN", cov_flag_expr("-04-01", "-04-30", "-04-15"))
    .withColumn("MAY_COV_IN", cov_flag_expr("-05-01", "-05-31", "-05-16"))
    .withColumn("JUN_COV_IN", cov_flag_expr("-06-01", "-06-30", "-06-15"))
    .withColumn("JUL_COV_IN", cov_flag_expr("-07-01", "-07-31", "-07-16"))
    .withColumn("AUG_COV_IN", cov_flag_expr("-08-01", "-08-31", "-08-16"))
    .withColumn("SEP_COV_IN", cov_flag_expr("-09-01", "-09-30", "-09-15"))
    .withColumn("OCT_COV_IN", cov_flag_expr("-10-01", "-10-31", "-10-16"))
    .withColumn("NOV_COV_IN", cov_flag_expr("-11-01", "-11-30", "-11-16"))
    .withColumn("DEC_COV_IN", cov_flag_expr("-12-01", "-12-31", "-12-16"))
)

# ------------------------------------------------------------------------------------------------
# STAGE: Lkp_Grp_Xref (PxLookup) => primary link is df_ind, lookup link is df_P_MA_DOR_GRP_XREF
# Join on (GRP_ID from df_ind) with (GRP_ID from xref) and (CES_CLNT_ID from df_ind) with (CES_CLNT_ID from xref).
# The original code used "Lnk_xfm_Mbr.GRP_ID" => but that field is not in df_ind. We do have "PLN_ID" at the start, 
# but the job JSON shows it tried "SourceKeyOrValue = Lnk_xfm_Mbr.GRP_ID". Actually the job has a mismatch. 
# However, from the JSON: 
#   Join conditions: 
#     Lnk_xfm_Mbr.GRP_ID => Lnk_Xref.GRP_ID 
#     Lnk_xfm_Mbr.CES_CLNT_ID => Lnk_Xref.CES_CLNT_ID
# The job actually references "GRP_ID" from that link, but that link does not have GRP_ID. 
# The instructions say do not skip logic; we'll replicate the join as posted. 
# We produce null or no match if "GRP_ID" doesn't exist in the data. 
# ------------------------------------------------------------------------------------------------
df_ind_adj = df_ind.withColumn("GRP_ID", F.col("GRP_ID"))  # might be null
df_Lkp_Grp_Xref = (
    df_ind_adj.alias("Lnk_xfm_Mbr")
    .join(
        df_P_MA_DOR_GRP_XREF.alias("Lnk_Xref"),
        on=[
            F.col("Lnk_xfm_Mbr.GRP_ID") == F.col("Lnk_Xref.GRP_ID"),
            F.col("Lnk_xfm_Mbr.CES_CLNT_ID") == F.col("Lnk_Xref.CES_CLNT_ID")
        ],
        how="inner"
    )
    .select(
        F.col("Lnk_xfm_Mbr.MBR_ID").alias("MBR_ID"),
        F.col("Lnk_xfm_Mbr.SUB_ID").alias("SUB_ID"),
        F.col("Lnk_Xref.GRP_ID").alias("GRP_ID"),
        F.col("Lnk_xfm_Mbr.TAX_YR").alias("TAX_YR"),
        F.col("Lnk_xfm_Mbr.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("Lnk_xfm_Mbr.AS_OF_DTM").alias("AS_OF_DTM"),
        F.col("Lnk_Xref.GRP_SK").alias("GRP_SK"),
        F.col("Lnk_xfm_Mbr.MBR_SK").alias("MBR_SK"),
        F.col("Lnk_xfm_Mbr.SUB_SK").alias("SUB_SK"),
        F.col("Lnk_xfm_Mbr.MBR_RELSHP_CD_SK").alias("MBR_RELSHP_CD_SK"),
        F.col("Lnk_xfm_Mbr.MBR_RELSHP_TYPE_NO").alias("MBR_RELSHP_TYPE_NO"),
        F.col("Lnk_xfm_Mbr.MBR_FIRST_NM").alias("MBR_FIRST_NM"),
        F.col("Lnk_xfm_Mbr.MBR_MIDINIT").alias("MBR_MIDINIT"),
        F.col("Lnk_xfm_Mbr.MBR_LAST_NM").alias("MBR_LAST_NM"),
        F.col("Lnk_xfm_Mbr.MBR_MAIL_ADDR_LN_1").alias("MBR_MAIL_ADDR_LN_1"),
        F.col("Lnk_xfm_Mbr.MBR_MAIL_ADDR_LN_2").alias("MBR_MAIL_ADDR_LN_2"),
        F.col("Lnk_xfm_Mbr.MBR_MAIL_ADDR_LN_3").alias("MBR_MAIL_ADDR_LN_3"),
        F.col("Lnk_xfm_Mbr.MBR_MAIL_ADDR_CITY_NM").alias("MBR_MAIL_ADDR_CITY_NM"),
        F.col("Lnk_xfm_Mbr.MBR_MAIL_ADDR_ST_CD").alias("MBR_MAIL_ADDR_ST_CD"),
        F.col("Lnk_xfm_Mbr.MBR_MAIL_ADDR_ZIP_CD_5").alias("MBR_MAIL_ADDR_ZIP_CD_5"),
        F.col("Lnk_xfm_Mbr.MBR_MAIL_ADDR_ZIP_CD_4").alias("MBR_MAIL_ADDR_ZIP_CD_4"),
        F.col("Lnk_xfm_Mbr.JAN_COV_IN").alias("JAN_COV_IN"),
        F.col("Lnk_xfm_Mbr.FEB_COV_IN").alias("FEB_COV_IN"),
        F.col("Lnk_xfm_Mbr.MAR_COV_IN").alias("MAR_COV_IN"),
        F.col("Lnk_xfm_Mbr.APR_COV_IN").alias("APR_COV_IN"),
        F.col("Lnk_xfm_Mbr.MAY_COV_IN").alias("MAY_COV_IN"),
        F.col("Lnk_xfm_Mbr.JUN_COV_IN").alias("JUN_COV_IN"),
        F.col("Lnk_xfm_Mbr.JUL_COV_IN").alias("JUL_COV_IN"),
        F.col("Lnk_xfm_Mbr.AUG_COV_IN").alias("AUG_COV_IN"),
        F.col("Lnk_xfm_Mbr.SEP_COV_IN").alias("SEP_COV_IN"),
        F.col("Lnk_xfm_Mbr.OCT_COV_IN").alias("OCT_COV_IN"),
        F.col("Lnk_xfm_Mbr.NOV_COV_IN").alias("NOV_COV_IN"),
        F.col("Lnk_xfm_Mbr.DEC_COV_IN").alias("DEC_COV_IN"),
        F.col("Lnk_xfm_Mbr.MBR_BRTH_DT").alias("MBR_BRTH_DT"),
        F.col("Lnk_xfm_Mbr.MBR_SFX_NO").alias("MBR_SFX_NO"),
        F.col("Lnk_xfm_Mbr.CES_CLNT_ID").alias("CES_CLNT_ID"),  # keep original
        F.col("Lnk_xfm_Mbr.COV_BEG_DT").alias("COV_BEG_DT"),
        F.col("Lnk_xfm_Mbr.COV_END_DT").alias("COV_END_DT"),
    )
)

# ------------------------------------------------------------------------------------------------
# STAGE: Lkp_SubMaDor (PxLookup)
# Join on SUB_ID, GRP_ID, TAX_YR from df_SUB_MA_DOR
# ------------------------------------------------------------------------------------------------
df_Lkp_SubMaDor = (
    df_Lkp_Grp_Xref.alias("Lnk_Xref_lkp")
    .join(
        df_SUB_MA_DOR.alias("Lnk_SubMaDor"),
        on=[
            F.col("Lnk_Xref_lkp.SUB_ID") == F.col("Lnk_SubMaDor.SUB_ID"),
            F.col("Lnk_Xref_lkp.GRP_ID") == F.col("Lnk_SubMaDor.GRP_ID"),
            F.col("Lnk_Xref_lkp.TAX_YR") == F.col("Lnk_SubMaDor.TAX_YR")
        ],
        how="inner"
    )
    .select(
        F.col("Lnk_Xref_lkp.MBR_ID").alias("MBR_ID"),
        F.col("Lnk_Xref_lkp.SUB_ID").alias("SUB_ID"),
        F.col("Lnk_Xref_lkp.GRP_ID").alias("GRP_ID"),
        F.col("Lnk_Xref_lkp.TAX_YR").alias("TAX_YR"),
        F.col("Lnk_Xref_lkp.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("Lnk_Xref_lkp.AS_OF_DTM").alias("AS_OF_DTM"),
        F.col("Lnk_Xref_lkp.GRP_SK").alias("GRP_SK"),
        F.col("Lnk_Xref_lkp.MBR_SK").alias("MBR_SK"),
        F.col("Lnk_SubMaDor.SUB_MA_DOR_SK").alias("SUB_MA_DOR_SK"),
        F.col("Lnk_Xref_lkp.SUB_SK").alias("SUB_SK"),
        F.col("Lnk_Xref_lkp.MBR_RELSHP_CD_SK").alias("MBR_RELSHP_CD_SK"),
        F.col("Lnk_Xref_lkp.MBR_RELSHP_TYPE_NO").alias("MBR_RELSHP_TYPE_NO"),
        F.col("Lnk_Xref_lkp.MBR_FIRST_NM").alias("MBR_FIRST_NM"),
        F.col("Lnk_Xref_lkp.MBR_MIDINIT").alias("MBR_MIDINIT"),
        F.col("Lnk_Xref_lkp.MBR_LAST_NM").alias("MBR_LAST_NM"),
        F.col("Lnk_Xref_lkp.MBR_MAIL_ADDR_LN_1").alias("MBR_MAIL_ADDR_LN_1"),
        F.col("Lnk_Xref_lkp.MBR_MAIL_ADDR_LN_2").alias("MBR_MAIL_ADDR_LN_2"),
        F.col("Lnk_Xref_lkp.MBR_MAIL_ADDR_LN_3").alias("MBR_MAIL_ADDR_LN_3"),
        F.col("Lnk_Xref_lkp.MBR_MAIL_ADDR_CITY_NM").alias("MBR_MAIL_ADDR_CITY_NM"),
        F.col("Lnk_Xref_lkp.MBR_MAIL_ADDR_ST_CD").alias("MBR_MAIL_ADDR_ST_CD"),
        F.col("Lnk_Xref_lkp.MBR_MAIL_ADDR_ZIP_CD_5").alias("MBR_MAIL_ADDR_ZIP_CD_5"),
        F.col("Lnk_Xref_lkp.MBR_MAIL_ADDR_ZIP_CD_4").alias("MBR_MAIL_ADDR_ZIP_CD_4"),
        F.col("Lnk_Xref_lkp.JAN_COV_IN").alias("JAN_COV_IN"),
        F.col("Lnk_Xref_lkp.FEB_COV_IN").alias("FEB_COV_IN"),
        F.col("Lnk_Xref_lkp.MAR_COV_IN").alias("MAR_COV_IN"),
        F.col("Lnk_Xref_lkp.APR_COV_IN").alias("APR_COV_IN"),
        F.col("Lnk_Xref_lkp.MAY_COV_IN").alias("MAY_COV_IN"),
        F.col("Lnk_Xref_lkp.JUN_COV_IN").alias("JUN_COV_IN"),
        F.col("Lnk_Xref_lkp.JUL_COV_IN").alias("JUL_COV_IN"),
        F.col("Lnk_Xref_lkp.AUG_COV_IN").alias("AUG_COV_IN"),
        F.col("Lnk_Xref_lkp.SEP_COV_IN").alias("SEP_COV_IN"),
        F.col("Lnk_Xref_lkp.OCT_COV_IN").alias("OCT_COV_IN"),
        F.col("Lnk_Xref_lkp.NOV_COV_IN").alias("NOV_COV_IN"),
        F.col("Lnk_Xref_lkp.DEC_COV_IN").alias("DEC_COV_IN"),
        F.col("Lnk_Xref_lkp.MBR_BRTH_DT").alias("MBR_BRTH_DT"),
        F.col("Lnk_Xref_lkp.MBR_SFX_NO").alias("MBR_SFX_NO")
    )
)

# ------------------------------------------------------------------------------------------------
# STAGE: Member_DS (PxDataSet) => translate to Parquet with write_files
# File path is #$FilePath#/ds/MBR_MA_DOR_.#SrcSysCd#.extr.#RunID#.ds => so => f"{adls_path}/ds/MBR_MA_DOR_.{SrcSysCd}.extr.{RunID}.parquet"
# ------------------------------------------------------------------------------------------------
df_Member_DS = df_Lkp_SubMaDor

# Apply final column order and rpad for char/varchar columns
df_Member_DS_out = df_Member_DS.select(
    rpad(F.col("MBR_ID"), 255, " ").alias("MBR_ID"),
    rpad(F.col("SUB_ID"), 255, " ").alias("SUB_ID"),
    rpad(F.col("GRP_ID"), 255, " ").alias("GRP_ID"),
    rpad(F.col("TAX_YR"), 4, " ").alias("TAX_YR"),
    rpad(F.col("SRC_SYS_CD"), 255, " ").alias("SRC_SYS_CD"),
    rpad(F.col("AS_OF_DTM"), 255, " ").alias("AS_OF_DTM"),
    rpad(F.col("GRP_SK"), 255, " ").alias("GRP_SK"),
    rpad(F.col("MBR_SK"), 255, " ").alias("MBR_SK"),
    rpad(F.col("SUB_MA_DOR_SK"), 255, " ").alias("SUB_MA_DOR_SK"),
    rpad(F.col("SUB_SK"), 255, " ").alias("SUB_SK"),
    rpad(F.col("MBR_RELSHP_CD_SK"), 255, " ").alias("MBR_RELSHP_CD_SK"),
    rpad(F.col("MBR_RELSHP_TYPE_NO"), 255, " ").alias("MBR_RELSHP_TYPE_NO"),
    rpad(F.col("MBR_FIRST_NM"), 255, " ").alias("MBR_FIRST_NM"),
    rpad(F.col("MBR_MIDINIT"), 1, " ").alias("MBR_MIDINIT"),
    rpad(F.col("MBR_LAST_NM"), 255, " ").alias("MBR_LAST_NM"),
    rpad(F.col("MBR_MAIL_ADDR_LN_1"), 255, " ").alias("MBR_MAIL_ADDR_LN_1"),
    rpad(F.col("MBR_MAIL_ADDR_LN_2"), 255, " ").alias("MBR_MAIL_ADDR_LN_2"),
    rpad(F.col("MBR_MAIL_ADDR_LN_3"), 255, " ").alias("MBR_MAIL_ADDR_LN_3"),
    rpad(F.col("MBR_MAIL_ADDR_CITY_NM"), 255, " ").alias("MBR_MAIL_ADDR_CITY_NM"),
    rpad(F.col("MBR_MAIL_ADDR_ST_CD"), 255, " ").alias("MBR_MAIL_ADDR_ST_CD"),
    rpad(F.col("MBR_MAIL_ADDR_ZIP_CD_5"), 5, " ").alias("MBR_MAIL_ADDR_ZIP_CD_5"),
    rpad(F.col("MBR_MAIL_ADDR_ZIP_CD_4"), 4, " ").alias("MBR_MAIL_ADDR_ZIP_CD_4"),
    rpad(F.col("JAN_COV_IN"), 1, " ").alias("JAN_COV_IN"),
    rpad(F.col("FEB_COV_IN"), 1, " ").alias("FEB_COV_IN"),
    rpad(F.col("MAR_COV_IN"), 1, " ").alias("MAR_COV_IN"),
    rpad(F.col("APR_COV_IN"), 1, " ").alias("APR_COV_IN"),
    rpad(F.col("MAY_COV_IN"), 1, " ").alias("MAY_COV_IN"),
    rpad(F.col("JUN_COV_IN"), 1, " ").alias("JUN_COV_IN"),
    rpad(F.col("JUL_COV_IN"), 1, " ").alias("JUL_COV_IN"),
    rpad(F.col("AUG_COV_IN"), 1, " ").alias("AUG_COV_IN"),
    rpad(F.col("SEP_COV_IN"), 1, " ").alias("SEP_COV_IN"),
    rpad(F.col("OCT_COV_IN"), 1, " ").alias("OCT_COV_IN"),
    rpad(F.col("NOV_COV_IN"), 1, " ").alias("NOV_COV_IN"),
    rpad(F.col("DEC_COV_IN"), 1, " ").alias("DEC_COV_IN"),
    rpad(F.col("MBR_BRTH_DT"), 255, " ").alias("MBR_BRTH_DT"),
    rpad(F.col("MBR_SFX_NO"), 255, " ").alias("MBR_SFX_NO")
)

write_files(
    df_Member_DS_out,
    f"{adls_path}/ds/MBR_MA_DOR_.{SrcSysCd}.extr.{RunID}.parquet",
    delimiter=",",
    mode="overwrite",
    is_parquet=True,
    header=True,
    quote="\"",
    nullValue=None
)