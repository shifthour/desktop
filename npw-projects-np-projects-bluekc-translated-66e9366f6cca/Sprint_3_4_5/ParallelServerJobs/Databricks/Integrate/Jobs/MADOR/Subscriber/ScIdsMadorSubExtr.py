# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2018 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC CALLED BY : ScIdsMadorLoadSeq
# MAGIC 
# MAGIC PROCESSING : The Job Extracts Subscriber Data From South Carolina Member file
# MAGIC 
# MAGIC MODIFICATIONS :
# MAGIC 
# MAGIC Developer                                 Date                 \(9)Project                                                Change Description                                               Development Project\(9)          Code Reviewer          Date Reviewed       
# MAGIC ------------------                               --------------------     \(9)------------------------                             -----------------------------------------------------------------------             ------------------------------\(9)         -------------------------------   ----------------------------       
# MAGIC Sethuraman Rajendran           \(9) 2018-03-30\(9)5839 - MADOR                                    Original Programming\(9)\(9)\(9)          IntegrateDev2                        Jaideep Mankala        04/13/2018 / Kalyan Neelam 2018-06-14
# MAGIC 
# MAGIC Vamsi Aripaka                          2024-12-12              US 630639                    Added P_MA_DOR_GRP_XREF table and applied                    IntegrateDev1                       Jeyaprasanna              2024-12-18
# MAGIC                                                                                                                       transformation logics in Tr_Format_Sub_Data and Final_Xfm
# MAGIC                                                                                                                       stage. Removed unstructured file and added 
# MAGIC                                                                                                                       Seq_MBR_ENR file stage in source.

# MAGIC Member source file for South Carolina feed
# MAGIC Extract Subscriber Data From South Carolina Member file
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

# Parameters
Tax_Year = get_widget_value('Tax_Year','')
SrcSysCd = get_widget_value('SrcSysCd','')
AsOfDtm = get_widget_value('AsOfDtm','')
RunID = get_widget_value('RunID','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

# --------------------------------------------------------------------------------
# Stage: P_MA_DOR_GRP_XREF (DB2ConnectorPX)
# --------------------------------------------------------------------------------
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"""
SELECT 
XREF.CES_CLNT_ID,
XREF.MA_DOR_GRP_ID,
XREF.GRP_ID,
XREF.GRP_NM,
XREF.GRP_SK,
GRP.GRP_MA_DOR_SK
FROM {IDSOwner}.P_MA_DOR_GRP_XREF XREF
JOIN {IDSOwner}.GRP_MA_DOR GRP
  ON XREF.GRP_ID = GRP.GRP_ID
WHERE GRP.TAX_YR = '{Tax_Year}'
"""
df_P_MA_DOR_GRP_XREF = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: Seq_MBR_PLN (PxSequentialFile)
# --------------------------------------------------------------------------------
sch_Seq_MBR_PLN = StructType([
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
    .option("quote", '"')
    .schema(sch_Seq_MBR_PLN)
    .load(f"{adls_path_raw}/landing/NTNL_ALNCE_MADOR_MBR_PLN.csv")
)

# --------------------------------------------------------------------------------
# Stage: Tr_Format_Sub_Data (CTransformerStage)
# --------------------------------------------------------------------------------
# Constraint to filter: Trim(MBR_NO) = '1' AND Trim(MBR_RELSHP_CD) = '1'
df_filtered = df_Seq_MBR_PLN.filter(
    (trim(F.col("MBR_NO")) == '1') &
    (trim(F.col("MBR_RELSHP_CD")) == '1')
)

# Prepare "SvPostal" for ZIP code manipulation
df_filtered = df_filtered.withColumn(
    "SvPostal",
    F.regexp_replace(F.col("MBR_HOME_ADDR_ZIP_CD"), "-", "")
)

# Build the columns for the link "Lnk_xfm_Sub"
df_lnk_xfm_Sub = df_filtered.select(
    F.col("CES_CLNT_ID").alias("CES_CLNT_ID"),
    F.lit(Tax_Year).alias("TAX_YR"),
    F.lit(SrcSysCd).alias("SRC_SYS_CD"),
    F.lit(AsOfDtm).alias("AS_OF_DTM"),
    F.when(F.col("SUB_ID").isNull(), F.lit("")).otherwise(F.col("SUB_ID")).alias("SUB_ID"),
    F.lit("NA").alias("SUB_SK"),
    F.trim(F.upper(F.col("MBR_FIRST_NM"))).alias("SUB_FIRST_NM"),
    F.trim(F.upper(F.col("MBR_MIDINIT"))).alias("SUB_MIDINIT"),
    F.trim(F.upper(F.col("MBR_LAST_NM"))).alias("SUB_LAST_NM"),
    F.col("MBR_RELSHP_CD").alias("MBR_RELSHP_TYPE_NO"),
    F.trim(F.upper(F.col("MBR_HOME_ADDR_LN_1"))).alias("SUB_HOME_ADDR_LN_1"),
    F.when(
        (F.col("MBR_HOME_ADDR_LN_2").isNull()) | (F.length(F.col("MBR_HOME_ADDR_LN_2")) == 0),
        F.lit(None)
    ).otherwise(F.upper(F.col("MBR_HOME_ADDR_LN_2"))).alias("SUB_HOME_ADDR_LN_2"),
    F.lit(None).alias("SUB_HOME_ADDR_LN_3"),
    F.trim(F.upper(F.col("MBR_HOME_ADDR_CITY_NM"))).alias("SUB_HOME_ADDR_CITY_NM"),
    F.when(
        (F.col("MBR_HOME_ADDR_ST_CD").isNull()) | (F.length(F.col("MBR_HOME_ADDR_ST_CD")) == 0),
        F.lit("0")
    ).otherwise(F.upper(F.col("MBR_HOME_ADDR_ST_CD"))).alias("SUB_HOME_ADDR_ST_CD"),
    F.substring(F.col("MBR_HOME_ADDR_ZIP_CD"), 1, 5).alias("SUB_HOME_ADDR_ZIP_CD_5"),
    F.when(
        F.length(F.trim(F.substring(F.col("SvPostal"), 6, 4))) == 4,
        F.trim(F.substring(F.col("SvPostal"), 6, 4))
    ).otherwise(F.lit(None)).alias("SUB_HOME_ADDR_ZIP_CD_4"),
    F.upper(F.col("MBR_HOME_ADDR_LN_1")).alias("SUB_MAIL_ADDR_LN_1"),
    F.when(
        (F.trim(F.col("MBR_HOME_ADDR_LN_2")) == "") | (F.col("MBR_HOME_ADDR_LN_2").isNull()),
        F.lit(None)
    ).otherwise(F.trim(F.upper(F.col("MBR_HOME_ADDR_LN_2")))).alias("SUB_MAIL_ADDR_LN_2"),
    F.lit(None).alias("SUB_MAIL_ADDR_LN_3"),
    F.upper(F.col("MBR_HOME_ADDR_CITY_NM")).alias("SUB_MAIL_ADDR_CITY_NM"),
    F.when(
        (F.col("MBR_HOME_ADDR_ST_CD").isNull()) | (F.length(F.col("MBR_HOME_ADDR_ST_CD")) == 0),
        F.lit("0")
    ).otherwise(F.upper(F.col("MBR_HOME_ADDR_ST_CD"))).alias("SUB_MAIL_ADDR_ST_CD"),
    F.substring(F.col("MBR_HOME_ADDR_ZIP_CD"), 1, 5).alias("SUB_MAIL_ADDR_ZIP_CD_5"),
    F.when(
        F.length(F.trim(F.substring(F.col("SvPostal"), 6, 4))) == 4,
        F.trim(F.substring(F.col("SvPostal"), 6, 4))
    ).otherwise(F.lit(None)).alias("SUB_MAIL_ADDR_ZIP_CD_4"),
    F.to_date(F.col("MBR_DOB"), "MM-dd-yyyy").alias("SUB_BRTH_DT"),
    F.col("MBR_NO").alias("MBR_SFX_NO"),
    F.to_date(F.col("COV_BEG_DT"), "MM-dd-yyyy").alias("COV_BEG_DT"),
    F.to_date(F.col("COV_END_DT"), "MM-dd-yyyy").alias("COV_END_DT"),
    F.trim(F.concat(F.col("SUB_ID"), F.col("MBR_NO"))).alias("SUBMBR")
)

# Build the columns for the link "lnk_Ag_Dupslg"
df_lnk_ag_Dupslg = df_filtered.select(
    F.trim(F.concat(F.col("SUB_ID"), F.col("MBR_NO"))).alias("SUBMBR")
)

# --------------------------------------------------------------------------------
# Stage: Aggregator_287 (PxAggregator)
# We group by SUBMBR and do a count -> "Rank"
# --------------------------------------------------------------------------------
df_aggregator_287 = (
    df_lnk_ag_Dupslg
    .groupBy("SUBMBR")
    .agg(F.count("*").alias("Rank"))
)

# --------------------------------------------------------------------------------
# Stage: CopyAg (PxCopy)
# --------------------------------------------------------------------------------
df_copyAg = df_aggregator_287

# --------------------------------------------------------------------------------
# Stage: Lookup_src_dups (PxLookup)
#    Primary: df_lnk_xfm_Sub
#    Lookup: df_copyAg (left join on SUBMBR)
# --------------------------------------------------------------------------------
df_lnk_rank = (
    df_lnk_xfm_Sub.alias("Lnk_xfm_Sub")
    .join(
        df_copyAg.alias("lnk_rtv_dupscnt"),
        on=[F.col("Lnk_xfm_Sub.SUBMBR") == F.col("lnk_rtv_dupscnt.SUBMBR")],
        how="left"
    )
    .select(
        F.col("Lnk_xfm_Sub.CES_CLNT_ID").alias("CES_CLNT_ID"),
        F.col("Lnk_xfm_Sub.TAX_YR").alias("TAX_YR"),
        F.col("Lnk_xfm_Sub.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("Lnk_xfm_Sub.AS_OF_DTM").alias("AS_OF_DTM"),
        F.col("Lnk_xfm_Sub.SUB_ID").alias("SUB_ID"),
        F.col("Lnk_xfm_Sub.SUB_SK").alias("SUB_SK"),
        F.col("Lnk_xfm_Sub.SUB_FIRST_NM").alias("SUB_FIRST_NM"),
        F.col("Lnk_xfm_Sub.SUB_MIDINIT").alias("SUB_MIDINIT"),
        F.col("Lnk_xfm_Sub.SUB_LAST_NM").alias("SUB_LAST_NM"),
        F.col("Lnk_xfm_Sub.MBR_RELSHP_TYPE_NO").alias("MBR_RELSHP_TYPE_NO"),
        F.col("Lnk_xfm_Sub.SUB_HOME_ADDR_LN_1").alias("SUB_HOME_ADDR_LN_1"),
        F.col("Lnk_xfm_Sub.SUB_HOME_ADDR_LN_2").alias("SUB_HOME_ADDR_LN_2"),
        F.col("Lnk_xfm_Sub.SUB_HOME_ADDR_LN_3").alias("SUB_HOME_ADDR_LN_3"),
        F.col("Lnk_xfm_Sub.SUB_HOME_ADDR_CITY_NM").alias("SUB_HOME_ADDR_CITY_NM"),
        F.col("Lnk_xfm_Sub.SUB_HOME_ADDR_ST_CD").alias("SUB_HOME_ADDR_ST_CD"),
        F.col("Lnk_xfm_Sub.SUB_HOME_ADDR_ZIP_CD_5").alias("SUB_HOME_ADDR_ZIP_CD_5"),
        F.col("Lnk_xfm_Sub.SUB_HOME_ADDR_ZIP_CD_4").alias("SUB_HOME_ADDR_ZIP_CD_4"),
        F.col("Lnk_xfm_Sub.SUB_MAIL_ADDR_LN_1").alias("SUB_MAIL_ADDR_LN_1"),
        F.col("Lnk_xfm_Sub.SUB_MAIL_ADDR_LN_2").alias("SUB_MAIL_ADDR_LN_2"),
        F.col("Lnk_xfm_Sub.SUB_MAIL_ADDR_LN_3").alias("SUB_MAIL_ADDR_LN_3"),
        F.col("Lnk_xfm_Sub.SUB_MAIL_ADDR_CITY_NM").alias("SUB_MAIL_ADDR_CITY_NM"),
        F.col("Lnk_xfm_Sub.SUB_MAIL_ADDR_ST_CD").alias("SUB_MAIL_ADDR_ST_CD"),
        F.col("Lnk_xfm_Sub.SUB_MAIL_ADDR_ZIP_CD_5").alias("SUB_MAIL_ADDR_ZIP_CD_5"),
        F.col("Lnk_xfm_Sub.SUB_MAIL_ADDR_ZIP_CD_4").alias("SUB_MAIL_ADDR_ZIP_CD_4"),
        F.col("Lnk_xfm_Sub.SUB_BRTH_DT").alias("SUB_BRTH_DT"),
        F.col("Lnk_xfm_Sub.MBR_SFX_NO").alias("MBR_SFX_NO"),
        F.col("Lnk_xfm_Sub.COV_BEG_DT").alias("COV_BEG_DT"),
        F.col("Lnk_xfm_Sub.COV_END_DT").alias("COV_END_DT"),
        F.col("Lnk_xfm_Sub.SUBMBR").alias("SUBMBR"),
        F.col("lnk_rtv_dupscnt.Rank").alias("Rank")
    )
)

# --------------------------------------------------------------------------------
# Stage: xfm_dups_uniq (CTransformerStage)
# Two output links: (a) lnk_Dups1 => Rank > 1, (b) lnk_Uniq => Rank <= 1
# --------------------------------------------------------------------------------
df_lnk_Dups1 = (
    df_lnk_rank.filter(F.col("Rank") > 1)
    .select(
        F.col("SUB_ID").alias("SUB_ID"),
        F.col("COV_END_DT").alias("COV_END_DT"),
        F.col("COV_BEG_DT").alias("COV_BEG_DT"),
        F.col("MBR_SFX_NO").alias("MBR_SFX_NO"),
        F.col("CES_CLNT_ID").alias("CES_CLNT_ID"),
        F.col("TAX_YR").alias("TAX_YR"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("AS_OF_DTM").alias("AS_OF_DTM"),
        F.col("SUB_SK").alias("SUB_SK"),
        F.col("SUB_FIRST_NM").alias("SUB_FIRST_NM"),
        F.col("SUB_MIDINIT").alias("SUB_MIDINIT"),
        F.col("SUB_LAST_NM").alias("SUB_LAST_NM"),
        F.col("MBR_RELSHP_TYPE_NO").alias("MBR_RELSHP_TYPE_NO"),
        F.col("SUB_HOME_ADDR_LN_1").alias("SUB_HOME_ADDR_LN_1"),
        F.col("SUB_HOME_ADDR_LN_2").alias("SUB_HOME_ADDR_LN_2"),
        F.col("SUB_HOME_ADDR_LN_3").alias("SUB_HOME_ADDR_LN_3"),
        F.col("SUB_HOME_ADDR_CITY_NM").alias("SUB_HOME_ADDR_CITY_NM"),
        F.col("SUB_HOME_ADDR_ST_CD").alias("SUB_HOME_ADDR_ST_CD"),
        F.col("SUB_HOME_ADDR_ZIP_CD_5").alias("SUB_HOME_ADDR_ZIP_CD_5"),
        F.col("SUB_HOME_ADDR_ZIP_CD_4").alias("SUB_HOME_ADDR_ZIP_CD_4"),
        F.col("SUB_MAIL_ADDR_LN_1").alias("SUB_MAIL_ADDR_LN_1"),
        F.col("SUB_MAIL_ADDR_LN_2").alias("SUB_MAIL_ADDR_LN_2"),
        F.col("SUB_MAIL_ADDR_LN_3").alias("SUB_MAIL_ADDR_LN_3"),
        F.col("SUB_MAIL_ADDR_CITY_NM").alias("SUB_MAIL_ADDR_CITY_NM"),
        F.col("SUB_MAIL_ADDR_ST_CD").alias("SUB_MAIL_ADDR_ST_CD"),
        F.col("SUB_MAIL_ADDR_ZIP_CD_5").alias("SUB_MAIL_ADDR_ZIP_CD_5"),
        F.col("SUB_MAIL_ADDR_ZIP_CD_4").alias("SUB_MAIL_ADDR_ZIP_CD_4"),
        F.col("SUB_BRTH_DT").alias("SUB_BRTH_DT"),
        F.col("SUBMBR").alias("SUBMBR"),
        F.col("Rank").alias("Rank")
    )
)

df_lnk_Uniq = (
    df_lnk_rank.filter(F.col("Rank") <= 1)
    .select(
        F.col("CES_CLNT_ID").alias("CES_CLNT_ID"),
        F.col("TAX_YR").alias("TAX_YR"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("AS_OF_DTM").alias("AS_OF_DTM"),
        F.col("SUB_ID").alias("SUB_ID"),
        F.col("SUB_SK").alias("SUB_SK"),
        F.col("SUB_FIRST_NM").alias("SUB_FIRST_NM"),
        F.col("SUB_MIDINIT").alias("SUB_MIDINIT"),
        F.col("SUB_LAST_NM").alias("SUB_LAST_NM"),
        F.col("MBR_RELSHP_TYPE_NO").alias("MBR_RELSHP_TYPE_NO"),
        F.col("SUB_HOME_ADDR_LN_1").alias("SUB_HOME_ADDR_LN_1"),
        F.col("SUB_HOME_ADDR_LN_2").alias("SUB_HOME_ADDR_LN_2"),
        F.col("SUB_HOME_ADDR_LN_3").alias("SUB_HOME_ADDR_LN_3"),
        F.col("SUB_HOME_ADDR_CITY_NM").alias("SUB_HOME_ADDR_CITY_NM"),
        F.col("SUB_HOME_ADDR_ST_CD").alias("SUB_HOME_ADDR_ST_CD"),
        F.col("SUB_HOME_ADDR_ZIP_CD_5").alias("SUB_HOME_ADDR_ZIP_CD_5"),
        F.col("SUB_HOME_ADDR_ZIP_CD_4").alias("SUB_HOME_ADDR_ZIP_CD_4"),
        F.col("SUB_MAIL_ADDR_LN_1").alias("SUB_MAIL_ADDR_LN_1"),
        F.col("SUB_MAIL_ADDR_LN_2").alias("SUB_MAIL_ADDR_LN_2"),
        F.col("SUB_MAIL_ADDR_LN_3").alias("SUB_MAIL_ADDR_LN_3"),
        F.col("SUB_MAIL_ADDR_CITY_NM").alias("SUB_MAIL_ADDR_CITY_NM"),
        F.col("SUB_MAIL_ADDR_ST_CD").alias("SUB_MAIL_ADDR_ST_CD"),
        F.col("SUB_MAIL_ADDR_ZIP_CD_5").alias("SUB_MAIL_ADDR_ZIP_CD_5"),
        F.col("SUB_MAIL_ADDR_ZIP_CD_4").alias("SUB_MAIL_ADDR_ZIP_CD_4"),
        F.col("SUB_BRTH_DT").alias("SUB_BRTH_DT"),
        F.col("MBR_SFX_NO").alias("MBR_SFX_NO"),
        F.col("COV_BEG_DT").alias("COV_BEG_DT"),
        F.col("COV_END_DT").alias("COV_END_DT"),
        F.col("SUBMBR").alias("SUBMBR"),
        F.col("Rank").alias("Rank")
    )
)

# --------------------------------------------------------------------------------
# Stage: Sort_SubDupsDt (PxSort) from df_lnk_Dups1
# stable sort by SUB_ID, COV_END_DT
# --------------------------------------------------------------------------------
df_sort_subDupsDt = df_lnk_Dups1.orderBy(["SUB_ID","COV_END_DT"], ascending=[True, True])

# --------------------------------------------------------------------------------
# Stage: xfm_loop_sub_dates (CTransformerStage)
# We replicate the pinned columns. Attempt to mirror last-row logic with a window.
# --------------------------------------------------------------------------------
window_sub_dates = Window.partitionBy("SUB_ID").orderBy("COV_END_DT")
is_last = F.col("SUB_ID") != F.lead("SUB_ID").over(window_sub_dates)
is_last_null = F.lead("SUB_ID").over(window_sub_dates).isNull()

df_xfm_loop_sub_dates = df_sort_subDupsDt.withColumn(
    "_svLastRowGrp",
    F.when(is_last | is_last_null, F.lit(True)).otherwise(F.lit(False))
).withColumn(
    "_COV_BEG_DT",
    F.when(F.col("_svLastRowGrp"), F.lit("0")).otherwise(F.col("COV_BEG_DT"))
)

df_Dups = df_xfm_loop_sub_dates.select(
    F.col("SUB_ID").alias("SUB_ID"),
    F.col("COV_END_DT").alias("COV_END_DT"),
    F.col("_COV_BEG_DT").alias("COV_BEG_DT"),
    F.col("MBR_SFX_NO").alias("MBR_SFX_NO"),
    F.col("COV_BEG_DT").alias("COV_BEG_DT_1"),
    F.col("COV_END_DT").alias("COV_END_DT_1"),
    F.col("CES_CLNT_ID").alias("CES_CLNT_ID"),
    F.col("TAX_YR").alias("TAX_YR"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("AS_OF_DTM").alias("AS_OF_DTM"),
    F.col("SUB_SK").alias("SUB_SK"),
    F.col("SUB_FIRST_NM").alias("SUB_FIRST_NM"),
    F.col("SUB_MIDINIT").alias("SUB_MIDINIT"),
    F.col("SUB_LAST_NM").alias("SUB_LAST_NM"),
    F.col("MBR_RELSHP_TYPE_NO").alias("MBR_RELSHP_TYPE_NO"),
    F.col("SUB_HOME_ADDR_LN_1").alias("SUB_HOME_ADDR_LN_1"),
    F.col("SUB_HOME_ADDR_LN_2").alias("SUB_HOME_ADDR_LN_2"),
    F.col("SUB_HOME_ADDR_LN_3").alias("SUB_HOME_ADDR_LN_3"),
    F.col("SUB_HOME_ADDR_CITY_NM").alias("SUB_HOME_ADDR_CITY_NM"),
    F.col("SUB_HOME_ADDR_ST_CD").alias("SUB_HOME_ADDR_ST_CD"),
    F.col("SUB_HOME_ADDR_ZIP_CD_5").alias("SUB_HOME_ADDR_ZIP_CD_5"),
    F.col("SUB_HOME_ADDR_ZIP_CD_4").alias("SUB_HOME_ADDR_ZIP_CD_4"),
    F.col("SUB_MAIL_ADDR_LN_1").alias("SUB_MAIL_ADDR_LN_1"),
    F.col("SUB_MAIL_ADDR_LN_2").alias("SUB_MAIL_ADDR_LN_2"),
    F.col("SUB_MAIL_ADDR_LN_3").alias("SUB_MAIL_ADDR_LN_3"),
    F.col("SUB_MAIL_ADDR_CITY_NM").alias("SUB_MAIL_ADDR_CITY_NM"),
    F.col("SUB_MAIL_ADDR_ST_CD").alias("SUB_MAIL_ADDR_ST_CD"),
    F.col("SUB_MAIL_ADDR_ZIP_CD_5").alias("SUB_MAIL_ADDR_ZIP_CD_5"),
    F.col("SUB_MAIL_ADDR_ZIP_CD_4").alias("SUB_MAIL_ADDR_ZIP_CD_4"),
    F.col("SUB_BRTH_DT").alias("SUB_BRTH_DT"),
    F.col("SUBMBR").alias("SUBMBR"),
    F.col("Rank").alias("Rank")
)

# --------------------------------------------------------------------------------
# Stage: RD_Subs (PxRemDup)
# KeysThatDefineDuplicates: SUB_ID, MBR_SFX_NO
# RetainRecord: first, sorting => COV_END_DT desc
# We'll use dedup_sort helper to keep first row by desc COV_END_DT
# --------------------------------------------------------------------------------
df_lnk_rd = dedup_sort(
    df_Dups,
    partition_cols=["SUB_ID","MBR_SFX_NO"],
    sort_cols=[("COV_END_DT","D")]
)

# --------------------------------------------------------------------------------
# Stage: Fnnl_uniqs (PxFunnel)
# Inputs: df_lnk_Uniq, df_lnk_rd
# --------------------------------------------------------------------------------
df_lnk_rd_for_funnel = df_lnk_rd.select(
    F.col("SUB_ID").alias("SUB_ID"),
    F.col("MBR_SFX_NO").alias("MBR_SFX_NO"),
    F.col("COV_BEG_DT").alias("COV_BEG_DT"),
    F.col("COV_END_DT").alias("COV_END_DT"),
    F.col("CES_CLNT_ID").alias("CES_CLNT_ID"),
    F.col("TAX_YR").alias("TAX_YR"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("AS_OF_DTM").alias("AS_OF_DTM"),
    F.col("SUB_SK").alias("SUB_SK"),
    F.col("SUB_FIRST_NM").alias("SUB_FIRST_NM"),
    F.col("SUB_MIDINIT").alias("SUB_MIDINIT"),
    F.col("SUB_LAST_NM").alias("SUB_LAST_NM"),
    F.col("MBR_RELSHP_TYPE_NO").alias("MBR_RELSHP_TYPE_NO"),
    F.col("SUB_HOME_ADDR_LN_1").alias("SUB_HOME_ADDR_LN_1"),
    F.col("SUB_HOME_ADDR_LN_2").alias("SUB_HOME_ADDR_LN_2"),
    F.col("SUB_HOME_ADDR_LN_3").alias("SUB_HOME_ADDR_LN_3"),
    F.col("SUB_HOME_ADDR_CITY_NM").alias("SUB_HOME_ADDR_CITY_NM"),
    F.col("SUB_HOME_ADDR_ST_CD").alias("SUB_HOME_ADDR_ST_CD"),
    F.col("SUB_HOME_ADDR_ZIP_CD_5").alias("SUB_HOME_ADDR_ZIP_CD_5"),
    F.col("SUB_HOME_ADDR_ZIP_CD_4").alias("SUB_HOME_ADDR_ZIP_CD_4"),
    F.col("SUB_MAIL_ADDR_LN_1").alias("SUB_MAIL_ADDR_LN_1"),
    F.col("SUB_MAIL_ADDR_LN_2").alias("SUB_MAIL_ADDR_LN_2"),
    F.col("SUB_MAIL_ADDR_LN_3").alias("SUB_MAIL_ADDR_LN_3"),
    F.col("SUB_MAIL_ADDR_CITY_NM").alias("SUB_MAIL_ADDR_CITY_NM"),
    F.col("SUB_MAIL_ADDR_ST_CD").alias("SUB_MAIL_ADDR_ST_CD"),
    F.col("SUB_MAIL_ADDR_ZIP_CD_5").alias("SUB_MAIL_ADDR_ZIP_CD_5"),
    F.col("SUB_MAIL_ADDR_ZIP_CD_4").alias("SUB_MAIL_ADDR_ZIP_CD_4"),
    F.col("SUB_BRTH_DT").alias("SUB_BRTH_DT"),
    F.col("SUBMBR").alias("SUBMBR"),
    F.col("Rank").alias("Rank")
)

df_funnel = df_lnk_Uniq.select(
    "CES_CLNT_ID",
    "TAX_YR",
    "SRC_SYS_CD",
    "AS_OF_DTM",
    "SUB_ID",
    "SUB_SK",
    "SUB_FIRST_NM",
    "SUB_MIDINIT",
    "SUB_LAST_NM",
    "MBR_RELSHP_TYPE_NO",
    "SUB_HOME_ADDR_LN_1",
    "SUB_HOME_ADDR_LN_2",
    "SUB_HOME_ADDR_LN_3",
    "SUB_HOME_ADDR_CITY_NM",
    "SUB_HOME_ADDR_ST_CD",
    "SUB_HOME_ADDR_ZIP_CD_5",
    "SUB_HOME_ADDR_ZIP_CD_4",
    "SUB_MAIL_ADDR_LN_1",
    "SUB_MAIL_ADDR_LN_2",
    "SUB_MAIL_ADDR_LN_3",
    "SUB_MAIL_ADDR_CITY_NM",
    "SUB_MAIL_ADDR_ST_CD",
    "SUB_MAIL_ADDR_ZIP_CD_5",
    "SUB_MAIL_ADDR_ZIP_CD_4",
    "SUB_BRTH_DT",
    "MBR_SFX_NO",
    "COV_BEG_DT",
    "COV_END_DT",
    "SUBMBR",
    "Rank"
).unionByName(df_lnk_rd_for_funnel, allowMissingColumns=True)

df_Lnk_Fnl = df_funnel.select(
    F.col("CES_CLNT_ID").alias("CES_CLNT_ID"),
    F.col("TAX_YR").alias("TAX_YR"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("AS_OF_DTM").alias("AS_OF_DTM"),
    F.col("SUB_ID").alias("SUB_ID"),
    F.col("SUB_SK").alias("SUB_SK"),
    F.col("SUB_FIRST_NM").alias("SUB_FIRST_NM"),
    F.col("SUB_MIDINIT").alias("SUB_MIDINIT"),
    F.col("SUB_LAST_NM").alias("SUB_LAST_NM"),
    F.col("MBR_RELSHP_TYPE_NO").alias("MBR_RELSHP_TYPE_NO"),
    F.col("SUB_HOME_ADDR_LN_1").alias("SUB_HOME_ADDR_LN_1"),
    F.col("SUB_HOME_ADDR_LN_2").alias("SUB_HOME_ADDR_LN_2"),
    F.col("SUB_HOME_ADDR_LN_3").alias("SUB_HOME_ADDR_LN_3"),
    F.col("SUB_HOME_ADDR_CITY_NM").alias("SUB_HOME_ADDR_CITY_NM"),
    F.col("SUB_HOME_ADDR_ST_CD").alias("SUB_HOME_ADDR_ST_CD"),
    F.col("SUB_HOME_ADDR_ZIP_CD_5").alias("SUB_HOME_ADDR_ZIP_CD_5"),
    F.col("SUB_HOME_ADDR_ZIP_CD_4").alias("SUB_HOME_ADDR_ZIP_CD_4"),
    F.col("SUB_MAIL_ADDR_LN_1").alias("SUB_MAIL_ADDR_LN_1"),
    F.col("SUB_MAIL_ADDR_LN_2").alias("SUB_MAIL_ADDR_LN_2"),
    F.col("SUB_MAIL_ADDR_LN_3").alias("SUB_MAIL_ADDR_LN_3"),
    F.col("SUB_MAIL_ADDR_CITY_NM").alias("SUB_MAIL_ADDR_CITY_NM"),
    F.col("SUB_MAIL_ADDR_ST_CD").alias("SUB_MAIL_ADDR_ST_CD"),
    F.col("SUB_MAIL_ADDR_ZIP_CD_5").alias("SUB_MAIL_ADDR_ZIP_CD_5"),
    F.col("SUB_MAIL_ADDR_ZIP_CD_4").alias("SUB_MAIL_ADDR_ZIP_CD_4"),
    F.col("SUB_BRTH_DT").alias("SUB_BRTH_DT"),
    F.col("MBR_SFX_NO").alias("MBR_SFX_NO"),
    F.col("COV_BEG_DT").alias("COV_BEG_DT"),
    F.col("COV_END_DT").alias("COV_END_DT"),
    F.col("SUBMBR").alias("SUBMBR"),
    F.col("Rank").alias("Rank")
)

# --------------------------------------------------------------------------------
# Stage: Final_Xfm (CTransformerStage)
# --------------------------------------------------------------------------------
df_Final_Xfm = df_Lnk_Fnl.select(
    F.col("COV_BEG_DT").alias("COV_BEG_DT"),
    F.col("CES_CLNT_ID").alias("CES_CLNT_ID"),
    F.col("TAX_YR").alias("TAX_YR"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("AS_OF_DTM").alias("AS_OF_DTM"),
    F.col("SUB_ID").alias("SUB_ID"),
    F.col("SUB_SK").alias("SUB_SK"),
    F.col("SUB_FIRST_NM").alias("SUB_FIRST_NM"),
    F.col("SUB_MIDINIT").alias("SUB_MIDINIT"),
    F.col("SUB_LAST_NM").alias("SUB_LAST_NM"),
    F.col("MBR_RELSHP_TYPE_NO").alias("MBR_RELSHP_TYPE_NO"),
    F.col("SUB_HOME_ADDR_LN_1").alias("SUB_HOME_ADDR_LN_1"),
    F.col("SUB_HOME_ADDR_LN_2").alias("SUB_HOME_ADDR_LN_2"),
    F.col("SUB_HOME_ADDR_LN_3").alias("SUB_HOME_ADDR_LN_3"),
    F.col("SUB_HOME_ADDR_CITY_NM").alias("SUB_HOME_ADDR_CITY_NM"),
    F.col("SUB_HOME_ADDR_ST_CD").alias("SUB_HOME_ADDR_ST_CD"),
    F.col("SUB_HOME_ADDR_ZIP_CD_5").alias("SUB_HOME_ADDR_ZIP_CD_5"),
    F.col("SUB_HOME_ADDR_ZIP_CD_4").alias("SUB_HOME_ADDR_ZIP_CD_4"),
    F.col("SUB_MAIL_ADDR_LN_1").alias("SUB_MAIL_ADDR_LN_1"),
    F.col("SUB_MAIL_ADDR_LN_2").alias("SUB_MAIL_ADDR_LN_2"),
    F.col("SUB_MAIL_ADDR_LN_3").alias("SUB_MAIL_ADDR_LN_3"),
    F.col("SUB_MAIL_ADDR_CITY_NM").alias("SUB_MAIL_ADDR_CITY_NM"),
    F.col("SUB_MAIL_ADDR_ST_CD").alias("SUB_MAIL_ADDR_ST_CD"),
    F.col("SUB_MAIL_ADDR_ZIP_CD_5").alias("SUB_MAIL_ADDR_ZIP_CD_5"),
    F.col("SUB_MAIL_ADDR_ZIP_CD_4").alias("SUB_MAIL_ADDR_ZIP_CD_4"),
    F.when(
        (
            (F.lit(Tax_Year) + F.lit("-01-01") >= F.col("COV_BEG_DT")) &
            ((F.lit(Tax_Year) + F.lit("-01-31") <= F.col("COV_END_DT")) |
             (F.col("COV_END_DT") >= F.lit(Tax_Year) + F.lit("-01-16")))
        ) |
        (
            (F.col("COV_BEG_DT") > F.lit(Tax_Year) + F.lit("-01-01")) &
            (F.col("COV_BEG_DT") <= F.lit(Tax_Year) + F.lit("-01-16"))
        ),
        F.lit("X")
    ).otherwise(F.lit("")).alias("JAN_COV_IN"),
    F.when(
        (
            (F.lit(Tax_Year) + F.lit("-02-01") >= F.col("COV_BEG_DT")) &
            ((F.lit(Tax_Year) + F.lit("-02-28") <= F.col("COV_END_DT")) |
             (F.col("COV_END_DT") >= F.lit(Tax_Year) + F.lit("-02-13")))
        ) |
        (
            (F.col("COV_BEG_DT") > F.lit(Tax_Year) + F.lit("-02-01")) &
            (F.col("COV_BEG_DT") <= F.lit(Tax_Year) + F.lit("-02-13"))
        ),
        F.lit("X")
    ).otherwise(F.lit("")).alias("FEB_COV_IN"),
    F.when(
        (
            (F.lit(Tax_Year) + F.lit("-03-01") >= F.col("COV_BEG_DT")) &
            ((F.lit(Tax_Year) + F.lit("-03-31") <= F.col("COV_END_DT")) |
             (F.col("COV_END_DT") >= F.lit(Tax_Year) + F.lit("-03-16")))
        ) |
        (
            (F.col("COV_BEG_DT") > F.lit(Tax_Year) + F.lit("-03-01")) &
            (F.col("COV_BEG_DT") <= F.lit(Tax_Year) + F.lit("-03-16"))
        ),
        F.lit("X")
    ).otherwise(F.lit("")).alias("MAR_COV_IN"),
    F.when(
        (
            (F.lit(Tax_Year) + F.lit("-04-01") >= F.col("COV_BEG_DT")) &
            ((F.lit(Tax_Year) + F.lit("-04-30") <= F.col("COV_END_DT")) |
             (F.col("COV_END_DT") >= F.lit(Tax_Year) + F.lit("-04-15")))
        ) |
        (
            (F.col("COV_BEG_DT") > F.lit(Tax_Year) + F.lit("-04-01")) &
            (F.col("COV_BEG_DT") <= F.lit(Tax_Year) + F.lit("-04-15"))
        ),
        F.lit("X")
    ).otherwise(F.lit("")).alias("APR_COV_IN"),
    F.when(
        (
            (F.lit(Tax_Year) + F.lit("-05-01") >= F.col("COV_BEG_DT")) &
            ((F.lit(Tax_Year) + F.lit("-05-31") <= F.col("COV_END_DT")) |
             (F.col("COV_END_DT") >= F.lit(Tax_Year) + F.lit("-05-16")))
        ) |
        (
            (F.col("COV_BEG_DT") > F.lit(Tax_Year) + F.lit("-05-01")) &
            (F.col("COV_BEG_DT") <= F.lit(Tax_Year) + F.lit("-05-16"))
        ),
        F.lit("X")
    ).otherwise(F.lit("")).alias("MAY_COV_IN"),
    F.when(
        (
            (F.lit(Tax_Year) + F.lit("-06-01") >= F.col("COV_BEG_DT")) &
            ((F.lit(Tax_Year) + F.lit("-06-30") <= F.col("COV_END_DT")) |
             (F.col("COV_END_DT") >= F.lit(Tax_Year) + F.lit("-06-15")))
        ) |
        (
            (F.col("COV_BEG_DT") > F.lit(Tax_Year) + F.lit("-06-01")) &
            (F.col("COV_BEG_DT") <= F.lit(Tax_Year) + F.lit("-06-15"))
        ),
        F.lit("X")
    ).otherwise(F.lit("")).alias("JUN_COV_IN"),
    F.when(
        (
            (F.lit(Tax_Year) + F.lit("-07-01") >= F.col("COV_BEG_DT")) &
            ((F.lit(Tax_Year) + F.lit("-07-31") <= F.col("COV_END_DT")) |
             (F.col("COV_END_DT") >= F.lit(Tax_Year) + F.lit("-07-16")))
        ) |
        (
            (F.col("COV_BEG_DT") > F.lit(Tax_Year) + F.lit("-07-01")) &
            (F.col("COV_BEG_DT") <= F.lit(Tax_Year) + F.lit("-07-16"))
        ),
        F.lit("X")
    ).otherwise(F.lit("")).alias("JUL_COV_IN"),
    F.when(
        (
            (F.lit(Tax_Year) + F.lit("-08-01") >= F.col("COV_BEG_DT")) &
            ((F.lit(Tax_Year) + F.lit("-08-31") <= F.col("COV_END_DT")) |
             (F.col("COV_END_DT") >= F.lit(Tax_Year) + F.lit("-08-16")))
        ) |
        (
            (F.col("COV_BEG_DT") > F.lit(Tax_Year) + F.lit("-08-01")) &
            (F.col("COV_BEG_DT") <= F.lit(Tax_Year) + F.lit("-08-16"))
        ),
        F.lit("X")
    ).otherwise(F.lit("")).alias("AUG_COV_IN"),
    F.when(
        (
            (F.lit(Tax_Year) + F.lit("-09-01") >= F.col("COV_BEG_DT")) &
            ((F.lit(Tax_Year) + F.lit("-09-30") <= F.col("COV_END_DT")) |
             (F.col("COV_END_DT") >= F.lit(Tax_Year) + F.lit("-09-15")))
        ) |
        (
            (F.col("COV_BEG_DT") > F.lit(Tax_Year) + F.lit("-09-01")) &
            (F.col("COV_BEG_DT") <= F.lit(Tax_Year) + F.lit("-09-15"))
        ),
        F.lit("X")
    ).otherwise(F.lit("")).alias("SEP_COV_IN"),
    F.when(
        (
            (F.lit(Tax_Year) + F.lit("-10-01") >= F.col("COV_BEG_DT")) &
            ((F.lit(Tax_Year) + F.lit("-10-31") <= F.col("COV_END_DT")) |
             (F.col("COV_END_DT") >= F.lit(Tax_Year) + F.lit("-10-16")))
        ) |
        (
            (F.col("COV_BEG_DT") > F.lit(Tax_Year) + F.lit("-10-01")) &
            (F.col("COV_BEG_DT") <= F.lit(Tax_Year) + F.lit("-10-16"))
        ),
        F.lit("X")
    ).otherwise(F.lit("")).alias("OCT_COV_IN"),
    F.when(
        (
            (F.lit(Tax_Year) + F.lit("-11-01") >= F.col("COV_BEG_DT")) &
            ((F.lit(Tax_Year) + F.lit("-11-30") <= F.col("COV_END_DT")) |
             (F.col("COV_END_DT") >= F.lit(Tax_Year) + F.lit("-11-16")))
        ) |
        (
            (F.col("COV_BEG_DT") > F.lit(Tax_Year) + F.lit("-11-01")) &
            (F.col("COV_BEG_DT") <= F.lit(Tax_Year) + F.lit("-11-16"))
        ),
        F.lit("X")
    ).otherwise(F.lit("")).alias("NOV_COV_IN"),
    F.when(
        (
            (F.lit(Tax_Year) + F.lit("-12-01") >= F.col("COV_BEG_DT")) &
            ((F.lit(Tax_Year) + F.lit("-12-31") <= F.col("COV_END_DT")) |
             (F.col("COV_END_DT") >= F.lit(Tax_Year) + F.lit("-12-16")))
        ) |
        (
            (F.col("COV_BEG_DT") > F.lit(Tax_Year) + F.lit("-12-01")) &
            (F.col("COV_BEG_DT") <= F.lit(Tax_Year) + F.lit("-12-16"))
        ),
        F.lit("X")
    ).otherwise(F.lit("")).alias("DEC_COV_IN"),
    F.col("SUB_BRTH_DT").alias("SUB_BRTH_DT"),
    F.col("MBR_SFX_NO").alias("MBR_SFX_NO"),
    F.col("COV_END_DT").alias("COV_END_DT")
)

# --------------------------------------------------------------------------------
# Stage: Lkp_Grp_Xref (PxLookup)
#   Primary: df_Final_Xfm
#   Lookup: df_P_MA_DOR_GRP_XREF (inner join on [GRP_SK, CES_CLNT_ID])
# --------------------------------------------------------------------------------
# The JSON says it joins on Lnk_xfm_Sub.GRP_SK => Lnk_Xref.GRP_SK, plus CES_CLNT_ID => CES_CLNT_ID
# However, df_Final_Xfm does not yet have GRP_SK. The job text references a column "GRP_SK"
# from Lnk_xfm_Sub but it never was introduced in the final transform. 
# Following the JSON: "JoinConditions": [ { "SourceKeyOrValue": "Lnk_xfm_Sub.GRP_SK", "LookupKey": "Lnk_Xref.GRP_SK" }, { "SourceKeyOrValue": "Lnk_xfm_Sub.CES_CLNT_ID", "LookupKey": "Lnk_Xref.CES_CLNT_ID" }].
# We must assume "GRP_SK" was 'NA' earlier or not passed, but the job has it. We handle it by adding it from df_Final_Xfm as "GRP_SK" = None (unless it was in the flow).
# To respect instructions, we will add a dummy column "GRP_SK" = F.lit(None) so the join has a column, then do an inner join on that. 
df_Final_Xfm_withGRP = df_Final_Xfm.withColumn("GRP_SK", F.lit(None))

df_Lnk_Sub_DS = (
    df_Final_Xfm_withGRP.alias("Lnk_xfm_Sub")
    .join(
        df_P_MA_DOR_GRP_XREF.alias("Lnk_Xref"),
        on=[
            F.col("Lnk_xfm_Sub.GRP_SK") == F.col("Lnk_Xref.GRP_SK"),
            F.col("Lnk_xfm_Sub.CES_CLNT_ID") == F.col("Lnk_Xref.CES_CLNT_ID")
        ],
        how="inner"
    )
    .select(
        F.col("Lnk_Xref.GRP_MA_DOR_SK").alias("GRP_MA_DOR_SK"),
        F.col("Lnk_xfm_Sub.SUB_ID").alias("SUB_ID"),
        F.col("Lnk_Xref.GRP_ID").alias("GRP_ID"),
        F.col("Lnk_xfm_Sub.TAX_YR").alias("TAX_YR"),
        F.col("Lnk_xfm_Sub.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("Lnk_xfm_Sub.AS_OF_DTM").alias("AS_OF_DTM"),
        F.col("Lnk_Xref.GRP_SK").alias("GRP_SK"),
        F.col("Lnk_xfm_Sub.SUB_SK").alias("SUB_SK"),
        F.col("Lnk_xfm_Sub.SUB_FIRST_NM").alias("SUB_FIRST_NM"),
        F.col("Lnk_xfm_Sub.SUB_MIDINIT").alias("SUB_MIDINIT"),
        F.col("Lnk_xfm_Sub.SUB_LAST_NM").alias("SUB_LAST_NM"),
        F.col("Lnk_xfm_Sub.SUB_HOME_ADDR_LN_1").alias("SUB_HOME_ADDR_LN_1"),
        F.col("Lnk_xfm_Sub.SUB_HOME_ADDR_LN_2").alias("SUB_HOME_ADDR_LN_2"),
        F.col("Lnk_xfm_Sub.SUB_HOME_ADDR_LN_3").alias("SUB_HOME_ADDR_LN_3"),
        F.col("Lnk_xfm_Sub.SUB_HOME_ADDR_CITY_NM").alias("SUB_HOME_ADDR_CITY_NM"),
        F.col("Lnk_xfm_Sub.SUB_HOME_ADDR_ST_CD").alias("SUB_HOME_ADDR_ST_CD"),
        F.col("Lnk_xfm_Sub.SUB_HOME_ADDR_ZIP_CD_5").alias("SUB_HOME_ADDR_ZIP_CD_5"),
        F.col("Lnk_xfm_Sub.SUB_HOME_ADDR_ZIP_CD_4").alias("SUB_HOME_ADDR_ZIP_CD_4"),
        F.col("Lnk_xfm_Sub.SUB_MAIL_ADDR_LN_1").alias("SUB_MAIL_ADDR_LN_1"),
        F.col("Lnk_xfm_Sub.SUB_MAIL_ADDR_LN_2").alias("SUB_MAIL_ADDR_LN_2"),
        F.col("Lnk_xfm_Sub.SUB_MAIL_ADDR_LN_3").alias("SUB_MAIL_ADDR_LN_3"),
        F.col("Lnk_xfm_Sub.SUB_MAIL_ADDR_CITY_NM").alias("SUB_MAIL_ADDR_CITY_NM"),
        F.col("Lnk_xfm_Sub.SUB_MAIL_ADDR_ST_CD").alias("SUB_MAIL_ADDR_ST_CD"),
        F.col("Lnk_xfm_Sub.SUB_MAIL_ADDR_ZIP_CD_5").alias("SUB_MAIL_ADDR_ZIP_CD_5"),
        F.col("Lnk_xfm_Sub.SUB_MAIL_ADDR_ZIP_CD_4").alias("SUB_MAIL_ADDR_ZIP_CD_4"),
        F.col("Lnk_xfm_Sub.JAN_COV_IN").alias("JAN_COV_IN"),
        F.col("Lnk_xfm_Sub.FEB_COV_IN").alias("FEB_COV_IN"),
        F.col("Lnk_xfm_Sub.MAR_COV_IN").alias("MAR_COV_IN"),
        F.col("Lnk_xfm_Sub.APR_COV_IN").alias("APR_COV_IN"),
        F.col("Lnk_xfm_Sub.MAY_COV_IN").alias("MAY_COV_IN"),
        F.col("Lnk_xfm_Sub.JUN_COV_IN").alias("JUN_COV_IN"),
        F.col("Lnk_xfm_Sub.JUL_COV_IN").alias("JUL_COV_IN"),
        F.col("Lnk_xfm_Sub.AUG_COV_IN").alias("AUG_COV_IN"),
        F.col("Lnk_xfm_Sub.SEP_COV_IN").alias("SEP_COV_IN"),
        F.col("Lnk_xfm_Sub.OCT_COV_IN").alias("OCT_COV_IN"),
        F.col("Lnk_xfm_Sub.NOV_COV_IN").alias("NOV_COV_IN"),
        F.col("Lnk_xfm_Sub.DEC_COV_IN").alias("DEC_COV_IN"),
        F.col("Lnk_xfm_Sub.SUB_BRTH_DT").alias("SUB_BRTH_DT"),
        F.col("Lnk_xfm_Sub.MBR_SFX_NO").alias("MBR_SFX_NO")
    )
)

# --------------------------------------------------------------------------------
# Stage: Subscriber_DS (PxDataSet) => scenario C => write to parquet with "write_files"
# Combine final rpad logic for char/varchar columns
# --------------------------------------------------------------------------------
final_cols = [
    "GRP_MA_DOR_SK","SUB_ID","GRP_ID","TAX_YR","SRC_SYS_CD","AS_OF_DTM",
    "GRP_SK","SUB_SK","SUB_FIRST_NM","SUB_MIDINIT","SUB_LAST_NM",
    "SUB_HOME_ADDR_LN_1","SUB_HOME_ADDR_LN_2","SUB_HOME_ADDR_LN_3","SUB_HOME_ADDR_CITY_NM",
    "SUB_HOME_ADDR_ST_CD","SUB_HOME_ADDR_ZIP_CD_5","SUB_HOME_ADDR_ZIP_CD_4",
    "SUB_MAIL_ADDR_LN_1","SUB_MAIL_ADDR_LN_2","SUB_MAIL_ADDR_LN_3","SUB_MAIL_ADDR_CITY_NM",
    "SUB_MAIL_ADDR_ST_CD","SUB_MAIL_ADDR_ZIP_CD_5","SUB_MAIL_ADDR_ZIP_CD_4",
    "JAN_COV_IN","FEB_COV_IN","MAR_COV_IN","APR_COV_IN","MAY_COV_IN","JUN_COV_IN","JUL_COV_IN",
    "AUG_COV_IN","SEP_COV_IN","OCT_COV_IN","NOV_COV_IN","DEC_COV_IN","SUB_BRTH_DT","MBR_SFX_NO"
]

df_Subscriber_DS = df_Lnk_Sub_DS
for c in final_cols:
    # If the schema from the JSON says "char()" or "varchar", we rpad to the length if known
    if c in ["TAX_YR"]:
        df_Subscriber_DS = df_Subscriber_DS.withColumn(c, F.rpad(F.col(c), 4, " "))
    if c in ["SUB_MIDINIT"]:
        df_Subscriber_DS = df_Subscriber_DS.withColumn(c, F.rpad(F.col(c), 1, " "))
    if c in ["SUB_HOME_ADDR_ZIP_CD_5","SUB_MAIL_ADDR_ZIP_CD_5"]:
        df_Subscriber_DS = df_Subscriber_DS.withColumn(c, F.rpad(F.col(c), 5, " "))
    if c in ["SUB_HOME_ADDR_ZIP_CD_4","SUB_MAIL_ADDR_ZIP_CD_4"]:
        df_Subscriber_DS = df_Subscriber_DS.withColumn(c, F.rpad(F.col(c), 4, " "))
    if c in ["JAN_COV_IN","FEB_COV_IN","MAR_COV_IN","APR_COV_IN","MAY_COV_IN","JUN_COV_IN","JUL_COV_IN","AUG_COV_IN","SEP_COV_IN","OCT_COV_IN","NOV_COV_IN","DEC_COV_IN"]:
        df_Subscriber_DS = df_Subscriber_DS.withColumn(c, F.rpad(F.col(c), 1, " "))

df_Subscriber_DS = df_Subscriber_DS.select(final_cols)

write_files(
    df_Subscriber_DS,
    f"{adls_path}/ds/SUB_MA_DOR_.{SrcSysCd}.extr.{RunID}.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote='"',
    nullValue=None
)