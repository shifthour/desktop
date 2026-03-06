# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2018 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC CALLED BY : EdwMassKc1099ExtCntl
# MAGIC 
# MAGIC PROCESSING : Extracts MADOR data from EDW and create the XML
# MAGIC 
# MAGIC MODIFICATIONS :
# MAGIC 
# MAGIC Developer                                 Date                 \(9)Project                                                Change Description                                               \(9)Development Project\(9)          Code Reviewer          Date Reviewed       
# MAGIC ------------------                               --------------------     \(9)------------------------                             -----------------------------------------------------------------------             \(9)------------------------------\(9)         -------------------------------   ----------------------------       
# MAGIC Sethuraman Rajendran           \(9) 2018-06-05\(9)5839\(9)                                          Original Programming\(9)\(9)\(9)          \(9)EnterpriseDev2                         Kalyan Neelam         2018-06-12
# MAGIC Abhiram Dasarathy\(9)\(9)2019-12-24\(9)F-114877\(9)\(9)\(9)Cleaned up errors in the XML job\(9)\(9)\(9)EnterpriseDev2\(9)\(9) Jaideep Mankala      12/26/2019
# MAGIC Deepika C                                2023-01-25             US 557759                              Updated EDWMassAllExtr SQL to use WHERE                      EnterpriseDev2                         Jeyaprasanna            2023-01-25
# MAGIC                                                                                                                                MBR.LAST_UPDT_RUN_CYC_EXCTN_DT_SK > 
# MAGIC                                                                                                                                '#LastUpdRunDt#' instead of >= so that old records are 
# MAGIC                                                                                                                                not extracted.
# MAGIC                                                                                                                                Added distinct to select statement to avoid duplicates due to multiple coverage

# MAGIC EDWMassAllExtrToNextpage - Extracts MADOR data from EDW and create the XML
# MAGIC Extract Data from All the four tables to create the output XML
# MAGIC Edit data and format into Output (XML) format
# MAGIC Create the output XML file
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DateType, TimestampType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Enterprise
# COMMAND ----------


# Retrieve Job/Stage Parameters
EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
Tax_Year = get_widget_value('Tax_Year','')
ExtractTimestamp = get_widget_value('ExtractTimestamp','')
LastUpdRunCyc = get_widget_value('LastUpdRunCyc','')
LastUpdRunDt = get_widget_value('LastUpdRunDt','')
AsOfDtm = get_widget_value('AsOfDtm','')

# EDWMassAllExtr (DB2ConnectorPX) - Read from EDW
extract_query = f"""
SELECT DISTINCT
MBR.TAX_YR,
MBR.MBR_ID,
MBR.GRP_ID,
SUB.SUB_ID,
MBR.MBR_FIRST_NM,
MBR.MBR_MIDINIT,
MBR.MBR_LAST_NM,
MBR.MBR_BRTH_DT,
MBR.MBR_MAIL_ADDR_LN_1,
MBR.MBR_MAIL_ADDR_LN_2,
MBR.MBR_MAIL_ADDR_CITY_NM,
MBR.MBR_MAIL_ADDR_ST_CD,
MBR.MBR_MAIL_ADDR_ZIP_CD_5,
MBR.MBR_MAIL_ADDR_ZIP_CD_4,
MBR.MBR_RELSHP_CD,
COV.MBR_COV_EFF_DT,
COV.MBR_COV_TERM_DT,
MBR.CRCTN_IN
FROM {EDWOwner}.GRP_MA_DOR_D GRP
INNER JOIN {EDWOwner}.SUB_MA_DOR_D SUB
  ON GRP.GRP_MA_DOR_SK = SUB.GRP_MA_DOR_SK
INNER JOIN {EDWOwner}.MBR_MA_DOR_D MBR
  ON SUB.SUB_MA_DOR_SK = MBR.SUB_MA_DOR_SK
INNER JOIN {EDWOwner}.MBR_MA_DOR_COV_F COV
  ON MBR.MBR_MA_DOR_SK = COV.MBR_MA_DOR_SK
WHERE COV.PROD_CRBL_COV_CD = 'Y'
  AND COV.LAST_UPDT_RUN_CYC_EXCTN_DT_SK > '{LastUpdRunDt}'

UNION

SELECT DISTINCT
MBR.TAX_YR,
MBR.MBR_ID,
MBR.GRP_ID,
SUB.SUB_ID,
MBR.MBR_FIRST_NM,
MBR.MBR_MIDINIT,
MBR.MBR_LAST_NM,
MBR.MBR_BRTH_DT,
MBR.MBR_MAIL_ADDR_LN_1,
MBR.MBR_MAIL_ADDR_LN_2,
MBR.MBR_MAIL_ADDR_CITY_NM,
MBR.MBR_MAIL_ADDR_ST_CD,
MBR.MBR_MAIL_ADDR_ZIP_CD_5,
MBR.MBR_MAIL_ADDR_ZIP_CD_4,
MBR.MBR_RELSHP_CD,
COV.MBR_COV_EFF_DT,
COV.MBR_COV_TERM_DT,
MBR.CRCTN_IN
FROM {EDWOwner}.GRP_MA_DOR_D GRP
INNER JOIN {EDWOwner}.SUB_MA_DOR_D SUB
  ON GRP.GRP_MA_DOR_SK = SUB.GRP_MA_DOR_SK
INNER JOIN {EDWOwner}.MBR_MA_DOR_D MBR
  ON SUB.SUB_MA_DOR_SK = MBR.SUB_MA_DOR_SK
INNER JOIN {EDWOwner}.MBR_MA_DOR_COV_F COV
  ON MBR.MBR_MA_DOR_SK = COV.MBR_MA_DOR_SK
WHERE MBR.SRC_SYS_CD = 'BCBSSC'
  AND COV.LAST_UPDT_RUN_CYC_EXCTN_DT_SK > '{LastUpdRunDt}'
"""

jdbc_url, jdbc_props = get_db_config(edw_secret_name)
df_EDWMassAllExtr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# Tr_Format_Mbr_Cov (CTransformerStage) - Produce two outputs:
# 1) EdwMassMbrCovAllOut  -> used by Mbr_Cnt
# 2) Join                 -> used by Join_30

df_temp = (
    df_EDWMassAllExtr
    .withColumn(
        "CRCTN_IN",
        F.when(
            trim(
                F.when(F.col("CRCTN_IN").isNotNull(), F.col("CRCTN_IN")).otherwise(F.lit(""))
            ) == "",
            F.lit("")
        ).otherwise(F.col("CRCTN_IN"))
    )
    .withColumn(
        "STTUS",
        F.when(F.col("MBR_RELSHP_CD") == "SUB", F.lit("S")).otherwise(F.lit("D"))
    )
    .withColumn(
        "PRNT_SUB_NO",
        F.when(F.col("MBR_RELSHP_CD") == "DPNDT", F.col("SUB_ID")).otherwise(F.lit(None))
    )
    .withColumn("SUB_NO", F.col("MBR_ID"))
    .withColumn(
        "ADDR_ZIP_CD",
        F.when(
            trim(
                F.when(F.col("MBR_MAIL_ADDR_ZIP_CD_4").isNotNull(), F.col("MBR_MAIL_ADDR_ZIP_CD_4")).otherwise(F.lit(""))
            ) == "",
            trim(
                F.when(F.col("MBR_MAIL_ADDR_ZIP_CD_5").isNotNull(), F.col("MBR_MAIL_ADDR_ZIP_CD_5")).otherwise(F.lit(""))
            )
        ).otherwise(
            F.concat(
                trim(
                    F.when(F.col("MBR_MAIL_ADDR_ZIP_CD_5").isNotNull(), F.col("MBR_MAIL_ADDR_ZIP_CD_5")).otherwise(F.lit(""))
                ),
                F.lit("-"),
                trim(
                    F.when(F.col("MBR_MAIL_ADDR_ZIP_CD_4").isNotNull(), F.col("MBR_MAIL_ADDR_ZIP_CD_4")).otherwise(F.lit(""))
                )
            )
        )
    )
    .withColumn(
        "DOC_NM",
        F.concat(F.lit("BCBSKC-"), F.col("TAX_YR"), F.lit("-"), F.lit(AsOfDtm))
    )
    .withColumn(
        "DOC_ID",
        F.concat(F.col("GRP_ID"), F.lit("-"), F.lit("MBR"), F.col("MBR_ID"))
    )
    .withColumn("CO_NM", F.lit("Blue Cross and Blue Shield of Kansas City"))
    .withColumn("CO_ID", F.lit("431257251"))
    .withColumn(
        "Location",
        F.lit("http://www.dor.state.ma.us/efile http://www.dor.state.ma.us/efile/1099HC/R1v2.3/MADORForm1099-HC.xsd")
    )
    .withColumn("header", F.lit("Test"))
    .withColumn("RUNCYCLE", F.lit(LastUpdRunCyc))
)

df_EdwMassMbrCovAllOut = df_temp.select("RUNCYCLE")

df_Join = df_temp.select(
    F.col("DOC_NM"),
    F.col("DOC_ID"),
    F.col("TAX_YR"),
    F.col("CO_NM"),
    F.col("CO_ID"),
    F.col("MBR_FIRST_NM").alias("SUB_FIRST_NM"),
    F.col("MBR_MIDINIT").alias("SUB_MIDINIT"),
    F.col("MBR_LAST_NM").alias("SUB_LAST_NM"),
    F.col("MBR_MAIL_ADDR_LN_1").alias("ADDR_LN_1"),
    F.col("MBR_MAIL_ADDR_LN_2").alias("ADDR_LN_2"),
    F.col("MBR_MAIL_ADDR_CITY_NM").alias("ADDR_CITY"),
    F.col("MBR_MAIL_ADDR_ST_CD").alias("ADDR_ST"),
    F.col("ADDR_ZIP_CD"),
    F.col("MBR_BRTH_DT"),
    F.col("STTUS"),
    F.col("SUB_NO"),
    F.col("PRNT_SUB_NO"),
    F.col("MBR_COV_EFF_DT").alias("COV_EFF_DT"),
    F.col("MBR_COV_TERM_DT").alias("COV_THRU_DT"),
    F.col("CRCTN_IN"),
    F.col("Location"),
    F.col("header"),
    F.col("RUNCYCLE")
)

# Mbr_Cnt (PxAggregator)
df_Mbr_Cnt = df_EdwMassMbrCovAllOut.groupBy("RUNCYCLE").agg(F.count("*").alias("MemberCount"))

# Join_30 (PxJoin) - left outer join on RUNCYCLE
df_Join_30 = (
    df_Join.alias("Join")
    .join(
        df_Mbr_Cnt.alias("Agg"),
        F.col("Join.RUNCYCLE") == F.col("Agg.RUNCYCLE"),
        "left"
    )
    .select(
        F.col("Join.SUB_NO").alias("SUB_NO"),
        F.col("Agg.MemberCount").alias("MemberCount"),
        F.col("Join.DOC_NM").alias("DOC_NM"),
        F.col("Join.DOC_ID").alias("DOC_ID"),
        F.col("Join.TAX_YR").alias("TAX_YR"),
        F.col("Join.CO_NM").alias("CO_NM"),
        F.col("Join.CO_ID").alias("CO_ID"),
        F.col("Join.SUB_FIRST_NM").alias("SUB_FIRST_NM"),
        F.col("Join.SUB_MIDINIT").alias("SUB_MIDINIT"),
        F.col("Join.SUB_LAST_NM").alias("SUB_LAST_NM"),
        F.col("Join.ADDR_LN_1").alias("ADDR_LN_1"),
        F.col("Join.ADDR_LN_2").alias("ADDR_LN_2"),
        F.col("Join.ADDR_CITY").alias("ADDR_CITY"),
        F.col("Join.ADDR_ST").alias("ADDR_ST"),
        F.col("Join.ADDR_ZIP_CD").alias("ADDR_ZIP_CD"),
        F.col("Join.MBR_BRTH_DT").alias("MBR_BRTH_DT"),
        F.col("Join.STTUS").alias("STTUS"),
        F.col("Join.PRNT_SUB_NO").alias("PRNT_SUB_NO"),
        F.col("Join.COV_EFF_DT").alias("COV_EFF_DT"),
        F.col("Join.COV_THRU_DT").alias("COV_THRU_DT"),
        F.col("Join.CRCTN_IN").alias("CRCTN_IN"),
        F.col("Join.Location").alias("Location"),
        F.col("Join.header").alias("header")
    )
)

# Trns_XML (CTransformerStage)
df_Trns_XML = (
    df_Join_30
    .select(
        F.col("Location").alias("schemaLocation"),
        F.col("DOC_NM").alias("documentName"),
        F.col("MemberCount").alias("documentCount"),
        F.col("TAX_YR").alias("TaxYear"),
        F.col("CO_NM").alias("CompanyName"),
        F.col("CO_ID").alias("CompanyID"),
        F.col("DOC_ID").alias("DOC_ID"),
        F.col("SUB_FIRST_NM").alias("SUB_FIRST_NM"),
        F.col("SUB_MIDINIT").alias("SUB_MID_NM"),
        F.col("SUB_LAST_NM").alias("SUB_LAST_NM"),
        F.col("ADDR_LN_1").alias("Address1"),
        F.col("ADDR_LN_2").alias("Address2"),
        F.col("ADDR_CITY").alias("City"),
        F.col("ADDR_ST").alias("State"),
        F.col("ADDR_ZIP_CD").alias("ZIPCode"),
        F.col("MBR_BRTH_DT").alias("DateOfBirth"),
        F.col("STTUS").alias("Status"),
        F.col("SUB_NO").alias("SubscriberNumber"),
        F.col("PRNT_SUB_NO").alias("Print_Sub_No"),
        F.col("COV_EFF_DT").alias("CoverageEffectiveDate"),
        F.col("COV_THRU_DT").alias("CoverageThroughDate"),
        F.col("CRCTN_IN").alias("CorrectedFlag")
    )
)

# XML_Output_18 (XMLOutputPX) - Write final XML file
df_XML_Output_18 = df_Trns_XML

# Apply rpad for all presumed string columns (unknown lengths shown as <...>).
# documentCount presumed numeric, so kept as is.
df_XML_Output_18_final = df_XML_Output_18.select(
    F.rpad(F.col("schemaLocation"), <...>, " ").alias("schemaLocation"),
    F.rpad(F.col("documentName"), <...>, " ").alias("documentName"),
    F.col("documentCount").alias("documentCount"),
    F.rpad(F.col("TaxYear"), <...>, " ").alias("TaxYear"),
    F.rpad(F.col("CompanyName"), <...>, " ").alias("CompanyName"),
    F.rpad(F.col("CompanyID"), <...>, " ").alias("CompanyID"),
    F.rpad(F.col("DOC_ID"), <...>, " ").alias("DOC_ID"),
    F.rpad(F.col("SUB_FIRST_NM"), <...>, " ").alias("SUB_FIRST_NM"),
    F.rpad(F.col("SUB_MID_NM"), <...>, " ").alias("SUB_MID_NM"),
    F.rpad(F.col("SUB_LAST_NM"), <...>, " ").alias("SUB_LAST_NM"),
    F.rpad(F.col("Address1"), <...>, " ").alias("Address1"),
    F.rpad(F.col("Address2"), <...>, " ").alias("Address2"),
    F.rpad(F.col("City"), <...>, " ").alias("City"),
    F.rpad(F.col("State"), <...>, " ").alias("State"),
    F.rpad(F.col("ZIPCode"), <...>, " ").alias("ZIPCode"),
    F.rpad(F.col("DateOfBirth"), <...>, " ").alias("DateOfBirth"),
    F.rpad(F.col("Status"), <...>, " ").alias("Status"),
    F.rpad(F.col("SubscriberNumber"), <...>, " ").alias("SubscriberNumber"),
    F.rpad(F.col("Print_Sub_No"), <...>, " ").alias("Print_Sub_No"),
    F.rpad(F.col("CoverageEffectiveDate"), <...>, " ").alias("CoverageEffectiveDate"),
    F.rpad(F.col("CoverageThroughDate"), <...>, " ").alias("CoverageThroughDate"),
    F.rpad(F.col("CorrectedFlag"), <...>, " ").alias("CorrectedFlag")
)

write_files(
    df_XML_Output_18_final,
    f"{adls_path_publish}/external/Mador1099_{ExtractTimestamp}.xml",
    delimiter='',
    mode='overwrite',
    is_parquet=False,
    header=True,
    quote='\"',
    nullValue=None
)