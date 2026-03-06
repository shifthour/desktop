# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2019 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC JOB NAME:  EdgeRiskAsmntCmsMbrEnrPerdHistBillMoCtUpdt
# MAGIC 
# MAGIC DESCRIPTION:  This job updates Member Enrollment Perd History data to CMS_MBR_ENR_PERD_HIST table 
# MAGIC 
# MAGIC CALLED BY:  EdgeRiskAsmntEnrSbmsnRAExtrSeq
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                   Date                     Project/Ticket #\(9)      Change Description\(9)\(9)\(9)          Development Project\(9)     Code Reviewer\(9)       Date Reviewed       
# MAGIC -------------------------          -------------------          --------------------------            ----------------------------------------------------------------------              ---------------------------------          ------------------------          -------------------------  
# MAGIC Harsha Ravuri           2019-03-05            5873 Risk Adjustment     Original Programming                                                  EnterpriseDev2                   Abhiram Dasrathy      2019-03-07


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
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DecimalType,
    TimestampType,
    BooleanType
)
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Enterprise
# COMMAND ----------


EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
EDGERiskAsmntOwner = get_widget_value('EDGERiskAsmntOwner','')
edgeriskasmnt_secret_name = get_widget_value('edgeriskasmnt_secret_name','')
AdjYear = get_widget_value('AdjYear','')

jdbc_url_edgeriskasmnt, jdbc_props_edgeriskasmnt = get_db_config(edgeriskasmnt_secret_name)
extract_query_ODBC_CMS_ENR_MO = """SELECT 
    CMS_ENR_MO.ENR_MO_NO,
    CMS_ENR_MO.RNG_MIN_ENR_DAYS_NO,
    CMS_ENR_MO.RNG_MAX_ENR_DAYS_NO
FROM 
    {}.CMS_ENR_MO
""".format(EDGERiskAsmntOwner)
df_ODBC_CMS_ENR_MO = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_edgeriskasmnt)
    .options(**jdbc_props_edgeriskasmnt)
    .option("query", extract_query_ODBC_CMS_ENR_MO)
    .load()
)

schema_CMS_MBR_ENR_PERD_HIST = StructType([
    StructField("ISSUER_ID", StringType(), nullable=False),
    StructField("FILE_ID", StringType(), nullable=False),
    StructField("MBR_RCRD_ID", IntegerType(), nullable=False),
    StructField("MBR_ENR_PERD_RCRD_ID", IntegerType(), nullable=False),
    StructField("SUB_MBR_UNIQ_KEY", IntegerType(), nullable=False),
    StructField("SUB_INDV_BE_KEY", DecimalType(38,10), nullable=True),
    StructField("MBR_INDV_BE_KEY", DecimalType(38,10), nullable=True),
    StructField("MBR_UNIQ_KEY", IntegerType(), nullable=False),
    StructField("MBR_GNDR_CD", StringType(), nullable=False),
    StructField("QHP_ID", StringType(), nullable=False),
    StructField("GNRTN_DTM", TimestampType(), nullable=False),
    StructField("MBR_ENR_TERM_DT_SK", TimestampType(), nullable=False),
    StructField("MBR_ENR_EFF_DT_SK", TimestampType(), nullable=False),
    StructField("MBR_BRTH_DT", TimestampType(), nullable=False),
    StructField("PERD_AGE_YR_NO", IntegerType(), nullable=True),
    StructField("PERD_AGE_MDL_TYP", StringType(), nullable=True),
    StructField("MBR_RELSHP_CD", StringType(), nullable=True),
])
df_CMS_MBR_ENR_PERD_HIST = (
    spark.read.format("csv")
    .schema(schema_CMS_MBR_ENR_PERD_HIST)
    .option("sep", ",")
    .option("quote", "\"")
    .option("header", "false")
    .option("nullValue", None)
    .load(f"{adls_path}/load/CMS_MBR_ENR_PERD_HIST_updt.dat")
)

df_Date_Diff_intermediate = (
    df_CMS_MBR_ENR_PERD_HIST
    .withColumn("svMaxTermDt", TimestampToDate(F.col("MBR_ENR_TERM_DT_SK")))
    .withColumn("svYearOfTermDt", YearFromDate(F.col("svMaxTermDt")))
    .withColumn("svMinEffDt", TimestampToDate(F.col("MBR_ENR_EFF_DT_SK")))
    .withColumn("svYearOfEffDt", YearFromDate(F.col("svMinEffDt")))
    .withColumn(
        "svEffDtCheck",
        F.when(
            F.col("svYearOfTermDt") == F.col(AdjYear),
            F.when(
                F.col("svYearOfEffDt") == F.col(AdjYear),
                F.col("svMinEffDt")
            ).otherwise(
                StringToDate(F.concat(F.col(AdjYear), F.lit("-01-01")), "%yyyy-%mm-%dd")
            )
        ).otherwise(F.col("svMinEffDt"))
    )
    .withColumn(
        "svMbrMO",
        LENGTH_DAYS_SYB_TS_EE(F.col("svMinEffDt"), F.col("svMaxTermDt"), F.lit("0"))
    )
)

df_Date_Diff = df_Date_Diff_intermediate.select(
    F.col("svMbrMO").alias("No_Of_Days"),
    F.col("ISSUER_ID").alias("ISSUER_ID"),
    F.col("FILE_ID").alias("FILE_ID"),
    F.col("MBR_RCRD_ID").alias("MBR_RCRD_ID"),
    F.col("MBR_ENR_PERD_RCRD_ID").alias("MBR_ENR_PERD_RCRD_ID"),
    F.col("SUB_MBR_UNIQ_KEY").alias("SUB_MBR_UNIQ_KEY"),
    F.col("SUB_INDV_BE_KEY").alias("SUB_INDV_BE_KEY"),
    F.col("MBR_INDV_BE_KEY").alias("MBR_INDV_BE_KEY"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("MBR_GNDR_CD").alias("MBR_GNDR_CD"),
    F.col("QHP_ID").alias("QHP_ID"),
    F.col("GNRTN_DTM").alias("GNRTN_DTM"),
    F.col("MBR_ENR_TERM_DT_SK").alias("MBR_ENR_TERM_DT_SK"),
    F.col("MBR_ENR_EFF_DT_SK").alias("MBR_ENR_EFF_DT_SK"),
    F.col("MBR_BRTH_DT").alias("MBR_BRTH_DT"),
    F.col("PERD_AGE_YR_NO").alias("PERD_AGE_YR_NO"),
    F.col("PERD_AGE_MDL_TYP").alias("PERD_AGE_MDL_TYP"),
    F.col("MBR_RELSHP_CD").alias("MBR_RELSHP_CD")
)

df_lkp_Months = (
    df_Date_Diff.alias("Age_Mdl_Typ")
    .join(
        df_ODBC_CMS_ENR_MO.alias("Age_MdlTyp"),
        F.lit(True),
        "left"
    )
    .select(
        F.col("Age_Mdl_Typ.ISSUER_ID").alias("ISSUER_ID"),
        F.col("Age_Mdl_Typ.FILE_ID").alias("FILE_ID"),
        F.col("Age_Mdl_Typ.MBR_RCRD_ID").alias("MBR_RCRD_ID"),
        F.col("Age_Mdl_Typ.MBR_ENR_PERD_RCRD_ID").alias("MBR_ENR_PERD_RCRD_ID"),
        F.col("Age_Mdl_Typ.SUB_MBR_UNIQ_KEY").alias("SUB_MBR_UNIQ_KEY"),
        F.col("Age_Mdl_Typ.SUB_INDV_BE_KEY").alias("SUB_INDV_BE_KEY"),
        F.col("Age_Mdl_Typ.MBR_INDV_BE_KEY").alias("MBR_INDV_BE_KEY"),
        F.col("Age_Mdl_Typ.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        F.col("Age_Mdl_Typ.MBR_GNDR_CD").alias("MBR_GNDR_CD"),
        F.col("Age_Mdl_Typ.QHP_ID").alias("QHP_ID"),
        F.col("Age_Mdl_Typ.GNRTN_DTM").alias("GNRTN_DTM"),
        F.col("Age_Mdl_Typ.MBR_ENR_TERM_DT_SK").alias("MBR_ENR_TERM_DT_SK"),
        F.col("Age_Mdl_Typ.MBR_ENR_EFF_DT_SK").alias("MBR_ENR_EFF_DT_SK"),
        F.col("Age_Mdl_Typ.MBR_BRTH_DT").alias("MBR_BRTH_DT"),
        F.col("Age_Mdl_Typ.PERD_AGE_YR_NO").alias("PERD_AGE_YR_NO"),
        F.col("Age_Mdl_Typ.PERD_AGE_MDL_TYP").alias("PERD_AGE_MDL_TYP"),
        F.col("Age_MdlTyp.ENR_MO_NO").alias("PERD_MBR_MO_CT"),
        F.col("Age_Mdl_Typ.MBR_RELSHP_CD").alias("MBR_RELSHP_CD")
    )
)

df_transform_NonDpndt = (
    df_lkp_Months
    .withColumn("svRelationshipCd", F.col("MBR_RELSHP_CD"))
    .withColumn("svMoMntCt", F.col("PERD_MBR_MO_CT"))
    .withColumn(
        "svBillMoCt",
        F.when(
            (F.col("svRelationshipCd") == 'SUB') | (F.col("svRelationshipCd") == 'SPOUSE'),
            F.col("svMoMntCt")
        ).otherwise(F.lit(None))
    )
)

df_DPNDT = df_transform_NonDpndt.filter(F.col("MBR_RELSHP_CD") == 'DPNDT').select(
    F.col("ISSUER_ID"),
    F.col("FILE_ID"),
    F.col("MBR_RCRD_ID"),
    F.col("MBR_ENR_PERD_RCRD_ID"),
    F.col("SUB_MBR_UNIQ_KEY"),
    F.col("SUB_INDV_BE_KEY"),
    F.col("MBR_INDV_BE_KEY"),
    F.col("MBR_UNIQ_KEY"),
    F.col("MBR_GNDR_CD"),
    F.col("QHP_ID"),
    F.col("MBR_ENR_TERM_DT_SK"),
    F.col("MBR_ENR_EFF_DT_SK"),
    F.col("MBR_BRTH_DT"),
    F.col("PERD_AGE_YR_NO"),
    F.col("PERD_AGE_MDL_TYP"),
    F.col("PERD_MBR_MO_CT"),
    F.col("MBR_RELSHP_CD")
)

df_non_DPNDT = df_transform_NonDpndt.filter(
    (F.col("svBillMoCt").isNotNull())
    | (
        (F.col("MBR_RELSHP_CD").isNotNull() & (F.col("MBR_RELSHP_CD") != 'DPNDT'))
        | (F.col("MBR_RELSHP_CD").isNull())
    )
).select(
    F.col("ISSUER_ID"),
    F.col("FILE_ID"),
    F.col("MBR_RCRD_ID"),
    F.col("MBR_ENR_PERD_RCRD_ID"),
    F.col("PERD_AGE_YR_NO"),
    F.col("PERD_AGE_MDL_TYP"),
    F.col("PERD_MBR_MO_CT"),
    F.when(
        F.col("svBillMoCt").isNull() & (F.col("MBR_RELSHP_CD") != 'DPNDT'),
        '0'
    ).otherwise(F.col("svBillMoCt")).alias("PERD_BILL_MO_CT")
)

df_DPNDT1 = df_transform_NonDpndt.filter(F.col("MBR_RELSHP_CD") == 'DPNDT').select(
    F.col("ISSUER_ID"),
    F.col("FILE_ID"),
    F.col("MBR_RCRD_ID"),
    F.col("MBR_ENR_PERD_RCRD_ID"),
    F.col("SUB_MBR_UNIQ_KEY"),
    F.col("MBR_INDV_BE_KEY"),
    F.col("SUB_INDV_BE_KEY").alias("SUB_INDV_BE_KEY"),
    F.col("MBR_ENR_TERM_DT_SK"),
    F.col("MBR_ENR_EFF_DT_SK"),
    F.lit("1").alias("Flag"),
    F.col("MBR_UNIQ_KEY"),
    F.col("MBR_GNDR_CD"),
    F.col("QHP_ID"),
    F.col("MBR_BRTH_DT"),
    F.col("PERD_AGE_YR_NO"),
    F.col("PERD_AGE_MDL_TYP"),
    F.col("PERD_MBR_MO_CT"),
    F.col("MBR_RELSHP_CD")
)

jdbc_url_edw, jdbc_props_edw = get_db_config(edw_secret_name)
extract_query_MBR = """SELECT DISTINCT
    mbr.MBR_UNIQ_KEY, 
    mbr.MBR_BRTH_DT_SK, 
    mbr.MBR_GNDR_CD, 
    mbr.MBR_INDV_BE_KEY, 
    mbr.SUB_UNIQ_KEY,
    mbr.MBR_RELSHP_CD,
    sub.SUB_INDV_BE_KEY,
    mbr.MBR_SFX_NO
FROM {}.MBR_D mbr, 
     {}.MBR_ENR_D mbrEnr,
     {}.GRP_D grp,
     {}.MBR_ENR_QHP_D MBR_QHP,
     {}.SUB_D sub
WHERE  
    mbr.MBR_SK = mbrEnr.MBR_SK 
    AND grp.GRP_SK = mbr.GRP_SK
    AND MBR_QHP.MBR_ENR_SK = mbrEnr.MBR_ENR_SK
    AND MBR_QHP.MBR_UNIQ_KEY = mbrEnr.MBR_UNIQ_KEY
    AND MBR_QHP.MBR_ENR_EFF_DT_SK = mbrEnr.MBR_ENR_EFF_DT_SK
    AND mbrEnr.MBR_ENR_ELIG_IN = 'Y'
    AND mbrEnr.MBR_ENR_CLS_PLN_PROD_CAT_CD = 'MED'
    AND MBR_QHP.MBR_ENR_CLS_PLN_PROD_CAT_CD = 'MED'
    AND mbrEnr.PROD_SH_NM IN ('PCB', 'PC', 'BCARE', 'BLUE-ACCESS', 'BLUE-SELECT')
    AND mbrEnr.CLS_ID <> 'MHIP'
    AND MBR_QHP.QHP_ID <> 'NA'
    AND mbr.SUB_UNIQ_KEY = sub.SUB_UNIQ_KEY
""".format(EDWOwner, EDWOwner, EDWOwner, EDWOwner, EDWOwner)
df_MBR = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_edw)
    .options(**jdbc_props_edw)
    .option("query", extract_query_MBR)
    .load()
)

df_Suffix = df_MBR.alias("Mbr").select(
    StringToTimestamp(F.concat(F.col("Mbr.MBR_BRTH_DT_SK"), F.lit(" 00:00:00")), "%yyyy-%mm-%dd %hh:%nn:%ss").alias("MBR_BRTH_DT"),
    F.col("Mbr.MBR_GNDR_CD").alias("MBR_GNDR_CD"),
    F.col("Mbr.MBR_INDV_BE_KEY").alias("MBR_INDV_BE_KEY"),
    F.col("Mbr.SUB_UNIQ_KEY").alias("SUB_MBR_UNIQ_KEY"),
    F.col("Mbr.SUB_INDV_BE_KEY").alias("SUB_INDV_BE_KEY"),
    F.col("Mbr.MBR_RELSHP_CD").alias("MBR_RELSHP_CD"),
    F.col("Mbr.MBR_SFX_NO").alias("MBR_SFX_NO")
)

df_Lookup_427 = df_DPNDT.alias("DPNDT").join(
    df_DPNDT1.alias("DPNDT1"),
    (
        (F.col("DPNDT.ISSUER_ID") == F.col("DPNDT1.ISSUER_ID")) &
        (F.col("DPNDT.FILE_ID") == F.col("DPNDT1.FILE_ID")) &
        (F.col("DPNDT.MBR_RCRD_ID") == F.col("DPNDT1.MBR_RCRD_ID")) &
        (F.col("DPNDT.MBR_ENR_PERD_RCRD_ID") == F.col("DPNDT1.MBR_ENR_PERD_RCRD_ID")) &
        (F.col("DPNDT.SUB_INDV_BE_KEY") == F.col("DPNDT1.SUB_INDV_BE_KEY")) &
        (F.col("DPNDT.MBR_ENR_TERM_DT_SK") == F.col("DPNDT1.MBR_ENR_TERM_DT_SK")) &
        (F.col("DPNDT.MBR_ENR_EFF_DT_SK") == F.col("DPNDT1.MBR_ENR_EFF_DT_SK"))
        # The JSON mentions a Range() function, but the actual column references are not present in the stage data.
        # Assuming no direct variable to use for that condition, leaving it as is.
    ),
    how="left"
).select(
    F.col("DPNDT.ISSUER_ID").alias("ISSUER_ID"),
    F.col("DPNDT.FILE_ID").alias("FILE_ID"),
    F.col("DPNDT.MBR_RCRD_ID").alias("MBR_RCRD_ID"),
    F.col("DPNDT.MBR_ENR_PERD_RCRD_ID").alias("MBR_ENR_PERD_RCRD_ID"),
    F.col("DPNDT.SUB_MBR_UNIQ_KEY").alias("SUB_MBR_UNIQ_KEY"),
    F.col("DPNDT.SUB_INDV_BE_KEY").alias("SUB_INDV_BE_KEY"),
    F.col("DPNDT.MBR_INDV_BE_KEY").alias("MBR_INDV_BE_KEY"),
    F.col("DPNDT.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("DPNDT.MBR_GNDR_CD").alias("MBR_GNDR_CD"),
    F.col("DPNDT.QHP_ID").alias("QHP_ID"),
    F.col("DPNDT.MBR_ENR_TERM_DT_SK").alias("MBR_ENR_TERM_DT_SK"),
    F.col("DPNDT.MBR_ENR_EFF_DT_SK").alias("MBR_ENR_EFF_DT_SK"),
    F.col("DPNDT.MBR_BRTH_DT").alias("MBR_BRTH_DT"),
    F.col("DPNDT.PERD_AGE_YR_NO").alias("PERD_AGE_YR_NO"),
    F.col("DPNDT.PERD_AGE_MDL_TYP").alias("PERD_AGE_MDL_TYP"),
    F.col("DPNDT.PERD_MBR_MO_CT").alias("PERD_MBR_MO_CT"),
    F.col("DPNDT.MBR_RELSHP_CD").alias("MBR_RELSHP_CD"),
    F.col("DPNDT1.Flag").alias("Flag")
)

df_Tfmr = df_Lookup_427.alias("Uniq").select(
    F.col("Uniq.ISSUER_ID").alias("ISSUER_ID"),
    F.col("Uniq.FILE_ID").alias("FILE_ID"),
    F.col("Uniq.MBR_RCRD_ID").alias("MBR_RCRD_ID"),
    F.col("Uniq.MBR_ENR_PERD_RCRD_ID").alias("MBR_ENR_PERD_RCRD_ID"),
    F.col("Uniq.SUB_MBR_UNIQ_KEY").alias("SUB_MBR_UNIQ_KEY"),
    F.col("Uniq.SUB_INDV_BE_KEY").alias("SUB_INDV_BE_KEY"),
    F.col("Uniq.MBR_INDV_BE_KEY").alias("MBR_INDV_BE_KEY"),
    F.col("Uniq.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("Uniq.MBR_GNDR_CD").alias("MBR_GNDR_CD"),
    F.col("Uniq.QHP_ID").alias("QHP_ID"),
    F.col("Uniq.MBR_ENR_TERM_DT_SK").alias("MBR_ENR_TERM_DT_SK"),
    F.col("Uniq.MBR_ENR_EFF_DT_SK").alias("MBR_ENR_EFF_DT_SK"),
    F.col("Uniq.MBR_BRTH_DT").alias("MBR_BRTH_DT"),
    F.col("Uniq.PERD_AGE_YR_NO").alias("PERD_AGE_YR_NO"),
    F.col("Uniq.PERD_AGE_MDL_TYP").alias("PERD_AGE_MDL_TYP"),
    F.col("Uniq.PERD_MBR_MO_CT").alias("PERD_MBR_MO_CT"),
    F.col("Uniq.MBR_RELSHP_CD").alias("MBR_RELSHP_CD"),
    F.col("Uniq.Flag").alias("Flag")
).filter(F.col("Flag") == '1')

df_Join = (
    df_Tfmr.alias("bill")
    .join(
        df_Suffix.alias("Mbr_D"),
        (
            (F.col("bill.SUB_INDV_BE_KEY") == F.col("Mbr_D.SUB_INDV_BE_KEY")) &
            (F.col("bill.MBR_GNDR_CD") == F.col("Mbr_D.MBR_GNDR_CD")) &
            (F.col("bill.MBR_RELSHP_CD") == F.col("Mbr_D.MBR_RELSHP_CD")) &
            (F.col("bill.MBR_BRTH_DT") == F.col("Mbr_D.MBR_BRTH_DT")) &
            (F.col("bill.SUB_MBR_UNIQ_KEY") == F.col("Mbr_D.SUB_MBR_UNIQ_KEY")) &
            (F.col("bill.MBR_INDV_BE_KEY") == F.col("Mbr_D.MBR_INDV_BE_KEY"))
        ),
        how="left"
    )
    .select(
        F.col("bill.ISSUER_ID").alias("ISSUER_ID"),
        F.col("bill.FILE_ID").alias("FILE_ID"),
        F.col("bill.MBR_RCRD_ID").alias("MBR_RCRD_ID"),
        F.col("bill.MBR_ENR_PERD_RCRD_ID").alias("MBR_ENR_PERD_RCRD_ID"),
        F.col("bill.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        F.col("bill.MBR_GNDR_CD").alias("MBR_GNDR_CD"),
        F.col("bill.QHP_ID").alias("QHP_ID"),
        F.col("Mbr_D.MBR_SFX_NO").alias("MBR_SFX_NO"),
        F.col("bill.SUB_INDV_BE_KEY").alias("SUB_INDV_BE_KEY"),
        F.col("bill.MBR_ENR_TERM_DT_SK").alias("MBR_ENR_TERM_DT_SK"),
        F.col("bill.MBR_ENR_EFF_DT_SK").alias("MBR_ENR_EFF_DT_SK"),
        F.col("bill.MBR_BRTH_DT").alias("MBR_BRTH_DT"),
        F.col("bill.PERD_AGE_YR_NO").alias("PERD_AGE_YR_NO"),
        F.col("bill.PERD_AGE_MDL_TYP").alias("PERD_AGE_MDL_TYP"),
        F.col("bill.PERD_MBR_MO_CT").alias("PERD_MBR_MO_CT"),
        F.col("bill.MBR_RELSHP_CD").alias("MBR_RELSHP_CD")
    )
)

df_Tfm_Bill_Mo_Ct_intermediate = (
    df_Join.alias("Bill_MO")
    .withColumn("svSubIndvBeKey", F.col("Bill_MO.SUB_INDV_BE_KEY"))
    .withColumn(
        "svKeyBreak",
        F.when(
            F.col("svSubIndvBeKey") == F.col("svPrevSubIndvBeKey"),
            F.lit("T")
        ).otherwise(F.lit("F"))
    )
    .withColumn(
        "svCurntRowCnt",
        F.when(
            F.col("svKeyBreak") == F.lit("F"),
            F.lit(1)
        ).otherwise(F.col("svPrvCnt") + F.lit(1))
    )
    .withColumn("svPrvCnt", F.col("svCurntRowCnt"))
    .withColumn(
        "svBillMoCt",
        F.when(F.col("svCurntRowCnt") <= 3, F.col("Bill_MO.PERD_MBR_MO_CT")).otherwise(F.lit("O"))
    )
    .withColumn("svPrevSubIndvBeKey", F.col("svSubIndvBeKey"))
)

df_Tfm_Bill_Mo_Ct = df_Tfm_Bill_Mo_Ct_intermediate.select(
    F.col("Bill_MO.ISSUER_ID").alias("ISSUER_ID"),
    F.col("Bill_MO.FILE_ID").alias("FILE_ID"),
    F.col("Bill_MO.MBR_RCRD_ID").alias("MBR_RCRD_ID"),
    F.col("Bill_MO.MBR_ENR_PERD_RCRD_ID").alias("MBR_ENR_PERD_RCRD_ID"),
    F.col("Bill_MO.PERD_AGE_YR_NO").alias("PERD_AGE_YR_NO"),
    F.col("Bill_MO.PERD_AGE_MDL_TYP").alias("PERD_AGE_MDL_TYP"),
    F.col("Bill_MO.PERD_MBR_MO_CT").alias("PERD_MBR_MO_CT"),
    F.col("svBillMoCt").alias("PERD_BILL_MO_CT")
)

df_Funnel = df_non_DPNDT.alias("non_DPNDT").unionByName(df_Tfm_Bill_Mo_Ct.alias("Dpndt"))

df_Final = df_Funnel.select(
    F.col("ISSUER_ID"),
    F.col("FILE_ID"),
    F.col("MBR_RCRD_ID"),
    F.col("MBR_ENR_PERD_RCRD_ID"),
    F.col("PERD_AGE_YR_NO"),
    F.col("PERD_AGE_MDL_TYP"),
    F.col("PERD_MBR_MO_CT"),
    F.col("PERD_BILL_MO_CT")
)

df_final_write = df_Final.select(
    F.rpad(F.col("ISSUER_ID"), 5, " ").alias("ISSUER_ID"),
    F.rpad(F.col("FILE_ID"), 12, " ").alias("FILE_ID"),
    F.col("MBR_RCRD_ID").alias("MBR_RCRD_ID"),
    F.col("MBR_ENR_PERD_RCRD_ID").alias("MBR_ENR_PERD_RCRD_ID"),
    F.col("PERD_AGE_YR_NO").alias("PERD_AGE_YR_NO"),
    F.rpad(F.col("PERD_AGE_MDL_TYP"), <...>, " ").alias("PERD_AGE_MDL_TYP"),
    F.col("PERD_MBR_MO_CT").alias("PERD_MBR_MO_CT"),
    F.rpad(F.col("PERD_BILL_MO_CT"), <...>, " ").alias("PERD_BILL_MO_CT")
)

write_files(
    df_final_write,
    f"{adls_path}/load/CMS_MBR_ENR_PERD_HIST.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)