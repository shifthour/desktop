# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2013 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:   Extracts Member DSS Condition data from IDEA extract and creates a key file for MBR_RISK_MESR
# MAGIC 
# MAGIC       
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                        Date                Change Description                                                     Project #          Development Project          Code Reviewer          Date Reviewed  
# MAGIC -----------------------                ----------------------   ----------------------------------------------------------------------------       ----------------         ------------------------------------       ----------------------------      -------------------------
# MAGIC Kalyan Neelam              2013-07-10           Initial Programming                                                    5056 FEP          IntegrateNewDevl             Bhoomi Dasari            7/24/2013
# MAGIC Santosh Bokka              2013-12-01          Added 7 new fields                                                   4917  PCMH      ntegrateNewDevl               Kalyan Neelam           2013-12-26
# MAGIC 
# MAGIC Krishnakanth                 2016-10-12          Added 4 new fields                                                    30001                 IntegrateNewDev2           Jag Yelavarthi             2016-11-16                                                                                            
# MAGIC         Manivannan                                      INDV_BE_MARA_RSLT_SK
# MAGIC                                                                   RISK_MTHDLGY_CD
# MAGIC                                                                   RISK_MTHDLGY_TYP_CD
# MAGIC                                                                   RISK_MTHDLGY_TYP_SCORE_NO

# MAGIC IDEA Member Risk Measure Extract
# MAGIC Looking upon 3 different conditions seperately and updating the files accordingly
# MAGIC Apply business logic
# MAGIC Writing Sequential File to /pkey
# MAGIC Concatenating the updated files based upon the conditions and sending it through the primary key process
# MAGIC Pulling only records which start with 'R' as we need to join only on them. To minimize pulling records.
# MAGIC Hash file (hf_mbr_risk_mesr_allcol) cleared from the container - MbrRiskMesrPK
# MAGIC Snapshot
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType, LongType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


CurrRunCycle = get_widget_value('CurrRunCycle','')
RunID = get_widget_value('RunID','')
RunDate = get_widget_value('RunDate','')
YearMonth = get_widget_value('YearMonth','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
SourceSk = get_widget_value('SourceSk','')
SrcSysCd = get_widget_value('SrcSysCd','')
PrevMonth = get_widget_value('PrevMonth','')

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

extract_query_IDS_MBR = f"SELECT MBR_UNIQ_KEY,BRTH_DT_SK,FEP_LOCAL_CNTR_IN,FIRST_NM,MBR_GNDR_CD_SK,SUB_SK FROM {IDSOwner}.MBR WHERE FEP_LOCAL_CNTR_IN = 'Y'"
df_IDS_MBR = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_IDS_MBR)
    .load()
)

df_hf_etrnl_cd_mppng = spark.read.parquet(f"{adls_path}/hf_etrnl_cd_mppng.parquet")

df_TrgtCd_join = df_IDS_MBR.alias("mbr").join(
    df_hf_etrnl_cd_mppng.alias("cdmppg"),
    (F.col("mbr.MBR_GNDR_CD_SK") == F.col("cdmppg.CD_MPPNG_SK")),
    "left"
)

df_TrgtCd_brthdt = df_TrgtCd_join.select(
    F.col("mbr.SUB_SK").alias("SUB_SK"),
    F.rpad(F.col("mbr.BRTH_DT_SK"),10," ").alias("BRTH_DT_SK"),
    F.col("mbr.FIRST_NM").alias("FIRST_NM"),
    F.when(
        F.col("cdmppg.CD_MPPNG_SK").isNull() | (F.length(F.col("cdmppg.CD_MPPNG_SK")) == 0),
        F.lit(0)
    ).otherwise(F.col("cdmppg.TRGT_CD")).alias("TRGT_CD"),
    F.col("mbr.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY")
)

df_TrgtCd_frstnm = df_TrgtCd_join.select(
    F.col("mbr.SUB_SK").alias("SUB_SK"),
    F.col("mbr.FIRST_NM").alias("FIRST_NM"),
    F.rpad(F.col("mbr.BRTH_DT_SK"),10," ").alias("BRTH_DT_SK"),
    F.when(
        F.col("cdmppg.CD_MPPNG_SK").isNull() | (F.length(F.col("cdmppg.CD_MPPNG_SK")) == 0),
        F.lit(0)
    ).otherwise(F.col("cdmppg.TRGT_CD")).alias("TRGT_CD"),
    F.col("mbr.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY")
)

df_TrgtCd_trgtcd = df_TrgtCd_join.select(
    F.col("mbr.SUB_SK").alias("SUB_SK"),
    F.rpad(F.col("mbr.BRTH_DT_SK"),10," ").alias("BRTH_DT_SK"),
    F.col("mbr.FIRST_NM").alias("FIRST_NM"),
    F.when(
        F.col("cdmppg.CD_MPPNG_SK").isNull() | (F.length(F.col("cdmppg.CD_MPPNG_SK")) == 0),
        F.lit(0)
    ).otherwise(F.col("cdmppg.TRGT_CD")).alias("TRGT_CD"),
    F.col("mbr.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY")
)

df_Sort_3_in = df_TrgtCd_frstnm
df_Sort_3_sorted = df_Sort_3_in.orderBy(
    F.col("SUB_SK").asc(),
    F.col("FIRST_NM").asc(),
    F.col("MBR_UNIQ_KEY").desc()
)
df_Sort_3_dedup = dedup_sort(
    df_Sort_3_sorted,
    ["SUB_SK","FIRST_NM"],
    [("SUB_SK","A"),("FIRST_NM","A"),("MBR_UNIQ_KEY","D")]
)
df_hf_idea_mbrriskmesr_frstnm = df_Sort_3_dedup.select(
    F.col("SUB_SK"),
    F.col("FIRST_NM"),
    F.col("BRTH_DT_SK"),
    F.col("TRGT_CD"),
    F.col("MBR_UNIQ_KEY")
)

df_Sort_2_in = df_TrgtCd_brthdt
df_Sort_2_sorted = df_Sort_2_in.orderBy(
    F.col("SUB_SK").asc(),
    F.col("BRTH_DT_SK").asc(),
    F.col("MBR_UNIQ_KEY").desc()
)
df_Sort_2_dedup = dedup_sort(
    df_Sort_2_sorted,
    ["SUB_SK","BRTH_DT_SK"],
    [("SUB_SK","A"),("BRTH_DT_SK","A"),("MBR_UNIQ_KEY","D")]
)
df_hf_idea_mbrriskmesr_brthdt = df_Sort_2_dedup.select(
    F.col("SUB_SK"),
    F.col("BRTH_DT_SK"),
    F.col("FIRST_NM"),
    F.col("TRGT_CD"),
    F.col("MBR_UNIQ_KEY")
)

df_Sort_1_in = df_TrgtCd_trgtcd
df_Sort_1_sorted = df_Sort_1_in.orderBy(
    F.col("SUB_SK").asc(),
    F.col("BRTH_DT_SK").asc(),
    F.col("FIRST_NM").asc(),
    F.col("TRGT_CD").asc(),
    F.col("MBR_UNIQ_KEY").desc()
)
df_Sort_1_dedup = dedup_sort(
    df_Sort_1_sorted,
    ["SUB_SK","BRTH_DT_SK","FIRST_NM","TRGT_CD"],
    [("SUB_SK","A"),("BRTH_DT_SK","A"),("FIRST_NM","A"),("TRGT_CD","A"),("MBR_UNIQ_KEY","D")]
)
df_hf_idea_mbrriskmesr_gndrcd = df_Sort_1_dedup.select(
    F.col("SUB_SK"),
    F.col("BRTH_DT_SK"),
    F.col("FIRST_NM"),
    F.col("TRGT_CD"),
    F.col("MBR_UNIQ_KEY")
)

extract_query_MBR_RISK_MESR = (
    f"SELECT CD.TRGT_CD AS SRC_SYS_CD, MBR_RISK.MBR_UNIQ_KEY, MBR_RISK.RISK_CAT_ID, "
    f"MBR_RISK.RISK_IDNT_DT_SK FROM {IDSOwner}.MBR_RISK_MESR MBR_RISK, {IDSOwner}.CD_MPPNG CD "
    f"WHERE MBR_RISK.SRC_SYS_CD_SK = CD.CD_MPPNG_SK "
    f"AND CD.TRGT_CD IN ('MCSOURCE', 'IDEA') "
    f"AND PRCS_YR_MO_SK = '{PrevMonth}'"
)
df_MBR_RISK_MESR = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_MBR_RISK_MESR)
    .load()
)

df_hf_idea_mbrriskmesr_idntdt_lkup_in = df_MBR_RISK_MESR.select(
    F.col("SRC_SYS_CD"),
    F.col("MBR_UNIQ_KEY"),
    F.col("RISK_CAT_ID"),
    F.rpad(F.col("RISK_IDNT_DT_SK"),10," ").alias("RISK_IDNT_DT_SK")
)
df_hf_idea_mbrriskmesr_idntdt_lkup_dedup = dedup_sort(
    df_hf_idea_mbrriskmesr_idntdt_lkup_in,
    ["SRC_SYS_CD","MBR_UNIQ_KEY","RISK_CAT_ID"],
    []
)
df_hf_idea_mbrriskmesr_idntdt_lkup = df_hf_idea_mbrriskmesr_idntdt_lkup_dedup.select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("RISK_CAT_ID").alias("RISK_CAT_ID"),
    F.col("RISK_IDNT_DT_SK").alias("RISK_IDNT_DT_SK")
)

extract_query_P_SRC_DOMAIN_TRNSLTN = (
    f"SELECT SRC_DOMAIN_TX, TRGT_DOMAIN_TX FROM {IDSOwner}.P_SRC_DOMAIN_TRNSLTN "
    f"WHERE SRC_SYS_CD = 'IDEA' AND DOMAIN_ID = 'SEVERITY CODE'"
)
df_P_SRC_DOMAIN_TRNSLTN = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_P_SRC_DOMAIN_TRNSLTN)
    .load()
)

df_hf_idea_mbrriskmesr_svrtycd_lkup_in = df_P_SRC_DOMAIN_TRNSLTN.select(
    F.col("SRC_DOMAIN_TX"),
    F.col("TRGT_DOMAIN_TX")
)
df_hf_idea_mbrriskmesr_svrtycd_lkup_dedup = dedup_sort(
    df_hf_idea_mbrriskmesr_svrtycd_lkup_in,
    ["SRC_DOMAIN_TX"],
    []
)
df_hf_idea_mbrriskmesr_svrtycd_lkup = df_hf_idea_mbrriskmesr_svrtycd_lkup_dedup.select(
    F.col("SRC_DOMAIN_TX"),
    F.col("TRGT_DOMAIN_TX")
)

extract_query_IDS_SUB = f"SELECT SUB_ID, SUB_SK FROM {IDSOwner}.SUB"
df_IDS_SUB = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_IDS_SUB)
    .load()
)

df_hf_idea_mbrriskmesr_subsk_in = df_IDS_SUB.select(
    F.col("SUB_ID"),
    F.col("SUB_SK")
)
df_hf_idea_mbrriskmesr_subsk_dedup = dedup_sort(
    df_hf_idea_mbrriskmesr_subsk_in,
    ["SUB_ID"],
    []
)
df_hf_idea_mbrriskmesr_subsk = df_hf_idea_mbrriskmesr_subsk_dedup.select(
    F.col("SUB_ID"),
    F.col("SUB_SK")
)

schema_FEP_MBR_DSS_ID = StructType([
    StructField("MBR_ID", StringType(), True),
    StructField("CNTR_ID", StringType(), True),
    StructField("DSS_CD", StringType(), True),
    StructField("FIRST_NM", StringType(), True),
    StructField("LAST_NM", StringType(), True),
    StructField("DOB", StringType(), True),
    StructField("CUR_AGE", IntegerType(), True),
    StructField("GNDR_CD", StringType(), True),
    StructField("ADDR_LN_1", StringType(), True),
    StructField("ADDR_CITY_NM", StringType(), True),
    StructField("ADDR_ST_CD", StringType(), True),
    StructField("ADDR_ZIP_CD", StringType(), True),
    StructField("HOME_PHN_NO", StringType(), True),
    StructField("PRI_COV_CD", StringType(), True),
    StructField("RISK_LVL", StringType(), True),
    StructField("NORM_RISK_SCORE", DecimalType(38,10), True),
    StructField("NORM_RISK_SCORE_YR_2", DecimalType(38,10), True)
])
df_FEP_MBR_DSS_ID = spark.read \
    .option("header", True) \
    .option("quote", "\"") \
    .schema(schema_FEP_MBR_DSS_ID) \
    .csv(f"{adls_path_raw}/landing/FEP_MBR_DSS_ID.dat")

df_Transformer_291 = df_FEP_MBR_DSS_ID.select(
    trim(strip_field(F.col("MBR_ID"))).alias("MBR_ID"),
    trim(strip_field(F.col("CNTR_ID"))).alias("CNTR_ID"),
    trim(strip_field(F.col("DSS_CD"))).alias("DSS_CD"),
    trim(strip_field(F.col("FIRST_NM"))).alias("FIRST_NM"),
    trim(strip_field(F.col("LAST_NM"))).alias("LAST_NM"),
    trim(strip_field(F.col("GNDR_CD"))).alias("GNDR_CD"),
    trim(strip_field(F.col("DOB"))).alias("DOB"),
    trim(strip_field(F.col("CUR_AGE"))).alias("CUR_AGE"),
    trim(strip_field(F.col("ADDR_LN_1"))).alias("ADDR_LN_1"),
    trim(strip_field(F.col("ADDR_CITY_NM"))).alias("ADDR_CITY_NM"),
    trim(strip_field(F.col("ADDR_ST_CD"))).alias("ADDR_ST_CD"),
    trim(strip_field(F.col("ADDR_ZIP_CD"))).alias("ADDR_ZIP_CD"),
    trim(strip_field(F.col("HOME_PHN_NO"))).alias("HOME_PHN_NO"),
    trim(strip_field(F.col("PRI_COV_CD"))).alias("PRI_COV_CD"),
    trim(strip_field(F.col("RISK_LVL"))).alias("RISK_LVL"),
    trim(strip_field(F.col("NORM_RISK_SCORE"))).alias("NORM_RISK_SCORE"),
    trim(strip_field(F.col("NORM_RISK_SCORE_YR_2"))).alias("NORM_RISK_SCORE_YR_2")
)

df_Transformer_217_join = df_Transformer_291.alias("Strip3").join(
    df_hf_idea_mbrriskmesr_subsk.alias("IdsSub"),
    (F.col("Strip3.CNTR_ID") == F.col("IdsSub.SUB_ID")),
    "left"
)

df_Transformer_217 = df_Transformer_217_join.select(
    F.col("Strip3.MBR_ID").alias("MBR_ID"),
    F.col("Strip3.CNTR_ID").alias("CNTR_ID"),
    F.col("Strip3.DSS_CD").alias("DSS_CD"),
    F.col("Strip3.FIRST_NM").alias("FIRST_NM"),
    F.col("Strip3.GNDR_CD").alias("GNDR_CD"),
    F.date_format(F.to_date(F.col("Strip3.DOB"), "MM/dd/yyyy"), "yyyy-MM-dd").alias("DOB"),
    F.col("Strip3.CUR_AGE").alias("CUR_AGE"),
    F.when(
        F.col("IdsSub.SUB_ID").isNull() | (F.length(F.col("IdsSub.SUB_ID")) == 0),
        F.lit(0)
    ).otherwise(F.col("IdsSub.SUB_SK")).alias("SUB_SK"),
    F.col("Strip3.LAST_NM").alias("LAST_NM"),
    F.col("Strip3.ADDR_LN_1").alias("ADDR_LN_1"),
    F.col("Strip3.ADDR_CITY_NM").alias("ADDR_CITY_NM"),
    F.col("Strip3.ADDR_ST_CD").alias("ADDR_ST_CD"),
    F.col("Strip3.ADDR_ZIP_CD").alias("ADDR_ZIP_CD"),
    F.col("Strip3.HOME_PHN_NO").alias("HOME_PHN_NO"),
    F.col("Strip3.PRI_COV_CD").alias("PRI_COV_CD"),
    F.col("Strip3.RISK_LVL").alias("RISK_LVL"),
    F.col("Strip3.NORM_RISK_SCORE").alias("NORM_RISK_SCORE"),
    F.col("Strip3.NORM_RISK_SCORE_YR_2").alias("NORM_RISK_SCORE_YR_2")
).alias("Strip")

df_hf_idea_mbrriskmesr_gndrcd_dedup = dedup_sort(
    df_hf_idea_mbrriskmesr_gndrcd,
    ["SUB_SK","BRTH_DT_SK","FIRST_NM","TRGT_CD"],
    []
).alias("query1")
df_Iteration1_join = df_Transformer_217.alias("Strip").join(
    df_hf_idea_mbrriskmesr_gndrcd_dedup.alias("query1"),
    (
        (F.col("Strip.SUB_SK") == F.col("query1.SUB_SK")) &
        (F.col("Strip.DOB") == F.col("query1.BRTH_DT_SK")) &
        (F.col("Strip.FIRST_NM") == F.col("query1.FIRST_NM")) &
        (F.col("Strip.GNDR_CD") == F.col("query1.TRGT_CD"))
    ),
    "left"
)

df_Iteration1_Load1 = df_Iteration1_join.filter(
    F.col("query1.SUB_SK").isNotNull() &
    F.col("query1.BRTH_DT_SK").isNotNull() &
    F.col("query1.FIRST_NM").isNotNull() &
    F.col("query1.TRGT_CD").isNotNull()
).select(
    F.col("query1.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("Strip.DSS_CD").alias("DSS_CD"),
    F.col("Strip.RISK_LVL").alias("RISK_LVL")
)

df_Iteration1_Out1 = df_Iteration1_join.filter(
    F.col("query1.SUB_SK").isNull() |
    F.col("query1.BRTH_DT_SK").isNull() |
    F.col("query1.FIRST_NM").isNull() |
    F.col("query1.TRGT_CD").isNull()
).select(
    F.col("Strip.MBR_ID").alias("MBR_ID"),
    F.col("Strip.CNTR_ID").alias("CNTR_ID"),
    F.col("Strip.DSS_CD").alias("DSS_CD"),
    F.col("Strip.FIRST_NM").alias("FIRST_NM"),
    F.col("Strip.GNDR_CD").alias("GNDR_CD"),
    F.rpad(F.col("Strip.DOB"),10," ").alias("DOB"),
    F.col("Strip.CUR_AGE").alias("CUR_AGE"),
    F.col("Strip.SUB_SK").alias("SUB_SK"),
    F.col("Strip.LAST_NM").alias("LAST_NM"),
    F.col("Strip.ADDR_LN_1").alias("ADDR_LN_1"),
    F.col("Strip.ADDR_CITY_NM").alias("ADDR_CITY_NM"),
    F.col("Strip.ADDR_ST_CD").alias("ADDR_ST_CD"),
    F.col("Strip.ADDR_ZIP_CD").alias("ADDR_ZIP_CD"),
    F.col("Strip.HOME_PHN_NO").alias("HOME_PHN_NO"),
    F.col("Strip.PRI_COV_CD").alias("PRI_COV_CD"),
    F.col("Strip.RISK_LVL").alias("RISK_LVL"),
    F.col("Strip.NORM_RISK_SCORE").alias("NORM_RISK_SCORE"),
    F.col("Strip.NORM_RISK_SCORE_YR_2").alias("NORM_RISK_SCORE_YR_2")
)

df_hf_idea_mbrriskmesr_brthdt_dedup = dedup_sort(
    df_hf_idea_mbrriskmesr_brthdt,
    ["SUB_SK","BRTH_DT_SK"],
    []
).alias("query2")
df_Iteration2_join = df_Iteration1_Out1.alias("Out1").join(
    df_hf_idea_mbrriskmesr_brthdt_dedup.alias("query2"),
    (
        (F.col("Out1.SUB_SK") == F.col("query2.SUB_SK")) &
        (F.col("Out1.DOB") == F.col("query2.BRTH_DT_SK"))
    ),
    "left"
)

df_Iteration2_Load2 = df_Iteration2_join.filter(
    F.col("query2.SUB_SK").isNotNull() &
    F.col("query2.BRTH_DT_SK").isNotNull()
).select(
    F.col("query2.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("Out1.DSS_CD").alias("DSS_CD"),
    F.col("Out1.RISK_LVL").alias("RISK_LVL")
)

df_Iteration2_Out2 = df_Iteration2_join.filter(
    F.col("query2.SUB_SK").isNull() |
    F.col("query2.BRTH_DT_SK").isNull()
).select(
    F.col("Out1.MBR_ID").alias("MBR_ID"),
    F.col("Out1.CNTR_ID").alias("CNTR_ID"),
    F.col("Out1.DSS_CD").alias("DSS_CD"),
    F.col("Out1.FIRST_NM").alias("FIRST_NM"),
    F.col("Out1.GNDR_CD").alias("GNDR_CD"),
    F.rpad(F.col("Out1.DOB"),10," ").alias("DOB"),
    F.col("Out1.CUR_AGE").alias("CUR_AGE"),
    F.col("Out1.SUB_SK").alias("SUB_SK"),
    F.col("Out1.LAST_NM").alias("LAST_NM"),
    F.col("Out1.ADDR_LN_1").alias("ADDR_LN_1"),
    F.col("Out1.ADDR_CITY_NM").alias("ADDR_CITY_NM"),
    F.col("Out1.ADDR_ST_CD").alias("ADDR_ST_CD"),
    F.col("Out1.ADDR_ZIP_CD").alias("ADDR_ZIP_CD"),
    F.col("Out1.HOME_PHN_NO").alias("HOME_PHN_NO"),
    F.col("Out1.PRI_COV_CD").alias("PRI_COV_CD"),
    F.col("Out1.RISK_LVL").alias("RISK_LVL"),
    F.col("Out1.NORM_RISK_SCORE").alias("NORM_RISK_SCORE"),
    F.col("Out1.NORM_RISK_SCORE_YR_2").alias("NORM_RISK_SCORE_YR_2")
)

df_hf_idea_mbrriskmesr_frstnm_dedup = dedup_sort(
    df_hf_idea_mbrriskmesr_frstnm,
    ["SUB_SK","FIRST_NM"],
    []
).alias("query3")
df_Iteration4_join = df_Iteration2_Out2.alias("Out2").join(
    df_hf_idea_mbrriskmesr_frstnm_dedup.alias("query3"),
    (
        (F.col("Out2.SUB_SK") == F.col("query3.SUB_SK")) &
        (F.col("Out2.FIRST_NM") == F.col("query3.FIRST_NM"))
    ),
    "left"
)

df_Iteration4_Output = df_Iteration4_join.filter(
    F.col("query3.SUB_SK").isNotNull() &
    F.col("query3.FIRST_NM").isNotNull()
).select(
    F.col("query3.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("Out2.DSS_CD").alias("DSS_CD"),
    F.col("Out2.RISK_LVL").alias("RISK_LVL")
)

df_Iteration4_Nomatch = df_Iteration4_join.filter(
    F.col("query3.SUB_SK").isNull() |
    F.col("query3.FIRST_NM").isNull()
).select(
    F.col("Out2.MBR_ID").alias("MBR_ID"),
    F.col("Out2.CNTR_ID").alias("CNTR_ID"),
    F.col("Out2.DSS_CD").alias("DSS_CD"),
    F.col("Out2.FIRST_NM").alias("FIRST_NM"),
    F.col("Out2.GNDR_CD").alias("GNDR_CD"),
    F.rpad(F.col("Out2.DOB"),10," ").alias("DOB"),
    F.col("Out2.CUR_AGE").alias("CUR_AGE"),
    F.col("Out2.SUB_SK").alias("SUB_SK"),
    F.col("Out2.LAST_NM").alias("LAST_NM"),
    F.col("Out2.ADDR_LN_1").alias("ADDR_LN_1"),
    F.col("Out2.ADDR_CITY_NM").alias("ADDR_CITY_NM"),
    F.col("Out2.ADDR_ST_CD").alias("ADDR_ST_CD"),
    F.col("Out2.ADDR_ZIP_CD").alias("ADDR_ZIP_CD"),
    F.col("Out2.HOME_PHN_NO").alias("HOME_PHN_NO"),
    F.col("Out2.PRI_COV_CD").alias("PRI_COV_CD"),
    F.col("Out2.RISK_LVL").alias("RISK_LVL"),
    F.col("Out2.NORM_RISK_SCORE").alias("NORM_RISK_SCORE"),
    F.col("Out2.NORM_RISK_SCORE_YR_2").alias("NORM_RISK_SCORE_YR_2")
)

write_files(
    df_Iteration4_Nomatch,
    f"{adls_path_publish}/external/IdeaMbrRiskMesr_NoMatch.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote="\"",
    nullValue=None
)

df_hf_idea_mbrriskmesr_match1_in = df_Iteration1_Load1.select(
    F.col("MBR_UNIQ_KEY"),
    F.col("DSS_CD"),
    F.col("RISK_LVL")
)
df_hf_idea_mbrriskmesr_match1_dedup = dedup_sort(
    df_hf_idea_mbrriskmesr_match1_in,
    ["MBR_UNIQ_KEY","DSS_CD","RISK_LVL"],
    []
)
df_hf_idea_mbrriskmesr_match1 = df_hf_idea_mbrriskmesr_match1_dedup.select(
    F.col("MBR_UNIQ_KEY"),
    F.col("DSS_CD"),
    F.col("RISK_LVL")
)

df_hf_idea_mbrriskmesr_match2_in = df_Iteration2_Load2.select(
    F.col("MBR_UNIQ_KEY"),
    F.col("DSS_CD"),
    F.col("RISK_LVL")
)
df_hf_idea_mbrriskmesr_match2_dedup = dedup_sort(
    df_hf_idea_mbrriskmesr_match2_in,
    ["MBR_UNIQ_KEY","DSS_CD","RISK_LVL"],
    []
)
df_hf_idea_mbrriskmesr_match2 = df_hf_idea_mbrriskmesr_match2_dedup.select(
    F.col("MBR_UNIQ_KEY"),
    F.col("DSS_CD"),
    F.col("RISK_LVL")
)

df_hf_idea_mbrriskmesr_match3_in = df_Iteration4_Output.select(
    F.col("MBR_UNIQ_KEY"),
    F.col("DSS_CD"),
    F.col("RISK_LVL")
)
df_hf_idea_mbrriskmesr_match3_dedup = dedup_sort(
    df_hf_idea_mbrriskmesr_match3_in,
    ["MBR_UNIQ_KEY","DSS_CD","RISK_LVL"],
    []
)
df_hf_idea_mbrriskmesr_match3 = df_hf_idea_mbrriskmesr_match3_dedup.select(
    F.col("MBR_UNIQ_KEY"),
    F.col("DSS_CD"),
    F.col("RISK_LVL")
)

df_Collector_All = df_hf_idea_mbrriskmesr_match1.unionByName(
    df_hf_idea_mbrriskmesr_match2
).unionByName(
    df_hf_idea_mbrriskmesr_match3
)

df_Transformer_296 = df_Collector_All.select(
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.when(
        (F.upper(F.col("DSS_CD")) == "CONGESTIVE HEART FAILURE") | (F.upper(F.col("DSS_CD")) == "CHF"),
        F.lit("500")
    ).when(
        (F.upper(F.col("DSS_CD")) == "DIABETES") | (F.upper(F.col("DSS_CD")) == "DIAB"),
        F.lit("501")
    ).when(
        F.upper(F.col("DSS_CD")) == "ASTHMA",
        F.lit("502")
    ).when(
        (F.upper(F.col("DSS_CD")) == "CHRONIC OBSTRUCTIVE PULMONARY DISEASE") | (F.upper(F.col("DSS_CD")) == "COPD"),
        F.lit("503")
    ).when(
        (F.upper(F.col("DSS_CD")) == "CORONARY HEART DISEASE") | (F.upper(F.col("DSS_CD")) == "CAD"),
        F.lit("504")
    ).when(
        F.upper(F.col("DSS_CD")) == "DEPRESSION",
        F.lit("505")
    ).otherwise(F.lit("UNK")).alias("RISK_CAT_ID"),
    F.trim(F.col("RISK_LVL")).substr(F.lit(1),F.lit(1)).alias("RISK_LVL")
).alias("Data")

df_Transform_idea = df_hf_idea_mbrriskmesr_idntdt_lkup.select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("RISK_CAT_ID").alias("RISK_CAT_ID"),
    F.col("RISK_IDNT_DT_SK").alias("RISK_IDNT_DT_SK")
).alias("idea")

df_Transform_mcsource = df_hf_idea_mbrriskmesr_idntdt_lkup.select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("RISK_CAT_ID").alias("RISK_CAT_ID"),
    F.col("RISK_IDNT_DT_SK").alias("RISK_IDNT_DT_SK")
).alias("mcsource")

df_Transform_svrtycd = df_hf_idea_mbrriskmesr_svrtycd_lkup.select(
    F.col("SRC_DOMAIN_TX").alias("SRC_DOMAIN_TX"),
    F.col("TRGT_DOMAIN_TX").alias("TRGT_DOMAIN_TX")
).alias("SvrtyCd_lkup")

df_Transform_join1 = df_Transformer_296.alias("Data").join(
    df_Transform_idea,
    (
        (F.lit("IDEA") == F.col("idea.SRC_SYS_CD")) &
        (F.col("Data.MBR_UNIQ_KEY") == F.col("idea.MBR_UNIQ_KEY")) &
        (F.col("Data.RISK_CAT_ID") == F.col("idea.RISK_CAT_ID"))
    ),
    "left"
).join(
    df_Transform_svrtycd,
    (
        F.col("Data.RISK_LVL") == F.col("SvrtyCd_lkup.SRC_DOMAIN_TX")
    ),
    "left"
).join(
    df_Transform_mcsource,
    (
        (F.lit("MCSOURCE") == F.col("mcsource.SRC_SYS_CD")) &
        (F.col("Data.MBR_UNIQ_KEY") == F.col("mcsource.MBR_UNIQ_KEY")) &
        (F.col("Data.RISK_CAT_ID") == F.col("mcsource.RISK_CAT_ID"))
    ),
    "left"
)

df_Transform = df_Transform_join1.select(
    F.col("Data.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("Data.RISK_CAT_ID").alias("RISK_CAT_ID"),
    F.col("Data.MBR_UNIQ_KEY").alias("MBR_CK"),
    F.col("Data.RISK_CAT_ID").alias("RISK_CAT_SK"),
    F.rpad(F.lit(YearMonth),6," ").alias("PRCS_YR_MO_SK"),
    F.lit("NA").alias("MBR_MED_MESRS_CD"),
    F.lit(0).alias("FTR_RELTV_RISK_NO"),
    F.when(
        F.trim(F.col("SvrtyCd_lkup.TRGT_DOMAIN_TX")).isNull(),
        F.lit("UNK")
    ).otherwise(
        F.trim(F.col("SvrtyCd_lkup.TRGT_DOMAIN_TX"))
    ).alias("RISK_SVRTY_CD"),
    F.when(
        F.col("idea.RISK_IDNT_DT_SK").isNull(),
        F.when(
            F.col("mcsource.RISK_IDNT_DT_SK").isNull(),
            F.lit(RunDate)
        ).otherwise(
            F.when(F.col("mcsource.RISK_IDNT_DT_SK") == F.lit("1753-01-01"), F.lit(RunDate))
            .otherwise(F.col("mcsource.RISK_IDNT_DT_SK"))
        )
    ).otherwise(
        F.when(F.col("idea.RISK_IDNT_DT_SK") == F.lit("1753-01-01"), F.lit(RunDate))
        .otherwise(F.col("idea.RISK_IDNT_DT_SK"))
    ).alias("RISK_IDNT_DT"),
    F.col("Data.RISK_LVL").alias("MCSRC_RISK_LVL_NO"),
    F.lit(0).alias("MDCSN_RISK_SCORE_NO"),
    F.lit(0).alias("MDCSN_HLTH_STTUS_MESR_NO")
).alias("Strip")

df_BusinessRules = df_Transform.select(
    F.lit(SourceSk).alias("SourceSk").alias("SourceSk"),
    F.col("Strip.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("Strip.RISK_CAT_ID").alias("RISK_CAT_ID"),
    F.col("Strip.PRCS_YR_MO_SK").alias("PRCS_YR_MO_SK"),
    F.lit("Y").alias("RowPassThru"),
    F.lit(SrcSysCd).alias("svSrcSysCd"),
    F.col("Strip.MBR_CK").alias("MBR_CK"),
    F.col("Strip.MBR_MED_MESRS_CD").alias("MBR_MED_MESRS_CD"),
    F.col("Strip.RISK_CAT_SK").alias("RISK_CAT_SK"),
    F.col("Strip.FTR_RELTV_RISK_NO").alias("FTR_RELTV_RISK_NO"),
    F.col("Strip.RISK_SVRTY_CD").alias("RISK_SVRTY_CD"),
    F.col("Strip.RISK_IDNT_DT").alias("RISK_IDNT_DT"),
    F.col("Strip.MCSRC_RISK_LVL_NO").alias("MCSRC_RISK_LVL_NO"),
    F.col("Strip.MDCSN_RISK_SCORE_NO").alias("MDCSN_RISK_SCORE_NO"),
    F.col("Strip.MDCSN_HLTH_STTUS_MESR_NO").alias("MDCSN_HLTH_STTUS_MESR_NO")
)

df_BusinessRules_AllCol = df_BusinessRules.select(
    F.lit(SourceSk).alias("SRC_SYS_CD_SK"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("RISK_CAT_ID").alias("RISK_CAT_ID"),
    F.col("PRCS_YR_MO_SK").alias("PRCS_YR_MO_SK"),
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.lit("I"),10," ").alias("INSRT_UPDT_CD"),
    F.rpad(F.lit("N"),1," ").alias("DISCARD_IN"),
    F.rpad(F.col("RowPassThru"),1," ").alias("PASS_THRU_IN"),
    F.lit(RunDate).alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.col("svSrcSysCd").alias("SRC_SYS_CD"),
    F.concat(F.col("svSrcSysCd"),F.lit(";"),F.col("MBR_UNIQ_KEY"),F.lit(";"),F.col("RISK_CAT_ID"),F.lit(";"),F.col("PRCS_YR_MO_SK")).alias("PRI_KEY_STRING"),
    F.lit(0).alias("MBR_RISK_MESR_SK"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("MBR_CK").alias("MBR_CK"),
    F.col("MBR_MED_MESRS_CD").alias("MBR_MED_MESRS_CD"),
    F.col("RISK_CAT_SK").alias("RISK_CAT_SK"),
    F.col("FTR_RELTV_RISK_NO").alias("FTR_RELTV_RISK_NO"),
    F.col("RISK_SVRTY_CD").alias("RISK_SVRTY_CD"),
    F.rpad(F.col("RISK_IDNT_DT"),10," ").alias("RISK_IDNT_DT"),
    F.col("MCSRC_RISK_LVL_NO").alias("MCSRC_RISK_LVL_NO"),
    F.col("MDCSN_RISK_SCORE_NO").alias("MDCSN_RISK_SCORE_NO"),
    F.col("MDCSN_HLTH_STTUS_MESR_NO").alias("MDCSN_HLTH_STTUS_MESR_NO"),
    F.lit("NA").alias("CRG_ID"),
    F.lit("NA").alias("CRG_DESC"),
    F.lit("NA").alias("AGG_CRG_BASE_3_ID"),
    F.lit("NA").alias("AGG_CRG_BASE_3_DESC"),
    F.lit(0).alias("CRG_WT"),
    F.lit("NA").alias("CRG_MDL_ID"),
    F.lit("NA").alias("CRG_VRSN_ID"),
    F.lit(1).alias("INDV_BE_MARA_RSLT_SK"),
    F.lit("NA").alias("RISK_MTHDLGY_CD"),
    F.lit("NA").alias("RISK_MTHDLGY_TYP_CD"),
    F.lit(None).alias("RISK_MTHDLGY_TYP_SCORE_NO")
)

df_BusinessRules_Transform = df_BusinessRules.select(
    F.lit(SourceSk).alias("SRC_SYS_CD_SK"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("RISK_CAT_ID").alias("RISK_CAT_ID"),
    F.col("PRCS_YR_MO_SK").alias("PRCS_YR_MO_SK")
)

df_BusinessRules_Snapshot = df_BusinessRules.select(
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("RISK_CAT_ID").alias("RISK_CAT_ID"),
    F.col("PRCS_YR_MO_SK").alias("PRCS_YR_MO_SK")
)

# MAGIC %run ../../../../shared_containers/PrimaryKey/MbrRiskMesrPK
# COMMAND ----------

params_MbrRiskMesrPK = {
    "CurrRunCycle": CurrRunCycle,
    "SrcSysCd": SrcSysCd,
    "RunID": RunID,
    "CurrentDate": RunDate,
    "$IDSOwner": IDSOwner
}
df_MbrRiskMesrPK_AllCol, df_MbrRiskMesrPK_Transform = MbrRiskMesrPK(df_BusinessRules_AllCol, df_BusinessRules_Transform, params_MbrRiskMesrPK)

df_IdsMbrRiskMesrExtr = df_MbrRiskMesrPK_AllCol.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("INSRT_UPDT_CD"),10," ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("DISCARD_IN"),1," ").alias("DISCARD_IN"),
    F.rpad(F.col("PASS_THRU_IN"),1," ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("MBR_RISK_MESR_SK").alias("MBR_RISK_MESR_SK"),
    F.col("MBR_UNIQ_KEY"),
    F.col("RISK_CAT_ID"),
    F.rpad(F.col("PRCS_YR_MO_SK"),6," ").alias("PRCS_YR_MO_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("MBR_CK"),
    F.col("MBR_MED_MESRS_CD"),
    F.col("RISK_CAT_SK"),
    F.col("FTR_RELTV_RISK_NO"),
    F.col("RISK_SVRTY_CD"),
    F.rpad(F.col("RISK_IDNT_DT"),10," ").alias("RISK_IDNT_DT"),
    F.col("MCSRC_RISK_LVL_NO"),
    F.col("MDCSN_RISK_SCORE_NO"),
    F.col("MDCSN_HLTH_STTUS_MESR_NO"),
    F.col("CRG_ID"),
    F.col("CRG_DESC"),
    F.col("AGG_CRG_BASE_3_ID"),
    F.col("AGG_CRG_BASE_3_DESC"),
    F.col("CRG_WT"),
    F.col("CRG_MDL_ID"),
    F.col("CRG_VRSN_ID"),
    F.col("INDV_BE_MARA_RSLT_SK"),
    F.col("RISK_MTHDLGY_CD"),
    F.col("RISK_MTHDLGY_TYP_CD"),
    F.col("RISK_MTHDLGY_TYP_SCORE_NO")
)

write_files(
    df_IdsMbrRiskMesrExtr,
    f"{adls_path}/key/IdeaMbrRiskMesrExtr.MbrRiskMesr.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

df_Logic = df_BusinessRules_Snapshot.select(
    F.lit(SourceSk).alias("SRC_SYS_CD_SK"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("RISK_CAT_ID").alias("RISK_CAT_ID"),
    F.rpad(F.col("PRCS_YR_MO_SK"),6," ").alias("PRCS_YR_MO_SK")
)

write_files(
    df_Logic,
    f"{adls_path}/load/B_MBR_RISK_MESR.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)