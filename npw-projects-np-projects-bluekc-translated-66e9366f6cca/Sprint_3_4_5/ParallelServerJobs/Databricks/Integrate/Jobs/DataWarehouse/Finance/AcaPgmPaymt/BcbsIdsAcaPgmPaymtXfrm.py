# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2015 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC JOB NAME:  BcbsIdsAcaPgmPaymtXfrm
# MAGIC CALLED BY:  BcbsIdsAcaPgmPaymtExtrSeq
# MAGIC 
# MAGIC Processing:   Transform job for ACA_PGM_PAYMT table.
# MAGIC  
# MAGIC Modifications:                          
# MAGIC                                                                     Project/                                                                                                    Development                   Code                          Date
# MAGIC Developer                             Date              Altiris #                          Change Description                                              Environment                   Reviewer                  Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                    ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Kalyan Neelam                 2015-10-22         5128                              Initial Programming                                              IntegrateDev1             Bhoomi Dasari              10/23/2015
# MAGIC Prabhu ES                       2022-03-11          S2S                               MSSQL ODBC conn params added                    IntegrateDev5\(9)\(9)Ken Bradmon\(9)2022-06-06

# MAGIC Strip field function and Tranforamtion rules are applied on the data Extracted from Facets
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


CurrDate = get_widget_value('CurrDate','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
SrcSysCd = get_widget_value('SrcSysCd','')
RunID = get_widget_value('RunID','')
RunIDTimeStamp = get_widget_value('RunIDTimeStamp','')
IDSOwner = get_widget_value('IDSOwner','')
BCBSFINOwner = get_widget_value('BCBSFINOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
bcbsfin_secret_name = get_widget_value('bcbsfin_secret_name','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query_db2_QHP_in = f"""
SELECT
QHP.QHP_ID,
QHP.EFF_DT_SK,
QHP.TERM_DT_SK,
Q.COUNT_QHP
FROM {IDSOwner}.QHP QHP,
(SELECT QHP_ID, COUNT(QHP_SK) AS COUNT_QHP
 FROM {IDSOwner}.QHP
 GROUP BY QHP_ID) Q
WHERE
QHP.QHP_ID = Q.QHP_ID
"""
df_db2_QHP_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_QHP_in)
    .load()
    .select(
        F.col("QHP_ID"),
        F.col("EFF_DT_SK"),
        F.col("TERM_DT_SK"),
        F.col("COUNT_QHP"),
    )
)

extract_query_db2_QHP_ENR_in = f"""
SELECT
QHP_ENR.QHP_ID,
QHP_ENR.QHP_ENR_EFF_DT_SK,
QHP_ENR.QHP_ENR_TERM_DT_SK,
QHP.EFF_DT_SK
FROM {IDSOwner}.QHP QHP,
{IDSOwner}.QHP_ENR QHP_ENR
WHERE
QHP.QHP_SK = QHP_ENR.QHP_SK
AND QHP_ENR.GRP_ID = '10001000'
AND QHP_ENR.CLS_ID = 'A002'
ORDER BY
QHP_ENR.QHP_ID,
QHP_ENR.QHP_ENR_TERM_DT_SK
"""
df_db2_QHP_ENR_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_QHP_ENR_in)
    .load()
    .select(
        F.col("QHP_ID"),
        F.col("QHP_ENR_EFF_DT_SK"),
        F.col("QHP_ENR_TERM_DT_SK"),
        F.col("EFF_DT_SK")
    )
)

df_remove_duplicates = dedup_sort(
    df_db2_QHP_ENR_in,
    ["QHP_ID"],
    [("QHP_ENR_TERM_DT_SK", "D")]
).select(
    F.col("QHP_ID"),
    F.col("QHP_ENR_EFF_DT_SK"),
    F.col("QHP_ENR_TERM_DT_SK"),
    F.col("EFF_DT_SK")
)

df_ds_ACA_PGM_PAYMT_Extr = spark.read.parquet(
    f"{adls_path}/ds/ACA_PGM_PAYMT.{SrcSysCd}.extr.{RunID}.parquet"
)

df_strip = df_ds_ACA_PGM_PAYMT_Extr.select(
    F.col("CMS_ACA_PGM_PAYMT_CK").alias("CMS_ACA_PGM_PAYMT_CK"),
    trim(F.col("ACTVTY_YRMO")).alias("ACTVTY_YRMO"),
    trim(F.col("PAYMT_COV_YRMO")).alias("PAYMT_COV_YRMO"),
    trim(F.col("PAYMT_TYP_CD")).alias("PAYMT_TYP_CD"),
    trim(F.col("STATE_CODE")).alias("STATE_CODE"),
    trim(F.col("QHP_ID")).alias("QHP_ID"),
    F.col("CMS_ACA_PGM_PAYMT_SEQ_NO").alias("CMS_ACA_PGM_PAYMT_SEQ_NO"),
    F.substring(F.col("COV_START_DT"), 1, 10).alias("COV_START_DT"),
    F.substring(F.col("COV_END_DT"), 1, 10).alias("COV_END_DT"),
    F.col("TRANS_AMT").alias("TRANS_AMT"),
    F.col("ACA_PGM_PAYMT_UNIQ_KEY").alias("ACA_PGM_PAYMT_UNIQ_KEY"),
    trim(F.col("EXCH_RPT_DOC_CTL_NO")).alias("EXCH_RPT_DOC_CTL_NO"),
    trim(F.col("EXCH_RPT_NM")).alias("EXCH_RPT_NM"),
    trim(F.col("EFT_TRACE_ID")).alias("EFT_TRACE_ID"),
    F.lit("1753-01-01").alias("Start_DT"),
    F.lit("2199-12-31").alias("End_DT")
)

jdbc_url_bcbs, jdbc_props_bcbs = get_db_config(bcbsfin_secret_name)
extract_query_CMS_ACA_PGM_FILE_in = f"""
SELECT
CMS_PAYMT_FILE.EFT_TRACE_ID,
CMS_PAYMT_FILE.EFT_EFF_DT
FROM {BCBSFINOwner}.CMS_PAYMT_FILE CMS_PAYMT_FILE
"""
df_CMS_ACA_PGM_FILE_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_bcbs)
    .options(**jdbc_props_bcbs)
    .option("query", extract_query_CMS_ACA_PGM_FILE_in)
    .load()
    .select(
        F.col("EFT_TRACE_ID"),
        F.col("EFT_EFF_DT")
    )
)

df_Transformer_249 = df_CMS_ACA_PGM_FILE_in.select(
    trim(F.col("EFT_TRACE_ID")).alias("EFT_TRACE_ID"),
    F.substring(trim(F.col("EFT_EFF_DT")), 1, 10).alias("EFT_EFF_DT")
)

df_lookup_eftEffDt = (
    df_strip.alias("TrimmedData")
    .join(
        df_Transformer_249.alias("DSLink246"),
        F.col("TrimmedData.EFT_TRACE_ID") == F.col("DSLink246.EFT_TRACE_ID"),
        how="left"
    )
    .select(
        F.col("TrimmedData.CMS_ACA_PGM_PAYMT_CK").alias("CMS_ACA_PGM_PAYMT_CK"),
        F.col("TrimmedData.ACTVTY_YRMO").alias("ACTVTY_YRMO"),
        F.col("TrimmedData.PAYMT_COV_YRMO").alias("PAYMT_COV_YRMO"),
        F.col("TrimmedData.PAYMT_TYP_CD").alias("PAYMT_TYP_CD"),
        F.col("TrimmedData.STATE_CODE").alias("STATE_CODE"),
        F.col("TrimmedData.QHP_ID").alias("QHP_ID"),
        F.col("TrimmedData.CMS_ACA_PGM_PAYMT_SEQ_NO").alias("CMS_ACA_PGM_PAYMT_SEQ_NO"),
        F.col("TrimmedData.COV_START_DT").alias("COV_START_DT"),
        F.col("TrimmedData.COV_END_DT").alias("COV_END_DT"),
        F.col("TrimmedData.TRANS_AMT").alias("TRANS_AMT"),
        F.col("TrimmedData.ACA_PGM_PAYMT_UNIQ_KEY").alias("ACA_PGM_PAYMT_UNIQ_KEY"),
        F.col("TrimmedData.EXCH_RPT_DOC_CTL_NO").alias("EXCH_RPT_DOC_CTL_NO"),
        F.col("TrimmedData.EXCH_RPT_NM").alias("EXCH_RPT_NM"),
        F.col("TrimmedData.EFT_TRACE_ID").alias("EFT_TRACE_ID"),
        F.col("DSLink246.EFT_EFF_DT").alias("EFT_EFF_DT"),
        F.col("TrimmedData.Start_DT").alias("Start_DT"),
        F.col("TrimmedData.End_DT").alias("End_DT")
    )
)

df_lookup_qhpEnr = df_lookup_eftEffDt.alias("Data1").join(
    df_remove_duplicates.alias("DSLink241"),
    (
        (F.col("Data1.QHP_ID") == F.col("DSLink241.QHP_ID"))
        & (F.col("DSLink241.QHP_ENR_EFF_DT_SK") >= F.col("Data1.COV_END_DT"))
        & (F.col("DSLink241.QHP_ENR_EFF_DT_SK") <= F.col("Data1.Start_DT"))
        & (F.col("DSLink241.QHP_ENR_TERM_DT_SK") <= F.col("Data1.COV_START_DT"))
        & (F.col("DSLink241.QHP_ENR_TERM_DT_SK") >= F.col("Data1.End_DT"))
    ),
    how="left"
).select(
    F.col("Data1.CMS_ACA_PGM_PAYMT_CK").alias("CMS_ACA_PGM_PAYMT_CK"),
    F.col("Data1.ACTVTY_YRMO").alias("ACTVTY_YRMO"),
    F.col("Data1.PAYMT_COV_YRMO").alias("PAYMT_COV_YRMO"),
    F.col("Data1.PAYMT_TYP_CD").alias("PAYMT_TYP_CD"),
    F.col("Data1.STATE_CODE").alias("STATE_CODE"),
    F.col("Data1.QHP_ID").alias("QHP_ID"),
    F.col("Data1.CMS_ACA_PGM_PAYMT_SEQ_NO").alias("CMS_ACA_PGM_PAYMT_SEQ_NO"),
    F.col("Data1.COV_START_DT").alias("COV_START_DT"),
    F.col("Data1.COV_END_DT").alias("COV_END_DT"),
    F.col("Data1.TRANS_AMT").alias("TRANS_AMT"),
    F.col("Data1.ACA_PGM_PAYMT_UNIQ_KEY").alias("ACA_PGM_PAYMT_UNIQ_KEY"),
    F.col("Data1.EXCH_RPT_DOC_CTL_NO").alias("EXCH_RPT_DOC_CTL_NO"),
    F.col("Data1.EXCH_RPT_NM").alias("EXCH_RPT_NM"),
    F.col("Data1.EFT_TRACE_ID").alias("EFT_TRACE_ID"),
    F.col("Data1.EFT_EFF_DT").alias("EFT_EFF_DT"),
    F.col("Data1.Start_DT").alias("Start_DT"),
    F.col("Data1.End_DT").alias("End_DT"),
    F.col("DSLink241.EFF_DT_SK").alias("EFF_DT_SK")
)

df_lookup_qhp = df_lookup_qhpEnr.alias("Data2").join(
    df_db2_QHP_in.alias("lnk_QHP_out"),
    (
        (F.col("Data2.QHP_ID") == F.col("lnk_QHP_out.QHP_ID"))
        & (F.col("lnk_QHP_out.EFF_DT_SK") >= F.col("Data2.Start_DT"))
        & (F.col("lnk_QHP_out.EFF_DT_SK") <= F.col("Data2.COV_END_DT"))
        & (F.col("lnk_QHP_out.TERM_DT_SK") >= F.col("Data2.COV_START_DT"))
        & (F.col("lnk_QHP_out.TERM_DT_SK") <= F.col("Data2.End_DT"))
    ),
    how="left"
).select(
    F.col("Data2.CMS_ACA_PGM_PAYMT_CK").alias("CMS_ACA_PGM_PAYMT_CK"),
    F.col("Data2.ACTVTY_YRMO").alias("ACTVTY_YR_MO"),
    F.col("Data2.PAYMT_COV_YRMO").alias("PAYMT_COV_YR_MO"),
    F.col("Data2.PAYMT_TYP_CD").alias("PAYMT_TYP_CD"),
    F.col("Data2.STATE_CODE").alias("STATE_CODE"),
    F.col("Data2.QHP_ID").alias("QHP_ID"),
    F.col("Data2.CMS_ACA_PGM_PAYMT_SEQ_NO").alias("CMS_ACA_PGM_PAYMT_SEQ_NO"),
    F.col("Data2.COV_START_DT").alias("COV_START_DT"),
    F.col("Data2.COV_END_DT").alias("COV_END_DT"),
    F.col("Data2.TRANS_AMT").alias("TRANS_AMT"),
    F.col("Data2.ACA_PGM_PAYMT_UNIQ_KEY").alias("ACA_PGM_PAYMT_UNIQ_KEY"),
    F.col("Data2.EXCH_RPT_DOC_CTL_NO").alias("EXCH_RPT_DOC_CTL_NO"),
    F.col("Data2.EXCH_RPT_NM").alias("EXCH_RPT_NM"),
    F.col("Data2.EFT_TRACE_ID").alias("EFT_TRACE_ID"),
    F.col("Data2.EFT_EFF_DT").alias("EFT_EFF_DT"),
    F.col("Data2.Start_DT").alias("Start_DT"),
    F.col("Data2.End_DT").alias("End_DT"),
    F.col("Data2.EFF_DT_SK").alias("EFF_DT_SK"),
    F.col("lnk_QHP_out.EFF_DT_SK").alias("EFF_DT_SK_1"),
    F.col("lnk_QHP_out.COUNT_QHP").alias("COUNT_QHP")
)

df_Transformer_220_stage = df_lookup_qhp.select(
    "*",
    F.when(F.col("QHP_ID").isNull(), 'NA').otherwise(F.col("QHP_ID")).alias("svQhpId")
)

df_Transformer_220_PkeyOut = df_Transformer_220_stage.select(
    (
        F.col("ACTVTY_YR_MO")
        + F.lit(";")
        + F.col("PAYMT_COV_YR_MO")
        + F.lit(";")
        + F.col("PAYMT_TYP_CD")
        + F.lit(";")
        + F.col("STATE_CODE")
        + F.lit(";")
        + F.col("svQhpId")
        + F.lit(";")
        + F.col("CMS_ACA_PGM_PAYMT_SEQ_NO")
        + F.lit(";")
        + F.col("SrcSysCd")
    ).alias("PRI_NAT_KEY_STRING"),
    F.col("RunIDTimeStamp").alias("FIRST_RECYC_TS"),
    F.lit(0).alias("ACA_PGM_PAYMT_SK"),
    F.col("ACTVTY_YR_MO").alias("ACTVTY_YR_MO"),
    F.col("PAYMT_COV_YR_MO").alias("PAYMT_COV_YR_MO"),
    F.col("PAYMT_TYP_CD").alias("PAYMT_TYP_CD"),
    F.col("STATE_CODE").alias("ST_CD"),
    F.col("svQhpId").alias("QHP_ID"),
    F.col("CMS_ACA_PGM_PAYMT_SEQ_NO").alias("ACA_PGM_PAYMT_SEQ_NO"),
    F.col("SrcSysCd").alias("SRC_SYS_CD"),
    F.col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
    F.col("COV_START_DT").alias("COV_STRT_DT_SK"),
    F.col("COV_END_DT").alias("COV_END_DT_SK"),
    F.col("EFT_EFF_DT").alias("EFT_EFF_DT_SK"),
    F.col("TRANS_AMT").alias("ACA_PGM_TRANS_AMT"),
    F.col("ACA_PGM_PAYMT_UNIQ_KEY").alias("ACA_PGM_PAYMT_UNIQ_KEY"),
    F.col("EFT_TRACE_ID").alias("EFT_TRACE_ID"),
    F.when(
        F.col("EXCH_RPT_DOC_CTL_NO").isNull() | (F.col("EXCH_RPT_DOC_CTL_NO") == "NULL"),
        '0'
    ).otherwise(F.col("EXCH_RPT_DOC_CTL_NO")).alias("EXCH_RPT_DOC_CTL_ID"),
    F.when(
        F.col("EXCH_RPT_NM").isNull() | (F.col("EXCH_RPT_NM") == "NULL"),
        'UNK'
    ).otherwise(F.col("EXCH_RPT_NM")).alias("EXCH_RPT_NM"),
    F.when(
        F.col("COUNT_QHP") > F.lit(1),
        F.when(
            F.length(trim(F.col("EFF_DT_SK"))) == 0,
            F.when(
                F.length(trim(F.col("EFF_DT_SK_1"))) == 0,
                F.lit("1753-01-01")
            ).otherwise(F.col("EFF_DT_SK_1"))
        ).otherwise(F.col("EFF_DT_SK"))
    ).otherwise(
        F.when(
            F.length(trim(F.col("EFF_DT_SK_1"))) == 0,
            F.when(
                F.length(trim(F.col("EFF_DT_SK"))) == 0,
                F.lit("1753-01-01")
            ).otherwise(F.col("EFF_DT_SK"))
        ).otherwise(F.col("EFF_DT_SK_1"))
    ).alias("EFF_DT_SK")
)

df_Transformer_220_Snapshot = df_Transformer_220_stage.select(
    F.col("ACTVTY_YR_MO").alias("ACTVTY_YR_MO"),
    F.col("PAYMT_COV_YR_MO").alias("PAYMT_COV_YR_MO"),
    F.col("PAYMT_TYP_CD").alias("PAYMT_TYP_CD"),
    F.col("STATE_CODE").alias("ST_CD"),
    F.col("svQhpId").alias("QHP_ID"),
    F.col("CMS_ACA_PGM_PAYMT_SEQ_NO").alias("ACA_PGM_PAYMT_SEQ_NO"),
    F.col("SrcSysCdSk").alias("SRC_SYS_CD_SK")
)

df_Transformer_220_PkeyOut_final = df_Transformer_220_PkeyOut.select(
    "PRI_NAT_KEY_STRING",
    "FIRST_RECYC_TS",
    "ACA_PGM_PAYMT_SK",
    "ACTVTY_YR_MO",
    "PAYMT_COV_YR_MO",
    "PAYMT_TYP_CD",
    "ST_CD",
    "QHP_ID",
    "ACA_PGM_PAYMT_SEQ_NO",
    "SRC_SYS_CD",
    "SRC_SYS_CD_SK",
    "COV_STRT_DT_SK",
    "COV_END_DT_SK",
    "EFT_EFF_DT_SK",
    "ACA_PGM_TRANS_AMT",
    "ACA_PGM_PAYMT_UNIQ_KEY",
    "EFT_TRACE_ID",
    "EXCH_RPT_DOC_CTL_ID",
    "EXCH_RPT_NM",
    "EFF_DT_SK"
)

df_Transformer_220_PkeyOut_final = df_Transformer_220_PkeyOut_final.withColumn(
    "ACTVTY_YR_MO",
    F.rpad(F.col("ACTVTY_YR_MO"), 6, " ")
).withColumn(
    "PAYMT_COV_YR_MO",
    F.rpad(F.col("PAYMT_COV_YR_MO"), 6, " ")
).withColumn(
    "COV_STRT_DT_SK",
    F.rpad(F.col("COV_STRT_DT_SK"), 10, " ")
).withColumn(
    "COV_END_DT_SK",
    F.rpad(F.col("COV_END_DT_SK"), 10, " ")
).withColumn(
    "EFT_EFF_DT_SK",
    F.rpad(F.col("EFT_EFF_DT_SK"), 10, " ")
).withColumn(
    "EFF_DT_SK",
    F.rpad(F.col("EFF_DT_SK"), 10, " ")
)

write_files(
    df_Transformer_220_PkeyOut_final,
    f"{adls_path}/ds/ACA_PGM_PAYMT.{SrcSysCd}.xfrm.{RunID}.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

df_B_ACA_PGM_PAYMT_final = df_Transformer_220_Snapshot.select(
    "ACTVTY_YR_MO",
    "PAYMT_COV_YR_MO",
    "PAYMT_TYP_CD",
    "ST_CD",
    "QHP_ID",
    "ACA_PGM_PAYMT_SEQ_NO",
    "SRC_SYS_CD_SK"
)

df_B_ACA_PGM_PAYMT_final = df_B_ACA_PGM_PAYMT_final.withColumn(
    "ACTVTY_YR_MO",
    F.rpad(F.col("ACTVTY_YR_MO"), 6, " ")
).withColumn(
    "PAYMT_COV_YR_MO",
    F.rpad(F.col("PAYMT_COV_YR_MO"), 6, " ")
)

write_files(
    df_B_ACA_PGM_PAYMT_final,
    f"{adls_path}/load/B_ACA_PGM_PAYMT.{SrcSysCd}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)