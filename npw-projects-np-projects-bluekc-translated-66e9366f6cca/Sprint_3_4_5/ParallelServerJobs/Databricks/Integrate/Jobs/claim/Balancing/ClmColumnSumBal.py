# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 03/10/09 09:25:21 Batch  15045_33943 INIT bckcetl ids20 dcg01 sa Bringing ALL Claim code down to devlIDS
# MAGIC ^1_3 02/10/09 11:16:45 Batch  15017_40611 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 01/29/08 08:55:59 Batch  14639_32164 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 12/28/07 10:13:19 Batch  14607_36806 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 11/26/07 09:37:10 Batch  14575_34636 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_2 10/29/07 14:28:38 Batch  14547_52124 PROMOTE bckcetl ids20 dsadm bls for on
# MAGIC ^1_2 10/29/07 14:19:25 Batch  14547_51567 INIT bckcett testIDS30 dsadm bls for on
# MAGIC ^1_1 10/26/07 12:41:15 Batch  14544_45689 PROMOTE bckcett testIDS30 u10913 Ollie move column sum bal changes
# MAGIC ^1_1 10/26/07 12:39:27 Batch  14544_45574 INIT bckcett devlIDS30 u10913 Ollie
# MAGIC ^1_2 09/30/07 15:29:43 Batch  14518_55788 INIT bckcett devlIDS30 u03651 steffy
# MAGIC ^1_2 09/29/07 17:49:00 Batch  14517_64144 INIT bckcett devlIDS30 u03651 steffy
# MAGIC ^1_1 09/25/07 12:58:00 Batch  14513_46683 INIT bckcett devlIDS30 u03651 steffy
# MAGIC 
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is where the actual balancing takes place and the Source and Target files are compared to see if there are any discrepancies 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Oliver Nielsen                   07/01/2007          3264                              Originally Programmed                           devlIDS30


# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql import types as T
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# Retrieve parameters
RunID = get_widget_value("RunID","14094457")
ToleranceCd = get_widget_value("ToleranceCd","")
ExtrRunCycle = get_widget_value("ExtrRunCycle","09122127")
SrcTableName = get_widget_value("SrcTableName","B_CLM")
SrcColumnName = get_widget_value("SrcColumnName","CHRG_AMT")
TrgtTableName = get_widget_value("TrgtTableName","CLM")
TrgtColumnName = get_widget_value("TrgtColumnName","CHRG_AMT")
SrcDSN = get_widget_value("SrcDSN","ids_devl")
SrcOwner = get_widget_value("SrcOwner","db2devl")
SrcAcct = get_widget_value("SrcAcct","u10913")
SrcPW = get_widget_value("SrcPW","L4;@1KVAL9:M0GGI5KN<1OAGF<?")
TrgtDSN = get_widget_value("TrgtDSN","ids_devl")
TrgtOwner = get_widget_value("TrgtOwner","db2devl")
TrgtAcct = get_widget_value("TrgtAcct","u10913")
TrgtPW = get_widget_value("TrgtPW","L4;@1KVAL9:M0GGI5KN<1OAGF<?")
UWSOwner = get_widget_value("$UWSOwner","uws_devl.dbo")
CurrentDate = get_widget_value("CurrentDate","")
Source = get_widget_value("Source","FACETS")

# Prepare JDBC connections
srcdsn_secret_name = SrcDSN
jdbc_url_src, jdbc_props_src = get_db_config(srcdsn_secret_name)

trgtdsn_secret_name = TrgtDSN
jdbc_url_trgt, jdbc_props_trgt = get_db_config(trgtdsn_secret_name)

# ------------------------------------------------------------------------------
# Stage: SRC_SYS_CD (CHashedFileStage) => Scenario C (read from parquet)
# ------------------------------------------------------------------------------
df_SRC_SYS_CD = spark.read.parquet(f"{adls_path}/hf_etrnl_cd_mppng.parquet")

# ------------------------------------------------------------------------------
# Stage: Source (CODBCStage) => read from database
# ------------------------------------------------------------------------------
extract_query_src = (
    f"SELECT SUM({SrcColumnName}) as VALUE, SRC_SYS_CD_SK "
    f"FROM {SrcOwner}.{SrcTableName} "
    f"WHERE CLM_STTUS_CD_SK IN ( "
    f"    SELECT CD_MPPNG_SK "
    f"    FROM {SrcOwner}.CD_MPPNG "
    f"    WHERE (SRC_DOMAIN_NM='CLAIM STATUS') "
    f"          AND ((TRGT_CD='A01') OR (TRGT_CD='A02') OR (TRGT_CD='A09')) "
    f") "
    f"AND CLM_CAT_CD_SK IN ( "
    f"    SELECT CD_MPPNG_SK "
    f"    FROM {SrcOwner}.CD_MPPNG "
    f"    WHERE (TRGT_CD='STD' AND SRC_CD IN ('01','02','91')) "
    f") "
    f"GROUP BY SRC_SYS_CD_SK"
)

df_Source = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_src)
    .options(**jdbc_props_src)
    .option("query", extract_query_src)
    .load()
)

# ------------------------------------------------------------------------------
# Stage: Target (CODBCStage) => read from database
# ------------------------------------------------------------------------------
extract_query_trgt = (
    f"SELECT SUM({TrgtColumnName}) as VALUE "
    f"FROM {TrgtOwner}.{TrgtTableName} "
    f"WHERE SRC_SYS_CD_SK IN ( "
    f"    SELECT CD_MPPNG_SK "
    f"    FROM {TrgtOwner}.CD_MPPNG "
    f"    WHERE SRC_CD='{Source}' "
    f"          AND SRC_DOMAIN_NM='SOURCE SYSTEM' "
    f") "
    f"AND LAST_UPDT_RUN_CYC_EXCTN_SK = {ExtrRunCycle} "
    f"AND CLM_STTUS_CD_SK IN ( "
    f"    SELECT CD_MPPNG_SK "
    f"    FROM {TrgtOwner}.CD_MPPNG "
    f"    WHERE (SRC_DOMAIN_NM='CLAIM STATUS') "
    f"          AND ((TRGT_CD='A01') OR (TRGT_CD='A02') OR (TRGT_CD='A09')) "
    f") "
    f"AND CLM_CAT_CD_SK IN ( "
    f"    SELECT CD_MPPNG_SK "
    f"    FROM {TrgtOwner}.CD_MPPNG "
    f"    WHERE (TRGT_CD='STD' AND SRC_CD IN ('01','02','91')) "
    f")"
)

df_Target = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_trgt)
    .options(**jdbc_props_trgt)
    .option("query", extract_query_trgt)
    .load()
)

# ------------------------------------------------------------------------------
# Stage: ValueCompare (CTransformerStage)
#   Primary Link: df_Source as "Src"
#   Lookup Link: df_Target as "Trgt" (left join on Src.VALUE = Trgt.VALUE)
#   Constraint: "@INROWNUM = 1" (pass only the first row)
# ------------------------------------------------------------------------------
df_ValueCompare_joined = df_Source.alias("Src").join(
    df_Target.alias("Trgt"),
    F.col("Src.VALUE") == F.col("Trgt.VALUE"),
    how="left"
)

df_ValueCompare_limited = df_ValueCompare_joined.limit(1)

df_COL_SUM = (
    df_ValueCompare_limited
    .withColumn("SRC_TABLE_NAME", F.lit(SrcTableName))
    .withColumn("SRC_COLUMN_NAME", F.lit(SrcColumnName))
    .withColumn("TRGT_TABLE_NAME", F.lit(TrgtTableName))
    .withColumn("TRGT_COLUMN_NAME", F.lit(TrgtColumnName))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(ExtrRunCycle))
    .withColumn("SRC_VALUE", F.col("Src.VALUE"))
    .withColumn("TRGT_VALUE", F.col("Trgt.VALUE"))
    .withColumn("DIFF", F.col("Src.VALUE") - F.col("Trgt.VALUE"))
    .withColumn("SRC_SYS_CD_SK", F.col("Src.SRC_SYS_CD_SK"))
)

# ------------------------------------------------------------------------------
# Stage: TransformLogic (CTransformerStage)
#   Primary Link: df_COL_SUM as "COL_SUM"
#   Lookup Link: df_SRC_SYS_CD as "SRC_SYS_CD" (left join)
#   Stage vars:
#       svTlrnc = ToleranceCd
#       svTlrncCd = if DIFF=0 => 'BAL', elif abs(DIFF)>ToleranceCd => 'OUT' else 'IN'
# ------------------------------------------------------------------------------
df_TransformLogic_joined = df_COL_SUM.alias("COL_SUM").join(
    df_SRC_SYS_CD.alias("SRC_SYS_CD"),
    F.col("COL_SUM.SRC_SYS_CD_SK") == F.col("SRC_SYS_CD.CD_MPPNG_SK"),
    how="left"
)

df_TransformLogic = (
    df_TransformLogic_joined
    .withColumn(
        "svTlrncCd",
        F.when(F.col("COL_SUM.DIFF") == 0, F.lit("BAL"))
         .otherwise(
             F.when(F.abs(F.col("COL_SUM.DIFF")) > F.lit(ToleranceCd), F.lit("OUT")).otherwise(F.lit("IN"))
         )
    )
)

# ------------------------------------------------------------------------------
# Stage: TransformLogic Output => COL_SUM_DTL (CTransformerStage output columns)
# ------------------------------------------------------------------------------
df_COL_SUM_DTL_unordered = (
    df_TransformLogic
    .withColumn("TRGT_SYS_CD", F.lit("IDS"))
    .withColumn("SRC_SYS_CD", F.col("SRC_SYS_CD.TRGT_CD"))
    .withColumn("SUBJ_AREA_NM", F.lit("Claims"))
    .withColumn("TRGT_TBL_NM", F.col("COL_SUM.TRGT_TABLE_NAME"))
    .withColumn("TRGT_CLMN_NM", F.col("COL_SUM.TRGT_COLUMN_NAME"))
    .withColumn("TRGT_AMT", F.col("COL_SUM.TRGT_VALUE"))
    .withColumn("SRC_AMT", F.col("COL_SUM.SRC_VALUE"))
    .withColumn("DIFF_AMT", F.col("COL_SUM.DIFF"))
    .withColumn("TLRNC_CD", F.col("svTlrncCd"))
    .withColumn("TLRNC_AMT", F.lit(ToleranceCd))
    .withColumn("TLRNC_MULT_AMT", F.lit(ToleranceCd))
    .withColumn("FREQ_CD", F.lit("Daily"))
    .withColumn("TRGT_RUN_CYC_NO", F.lit(ExtrRunCycle))
    .withColumn("CRCTN_NOTE", F.lit(" "))
    .withColumn("CRT_DTM", current_timestamp())
    .withColumn("LAST_UPDT_DTM", current_timestamp())
    .withColumn("USER_ID", F.lit("<...>"))  # "$UWSAcct" unknown
)

# Apply column order and rpad for char/varchar fields (length unknown => <...>)
df_COL_SUM_DTL = df_COL_SUM_DTL_unordered.select(
    F.rpad(F.col("TRGT_SYS_CD"), <...>, " ").alias("TRGT_SYS_CD"),
    F.rpad(F.col("SRC_SYS_CD"), <...>, " ").alias("SRC_SYS_CD"),
    F.rpad(F.col("SUBJ_AREA_NM"), <...>, " ").alias("SUBJ_AREA_NM"),
    F.rpad(F.col("TRGT_TBL_NM"), <...>, " ").alias("TRGT_TBL_NM"),
    F.rpad(F.col("TRGT_CLMN_NM"), <...>, " ").alias("TRGT_CLMN_NM"),
    F.col("TRGT_AMT").alias("TRGT_AMT"),
    F.col("SRC_AMT").alias("SRC_AMT"),
    F.col("DIFF_AMT").alias("DIFF_AMT"),
    F.rpad(F.col("TLRNC_CD"), <...>, " ").alias("TLRNC_CD"),
    F.col("TLRNC_AMT").alias("TLRNC_AMT"),
    F.col("TLRNC_MULT_AMT").alias("TLRNC_MULT_AMT"),
    F.rpad(F.col("FREQ_CD"), <...>, " ").alias("FREQ_CD"),
    F.col("TRGT_RUN_CYC_NO").alias("TRGT_RUN_CYC_NO"),
    F.rpad(F.col("CRCTN_NOTE"), <...>, " ").alias("CRCTN_NOTE"),
    F.col("CRT_DTM").alias("CRT_DTM"),
    F.col("LAST_UPDT_DTM").alias("LAST_UPDT_DTM"),
    F.rpad(F.col("USER_ID"), <...>, " ").alias("USER_ID")
)

# ------------------------------------------------------------------------------
# Stage: COL_SUM_DTL (CODBCStage) => Insert into #$UWSOwner#.CLMN_SUM_DTL
#   Implementing via staging physical table then insert-select
# ------------------------------------------------------------------------------
temp_table_name = "STAGING.ClmColumnSumBal_COL_SUM_DTL_temp"
execute_dml(f"DROP TABLE IF EXISTS {temp_table_name}", jdbc_url_trgt, jdbc_props_trgt)

df_COL_SUM_DTL.write.jdbc(
    url=jdbc_url_trgt,
    table=temp_table_name,
    mode="overwrite",
    properties=jdbc_props_trgt
)

insert_sql = (
    f"INSERT INTO {UWSOwner}.CLMN_SUM_DTL "
    f"(TRGT_SYS_CD, SRC_SYS_CD, SUBJ_AREA_NM, TRGT_TBL_NM, TRGT_CLMN_NM, "
    f"TRGT_AMT, SRC_AMT, DIFF_AMT, TLRNC_CD, TLRNC_AMT, TLRNC_MULT_AMT, "
    f"FREQ_CD, TRGT_RUN_CYC_NO, CRCTN_NOTE, CRT_DTM, LAST_UPDT_DTM, USER_ID) "
    f"SELECT TRGT_SYS_CD, SRC_SYS_CD, SUBJ_AREA_NM, TRGT_TBL_NM, TRGT_CLMN_NM, "
    f"TRGT_AMT, SRC_AMT, DIFF_AMT, TLRNC_CD, TLRNC_AMT, TLRNC_MULT_AMT, "
    f"FREQ_CD, TRGT_RUN_CYC_NO, CRCTN_NOTE, CRT_DTM, LAST_UPDT_DTM, USER_ID "
    f"FROM {temp_table_name}"
)

execute_dml(insert_sql, jdbc_url_trgt, jdbc_props_trgt)