# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 04/24/09 09:45:30 Batch  15090_35139 PROMOTE bckcetl ids20 dsadm bls for sa
# MAGIC ^1_1 04/24/09 09:36:12 Batch  15090_34576 INIT bckcett testIDS dsadm bls for sa
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
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
# MAGIC Sharon Andrew                 2009-04-31       Production Suport           Originally Programmed                           devlIDS                        Steph Goddard             04/09/2009
# MAGIC Brent Leland                     2010-02-11    TTR- 719                        Added Src_Sys_Cd = IDS to ref. SQL    IntegrateWrhsDevl         Steph Goddard             02/17/2010
# MAGIC                                                                                                       to eliminate multiple rows returned


# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, DecimalType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


CurrentDate = get_widget_value('CurrentDate','')
Source = get_widget_value('Source','')
RunID = get_widget_value('RunID','100')
ToleranceCd = get_widget_value('ToleranceCd','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','100')
SrcTableName = get_widget_value('SrcTableName','B_CLM_LN_REMIT')
SrcColumnName = get_widget_value('SrcColumnName','CHRG_AMT')
TrgtTableName = get_widget_value('TrgtTableName','CLM_LN_REMIT')
TrgtColumnName = get_widget_value('TrgtColumnName','')
SrcAcct = get_widget_value('SrcAcct','')
SrcPW = get_widget_value('SrcPW','')
SrcDSN = get_widget_value('SrcDSN','')
SrcOwner = get_widget_value('SrcOwner','')
TrgtAcct = get_widget_value('TrgtAcct','')
TrgtPW = get_widget_value('TrgtPW','')
TrgtDSN = get_widget_value('TrgtDSN','')
TrgtOwner = get_widget_value('TrgtOwner','')
UWSOwner = get_widget_value('$UWSOwner','$PROJDEF')
UWSAcct = get_widget_value('UWSAcct','')
db_secret_name = get_widget_value('db_secret_name','')

jdbc_url, jdbc_props = get_db_config(db_secret_name)

df_SRC_SYS_CD = spark.read.parquet(f"{adls_path}/hf_etrnl_cd_mppng.parquet")

sql_source = f"SELECT SUM({SrcColumnName}) AS VALUE, SRC_SYS_CD_SK FROM {SrcOwner}.{SrcTableName} GROUP BY SRC_SYS_CD_SK"
df_Source = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", sql_source)
    .load()
)

sql_target = (
    f"SELECT SUM({TrgtColumnName}) AS VALUE "
    f"FROM {TrgtOwner}.{TrgtTableName} "
    f"WHERE LAST_UPDT_RUN_CYC_EXCTN_SK = {ExtrRunCycle} "
    f"  AND SRC_SYS_CD_SK = ( SELECT CD_MPPNG_SK "
    f"                        FROM {TrgtOwner}.CD_MPPNG "
    f"                       WHERE SRC_CD='{Source}' "
    f"                         AND SRC_DOMAIN_NM='SOURCE SYSTEM' "
    f"                         AND SRC_SYS_CD='IDS')"
)
df_Target = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", sql_target)
    .load()
)

df_ValueCompare = df_Source.alias("Src").join(
    df_Target.alias("Trgt"), F.col("Src.VALUE") == F.col("Trgt.VALUE"), "left"
)
df_ValueCompare = df_ValueCompare.limit(1)

df_ValueCompare_out = df_ValueCompare.select(
    F.lit(SrcTableName).alias("SRC_TABLE_NAME"),
    F.lit(SrcColumnName).alias("SRC_COLUMN_NAME"),
    F.lit(TrgtTableName).alias("TRGT_TABLE_NAME"),
    F.lit(TrgtColumnName).alias("TRGT_COLUMN_NAME"),
    F.lit(ExtrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("Src.VALUE").alias("SRC_VALUE"),
    F.col("Trgt.VALUE").alias("TRGT_VALUE"),
    (F.col("Src.VALUE") - F.col("Trgt.VALUE")).alias("DIFF"),
    F.col("Src.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK")
)

df_TransformLogic = df_ValueCompare_out.alias("COL_SUM").join(
    df_SRC_SYS_CD.alias("SRC_SYS_CD"),
    F.col("COL_SUM.SRC_SYS_CD_SK") == F.col("SRC_SYS_CD.CD_MPPNG_SK"),
    "left"
)

df_TransformLogic = df_TransformLogic.withColumn(
    "svTlrncCd",
    F.when(F.col("DIFF") == 0, F.lit("BAL")).otherwise(
        F.when(F.abs(F.col("DIFF")) > F.lit(ToleranceCd), F.lit("OUT")).otherwise(F.lit("IN"))
    )
)

df_COL_SUM_DTL = df_TransformLogic.select(
    F.lit("IDS").alias("TRGT_SYS_CD"),
    F.col("SRC_SYS_CD.TRGT_CD").alias("SRC_SYS_CD"),
    F.lit("ClaimLineRemit").alias("SUBJ_AREA_NM"),
    F.col("COL_SUM.TRGT_TABLE_NAME").alias("TRGT_TBL_NM"),
    F.col("COL_SUM.TRGT_COLUMN_NAME").alias("TRGT_CLMN_NM"),
    F.col("COL_SUM.TRGT_VALUE").alias("TRGT_AMT"),
    F.col("COL_SUM.SRC_VALUE").alias("SRC_AMT"),
    F.col("COL_SUM.DIFF").alias("DIFF_AMT"),
    F.col("svTlrncCd").alias("TLRNC_CD"),
    F.lit(ToleranceCd).alias("TLRNC_AMT"),
    F.lit(ToleranceCd).alias("TLRNC_MULT_AMT"),
    F.lit("Daily").alias("FREQ_CD"),
    F.col("COL_SUM.CRT_RUN_CYC_EXCTN_SK").alias("TRGT_RUN_CYC_NO"),
    F.lit(" ").alias("CRCTN_NOTE"),
    current_timestamp().alias("CRT_DTM"),
    current_timestamp().alias("LAST_UPDT_DTM"),
    F.lit(UWSAcct).alias("USER_ID")
)

df_COL_SUM_DTL = df_COL_SUM_DTL.withColumn("TRGT_SYS_CD", F.rpad(F.col("TRGT_SYS_CD"), <...>, " "))
df_COL_SUM_DTL = df_COL_SUM_DTL.withColumn("SRC_SYS_CD", F.rpad(F.col("SRC_SYS_CD"), <...>, " "))
df_COL_SUM_DTL = df_COL_SUM_DTL.withColumn("SUBJ_AREA_NM", F.rpad(F.col("SUBJ_AREA_NM"), <...>, " "))
df_COL_SUM_DTL = df_COL_SUM_DTL.withColumn("TRGT_TBL_NM", F.rpad(F.col("TRGT_TBL_NM"), <...>, " "))
df_COL_SUM_DTL = df_COL_SUM_DTL.withColumn("TRGT_CLMN_NM", F.rpad(F.col("TRGT_CLMN_NM"), <...>, " "))
df_COL_SUM_DTL = df_COL_SUM_DTL.withColumn("TLRNC_CD", F.rpad(F.col("TLRNC_CD"), <...>, " "))
df_COL_SUM_DTL = df_COL_SUM_DTL.withColumn("FREQ_CD", F.rpad(F.col("FREQ_CD"), <...>, " "))
df_COL_SUM_DTL = df_COL_SUM_DTL.withColumn("CRCTN_NOTE", F.rpad(F.col("CRCTN_NOTE"), <...>, " "))
df_COL_SUM_DTL = df_COL_SUM_DTL.withColumn("USER_ID", F.rpad(F.col("USER_ID"), <...>, " "))

execute_dml("DROP TABLE IF EXISTS STAGING.ClmLnRemitColumnSumBal_COL_SUM_DTL_temp", jdbc_url, jdbc_props)

df_COL_SUM_DTL.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.ClmLnRemitColumnSumBal_COL_SUM_DTL_temp") \
    .mode("overwrite") \
    .save()

merge_sql = f"""
MERGE {UWSOwner}.CLMN_SUM_DTL AS T
USING STAGING.ClmLnRemitColumnSumBal_COL_SUM_DTL_temp AS S
ON 1=0
WHEN NOT MATCHED THEN
INSERT (
    TRGT_SYS_CD, SRC_SYS_CD, SUBJ_AREA_NM, TRGT_TBL_NM, TRGT_CLMN_NM, TRGT_AMT,
    SRC_AMT, DIFF_AMT, TLRNC_CD, TLRNC_AMT, TLRNC_MULT_AMT, FREQ_CD,
    TRGT_RUN_CYC_NO, CRCTN_NOTE, CRT_DTM, LAST_UPDT_DTM, USER_ID
)
VALUES (
    S.TRGT_SYS_CD, S.SRC_SYS_CD, S.SUBJ_AREA_NM, S.TRGT_TBL_NM, S.TRGT_CLMN_NM, S.TRGT_AMT,
    S.SRC_AMT, S.DIFF_AMT, S.TLRNC_CD, S.TLRNC_AMT, S.TLRNC_MULT_AMT, S.FREQ_CD,
    S.TRGT_RUN_CYC_NO, S.CRCTN_NOTE, S.CRT_DTM, S.LAST_UPDT_DTM, S.USER_ID
);
"""
execute_dml(merge_sql, jdbc_url, jdbc_props)