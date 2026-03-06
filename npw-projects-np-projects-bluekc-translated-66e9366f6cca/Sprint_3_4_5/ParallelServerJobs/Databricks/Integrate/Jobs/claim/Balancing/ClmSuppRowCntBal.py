# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
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
# MAGIC Developer                                    Date                 Project/Altiris #                       Change Description                                       Development Project          Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Geetanjali Rajendran              22/04/2021          200015                                        Originally Programmed                           IntegrateDev2		Abhiram Dasarathy	2021-05-06

# MAGIC File checked later for rows and email to on-call
# MAGIC Detail file for on-call to research errors
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DecimalType, BooleanType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# PARAMETERS
RunID = get_widget_value('RunID','')
ToleranceCd = get_widget_value('ToleranceCd','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
ExtrRunCycle = get_widget_value('ExtrRunCycle','')
TrgtTable = get_widget_value('TrgtTable','')
SrcTable = get_widget_value('SrcTable','')
NotifyMessage = get_widget_value('NotifyMessage','')
UWSOwner = get_widget_value('UWSOwner','')
uws_secret_name = get_widget_value('uws_secret_name','')
SubjectArea = get_widget_value('SubjectArea','')
CurrentDate = get_widget_value('CurrentDate','')  # Not directly used in expressions

# CAST OR PREPARE ANY PYTHON VARIABLES
intToleranceCd = int(ToleranceCd if ToleranceCd.strip() else 0)

# IDS DATABASE CONNECTION FOR "Compare" STAGE
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
extract_query_Compare = (
    f"SELECT TRGT.SRC_SYS_CD as TRGT_SRC_SYS_CD, TRGT.CLM_ID as TRGT_CLM_ID, "
    f"SRC.SRC_SYS_CD as SRC_SRC_SYS_CD, SRC.CLM_ID as SRC_CLM_ID "
    f"FROM {IDSOwner}.{TrgtTable} AS TRGT "
    f"FULL OUTER JOIN {IDSOwner}.{SrcTable} AS SRC "
    f"ON TRGT.SRC_SYS_CD=SRC.SRC_SYS_CD AND TRGT.CLM_ID=SRC.CLM_ID "
    f"WHERE TRGT.LAST_UPDT_RUN_CYC_EXCTN_SK={ExtrRunCycle} "
    f"ORDER BY SRC.SRC_SYS_CD"
)
df_Compare = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_Compare)
    .load()
)

# TRANSFORM LOGIC (Stage "TransformLogic")
w = Window.orderBy(F.col("SRC_SRC_SYS_CD"))
df_Compare_with_rn = df_Compare.withColumn("row_in", F.row_number().over(w))

# Research dataset
df_Research = (
    df_Compare_with_rn.filter(
        F.col("TRGT_SRC_SYS_CD").isNull()
        | F.col("TRGT_CLM_ID").isNull()
        | F.col("SRC_SRC_SYS_CD").isNull()
        | F.col("SRC_CLM_ID").isNull()
    )
    .select(
        F.col("TRGT_SRC_SYS_CD").alias("TRGT_SRC_SYS_CD_SK"),
        F.col("TRGT_CLM_ID").alias("TRGT_CLM_ID"),
        F.col("SRC_SRC_SYS_CD").alias("SRC_SRC_SYS_CD_SK"),
        F.col("SRC_CLM_ID").alias("SRC_CLM_ID")
    )
)

# Notify dataset
df_Notify = (
    df_Compare_with_rn.filter(
        (F.col("row_in") == 1) & (F.col("row_in") > intToleranceCd)
    )
    .select(
        F.rpad(F.lit(NotifyMessage), 70, " ").alias("NOTIFICATION")
    )
)

# ROW_CNT dataset
# The column SRC_SYS_CD is a varchar -> use rpad with unknown length <...>
df_ROW_CNT = (
    df_Compare_with_rn.filter(F.col("row_in") == 1)
    .select(
        F.rpad(F.col("SRC_SRC_SYS_CD"), <...>, " ").alias("SRC_SYS_CD")
    )
)

# WRITE ResearchFile (Stage "ResearchFile")
file_path_ResearchFile = f"{adls_path}/balancing/research/{SrcTable}.{TrgtTable}.dat.{RunID}"
write_files(
    df_Research,
    file_path_ResearchFile,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# WRITE ErrorNotificationFile (Stage "ErrorNotificationFile")
file_path_ErrorNotificationFile = f"{adls_path}/balancing/notify/ClaimBalancingNotification.dat"
write_files(
    df_Notify,
    file_path_ErrorNotificationFile,
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# TRANSFORM (Stage "Transform") - Derive stage variables, produce two outputs
df_TRANS_input = df_ROW_CNT.limit(1).withColumn(
    "svTlrnc", F.lit(intToleranceCd)
).withColumn(
    "svSrcCnt", GetSrcCount(IDSOwner, <...>, <...>, <...>, F.lit(SrcTable))
).withColumn(
    "svTrgtCnt", GetTrgtCount(IDSOwner, <...>, <...>, <...>, F.lit(TrgtTable), F.lit(ExtrRunCycle))
).withColumn(
    "Diff", F.col("svTrgtCnt") - F.col("svSrcCnt")
).withColumn(
    "svTlrncCd",
    F.when(F.col("Diff") == 0, F.lit("BAL"))
    .when(F.abs(F.col("Diff")) > F.col("svTlrnc"), F.lit("OUT"))
    .otherwise(F.lit("IN"))
)

# ROW_CNT_OUT (Stage "Transform") => Link "ROW_CNT_OUT"
df_ROW_CNT_OUT = df_TRANS_input.select(
    F.rpad(F.lit("IDS"), <...>, " ").alias("TRGT_SYS_CD"),
    F.rpad(F.col("SRC_SYS_CD"), <...>, " ").alias("SRC_SYS_CD"),
    F.rpad(F.lit(SubjectArea), <...>, " ").alias("SUBJ_AREA_NM"),
    F.rpad(F.lit(TrgtTable), <...>, " ").alias("TRGT_TBL_NM"),
    F.col("svTrgtCnt").alias("TRGT_CT"),
    F.col("svSrcCnt").alias("SRC_CT"),
    F.col("Diff").alias("DIFF_CT"),
    F.rpad(F.col("svTlrncCd"), <...>, " ").alias("TLRNC_CD"),
    F.col("svTlrnc").alias("TLRNC_AMT"),
    F.col("svTlrnc").cast(DecimalType(10,2)).alias("TLRNC_MULT_AMT"),
    F.rpad(F.lit("DAILY"), <...>, " ").alias("FREQ_CD"),
    F.lit(ExtrRunCycle).cast(IntegerType()).alias("TRGT_RUN_CYC_NO"),
    F.rpad(F.lit(" "), <...>, " ").alias("CRCTN_NOTE"),
    current_timestamp().alias("CRT_DTM"),
    current_timestamp().alias("LAST_UPDT_DTM"),
    F.rpad(F.lit(<...>), <...>, " ").alias("USER_ID")  # $UWSAcct unknown
)

# TBL_BAL_SUM (Stage "Transform") => Link "TBL_BAL_SUM"
df_TBL_BAL_SUM = df_TRANS_input.select(
    F.rpad(F.lit("IDS"), <...>, " ").alias("TRGT_SYS_CD"),
    F.rpad(F.lit(SubjectArea), <...>, " ").alias("SUBJ_AREA_NM"),
    F.rpad(F.lit(TrgtTable), <...>, " ").alias("TRGT_TBL_NM"),
    current_timestamp().alias("BAL_DT"),
    F.col("svTlrncCd").alias("ROW_CT_TLRNC_CD"),
    F.rpad(F.lit("NA"), <...>, " ").alias("CLMN_SUM_TLRNC_CD"),
    F.rpad(F.lit("NA"), <...>, " ").alias("ROW_TO_ROW_TLRNC_CD"),
    F.rpad(F.lit("NA"), <...>, " ").alias("RI_TLRNC_CD"),
    F.rpad(F.lit("NA"), <...>, " ").alias("RELSHP_CLMN_SUM_TLRNC_CD"),
    F.rpad(F.lit("NA"), <...>, " ").alias("CRS_FOOT_TLRNC_CD"),
    F.rpad(F.lit("NA"), <...>, " ").alias("HIST_RSNBL_TLRNC_CD"),
    F.lit(ExtrRunCycle).cast(IntegerType()).alias("TRGT_RUN_CYC_NO"),
    current_timestamp().alias("CRT_DTM"),
    current_timestamp().alias("LAST_UPDT_DTM"),
    F.rpad(F.lit(<...>), <...>, " ").alias("USER_ID")  # $UWSAcct unknown
)

# WRITE ROW_CNT_OUT to file (Stage "ROW_CT_UPDATE_FILE")
file_path_ROW_CT_UPDATE_FILE = f"{adls_path}/balancing/{SrcTable}{TrgtTable}RowCnt.dat"
write_files(
    df_ROW_CNT_OUT,
    file_path_ROW_CT_UPDATE_FILE,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# BUILD SCHEMA FOR READING ROW_CT_UPDATE_FILE
schema_ROW_CT_UPDATE_FILE = StructType([
    StructField("TRGT_SYS_CD", StringType(), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("SUBJ_AREA_NM", StringType(), False),
    StructField("TRGT_TBL_NM", StringType(), False),
    StructField("TRGT_CT", IntegerType(), False),
    StructField("SRC_CT", IntegerType(), False),
    StructField("DIFF_CT", IntegerType(), False),
    StructField("TLRNC_CD", StringType(), False),
    StructField("TLRNC_AMT", IntegerType(), False),
    StructField("TLRNC_MULT_AMT", DecimalType(10,2), False),
    StructField("FREQ_CD", StringType(), False),
    StructField("TRGT_RUN_CYC_NO", IntegerType(), False),
    StructField("CRCTN_NOTE", StringType(), True),
    StructField("CRT_DTM", TimestampType(), False),
    StructField("LAST_UPDT_DTM", TimestampType(), False),
    StructField("USER_ID", StringType(), False)
])

df_ROW_CT_UPDATE_FILE = spark.read.csv(
    file_path_ROW_CT_UPDATE_FILE,
    schema=schema_ROW_CT_UPDATE_FILE,
    sep=",",
    quote="\"",
    header=False
)

# DB OUTPUT FOR TBL_BAL_SUM (Stage "TBL_BAL_SUM" -> "CODBCStage")
jdbc_url_uws, jdbc_props_uws = get_db_config(uws_secret_name)
temp_table_TBL_BAL_SUM = "STAGING.ClmSuppRowCntBal_TBL_BAL_SUM_temp"
execute_dml(f"DROP TABLE IF EXISTS {temp_table_TBL_BAL_SUM}", jdbc_url_uws, jdbc_props_uws)
df_TBL_BAL_SUM.write.jdbc(
    url=jdbc_url_uws,
    table=temp_table_TBL_BAL_SUM,
    mode="errorifexists",
    properties=jdbc_props_uws
)
merge_sql_TBL_BAL_SUM = f"""
MERGE {UWSOwner}.TBL_BAL_SUM AS T
USING {temp_table_TBL_BAL_SUM} AS S
ON 1=0
WHEN NOT MATCHED THEN
    INSERT (
        TRGT_SYS_CD,
        SUBJ_AREA_NM,
        TRGT_TBL_NM,
        BAL_DT,
        ROW_CT_TLRNC_CD,
        CLMN_SUM_TLRNC_CD,
        ROW_TO_ROW_TLRNC_CD,
        RI_TLRNC_CD,
        RELSHP_CLMN_SUM_TLRNC_CD,
        CRS_FOOT_TLRNC_CD,
        HIST_RSNBL_TLRNC_CD,
        TRGT_RUN_CYC_NO,
        CRT_DTM,
        LAST_UPDT_DTM,
        USER_ID
    )
    VALUES (
        S.TRGT_SYS_CD,
        S.SUBJ_AREA_NM,
        S.TRGT_TBL_NM,
        S.BAL_DT,
        S.ROW_CT_TLRNC_CD,
        S.CLMN_SUM_TLRNC_CD,
        S.ROW_TO_ROW_TLRNC_CD,
        S.RI_TLRNC_CD,
        S.RELSHP_CLMN_SUM_TLRNC_CD,
        S.CRS_FOOT_TLRNC_CD,
        S.HIST_RSNBL_TLRNC_CD,
        S.TRGT_RUN_CYC_NO,
        S.CRT_DTM,
        S.LAST_UPDT_DTM,
        S.USER_ID
    );
"""
execute_dml(merge_sql_TBL_BAL_SUM, jdbc_url_uws, jdbc_props_uws)

# DB OUTPUT FOR ROW_CT_DTL (Stage "ROW_CT_DTL" -> "CODBCStage")
temp_table_ROW_CT_DTL = "STAGING.ClmSuppRowCntBal_ROW_CT_DTL_temp"
execute_dml(f"DROP TABLE IF EXISTS {temp_table_ROW_CT_DTL}", jdbc_url_uws, jdbc_props_uws)
df_ROW_CT_UPDATE_FILE.write.jdbc(
    url=jdbc_url_uws,
    table=temp_table_ROW_CT_DTL,
    mode="errorifexists",
    properties=jdbc_props_uws
)
merge_sql_ROW_CT_DTL = f"""
MERGE {UWSOwner}.ROW_CT_DTL AS T
USING {temp_table_ROW_CT_DTL} AS S
ON 1=0
WHEN NOT MATCHED THEN
    INSERT (
        TRGT_SYS_CD,
        SRC_SYS_CD,
        SUBJ_AREA_NM,
        TRGT_TBL_NM,
        TRGT_CT,
        SRC_CT,
        DIFF_CT,
        TLRNC_CD,
        TLRNC_AMT,
        TLRNC_MULT_AMT,
        FREQ_CD,
        TRGT_RUN_CYC_NO,
        CRCTN_NOTE,
        CRT_DTM,
        LAST_UPDT_DTM,
        USER_ID
    )
    VALUES (
        S.TRGT_SYS_CD,
        S.SRC_SYS_CD,
        S.SUBJ_AREA_NM,
        S.TRGT_TBL_NM,
        S.TRGT_CT,
        S.SRC_CT,
        S.DIFF_CT,
        S.TLRNC_CD,
        S.TLRNC_AMT,
        S.TLRNC_MULT_AMT,
        S.FREQ_CD,
        S.TRGT_RUN_CYC_NO,
        S.CRCTN_NOTE,
        S.CRT_DTM,
        S.LAST_UPDT_DTM,
        S.USER_ID
    );
"""
execute_dml(merge_sql_ROW_CT_DTL, jdbc_url_uws, jdbc_props_uws)