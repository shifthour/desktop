# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_2 03/10/09 09:25:21 Batch  15045_33943 INIT bckcetl ids20 dcg01 sa Bringing ALL Claim code down to devlIDS
# MAGIC ^1_4 02/10/09 11:16:45 Batch  15017_40611 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_3 10/02/08 15:44:02 Batch  14886_56722 PROMOTE bckcetl ids20 dsadm bls for on
# MAGIC ^1_3 10/02/08 15:37:31 Batch  14886_56255 INIT bckcett devlIDSnew dsadm bls for on
# MAGIC ^1_1 10/01/08 10:51:59 Batch  14885_39126 INIT bckcett devlIDSnew u03651 steffy
# MAGIC ^1_1 01/29/08 08:55:59 Batch  14639_32164 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 12/28/07 10:13:19 Batch  14607_36806 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 11/26/07 09:37:10 Batch  14575_34636 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 10/24/07 14:13:49 Batch  14542_51233 PROMOTE bckcetl ids20 dsadm bls for on
# MAGIC ^1_1 10/24/07 14:09:22 Batch  14542_50966 INIT bckcett testIDS30 dsadm bls for on
# MAGIC ^1_2 10/19/07 07:32:17 Batch  14537_27150 PROMOTE bckcett testIDS30 u10913 Ollie move from devl to test
# MAGIC ^1_2 10/19/07 07:30:04 Batch  14537_27018 INIT bckcett devlIDS30 u10913 Ollie move from devl to test
# MAGIC ^1_1 10/17/07 10:10:27 Batch  14535_36634 INIT bckcett devlIDS30 u03651 steffy to test for Ollie
# MAGIC 
# MAGIC 
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
# MAGIC Oliver Nielsen                  10/13/2007          3264                              Originally Programmed                           devlIDS30                 
# MAGIC Oliver Nielsen                   09/22/2008    Prod Supp                     Added IDSDSN to job and to function
# MAGIC                                                                                                       call for GetSrcCnt and GetTrgtCnt             devlIDSnew                 Steph Goddard            09/26/2008
# MAGIC Karthik Chintalaphani       09/01/2012       4784                           Added Order By clause to the 
# MAGIC                                                                                                       source query                                            IntegrateWrhsDevl       Bhoomi Dasari         10/20/2012

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
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DecimalType
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


job_name = "ClmExtraColumnRowCntBal"

RunID = get_widget_value("RunID","")
ToleranceCd = get_widget_value("ToleranceCd","")
IDSOwner = get_widget_value("IDSOwner","")
ExtrRunCycle = get_widget_value("ExtrRunCycle","")
TrgtTable = get_widget_value("TrgtTable","")
SrcTable = get_widget_value("SrcTable","")
NotifyMessage = get_widget_value("NotifyMessage","")
UWSOwner = get_widget_value("UWSOwner","")
SubjectArea = get_widget_value("SubjectArea","")
CurrentDate = get_widget_value("CurrentDate","")
ExtraColumn = get_widget_value("ExtraColumn","")

ids_secret_name = get_widget_value("ids_secret_name","")
uws_secret_name = get_widget_value("uws_secret_name","")

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

extract_query_compare = (
    f"SELECT TRGT.SRC_SYS_CD_SK, "
    f"TRGT.CLM_ID, "
    f"SRC.SRC_SYS_CD_SK, "
    f"SRC.CLM_ID, "
    f"TRGT.{ExtraColumn} "
    f"FROM {IDSOwner}.{TrgtTable} AS TRGT "
    f"FULL OUTER JOIN {IDSOwner}.{SrcTable} AS SRC "
    f"ON TRGT.SRC_SYS_CD_SK=SRC.SRC_SYS_CD_SK "
    f"AND TRGT.CLM_ID=SRC.CLM_ID "
    f"AND TRGT.{ExtraColumn}=SRC.{ExtraColumn} "
    f"WHERE TRGT.LAST_UPDT_RUN_CYC_EXCTN_SK={ExtrRunCycle} "
    f"ORDER BY SRC.SRC_SYS_CD_SK"
)

df_Compare = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_compare)
    .load()
)

df_Missing = df_Compare

df_SRC_SYS_CD = spark.read.parquet(f"{adls_path}/hf_etrnl_cd_mppng.parquet")

df_transformlogic_temp = df_Missing.alias("Missing").join(
    df_SRC_SYS_CD.alias("SRC_SYS_CD"),
    F.col("Missing.TRGT_SRC_SYS_CD_SK") == F.col("SRC_SYS_CD.CD_MPPNG_SK"),
    "left"
)

windowSpec_transformlogic = Window.orderBy(F.lit(1))
df_transformlogic_temp = df_transformlogic_temp.withColumn(
    "ds_row_num",
    F.row_number().over(windowSpec_transformlogic)
)

df_Research = df_transformlogic_temp.filter(
    F.col("Missing.TRGT_SRC_SYS_CD_SK").isNull()
    | F.col("Missing.TRGT_CLM_ID").isNull()
    | F.col("Missing.SRC_SRC_SYS_CD_SK").isNull()
    | F.col("Missing.SRC_CLM_ID").isNull()
    | F.col("Missing.TRGT_EXTRA_COLUMN").isNull()
).select(
    F.col("Missing.TRGT_SRC_SYS_CD_SK").alias("TRGT_SRC_SYS_CD_SK"),
    F.col("Missing.TRGT_CLM_ID").alias("TRGT_CLM_ID"),
    F.col("Missing.SRC_SRC_SYS_CD_SK").alias("SRC_SRC_SYS_CD_SK"),
    F.col("Missing.SRC_CLM_ID").alias("SRC_CLM_ID"),
    F.col("Missing.TRGT_EXTRA_COLUMN").alias("TRGT_EXTRA_COLUMN")
)

df_Notify = df_transformlogic_temp.filter(
    (F.col("ds_row_num") == 1) & (F.col("ds_row_num") > F.lit(ToleranceCd).cast("int"))
).select(
    F.lit(NotifyMessage).alias("NOTIFICATION")
)

df_ROW_CNT = df_transformlogic_temp.filter(
    F.col("ds_row_num") == 1
).select(
    F.col("SRC_SYS_CD.TRGT_CD").alias("SRC_SYS_CD")
)

write_files(
    df_Research,
    f"{adls_path}/balancing/research/{SrcTable}.{TrgtTable}.dat.{RunID}",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)

write_files(
    df_Notify,
    f"{adls_path}/balancing/notify/ClaimBalancingNotification.dat",
    ",",
    "append",
    False,
    False,
    "\"",
    None
)

df_copy_of_transformlogic_in = df_ROW_CNT

df_copy_of_transformlogic_out = (
    df_copy_of_transformlogic_in
    .withColumn("svTlrnc", F.lit(ToleranceCd))
    .withColumn("svSrcCnt", F.lit(GetSrcCount(IDSOwner, "<...>", "<...>", "<...>", SrcTable)))
    .withColumn("svTrgtCnt", F.lit(GetTrgtCount(IDSOwner, "<...>", "<...>", "<...>", TrgtTable, ExtrRunCycle)))
    .withColumn("Diff", (F.col("svTrgtCnt").cast("long") - F.col("svSrcCnt").cast("long")))
    .withColumn(
        "svTlrncCd",
        F.when(F.col("Diff") == 0, F.lit("BAL"))
        .when(F.abs(F.col("Diff")) > F.col("svTlrnc").cast("long"), F.lit("OUT"))
        .otherwise(F.lit("IN"))
    )
)

windowSpec_copy = Window.orderBy(F.lit(1))
df_copy_of_transformlogic_out = df_copy_of_transformlogic_out.withColumn(
    "ds_row_num",
    F.row_number().over(windowSpec_copy)
)

df_ROW_CNT_OUT = df_copy_of_transformlogic_out.filter(
    F.col("ds_row_num") == 1
).select(
    F.lit("IDS").alias("TRGT_SYS_CD"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.lit(SubjectArea).alias("SUBJ_AREA_NM"),
    F.lit(TrgtTable).alias("TRGT_TBL_NM"),
    F.col("svTrgtCnt").alias("TRGT_CT"),
    F.col("svSrcCnt").alias("SRC_CT"),
    F.col("Diff").alias("DIFF_CT"),
    F.col("svTlrncCd").alias("TLRNC_CD"),
    F.col("svTlrnc").alias("TLRNC_AMT"),
    F.col("svTlrnc").alias("TLRNC_MULT_AMT"),
    F.lit("DAILY").alias("FREQ_CD"),
    F.lit(ExtrRunCycle).alias("TRGT_RUN_CYC_NO"),
    F.lit(" ").alias("CRCTN_NOTE"),
    current_timestamp().alias("CRT_DTM"),
    current_timestamp().alias("LAST_UPDT_DTM"),
    F.lit("<...>").alias("USER_ID")
)

df_TBL_BAL_SUM = df_copy_of_transformlogic_out.filter(
    F.col("ds_row_num") == 1
).select(
    F.lit("IDS").alias("TRGT_SYS_CD"),
    F.lit(SubjectArea).alias("SUBJ_AREA_NM"),
    F.lit(TrgtTable).alias("TRGT_TBL_NM"),
    F.lit(CurrentDate).alias("BAL_DT"),
    F.col("svTlrncCd").alias("ROW_CT_TLRNC_CD"),
    F.lit("NA").alias("CLMN_SUM_TLRNC_CD"),
    F.lit("NA").alias("ROW_TO_ROW_TLRNC_CD"),
    F.lit("NA").alias("RI_TLRNC_CD"),
    F.lit("NA").alias("RELSHP_CLMN_SUM_TLRNC_CD"),
    F.lit("NA").alias("CRS_FOOT_TLRNC_CD"),
    F.lit("NA").alias("HIST_RSNBL_TLRNC_CD"),
    F.lit(ExtrRunCycle).alias("TRGT_RUN_CYC_NO"),
    current_timestamp().alias("CRT_DTM"),
    current_timestamp().alias("LAST_UPDT_DTM"),
    F.lit("<...>").alias("USER_ID")
)

jdbc_url_uws, jdbc_props_uws = get_db_config(uws_secret_name)

temp_table_name_tbl_bal_sum = f"STAGING.{job_name}_TBL_BAL_SUM_temp"
merge_sql_tbl_bal_sum = (
    f"DROP TABLE IF EXISTS {temp_table_name_tbl_bal_sum} "
)
execute_dml(merge_sql_tbl_bal_sum, jdbc_url_uws, jdbc_props_uws)

df_TBL_BAL_SUM.write.jdbc(
    url=jdbc_url_uws,
    table=temp_table_name_tbl_bal_sum,
    mode="overwrite",
    properties=jdbc_props_uws
)

merge_sql_tbl_bal_sum = (
    f"MERGE INTO {UWSOwner}.TBL_BAL_SUM AS T "
    f"USING {temp_table_name_tbl_bal_sum} AS S "
    f"ON 1=0 "
    f"WHEN NOT MATCHED THEN INSERT "
    f"(TRGT_SYS_CD, SUBJ_AREA_NM, TRGT_TBL_NM, BAL_DT, ROW_CT_TLRNC_CD, CLMN_SUM_TLRNC_CD, ROW_TO_ROW_TLRNC_CD, RI_TLRNC_CD, RELSHP_CLMN_SUM_TLRNC_CD, CRS_FOOT_TLRNC_CD, HIST_RSNBL_TLRNC_CD, TRGT_RUN_CYC_NO, CRT_DTM, LAST_UPDT_DTM, USER_ID) "
    f"VALUES (S.TRGT_SYS_CD, S.SUBJ_AREA_NM, S.TRGT_TBL_NM, S.BAL_DT, S.ROW_CT_TLRNC_CD, S.CLMN_SUM_TLRNC_CD, S.ROW_TO_ROW_TLRNC_CD, S.RI_TLRNC_CD, S.RELSHP_CLMN_SUM_TLRNC_CD, S.CRS_FOOT_TLRNC_CD, S.HIST_RSNBL_TLRNC_CD, S.TRGT_RUN_CYC_NO, S.CRT_DTM, S.LAST_UPDT_DTM, S.USER_ID);"
)
execute_dml(merge_sql_tbl_bal_sum, jdbc_url_uws, jdbc_props_uws)

df_ROW_CT_UPDATE_FILE = df_ROW_CNT_OUT
df_ROW_CT_UPDATE_FILE = (
    df_ROW_CT_UPDATE_FILE
    .withColumn("TRGT_SYS_CD", rpad(F.col("TRGT_SYS_CD"), <...>, " "))
    .withColumn("SRC_SYS_CD", rpad(F.col("SRC_SYS_CD"), <...>, " "))
    .withColumn("SUBJ_AREA_NM", rpad(F.col("SUBJ_AREA_NM"), <...>, " "))
    .withColumn("TRGT_TBL_NM", rpad(F.col("TRGT_TBL_NM"), <...>, " "))
    .withColumn("TLRNC_CD", rpad(F.col("TLRNC_CD"), <...>, " "))
    .withColumn("FREQ_CD", rpad(F.col("FREQ_CD"), <...>, " "))
    .withColumn("CRCTN_NOTE", rpad(F.col("CRCTN_NOTE"), <...>, " "))
    .withColumn("USER_ID", rpad(F.col("USER_ID"), <...>, " "))
    .select(
        "TRGT_SYS_CD",
        "SRC_SYS_CD",
        "SUBJ_AREA_NM",
        "TRGT_TBL_NM",
        "TRGT_CT",
        "SRC_CT",
        "DIFF_CT",
        "TLRNC_CD",
        "TLRNC_AMT",
        "TLRNC_MULT_AMT",
        "FREQ_CD",
        "TRGT_RUN_CYC_NO",
        "CRCTN_NOTE",
        "CRT_DTM",
        "LAST_UPDT_DTM",
        "USER_ID"
    )
)

write_files(
    df_ROW_CT_UPDATE_FILE,
    f"{adls_path}/balancing/{SrcTable}{TrgtTable}RowCnt.dat",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)

row_ct_schema = StructType([
    StructField("TRGT_SYS_CD", StringType(), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("SUBJ_AREA_NM", StringType(), False),
    StructField("TRGT_TBL_NM", StringType(), False),
    StructField("TRGT_CT", IntegerType(), False),
    StructField("SRC_CT", IntegerType(), False),
    StructField("DIFF_CT", IntegerType(), False),
    StructField("TLRNC_CD", StringType(), False),
    StructField("TLRNC_AMT", IntegerType(), False),
    StructField("TLRNC_MULT_AMT", DecimalType(), False),
    StructField("FREQ_CD", StringType(), False),
    StructField("TRGT_RUN_CYC_NO", IntegerType(), False),
    StructField("CRCTN_NOTE", StringType(), True),
    StructField("CRT_DTM", TimestampType(), False),
    StructField("LAST_UPDT_DTM", TimestampType(), False),
    StructField("USER_ID", StringType(), False)
])

df_ROW_CT_UPDATE_FILE_out = (
    spark.read.format("csv")
    .option("header", False)
    .option("quote", "\"")
    .schema(row_ct_schema)
    .load(f"{adls_path}/balancing/{SrcTable}{TrgtTable}RowCnt.dat")
)

df_ROW_CT_DTL = df_ROW_CT_UPDATE_FILE_out

temp_table_name_row_ct_dtl = f"STAGING.{job_name}_ROW_CT_DTL_temp"
merge_sql_row_ct_dtl = (
    f"DROP TABLE IF EXISTS {temp_table_name_row_ct_dtl} "
)
execute_dml(merge_sql_row_ct_dtl, jdbc_url_uws, jdbc_props_uws)

df_ROW_CT_DTL.write.jdbc(
    url=jdbc_url_uws,
    table=temp_table_name_row_ct_dtl,
    mode="overwrite",
    properties=jdbc_props_uws
)

merge_sql_row_ct_dtl = (
    f"MERGE INTO {UWSOwner}.ROW_CT_DTL AS T "
    f"USING {temp_table_name_row_ct_dtl} AS S "
    f"ON 1=0 "
    f"WHEN NOT MATCHED THEN INSERT "
    f"(TRGT_SYS_CD, SRC_SYS_CD, SUBJ_AREA_NM, TRGT_TBL_NM, TRGT_CT, SRC_CT, DIFF_CT, TLRNC_CD, TLRNC_AMT, TLRNC_MULT_AMT, FREQ_CD, TRGT_RUN_CYC_NO, CRCTN_NOTE, CRT_DTM, LAST_UPDT_DTM, USER_ID) "
    f"VALUES (S.TRGT_SYS_CD, S.SRC_SYS_CD, S.SUBJ_AREA_NM, S.TRGT_TBL_NM, S.TRGT_CT, S.SRC_CT, S.DIFF_CT, S.TLRNC_CD, S.TLRNC_AMT, S.TLRNC_MULT_AMT, S.FREQ_CD, S.TRGT_RUN_CYC_NO, S.CRCTN_NOTE, S.CRT_DTM, S.LAST_UPDT_DTM, S.USER_ID);"
)
execute_dml(merge_sql_row_ct_dtl, jdbc_url_uws, jdbc_props_uws)