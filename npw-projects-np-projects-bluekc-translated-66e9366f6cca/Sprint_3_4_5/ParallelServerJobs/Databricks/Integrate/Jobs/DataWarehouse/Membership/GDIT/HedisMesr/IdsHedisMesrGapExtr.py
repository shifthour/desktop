# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2005, 2006, 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC Job Name : IdsHedisMesrGapExtr
# MAGIC 
# MAGIC Called By: IdsMbrHedisMesrGapCntl
# MAGIC 
# MAGIC                    
# MAGIC PROCESSING:
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                         Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                    ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Goutham Kalidindi             2024-09-10        US-628702                                 Orig Development                                      IntegrateDev2          Reddy Sanam             09-30-2024
# MAGIC 
# MAGIC Goutham Kalidindi             2024-12-23       US-636614          Added new field HEDIS_Measure_Inverted_Indicator       IntegrateDev2          Reddy Sanam             01/02/2025
# MAGIC 
# MAGIC 
# MAGIC Goutham Kalidindi             2025-02-18       US-642701         Added convert function to remove special charecters        IntegrateDev2          Reddy Sanam             02/18/2025
# MAGIC                                                                                               on the DPLY file

# MAGIC Create Primary Key for HEDIS_MESR table
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, lit, when, length, to_timestamp, concat, translate
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------

df_dummy_for_import = None  # Avoid unused import errors

RunID = get_widget_value('RunID','')
RunDate = get_widget_value('RunDate','')
RunCycExctnSK = get_widget_value('RunCycExctnSK','')
IDSOwner = get_widget_value('$IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
SrcSysCd = get_widget_value('SrcSysCd','')
SourceFileName = get_widget_value('SourceFileName','')

schema_HEDIS_Crosswalk_Layout_csv = StructType([
    StructField("HEDIS_Measure_Name", StringType(), True),
    StructField("HEDIS_Sub_Measure_Name", StringType(), True),
    StructField("HEDIS_Member_Bucket_Identifier", StringType(), True),
    StructField("Modality_Identifier", StringType(), True),
    StructField("HEDIS_Measure_Gap_Display_Modality_Effective_Date", StringType(), True),
    StructField("HEDIS_Measure_Abbreviation_Identifier", StringType(), True),
    StructField("HEDIS_Measure_Gap_Display_Modality_Termination_Date", StringType(), True),
    StructField("HEDIS_Measure_Gap_Modality_Category_Identifier", StringType(), True),
    StructField("HEDIS_Measure_Gap_Modality_Display_Text", StringType(), True),
    StructField("HEDIS_Measure_Gap_Modality_Priority_Number", IntegerType(), True),
    StructField("HEDIS_Measure_Gap_Modality_Script_Text", StringType(), True),
    StructField("HEDIS_Measure_Modality_Key_Identifier", StringType(), True),
    StructField("HEDIS_Measure_Inverted_Indicator", StringType(), True),
])

df_HEDIS_Crosswalk_Layout_csv = spark.read.csv(
    path=f"{adls_path_raw}/landing/{SourceFileName}",
    schema=schema_HEDIS_Crosswalk_Layout_csv,
    sep=",",
    quote='"',
    header=True,
    nullValue=None
)

df_tmp = df_HEDIS_Crosswalk_Layout_csv.withColumn(
    "svEFFDT",
    when(length(col("HEDIS_Measure_Gap_Display_Modality_Effective_Date")) == 9,
         concat(lit("0"), col("HEDIS_Measure_Gap_Display_Modality_Effective_Date")))
    .otherwise(col("HEDIS_Measure_Gap_Display_Modality_Effective_Date"))
).withColumn(
    "svTRMDT",
    when(length(col("HEDIS_Measure_Gap_Display_Modality_Termination_Date")) == 9,
         concat(lit("0"), col("HEDIS_Measure_Gap_Display_Modality_Termination_Date")))
    .otherwise(col("HEDIS_Measure_Gap_Display_Modality_Termination_Date"))
)

df_Lnk_MESR_GAP_In = df_tmp.select(
    col("HEDIS_Measure_Name").alias("HEDIS_MESR_NM"),
    col("HEDIS_Sub_Measure_Name").alias("HEDIS_SUB_MESR_NM"),
    col("HEDIS_Member_Bucket_Identifier").alias("HEDIS_MBR_BUCKET_ID"),
    lit("BCBSKC").alias("SRC_SYS_CD"),
    col("HEDIS_Measure_Abbreviation_Identifier").alias("HEDIS_MESR_ABBR_ID")
)

fromString = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789, '!@#$%^&*-+=./<>{}[];:?~` "

df_Lnk_MESR_GAP_DPLY_In = df_tmp.select(
    when(col("HEDIS_Measure_Name").isNotNull(), col("HEDIS_Measure_Name")).otherwise(lit(" ")).alias("HEDIS_MESR_NM"),
    when(col("HEDIS_Sub_Measure_Name").isNotNull(), col("HEDIS_Sub_Measure_Name")).otherwise(lit(" ")).alias("HEDIS_SUB_MESR_NM"),
    when(col("HEDIS_Member_Bucket_Identifier").isNotNull(), col("HEDIS_Member_Bucket_Identifier")).otherwise(lit(" ")).alias("HEDIS_MBR_BUCKET_ID"),
    when(col("Modality_Identifier").isNotNull(), col("Modality_Identifier")).otherwise(lit(" ")).alias("MOD_ID"),
    to_timestamp(col("svEFFDT"), "MM/dd/yyyy").alias("HEDIS_MESR_GAP_DPLY_MOD_EFF_DT"),
    lit("BCBSKC").alias("SRC_SYS_CD"),
    to_timestamp(col("svTRMDT"), "MM/dd/yyyy").alias("HEDIS_MESR_GAP_DPLY_MOD_TERM_DT"),
    when(col("HEDIS_Measure_Gap_Modality_Priority_Number").isNotNull(), col("HEDIS_Measure_Gap_Modality_Priority_Number")).otherwise(lit(" ")).alias("HEDIS_MESR_GAP_MOD_PRTY_NO"),
    when(col("HEDIS_Measure_Abbreviation_Identifier").isNotNull(), col("HEDIS_Measure_Abbreviation_Identifier")).otherwise(lit(" ")).alias("HEDIS_MESR_ABBR_ID"),
    when(col("HEDIS_Measure_Gap_Modality_Category_Identifier").isNotNull(), col("HEDIS_Measure_Gap_Modality_Category_Identifier")).otherwise(lit(" ")).alias("HEDIS_MESR_GAP_MOD_CAT_ID"),
    when(col("HEDIS_Measure_Gap_Modality_Display_Text").isNotNull(), col("HEDIS_Measure_Gap_Modality_Display_Text")).otherwise(lit(" ")).alias("HEDIS_MESR_GAP_MOD_DPLY_TX"),
    translate(
        when(col("HEDIS_Measure_Gap_Modality_Script_Text").isNotNull(), col("HEDIS_Measure_Gap_Modality_Script_Text")).otherwise(lit(" ")),
        fromString,
        None
    ).alias("HEDIS_MESR_GAP_MOD_SCRIPT_TX"),
    when(col("HEDIS_Measure_Modality_Key_Identifier").isNotNull(), col("HEDIS_Measure_Modality_Key_Identifier")).otherwise(lit(" ")).alias("HEDIS_MESR_MOD_KEY_ID"),
    col("HEDIS_Measure_Inverted_Indicator").alias("HEDIS_MESR_GAP_INVRT_IN")
)

df_Lnk_MESR_GAP_In_deduped = dedup_sort(
    df_Lnk_MESR_GAP_In,
    ["HEDIS_MBR_BUCKET_ID", "HEDIS_MESR_ABBR_ID", "HEDIS_MESR_NM", "HEDIS_SUB_MESR_NM", "SRC_SYS_CD"],
    [
        ("HEDIS_MBR_BUCKET_ID","A"),
        ("HEDIS_MESR_ABBR_ID","A"),
        ("HEDIS_MESR_NM","A"),
        ("HEDIS_SUB_MESR_NM","A"),
        ("SRC_SYS_CD","A")
    ]
)

df_HEDIS_MESR_GAP = df_Lnk_MESR_GAP_In_deduped.select(
    col("HEDIS_MESR_NM"),
    col("HEDIS_SUB_MESR_NM"),
    col("HEDIS_MBR_BUCKET_ID"),
    col("SRC_SYS_CD"),
    col("HEDIS_MESR_ABBR_ID")
)

write_files(
    df_HEDIS_MESR_GAP,
    f"{adls_path}/verified/K_HEDIS_MESR_GAP.dat",
    delimiter="^",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

df_Lnk_MESR_GAP_DPLY_In_deduped = dedup_sort(
    df_Lnk_MESR_GAP_DPLY_In,
    [
        "HEDIS_MBR_BUCKET_ID",
        "HEDIS_MESR_ABBR_ID",
        "HEDIS_MESR_GAP_DPLY_MOD_EFF_DT",
        "HEDIS_MESR_GAP_DPLY_MOD_TERM_DT",
        "HEDIS_MESR_GAP_MOD_CAT_ID",
        "HEDIS_MESR_GAP_MOD_DPLY_TX",
        "HEDIS_MESR_GAP_MOD_PRTY_NO",
        "HEDIS_MESR_GAP_MOD_SCRIPT_TX",
        "HEDIS_MESR_NM",
        "HEDIS_SUB_MESR_NM",
        "MOD_ID",
        "SRC_SYS_CD",
        "HEDIS_MESR_GAP_INVRT_IN"
    ],
    [
        ("HEDIS_MBR_BUCKET_ID","A"),
        ("HEDIS_MESR_ABBR_ID","A"),
        ("HEDIS_MESR_GAP_DPLY_MOD_EFF_DT","A"),
        ("HEDIS_MESR_GAP_DPLY_MOD_TERM_DT","A"),
        ("HEDIS_MESR_GAP_MOD_CAT_ID","A"),
        ("HEDIS_MESR_GAP_MOD_DPLY_TX","A"),
        ("HEDIS_MESR_GAP_MOD_PRTY_NO","A"),
        ("HEDIS_MESR_GAP_MOD_SCRIPT_TX","A"),
        ("HEDIS_MESR_NM","A"),
        ("HEDIS_SUB_MESR_NM","A"),
        ("MOD_ID","A"),
        ("SRC_SYS_CD","A"),
        ("HEDIS_MESR_GAP_INVRT_IN","A")
    ]
)

df_HEDIS_MESR_GAP_DPLY = df_Lnk_MESR_GAP_DPLY_In_deduped.select(
    col("HEDIS_MESR_NM"),
    col("HEDIS_SUB_MESR_NM"),
    col("HEDIS_MBR_BUCKET_ID"),
    col("MOD_ID"),
    col("HEDIS_MESR_GAP_DPLY_MOD_EFF_DT"),
    col("SRC_SYS_CD"),
    col("HEDIS_MESR_GAP_DPLY_MOD_TERM_DT"),
    col("HEDIS_MESR_GAP_MOD_PRTY_NO"),
    col("HEDIS_MESR_ABBR_ID"),
    col("HEDIS_MESR_GAP_MOD_CAT_ID"),
    col("HEDIS_MESR_GAP_MOD_DPLY_TX"),
    col("HEDIS_MESR_GAP_MOD_SCRIPT_TX"),
    col("HEDIS_MESR_MOD_KEY_ID"),
    when(col("HEDIS_MESR_GAP_INVRT_IN").isNotNull(), col("HEDIS_MESR_GAP_INVRT_IN")).otherwise(lit(" ")).alias("HEDIS_MESR_GAP_INVRT_IN")
)

df_HEDIS_MESR_GAP_DPLY = df_HEDIS_MESR_GAP_DPLY.withColumn(
    "HEDIS_MESR_GAP_INVRT_IN",
    col("HEDIS_MESR_GAP_INVRT_IN").rpad(1, " ")
)

write_files(
    df_HEDIS_MESR_GAP_DPLY,
    f"{adls_path}/verified/K_HEDIS_MESR_GAP_DPLY.dat",
    delimiter="^",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)