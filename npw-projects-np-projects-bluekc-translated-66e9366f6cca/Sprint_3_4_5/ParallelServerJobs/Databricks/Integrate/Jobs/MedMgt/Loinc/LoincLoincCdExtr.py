# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 01/30/08 05:00:10 Batch  14640_18019 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_2 10/31/07 13:54:42 Batch  14549_50100 PROMOTE bckcetl ids20 dsadm bls for on
# MAGIC ^1_2 10/31/07 13:39:50 Batch  14549_49193 INIT bckcett testIDS30 dsadm bls for on
# MAGIC ^1_1 10/30/07 07:53:48 Batch  14548_28437 PROMOTE bckcett testIDS30 u03651 steffy
# MAGIC ^1_1 10/30/07 07:44:34 Batch  14548_27877 INIT bckcett devlIDS30 u03651 steffy
# MAGIC ^1_1 05/26/06 08:11:36 Batch  14026_29498 INIT bckcett devlIDS30 u03651 steffy
# MAGIC ^1_2 05/19/06 12:49:55 Batch  14019_46199 INIT bckcett devlIDS30 u03651 steffy
# MAGIC ^1_1 05/12/06 15:18:48 Batch  14012_55131 INIT bckcett devlIDS30 u03651 steffy
# MAGIC 
# MAGIC 
# MAGIC COPYRIGHT 2006 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:    LoincLoincCdExtr
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:    Pulls data from Loinc.txt to a landing file for the IDS
# MAGIC       
# MAGIC 
# MAGIC INPUTS:	Loinc.txt
# MAGIC   
# MAGIC 
# MAGIC HASH FILES:  hf_loinc
# MAGIC 
# MAGIC 
# MAGIC TRANSFORMS:  STRIP.FIELD
# MAGIC                             FORMAT.DATE
# MAGIC 
# MAGIC 
# MAGIC PROCESSING:  Output file is created with a temp. name.  File renamed in job control
# MAGIC   
# MAGIC 
# MAGIC OUTPUTS:    Sequential file 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Date                 Developer                Change Description
# MAGIC ------------------      ----------------------------     --------------------------------------------------------------------------------------------------------
# MAGIC 2006-04-06      Suzanne Saylor         Original Programming.
# MAGIC 2006-05-19      Steph Goddard          truncated descriptions to 255
# MAGIC 
# MAGIC 
# MAGIC Developer                          Date                    Project/Altiris #                     Change Description                                                       Development Project               Code Reviewer                Date Reviewed
# MAGIC ----------------------------------      -------------------      -----------------------------------           ---------------------------------------------------------                                 ----------------------------------              -----------------------------             -------------------------
# MAGIC    Parik                               08/22/2007              3264                            Added Balancing process to the overall job that takes           devlIDS30                              Steph Goddard              9/14/07
# MAGIC                                                                                                               a snapshot of the source data

# MAGIC Extract Loinc Data
# MAGIC Apply business logic
# MAGIC Writing Sequential File to ../key
# MAGIC Strip Carriage Return, Line Feed, and  Tab
# MAGIC 
# MAGIC Use STRIP.FIELD function on each character column coming in
# MAGIC Assign primary surrogate key
# MAGIC Balancing snapshot of source file
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


RunID = get_widget_value('RunID','')
CurrRunCycle = get_widget_value('CurrRunCycle','4')
CurrDate = get_widget_value('CurrDate','')

jdbc_url, jdbc_props = get_db_config(<...>)

df_hf_lkup = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", "SELECT LOINC_CD, CRT_RUN_CYC_EXCTN_SK, LOINC_CD_SK FROM dummy_hf_loinc")
    .load()
)

schema_LoincData = StructType([
    StructField("LOINC_NUM", StringType(), nullable=False),
    StructField("COMPONENT", StringType(), nullable=False),
    StructField("RELAT_NMS", StringType(), nullable=False),
    StructField("SHORTNAME", StringType(), nullable=False)
])

df_LoincData = (
    spark.read.format("csv")
    .option("header", True)
    .option("inferSchema", False)
    .option("sep", ",")
    .option("quote", '"')
    .schema(schema_LoincData)
    .load(f"{adls_path_raw}/landing/Loinc_format.dat")
)

df_StripField = (
    df_LoincData
    .withColumn("LOINC_NUM", trim(F.regexp_replace(F.col("LOINC_NUM"), "[\r\n\t]", "")))
    .withColumn("COMPONENT", trim(F.regexp_replace(F.col("COMPONENT"), "[\r\n\t]", "")))
    .withColumn("RELATEDNAMES2_NMS", trim(F.regexp_replace(F.substring(F.col("RELAT_NMS"), 1, 255), "[\r\n\t]", "")))
    .withColumn("SHORTNAME", trim(F.regexp_replace(F.col("SHORTNAME"), "[\r\n\t]", "")))
)

df_BusinessRules = df_StripField.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    F.lit(CurrDate).alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit("LOINC").alias("SRC_SYS_CD"),
    F.concat(F.lit("LOINC"), F.lit(";"), F.col("LOINC_NUM")).alias("PRI_KEY_STRING"),
    F.col("LOINC_NUM").alias("LOINC_CD"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.when(F.col("RELATEDNAMES2_NMS").isNull(), F.col("COMPONENT")).otherwise(F.substring(F.col("RELATEDNAMES2_NMS"), 1, 255)).alias("LOINC_CD_REL_NM"),
    F.when(
        F.col("SHORTNAME").isNull() & F.col("RELATEDNAMES2_NMS").isNull(),
        F.substring(F.col("COMPONENT"), 1, 70)
    )
    .when(
        F.col("SHORTNAME").isNull(),
        F.substring(F.col("RELATEDNAMES2_NMS"), 1, 70)
    )
    .otherwise(F.substring(F.col("SHORTNAME"), 1, 70)).alias("LOINC_CD_SH_NM")
)

df_Joined = (
    df_BusinessRules.alias("Transform")
    .join(
        df_hf_lkup.alias("lkup"),
        on=[F.col("Transform.LOINC_CD") == F.col("lkup.LOINC_CD")],
        how="left"
    )
    .select(
        F.col("Transform.JOB_EXCTN_RCRD_ERR_SK"),
        F.col("Transform.INSRT_UPDT_CD"),
        F.col("Transform.DISCARD_IN"),
        F.col("Transform.PASS_THRU_IN"),
        F.col("Transform.FIRST_RECYC_DT"),
        F.col("Transform.ERR_CT"),
        F.col("Transform.RECYCLE_CT"),
        F.col("Transform.SRC_SYS_CD"),
        F.col("Transform.PRI_KEY_STRING"),
        F.col("Transform.LOINC_CD"),
        F.col("Transform.CRT_RUN_CYC_EXCTN_SK"),
        F.col("Transform.LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("Transform.LOINC_CD_REL_NM"),
        F.col("Transform.LOINC_CD_SH_NM"),
        F.col("lkup.LOINC_CD").alias("lkup_LOINC_CD"),
        F.col("lkup.CRT_RUN_CYC_EXCTN_SK").alias("lkup_CRT_RUN_CYC_EXCTN_SK"),
        F.col("lkup.LOINC_CD_SK").alias("lkup_LOINC_CD_SK")
    )
)

df_enriched = (
    df_Joined
    .withColumn(
        "LOINC_CD_SK",
        F.when(F.col("lkup_LOINC_CD_SK").isNull(), F.lit(None).cast(IntegerType()))
         .otherwise(F.col("lkup_LOINC_CD_SK"))
    )
    .withColumn(
        "NewCrtRunCycExtcnSk",
        F.when(F.col("lkup_LOINC_CD_SK").isNull(), F.lit(CurrRunCycle).cast(IntegerType()))
         .otherwise(F.col("lkup_CRT_RUN_CYC_EXCTN_SK"))
    )
)

df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"LOINC_CD_SK",<schema>,<secret_name>)

df_Key = df_enriched.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD"),
    F.col("DISCARD_IN"),
    F.col("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("LOINC_CD_SK"),
    F.col("LOINC_CD"),
    F.col("NewCrtRunCycExtcnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(CurrRunCycle).cast(IntegerType()).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("LOINC_CD_REL_NM"),
    F.col("LOINC_CD_SH_NM")
)

df_Updt = df_enriched.filter(F.col("lkup_LOINC_CD_SK").isNull()).select(
    F.col("LOINC_CD").alias("LOINC_CD"),
    F.col("NewCrtRunCycExtcnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LOINC_CD_SK").alias("LOINC_CD_SK")
)

execute_dml(f"DROP TABLE IF EXISTS STAGING.LoincLoincCdExtr_hf_write_temp", jdbc_url, jdbc_props)

(
    df_Updt.write.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .mode("overwrite")
    .option("dbtable", "STAGING.LoincLoincCdExtr_hf_write_temp")
    .save()
)

merge_sql = """
MERGE dummy_hf_loinc as T
USING STAGING.LoincLoincCdExtr_hf_write_temp as S
ON T.LOINC_CD = S.LOINC_CD
WHEN MATCHED THEN
  UPDATE SET
    T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK,
    T.LOINC_CD_SK = S.LOINC_CD_SK
WHEN NOT MATCHED THEN
  INSERT (LOINC_CD, CRT_RUN_CYC_EXCTN_SK, LOINC_CD_SK)
  VALUES (S.LOINC_CD, S.CRT_RUN_CYC_EXCTN_SK, S.LOINC_CD_SK);
"""
execute_dml(merge_sql, jdbc_url, jdbc_props)

df_IdsLoincCdExtr = df_Key.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("LOINC_CD_SK"),
    F.col("LOINC_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("LOINC_CD_REL_NM"),
    F.col("LOINC_CD_SH_NM")
)

write_files(
    df_IdsLoincCdExtr,
    f"{adls_path}/key/LoincLoincCdExtr.LoincCd.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

df_Loinc_Source = (
    spark.read.format("csv")
    .option("header", True)
    .option("inferSchema", False)
    .option("sep", ",")
    .option("quote", '"')
    .schema(schema_LoincData)
    .load(f"{adls_path_raw}/landing/Loinc_format.dat")
)

df_Transform = df_Loinc_Source.withColumn(
    "LOINC_CD",
    trim(F.regexp_replace(F.col("LOINC_NUM"), "[\r\n\t]", ""))
)

df_BLoincCd = df_Transform.select(F.col("LOINC_CD"))

write_files(
    df_BLoincCd,
    f"{adls_path}/load/B_LOINC_CD.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)