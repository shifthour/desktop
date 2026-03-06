# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2015 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC JOB NAME:  EdgeRiskAsmtCmsMbrEnrHistLoad
# MAGIC DESCRIPTION:  This job loads  Member Enrollment History data to CMS_MBR_ENR_HIST table 
# MAGIC CALLED BY:  EdgeRiskAsmntEnrSbmsnRAExtrSeq
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                   Date                     Project/Ticket #\(9)      Change Description\(9)\(9)\(9)          Development Project\(9)     Code Reviewer\(9)       Date Reviewed       
# MAGIC -------------------------          -------------------          --------------------------            ----------------------------------------------------------------------              ---------------------------------          ------------------------          -------------------------  
# MAGIC Pooja Sunkara           2015-02-11            5125 Risk Adjustment     Original Programming                                                  EnterpriseNewDevl             Kalyan Neelam           2015-03-03
# MAGIC 
# MAGIC Raja Gummadi            2015-07-30            5125                             Added MBR_INDV_BE_KEY and                                  EnterpriseDev2                 Bhoomi Dasari            07/30/2015
# MAGIC                                                                                                       RISK_ADJ_YR columns
# MAGIC Harsha Ravuri\(9)2018-12-27\(9)5873 Risk Adjustment     Added MBR_RELSHP_CD column to file\(9)\(9)EnterpriseDev2                   Kalyan Neelam            2019-01-30
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)       AGE_MDL_TYP, AGE_YR_NO,TOT_MBR_MO_CT
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)       TOT_BILL_MO_CT
# MAGIC Harsha Ravuri\(9)2023-10-03\(9)US#597589\(9)\(9)Added below 2 new fields to source/target  \(9)EnterpriseDev2                Jeyaprasanna           2023-10-08
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)insuredMemberRace, insuredMemberEthnicity

# MAGIC File created in extract job is used to load CMS_MBR_ENR_HIST table
# MAGIC Job Name: EdgeRiskAsmtCmsMbrEnrHistLoad
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType
from pyspark.sql.functions import col, sort_array, lit, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Enterprise
# COMMAND ----------


# MAGIC %run ../../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

EDGERiskAsmntOwner = get_widget_value('EDGERiskAsmntOwner','')
edgeriskasmnt_secret_name = get_widget_value('edgeriskasmnt_secret_name','')
RunID = get_widget_value('RunID','')

schema_Seq_CMS_MBR_ENR_HIST_Extr = StructType([
    StructField("ISSUER_ID", StringType(), False),
    StructField("FILE_ID", StringType(), False),
    StructField("MBR_RCRD_ID", IntegerType(), False),
    StructField("MBR_UNIQ_KEY", IntegerType(), False),
    StructField("MBR_BRTH_DT", TimestampType(), False),
    StructField("MBR_GNDR_CD", StringType(), False),
    StructField("MBR_PRCS_STTUS_CD", StringType(), False),
    StructField("GNRTN_DTM", TimestampType(), False),
    StructField("LAST_UPDT_DTM", TimestampType(), True),
    StructField("LAST_UPDT_USER_ID", StringType(), True),
    StructField("MBR_INDV_BE_KEY", DecimalType(38,10), True),
    StructField("RISK_ADJ_YR", StringType(), False),
    StructField("AGE_MDL_TYP", StringType(), True),
    StructField("AGE_YR_NO", IntegerType(), True),
    StructField("TOT_MBR_MO_CT", DecimalType(38,10), True),
    StructField("TOT_BILL_MO_CT", DecimalType(38,10), True),
    StructField("MBR_RELSHP_CD", StringType(), True),
    StructField("MBR_RACE_CD", StringType(), True),
    StructField("MBR_ETHNCTY_CD", StringType(), True)
])

df_Seq_CMS_MBR_ENR_HIST_Extr = (
    spark.read.format("csv")
    .option("header", "false")
    .option("delimiter", ",")
    .option("quote", "\"")
    .option("escape", "\"")
    .option("nullValue", None)
    .schema(schema_Seq_CMS_MBR_ENR_HIST_Extr)
    .load(f"{adls_path}/load/CMS_MBR_ENR_HIST.{RunID}.dat")
)

df_cp_buffer = df_Seq_CMS_MBR_ENR_HIST_Extr.sort("ISSUER_ID", "FILE_ID", "MBR_RCRD_ID")

df_Odbc_CMS_MBR_ENR_HIST_Out = df_cp_buffer.select(
    "ISSUER_ID",
    "FILE_ID",
    "MBR_RCRD_ID",
    "MBR_UNIQ_KEY",
    "MBR_BRTH_DT",
    "MBR_GNDR_CD",
    "MBR_PRCS_STTUS_CD",
    "GNRTN_DTM",
    "LAST_UPDT_DTM",
    "LAST_UPDT_USER_ID",
    "MBR_INDV_BE_KEY",
    "RISK_ADJ_YR",
    "MBR_RELSHP_CD",
    "MBR_RACE_CD",
    "MBR_ETHNCTY_CD"
)

jdbc_url, jdbc_props = get_db_config(edgeriskasmnt_secret_name)
staging_table = "STAGING.EdgeRiskAsmntCmsMbrEnrHistLoad_Odbc_CMS_MBR_ENR_HIST_Out_temp"

execute_dml(f"DROP TABLE IF EXISTS {staging_table}", jdbc_url, jdbc_props)

df_Odbc_CMS_MBR_ENR_HIST_Out.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", staging_table) \
    .mode("overwrite") \
    .save()

merge_sql = f"""
MERGE INTO {EDGERiskAsmntOwner}.CMS_MBR_ENR_HIST AS T
USING {staging_table} AS S
ON T.ISSUER_ID = S.ISSUER_ID
   AND T.FILE_ID = S.FILE_ID
   AND T.MBR_RCRD_ID = S.MBR_RCRD_ID
WHEN MATCHED THEN
  UPDATE SET
    T.MBR_UNIQ_KEY = S.MBR_UNIQ_KEY,
    T.MBR_BRTH_DT = S.MBR_BRTH_DT,
    T.MBR_GNDR_CD = S.MBR_GNDR_CD,
    T.MBR_PRCS_STTUS_CD = S.MBR_PRCS_STTUS_CD,
    T.GNRTN_DTM = S.GNRTN_DTM,
    T.LAST_UPDT_DTM = S.LAST_UPDT_DTM,
    T.LAST_UPDT_USER_ID = S.LAST_UPDT_USER_ID,
    T.MBR_INDV_BE_KEY = S.MBR_INDV_BE_KEY,
    T.RISK_ADJ_YR = S.RISK_ADJ_YR,
    T.MBR_RELSHP_CD = S.MBR_RELSHP_CD,
    T.MBR_RACE_CD = S.MBR_RACE_CD,
    T.MBR_ETHNCTY_CD = S.MBR_ETHNCTY_CD
WHEN NOT MATCHED THEN
  INSERT (
    ISSUER_ID,
    FILE_ID,
    MBR_RCRD_ID,
    MBR_UNIQ_KEY,
    MBR_BRTH_DT,
    MBR_GNDR_CD,
    MBR_PRCS_STTUS_CD,
    GNRTN_DTM,
    LAST_UPDT_DTM,
    LAST_UPDT_USER_ID,
    MBR_INDV_BE_KEY,
    RISK_ADJ_YR,
    MBR_RELSHP_CD,
    MBR_RACE_CD,
    MBR_ETHNCTY_CD
  )
  VALUES (
    S.ISSUER_ID,
    S.FILE_ID,
    S.MBR_RCRD_ID,
    S.MBR_UNIQ_KEY,
    S.MBR_BRTH_DT,
    S.MBR_GNDR_CD,
    S.MBR_PRCS_STTUS_CD,
    S.GNRTN_DTM,
    S.LAST_UPDT_DTM,
    S.LAST_UPDT_USER_ID,
    S.MBR_INDV_BE_KEY,
    S.RISK_ADJ_YR,
    S.MBR_RELSHP_CD,
    S.MBR_RACE_CD,
    S.MBR_ETHNCTY_CD
  );
"""
execute_dml(merge_sql, jdbc_url, jdbc_props)

schema_seq_CMS_MBR_ENR_HIST_rej = StructType([
    StructField("ISSUER_ID", StringType(), True),
    StructField("FILE_ID", StringType(), True),
    StructField("MBR_RCRD_ID", IntegerType(), True),
    StructField("MBR_UNIQ_KEY", IntegerType(), True),
    StructField("MBR_BRTH_DT", TimestampType(), True),
    StructField("MBR_GNDR_CD", StringType(), True),
    StructField("MBR_PRCS_STTUS_CD", StringType(), True),
    StructField("GNRTN_DTM", TimestampType(), True),
    StructField("LAST_UPDT_DTM", TimestampType(), True),
    StructField("LAST_UPDT_USER_ID", StringType(), True),
    StructField("MBR_INDV_BE_KEY", DecimalType(38,10), True),
    StructField("RISK_ADJ_YR", StringType(), True),
    StructField("MBR_RELSHP_CD", StringType(), True),
    StructField("MBR_RACE_CD", StringType(), True),
    StructField("MBR_ETHNCTY_CD", StringType(), True),
    StructField("ERRORCODE", StringType(), True),
    StructField("ERRORTEXT", StringType(), True)
])

df_seq_CMS_MBR_ENR_HIST_rej_empty = spark.createDataFrame([], schema_seq_CMS_MBR_ENR_HIST_rej)

df_seq_CMS_MBR_ENR_HIST_rej_final = df_seq_CMS_MBR_ENR_HIST_rej_empty.select(
    rpad(col("ISSUER_ID"), 5, " ").alias("ISSUER_ID"),
    rpad(col("FILE_ID"), 12, " ").alias("FILE_ID"),
    col("MBR_RCRD_ID"),
    col("MBR_UNIQ_KEY"),
    col("MBR_BRTH_DT"),
    rpad(col("MBR_GNDR_CD"), <...>, " ").alias("MBR_GNDR_CD"),
    rpad(col("MBR_PRCS_STTUS_CD"), <...>, " ").alias("MBR_PRCS_STTUS_CD"),
    col("GNRTN_DTM"),
    col("LAST_UPDT_DTM"),
    rpad(col("LAST_UPDT_USER_ID"), <...>, " ").alias("LAST_UPDT_USER_ID"),
    col("MBR_INDV_BE_KEY"),
    rpad(col("RISK_ADJ_YR"), 4, " ").alias("RISK_ADJ_YR"),
    rpad(col("MBR_RELSHP_CD"), <...>, " ").alias("MBR_RELSHP_CD"),
    rpad(col("MBR_RACE_CD"), <...>, " ").alias("MBR_RACE_CD"),
    rpad(col("MBR_ETHNCTY_CD"), <...>, " ").alias("MBR_ETHNCTY_CD"),
    rpad(col("ERRORCODE"), <...>, " ").alias("ERRORCODE"),
    rpad(col("ERRORTEXT"), <...>, " ").alias("ERRORTEXT")
)

write_files(
    df_seq_CMS_MBR_ENR_HIST_rej_final,
    f"{adls_path}/load/CMS_MBR_ENR_HIST.{RunID}_Rej.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)