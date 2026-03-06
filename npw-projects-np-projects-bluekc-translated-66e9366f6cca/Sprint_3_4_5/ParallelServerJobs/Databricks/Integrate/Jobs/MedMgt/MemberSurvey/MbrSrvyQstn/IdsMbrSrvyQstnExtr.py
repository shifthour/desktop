# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2011 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  F2FMbrSrvyQstnAnswrExtrSeq, 
# MAGIC                       NDBHMbrSrvyQstnAnswrExtrSeq, 
# MAGIC                       AlineoMbrSrvyQstnExtrSeq, 
# MAGIC                       HRAMbrSrvyQstnAnswrExtrSeq
# MAGIC                
# MAGIC 
# MAGIC PROCESSING: Common Extract job for the MBR_SRVY_QSTN table in IDS. The job processes landing files generated from the Face2Face, NDBH and Healthways File validation jobs and creates the key file to be loaded into IDS MBR_SRVY_QSTN table
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 \(9)Project/Altiris #\(9)Change Description\(9)\(9)\(9)\(9)Development Project\(9)Code Reviewer\(9)Date Reviewed       
# MAGIC ------------------              --------------------     \(9)------------------------\(9)-----------------------------------------------------------------------\(9)--------------------------------\(9)-------------------------------\(9)----------------------------       
# MAGIC Bhoomi Dasari        2011-04-06               4673                       Original Programming                                                IntegrateWrhsDevl                  SAndrew                 2011-04-20
# MAGIC Kalyan Neelam       2012-09-19               4830 & 4735           Added MBR_SRVY_QSTN_FLTR lookup                IntegrateNewDevl                   Bhoomi Dasari        2012-09-25
# MAGIC                                                                                               Also Added new column on end MBR_SRVY_QSTN_RSPN_STORED_IN            Bhoomi Dasari         10/20/2014
# MAGIC Kalyan Neelam       2014-10-15               TFS 9558                Added balancing snap shot file                                IntegrateNewDevl

# MAGIC MBR_SRVY_QSTN Common Extract Job
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
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
SrcSysCd = get_widget_value('SrcSysCd','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
CurrDate = get_widget_value('CurrDate','')
InFile = get_widget_value('InFile','')
OutFile = get_widget_value('OutFile','')

schema_Qstn_Common_File = StructType([
    StructField("MBR_SRVY_QSTN_SK", IntegerType(), False),
    StructField("SRC_SYS_CD", IntegerType(), False),
    StructField("MBR_SRVY_TYP_CD", StringType(), False),
    StructField("QSTN_CD_TX", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("MBR_SRVY_SK", IntegerType(), True),
    StructField("MBR_SRVY_TYP_CD_SK", IntegerType(), False),
    StructField("EFF_YR_MO_SK", StringType(), False),
    StructField("TERM_YR_MO_SK", StringType(), False),
    StructField("ANSWER_DTYP_TX", StringType(), True),
    StructField("MBR_SRVY_QSTN_ANSWER_TYP_TX", StringType(), False),
    StructField("QSTN_TX", StringType(), True),
    StructField("MBR_SRVY_TYP_CD_SRC_CD", StringType(), False)
])

df_Qstn_Common_File = (
    spark.read
    .option("header", "false")
    .option("sep", ",")
    .option("quote", "\"")
    .schema(schema_Qstn_Common_File)
    .csv(f"{adls_path_raw}/landing/{InFile}")
)

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
extract_query_MBR_SRVY_QSTN_FLTR = (
    f"SELECT FLTR.MBR_SRVY_TYP_CD, "
    f"FLTR.MBR_SRVY_QSTN_CD_TX, "
    f"FLTR.INCLD_RSPN_IN "
    f"FROM {IDSOwner}.MBR_SRVY_QSTN_FLTR FLTR, {IDSOwner}.CD_MPPNG CD "
    f"WHERE FLTR.SRC_SYS_CD_SK = CD.CD_MPPNG_SK "
    f"AND CD.TRGT_CD = '{SrcSysCd}'"
)
df_MBR_SRVY_QSTN_FLTR = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_MBR_SRVY_QSTN_FLTR)
    .load()
)

df_hf_mbrsrvyqstn_rspnind_fltrlkup = df_MBR_SRVY_QSTN_FLTR.dropDuplicates(["MBR_SRVY_TYP_CD", "MBR_SRVY_QSTN_CD_TX"])

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
query_hf_mbr_srvy_qstn_lkup = (
    "SELECT SRC_SYS_CD, MBR_SRVY_TYP_CD, QSTN_CD_TX, CRT_RUN_CYC_EXCTN_SK, MBR_SRVY_QSTN_SK "
    "FROM dummy_hf_mbr_srvy_qstn"
)
df_hf_mbr_srvy_qstn_lkup = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_hf_mbr_srvy_qstn_lkup)
    .load()
)

df_lookup = df_Qstn_Common_File.alias("Extract").join(
    df_hf_mbrsrvyqstn_rspnind_fltrlkup.alias("Fltr_lkup"),
    (
        (F.col("Extract.MBR_SRVY_TYP_CD") == F.col("Fltr_lkup.MBR_SRVY_TYP_CD"))
        & (F.col("Extract.QSTN_CD_TX") == F.col("Fltr_lkup.MBR_SRVY_QSTN_CD_TX"))
    ),
    "left"
)

df_lookUp_Transform = df_lookup.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    F.lit(CurrDate).alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    trim(F.col("Extract.SRC_SYS_CD").cast(StringType())).alias("SRC_SYS_CD"),
    F.concat(trim(F.col("Extract.SRC_SYS_CD").cast(StringType())), F.lit(";"), F.col("Extract.MBR_SRVY_TYP_CD")).alias("PRI_KEY_STRING"),
    F.lit(0).alias("MBR_SRVY_QSTN_SK"),
    F.lit(0).alias("SRC_SYS_CD_SK"),
    F.col("Extract.MBR_SRVY_TYP_CD").alias("MBR_SRVY_TYP_CD"),
    F.col("Extract.QSTN_CD_TX").alias("QSTN_CD_TX"),
    F.lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("Extract.MBR_SRVY_SK").alias("MBR_SRVY_SK"),
    F.lit(0).alias("MBR_SRVY_TYP_CD_SK"),
    F.when(
        F.col("Extract.EFF_YR_MO_SK").isNull() | (F.length(F.col("Extract.EFF_YR_MO_SK")) == 0),
        F.lit("175301")
    ).otherwise(F.col("Extract.EFF_YR_MO_SK")).alias("EFF_YR_MO_SK"),
    F.when(
        F.col("Extract.TERM_YR_MO_SK").isNull() | (F.length(F.col("Extract.TERM_YR_MO_SK")) == 0),
        F.lit("219912")
    ).otherwise(F.col("Extract.TERM_YR_MO_SK")).alias("TERM_YR_MO_SK"),
    F.col("Extract.ANSWER_DTYP_TX").alias("ANSWER_DTYP_TX"),
    F.col("Extract.MBR_SRVY_QSTN_ANSWER_TYP_TX").alias("MBR_SRVY_QSTN_ANSWER_TYP_TX"),
    F.when(F.length(F.col("Extract.QSTN_TX")) > 256, F.col("Extract.QSTN_TX").substr(1, 256))
     .otherwise(F.col("Extract.QSTN_TX")).alias("QSTN_TX"),
    F.col("Extract.MBR_SRVY_TYP_CD_SRC_CD").alias("MBR_SRVY_TYP_CD_SRC_CD"),
    F.when(
        F.col("Fltr_lkup.INCLD_RSPN_IN").isNull()
        | (F.length(F.trim(F.col("Fltr_lkup.INCLD_RSPN_IN"))) == 0),
        F.when(F.col("Extract.SRC_SYS_CD") == F.lit("ALINEO"), F.lit("N")).otherwise(F.lit("Y"))
    ).otherwise(F.col("Fltr_lkup.INCLD_RSPN_IN")).alias("MBR_SRVY_QSTN_RSPN_STORED_IN")
)

df_lookUp_Snapshot = df_lookup.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("Extract.MBR_SRVY_TYP_CD").alias("MBR_SRVY_TYP_CD"),
    F.col("Extract.QSTN_CD_TX").alias("QSTN_CD_TX")
)

write_files(
    df_lookUp_Snapshot.select("SRC_SYS_CD_SK", "MBR_SRVY_TYP_CD", "QSTN_CD_TX"),
    f"{adls_path}/load/B_MBR_SRVY_QSTN.{SrcSysCd}.dat",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)

df_pkey_input = df_lookUp_Transform.alias("Transform").join(
    df_hf_mbr_srvy_qstn_lkup.alias("lkup"),
    (
        (F.col("Transform.SRC_SYS_CD") == F.col("lkup.SRC_SYS_CD"))
        & (F.col("Transform.MBR_SRVY_TYP_CD") == F.col("lkup.MBR_SRVY_TYP_CD"))
        & (F.col("Transform.QSTN_CD_TX") == F.col("lkup.QSTN_CD_TX"))
    ),
    "left"
)

df_enriched = (
    df_pkey_input
    .withColumn(
        "MBR_SRVY_QSTN_SK",
        F.when(F.col("lkup.MBR_SRVY_QSTN_SK").isNull(), F.lit(None)).otherwise(F.col("lkup.MBR_SRVY_QSTN_SK"))
    )
)

df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"MBR_SRVY_QSTN_SK",<schema>,<secret_name>)

df_enriched = df_enriched.withColumn(
    "NewCrtRunCycExtcnSk",
    F.when(F.col("lkup.MBR_SRVY_QSTN_SK").isNull(), F.lit(CurrRunCycle)).otherwise(F.col("lkup.CRT_RUN_CYC_EXCTN_SK"))
)

df_pkey_Key = df_enriched.select(
    F.col("Transform.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("Transform.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("Transform.DISCARD_IN").alias("DISCARD_IN"),
    F.col("Transform.PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("Transform.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("Transform.ERR_CT").alias("ERR_CT"),
    F.col("Transform.RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("Transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Transform.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("MBR_SRVY_QSTN_SK").alias("MBR_SRVY_QSTN_SK"),
    F.col("Transform.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("Transform.MBR_SRVY_TYP_CD").alias("MBR_SRVY_TYP_CD"),
    F.col("Transform.QSTN_CD_TX").alias("QSTN_CD_TX"),
    F.col("NewCrtRunCycExtcnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("Transform.MBR_SRVY_SK").alias("MBR_SRVY_SK"),
    F.col("Transform.MBR_SRVY_TYP_CD_SK").alias("MBR_SRVY_TYP_CD_SK"),
    F.col("Transform.EFF_YR_MO_SK").alias("EFF_YR_MO_SK"),
    F.col("Transform.TERM_YR_MO_SK").alias("TERM_YR_MO_SK"),
    F.col("Transform.ANSWER_DTYP_TX").alias("ANSWER_DTYP_TX"),
    F.col("Transform.MBR_SRVY_QSTN_ANSWER_TYP_TX").alias("MBR_SRVY_QSTN_ANSWER_TYP_TX"),
    F.col("Transform.QSTN_TX").alias("QSTN_TX"),
    F.col("Transform.MBR_SRVY_TYP_CD_SRC_CD").alias("MBR_SRVY_TYP_CD_SRC_CD"),
    F.col("Transform.MBR_SRVY_QSTN_RSPN_STORED_IN").alias("MBR_SRVY_QSTN_RSPN_STORED_IN")
)

df_updt = (
    df_enriched
    .filter(F.col("lkup.MBR_SRVY_QSTN_SK").isNull())
    .select(
        F.col("Transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("Transform.MBR_SRVY_TYP_CD").alias("MBR_SRVY_TYP_CD"),
        F.col("Transform.QSTN_CD_TX").alias("QSTN_CD_TX"),
        F.lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("MBR_SRVY_QSTN_SK").alias("MBR_SRVY_QSTN_SK")
    )
)

df_MbrSrvyQstn = df_pkey_Key.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("MBR_SRVY_QSTN_SK"),
    F.col("SRC_SYS_CD_SK"),
    F.rpad(F.col("MBR_SRVY_TYP_CD"), 100, " ").alias("MBR_SRVY_TYP_CD"),
    F.rpad(F.col("QSTN_CD_TX"), 100, " ").alias("QSTN_CD_TX"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("MBR_SRVY_SK"),
    F.col("MBR_SRVY_TYP_CD_SK"),
    F.rpad(F.col("EFF_YR_MO_SK"), 6, " ").alias("EFF_YR_MO_SK"),
    F.rpad(F.col("TERM_YR_MO_SK"), 6, " ").alias("TERM_YR_MO_SK"),
    F.rpad(F.col("ANSWER_DTYP_TX"), 100, " ").alias("ANSWER_DTYP_TX"),
    F.rpad(F.col("MBR_SRVY_QSTN_ANSWER_TYP_TX"), 100, " ").alias("MBR_SRVY_QSTN_ANSWER_TYP_TX"),
    F.rpad(F.col("QSTN_TX"), 100, " ").alias("QSTN_TX"),
    F.rpad(F.col("MBR_SRVY_TYP_CD_SRC_CD"), 100, " ").alias("MBR_SRVY_TYP_CD_SRC_CD"),
    F.rpad(F.col("MBR_SRVY_QSTN_RSPN_STORED_IN"), 1, " ").alias("MBR_SRVY_QSTN_RSPN_STORED_IN")
)

write_files(
    df_MbrSrvyQstn,
    f"{adls_path}/key/{OutFile}",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)

if df_updt.count() > 0:
    temp_table = "STAGING.IdsMbrSrvyQstnExtr_hf_mbr_srvy_qstn_updt_temp"
    execute_dml(f"DROP TABLE IF EXISTS {temp_table}", jdbc_url, jdbc_props)
    df_updt.write.jdbc(jdbc_url, temp_table, mode="overwrite", properties=jdbc_props)
    merge_sql = (
        f"MERGE dummy_hf_mbr_srvy_qstn AS T "
        f"USING {temp_table} AS S "
        f"ON (T.SRC_SYS_CD=S.SRC_SYS_CD AND T.MBR_SRVY_TYP_CD=S.MBR_SRVY_TYP_CD AND T.QSTN_CD_TX=S.QSTN_CD_TX) "
        f"WHEN MATCHED THEN UPDATE SET "
        f"T.CRT_RUN_CYC_EXCTN_SK=S.CRT_RUN_CYC_EXCTN_SK, "
        f"T.MBR_SRVY_QSTN_SK=S.MBR_SRVY_QSTN_SK "
        f"WHEN NOT MATCHED THEN INSERT (SRC_SYS_CD, MBR_SRVY_TYP_CD, QSTN_CD_TX, CRT_RUN_CYC_EXCTN_SK, MBR_SRVY_QSTN_SK) "
        f"VALUES (S.SRC_SYS_CD, S.MBR_SRVY_TYP_CD, S.QSTN_CD_TX, S.CRT_RUN_CYC_EXCTN_SK, S.MBR_SRVY_QSTN_SK);"
    )
    execute_dml(merge_sql, jdbc_url, jdbc_props)