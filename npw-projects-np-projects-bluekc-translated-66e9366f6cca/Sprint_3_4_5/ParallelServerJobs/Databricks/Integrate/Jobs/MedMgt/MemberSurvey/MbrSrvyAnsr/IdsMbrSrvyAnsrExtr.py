# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2011 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  F2FMbrSrvyQstnAnswrExtrSeq
# MAGIC                
# MAGIC 
# MAGIC PROCESSING: Common Extract job for the MBR_SRVY_ANSWER table in IDS. The job processes landing files generated from the Face2Face, NDBH and Healthways File validation jobs and creates the key file to be loaded into IDS MBR_SRVY_ANSWER table
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 \(9)Project/Altiris #\(9)Change Description\(9)\(9)\(9)\(9)Development Project\(9)Code Reviewer\(9)Date Reviewed       
# MAGIC ------------------              --------------------     \(9)------------------------\(9)-----------------------------------------------------------------------\(9)--------------------------------\(9)-------------------------------\(9)----------------------------       
# MAGIC Bhoomi Dasari        2011-04-06               4673                       Original Programming                                                IntegrateWrhsDevl                    SAndrew                2011-04-20
# MAGIC Kalyan Neelam        2014-10-15               TFS 9558                Added balancing snap shot file                                IntegrateNewDevl                   Bhoomi Dasari         10/20/2014
# MAGIC 
# MAGIC Reddy Sanam         2022-06-15              US492429               Changed column Length from 100                            IntegrateDev2                          Goutham Kalidindi    2022-07-14
# MAGIC                                                                                                to 255 for "MBR_SRVY_ANSWER_CD_TX"
# MAGIC                                                                                                field

# MAGIC MBR_SRVY_ANSWER Common Extract Job
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import col, lit, trim as pyspark_trim, when, isnull, concat, rpad, concat_ws
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


SrcSysCd = get_widget_value('SrcSysCd','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','1000')
CurrRunCycle = get_widget_value('CurrRunCycle','100')
CurrDate = get_widget_value('CurrDate','2011-04-06')
InFile = get_widget_value('InFile','')
OutFile = get_widget_value('OutFile','')
ids_secret_name = get_widget_value('ids_secret_name','')

schema_Srvy_Common_File = StructType([
    StructField("MBR_SRVY_ANSWER_SK", IntegerType(), False),
    StructField("SRC_SYS_CD", IntegerType(), False),
    StructField("MBR_SRVY_TYP_CD", StringType(), False),
    StructField("MBR_SRVY_QSTN_CD_TX", StringType(), True),
    StructField("MBR_SRVY_ANSWER_CD_TX", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("MBR_SRVY_QSTN_SK", IntegerType(), False)
])

df_Srvy_Common_File = (
    spark.read.format("csv")
    .option("header", "false")
    .option("sep", ",")
    .option("quote", "\"")
    .schema(schema_Srvy_Common_File)
    .load(f"{adls_path_raw}/landing/{InFile}")
)

df_BusinessRule_Transform = df_Srvy_Common_File.select(
    lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    lit("I").alias("INSRT_UPDT_CD"),
    lit("N").alias("DISCARD_IN"),
    lit("Y").alias("PASS_THRU_IN"),
    lit(CurrDate).alias("FIRST_RECYC_DT"),
    lit(0).alias("ERR_CT"),
    lit(0).alias("RECYCLE_CT"),
    pyspark_trim(col("SRC_SYS_CD").cast(StringType())).alias("SRC_SYS_CD"),
    concat(
        pyspark_trim(col("SRC_SYS_CD").cast(StringType())),
        lit(";"),
        col("MBR_SRVY_TYP_CD")
    ).alias("PRI_KEY_STRING"),
    lit(0).alias("MBR_SRVY_ANSWER_SK"),
    lit(0).alias("SRC_SYS_CD_SK"),
    col("MBR_SRVY_TYP_CD").alias("MBR_SRVY_TYP_CD"),
    col("MBR_SRVY_QSTN_CD_TX").alias("MBR_SRVY_QSTN_CD_TX"),
    col("MBR_SRVY_ANSWER_CD_TX").alias("MBR_SRVY_ANSWER_CD_TX"),
    lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("MBR_SRVY_QSTN_SK").alias("MBR_SRVY_QSTN_SK")
)

df_BusinessRule_Snapshot = df_Srvy_Common_File.select(
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    col("MBR_SRVY_TYP_CD").alias("MBR_SRVY_TYP_CD"),
    col("MBR_SRVY_QSTN_CD_TX").alias("MBR_SRVY_QSTN_CD_TX"),
    col("MBR_SRVY_ANSWER_CD_TX").alias("MBR_SRVY_ANSWER_CD_TX")
)

df_BusinessRule_Snapshot_for_write = df_BusinessRule_Snapshot.select(
    rpad(col("SRC_SYS_CD_SK").cast(StringType()),  <...>, " ").alias("SRC_SYS_CD_SK"),
    rpad(col("MBR_SRVY_TYP_CD"),                  <...>, " ").alias("MBR_SRVY_TYP_CD"),
    rpad(col("MBR_SRVY_QSTN_CD_TX"),              <...>, " ").alias("MBR_SRVY_QSTN_CD_TX"),
    rpad(col("MBR_SRVY_ANSWER_CD_TX"),            <...>, " ").alias("MBR_SRVY_ANSWER_CD_TX")
)

write_files(
    df_BusinessRule_Snapshot_for_write,
    f"{adls_path}/load/B_MBR_SRVY_ANSWER.{SrcSysCd}.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
df_hf_mbr_srvy_ansr_lkup = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        "SELECT SRC_SYS_CD, MBR_SRVY_TYP_CD, MBR_SRVY_QSTN_CD_TX, MBR_SRVY_ANSWER_CD_TX, CRT_RUN_CYC_EXCTN_SK, MBR_SRVY_ANSWER_SK FROM dummy_hf_mbr_srvy_ansr"
    )
    .load()
)

df_Pkey_preSK = (
    df_BusinessRule_Transform.alias("Transform")
    .join(
        df_hf_mbr_srvy_ansr_lkup.alias("lkup"),
        [
            col("Transform.SRC_SYS_CD") == col("lkup.SRC_SYS_CD"),
            col("Transform.MBR_SRVY_TYP_CD") == col("lkup.MBR_SRVY_TYP_CD"),
            col("Transform.MBR_SRVY_QSTN_CD_TX") == col("lkup.MBR_SRVY_QSTN_CD_TX"),
            col("Transform.MBR_SRVY_ANSWER_CD_TX") == col("lkup.MBR_SRVY_ANSWER_CD_TX")
        ],
        how="left"
    )
)

df_enriched = df_Pkey_preSK.withColumn(
    "SK",
    when(isnull(col("lkup.MBR_SRVY_ANSWER_SK")), lit(None).cast(IntegerType()))
    .otherwise(col("lkup.MBR_SRVY_ANSWER_SK"))
).withColumn(
    "NewCrtRunCycExtcnSk",
    when(isnull(col("lkup.MBR_SRVY_ANSWER_SK")), lit(CurrRunCycle).cast(IntegerType()))
    .otherwise(col("lkup.CRT_RUN_CYC_EXCTN_SK"))
)

df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"SK",<schema>,<secret_name>)

df_Pkey_Key = df_enriched.select(
    col("Transform.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    col("Transform.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    col("Transform.DISCARD_IN").alias("DISCARD_IN"),
    col("Transform.PASS_THRU_IN").alias("PASS_THRU_IN"),
    col("Transform.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    col("Transform.ERR_CT").alias("ERR_CT"),
    col("Transform.RECYCLE_CT").alias("RECYCLE_CT"),
    col("Transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("Transform.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    col("SK").alias("MBR_SRVY_ANSWER_SK"),
    col("Transform.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    col("Transform.MBR_SRVY_TYP_CD").alias("MBR_SRVY_TYP_CD"),
    col("Transform.MBR_SRVY_QSTN_CD_TX").alias("MBR_SRVY_QSTN_CD_TX"),
    col("Transform.MBR_SRVY_ANSWER_CD_TX").alias("MBR_SRVY_ANSWER_CD_TX"),
    col("NewCrtRunCycExtcnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("Transform.MBR_SRVY_QSTN_SK").alias("MBR_SRVY_QSTN_SK")
)

df_Pkey_updt = df_enriched.select(
    col("Transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("Transform.MBR_SRVY_TYP_CD").alias("MBR_SRVY_TYP_CD"),
    col("Transform.MBR_SRVY_QSTN_CD_TX").alias("MBR_SRVY_QSTN_CD_TX"),
    col("Transform.MBR_SRVY_ANSWER_CD_TX").alias("MBR_SRVY_ANSWER_CD_TX"),
    lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    col("SK").alias("MBR_SRVY_ANSWER_SK")
)

df_Pkey_Key_for_write = df_Pkey_Key.select(
    col("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    rpad(col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    rpad(col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    col("FIRST_RECYC_DT"),
    col("ERR_CT"),
    col("RECYCLE_CT"),
    rpad(col("SRC_SYS_CD"), <...>, " ").alias("SRC_SYS_CD"),
    rpad(col("PRI_KEY_STRING"), <...>, " ").alias("PRI_KEY_STRING"),
    col("MBR_SRVY_ANSWER_SK"),
    col("SRC_SYS_CD_SK"),
    rpad(col("MBR_SRVY_TYP_CD"), <...>, " ").alias("MBR_SRVY_TYP_CD"),
    rpad(col("MBR_SRVY_QSTN_CD_TX"), <...>, " ").alias("MBR_SRVY_QSTN_CD_TX"),
    rpad(col("MBR_SRVY_ANSWER_CD_TX"), <...>, " ").alias("MBR_SRVY_ANSWER_CD_TX"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("MBR_SRVY_QSTN_SK")
)

write_files(
    df_Pkey_Key_for_write,
    f"{adls_path}/key/{OutFile}",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)

drop_temp_sql = "DROP TABLE IF EXISTS STAGING.IdsMbrSrvyAnsrExtr_hf_mbr_srvy_ansr_updt_temp"
execute_dml(drop_temp_sql, jdbc_url, jdbc_props)

(
    df_Pkey_updt.write.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("dbtable", "STAGING.IdsMbrSrvyAnsrExtr_hf_mbr_srvy_ansr_updt_temp")
    .mode("overwrite")
    .save()
)

merge_sql = """
MERGE dummy_hf_mbr_srvy_ansr AS T
USING STAGING.IdsMbrSrvyAnsrExtr_hf_mbr_srvy_ansr_updt_temp AS S
ON 
    T.SRC_SYS_CD = S.SRC_SYS_CD
    AND T.MBR_SRVY_TYP_CD = S.MBR_SRVY_TYP_CD
    AND T.MBR_SRVY_QSTN_CD_TX = S.MBR_SRVY_QSTN_CD_TX
    AND T.MBR_SRVY_ANSWER_CD_TX = S.MBR_SRVY_ANSWER_CD_TX
WHEN MATCHED THEN
    UPDATE SET
        T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK,
        T.MBR_SRVY_ANSWER_SK = S.MBR_SRVY_ANSWER_SK
WHEN NOT MATCHED THEN
    INSERT (SRC_SYS_CD, MBR_SRVY_TYP_CD, MBR_SRVY_QSTN_CD_TX, MBR_SRVY_ANSWER_CD_TX, CRT_RUN_CYC_EXCTN_SK, MBR_SRVY_ANSWER_SK)
    VALUES (S.SRC_SYS_CD, S.MBR_SRVY_TYP_CD, S.MBR_SRVY_QSTN_CD_TX, S.MBR_SRVY_ANSWER_CD_TX, S.CRT_RUN_CYC_EXCTN_SK, S.MBR_SRVY_ANSWER_SK);
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)