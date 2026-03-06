# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2005-2010 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC Called by:
# MAGIC                    FctsIdsWorkCompClmSeq
# MAGIC 
# MAGIC Processing:
# MAGIC                    To generate Sequential File to load EXCD Table
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer          Date              Project/Altiris #   Change Description			                          Development Project   Code Reviewer       Date Reviewed       
# MAGIC -----------------------   -------------------   ------------------------   --------------------------------------------------------------------------------------------   ---------------------------------   ----------------------------   -------------------------       
# MAGIC Kailashnath J     2017-03-15    5628                   Originally Programmed                                                            IntegrateDev2              Hugh Sisson           2017-03-17

# MAGIC Excd Rej File generated in job FctsIdsWorkCompClmLnDsalwExtr
# MAGIC Apply business logic
# MAGIC sequential File to load Excd Table
# MAGIC Assign primary surrogate key
# MAGIC Derive Excd_sk, Excd_id, Src_sys_cd_sk and default other fields.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType
from pyspark.sql.functions import col, lit, concat, expr, rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../shared_containers/PrimaryKey/ExcdPkey
# COMMAND ----------

CurrRunCycle = get_widget_value("CurrRunCycle","")
CurTimestamp = get_widget_value("CurTimestamp","")
SrcSysCdSk = get_widget_value("SrcSysCdSk","")

schema_Seq_Excd_Rej = StructType([
    StructField("EXCD_ID", StringType(), False),
    StructField("CLCL_ID", StringType(), False),
    StructField("CDML_SEQ_NO", IntegerType(), False),
    StructField("CDMD_TYPE", StringType(), False),
    StructField("CDMD_DISALL_AMT", DecimalType(38, 10), False),
    StructField("TRGT_CD", StringType(), False),
    StructField("CD_MPPNG_SK", IntegerType(), True),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), True)
])

df_Seq_Excd_Rej = (
    spark.read.format("csv")
    .option("header", "false")
    .option("quote", "000")
    .option("sep", ",")
    .schema(schema_Seq_Excd_Rej)
    .load(f"{adls_path_publish}/external/IDS_EXCD_REJ.dat")
)

df_business_logic_out = df_Seq_Excd_Rej.select(
    lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    lit("I").alias("INSRT_UPDT_CD"),
    lit("N").alias("DISCARD_IN"),
    lit("Y").alias("PASS_THRU_IN"),
    current_timestamp().alias("FIRST_RECYC_DT"),
    lit(0).alias("ERR_CT"),
    lit(0).alias("RECYCLE_CT"),
    lit("FACETS").alias("SRC_SYS_CD"),
    concat(lit("FACETS"), lit(";"), trim(col("EXCD_ID"))).alias("PRI_KEY_STRING"),
    lit(0).alias("EXCD_SK"),
    trim(col("EXCD_ID")).alias("EXCD_ID"),
    lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    expr("space(5)").alias("EXCD_HC_ADJ_CD"),
    expr("space(1)").alias("EXCD_PT_LIAB_IND"),
    lit("NA").alias("EXCD_PROV_ADJ_CD"),
    lit("NA").alias("EXCD_REMIT_REMARK_CD"),
    expr("space(1)").alias("EXCD_STS"),
    expr("space(1)").alias("EXCD_TYPE"),
    expr("space(1)").alias("EXCD_LONG_TX1"),
    expr("space(1)").alias("EXCD_LONG_TX2"),
    expr("space(1)").alias("EXCD_SH_TX")
)

params_ExcdPkey = {
    "CurrRunCycle": CurrRunCycle
}
df_ExcdPkeyOut = ExcdPkey(df_business_logic_out, params_ExcdPkey)

df_Trans_Excd = df_ExcdPkeyOut.select(
    col("EXCD_SK").alias("EXCD_SK"),
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    col("EXCD_ID").alias("EXCD_ID"),
    lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("EXCD_HLTHCARE_ADJ_RSN_CD_SK"),
    lit(0).alias("EXCD_LIAB_CD_SK"),
    lit(0).alias("EXCD_HIPAA_PROV_ADJ_CD_SK"),
    lit(0).alias("EXCD_HIPAA_REMIT_REMARK_CD_SK"),
    lit(0).alias("EXCD_STTUS_CD_SK"),
    lit(0).alias("EXCD_TYP_CD_SK"),
    lit("Unknown - Created in Claim Line Disallow").alias("EXCD_LONG_TX1"),
    lit("Unknown - Created in Claim Line Disallow").alias("EXCD_LONG_TX2"),
    lit("Unknown - Created in Claim Line Disallow").alias("EXCD_SH_TX")
)

df_Hf_Excd_Cd_dedup = dedup_sort(df_Trans_Excd, ["EXCD_SK"], [])

df_final = df_Hf_Excd_Cd_dedup.select(
    col("EXCD_SK"),
    col("SRC_SYS_CD_SK"),
    col("EXCD_ID"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("EXCD_HLTHCARE_ADJ_RSN_CD_SK"),
    col("EXCD_LIAB_CD_SK"),
    col("EXCD_HIPAA_PROV_ADJ_CD_SK"),
    col("EXCD_HIPAA_REMIT_REMARK_CD_SK"),
    col("EXCD_STTUS_CD_SK"),
    col("EXCD_TYP_CD_SK"),
    rpad(col("EXCD_LONG_TX1"), <...>, " ").alias("EXCD_LONG_TX1"),
    rpad(col("EXCD_LONG_TX2"), <...>, " ").alias("EXCD_LONG_TX2"),
    rpad(col("EXCD_SH_TX"), <...>, " ").alias("EXCD_SH_TX")
)

write_files(
    df_final,
    f"{adls_path}/load/WorkComp_EXCD.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)