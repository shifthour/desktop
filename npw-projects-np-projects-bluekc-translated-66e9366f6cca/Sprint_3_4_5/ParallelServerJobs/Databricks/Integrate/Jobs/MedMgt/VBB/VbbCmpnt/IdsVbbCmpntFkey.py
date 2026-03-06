# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2013 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  
# MAGIC 
# MAGIC PROCESSING:   Assigns Foreign Keys for the IDS table VBB_CMPNT.
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 	Project/Altiris #	Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------	------------------------	-----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Raja Gummadi         2013-05-13           4963 VBB Phase III     Initial Programming                                                                      IntegrateNewDevl          Bhoomi Dasari            5/16/2013

# MAGIC Set all foreign surragote keys
# MAGIC Merge source data with default rows
# MAGIC Writing Sequential File to /load
# MAGIC Read common record format file from extract job.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


InFile = get_widget_value("InFile","")
Logging = get_widget_value("Logging","")
RunID = get_widget_value("RunID","")

schema_IdsVbbCmpntExtr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False),
    StructField("INSRT_UPDT_CD", StringType(), False),
    StructField("DISCARD_IN", StringType(), False),
    StructField("PASS_THRU_IN", StringType(), False),
    StructField("FIRST_RECYC_DT", TimestampType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", DecimalType(38,10), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PRI_KEY_STRING", StringType(), False),
    StructField("VBB_CMPNT_SK", IntegerType(), False),
    StructField("VBB_CMPNT_UNIQ_KEY", IntegerType(), False),
    StructField("SRC_SYS_CD_SK", IntegerType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("HIPL_ID", IntegerType(), False),
    StructField("INPR_FINISH_RULE", StringType(), False),
    StructField("CDVL_VALUE_FUNCTION", StringType(), False),
    StructField("INPR_COMPLIANCE_METHOD", StringType(), True),
    StructField("MBR_ACHV_VALID_ALW_IN", StringType(), False),
    StructField("VBB_CMPNT_REENR_IN", StringType(), False),
    StructField("SRC_SYS_CRT_DTM", TimestampType(), False),
    StructField("SRC_SYS_UPDT_DTM", TimestampType(), False),
    StructField("VBB_CMPNT_ACHV_LVL_CT", IntegerType(), False),
    StructField("VBB_CMPNT_FNSH_DAYS_NO", IntegerType(), False),
    StructField("VBB_CMPNT_REINST_DAYS_NO", IntegerType(), False),
    StructField("VBB_TMPLT_UNIQ_KEY", IntegerType(), False),
    StructField("VBB_CMPNT_NM", StringType(), False),
    StructField("VBB_CMPNT_TYP_NM", StringType(), False)
])

df_IdsVbbCmpntExtr = (
    spark.read.format("csv")
    .schema(schema_IdsVbbCmpntExtr)
    .option("header", "false")
    .option("quote", "\"")
    .option("sep", ",")
    .load(f"{adls_path}/key/{InFile}")
)

w = Window.orderBy(F.lit(1))

df_foreignKey = (
    df_IdsVbbCmpntExtr
    .withColumn("PassThru", F.col("PASS_THRU_IN"))
    .withColumn("svVbbPlnSk", GetFkeyVbbPln(F.col("SRC_SYS_CD"), F.col("VBB_CMPNT_SK"), F.col("HIPL_ID"), Logging))
    .withColumn("svVbbCmplncMethCdSk", GetFkeyClctnDomainCodes(
        F.col("SRC_SYS_CD"), 
        F.col("VBB_CMPNT_SK"), 
        F.lit("VBB COMPLIANCE METHOD"), 
        F.lit("IHMF CONSTITUENT"), 
        F.lit("VBB COMPLIANCE METHOD"), 
        F.lit("IDS"), 
        F.col("INPR_COMPLIANCE_METHOD"), 
        Logging
    ))
    .withColumn("svVbbCmpntFnshRuleCdSk", GetFkeyClctnDomainCodes(
        F.col("SRC_SYS_CD"),
        F.col("VBB_CMPNT_SK"),
        F.lit("VBB COMPONENT FINISH RULE"),
        F.lit("IHMF CONSTITUENT"),
        F.lit("VBB COMPONENT FINISH RULE"),
        F.lit("IDS"),
        F.col("INPR_FINISH_RULE"),
        Logging
    ))
    .withColumn("svVbbCmpntFuncCatCdSk", GetFkeyClctnDomainCodes(
        F.col("SRC_SYS_CD"),
        F.col("VBB_CMPNT_SK"),
        F.lit("VBB COMPONENT FUNCTION"),
        F.lit("IHMF CONSTITUENT"),
        F.lit("VBB COMPONENT FUNCTION CATEGORY"),
        F.lit("IDS"),
        F.col("CDVL_VALUE_FUNCTION"),
        Logging
    ))
    .withColumn("svVbbCmpntFuncCdSk", GetFkeyClctnDomainCodes(
        F.col("SRC_SYS_CD"),
        F.col("VBB_CMPNT_SK"),
        F.lit("VBB COMPONENT FUNCTION"),
        F.lit("IHMF CONSTITUENT"),
        F.lit("VBB COMPONENT FUNCTION"),
        F.lit("IDS"),
        F.col("CDVL_VALUE_FUNCTION"),
        Logging
    ))
    .withColumn("ErrCount", GetFkeyErrorCnt(F.col("VBB_CMPNT_SK")))
    .withColumn("RowNumber", F.row_number().over(w))
)

df_Fkey = df_foreignKey.filter((F.col("ErrCount") == 0) | (F.col("PassThru") == 'Y')).select(
    F.col("VBB_CMPNT_SK").alias("VBB_CMPNT_SK"),
    F.col("VBB_CMPNT_UNIQ_KEY").alias("VBB_CMPNT_UNIQ_KEY"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("svVbbPlnSk").alias("VBB_PLN_SK"),
    F.col("svVbbCmplncMethCdSk").alias("VBB_CMPLNC_METH_CD_SK"),
    F.col("svVbbCmpntFnshRuleCdSk").alias("VBB_CMPNT_FNSH_RULE_CD_SK"),
    F.col("svVbbCmpntFuncCatCdSk").alias("VBB_CMPNT_FUNC_CAT_CD_SK"),
    F.col("svVbbCmpntFuncCdSk").alias("VBB_CMPNT_FUNC_CD_SK"),
    F.col("MBR_ACHV_VALID_ALW_IN").alias("MBR_ACHV_VALID_ALW_IN"),
    F.col("VBB_CMPNT_REENR_IN").alias("VBB_CMPNT_REENR_IN"),
    F.col("SRC_SYS_CRT_DTM").alias("SRC_SYS_CRT_DTM"),
    F.col("SRC_SYS_UPDT_DTM").alias("SRC_SYS_UPDT_DTM"),
    F.col("VBB_CMPNT_ACHV_LVL_CT").alias("VBB_CMPNT_ACHV_LVL_CT"),
    F.col("VBB_CMPNT_FNSH_DAYS_NO").alias("VBB_CMPNT_FNSH_DAYS_NO"),
    F.col("VBB_CMPNT_REINST_DAYS_NO").alias("VBB_CMPNT_REINST_DAYS_NO"),
    F.col("VBB_TMPLT_UNIQ_KEY").alias("VBB_TMPLT_UNIQ_KEY"),
    F.col("VBB_CMPNT_NM").alias("VBB_CMPNT_NM"),
    F.col("VBB_CMPNT_TYP_NM").alias("VBB_CMPNT_TYP_NM")
)

df_Recycle = df_foreignKey.filter(F.col("ErrCount") > 0).select(
    GetRecycleKey(F.col("VBB_CMPNT_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("DISCARD_IN").alias("DISCARD_IN"),
    F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("ErrCount").alias("ERR_CT"),
    (F.col("RECYCLE_CT") + F.lit(1)).alias("RECYCLE_CT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("VBB_CMPNT_SK").alias("VBB_CMPNT_SK"),
    F.col("VBB_CMPNT_UNIQ_KEY").alias("VBB_CMPNT_UNIQ_KEY"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("HIPL_ID").alias("HIPL_ID"),
    F.col("INPR_FINISH_RULE").alias("INPR_FINISH_RULE"),
    F.col("CDVL_VALUE_FUNCTION").alias("CDVL_VALUE_FUNCTION"),
    F.col("INPR_COMPLIANCE_METHOD").alias("INPR_COMPLIANCE_METHOD"),
    F.col("MBR_ACHV_VALID_ALW_IN").alias("MBR_ACHV_VALID_ALW_IN"),
    F.col("VBB_CMPNT_REENR_IN").alias("VBB_CMPNT_REENR_IN"),
    F.col("SRC_SYS_CRT_DTM").alias("SRC_SYS_CRT_DTM"),
    F.col("SRC_SYS_UPDT_DTM").alias("SRC_SYS_UPDT_DTM"),
    F.col("VBB_CMPNT_ACHV_LVL_CT").alias("VBB_CMPNT_ACHV_LVL_CT"),
    F.col("VBB_CMPNT_FNSH_DAYS_NO").alias("VBB_CMPNT_FNSH_DAYS_NO"),
    F.col("VBB_CMPNT_REINST_DAYS_NO").alias("VBB_CMPNT_REINST_DAYS_NO"),
    F.col("VBB_TMPLT_UNIQ_KEY").alias("VBB_TMPLT_UNIQ_KEY"),
    F.col("VBB_CMPNT_NM").alias("VBB_CMPNT_NM"),
    F.col("VBB_CMPNT_TYP_NM").alias("VBB_CMPNT_TYP_NM")
)

df_DefaultUNK_source = df_foreignKey.filter(F.col("RowNumber") == 1).select(
    F.lit(0).alias("VBB_CMPNT_SK"),
    F.lit(0).alias("VBB_CMPNT_UNIQ_KEY"),
    F.lit(0).alias("SRC_SYS_CD_SK"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("VBB_PLN_SK"),
    F.lit(0).alias("VBB_CMPLNC_METH_CD_SK"),
    F.lit(0).alias("VBB_CMPNT_FNSH_RULE_CD_SK"),
    F.lit(0).alias("VBB_CMPNT_FUNC_CAT_CD_SK"),
    F.lit(0).alias("VBB_CMPNT_FUNC_CD_SK"),
    F.lit("U").alias("MBR_ACHV_VALID_ALW_IN"),
    F.lit("U").alias("VBB_CMPNT_REENR_IN"),
    F.lit("1753-01-01 00:00:00.000").cast(TimestampType()).alias("SRC_SYS_CRT_DTM"),
    F.lit("1753-01-01 00:00:00.000").cast(TimestampType()).alias("SRC_SYS_UPDT_DTM"),
    F.lit(0).alias("VBB_CMPNT_ACHV_LVL_CT"),
    F.lit(0).alias("VBB_CMPNT_FNSH_DAYS_NO"),
    F.lit(0).alias("VBB_CMPNT_REINST_DAYS_NO"),
    F.lit(0).alias("VBB_TMPLT_UNIQ_KEY"),
    F.lit("UNK").alias("VBB_CMPNT_NM"),
    F.lit("UNK").alias("VBB_CMPNT_TYP_NM")
)

df_DefaultNA_source = df_foreignKey.filter(F.col("RowNumber") == 1).select(
    F.lit(1).alias("VBB_CMPNT_SK"),
    F.lit(1).alias("VBB_CMPNT_UNIQ_KEY"),
    F.lit(1).alias("SRC_SYS_CD_SK"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("VBB_PLN_SK"),
    F.lit(1).alias("VBB_CMPLNC_METH_CD_SK"),
    F.lit(1).alias("VBB_CMPNT_FNSH_RULE_CD_SK"),
    F.lit(1).alias("VBB_CMPNT_FUNC_CAT_CD_SK"),
    F.lit(1).alias("VBB_CMPNT_FUNC_CD_SK"),
    F.lit("X").alias("MBR_ACHV_VALID_ALW_IN"),
    F.lit("X").alias("VBB_CMPNT_REENR_IN"),
    F.lit("1753-01-01 00:00:00.000").cast(TimestampType()).alias("SRC_SYS_CRT_DTM"),
    F.lit("1753-01-01 00:00:00.000").cast(TimestampType()).alias("SRC_SYS_UPDT_DTM"),
    F.lit(1).alias("VBB_CMPNT_ACHV_LVL_CT"),
    F.lit(1).alias("VBB_CMPNT_FNSH_DAYS_NO"),
    F.lit(1).alias("VBB_CMPNT_REINST_DAYS_NO"),
    F.lit(1).alias("VBB_TMPLT_UNIQ_KEY"),
    F.lit("NA").alias("VBB_CMPNT_NM"),
    F.lit("NA").alias("VBB_CMPNT_TYP_NM")
)

# Hashed File => Scenario C => write_to_parquet
df_RecycleFinal = df_Recycle.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("ERR_CT").alias("ERR_CT"),
    F.col("RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("VBB_CMPNT_SK").alias("VBB_CMPNT_SK"),
    F.col("VBB_CMPNT_UNIQ_KEY").alias("VBB_CMPNT_UNIQ_KEY"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("HIPL_ID").alias("HIPL_ID"),
    F.col("INPR_FINISH_RULE").alias("INPR_FINISH_RULE"),
    F.col("CDVL_VALUE_FUNCTION").alias("CDVL_VALUE_FUNCTION"),
    F.col("INPR_COMPLIANCE_METHOD").alias("INPR_COMPLIANCE_METHOD"),
    F.rpad(F.col("MBR_ACHV_VALID_ALW_IN"), 1, " ").alias("MBR_ACHV_VALID_ALW_IN"),
    F.rpad(F.col("VBB_CMPNT_REENR_IN"), 1, " ").alias("VBB_CMPNT_REENR_IN"),
    F.col("SRC_SYS_CRT_DTM").alias("SRC_SYS_CRT_DTM"),
    F.col("SRC_SYS_UPDT_DTM").alias("SRC_SYS_UPDT_DTM"),
    F.col("VBB_CMPNT_ACHV_LVL_CT").alias("VBB_CMPNT_ACHV_LVL_CT"),
    F.col("VBB_CMPNT_FNSH_DAYS_NO").alias("VBB_CMPNT_FNSH_DAYS_NO"),
    F.col("VBB_CMPNT_REINST_DAYS_NO").alias("VBB_CMPNT_REINST_DAYS_NO"),
    F.col("VBB_TMPLT_UNIQ_KEY").alias("VBB_TMPLT_UNIQ_KEY"),
    F.col("VBB_CMPNT_NM").alias("VBB_CMPNT_NM"),
    F.col("VBB_CMPNT_TYP_NM").alias("VBB_CMPNT_TYP_NM")
)

write_files(
    df_RecycleFinal,
    "hf_recycle.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

df_Fkey_for_union = df_Fkey.select(
    F.col("VBB_CMPNT_SK"),
    F.col("VBB_CMPNT_UNIQ_KEY"),
    F.col("SRC_SYS_CD_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("VBB_PLN_SK"),
    F.col("VBB_CMPLNC_METH_CD_SK"),
    F.col("VBB_CMPNT_FNSH_RULE_CD_SK"),
    F.col("VBB_CMPNT_FUNC_CAT_CD_SK"),
    F.col("VBB_CMPNT_FUNC_CD_SK"),
    F.col("MBR_ACHV_VALID_ALW_IN"),
    F.col("VBB_CMPNT_REENR_IN"),
    F.col("SRC_SYS_CRT_DTM"),
    F.col("SRC_SYS_UPDT_DTM"),
    F.col("VBB_CMPNT_ACHV_LVL_CT"),
    F.col("VBB_CMPNT_FNSH_DAYS_NO"),
    F.col("VBB_CMPNT_REINST_DAYS_NO"),
    F.col("VBB_TMPLT_UNIQ_KEY"),
    F.col("VBB_CMPNT_NM"),
    F.col("VBB_CMPNT_TYP_NM")
)

df_DefaultUNK_for_union = df_DefaultUNK_source.select(
    F.col("VBB_CMPNT_SK"),
    F.col("VBB_CMPNT_UNIQ_KEY"),
    F.col("SRC_SYS_CD_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("VBB_PLN_SK"),
    F.col("VBB_CMPLNC_METH_CD_SK"),
    F.col("VBB_CMPNT_FNSH_RULE_CD_SK"),
    F.col("VBB_CMPNT_FUNC_CAT_CD_SK"),
    F.col("VBB_CMPNT_FUNC_CD_SK"),
    F.col("MBR_ACHV_VALID_ALW_IN"),
    F.col("VBB_CMPNT_REENR_IN"),
    F.col("SRC_SYS_CRT_DTM"),
    F.col("SRC_SYS_UPDT_DTM"),
    F.col("VBB_CMPNT_ACHV_LVL_CT"),
    F.col("VBB_CMPNT_FNSH_DAYS_NO"),
    F.col("VBB_CMPNT_REINST_DAYS_NO"),
    F.col("VBB_TMPLT_UNIQ_KEY"),
    F.col("VBB_CMPNT_NM"),
    F.col("VBB_CMPNT_TYP_NM")
)

df_DefaultNA_for_union = df_DefaultNA_source.select(
    F.col("VBB_CMPNT_SK"),
    F.col("VBB_CMPNT_UNIQ_KEY"),
    F.col("SRC_SYS_CD_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("VBB_PLN_SK"),
    F.col("VBB_CMPLNC_METH_CD_SK"),
    F.col("VBB_CMPNT_FNSH_RULE_CD_SK"),
    F.col("VBB_CMPNT_FUNC_CAT_CD_SK"),
    F.col("VBB_CMPNT_FUNC_CD_SK"),
    F.col("MBR_ACHV_VALID_ALW_IN"),
    F.col("VBB_CMPNT_REENR_IN"),
    F.col("SRC_SYS_CRT_DTM"),
    F.col("SRC_SYS_UPDT_DTM"),
    F.col("VBB_CMPNT_ACHV_LVL_CT"),
    F.col("VBB_CMPNT_FNSH_DAYS_NO"),
    F.col("VBB_CMPNT_REINST_DAYS_NO"),
    F.col("VBB_TMPLT_UNIQ_KEY"),
    F.col("VBB_CMPNT_NM"),
    F.col("VBB_CMPNT_TYP_NM")
)

df_collector = (
    df_Fkey_for_union
    .unionByName(df_DefaultUNK_for_union)
    .unionByName(df_DefaultNA_for_union)
)

df_collectorFinal = df_collector.select(
    F.col("VBB_CMPNT_SK"),
    F.col("VBB_CMPNT_UNIQ_KEY"),
    F.col("SRC_SYS_CD_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("VBB_PLN_SK"),
    F.col("VBB_CMPLNC_METH_CD_SK"),
    F.col("VBB_CMPNT_FNSH_RULE_CD_SK"),
    F.col("VBB_CMPNT_FUNC_CAT_CD_SK"),
    F.col("VBB_CMPNT_FUNC_CD_SK"),
    F.rpad(F.col("MBR_ACHV_VALID_ALW_IN"), 1, " ").alias("MBR_ACHV_VALID_ALW_IN"),
    F.rpad(F.col("VBB_CMPNT_REENR_IN"), 1, " ").alias("VBB_CMPNT_REENR_IN"),
    F.col("SRC_SYS_CRT_DTM"),
    F.col("SRC_SYS_UPDT_DTM"),
    F.col("VBB_CMPNT_ACHV_LVL_CT"),
    F.col("VBB_CMPNT_FNSH_DAYS_NO"),
    F.col("VBB_CMPNT_REINST_DAYS_NO"),
    F.col("VBB_TMPLT_UNIQ_KEY"),
    F.col("VBB_CMPNT_NM"),
    F.col("VBB_CMPNT_TYP_NM")
)

write_files(
    df_collectorFinal,
    f"{adls_path}/load/VBB_CMPNT.dat",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)