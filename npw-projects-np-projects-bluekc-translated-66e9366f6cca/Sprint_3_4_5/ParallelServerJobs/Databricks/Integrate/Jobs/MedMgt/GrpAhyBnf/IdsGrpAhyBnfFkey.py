# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2010 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC Called by:
# MAGIC                     
# MAGIC 
# MAGIC Control Job Rerun Information: 
# MAGIC                     Previous Run Successful:    Restart, no other steps necessary
# MAGIC                     Previous Run Aborted:         Restart, no other steps necessary
# MAGIC 
# MAGIC Modifications:                         
# MAGIC                                                    Project/                                                                                                            Code                        Date
# MAGIC Developer             Date              Altiris #        Change Description                                                                      Reviewer                  Reviewed
# MAGIC ---------------------------  -------------------   -----------------  ---------------------------------------------------------------------------------------------------   ---------------------------     -------------------   
# MAGIC Kalyan Neelam      2010-05-10     4404            Initial Programming                                                                      Steph Goddard        05/13/2010

# MAGIC Set all foreign surrogate keys
# MAGIC Merge source data with default rows
# MAGIC Writing Sequential File to /load
# MAGIC Read common record format file from extract job.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DecimalType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


Logging = get_widget_value("Logging","X")
InFile = get_widget_value("InFile","")
SrcSysCdSk = get_widget_value("SrcSysCdSk","")

schema_IdsGrpAhyBnfExtr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False),
    StructField("INSRT_UPDT_CD", StringType(), False),
    StructField("DISCARD_IN", StringType(), False),
    StructField("PASS_THRU_IN", StringType(), False),
    StructField("FIRST_RECYC_DT", TimestampType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", DecimalType(38, 10), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PRI_KEY_STRING", StringType(), False),
    StructField("GRP_AHY_BNF_SK", IntegerType(), False),
    StructField("GRP_ID", StringType(), False),
    StructField("EFF_DT_SK", StringType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("GRP_SK", IntegerType(), False),
    StructField("A_SLIM_YOU_ADTNL_SLOTS_IN", StringType(), False),
    StructField("A_SLIM_YOU_WT_MGT_PGM_IN", StringType(), False),
    StructField("AHY_ONLY_MBRSH_AVLBL_IN", StringType(), False),
    StructField("AHY_EFF_DT_SK", StringType(), False),
    StructField("AHY_TERM_DT_SK", StringType(), False),
    StructField("DDCT_HLTH_MGT_STRTGT_IN", StringType(), False),
    StructField("DDCT_ONST_WELNS_CRDNTR_IN", StringType(), False),
    StructField("EMPLR_WELNS_ASMNT_IN", StringType(), False),
    StructField("FACE_TO_FACE_ADTNL_SLOTS_IN", StringType(), False),
    StructField("FACE_TO_FACE_COACH_IN", StringType(), False),
    StructField("HLTH_COACH_AVLBL_IN", StringType(), False),
    StructField("HLTH_RISK_ASMNT_ONLY_IN", StringType(), False),
    StructField("INCLD_SPOUSE_IN", StringType(), False),
    StructField("ONE_HR_EDUC_SESS_IN", StringType(), False),
    StructField("ONLN_HLTH_RISK_ASMNT_IN", StringType(), False),
    StructField("ONST_ASMNT_IN", StringType(), False),
    StructField("ONST_CRDNTR_ADTNL_DAYS_IN", StringType(), False),
    StructField("ONST_HLTH_SCRN_IN", StringType(), False),
    StructField("PT_TO_BLUE_INCNTV_PGM_IN", StringType(), False),
    StructField("TERM_DT_SK", StringType(), False),
    StructField("WEB_TOOL_IN", StringType(), False),
    StructField("WELNS_BNF_LVL_CD", StringType(), False),
    StructField("WELNS_CLS_IN", StringType(), False)
])

df_IdsGrpAhyBnfExtr = (
    spark.read
    .format("csv")
    .option("header", "false")
    .option("delimiter", ",")
    .option("quote", "\"")
    .schema(schema_IdsGrpAhyBnfExtr)
    .load(f"{adls_path}/key/{InFile}")
)

df_foreignKey = (
    df_IdsGrpAhyBnfExtr
    .withColumn("svSrcSysCdSk", F.lit(SrcSysCdSk))
    .withColumn("PassThru", F.col("PASS_THRU_IN"))
    .withColumn("svEffDtSk", GetFkeyDate('IDS', F.col("GRP_AHY_BNF_SK"), F.col("EFF_DT_SK"), Logging))
    .withColumn("svWelnsBnfLvlCdSk", GetFkeyCodes('IDS', F.col("GRP_AHY_BNF_SK"), F.lit("WELLNESS BENEFIT LEVEL"), F.col("WELNS_BNF_LVL_CD"), Logging))
    .withColumn("ErrCount", GetFkeyErrorCnt(F.col("GRP_AHY_BNF_SK")))
)

df_fkey = (
    df_foreignKey
    .filter((F.col("ErrCount") == 0) | (F.col("PassThru") == 'Y'))
    .select(
        F.col("GRP_AHY_BNF_SK").alias("GRP_AHY_BNF_SK"),
        F.col("svSrcSysCdSk").alias("SRC_SYS_CD_SK"),
        F.col("GRP_ID").alias("GRP_ID"),
        F.col("svEffDtSk").alias("EFF_DT_SK"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("GRP_SK").alias("GRP_SK"),
        F.col("A_SLIM_YOU_ADTNL_SLOTS_IN").alias("A_SLIM_YOU_ADTNL_SLOTS_IN"),
        F.col("A_SLIM_YOU_WT_MGT_PGM_IN").alias("A_SLIM_YOU_WT_MGT_PGM_IN"),
        F.col("AHY_ONLY_MBRSH_AVLBL_IN").alias("AHY_ONLY_MBRSH_AVLBL_IN"),
        F.col("AHY_EFF_DT_SK").alias("AHY_EFF_DT_SK"),
        F.col("AHY_TERM_DT_SK").alias("AHY_TERM_DT_SK"),
        F.col("DDCT_HLTH_MGT_STRTGT_IN").alias("DDCT_HLTH_MGT_STRTGT_IN"),
        F.col("DDCT_ONST_WELNS_CRDNTR_IN").alias("DDCT_ONST_WELNS_CRDNTR_IN"),
        F.col("EMPLR_WELNS_ASMNT_IN").alias("EMPLR_WELNS_ASMNT_IN"),
        F.col("FACE_TO_FACE_ADTNL_SLOTS_IN").alias("FACE_TO_FACE_ADTNL_SLOTS_IN"),
        F.col("FACE_TO_FACE_COACH_IN").alias("FACE_TO_FACE_COACH_IN"),
        F.col("HLTH_COACH_AVLBL_IN").alias("HLTH_COACH_AVLBL_IN"),
        F.col("HLTH_RISK_ASMNT_ONLY_IN").alias("HLTH_RISK_ASMNT_ONLY_IN"),
        F.col("INCLD_SPOUSE_IN").alias("INCLD_SPOUSE_IN"),
        F.col("ONE_HR_EDUC_SESS_IN").alias("ONE_HR_EDUC_SESS_IN"),
        F.col("ONLN_HLTH_RISK_ASMNT_IN").alias("ONLN_HLTH_RISK_ASMNT_IN"),
        F.col("ONST_ASMNT_IN").alias("ONST_ASMNT_IN"),
        F.col("ONST_CRDNTR_ADTNL_DAYS_IN").alias("ONST_CRDNTR_ADTNL_DAYS_IN"),
        F.col("ONST_HLTH_SCRN_IN").alias("ONST_HLTH_SCRN_IN"),
        F.col("PT_TO_BLUE_INCNTV_PGM_IN").alias("PT_TO_BLUE_INCNTV_PGM_IN"),
        F.col("TERM_DT_SK").alias("TERM_DT_SK"),
        F.col("WEB_TOOL_IN").alias("WEB_TOOL_IN"),
        F.col("svWelnsBnfLvlCdSk").alias("WELNS_BNF_LVL_CD_SK"),
        F.col("WELNS_CLS_IN").alias("WELNS_CLS_IN")
    )
)

df_recycle = (
    df_foreignKey
    .filter(F.col("ErrCount") > 0)
    .select(
        GetRecycleKey(F.col("GRP_AHY_BNF_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        F.col("DISCARD_IN").alias("DISCARD_IN"),
        F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
        F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        F.col("ErrCount").alias("ERR_CT"),
        (F.col("RECYCLE_CT") + F.lit(1)).alias("RECYCLE_CT"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        F.col("GRP_AHY_BNF_SK").alias("GRP_AHY_BNF_SK"),
        F.col("GRP_ID").alias("GRP_ID"),
        F.col("EFF_DT_SK").alias("EFF_DT_SK"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("GRP_SK").alias("GRP_SK"),
        F.col("A_SLIM_YOU_ADTNL_SLOTS_IN").alias("A_SLIM_YOU_ADTNL_SLOTS_IN"),
        F.col("A_SLIM_YOU_WT_MGT_PGM_IN").alias("A_SLIM_YOU_WT_MGT_PGM_IN"),
        F.col("AHY_ONLY_MBRSH_AVLBL_IN").alias("AHY_ONLY_MBRSH_AVLBL_IN"),
        F.col("AHY_EFF_DT_SK").alias("AHY_EFF_DT_SK"),
        F.col("AHY_TERM_DT_SK").alias("AHY_TERM_DT_SK"),
        F.col("DDCT_HLTH_MGT_STRTGT_IN").alias("DDCT_HLTH_MGT_STRTGT_IN"),
        F.col("DDCT_ONST_WELNS_CRDNTR_IN").alias("DDCT_ONST_WELNS_CRDNTR_IN"),
        F.col("EMPLR_WELNS_ASMNT_IN").alias("EMPLR_WELNS_ASMNT_IN"),
        F.col("FACE_TO_FACE_ADTNL_SLOTS_IN").alias("FACE_TO_FACE_ADTNL_SLOTS_IN"),
        F.col("FACE_TO_FACE_COACH_IN").alias("FACE_TO_FACE_COACH_IN"),
        F.col("HLTH_COACH_AVLBL_IN").alias("HLTH_COACH_AVLBL_IN"),
        F.col("HLTH_RISK_ASMNT_ONLY_IN").alias("HLTH_RISK_ASMNT_ONLY_IN"),
        F.col("INCLD_SPOUSE_IN").alias("INCLD_SPOUSE_IN"),
        F.col("ONE_HR_EDUC_SESS_IN").alias("ONE_HR_EDUC_SESS_IN"),
        F.col("ONLN_HLTH_RISK_ASMNT_IN").alias("ONLN_HLTH_RISK_ASMNT_IN"),
        F.col("ONST_ASMNT_IN").alias("ONST_ASMNT_IN"),
        F.col("ONST_CRDNTR_ADTNL_DAYS_IN").alias("ONST_CRDNTR_ADTNL_DAYS_IN"),
        F.col("ONST_HLTH_SCRN_IN").alias("ONST_HLTH_SCRN_IN"),
        F.col("PT_TO_BLUE_INCNTV_PGM_IN").alias("PT_TO_BLUE_INCNTV_PGM_IN"),
        F.col("TERM_DT_SK").alias("TERM_DT_SK"),
        F.col("WEB_TOOL_IN").alias("WEB_TOOL_IN"),
        F.col("WELNS_BNF_LVL_CD").alias("WELNS_BNF_LVL_CD"),
        F.col("WELNS_CLS_IN").alias("WELNS_CLS_IN")
    )
)

write_files(
    df_recycle,
    "hf_recycle.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

w = Window.orderBy(F.lit(1))

df_defaultNA_temp = (
    df_foreignKey
    .withColumn("rownum", F.row_number().over(w))
    .filter(F.col("rownum") == 1)
)

df_defaultNA = df_defaultNA_temp.select(
    F.lit(1).cast(IntegerType()).alias("GRP_AHY_BNF_SK"),
    F.lit(1).cast(IntegerType()).alias("SRC_SYS_CD_SK"),
    F.lit(1).cast(IntegerType()).alias("GRP_ID"),
    F.lit("1753-01-01").alias("EFF_DT_SK"),
    F.lit(1).cast(IntegerType()).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(1).cast(IntegerType()).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(1).cast(IntegerType()).alias("GRP_SK"),
    F.lit("X").alias("A_SLIM_YOU_ADTNL_SLOTS_IN"),
    F.lit("X").alias("A_SLIM_YOU_WT_MGT_PGM_IN"),
    F.lit("X").alias("AHY_ONLY_MBRSH_AVLBL_IN"),
    F.lit("1753-01-01").alias("AHY_EFF_DT_SK"),
    F.lit("2199-12-31").alias("AHY_TERM_DT_SK"),
    F.lit("X").alias("DDCT_HLTH_MGT_STRTGT_IN"),
    F.lit("X").alias("DDCT_ONST_WELNS_CRDNTR_IN"),
    F.lit("X").alias("EMPLR_WELNS_ASMNT_IN"),
    F.lit("X").alias("FACE_TO_FACE_ADTNL_SLOTS_IN"),
    F.lit("X").alias("FACE_TO_FACE_COACH_IN"),
    F.lit("X").alias("HLTH_COACH_AVLBL_IN"),
    F.lit("X").alias("HLTH_RISK_ASMNT_ONLY_IN"),
    F.lit("X").alias("INCLD_SPOUSE_IN"),
    F.lit("X").alias("ONE_HR_EDUC_SESS_IN"),
    F.lit("X").alias("ONLN_HLTH_RISK_ASMNT_IN"),
    F.lit("X").alias("ONST_ASMNT_IN"),
    F.lit("X").alias("ONST_CRDNTR_ADTNL_DAYS_IN"),
    F.lit("X").alias("ONST_HLTH_SCRN_IN"),
    F.lit("X").alias("PT_TO_BLUE_INCNTV_PGM_IN"),
    F.lit("2199-12-31").alias("TERM_DT_SK"),
    F.lit("X").alias("WEB_TOOL_IN"),
    F.lit(1).cast(IntegerType()).alias("WELNS_BNF_LVL_CD_SK"),
    F.lit("X").alias("WELNS_CLS_IN")
)

df_defaultUNK_temp = (
    df_foreignKey
    .withColumn("rownum", F.row_number().over(w))
    .filter(F.col("rownum") == 1)
)

df_defaultUNK = df_defaultUNK_temp.select(
    F.lit(0).cast(IntegerType()).alias("GRP_AHY_BNF_SK"),
    F.lit(0).cast(IntegerType()).alias("SRC_SYS_CD_SK"),
    F.lit(0).cast(IntegerType()).alias("GRP_ID"),
    F.lit("1753-01-01").alias("EFF_DT_SK"),
    F.lit(0).cast(IntegerType()).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).cast(IntegerType()).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).cast(IntegerType()).alias("GRP_SK"),
    F.lit("X").alias("A_SLIM_YOU_ADTNL_SLOTS_IN"),
    F.lit("X").alias("A_SLIM_YOU_WT_MGT_PGM_IN"),
    F.lit("X").alias("AHY_ONLY_MBRSH_AVLBL_IN"),
    F.lit("1753-01-01").alias("AHY_EFF_DT_SK"),
    F.lit("2199-12-31").alias("AHY_TERM_DT_SK"),
    F.lit("X").alias("DDCT_HLTH_MGT_STRTGT_IN"),
    F.lit("X").alias("DDCT_ONST_WELNS_CRDNTR_IN"),
    F.lit("X").alias("EMPLR_WELNS_ASMNT_IN"),
    F.lit("X").alias("FACE_TO_FACE_ADTNL_SLOTS_IN"),
    F.lit("X").alias("FACE_TO_FACE_COACH_IN"),
    F.lit("X").alias("HLTH_COACH_AVLBL_IN"),
    F.lit("X").alias("HLTH_RISK_ASMNT_ONLY_IN"),
    F.lit("X").alias("INCLD_SPOUSE_IN"),
    F.lit("X").alias("ONE_HR_EDUC_SESS_IN"),
    F.lit("X").alias("ONLN_HLTH_RISK_ASMNT_IN"),
    F.lit("X").alias("ONST_ASMNT_IN"),
    F.lit("X").alias("ONST_CRDNTR_ADTNL_DAYS_IN"),
    F.lit("X").alias("ONST_HLTH_SCRN_IN"),
    F.lit("X").alias("PT_TO_BLUE_INCNTV_PGM_IN"),
    F.lit("2199-12-31").alias("TERM_DT_SK"),
    F.lit("X").alias("WEB_TOOL_IN"),
    F.lit(0).cast(IntegerType()).alias("WELNS_BNF_LVL_CD_SK"),
    F.lit("X").alias("WELNS_CLS_IN")
)

df_collector = df_fkey.unionByName(df_defaultNA).unionByName(df_defaultUNK)

df_final = df_collector.select(
    F.col("GRP_AHY_BNF_SK"),
    F.col("SRC_SYS_CD_SK"),
    F.col("GRP_ID"),
    F.rpad(F.col("EFF_DT_SK"), 10, " ").alias("EFF_DT_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("GRP_SK"),
    F.rpad(F.col("A_SLIM_YOU_ADTNL_SLOTS_IN"), 1, " ").alias("A_SLIM_YOU_ADTNL_SLOTS_IN"),
    F.rpad(F.col("A_SLIM_YOU_WT_MGT_PGM_IN"), 1, " ").alias("A_SLIM_YOU_WT_MGT_PGM_IN"),
    F.rpad(F.col("AHY_ONLY_MBRSH_AVLBL_IN"), 1, " ").alias("AHY_ONLY_MBRSH_AVLBL_IN"),
    F.rpad(F.col("AHY_EFF_DT_SK"), 10, " ").alias("AHY_EFF_DT_SK"),
    F.rpad(F.col("AHY_TERM_DT_SK"), 10, " ").alias("AHY_TERM_DT_SK"),
    F.rpad(F.col("DDCT_HLTH_MGT_STRTGT_IN"), 1, " ").alias("DDCT_HLTH_MGT_STRTGT_IN"),
    F.rpad(F.col("DDCT_ONST_WELNS_CRDNTR_IN"), 1, " ").alias("DDCT_ONST_WELNS_CRDNTR_IN"),
    F.rpad(F.col("EMPLR_WELNS_ASMNT_IN"), 1, " ").alias("EMPLR_WELNS_ASMNT_IN"),
    F.rpad(F.col("FACE_TO_FACE_ADTNL_SLOTS_IN"), 1, " ").alias("FACE_TO_FACE_ADTNL_SLOTS_IN"),
    F.rpad(F.col("FACE_TO_FACE_COACH_IN"), 1, " ").alias("FACE_TO_FACE_COACH_IN"),
    F.rpad(F.col("HLTH_COACH_AVLBL_IN"), 1, " ").alias("HLTH_COACH_AVLBL_IN"),
    F.rpad(F.col("HLTH_RISK_ASMNT_ONLY_IN"), 1, " ").alias("HLTH_RISK_ASMNT_ONLY_IN"),
    F.rpad(F.col("INCLD_SPOUSE_IN"), 1, " ").alias("INCLD_SPOUSE_IN"),
    F.rpad(F.col("ONE_HR_EDUC_SESS_IN"), 1, " ").alias("ONE_HR_EDUC_SESS_IN"),
    F.rpad(F.col("ONLN_HLTH_RISK_ASMNT_IN"), 1, " ").alias("ONLN_HLTH_RISK_ASMNT_IN"),
    F.rpad(F.col("ONST_ASMNT_IN"), 1, " ").alias("ONST_ASMNT_IN"),
    F.rpad(F.col("ONST_CRDNTR_ADTNL_DAYS_IN"), 1, " ").alias("ONST_CRDNTR_ADTNL_DAYS_IN"),
    F.rpad(F.col("ONST_HLTH_SCRN_IN"), 1, " ").alias("ONST_HLTH_SCRN_IN"),
    F.rpad(F.col("PT_TO_BLUE_INCNTV_PGM_IN"), 1, " ").alias("PT_TO_BLUE_INCNTV_PGM_IN"),
    F.rpad(F.col("TERM_DT_SK"), 10, " ").alias("TERM_DT_SK"),
    F.rpad(F.col("WEB_TOOL_IN"), 1, " ").alias("WEB_TOOL_IN"),
    F.col("WELNS_BNF_LVL_CD_SK"),
    F.rpad(F.col("WELNS_CLS_IN"), 1, " ").alias("WELNS_CLS_IN")
)

write_files(
    df_final,
    f"{adls_path}/load/GRP_AHY_BNF.dat",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)