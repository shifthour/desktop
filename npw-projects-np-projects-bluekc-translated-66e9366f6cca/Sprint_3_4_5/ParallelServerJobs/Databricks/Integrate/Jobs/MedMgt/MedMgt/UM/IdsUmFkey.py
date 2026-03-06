# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2006, 2018 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:          IdsUmFkey
# MAGIC 
# MAGIC DESCRIPTION:   Takes the primary key file and applies the foreign keys.   
# MAGIC       
# MAGIC 
# MAGIC INPUTS:              Primary Key output file
# MAGIC                             
# MAGIC PROCESSING:   Natural keys are converted to surrogate keys
# MAGIC 
# MAGIC OUTPUTS:         UM table load file
# MAGIC                            hf_recycle - If recycling was used for this job records would be written to this hash file that had errors.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC       
# MAGIC Developer                 Date              Project/Altiris #             Change Description                                                           Development Project   Code Reviewer            Date Reviewed
# MAGIC ------------------------------   -------------------   ---------------------------------    ----------------------------------------------------------------------------------------  ----------------------------------  ---------------------------------   -------------------------
# MAGIC Hugh Sisson             02/2006                                             Originally programmed
# MAGIC Hugh Sisson             05/2006                                             Corrected GetFkeyCodes lookup for source system code 
# MAGIC                                                                                             by changingparameter value from "IDS" to 
# MAGIC                                                                                             Key.SRC_SYS_CD
# MAGIC Bhoomi Dasari          03/25/2009   3808                             Added SrcSysCdSk parameter                                          devlIDS                        Steph Goddard            03/30/2009
# MAGIC Rick Henry               05/10/2012   4896                             Added Diag_Cd_Typ_Cd to SK Lookup                            IntegrateNewDevl        Sharon Andrew           2012-05-20
# MAGIC Sethuraman R          10/03/2018   5569-Oncology UM      To add "UM Alternate Ref ID" as part of Oncology UM    IntegrateDev1              Hugh Sisson                2018-10-03

# MAGIC Set all foreign surrogate keys
# MAGIC Merge source data with default rows
# MAGIC Writing Sequential File to /load
# MAGIC Extracts SUBGRP_UNIQ_KEY and SUBGP_SK from the IDS SUBGRP table
# MAGIC Read common record format file from extract job.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, DecimalType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner', '$PROJDEF')
ids_secret_name = get_widget_value('ids_secret_name', '')
InFile = get_widget_value('InFile', 'IdsUmExtr.tmp')
OutFile = get_widget_value('OutFile', 'UM.dat')
Logging = get_widget_value('Logging', 'X')
SrcSysCdSk = get_widget_value('SrcSysCdSk', '')

schema_IdsUmExtr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), nullable=False),
    StructField("INSRT_UPDT_CD", StringType(), nullable=False),
    StructField("DISCARD_IN", StringType(), nullable=False),
    StructField("PASS_THRU_IN", StringType(), nullable=False),
    StructField("FIRST_RECYC_DT", TimestampType(), nullable=False),
    StructField("ERR_CT", IntegerType(), nullable=False),
    StructField("RECYCLE_CT", DecimalType(38, 10), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("PRI_KEY_STRING", StringType(), nullable=False),
    StructField("UM_SK", IntegerType(), nullable=False),
    StructField("UM_REF_ID", StringType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("CASE_MGT", StringType(), nullable=False),
    StructField("CRT_USER", StringType(), nullable=False),
    StructField("GRP", IntegerType(), nullable=False),
    StructField("MBR", IntegerType(), nullable=False),
    StructField("PRI_DIAG_CD", StringType(), nullable=False),
    StructField("PRI_RESP_USER", StringType(), nullable=False),
    StructField("PROD", StringType(), nullable=False),
    StructField("SUBGRP", IntegerType(), nullable=False),
    StructField("SUB", IntegerType(), nullable=False),
    StructField("QLTY_OF_SVC_LVL_CD", StringType(), nullable=False),
    StructField("RISK_LVL_CD", StringType(), nullable=False),
    StructField("UM_CLS_PLN_PROD_CAT", StringType(), nullable=False),
    StructField("IP_RCRD_IN", StringType(), nullable=False),
    StructField("ATCHMT_SRC_DTM", TimestampType(), nullable=False),
    StructField("CRT_DT", StringType(), nullable=False),
    StructField("MED_MGT_NOTE_DTM", TimestampType(), nullable=False),
    StructField("FINL_CARE_DT", StringType(), nullable=False),
    StructField("ACTVTY_SEQ_NO", IntegerType(), nullable=False),
    StructField("DIAG_CD_TYP_CD", StringType(), nullable=False),
    StructField("ALT_REF_ID", StringType(), nullable=False),
])

df_IdsUmExtr = (
    spark.read.format("csv")
    .option("header", "false")
    .option("delimiter", ",")
    .option("quote", "\"")
    .schema(schema_IdsUmExtr)
    .load(f"{adls_path}/key/{InFile}")
)

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"SELECT SG.SUBGRP_UNIQ_KEY as SUBGRP_UNIQ_KEY, SG.SUBGRP_SK as SUBGRP_SK FROM {IDSOwner}.SUBGRP SG"
df_IDS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_hf_um_subgrp = df_IDS.dropDuplicates(["SUBGRP_UNIQ_KEY"])

df_ForeignKey = (
    df_IdsUmExtr.alias("Key")
    .join(
        df_hf_um_subgrp.alias("lnkUmSubgrp"),
        F.col("Key.SUBGRP") == F.col("lnkUmSubgrp.SUBGRP_UNIQ_KEY"),
        how="left"
    )
    .withColumn("PassThru", F.col("Key.PASS_THRU_IN"))
    .withColumn("svCaseMgtSk", GetFkeyCaseMgt(F.col("Key.SRC_SYS_CD"), F.col("Key.UM_SK"), F.col("Key.CASE_MGT"), F.lit(Logging)))
    .withColumn("svCrtUserSk", GetFkeyAppUsr(F.col("Key.SRC_SYS_CD"), F.col("Key.UM_SK"), F.col("Key.CRT_USER"), F.lit(Logging)))
    .withColumn("svGrpSk", GetFkeyGrp(F.col("Key.SRC_SYS_CD"), F.col("Key.UM_SK"), F.col("Key.GRP"), F.lit(Logging)))
    .withColumn("svMbrSk", GetFkeyMbr(F.col("Key.SRC_SYS_CD"), F.col("Key.UM_SK"), F.col("Key.MBR"), F.lit(Logging)))
    .withColumn("svPriDiagCdSk", GetFkeyDiagCd(F.col("Key.SRC_SYS_CD"), F.col("Key.UM_SK"), F.col("Key.PRI_DIAG_CD"), F.col("Key.DIAG_CD_TYP_CD"), F.lit(Logging)))
    .withColumn("svPriRespUserSk", GetFkeyAppUsr(F.col("Key.SRC_SYS_CD"), F.col("Key.UM_SK"), F.col("Key.PRI_RESP_USER"), F.lit(Logging)))
    .withColumn("svProdSk", GetFkeyProd(F.col("Key.SRC_SYS_CD"), F.col("Key.UM_SK"), F.col("Key.PROD"), F.lit(Logging)))
    .withColumn("svSubgrpSk", F.when(F.col("lnkUmSubgrp.SUBGRP_SK").isNull(), F.lit(1)).otherwise(F.col("lnkUmSubgrp.SUBGRP_SK")))
    .withColumn("svSubSk", GetFkeySub(F.col("Key.SRC_SYS_CD"), F.col("Key.UM_SK"), F.col("Key.SUB"), F.lit(Logging)))
    .withColumn("svQltySvcLvlSk", GetFkeyCodes(F.col("Key.SRC_SYS_CD"), F.col("Key.UM_SK"), F.lit("UTILIZATION MANAGEMENT QUALITY OF SERVICE LEVEL"), F.col("Key.QLTY_OF_SVC_LVL_CD"), F.lit(Logging)))
    .withColumn("svRiskLvlCdSk", GetFkeyCodes(F.col("Key.SRC_SYS_CD"), F.col("Key.UM_SK"), F.lit("UTILIZATION MANAGEMENT RISK LEVEL"), F.col("Key.RISK_LVL_CD"), F.lit(Logging)))
    .withColumn("svUmClsPlProdSk", GetFkeyCodes(F.col("Key.SRC_SYS_CD"), F.col("Key.UM_SK"), F.lit("CLASS PLAN PRODUCT CATEGORY"), F.col("Key.UM_CLS_PLN_PROD_CAT"), F.lit(Logging)))
    .withColumn("svCrtDtSk", GetFkeyDate(F.lit("IDS"), F.col("Key.UM_SK"), F.col("Key.CRT_DT"), F.lit(Logging)))
    .withColumn("svFinlCareDtSk", GetFkeyDate(F.lit("IDS"), F.col("Key.UM_SK"), F.col("Key.FINL_CARE_DT"), F.lit(Logging)))
    .withColumn("svDefaultDate", F.lit("1753-01-01 00:00:00.000000"))
    .withColumn("ErrCount", GetFkeyErrorCnt(F.col("Key.UM_SK")))
)

df_ForeignKeyFkey = df_ForeignKey.filter((F.col("ErrCount") == 0) | (F.col("PassThru") == 'Y'))
df_fkey_select = df_ForeignKeyFkey.select(
    F.col("Key.UM_SK").alias("UM_SK"),
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("Key.UM_REF_ID").alias("UM_REF_ID"),
    F.col("Key.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("Key.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("svCaseMgtSk").alias("CASE_MGT_SK"),
    F.col("svCrtUserSk").alias("CRT_USER_SK"),
    F.col("svGrpSk").alias("GRP_SK"),
    F.col("svMbrSk").alias("MBR_SK"),
    F.col("svPriDiagCdSk").alias("PRI_DIAG_CD_SK"),
    F.col("svPriRespUserSk").alias("PRI_RESP_USER_SK"),
    F.col("svProdSk").alias("PROD_SK"),
    F.col("svSubgrpSk").alias("SUBGRP_SK"),
    F.col("svSubSk").alias("SUB_SK"),
    F.col("svQltySvcLvlSk").alias("QLTY_OF_SVC_LVL_CD_SK"),
    F.col("svRiskLvlCdSk").alias("RISK_LVL_CD_SK"),
    F.col("svUmClsPlProdSk").alias("UM_CLS_PLN_PROD_CAT_CD_SK"),
    F.col("Key.IP_RCRD_IN").alias("IP_RCRD_IN"),
    F.col("Key.ATCHMT_SRC_DTM").alias("ATCHMT_SRC_DTM"),
    F.col("svCrtDtSk").alias("CRT_DT_SK"),
    F.col("Key.MED_MGT_NOTE_DTM").alias("MED_MGT_NOTE_DTM"),
    F.col("svFinlCareDtSk").alias("FINL_CARE_DT_SK"),
    F.col("Key.ACTVTY_SEQ_NO").alias("ACTVTY_SEQ_NO"),
    F.col("Key.ALT_REF_ID").alias("ALT_REF_ID")
)

df_ForeignKeyRecycle = df_ForeignKey.filter(F.col("ErrCount") > 0).select(
    GetRecycleKey(F.col("Key.UM_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("Key.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("Key.DISCARD_IN").alias("DISCARD_IN"),
    F.col("Key.PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("Key.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("ErrCount").alias("ERR_CT"),
    (F.col("Key.RECYCLE_CT") + F.lit(1)).alias("RECYCLE_CT"),
    F.col("Key.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Key.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("Key.UM_SK").alias("UM_SK"),
    F.col("Key.UM_REF_ID").alias("UM_REF_ID"),
    F.col("Key.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("Key.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("Key.CASE_MGT").alias("CASE_MGT"),
    F.col("Key.CRT_USER").alias("CRT_USER"),
    F.col("Key.GRP").alias("GRP"),
    F.col("Key.MBR").alias("MBR"),
    F.col("Key.PRI_DIAG_CD").alias("PRI_DIAG_CD"),
    F.col("Key.PRI_RESP_USER").alias("PRI_RESP_USER"),
    F.col("Key.PROD").alias("PROD"),
    F.col("Key.SUBGRP").alias("SUBGRP"),
    F.col("Key.SUB").alias("SUB"),
    F.col("Key.QLTY_OF_SVC_LVL_CD").alias("QLTY_OF_SVC_LVL_CD"),
    F.col("Key.RISK_LVL_CD").alias("RISK_LVL_CD"),
    F.col("Key.UM_CLS_PLN_PROD_CAT").alias("UM_CLS_PLN_PROD_CAT"),
    F.col("Key.IP_RCRD_IN").alias("IP_RCRD_IN"),
    F.col("Key.ATCHMT_SRC_DTM").alias("ATCHMT_SRC_DTM"),
    F.col("Key.CRT_DT").alias("CRT_DT"),
    F.col("Key.MED_MGT_NOTE_DTM").alias("MED_MGT_NOTE_DTM"),
    F.col("Key.FINL_CARE_DT").alias("FINL_CARE_DT"),
    F.col("Key.ACTVTY_SEQ_NO").alias("ACTVTY_SEQ_NO"),
    F.col("Key.ALT_REF_ID").alias("ALT_REF_ID")
)

df_recycle_padded = (
    df_ForeignKeyRecycle
    .withColumn("INSRT_UPDT_CD", F.rpad(F.col("INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", F.rpad(F.col("DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", F.rpad(F.col("PASS_THRU_IN"), 1, " "))
    .withColumn("CASE_MGT", F.rpad(F.col("CASE_MGT"), 10, " "))
    .withColumn("CRT_USER", F.rpad(F.col("CRT_USER"), 10, " "))
    .withColumn("PRI_DIAG_CD", F.rpad(F.col("PRI_DIAG_CD"), 10, " "))
    .withColumn("PRI_RESP_USER", F.rpad(F.col("PRI_RESP_USER"), 10, " "))
    .withColumn("PROD", F.rpad(F.col("PROD"), 10, " "))
    .withColumn("QLTY_OF_SVC_LVL_CD", F.rpad(F.col("QLTY_OF_SVC_LVL_CD"), 4, " "))
    .withColumn("RISK_LVL_CD", F.rpad(F.col("RISK_LVL_CD"), 4, " "))
    .withColumn("UM_CLS_PLN_PROD_CAT", F.rpad(F.col("UM_CLS_PLN_PROD_CAT"), 10, " "))
    .withColumn("IP_RCRD_IN", F.rpad(F.col("IP_RCRD_IN"), 1, " "))
    .withColumn("CRT_DT", F.rpad(F.col("CRT_DT"), 10, " "))
    .withColumn("FINL_CARE_DT", F.rpad(F.col("FINL_CARE_DT"), 10, " "))
)

write_files(
    df_recycle_padded,
    "hf_recycle.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

df_ForeignKey = df_ForeignKey.withColumn("_row_num", F.row_number().over(Window.orderBy(F.lit(1))))

df_DefaultUNK = df_ForeignKey.filter(F.col("_row_num") == 1).select(
    F.lit(0).alias("UM_SK"),
    F.lit(0).alias("SRC_SYS_CD_SK"),
    F.lit("UNK").alias("UM_REF_ID"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("CASE_MGT_SK"),
    F.lit(0).alias("CRT_USER_SK"),
    F.lit(0).alias("GRP_SK"),
    F.lit(0).alias("MBR_SK"),
    F.lit(0).alias("PRI_DIAG_CD_SK"),
    F.lit(0).alias("PRI_RESP_USER_SK"),
    F.lit(0).alias("PROD_SK"),
    F.lit(0).alias("SUBGRP_SK"),
    F.lit(0).alias("SUB_SK"),
    F.lit(0).alias("QLTY_OF_SVC_LVL_CD_SK"),
    F.lit(0).alias("RISK_LVL_CD_SK"),
    F.lit(0).alias("UM_CLS_PLN_PROD_CAT_CD_SK"),
    F.lit("U").alias("IP_RCRD_IN"),
    F.col("svDefaultDate").alias("ATCHMT_SRC_DTM"),
    F.lit("1753-01-01").alias("CRT_DT_SK"),
    F.col("svDefaultDate").alias("MED_MGT_NOTE_DTM"),
    F.lit("2199-12-31").alias("FINL_CARE_DT_SK"),
    F.lit(0).alias("ACTVTY_SEQ_NO"),
    F.lit(0).alias("ALT_REF_ID")
)

df_DefaultNA = df_ForeignKey.filter(F.col("_row_num") == 1).select(
    F.lit(1).alias("UM_SK"),
    F.lit(1).alias("SRC_SYS_CD_SK"),
    F.lit("NA").alias("UM_REF_ID"),
    F.lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("CASE_MGT_SK"),
    F.lit(1).alias("CRT_USER_SK"),
    F.lit(1).alias("GRP_SK"),
    F.lit(1).alias("MBR_SK"),
    F.lit(1).alias("PRI_DIAG_CD_SK"),
    F.lit(1).alias("PRI_RESP_USER_SK"),
    F.lit(1).alias("PROD_SK"),
    F.lit(1).alias("SUBGRP_SK"),
    F.lit(1).alias("SUB_SK"),
    F.lit(1).alias("QLTY_OF_SVC_LVL_CD_SK"),
    F.lit(1).alias("RISK_LVL_CD_SK"),
    F.lit(1).alias("UM_CLS_PLN_PROD_CAT_CD_SK"),
    F.lit("X").alias("IP_RCRD_IN"),
    F.col("svDefaultDate").alias("ATCHMT_SRC_DTM"),
    F.lit("1753-01-01").alias("CRT_DT_SK"),
    F.col("svDefaultDate").alias("MED_MGT_NOTE_DTM"),
    F.lit("2199-1231").alias("FINL_CARE_DT_SK"),
    F.lit(1).alias("ACTVTY_SEQ_NO"),
    F.lit(1).alias("ALT_REF_ID")
)

df_collector = df_fkey_select.unionAll(df_DefaultUNK).unionAll(df_DefaultNA)

df_collector_padded = (
    df_collector
    .withColumn("IP_RCRD_IN", F.rpad(F.col("IP_RCRD_IN"), 1, " "))
    .withColumn("CRT_DT_SK", F.rpad(F.col("CRT_DT_SK"), 10, " "))
    .withColumn("FINL_CARE_DT_SK", F.rpad(F.col("FINL_CARE_DT_SK"), 10, " "))
)

write_files(
    df_collector_padded,
    f"{adls_path}/load/{OutFile}",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)