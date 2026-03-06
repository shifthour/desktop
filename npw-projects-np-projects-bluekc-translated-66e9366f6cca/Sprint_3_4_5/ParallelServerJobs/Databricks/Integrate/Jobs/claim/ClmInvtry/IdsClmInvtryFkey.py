# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC CALLED BY:  OpsDashboardClmLoadSeq
# MAGIC 
# MAGIC                            
# MAGIC PROCESSING:   The Foreign Key process is done to load the CLM_INVTRY table in IDS
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC                                                             Project/                                                                                                                  Development 
# MAGIC Developer                     Date               Altiris #       Change Description                                                                             Project               Code Reviewer      Date Reviewed
# MAGIC ----------------------------------   -------------------    ---------------   ---------------------------------------------------------------------------------------------------------   -----------------------   ----------------------------   -------------------------    
# MAGIC Parikshith Chada          12/19/2007    3531          Original Programming                                                                           devlIDS30          Steph Goddard      01/04/2008
# MAGIC Hugh Sisson                 01/22/2008    3531          Changed 3 foreign SK lookups to use actual source code instead     devlIDS              Steph Goddard      03/27/2008
# MAGIC                                                                               of "FACETS"                                                                                                                    
# MAGIC Abhiram Dasarathy	     07/20/2015     5407	Added the colum ASG_USER_SK at the end.		          IntegrateDev1     Kalyan Neelam       2015-07-20

# MAGIC Read common record format file.
# MAGIC Writing Sequential File to /load
# MAGIC Assign foreign keys and create default rows for unknown and not applicable.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType
from pyspark.sql.functions import col, when, lit, row_number, rpad
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


InFile = get_widget_value('InFile','')
Logging = get_widget_value('Logging','')

schema_ClmInvtryExtr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False),
    StructField("INSRT_UPDT_CD", StringType(), False),
    StructField("DISCARD_IN", StringType(), False),
    StructField("PASS_THRU_IN", StringType(), False),
    StructField("FIRST_RECYC_DT", TimestampType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", DecimalType(38, 10), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PRI_KEY_STRING", StringType(), False),
    StructField("CLM_INVTRY_SK", IntegerType(), False),
    StructField("CLM_INVTRY_KEY_ID", StringType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("GRP_ID", StringType(), False),
    StructField("OPS_WORK_UNIT_ID", StringType(), False),
    StructField("PDPD_ID", StringType(), False),
    StructField("PRPR_ID", StringType(), False),
    StructField("CLM_INVTRY_PEND_CAT_CD", StringType(), False),
    StructField("CLM_STTUS_CHG_RSN_CD", StringType(), False),
    StructField("CLST_STS", StringType(), False),
    StructField("CLCL_CL_SUB_TYPE", StringType(), False),
    StructField("CLCL_CL_TYPE", StringType(), False),
    StructField("INPT_DT_SK", StringType(), False),
    StructField("RCVD_DT_SK", StringType(), False),
    StructField("EXTR_DT_SK", StringType(), False),
    StructField("STTUS_DT_SK", StringType(), False),
    StructField("INVTRY_CT", IntegerType(), False),
    StructField("ASG_USER_SK", IntegerType(), False),
    StructField("WORK_ITEM_CT", IntegerType(), False)
])

df_ClmInvtryExtr_raw = (
    spark.read.format("csv")
    .option("header", "false")
    .option("inferSchema", "false")
    .option("delimiter", ",")
    .option("quote", "\"")
    .schema(schema_ClmInvtryExtr)
    .load(f"{adls_path}/key/{InFile}")
)

df_ClmInvtryExtr = df_ClmInvtryExtr_raw.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "CLM_INVTRY_SK",
    "CLM_INVTRY_KEY_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "GRP_ID",
    "OPS_WORK_UNIT_ID",
    "PDPD_ID",
    "PRPR_ID",
    "CLM_INVTRY_PEND_CAT_CD",
    "CLM_STTUS_CHG_RSN_CD",
    "CLST_STS",
    "CLCL_CL_SUB_TYPE",
    "CLCL_CL_TYPE",
    "INPT_DT_SK",
    "RCVD_DT_SK",
    "EXTR_DT_SK",
    "STTUS_DT_SK",
    "INVTRY_CT",
    "ASG_USER_SK",
    "WORK_ITEM_CT"
)

df_withStageVars = (
    df_ClmInvtryExtr
    .withColumn(
        "SrcSys",
        when(col("SRC_SYS_CD") == lit("AWD"), lit("AWD"))
        .when(col("SRC_SYS_CD") == lit("IMAGENOW"), lit("IMAGENOW"))
        .when(col("SRC_SYS_CD") == lit("EEC"), lit("NPS"))
        .otherwise(lit("FACETS"))
    )
    .withColumn(
        "SrcSysCdSk",
        GetFkeyCodes(
            lit("IDS"),
            col("CLM_INVTRY_SK"),
            lit("SOURCE SYSTEM"),
            col("SRC_SYS_CD"),
            Logging
        )
    )
    .withColumn(
        "GrpSk",
        GetFkeyGrp(
            lit("FACETS"),
            col("CLM_INVTRY_SK"),
            col("GRP_ID"),
            Logging
        )
    )
    .withColumn(
        "OpsWorkUnitSk",
        GetFkeyOpsWorkUnit(
            lit("UWS"),
            col("CLM_INVTRY_SK"),
            col("OPS_WORK_UNIT_ID"),
            Logging
        )
    )
    .withColumn(
        "ProdSk",
        GetFkeyProd(
            lit("FACETS"),
            col("CLM_INVTRY_SK"),
            col("PDPD_ID"),
            Logging
        )
    )
    .withColumn(
        "ProvSk",
        GetFkeyProv(
            lit("FACETS"),
            col("CLM_INVTRY_SK"),
            col("PRPR_ID"),
            Logging
        )
    )
    .withColumn(
        "ClmInvtryPendCatCdSk",
        GetFkeyCodes(
            col("SrcSys"),
            col("CLM_INVTRY_SK"),
            lit("CLAIM INVENTORY PEND CATEGORY"),
            col("CLM_INVTRY_PEND_CAT_CD"),
            Logging
        )
    )
    .withColumn(
        "ClmSttusChgRsnCdSk",
        GetFkeyCodes(
            lit("FACETS"),
            col("CLM_INVTRY_SK"),
            lit("CLAIM STATUS REASON"),
            col("CLM_STTUS_CHG_RSN_CD"),
            Logging
        )
    )
    .withColumn(
        "ClmSttusCdSk",
        GetFkeyCodes(
            col("SrcSys"),
            col("CLM_INVTRY_SK"),
            lit("CLAIM STATUS"),
            col("CLST_STS"),
            Logging
        )
    )
    .withColumn(
        "ClmSubtypCdSk",
        GetFkeyCodes(
            col("SrcSys"),
            col("CLM_INVTRY_SK"),
            lit("CLAIM INVENTORY CLAIM SUBTYPE"),
            col("CLCL_CL_SUB_TYPE"),
            Logging
        )
    )
    .withColumn(
        "ClmTypCdSk",
        GetFkeyCodes(
            col("SrcSys"),
            col("CLM_INVTRY_SK"),
            lit("CLAIM TYPE"),
            col("CLCL_CL_TYPE"),
            Logging
        )
    )
    .withColumn(
        "InptDtSk",
        GetFkeyDate(
            lit("IDS"),
            col("CLM_INVTRY_SK"),
            col("INPT_DT_SK"),
            Logging
        )
    )
    .withColumn(
        "RcvdDtSk",
        GetFkeyDate(
            lit("IDS"),
            col("CLM_INVTRY_SK"),
            col("RCVD_DT_SK"),
            Logging
        )
    )
    .withColumn(
        "SttusDtSk",
        GetFkeyDate(
            lit("IDS"),
            col("CLM_INVTRY_SK"),
            col("STTUS_DT_SK"),
            Logging
        )
    )
    .withColumn("PassThru", col("PASS_THRU_IN"))
    .withColumn("ErrCount", GetFkeyErrorCnt(col("CLM_INVTRY_SK")))
)

df_ForeignKey_Fkey_temp = df_withStageVars.filter(
    (col("ErrCount") == lit(0)) | (col("PassThru") == lit("Y"))
)

df_ForeignKey_Fkey = (
    df_ForeignKey_Fkey_temp
    .select(
        col("CLM_INVTRY_SK"),
        col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
        col("CLM_INVTRY_KEY_ID"),
        col("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("GrpSk").alias("GRP_SK"),
        col("OpsWorkUnitSk").alias("OPS_WORK_UNIT_SK"),
        col("ProdSk").alias("PROD_SK"),
        col("ProvSk").alias("PROV_SK"),
        col("ClmInvtryPendCatCdSk").alias("CLM_INVTRY_PEND_CAT_CD_SK"),
        col("ClmSttusChgRsnCdSk").alias("CLM_STTUS_CHG_RSN_CD_SK"),
        col("ClmSttusCdSk").alias("CLM_STTUS_CD_SK"),
        col("ClmSubtypCdSk").alias("CLM_SUBTYP_CD_SK"),
        col("ClmTypCdSk").alias("CLM_TYP_CD_SK"),
        col("InptDtSk").alias("INPT_DT_SK"),
        col("RcvdDtSk").alias("RCVD_DT_SK"),
        col("EXTR_DT_SK"),
        col("SttusDtSk").alias("STTUS_DT_SK"),
        col("INVTRY_CT"),
        col("ASG_USER_SK"),
        col("WORK_ITEM_CT")
    )
    .withColumn("INPT_DT_SK", rpad(col("INPT_DT_SK"), 10, " "))
    .withColumn("RCVD_DT_SK", rpad(col("RCVD_DT_SK"), 10, " "))
    .withColumn("EXTR_DT_SK", rpad(col("EXTR_DT_SK"), 10, " "))
    .withColumn("STTUS_DT_SK", rpad(col("STTUS_DT_SK"), 10, " "))
)

df_recycle_temp = df_withStageVars.filter(col("ErrCount") > lit(0))

df_ForeignKey_recycle = (
    df_recycle_temp
    .select(
        GetRecycleKey(col("CLM_INVTRY_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
        col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        col("DISCARD_IN").alias("DISCARD_IN"),
        col("PASS_THRU_IN").alias("PASS_THRU_IN"),
        col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        col("ErrCount").alias("ERR_CT"),
        (col("RECYCLE_CT") + lit(1)).alias("RECYCLE_CT"),
        col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        col("CLM_INVTRY_SK").alias("CLM_INVTRY_SK"),
        col("CLM_INVTRY_KEY_ID").alias("CLM_INVTRY_KEY_ID"),
        col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("GRP_ID").alias("GRP_ID"),
        col("OPS_WORK_UNIT_ID").alias("OPS_WORK_UNIT_ID"),
        col("PDPD_ID").alias("PDPD_ID"),
        col("PRPR_ID").alias("PRPR_ID"),
        col("CLM_INVTRY_PEND_CAT_CD").alias("CLM_INVTRY_PEND_CAT_CD"),
        col("CLM_STTUS_CHG_RSN_CD").alias("CLM_STTUS_CHG_RSN_CD"),
        col("CLST_STS").alias("CLST_STS"),
        col("CLCL_CL_SUB_TYPE").alias("CLCL_CL_SUB_TYPE"),
        col("CLCL_CL_TYPE").alias("CLCL_CL_TYPE"),
        col("INPT_DT_SK").alias("INPT_DT_SK"),
        col("RCVD_DT_SK").alias("RCVD_DT_SK"),
        col("EXTR_DT_SK").alias("EXTR_DT_SK"),
        col("STTUS_DT_SK").alias("STTUS_DT_SK"),
        col("INVTRY_CT").alias("INVTRY_CT"),
        col("ASG_USER_SK").alias("ASG_USER_SK"),
        col("WORK_ITEM_CT").alias("WORK_ITEM_CT")
    )
    .withColumn("INSRT_UPDT_CD", rpad(col("INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", rpad(col("DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", rpad(col("PASS_THRU_IN"), 1, " "))
    .withColumn("PDPD_ID", rpad(col("PDPD_ID"), 8, " "))
    .withColumn("PRPR_ID", rpad(col("PRPR_ID"), 12, " "))
    .withColumn("CLST_STS", rpad(col("CLST_STS"), 2, " "))
    .withColumn("CLCL_CL_SUB_TYPE", rpad(col("CLCL_CL_SUB_TYPE"), 1, " "))
    .withColumn("CLCL_CL_TYPE", rpad(col("CLCL_CL_TYPE"), 1, " "))
    .withColumn("INPT_DT_SK", rpad(col("INPT_DT_SK"), 10, " "))
    .withColumn("RCVD_DT_SK", rpad(col("RCVD_DT_SK"), 10, " "))
    .withColumn("EXTR_DT_SK", rpad(col("EXTR_DT_SK"), 10, " "))
    .withColumn("STTUS_DT_SK", rpad(col("STTUS_DT_SK"), 10, " "))
)

write_files(
    df_ForeignKey_recycle,
    "hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

w = Window.orderBy(lit(1))

df_rownum = df_withStageVars.withColumn("rownum", row_number().over(w))

df_ForeignKey_DefaultUNK_tmp = df_rownum.filter(col("rownum") == lit(1))
df_ForeignKey_DefaultUNK = (
    df_ForeignKey_DefaultUNK_tmp
    .select(
        lit(0).alias("CLM_INVTRY_SK"),
        lit(0).alias("SRC_SYS_CD_SK"),
        lit("UNK").alias("CLM_INVTRY_KEY_ID"),
        lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        lit(0).alias("GRP_SK"),
        lit(0).alias("OPS_WORK_UNIT_SK"),
        lit(0).alias("PROD_SK"),
        lit(0).alias("PROV_SK"),
        lit(0).alias("CLM_INVTRY_PEND_CAT_CD_SK"),
        lit(0).alias("CLM_STTUS_CHG_RSN_CD_SK"),
        lit(0).alias("CLM_STTUS_CD_SK"),
        lit(0).alias("CLM_SUBTYP_CD_SK"),
        lit(0).alias("CLM_TYP_CD_SK"),
        lit("1753-01-01").alias("INPT_DT_SK"),
        lit("1753-01-01").alias("RCVD_DT_SK"),
        lit("1753-01-01").alias("EXTR_DT_SK"),
        lit("1753-01-01").alias("STTUS_DT_SK"),
        lit(0).alias("INVTRY_CT"),
        lit(0).alias("ASG_USER_SK"),
        lit(0).alias("WORK_ITEM_CT")
    )
    .withColumn("INPT_DT_SK", rpad(col("INPT_DT_SK"), 10, " "))
    .withColumn("RCVD_DT_SK", rpad(col("RCVD_DT_SK"), 10, " "))
    .withColumn("EXTR_DT_SK", rpad(col("EXTR_DT_SK"), 10, " "))
    .withColumn("STTUS_DT_SK", rpad(col("STTUS_DT_SK"), 10, " "))
)

df_ForeignKey_DefaultNA_tmp = df_rownum.filter(col("rownum") == lit(1))
df_ForeignKey_DefaultNA = (
    df_ForeignKey_DefaultNA_tmp
    .select(
        lit(1).alias("CLM_INVTRY_SK"),
        lit(1).alias("SRC_SYS_CD_SK"),
        lit("NA").alias("CLM_INVTRY_KEY_ID"),
        lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        lit(1).alias("GRP_SK"),
        lit(1).alias("OPS_WORK_UNIT_SK"),
        lit(1).alias("PROD_SK"),
        lit(1).alias("PROV_SK"),
        lit(1).alias("CLM_INVTRY_PEND_CAT_CD_SK"),
        lit(1).alias("CLM_STTUS_CHG_RSN_CD_SK"),
        lit(1).alias("CLM_STTUS_CD_SK"),
        lit(1).alias("CLM_SUBTYP_CD_SK"),
        lit(1).alias("CLM_TYP_CD_SK"),
        lit("1753-01-01").alias("INPT_DT_SK"),
        lit("1753-01-01").alias("RCVD_DT_SK"),
        lit("1753-01-01").alias("EXTR_DT_SK"),
        lit("1753-01-01").alias("STTUS_DT_SK"),
        lit(0).alias("INVTRY_CT"),
        lit(1).alias("ASG_USER_SK"),
        lit(0).alias("WORK_ITEM_CT")
    )
    .withColumn("INPT_DT_SK", rpad(col("INPT_DT_SK"), 10, " "))
    .withColumn("RCVD_DT_SK", rpad(col("RCVD_DT_SK"), 10, " "))
    .withColumn("EXTR_DT_SK", rpad(col("EXTR_DT_SK"), 10, " "))
    .withColumn("STTUS_DT_SK", rpad(col("STTUS_DT_SK"), 10, " "))
)

df_Collector = (
    df_ForeignKey_DefaultUNK.union(df_ForeignKey_DefaultNA)
    .union(df_ForeignKey_Fkey.select(
        col("CLM_INVTRY_SK"),
        col("SRC_SYS_CD_SK"),
        col("CLM_INVTRY_KEY_ID"),
        col("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("GRP_SK"),
        col("OPS_WORK_UNIT_SK"),
        col("PROD_SK"),
        col("PROV_SK"),
        col("CLM_INVTRY_PEND_CAT_CD_SK"),
        col("CLM_STTUS_CHG_RSN_CD_SK"),
        col("CLM_STTUS_CD_SK"),
        col("CLM_SUBTYP_CD_SK"),
        col("CLM_TYP_CD_SK"),
        col("INPT_DT_SK"),
        col("RCVD_DT_SK"),
        col("EXTR_DT_SK"),
        col("STTUS_DT_SK"),
        col("INVTRY_CT"),
        col("ASG_USER_SK"),
        col("WORK_ITEM_CT")
    ))
)

df_final = df_Collector.select(
    "CLM_INVTRY_SK",
    "SRC_SYS_CD_SK",
    "CLM_INVTRY_KEY_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "GRP_SK",
    "OPS_WORK_UNIT_SK",
    "PROD_SK",
    "PROV_SK",
    "CLM_INVTRY_PEND_CAT_CD_SK",
    "CLM_STTUS_CHG_RSN_CD_SK",
    "CLM_STTUS_CD_SK",
    "CLM_SUBTYP_CD_SK",
    "CLM_TYP_CD_SK",
    "INPT_DT_SK",
    "RCVD_DT_SK",
    "EXTR_DT_SK",
    "STTUS_DT_SK",
    "INVTRY_CT",
    "ASG_USER_SK",
    "WORK_ITEM_CT"
)

write_files(
    df_final,
    f"{adls_path}/load/CLM_INVTRY.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)