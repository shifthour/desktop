# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2010 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC Called by:
# MAGIC                     FctsIncomeLoadSeq
# MAGIC 
# MAGIC Processing:
# MAGIC                     *
# MAGIC 
# MAGIC Control Job Rerun Information: 
# MAGIC                     Previous Run Successful:    What needs to happen before a special run can occur?
# MAGIC                     Previous Run Aborted:         Restart, no other steps necessary
# MAGIC 
# MAGIC Modifications:                        
# MAGIC                                                  Project/                                                                                                                       Code                   Date
# MAGIC Developer           Date              Altiris #     Change Description                                                                                     Reviewer            Reviewed
# MAGIC -------------------------  -------------------   -------------   -----------------------------------------------------------------------------------------------------------------   -------------------------  -------------------
# MAGIC Hugh Sisson       2010-09-13    3346        Original program                                                                                           Steph Goddard   09/21/2010

# MAGIC Read common record format file.
# MAGIC Writing Sequential File to /load
# MAGIC A reference lookup is necessary because of requirements that PROD_BILL_CMPNT_EFF_DT_SK <= lookup value and PROD_BILL_CMPNT_TERM_DT_SK >= lookup value
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
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType, NumericType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


Logging = get_widget_value("Logging", "Y")
SrcSysCdSk = get_widget_value("SrcSysCdSk", "1581")
InFile = get_widget_value("InFile", "IdsInvcCmpntCtPkey.InvcCmpntCtTmp.dat")
IDSOwner = get_widget_value("IDSOwner", "")
ids_secret_name = get_widget_value("ids_secret_name", "")

schema_IdsInvcCmpntCtKey = StructType([
    StructField("INSRT_UPDT_CD", StringType(), False),
    StructField("DISCARD_IN", StringType(), False),
    StructField("PASS_THRU_IN", StringType(), False),
    StructField("FIRST_RECYC_DT", TimestampType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", DecimalType(38,10), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PRI_KEY_STRING", StringType(), False),
    StructField("INVC_CMPNT_CT_SK", IntegerType(), False),
    StructField("BILL_INVC_ID", StringType(), False),
    StructField("CLS_PLN_ID", StringType(), False),
    StructField("PROD_ID", StringType(), False),
    StructField("PROD_BILL_CMPNT_ID", StringType(), False),
    StructField("INVC_CMPNT_CT_DISP_CD", StringType(), False),
    StructField("INVC_CMPNT_CT_PRM_TYP_CD", StringType(), False),
    StructField("INVC_CMPNT_CT_SRC_CD", StringType(), False),
    StructField("SRC_SYS_CD_SK", IntegerType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("BILL_DUE_DT_SK", StringType(), False),
    StructField("BILL_AMT", DecimalType(38,10), False),
    StructField("BILL_ENTY_UNIQ_KEY", IntegerType(), False),
    StructField("DPNDT_LVS_CT", IntegerType(), False),
    StructField("SUB_LVS_CT", IntegerType(), False),
    StructField("BLCT_DISP_CD", StringType(), False),
    StructField("BLCT_PREM_TYPE", StringType(), False),
    StructField("BLCT_SOURCE", StringType(), False),
    StructField("PROD_CMPNT_PFX_ID", StringType(), False)
])

df_key = (
    spark.read.format("csv")
    .option("sep", ",")
    .option("header", "false")
    .option("quote", "\"")
    .schema(schema_IdsInvcCmpntCtKey)
    .load(f"{adls_path}/key/{InFile}")
)

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"SELECT PBC.PROD_CMPNT_PFX_ID, PBC.SRC_SYS_CD_SK, PBC.PROD_BILL_CMPNT_ID, PBC.PROD_BILL_CMPNT_EFF_DT_SK, PBC.PROD_BILL_CMPNT_TERM_DT_SK, PBC.PROD_BILL_CMPNT_SK FROM {IDSOwner}.PROD_BILL_CMPNT PBC"
df_PROD_BILL_CMPNT = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_joined = df_key.alias("Key").join(
    df_PROD_BILL_CMPNT.alias("ProdBillCmpntSkLkup"),
    (
        (F.col("Key.PROD_CMPNT_PFX_ID") == F.col("ProdBillCmpntSkLkup.PROD_CMPNT_PFX_ID"))
        & (F.col("Key.SRC_SYS_CD_SK") == F.col("ProdBillCmpntSkLkup.SRC_SYS_CD_SK"))
        & (F.col("Key.PROD_BILL_CMPNT_ID") == F.col("ProdBillCmpntSkLkup.PROD_BILL_CMPNT_ID"))
        & (F.col("ProdBillCmpntSkLkup.PROD_BILL_CMPNT_EFF_DT_SK") <= F.col("Key.BILL_DUE_DT_SK"))
        & (F.col("ProdBillCmpntSkLkup.PROD_BILL_CMPNT_TERM_DT_SK") >= F.col("Key.BILL_DUE_DT_SK"))
    ),
    "left"
)

df_xformed = (
    df_joined
    .withColumn("PassThru", F.col("Key.PASS_THRU_IN"))
    .withColumn("svBillEntySk", GetFkeyBillEnty(F.col("Key.SRC_SYS_CD"), F.col("Key.INVC_CMPNT_CT_SK"), F.col("Key.BILL_ENTY_UNIQ_KEY"), F.lit(Logging)))
    .withColumn("svClsPlnSk", GetFkeyClsPln(F.col("Key.SRC_SYS_CD"), F.col("Key.INVC_CMPNT_CT_SK"), F.col("Key.CLS_PLN_ID"), F.lit(Logging)))
    .withColumn("svInvcSk", GetFkeyInvc(F.col("Key.SRC_SYS_CD"), F.col("Key.INVC_CMPNT_CT_SK"), F.col("Key.BILL_INVC_ID"), F.lit(Logging)))
    .withColumn("svProdSk", GetFkeyProd(F.col("Key.SRC_SYS_CD"), F.col("Key.INVC_CMPNT_CT_SK"), F.col("Key.PROD_ID"), F.lit(Logging)))
    .withColumn("svInvcCmpntCtDispCdSk", GetFkeyCodes(F.col("Key.SRC_SYS_CD"), F.col("Key.INVC_CMPNT_CT_SK"), F.lit("COMMISSION DETAIL DISPOSITION"), F.col("Key.BLCT_DISP_CD"), F.lit(Logging)))
    .withColumn("svInvcCmpntCtPrmTypCdSk", GetFkeyCodes(F.col("Key.SRC_SYS_CD"), F.col("Key.INVC_CMPNT_CT_SK"), F.lit("INVOICE SUBSCRIBER PREMIUM TYPE"), F.col("Key.BLCT_PREM_TYPE"), F.lit(Logging)))
    .withColumn("svInvcCmpntCtSrcCdSk", GetFkeyCodes(F.col("Key.SRC_SYS_CD"), F.col("Key.INVC_CMPNT_CT_SK"), F.lit("INVOICE COMPONENT COUNT SOURCE"), F.col("Key.BLCT_SOURCE"), F.lit(Logging)))
    .withColumn("svBillDueDtSk", GetFkeyDate(F.lit("IDS"), F.col("Key.INVC_CMPNT_CT_SK"), F.col("Key.BILL_DUE_DT_SK"), F.lit(Logging)))
    .withColumn("ErrCount", GetFkeyErrorCnt(F.col("Key.INVC_CMPNT_CT_SK")))
)

df_fkey = (
    df_xformed
    .filter((F.col("ErrCount") == 0) | (F.col("PassThru") == "Y"))
    .select(
        F.col("Key.INVC_CMPNT_CT_SK").alias("INVC_CMPNT_CT_SK"),
        F.col("Key.BILL_INVC_ID").alias("BILL_INVC_ID"),
        F.col("Key.CLS_PLN_ID").alias("CLS_PLN_ID"),
        F.col("Key.PROD_ID").alias("PROD_ID"),
        F.col("Key.PROD_BILL_CMPNT_ID").alias("PROD_BILL_CMPNT_ID"),
        F.col("Key.INVC_CMPNT_CT_DISP_CD").alias("INVC_CMPNT_CT_DISP_CD"),
        F.col("Key.INVC_CMPNT_CT_PRM_TYP_CD").alias("INVC_CMPNT_CT_PRM_TYP_CD"),
        F.col("Key.INVC_CMPNT_CT_SRC_CD").alias("INVC_CMPNT_CT_SRC_CD"),
        F.col("Key.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.col("Key.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("Key.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("svBillEntySk").alias("BILL_ENTY_SK"),
        F.col("svClsPlnSk").alias("CLS_PLN_SK"),
        F.col("svInvcSk").alias("INVC_SK"),
        F.col("svProdSk").alias("PROD_SK"),
        F.when(F.isnull(F.col("ProdBillCmpntSkLkup.PROD_BILL_CMPNT_SK")), F.lit(0)).otherwise(F.col("ProdBillCmpntSkLkup.PROD_BILL_CMPNT_SK")).alias("PROD_BILL_CMPNT_SK"),
        F.col("svInvcCmpntCtDispCdSk").alias("INVC_CMPNT_CT_DISP_CD_SK"),
        F.col("svInvcCmpntCtPrmTypCdSk").alias("INVC_CMPNT_CT_PRM_TYP_CD_SK"),
        F.col("svInvcCmpntCtSrcCdSk").alias("INVC_CMPNT_CT_SRC_CD_SK"),
        F.col("Key.BILL_DUE_DT_SK").alias("BILL_DUE_DT_SK"),
        F.col("Key.BILL_AMT").alias("BILL_AMT"),
        F.col("Key.BILL_ENTY_UNIQ_KEY").alias("BILL_ENTY_UNIQ_KEY"),
        F.col("Key.DPNDT_LVS_CT").alias("DPNDT_LVS_CT"),
        F.col("Key.SUB_LVS_CT").alias("SUB_LVS_CT")
    )
)

df_recycle = (
    df_xformed
    .filter(F.col("ErrCount") > 0)
    .select(
        GetRecycleKey(F.col("Key.INVC_CMPNT_CT_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.col("Key.DISCARD_IN").alias("INSRT_UPDT_CD"),
        F.col("Key.PASS_THRU_IN").alias("DISCARD_IN"),
        F.col("Key.FIRST_RECYC_DT").alias("PASS_THRU_IN"),
        F.col("Key.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        F.col("ErrCount").alias("ERR_CT"),
        F.col("Key.RECYCLE_CT").alias("RECYCLE_CT"),
        F.col("Key.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("Key.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        F.col("Key.INVC_CMPNT_CT_SK").alias("INVC_CMPNT_CT_SK"),
        F.col("Key.BILL_INVC_ID").alias("BILL_INVC_ID"),
        F.col("Key.CLS_PLN_ID").alias("CLS_PLN_ID"),
        F.col("Key.PROD_ID").alias("PROD_ID"),
        F.col("Key.PROD_BILL_CMPNT_ID").alias("PROD_BILL_CMPNT_ID"),
        F.col("Key.INVC_CMPNT_CT_DISP_CD").alias("INVC_CMPNT_CT_DISP_CD"),
        F.col("Key.INVC_CMPNT_CT_PRM_TYP_CD").alias("INVC_CMPNT_CT_PRM_TYP_CD"),
        F.col("Key.INVC_CMPNT_CT_SRC_CD").alias("INVC_CMPNT_CT_SRC_CD"),
        F.col("Key.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.col("Key.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("Key.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("Key.BILL_DUE_DT_SK").alias("BILL_DUE_DT_SK"),
        F.col("Key.BILL_AMT").alias("BILL_AMT"),
        F.col("Key.BILL_ENTY_UNIQ_KEY").alias("BILL_ENTY_UNIQ_KEY"),
        F.col("Key.DPNDT_LVS_CT").alias("DPNDT_LVS_CT"),
        F.col("Key.SUB_LVS_CT").alias("SUB_LVS_CT"),
        F.col("Key.BLCT_DISP_CD").alias("BLCT_DISP_CD"),
        F.col("Key.BLCT_PREM_TYPE").alias("BLCT_PREM_TYPE"),
        F.col("Key.BLCT_SOURCE").alias("BLCT_SOURCE")
    )
)

df_recycle_rpad = (
    df_recycle
    .withColumn("INSRT_UPDT_CD", F.rpad(F.col("INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", F.rpad(F.col("DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", F.rpad(F.col("PASS_THRU_IN"), 1, " "))
    .withColumn("BILL_DUE_DT_SK", F.rpad(F.col("BILL_DUE_DT_SK"), 10, " "))
    .withColumn("BLCT_DISP_CD", F.rpad(F.col("BLCT_DISP_CD"), 1, " "))
    .withColumn("BLCT_PREM_TYPE", F.rpad(F.col("BLCT_PREM_TYPE"), 1, " "))
    .withColumn("BLCT_SOURCE", F.rpad(F.col("BLCT_SOURCE"), 1, " "))
)

write_files(
    df_recycle_rpad.select(
        "JOB_EXCTN_RCRD_ERR_SK",
        "INSRT_UPDT_CD",
        "DISCARD_IN",
        "PASS_THRU_IN",
        "FIRST_RECYC_DT",
        "ERR_CT",
        "RECYCLE_CT",
        "SRC_SYS_CD",
        "PRI_KEY_STRING",
        "INVC_CMPNT_CT_SK",
        "BILL_INVC_ID",
        "CLS_PLN_ID",
        "PROD_ID",
        "PROD_BILL_CMPNT_ID",
        "INVC_CMPNT_CT_DISP_CD",
        "INVC_CMPNT_CT_PRM_TYP_CD",
        "INVC_CMPNT_CT_SRC_CD",
        "SRC_SYS_CD_SK",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "BILL_DUE_DT_SK",
        "BILL_AMT",
        "BILL_ENTY_UNIQ_KEY",
        "DPNDT_LVS_CT",
        "SUB_LVS_CT",
        "BLCT_DISP_CD",
        "BLCT_PREM_TYPE",
        "BLCT_SOURCE"
    ),
    "hf_recycle.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

df_default_na = spark.createDataFrame(
    [
        (
            1,
            "NA",
            "NA",
            "NA",
            "NA",
            "NA",
            "NA",
            "NA",
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            "1753-01-01",
            0,
            1,
            0,
            0
        )
    ],
    [
        "INVC_CMPNT_CT_SK",
        "BILL_INVC_ID",
        "CLS_PLN_ID",
        "PROD_ID",
        "PROD_BILL_CMPNT_ID",
        "INVC_CMPNT_CT_DISP_CD",
        "INVC_CMPNT_CT_PRM_TYP_CD",
        "INVC_CMPNT_CT_SRC_CD",
        "SRC_SYS_CD_SK",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "BILL_ENTY_SK",
        "CLS_PLN_SK",
        "INVC_SK",
        "PROD_SK",
        "PROD_BILL_CMPNT_SK",
        "INVC_CMPNT_CT_DISP_CD_SK",
        "INVC_CMPNT_CT_PRM_TYP_CD_SK",
        "INVC_CMPNT_CT_SRC_CD_SK",
        "BILL_DUE_DT_SK",
        "BILL_AMT",
        "BILL_ENTY_UNIQ_KEY",
        "DPNDT_LVS_CT",
        "SUB_LVS_CT"
    ]
)

df_default_unk = spark.createDataFrame(
    [
        (
            0,
            "UNK",
            "UNK",
            "UNK",
            "UNK",
            "UNK",
            "UNK",
            "UNK",
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            "1753-01-01",
            0,
            0,
            0,
            0
        )
    ],
    [
        "INVC_CMPNT_CT_SK",
        "BILL_INVC_ID",
        "CLS_PLN_ID",
        "PROD_ID",
        "PROD_BILL_CMPNT_ID",
        "INVC_CMPNT_CT_DISP_CD",
        "INVC_CMPNT_CT_PRM_TYP_CD",
        "INVC_CMPNT_CT_SRC_CD",
        "SRC_SYS_CD_SK",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "BILL_ENTY_SK",
        "CLS_PLN_SK",
        "INVC_SK",
        "PROD_SK",
        "PROD_BILL_CMPNT_SK",
        "INVC_CMPNT_CT_DISP_CD_SK",
        "INVC_CMPNT_CT_PRM_TYP_CD_SK",
        "INVC_CMPNT_CT_SRC_CD_SK",
        "BILL_DUE_DT_SK",
        "BILL_AMT",
        "BILL_ENTY_UNIQ_KEY",
        "DPNDT_LVS_CT",
        "SUB_LVS_CT"
    ]
)

df_collector = df_fkey.unionByName(df_default_na).unionByName(df_default_unk)

df_collector_rpad = (
    df_collector
    .withColumn("BILL_DUE_DT_SK", F.rpad(F.col("BILL_DUE_DT_SK"), 10, " "))
)

write_files(
    df_collector_rpad.select(
        "INVC_CMPNT_CT_SK",
        "BILL_INVC_ID",
        "CLS_PLN_ID",
        "PROD_ID",
        "PROD_BILL_CMPNT_ID",
        "INVC_CMPNT_CT_DISP_CD",
        "INVC_CMPNT_CT_PRM_TYP_CD",
        "INVC_CMPNT_CT_SRC_CD",
        "SRC_SYS_CD_SK",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "BILL_ENTY_SK",
        "CLS_PLN_SK",
        "INVC_SK",
        "PROD_SK",
        "PROD_BILL_CMPNT_SK",
        "INVC_CMPNT_CT_DISP_CD_SK",
        "INVC_CMPNT_CT_PRM_TYP_CD_SK",
        "INVC_CMPNT_CT_SRC_CD_SK",
        "BILL_DUE_DT_SK",
        "BILL_AMT",
        "BILL_ENTY_UNIQ_KEY",
        "DPNDT_LVS_CT",
        "SUB_LVS_CT"
    ),
    f"{adls_path}/load/INVC_CMPNT_CT.dat",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)