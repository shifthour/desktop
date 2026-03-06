# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC PROCESSING: Extracts data from IDS Ops_Group_ Work_Unit_Xref
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date               Project/Altiris #               Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------   -----------------------------------    ---------------------------------------------------------   ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Madhav Mynampati         1/4/2009         TTR 345/15                    Originally Programmed                        devlEDW                    
# MAGIC 
# MAGIC Archana Palivela                 3/6/14         5114                           Originally Programmed (In Parallel)                EnterpriseWhseDevl     Jag Yelavarthi         2014-03-28

# MAGIC Job name: IdsEdwIdsOpsGrpWorkUnitXrefDExtr
# MAGIC 
# MAGIC 
# MAGIC Operations Group Work Unit Xref
# MAGIC Read data from source table P_OPS_GRP_WORK_UNIT_XREF
# MAGIC Write OPS_GRP_WORK_UNIT_XREF_D  Data into a Sequential file for Load Ready Job.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
EDWRunCycle = get_widget_value('EDWRunCycle','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
ids_secret_name = get_widget_value('ids_secret_name','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query_1 = f"""SELECT 
UNIT_XREF.GRP_SK,
UNIT_XREF.GRP_ID,
UNIT_XREF.OPS_WORK_UNIT_ID,
UNIT_XREF.USER_ID,
UNIT_XREF.LAST_UPDT_DT_SK

FROM {IDSOwner}.P_OPS_GRP_WORK_UNIT_XREF UNIT_XREF
"""

df_db2_P_OPS_GRP_WORK_UNIT_XREF_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_1)
    .load()
)

extract_query_2 = f"""SELECT 
OPS_WORK_UNIT.OPS_WORK_UNIT_SK, 
OPS_WORK_UNIT.OPS_WORK_UNIT_ID

FROM {IDSOwner}.OPS_WORK_UNIT OPS_WORK_UNIT
"""

df_db2_OPS_WORK_UNIT_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_2)
    .load()
)

df_lkp_Codes = (
    df_db2_P_OPS_GRP_WORK_UNIT_XREF_Extr.alias("Ink_IdsEdwIdsOpsGrpWorkUnitXrefDExtr_InABC")
    .join(
        df_db2_OPS_WORK_UNIT_Extr.alias("Ink_OpsWorkUnit_InABC"),
        F.col("Ink_IdsEdwIdsOpsGrpWorkUnitXrefDExtr_InABC.OPS_WORK_UNIT_ID")
        == F.col("Ink_OpsWorkUnit_InABC.OPS_WORK_UNIT_ID"),
        "left"
    )
    .select(
        F.col("Ink_IdsEdwIdsOpsGrpWorkUnitXrefDExtr_InABC.GRP_SK").alias("GRP_SK"),
        F.col("Ink_IdsEdwIdsOpsGrpWorkUnitXrefDExtr_InABC.GRP_ID").alias("GRP_ID"),
        F.col("Ink_IdsEdwIdsOpsGrpWorkUnitXrefDExtr_InABC.OPS_WORK_UNIT_ID").alias("OPS_WORK_UNIT_ID"),
        F.col("Ink_IdsEdwIdsOpsGrpWorkUnitXrefDExtr_InABC.USER_ID").alias("USER_ID"),
        F.col("Ink_IdsEdwIdsOpsGrpWorkUnitXrefDExtr_InABC.LAST_UPDT_DT_SK").alias("LAST_UPDT_DT_SK"),
        F.col("Ink_OpsWorkUnit_InABC.OPS_WORK_UNIT_SK").alias("OPS_WORK_UNIT_SK")
    )
)

df_Main_OutABC = (
    df_lkp_Codes
    .filter((F.col("GRP_SK") != 0) & (F.col("GRP_SK") != 1))
    .withColumn("GRP_SK", F.col("GRP_SK"))
    .withColumn("SRC_SYS_CD", F.lit("UWS"))
    .withColumn("GRP_ID", F.col("GRP_ID"))
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", F.lit(EDWRunCycleDate))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.lit(EDWRunCycleDate))
    .withColumn(
        "OPS_WORK_UNIT_SK",
        F.when(
            F.col("OPS_WORK_UNIT_SK").isNull()
            | (F.length(F.col("OPS_WORK_UNIT_SK").cast("string")) == 0),
            F.lit("UNK")
        ).otherwise(F.col("OPS_WORK_UNIT_SK").cast("string"))
    )
    .withColumn(
        "OPS_WORK_UNIT_ID",
        F.when(
            F.col("OPS_WORK_UNIT_ID").isNull()
            | (F.length(F.col("OPS_WORK_UNIT_ID")) == 0),
            F.lit("NA")
        ).otherwise(F.col("OPS_WORK_UNIT_ID"))
    )
    .withColumn("USER_ID", F.col("USER_ID"))
    .withColumn("LAST_UPDT_DT_SK", F.col("LAST_UPDT_DT_SK"))
)[
    [
        "GRP_SK",
        "SRC_SYS_CD",
        "GRP_ID",
        "CRT_RUN_CYC_EXCTN_DT_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        "OPS_WORK_UNIT_SK",
        "OPS_WORK_UNIT_ID",
        "USER_ID",
        "LAST_UPDT_DT_SK"
    ]
]

df_UNK_Out = spark.createDataFrame(
    [
        (
            0,
            "UNK",
            "UNK",
            "1753-01-01",
            "1753-01-01",
            0,
            "UNK",
            "UNK",
            "1753-01-01"
        )
    ],
    [
        "GRP_SK",
        "SRC_SYS_CD",
        "GRP_ID",
        "CRT_RUN_CYC_EXCTN_DT_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        "OPS_WORK_UNIT_SK",
        "OPS_WORK_UNIT_ID",
        "USER_ID",
        "LAST_UPDT_DT_SK"
    ]
)

df_NA_Out = spark.createDataFrame(
    [
        (
            1,
            "NA",
            "NA",
            "1753-01-01",
            "1753-01-01",
            1,
            "NA",
            "NA",
            "1753-01-01"
        )
    ],
    [
        "GRP_SK",
        "SRC_SYS_CD",
        "GRP_ID",
        "CRT_RUN_CYC_EXCTN_DT_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        "OPS_WORK_UNIT_SK",
        "OPS_WORK_UNIT_ID",
        "USER_ID",
        "LAST_UPDT_DT_SK"
    ]
)

df_Fnl_Main = df_Main_OutABC.unionByName(df_UNK_Out).unionByName(df_NA_Out)

df_Fnl_Main = df_Fnl_Main.select(
    "GRP_SK",
    "SRC_SYS_CD",
    "GRP_ID",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "OPS_WORK_UNIT_SK",
    "OPS_WORK_UNIT_ID",
    "USER_ID",
    "LAST_UPDT_DT_SK"
)

df_Fnl_Main = df_Fnl_Main.withColumn(
    "CRT_RUN_CYC_EXCTN_DT_SK",
    F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ")
).withColumn(
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ")
).withColumn(
    "LAST_UPDT_DT_SK",
    F.rpad(F.col("LAST_UPDT_DT_SK"), 10, " ")
)

write_files(
    df_Fnl_Main,
    f"{adls_path}/load/OPS_GRP_WORK_UNIT_XREF_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)