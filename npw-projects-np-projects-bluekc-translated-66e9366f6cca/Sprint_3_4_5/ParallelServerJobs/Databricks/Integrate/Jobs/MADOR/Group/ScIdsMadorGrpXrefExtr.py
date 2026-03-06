# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2024 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC CALLED BY: ScIdsMadorXrefCovLoadSeq
# MAGIC               
# MAGIC PROCESSING: 
# MAGIC   
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                        Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------               --------------------------------       -------------------------------   ----------------------------       
# MAGIC Revathi BoojiReddy 2024-10-24      US 629049             Original Development                                                      IntegrateDev1               Jeyaprasanna             2024-12-18


# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import Window
from pyspark.sql.functions import col, lit, regexp_replace, upper, substring, row_number, concat
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


RunID = get_widget_value("RunID","")
IDSOwner = get_widget_value("IDSOwner","")
EDWOwner = get_widget_value("EDWOwner","")
TaxYear = get_widget_value("TaxYear","")
edw_secret_name = get_widget_value("edw_secret_name","")
ids_secret_name = get_widget_value("ids_secret_name","")

jdbc_url_EDW, jdbc_props_EDW = get_db_config(edw_secret_name)
query_DB2_GRP_D = f"""Select 
GRP_SK,
GRP_ID,
GRP_NM,
trim(replace(replace(replace(replace(replace(replace(GRP_NM, ' ', ''), 'INC', ''), 'LLC', ''), ',', ''), '''',''), '-', '')) as GRP_NM_LKP,
'1' as Dummy
from {EDWOwner}.GRP_D
where GRP_CLNT_ID = 'SC' and GRP_STTUS_CD = 'ACTV'"""
df_DB2_GRP_D = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_EDW)
    .options(**jdbc_props_EDW)
    .option("query", query_DB2_GRP_D)
    .load()
)

jdbc_url_IDS, jdbc_props_IDS = get_db_config(ids_secret_name)
query_DB2_P_MA_DOR_GRP_XREF = f"""SELECT 
trim(XR.CES_CLNT_ID) as CLNT_NO,
(Select MAX(MA_DOR_GRP_ID) from {IDSOwner}.P_MA_DOR_GRP_XREF) as MA_DOR_GRP_ID_MAX
FROM {IDSOwner}.P_MA_DOR_GRP_XREF XR"""
df_DB2_P_MA_DOR_GRP_XREF = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_IDS)
    .options(**jdbc_props_IDS)
    .option("query", query_DB2_P_MA_DOR_GRP_XREF)
    .load()
)

schema_seq_GRP_MA_DOR_XREF = StructType([
    StructField("ACCT_NM", StringType(), True),
    StructField("CLNT_NO", StringType(), True),
    StructField("GroupLeader", StringType(), True),
    StructField("ADDR_LN_1", StringType(), True),
    StructField("ADDR_LN_2", StringType(), True),
    StructField("ADDR_CITY_NM", StringType(), True),
    StructField("ADDR_ST_NM", StringType(), True),
    StructField("ADDR_ST_CD", StringType(), True)
])
df_seq_GRP_MA_DOR_XREF = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "false")
    .option("sep", ",")
    .option("quote", "\"")
    .option("nullValue", None)
    .schema(schema_seq_GRP_MA_DOR_XREF)
    .load(f"{adls_path_raw}/landing/NTNL_ALNCE_MADOR_GRP.csv")
)

df_xfm_Format_Grp_Acc_Data = (
    df_seq_GRP_MA_DOR_XREF
    .withColumn("StageVar3", trim(col("ACCT_NM")))
    .withColumn("StageVar4", regexp_replace(col("StageVar3"), ",", ""))
    .withColumn("StageVar5", regexp_replace(col("StageVar4"), "\\.", ""))
    .withColumn("StageVar6", regexp_replace(col("StageVar5"), "'", ""))
    .withColumn("StageVar7", regexp_replace(col("StageVar6"), "-", ""))
    .withColumn("StageVar8", regexp_replace(col("StageVar7"), "Inc", ""))
    .withColumn("StageVar9", regexp_replace(col("StageVar8"), "LLC", ""))
    .withColumn("StageVar10", upper(col("StageVar9")))
    .withColumn("CLNT_NO", trim(col("CLNT_NO")))
    .withColumn("ACCT_NM", col("StageVar10"))
    .withColumn("Dummy", lit("1"))
)

df_xfm_Format_Grp_Acc_Data = df_xfm_Format_Grp_Acc_Data.select("CLNT_NO","ACCT_NM","Dummy")

df_Lookup_Clnt = (
    df_xfm_Format_Grp_Acc_Data.alias("lnk_xfm")
    .join(
        df_DB2_P_MA_DOR_GRP_XREF.alias("lnk_Xref"),
        col("lnk_xfm.CLNT_NO") == col("lnk_Xref.CLNT_NO"),
        how="left"
    )
    .select(
        col("lnk_xfm.CLNT_NO").alias("CLNT_NO"),
        col("lnk_xfm.ACCT_NM").alias("ACCT_NM"),
        col("lnk_xfm.Dummy").alias("Dummy"),
        col("lnk_Xref.MA_DOR_GRP_ID_MAX").alias("MA_DOR_GRP_ID_MAX")
    )
)

df_lnk_unmatch = (
    df_Lookup_Clnt
    .filter(col("MA_DOR_GRP_ID_MAX").isNull())
    .select(
        col("CLNT_NO"),
        col("ACCT_NM").alias("ACCT_NM"),
        col("MA_DOR_GRP_ID_MAX"),
        col("Dummy"),
        lit("Missing Groups in P_MA_DOR_GRP_XREF").alias("MissingGroups")
    )
)

df_lnk_match_max = (
    df_Lookup_Clnt
    .filter(col("MA_DOR_GRP_ID_MAX").isNotNull())
    .select(
        col("CLNT_NO"),
        col("ACCT_NM").alias("ACCT_NM"),
        col("MA_DOR_GRP_ID_MAX"),
        col("Dummy"),
        lit("Missing Groups in P_MA_DOR_GRP_XREF").alias("MissingGroups")
    )
)

df_LkupGrp_1 = (
    df_lnk_unmatch.alias("unm")
    .join(
        df_lnk_match_max.alias("mat"),
        col("unm.Dummy") == col("mat.Dummy"),
        "inner"
    )
)

df_LkupGrp = (
    df_LkupGrp_1.alias("lu")
    .join(
        df_DB2_GRP_D.alias("ref_Grp_D"),
        (col("lu.ACCT_NM") == col("ref_Grp_D.GRP_NM_LKP")) & (col("lu.Dummy") == col("ref_Grp_D.Dummy")),
        "left"
    )
    .select(
        col("lu.CLNT_NO").alias("CLNT_NO"),
        col("ref_Grp_D.GRP_NM").alias("GRP_NM"),
        col("mat.MA_DOR_GRP_ID_MAX").alias("MA_DOR_GRP_ID_MAX"),
        col("ref_Grp_D.GRP_SK").alias("GRP_SK"),
        col("ref_Grp_D.GRP_ID").alias("GRP_ID"),
        col("unm.MissingGroups").alias("MissingGroups")
    )
)

df_lnk_grp_nm_rej = (
    df_lnk_unmatch.alias("unm")
    .join(
        df_lnk_match_max.alias("mat"),
        col("unm.Dummy") == col("mat.Dummy"),
        "left_anti"
    )
    .select(
        col("unm.CLNT_NO").alias("CLNT_NO"),
        col("unm.ACCT_NM").alias("GRP_NM"),
        col("unm.MA_DOR_GRP_ID_MAX").alias("MA_DOR_GRP_ID_MAX"),
        lit(None).cast(StringType()).alias("GRP_SK"),
        lit(None).cast(StringType()).alias("GRP_ID"),
        col("unm.MissingGroups").alias("MissingGroups")
    )
)

df_lnk_grp_nm_rej = df_lnk_grp_nm_rej.select(
    "CLNT_NO",
    "GRP_NM",
    "MA_DOR_GRP_ID_MAX",
    "GRP_SK",
    "GRP_ID",
    "MissingGroups"
)

df_lnk_grp_nm_rej = (
    df_lnk_grp_nm_rej
    .withColumn("CLNT_NO", rpad(col("CLNT_NO"), <...>, " "))
    .withColumn("GRP_NM", rpad(col("GRP_NM"), <...>, " "))
    .withColumn("MA_DOR_GRP_ID_MAX", rpad(col("MA_DOR_GRP_ID_MAX"), <...>, " "))
    .withColumn("GRP_SK", rpad(col("GRP_SK"), <...>, " "))
    .withColumn("GRP_ID", rpad(col("GRP_ID"), <...>, " "))
    .withColumn("MissingGroups", rpad(col("MissingGroups"), <...>, " "))
)

write_files(
    df_lnk_grp_nm_rej,
    f"{adls_path_publish}/external/MissingGroups.csv",
    delimiter="|",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

windowSpec = Window.orderBy(lit(1))
df_xfm_logic = (
    df_LkupGrp
    .withColumn("StageVar", col("MA_DOR_GRP_ID_MAX"))
    .withColumn("StageVar1", row_number().over(windowSpec) - lit(1))
    .withColumn("StageVar2", concat(substring(col("StageVar"), 7, 6), col("StageVar1")))
    .withColumn("CES_CLNT_ID", col("CLNT_NO"))
    .withColumn("MA_DOR_GRP_ID", concat(lit("BCBSSC"), col("StageVar2")))
    .withColumn("GRP_ID", col("GRP_ID"))
    .withColumn("GRP_NM", col("GRP_NM"))
    .withColumn("GRP_SK", col("GRP_SK"))
    .select("CES_CLNT_ID","MA_DOR_GRP_ID","GRP_ID","GRP_NM","GRP_SK")
)

df_xfm_logic = (
    df_xfm_logic
    .withColumn("CES_CLNT_ID", rpad(col("CES_CLNT_ID"), <...>, " "))
    .withColumn("MA_DOR_GRP_ID", rpad(col("MA_DOR_GRP_ID"), <...>, " "))
    .withColumn("GRP_ID", rpad(col("GRP_ID"), <...>, " "))
    .withColumn("GRP_NM", rpad(col("GRP_NM"), <...>, " "))
    .withColumn("GRP_SK", rpad(col("GRP_SK"), <...>, " "))
)

write_files(
    df_xfm_logic,
    f"{adls_path}/load/P_MA_DOR_GRP_XREF.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)