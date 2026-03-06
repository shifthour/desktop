# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Copyright 2021 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC Job Name: LivongoEncClmProvExtr
# MAGIC Called By: LivongoClmExtrSeq
# MAGIC 
# MAGIC DESCRIPTION: Runs LivongoEncounter Claims  Provider extract.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer\(9)\(9)Date                 \(9)Project/Altiris #      \(9)Change Description\(9)\(9)\(9)\(9)\(9)Development Project \(9)Code Reviewer\(9)Date Reviewed       
# MAGIC =================================================================================================================================================================================
# MAGIC SravyaSree Yarlagadda\(9)2020-12- 21  \(9)  311337              \(9)Original Programming\(9)\(9)\(9)\(9)IntegrateDev2\(9)\(9)Manasa Andru\(9)2021-03-17          
# MAGIC Abhishek Pulluri\(9)\(9)2021-07-01\(9)\(9)\(9)Added BILL and SVC to CLM_PROV_ROLE_TYP_CD\(9)\(9)IntegrateDev2\(9)\(9)Ken Bradmon\(9)2021-07-06

# MAGIC Writing Sequential File to /key
# MAGIC Assign primary surrogate key
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, lit, when, isnull, rpad, concat
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../shared_containers/PrimaryKey/ClmProvPK
# COMMAND ----------

IDSOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")
SrcSysCdSK = get_widget_value("SrcSysCdSK","")
CurrentDate = get_widget_value("CurrentDate","")
SrcSysCd = get_widget_value("SrcSysCd","")
RunID = get_widget_value("RunID","")
RunCycle = get_widget_value("RunCycle","")
InFile_F = get_widget_value("InFile_F","")

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query_PROV = f"""
SELECT '1' AS FLAG,
       PROV.PROV_ID AS PROV_ID,
       PROV.TAX_ID AS TAX_ID,
       PROV.NTNL_PROV_ID AS NTNL_PROV_ID
  FROM {IDSOwner}.PROV PROV
       INNER JOIN {IDSOwner}.P_SRC_DOMAIN_TRNSLTN SRC_DOMAIN
          ON SRC_DOMAIN.TRGT_DOMAIN_TX = PROV.NTNL_PROV_ID
 WHERE SRC_DOMAIN.SRC_SYS_CD = 'LVNGHLTH'
   AND SRC_DOMAIN.DOMAIN_ID = 'LIVONGO_NPI'
   AND PROV.SRC_SYS_CD_SK IN (
          SELECT DISTINCT SRC_CD_SK
            FROM {IDSOwner}.CD_MPPNG CD
           WHERE CD.SRC_SYS_CD = 'FACETS'
       )
"""

df_PROV = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_PROV)
    .load()
)

df_PROV_dedup = dedup_sort(df_PROV, ["FLAG"], [])

schema_livongo = StructType([
    StructField("livongo_id", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("birth_date", StringType(), True),
    StructField("client_name", StringType(), True),
    StructField("member_id", StringType(), True),
    StructField("claim_code", StringType(), True),
    StructField("service_date", StringType(), True),
    StructField("quantity", StringType(), True)
])

df_LivongoEncClmLanding = (
    spark.read.format("csv")
    .option("header", "false")
    .option("quote", "\"")
    .schema(schema_livongo)
    .load(f"{adls_path}/verified/{InFile_F}")
)

df_livongo_dedup = dedup_sort(df_LivongoEncClmLanding, ["livongo_id"], [])

df_BusinessRules_PD = (
    df_livongo_dedup
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", lit(0))
    .withColumn("INSRT_UPDT_CD", lit("I"))
    .withColumn("DISCARD_IN", lit("N"))
    .withColumn("PASS_THRU_IN", lit("Y"))
    .withColumn("FIRST_RECYC_DT", lit(CurrentDate))
    .withColumn("ERR_CT", lit(0))
    .withColumn("RECYCLE_CT", lit(0))
    .withColumn("SRC_SYS_CD", lit(SrcSysCd))
    .withColumn("PRI_KEY_STRING", concat(lit(SrcSysCd), lit(";"), col("livongo_id"), lit(";"), lit("PD")))
    .withColumn("CLM_PROV_SK", lit(0))
    .withColumn("CLM_ID", col("livongo_id"))
    .withColumn("CLM_PROV_ROLE_TYP_CD", lit("PD"))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", lit(RunCycle))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", lit(RunCycle))
    .withColumn("PROV_ID", lit("1"))
    .withColumn("TAX_ID", lit("1"))
    .withColumn("NTNL_PROV_ID", lit("1"))
)

df_BusinessRules_BILL = (
    df_livongo_dedup
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", lit(0))
    .withColumn("INSRT_UPDT_CD", lit("I"))
    .withColumn("DISCARD_IN", lit("N"))
    .withColumn("PASS_THRU_IN", lit("Y"))
    .withColumn("FIRST_RECYC_DT", lit(CurrentDate))
    .withColumn("ERR_CT", lit(0))
    .withColumn("RECYCLE_CT", lit(0))
    .withColumn("SRC_SYS_CD", lit(SrcSysCd))
    .withColumn("PRI_KEY_STRING", concat(lit(SrcSysCd), lit(";"), col("livongo_id"), lit(";"), lit("PD")))
    .withColumn("CLM_PROV_SK", lit(0))
    .withColumn("CLM_ID", col("livongo_id"))
    .withColumn("CLM_PROV_ROLE_TYP_CD", lit("BILL"))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", lit(RunCycle))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", lit(RunCycle))
    .withColumn("PROV_ID", lit("1"))
    .withColumn("TAX_ID", lit("1"))
    .withColumn("NTNL_PROV_ID", lit("1"))
)

df_BusinessRules_SVC = (
    df_livongo_dedup
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", lit(0))
    .withColumn("INSRT_UPDT_CD", lit("I"))
    .withColumn("DISCARD_IN", lit("N"))
    .withColumn("PASS_THRU_IN", lit("Y"))
    .withColumn("FIRST_RECYC_DT", lit(CurrentDate))
    .withColumn("ERR_CT", lit(0))
    .withColumn("RECYCLE_CT", lit(0))
    .withColumn("SRC_SYS_CD", lit(SrcSysCd))
    .withColumn("PRI_KEY_STRING", concat(lit(SrcSysCd), lit(";"), col("livongo_id"), lit(";"), lit("PD")))
    .withColumn("CLM_PROV_SK", lit(0))
    .withColumn("CLM_ID", col("livongo_id"))
    .withColumn("CLM_PROV_ROLE_TYP_CD", lit("SVC"))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", lit(RunCycle))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", lit(RunCycle))
    .withColumn("PROV_ID", lit("1"))
    .withColumn("TAX_ID", lit("1"))
    .withColumn("NTNL_PROV_ID", lit("1"))
)

df_xfm_snapshot_PD = df_BusinessRules_PD.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "CLM_PROV_SK",
    "CLM_ID",
    "CLM_PROV_ROLE_TYP_CD",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "PROV_ID",
    "TAX_ID",
    "NTNL_PROV_ID"
)

df_Lnk_Snapshot_BILL = df_BusinessRules_BILL.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "CLM_PROV_SK",
    "CLM_ID",
    "CLM_PROV_ROLE_TYP_CD",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "PROV_ID",
    "TAX_ID",
    "NTNL_PROV_ID"
)

df_Lnk_Snapshot_SVC = df_BusinessRules_SVC.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "CLM_PROV_SK",
    "CLM_ID",
    "CLM_PROV_ROLE_TYP_CD",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "PROV_ID",
    "TAX_ID",
    "NTNL_PROV_ID"
)

df_xfm_snapshot = df_xfm_snapshot_PD.unionByName(df_Lnk_Snapshot_BILL).unionByName(df_Lnk_Snapshot_SVC)

df_snapshot_join = (
    df_xfm_snapshot.alias("xfm_snapshot")
    .join(
        df_PROV_dedup.alias("PROV_IDlkp"),
        col("xfm_snapshot.NTNL_PROV_ID") == col("PROV_IDlkp.FLAG"),
        "left"
    )
)

df_snapshot_SnapShot = df_snapshot_join.select(
    col("xfm_snapshot.CLM_ID").alias("CLM_ID"),
    col("xfm_snapshot.CLM_PROV_ROLE_TYP_CD").alias("CLM_PROV_ROLE_TYP_CD"),
    col("PROV_IDlkp.PROV_ID").alias("PROV_ID")
)

df_snapshot_AllCol = df_snapshot_join.select(
    lit(SrcSysCdSK).alias("SRC_SYS_CD_SK"),
    col("xfm_snapshot.CLM_ID").alias("CLM_ID"),
    col("xfm_snapshot.CLM_PROV_ROLE_TYP_CD").alias("CLM_PROV_ROLE_TYP_CD"),
    col("xfm_snapshot.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    col("xfm_snapshot.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    col("xfm_snapshot.DISCARD_IN").alias("DISCARD_IN"),
    col("xfm_snapshot.PASS_THRU_IN").alias("PASS_THRU_IN"),
    col("xfm_snapshot.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    col("xfm_snapshot.ERR_CT").alias("ERR_CT"),
    col("xfm_snapshot.RECYCLE_CT").alias("RECYCLE_CT"),
    col("xfm_snapshot.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("xfm_snapshot.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    col("xfm_snapshot.CLM_PROV_SK").alias("CLM_PROV_SK"),
    col("xfm_snapshot.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("xfm_snapshot.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    when(
        isnull(col("PROV_IDlkp.PROV_ID")) | (trim(col("PROV_IDlkp.PROV_ID")) == ""),
        lit("UNK")
    ).otherwise(trim(col("PROV_IDlkp.PROV_ID"))).alias("PROV_ID"),
    when(
        isnull(col("PROV_IDlkp.TAX_ID")) | (trim(col("PROV_IDlkp.TAX_ID")) == ""),
        lit("UNK")
    ).otherwise(trim(col("PROV_IDlkp.TAX_ID"))).alias("TAX_ID"),
    lit("NA").alias("SVC_FCLTY_LOC_NTNL_PROV_ID"),
    when(
        isnull(col("PROV_IDlkp.NTNL_PROV_ID")) | (trim(col("PROV_IDlkp.NTNL_PROV_ID")) == ""),
        lit("UNK")
    ).otherwise(trim(col("PROV_IDlkp.NTNL_PROV_ID"))).alias("NTNL_PROV_ID")
)

df_snapshot_xfm = df_snapshot_join.select(
    lit(SrcSysCdSK).alias("SRC_SYS_CD_SK"),
    col("xfm_snapshot.CLM_ID").alias("CLM_ID"),
    col("xfm_snapshot.CLM_PROV_ROLE_TYP_CD").alias("CLM_PROV_ROLE_TYP_CD")
)

params_ClmProvPK = {
    "CurrRunCycle": RunCycle,
    "SrcSysCd": SrcSysCd,
    "$IDSOwner": IDSOwner
}

df_Key, = ClmProvPK(df_snapshot_AllCol, df_snapshot_xfm, params_ClmProvPK)

df_Key_ordered = df_Key.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "CLM_PROV_SK",
    "SRC_SYS_CD_SK",
    "CLM_ID",
    "CLM_PROV_ROLE_TYP_CD",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "PROV_ID",
    "TAX_ID",
    "SVC_FCLTY_LOC_NTNL_PROV_ID",
    "NTNL_PROV_ID"
).withColumn(
    "INSRT_UPDT_CD", rpad(col("INSRT_UPDT_CD"), 10, " ")
).withColumn(
    "DISCARD_IN", rpad(col("DISCARD_IN"), 1, " ")
).withColumn(
    "PASS_THRU_IN", rpad(col("PASS_THRU_IN"), 1, " ")
).withColumn(
    "TAX_ID", rpad(col("TAX_ID"), 9, " ")
)

write_files(
    df_Key_ordered,
    f"{adls_path}/key/LivongoEncClmProvExtr.LivongoEncClmProv.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

df_Transformer = df_snapshot_SnapShot.withColumn(
    "CLM_PROV_ROLE_TYP_CD_SK",
    GetFkeyCodes(
        lit(SrcSysCd),
        lit(0),
        lit("CLAIM PROVIDER ROLE TYPE"),
        col("CLM_PROV_ROLE_TYP_CD"),
        lit("X")
    )
)

df_Transformer_out = df_Transformer.select(
    lit(SrcSysCdSK).alias("SRC_SYS_CD_SK"),
    rpad(col("CLM_ID"), 18, " ").alias("CLM_ID"),
    col("CLM_PROV_ROLE_TYP_CD_SK").alias("CLM_PROV_ROLE_TYP_CD_SK"),
    rpad(col("PROV_ID"), 12, " ").alias("PROV_ID")
)

write_files(
    df_Transformer_out,
    f"{adls_path}/load/B_CLM_PROV.{SrcSysCd}.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)