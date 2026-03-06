# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC **************************************************************************************
# MAGIC COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC CALLED BY:  OptumProvExtrSeq
# MAGIC 
# MAGIC DESCRIPTION:    Pulls data from Optum PBM.PROVNTWK file to load Provider Location Table
# MAGIC 
# MAGIC       
# MAGIC MODIFICATIONS:
# MAGIC Developer                                  Date                      Project/Altiris #                                                 Change Description                             Development Project                                Code Reviewer                 Date Reviewed
# MAGIC ---------------------                       ------------------      -------------------------------------------                                ---------------------------------------------------------      -------------------------------------------------------           -----------------------------------------      -------------------------   
# MAGIC Deepika C                            2021-05-24                        373781                                                        Initial Programming                                IntegrateDev2

# MAGIC Land into Seq File for the FKEY job
# MAGIC Insert Only the Newly generated Pkeys into the K Table
# MAGIC Read K_PROV_LOC Table to pull the Natural Keys and the Skey.
# MAGIC Inner Join primary key info with table info
# MAGIC New PKEYs are generated in this transformer. A database sequence will be used to create next PKEY value.
# MAGIC Remove Duplicates stage takes care of getting rid of duplicate rows with the same Natural key get into the PKEYing step.
# MAGIC A concious effort need to be made to evaluate which one of the duplicate set row to drop is a case by case basis decision.
# MAGIC Left Join on Natural Keys
# MAGIC Balancing snapshot of source table
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
SrcSysCd = get_widget_value('SrcSysCd','')
RunID = get_widget_value('RunID','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
CurrRunCycleDate = get_widget_value('CurrRunCycleDate','')
InFile = get_widget_value('InFile','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"""SELECT
PROV_ID,
PROV_ADDR_ID,
PROV_ADDR_TYP_CD,
PROV_ADDR_EFF_DT_SK as PROV_ADDR_EFF_DT,
SRC_SYS_CD,
CRT_RUN_CYC_EXCTN_SK,
PROV_LOC_SK
FROM {IDSOwner}.K_PROV_LOC
"""
df_db2_K_PROV_LOC_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_ds_CD_MPPNG_Data = spark.read.parquet(f"{adls_path}/ds/CD_MPPNG.parquet")

df_fltr_Cd_MppngData = df_ds_CD_MPPNG_Data.filter(
    (F.col("SRC_SYS_CD") == SrcSysCd)
    & (F.col("SRC_CD") == "P")
    & (F.col("TRGT_DOMAIN_NM") == "PROVIDER ADDRESS TYPE")
)
df_ref_ProvAddrTypeCdSK = df_fltr_Cd_MppngData.select("CD_MPPNG_SK", "SRC_SYS_CD")

schema_PullOptumProv = StructType([
    StructField("NABP", StringType(), True),
    StructField("NTNL_PROV_ID", StringType(), True),
    StructField("PDX_NM", StringType(), True),
    StructField("ADDR", StringType(), True),
    StructField("CITY", StringType(), True),
    StructField("CNTY", StringType(), True),
    StructField("ST", StringType(), True),
    StructField("ZIP_CD", StringType(), True),
    StructField("PHN_NO", StringType(), True),
    StructField("FAX_NO", StringType(), True),
    StructField("HR_IN_24", StringType(), True),
    StructField("EFF_DATE", StringType(), True),
    StructField("PRFRD_IN", StringType(), True),
    StructField("NETWORK_ID", StringType(), True)
])
df_PullOptumProv = (
    spark.read.format("csv")
    .option("header", "false")
    .option("sep", "\\s+")
    .schema(schema_PullOptumProv)
    .load(f"{adls_path_raw}/landing/{InFile}")
)

df_hf_rm_dup_optum_prov_loc = dedup_sort(df_PullOptumProv, ["NABP"], [])

df_BusinessRules_input = df_hf_rm_dup_optum_prov_loc.select(
    F.col("NABP"),
    F.col("NTNL_PROV_ID"),
    F.col("PDX_NM"),
    F.col("ADDR"),
    F.col("CITY"),
    F.col("CNTY"),
    F.col("ST"),
    F.col("ZIP_CD"),
    F.col("PHN_NO"),
    F.col("FAX_NO"),
    F.col("HR_IN_24"),
    F.col("EFF_DATE"),
    F.col("PRFRD_IN"),
    F.col("NETWORK_ID")
)

df_BusinessRules = (
    df_BusinessRules_input
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", F.lit(0))
    .withColumn("INSRT_UPDT_CD", F.lit("I"))
    .withColumn("DISCARD_IN", F.lit("N"))
    .withColumn("PASS_THRU_IN", F.lit("Y"))
    .withColumn(
        "FIRST_RECYC_DT",
        F.to_timestamp(
            F.concat_ws(
                "",
                F.date_format(F.lit(CurrRunCycleDate), "yyyyMMdd"),
                F.lit("000000")
            ),
            "yyyyMMddHHmmss"
        )
    )
    .withColumn("ERR_CT", F.lit(0))
    .withColumn("RECYCLE_CT", F.lit(0))
    .withColumn("SRC_SYS_CD", F.trim(F.lit(SrcSysCd)))
    .withColumn(
        "PRI_KEY_STRING",
        F.concat(
            F.lit(SrcSysCd), F.lit(";"),
            F.trim(F.col("NABP")), F.lit(";"),
            F.trim(F.col("NABP"))
        )
    )
    .withColumn("SRC_SYS_CD_SK", F.lit(SrcSysCdSk))
    .withColumn("PROV_ID", F.trim(F.col("NABP")))
    .withColumn("PROV_ADDR_ID", F.trim(F.col("NABP")))
    .withColumn("PROV_ADDR_TYP_CD", F.lit("P"))
    .withColumn("PROV_ADDR_EFF_DT", F.lit("1753-01-01"))
    .withColumn("PROV_ADDR_SK", F.lit(0))
    .withColumn("PROV_SK", F.lit(0))
    .withColumn("PRI_ADDR_IN", F.lit("Y"))
    .withColumn("REMIT_ADDR_IN", F.lit("Y"))
)

df_Lookup_joined = df_BusinessRules.alias("lkup").join(
    df_ref_ProvAddrTypeCdSK.alias("ref_ProvAddrTypeCdSK"),
    [
        F.col("lkup.SRC_SYS_CD") == F.col("ref_ProvAddrTypeCdSK.SRC_SYS_CD")
    ],
    "left"
)

df_Lookup = df_Lookup_joined.select(
    F.col("lkup.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("lkup.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("lkup.DISCARD_IN").alias("DISCARD_IN"),
    F.col("lkup.PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("lkup.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("lkup.ERR_CT").alias("ERR_CT"),
    F.col("lkup.RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("lkup.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lkup.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("lkup.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("lkup.PROV_ID").alias("PROV_ID"),
    F.col("lkup.PROV_ADDR_ID").alias("PROV_ADDR_ID"),
    F.col("lkup.PROV_ADDR_TYP_CD").alias("PROV_ADDR_TYP_CD"),
    F.col("lkup.PROV_ADDR_EFF_DT").alias("PROV_ADDR_EFF_DT"),
    F.col("lkup.PROV_ADDR_SK").alias("PROV_ADDR_SK"),
    F.col("lkup.PROV_SK").alias("PROV_SK"),
    F.col("lkup.PRI_ADDR_IN").alias("PRI_ADDR_IN"),
    F.col("lkup.REMIT_ADDR_IN").alias("REMIT_ADDR_IN"),
    F.col("ref_ProvAddrTypeCdSK.CD_MPPNG_SK").alias("CD_MPPNG_SK")
)

df_lnk_rm_dup_OptumProvLoc = df_Lookup.select(
    F.col("PROV_ID"),
    F.col("PROV_ADDR_ID"),
    F.lit("P").alias("PROV_ADDR_TYP_CD"),
    F.lit("1753-01-01").alias("PROV_ADDR_EFF_DT"),
    F.col("SRC_SYS_CD")
)

df_lnk_OptumProvLocPkey_All = df_Lookup.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    F.to_timestamp(
        F.concat_ws(
            "",
            F.date_format(F.lit(CurrRunCycleDate), "yyyyMMdd"),
            F.lit("000000")
        ),
        "yyyyMMddHHmmss"
    ).alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("PROV_ID"),
    F.col("PROV_ADDR_ID"),
    F.lit("P").alias("PROV_ADDR_TYP_CD"),
    F.lit("1753-01-01").alias("PROV_ADDR_EFF_DT"),
    F.lit(0).alias("PROV_ADDR_SK"),
    F.lit(0).alias("PROV_SK"),
    F.lit("Y").alias("PRI_ADDR_IN"),
    F.lit("Y").alias("REMIT_ADDR_IN")
)

df_Snapshot_File = df_Lookup.select(
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("PROV_ID").alias("PROV_ID"),
    F.col("PROV_ADDR_ID").alias("PROV_ADDR_ID"),
    F.col("CD_MPPNG_SK").alias("PROV_ADDR_TYP_CD_SK"),
    F.lit("1753-01-01").alias("PROV_ADDR_EFF_DT_SK")
)

df_Snapshot_File_final = df_Snapshot_File.select(
    F.col("SRC_SYS_CD_SK"),
    F.col("PROV_ID"),
    F.col("PROV_ADDR_ID"),
    F.col("PROV_ADDR_TYP_CD_SK"),
    F.rpad(F.col("PROV_ADDR_EFF_DT_SK"), 10, " ").alias("PROV_ADDR_EFF_DT_SK")
)

write_files(
    df_Snapshot_File_final,
    f"{adls_path}/load/B_PROV_LOC.OPTUM.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

df_rdup_Natural_Keys = dedup_sort(
    df_lnk_rm_dup_OptumProvLoc,
    ["PROV_ID", "PROV_ADDR_ID", "PROV_ADDR_TYP_CD", "PROV_ADDR_EFF_DT", "SRC_SYS_CD"],
    []
)

df_jn_ProvLoc_left_input = df_rdup_Natural_Keys.select(
    "PROV_ID",
    "PROV_ADDR_ID",
    "PROV_ADDR_TYP_CD",
    "PROV_ADDR_EFF_DT",
    "SRC_SYS_CD"
)

df_jn_ProvLoc = df_jn_ProvLoc_left_input.alias("lnk_rm_dup_DataOut").join(
    df_db2_K_PROV_LOC_In.alias("lnk_KProvLoc_extr"),
    [
        F.col("lnk_rm_dup_DataOut.PROV_ID") == F.col("lnk_KProvLoc_extr.PROV_ID"),
        F.col("lnk_rm_dup_DataOut.PROV_ADDR_ID") == F.col("lnk_KProvLoc_extr.PROV_ADDR_ID"),
        F.col("lnk_rm_dup_DataOut.PROV_ADDR_TYP_CD") == F.col("lnk_KProvLoc_extr.PROV_ADDR_TYP_CD"),
        F.col("lnk_rm_dup_DataOut.PROV_ADDR_EFF_DT") == F.col("lnk_KProvLoc_extr.PROV_ADDR_EFF_DT"),
        F.col("lnk_rm_dup_DataOut.SRC_SYS_CD") == F.col("lnk_KProvLoc_extr.SRC_SYS_CD")
    ],
    "left"
)

df_jn_ProvLoc = df_jn_ProvLoc.select(
    F.col("lnk_rm_dup_DataOut.PROV_ID").alias("PROV_ID"),
    F.col("lnk_rm_dup_DataOut.PROV_ADDR_ID").alias("PROV_ADDR_ID"),
    F.col("lnk_rm_dup_DataOut.PROV_ADDR_TYP_CD").alias("PROV_ADDR_TYP_CD"),
    F.col("lnk_rm_dup_DataOut.PROV_ADDR_EFF_DT").alias("PROV_ADDR_EFF_DT"),
    F.col("lnk_rm_dup_DataOut.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnk_KProvLoc_extr.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("lnk_KProvLoc_extr.PROV_LOC_SK").alias("PROV_LOC_SK")
)

df_enriched = df_jn_ProvLoc.withColumn("original_PROV_LOC_SK", F.col("PROV_LOC_SK"))
df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"PROV_LOC_SK",<schema>,<secret_name>)
df_enriched = df_enriched.withColumn(
    "CRT_RUN_CYC_EXCTN_SK",
    F.when(F.col("original_PROV_LOC_SK").isNull(), F.lit(CurrRunCycle))
     .otherwise(F.col("CRT_RUN_CYC_EXCTN_SK"))
)
df_enriched = df_enriched.withColumn(
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    F.lit(CurrRunCycle)
)

df_lnk_PKEYxfmOut = df_enriched.select(
    F.col("PROV_ID"),
    F.col("PROV_ADDR_ID"),
    F.col("PROV_ADDR_TYP_CD"),
    F.col("PROV_ADDR_EFF_DT"),
    F.col("SRC_SYS_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("PROV_LOC_SK")
)

df_lnk_KProvLoc_new = df_enriched.filter(
    F.col("original_PROV_LOC_SK").isNull()
).select(
    F.col("PROV_ID"),
    F.col("PROV_ADDR_ID"),
    F.col("PROV_ADDR_TYP_CD"),
    F.col("PROV_ADDR_EFF_DT").alias("PROV_ADDR_EFF_DT_SK"),
    F.col("SRC_SYS_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("PROV_LOC_SK")
)

df_db2_K_PROV_LOC_load = df_lnk_KProvLoc_new

drop_sql = f"""DROP TABLE IF EXISTS STAGING.OptumProvLocExtr_db2_K_PROV_LOC_load_temp"""
execute_dml(drop_sql, jdbc_url, jdbc_props)

(
    df_db2_K_PROV_LOC_load.write
    .format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("dbtable", "STAGING.OptumProvLocExtr_db2_K_PROV_LOC_load_temp")
    .mode("append")
    .save()
)

merge_sql = f"""MERGE INTO {IDSOwner}.K_PROV_LOC AS T
USING STAGING.OptumProvLocExtr_db2_K_PROV_LOC_load_temp AS S
ON (
 T.PROV_ID = S.PROV_ID
 AND T.PROV_ADDR_ID = S.PROV_ADDR_ID
 AND T.PROV_ADDR_TYP_CD = S.PROV_ADDR_TYP_CD
 AND T.PROV_ADDR_EFF_DT_SK = S.PROV_ADDR_EFF_DT_SK
 AND T.SRC_SYS_CD = S.SRC_SYS_CD
)
WHEN MATCHED THEN UPDATE SET
 T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK,
 T.PROV_LOC_SK = S.PROV_LOC_SK
WHEN NOT MATCHED THEN INSERT
 (PROV_ID, PROV_ADDR_ID, PROV_ADDR_TYP_CD, PROV_ADDR_EFF_DT_SK, SRC_SYS_CD, CRT_RUN_CYC_EXCTN_SK, PROV_LOC_SK)
 VALUES
 (S.PROV_ID, S.PROV_ADDR_ID, S.PROV_ADDR_TYP_CD, S.PROV_ADDR_EFF_DT_SK, S.SRC_SYS_CD, S.CRT_RUN_CYC_EXCTN_SK, S.PROV_LOC_SK);"""
execute_dml(merge_sql, jdbc_url, jdbc_props)

df_jn_PKEYs = df_lnk_PKEYxfmOut.alias("lnk_PKEYxfmOut").join(
    df_lnk_OptumProvLocPkey_All.alias("lnk_OptumProvLocPkey_All"),
    [
        F.col("lnk_PKEYxfmOut.PROV_ID") == F.col("lnk_OptumProvLocPkey_All.PROV_ID"),
        F.col("lnk_PKEYxfmOut.PROV_ADDR_ID") == F.col("lnk_OptumProvLocPkey_All.PROV_ADDR_ID"),
        F.col("lnk_PKEYxfmOut.PROV_ADDR_TYP_CD") == F.col("lnk_OptumProvLocPkey_All.PROV_ADDR_TYP_CD"),
        F.col("lnk_PKEYxfmOut.PROV_ADDR_EFF_DT") == F.col("lnk_OptumProvLocPkey_All.PROV_ADDR_EFF_DT"),
        F.col("lnk_PKEYxfmOut.SRC_SYS_CD") == F.col("lnk_OptumProvLocPkey_All.SRC_SYS_CD")
    ],
    "inner"
)

df_jn_PKEYs = df_jn_PKEYs.select(
    F.col("lnk_OptumProvLocPkey_All.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("lnk_OptumProvLocPkey_All.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("lnk_PKEYxfmOut.PROV_LOC_SK").alias("PROV_LOC_SK"),
    F.col("lnk_PKEYxfmOut.PROV_ID").alias("PROV_ID"),
    F.col("lnk_PKEYxfmOut.PROV_ADDR_ID").alias("PROV_ADDR_ID"),
    F.col("lnk_PKEYxfmOut.PROV_ADDR_TYP_CD").alias("PROV_ADDR_TYP_CD"),
    F.col("lnk_PKEYxfmOut.PROV_ADDR_EFF_DT").alias("PROV_ADDR_EFF_DT"),
    F.col("lnk_PKEYxfmOut.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnk_PKEYxfmOut.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("lnk_PKEYxfmOut.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("lnk_OptumProvLocPkey_All.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("lnk_OptumProvLocPkey_All.PROV_ADDR_SK").alias("PROV_ADDR_SK"),
    F.col("lnk_OptumProvLocPkey_All.PROV_SK").alias("PROV_SK"),
    F.col("lnk_OptumProvLocPkey_All.PRI_ADDR_IN").alias("PRI_ADDR_IN"),
    F.col("lnk_OptumProvLocPkey_All.REMIT_ADDR_IN").alias("REMIT_ADDR_IN")
)

df_Transformer_64 = df_jn_PKEYs.select(
    F.col("PRI_KEY_STRING"),
    F.col("FIRST_RECYC_DT"),
    F.col("PROV_LOC_SK"),
    F.trim(F.col("PROV_ID")).alias("PROV_ID"),
    F.trim(F.col("PROV_ADDR_ID")).alias("PROV_ADDR_ID"),
    F.trim(F.col("PROV_ADDR_TYP_CD")).alias("PROV_ADDR_TYP_CD"),
    F.trim(F.col("PROV_ADDR_EFF_DT")).alias("PROV_ADDR_EFF_DT"),
    F.trim(F.col("SRC_SYS_CD")).alias("SRC_SYS_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("SRC_SYS_CD_SK"),
    F.col("PROV_ADDR_SK"),
    F.col("PROV_SK"),
    F.col("PRI_ADDR_IN"),
    F.col("REMIT_ADDR_IN")
)

df_final_OptumProvLocExtr = df_Transformer_64.select(
    F.col("PRI_KEY_STRING"),
    F.col("FIRST_RECYC_DT"),
    F.col("PROV_LOC_SK"),
    F.trim(F.col("PROV_ID")).alias("PROV_ID"),
    F.trim(F.col("PROV_ADDR_ID")).alias("PROV_ADDR_ID"),
    F.rpad(F.col("PROV_ADDR_TYP_CD"), 12, " ").alias("PROV_ADDR_TYP_CD"),
    F.rpad(F.col("PROV_ADDR_EFF_DT"), 10, " ").alias("PROV_ADDR_EFF_DT"),
    F.trim(F.col("SRC_SYS_CD")).alias("SRC_SYS_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("SRC_SYS_CD_SK"),
    F.col("PROV_ADDR_SK"),
    F.col("PROV_SK"),
    F.rpad(F.col("PRI_ADDR_IN"), 1, " ").alias("PRI_ADDR_IN"),
    F.rpad(F.col("REMIT_ADDR_IN"), 1, " ").alias("REMIT_ADDR_IN")
)

write_files(
    df_final_OptumProvLocExtr,
    f"{adls_path}/key/PROV_LOC.{SrcSysCd}.pkey.{RunID}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)