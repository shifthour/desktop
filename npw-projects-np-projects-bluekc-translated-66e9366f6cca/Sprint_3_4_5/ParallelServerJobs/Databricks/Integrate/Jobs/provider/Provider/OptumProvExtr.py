# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC **************************************************************************************
# MAGIC COPYRIGHT 2019 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:   OptumProvExtrSeq
# MAGIC 
# MAGIC DESCRIPTION:    Pulls data from Optum PBM.PROVNTWK file to load Provider Table
# MAGIC       
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                                  Date                      Project/Altiris #                                                 Change Description                             Development Project                                Code Reviewer                 Date Reviewed
# MAGIC ---------------------                       ------------------      -------------------------------------------                                ---------------------------------------------------------      -------------------------------------------------------           -----------------------------------------      -------------------------   
# MAGIC Deepika C                            2021-05-24                        373781                                                         Initial Programming                                 IntegrateDev2

# MAGIC Remove Duplicates stage takes care of getting rid of duplicate rows with the same Natural key get into the PKEYing step.
# MAGIC A concious effort need to be made to evaluate which one of the duplicate set row to drop is a case by case basis decision.
# MAGIC Land into Seq File for the FKEY job
# MAGIC New PKEYs are generated in this transformer. A database sequence will be used to create next PKEY value.
# MAGIC Read K_PROV Table to pull the Natural Keys and the Skey.
# MAGIC Insert Only the Newly generated Pkeys into the K Table
# MAGIC Left Join on Natural Keys
# MAGIC Inner Join primary key info with table info
# MAGIC Balancing snapshot of source table
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType
import pyspark.sql.functions as F
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# Retrieve parameter values
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
SrcSysCd = get_widget_value('SrcSysCd','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
RunID = get_widget_value('RunID','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
CurrRunCycleDate = get_widget_value('CurrRunCycleDate','')
InFile = get_widget_value('InFile','')

# ---------------------------------------------------------
# Stage: db2_K_PROV_In (DB2ConnectorPX) -> lnk_KProv_extr
# ---------------------------------------------------------
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
df_db2_K_PROV_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"SELECT PROV_ID, SRC_SYS_CD, CRT_RUN_CYC_EXCTN_SK, PROV_SK FROM {IDSOwner}.K_PROV")
    .load()
)
df_lnk_KProv_extr = df_db2_K_PROV_In

# ---------------------------------------------------------
# Stage: ds_CD_MPPNG_Data (PxDataSet) -> CdMppngExtr
# ---------------------------------------------------------
df_ds_CD_MPPNG_Data = spark.read.parquet(f"{adls_path}/ds/CD_MPPNG.parquet")
df_CdMppngExtr = df_ds_CD_MPPNG_Data

# ---------------------------------------------------------
# Stage: fltr_Cd_MppngData (PxFilter) -> ref_ProvEntyCdSK
# ---------------------------------------------------------
df_fltr_Cd_MppngData = df_CdMppngExtr.filter(
    (F.col("SRC_SYS_CD") == SrcSysCd) & 
    (F.col("SRC_CD") == "RX") & 
    (F.col("TRGT_DOMAIN_NM") == "PROVIDER ENTITY")
)
df_ref_ProvEntyCdSK = df_fltr_Cd_MppngData.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD")
)

# ---------------------------------------------------------
# Stage: PullOptumProv (PxSequentialFile) -> Extract
# ---------------------------------------------------------
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
    spark.read
    .option("header", "false")
    .option("delimiter", " ")
    .schema(schema_PullOptumProv)
    .csv(f"{adls_path_raw}/landing/{InFile}")
)
df_Extract = df_PullOptumProv

# ---------------------------------------------------------
# Stage: hf_rm_dup_optum_prov (PxRemDup) -> Strip
# ---------------------------------------------------------
df_Strip_dedup = dedup_sort(df_Extract, ["NABP"], [])
df_Strip = df_Strip_dedup

# ---------------------------------------------------------
# Stage: BusinessRules (CTransformerStage) -> lkup
# ---------------------------------------------------------
df_enriched = df_Strip

df_enriched = df_enriched.withColumn(
    "PRI_KEY_STRING",
    F.concat(F.lit(SrcSysCd), F.lit(";"), trim(F.col("NABP")))
)
df_enriched = df_enriched.withColumn(
    "FIRST_RECYC_DT",
    F.to_timestamp(F.concat(F.lit(CurrRunCycleDate), F.lit("000000")), "yyyyMMddHHmmss")
)
df_enriched = df_enriched.withColumn("PROV_ID", trim(F.col("NABP")))
df_enriched = df_enriched.withColumn("SRC_SYS_CD", trim(F.lit(SrcSysCd)))
df_enriched = df_enriched.withColumn("JOB_EXCTN_RCRD_ERR_SK", F.lit(0))
df_enriched = df_enriched.withColumn("INSRT_UPDT_CD", F.lit("I"))
df_enriched = df_enriched.withColumn("DISCARD_IN", F.lit("N"))
df_enriched = df_enriched.withColumn("PASS_THRU_IN", F.lit("Y"))
df_enriched = df_enriched.withColumn("ERR_CT", F.lit(0))
df_enriched = df_enriched.withColumn("RECYCLE_CT", F.lit(0))
df_enriched = df_enriched.withColumn("CMN_PRCT", F.lit("NA"))
df_enriched = df_enriched.withColumn("PROV_BILL_SVC_SK", F.lit("NA"))
df_enriched = df_enriched.withColumn("REL_GRP_PROV", F.lit("NA"))
df_enriched = df_enriched.withColumn("REL_IPA_PROV", F.lit("NA"))
df_enriched = df_enriched.withColumn("PROV_CAP_PAYMT_EFT_METH_CD", F.lit("NA"))
df_enriched = df_enriched.withColumn("PROV_CLM_PAYMT_EFT_METH_CD", F.lit("NA"))
df_enriched = df_enriched.withColumn("PROV_CLM_PAYMT_METH_CD", F.lit("NA"))
df_enriched = df_enriched.withColumn("PROV_ENTY_CD", F.lit("RX"))
df_enriched = df_enriched.withColumn("PROV_FCLTY_TYP_CD", F.lit("NA"))
df_enriched = df_enriched.withColumn("PROV_PRCTC_TYP_CD", F.lit("NA"))
df_enriched = df_enriched.withColumn("PROV_SVC_CAT_CD", F.lit("NA"))
df_enriched = df_enriched.withColumn("PROV_SPEC_CD", F.lit("NA"))
df_enriched = df_enriched.withColumn("PROV_STTUS_CD", F.lit("NA"))
df_enriched = df_enriched.withColumn("PROV_TERM_RSN_CD", F.lit("NA"))
df_enriched = df_enriched.withColumn("PROV_TYP_CD", F.lit("0012"))
df_enriched = df_enriched.withColumn(
    "TERM_DT",
    F.to_timestamp(F.lit("21991231000000"), "yyyyMMddHHmmss")
)
df_enriched = df_enriched.withColumn(
    "PAYMT_HOLD_DT",
    F.to_timestamp(F.lit("21991231000000"), "yyyyMMddHHmmss")
)
df_enriched = df_enriched.withColumn("CLRNGHOUSE_ID", F.lit("NA"))
df_enriched = df_enriched.withColumn("EDI_DEST_ID", F.lit("NA"))
df_enriched = df_enriched.withColumn("EDI_DEST_QUAL", F.lit("NA"))
df_enriched = df_enriched.withColumn(
    "NTNL_PROV_ID",
    F.when(
        F.length(trim(F.col("NTNL_PROV_ID"))) == 0, "NA"
    ).otherwise(
        trim(F.col("NTNL_PROV_ID"))
    )
)
df_enriched = df_enriched.withColumn("PROV_ADDR_ID", trim(F.col("NABP")))
df_enriched = df_enriched.withColumn(
    "PROV_NM",
    F.when(
        (F.length(trim(F.col("PDX_NM"))) == 0) | (F.col("PDX_NM").isNull()),
        F.lit(None)
    ).otherwise(trim(F.col("PDX_NM")))
)
df_enriched = df_enriched.withColumn("TAX_ID", F.lit("NA"))
df_enriched = df_enriched.withColumn("TXNMY_CD", F.lit("UNK"))
df_enriched = df_enriched.withColumn("SRC_SYS_CD_SK", F.lit(SrcSysCdSk))

df_lkup = (
    df_enriched.alias("lkup")
    .withColumn("CLM_LN_PROC_CD_MOD_ORDNL_CD_SK", F.lit(None).cast(StringType()))
)

# ---------------------------------------------------------
# Stage: Lookup (PxLookup) -> Output
# (Left join on the contrived condition in the JSON)
# ---------------------------------------------------------
df_Lookup = df_lkup.join(
    df_ref_ProvEntyCdSK.alias("ref_ProvEntyCdSK"),
    (
        (F.col("lkup.CLM_LN_PROC_CD_MOD_ORDNL_CD_SK") == F.col("ref_ProvEntyCdSK.CD_MPPNG_SK")) &
        (F.col("lkup.SRC_SYS_CD") == F.col("ref_ProvEntyCdSK.SRC_SYS_CD"))
    ),
    "left"
)

df_Output = df_Lookup.select(
    F.col("lkup.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("lkup.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("lkup.PROV_ID").alias("PROV_ID"),
    F.col("lkup.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lkup.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("lkup.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("lkup.DISCARD_IN").alias("DISCARD_IN"),
    F.col("lkup.PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("lkup.ERR_CT").alias("ERR_CT"),
    F.col("lkup.RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("lkup.CMN_PRCT").alias("CMN_PRCT"),
    F.col("lkup.PROV_BILL_SVC_SK").alias("PROV_BILL_SVC_SK"),
    F.col("lkup.REL_GRP_PROV").alias("REL_GRP_PROV"),
    F.col("lkup.REL_IPA_PROV").alias("REL_IPA_PROV"),
    F.col("lkup.PROV_CAP_PAYMT_EFT_METH_CD").alias("PROV_CAP_PAYMT_EFT_METH_CD"),
    F.col("lkup.PROV_CLM_PAYMT_EFT_METH_CD").alias("PROV_CLM_PAYMT_EFT_METH_CD"),
    F.col("lkup.PROV_CLM_PAYMT_METH_CD").alias("PROV_CLM_PAYMT_METH_CD"),
    F.col("lkup.PROV_ENTY_CD").alias("PROV_ENTY_CD"),
    F.col("lkup.PROV_FCLTY_TYP_CD").alias("PROV_FCLTY_TYP_CD"),
    F.col("lkup.PROV_PRCTC_TYP_CD").alias("PROV_PRCTC_TYP_CD"),
    F.col("lkup.PROV_SVC_CAT_CD").alias("PROV_SVC_CAT_CD"),
    F.col("lkup.PROV_SPEC_CD").alias("PROV_SPEC_CD"),
    F.col("lkup.PROV_STTUS_CD").alias("PROV_STTUS_CD"),
    F.col("lkup.PROV_TERM_RSN_CD").alias("PROV_TERM_RSN_CD"),
    F.col("lkup.PROV_TYP_CD").alias("PROV_TYP_CD"),
    F.col("lkup.TERM_DT").alias("TERM_DT"),
    F.col("lkup.PAYMT_HOLD_DT").alias("PAYMT_HOLD_DT"),
    F.col("lkup.CLRNGHOUSE_ID").alias("CLRNGHOUSE_ID"),
    F.col("lkup.EDI_DEST_ID").alias("EDI_DEST_ID"),
    F.col("lkup.EDI_DEST_QUAL").alias("EDI_DEST_QUAL"),
    F.col("lkup.NTNL_PROV_ID").alias("NTNL_PROV_ID"),
    F.col("lkup.PROV_ADDR_ID").alias("PROV_ADDR_ID"),
    F.col("lkup.PROV_NM").alias("PROV_NM"),
    F.col("lkup.TAX_ID").alias("TAX_ID"),
    F.col("lkup.TXNMY_CD").alias("TXNMY_CD"),
    F.col("lkup.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("ref_ProvEntyCdSK.CD_MPPNG_SK").alias("CD_MPPNG_SK")
)

df_Transform = df_Output

# ---------------------------------------------------------
# Stage: Transform (CTransformerStage) -> 
#        1) lnk_rm_dup_OptumProv -> rdp_NaturalKeys
#        2) lnk_OptumProvPkey_All -> jn_PKEYs
#        3) Snapshot -> Snapshot_File
# ---------------------------------------------------------
df_lnk_rm_dup_OptumProv = df_Transform.select(
    F.col("PROV_ID").alias("PROV_ID"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD")
)

df_lnk_OptumProvPkey_All = df_Transform.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD"),
    F.col("DISCARD_IN"),
    F.col("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("PROV_ID"),
    F.col("CMN_PRCT"),
    F.col("PROV_BILL_SVC_SK"),
    F.col("REL_GRP_PROV"),
    F.col("REL_IPA_PROV"),
    F.col("PROV_CAP_PAYMT_EFT_METH_CD"),
    F.col("PROV_CLM_PAYMT_EFT_METH_CD"),
    F.col("PROV_CLM_PAYMT_METH_CD"),
    F.col("PROV_ENTY_CD"),
    F.col("PROV_FCLTY_TYP_CD"),
    F.col("PROV_PRCTC_TYP_CD"),
    F.col("PROV_SVC_CAT_CD"),
    F.col("PROV_SPEC_CD"),
    F.col("PROV_STTUS_CD"),
    F.col("PROV_TERM_RSN_CD"),
    F.col("PROV_TYP_CD"),
    F.col("TERM_DT"),
    F.col("PAYMT_HOLD_DT"),
    F.col("CLRNGHOUSE_ID"),
    F.col("EDI_DEST_ID"),
    F.col("EDI_DEST_QUAL"),
    F.col("NTNL_PROV_ID"),
    F.col("PROV_ADDR_ID"),
    F.col("PROV_NM"),
    F.col("TAX_ID"),
    F.col("TXNMY_CD"),
    F.col("SRC_SYS_CD_SK"),
    F.col("CD_MPPNG_SK")
)

df_Snapshot = df_Transform.select(
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("PROV_ID").alias("PROV_ID"),
    F.lit(1).alias("CMN_PRCT_SK"),
    F.lit(1).alias("REL_GRP_PROV_SK"),
    F.lit(1).alias("REL_IPA_PROV_SK"),
    F.col("CD_MPPNG_SK").alias("PROV_ENTY_CD_SK")
)

# ---------------------------------------------------------
# Stage: Snapshot_File (PxSequentialFile)
# ---------------------------------------------------------
# Apply any padding for final writing if needed; 
# here, no lengths were explicitly declared for these snapshot columns, 
# so writing them directly:
write_files(
    df_Snapshot,
    f"{adls_path}/load/B_PROV.OPTUM.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# ---------------------------------------------------------
# Stage: rdp_NaturalKeys (PxRemDup) -> lnk_rm_dup_DataOut
# ---------------------------------------------------------
df_rdp_NaturalKeys_dedup = dedup_sort(df_lnk_rm_dup_OptumProv, ["PROV_ID", "SRC_SYS_CD"], [])
df_lnk_rm_dup_DataOut = df_rdp_NaturalKeys_dedup.select(
    F.col("PROV_ID"),
    F.col("SRC_SYS_CD")
)

# ---------------------------------------------------------
# Stage: jn_Prov (PxJoin) -> lnk_Prov_JoinOut
#            operator=leftouterjoin on (PROV_ID, SRC_SYS_CD)
# ---------------------------------------------------------
df_jn_Prov = df_lnk_rm_dup_DataOut.alias("lnk_rm_dup_DataOut").join(
    df_lnk_KProv_extr.alias("lnk_KProv_extr"),
    (
        (F.col("lnk_rm_dup_DataOut.PROV_ID") == F.col("lnk_KProv_extr.PROV_ID")) &
        (F.col("lnk_rm_dup_DataOut.SRC_SYS_CD") == F.col("lnk_KProv_extr.SRC_SYS_CD"))
    ),
    "left"
)

df_lnk_Prov_JoinOut = df_jn_Prov.select(
    F.col("lnk_rm_dup_DataOut.PROV_ID").alias("PROV_ID"),
    F.col("lnk_rm_dup_DataOut.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnk_KProv_extr.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("lnk_KProv_extr.PROV_SK").alias("PROV_SK")
)

# ---------------------------------------------------------
# Stage: xfm_PKEYgen (CTransformerStage)
#    - Uses NextSurrogateKey() => SurrogateKeyGen
#    - Output constraints:
#        lnk_KProv_New if IsNull(PROV_SK)
#        lnk_PKEYxfmOut otherwise
# ---------------------------------------------------------
df_enriched_pkey = df_lnk_Prov_JoinOut.withColumn("svProvSK", F.col("PROV_SK"))
df_enriched_pkey = SurrogateKeyGen(df_enriched_pkey,<DB sequence name>,"svProvSK",<schema>,<secret_name>)

df_lnk_KProv_New = df_enriched_pkey.filter(F.col("PROV_SK").isNull()).select(
    F.col("PROV_ID").alias("PROV_ID"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("svProvSK").alias("PROV_SK")
)

df_lnk_PKEYxfmOut = df_enriched_pkey.select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.trim(F.col("PROV_ID")).alias("PROV_ID"),
    F.when(
        F.col("PROV_SK").isNull(),
        F.lit(CurrRunCycle)
    ).otherwise(
        F.col("CRT_RUN_CYC_EXCTN_SK")
    ).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("svProvSK").alias("PROV_SK")
)

# ---------------------------------------------------------
# Stage: db2_K_PROV_load (DB2ConnectorPX) -> Merge into K_PROV
# ---------------------------------------------------------
jdbc_url_load, jdbc_props_load = get_db_config(ids_secret_name)
temp_table = "STAGING.OptumProvExtr_db2_K_PROV_load_temp"
execute_dml(f"DROP TABLE IF EXISTS {temp_table}", jdbc_url_load, jdbc_props_load)

df_lnk_KProv_New.write.format("jdbc") \
    .option("url", jdbc_url_load) \
    .options(**jdbc_props_load) \
    .option("dbtable", temp_table) \
    .mode("overwrite") \
    .save()

merge_sql = f"""
MERGE INTO {IDSOwner}.K_PROV AS T
USING {temp_table} AS S
ON T.PROV_ID=S.PROV_ID AND T.SRC_SYS_CD=S.SRC_SYS_CD
WHEN MATCHED THEN
  UPDATE SET
    T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK,
    T.PROV_SK = S.PROV_SK
WHEN NOT MATCHED THEN
  INSERT (PROV_ID, SRC_SYS_CD, CRT_RUN_CYC_EXCTN_SK, PROV_SK)
  VALUES (S.PROV_ID, S.SRC_SYS_CD, S.CRT_RUN_CYC_EXCTN_SK, S.PROV_SK);
"""
execute_dml(merge_sql, jdbc_url_load, jdbc_props_load)

# ---------------------------------------------------------
# Stage: jn_PKEYs (PxJoin) -> lnk_OptumProvPkey
#           operator=innerjoin on (PROV_ID, SRC_SYS_CD)
# ---------------------------------------------------------
df_jn_PKEYs = df_lnk_PKEYxfmOut.alias("lnk_PKEYxfmOut").join(
    df_lnk_OptumProvPkey_All.alias("lnk_OptumProvPkey_All"),
    (
        (F.col("lnk_PKEYxfmOut.PROV_ID") == F.col("lnk_OptumProvPkey_All.PROV_ID")) &
        (F.col("lnk_PKEYxfmOut.SRC_SYS_CD") == F.col("lnk_OptumProvPkey_All.SRC_SYS_CD"))
    ),
    "inner"
)

df_lnk_OptumProvPkey = df_jn_PKEYs.select(
    F.col("lnk_OptumProvPkey_All.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("lnk_OptumProvPkey_All.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("lnk_PKEYxfmOut.PROV_SK").alias("PROV_SK"),
    F.col("lnk_PKEYxfmOut.PROV_ID").alias("PROV_ID"),
    F.col("lnk_PKEYxfmOut.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnk_PKEYxfmOut.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("lnk_PKEYxfmOut.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("lnk_OptumProvPkey_All.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("lnk_OptumProvPkey_All.CMN_PRCT").alias("CMN_PRCT"),
    F.col("lnk_OptumProvPkey_All.REL_GRP_PROV").alias("REL_GRP_PROV"),
    F.col("lnk_OptumProvPkey_All.REL_IPA_PROV").alias("REL_IPA_PROV"),
    F.col("lnk_OptumProvPkey_All.PROV_CAP_PAYMT_EFT_METH_CD").alias("PROV_CAP_PAYMT_EFT_METH_CD"),
    F.col("lnk_OptumProvPkey_All.PROV_CLM_PAYMT_EFT_METH_CD").alias("PROV_CLM_PAYMT_EFT_METH_CD"),
    F.col("lnk_OptumProvPkey_All.PROV_CLM_PAYMT_METH_CD").alias("PROV_CLM_PAYMT_METH_CD"),
    F.col("lnk_OptumProvPkey_All.PROV_ENTY_CD").alias("PROV_ENTY_CD"),
    F.col("lnk_OptumProvPkey_All.PROV_FCLTY_TYP_CD").alias("PROV_FCLTY_TYP_CD"),
    F.col("lnk_OptumProvPkey_All.PROV_PRCTC_TYP_CD").alias("PROV_PRCTC_TYP_CD"),
    F.col("lnk_OptumProvPkey_All.PROV_SVC_CAT_CD").alias("PROV_SVC_CAT_CD"),
    F.col("lnk_OptumProvPkey_All.PROV_SPEC_CD").alias("PROV_SPEC_CD"),
    F.col("lnk_OptumProvPkey_All.PROV_STTUS_CD").alias("PROV_STTUS_CD"),
    F.col("lnk_OptumProvPkey_All.PROV_TERM_RSN_CD").alias("PROV_TERM_RSN_CD"),
    F.col("lnk_OptumProvPkey_All.PROV_TYP_CD").alias("PROV_TYP_CD"),
    F.col("lnk_OptumProvPkey_All.TERM_DT").alias("TERM_DT"),
    F.col("lnk_OptumProvPkey_All.PAYMT_HOLD_DT").alias("PAYMT_HOLD_DT"),
    F.col("lnk_OptumProvPkey_All.CLRNGHOUSE_ID").alias("CLRNGHOUSE_ID"),
    F.col("lnk_OptumProvPkey_All.EDI_DEST_ID").alias("EDI_DEST_ID"),
    F.col("lnk_OptumProvPkey_All.EDI_DEST_QUAL").alias("EDI_DEST_QUAL"),
    F.col("lnk_OptumProvPkey_All.NTNL_PROV_ID").alias("NTNL_PROV_ID"),
    F.col("lnk_OptumProvPkey_All.PROV_ADDR_ID").alias("PROV_ADDR_ID"),
    F.col("lnk_OptumProvPkey_All.PROV_NM").alias("PROV_NM"),
    F.col("lnk_OptumProvPkey_All.TAX_ID").alias("TAX_ID"),
    F.col("lnk_OptumProvPkey_All.TXNMY_CD").alias("TXNMY_CD")
)

# ---------------------------------------------------------
# Stage: OptumProvExtr (PxSequentialFile)
#         Write final file with columns in order
#         Use rpad for char/varchar columns
# ---------------------------------------------------------
df_final = df_lnk_OptumProvPkey

# Below is an example of applying rpad to selected columns known to be char‐type with lengths as per original definitions
df_final = df_final \
    .withColumn("PROV_ID", F.rpad(F.col("PROV_ID"), 7, " ")) \
    .withColumn("NTNL_PROV_ID", F.rpad(F.col("NTNL_PROV_ID"), 10, " ")) \
    .withColumn("CMN_PRCT", F.rpad(F.col("CMN_PRCT"), 12, " ")) \
    .withColumn("REL_GRP_PROV", F.rpad(F.col("REL_GRP_PROV"), 12, " ")) \
    .withColumn("REL_IPA_PROV", F.rpad(F.col("REL_IPA_PROV"), 12, " ")) \
    .withColumn("TERM_DT", F.rpad(F.col("TERM_DT").cast(StringType()), 10, " ")) \
    .withColumn("PAYMT_HOLD_DT", F.rpad(F.col("PAYMT_HOLD_DT").cast(StringType()), 10, " ")) \
    .withColumn("PROV_TYP_CD", F.rpad(F.col("PROV_TYP_CD"), 4, " ")) \
    .withColumn("DISCARD_IN", F.when(F.col("DISCARD_IN").isNull(), "").otherwise(F.rpad(F.col("DISCARD_IN"), 1, " "))) \
    .withColumn("PASS_THRU_IN", F.when(F.col("PASS_THRU_IN").isNull(), "").otherwise(F.rpad(F.col("PASS_THRU_IN"), 1, " "))) \
    .withColumn("INSRT_UPDT_CD", F.when(F.col("INSRT_UPDT_CD").isNull(), "").otherwise(F.rpad(F.col("INSRT_UPDT_CD"), 10, " "))) \
    .withColumn("SRC_SYS_CD", F.rpad(F.col("SRC_SYS_CD"), 10, " "))  # Example fallback for unknown length

write_files(
    df_final,
    f"{adls_path}/key/PROV.{SrcSysCd}.pkey.{RunID}.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)