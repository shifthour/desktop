# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC PROCESSING:  This job assigns primary key value using SEQ_K_CLM_LN_CCHG 
# MAGIC                                                                 
# MAGIC      
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC Kalyan Neelam           2015-08-17           5460                            Original Programming                                                                             IntegrateDev2            Bhoomi Dasari              8/30/2015 
# MAGIC Raja Gummadi            2017-03-14           Prod Supp                   Changed Primary Keying process                                                          IntegrateDev1           Kalyan Neelam             2017-03-22

# MAGIC Remove Duplicates stage takes care of getting rid of duplicate rows with the same Natural key get into the PKEYing step.
# MAGIC A concious effort need to be made to evaluate which one of the duplicate set row to drop is a case by case basis decision.
# MAGIC Table K_CLM_LN_CCHG. Insert new SKEY rows generated as part of this run. This is a database insert operation.
# MAGIC Land into Seq File for the FKEY job
# MAGIC New PKEYs are generated in this transformer.
# MAGIC Left Outer Join Here
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, lit, when, row_number, rpad
from pyspark.sql import Window
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

# Retrieve parameters
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
IDSRunCycle = get_widget_value('IDSRunCycle','100')
SrcSysCd = get_widget_value('SrcSysCd','COBALTTALON')
RunID = get_widget_value('RunID','100')
CurrDate = get_widget_value('CurrDate','2015-08-12')
YearMo = get_widget_value('YearMo','')

# Read from PxDataSet ds_CLM_LN_CCHG_Xfrm (.ds -> .parquet)
df_ds_CLM_LN_CCHG_Xfrm = spark.read.parquet(f"{adls_path}/ds/CLM_LN_CCHG.{SrcSysCd}.xfrm.{RunID}.parquet") \
    .select(
        "PRI_NAT_KEY_STRING",
        "FIRST_RECYC_TS",
        "CLM_LN_CCHG_SK",
        "CLM_ID",
        "CLM_LN_SEQ_NO",
        "CLM_SRC_SYS_CD",
        "PRCS_YR_MO_SK",
        "SRC_SYS_CD",
        "SRC_SYS_CD_SK",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "CCHG_MULT_CAT_GRP"
    )

# cpy_MultiStreams: produce two outputs
# 1) lnkRemDupDataIn
df_cpy_MultiStreams_1 = df_ds_CLM_LN_CCHG_Xfrm.select(
    col("CLM_ID").alias("CLM_ID"),
    col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    col("CLM_SRC_SYS_CD").alias("CLM_SRC_SYS_CD"),
    col("PRCS_YR_MO_SK").alias("PRCS_YR_MO_SK"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD")
)

# 2) lnkFullDataJnIn
df_cpy_MultiStreams_2 = df_ds_CLM_LN_CCHG_Xfrm.select(
    col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    col("CLM_ID").alias("CLM_ID"),
    col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    col("CLM_SRC_SYS_CD").alias("CLM_SRC_SYS_CD"),
    col("PRCS_YR_MO_SK").alias("PRCS_YR_MO_SK"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    col("CCHG_MULT_CAT_GRP").alias("CCHG_MULT_CAT_GRP")
)

# max_K_CLM_LN_CCHG_In (DB2ConnectorPX) - read from IDS
ids_jdbc_url, ids_jdbc_props = get_db_config(ids_secret_name)
extract_query_max = f"""
SELECT
'{YearMo}' AS PRCS_YR_MO_SK,
COALESCE(MAX(CLM_LN_CCHG_SK),0) AS MAX_CLM_LN_CCHG_SK
FROM {IDSOwner}.K_CLM_LN_CCHG
"""
df_max_K_CLM_LN_CCHG_In = (
    spark.read.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("query", extract_query_max)
        .load()
)

# rdp_NaturalKeys (PxRemDup) on df_cpy_MultiStreams_1
df_rdp_NaturalKeys = dedup_sort(
    df_cpy_MultiStreams_1,
    partition_cols=["CLM_ID","CLM_LN_SEQ_NO","CLM_SRC_SYS_CD","PRCS_YR_MO_SK","SRC_SYS_CD"],
    sort_cols=[]
)

# db2_K_CLM_LN_CCHG_In (DB2ConnectorPX) - read from IDS
extract_query_db2 = f"""
SELECT
CLM_ID,
CLM_LN_SEQ_NO,
CLM_SRC_SYS_CD,
PRCS_YR_MO_SK,
SRC_SYS_CD,
CRT_RUN_CYC_EXCTN_SK,
CLM_LN_CCHG_SK
FROM {IDSOwner}.K_CLM_LN_CCHG
WHERE PRCS_YR_MO_SK = '{YearMo}'
"""
df_db2_K_CLM_LN_CCHG_In = (
    spark.read.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("query", extract_query_db2)
        .load()
)

# jn_KClmLnCchg (PxJoin - left outer join)
df_jn_KClmLnCchg = (
    df_rdp_NaturalKeys.alias("lnkRemDupDataOut")
    .join(
        df_db2_K_CLM_LN_CCHG_In.alias("Extr"),
        on=[
            col("lnkRemDupDataOut.CLM_ID") == col("Extr.CLM_ID"),
            col("lnkRemDupDataOut.CLM_LN_SEQ_NO") == col("Extr.CLM_LN_SEQ_NO"),
            col("lnkRemDupDataOut.CLM_SRC_SYS_CD") == col("Extr.CLM_SRC_SYS_CD"),
            col("lnkRemDupDataOut.PRCS_YR_MO_SK") == col("Extr.PRCS_YR_MO_SK"),
            col("lnkRemDupDataOut.SRC_SYS_CD") == col("Extr.SRC_SYS_CD")
        ],
        how="left"
    )
    .select(
        col("lnkRemDupDataOut.CLM_ID").alias("CLM_ID"),
        col("lnkRemDupDataOut.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
        col("lnkRemDupDataOut.CLM_SRC_SYS_CD").alias("CLM_SRC_SYS_CD"),
        col("lnkRemDupDataOut.PRCS_YR_MO_SK").alias("PRCS_YR_MO_SK"),
        col("lnkRemDupDataOut.SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("Extr.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("Extr.CLM_LN_CCHG_SK").alias("CLM_LN_CCHG_SK")
    )
)

# Lookup_15 (PxLookup) - left join with df_max_K_CLM_LN_CCHG_In on PRCS_YR_MO_SK
df_lookup_15 = (
    df_jn_KClmLnCchg.alias("JoinOut")
    .join(
        df_max_K_CLM_LN_CCHG_In.alias("Extr"),
        on=[
            col("JoinOut.PRCS_YR_MO_SK") == col("Extr.PRCS_YR_MO_SK")
        ],
        how="left"
    )
    .select(
        col("JoinOut.CLM_ID").alias("CLM_ID"),
        col("JoinOut.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
        col("JoinOut.CLM_SRC_SYS_CD").alias("CLM_SRC_SYS_CD"),
        col("JoinOut.PRCS_YR_MO_SK").alias("PRCS_YR_MO_SK"),
        col("JoinOut.SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("JoinOut.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("JoinOut.CLM_LN_CCHG_SK").alias("CLM_LN_CCHG_SK"),
        col("Extr.MAX_CLM_LN_CCHG_SK").alias("MAX_CLM_LN_CCHG_SK")
    )
)

# xfm_PKEYgen (CTransformerStage)
# Replicate partition/row logic as best as possible using a row_number window
w = Window.orderBy(lit(1))
df_xfm_pkeygen_base = (
    df_lookup_15
    .withColumn("rownum", row_number().over(w))
    .withColumn(
        "svClmLnCchgSK",
        when(
            col("CLM_LN_CCHG_SK").isNull(),
            (col("rownum") - lit(1)) + col("MAX_CLM_LN_CCHG_SK")
        ).otherwise(col("CLM_LN_CCHG_SK"))
    )
    .withColumn(
        "svRunCyle",
        when(
            col("CLM_LN_CCHG_SK").isNull(),
            lit(IDSRunCycle)
        ).otherwise(col("CRT_RUN_CYC_EXCTN_SK"))
    )
)

# Output link: New (IsNull(LkupOut.CLM_LN_CCHG_SK) = True)
df_xfm_pkeygen_new = df_xfm_pkeygen_base.filter(col("CLM_LN_CCHG_SK").isNull()) \
    .select(
        col("CLM_ID"),
        col("CLM_LN_SEQ_NO"),
        col("CLM_SRC_SYS_CD"),
        col("PRCS_YR_MO_SK"),
        col("SRC_SYS_CD"),
        col("svRunCyle").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("svClmLnCchgSK").alias("CLM_LN_CCHG_SK")
    )

# Output link: lnkPKEYxfmOut (all rows)
df_xfm_pkeygen_out = df_xfm_pkeygen_base.select(
    col("CLM_ID"),
    col("CLM_LN_SEQ_NO"),
    col("CLM_SRC_SYS_CD"),
    col("PRCS_YR_MO_SK"),
    col("SRC_SYS_CD"),
    col("svRunCyle").alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(IDSRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("svClmLnCchgSK").alias("CLM_LN_CCHG_SK")
)

# db2_K_CLM_LN_CCHG_Load (DB2ConnectorPX) - write via MERGE into {IDSOwner}.K_CLM_LN_CCHG
# First, stage df_xfm_pkeygen_new in a STAGING table
jdbc_url_load, jdbc_props_load = get_db_config(ids_secret_name)
execute_dml(f"DROP TABLE IF EXISTS STAGING.IdsClmLnCchgPkey_db2_K_CLM_LN_CCHG_Load_temp", jdbc_url_load, jdbc_props_load)

df_xfm_pkeygen_new.write \
    .format("jdbc") \
    .option("url", jdbc_url_load) \
    .options(**jdbc_props_load) \
    .option("dbtable", "STAGING.IdsClmLnCchgPkey_db2_K_CLM_LN_CCHG_Load_temp") \
    .mode("overwrite") \
    .save()

merge_sql = f"""
MERGE INTO {IDSOwner}.K_CLM_LN_CCHG AS T
USING STAGING.IdsClmLnCchgPkey_db2_K_CLM_LN_CCHG_Load_temp AS S
ON 
(
    T.CLM_ID = S.CLM_ID
    AND T.CLM_LN_SEQ_NO = S.CLM_LN_SEQ_NO
    AND T.CLM_SRC_SYS_CD = S.CLM_SRC_SYS_CD
    AND T.PRCS_YR_MO_SK = S.PRCS_YR_MO_SK
    AND T.SRC_SYS_CD = S.SRC_SYS_CD
)
WHEN MATCHED THEN
  UPDATE SET
    T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK,
    T.CLM_LN_CCHG_SK = S.CLM_LN_CCHG_SK
WHEN NOT MATCHED THEN
  INSERT 
  (
    CLM_ID,
    CLM_LN_SEQ_NO,
    CLM_SRC_SYS_CD,
    PRCS_YR_MO_SK,
    SRC_SYS_CD,
    CRT_RUN_CYC_EXCTN_SK,
    CLM_LN_CCHG_SK
  )
  VALUES
  (
    S.CLM_ID,
    S.CLM_LN_SEQ_NO,
    S.CLM_SRC_SYS_CD,
    S.PRCS_YR_MO_SK,
    S.SRC_SYS_CD,
    S.CRT_RUN_CYC_EXCTN_SK,
    S.CLM_LN_CCHG_SK
  );
"""
execute_dml(merge_sql, jdbc_url_load, jdbc_props_load)

# jn_PKEYs (PxJoin - inner join) => lnkFullDataJnIn, lnkPKEYxfmOut
df_jn_PKEYs = (
    df_cpy_MultiStreams_2.alias("lnkFullDataJnIn")
    .join(
        df_xfm_pkeygen_out.alias("lnkPKEYxfmOut"),
        on=[
            col("lnkFullDataJnIn.CLM_ID") == col("lnkPKEYxfmOut.CLM_ID"),
            col("lnkFullDataJnIn.CLM_LN_SEQ_NO") == col("lnkPKEYxfmOut.CLM_LN_SEQ_NO"),
            col("lnkFullDataJnIn.CLM_SRC_SYS_CD") == col("lnkPKEYxfmOut.CLM_SRC_SYS_CD"),
            col("lnkFullDataJnIn.PRCS_YR_MO_SK") == col("lnkPKEYxfmOut.PRCS_YR_MO_SK"),
            col("lnkFullDataJnIn.SRC_SYS_CD") == col("lnkPKEYxfmOut.SRC_SYS_CD")
        ],
        how="inner"
    )
    .select(
        col("lnkFullDataJnIn.PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
        col("lnkFullDataJnIn.FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
        col("lnkPKEYxfmOut.CLM_LN_CCHG_SK").alias("CLM_LN_CCHG_SK"),
        col("lnkFullDataJnIn.CLM_ID").alias("CLM_ID"),
        col("lnkFullDataJnIn.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
        col("lnkFullDataJnIn.CLM_SRC_SYS_CD").alias("CLM_SRC_SYS_CD"),
        col("lnkFullDataJnIn.PRCS_YR_MO_SK").alias("PRCS_YR_MO_SK"),
        col("lnkFullDataJnIn.SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("lnkFullDataJnIn.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        col("lnkPKEYxfmOut.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("lnkPKEYxfmOut.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("lnkFullDataJnIn.CCHG_MULT_CAT_GRP").alias("CCHG_MULT_CAT_GRP")
    )
)

# seq_CLM_LN_CCHG_Pkey (PxSequentialFile) - write to key/CLM_LN_CCHG.#SrcSysCd#.pkey.#RunID#.dat
df_final = df_jn_PKEYs.withColumn(
    "PRCS_YR_MO_SK", 
    rpad(col("PRCS_YR_MO_SK"), 6, " ")
).select(
    "PRI_NAT_KEY_STRING",
    "FIRST_RECYC_TS",
    "CLM_LN_CCHG_SK",
    "CLM_ID",
    "CLM_LN_SEQ_NO",
    "CLM_SRC_SYS_CD",
    "PRCS_YR_MO_SK",
    "SRC_SYS_CD",
    "SRC_SYS_CD_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "CCHG_MULT_CAT_GRP"
)

write_files(
    df_final,
    f"{adls_path}/key/CLM_LN_CCHG.{SrcSysCd}.pkey.{RunID}.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)