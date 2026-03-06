# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2009 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #               Change Description                             Development Project      Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------     ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC 
# MAGIC Razia                                 2021-10-19           US-428909                  Copied over from                                            IntegrateDev1        Goutham K               11/9/2021
# MAGIC                                                                                                                     CblTalnClmLnHlthCstGrpCntl
# MAGIC                                                                                                                     to Load the new Rpcl Table
# MAGIC 
# MAGIC Goutham Kalidindi             2023-05-14            US-583538                      Added SRC_SYS_CD to the Ref Join    IntegrateDev1           Reddy Sanam            05/17/2023
# MAGIC                                                                                                                 WHERE Clause

# MAGIC Remove Duplicates stage takes care of getting rid of duplicate rows with the same Natural key get into the PKEYing step.
# MAGIC A concious effort need to be made to evaluate which one of the duplicate set row to drop is a case by case basis decision.
# MAGIC Land into Seq File for the FKEY job
# MAGIC Left Outer Join Here
# MAGIC New PKEYs are generated in this transformer.
# MAGIC Table K_CLM_LN_HLTH_CST_GRP.Insert new SKEY rows generated as part of this run. This is a database insert operation.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql import Window
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
IDSRunCycle = get_widget_value('IDSRunCycle','')
SrcSysCd = get_widget_value('SrcSysCd','FACETS')
RunID = get_widget_value('RunID','')
YearMo = get_widget_value('YearMo','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

# Read the PxDataSet (ds_CLM_LN_HLTH_CST_GRP_Xfrm)
df_ds_CLM_LN_HLTH_CST_GRP_Xfrm = spark.read.parquet(
    f"{adls_path}/ds/CLM_LN_HLTH_CST_GRP.{SrcSysCd}.xfrm.{RunID}.parquet"
)

# cpy_MultiStreams outputs
df_lnkRemDupDataIn = df_ds_CLM_LN_HLTH_CST_GRP_Xfrm.select(
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("CLM_SRC_SYS_CD").alias("CLM_SRC_SYS_CD"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD")
)

df_lnkFullDataJnIn = df_ds_CLM_LN_HLTH_CST_GRP_Xfrm.select(
    F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("CLM_SRC_SYS_CD").alias("CLM_SRC_SYS_CD"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("CASE_ADM_ID").alias("CASE_ADM_ID"),
    F.col("MR_LN").alias("MR_LN"),
    F.col("MR_LN_DTL").alias("MR_LN_DTL"),
    F.col("MR_LN_CASE").alias("MR_LN_CASE"),
    F.col("MR_CASES_ADM").alias("MR_CASES_ADM"),
    F.col("MR_UNIT_DAYS").alias("MR_UNIT_DAYS"),
    F.col("MR_PROC").alias("MR_PROC"),
    F.col("FCLTY_CASE_ID").alias("FCLTY_CASE_ID")
)

# K_CLM_LN_HLTH_CST_GRP_SK_In
df_K_CLM_LN_HLTH_CST_GRP_SK_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"SELECT '{SrcSysCd}' AS SRC_SYS_CD, COALESCE(MAX(CLM_LN_HLTH_CST_GRP_SK),0) AS MAX_CLM_LN_HLTH_CST_GRP_SK FROM {IDSOwner}.K_CLM_LN_HLTH_CST_GRP"
    )
    .load()
)

# rdp_NaturalKeys (PxRemDup)
df_temp_rdp_NaturalKeys = dedup_sort(
    df_lnkRemDupDataIn,
    ["CLM_ID","CLM_LN_SEQ_NO","CLM_SRC_SYS_CD","SRC_SYS_CD"],
    []
)
df_rdp_NaturalKeys = df_temp_rdp_NaturalKeys.select(
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("CLM_SRC_SYS_CD").alias("CLM_SRC_SYS_CD"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD")
)

# db2_K_CLM_LN_HLTH_CST_GRP_In
df_db2_K_CLM_LN_HLTH_CST_GRP_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"SELECT CLM_ID, CLM_LN_SEQ_NO, CLM_SRC_SYS_CD, SRC_SYS_CD, CRT_RUN_CYC_EXCTN_SK, CLM_LN_HLTH_CST_GRP_SK FROM {IDSOwner}.K_CLM_LN_HLTH_CST_GRP WHERE SRC_SYS_CD = '{SrcSysCd}'"
    )
    .load()
)

# jn_ClmLnHlth (left join)
df_jn_ClmLnHlth_intermediate = df_rdp_NaturalKeys.alias("lnkRemDupDataOut").join(
    df_db2_K_CLM_LN_HLTH_CST_GRP_In.alias("Extr"),
    (
        (F.col("lnkRemDupDataOut.CLM_ID") == F.col("Extr.CLM_ID")) &
        (F.col("lnkRemDupDataOut.CLM_LN_SEQ_NO") == F.col("Extr.CLM_LN_SEQ_NO")) &
        (F.col("lnkRemDupDataOut.CLM_SRC_SYS_CD") == F.col("Extr.CLM_SRC_SYS_CD")) &
        (F.col("lnkRemDupDataOut.SRC_SYS_CD") == F.col("Extr.SRC_SYS_CD"))
    ),
    how="left"
)
df_jn_ClmLnHlth = df_jn_ClmLnHlth_intermediate.select(
    F.col("lnkRemDupDataOut.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnkRemDupDataOut.CLM_SRC_SYS_CD").alias("CLM_SRC_SYS_CD"),
    F.col("lnkRemDupDataOut.CLM_ID").alias("CLM_ID"),
    F.col("lnkRemDupDataOut.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("Extr.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("Extr.CLM_LN_HLTH_CST_GRP_SK").alias("CLM_LN_HLTH_CST_GRP_SK")
)

# Lookup_15 (left join)
df_Lookup_15_intermediate = df_jn_ClmLnHlth.alias("Joinin").join(
    df_K_CLM_LN_HLTH_CST_GRP_SK_In.alias("MaxSK"),
    F.col("Joinin.SRC_SYS_CD") == F.col("MaxSK.SRC_SYS_CD"),
    how="left"
)
df_Lookup_15 = df_Lookup_15_intermediate.select(
    F.col("Joinin.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Joinin.CLM_ID").alias("CLM_ID"),
    F.col("Joinin.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("Joinin.CLM_SRC_SYS_CD").alias("CLM_SRC_SYS_CD"),
    F.col("Joinin.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("Joinin.CLM_LN_HLTH_CST_GRP_SK").alias("CLM_LN_HLTH_CST_GRP_SK"),
    F.col("MaxSK.MAX_CLM_LN_HLTH_CST_GRP_SK").alias("MAX_CLM_LN_HLTH_CST_GRP_SK")
)

# xfm_PKEYgen
df_xfmPKEYgen_New = df_Lookup_15.filter(F.col("CLM_LN_HLTH_CST_GRP_SK").isNull())
df_xfmPKEYgen_New = df_xfmPKEYgen_New.select(
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("CLM_SRC_SYS_CD").alias("CLM_SRC_SYS_CD"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.lit(IDSRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("CLM_LN_HLTH_CST_GRP_SK"),
    F.lit(IDSRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("MAX_CLM_LN_HLTH_CST_GRP_SK").alias("MAX_CLM_LN_HLTH_CST_GRP_SK")
)

df_xfmPKEYgen_ExistingKeys = df_Lookup_15.filter(F.col("CLM_LN_HLTH_CST_GRP_SK").isNotNull())
df_xfmPKEYgen_ExistingKeys = df_xfmPKEYgen_ExistingKeys.select(
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("CLM_SRC_SYS_CD").alias("CLM_SRC_SYS_CD"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(IDSRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CLM_LN_HLTH_CST_GRP_SK").alias("CLM_LN_HLTH_CST_GRP_SK")
)

# AssignKeys (transforming "New")
df_assignKeys_input = df_xfmPKEYgen_New

windowSpec = Window.orderBy(F.lit(1))
df_with_rownum = df_assignKeys_input.withColumn("_rownum", F.row_number().over(windowSpec))

df_with_svSK = df_with_rownum.withColumn(
    "svSK",
    F.when(
        (F.col("MAX_CLM_LN_HLTH_CST_GRP_SK").isNull()) | (F.col("MAX_CLM_LN_HLTH_CST_GRP_SK") == 0),
        F.col("_rownum") + F.col("MAX_CLM_LN_HLTH_CST_GRP_SK") + F.lit(99)
    ).otherwise(
        F.col("_rownum") + F.col("MAX_CLM_LN_HLTH_CST_GRP_SK")
    )
)

df_AssignKeys_NewLoad = df_with_svSK.select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("CLM_SRC_SYS_CD").alias("CLM_SRC_SYS_CD"),
    F.lit(IDSRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("svSK").alias("CLM_LN_HLTH_CST_GRP_SK")
)

df_AssignKeys_NewPkeys = df_with_svSK.select(
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("CLM_SRC_SYS_CD").alias("CLM_SRC_SYS_CD"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("svSK").alias("CLM_LN_HLTH_CST_GRP_SK")
)

# db2_K_CLM_LN_HLTH_CST_GRP_Load (MERGE logic)
staging_table = "STAGING.CblTalnIdsClmLnHlthCstGrpReplPkey_db2_K_CLM_LN_HLTH_CST_GRP_Load_temp"
execute_dml(
    f"DROP TABLE IF EXISTS {staging_table}",
    jdbc_url,
    jdbc_props
)

df_AssignKeys_NewLoad.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", staging_table) \
    .mode("overwrite") \
    .save()

merge_sql = f"""
MERGE {IDSOwner}.K_CLM_LN_HLTH_CST_GRP AS T
USING {staging_table} AS S
ON 
    T.CLM_ID = S.CLM_ID AND
    T.CLM_LN_SEQ_NO = S.CLM_LN_SEQ_NO AND
    T.CLM_SRC_SYS_CD = S.CLM_SRC_SYS_CD AND
    T.SRC_SYS_CD = S.SRC_SYS_CD
WHEN MATCHED THEN UPDATE SET
    T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK,
    T.CLM_LN_HLTH_CST_GRP_SK = S.CLM_LN_HLTH_CST_GRP_SK
WHEN NOT MATCHED THEN
INSERT
(
    SRC_SYS_CD,
    CLM_ID,
    CLM_LN_SEQ_NO,
    CLM_SRC_SYS_CD,
    CRT_RUN_CYC_EXCTN_SK,
    CLM_LN_HLTH_CST_GRP_SK
)
VALUES
(
    S.SRC_SYS_CD,
    S.CLM_ID,
    S.CLM_LN_SEQ_NO,
    S.CLM_SRC_SYS_CD,
    S.CRT_RUN_CYC_EXCTN_SK,
    S.CLM_LN_HLTH_CST_GRP_SK
);
"""
execute_dml(merge_sql, jdbc_url, jdbc_props)

# Funnel_17 (union of existing keys + new pkeys)
df_ExistingKeys_union = df_xfmPKEYgen_ExistingKeys.select(
    F.col("CLM_ID"),
    F.col("CLM_LN_SEQ_NO"),
    F.col("CLM_SRC_SYS_CD"),
    F.col("SRC_SYS_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CLM_LN_HLTH_CST_GRP_SK")
)
df_NewPkeys_union = df_AssignKeys_NewPkeys.select(
    F.col("CLM_ID"),
    F.col("CLM_LN_SEQ_NO"),
    F.col("CLM_SRC_SYS_CD"),
    F.col("SRC_SYS_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CLM_LN_HLTH_CST_GRP_SK")
)
df_Funnel_17 = df_ExistingKeys_union.unionByName(df_NewPkeys_union)

# jn_PKEYs (inner join)
df_jn_PKEYs_intermediate = df_lnkFullDataJnIn.alias("lnkFullDataJnIn").join(
    df_Funnel_17.alias("Pkeys"),
    (
        (F.col("lnkFullDataJnIn.CLM_ID") == F.col("Pkeys.CLM_ID")) &
        (F.col("lnkFullDataJnIn.CLM_LN_SEQ_NO") == F.col("Pkeys.CLM_LN_SEQ_NO")) &
        (F.col("lnkFullDataJnIn.CLM_SRC_SYS_CD") == F.col("Pkeys.CLM_SRC_SYS_CD")) &
        (F.col("lnkFullDataJnIn.SRC_SYS_CD") == F.col("Pkeys.SRC_SYS_CD"))
    ),
    how="inner"
)
df_jn_PKEYs = df_jn_PKEYs_intermediate.select(
    F.col("lnkFullDataJnIn.PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("lnkFullDataJnIn.FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("Pkeys.CLM_LN_HLTH_CST_GRP_SK").alias("CLM_LN_HLTH_CST_GRP_SK"),
    F.col("lnkFullDataJnIn.CLM_ID").alias("CLM_ID"),
    F.col("lnkFullDataJnIn.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("lnkFullDataJnIn.CLM_SRC_SYS_CD").alias("CLM_SRC_SYS_CD"),
    F.col("lnkFullDataJnIn.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Pkeys.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("Pkeys.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("lnkFullDataJnIn.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("lnkFullDataJnIn.CASE_ADM_ID").alias("CASE_ADM_ID"),
    F.col("lnkFullDataJnIn.MR_LN").alias("MR_LN"),
    F.col("lnkFullDataJnIn.MR_LN_DTL").alias("MR_LN_DTL"),
    F.col("lnkFullDataJnIn.MR_LN_CASE").alias("MR_LN_CASE"),
    F.col("lnkFullDataJnIn.MR_CASES_ADM").alias("MR_CASES_ADM"),
    F.col("lnkFullDataJnIn.MR_UNIT_DAYS").alias("MR_UNIT_DAYS"),
    F.col("lnkFullDataJnIn.MR_PROC").alias("MR_PROC"),
    F.col("lnkFullDataJnIn.FCLTY_CASE_ID").alias("FCLTY_CASE_ID")
)

# seq_CLM_LN_HLTH_CST_GRP_Pkey
write_files(
    df_jn_PKEYs,
    f"{adls_path}/key/CLM_LN_HLTH_CST_GRP.{SrcSysCd}.pkey.{RunID}.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)