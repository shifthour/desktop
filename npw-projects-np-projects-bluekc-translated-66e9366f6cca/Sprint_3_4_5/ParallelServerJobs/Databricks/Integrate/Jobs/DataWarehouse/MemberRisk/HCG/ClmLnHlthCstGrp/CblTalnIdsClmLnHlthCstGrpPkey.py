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
# MAGIC Raja Gummadi                  2015-04-22      5460                                Originally Programmed                            IntegrateNewDevl          Kalyan Neelam             2015-04-23
# MAGIC Raja Gummadi                  2015-05-12      5460                                Deleted 32 Columns                               IntegrateNewDevl          Kalyan Neelam             2015-05-15
# MAGIC Raja Gummadi                  2016-08-22      TFS-12593                      Added FCLTY_CASE_ID column           IntegrateDev1                Kalyan Neelam             2016-08-24
# MAGIC Goutham Kalidindi             2023-05-14     US-583538                      Added SRC_SYS_CD to the Ref Join    IntegrateDev1                 Reddy Sanam               2023-05-17
# MAGIC                                                                                                         WHERE Clause

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
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

# Retrieve all required parameter values
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
IDSRunCycle = get_widget_value('IDSRunCycle','')
SrcSysCd = get_widget_value('SrcSysCd','')
RunID = get_widget_value('RunID','')
YearMo = get_widget_value('YearMo','')

#--------------------------------------------------------------------------------
# ds_CLM_LN_HLTH_CST_GRP_Xfrm (PxDataSet) - Read from .ds => translate to .parquet
#--------------------------------------------------------------------------------
schema_ds_CLM_LN_HLTH_CST_GRP_Xfrm = StructType([
    StructField("PRI_NAT_KEY_STRING", StringType(), True),
    StructField("FIRST_RECYC_TS", StringType(), True),
    StructField("CLM_LN_HLTH_CST_GRP_SK", StringType(), True),
    StructField("CLM_ID", StringType(), True),
    StructField("CLM_LN_SEQ_NO", StringType(), True),
    StructField("CLM_SRC_SYS_CD", StringType(), True),
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_SK", StringType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", StringType(), True),
    StructField("SRC_SYS_CD_SK", StringType(), True),
    StructField("CASE_ADM_ID", StringType(), True),
    StructField("MR_LN", StringType(), True),
    StructField("MR_LN_DTL", StringType(), True),
    StructField("MR_LN_CASE", StringType(), True),
    StructField("MR_CASES_ADM", StringType(), True),
    StructField("MR_UNIT_DAYS", StringType(), True),
    StructField("MR_PROC", StringType(), True),
    StructField("FCLTY_CASE_ID", StringType(), True)
])

df_ds_CLM_LN_HLTH_CST_GRP_Xfrm = (
    spark.read.schema(schema_ds_CLM_LN_HLTH_CST_GRP_Xfrm)
    .parquet(f"{adls_path}/ds/CLM_LN_HLTH_CST_GRP.{SrcSysCd}.xfrm.{RunID}.parquet")
)

#--------------------------------------------------------------------------------
# cpy_MultiStreams (PxCopy)
#--------------------------------------------------------------------------------
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

#--------------------------------------------------------------------------------
# K_CLM_LN_HLTH_CST_GRP_SK_In (DB2ConnectorPX) - Read from IDS
#--------------------------------------------------------------------------------
ids_jdbc_url, ids_jdbc_props = get_db_config(ids_secret_name)
extract_query_1 = (
    f"SELECT '{SrcSysCd}' AS SRC_SYS_CD, "
    f"COALESCE(MAX(CLM_LN_HLTH_CST_GRP_SK),0) AS MAX_CLM_LN_HLTH_CST_GRP_SK "
    f"FROM {IDSOwner}.K_CLM_LN_HLTH_CST_GRP"
)

df_K_CLM_LN_HLTH_CST_GRP_SK_In = (
    spark.read.format("jdbc")
    .option("url", ids_jdbc_url)
    .options(**ids_jdbc_props)
    .option("query", extract_query_1)
    .load()
)

#--------------------------------------------------------------------------------
# rdp_NaturalKeys (PxRemDup) - Deduplicate
#--------------------------------------------------------------------------------
df_rdp_NaturalKeys = dedup_sort(
    df_lnkRemDupDataIn,
    partition_cols=["CLM_ID", "CLM_LN_SEQ_NO", "CLM_SRC_SYS_CD", "SRC_SYS_CD"],
    sort_cols=[]
)

#--------------------------------------------------------------------------------
# db2_K_CLM_LN_HLTH_CST_GRP_In (DB2ConnectorPX) - Read from IDS
#--------------------------------------------------------------------------------
extract_query_2 = (
    f"SELECT CLM_ID, "
    f"CLM_LN_SEQ_NO, "
    f"CLM_SRC_SYS_CD, "
    f"SRC_SYS_CD, "
    f"CRT_RUN_CYC_EXCTN_SK, "
    f"CLM_LN_HLTH_CST_GRP_SK "
    f"FROM {IDSOwner}.K_CLM_LN_HLTH_CST_GRP "
    f"WHERE SRC_SYS_CD = '{SrcSysCd}'"
)

df_db2_K_CLM_LN_HLTH_CST_GRP_In = (
    spark.read.format("jdbc")
    .option("url", ids_jdbc_url)
    .options(**ids_jdbc_props)
    .option("query", extract_query_2)
    .load()
)

#--------------------------------------------------------------------------------
# jn_ClmLnHlth (PxJoin) - leftouterjoin
#--------------------------------------------------------------------------------
df_jn_ClmLnHlth = (
    df_rdp_NaturalKeys.alias("lnkRemDupDataOut")
    .join(
        df_db2_K_CLM_LN_HLTH_CST_GRP_In.alias("Extr"),
        on=[
            F.col("lnkRemDupDataOut.CLM_ID") == F.col("Extr.CLM_ID"),
            F.col("lnkRemDupDataOut.CLM_LN_SEQ_NO") == F.col("Extr.CLM_LN_SEQ_NO"),
            F.col("lnkRemDupDataOut.CLM_SRC_SYS_CD") == F.col("Extr.CLM_SRC_SYS_CD"),
            F.col("lnkRemDupDataOut.SRC_SYS_CD") == F.col("Extr.SRC_SYS_CD"),
        ],
        how="left"
    )
    .select(
        F.col("lnkRemDupDataOut.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("lnkRemDupDataOut.CLM_SRC_SYS_CD").alias("CLM_SRC_SYS_CD"),
        F.col("lnkRemDupDataOut.CLM_ID").alias("CLM_ID"),
        F.col("lnkRemDupDataOut.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
        F.col("Extr.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("Extr.CLM_LN_HLTH_CST_GRP_SK").alias("CLM_LN_HLTH_CST_GRP_SK"),
    )
)

#--------------------------------------------------------------------------------
# Lookup_15 (PxLookup) - left join
#--------------------------------------------------------------------------------
df_Lookup_15 = (
    df_jn_ClmLnHlth.alias("Joinin")
    .join(
        df_K_CLM_LN_HLTH_CST_GRP_SK_In.alias("MaxSK"),
        on=[
            F.col("Joinin.SRC_SYS_CD") == F.col("MaxSK.SRC_SYS_CD")
        ],
        how="left"
    )
    .select(
        F.col("Joinin.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("Joinin.CLM_ID").alias("CLM_ID"),
        F.col("Joinin.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
        F.col("Joinin.CLM_SRC_SYS_CD").alias("CLM_SRC_SYS_CD"),
        F.col("Joinin.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("Joinin.CLM_LN_HLTH_CST_GRP_SK").alias("CLM_LN_HLTH_CST_GRP_SK"),
        F.col("MaxSK.MAX_CLM_LN_HLTH_CST_GRP_SK").alias("MAX_CLM_LN_HLTH_CST_GRP_SK")
    )
)

#--------------------------------------------------------------------------------
# xfm_PKEYgen (CTransformerStage)
#   Split into two outputs: New (CLM_LN_HLTH_CST_GRP_SK is null) and ExistingKeys (CLM_LN_HLTH_CST_GRP_SK is not null)
#--------------------------------------------------------------------------------
df_xfm_PKEYgen_in = df_Lookup_15

df_new = (
    df_xfm_PKEYgen_in
    .filter(F.col("CLM_LN_HLTH_CST_GRP_SK").isNull())
    .select(
        F.col("CLM_ID").alias("CLM_ID"),
        F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
        F.col("CLM_SRC_SYS_CD").alias("CLM_SRC_SYS_CD"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.lit(IDSRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("CLM_LN_HLTH_CST_GRP_SK"),
        F.lit(IDSRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("MAX_CLM_LN_HLTH_CST_GRP_SK").alias("MAX_CLM_LN_HLTH_CST_GRP_SK")
    )
)

df_existingKeys = (
    df_xfm_PKEYgen_in
    .filter(F.col("CLM_LN_HLTH_CST_GRP_SK").isNotNull())
    .select(
        F.col("CLM_ID").alias("CLM_ID"),
        F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
        F.col("CLM_SRC_SYS_CD").alias("CLM_SRC_SYS_CD"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(IDSRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("CLM_LN_HLTH_CST_GRP_SK").alias("CLM_LN_HLTH_CST_GRP_SK")
    )
)

#--------------------------------------------------------------------------------
# AssignKeys (CTransformerStage) 
#   Stage variable svSK uses row-based logic. We approximate using row_number + expression.
#   Input: df_new
#   Outputs: NewLoad, NewPkeys
#--------------------------------------------------------------------------------
w_assignkeys = Window.orderBy(F.monotonically_increasing_id())
df_AssignKeys_swiss = (
    df_new
    .withColumn("row_number", F.row_number().over(w_assignkeys))
    .withColumn(
        "svSK",
        F.when(
            (F.col("MAX_CLM_LN_HLTH_CST_GRP_SK").isNull()) | (F.col("MAX_CLM_LN_HLTH_CST_GRP_SK") == 0),
            (F.col("row_number")) + F.col("MAX_CLM_LN_HLTH_CST_GRP_SK") + F.lit(99)
        ).otherwise(
            (F.col("row_number")) + F.col("MAX_CLM_LN_HLTH_CST_GRP_SK")
        )
    )
)

df_newLoad = (
    df_AssignKeys_swiss
    .select(
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("CLM_ID").alias("CLM_ID"),
        F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
        F.col("CLM_SRC_SYS_CD").alias("CLM_SRC_SYS_CD"),
        F.lit(IDSRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("svSK").alias("CLM_LN_HLTH_CST_GRP_SK")
    )
)

df_newPkeys = (
    df_AssignKeys_swiss
    .select(
        F.col("CLM_ID").alias("CLM_ID"),
        F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
        F.col("CLM_SRC_SYS_CD").alias("CLM_SRC_SYS_CD"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("svSK").alias("CLM_LN_HLTH_CST_GRP_SK")
    )
)

#--------------------------------------------------------------------------------
# db2_K_CLM_LN_HLTH_CST_GRP_Load (DB2ConnectorPX) - Merge (upsert) into {IDSOwner}.K_CLM_LN_HLTH_CST_GRP
#   Input: df_newLoad
#--------------------------------------------------------------------------------
drop_temp_table_sql = (
    "DROP TABLE IF EXISTS STAGING.CblTalnIdsClmLnHlthCstGrpPkey_db2_K_CLM_LN_HLTH_CST_GRP_Load_temp"
)
execute_dml(drop_temp_table_sql, ids_jdbc_url, ids_jdbc_props)

df_newLoad.write.format("jdbc") \
    .option("url", ids_jdbc_url) \
    .options(**ids_jdbc_props) \
    .option("dbtable", "STAGING.CblTalnIdsClmLnHlthCstGrpPkey_db2_K_CLM_LN_HLTH_CST_GRP_Load_temp") \
    .mode("overwrite") \
    .save()

merge_sql = f"""
MERGE INTO {IDSOwner}.K_CLM_LN_HLTH_CST_GRP AS T
USING STAGING.CblTalnIdsClmLnHlthCstGrpPkey_db2_K_CLM_LN_HLTH_CST_GRP_Load_temp AS S
ON T.SRC_SYS_CD=S.SRC_SYS_CD 
   AND T.CLM_ID=S.CLM_ID 
   AND T.CLM_LN_SEQ_NO=S.CLM_LN_SEQ_NO 
   AND T.CLM_SRC_SYS_CD=S.CLM_SRC_SYS_CD
WHEN MATCHED THEN UPDATE SET
   T.CRT_RUN_CYC_EXCTN_SK=S.CRT_RUN_CYC_EXCTN_SK,
   T.CLM_LN_HLTH_CST_GRP_SK=S.CLM_LN_HLTH_CST_GRP_SK
WHEN NOT MATCHED THEN
   INSERT (SRC_SYS_CD, CLM_ID, CLM_LN_SEQ_NO, CLM_SRC_SYS_CD, CRT_RUN_CYC_EXCTN_SK, CLM_LN_HLTH_CST_GRP_SK)
   VALUES (S.SRC_SYS_CD, S.CLM_ID, S.CLM_LN_SEQ_NO, S.CLM_SRC_SYS_CD, S.CRT_RUN_CYC_EXCTN_SK, S.CLM_LN_HLTH_CST_GRP_SK);
"""
execute_dml(merge_sql, ids_jdbc_url, ids_jdbc_props)

#--------------------------------------------------------------------------------
# Funnel_17 (PxFunnel): Union of ExistingKeys and NewPkeys
#--------------------------------------------------------------------------------
df_funnel_17 = (
    df_existingKeys.unionByName(df_newPkeys)
)

#--------------------------------------------------------------------------------
# jn_PKEYs (PxJoin) - inner join: lnkFullDataJnIn & funnel_17
#--------------------------------------------------------------------------------
df_jn_PKEYs = (
    df_lnkFullDataJnIn.alias("lnkFullDataJnIn")
    .join(
        df_funnel_17.alias("Pkeys"),
        on=[
            F.col("lnkFullDataJnIn.CLM_ID") == F.col("Pkeys.CLM_ID"),
            F.col("lnkFullDataJnIn.CLM_LN_SEQ_NO") == F.col("Pkeys.CLM_LN_SEQ_NO"),
            F.col("lnkFullDataJnIn.CLM_SRC_SYS_CD") == F.col("Pkeys.CLM_SRC_SYS_CD"),
            F.col("lnkFullDataJnIn.SRC_SYS_CD") == F.col("Pkeys.SRC_SYS_CD")
        ],
        how="inner"
    )
    .select(
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
)

#--------------------------------------------------------------------------------
# seq_CLM_LN_HLTH_CST_GRP_Pkey (PxSequentialFile) - write to .dat
#   File: key/CLM_LN_HLTH_CST_GRP.#SrcSysCd#.pkey.#RunID#.dat
#   Delimiter = ',', quoteChar = '^', header=False
#--------------------------------------------------------------------------------

df_final = df_jn_PKEYs.select(
    rpad(F.col("PRI_NAT_KEY_STRING"), <...>, " ").alias("PRI_NAT_KEY_STRING"),
    rpad(F.col("FIRST_RECYC_TS"), <...>, " ").alias("FIRST_RECYC_TS"),
    rpad(F.col("CLM_LN_HLTH_CST_GRP_SK"), <...>, " ").alias("CLM_LN_HLTH_CST_GRP_SK"),
    rpad(F.col("CLM_ID"), <...>, " ").alias("CLM_ID"),
    rpad(F.col("CLM_LN_SEQ_NO"), <...>, " ").alias("CLM_LN_SEQ_NO"),
    rpad(F.col("CLM_SRC_SYS_CD"), <...>, " ").alias("CLM_SRC_SYS_CD"),
    rpad(F.col("SRC_SYS_CD"), <...>, " ").alias("SRC_SYS_CD"),
    rpad(F.col("CRT_RUN_CYC_EXCTN_SK"), <...>, " ").alias("CRT_RUN_CYC_EXCTN_SK"),
    rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"), <...>, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    rpad(F.col("SRC_SYS_CD_SK"), <...>, " ").alias("SRC_SYS_CD_SK"),
    rpad(F.col("CASE_ADM_ID"), <...>, " ").alias("CASE_ADM_ID"),
    rpad(F.col("MR_LN"), <...>, " ").alias("MR_LN"),
    rpad(F.col("MR_LN_DTL"), <...>, " ").alias("MR_LN_DTL"),
    rpad(F.col("MR_LN_CASE"), <...>, " ").alias("MR_LN_CASE"),
    rpad(F.col("MR_CASES_ADM"), <...>, " ").alias("MR_CASES_ADM"),
    rpad(F.col("MR_UNIT_DAYS"), <...>, " ").alias("MR_UNIT_DAYS"),
    rpad(F.col("MR_PROC"), <...>, " ").alias("MR_PROC"),
    rpad(F.col("FCLTY_CASE_ID"), <...>, " ").alias("FCLTY_CASE_ID")
)

write_files(
    df_final,
    f"{adls_path}/key/CLM_LN_HLTH_CST_GRP.{SrcSysCd}.pkey.{RunID}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)