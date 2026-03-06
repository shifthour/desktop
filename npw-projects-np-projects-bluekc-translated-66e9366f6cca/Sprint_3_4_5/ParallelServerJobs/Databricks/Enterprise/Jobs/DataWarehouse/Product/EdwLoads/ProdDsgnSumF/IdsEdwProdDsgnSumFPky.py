# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------            
# MAGIC 
# MAGIC Nagesh Bandi                   2018-03-29           5832-Spira Care             Originally Programmed                      EnterpriseDev1             Jaideep Mankala           03/30/2018
# MAGIC 
# MAGIC Anitha Perumal                  2019-03-05           5832-Spira Care            Replaced source dataset with 
# MAGIC                                                                                                             sequential file stage                          EnterpriseDev1              Kalyan Neelam              2019-03-12

# MAGIC if there is no FKEY needed, just land it to Seq File for the load job follows.
# MAGIC New PKEYs are generated in this transformer. A database sequence will be used to create next PKEY value.
# MAGIC 
# MAGIC IMPORTANT: Make sure to change the Database SEQUENCE Name to the corresponding table name.
# MAGIC This job will be used when there is a need to Assign primary keys to incoming data before loading into Target table.
# MAGIC 
# MAGIC Extract dataset is created in the extract job ran prior to this.
# MAGIC 
# MAGIC Every K table will need a dedicated Database SEQUENCE to provide and track new numbers to use.
# MAGIC 
# MAGIC New KEYS generated for each run will be Inserted into the K table.
# MAGIC 
# MAGIC Job Name: IdsEdwProddEDCTSumFPky
# MAGIC Remove Duplicates stage takes care of getting rid of duplicate rows with the same Natural key get into the PKEYing step.
# MAGIC A concious effort need to be made to evaluate which one of the duplicate set row to drop is a case by case basis decision.
# MAGIC Insert new SKEY rows generated as part of this run. This is a database insert operation.
# MAGIC Left Outer Join Here
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, when, lit, rpad
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------

# MAGIC %run ../../../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','2018-03-22')
EDWRunCycle = get_widget_value('EDWRunCycle','100')

# Read from DB2ConnectorPX: db2_KProdDsgnSumFRead
jdbc_url, jdbc_props = get_db_config(edw_secret_name)
extract_query_db2_KProdDsgnSumFRead = """SELECT
SRC_SYS_CD,
TRIM(PROD_ID) AS PROD_ID,
PROD_CMPNT_EFF_DT_SK,
CRT_RUN_CYC_EXCTN_DT_SK,
PROD_DSGN_SUM_SK,
CRT_RUN_CYC_EXCTN_SK
FROM {0}.K_PROD_DSGN_SUM_F""".format(EDWOwner)
df_db2_KProdDsgnSumFRead = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_KProdDsgnSumFRead)
    .load()
)

# Read from PxSequentialFile: seq_ProdDsgnSumFPKey
schema_seq_ProdDsgnSumFPKey = StructType([
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PROD_ID", StringType(), False),
    StructField("PROD_CMPNT_EFF_DT_SK", StringType(), False),
    StructField("PROD_SK", IntegerType(), False),
    StructField("PROD_CMPNT_TERM_DT_SK", StringType(), False),
    StructField("SPIRA_BNF_ID", StringType(), False),
    StructField("UPP_XREF_ID", StringType(), False),
    StructField("CRT_RUN_CYC_EXCTN_DT_SK", StringType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", StringType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False)
])
df_seq_ProdDsgnSumFPKey = (
    spark.read
    .option("delimiter", ",")
    .option("quote", "\"")
    .schema(schema_seq_ProdDsgnSumFPKey)
    .csv(f"{adls_path}/verified/PROD_DSGN_SUM_F_Pkey.dat")
)

# cpy_MultiStreams (PxCopy)
df_cpy_MultiStreams_1 = df_seq_ProdDsgnSumFPKey.select(
    col("SRC_SYS_CD"),
    col("PROD_ID"),
    col("PROD_CMPNT_EFF_DT_SK"),
    col("PROD_SK"),
    col("PROD_CMPNT_TERM_DT_SK"),
    col("SPIRA_BNF_ID"),
    col("UPP_XREF_ID"),
    col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

df_cpy_MultiStreams_2 = df_seq_ProdDsgnSumFPKey.select(
    col("SRC_SYS_CD"),
    col("PROD_ID"),
    col("PROD_CMPNT_EFF_DT_SK")
)

# rdp_NaturalKeys (PxRemDup)
df_rdp_NaturalKeys = dedup_sort(
    df_cpy_MultiStreams_2,
    ["PROD_ID", "SRC_SYS_CD", "PROD_CMPNT_EFF_DT_SK"],
    [("PROD_ID", "A"), ("SRC_SYS_CD", "A"), ("PROD_CMPNT_EFF_DT_SK", "A")]
)

# jn_ProdDsgnSumF (PxJoin) - left outer join
df_jn_ProdDsgnSumF = df_rdp_NaturalKeys.alias("lnkRemDupDataOut").join(
    df_db2_KProdDsgnSumFRead.alias("lnkKProdDsgnSumF"),
    [
        col("lnkRemDupDataOut.PROD_ID") == col("lnkKProdDsgnSumF.PROD_ID"),
        col("lnkRemDupDataOut.SRC_SYS_CD") == col("lnkKProdDsgnSumF.SRC_SYS_CD"),
        col("lnkRemDupDataOut.PROD_CMPNT_EFF_DT_SK") == col("lnkKProdDsgnSumF.PROD_CMPNT_EFF_DT_SK")
    ],
    how="left"
)
df_jn_ProdDsgnSumF_select = df_jn_ProdDsgnSumF.select(
    col("lnkRemDupDataOut.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("lnkRemDupDataOut.PROD_ID").alias("PROD_ID"),
    col("lnkRemDupDataOut.PROD_CMPNT_EFF_DT_SK").alias("PROD_CMPNT_EFF_DT_SK"),
    col("lnkKProdDsgnSumF.CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    col("lnkKProdDsgnSumF.PROD_DSGN_SUM_SK").alias("PROD_DSGN_SUM_SK"),
    col("lnkKProdDsgnSumF.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK")
)

# xfm_PKEYgen (CTransformerStage)
df_enriched = df_jn_ProdDsgnSumF_select.withColumn(
    "svProdDedctSumFSK",
    when(col("PROD_DSGN_SUM_SK").isNull(), lit(None)).otherwise(col("PROD_DSGN_SUM_SK"))
)
df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"svProdDedctSumFSK",<schema>,<secret_name>)

df_insKProdDsgnSumFOut = df_enriched.filter(col("PROD_DSGN_SUM_SK").isNull()).select(
    col("PROD_ID").alias("PROD_ID"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("PROD_CMPNT_EFF_DT_SK").alias("PROD_CMPNT_EFF_DT_SK"),
    lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    col("svProdDedctSumFSK").alias("PROD_DSGN_SUM_SK"),
    lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK")
)

df_lnkPKEYxfmOut = df_enriched.select(
    col("PROD_ID").alias("PROD_ID"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("PROD_CMPNT_EFF_DT_SK").alias("PROD_CMPNT_EFF_DT_SK"),
    col("svProdDedctSumFSK").alias("PROD_DSGN_SUM_SK"),
    when(col("PROD_DSGN_SUM_SK").isNull(), lit(EDWRunCycleDate)).otherwise(col("CRT_RUN_CYC_EXCTN_DT_SK")).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    when(col("PROD_DSGN_SUM_SK").isNull(), lit(EDWRunCycle)).otherwise(col("CRT_RUN_CYC_EXCTN_SK")).alias("CRT_RUN_CYC_EXCTN_SK")
)

# db2_KProdDsgnSumFLoad (DB2ConnectorPX) - Merge logic
temp_table_db2_KProdDsgnSumFLoad = "STAGING.IdsEdwProdDsgnSumFPky_db2_KProdDsgnSumFLoad_temp"
merge_sql_db2_drop = f"DROP TABLE IF EXISTS {temp_table_db2_KProdDsgnSumFLoad}"
execute_dml(merge_sql_db2_drop, jdbc_url, jdbc_props)
df_insKProdDsgnSumFOut.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", temp_table_db2_KProdDsgnSumFLoad) \
    .mode("overwrite") \
    .save()
merge_sql_db2 = f"""
MERGE INTO {EDWOwner}.K_PROD_DSGN_SUM_F AS T
USING {temp_table_db2_KProdDsgnSumFLoad} AS S
ON
    T.SRC_SYS_CD = S.SRC_SYS_CD
    AND T.PROD_ID = S.PROD_ID
    AND T.PROD_CMPNT_EFF_DT_SK = S.PROD_CMPNT_EFF_DT_SK
WHEN MATCHED THEN
    UPDATE SET
        T.SRC_SYS_CD = S.SRC_SYS_CD,
        T.PROD_ID = S.PROD_ID,
        T.PROD_CMPNT_EFF_DT_SK = S.PROD_CMPNT_EFF_DT_SK,
        T.CRT_RUN_CYC_EXCTN_DT_SK = S.CRT_RUN_CYC_EXCTN_DT_SK,
        T.PROD_DSGN_SUM_SK = S.PROD_DSGN_SUM_SK,
        T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK
WHEN NOT MATCHED THEN
    INSERT (
        SRC_SYS_CD,
        PROD_ID,
        PROD_CMPNT_EFF_DT_SK,
        CRT_RUN_CYC_EXCTN_DT_SK,
        PROD_DSGN_SUM_SK,
        CRT_RUN_CYC_EXCTN_SK
    )
    VALUES (
        S.SRC_SYS_CD,
        S.PROD_ID,
        S.PROD_CMPNT_EFF_DT_SK,
        S.CRT_RUN_CYC_EXCTN_DT_SK,
        S.PROD_DSGN_SUM_SK,
        S.CRT_RUN_CYC_EXCTN_SK
    );
"""
execute_dml(merge_sql_db2, jdbc_url, jdbc_props)

# jn_PKEYs (PxJoin) - left outer join
df_jn_PKEYs = df_cpy_MultiStreams_1.alias("lnkFullDataJnIn").join(
    df_lnkPKEYxfmOut.alias("lnkPKEYxfmOut"),
    [
        col("lnkFullDataJnIn.PROD_ID") == col("lnkPKEYxfmOut.PROD_ID"),
        col("lnkFullDataJnIn.SRC_SYS_CD") == col("lnkPKEYxfmOut.SRC_SYS_CD"),
        col("lnkFullDataJnIn.PROD_CMPNT_EFF_DT_SK") == col("lnkPKEYxfmOut.PROD_CMPNT_EFF_DT_SK")
    ],
    how="left"
)
df_jn_PKEYs_select = df_jn_PKEYs.select(
    col("lnkPKEYxfmOut.PROD_DSGN_SUM_SK").alias("PROD_DSGN_SUM_SK"),
    col("lnkFullDataJnIn.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("lnkFullDataJnIn.PROD_ID").alias("PROD_ID"),
    col("lnkFullDataJnIn.PROD_CMPNT_EFF_DT_SK").alias("PROD_CMPNT_EFF_DT_SK"),
    col("lnkPKEYxfmOut.CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    col("lnkFullDataJnIn.LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("lnkFullDataJnIn.PROD_SK").alias("PROD_SK"),
    col("lnkFullDataJnIn.PROD_CMPNT_TERM_DT_SK").alias("PROD_CMPNT_TERM_DT_SK"),
    col("lnkFullDataJnIn.SPIRA_BNF_ID").alias("SPIRA_BNF_ID"),
    col("lnkFullDataJnIn.UPP_XREF_ID").alias("UPP_XREF_ID"),
    col("lnkPKEYxfmOut.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("lnkFullDataJnIn.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

# sf_Prod_Dsgn_Sum_F_load (PxSequentialFile) - final write
df_sf_Prod_Dsgn_Sum_F_load = df_jn_PKEYs_select.withColumn(
    "SRC_SYS_CD", rpad(col("SRC_SYS_CD"), <...>, " ")
).withColumn(
    "PROD_ID", rpad(col("PROD_ID"), <...>, " ")
).withColumn(
    "PROD_CMPNT_EFF_DT_SK", rpad(col("PROD_CMPNT_EFF_DT_SK"), 10, " ")
).withColumn(
    "CRT_RUN_CYC_EXCTN_DT_SK", rpad(col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ")
).withColumn(
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK", rpad(col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ")
).withColumn(
    "PROD_CMPNT_TERM_DT_SK", rpad(col("PROD_CMPNT_TERM_DT_SK"), 10, " ")
).withColumn(
    "SPIRA_BNF_ID", rpad(col("SPIRA_BNF_ID"), <...>, " ")
).withColumn(
    "UPP_XREF_ID", rpad(col("UPP_XREF_ID"), <...>, " ")
)

df_sf_Prod_Dsgn_Sum_F_load_out = df_sf_Prod_Dsgn_Sum_F_load.select(
    "PROD_DSGN_SUM_SK",
    "SRC_SYS_CD",
    "PROD_ID",
    "PROD_CMPNT_EFF_DT_SK",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "PROD_SK",
    "PROD_CMPNT_TERM_DT_SK",
    "SPIRA_BNF_ID",
    "UPP_XREF_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK"
)

write_files(
    df_sf_Prod_Dsgn_Sum_F_load_out,
    f"{adls_path}/load/PROD_DSGN_SUM_F.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)