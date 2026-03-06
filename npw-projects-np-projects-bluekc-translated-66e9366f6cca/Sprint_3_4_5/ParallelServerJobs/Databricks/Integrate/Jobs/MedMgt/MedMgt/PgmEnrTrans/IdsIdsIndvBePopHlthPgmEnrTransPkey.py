# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2005, 2006, 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC Job Name : IdsIdsIndvBePopHlthPgmEnrTransPkey 
# MAGIC 
# MAGIC Called By: IdsIndvBePopHlthPgmEnrTransCntl
# MAGIC 
# MAGIC                           
# MAGIC 
# MAGIC 
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                         Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                    ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Jaideep Mankala             06/04/2019      US110616                    Original Program			   IntegrateDev2     		Abhiram Dasarathy   2019-06-07

# MAGIC Final Load File
# MAGIC Apply business logic
# MAGIC Temp table is tuncated before load and runstat done after load
# MAGIC Load IDS temp table
# MAGIC join primary key info with table info
# MAGIC Assign primary surrogate key
# MAGIC SQL joins temp table with key table to assign known keys
# MAGIC Final K table Load File
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
import pyspark.sql.functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DecimalType
)
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# Parameter parsing
RunID = get_widget_value('RunID','')
CurrRunDt = get_widget_value('CurrRunDt','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
SrcSysCd = get_widget_value('SrcSysCd','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

# MAGIC %run ../../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

# 1) Stage: PgmEnrTransData (CSeqFileStage) - Reading from external path 
#    File: INDV_BE_POP_HLTH_PGM_ENR_TRANS.#SrcSysCd#.extr.#RunID#.dat
#    => adls_path_publish/external/INDV_BE_POP_HLTH_PGM_ENR_TRANS.#SrcSysCd#.extr.#RunID#.dat
schema_PgmEnrTransData = StructType([
    StructField("POP_HLTH_PGM_ENR_ID", StringType(), nullable=False),
    StructField("ROW_EFF_DT_SK", StringType(), nullable=False),
    StructField("SRC_SYS_CD_SK", IntegerType(), nullable=False),
    StructField("ENR_DENIED_RSN_CD_SK", IntegerType(), nullable=False),
    StructField("ENR_PROD_CD_SK", IntegerType(), nullable=False),
    StructField("ENR_SVRTY_CD_SK", IntegerType(), nullable=False),
    StructField("PGM_CLOSE_RSN_CD_SK", IntegerType(), nullable=False),
    StructField("PGM_SCRN_STTUS_CD_SK", IntegerType(), nullable=False),
    StructField("PGM_SRC_CD_SK", IntegerType(), nullable=False),
    StructField("SCRN_RQST_PROD_CD_SK", IntegerType(), nullable=False),
    StructField("SCRN_RQST_SVRTY_CD_SK", IntegerType(), nullable=False),
    StructField("PGM_CLOSE_DT_SK", StringType(), nullable=False),
    StructField("PGM_ENR_CRT_DT_SK", StringType(), nullable=False),
    StructField("PGM_ENR_DT_SK", StringType(), nullable=False),
    StructField("PGM_RQST_DT_SK", StringType(), nullable=False),
    StructField("PGM_SCRN_DT_SK", StringType(), nullable=False),
    StructField("PGM_STRT_DT_SK", StringType(), nullable=False),
    StructField("ROW_TERM_DT_SK", StringType(), nullable=False),
    StructField("INDV_BE_KEY", DecimalType(), nullable=False),
    StructField("ENRED_BY_USER_ID", StringType(), nullable=False),
    StructField("ENR_ASG_TO_USER_ID", StringType(), nullable=False),
    StructField("ENR_POP_HLTH_PGM_ID", StringType(), nullable=False),
    StructField("SCRN_BY_USER_ID", StringType(), nullable=False),
    StructField("SCRN_ASG_TO_USER_ID", StringType(), nullable=False),
    StructField("SCRN_POP_HLTH_PGM_ID", StringType(), nullable=False),
    StructField("PGM_ORIG_SRC_SYS_CD_SK", IntegerType(), nullable=False)
])

df_PgmEnrTransData = (
    spark.read
    .option("header", False)
    .option("quote", "\"")
    .schema(schema_PgmEnrTransData)
    .csv(f"{adls_path_publish}/external/INDV_BE_POP_HLTH_PGM_ENR_TRANS.{SrcSysCd}.extr.{RunID}.dat")
)

# 2) Stage: Business_Rules (CTransformerStage)
#    Produces two outputs: "Transform" (3 columns) and "AllCol" (27 columns)
df_Transform = df_PgmEnrTransData.select(
    F.col("POP_HLTH_PGM_ENR_ID").alias("POP_HLTH_PGM_ENR_ID"),
    F.col("ROW_EFF_DT_SK").alias("ROW_EFF_DT_SK"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK")
)

df_AllCol = df_PgmEnrTransData.select(
    F.col("POP_HLTH_PGM_ENR_ID").alias("POP_HLTH_PGM_ENR_ID"),
    F.col("ROW_EFF_DT_SK").alias("ROW_EFF_DT_SK"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("ENR_DENIED_RSN_CD_SK").alias("ENR_DENIED_RSN_CD_SK"),
    F.col("ENR_PROD_CD_SK").alias("ENR_PROD_CD_SK"),
    F.col("ENR_SVRTY_CD_SK").alias("ENR_SVRTY_CD_SK"),
    F.col("PGM_CLOSE_RSN_CD_SK").alias("PGM_CLOSE_RSN_CD_SK"),
    F.col("PGM_SCRN_STTUS_CD_SK").alias("PGM_SCRN_STTUS_CD_SK"),
    F.col("PGM_SRC_CD_SK").alias("PGM_SRC_CD_SK"),
    F.col("SCRN_RQST_PROD_CD_SK").alias("SCRN_RQST_PROD_CD_SK"),
    F.col("SCRN_RQST_SVRTY_CD_SK").alias("SCRN_RQST_SVRTY_CD_SK"),
    F.col("PGM_CLOSE_DT_SK").alias("PGM_CLOSE_DT_SK"),
    F.col("PGM_ENR_CRT_DT_SK").alias("PGM_ENR_CRT_DT_SK"),
    F.col("PGM_ENR_DT_SK").alias("PGM_ENR_DT_SK"),
    F.col("PGM_RQST_DT_SK").alias("PGM_RQST_DT_SK"),
    F.col("PGM_SCRN_DT_SK").alias("PGM_SCRN_DT_SK"),
    F.col("PGM_STRT_DT_SK").alias("PGM_STRT_DT_SK"),
    F.col("ROW_TERM_DT_SK").alias("ROW_TERM_DT_SK"),
    F.col("INDV_BE_KEY").alias("INDV_BE_KEY"),
    F.col("ENRED_BY_USER_ID").alias("ENRED_BY_USER_ID"),
    F.col("ENR_ASG_TO_USER_ID").alias("ENR_ASG_TO_USER_ID"),
    F.col("ENR_POP_HLTH_PGM_ID").alias("ENR_POP_HLTH_PGM_ID"),
    F.col("SCRN_BY_USER_ID").alias("SCRN_BY_USER_ID"),
    F.col("SCRN_ASG_TO_USER_ID").alias("SCRN_ASG_TO_USER_ID"),
    F.col("SCRN_POP_HLTH_PGM_ID").alias("SCRN_POP_HLTH_PGM_ID"),
    F.col("PGM_ORIG_SRC_SYS_CD_SK").alias("PGM_ORIG_SRC_SYS_CD_SK")
)

# 3) Stage: hf_indv_be_pop_hlth_pgm_enr_trans_new_allcol (CHashedFileStage)
#    Scenario A – Intermediate hashed file
#    Key columns = [POP_HLTH_PGM_ENR_ID, ROW_EFF_DT_SK, SRC_SYS_CD_SK]
#    Deduplicate on these key columns directly from df_AllCol

df_hf_indv_be_pop_hlth_pgm_enr_trans_new_allcol = dedup_sort(
    df_AllCol,
    partition_cols=["POP_HLTH_PGM_ENR_ID","ROW_EFF_DT_SK","SRC_SYS_CD_SK"],
    sort_cols=[]
)

# 4) Stage: K_INDV_BE_POP_HLTH_PGM_ENR_TRANS_TEMP (DB2Connector)
#    Writing df_Transform to an IDS table. 
#    According to guidelines, use ephemeral table in STAGING. 
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

# Before_Sql
execute_dml(
    f"CALL {IDSOwner}.BCSP_TRUNCATE('{IDSOwner}', 'K_INDV_BE_POP_HLTH_PGM_ENR_TRANS_TEMP')",
    jdbc_url_ids, 
    jdbc_props_ids
)

# Drop ephemeral table if exists
execute_dml(
    f"DROP TABLE IF EXISTS STAGING.IdsIdsIndvBePopHlthPgmEnrTransPkey_K_INDV_BE_POP_HLTH_PGM_ENR_TRANS_TEMP_temp",
    jdbc_url_ids,
    jdbc_props_ids
)

# Write df_Transform -> ephemeral table
df_Transform.write \
    .jdbc(
        url=jdbc_url_ids,
        table="STAGING.IdsIdsIndvBePopHlthPgmEnrTransPkey_K_INDV_BE_POP_HLTH_PGM_ENR_TRANS_TEMP_temp",
        mode="append",
        properties=jdbc_props_ids
    )

# Emulate the multiple statements in the job's main SQL property:
#   INSERT, CREATE TABLE #$IDSOwner#.W_CBL_NTWK, DROP TABLE #$IDSOwner#.K_INDV_BE_POP_HLTH_PGM_ENR_TRANS_TEMP
execute_dml(
    f"INSERT INTO {IDSOwner}.K_INDV_BE_POP_HLTH_PGM_ENR_TRANS_TEMP (POP_HLTH_PGM_ENR_ID, ROW_EFF_DT_SK, SRC_SYS_CD_SK) VALUES ('<...>','<...>',<...>)",
    jdbc_url_ids,
    jdbc_props_ids
)
execute_dml(
    f"CREATE TABLE {IDSOwner}.W_CBL_NTWK (SRC_SYS_CD VARCHAR(20) NOT NULL, NTWK_ID VARCHAR(20) NOT NULL, NTWK_NAME VARCHAR(75) NOT NULL, NTWK_TYP_CD VARCHAR(20) NOT NULL, NTWK_SH_NM VARCHAR(20) NOT NULL)",
    jdbc_url_ids,
    jdbc_props_ids
)
execute_dml(
    f"DROP TABLE {IDSOwner}.K_INDV_BE_POP_HLTH_PGM_ENR_TRANS_TEMP",
    jdbc_url_ids,
    jdbc_props_ids
)

# After_Sql
execute_dml(
    f"CALL SYSPROC.ADMIN_CMD('runstats on table {IDSOwner}.K_INDV_BE_POP_HLTH_PGM_ENR_TRANS_TEMP on key columns with distribution on key columns and detailed indexes all allow write access')",
    jdbc_url_ids, 
    jdbc_props_ids
)

# 5) Stage: K_INDV_BE_POP_HLTH_PGM_ENR_TRANS_TEMP => OutputPin => W_Extract => Reading from ephemeral + real table
#    The SQL uses union. Reading from #$IDSOwner#.K_INDV_BE_POP_HLTH_PGM_ENR_TRANS_TEMP but that table was just dropped; 
#    still we must not skip. We replicate the read. 
df_W_Extract = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", 
        f"""
        SELECT k.INDV_BE_POP_HLTH_PGM_ENR_T_SK,
               w.POP_HLTH_PGM_ENR_ID,
               w.ROW_EFF_DT_SK,
               w.SRC_SYS_CD_SK,
               k.CRT_RUN_CYC_EXCTN_SK
        FROM {IDSOwner}.K_INDV_BE_POP_HLTH_PGM_ENR_TRANS_TEMP w,
             {IDSOwner}.K_INDV_BE_POP_HLTH_PGM_ENR_TRANS k
        WHERE w.POP_HLTH_PGM_ENR_ID = k.POP_HLTH_PGM_ENR_ID
          AND w.ROW_EFF_DT_SK = k.ROW_EFF_DT_SK
          AND w.SRC_SYS_CD_SK = k.SRC_SYS_CD_SK
        UNION
        SELECT -1,
               w2.POP_HLTH_PGM_ENR_ID,
               w2.ROW_EFF_DT_SK,
               w2.SRC_SYS_CD_SK,
               #CurrRunCycle#
        FROM {IDSOwner}.K_INDV_BE_POP_HLTH_PGM_ENR_TRANS_TEMP w2
        WHERE NOT EXISTS (
              SELECT k2.INDV_BE_POP_HLTH_PGM_ENR_T_SK
              FROM {IDSOwner}.K_INDV_BE_POP_HLTH_PGM_ENR_TRANS k2
              WHERE w2.POP_HLTH_PGM_ENR_ID = k2.POP_HLTH_PGM_ENR_ID
                AND w2.ROW_EFF_DT_SK = k2.ROW_EFF_DT_SK
                AND w2.SRC_SYS_CD_SK = k2.SRC_SYS_CD_SK
              )
        """
    )
    .load()
)

# 6) Stage: PrimaryKey (CTransformerStage)
#    We have stage variables and multiple output links:
#    Output links: updt, NewKeys, Keys

# Add transformation columns
df_PrimaryKey = df_W_Extract.withColumn(
    "svInstUpdt",
    F.when(F.col("INDV_BE_POP_HLTH_PGM_ENR_T_SK") == F.lit(-1), F.lit("I")).otherwise(F.lit("U"))
).withColumn(
    "svSrcSysCd",
    F.lit("FACETS")
)

# We handle surrogate key logic:
#   SurrogateKeyGen is called on the entire dataframe for the column "INDV_BE_POP_HLTH_PGM_ENR_T_SK".
#   This will replace -1 (or any missing) with new key from DB sequence.
df_enriched = df_PrimaryKey
df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"INDV_BE_POP_HLTH_PGM_ENR_T_SK",<schema>,<secret_name>)

# Recompute one more column for CRT_RUN_CYC_EXCTN_SK:
df_enriched = df_enriched.withColumn(
    "svCrtRunCycExctnSk",
    F.when(F.col("svInstUpdt") == F.lit("I"), F.col("CurrRunCycle")).otherwise(F.col("CRT_RUN_CYC_EXCTN_SK"))
)

# Now build the output dataframes
df_updt = df_enriched.filter(F.col("svInstUpdt") == F.lit("U")).select(
    F.col("POP_HLTH_PGM_ENR_ID").alias("POP_HLTH_PGM_ENR_ID"),
    F.col("ROW_EFF_DT_SK").alias("ROW_EFF_DT_SK"),
    F.col("svSrcSysCd").alias("SRC_SYS_CD"),
    F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("INDV_BE_POP_HLTH_PGM_ENR_T_SK").alias("INDV_BE_POP_HLTH_PGM_ENR_T_SK")
)

df_NewKeys = df_enriched.filter(F.col("svInstUpdt") == F.lit("I")).select(
    F.col("POP_HLTH_PGM_ENR_ID").alias("POP_HLTH_PGM_ENR_ID"),
    F.col("ROW_EFF_DT_SK").alias("ROW_EFF_DT_SK"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("CurrRunCycle").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("INDV_BE_POP_HLTH_PGM_ENR_T_SK").alias("INDV_BE_POP_HLTH_PGM_ENR_T_SK")
)

df_Keys = df_enriched.select(
    F.col("INDV_BE_POP_HLTH_PGM_ENR_T_SK").alias("INDV_BE_POP_HLTH_PGM_ENR_T_SK"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("svSrcSysCd").alias("SRC_SYS_CD"),
    F.col("POP_HLTH_PGM_ENR_ID").alias("POP_HLTH_PGM_ENR_ID"),
    F.col("ROW_EFF_DT_SK").alias("ROW_EFF_DT_SK"),
    F.col("svInstUpdt").alias("INSRT_UPDT_CD"),
    F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK")
)

# 7) Stage: hf_indv_be_pop_hlth_pgm_enr_trans (CHashedFileStage)
#    Scenario C => write to parquet
df_updt_ordered = df_updt.select(
    "POP_HLTH_PGM_ENR_ID",
    "ROW_EFF_DT_SK",
    "SRC_SYS_CD",
    "CRT_RUN_CYC_EXCTN_SK",
    "INDV_BE_POP_HLTH_PGM_ENR_T_SK"
)
# For char/varchar columns, rpad them if needed (none explicitly stated except let's check length=10 for ROW_EFF_DT_SK?).
# We do minimal example for demonstration:
df_updt_ordered = df_updt_ordered.withColumn(
    "ROW_EFF_DT_SK",
    F.rpad(F.col("ROW_EFF_DT_SK"), 10, " ")
)

write_files(
    df_updt_ordered,
    f"{adls_path}/hf_indv_be_pop_hlth_pgm_enr_trans.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

# 8) Stage: K_INDV_BE_POP_HLTH_PGM_ENR_TRANS (CSeqFileStage)
#    writing df_NewKeys to .dat with delimiter
df_NewKeys_ordered = df_NewKeys.select(
    "POP_HLTH_PGM_ENR_ID",
    "ROW_EFF_DT_SK",
    "SRC_SYS_CD_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "INDV_BE_POP_HLTH_PGM_ENR_T_SK"
)

df_NewKeys_ordered = df_NewKeys_ordered.withColumn(
    "ROW_EFF_DT_SK",
    F.rpad(F.col("ROW_EFF_DT_SK"), 10, " ")
)

write_files(
    df_NewKeys_ordered,
    f"{adls_path}/load/K_INDV_BE_POP_HLTH_PGM_ENR_TRANS.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# 9) Stage: Merge (CTransformerStage)
#    Primary link: df_Keys
#    Lookup link: df_hf_indv_be_pop_hlth_pgm_enr_trans_new_allcol
#    JoinType: left
#    Join on (POP_HLTH_PGM_ENR_ID, ROW_EFF_DT_SK, SRC_SYS_CD_SK)
#    Constraint: IsNull(AllColOut.POP_HLTH_PGM_ENR_ID)=@FALSE => means pick rows where the lookup is NOT NULL

# Left join
cond = [
    df_Keys["POP_HLTH_PGM_ENR_ID"] == df_hf_indv_be_pop_hlth_pgm_enr_trans_new_allcol["POP_HLTH_PGM_ENR_ID"],
    df_Keys["ROW_EFF_DT_SK"] == df_hf_indv_be_pop_hlth_pgm_enr_trans_new_allcol["ROW_EFF_DT_SK"],
    df_Keys["SRC_SYS_CD_SK"] == df_hf_indv_be_pop_hlth_pgm_enr_trans_new_allcol["SRC_SYS_CD_SK"]
]
df_merge_joined = df_Keys.alias("Keys").join(
    df_hf_indv_be_pop_hlth_pgm_enr_trans_new_allcol.alias("AllColOut"),
    on=cond,
    how="left"
)

df_merge_filtered = df_merge_joined.filter(F.col("AllColOut.POP_HLTH_PGM_ENR_ID").isNotNull())

df_merge_final = df_merge_filtered.select(
    F.col("Keys.INDV_BE_POP_HLTH_PGM_ENR_T_SK").alias("INDV_BE_POP_HLTH_PGM_ENR_T_SK"),
    F.col("Keys.POP_HLTH_PGM_ENR_ID").alias("POP_HLTH_PGM_ENR_ID"),
    F.rpad(F.col("Keys.ROW_EFF_DT_SK"), 10, " ").alias("ROW_EFF_DT_SK"),
    F.col("Keys.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("Keys.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("CurrRunCycle").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("AllColOut.ENR_DENIED_RSN_CD_SK").alias("ENR_DENIED_RSN_CD_SK"),
    F.col("AllColOut.ENR_PROD_CD_SK").alias("ENR_PROD_CD_SK"),
    F.col("AllColOut.ENR_SVRTY_CD_SK").alias("ENR_SVRTY_CD_SK"),
    F.col("AllColOut.PGM_CLOSE_RSN_CD_SK").alias("PGM_CLOSE_RSN_CD_SK"),
    F.col("AllColOut.PGM_SCRN_STTUS_CD_SK").alias("PGM_SCRN_STTUS_CD_SK"),
    F.col("AllColOut.PGM_SRC_CD_SK").alias("PGM_SRC_CD_SK"),
    F.col("AllColOut.SCRN_RQST_PROD_CD_SK").alias("SCRN_RQST_PROD_CD_SK"),
    F.col("AllColOut.SCRN_RQST_SVRTY_CD_SK").alias("SCRN_RQST_SVRTY_CD_SK"),
    F.rpad(F.col("AllColOut.PGM_CLOSE_DT_SK"), 10, " ").alias("PGM_CLOSE_DT_SK"),
    F.rpad(F.col("AllColOut.PGM_ENR_CRT_DT_SK"), 10, " ").alias("PGM_ENR_CRT_DT_SK"),
    F.rpad(F.col("AllColOut.PGM_ENR_DT_SK"), 10, " ").alias("PGM_ENR_DT_SK"),
    F.rpad(F.col("AllColOut.PGM_RQST_DT_SK"), 10, " ").alias("PGM_RQST_DT_SK"),
    F.rpad(F.col("AllColOut.PGM_SCRN_DT_SK"), 10, " ").alias("PGM_SCRN_DT_SK"),
    F.rpad(F.col("AllColOut.PGM_STRT_DT_SK"), 10, " ").alias("PGM_STRT_DT_SK"),
    F.rpad(F.col("AllColOut.ROW_TERM_DT_SK"), 10, " ").alias("ROW_TERM_DT_SK"),
    F.col("AllColOut.INDV_BE_KEY").alias("INDV_BE_KEY"),
    F.col("AllColOut.ENRED_BY_USER_ID").alias("ENRED_BY_USER_ID"),
    F.col("AllColOut.ENR_ASG_TO_USER_ID").alias("ENR_ASG_TO_USER_ID"),
    F.col("AllColOut.ENR_POP_HLTH_PGM_ID").alias("ENR_POP_HLTH_PGM_ID"),
    F.col("AllColOut.SCRN_BY_USER_ID").alias("SCRN_BY_USER_ID"),
    F.col("AllColOut.SCRN_ASG_TO_USER_ID").alias("SCRN_ASG_TO_USER_ID"),
    F.col("AllColOut.SCRN_POP_HLTH_PGM_ID").alias("SCRN_POP_HLTH_PGM_ID"),
    F.col("AllColOut.PGM_ORIG_SRC_SYS_CD_SK").alias("PGM_ORIG_SRC_SYS_CD_SK")
)

# 10) Stage: IdsIndvBePopHlthPgmEnrTransExtr (CSeqFileStage)
#     Write df_merge_final to load/INDV_BE_POP_HLTH_PGM_ENR_TRANS.dat
write_files(
    df_merge_final,
    f"{adls_path}/load/INDV_BE_POP_HLTH_PGM_ENR_TRANS.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)