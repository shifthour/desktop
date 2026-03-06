# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2015 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC                            
# MAGIC PROCESSING: Generates Primary Key for BCBSA_FEP_MBR_ENR table
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC ========================================================================================================================================
# MAGIC 												DATASTAGE	CODE		DATE OF
# MAGIC DEVELOPER	DATE		PROJECT	DESCRIPTION					ENVIRONMENT	REVIEWER	REVIEW
# MAGIC ========================================================================================================================================
# MAGIC Kalyan Neelam	2015-11-24	5403		Initial Programming					IntegrateDev1	Bhoomi Dasari	12/2/2015
# MAGIC Abhiram Dasarathy	2016-11-02	5568 - HEDIS	Added FEP_MBR_ID column to end of the processing	IntegrateDev2          Kalyan Neelam       2016-11-15
# MAGIC Kailashnath Jadhav	2017-06-27	5781 - HEDIS	Added FEP_COV_TYP_TX column to end of the processing	IntegrateDev1          Kalyan Neelam       2017-06-29

# MAGIC New PKEYs are generated in this transformer. A database sequence will be used to create next PKEY value.
# MAGIC Insert Only the Newly generated Pkeys into the K Table
# MAGIC Read K_BCBSA_FEP_MBR_ENR Table to pull the Natural Keys and the Skey.
# MAGIC Inner Join primary key info with table info
# MAGIC Left Join on Natural Keys
# MAGIC Transformed Data will land into a Dataset for Primary Keying job.
# MAGIC 
# MAGIC Data is partitioned on Natural Key Columns.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, when, rpad
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


# Acquire parameter values
SrcSysCd = get_widget_value('SrcSysCd','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
IDSRunCycle = get_widget_value('IDSRunCycle','')
RunID = get_widget_value('RunID','')

# STAGE: ds_BCBSA_FEP_MBR_ENR_Xfm (PxDataSet)
# Translate DataSet (.ds) to .parquet for reading.
df_ds_BCBSA_FEP_MBR_ENR_Xfm = spark.read.parquet(
    f"{adls_path}/ds/BCBSA_FEP_MBR_ENR.{SrcSysCd}.xfrm.{RunID}.parquet"
)

# STAGE: cp_pk (PxCopy)
df_cp_pk = df_ds_BCBSA_FEP_MBR_ENR_Xfm

df_lnk_Transforms_Out = df_cp_pk.select(
    col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    col("PROD_SH_NM").alias("PROD_SH_NM"),
    col("FEP_MBR_ENR_EFF_DT_SK").alias("FEP_MBR_ENR_EFF_DT_SK"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD")
)

df_Lnk_cp_Out = df_cp_pk.select(
    col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    col("PROD_SH_NM").alias("PROD_SH_NM"),
    col("FEP_MBR_ENR_EFF_DT_SK").alias("FEP_MBR_ENR_EFF_DT_SK"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    col("BC_PLN_CD").alias("BC_PLN_CD"),
    col("BS_PLN_CD").alias("BS_PLN_CD"),
    col("CHMCL_DPNDC_BNF_IN").alias("CHMCL_DPNDC_BNF_IN"),
    col("CHMCL_DPNDC_IP_BNF_IN").alias("CHMCL_DPNDC_IP_BNF_IN"),
    col("CHMCL_DPNDC_IP_PRTL_DAY_BNF_IN").alias("CHMCL_DPNDC_IP_PRTL_DAY_BNF_IN"),
    col("CHMCL_DPNDC_OP_BNF_IN").alias("CHMCL_DPNDC_OP_BNF_IN"),
    col("CONF_COMM_IN").alias("CONF_COMM_IN"),
    col("DNTL_BNF_IN").alias("DNTL_BNF_IN"),
    col("MNTL_HLTH_BNF_IN").alias("MNTL_HLTH_BNF_IN"),
    col("MNTL_HLTH_IP_BNF_IN").alias("MNTL_HLTH_IP_BNF_IN"),
    col("MNTL_HLTH_IP_PRTL_DAY_BNF_IN").alias("MNTL_HLTH_IP_PRTL_DAY_BNF_IN"),
    col("MNTL_HLTH_OP_BNF_IN").alias("MNTL_HLTH_OP_BNF_IN"),
    col("OP_BNF_IN").alias("OP_BNF_IN"),
    col("PDX_BNF_IN").alias("PDX_BNF_IN"),
    col("FEP_MBR_ENR_TERM_DT_SK").alias("FEP_MBR_ENR_TERM_DT_SK"),
    col("SRC_SYS_LAST_UPDT_DT_SK").alias("SRC_SYS_LAST_UPDT_DT_SK"),
    col("FEP_PLN_PROD_ID").alias("FEP_PLN_PROD_ID"),
    col("FEP_PLN_RGN_ID").alias("FEP_PLN_RGN_ID"),
    col("MBR_ID").alias("MBR_ID"),
    col("MBR_EMPLMT_STTUS_NM").alias("MBR_EMPLMT_STTUS_NM"),
    col("PROD_DESC").alias("PROD_DESC"),
    col("FEP_MBR_ID").alias("FEP_MBR_ID"),
    col("FEP_COV_TYP_TX").alias("FEP_COV_TYP_TX")
)

# STAGE: rdup_Natural_Keys (PxRemDup)
df_rdup_Natural_Keys = dedup_sort(
    df_lnk_Transforms_Out,
    partition_cols=["MBR_UNIQ_KEY", "PROD_SH_NM", "FEP_MBR_ENR_EFF_DT_SK", "SRC_SYS_CD"],
    sort_cols=[]
)

df_lnk_Natural_Keys_out = df_rdup_Natural_Keys.select(
    col("MBR_UNIQ_KEY"),
    col("PROD_SH_NM"),
    col("FEP_MBR_ENR_EFF_DT_SK"),
    col("SRC_SYS_CD")
)

# STAGE: db2_K_BCBSA_FEP_MBR_ENR_in (DB2ConnectorPX) - Reading from IDS
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
extract_query_ids = (
    f"SELECT MBR_UNIQ_KEY, PROD_SH_NM, FEP_MBR_ENR_EFF_DT_SK, SRC_SYS_CD, "
    f"CRT_RUN_CYC_EXCTN_SK, BCBSA_FEP_MBR_ENR_SK "
    f"FROM {IDSOwner}.K_BCBSA_FEP_MBR_ENR"
)
df_db2_K_BCBSA_FEP_MBR_ENR_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_ids)
    .load()
)

# STAGE: jn_BcbsaFepMbrEnr (PxJoin, left outer join)
df_jn_BcbsaFepMbrEnr_temp = (
    df_lnk_Natural_Keys_out.alias("lnk_Natural_Keys_out")
    .join(
        df_db2_K_BCBSA_FEP_MBR_ENR_in.alias("lnk_KBcbsaFepMbrEnrPkey_out"),
        on=[
            col("lnk_Natural_Keys_out.MBR_UNIQ_KEY") == col("lnk_KBcbsaFepMbrEnrPkey_out.MBR_UNIQ_KEY"),
            col("lnk_Natural_Keys_out.PROD_SH_NM") == col("lnk_KBcbsaFepMbrEnrPkey_out.PROD_SH_NM"),
            col("lnk_Natural_Keys_out.FEP_MBR_ENR_EFF_DT_SK") == col("lnk_KBcbsaFepMbrEnrPkey_out.FEP_MBR_ENR_EFF_DT_SK"),
            col("lnk_Natural_Keys_out.SRC_SYS_CD") == col("lnk_KBcbsaFepMbrEnrPkey_out.SRC_SYS_CD")
        ],
        how="left"
    )
)

df_jn_BcbsaFepMbrEnr = df_jn_BcbsaFepMbrEnr_temp.select(
    col("lnk_Natural_Keys_out.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    col("lnk_Natural_Keys_out.PROD_SH_NM").alias("PROD_SH_NM"),
    col("lnk_Natural_Keys_out.FEP_MBR_ENR_EFF_DT_SK").alias("FEP_MBR_ENR_EFF_DT_SK"),
    col("lnk_Natural_Keys_out.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("lnk_KBcbsaFepMbrEnrPkey_out.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("lnk_KBcbsaFepMbrEnrPkey_out.BCBSA_FEP_MBR_ENR_SK").alias("BCBSA_FEP_MBR_ENR_SK")
)

# STAGE: xfrm_PKEYgen (CTransformerStage)
# The DataStage logic: 
#   svBcbsaFepMbrEnrSK = if IsNull(BCBSA_FEP_MBR_ENR_SK) then NextSurrogateKey() else BCBSA_FEP_MBR_ENR_SK
#   svRunCyle = if IsNull(BCBSA_FEP_MBR_ENR_SK) then IDSRunCycle else CRT_RUN_CYC_EXCTN_SK
#   LAST_UPDT_RUN_CYC_EXCTN_SK = IDSRunCycle 
#
# Output links:
#   1) lnk_Pkey_out (all rows)
#   2) lnk_KBcbsaFepMbrEnr (only those where the original BCBSA_FEP_MBR_ENR_SK was Null)

df_xfrm_input = df_jn_BcbsaFepMbrEnr

# Identify new rows (where BCBSA_FEP_MBR_ENR_SK is null)
df_new = df_xfrm_input.filter(col("BCBSA_FEP_MBR_ENR_SK").isNull())
df_new = df_new.withColumn("CRT_RUN_CYC_EXCTN_SK", lit(IDSRunCycle))
df_new = df_new.withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", lit(IDSRunCycle))
df_new = SurrogateKeyGen(df_new,<DB sequence name>,"BCBSA_FEP_MBR_ENR_SK",<schema>,<secret_name>)

# Identify existing rows (where BCBSA_FEP_MBR_ENR_SK is not null)
df_old = df_xfrm_input.filter(col("BCBSA_FEP_MBR_ENR_SK").isNotNull())
df_old = df_old.withColumn(
    "CRT_RUN_CYC_EXCTN_SK",
    col("CRT_RUN_CYC_EXCTN_SK")
)
df_old = df_old.withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", lit(IDSRunCycle))

# Union them to get all rows with final columns adjusted
df_final_enriched = df_new.unionByName(df_old)

# lnk_Pkey_out
df_lnk_Pkey_out = df_final_enriched.select(
    col("MBR_UNIQ_KEY"),
    col("PROD_SH_NM"),
    col("FEP_MBR_ENR_EFF_DT_SK"),
    col("SRC_SYS_CD"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("BCBSA_FEP_MBR_ENR_SK")
)

# lnk_KBcbsaFepMbrEnr (only new rows that had BCBSA_FEP_MBR_ENR_SK null in input)
# We reuse df_new, which already has the new SK assigned.
df_lnk_KBcbsaFepMbrEnr = df_new.select(
    col("MBR_UNIQ_KEY"),
    col("PROD_SH_NM"),
    col("FEP_MBR_ENR_EFF_DT_SK"),
    col("SRC_SYS_CD"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("BCBSA_FEP_MBR_ENR_SK")
)

# STAGE: db2_K_BCBSA_FEP_MBR_ENR_Load (DB2ConnectorPX) to IDS - purely inserting new rows
# We implement as a MERGE with only WHEN NOT MATCHED to replicate "append".
drop_temp_sql_load = (
    f"DROP TABLE IF EXISTS STAGING.{'IdsBcbsaFepMbrEnrExtrPkey'}_db2_K_BCBSA_FEP_MBR_ENR_Load_temp"
)
execute_dml(drop_temp_sql_load, jdbc_url_ids, jdbc_props_ids)

# Write df_lnk_KBcbsaFepMbrEnr to the temp table
(
    df_lnk_KBcbsaFepMbrEnr.write
    .format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("dbtable", f"STAGING.{'IdsBcbsaFepMbrEnrExtrPkey'}_db2_K_BCBSA_FEP_MBR_ENR_Load_temp")
    .mode("overwrite")
    .save()
)

merge_sql_load = (
    f"MERGE INTO {IDSOwner}.K_BCBSA_FEP_MBR_ENR AS T "
    f"USING STAGING.{'IdsBcbsaFepMbrEnrExtrPkey'}_db2_K_BCBSA_FEP_MBR_ENR_Load_temp AS S "
    f"ON (T.MBR_UNIQ_KEY=S.MBR_UNIQ_KEY AND T.PROD_SH_NM=S.PROD_SH_NM AND "
    f"T.FEP_MBR_ENR_EFF_DT_SK=S.FEP_MBR_ENR_EFF_DT_SK AND T.SRC_SYS_CD=S.SRC_SYS_CD) "
    f"WHEN NOT MATCHED THEN INSERT (MBR_UNIQ_KEY, PROD_SH_NM, FEP_MBR_ENR_EFF_DT_SK, SRC_SYS_CD, CRT_RUN_CYC_EXCTN_SK, BCBSA_FEP_MBR_ENR_SK) "
    f"VALUES (S.MBR_UNIQ_KEY, S.PROD_SH_NM, S.FEP_MBR_ENR_EFF_DT_SK, S.SRC_SYS_CD, S.CRT_RUN_CYC_EXCTN_SK, S.BCBSA_FEP_MBR_ENR_SK);"
)
execute_dml(merge_sql_load, jdbc_url_ids, jdbc_props_ids)

# STAGE: jn_PKey (PxJoin, inner join)
df_jn_PKey_temp = (
    df_Lnk_cp_Out.alias("Lnk_cp_Out")
    .join(
        df_lnk_Pkey_out.alias("lnk_Pkey_out"),
        on=[
            col("Lnk_cp_Out.MBR_UNIQ_KEY") == col("lnk_Pkey_out.MBR_UNIQ_KEY"),
            col("Lnk_cp_Out.PROD_SH_NM") == col("lnk_Pkey_out.PROD_SH_NM"),
            col("Lnk_cp_Out.FEP_MBR_ENR_EFF_DT_SK") == col("lnk_Pkey_out.FEP_MBR_ENR_EFF_DT_SK"),
            col("Lnk_cp_Out.SRC_SYS_CD") == col("lnk_Pkey_out.SRC_SYS_CD")
        ],
        how="inner"
    )
)

df_jn_PKey = df_jn_PKey_temp.select(
    col("Lnk_cp_Out.PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    col("Lnk_cp_Out.FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    col("lnk_Pkey_out.BCBSA_FEP_MBR_ENR_SK").alias("BCBSA_FEP_MBR_ENR_SK"),
    col("Lnk_cp_Out.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    col("Lnk_cp_Out.PROD_SH_NM").alias("PROD_SH_NM"),
    col("Lnk_cp_Out.FEP_MBR_ENR_EFF_DT_SK").alias("FEP_MBR_ENR_EFF_DT"),
    col("Lnk_cp_Out.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("Lnk_cp_Out.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    col("lnk_Pkey_out.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("lnk_Pkey_out.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("Lnk_cp_Out.BC_PLN_CD").alias("BC_PLN_CD"),
    col("Lnk_cp_Out.BS_PLN_CD").alias("BS_PLN_CD"),
    col("Lnk_cp_Out.CHMCL_DPNDC_BNF_IN").alias("CHMCL_DPNDC_BNF_IN"),
    col("Lnk_cp_Out.CHMCL_DPNDC_IP_BNF_IN").alias("CHMCL_DPNDC_IP_BNF_IN"),
    col("Lnk_cp_Out.CHMCL_DPNDC_IP_PRTL_DAY_BNF_IN").alias("CHMCL_DPNDC_IP_PRTL_DAY_BNF_IN"),
    col("Lnk_cp_Out.CHMCL_DPNDC_OP_BNF_IN").alias("CHMCL_DPNDC_OP_BNF_IN"),
    col("Lnk_cp_Out.CONF_COMM_IN").alias("CONF_COMM_IN"),
    col("Lnk_cp_Out.DNTL_BNF_IN").alias("DNTL_BNF_IN"),
    col("Lnk_cp_Out.MNTL_HLTH_BNF_IN").alias("MNTL_HLTH_BNF_IN"),
    col("Lnk_cp_Out.MNTL_HLTH_IP_BNF_IN").alias("MNTL_HLTH_IP_BNF_IN"),
    col("Lnk_cp_Out.MNTL_HLTH_IP_PRTL_DAY_BNF_IN").alias("MNTL_HLTH_IP_PRTL_DAY_BNF_IN"),
    col("Lnk_cp_Out.MNTL_HLTH_OP_BNF_IN").alias("MNTL_HLTH_OP_BNF_IN"),
    col("Lnk_cp_Out.OP_BNF_IN").alias("OP_BNF_IN"),
    col("Lnk_cp_Out.PDX_BNF_IN").alias("PDX_BNF_IN"),
    col("Lnk_cp_Out.FEP_MBR_ENR_TERM_DT_SK").alias("FEP_MBR_ENR_TERM_DT"),
    col("Lnk_cp_Out.SRC_SYS_LAST_UPDT_DT_SK").alias("SRC_SYS_LAST_UPDT_DT"),
    col("Lnk_cp_Out.FEP_PLN_PROD_ID").alias("FEP_PLN_PROD_ID"),
    col("Lnk_cp_Out.FEP_PLN_RGN_ID").alias("FEP_PLN_RGN_ID"),
    col("Lnk_cp_Out.MBR_ID").alias("MBR_ID"),
    col("Lnk_cp_Out.MBR_EMPLMT_STTUS_NM").alias("MBR_EMPLMT_STTUS_NM"),
    col("Lnk_cp_Out.PROD_DESC").alias("PROD_DESC"),
    col("Lnk_cp_Out.FEP_MBR_ID").alias("FEP_MBR_ID"),
    col("Lnk_cp_Out.FEP_COV_TYP_TX").alias("FEP_COV_TYP_TX")
)

# STAGE: seq_BCBSA_FEP_MBR_ENR (PxSequentialFile)
# Write to "BCBSA_FEP_MBR_ENR.#SrcSysCd#.pkey.#RunID#.dat"
# Apply rpad for char columns before writing.
df_seq_BCBSA_FEP_MBR_ENR = df_jn_PKey.select(
    col("PRI_NAT_KEY_STRING"),
    col("FIRST_RECYC_TS"),
    col("BCBSA_FEP_MBR_ENR_SK"),
    col("MBR_UNIQ_KEY"),
    col("PROD_SH_NM"),
    rpad(col("FEP_MBR_ENR_EFF_DT"), 10, " ").alias("FEP_MBR_ENR_EFF_DT"),
    col("SRC_SYS_CD"),
    col("SRC_SYS_CD_SK"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("BC_PLN_CD"),
    col("BS_PLN_CD"),
    rpad(col("CHMCL_DPNDC_BNF_IN"), 1, " ").alias("CHMCL_DPNDC_BNF_IN"),
    rpad(col("CHMCL_DPNDC_IP_BNF_IN"), 1, " ").alias("CHMCL_DPNDC_IP_BNF_IN"),
    rpad(col("CHMCL_DPNDC_IP_PRTL_DAY_BNF_IN"), 1, " ").alias("CHMCL_DPNDC_IP_PRTL_DAY_BNF_IN"),
    rpad(col("CHMCL_DPNDC_OP_BNF_IN"), 1, " ").alias("CHMCL_DPNDC_OP_BNF_IN"),
    rpad(col("CONF_COMM_IN"), 1, " ").alias("CONF_COMM_IN"),
    rpad(col("DNTL_BNF_IN"), 1, " ").alias("DNTL_BNF_IN"),
    rpad(col("MNTL_HLTH_BNF_IN"), 1, " ").alias("MNTL_HLTH_BNF_IN"),
    rpad(col("MNTL_HLTH_IP_BNF_IN"), 1, " ").alias("MNTL_HLTH_IP_BNF_IN"),
    rpad(col("MNTL_HLTH_IP_PRTL_DAY_BNF_IN"), 1, " ").alias("MNTL_HLTH_IP_PRTL_DAY_BNF_IN"),
    rpad(col("MNTL_HLTH_OP_BNF_IN"), 1, " ").alias("MNTL_HLTH_OP_BNF_IN"),
    rpad(col("OP_BNF_IN"), 1, " ").alias("OP_BNF_IN"),
    rpad(col("PDX_BNF_IN"), 1, " ").alias("PDX_BNF_IN"),
    rpad(col("FEP_MBR_ENR_TERM_DT"), 10, " ").alias("FEP_MBR_ENR_TERM_DT"),
    rpad(col("SRC_SYS_LAST_UPDT_DT"), 10, " ").alias("SRC_SYS_LAST_UPDT_DT"),
    col("FEP_PLN_PROD_ID"),
    col("FEP_PLN_RGN_ID"),
    col("MBR_ID"),
    col("MBR_EMPLMT_STTUS_NM"),
    col("PROD_DESC"),
    col("FEP_MBR_ID"),
    col("FEP_COV_TYP_TX")
)

write_files(
    df_seq_BCBSA_FEP_MBR_ENR,
    f"{adls_path}/key/BCBSA_FEP_MBR_ENR.{SrcSysCd}.pkey.{RunID}.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)