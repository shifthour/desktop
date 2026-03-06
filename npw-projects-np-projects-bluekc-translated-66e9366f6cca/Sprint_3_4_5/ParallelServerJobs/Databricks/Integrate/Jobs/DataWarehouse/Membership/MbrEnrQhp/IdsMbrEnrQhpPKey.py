# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2005, 2006, 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC Job Name : IdsMbrEnrQhpPKey
# MAGIC 
# MAGIC Called By: IdsIdsMbrEnrQhpExtCntl
# MAGIC 
# MAGIC Process Description: Build primary key for  MBR_ENR_QHP.  K table will be read to see if there is an SK already available for the Natural Keys. 
# MAGIC 
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC     Build primary key for  PROD_QHP table load data 
# MAGIC 
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                         Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                    ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Jaideep Mankala              2016-07-22         5605                            Original Programming                                          IntegrateDev2               Kalyan Neelam              2016-08-10
# MAGIC Jaideep Mankala              2016-08-24         5605                            Replaced qhp_eff date with prod_qhp_eff	   IntegrateDev2               Kalyan Neelam              2016-09-21
# MAGIC 						        as part of Pkey assignment

# MAGIC New PKEYs are generated in this transformer. A database sequence will be used to create next PKEY value.
# MAGIC Insert Only the Newly generated Pkeys into the K Table
# MAGIC Read K_MBR_ENR_QHP Table to pull the Natural Keys and the Skey.
# MAGIC Inner Join primary key info with table info
# MAGIC Left Join on Natural Keys
# MAGIC Remove Duplicates stage takes care of getting rid of duplicate rows with the same Natural key get into the PKEYing step.
# MAGIC JobName: IdsMbrEnrQhpPkey
# MAGIC 
# MAGIC Strip field function and Tranforamtion rules are applied on the data Extracted from IDS
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# Parameters
SrcSysCd = get_widget_value('SrcSysCd','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
IDSRunCycle = get_widget_value('IDSRunCycle','')
RunID = get_widget_value('RunID','')

# Read ds_MBR_ENR_QHP_Xfm (PxDataSet)
schema_ds_MBR_ENR_QHP_Xfm = StructType([
    StructField("PRI_NAT_KEY_STRING", StringType(), True),
    StructField("FIRST_RECYC_TS", StringType(), True),
    StructField("SRC_SYS_CD_SK", StringType(), True),
    StructField("MBR_UNIQ_KEY", StringType(), True),
    StructField("MBR_ENR_CLS_PLN_PROD_CAT_CD_SK", StringType(), True),
    StructField("MBR_EFF_DT_SK", StringType(), True),
    StructField("QHP_EFF_DT_SK", StringType(), True),
    StructField("MBR_TERM_DT_SK", StringType(), True),
    StructField("QHP_SK", StringType(), True),
    StructField("QHP_TERM_DT_SK", StringType(), True),
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("MBR_ENR_CLS_PLN_PROD_CAT_CD", StringType(), True),
    StructField("MBR_ENR_SK", StringType(), True),
    StructField("QHP_ID", StringType(), True),
    StructField("PROD_QHP_EFF_DT_SK", StringType(), True),
    StructField("PROD_QHP_TERM_DT_SK", StringType(), True),
])
df_ds_MBR_ENR_QHP_Xfm = (
    spark.read.schema(schema_ds_MBR_ENR_QHP_Xfm)
    .parquet(f"{adls_path}/ds/MBR_ENR_QHP.{SrcSysCd}.xfrm.{RunID}.parquet")
)

# Cp_Pk (PxCopy) - single input -> two outputs
df_cp_pk = df_ds_MBR_ENR_QHP_Xfm

df_cp_pk_out1 = df_cp_pk.select(
    df_cp_pk["PRI_NAT_KEY_STRING"].alias("PRI_NAT_KEY_STRING"),
    df_cp_pk["FIRST_RECYC_TS"].alias("FIRST_RECYC_TS"),
    df_cp_pk["SRC_SYS_CD_SK"].alias("SRC_SYS_CD_SK"),
    df_cp_pk["MBR_UNIQ_KEY"].alias("MBR_UNIQ_KEY"),
    df_cp_pk["MBR_ENR_CLS_PLN_PROD_CAT_CD_SK"].alias("MBR_ENR_CLS_PLN_PROD_CAT_CD_SK"),
    df_cp_pk["MBR_EFF_DT_SK"].alias("MBR_ENR_EFF_DT_SK"),
    df_cp_pk["PROD_QHP_EFF_DT_SK"].alias("MBR_ENR_QHP_EFF_DT_SK"),
    df_cp_pk["MBR_TERM_DT_SK"].alias("MBR_ENR_TERM_DT_SK"),
    df_cp_pk["QHP_SK"].alias("QHP_SK"),
    df_cp_pk["PROD_QHP_TERM_DT_SK"].alias("MBR_ENR_QHP_TERM_DT_SK"),
    df_cp_pk["SRC_SYS_CD"].alias("SRC_SYS_CD"),
    df_cp_pk["MBR_ENR_CLS_PLN_PROD_CAT_CD"].alias("MBR_ENR_CLS_PLN_PROD_CAT_CD"),
    df_cp_pk["MBR_ENR_SK"].alias("MBR_ENR_SK"),
    df_cp_pk["QHP_ID"].alias("QHP_ID"),
    df_cp_pk["QHP_EFF_DT_SK"].alias("QHP_EFF_DT_SK"),
    df_cp_pk["QHP_TERM_DT_SK"].alias("QHP_TERM_DT_SK")
)

df_cp_pk_out2 = df_cp_pk.select(
    df_cp_pk["SRC_SYS_CD"].alias("SRC_SYS_CD"),
    df_cp_pk["MBR_UNIQ_KEY"].alias("MBR_UNIQ_KEY"),
    df_cp_pk["MBR_ENR_CLS_PLN_PROD_CAT_CD"].alias("MBR_ENR_CLS_PLN_PROD_CAT_CD"),
    df_cp_pk["MBR_EFF_DT_SK"].alias("MBR_ENR_EFF_DT_SK"),
    df_cp_pk["PROD_QHP_EFF_DT_SK"].alias("MBR_ENR_QHP_EFF_DT_SK")
)

# rdup_Natural_Keys (PxRemDup)
df_rdup_nk = dedup_sort(
    df_cp_pk_out2,
    [
        "SRC_SYS_CD",
        "MBR_UNIQ_KEY",
        "MBR_ENR_CLS_PLN_PROD_CAT_CD",
        "MBR_ENR_EFF_DT_SK",
        "MBR_ENR_QHP_EFF_DT_SK",
    ],
    []
)
df_rdup_nk_out = df_rdup_nk.select(
    df_rdup_nk["SRC_SYS_CD"].alias("SRC_SYS_CD"),
    df_rdup_nk["MBR_UNIQ_KEY"].alias("MBR_UNIQ_KEY"),
    df_rdup_nk["MBR_ENR_CLS_PLN_PROD_CAT_CD"].alias("MBR_ENR_CLS_PLN_PROD_CAT_CD"),
    df_rdup_nk["MBR_ENR_EFF_DT_SK"].alias("MBR_ENR_EFF_DT_SK"),
    df_rdup_nk["MBR_ENR_QHP_EFF_DT_SK"].alias("MBR_ENR_QHP_EFF_DT_SK")
)

# db2_K_MBR_ENR_QHP_in (DB2ConnectorPX)
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
extract_query_db2_K_MBR_ENR_QHP_in = (
    f"SELECT SRC_SYS_CD, MBR_UNIQ_KEY, MBR_ENR_CLS_PLN_PROD_CAT_CD, "
    f"MBR_ENR_EFF_DT_SK, MBR_ENR_QHP_EFF_DT_SK, CRT_RUN_CYC_EXCTN_SK, MBR_ENR_QHP_SK "
    f"FROM {IDSOwner}.K_MBR_ENR_QHP"
)
df_db2_K_MBR_ENR_QHP_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_K_MBR_ENR_QHP_in)
    .load()
)

# jn_QhpEnr (PxJoin) - leftouterjoin
df_jn_QhpEnr = (
    df_rdup_nk_out.alias("lnk_Natural_Keys_out")
    .join(
        df_db2_K_MBR_ENR_QHP_in.alias("lnk_KMbrEnrQhpPkey_out"),
        on=[
            F.col("lnk_Natural_Keys_out.SRC_SYS_CD")
            == F.col("lnk_KMbrEnrQhpPkey_out.SRC_SYS_CD"),
            F.col("lnk_Natural_Keys_out.MBR_UNIQ_KEY")
            == F.col("lnk_KMbrEnrQhpPkey_out.MBR_UNIQ_KEY"),
            F.col("lnk_Natural_Keys_out.MBR_ENR_CLS_PLN_PROD_CAT_CD")
            == F.col("lnk_KMbrEnrQhpPkey_out.MBR_ENR_CLS_PLN_PROD_CAT_CD"),
            F.col("lnk_Natural_Keys_out.MBR_ENR_EFF_DT_SK")
            == F.col("lnk_KMbrEnrQhpPkey_out.MBR_ENR_EFF_DT_SK"),
            F.col("lnk_Natural_Keys_out.MBR_ENR_QHP_EFF_DT_SK")
            == F.col("lnk_KMbrEnrQhpPkey_out.MBR_ENR_QHP_EFF_DT_SK"),
        ],
        how="left",
    )
)

df_jn_QhpEnr_out = df_jn_QhpEnr.select(
    F.col("lnk_Natural_Keys_out.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnk_Natural_Keys_out.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("lnk_Natural_Keys_out.MBR_ENR_CLS_PLN_PROD_CAT_CD").alias("MBR_ENR_CLS_PLN_PROD_CAT_CD"),
    F.col("lnk_Natural_Keys_out.MBR_ENR_EFF_DT_SK").alias("MBR_ENR_EFF_DT_SK"),
    F.col("lnk_Natural_Keys_out.MBR_ENR_QHP_EFF_DT_SK").alias("MBR_ENR_QHP_EFF_DT_SK"),
    F.col("lnk_KMbrEnrQhpPkey_out.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("lnk_KMbrEnrQhpPkey_out.MBR_ENR_QHP_SK").alias("MBR_ENR_QHP_SK"),
)

# xfrm_PKEYgen (CTransformerStage)
df_xfrm_pkeygen_in = df_jn_QhpEnr_out.withColumn("MBR_ENR_QHP_SK_old", F.col("MBR_ENR_QHP_SK"))

df_xfrm_pkeygen_intermediate = (
    df_xfrm_pkeygen_in
    .withColumn(
        "CRT_RUN_CYC_EXCTN_SK",
        F.when(F.col("MBR_ENR_QHP_SK_old").isNull(), F.lit(IDSRunCycle))
         .otherwise(F.col("CRT_RUN_CYC_EXCTN_SK"))
    )
    .withColumn("LAST_RUN_CYC_EXCTN_SK", F.lit(IDSRunCycle))
)

df_enriched = df_xfrm_pkeygen_intermediate
df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"MBR_ENR_QHP_SK",<schema>,<secret_name>)

df_xfrm_pkeygen = df_enriched

# lnk_Pkey_out (to jn_PKey) - all rows
df_xfrm_pkeygen_out1 = df_xfrm_pkeygen.select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("MBR_ENR_CLS_PLN_PROD_CAT_CD").alias("MBR_ENR_CLS_PLN_PROD_CAT_CD"),
    F.col("MBR_ENR_EFF_DT_SK").alias("MBR_ENR_EFF_DT_SK"),
    F.col("MBR_ENR_QHP_EFF_DT_SK").alias("MBR_ENR_QHP_EFF_DT_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_RUN_CYC_EXCTN_SK").alias("LAST_RUN_CYC_EXCTN_SK"),
    F.col("MBR_ENR_QHP_SK").alias("MBR_ENR_QHP_SK"),
)

# lnk_KMbrEnrQhp_New (to DB2_K_MBR_ENR_QHP_Load) - only rows for which original MBR_ENR_QHP_SK was null
df_xfrm_pkeygen_out2 = df_xfrm_pkeygen.filter(
    F.col("MBR_ENR_QHP_SK_old").isNull()
).select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("MBR_ENR_CLS_PLN_PROD_CAT_CD").alias("MBR_ENR_CLS_PLN_PROD_CAT_CD"),
    F.col("MBR_ENR_EFF_DT_SK").alias("MBR_ENR_EFF_DT_SK"),
    F.col("MBR_ENR_QHP_EFF_DT_SK").alias("MBR_ENR_QHP_EFF_DT_SK"),
    F.when(
        F.col("MBR_ENR_QHP_SK_old").isNull(), F.lit(IDSRunCycle)
    ).otherwise(
        F.col("CRT_RUN_CYC_EXCTN_SK")
    ).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("MBR_ENR_QHP_SK").alias("MBR_ENR_QHP_SK"),
)

# DB2_K_MBR_ENR_QHP_Load (DB2ConnectorPX) - insert => translate to MERGE that only inserts
if df_xfrm_pkeygen_out2.head(1):
    jdbc_url_ids_load, jdbc_props_ids_load = get_db_config(ids_secret_name)
    temp_table_name = "STAGING.IdsMbrEnrQhpPKey_DB2_K_MBR_ENR_QHP_Load_temp"
    spark.sql(f"DROP TABLE IF EXISTS {temp_table_name}")
    (
        df_xfrm_pkeygen_out2.write
        .format("jdbc")
        .option("url", jdbc_url_ids_load)
        .options(**jdbc_props_ids_load)
        .option("dbtable", temp_table_name)
        .save()
    )
    merge_sql = f"""
    MERGE INTO {IDSOwner}.K_MBR_ENR_QHP AS T
    USING {temp_table_name} AS S
    ON T.MBR_ENR_QHP_SK = S.MBR_ENR_QHP_SK
    WHEN NOT MATCHED THEN
      INSERT
      (
        SRC_SYS_CD,
        MBR_UNIQ_KEY,
        MBR_ENR_CLS_PLN_PROD_CAT_CD,
        MBR_ENR_EFF_DT_SK,
        MBR_ENR_QHP_EFF_DT_SK,
        CRT_RUN_CYC_EXCTN_SK,
        MBR_ENR_QHP_SK
      )
      VALUES
      (
        S.SRC_SYS_CD,
        S.MBR_UNIQ_KEY,
        S.MBR_ENR_CLS_PLN_PROD_CAT_CD,
        S.MBR_ENR_EFF_DT_SK,
        S.MBR_ENR_QHP_EFF_DT_SK,
        S.CRT_RUN_CYC_EXCTN_SK,
        S.MBR_ENR_QHP_SK
      );
    """
    execute_dml(merge_sql, jdbc_url_ids_load, jdbc_props_ids_load)

# jn_PKey (PxJoin) - inner join
df_jn_PKey = (
    df_xfrm_pkeygen_out1.alias("lnk_Pkey_out")
    .join(
        df_cp_pk_out1.alias("CP_Out"),
        on=[
            F.col("lnk_Pkey_out.SRC_SYS_CD") == F.col("CP_Out.SRC_SYS_CD"),
            F.col("lnk_Pkey_out.MBR_UNIQ_KEY") == F.col("CP_Out.MBR_UNIQ_KEY"),
            F.col("lnk_Pkey_out.MBR_ENR_CLS_PLN_PROD_CAT_CD")
            == F.col("CP_Out.MBR_ENR_CLS_PLN_PROD_CAT_CD"),
            F.col("lnk_Pkey_out.MBR_ENR_EFF_DT_SK")
            == F.col("CP_Out.MBR_ENR_EFF_DT_SK"),
            F.col("lnk_Pkey_out.MBR_ENR_QHP_EFF_DT_SK")
            == F.col("CP_Out.MBR_ENR_QHP_EFF_DT_SK"),
        ],
        how="inner",
    )
)
df_jn_PKey_out = df_jn_PKey.select(
    F.col("CP_Out.PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("CP_Out.FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("lnk_Pkey_out.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnk_Pkey_out.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("lnk_Pkey_out.MBR_ENR_CLS_PLN_PROD_CAT_CD").alias("MBR_ENR_CLS_PLN_PROD_CAT_CD"),
    F.col("lnk_Pkey_out.MBR_ENR_EFF_DT_SK").alias("MBR_ENR_EFF_DT_SK"),
    F.col("lnk_Pkey_out.MBR_ENR_QHP_EFF_DT_SK").alias("MBR_ENR_QHP_EFF_DT_SK"),
    F.col("lnk_Pkey_out.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("lnk_Pkey_out.LAST_RUN_CYC_EXCTN_SK").alias("LAST_RUN_CYC_EXCTN_SK"),
    F.col("lnk_Pkey_out.MBR_ENR_QHP_SK").alias("MBR_ENR_QHP_SK"),
    F.col("CP_Out.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("CP_Out.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK").alias("MBR_ENR_CLS_PLN_PROD_CAT_CD_SK"),
    F.col("CP_Out.MBR_ENR_TERM_DT_SK").alias("MBR_ENR_TERM_DT_SK"),
    F.col("CP_Out.QHP_SK").alias("QHP_SK"),
    F.col("CP_Out.MBR_ENR_QHP_TERM_DT_SK").alias("MBR_ENR_QHP_TERM_DT_SK"),
    F.col("CP_Out.MBR_ENR_SK").alias("MBR_ENR_SK"),
    F.col("CP_Out.QHP_ID").alias("QHP_ID"),
    F.col("CP_Out.QHP_EFF_DT_SK").alias("QHP_EFF_DT_SK"),
    F.col("CP_Out.QHP_TERM_DT_SK").alias("QHP_TERM_DT_SK"),
)

# seq_MBR_ENR_QHP_PKEY (PxSequentialFile) - final write
final_cols = [
    "PRI_NAT_KEY_STRING",
    "FIRST_RECYC_TS",
    "SRC_SYS_CD",
    "MBR_UNIQ_KEY",
    "MBR_ENR_CLS_PLN_PROD_CAT_CD",
    "MBR_ENR_EFF_DT_SK",
    "MBR_ENR_QHP_EFF_DT_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_RUN_CYC_EXCTN_SK",
    "MBR_ENR_QHP_SK",
    "SRC_SYS_CD_SK",
    "MBR_ENR_CLS_PLN_PROD_CAT_CD_SK",
    "MBR_ENR_TERM_DT_SK",
    "QHP_SK",
    "MBR_ENR_QHP_TERM_DT_SK",
    "MBR_ENR_SK",
    "QHP_ID",
    "QHP_EFF_DT_SK",
    "QHP_TERM_DT_SK",
]

df_final = df_jn_PKey_out
df_final = df_final.select(
    F.col("PRI_NAT_KEY_STRING"),
    F.col("FIRST_RECYC_TS"),
    F.col("SRC_SYS_CD"),
    F.col("MBR_UNIQ_KEY"),
    F.col("MBR_ENR_CLS_PLN_PROD_CAT_CD"),
    F.rpad(F.col("MBR_ENR_EFF_DT_SK"), 10, " ").alias("MBR_ENR_EFF_DT_SK"),
    F.rpad(F.col("MBR_ENR_QHP_EFF_DT_SK"), 10, " ").alias("MBR_ENR_QHP_EFF_DT_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_RUN_CYC_EXCTN_SK"),
    F.col("MBR_ENR_QHP_SK"),
    F.col("SRC_SYS_CD_SK"),
    F.col("MBR_ENR_CLS_PLN_PROD_CAT_CD_SK"),
    F.rpad(F.col("MBR_ENR_TERM_DT_SK"), 10, " ").alias("MBR_ENR_TERM_DT_SK"),
    F.col("QHP_SK"),
    F.rpad(F.col("MBR_ENR_QHP_TERM_DT_SK"), 10, " ").alias("MBR_ENR_QHP_TERM_DT_SK"),
    F.col("MBR_ENR_SK"),
    F.col("QHP_ID"),
    F.rpad(F.col("QHP_EFF_DT_SK"), 10, " ").alias("QHP_EFF_DT_SK"),
    F.rpad(F.col("QHP_TERM_DT_SK"), 10, " ").alias("QHP_TERM_DT_SK"),
)

write_files(
    df_final,
    f"{adls_path}/key/MBR_ENR_QHP.{SrcSysCd}.pkey.{RunID}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)