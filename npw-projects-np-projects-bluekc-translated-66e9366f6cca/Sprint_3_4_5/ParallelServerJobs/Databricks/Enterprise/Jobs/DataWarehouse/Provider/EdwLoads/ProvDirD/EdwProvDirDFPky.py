# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC Aditya Raju                05/30/2013        5114                              Originally Programmed  (In Parallel)                                                       EnterpriseWhseDevl

# MAGIC Left Outer Join Here
# MAGIC New PKEYs are generated in this transformer. A database sequence will be used to create next PKEY value.
# MAGIC if there is no FKEY needed, just land it to Seq File for the load job follows.
# MAGIC Table K_PROV_DIR_D_F.               Insert new SKEY rows generated as part of this run. This is a database insert operation.
# MAGIC Remove Duplicates stage takes care of getting rid of duplicate rows with the same Natural key get into the PKEYing step.
# MAGIC A concious effort need to be made to evaluate which one of the duplicate set row to drop is a case by case basis decision.
# MAGIC Load type is replace in order to avoid the duplicates from source a remove duplicate stage is used ahead of the copy stage.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
EDWRuncycle = get_widget_value('EDWRuncycle','')
EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')

jdbc_url, jdbc_props = get_db_config(edw_secret_name)

extract_query = (
    "SELECT "
    "PROV_ID,"
    "PROV_ADDR_TYP_CD,"
    "PROV_ADDR_EFF_DT_SK,"
    "PROV_ADDR_TERM_DT_SK,"
    "NTWK_ID,"
    "PROV_NTWK_TERM_DT_SK,"
    "PROV_NTWK_PFX_ID,"
    "SRC_SYS_CD,"
    "CRT_RUN_CYC_EXCTN_DT_SK,"
    "PROV_DIR_SK,"
    "CRT_RUN_CYC_EXCTN_SK "
    f"FROM {EDWOwner}.K_PROV_DIR_D"
)

df_db2_KProvDirDFExt = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_Data_Set_45 = spark.read.parquet(f"{adls_path}/ds/PROV_DIR_D.parquet")

df_rdp_NaturalKeys = dedup_sort(
    df_Data_Set_45,
    ["PROV_ID", "PROV_ADDR_TYP_CD", "PROV_ADDR_EFF_DT_SK", "PROV_ADDR_TERM_DT_SK", "NTWK_ID", "PROV_NTWK_TERM_DT_SK", "PROV_NTWK_PFX_ID", "SRC_SYS_CD"],
    [
        ("PROV_ID","A"),
        ("PROV_ADDR_TYP_CD","A"),
        ("PROV_ADDR_EFF_DT_SK","A"),
        ("PROV_ADDR_TERM_DT_SK","A"),
        ("NTWK_ID","A"),
        ("PROV_NTWK_TERM_DT_SK","A"),
        ("PROV_NTWK_PFX_ID","A"),
        ("SRC_SYS_CD","A")
    ]
)

# cpy_Data_out produces two outputs from df_rdp_NaturalKeys

df_cpy_Data_out_1 = df_rdp_NaturalKeys.select(
    F.col("PROV_ID").alias("PROV_ID"),
    F.col("PROV_ADDR_TYP_CD").alias("PROV_ADDR_TYP_CD"),
    F.col("PROV_ADDR_EFF_DT_SK").alias("PROV_ADDR_EFF_DT_SK"),
    F.col("PROV_ADDR_TERM_DT_SK").alias("PROV_ADDR_TERM_DT_SK"),
    F.col("NTWK_ID").alias("NTWK_ID"),
    F.col("PROV_NTWK_TERM_DT_SK").alias("PROV_NTWK_TERM_DT_SK"),
    F.col("PROV_NTWK_PFX_ID").alias("PROV_NTWK_PFX_ID"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("PROV_SPEC_NM").alias("PROV_SPEC_NM"),
    F.col("PROV_REL_GRP_PROV_ID").alias("PROV_REL_GRP_PROV_ID"),
    F.col("PROV_REL_GRP_PROV_NM").alias("PROV_REL_GRP_PROV_NM"),
    F.col("PROV_NM").alias("PROV_NM"),
    F.col("CMN_PRCT_TTL").alias("CMN_PRCT_TTL"),
    F.col("CMN_PRCT_SSN").alias("CMN_PRCT_SSN"),
    F.col("CMN_PRCT_GNDR_CD").alias("CMN_PRCT_GNDR_CD"),
    F.col("CMN_PRCT_BRTH_DT_SK").alias("CMN_PRCT_BRTH_DT_SK"),
    F.col("PROV_ADDR_LN_1").alias("PROV_ADDR_LN_1"),
    F.col("PROV_ADDR_LN_2").alias("PROV_ADDR_LN_2"),
    F.col("PROV_ADDR_LN_3").alias("PROV_ADDR_LN_3"),
    F.col("PROV_ADDR_CITY_NM").alias("PROV_ADDR_CITY_NM"),
    F.col("PROV_ADDR_ST_CD").alias("PROV_ADDR_ST_CD"),
    F.col("PROV_ADDR_CNTY_NM").alias("PROV_ADDR_CNTY_NM"),
    F.col("PROV_ADDR_ZIP_CD_5").alias("PROV_ADDR_ZIP_CD_5"),
    F.col("PROV_ADDR_ZIP_CD_4").alias("PROV_ADDR_ZIP_CD_4"),
    F.col("PROV_ADDR_PHN_NO").alias("PROV_ADDR_PHN_NO"),
    F.col("PROV_ADDR_FAX_NO").alias("PROV_ADDR_FAX_NO"),
    F.col("PROV_ADDR_HCAP_IN").alias("PROV_ADDR_HCAP_IN"),
    F.col("PROV_ENTY_CD").alias("PROV_ENTY_CD"),
    F.col("PAR_PROV_IN").alias("PAR_PROV_IN"),
    F.col("PROV_NTWK_PCP_IN").alias("PROV_NTWK_PCP_IN"),
    F.col("PROV_TAX_ID").alias("PROV_TAX_ID"),
    F.col("ENTY_LIC_ST_CD_1").alias("ENTY_LIC_ST_CD_1"),
    F.col("ENTY_LIC_NO_1").alias("ENTY_LIC_NO_1"),
    F.col("ENTY_LIC_ST_CD_2").alias("ENTY_LIC_ST_CD_2"),
    F.col("ENTY_LIC_NO_2").alias("ENTY_LIC_NO_2"),
    F.col("PROV_NTWK_ACPTNG_PATN_IN").alias("PROV_NTWK_ACPTNG_PATN_IN"),
    F.col("PROV_NTWK_ACPTNG_MCAID_PATN_IN").alias("PROV_NTWK_ACPTNG_MCAID_PATN_IN"),
    F.col("PROV_NTWK_ACPTNG_MCARE_PATN_IN").alias("PROV_NTWK_ACPTNG_MCARE_PATN_IN"),
    F.col("PROV_NTWK_MAX_PATN_QTY").alias("PROV_NTWK_MAX_PATN_QTY"),
    F.col("PROV_NTWK_MAX_PATN_AGE").alias("PROV_NTWK_MAX_PATN_AGE"),
    F.col("PROV_TYP_CD").alias("PROV_TYP_CD"),
    F.col("PROV_TYP_NM").alias("PROV_TYP_NM"),
    F.col("PROV_FCLTY_TYP_CD").alias("PROV_FCLTY_TYP_CD"),
    F.col("PROV_FCLTY_TYP_NM").alias("PROV_FCLTY_TYP_NM"),
    F.col("PROV_SPEC_CD").alias("PROV_SPEC_CD"),
    F.col("PROV_NTWK_MIN_PATN_AGE").alias("PROV_NTWK_MIN_PATN_AGE"),
    F.col("PROV_NTWK_GNDR_ACPTD_CD").alias("PROV_NTWK_GNDR_ACPTD_CD"),
    F.col("EXTR_DT").alias("EXTR_DT"),
    F.col("CMN_PRCT_ID").alias("CMN_PRCT_ID"),
    F.col("LEAPFROG_IN").alias("LEAPFROG_IN"),
    F.col("PROV_SK").alias("PROV_SK"),
    F.col("PROV_ADDR_GEO_ACES_RTRN_CD_TX").alias("PROV_ADDR_GEO_ACES_RTRN_CD_TX"),
    F.col("PROV_ADDR_LAT_TX").alias("PROV_ADDR_LAT_TX"),
    F.col("PROV_ADDR_LONG_TX").alias("PROV_ADDR_LONG_TX"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("PCMH_IN").alias("PCMH_IN")
)

df_cpy_Data_out_2 = df_rdp_NaturalKeys.select(
    F.col("PROV_ID").alias("PROV_ID"),
    F.col("PROV_ADDR_TYP_CD").alias("PROV_ADDR_TYP_CD"),
    F.col("PROV_ADDR_EFF_DT_SK").alias("PROV_ADDR_EFF_DT_SK"),
    F.col("PROV_ADDR_TERM_DT_SK").alias("PROV_ADDR_TERM_DT_SK"),
    F.col("NTWK_ID").alias("NTWK_ID"),
    F.col("PROV_NTWK_TERM_DT_SK").alias("PROV_NTWK_TERM_DT_SK"),
    F.col("PROV_NTWK_PFX_ID").alias("PROV_NTWK_PFX_ID"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD")
)

# jn_ProvDirDF: left join df_cpy_Data_out_2 with df_db2_KProvDirDFExt on 8 columns
join_cols_jn_ProvDirDF = [
    "PROV_ID","PROV_ADDR_TYP_CD","PROV_ADDR_EFF_DT_SK","PROV_ADDR_TERM_DT_SK",
    "NTWK_ID","PROV_NTWK_TERM_DT_SK","PROV_NTWK_PFX_ID","SRC_SYS_CD"
]
df_jn_ProvDirDF = df_cpy_Data_out_2.alias("lnk_Natural_Keys_out").join(
    df_db2_KProvDirDFExt.alias("lnkKProvDirDFIExt"),
    on=[
        F.col("lnk_Natural_Keys_out.PROV_ID")==F.col("lnkKProvDirDFIExt.PROV_ID"),
        F.col("lnk_Natural_Keys_out.PROV_ADDR_TYP_CD")==F.col("lnkKProvDirDFIExt.PROV_ADDR_TYP_CD"),
        F.col("lnk_Natural_Keys_out.PROV_ADDR_EFF_DT_SK")==F.col("lnkKProvDirDFIExt.PROV_ADDR_EFF_DT_SK"),
        F.col("lnk_Natural_Keys_out.PROV_ADDR_TERM_DT_SK")==F.col("lnkKProvDirDFIExt.PROV_ADDR_TERM_DT_SK"),
        F.col("lnk_Natural_Keys_out.NTWK_ID")==F.col("lnkKProvDirDFIExt.NTWK_ID"),
        F.col("lnk_Natural_Keys_out.PROV_NTWK_TERM_DT_SK")==F.col("lnkKProvDirDFIExt.PROV_NTWK_TERM_DT_SK"),
        F.col("lnk_Natural_Keys_out.PROV_NTWK_PFX_ID")==F.col("lnkKProvDirDFIExt.PROV_NTWK_PFX_ID"),
        F.col("lnk_Natural_Keys_out.SRC_SYS_CD")==F.col("lnkKProvDirDFIExt.SRC_SYS_CD"),
    ],
    how="left"
).select(
    F.col("lnk_Natural_Keys_out.PROV_ID").alias("PROV_ID"),
    F.col("lnk_Natural_Keys_out.PROV_ADDR_TYP_CD").alias("PROV_ADDR_TYP_CD"),
    F.col("lnk_Natural_Keys_out.PROV_ADDR_EFF_DT_SK").alias("PROV_ADDR_EFF_DT_SK"),
    F.col("lnk_Natural_Keys_out.PROV_ADDR_TERM_DT_SK").alias("PROV_ADDR_TERM_DT_SK"),
    F.col("lnk_Natural_Keys_out.NTWK_ID").alias("NTWK_ID"),
    F.col("lnk_Natural_Keys_out.PROV_NTWK_TERM_DT_SK").alias("PROV_NTWK_TERM_DT_SK"),
    F.col("lnk_Natural_Keys_out.PROV_NTWK_PFX_ID").alias("PROV_NTWK_PFX_ID"),
    F.col("lnk_Natural_Keys_out.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnkKProvDirDFIExt.PROV_DIR_SK").alias("PROV_DIR_SK"),
    F.col("lnkKProvDirDFIExt.CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("lnkKProvDirDFIExt.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK")
)

# xfm_PKEYgen logic:
df_enriched = df_jn_ProvDirDF.withColumnRenamed("PROV_DIR_SK","PROV_DIR_SK_original")

df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"PROV_DIR_SK",<schema>,<secret_name>)

# We produce two outputs based on constraint IsNull(PROV_DIR_SK_original)
df_lnk_IdsEdwKProvDirDF_Out = df_enriched.filter(
    F.col("PROV_DIR_SK_original").isNull()
).select(
    F.col("PROV_ID").alias("PROV_ID"),
    F.col("PROV_ADDR_TYP_CD").alias("PROV_ADDR_TYP_CD"),
    F.col("PROV_ADDR_EFF_DT_SK").alias("PROV_ADDR_EFF_DT_SK"),
    F.col("PROV_ADDR_TERM_DT_SK").alias("PROV_ADDR_TERM_DT_SK"),
    F.col("NTWK_ID").alias("NTWK_ID"),
    F.col("PROV_NTWK_TERM_DT_SK").alias("PROV_NTWK_TERM_DT_SK"),
    F.col("PROV_NTWK_PFX_ID").alias("PROV_NTWK_PFX_ID"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("PROV_DIR_SK").alias("PROV_DIR_SK"),
    F.lit(EDWRuncycle).alias("CRT_RUN_CYC_EXCTN_SK")
)

df_lnkPKEYxfmOut = df_enriched.select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PROV_DIR_SK").alias("PROV_DIR_SK"),
    F.col("PROV_ID").alias("PROV_ID"),
    F.col("PROV_ADDR_TYP_CD").alias("PROV_ADDR_TYP_CD"),
    F.col("PROV_ADDR_EFF_DT_SK").alias("PROV_ADDR_EFF_DT_SK"),
    F.col("PROV_ADDR_TERM_DT_SK").alias("PROV_ADDR_TERM_DT_SK"),
    F.col("NTWK_ID").alias("NTWK_ID"),
    F.col("PROV_NTWK_TERM_DT_SK").alias("PROV_NTWK_TERM_DT_SK"),
    F.col("PROV_NTWK_PFX_ID").alias("PROV_NTWK_PFX_ID"),
    F.when(F.col("PROV_DIR_SK_original").isNull(), EDWRunCycleDate)
     .otherwise(F.col("CRT_RUN_CYC_EXCTN_DT_SK")).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.when(F.col("PROV_DIR_SK_original").isNull(), EDWRuncycle)
     .otherwise(F.col("CRT_RUN_CYC_EXCTN_SK")).alias("CRT_RUN_CYC_EXCTN_SK")
)

# db2_ProvDirDFLoad -> Insert logic with merge semantics
# Create temp table, then MERGE
df_lnk_IdsEdwKProvDirDF_Out.createOrReplaceTempView("temp_df_lnk_IdsEdwKProvDirDF_Out_view")

execute_dml(
    "DROP TABLE IF EXISTS STAGING.EdwProvDirDFPky_db2_ProvDirDFLoad_temp",
    jdbc_url,
    jdbc_props
)

(
    spark.table("temp_df_lnk_IdsEdwKProvDirDF_Out_view")
    .write
    .format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("dbtable", "STAGING.EdwProvDirDFPky_db2_ProvDirDFLoad_temp")
    .mode("overwrite")
    .save()
)

merge_sql_db2_ProvDirDFLoad = (
    f"MERGE INTO {EDWOwner}.K_PROV_DIR_D AS T "
    f"USING STAGING.EdwProvDirDFPky_db2_ProvDirDFLoad_temp AS S "
    "ON "
    "T.PROV_ID = S.PROV_ID AND "
    "T.PROV_ADDR_TYP_CD = S.PROV_ADDR_TYP_CD AND "
    "T.PROV_ADDR_EFF_DT_SK = S.PROV_ADDR_EFF_DT_SK AND "
    "T.PROV_ADDR_TERM_DT_SK = S.PROV_ADDR_TERM_DT_SK AND "
    "T.NTWK_ID = S.NTWK_ID AND "
    "T.PROV_NTWK_TERM_DT_SK = S.PROV_NTWK_TERM_DT_SK AND "
    "T.PROV_NTWK_PFX_ID = S.PROV_NTWK_PFX_ID AND "
    "T.SRC_SYS_CD = S.SRC_SYS_CD "
    "WHEN MATCHED THEN UPDATE SET "
    "T.CRT_RUN_CYC_EXCTN_DT_SK = S.CRT_RUN_CYC_EXCTN_DT_SK, "
    "T.PROV_DIR_SK = S.PROV_DIR_SK, "
    "T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK "
    "WHEN NOT MATCHED THEN INSERT ( "
    "PROV_ID, PROV_ADDR_TYP_CD, PROV_ADDR_EFF_DT_SK, PROV_ADDR_TERM_DT_SK, "
    "NTWK_ID, PROV_NTWK_TERM_DT_SK, PROV_NTWK_PFX_ID, SRC_SYS_CD, "
    "CRT_RUN_CYC_EXCTN_DT_SK, PROV_DIR_SK, CRT_RUN_CYC_EXCTN_SK "
    ") VALUES ( "
    "S.PROV_ID, S.PROV_ADDR_TYP_CD, S.PROV_ADDR_EFF_DT_SK, S.PROV_ADDR_TERM_DT_SK, "
    "S.NTWK_ID, S.PROV_NTWK_TERM_DT_SK, S.PROV_NTWK_PFX_ID, S.SRC_SYS_CD, "
    "S.CRT_RUN_CYC_EXCTN_DT_SK, S.PROV_DIR_SK, S.CRT_RUN_CYC_EXCTN_SK "
    ");"
)

execute_dml(merge_sql_db2_ProvDirDFLoad, jdbc_url, jdbc_props)

# jn_PKEYs: left join df_cpy_Data_out_1 (as lnkFullDataJnIn) with df_lnkPKEYxfmOut (as lnkPKEYxfmOut)
join_cols_jn_PKEYs = [
    "PROV_ID","PROV_ADDR_TYP_CD","PROV_ADDR_EFF_DT_SK","PROV_ADDR_TERM_DT_SK",
    "NTWK_ID","PROV_NTWK_TERM_DT_SK","PROV_NTWK_PFX_ID","SRC_SYS_CD"
]
df_jn_PKEYs = df_cpy_Data_out_1.alias("lnkFullDataJnIn").join(
    df_lnkPKEYxfmOut.alias("lnkPKEYxfmOut"),
    on=[
        F.col("lnkFullDataJnIn.PROV_ID")==F.col("lnkPKEYxfmOut.PROV_ID"),
        F.col("lnkFullDataJnIn.PROV_ADDR_TYP_CD")==F.col("lnkPKEYxfmOut.PROV_ADDR_TYP_CD"),
        F.col("lnkFullDataJnIn.PROV_ADDR_EFF_DT_SK")==F.col("lnkPKEYxfmOut.PROV_ADDR_EFF_DT_SK"),
        F.col("lnkFullDataJnIn.PROV_ADDR_TERM_DT_SK")==F.col("lnkPKEYxfmOut.PROV_ADDR_TERM_DT_SK"),
        F.col("lnkFullDataJnIn.NTWK_ID")==F.col("lnkPKEYxfmOut.NTWK_ID"),
        F.col("lnkFullDataJnIn.PROV_NTWK_TERM_DT_SK")==F.col("lnkPKEYxfmOut.PROV_NTWK_TERM_DT_SK"),
        F.col("lnkFullDataJnIn.PROV_NTWK_PFX_ID")==F.col("lnkPKEYxfmOut.PROV_NTWK_PFX_ID"),
        F.col("lnkFullDataJnIn.SRC_SYS_CD")==F.col("lnkPKEYxfmOut.SRC_SYS_CD"),
    ],
    how="left"
).select(
    F.col("lnkPKEYxfmOut.PROV_DIR_SK").alias("PROV_DIR_SK"),
    F.col("lnkFullDataJnIn.PROV_ID").alias("PROV_ID"),
    F.col("lnkFullDataJnIn.PROV_ADDR_TYP_CD").alias("PROV_ADDR_TYP_CD"),
    F.col("lnkFullDataJnIn.PROV_ADDR_EFF_DT_SK").alias("PROV_ADDR_EFF_DT_SK"),
    F.col("lnkFullDataJnIn.PROV_ADDR_TERM_DT_SK").alias("PROV_ADDR_TERM_DT_SK"),
    F.col("lnkFullDataJnIn.NTWK_ID").alias("NTWK_ID"),
    F.col("lnkFullDataJnIn.PROV_NTWK_TERM_DT_SK").alias("PROV_NTWK_TERM_DT_SK"),
    F.col("lnkFullDataJnIn.PROV_NTWK_PFX_ID").alias("PROV_NTWK_PFX_ID"),
    F.col("lnkFullDataJnIn.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnkPKEYxfmOut.CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("lnkFullDataJnIn.LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("lnkFullDataJnIn.PROV_SPEC_NM").alias("PROV_SPEC_NM"),
    F.col("lnkFullDataJnIn.PROV_REL_GRP_PROV_ID").alias("PROV_REL_GRP_PROV_ID"),
    F.col("lnkFullDataJnIn.PROV_REL_GRP_PROV_NM").alias("PROV_REL_GRP_PROV_NM"),
    F.col("lnkFullDataJnIn.PROV_NM").alias("PROV_NM"),
    F.col("lnkFullDataJnIn.CMN_PRCT_TTL").alias("CMN_PRCT_TTL"),
    F.col("lnkFullDataJnIn.CMN_PRCT_SSN").alias("CMN_PRCT_SSN"),
    F.col("lnkFullDataJnIn.CMN_PRCT_GNDR_CD").alias("CMN_PRCT_GNDR_CD"),
    F.col("lnkFullDataJnIn.CMN_PRCT_BRTH_DT_SK").alias("CMN_PRCT_BRTH_DT_SK"),
    F.col("lnkFullDataJnIn.PROV_ADDR_LN_1").alias("PROV_ADDR_LN_1"),
    F.col("lnkFullDataJnIn.PROV_ADDR_LN_2").alias("PROV_ADDR_LN_2"),
    F.col("lnkFullDataJnIn.PROV_ADDR_LN_3").alias("PROV_ADDR_LN_3"),
    F.col("lnkFullDataJnIn.PROV_ADDR_CITY_NM").alias("PROV_ADDR_CITY_NM"),
    F.col("lnkFullDataJnIn.PROV_ADDR_ST_CD").alias("PROV_ADDR_ST_CD"),
    F.col("lnkFullDataJnIn.PROV_ADDR_CNTY_NM").alias("PROV_ADDR_CNTY_NM"),
    F.col("lnkFullDataJnIn.PROV_ADDR_ZIP_CD_5").alias("PROV_ADDR_ZIP_CD_5"),
    F.col("lnkFullDataJnIn.PROV_ADDR_ZIP_CD_4").alias("PROV_ADDR_ZIP_CD_4"),
    F.col("lnkFullDataJnIn.PROV_ADDR_PHN_NO").alias("PROV_ADDR_PHN_NO"),
    F.col("lnkFullDataJnIn.PROV_ADDR_FAX_NO").alias("PROV_ADDR_FAX_NO"),
    F.col("lnkFullDataJnIn.PROV_ADDR_HCAP_IN").alias("PROV_ADDR_HCAP_IN"),
    F.col("lnkFullDataJnIn.PROV_ENTY_CD").alias("PROV_ENTY_CD"),
    F.col("lnkFullDataJnIn.PAR_PROV_IN").alias("PAR_PROV_IN"),
    F.col("lnkFullDataJnIn.PROV_NTWK_PCP_IN").alias("PROV_NTWK_PCP_IN"),
    F.col("lnkFullDataJnIn.PROV_TAX_ID").alias("PROV_TAX_ID"),
    F.col("lnkFullDataJnIn.ENTY_LIC_ST_CD_1").alias("ENTY_LIC_ST_CD_1"),
    F.col("lnkFullDataJnIn.ENTY_LIC_NO_1").alias("ENTY_LIC_NO_1"),
    F.col("lnkFullDataJnIn.ENTY_LIC_ST_CD_2").alias("ENTY_LIC_ST_CD_2"),
    F.col("lnkFullDataJnIn.ENTY_LIC_NO_2").alias("ENTY_LIC_NO_2"),
    F.col("lnkFullDataJnIn.PROV_NTWK_ACPTNG_PATN_IN").alias("PROV_NTWK_ACPTNG_PATN_IN"),
    F.col("lnkFullDataJnIn.PROV_NTWK_ACPTNG_MCAID_PATN_IN").alias("PROV_NTWK_ACPTNG_MCAID_PATN_IN"),
    F.col("lnkFullDataJnIn.PROV_NTWK_ACPTNG_MCARE_PATN_IN").alias("PROV_NTWK_ACPTNG_MCARE_PATN_IN"),
    F.col("lnkFullDataJnIn.PROV_NTWK_MAX_PATN_QTY").alias("PROV_NTWK_MAX_PATN_QTY"),
    F.col("lnkFullDataJnIn.PROV_NTWK_MAX_PATN_AGE").alias("PROV_NTWK_MAX_PATN_AGE"),
    F.col("lnkFullDataJnIn.PROV_TYP_CD").alias("PROV_TYP_CD"),
    F.col("lnkFullDataJnIn.PROV_TYP_NM").alias("PROV_TYP_NM"),
    F.col("lnkFullDataJnIn.PROV_FCLTY_TYP_CD").alias("PROV_FCLTY_TYP_CD"),
    F.col("lnkFullDataJnIn.PROV_FCLTY_TYP_NM").alias("PROV_FCLTY_TYP_NM"),
    F.col("lnkFullDataJnIn.PROV_SPEC_CD").alias("PROV_SPEC_CD"),
    F.col("lnkFullDataJnIn.PROV_NTWK_MIN_PATN_AGE").alias("PROV_NTWK_MIN_PATN_AGE"),
    F.col("lnkFullDataJnIn.PROV_NTWK_GNDR_ACPTD_CD").alias("PROV_NTWK_GNDR_ACPTD_CD"),
    F.col("lnkFullDataJnIn.EXTR_DT").alias("EXTR_DT"),
    F.col("lnkFullDataJnIn.CMN_PRCT_ID").alias("CMN_PRCT_ID"),
    F.col("lnkFullDataJnIn.LEAPFROG_IN").alias("LEAPFROG_IN"),
    F.col("lnkFullDataJnIn.PROV_SK").alias("PROV_SK"),
    F.col("lnkFullDataJnIn.PROV_ADDR_GEO_ACES_RTRN_CD_TX").alias("PROV_ADDR_GEO_ACES_RTRN_CD_TX"),
    F.col("lnkFullDataJnIn.PROV_ADDR_LAT_TX").alias("PROV_ADDR_LAT_TX"),
    F.col("lnkFullDataJnIn.PROV_ADDR_LONG_TX").alias("PROV_ADDR_LONG_TX"),
    F.col("lnkPKEYxfmOut.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("lnkFullDataJnIn.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("lnkFullDataJnIn.PCMH_IN").alias("PCMH_IN")
)

# seq_ProvDirDPKey -> write PROV_DIR_D.dat
# Apply rpad on columns with char length and keep final order of 52 columns
df_seq_ProvDirDPKey = df_jn_PKEYs.select(
    F.col("PROV_DIR_SK"),
    F.col("PROV_ID"),
    F.col("PROV_ADDR_TYP_CD"),
    F.rpad(F.col("PROV_ADDR_EFF_DT_SK"), 10, " ").alias("PROV_ADDR_EFF_DT_SK"),
    F.rpad(F.col("PROV_ADDR_TERM_DT_SK"), 10, " ").alias("PROV_ADDR_TERM_DT_SK"),
    F.col("NTWK_ID"),
    F.rpad(F.col("PROV_NTWK_TERM_DT_SK"), 10, " ").alias("PROV_NTWK_TERM_DT_SK"),
    F.col("PROV_NTWK_PFX_ID"),
    F.col("SRC_SYS_CD"),
    F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("PROV_SPEC_NM"),
    F.col("PROV_REL_GRP_PROV_ID"),
    F.col("PROV_REL_GRP_PROV_NM"),
    F.col("PROV_NM"),
    F.col("CMN_PRCT_TTL"),
    F.col("CMN_PRCT_SSN"),
    F.col("CMN_PRCT_GNDR_CD"),
    F.rpad(F.col("CMN_PRCT_BRTH_DT_SK"), 10, " ").alias("CMN_PRCT_BRTH_DT_SK"),
    F.col("PROV_ADDR_LN_1"),
    F.col("PROV_ADDR_LN_2"),
    F.col("PROV_ADDR_LN_3"),
    F.col("PROV_ADDR_CITY_NM"),
    F.col("PROV_ADDR_ST_CD"),
    F.col("PROV_ADDR_CNTY_NM"),
    F.rpad(F.col("PROV_ADDR_ZIP_CD_5"), 5, " ").alias("PROV_ADDR_ZIP_CD_5"),
    F.rpad(F.col("PROV_ADDR_ZIP_CD_4"), 4, " ").alias("PROV_ADDR_ZIP_CD_4"),
    F.col("PROV_ADDR_PHN_NO"),
    F.col("PROV_ADDR_FAX_NO"),
    F.rpad(F.col("PROV_ADDR_HCAP_IN"), 1, " ").alias("PROV_ADDR_HCAP_IN"),
    F.col("PROV_ENTY_CD"),
    F.rpad(F.col("PAR_PROV_IN"), 1, " ").alias("PAR_PROV_IN"),
    F.rpad(F.col("PROV_NTWK_PCP_IN"), 1, " ").alias("PROV_NTWK_PCP_IN"),
    F.col("PROV_TAX_ID"),
    F.col("ENTY_LIC_ST_CD_1"),
    F.col("ENTY_LIC_NO_1"),
    F.col("ENTY_LIC_ST_CD_2"),
    F.col("ENTY_LIC_NO_2"),
    F.rpad(F.col("PROV_NTWK_ACPTNG_PATN_IN"), 1, " ").alias("PROV_NTWK_ACPTNG_PATN_IN"),
    F.rpad(F.col("PROV_NTWK_ACPTNG_MCAID_PATN_IN"), 1, " ").alias("PROV_NTWK_ACPTNG_MCAID_PATN_IN"),
    F.rpad(F.col("PROV_NTWK_ACPTNG_MCARE_PATN_IN"), 1, " ").alias("PROV_NTWK_ACPTNG_MCARE_PATN_IN"),
    F.col("PROV_NTWK_MAX_PATN_QTY"),
    F.col("PROV_NTWK_MAX_PATN_AGE"),
    F.col("PROV_TYP_CD"),
    F.col("PROV_TYP_NM"),
    F.col("PROV_FCLTY_TYP_CD"),
    F.col("PROV_FCLTY_TYP_NM"),
    F.col("PROV_SPEC_CD"),
    F.col("PROV_NTWK_MIN_PATN_AGE"),
    F.col("PROV_NTWK_GNDR_ACPTD_CD"),
    F.col("EXTR_DT"),
    F.col("CMN_PRCT_ID"),
    F.rpad(F.col("LEAPFROG_IN"), 1, " ").alias("LEAPFROG_IN"),
    F.col("PROV_SK"),
    F.col("PROV_ADDR_GEO_ACES_RTRN_CD_TX"),
    F.col("PROV_ADDR_LAT_TX"),
    F.col("PROV_ADDR_LONG_TX"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.rpad(F.col("PCMH_IN"), 1, " ").alias("PCMH_IN")
)

write_files(
    df_seq_ProvDirDPKey,
    f"{adls_path}/load/PROV_DIR_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)