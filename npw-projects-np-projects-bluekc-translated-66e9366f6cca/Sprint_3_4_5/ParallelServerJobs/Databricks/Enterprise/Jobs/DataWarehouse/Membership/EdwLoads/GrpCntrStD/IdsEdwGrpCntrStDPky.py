# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC Rama Kamjula    07/30/2013        5114                              Originally Programmed  (In Parallel)                                                       EnterpriseWhseDevl

# MAGIC Left Outer Join Here
# MAGIC New PKEYs are generated in this transformer. A database sequence will be used to create next PKEY value.
# MAGIC Table K_GRP_CNTR_ST_D.Insert new SKEY rows generated as part of this run. This is a database insert operation.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, lit, when, rpad
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


edw_secret_name = get_widget_value('edw_secret_name','')
EDWOwner = get_widget_value('EDWOwner','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
EDWRunCycle = get_widget_value('EDWRunCycle','')

jdbc_url, jdbc_props = get_db_config(edw_secret_name)

df_ds_GrpCntrStD = spark.read.parquet(f"{adls_path}/ds/GRP_CNTR_ST_D.parquet")
df_ds_GrpCntrStD = df_ds_GrpCntrStD.select(
    "GRP_CNTR_ST_SK",
    "GRP_ID",
    "GRP_CNTR_ST_EFF_DT_SK",
    "SRC_SYS_CD",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "GRP_SK",
    "GRP_CNTR_ST_CD",
    "GRP_CNTR_ST_NM",
    "GRP_CNTR_ST_TERM_DT_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "IDS_LAST_UPDT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "GRP_CNTR_ST_CD_SK"
)

df_lnk_NaturalKeys_In = df_ds_GrpCntrStD.select(
    col("GRP_ID").alias("GRP_ID"),
    col("GRP_CNTR_ST_EFF_DT_SK").alias("GRP_CNTR_ST_EFF_DT_SK"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD")
)

df_lnkFullDataJnIn = df_ds_GrpCntrStD.select(
    col("GRP_ID").alias("GRP_ID"),
    col("GRP_CNTR_ST_EFF_DT_SK").alias("GRP_CNTR_ST_EFF_DT_SK"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("GRP_SK").alias("GRP_SK"),
    col("GRP_CNTR_ST_CD").alias("GRP_CNTR_ST_CD"),
    col("GRP_CNTR_ST_NM").alias("GRP_CNTR_ST_NM"),
    col("GRP_CNTR_ST_TERM_DT_SK").alias("GRP_CNTR_ST_TERM_DT_SK"),
    col("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK").alias("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("GRP_CNTR_ST_CD_SK").alias("GRP_CNTR_ST_CD_SK")
)

extract_query = """SELECT 
GRP_ID,
GRP_CNTR_ST_EFF_DT_SK,
SRC_SYS_CD,
CRT_RUN_CYC_EXCTN_DT_SK,
GRP_CNTR_ST_SK,
CRT_RUN_CYC_EXCTN_SK
FROM #$EDWOwner#.K_GRP_CNTR_ST_D"""

df_db2_KGrpCntrStD = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_jn_GrpCntrStD = (
    df_lnk_NaturalKeys_In.alias("lnk_NaturalKeys_In")
    .join(
        df_db2_KGrpCntrStD.alias("lnk_KGrpCntrStD_In"),
        (
            (col("lnk_NaturalKeys_In.GRP_ID") == col("lnk_KGrpCntrStD_In.GRP_ID"))
            & (col("lnk_NaturalKeys_In.GRP_CNTR_ST_EFF_DT_SK") == col("lnk_KGrpCntrStD_In.GRP_CNTR_ST_EFF_DT_SK"))
            & (col("lnk_NaturalKeys_In.SRC_SYS_CD") == col("lnk_KGrpCntrStD_In.SRC_SYS_CD"))
        ),
        how="left"
    )
    .select(
        col("lnk_NaturalKeys_In.GRP_ID").alias("GRP_ID"),
        col("lnk_NaturalKeys_In.GRP_CNTR_ST_EFF_DT_SK").alias("GRP_CNTR_ST_EFF_DT_SK"),
        col("lnk_NaturalKeys_In.SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("lnk_KGrpCntrStD_In.CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        col("lnk_KGrpCntrStD_In.GRP_CNTR_ST_SK").alias("GRP_CNTR_ST_SK"),
        col("lnk_KGrpCntrStD_In.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK")
    )
)

df_xfm_PKEYgen = df_jn_GrpCntrStD.withColumn("isnull_key", col("GRP_CNTR_ST_SK").isNull())
df_enriched = SurrogateKeyGen(df_xfm_PKEYgen,<DB sequence name>,"GRP_CNTR_ST_SK",<schema>,<secret_name>)

df_lnk_IdsEdwKGrpCntrStDPKey_Out = df_enriched.filter(col("isnull_key") == True).select(
    col("GRP_ID").alias("GRP_ID"),
    col("GRP_CNTR_ST_EFF_DT_SK").alias("GRP_CNTR_ST_EFF_DT_SK"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    col("GRP_CNTR_ST_SK").alias("GRP_CNTR_ST_SK"),
    lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK")
)

drop_sql = "DROP TABLE IF EXISTS STAGING.IdsEdwGrpCntrStDPky_db2_KGrpCntrStD_Load_temp"
execute_dml(drop_sql, jdbc_url, jdbc_props)

df_lnk_IdsEdwKGrpCntrStDPKey_Out.write.jdbc(
    url=jdbc_url,
    table="STAGING.IdsEdwGrpCntrStDPky_db2_KGrpCntrStD_Load_temp",
    mode="overwrite",
    properties=jdbc_props
)

merge_sql = f"""
MERGE #$EDWOwner#.K_GRP_CNTR_ST_D as T
USING STAGING.IdsEdwGrpCntrStDPky_db2_KGrpCntrStD_Load_temp as S
ON
    T.GRP_ID = S.GRP_ID
    AND T.GRP_CNTR_ST_EFF_DT_SK = S.GRP_CNTR_ST_EFF_DT_SK
    AND T.SRC_SYS_CD = S.SRC_SYS_CD
WHEN MATCHED THEN
    UPDATE SET
        T.CRT_RUN_CYC_EXCTN_DT_SK = S.CRT_RUN_CYC_EXCTN_DT_SK,
        T.GRP_CNTR_ST_SK = S.GRP_CNTR_ST_SK,
        T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK
WHEN NOT MATCHED THEN
    INSERT (
        GRP_ID,
        GRP_CNTR_ST_EFF_DT_SK,
        SRC_SYS_CD,
        CRT_RUN_CYC_EXCTN_DT_SK,
        GRP_CNTR_ST_SK,
        CRT_RUN_CYC_EXCTN_SK
    )
    VALUES (
        S.GRP_ID,
        S.GRP_CNTR_ST_EFF_DT_SK,
        S.SRC_SYS_CD,
        S.CRT_RUN_CYC_EXCTN_DT_SK,
        S.GRP_CNTR_ST_SK,
        S.CRT_RUN_CYC_EXCTN_SK
    )
;
"""
execute_dml(merge_sql, jdbc_url, jdbc_props)

df_lnkPKEYxfmOut = df_enriched.select(
    col("GRP_CNTR_ST_SK").alias("GRP_CNTR_ST_SK"),
    col("GRP_ID").alias("GRP_ID"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("GRP_CNTR_ST_EFF_DT_SK").alias("GRP_CNTR_ST_EFF_DT_SK"),
    when(col("isnull_key") == True, EDWRunCycleDate).otherwise(col("CRT_RUN_CYC_EXCTN_DT_SK")).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    when(col("isnull_key") == True, EDWRunCycle).otherwise(col("CRT_RUN_CYC_EXCTN_SK")).alias("CRT_RUN_CYC_EXCTN_SK")
)

df_jn_PKEYs = (
    df_lnkFullDataJnIn.alias("lnkFullDataJnIn")
    .join(
        df_lnkPKEYxfmOut.alias("lnkPKEYxfmOut"),
        (
            (col("lnkFullDataJnIn.GRP_ID") == col("lnkPKEYxfmOut.GRP_ID"))
            & (col("lnkFullDataJnIn.GRP_CNTR_ST_EFF_DT_SK") == col("lnkPKEYxfmOut.GRP_CNTR_ST_EFF_DT_SK"))
            & (col("lnkFullDataJnIn.SRC_SYS_CD") == col("lnkPKEYxfmOut.SRC_SYS_CD"))
        ),
        how="left"
    )
    .select(
        col("lnkPKEYxfmOut.GRP_CNTR_ST_SK").alias("GRP_CNTR_ST_SK"),
        col("lnkFullDataJnIn.GRP_ID").alias("GRP_ID"),
        col("lnkFullDataJnIn.GRP_CNTR_ST_EFF_DT_SK").alias("GRP_CNTR_ST_EFF_DT_SK"),
        col("lnkFullDataJnIn.SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("lnkPKEYxfmOut.CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        col("lnkFullDataJnIn.LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        col("lnkFullDataJnIn.GRP_SK").alias("GRP_SK"),
        col("lnkFullDataJnIn.GRP_CNTR_ST_CD").alias("GRP_CNTR_ST_CD"),
        col("lnkFullDataJnIn.GRP_CNTR_ST_NM").alias("GRP_CNTR_ST_NM"),
        col("lnkFullDataJnIn.GRP_CNTR_ST_TERM_DT_SK").alias("GRP_CNTR_ST_TERM_DT_SK"),
        col("lnkPKEYxfmOut.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("lnkFullDataJnIn.IDS_LAST_UPDT_RUN_CYC_EXCTN_SK").alias("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("lnkFullDataJnIn.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("lnkFullDataJnIn.GRP_CNTR_ST_CD_SK").alias("GRP_CNTR_ST_CD_SK")
    )
)

df_final = df_jn_PKEYs.select(
    col("GRP_CNTR_ST_SK"),
    col("GRP_ID"),
    rpad("GRP_CNTR_ST_EFF_DT_SK", 10, " ").alias("GRP_CNTR_ST_EFF_DT_SK"),
    col("SRC_SYS_CD"),
    rpad("CRT_RUN_CYC_EXCTN_DT_SK", 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    rpad("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("GRP_SK"),
    col("GRP_CNTR_ST_CD"),
    col("GRP_CNTR_ST_NM"),
    rpad("GRP_CNTR_ST_TERM_DT_SK", 10, " ").alias("GRP_CNTR_ST_TERM_DT_SK"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("GRP_CNTR_ST_CD_SK")
)

write_files(
    df_final,
    f"{adls_path}/load/GRP_CNTR_ST_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)