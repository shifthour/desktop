# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC Shiva Devagiri               07/17/2013        5114                             Load DM Table MBRSHP_DM_CLS_PLN_DTL                              IntegrateWrhsDevl    Peter Marshall              10/21/2013    
# MAGIC 
# MAGIC Jag Yelavarthi              2013-12-18          5114 - Daptiv#627        Sort in the Copy stage is taken out                                                   IntegrateWrhsDevl    Bhoomi Dasari           2013-12-18
# MAGIC 
# MAGIC Jag Yelavarthi                  2015-04-08       5345-  Daptiv#253        Added Sorting in-addition to Partitioning in Copy stage.                  IntegrateNewDevl         Kalyan Neelam            2015-04-13
# MAGIC                                                                                                       Sort will let the job run without hanging in a multi-configuration
# MAGIC                                                                                                       environment. This is to support new DataDirect driver.
# MAGIC 
# MAGIC Karthik Chintalapani        2016-11-11               5634      Added new column PLN_YR_BEG_DT_MO_DAY                                          IntegrateDev2             Kalyan Neelam            2016-11-28

# MAGIC Job Name: IdsDmMbrshpDmClsPlnDtlLoad
# MAGIC Read Load File created in the IdsDmMbrshpDmClsPlnDtlExtr Job
# MAGIC Copy Stage for buffer
# MAGIC Truncate then Insert the MBRSHP_CLS_PLN_DTL Data.
# MAGIC Load rejects are redirected into a Reject file
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


ClmMartOwner = get_widget_value('ClmMartOwner','')
clmmart_secret_name = get_widget_value('clmmart_secret_name','')
ClmMartArraySize = get_widget_value('ClmMartArraySize','')
ClmMartRecordCount = get_widget_value('ClmMartRecordCount','')

schema_seq_MBR_DM_CLS_PLN_DTL_csv_load = StructType([
    StructField("GRP_ID", StringType(), False),
    StructField("CLS_ID", StringType(), False),
    StructField("CLS_PLN_ID", StringType(), False),
    StructField("CLS_PLN_PROD_CAT_CD", StringType(), False),
    StructField("CLS_PLN_EFF_DT", TimestampType(), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PROD_ID", StringType(), False),
    StructField("CLS_PLN_DTL_NTWK_SET_PFX_CD", StringType(), False),
    StructField("CLS_PLN_DTL_NTWK_SET_PFX_NM", StringType(), False),
    StructField("CLS_PLN_TERM_DT", TimestampType(), False),
    StructField("CLS_PLN_PROD_CAT_NM", StringType(), False),
    StructField("LAST_UPDT_RUN_CYC_NO", IntegerType(), False),
    StructField("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("ALPHA_PFX_CD", StringType(), True),
    StructField("CLS_DESC", StringType(), True),
    StructField("PLN_YR_BEG_DT_MO_DAY", StringType(), False)
])

df_seq_MBR_DM_CLS_PLN_DTL_csv_load = (
    spark.read
    .option("sep", ",")
    .option("quote", "^")
    .option("header", "false")
    .schema(schema_seq_MBR_DM_CLS_PLN_DTL_csv_load)
    .csv(f"{adls_path}/load/MBRSHP_DM_CLS_PLN_DTL.dat")
)

df_cpy_forBuffer = df_seq_MBR_DM_CLS_PLN_DTL_csv_load.select(
    F.col("GRP_ID"),
    F.col("CLS_ID"),
    F.col("CLS_PLN_ID"),
    F.col("CLS_PLN_PROD_CAT_CD"),
    F.col("CLS_PLN_EFF_DT"),
    F.col("SRC_SYS_CD"),
    F.col("PROD_ID"),
    F.col("CLS_PLN_DTL_NTWK_SET_PFX_CD"),
    F.col("CLS_PLN_DTL_NTWK_SET_PFX_NM"),
    F.col("CLS_PLN_TERM_DT"),
    F.col("CLS_PLN_PROD_CAT_NM"),
    F.col("LAST_UPDT_RUN_CYC_NO"),
    F.col("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("ALPHA_PFX_CD"),
    F.col("CLS_DESC"),
    F.col("PLN_YR_BEG_DT_MO_DAY")
)

df_odbc_MBRSHP_DM_CLS_PLN_DTL_out = (
    df_cpy_forBuffer
    .withColumn("GRP_ID", F.rpad(F.col("GRP_ID"), F.lit(<...>), F.lit(" ")))
    .withColumn("CLS_ID", F.rpad(F.col("CLS_ID"), F.lit(<...>), F.lit(" ")))
    .withColumn("CLS_PLN_ID", F.rpad(F.col("CLS_PLN_ID"), F.lit(<...>), F.lit(" ")))
    .withColumn("CLS_PLN_PROD_CAT_CD", F.rpad(F.col("CLS_PLN_PROD_CAT_CD"), F.lit(<...>), F.lit(" ")))
    .withColumn("SRC_SYS_CD", F.rpad(F.col("SRC_SYS_CD"), F.lit(<...>), F.lit(" ")))
    .withColumn("PROD_ID", F.rpad(F.col("PROD_ID"), F.lit(8), F.lit(" ")))
    .withColumn("CLS_PLN_DTL_NTWK_SET_PFX_CD", F.rpad(F.col("CLS_PLN_DTL_NTWK_SET_PFX_CD"), F.lit(<...>), F.lit(" ")))
    .withColumn("CLS_PLN_DTL_NTWK_SET_PFX_NM", F.rpad(F.col("CLS_PLN_DTL_NTWK_SET_PFX_NM"), F.lit(<...>), F.lit(" ")))
    .withColumn("CLS_PLN_TERM_DT", F.col("CLS_PLN_TERM_DT"))
    .withColumn("CLS_PLN_PROD_CAT_NM", F.rpad(F.col("CLS_PLN_PROD_CAT_NM"), F.lit(<...>), F.lit(" ")))
    .withColumn("LAST_UPDT_RUN_CYC_NO", F.col("LAST_UPDT_RUN_CYC_NO"))
    .withColumn("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK", F.col("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK"))
    .withColumn("ALPHA_PFX_CD", F.rpad(F.col("ALPHA_PFX_CD"), F.lit(<...>), F.lit(" ")))
    .withColumn("CLS_DESC", F.rpad(F.col("CLS_DESC"), F.lit(<...>), F.lit(" ")))
    .withColumn("PLN_YR_BEG_DT_MO_DAY", F.rpad(F.col("PLN_YR_BEG_DT_MO_DAY"), F.lit(4), F.lit(" ")))
    .select(
        "GRP_ID",
        "CLS_ID",
        "CLS_PLN_ID",
        "CLS_PLN_PROD_CAT_CD",
        "CLS_PLN_EFF_DT",
        "SRC_SYS_CD",
        "PROD_ID",
        "CLS_PLN_DTL_NTWK_SET_PFX_CD",
        "CLS_PLN_DTL_NTWK_SET_PFX_NM",
        "CLS_PLN_TERM_DT",
        "CLS_PLN_PROD_CAT_NM",
        "LAST_UPDT_RUN_CYC_NO",
        "IDS_LAST_UPDT_RUN_CYC_EXCTN_SK",
        "ALPHA_PFX_CD",
        "CLS_DESC",
        "PLN_YR_BEG_DT_MO_DAY"
    )
)

jdbc_url, jdbc_props = get_db_config(clmmart_secret_name)
drop_sql = "DROP TABLE IF EXISTS STAGING.IdsDmMbrshDmClsPlnDtlLoad_ODBC_MBRSHP_DM_CLS_PLN_DTL_out_temp"
execute_dml(drop_sql, jdbc_url, jdbc_props)

df_odbc_MBRSHP_DM_CLS_PLN_DTL_out.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.IdsDmMbrshDmClsPlnDtlLoad_ODBC_MBRSHP_DM_CLS_PLN_DTL_out_temp") \
    .mode("overwrite") \
    .save()

merge_sql = f"""
MERGE INTO {ClmMartOwner}.MBRSH_DM_CLS_PLN_DTL AS T
USING STAGING.IdsDmMbrshDmClsPlnDtlLoad_ODBC_MBRSHP_DM_CLS_PLN_DTL_out_temp AS S
ON 
    T.GRP_ID = S.GRP_ID
    AND T.CLS_ID = S.CLS_ID
    AND T.CLS_PLN_ID = S.CLS_PLN_ID
    AND T.CLS_PLN_PROD_CAT_CD = S.CLS_PLN_PROD_CAT_CD
    AND T.CLS_PLN_EFF_DT = S.CLS_PLN_EFF_DT
    AND T.SRC_SYS_CD = S.SRC_SYS_CD
WHEN MATCHED THEN UPDATE SET
    T.PROD_ID = S.PROD_ID,
    T.CLS_PLN_DTL_NTWK_SET_PFX_CD = S.CLS_PLN_DTL_NTWK_SET_PFX_CD,
    T.CLS_PLN_DTL_NTWK_SET_PFX_NM = S.CLS_PLN_DTL_NTWK_SET_PFX_NM,
    T.CLS_PLN_TERM_DT = S.CLS_PLN_TERM_DT,
    T.CLS_PLN_PROD_CAT_NM = S.CLS_PLN_PROD_CAT_NM,
    T.LAST_UPDT_RUN_CYC_NO = S.LAST_UPDT_RUN_CYC_NO,
    T.IDS_LAST_UPDT_RUN_CYC_EXCTN_SK = S.IDS_LAST_UPDT_RUN_CYC_EXCTN_SK,
    T.ALPHA_PFX_CD = S.ALPHA_PFX_CD,
    T.CLS_DESC = S.CLS_DESC,
    T.PLN_YR_BEG_DT_MO_DAY = S.PLN_YR_BEG_DT_MO_DAY
WHEN NOT MATCHED THEN INSERT (
    GRP_ID,
    CLS_ID,
    CLS_PLN_ID,
    CLS_PLN_PROD_CAT_CD,
    CLS_PLN_EFF_DT,
    SRC_SYS_CD,
    PROD_ID,
    CLS_PLN_DTL_NTWK_SET_PFX_CD,
    CLS_PLN_DTL_NTWK_SET_PFX_NM,
    CLS_PLN_TERM_DT,
    CLS_PLN_PROD_CAT_NM,
    LAST_UPDT_RUN_CYC_NO,
    IDS_LAST_UPDT_RUN_CYC_EXCTN_SK,
    ALPHA_PFX_CD,
    CLS_DESC,
    PLN_YR_BEG_DT_MO_DAY
) VALUES (
    S.GRP_ID,
    S.CLS_ID,
    S.CLS_PLN_ID,
    S.CLS_PLN_PROD_CAT_CD,
    S.CLS_PLN_EFF_DT,
    S.SRC_SYS_CD,
    S.PROD_ID,
    S.CLS_PLN_DTL_NTWK_SET_PFX_CD,
    S.CLS_PLN_DTL_NTWK_SET_PFX_NM,
    S.CLS_PLN_TERM_DT,
    S.CLS_PLN_PROD_CAT_NM,
    S.LAST_UPDT_RUN_CYC_NO,
    S.IDS_LAST_UPDT_RUN_CYC_EXCTN_SK,
    S.ALPHA_PFX_CD,
    S.CLS_DESC,
    S.PLN_YR_BEG_DT_MO_DAY
)
;
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)

df_seq_MBRSHP_DM_CLS_PLN_DTL_csv_rej = (
    df_odbc_MBRSHP_DM_CLS_PLN_DTL_out
    .withColumn("ERRORCODE", F.lit(None).cast(StringType()))
    .withColumn("ERRORTEXT", F.lit(None).cast(StringType()))
    .withColumn("GRP_ID", F.rpad(F.col("GRP_ID"), F.lit(<...>), F.lit(" ")))
    .withColumn("CLS_ID", F.rpad(F.col("CLS_ID"), F.lit(<...>), F.lit(" ")))
    .withColumn("CLS_PLN_ID", F.rpad(F.col("CLS_PLN_ID"), F.lit(<...>), F.lit(" ")))
    .withColumn("CLS_PLN_PROD_CAT_CD", F.rpad(F.col("CLS_PLN_PROD_CAT_CD"), F.lit(<...>), F.lit(" ")))
    .withColumn("SRC_SYS_CD", F.rpad(F.col("SRC_SYS_CD"), F.lit(<...>), F.lit(" ")))
    .withColumn("PROD_ID", F.rpad(F.col("PROD_ID"), F.lit(8), F.lit(" ")))
    .withColumn("CLS_PLN_DTL_NTWK_SET_PFX_CD", F.rpad(F.col("CLS_PLN_DTL_NTWK_SET_PFX_CD"), F.lit(<...>), F.lit(" ")))
    .withColumn("CLS_PLN_DTL_NTWK_SET_PFX_NM", F.rpad(F.col("CLS_PLN_DTL_NTWK_SET_PFX_NM"), F.lit(<...>), F.lit(" ")))
    .withColumn("CLS_PLN_TERM_DT", F.col("CLS_PLN_TERM_DT"))
    .withColumn("CLS_PLN_PROD_CAT_NM", F.rpad(F.col("CLS_PLN_PROD_CAT_NM"), F.lit(<...>), F.lit(" ")))
    .withColumn("LAST_UPDT_RUN_CYC_NO", F.col("LAST_UPDT_RUN_CYC_NO"))
    .withColumn("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK", F.col("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK"))
    .withColumn("ALPHA_PFX_CD", F.rpad(F.col("ALPHA_PFX_CD"), F.lit(<...>), F.lit(" ")))
    .withColumn("CLS_DESC", F.rpad(F.col("CLS_DESC"), F.lit(<...>), F.lit(" ")))
    .withColumn("PLN_YR_BEG_DT_MO_DAY", F.rpad(F.col("PLN_YR_BEG_DT_MO_DAY"), F.lit(4), F.lit(" ")))
    .filter(F.lit(False))
    .select(
        "GRP_ID",
        "CLS_ID",
        "CLS_PLN_ID",
        "CLS_PLN_PROD_CAT_CD",
        "CLS_PLN_EFF_DT",
        "SRC_SYS_CD",
        "PROD_ID",
        "CLS_PLN_DTL_NTWK_SET_PFX_CD",
        "CLS_PLN_DTL_NTWK_SET_PFX_NM",
        "CLS_PLN_TERM_DT",
        "CLS_PLN_PROD_CAT_NM",
        "LAST_UPDT_RUN_CYC_NO",
        "IDS_LAST_UPDT_RUN_CYC_EXCTN_SK",
        "ALPHA_PFX_CD",
        "CLS_DESC",
        "PLN_YR_BEG_DT_MO_DAY",
        "ERRORCODE",
        "ERRORTEXT"
    )
)

write_files(
    df_seq_MBRSHP_DM_CLS_PLN_DTL_csv_rej,
    f"{adls_path}/load/MBRSHP_DM_CLS_PLN_DTL_Rej.dat",
    delimiter=",",
    mode="append",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)