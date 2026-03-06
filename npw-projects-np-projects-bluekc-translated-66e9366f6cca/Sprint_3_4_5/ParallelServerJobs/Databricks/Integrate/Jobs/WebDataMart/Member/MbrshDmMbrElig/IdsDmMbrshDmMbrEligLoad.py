# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC 
# MAGIC Shiva Devagiri         08/12/2013        5114                             Load DM Table MBRSH_DM_MBR_ELIG                                            IntegrateWrhsDevl     Peter Marshall              10/21/2013  
# MAGIC 
# MAGIC Pooja Sunkara           07/30/2014         5345                          After job sub-routine call is modified with the correct file name.             IntegrateNewDevl         Jag Yelavarthi           2014-08-01
# MAGIC     
# MAGIC Jag Yelavarthi          2015-04-08          5345 -Daptiv#253         Added Sorting in-addition to Partitioning in Copy stage.                         IntegrateNewDevl         Kalyan Neelam           2015-04-13  
# MAGIC                                                                                                   Sort will let the job run without hanging in a multi-configuration
# MAGIC                                                                                                   environment. 
# MAGIC Kalyan Neelam           2015-05-06           5398 DPS2E               Added 3 new columns on the end - PROD_SH_NM_CAT_CD,            IntegrateNewDevl     Bhoomi Dasari           5/11/2015
# MAGIC                                                                                                    PROD_SH_NM_CAT_NM,  PROD_SH_NM_MCARE_SUPLMT_COV_IN
# MAGIC 
# MAGIC   
# MAGIC Neelima Tummala     2021-10-03           US 433707               Added 1 new columns on the end -BILL_ELIG_IN,                                 Integrate Dev1            Reddy Sanam            11/18/2021

# MAGIC Job Name: IdsDmMbrshDmIndBeLtrLoad
# MAGIC Read Load File created in the IdsDmMbrshDmMbrEligExtr Job
# MAGIC Copy Stage for buffer
# MAGIC Update then Insert the MBRSH_DM_MBR_ELIG Data.
# MAGIC Load rejects are redirected into a Reject file
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


ClmMartOwner = get_widget_value('ClmMartOwner','')
clmmart_secret_name = get_widget_value('clmmart_secret_name','')
ClmMartArraySize = get_widget_value('ClmMartArraySize','')
ClmMartRecordCount = get_widget_value('ClmMartRecordCount','')

schema_seq_MBRSH_DM_MBR_ELIG_csv_load = StructType([
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("MBR_UNIQ_KEY", IntegerType(), False),
    StructField("MBR_ENR_CLS_PLN_PROD_CAT_CD", StringType(), False),
    StructField("MBR_ENR_EFF_DT", TimestampType(), False),
    StructField("MBR_ENR_CLS_PLN_PROD_CAT_NM", StringType(), True),
    StructField("MBR_ENR_ELIG_RSN_CD", StringType(), False),
    StructField("MBR_ENR_ELIG_RSN_NM", StringType(), True),
    StructField("MBR_ENR_INELGY_EXCD", StringType(), False),
    StructField("MBR_ENR_INELGY_EXCD_SH_TX", StringType(), True),
    StructField("MBR_ENR_OVALL_ELIG_EXCD", StringType(), False),
    StructField("MBR_ENR_OVALL_ELIG_EXCD_SH_TX", StringType(), True),
    StructField("MBR_ENR_PRCS_STTUS_CD", StringType(), False),
    StructField("MBR_ENR_PRCS_STTUS_NM", StringType(), True),
    StructField("MBR_ENR_ELIG_IN", StringType(), False),
    StructField("MBR_ENR_PLN_ENTRY_DT", TimestampType(), True),
    StructField("MBR_ENR_SRC_SYS_CRT_DT", TimestampType(), True),
    StructField("MBR_ENR_TERM_DT", TimestampType(), True),
    StructField("CLS_ID", StringType(), False),
    StructField("CLS_DESC", StringType(), True),
    StructField("CLS_PLN_ID", StringType(), False),
    StructField("CLS_PLN_DESC", StringType(), True),
    StructField("GRP_UNIQ_KEY", IntegerType(), False),
    StructField("GRP_ID", StringType(), False),
    StructField("PROD_ID", StringType(), False),
    StructField("PROD_DESC", StringType(), True),
    StructField("LAST_UPDT_RUN_CYC_NO", IntegerType(), False),
    StructField("PROD_SH_NM", StringType(), True),
    StructField("SUBGRP_ID", StringType(), True),
    StructField("SUBGRP_NM", StringType(), True),
    StructField("PROD_SH_NM_DLVRY_METH_CD", StringType(), True),
    StructField("PROD_SH_NM_DLVRY_METH_NM", StringType(), True),
    StructField("PROD_SUBPROD_CD", StringType(), True),
    StructField("PROD_SUBPROD_NM", StringType(), True),
    StructField("MBR_ENR_ACTV_ROW_IN", StringType(), True),
    StructField("MBR_ENR_ELIG_ST_CONT_CD", StringType(), True),
    StructField("MBR_ENR_ELIG_ST_CONT_NM", StringType(), True),
    StructField("FUND_CAT_CD", StringType(), False),
    StructField("FUND_CAT_NM", StringType(), False),
    StructField("PROD_SH_NM_CAT_CD", StringType(), False),
    StructField("PROD_SH_NM_CAT_NM", StringType(), False),
    StructField("PROD_SH_NM_MCARE_SUPLMT_COV_IN", StringType(), False),
    StructField("BILL_ELIG_IN", StringType(), True)
])

df_seq_MBRSH_DM_MBR_ELIG_csv_load = (
    spark.read.format("csv")
    .option("header", "false")
    .option("delimiter", ",")
    .option("quote", "^")
    .option("nullValue", None)
    .schema(schema_seq_MBRSH_DM_MBR_ELIG_csv_load)
    .load(f"{adls_path}/load/MBRSH_DM_MBR_ELIG.dat")
)

df_cpy_forBuffer = df_seq_MBRSH_DM_MBR_ELIG_csv_load

df_ODBC_MBRSH_DM_MBR_ELIG_out = df_cpy_forBuffer.select(
    F.col("SRC_SYS_CD"),
    F.col("MBR_UNIQ_KEY"),
    F.col("MBR_ENR_CLS_PLN_PROD_CAT_CD"),
    F.col("MBR_ENR_EFF_DT"),
    F.col("MBR_ENR_CLS_PLN_PROD_CAT_NM"),
    F.col("MBR_ENR_ELIG_RSN_CD"),
    F.col("MBR_ENR_ELIG_RSN_NM"),
    F.col("MBR_ENR_INELGY_EXCD"),
    F.col("MBR_ENR_INELGY_EXCD_SH_TX"),
    F.col("MBR_ENR_OVALL_ELIG_EXCD"),
    F.col("MBR_ENR_OVALL_ELIG_EXCD_SH_TX"),
    F.col("MBR_ENR_PRCS_STTUS_CD"),
    F.col("MBR_ENR_PRCS_STTUS_NM"),
    F.rpad(F.col("MBR_ENR_ELIG_IN"), 1, " ").alias("MBR_ENR_ELIG_IN"),
    F.col("MBR_ENR_PLN_ENTRY_DT"),
    F.col("MBR_ENR_SRC_SYS_CRT_DT"),
    F.col("MBR_ENR_TERM_DT"),
    F.col("CLS_ID"),
    F.col("CLS_DESC"),
    F.col("CLS_PLN_ID"),
    F.col("CLS_PLN_DESC"),
    F.col("GRP_UNIQ_KEY"),
    F.col("GRP_ID"),
    F.col("PROD_ID"),
    F.col("PROD_DESC"),
    F.col("LAST_UPDT_RUN_CYC_NO"),
    F.col("PROD_SH_NM"),
    F.col("SUBGRP_ID"),
    F.col("SUBGRP_NM"),
    F.col("PROD_SH_NM_DLVRY_METH_CD"),
    F.col("PROD_SH_NM_DLVRY_METH_NM"),
    F.col("PROD_SUBPROD_CD"),
    F.col("PROD_SUBPROD_NM"),
    F.rpad(F.col("MBR_ENR_ACTV_ROW_IN"), 1, " ").alias("MBR_ENR_ACTV_ROW_IN"),
    F.col("MBR_ENR_ELIG_ST_CONT_CD"),
    F.col("MBR_ENR_ELIG_ST_CONT_NM"),
    F.col("FUND_CAT_CD"),
    F.col("FUND_CAT_NM"),
    F.col("PROD_SH_NM_CAT_CD"),
    F.col("PROD_SH_NM_CAT_NM"),
    F.rpad(F.col("PROD_SH_NM_MCARE_SUPLMT_COV_IN"), 1, " ").alias("PROD_SH_NM_MCARE_SUPLMT_COV_IN"),
    F.rpad(F.col("BILL_ELIG_IN"), 1, " ").alias("BILL_ELIG_IN")
)

jdbc_url, jdbc_props = get_db_config(clmmart_secret_name)
temp_table = "STAGING.IdsDmMbrshDmMbrEligLoad_ODBC_MBRSH_DM_MBR_ELIG_out_temp"
execute_dml(f"DROP TABLE IF EXISTS {temp_table}", jdbc_url, jdbc_props)

(
    df_ODBC_MBRSH_DM_MBR_ELIG_out.write
    .format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("dbtable", temp_table)
    .mode("overwrite")
    .save()
)

merge_sql = f"""
MERGE INTO {ClmMartOwner}.MBRSH_DM_MBR_ELIG AS T
USING (
    SELECT
        SRC_SYS_CD,
        MBR_UNIQ_KEY,
        MBR_ENR_CLS_PLN_PROD_CAT_CD,
        MBR_ENR_EFF_DT,
        MBR_ENR_CLS_PLN_PROD_CAT_NM,
        MBR_ENR_ELIG_RSN_CD,
        MBR_ENR_ELIG_RSN_NM,
        MBR_ENR_INELGY_EXCD,
        MBR_ENR_INELGY_EXCD_SH_TX,
        MBR_ENR_OVALL_ELIG_EXCD,
        MBR_ENR_OVALL_ELIG_EXCD_SH_TX,
        MBR_ENR_PRCS_STTUS_CD,
        MBR_ENR_PRCS_STTUS_NM,
        MBR_ENR_ELIG_IN,
        MBR_ENR_PLN_ENTRY_DT,
        MBR_ENR_SRC_SYS_CRT_DT,
        MBR_ENR_TERM_DT,
        CLS_ID,
        CLS_DESC,
        CLS_PLN_ID,
        CLS_PLN_DESC,
        GRP_UNIQ_KEY,
        GRP_ID,
        PROD_ID,
        PROD_DESC,
        LAST_UPDT_RUN_CYC_NO,
        PROD_SH_NM,
        SUBGRP_ID,
        SUBGRP_NM,
        PROD_SH_NM_DLVRY_METH_CD,
        PROD_SH_NM_DLVRY_METH_NM,
        PROD_SUBPROD_CD,
        PROD_SUBPROD_NM,
        MBR_ENR_ACTV_ROW_IN,
        MBR_ENR_ELIG_ST_CONT_CD,
        MBR_ENR_ELIG_ST_CONT_NM,
        FUND_CAT_CD,
        FUND_CAT_NM,
        PROD_SH_NM_CAT_CD,
        PROD_SH_NM_CAT_NM,
        PROD_SH_NM_MCARE_SUPLMT_COV_IN,
        BILL_ELIG_IN
    FROM {temp_table}
) AS S
ON
(
    T.SRC_SYS_CD = S.SRC_SYS_CD
    AND T.MBR_UNIQ_KEY = S.MBR_UNIQ_KEY
    AND T.MBR_ENR_CLS_PLN_PROD_CAT_CD = S.MBR_ENR_CLS_PLN_PROD_CAT_CD
    AND T.MBR_ENR_EFF_DT = S.MBR_ENR_EFF_DT
)
WHEN MATCHED THEN
UPDATE SET
    T.MBR_ENR_CLS_PLN_PROD_CAT_NM = S.MBR_ENR_CLS_PLN_PROD_CAT_NM,
    T.MBR_ENR_ELIG_RSN_CD = S.MBR_ENR_ELIG_RSN_CD,
    T.MBR_ENR_ELIG_RSN_NM = S.MBR_ENR_ELIG_RSN_NM,
    T.MBR_ENR_INELGY_EXCD = S.MBR_ENR_INELGY_EXCD,
    T.MBR_ENR_INELGY_EXCD_SH_TX = S.MBR_ENR_INELGY_EXCD_SH_TX,
    T.MBR_ENR_OVALL_ELIG_EXCD = S.MBR_ENR_OVALL_ELIG_EXCD,
    T.MBR_ENR_OVALL_ELIG_EXCD_SH_TX = S.MBR_ENR_OVALL_ELIG_EXCD_SH_TX,
    T.MBR_ENR_PRCS_STTUS_CD = S.MBR_ENR_PRCS_STTUS_CD,
    T.MBR_ENR_PRCS_STTUS_NM = S.MBR_ENR_PRCS_STTUS_NM,
    T.MBR_ENR_ELIG_IN = S.MBR_ENR_ELIG_IN,
    T.MBR_ENR_PLN_ENTRY_DT = S.MBR_ENR_PLN_ENTRY_DT,
    T.MBR_ENR_SRC_SYS_CRT_DT = S.MBR_ENR_SRC_SYS_CRT_DT,
    T.MBR_ENR_TERM_DT = S.MBR_ENR_TERM_DT,
    T.CLS_ID = S.CLS_ID,
    T.CLS_DESC = S.CLS_DESC,
    T.CLS_PLN_ID = S.CLS_PLN_ID,
    T.CLS_PLN_DESC = S.CLS_PLN_DESC,
    T.GRP_UNIQ_KEY = S.GRP_UNIQ_KEY,
    T.GRP_ID = S.GRP_ID,
    T.PROD_ID = S.PROD_ID,
    T.PROD_DESC = S.PROD_DESC,
    T.LAST_UPDT_RUN_CYC_NO = S.LAST_UPDT_RUN_CYC_NO,
    T.PROD_SH_NM = S.PROD_SH_NM,
    T.SUBGRP_ID = S.SUBGRP_ID,
    T.SUBGRP_NM = S.SUBGRP_NM,
    T.PROD_SH_NM_DLVRY_METH_CD = S.PROD_SH_NM_DLVRY_METH_CD,
    T.PROD_SH_NM_DLVRY_METH_NM = S.PROD_SH_NM_DLVRY_METH_NM,
    T.PROD_SUBPROD_CD = S.PROD_SUBPROD_CD,
    T.PROD_SUBPROD_NM = S.PROD_SUBPROD_NM,
    T.MBR_ENR_ACTV_ROW_IN = S.MBR_ENR_ACTV_ROW_IN,
    T.MBR_ENR_ELIG_ST_CONT_CD = S.MBR_ENR_ELIG_ST_CONT_CD,
    T.MBR_ENR_ELIG_ST_CONT_NM = S.MBR_ENR_ELIG_ST_CONT_NM,
    T.FUND_CAT_CD = S.FUND_CAT_CD,
    T.FUND_CAT_NM = S.FUND_CAT_NM,
    T.PROD_SH_NM_CAT_CD = S.PROD_SH_NM_CAT_CD,
    T.PROD_SH_NM_CAT_NM = S.PROD_SH_NM_CAT_NM,
    T.PROD_SH_NM_MCARE_SUPLMT_COV_IN = S.PROD_SH_NM_MCARE_SUPLMT_COV_IN,
    T.BILL_ELIG_IN = S.BILL_ELIG_IN
WHEN NOT MATCHED THEN
INSERT
(
    SRC_SYS_CD,
    MBR_UNIQ_KEY,
    MBR_ENR_CLS_PLN_PROD_CAT_CD,
    MBR_ENR_EFF_DT,
    MBR_ENR_CLS_PLN_PROD_CAT_NM,
    MBR_ENR_ELIG_RSN_CD,
    MBR_ENR_ELIG_RSN_NM,
    MBR_ENR_INELGY_EXCD,
    MBR_ENR_INELGY_EXCD_SH_TX,
    MBR_ENR_OVALL_ELIG_EXCD,
    MBR_ENR_OVALL_ELIG_EXCD_SH_TX,
    MBR_ENR_PRCS_STTUS_CD,
    MBR_ENR_PRCS_STTUS_NM,
    MBR_ENR_ELIG_IN,
    MBR_ENR_PLN_ENTRY_DT,
    MBR_ENR_SRC_SYS_CRT_DT,
    MBR_ENR_TERM_DT,
    CLS_ID,
    CLS_DESC,
    CLS_PLN_ID,
    CLS_PLN_DESC,
    GRP_UNIQ_KEY,
    GRP_ID,
    PROD_ID,
    PROD_DESC,
    LAST_UPDT_RUN_CYC_NO,
    PROD_SH_NM,
    SUBGRP_ID,
    SUBGRP_NM,
    PROD_SH_NM_DLVRY_METH_CD,
    PROD_SH_NM_DLVRY_METH_NM,
    PROD_SUBPROD_CD,
    PROD_SUBPROD_NM,
    MBR_ENR_ACTV_ROW_IN,
    MBR_ENR_ELIG_ST_CONT_CD,
    MBR_ENR_ELIG_ST_CONT_NM,
    FUND_CAT_CD,
    FUND_CAT_NM,
    PROD_SH_NM_CAT_CD,
    PROD_SH_NM_CAT_NM,
    PROD_SH_NM_MCARE_SUPLMT_COV_IN,
    BILL_ELIG_IN
)
VALUES
(
    S.SRC_SYS_CD,
    S.MBR_UNIQ_KEY,
    S.MBR_ENR_CLS_PLN_PROD_CAT_CD,
    S.MBR_ENR_EFF_DT,
    S.MBR_ENR_CLS_PLN_PROD_CAT_NM,
    S.MBR_ENR_ELIG_RSN_CD,
    S.MBR_ENR_ELIG_RSN_NM,
    S.MBR_ENR_INELGY_EXCD,
    S.MBR_ENR_INELGY_EXCD_SH_TX,
    S.MBR_ENR_OVALL_ELIG_EXCD,
    S.MBR_ENR_OVALL_ELIG_EXCD_SH_TX,
    S.MBR_ENR_PRCS_STTUS_CD,
    S.MBR_ENR_PRCS_STTUS_NM,
    S.MBR_ENR_ELIG_IN,
    S.MBR_ENR_PLN_ENTRY_DT,
    S.MBR_ENR_SRC_SYS_CRT_DT,
    S.MBR_ENR_TERM_DT,
    S.CLS_ID,
    S.CLS_DESC,
    S.CLS_PLN_ID,
    S.CLS_PLN_DESC,
    S.GRP_UNIQ_KEY,
    S.GRP_ID,
    S.PROD_ID,
    S.PROD_DESC,
    S.LAST_UPDT_RUN_CYC_NO,
    S.PROD_SH_NM,
    S.SUBGRP_ID,
    S.SUBGRP_NM,
    S.PROD_SH_NM_DLVRY_METH_CD,
    S.PROD_SH_NM_DLVRY_METH_NM,
    S.PROD_SUBPROD_CD,
    S.PROD_SUBPROD_NM,
    S.MBR_ENR_ACTV_ROW_IN,
    S.MBR_ENR_ELIG_ST_CONT_CD,
    S.MBR_ENR_ELIG_ST_CONT_NM,
    S.FUND_CAT_CD,
    S.FUND_CAT_NM,
    S.PROD_SH_NM_CAT_CD,
    S.PROD_SH_NM_CAT_NM,
    S.PROD_SH_NM_MCARE_SUPLMT_COV_IN,
    S.BILL_ELIG_IN
);
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)

df_seq_MBRSH_DM_MBR_ELIG_csv_rej = spark.createDataFrame([], StructType([
    StructField("ERRORCODE", StringType(), True),
    StructField("ERRORTEXT", StringType(), True)
]))

df_seq_MBRSH_DM_MBR_ELIG_csv_rej = df_seq_MBRSH_DM_MBR_ELIG_csv_rej.select("ERRORCODE","ERRORTEXT")

write_files(
    df_seq_MBRSH_DM_MBR_ELIG_csv_rej,
    f"{adls_path}/load/MBRSH_DM_MBR_ELIG_Rej.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)