# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                        DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                          ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        ----------------------------------------------                                                   -------------------------------    ------------------------------       --------------------
# MAGIC Raj Mangalampally    09/03/2013             P5114                       Original Programming                                                                 EnterpriseWrhsDevl    Peter Marshall               12/10/2013
# MAGIC                                                                                                     (Server to Parallel Conversion)
# MAGIC 
# MAGIC Manasa Andru           2016-06-08        TFS - 12538         Updated the data type of   ALT_FUND_INVC_PAYMT_SEQ_NO       EnterpriseDev1           Jag Yelavarthi              2016-06-08
# MAGIC                                                                                         field from SMALLINT to INTEGER so that 10 digit values are loaded

# MAGIC Write ALT_FUND_INVC_DTL_F.dat for the IdsEdwAltFundInvcDtlFLoad Job
# MAGIC New PKEYs are generated in this transformer. A database sequence will be used to create next PKEY value.
# MAGIC This job will be used when there is a need to Assign primary keys to incoming data before loading into Target table.
# MAGIC 
# MAGIC Extract dataset is created in the extract job ran prior to this.
# MAGIC 
# MAGIC New KEYS generated for each run will be Inserted into the K table.
# MAGIC 
# MAGIC Job Name: IdsEdwAltFundInvcDtlFPkey
# MAGIC Remove Duplicates stage takes care of getting rid of duplicate rows with the same Natural key get into the PKEYing step.
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
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
CurrRunCycle = get_widget_value('CurrRunCycle','')

jdbc_url, jdbc_props = get_db_config(edw_secret_name)
df_db2_K_ALT_FUND_INVC_DTL_F_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"SELECT SRC_SYS_CD, ALT_FUND_INVC_ID, ALT_FUND_INVC_DTL_CD, ALT_FUND_INVC_DTL_SEQ_NO, ALT_FUND_INVC_DTL_SK FROM {EDWOwner}.K_ALT_FUND_INVC_DTL_F")
    .load()
)

df_ds_ALT_FUND_INVC_DTL_F_out = spark.read.parquet(f"{adls_path}/ds/ALT_FUND_INVC_DTL_F.parquet")

df_cpy_MultiStreams_temp = df_ds_ALT_FUND_INVC_DTL_F_out

df_lnk_AltFundInvcDtlFFData_L_in = df_cpy_MultiStreams_temp.select(
    "ALT_FUND_INVC_DTL_SK",
    "SRC_SYS_CD",
    "ALT_FUND_INVC_ID",
    "ALT_FUND_INVC_DTL_CD",
    "ALT_FUND_INVC_DTL_SEQ_NO",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "ALT_FUND_SK",
    "ALT_FUND_INVC_SK",
    "ALT_FUND_INVC_DSCRTN_SK",
    "ALT_FUND_INVC_PAYMT_SK",
    "BILL_ENTY_SK",
    "BILL_RDUCTN_HIST_SK",
    "CLM_SK",
    "CLS_SK",
    "CLS_PLN_SK",
    "GRP_SK",
    "SUBGRP_SK",
    "SUB_SK",
    "MBR_SK",
    "PROD_SK",
    "ALT_FUND_INVC_DSCRTN_BEG_DT_SK",
    "ALT_FUND_INVC_DSCRTN_END_DT_SK",
    "ALT_FUND_INVC_FUND_THRU_DT_SK",
    "ALT_FUND_INVC_FUND_FROM_DT_SK",
    "CLM_PD_DT_SK",
    "CLM_PD_YR_MO_SK",
    "CLM_SVC_STRT_DT_SK",
    "CLM_SVC_STRT_YR_MO_SK",
    "ALT_FUND_INVC_DTL_BILL_AMT",
    "ALT_FUND_CNTR_PERD_NO",
    "ALT_FUND_INVC_DSCRTN_MO_QTY",
    "ALT_FUND_INVC_DSCRTN_SEQ_NO",
    "ALT_FUND_INVC_PAYMT_SEQ_NO",
    "BILL_RDUCTN_HIST_SEQ_NO",
    "BILL_ENTY_UNIQ_KEY",
    "MBR_UNIQ_KEY",
    "SUB_UNIQ_KEY",
    "ALT_FUND_ID",
    "ALT_FUND_INVC_DTL_NM",
    "ALT_FUND_INVC_DSCRTN_DESC",
    "AF_INVC_DSCRTN_PRSN_ID_TX",
    "ALT_FUND_INVC_DSCRTN_SH_DESC",
    "ALT_FUND_NM",
    "CLM_ID",
    "CLS_ID",
    "CLS_PLN_ID",
    "GRP_ID",
    "GRP_NM",
    "PROD_ID",
    "SUBGRP_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "ALT_FUND_INVC_DTL_CD_SK"
)

df_lnk_cpy_NKEY = df_cpy_MultiStreams_temp.select(
    "SRC_SYS_CD",
    "ALT_FUND_INVC_ID",
    "ALT_FUND_INVC_DTL_CD",
    "ALT_FUND_INVC_DTL_SEQ_NO"
)

df_rdp_NaturalKeys = dedup_sort(
    df_lnk_cpy_NKEY,
    ["SRC_SYS_CD", "ALT_FUND_INVC_ID", "ALT_FUND_INVC_DTL_CD", "ALT_FUND_INVC_DTL_SEQ_NO"],
    []
)

df_lnk_AltFundInvcDtlFData_L_in = df_rdp_NaturalKeys.select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("ALT_FUND_INVC_ID").alias("ALT_FUND_INVC_ID"),
    F.col("ALT_FUND_INVC_DTL_CD").alias("ALT_FUND_INVC_DTL_CD"),
    F.col("ALT_FUND_INVC_DTL_SEQ_NO").alias("ALT_FUND_INVC_DTL_SEQ_NO")
)

df_jn_AltFundInvcDtlF = df_lnk_AltFundInvcDtlFData_L_in.alias("l").join(
    df_db2_K_ALT_FUND_INVC_DTL_F_in.alias("r"),
    (F.col("l.SRC_SYS_CD") == F.col("r.SRC_SYS_CD"))
    & (F.col("l.ALT_FUND_INVC_ID") == F.col("r.ALT_FUND_INVC_ID"))
    & (F.col("l.ALT_FUND_INVC_DTL_CD") == F.col("r.ALT_FUND_INVC_DTL_CD"))
    & (F.col("l.ALT_FUND_INVC_DTL_SEQ_NO") == F.col("r.ALT_FUND_INVC_DTL_SEQ_NO")),
    how="left"
)

df_lnk_AltFundInvcDtlFJnData_Out = df_jn_AltFundInvcDtlF.select(
    F.col("l.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("l.ALT_FUND_INVC_ID").alias("ALT_FUND_INVC_ID"),
    F.col("l.ALT_FUND_INVC_DTL_CD").alias("ALT_FUND_INVC_DTL_CD"),
    F.col("l.ALT_FUND_INVC_DTL_SEQ_NO").alias("ALT_FUND_INVC_DTL_SEQ_NO"),
    F.col("r.ALT_FUND_INVC_DTL_SK").alias("ALT_FUND_INVC_DTL_SK")
)

df_enriched = df_lnk_AltFundInvcDtlFJnData_Out.withColumn("orig_ALT_FUND_INVC_DTL_SK", F.col("ALT_FUND_INVC_DTL_SK"))
df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"ALT_FUND_INVC_DTL_SK",<schema>,<secret_name>)

df_lnk_KAltFundInvcDtlF_out = df_enriched.filter(F.col("orig_ALT_FUND_INVC_DTL_SK").isNull()).select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("ALT_FUND_INVC_ID").alias("ALT_FUND_INVC_ID"),
    F.col("ALT_FUND_INVC_DTL_CD").alias("ALT_FUND_INVC_DTL_CD"),
    F.col("ALT_FUND_INVC_DTL_SEQ_NO").alias("ALT_FUND_INVC_DTL_SEQ_NO"),
    F.lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("ALT_FUND_INVC_DTL_SK").alias("ALT_FUND_INVC_DTL_SK"),
    F.lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK")
)

df_lnk_Pkey_Out = df_enriched.select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("ALT_FUND_INVC_ID").alias("ALT_FUND_INVC_ID"),
    F.col("ALT_FUND_INVC_DTL_CD").alias("ALT_FUND_INVC_DTL_CD"),
    F.col("ALT_FUND_INVC_DTL_SEQ_NO").alias("ALT_FUND_INVC_DTL_SEQ_NO"),
    F.col("ALT_FUND_INVC_DTL_SK").alias("ALT_FUND_INVC_DTL_SK")
)

temp_table_db2_K_ALT_FUND_INVC_DTL_F_Load = "STAGING.IdsEdwAltFundInvcDtlFPkey_db2_K_ALT_FUND_INVC_DTL_F_Load_temp"
execute_dml(f"DROP TABLE IF EXISTS {temp_table_db2_K_ALT_FUND_INVC_DTL_F_Load}", jdbc_url, jdbc_props)
df_lnk_KAltFundInvcDtlF_out.write.jdbc(
    url=jdbc_url,
    table=temp_table_db2_K_ALT_FUND_INVC_DTL_F_Load,
    mode="overwrite",
    properties=jdbc_props
)
merge_sql_db2_K_ALT_FUND_INVC_DTL_F_Load = f"""
MERGE INTO {EDWOwner}.K_ALT_FUND_INVC_DTL_F AS t
USING {temp_table_db2_K_ALT_FUND_INVC_DTL_F_Load} AS s
ON (
    t.SRC_SYS_CD = s.SRC_SYS_CD
    AND t.ALT_FUND_INVC_ID = s.ALT_FUND_INVC_ID
    AND t.ALT_FUND_INVC_DTL_CD = s.ALT_FUND_INVC_DTL_CD
    AND t.ALT_FUND_INVC_DTL_SEQ_NO = s.ALT_FUND_INVC_DTL_SEQ_NO
)
WHEN MATCHED THEN UPDATE SET
    t.CRT_RUN_CYC_EXCTN_DT_SK = s.CRT_RUN_CYC_EXCTN_DT_SK,
    t.ALT_FUND_INVC_DTL_SK = s.ALT_FUND_INVC_DTL_SK,
    t.CRT_RUN_CYC_EXCTN_SK = s.CRT_RUN_CYC_EXCTN_SK
WHEN NOT MATCHED THEN INSERT (
    SRC_SYS_CD,
    ALT_FUND_INVC_ID,
    ALT_FUND_INVC_DTL_CD,
    ALT_FUND_INVC_DTL_SEQ_NO,
    CRT_RUN_CYC_EXCTN_DT_SK,
    ALT_FUND_INVC_DTL_SK,
    CRT_RUN_CYC_EXCTN_SK
)
VALUES (
    s.SRC_SYS_CD,
    s.ALT_FUND_INVC_ID,
    s.ALT_FUND_INVC_DTL_CD,
    s.ALT_FUND_INVC_DTL_SEQ_NO,
    s.CRT_RUN_CYC_EXCTN_DT_SK,
    s.ALT_FUND_INVC_DTL_SK,
    s.CRT_RUN_CYC_EXCTN_SK
)
;
"""
execute_dml(merge_sql_db2_K_ALT_FUND_INVC_DTL_F_Load, jdbc_url, jdbc_props)

df_jn_Pkey = df_lnk_AltFundInvcDtlFFData_L_in.alias("l").join(
    df_lnk_Pkey_Out.alias("r"),
    (F.col("l.SRC_SYS_CD") == F.col("r.SRC_SYS_CD"))
    & (F.col("l.ALT_FUND_INVC_ID") == F.col("r.ALT_FUND_INVC_ID"))
    & (F.col("l.ALT_FUND_INVC_DTL_CD") == F.col("r.ALT_FUND_INVC_DTL_CD"))
    & (F.col("l.ALT_FUND_INVC_DTL_SEQ_NO") == F.col("r.ALT_FUND_INVC_DTL_SEQ_NO")),
    how="left"
)

df_lnk_IdsEdwAltFundInvcDtlFPkey_OutABC = df_jn_Pkey.select(
    F.col("r.ALT_FUND_INVC_DTL_SK").alias("ALT_FUND_INVC_DTL_SK"),
    F.col("l.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("l.ALT_FUND_INVC_ID").alias("ALT_FUND_INVC_ID"),
    F.col("l.ALT_FUND_INVC_DTL_CD").alias("ALT_FUND_INVC_DTL_CD"),
    F.col("l.ALT_FUND_INVC_DTL_SEQ_NO").alias("ALT_FUND_INVC_DTL_SEQ_NO"),
    F.col("l.CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("l.LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("l.ALT_FUND_SK").alias("ALT_FUND_SK"),
    F.col("l.ALT_FUND_INVC_SK").alias("ALT_FUND_INVC_SK"),
    F.col("l.ALT_FUND_INVC_DSCRTN_SK").alias("ALT_FUND_INVC_DSCRTN_SK"),
    F.col("l.ALT_FUND_INVC_PAYMT_SK").alias("ALT_FUND_INVC_PAYMT_SK"),
    F.col("l.BILL_ENTY_SK").alias("BILL_ENTY_SK"),
    F.col("l.BILL_RDUCTN_HIST_SK").alias("BILL_RDUCTN_HIST_SK"),
    F.col("l.CLM_SK").alias("CLM_SK"),
    F.col("l.CLS_SK").alias("CLS_SK"),
    F.col("l.CLS_PLN_SK").alias("CLS_PLN_SK"),
    F.col("l.GRP_SK").alias("GRP_SK"),
    F.col("l.SUBGRP_SK").alias("SUBGRP_SK"),
    F.col("l.SUB_SK").alias("SUB_SK"),
    F.col("l.MBR_SK").alias("MBR_SK"),
    F.col("l.PROD_SK").alias("PROD_SK"),
    F.col("l.ALT_FUND_INVC_DSCRTN_BEG_DT_SK").alias("ALT_FUND_INVC_DSCRTN_BEG_DT_SK"),
    F.col("l.ALT_FUND_INVC_DSCRTN_END_DT_SK").alias("ALT_FUND_INVC_DSCRTN_END_DT_SK"),
    F.col("l.ALT_FUND_INVC_FUND_THRU_DT_SK").alias("ALT_FUND_INVC_FUND_THRU_DT_SK"),
    F.col("l.ALT_FUND_INVC_FUND_FROM_DT_SK").alias("ALT_FUND_INVC_FUND_FROM_DT_SK"),
    F.col("l.CLM_PD_DT_SK").alias("CLM_PD_DT_SK"),
    F.col("l.CLM_PD_YR_MO_SK").alias("CLM_PD_YR_MO_SK"),
    F.col("l.CLM_SVC_STRT_DT_SK").alias("CLM_SVC_STRT_DT_SK"),
    F.col("l.CLM_SVC_STRT_YR_MO_SK").alias("CLM_SVC_STRT_YR_MO_SK"),
    F.col("l.ALT_FUND_INVC_DTL_BILL_AMT").alias("ALT_FUND_INVC_DTL_BILL_AMT"),
    F.col("l.ALT_FUND_CNTR_PERD_NO").alias("ALT_FUND_CNTR_PERD_NO"),
    F.col("l.ALT_FUND_INVC_DSCRTN_MO_QTY").alias("ALT_FUND_INVC_DSCRTN_MO_QTY"),
    F.col("l.ALT_FUND_INVC_DSCRTN_SEQ_NO").alias("ALT_FUND_INVC_DSCRTN_SEQ_NO"),
    F.col("l.ALT_FUND_INVC_PAYMT_SEQ_NO").alias("ALT_FUND_INVC_PAYMT_SEQ_NO"),
    F.col("l.BILL_RDUCTN_HIST_SEQ_NO").alias("BILL_RDUCTN_HIST_SEQ_NO"),
    F.col("l.BILL_ENTY_UNIQ_KEY").alias("BILL_ENTY_UNIQ_KEY"),
    F.col("l.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("l.SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    F.col("l.ALT_FUND_ID").alias("ALT_FUND_ID"),
    F.col("l.ALT_FUND_INVC_DTL_NM").alias("ALT_FUND_INVC_DTL_NM"),
    F.col("l.ALT_FUND_INVC_DSCRTN_DESC").alias("ALT_FUND_INVC_DSCRTN_DESC"),
    F.col("l.AF_INVC_DSCRTN_PRSN_ID_TX").alias("AF_INVC_DSCRTN_PRSN_ID_TX"),
    F.col("l.ALT_FUND_INVC_DSCRTN_SH_DESC").alias("ALT_FUND_INVC_DSCRTN_SH_DESC"),
    F.col("l.ALT_FUND_NM").alias("ALT_FUND_NM"),
    F.col("l.CLM_ID").alias("CLM_ID"),
    F.col("l.CLS_ID").alias("CLS_ID"),
    F.col("l.CLS_PLN_ID").alias("CLS_PLN_ID"),
    F.col("l.GRP_ID").alias("GRP_ID"),
    F.col("l.GRP_NM").alias("GRP_NM"),
    F.col("l.PROD_ID").alias("PROD_ID"),
    F.col("l.SUBGRP_ID").alias("SUBGRP_ID"),
    F.col("l.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("l.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("l.ALT_FUND_INVC_DTL_CD_SK").alias("ALT_FUND_INVC_DTL_CD_SK")
)

df_final = df_lnk_IdsEdwAltFundInvcDtlFPkey_OutABC.select(
    F.rpad(F.col("ALT_FUND_INVC_DTL_SK").cast("string"), 0, "").alias("ALT_FUND_INVC_DTL_SK"),
    F.col("SRC_SYS_CD"),
    F.col("ALT_FUND_INVC_ID"),
    F.col("ALT_FUND_INVC_DTL_CD"),
    F.col("ALT_FUND_INVC_DTL_SEQ_NO"),
    F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("ALT_FUND_SK"),
    F.col("ALT_FUND_INVC_SK"),
    F.col("ALT_FUND_INVC_DSCRTN_SK"),
    F.col("ALT_FUND_INVC_PAYMT_SK"),
    F.col("BILL_ENTY_SK"),
    F.col("BILL_RDUCTN_HIST_SK"),
    F.col("CLM_SK"),
    F.col("CLS_SK"),
    F.col("CLS_PLN_SK"),
    F.col("GRP_SK"),
    F.col("SUBGRP_SK"),
    F.col("SUB_SK"),
    F.col("MBR_SK"),
    F.col("PROD_SK"),
    F.rpad(F.col("ALT_FUND_INVC_DSCRTN_BEG_DT_SK"), 10, " ").alias("ALT_FUND_INVC_DSCRTN_BEG_DT_SK"),
    F.rpad(F.col("ALT_FUND_INVC_DSCRTN_END_DT_SK"), 10, " ").alias("ALT_FUND_INVC_DSCRTN_END_DT_SK"),
    F.rpad(F.col("ALT_FUND_INVC_FUND_THRU_DT_SK"), 10, " ").alias("ALT_FUND_INVC_FUND_THRU_DT_SK"),
    F.rpad(F.col("ALT_FUND_INVC_FUND_FROM_DT_SK"), 10, " ").alias("ALT_FUND_INVC_FUND_FROM_DT_SK"),
    F.rpad(F.col("CLM_PD_DT_SK"), 10, " ").alias("CLM_PD_DT_SK"),
    F.rpad(F.col("CLM_PD_YR_MO_SK"), 6, " ").alias("CLM_PD_YR_MO_SK"),
    F.rpad(F.col("CLM_SVC_STRT_DT_SK"), 10, " ").alias("CLM_SVC_STRT_DT_SK"),
    F.rpad(F.col("CLM_SVC_STRT_YR_MO_SK"), 6, " ").alias("CLM_SVC_STRT_YR_MO_SK"),
    F.col("ALT_FUND_INVC_DTL_BILL_AMT"),
    F.col("ALT_FUND_CNTR_PERD_NO"),
    F.col("ALT_FUND_INVC_DSCRTN_MO_QTY"),
    F.col("ALT_FUND_INVC_DSCRTN_SEQ_NO"),
    F.col("ALT_FUND_INVC_PAYMT_SEQ_NO"),
    F.col("BILL_RDUCTN_HIST_SEQ_NO"),
    F.col("BILL_ENTY_UNIQ_KEY"),
    F.col("MBR_UNIQ_KEY"),
    F.col("SUB_UNIQ_KEY"),
    F.col("ALT_FUND_ID"),
    F.col("ALT_FUND_INVC_DTL_NM"),
    F.col("ALT_FUND_INVC_DSCRTN_DESC"),
    F.col("AF_INVC_DSCRTN_PRSN_ID_TX"),
    F.col("ALT_FUND_INVC_DSCRTN_SH_DESC"),
    F.col("ALT_FUND_NM"),
    F.col("CLM_ID"),
    F.col("CLS_ID"),
    F.col("CLS_PLN_ID"),
    F.col("GRP_ID"),
    F.col("GRP_NM"),
    F.col("PROD_ID"),
    F.col("SUBGRP_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("ALT_FUND_INVC_DTL_CD_SK")
)

write_files(
    df_final,
    f"{adls_path}/load/ALT_FUND_INVC_DTL_F.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)