# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        ---------------------------------------------------------------------------                             ------------------------------    ------------------------------       --------------------
# MAGIC Bhupinder Kaur         2013 -08-08      5114                             Load  DM MBRSH_DM_MBR_APL                                                     IntegrateWrhsDevl       Jag Yelavarthi            2013-10-24

# MAGIC Job Name: IdsDmMbrshDmMbrAplLoad
# MAGIC Load rejects are redirected into a Reject file
# MAGIC Update then Insert MBRSH_DM_MBR_APL Data.
# MAGIC Copy Stage for buffer
# MAGIC Read Load File created in the IdsDmMbrshDmGrpExtr Job
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.functions import col
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------

ClmMartOwner = get_widget_value('ClmMartOwner','')
clmmart_secret_name = get_widget_value('clmmart_secret_name','')
ClmMartArraySize = get_widget_value('ClmMartArraySize','')
ClmMartRecordCount = get_widget_value('ClmMartRecordCount','')

schema_seq_MBRSH_DM_MBR_APL_csv_load = StructType([
    StructField("APL_ID", StringType(), True),
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("MBR_UNIQ_KEY", IntegerType(), True),
    StructField("APL_CAT_CD", StringType(), True),
    StructField("APL_CAT_NM", StringType(), True),
    StructField("APL_CUR_DCSN_CD", StringType(), True),
    StructField("APL_CUR_DCSN_NM", StringType(), True),
    StructField("APL_CUR_STTUS_CD", StringType(), True),
    StructField("APL_CUR_STTUS_NM", StringType(), True),
    StructField("APL_INITN_METH_CD", StringType(), True),
    StructField("APL_INITN_METH_NM", StringType(), True),
    StructField("APL_SUBTYP_CD", StringType(), True),
    StructField("APL_SUBTYP_NM", StringType(), True),
    StructField("APL_TYP_CD", StringType(), True),
    StructField("APL_TYP_NM", StringType(), True),
    StructField("CUR_APL_LVL_CD", StringType(), True),
    StructField("CUR_APL_LVL_NM", StringType(), True),
    StructField("APL_CRT_DT", TimestampType(), True),
    StructField("APL_CUR_STTUS_DT", TimestampType(), True),
    StructField("APL_END_DT", TimestampType(), True),
    StructField("APL_INITN_DT", TimestampType(), True),
    StructField("APL_DESC", StringType(), True),
    StructField("LAST_UPDT_DT", TimestampType(), True),
    StructField("LAST_UPDT_USER_ID", StringType(), True),
    StructField("LAST_UPDT_RUN_CYC_NO", IntegerType(), True)
])

df_seq_MBRSH_DM_MBR_APL_csv_load = (
    spark.read.format("csv")
    .option("header", "false")
    .option("delimiter", ",")
    .option("quote", "^")
    .option("escape", "^")
    .option("nullValue", None)
    .schema(schema_seq_MBRSH_DM_MBR_APL_csv_load)
    .load(f"{adls_path}/load/MBRSH_DM_MBR_APL.dat")
)

df_cpy_forBuffer = df_seq_MBRSH_DM_MBR_APL_csv_load.select(
    "APL_ID",
    "SRC_SYS_CD",
    "MBR_UNIQ_KEY",
    "APL_CAT_CD",
    "APL_CAT_NM",
    "APL_CUR_DCSN_CD",
    "APL_CUR_DCSN_NM",
    "APL_CUR_STTUS_CD",
    "APL_CUR_STTUS_NM",
    "APL_INITN_METH_CD",
    "APL_INITN_METH_NM",
    "APL_SUBTYP_CD",
    "APL_SUBTYP_NM",
    "APL_TYP_CD",
    "APL_TYP_NM",
    "CUR_APL_LVL_CD",
    "CUR_APL_LVL_NM",
    "APL_CRT_DT",
    "APL_CUR_STTUS_DT",
    "APL_END_DT",
    "APL_INITN_DT",
    "APL_DESC",
    "LAST_UPDT_DT",
    "LAST_UPDT_USER_ID",
    "LAST_UPDT_RUN_CYC_NO"
)

df_ODBC_MBRSH_DM_MBR_APL_out = df_cpy_forBuffer.select(
    "APL_ID",
    "SRC_SYS_CD",
    "MBR_UNIQ_KEY",
    "APL_CAT_CD",
    "APL_CAT_NM",
    "APL_CUR_DCSN_CD",
    "APL_CUR_DCSN_NM",
    "APL_CUR_STTUS_CD",
    "APL_CUR_STTUS_NM",
    "APL_INITN_METH_CD",
    "APL_INITN_METH_NM",
    "APL_SUBTYP_CD",
    "APL_SUBTYP_NM",
    "APL_TYP_CD",
    "APL_TYP_NM",
    "CUR_APL_LVL_CD",
    "CUR_APL_LVL_NM",
    "APL_CRT_DT",
    "APL_CUR_STTUS_DT",
    "APL_END_DT",
    "APL_INITN_DT",
    "APL_DESC",
    "LAST_UPDT_DT",
    "LAST_UPDT_USER_ID",
    "LAST_UPDT_RUN_CYC_NO"
)

jdbc_url, jdbc_props = get_db_config(clmmart_secret_name)
execute_dml(
    f"DROP TABLE IF EXISTS STAGING.IdsDmMbrshDmMbrAplLoad_ODBC_MBRSH_DM_MBR_APL_out_temp",
    jdbc_url,
    jdbc_props
)

df_ODBC_MBRSH_DM_MBR_APL_out.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.IdsDmMbrshDmMbrAplLoad_ODBC_MBRSH_DM_MBR_APL_out_temp") \
    .mode("overwrite") \
    .save()

merge_sql = (
    f"MERGE INTO {ClmMartOwner}.MBRSH_DM_MBR_APL AS T "
    f"USING STAGING.IdsDmMbrshDmMbrAplLoad_ODBC_MBRSH_DM_MBR_APL_out_temp AS S "
    f"ON T.APL_ID = S.APL_ID AND T.SRC_SYS_CD = S.SRC_SYS_CD "
    f"WHEN MATCHED THEN UPDATE SET "
    f"T.MBR_UNIQ_KEY = S.MBR_UNIQ_KEY, "
    f"T.APL_CAT_CD = S.APL_CAT_CD, "
    f"T.APL_CAT_NM = S.APL_CAT_NM, "
    f"T.APL_CUR_DCSN_CD = S.APL_CUR_DCSN_CD, "
    f"T.APL_CUR_DCSN_NM = S.APL_CUR_DCSN_NM, "
    f"T.APL_CUR_STTUS_CD = S.APL_CUR_STTUS_CD, "
    f"T.APL_CUR_STTUS_NM = S.APL_CUR_STTUS_NM, "
    f"T.APL_INITN_METH_CD = S.APL_INITN_METH_CD, "
    f"T.APL_INITN_METH_NM = S.APL_INITN_METH_NM, "
    f"T.APL_SUBTYP_CD = S.APL_SUBTYP_CD, "
    f"T.APL_SUBTYP_NM = S.APL_SUBTYP_NM, "
    f"T.APL_TYP_CD = S.APL_TYP_CD, "
    f"T.APL_TYP_NM = S.APL_TYP_NM, "
    f"T.CUR_APL_LVL_CD = S.CUR_APL_LVL_CD, "
    f"T.CUR_APL_LVL_NM = S.CUR_APL_LVL_NM, "
    f"T.APL_CRT_DT = S.APL_CRT_DT, "
    f"T.APL_CUR_STTUS_DT = S.APL_CUR_STTUS_DT, "
    f"T.APL_END_DT = S.APL_END_DT, "
    f"T.APL_INITN_DT = S.APL_INITN_DT, "
    f"T.APL_DESC = S.APL_DESC, "
    f"T.LAST_UPDT_DT = S.LAST_UPDT_DT, "
    f"T.LAST_UPDT_USER_ID = S.LAST_UPDT_USER_ID, "
    f"T.LAST_UPDT_RUN_CYC_NO = S.LAST_UPDT_RUN_CYC_NO "
    f"WHEN NOT MATCHED THEN INSERT ("
    f"APL_ID, SRC_SYS_CD, MBR_UNIQ_KEY, APL_CAT_CD, APL_CAT_NM, APL_CUR_DCSN_CD, APL_CUR_DCSN_NM, APL_CUR_STTUS_CD, "
    f"APL_CUR_STTUS_NM, APL_INITN_METH_CD, APL_INITN_METH_NM, APL_SUBTYP_CD, APL_SUBTYP_NM, APL_TYP_CD, APL_TYP_NM, "
    f"CUR_APL_LVL_CD, CUR_APL_LVL_NM, APL_CRT_DT, APL_CUR_STTUS_DT, APL_END_DT, APL_INITN_DT, APL_DESC, LAST_UPDT_DT, "
    f"LAST_UPDT_USER_ID, LAST_UPDT_RUN_CYC_NO) VALUES ("
    f"S.APL_ID, S.SRC_SYS_CD, S.MBR_UNIQ_KEY, S.APL_CAT_CD, S.APL_CAT_NM, S.APL_CUR_DCSN_CD, S.APL_CUR_DCSN_NM, "
    f"S.APL_CUR_STTUS_CD, S.APL_CUR_STTUS_NM, S.APL_INITN_METH_CD, S.APL_INITN_METH_NM, S.APL_SUBTYP_CD, "
    f"S.APL_SUBTYP_NM, S.APL_TYP_CD, S.APL_TYP_NM, S.CUR_APL_LVL_CD, S.CUR_APL_LVL_NM, S.APL_CRT_DT, "
    f"S.APL_CUR_STTUS_DT, S.APL_END_DT, S.APL_INITN_DT, S.APL_DESC, S.LAST_UPDT_DT, S.LAST_UPDT_USER_ID, "
    f"S.LAST_UPDT_RUN_CYC_NO);"
)
execute_dml(merge_sql, jdbc_url, jdbc_props)

df_ODBC_MBRSH_DM_MBR_APL_out_reject = spark.createDataFrame(
    [],
    StructType([
        StructField("ERRORCODE", StringType(), True),
        StructField("ERRORTEXT", StringType(), True)
    ])
)

write_files(
    df_ODBC_MBRSH_DM_MBR_APL_out_reject.select("ERRORCODE", "ERRORTEXT"),
    f"{adls_path}/load/MBRSH_DM_MBR_APL_Rej.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)