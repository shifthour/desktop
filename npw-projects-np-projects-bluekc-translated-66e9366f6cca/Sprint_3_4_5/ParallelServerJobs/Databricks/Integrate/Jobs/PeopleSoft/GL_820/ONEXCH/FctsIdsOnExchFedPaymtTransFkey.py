# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Called By:FctsBcbsExtOnExchExtrLoadSeq
# MAGIC 
# MAGIC 
# MAGIC Process Description: Extract Data from (GL_ON_EXCH_FED_PYMT_DTL) and Load it to IDS 
# MAGIC 
# MAGIC Modifications:                        
# MAGIC                                                               Project/                                                                                                                        Code                  Date
# MAGIC Developer                    Date                   Altiris #     Change Description                                                                                     Reviewer            Reviewed
# MAGIC -----------------------              -------------------    -------------   -----------------------------------------------------------------------------------------------------------------   -------------------------  -------------------
# MAGIC sudheer champati        06/10/2014       5128       Originally Programmed                                                                                 Kalyan Neelam    2015-06-18

# MAGIC This job is to generate the Foreign Keys to the ON_EXCH_FED_PYMT_DTL table.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F, types as T
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
SrcSysCd = get_widget_value('SrcSysCd','')
RunID = get_widget_value('RunID','')
PrefixFkeyFailedFileName = get_widget_value('PrefixFkeyFailedFileName','')
DSJobName = "FctsIdsOnExchFedPaymtTransFkey"

schema_seq_ONEXCH_Pkey = (
    T.StructType()
    .add("PRI_NAT_KEY_STRING", T.StringType(), False)
    .add("FIRST_RECYC_TS", T.TimestampType(), False)
    .add("ON_EXCH_FED_PAYMT_TRANS_SK", T.IntegerType(), False)
    .add("ON_EXCH_FED_PAYMT_TRANS_CK", T.DecimalType(38,10), False)
    .add("ACCTG_DT_SK", T.StringType(), False)
    .add("SOURCE_SYSTEM_CODE_SK", T.IntegerType(), False)
    .add("CRT_RUN_CYC_EXCTN_SK", T.IntegerType(), False)
    .add("LAST_UPDT_RUN_CYC_EXCTN_SK", T.IntegerType(), False)
    .add("FNCL_LOB_SK", T.IntegerType(), False)
    .add("GRP_SK", T.IntegerType(), False)
    .add("PROD_SK", T.IntegerType(), False)
    .add("RCVD_INCM_SBSDY_DTL_PAYMT_TYP_CD_SK", T.IntegerType(), False)
    .add("COV_STRT_DT_SK", T.StringType(), False)
    .add("COV_END_DT_SK", T.StringType(), False)
    .add("PROD_LOB_NO", T.StringType(), False)
    .add("EXCH_MBR_ID", T.StringType(), False)
    .add("SUB_ID", T.StringType(), False)
    .add("GRGR_ID", T.StringType(), False)
    .add("PDPD_ID", T.StringType(), False)
    .add("SRC_SYS_CD", T.StringType(), False)
    .add("GRGR_NAME", T.StringType(), False)
)

df_seq_ONEXCH_Pkey = (
    spark.read
    .option("sep", ",")
    .option("quote", "^")
    .option("header", False)
    .option("nullValue", None)
    .schema(schema_seq_ONEXCH_Pkey)
    .csv(f"{adls_path}/key/ON_EXCH_FED_PYMT_TRANS.{SrcSysCd}.pkey.{RunID}.dat")
)

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
extract_query_db2_K_FNCL_LOB = (
    f"SELECT FL.FNCL_LOB_CD,FL.FNCL_LOB_SK,CM.CD_MPPNG_SK as FL_CD_MPPNG_SK "
    f"FROM {IDSOwner}.K_FNCL_LOB FL, {IDSOwner}.CD_MPPNG CM "
    f"WHERE FL.SRC_SYS_CD = 'PSI' "
    f"AND FL.SRC_SYS_CD = CM.SRC_CD "
    f"AND CM.TRGT_DOMAIN_NM = 'SOURCE SYSTEM'"
)
df_db2_K_FNCL_LOB = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_K_FNCL_LOB)
    .load()
)

extract_query_db2_K_GRP = f"SELECT GRP.GRP_SK, GRP.GRP_ID FROM {IDSOwner}.K_GRP GRP"
df_db2_K_GRP = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_K_GRP)
    .load()
)

extract_query_db2_K_PROD = (
    f"SELECT Trim(PROD_ID) AS PROD_ID, SRC_SYS_CD, PROD_SK "
    f"FROM {IDSOwner}.K_PROD"
)
df_db2_K_PROD = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_K_PROD)
    .load()
)

df_LkupFkey_joined = (
    df_seq_ONEXCH_Pkey.alias("lnk_IdsONEXCHFkey_EE_InAbc")
    .join(
        df_db2_K_FNCL_LOB.alias("FNCL_LOB"),
        F.col("lnk_IdsONEXCHFkey_EE_InAbc.PROD_LOB_NO") == F.col("FNCL_LOB.FNCL_LOB_CD"),
        "left"
    )
    .join(
        df_db2_K_GRP.alias("GRP"),
        F.col("lnk_IdsONEXCHFkey_EE_InAbc.GRGR_ID") == F.col("GRP.GRP_ID"),
        "left"
    )
    .join(
        df_db2_K_PROD.alias("PROD"),
        [
            F.col("lnk_IdsONEXCHFkey_EE_InAbc.PDPD_ID") == F.col("PROD.PROD_ID"),
            F.col("lnk_IdsONEXCHFkey_EE_InAbc.SRC_SYS_CD") == F.col("PROD.SRC_SYS_CD")
        ],
        "left"
    )
)

df_LkupFkey = df_LkupFkey_joined.select(
    F.col("lnk_IdsONEXCHFkey_EE_InAbc.PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("lnk_IdsONEXCHFkey_EE_InAbc.FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("lnk_IdsONEXCHFkey_EE_InAbc.ON_EXCH_FED_PAYMT_TRANS_SK").alias("ON_EXCH_FED_PAYMT_TRANS_SK"),
    F.col("lnk_IdsONEXCHFkey_EE_InAbc.ON_EXCH_FED_PAYMT_TRANS_CK").alias("ON_EXCH_FED_PAYMT_TRANS_CK"),
    F.col("lnk_IdsONEXCHFkey_EE_InAbc.ACCTG_DT_SK").alias("ACCTG_DT_SK"),
    F.col("lnk_IdsONEXCHFkey_EE_InAbc.SOURCE_SYSTEM_CODE_SK").alias("SOURCE_SYSTEM_CODE_SK"),
    F.col("lnk_IdsONEXCHFkey_EE_InAbc.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("lnk_IdsONEXCHFkey_EE_InAbc.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("FNCL_LOB.FNCL_LOB_SK").alias("FNCL_LOB_SK"),
    F.col("GRP.GRP_SK").alias("GRP_SK"),
    F.col("PROD.PROD_SK").alias("PROD_SK"),
    F.col("lnk_IdsONEXCHFkey_EE_InAbc.RCVD_INCM_SBSDY_DTL_PAYMT_TYP_CD_SK").alias("RCVD_INCM_SBSDY_DTL_PAYMT_TYP_CD_SK"),
    F.col("lnk_IdsONEXCHFkey_EE_InAbc.COV_STRT_DT_SK").alias("COV_STRT_DT_SK"),
    F.col("lnk_IdsONEXCHFkey_EE_InAbc.COV_END_DT_SK").alias("COV_END_DT_SK"),
    F.col("lnk_IdsONEXCHFkey_EE_InAbc.PROD_LOB_NO").alias("PROD_LOB_NO"),
    F.col("lnk_IdsONEXCHFkey_EE_InAbc.EXCH_MBR_ID").alias("EXCH_MBR_ID"),
    F.col("lnk_IdsONEXCHFkey_EE_InAbc.SUB_ID").alias("SUB_ID"),
    F.col("lnk_IdsONEXCHFkey_EE_InAbc.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("FNCL_LOB.FL_CD_MPPNG_SK").alias("FL_SRC_SYS_CD")
)

df_xfm_stagevars = (
    df_LkupFkey
    .withColumn(
        "SvFnclLobFKeyLkpCheck",
        F.when(F.isnull(F.col("FNCL_LOB_SK")) | (F.col("FNCL_LOB_SK") != F.lit("NA")), F.lit("Y")).otherwise(F.lit("N"))
    )
    .withColumn(
        "SvGrpFKeyLkpCheck",
        F.when(F.isnull(F.col("GRP_SK")) | (F.col("GRP_SK") != F.lit("NA")), F.lit("Y")).otherwise(F.lit("N"))
    )
    .withColumn(
        "SvProdFKeyLkpCheck",
        F.when(F.isnull(F.col("PROD_SK")) | (F.col("PROD_SK") != F.lit("NA")), F.lit("Y")).otherwise(F.lit("N"))
    )
    .withColumn(
        "SvProdFCdMppngLkpCheck",
        F.when(F.isnull(F.col("SOURCE_SYSTEM_CODE_SK")) | (F.col("SOURCE_SYSTEM_CODE_SK") != F.lit("NA")), F.lit("Y")).otherwise(F.lit("N"))
    )
)

w_xfm = Window.orderBy(F.monotonically_increasing_id())
df_xfm_stagevars_2 = df_xfm_stagevars.withColumn("_row_num", F.row_number().over(w_xfm))

df_xfm_Lnk_Main = df_xfm_stagevars_2.select(
    F.col("ON_EXCH_FED_PAYMT_TRANS_SK"),
    F.col("ON_EXCH_FED_PAYMT_TRANS_CK"),
    F.col("ACCTG_DT_SK"),
    F.col("SOURCE_SYSTEM_CODE_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("FNCL_LOB_SK"),
    F.col("GRP_SK"),
    F.col("PROD_SK"),
    F.col("RCVD_INCM_SBSDY_DTL_PAYMT_TYP_CD_SK"),
    F.col("COV_STRT_DT_SK"),
    F.col("COV_END_DT_SK"),
    F.col("PROD_LOB_NO"),
    F.col("EXCH_MBR_ID"),
    F.col("SUB_ID")
)

df_xfm_lnk_FnclLobTypCdLkup_Fail = df_xfm_stagevars_2.filter(
    F.col("SvFnclLobFKeyLkpCheck") == "Y"
).select(
    F.col("ON_EXCH_FED_PAYMT_TRANS_SK").alias("PRI_SK"),
    F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("FL_SRC_SYS_CD").alias("SRC_SYS_CD_SK"),
    F.lit("FctsIdsOnExchFedPaymtTransFkey").alias("JOB_NM"),
    F.lit("FKLOOKUP").alias("ERROR_TYP"),
    F.lit("K_FNCL_LOB").alias("PHYSCL_FILE_NM"),
    F.expr("SrcSysCd || ';' || 'FACETS DBO' || ';' || 'IDS' || ';' || 'SOURCE SYSTEM' || ';' || 'SOURCE SYSTEM' || ';' || SRC_SYS_CD").alias("FRGN_NAT_KEY_STRING"),
    F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
)

df_xfm_Lnk_UNK = df_xfm_stagevars_2.filter(
    F.col("_row_num") == 1
).select(
    F.lit(0).alias("ON_EXCH_FED_PAYMT_TRANS_SK"),
    F.lit(0).alias("ON_EXCH_FED_PAYMT_TRANS_CK"),
    F.lit("UNK").alias("ACCTG_DT_SK"),
    F.lit("UNK").alias("SOURCE_SYSTEM_CODE_SK"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("FNCL_LOB_SK"),
    F.lit(0).alias("GRP_SK"),
    F.lit(0).alias("PROD_SK"),
    F.lit(0).alias("RCVD_INCM_SBSDY_DTL_PAYMT_TYP_CD_SK"),
    F.lit("1753-01-01").alias("COV_STRT_DT_SK"),
    F.lit("2199-12-31").alias("COV_END_DT_SK"),
    F.lit("UNK").alias("PROD_LOB_NO"),
    F.lit("UNK").alias("EXCH_MBR_ID"),
    F.lit("UNK").alias("SUB_ID")
)

df_xfm_Lnk_NA = df_xfm_stagevars_2.filter(
    F.col("_row_num") == 1
).select(
    F.lit(1).alias("ON_EXCH_FED_PAYMT_TRANS_SK"),
    F.lit(1).alias("ON_EXCH_FED_PAYMT_TRANS_CK"),
    F.lit("NA").alias("ACCTG_DT_SK"),
    F.lit("NA").alias("SOURCE_SYSTEM_CODE_SK"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("FNCL_LOB_SK"),
    F.lit(1).alias("GRP_SK"),
    F.lit(1).alias("PROD_SK"),
    F.lit(1).alias("RCVD_INCM_SBSDY_DTL_PAYMT_TYP_CD_SK"),
    F.lit("1753-01-01").alias("COV_STRT_DT_SK"),
    F.lit("2199-12-31").alias("COV_END_DT_SK"),
    F.lit("NA").alias("PROD_LOB_NO"),
    F.lit("NA").alias("EXCH_MBR_ID"),
    F.lit("NA").alias("SUB_ID")
)

df_xfm_lnk_GrpTypCdLkup_Fail = df_xfm_stagevars_2.filter(
    F.col("GRP_SK") == "Y"
).select(
    F.col("ON_EXCH_FED_PAYMT_TRANS_SK").alias("PRI_SK"),
    F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("SOURCE_SYSTEM_CODE_SK").alias("SRC_SYS_CD_SK"),
    F.lit("FctsIdsOnExchFedPaymtTransFkey").alias("JOB_NM"),
    F.lit("CDLOOKUP").alias("ERROR_TYP"),
    F.lit("K_GRP").alias("PHYSCL_FILE_NM"),
    F.expr("SrcSysCd || ';' || 'FACETS DBO' || ';' || 'IDS' || ';' || 'SOURCE SYSTEM' || ';' || 'SOURCE SYSTEM' || ';' || SRC_SYS_CD").alias("FRGN_NAT_KEY_STRING"),
    F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
)

df_xfm_lnk_ProdTypCdLkup_Fail = df_xfm_stagevars_2.filter(
    F.col("SvProdFKeyLkpCheck") == "Y"
).select(
    F.col("ON_EXCH_FED_PAYMT_TRANS_SK").alias("PRI_SK"),
    F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("SOURCE_SYSTEM_CODE_SK").alias("SRC_SYS_CD_SK"),
    F.lit("FctsIdsOnExchFedPaymtTransFkey").alias("JOB_NM"),
    F.lit("CDLOOKUP").alias("ERROR_TYP"),
    F.lit("K_PROD_TYP_CD").alias("PHYSCL_FILE_NM"),
    F.expr("SrcSysCd || ';' || 'FACETS DBO' || ';' || 'IDS' || ';' || 'SOURCE SYSTEM' || ';' || 'SOURCE SYSTEM' || ';' || SRC_SYS_CD").alias("FRGN_NAT_KEY_STRING"),
    F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
)

df_fnl_NA_UNK_Streams_main = df_xfm_Lnk_Main.select(
    "ON_EXCH_FED_PAYMT_TRANS_SK",
    "ON_EXCH_FED_PAYMT_TRANS_CK",
    "ACCTG_DT_SK",
    "SOURCE_SYSTEM_CODE_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "FNCL_LOB_SK",
    "GRP_SK",
    "PROD_SK",
    "RCVD_INCM_SBSDY_DTL_PAYMT_TYP_CD_SK",
    "COV_STRT_DT_SK",
    "COV_END_DT_SK",
    "PROD_LOB_NO",
    "EXCH_MBR_ID",
    "SUB_ID"
)
df_fnl_NA_UNK_Streams_unk = df_xfm_Lnk_UNK.select(
    "ON_EXCH_FED_PAYMT_TRANS_SK",
    "ON_EXCH_FED_PAYMT_TRANS_CK",
    "ACCTG_DT_SK",
    "SOURCE_SYSTEM_CODE_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "FNCL_LOB_SK",
    "GRP_SK",
    "PROD_SK",
    "RCVD_INCM_SBSDY_DTL_PAYMT_TYP_CD_SK",
    "COV_STRT_DT_SK",
    "COV_END_DT_SK",
    "PROD_LOB_NO",
    "EXCH_MBR_ID",
    "SUB_ID"
)
df_fnl_NA_UNK_Streams_na = df_xfm_Lnk_NA.select(
    "ON_EXCH_FED_PAYMT_TRANS_SK",
    "ON_EXCH_FED_PAYMT_TRANS_CK",
    "ACCTG_DT_SK",
    "SOURCE_SYSTEM_CODE_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "FNcl_LOB_SK",
    "GRP_SK",
    "PROD_SK",
    "RCVD_INCM_SBSDY_DTL_PAYMT_TYP_CD_SK",
    "COV_STRT_DT_SK",
    "COV_END_DT_SK",
    "PROD_LOB_NO",
    "EXCH_MBR_ID",
    "SUB_ID"
)

df_fnl_NA_UNK_Streams_union = df_fnl_NA_UNK_Streams_main.union(df_fnl_NA_UNK_Streams_unk).union(df_fnl_NA_UNK_Streams_na)

df_fnl_NA_UNK_Streams = df_fnl_NA_UNK_Streams_union.select(
    "ON_EXCH_FED_PAYMT_TRANS_SK",
    "ON_EXCH_FED_PAYMT_TRANS_CK",
    "ACCTG_DT_SK",
    "SOURCE_SYSTEM_CODE_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "FNCL_LOB_SK",
    "GRP_SK",
    "PROD_SK",
    "RCVD_INCM_SBSDY_DTL_PAYMT_TYP_CD_SK",
    "COV_STRT_DT_SK",
    "COV_END_DT_SK",
    "PROD_LOB_NO",
    "EXCH_MBR_ID",
    "SUB_ID"
)

df_seq_ONEXCH_FKey = df_fnl_NA_UNK_Streams

df_seq_ONEXCH_FKey_final = (
    df_seq_ONEXCH_FKey
    .withColumn("ACCTG_DT_SK", F.rpad(F.col("ACCTG_DT_SK"), 10, " "))
    .withColumn("COV_STRT_DT_SK", F.rpad(F.col("COV_STRT_DT_SK"), 10, " "))
    .withColumn("COV_END_DT_SK", F.rpad(F.col("COV_END_DT_SK"), 10, " "))
    .withColumn("PROD_LOB_NO", F.rpad(F.col("PROD_LOB_NO"), 4, " "))
    .withColumn("EXCH_MBR_ID", F.rpad(F.col("EXCH_MBR_ID"), <...>, " "))
    .withColumn("SUB_ID", F.rpad(F.col("SUB_ID"), <...>, " "))
)

write_files(
    df_seq_ONEXCH_FKey_final.select(
        "ON_EXCH_FED_PAYMT_TRANS_SK",
        "ON_EXCH_FED_PAYMT_TRANS_CK",
        "ACCTG_DT_SK",
        "SOURCE_SYSTEM_CODE_SK",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "FNCL_LOB_SK",
        "GRP_SK",
        "PROD_SK",
        "RCVD_INCM_SBSDY_DTL_PAYMT_TYP_CD_SK",
        "COV_STRT_DT_SK",
        "COV_END_DT_SK",
        "PROD_LOB_NO",
        "EXCH_MBR_ID",
        "SUB_ID"
    ),
    f"{adls_path}/load/ON_EXCH_FED_PYMT_TRANS.{SrcSysCd}.{RunID}.dat",
    ",",
    "overwrite",
    False,
    False,
    "^",
    None
)

df_fnlFkeyFailures_Grp = df_xfm_lnk_GrpTypCdLkup_Fail.select(
    "PRI_SK",
    "PRI_NAT_KEY_STRING",
    "SRC_SYS_CD_SK",
    "JOB_NM",
    "ERROR_TYP",
    "PHYSCL_FILE_NM",
    "FRGN_NAT_KEY_STRING",
    "FIRST_RECYC_TS",
    "JOB_EXCTN_SK"
)
df_fnlFkeyFailures_Prod = df_xfm_lnk_ProdTypCdLkup_Fail.select(
    "PRI_SK",
    "PRI_NAT_KEY_STRING",
    "SRC_SYS_CD_SK",
    "JOB_NM",
    "ERROR_TYP",
    "PHYSCL_FILE_NM",
    "FRGN_NAT_KEY_STRING",
    "FIRST_RECYC_TS",
    "JOB_EXCTN_SK"
)
df_fnlFkeyFailures_FnclLob = df_xfm_lnk_FnclLobTypCdLkup_Fail.select(
    "PRI_SK",
    "PRI_NAT_KEY_STRING",
    "SRC_SYS_CD_SK",
    "JOB_NM",
    "ERROR_TYP",
    "PHYSCL_FILE_NM",
    "FRGN_NAT_KEY_STRING",
    "FIRST_RECYC_TS",
    "JOB_EXCTN_SK"
)

df_FnlFkeyFailures_union = df_fnlFkeyFailures_Grp.union(df_fnlFkeyFailures_Prod).union(df_fnlFkeyFailures_FnclLob)

df_FnlFkeyFailures = df_FnlFkeyFailures_union.select(
    "PRI_SK",
    "PRI_NAT_KEY_STRING",
    "SRC_SYS_CD_SK",
    "JOB_NM",
    "ERROR_TYP",
    "PHYSCL_FILE_NM",
    "FRGN_NAT_KEY_STRING",
    "FIRST_RECYC_TS",
    "JOB_EXCTN_SK"
)

df_seq_FkeyFailedFile_raw = df_FnlFkeyFailures.withColumn(
    "PRI_NAT_KEY_STRING", F.rpad(F.col("PRI_NAT_KEY_STRING"), <...>, " ")
).withColumn(
    "JOB_NM", F.rpad(F.col("JOB_NM"), <...>, " ")
).withColumn(
    "ERROR_TYP", F.rpad(F.col("ERROR_TYP"), <...>, " ")
).withColumn(
    "PHYSCL_FILE_NM", F.rpad(F.col("PHYSCL_FILE_NM"), <...>, " ")
).withColumn(
    "FRGN_NAT_KEY_STRING", F.rpad(F.col("FRGN_NAT_KEY_STRING"), <...>, " ")
)

df_seq_FkeyFailedFile = df_seq_FkeyFailedFile_raw.select(
    "PRI_SK",
    "PRI_NAT_KEY_STRING",
    "SRC_SYS_CD_SK",
    "JOB_NM",
    "ERROR_TYP",
    "PHYSCL_FILE_NM",
    "FRGN_NAT_KEY_STRING",
    "FIRST_RECYC_TS",
    "JOB_EXCTN_SK"
)

write_files(
    df_seq_FkeyFailedFile,
    f"{adls_path}/fkey_failures/{PrefixFkeyFailedFileName}.{DSJobName}.dat",
    ",",
    "overwrite",
    False,
    False,
    "^",
    None
)