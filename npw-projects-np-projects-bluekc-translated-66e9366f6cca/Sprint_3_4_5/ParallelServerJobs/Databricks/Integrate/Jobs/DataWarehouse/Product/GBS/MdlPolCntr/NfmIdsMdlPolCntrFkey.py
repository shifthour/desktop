# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC 
# MAGIC DinakarS                2018-07-02            5205                           Original Programming                                                                             IntegrateWrhsDevl   Kalyan Neelam             2018-07-05

# MAGIC Change Data Capture to capture delete records(records that exist in target and not in source)
# MAGIC This is a load ready file that will go into Load job
# MAGIC Job Name: IdsMdlPolCntrFkey_EE
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F, types as T
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------

IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
SrcSysCd = get_widget_value('SrcSysCd','')
RunID = get_widget_value('RunID','')
PrefixFkeyFailedFileName = get_widget_value('PrefixFkeyFailedFileName','')
RunIDTimeStamp = get_widget_value('RunIDTimeStamp','')
currdate = get_widget_value('currdate','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query_db2_MDL_DOC_Lkp = f"SELECT MDL_DOC_SK, MDL_DOC_ID, MDL_DOC_EFF_DT FROM {IDSOwner}.K_MDL_DOC"
df_db2_MDL_DOC_Lkp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_MDL_DOC_Lkp)
    .load()
)

schema_seq_MDL_POL_CNTR_PKEY = T.StructType([
    T.StructField("MDL_POL_CNTR_SK", T.IntegerType(), False),
    T.StructField("MDL_DOC_ID", T.StringType(), False),
    T.StructField("POL_NO", T.StringType(), False),
    T.StructField("CNTR_ID", T.StringType(), False),
    T.StructField("CNTR_EFF_DT", T.DateType(), False),
    T.StructField("SRC_SYS_CD", T.StringType(), False),
    T.StructField("CRT_RUN_CYC_EXCTN_SK", T.IntegerType(), False),
    T.StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", T.IntegerType(), False),
    T.StructField("CNTR_EFF_ON_GRP_RNWL_IN", T.StringType(), False),
    T.StructField("CNTRSTTUS_ID", T.StringType(), False),
    T.StructField("CNTRTERM_DT", T.DateType(), False),
    T.StructField("MDL_DOC_EFF_DT", T.DateType(), False)
])
df_seq_MDL_POL_CNTR_PKEY = (
    spark.read.format("csv")
    .option("header", "false")
    .option("sep", "|")
    .option("nullValue", None)
    .schema(schema_seq_MDL_POL_CNTR_PKEY)
    .load(f"{adls_path}/key/MDLPOLCNTR.{SrcSysCd}.pkey.{RunID}.dat")
)

df_lkp_join = df_seq_MDL_POL_CNTR_PKEY.alias("Lnk_IdsMdlPolCntrPkey_OutAbc").join(
    df_db2_MDL_DOC_Lkp.alias("Ref_MDL_DOC"),
    (
        (F.col("Lnk_IdsMdlPolCntrPkey_OutAbc.MDL_DOC_ID") == F.col("Ref_MDL_DOC.MDL_DOC_ID")) &
        (F.col("Lnk_IdsMdlPolCntrPkey_OutAbc.MDL_DOC_EFF_DT") == F.col("Ref_MDL_DOC.MDL_DOC_EFF_DT"))
    ),
    "left"
)
df_lkp_MDL_DOC_SK = df_lkp_join.select(
    F.col("Lnk_IdsMdlPolCntrPkey_OutAbc.MDL_POL_CNTR_SK").alias("MDL_POL_CNTR_SK"),
    F.col("Lnk_IdsMdlPolCntrPkey_OutAbc.MDL_DOC_ID").alias("MDL_DOC_ID"),
    F.col("Lnk_IdsMdlPolCntrPkey_OutAbc.POL_NO").alias("POL_NO"),
    F.col("Lnk_IdsMdlPolCntrPkey_OutAbc.CNTR_ID").alias("CNTR_ID"),
    F.col("Lnk_IdsMdlPolCntrPkey_OutAbc.CNTR_EFF_DT").alias("CNTR_EFF_DT"),
    F.col("Lnk_IdsMdlPolCntrPkey_OutAbc.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Lnk_IdsMdlPolCntrPkey_OutAbc.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("Lnk_IdsMdlPolCntrPkey_OutAbc.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("Lnk_IdsMdlPolCntrPkey_OutAbc.CNTR_EFF_ON_GRP_RNWL_IN").alias("CNTR_EFF_ON_GRP_RNWL_IN"),
    F.col("Lnk_IdsMdlPolCntrPkey_OutAbc.CNTRSTTUS_ID").alias("CNTR_STATUS_ID"),
    F.col("Lnk_IdsMdlPolCntrPkey_OutAbc.CNTRTERM_DT").alias("CNTR_TERM_DT"),
    F.col("Lnk_IdsMdlPolCntrPkey_OutAbc.MDL_DOC_EFF_DT").alias("MDL_DOC_EFF_DT"),
    F.col("Ref_MDL_DOC.MDL_DOC_SK").alias("MDL_DOC_SK")
)

df_txn_fkey0 = df_lkp_MDL_DOC_SK.withColumn(
    "SvMdlDocLkpCheck",
    F.when((F.col("MDL_DOC_SK").isNull()) | (F.col("MDL_DOC_SK") == 0), F.lit("Y")).otherwise(F.lit("N"))
)

df_main = df_txn_fkey0.filter(F.col("SvMdlDocLkpCheck") != "Y").select(
    F.col("MDL_POL_CNTR_SK").alias("MDL_POL_CNTR_SK"),
    F.col("MDL_DOC_ID").alias("MDL_DOC_ID"),
    F.col("POL_NO").alias("POL_NO"),
    F.col("CNTR_ID").alias("CNTR_ID"),
    F.col("CNTR_EFF_DT").alias("CNTR_EFF_DT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.coalesce(F.col("MDL_DOC_SK"), F.lit(0)).alias("MDL_DOC_SK"),
    F.col("CNTR_EFF_ON_GRP_RNWL_IN").alias("CNTR_EFF_ON_GRP_RNWL_IN"),
    F.col("CNTR_STATUS_ID").alias("CNTR_STTUS_ID"),
    F.col("CNTR_TERM_DT").alias("CNTR_TERM_DT")
)

df_fail_pre = df_txn_fkey0.filter(F.col("SvMdlDocLkpCheck") == "Y").select(
    F.col("MDL_POL_CNTR_SK").alias("MDL_POL_AMNDMNT_SK"),
    F.lit("MDL_DOC_ID;POLICY_NO;CNTR_ID;CNTR_EFF_DT;SRC_SYS_CD").alias("PRI_NAT_KEY_STRING"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD_SK"),
    F.lit("NfmIdsMdlPolCntrFkey").alias("JOB_NM"),
    F.lit("MDLDOCFKEY").alias("ERROR_TYP"),
    F.lit("MDLDOCFKEYFailure").alias("PHYSCL_FILE_NM"),
    F.concat(
        F.col("SRC_SYS_CD"), F.lit(";"),
        F.col("MDL_DOC_ID"), F.lit(";"),
        F.col("MDL_DOC_EFF_DT")
    ).alias("FRGN_NAT_KEY_STRING"),
    F.lit(RunIDTimeStamp).alias("FIRST_RECYC_TS"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
)
df_fail = df_fail_pre.select(
    F.col("MDL_POL_AMNDMNT_SK"),
    F.rpad("PRI_NAT_KEY_STRING", <...>, " ").alias("PRI_NAT_KEY_STRING"),
    F.rpad("SRC_SYS_CD_SK", <...>, " ").alias("SRC_SYS_CD_SK"),
    F.rpad("JOB_NM", <...>, " ").alias("JOB_NM"),
    F.rpad("ERROR_TYP", <...>, " ").alias("ERROR_TYP"),
    F.rpad("PHYSCL_FILE_NM", <...>, " ").alias("PHYSCL_FILE_NM"),
    F.rpad("FRGN_NAT_KEY_STRING", <...>, " ").alias("FRGN_NAT_KEY_STRING"),
    F.col("FIRST_RECYC_TS"),
    F.col("JOB_EXCTN_SK")
)

write_files(
    df_fail,
    f"{adls_path}/fkey_failures/{PrefixFkeyFailedFileName}.NfmIdsMdlPolCntrFkey.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)

extract_query_db2_MDL_POL_CNTR_Lkp = f"SELECT MDL_POL_CNTR_SK, MDL_DOC_ID, POL_NO, CNTR_ID, CNTR_EFF_DT, SRC_SYS_CD, CRT_RUN_CYC_EXCTN_SK, LAST_UPDT_RUN_CYC_EXCTN_SK, MDL_DOC_SK, CNTR_EFF_ON_GRP_RNWL_IN, CNTR_STTUS_ID, CNTR_TERM_DT FROM {IDSOwner}.MDL_POL_CNTR"
df_db2_MDL_POL_CNTR_Lkp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_MDL_POL_CNTR_Lkp)
    .load()
)

df_ref = df_db2_MDL_POL_CNTR_Lkp.alias("Ref")
df_new = df_main.alias("New")
join_expr = F.col("Ref.MDL_POL_CNTR_SK") == F.col("New.MDL_POL_CNTR_SK")
df_cdc_full = df_ref.join(df_new, join_expr, "fullouter")

compare_cols = [
    "MDL_DOC_ID","POL_NO","CNTR_ID","CNTR_EFF_DT","SRC_SYS_CD",
    "CRT_RUN_CYC_EXCTN_SK","LAST_UPDT_RUN_CYC_EXCTN_SK","MDL_DOC_SK",
    "CNTR_EFF_ON_GRP_RNWL_IN","CNTR_STTUS_ID","CNTR_TERM_DT"
]
cond_insert = (F.col("Ref.MDL_POL_CNTR_SK").isNull() & F.col("New.MDL_POL_CNTR_SK").isNotNull())
cond_delete = (F.col("Ref.MDL_POL_CNTR_SK").isNotNull() & F.col("New.MDL_POL_CNTR_SK").isNull())
comparisons = []
for c in compare_cols:
    comparisons.append(
        (F.col(f"Ref.{c}").isNull() != F.col(f"New.{c}").isNull()) |
        (F.col(f"Ref.{c}") != F.col(f"New.{c}"))
    )
cond_edit = ~cond_insert & ~cond_delete & (comparisons[0] | F.lit(False))
for i in range(1, len(comparisons)):
    cond_edit = cond_edit | comparisons[i]
change_code_col = F.when(cond_insert, F.lit(1)) \
    .when(cond_delete, F.lit(3)) \
    .when(cond_edit, F.lit(2)) \
    .otherwise(F.lit(0))

df_cdc_with_code = df_cdc_full.withColumn("change_code", change_code_col)
df_CDC_MDL_POL_CNTR_pre = df_cdc_with_code.filter(F.col("change_code") != 0)

df_CDC_MDL_POL_CNTR = df_CDC_MDL_POL_CNTR_pre.select(
    F.col("New.MDL_POL_CNTR_SK").alias("MDL_POL_CNTR_SK"),
    F.col("New.MDL_DOC_ID").alias("MDL_DOC_ID"),
    F.col("New.POL_NO").alias("POL_NO"),
    F.col("New.CNTR_ID").alias("CNTR_ID"),
    F.col("New.CNTR_EFF_DT").alias("CNTR_EFF_DT"),
    F.col("New.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("New.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("New.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("New.MDL_DOC_SK").alias("MDL_DOC_SK"),
    F.col("New.CNTR_EFF_ON_GRP_RNWL_IN").alias("CNTR_EFF_ON_GRP_RNWL_IN"),
    F.col("New.CNTR_STTUS_ID").alias("CNTR_STTUS_ID"),
    F.col("New.CNTR_TERM_DT").alias("CNTR_TERM_DT"),
    F.col("change_code").alias("change_code")
)

df_txn_mdl_pol_cntr_0 = df_CDC_MDL_POL_CNTR.select(
    F.col("MDL_POL_CNTR_SK"),
    F.col("MDL_DOC_ID"),
    F.col("POL_NO"),
    F.col("CNTR_ID"),
    F.col("CNTR_EFF_DT"),
    F.col("SRC_SYS_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.when(F.col("change_code") == 2, F.lit(RunID).cast(T.IntegerType()))
     .otherwise(F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"))
     .alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("MDL_DOC_SK"),
    F.col("CNTR_EFF_ON_GRP_RNWL_IN"),
    F.when(F.col("change_code") == 2, F.lit(0))
     .otherwise(F.col("CNTR_STTUS_ID"))
     .alias("CNTR_STTUS_ID"),
    F.when(F.col("change_code") == 2,
           F.lit(dateFromDaysSince(F.lit(-1), stringToDate(F.lit(currdate), F.lit("%yyyy-%mm-%dd"))))
          ).otherwise(
           F.lit(stringToDate(F.lit("2199-12-31"), F.lit("%yyyy-%mm-%dd")))
          ).alias("CNTR_TERM_DT")
)

df_out_mdl_pol_cntr = df_txn_mdl_pol_cntr_0

df_out_mdlpolcntr_unk = df_txn_mdl_pol_cntr_0.limit(1).select(
    F.lit(0).alias("MDL_POL_CNTR_SK"),
    F.lit("UNK").alias("MDL_DOC_ID"),
    F.lit("UNK").alias("POL_NO"),
    F.lit("UNK").alias("CNTR_ID"),
    F.lit("1753-01-01").alias("CNTR_EFF_DT"),
    F.col("SRC_SYS_CD"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("MDL_DOC_SK"),
    F.lit("N").alias("CNTR_EFF_ON_GRP_RNWL_IN"),
    F.lit(0).alias("CNTR_STTUS_ID"),
    F.lit("1753-01-01").alias("CNTR_TERM_DT")
)

df_out_mdlpolcntr_na = df_txn_mdl_pol_cntr_0.limit(1).select(
    F.lit(1).alias("MDL_POL_CNTR_SK"),
    F.lit("NA").alias("MDL_DOC_ID"),
    F.lit("NA").alias("POL_NO"),
    F.lit("NA").alias("CNTR_ID"),
    F.lit("1753-01-01").alias("CNTR_EFF_DT"),
    F.col("SRC_SYS_CD"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("MDL_DOC_SK"),
    F.lit("N").alias("CNTR_EFF_ON_GRP_RNWL_IN"),
    F.lit(1).alias("CNTR_STTUS_ID"),
    F.lit("1753-01-01").alias("CNTR_TERM_DT")
)

df_fn_mdlpolcntr_main_unk_na_pre = df_out_mdl_pol_cntr.unionByName(df_out_mdlpolcntr_unk).unionByName(df_out_mdlpolcntr_na)

df_fn_mdlpolcntr_main_unk_na = df_fn_mdlpolcntr_main_unk_na_pre.select(
    F.col("MDL_POL_CNTR_SK"),
    F.rpad("MDL_DOC_ID", <...>, " ").alias("MDL_DOC_ID"),
    F.rpad("POL_NO", <...>, " ").alias("POL_NO"),
    F.rpad("CNTR_ID", <...>, " ").alias("CNTR_ID"),
    F.col("CNTR_EFF_DT"),
    F.rpad("SRC_SYS_CD", <...>, " ").alias("SRC_SYS_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("MDL_DOC_SK"),
    F.rpad("CNTR_EFF_ON_GRP_RNWL_IN", <...>, " ").alias("CNTR_EFF_ON_GRP_RNWL_IN"),
    F.rpad("CNTR_STTUS_ID", <...>, " ").alias("CNTR_STTUS_ID"),
    F.col("CNTR_TERM_DT")
)

write_files(
    df_fn_mdlpolcntr_main_unk_na,
    f"{adls_path}/load/MDLPOLCNTR.{SrcSysCd}.Fkey.{RunID}.dat",
    delimiter="|",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)