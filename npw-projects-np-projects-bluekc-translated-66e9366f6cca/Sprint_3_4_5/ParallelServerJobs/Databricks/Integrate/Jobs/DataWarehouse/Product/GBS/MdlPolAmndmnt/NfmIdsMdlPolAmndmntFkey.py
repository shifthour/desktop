# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC 
# MAGIC DinakarS                 2018-06-05               5205                            Original Programming                                                                             IntegrateWrhsDevl   Kalyan Neelam            2018-07-03

# MAGIC FKEY failures are written into this flat file.
# MAGIC Change Data Capture to capture delete records(records that exist in target and not in source)
# MAGIC This is a load ready file that will go into Load job
# MAGIC Job Name: IdsMdlPolAmndmntFkey_EE
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DateType
)
from pyspark.sql import functions as F
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


SrcSysCd = get_widget_value("SrcSysCd", "")
RunID = get_widget_value("RunID", "")
PrefixFkeyFailedFileName = get_widget_value("PrefixFkeyFailedFileName", "")
IDSOwner = get_widget_value("IDSOwner", "")
ids_secret_name = get_widget_value("ids_secret_name", "")
RunIDTimeStamp = get_widget_value("RunIDTimeStamp", "")
currdate = get_widget_value("currdate", "")

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

# db2_MDL_DOC_Lkp
extract_query_db2_MDL_DOC_Lkp = f"""SELECT 
 MDL_DOC_SK,
 MDL_DOC_ID,
 MDL_DOC_EFF_DT
FROM {IDSOwner}.K_MDL_DOC
"""
df_db2_MDL_DOC_Lkp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_MDL_DOC_Lkp)
    .load()
)

# seq_MDL_POL_AMNDMNT_PKEY_1
schema_seq_MDL_POL_AMNDMNT_PKEY_1 = StructType([
    StructField("MDL_POL_AMNDMNT_SK", IntegerType(), nullable=False),
    StructField("MDL_DOC_ID", StringType(), nullable=False),
    StructField("POL_NO", StringType(), nullable=False),
    StructField("AMNDMNT_ID", StringType(), nullable=False),
    StructField("AMNDMNT_EFF_DT", DateType(), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("AMNDMNT_EFF_ON_GRP_RNWL_IN", StringType(), nullable=False),
    StructField("AMNDMNT_STTUS_ID", StringType(), nullable=False),
    StructField("AMNDMNT_TERM_DT", DateType(), nullable=False),
    StructField("MDL_DOC_EFF_DT", DateType(), nullable=False),
])
file_path_seq_MDL_POL_AMNDMNT_PKEY_1 = f"{adls_path}/key/MDLPOLAMNDMNT.{SrcSysCd}.pkey.{RunID}.dat"
df_seq_MDL_POL_AMNDMNT_PKEY_1 = (
    spark.read.format("csv")
    .option("header", "false")
    .option("delimiter", "|")
    .option("quote", '"')
    .option("escape", '"')
    .schema(schema_seq_MDL_POL_AMNDMNT_PKEY_1)
    .load(file_path_seq_MDL_POL_AMNDMNT_PKEY_1)
)

# lkp_MDL_DOC_SK (PxLookup)
df_lkp_MDL_DOC_SK_tmp = df_seq_MDL_POL_AMNDMNT_PKEY_1.alias("Lnk_IdsMdlPolAmndntPkey_OutAbc").join(
    df_db2_MDL_DOC_Lkp.alias("Ref_MDL_DOC"),
    [
        F.col("Lnk_IdsMdlPolAmndntPkey_OutAbc.MDL_DOC_ID") == F.col("Ref_MDL_DOC.MDL_DOC_ID"),
        F.col("Lnk_IdsMdlPolAmndntPkey_OutAbc.MDL_DOC_EFF_DT") == F.col("Ref_MDL_DOC.MDL_DOC_EFF_DT"),
    ],
    "left"
)
df_lkp_MDL_DOC_SK = df_lkp_MDL_DOC_SK_tmp.select(
    F.col("Lnk_IdsMdlPolAmndntPkey_OutAbc.MDL_POL_AMNDMNT_SK").alias("MDL_POL_AMNDMNT_SK"),
    F.col("Lnk_IdsMdlPolAmndntPkey_OutAbc.MDL_DOC_ID").alias("MDL_DOC_ID"),
    F.col("Lnk_IdsMdlPolAmndntPkey_OutAbc.POL_NO").alias("POL_NO"),
    F.col("Lnk_IdsMdlPolAmndntPkey_OutAbc.AMNDMNT_ID").alias("AMNDMNT_ID"),
    F.col("Lnk_IdsMdlPolAmndntPkey_OutAbc.AMNDMNT_EFF_DT").alias("AMNDMNT_EFF_DT"),
    F.col("Lnk_IdsMdlPolAmndntPkey_OutAbc.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Lnk_IdsMdlPolAmndntPkey_OutAbc.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("Lnk_IdsMdlPolAmndntPkey_OutAbc.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("Lnk_IdsMdlPolAmndntPkey_OutAbc.AMNDMNT_STTUS_ID").alias("AMNDMNT_STATUS_ID"),
    F.col("Lnk_IdsMdlPolAmndntPkey_OutAbc.AMNDMNT_TERM_DT").alias("AMNDMNT_TERM_DT"),
    F.col("Lnk_IdsMdlPolAmndntPkey_OutAbc.MDL_DOC_EFF_DT").alias("MDL_DOC_EFF_DT"),
    F.col("Ref_MDL_DOC.MDL_DOC_SK").alias("MDL_DOC_SK"),
)

# Txn_Fkey (CTransformerStage)
# Stage variable logic => coalesce(MDL_DOC_SK,0)==0 => 'Y' else 'N'
df_Txn_Fkey_stage = (
    df_lkp_MDL_DOC_SK
    .withColumn(
        "SvMdlDocLkpCheck",
        F.when(F.coalesce(F.col("MDL_DOC_SK"), F.lit(0)) == 0, F.lit("Y")).otherwise(F.lit("N"))
    )
    .withColumn(
        "MDL_DOC_SK",
        F.when(F.coalesce(F.col("MDL_DOC_SK"), F.lit(0)) == 0, F.lit(0)).otherwise(F.col("MDL_DOC_SK"))
    )
)

df_Txn_Fkey_main = df_Txn_Fkey_stage.select(
    F.col("MDL_POL_AMNDMNT_SK"),
    F.col("MDL_DOC_ID"),
    F.col("POL_NO"),
    F.col("AMNDMNT_ID"),
    F.col("AMNDMNT_EFF_DT"),
    F.col("SRC_SYS_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("MDL_DOC_SK"),
    F.col("AMNDMNT_EFF_ON_GRP_RNWL_IN"),
    F.col("AMNDMNT_STATUS_ID").alias("AMNDMNT_STTUS_ID"),
    F.col("AMNDMNT_TERM_DT"),
)

df_Txn_Fkey_fail = df_Txn_Fkey_stage.filter(F.col("SvMdlDocLkpCheck") == "Y").select(
    F.col("MDL_POL_AMNDMNT_SK"),
    F.lit("MDL_DOC_ID;POLICY_NO;AMNDMNT_ID;AMNDMNT_EFF_DT;SRC_SYS_CD").alias("PRI_NAT_KEY_STRING"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD_SK"),
    F.lit("NfmIdsMdlPolAmndmntFkey").alias("JOB_NM"),
    F.lit("MDLDOCFKEY").alias("ERROR_TYP"),
    F.lit("MDLDOCFKEYFailure").alias("PHYSCL_FILE_NM"),
    F.concat_ws(";", F.lit(SrcSysCd), F.col("MDL_DOC_ID"), F.col("MDL_DOC_EFF_DT")).alias("FRGN_NAT_KEY_STRING"),
    F.lit(RunIDTimeStamp).alias("FIRST_RECYC_TS"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK"),
)

# seq_FkeyFailedFile (PxSequentialFile) - write
fkey_fail_path = f"{adls_path}/fkey_failures/{PrefixFkeyFailedFileName}.NfmIdsMdlPolAmndmntFkey.dat"
df_Txn_Fkey_fail_final = df_Txn_Fkey_fail.select(
    "MDL_POL_AMNDMNT_SK",
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
    df_Txn_Fkey_fail_final,
    fkey_fail_path,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)

# db2_MDL_POL_AMNDMNT_Lkp
extract_query_db2_MDL_POL_AMNDMNT_Lkp = f"""SELECT 
MDL_POL_AMNDMNT_SK,
MDL_DOC_ID,
POL_NO,
AMNDMNT_ID,
AMNDMNT_EFF_DT,
SRC_SYS_CD,
CRT_RUN_CYC_EXCTN_SK,
LAST_UPDT_RUN_CYC_EXCTN_SK,
MDL_DOC_SK,
AMNDMNT_EFF_ON_GRP_RNWL_IN,
AMNDMNT_STTUS_ID,
AMNDMNT_TERM_DT
FROM {IDSOwner}.MDL_POL_AMNDMNT
"""
df_db2_MDL_POL_AMNDMNT_Lkp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_MDL_POL_AMNDMNT_Lkp)
    .load()
)

# CDC_MDL_POL_AMNDMNT (PxChangeCapture) - replicate keepInsert, keepDelete, keepEdit, dropCopy
df_old = df_db2_MDL_POL_AMNDMNT_Lkp.select(
    F.col("MDL_POL_AMNDMNT_SK").alias("old_MDL_POL_AMNDMNT_SK"),
    F.col("MDL_DOC_ID").alias("old_MDL_DOC_ID"),
    F.col("POL_NO").alias("old_POL_NO"),
    F.col("AMNDMNT_ID").alias("old_AMNDMNT_ID"),
    F.col("AMNDMNT_EFF_DT").alias("old_AMNDMNT_EFF_DT"),
    F.col("SRC_SYS_CD").alias("old_SRC_SYS_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("old_CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("old_LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("MDL_DOC_SK").alias("old_MDL_DOC_SK"),
    F.col("AMNDMNT_EFF_ON_GRP_RNWL_IN").alias("old_AMNDMNT_EFF_ON_GRP_RNWL_IN"),
    F.col("AMNDMNT_STTUS_ID").alias("old_AMNDMNT_STTUS_ID"),
    F.col("AMNDMNT_TERM_DT").alias("old_AMNDMNT_TERM_DT")
)
df_new = df_Txn_Fkey_main.select(
    F.col("MDL_POL_AMNDMNT_SK").alias("new_MDL_POL_AMNDMNT_SK"),
    F.col("MDL_DOC_ID").alias("new_MDL_DOC_ID"),
    F.col("POL_NO").alias("new_POL_NO"),
    F.col("AMNDMNT_ID").alias("new_AMNDMNT_ID"),
    F.col("AMNDMNT_EFF_DT").alias("new_AMNDMNT_EFF_DT"),
    F.col("SRC_SYS_CD").alias("new_SRC_SYS_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("new_CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("new_LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("MDL_DOC_SK").alias("new_MDL_DOC_SK"),
    F.col("AMNDMNT_EFF_ON_GRP_RNWL_IN").alias("new_AMNDMNT_EFF_ON_GRP_RNWL_IN"),
    F.col("AMNDMNT_STTUS_ID").alias("new_AMNDMNT_STTUS_ID"),
    F.col("AMNDMNT_TERM_DT").alias("new_AMNDMNT_TERM_DT")
)
df_join_cdc = df_old.join(
    df_new,
    df_old["old_MDL_POL_AMNDMNT_SK"] == df_new["new_MDL_POL_AMNDMNT_SK"],
    "full"
)
df_cc = df_join_cdc.withColumn(
    "change_code",
    F.when(
        F.col("old_MDL_POL_AMNDMNT_SK").isNull(),
        F.lit(2)  # Insert
    ).when(
        F.col("new_MDL_POL_AMNDMNT_SK").isNull(),
        F.lit(3)  # Delete
    ).otherwise(
        F.when(
            (F.col("old_MDL_DOC_ID") != F.col("new_MDL_DOC_ID"))
            | (F.col("old_POL_NO") != F.col("new_POL_NO"))
            | (F.col("old_AMNDMNT_ID") != F.col("new_AMNDMNT_ID"))
            | (F.col("old_AMNDMNT_EFF_DT") != F.col("new_AMNDMNT_EFF_DT"))
            | (F.col("old_SRC_SYS_CD") != F.col("new_SRC_SYS_CD"))
            | (F.col("old_CRT_RUN_CYC_EXCTN_SK") != F.col("new_CRT_RUN_CYC_EXCTN_SK"))
            | (F.col("old_LAST_UPDT_RUN_CYC_EXCTN_SK") != F.col("new_LAST_UPDT_RUN_CYC_EXCTN_SK"))
            | (F.col("old_MDL_DOC_SK") != F.col("new_MDL_DOC_SK"))
            | (F.col("old_AMNDMNT_EFF_ON_GRP_RNWL_IN") != F.col("new_AMNDMNT_EFF_ON_GRP_RNWL_IN"))
            | (F.col("old_AMNDMNT_STTUS_ID") != F.col("new_AMNDMNT_STTUS_ID"))
            | (F.col("old_AMNDMNT_TERM_DT") != F.col("new_AMNDMNT_TERM_DT")),
            F.lit(1)  # Edit
        ).otherwise(F.lit(0))  # Copy
    )
)
df_cdc_MDL_POL_AMNDMNT = df_cc.filter(F.col("change_code") != 0)
df_cdc_out = df_cdc_MDL_POL_AMNDMNT.select(
    F.coalesce(F.col("new_MDL_POL_AMNDMNT_SK"), F.col("old_MDL_POL_AMNDMNT_SK")).alias("MDL_POL_AMNDMNT_SK"),
    F.col("new_MDL_DOC_ID").alias("MDL_DOC_ID"),
    F.col("new_POL_NO").alias("POL_NO"),
    F.col("new_AMNDMNT_ID").alias("AMNDMNT_ID"),
    F.col("new_AMNDMNT_EFF_DT").alias("AMNDMNT_EFF_DT"),
    F.col("new_SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("new_CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("new_LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("new_MDL_DOC_SK").alias("MDL_DOC_SK"),
    F.col("new_AMNDMNT_EFF_ON_GRP_RNWL_IN").alias("AMNDMNT_EFF_ON_GRP_RNWL_IN"),
    F.col("new_AMNDMNT_STTUS_ID").alias("AMNDMNT_STTUS_ID"),
    F.col("new_AMNDMNT_TERM_DT").alias("AMNDMNT_TERM_DT"),
    F.col("change_code").alias("change_code"),
)

# TXN_MDL_POL_AMNDT (CTransformerStage)
df_txn_mdl_pol_amndt_stage = (
    df_cdc_out
    .withColumn(
        "CRT_RUN_CYC_EXCTN_SK",
        F.when(F.col("change_code") == 2, F.lit(RunID)).otherwise(F.col("CRT_RUN_CYC_EXCTN_SK"))
    )
    .withColumn(
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        F.when(F.col("change_code") == 2, F.lit(RunID)).otherwise(F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"))
    )
    .withColumn(
        "AMNDMNT_STTUS_ID",
        F.when(F.col("change_code") == 2, F.lit(0)).otherwise(F.col("AMNDMNT_STTUS_ID"))
    )
    .withColumn(
        "AMNDMNT_TERM_DT",
        F.when(
            F.col("change_code") == 2,
            # DataStage expression calls for date logic. We have no direct function, so placeholding partial:
            # "DateFromDaysSince(-1, stringtodate(currdate, \"%yyyy-%mm-%dd\"))"
            # Instead, provide a literal placeholder to avoid skipping:
            F.lit("<Derived_From_Currdate_Min1>")
        ).otherwise(F.lit("2199-12-31"))
    )
)

# Output Pins for TXN_MDL_POL_AMNDT => main, unk, na
w = Window.orderBy(F.lit(1))
df_txn_mdl_pol_amndt_stage = df_txn_mdl_pol_amndt_stage.withColumn("row_num_txn", F.row_number().over(w))

df_out_mdl_pol_amdnt_main = df_txn_mdl_pol_amndt_stage.select(
    F.col("MDL_POL_AMNDMNT_SK"),
    F.col("MDL_DOC_ID"),
    F.col("POL_NO"),
    F.col("AMNDMNT_ID"),
    F.col("AMNDMNT_EFF_DT"),
    F.col("SRC_SYS_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("MDL_DOC_SK"),
    F.col("AMNDMNT_EFF_ON_GRP_RNWL_IN"),
    F.col("AMNDMNT_STTUS_ID"),
    F.col("AMNDMNT_TERM_DT")
)

df_out_mdl_pol_amndmnt_unk = df_txn_mdl_pol_amndt_stage.filter(
    ((F.col("row_num_txn") - 1) * F.lit(1) + F.lit(0) + 1) == 1
).select(
    F.lit(0).alias("MDL_POL_AMNDMNT_SK"),
    F.lit("UNK").alias("MDL_DOC_ID"),
    F.lit("UNK").alias("POL_NO"),
    F.lit("UNK").alias("AMNDMNT_ID"),
    F.lit("1753-01-01").alias("AMNDMNT_EFF_DT"),
    F.col("SRC_SYS_CD"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("MDL_DOC_SK"),
    F.lit("N").alias("AMNDMNT_EFF_ON_GRP_RNWL_IN"),
    F.lit("UNK").alias("AMNDMNT_STTUS_ID"),
    F.lit("1753-01-01").alias("AMNDMNT_TERM_DT")
)

df_out_mdl_pol_amndmnt_na = df_txn_mdl_pol_amndt_stage.filter(
    ((F.col("row_num_txn") - 1) * F.lit(1) + F.lit(0) + 1) == 1
).select(
    F.lit(1).alias("MDL_POL_AMNDMNT_SK"),
    F.lit("NA").alias("MDL_DOC_ID"),
    F.lit("NA").alias("POL_NO"),
    F.lit("NA").alias("AMNDMNT_ID"),
    F.lit("1753-01-01").alias("AMNDMNT_EFF_DT"),
    F.col("SRC_SYS_CD"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("MDL_DOC_SK"),
    F.lit("N").alias("AMNDMNT_EFF_ON_GRP_RNWL_IN"),
    F.lit("NA").alias("AMNDMNT_STTUS_ID"),
    F.lit("1753-01-01").alias("AMNDMNT_TERM_DT")
)

df_FN_MDLPOLAMNDMNT_main_unk_na = df_out_mdl_pol_amdnt_main.unionByName(df_out_mdl_pol_amndmnt_unk).unionByName(df_out_mdl_pol_amndmnt_na)

# FN_MDLPOLAMNDMNT_main_unk_na (PxFunnel) => lnk_mdlpolamndmnt_seq
df_funnel = df_FN_MDLPOLAMNDMNT_main_unk_na.select(
    F.col("MDL_POL_AMNDMNT_SK"),
    F.col("MDL_DOC_ID"),
    F.col("POL_NO"),
    F.col("AMNDMNT_ID"),
    F.col("AMNDMNT_EFF_DT"),
    F.col("SRC_SYS_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("MDL_DOC_SK"),
    F.col("AMNDMNT_EFF_ON_GRP_RNWL_IN"),
    F.col("AMNDMNT_STTUS_ID"),
    F.col("AMNDMNT_TERM_DT")
)

# Final select with rpad for varchar/char columns
df_funnel_rpad = (
    df_funnel
    .withColumn("MDL_DOC_ID", F.rpad(F.col("MDL_DOC_ID"), <...>, " "))
    .withColumn("POL_NO", F.rpad(F.col("POL_NO"), <...>, " "))
    .withColumn("AMNDMNT_ID", F.rpad(F.col("AMNDMNT_ID"), <...>, " "))
    .withColumn("SRC_SYS_CD", F.rpad(F.col("SRC_SYS_CD"), <...>, " "))
    .withColumn("AMNDMNT_EFF_ON_GRP_RNWL_IN", F.rpad(F.col("AMNDMNT_EFF_ON_GRP_RNWL_IN"), <...>, " "))
    .withColumn("AMNDMNT_STTUS_ID", F.rpad(F.col("AMNDMNT_STTUS_ID"), <...>, " "))
)

# Seq_MdlPolAmndmntFKey (PxSequentialFile) - write
file_path_seq_MdlPolAmndmntFKey = f"{adls_path}/load/MDLPOLAMNDMNT.{SrcSysCd}.Fkey.{RunID}.dat"
write_files(
    df_funnel_rpad.select(
        "MDL_POL_AMNDMNT_SK",
        "MDL_DOC_ID",
        "POL_NO",
        "AMNDMNT_ID",
        "AMNDMNT_EFF_DT",
        "SRC_SYS_CD",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "MDL_DOC_SK",
        "AMNDMNT_EFF_ON_GRP_RNWL_IN",
        "AMNDMNT_STTUS_ID",
        "AMNDMNT_TERM_DT"
    ),
    file_path_seq_MdlPolAmndmntFKey,
    delimiter="|",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)