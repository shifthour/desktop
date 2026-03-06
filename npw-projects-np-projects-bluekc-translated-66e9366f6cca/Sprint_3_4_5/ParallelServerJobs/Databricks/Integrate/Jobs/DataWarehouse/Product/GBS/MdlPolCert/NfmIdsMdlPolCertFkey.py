# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC 
# MAGIC Archana Palivela      2014-05-19            5345                             Original Programming                                                                             IntegrateWrhsDevl   Kalyan Neelam             2018-07-05

# MAGIC FKEY failures are written into this flat file.
# MAGIC Change Data Capture to capture delete records(records that exist in target and not in source)
# MAGIC This is a load ready file that will go into Load job
# MAGIC Job Name: IdsMdlPolCertFkey_EE
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


# Parameter Definitions
SrcSysCd = get_widget_value('SrcSysCd','')
RunID = get_widget_value('RunID','')
PrefixFkeyFailedFileName = get_widget_value('PrefixFkeyFailedFileName','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
RunIDTimeStamp = get_widget_value('RunIDTimeStamp','')
currdate = get_widget_value('currdate','')

# Read from db2_MDL_DOC_Lkp (DB2ConnectorPX)
jdbc_url_db2_MDL_DOC_Lkp, jdbc_props_db2_MDL_DOC_Lkp = get_db_config(ids_secret_name)
extract_query_db2_MDL_DOC_Lkp = f"SELECT DISTINCT MDL_DOC_SK, MDL_DOC_ID, MDL_DOC_EFF_DT FROM {IDSOwner}.K_MDL_DOC"
df_db2_MDL_DOC_Lkp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_MDL_DOC_Lkp)
    .options(**jdbc_props_db2_MDL_DOC_Lkp)
    .option("query", extract_query_db2_MDL_DOC_Lkp)
    .load()
)

# Read from seq_MDL_POL_CERT_PKEY (PxSequentialFile)
schema_seq_MDL_POL_CERT_PKEY = StructType([
    StructField("MDL_POL_CERT_SK", IntegerType(), nullable=False),
    StructField("MDL_DOC_ID", StringType(), nullable=False),
    StructField("POL_NO", StringType(), nullable=False),
    StructField("CERT_ID", StringType(), nullable=False),
    StructField("CERT_EFF_DT", DateType(), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("CERT_EFF_ON_GRP_RNWL_IN", StringType(), nullable=False),
    StructField("CERT_STTUS_ID", StringType(), nullable=False),
    StructField("CERT_TERM_DT", DateType(), nullable=False),
    StructField("MDL_DOC_EFF_DT", DateType(), nullable=False),
])
file_path_seq_MDL_POL_CERT_PKEY = f"{adls_path}/key/MDLPOLCERT.{SrcSysCd}.pkey.{RunID}.dat"
df_seq_MDL_POL_CERT_PKEY = (
    spark.read
    .option("header", "false")
    .option("delimiter", "|")
    .option("quote", None)
    .option("escape", None)
    .schema(schema_seq_MDL_POL_CERT_PKEY)
    .csv(file_path_seq_MDL_POL_CERT_PKEY)
)

# lkp_MDL_DOC_SK (PxLookup) - Left join db2_MDL_DOC_Lkp with seq_MDL_POL_CERT_PKEY
df_lkp_MDL_DOC_SK = (
    df_seq_MDL_POL_CERT_PKEY.alias("Lnk_IdsMdlPolCertPkey_OutAbc")
    .join(
        df_db2_MDL_DOC_Lkp.alias("Ref_MDL_DOC"),
        (
            (F.col("Lnk_IdsMdlPolCertPkey_OutAbc.MDL_DOC_ID") == F.col("Ref_MDL_DOC.MDL_DOC_ID"))
            & (F.col("Lnk_IdsMdlPolCertPkey_OutAbc.MDL_DOC_EFF_DT") == F.col("Ref_MDL_DOC.MDL_DOC_EFF_DT"))
        ),
        "left"
    )
    .select(
        F.col("Lnk_IdsMdlPolCertPkey_OutAbc.MDL_POL_CERT_SK").alias("MDL_POL_CERT_SK"),
        F.col("Lnk_IdsMdlPolCertPkey_OutAbc.MDL_DOC_ID").alias("MDL_DOC_ID"),
        F.col("Lnk_IdsMdlPolCertPkey_OutAbc.POL_NO").alias("POL_NO"),
        F.col("Lnk_IdsMdlPolCertPkey_OutAbc.CERT_ID").alias("CERT_ID"),
        F.col("Lnk_IdsMdlPolCertPkey_OutAbc.CERT_EFF_DT").alias("CERT_EFF_DT"),
        F.col("Lnk_IdsMdlPolCertPkey_OutAbc.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("Lnk_IdsMdlPolCertPkey_OutAbc.CERT_EFF_ON_GRP_RNWL_IN").alias("CERT_EFF_ON_GRP_RNWL_IN"),
        F.col("Lnk_IdsMdlPolCertPkey_OutAbc.CERT_STTUS_ID").alias("CERT_STATUS_ID"),
        F.col("Lnk_IdsMdlPolCertPkey_OutAbc.CERT_TERM_DT").alias("CERT_TERM_DT"),
        F.col("Lnk_IdsMdlPolCertPkey_OutAbc.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("Lnk_IdsMdlPolCertPkey_OutAbc.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("Ref_MDL_DOC.MDL_DOC_SK").alias("MDL_DOC_SK")
    )
)

# Txn_Fkey (CTransformerStage)
# Define stage variable SvMdlDocLkpCheck => 'Y' if MDL_DOC_SK is null or 0 else 'N'
df_txn_fkey = df_lkp_MDL_DOC_SK.withColumn(
    "SvMdlDocLkpCheck",
    F.when(F.col("MDL_DOC_SK").isNull() | (F.col("MDL_DOC_SK") == 0), F.lit("Y")).otherwise(F.lit("N"))
)

# Rows for Main output link => SvMdlDocLkpCheck != 'Y'
df_Lnk_MdlCertFkey_Main = df_txn_fkey.filter(F.col("SvMdlDocLkpCheck") != "Y").select(
    F.col("MDL_POL_CERT_SK").alias("MDL_POL_CERT_SK"),
    F.col("MDL_DOC_ID").alias("MDL_DOC_ID"),
    F.col("POL_NO").alias("POL_NO"),
    F.col("CERT_ID").alias("CERT_ID"),
    F.col("CERT_EFF_DT").alias("CERT_EFF_DT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("CERT_EFF_ON_GRP_RNWL_IN").alias("CERT_EFF_ON_GRP_RNWL_IN"),
    F.col("CERT_STATUS_ID").alias("CERT_STTUS_ID"),
    F.col("CERT_TERM_DT").alias("CERT_TERM_DT"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.when(F.col("MDL_DOC_SK").isNotNull(), F.col("MDL_DOC_SK")).otherwise(F.lit(0)).alias("MDL_DOC_SK")
)

# Rows for Fail output link => SvMdlDocLkpCheck = 'Y'
# Build each column as indicated in the job
df_Lnk_IdsMdCertFkeyFail_Out = df_txn_fkey.filter(F.col("SvMdlDocLkpCheck") == "Y").select(
    F.lit("MDL_DOC_ID;POLICY_NO;POLICY_FORM_ID;POLICY_FORM_DT").alias("PRI_NAT_KEY_STRING"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD_SK"),
    F.lit("NfmIdsMdlPolCertFkey").alias("JOB_NM"),
    F.lit("MDLDOCFKEY").alias("ERROR_TYP"),
    F.lit("MDLDOCFKEY").alias("PHYSCL_FILE_NM"),
    F.concat(F.col("SRC_SYS_CD"), F.lit(";"), F.col("MDL_DOC_ID"), F.lit(";"), F.lit("MDL_DOC_EFF_DT")).alias("FRGN_NAT_KEY_STRING"),
    F.lit(RunIDTimeStamp).alias("FIRST_RECYC_TS"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
)

# Write seq_FkeyFailedFile (PxSequentialFile)
# Must rpad for string columns, unknown lengths => use <...> to flag.
df_Lnk_IdsMdCertFkeyFail_Out_write = (
    df_Lnk_IdsMdCertFkeyFail_Out
    .withColumn("PRI_NAT_KEY_STRING", rpad(F.col("PRI_NAT_KEY_STRING"), <...>, " "))
    .withColumn("SRC_SYS_CD_SK", rpad(F.col("SRC_SYS_CD_SK"), <...>, " "))
    .withColumn("JOB_NM", rpad(F.col("JOB_NM"), <...>, " "))
    .withColumn("ERROR_TYP", rpad(F.col("ERROR_TYP"), <...>, " "))
    .withColumn("PHYSCL_FILE_NM", rpad(F.col("PHYSCL_FILE_NM"), <...>, " "))
    .withColumn("FRGN_NAT_KEY_STRING", rpad(F.col("FRGN_NAT_KEY_STRING"), <...>, " "))
    .withColumn("FIRST_RECYC_TS", rpad(F.col("FIRST_RECYC_TS"), <...>, " "))
)
file_path_seq_FkeyFailedFile = f"{adls_path}/fkey_failures/{PrefixFkeyFailedFileName}.NfmIdsMdlPolCertFkey.dat"
write_files(
    df_Lnk_IdsMdCertFkeyFail_Out_write.select(
        "PRI_NAT_KEY_STRING",
        "SRC_SYS_CD_SK",
        "JOB_NM",
        "ERROR_TYP",
        "PHYSCL_FILE_NM",
        "FRGN_NAT_KEY_STRING",
        "FIRST_RECYC_TS",
        "JOB_EXCTN_SK"
    ),
    file_path_seq_FkeyFailedFile,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)

# Read from db2_MDL_POL_CERT_Lkp (DB2ConnectorPX)
jdbc_url_db2_MDL_POL_CERT_Lkp, jdbc_props_db2_MDL_POL_CERT_Lkp = get_db_config(ids_secret_name)
extract_query_db2_MDL_POL_CERT_Lkp = f"""
SELECT MDL_POL_CERT_SK,
       MDL_DOC_ID,
       POL_NO,
       CERT_ID,
       CERT_EFF_DT,
       SRC_SYS_CD,
       CRT_RUN_CYC_EXCTN_SK,
       LAST_UPDT_RUN_CYC_EXCTN_SK,
       MDL_DOC_SK,
       CERT_EFF_ON_GRP_RNWL_IN,
       CERT_STTUS_ID,
       CERT_TERM_DT
FROM {IDSOwner}.MDL_POL_CERT
"""
df_db2_MDL_POL_CERT_Lkp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_MDL_POL_CERT_Lkp)
    .options(**jdbc_props_db2_MDL_POL_CERT_Lkp)
    .option("query", extract_query_db2_MDL_POL_CERT_Lkp)
    .load()
)

# CDC_MDL_POL_CERT (PxChangeCapture) logic.
# "Ref_MDL_POL_CERT" => before dataset, "Lnk_MdlCertFkey_Main" => after dataset
# We'll do a full outer join on MDL_POL_CERT_SK, produce change_code, keepInsert=2, keepDelete=1, keepEdit=3, dropCopy=4

df_before = df_db2_MDL_POL_CERT_Lkp.alias("Ref_MDL_POL_CERT")
df_after = df_Lnk_MdlCertFkey_Main.alias("Lnk_MdlCertFkey_Main")

df_joined_full = df_before.join(
    df_after,
    df_before["MDL_POL_CERT_SK"] == df_after["MDL_POL_CERT_SK"],
    "full"
)

df_joined_full = df_joined_full.select(
    df_after["MDL_POL_CERT_SK"].alias("AF_MDL_POL_CERT_SK"),
    df_after["MDL_DOC_ID"].alias("AF_MDL_DOC_ID"),
    df_after["POL_NO"].alias("AF_POL_NO"),
    df_after["CERT_ID"].alias("AF_CERT_ID"),
    df_after["CERT_EFF_DT"].alias("AF_CERT_EFF_DT"),
    df_after["SRC_SYS_CD"].alias("AF_SRC_SYS_CD"),
    df_after["CERT_EFF_ON_GRP_RNWL_IN"].alias("AF_CERT_EFF_ON_GRP_RNWL_IN"),
    df_after["CERT_STTUS_ID"].alias("AF_CERT_STTUS_ID"),
    df_after["CERT_TERM_DT"].alias("AF_CERT_TERM_DT"),
    df_after["CRT_RUN_CYC_EXCTN_SK"].alias("AF_CRT_RUN_CYC_EXCTN_SK"),
    df_after["LAST_UPDT_RUN_CYC_EXCTN_SK"].alias("AF_LAST_UPDT_RUN_CYC_EXCTN_SK"),
    df_after["MDL_DOC_SK"].alias("AF_MDL_DOC_SK"),

    df_before["MDL_POL_CERT_SK"].alias("BF_MDL_POL_CERT_SK"),
    df_before["MDL_DOC_ID"].alias("BF_MDL_DOC_ID"),
    df_before["POL_NO"].alias("BF_POL_NO"),
    df_before["CERT_ID"].alias("BF_CERT_ID"),
    df_before["CERT_EFF_DT"].alias("BF_CERT_EFF_DT"),
    df_before["SRC_SYS_CD"].alias("BF_SRC_SYS_CD"),
    df_before["CERT_EFF_ON_GRP_RNWL_IN"].alias("BF_CERT_EFF_ON_GRP_RNWL_IN"),
    df_before["CERT_STTUS_ID"].alias("BF_CERT_STTUS_ID"),
    df_before["CERT_TERM_DT"].alias("BF_CERT_TERM_DT"),
    df_before["CRT_RUN_CYC_EXCTN_SK"].alias("BF_CRT_RUN_CYC_EXCTN_SK"),
    df_before["LAST_UPDT_RUN_CYC_EXCTN_SK"].alias("BF_LAST_UPDT_RUN_CYC_EXCTN_SK"),
    df_before["MDL_DOC_SK"].alias("BF_MDL_DOC_SK")
)

df_with_flags = df_joined_full.withColumn(
    "is_in_after",
    F.when(F.col("AF_MDL_POL_CERT_SK").isNotNull(), F.lit(1)).otherwise(F.lit(0))
).withColumn(
    "is_in_before",
    F.when(F.col("BF_MDL_POL_CERT_SK").isNotNull(), F.lit(1)).otherwise(F.lit(0))
)

# compare ignoring CRT_RUN_CYC_EXCTN_SK, LAST_UPDT_RUN_CYC_EXCTN_SK
# columns_to_compare = [MDL_DOC_ID, POL_NO, CERT_ID, CERT_EFF_DT, SRC_SYS_CD, CERT_EFF_ON_GRP_RNWL_IN, CERT_STTUS_ID, CERT_TERM_DT, MDL_DOC_SK]
cond_diff = (
    (F.col("AF_MDL_DOC_ID") != F.col("BF_MDL_DOC_ID"))
    | (F.col("AF_POL_NO") != F.col("BF_POL_NO"))
    | (F.col("AF_CERT_ID") != F.col("BF_CERT_ID"))
    | (F.col("AF_CERT_EFF_DT") != F.col("BF_CERT_EFF_DT"))
    | (F.col("AF_SRC_SYS_CD") != F.col("BF_SRC_SYS_CD"))
    | (F.col("AF_CERT_EFF_ON_GRP_RNWL_IN") != F.col("BF_CERT_EFF_ON_GRP_RNWL_IN"))
    | (F.col("AF_CERT_STTUS_ID") != F.col("BF_CERT_STTUS_ID"))
    | (F.col("AF_CERT_TERM_DT") != F.col("BF_CERT_TERM_DT"))
    | (F.col("AF_MDL_DOC_SK") != F.col("BF_MDL_DOC_SK"))
)

df_with_change_code = df_with_flags.withColumn(
    "change_code",
    F.when( (F.col("is_in_before") == 0) & (F.col("is_in_after") == 1), F.lit(2))  # insert
    .when((F.col("is_in_before") == 1) & (F.col("is_in_after") == 0), F.lit(1))   # delete
    .when(
        (F.col("is_in_before") == 1) & (F.col("is_in_after") == 1) & cond_diff,
        F.lit(3)
    )  # edit
    .otherwise(F.lit(4))  # copy
)

# Keep keepInsert=2, keepDelete=1, keepEdit=3, dropCopy => remove code=4
df_filtered = df_with_change_code.filter(F.col("change_code") != 4)

# Build the final output columns as the stage does: always from after side
df_cdc_mdl_pol_cert = df_filtered.select(
    F.col("AF_MDL_POL_CERT_SK").alias("MDL_POL_CERT_SK"),
    F.col("AF_MDL_DOC_ID").alias("MDL_DOC_ID"),
    F.col("AF_POL_NO").alias("POL_NO"),
    F.col("AF_CERT_ID").alias("CERT_ID"),
    F.col("AF_CERT_EFF_DT").alias("CERT_EFF_DT"),
    F.col("AF_SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("AF_CERT_EFF_ON_GRP_RNWL_IN").alias("CERT_EFF_ON_GRP_RNWL_IN"),
    F.col("AF_CERT_STTUS_ID").alias("CERT_STTUS_ID"),
    F.col("AF_CERT_TERM_DT").alias("CERT_TERM_DT"),
    F.col("AF_CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("AF_LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("AF_MDL_DOC_SK").alias("MDL_DOC_SK"),
    F.col("change_code").alias("change_code")
)

# TXN_MDL_POL_CERT (CTransformerStage)
# We produce 3 outputs from df_cdc_mdl_pol_cert:
# 1) out_mdl_pol_cert (no constraint)
# 2) lnk_mdlpolcert_unk ((@INROWNUM - 1) * @NUMPARTITIONS + @PARTITIONNUM + 1 = 1)
# 3) lnk_mdlpolcert_na ((@INROWNUM - 1) * @NUMPARTITIONS + @PARTITIONNUM + 1 = 1)

# Create a row_number across entire DF to mimic DataStage partition row logic:
window_no_part = Window.orderBy(F.lit(1))
df_cdc_mdl_pol_cert_numbered = df_cdc_mdl_pol_cert.withColumn("rownum_global", F.row_number().over(window_no_part))

# out_mdl_pol_cert => all rows
df_out_mdl_pol_cert_raw = df_cdc_mdl_pol_cert_numbered.select(
    F.col("MDL_POL_CERT_SK"),
    F.col("MDL_DOC_ID"),
    F.col("POL_NO"),
    F.col("CERT_ID"),
    F.col("CERT_EFF_DT"),
    F.col("SRC_SYS_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.when(
        F.col("change_code") == 2,
        F.lit(RunID)
    ).otherwise(F.col("LAST_UPDT_RUN_CYC_EXCTN_SK")).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("MDL_DOC_SK"),
    F.col("CERT_EFF_ON_GRP_RNWL_IN"),
    F.when(F.col("change_code") == 2, F.lit(0)).otherwise(F.col("CERT_STTUS_ID")).alias("CERT_STTUS_ID"),
    F.when(
        F.col("change_code") == 2,
        F.date_sub(F.to_date(F.lit(currdate), "yyyy-MM-dd"), 1)
    ).otherwise(F.lit("2199-12-31")).alias("CERT_TERM_DT"),
    F.col("rownum_global"),
    F.col("change_code")
)

# lnk_mdlpolcert_unk => rownum_global = 1
df_lnk_mdlpolcert_unk = df_cdc_mdl_pol_cert_numbered.filter(F.col("rownum_global") == 1).select(
    F.lit(0).alias("MDL_POL_CERT_SK"),
    F.lit("UNK").alias("MDL_DOC_ID"),
    F.lit("UNK").alias("POL_NO"),
    F.lit("UNK").alias("CERT_ID"),
    F.lit("1753-01-01").alias("CERT_EFF_DT"),
    F.col("SRC_SYS_CD"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("MDL_DOC_SK"),
    F.lit("N").alias("CERT_EFF_ON_GRP_RNWL_IN"),
    F.lit("0").alias("CERT_STTUS_ID"),
    F.lit("1753-01-01").alias("CERT_TERM_DT"),
    F.col("rownum_global"),
    F.col("change_code")
)

# lnk_mdlpolcert_na => rownum_global = 1
df_lnk_mdlpolcert_na = df_cdc_mdl_pol_cert_numbered.filter(F.col("rownum_global") == 1).select(
    F.lit(1).alias("MDL_POL_CERT_SK"),
    F.lit("NA").alias("MDL_DOC_ID"),
    F.lit("NA").alias("POL_NO"),
    F.lit("NA").alias("CERT_ID"),
    F.lit("1753-01-01").alias("CERT_EFF_DT"),
    F.col("SRC_SYS_CD"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("MDL_DOC_SK"),
    F.lit("N").alias("CERT_EFF_ON_GRP_RNWL_IN"),
    F.lit("1").alias("CERT_STTUS_ID"),
    F.lit("1753-01-01").alias("CERT_TERM_DT"),
    F.col("rownum_global"),
    F.col("change_code")
)

# The main link: out_mdl_pol_cert => replicate the same columns as above minus rownum and change_code in final, but they are needed for funnel logic
df_out_mdl_pol_cert = df_out_mdl_pol_cert_raw.select(
    "MDL_POL_CERT_SK",
    "MDL_DOC_ID",
    "POL_NO",
    "CERT_ID",
    "CERT_EFF_DT",
    "SRC_SYS_CD",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "MDL_DOC_SK",
    "CERT_EFF_ON_GRP_RNWL_IN",
    "CERT_STTUS_ID",
    "CERT_TERM_DT",
    "rownum_global",
    "change_code"
)

# FN_MDLPOLCERT (PxFunnel) => funnel the three dataframes
df_funnel_unk = df_lnk_mdlpolcert_unk.select(
    "MDL_POL_CERT_SK",
    "MDL_DOC_ID",
    "POL_NO",
    "CERT_ID",
    "CERT_EFF_DT",
    "SRC_SYS_CD",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "MDL_DOC_SK",
    "CERT_EFF_ON_GRP_RNWL_IN",
    "CERT_STTUS_ID",
    "CERT_TERM_DT",
    "rownum_global",
    "change_code"
)
df_funnel_na = df_lnk_mdlpolcert_na.select(
    "MDL_POL_CERT_SK",
    "MDL_DOC_ID",
    "POL_NO",
    "CERT_ID",
    "CERT_EFF_DT",
    "SRC_SYS_CD",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "MDL_DOC_SK",
    "CERT_EFF_ON_GRP_RNWL_IN",
    "CERT_STTUS_ID",
    "CERT_TERM_DT",
    "rownum_global",
    "change_code"
)
df_funnel_out = df_out_mdl_pol_cert.select(
    "MDL_POL_CERT_SK",
    "MDL_DOC_ID",
    "POL_NO",
    "CERT_ID",
    "CERT_EFF_DT",
    "SRC_SYS_CD",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "MDL_DOC_SK",
    "CERT_EFF_ON_GRP_RNWL_IN",
    "CERT_STTUS_ID",
    "CERT_TERM_DT",
    "rownum_global",
    "change_code"
)

df_FN_MDLPOLCERT = df_funnel_unk.union(df_funnel_na).union(df_funnel_out)

# Seq_MdlPolCertFKey (PxSequentialFile) => write MDLPOLCERT.#SrcSysCd#.Fkey.#RunID#.dat
# Final select to maintain column order and (if char/varchar) rpad with <...> for length.
df_FN_MDLPOLCERT_write = (
    df_FN_MDLPOLCERT
    .select(
        "MDL_POL_CERT_SK",
        "MDL_DOC_ID",
        "POL_NO",
        "CERT_ID",
        "CERT_EFF_DT",
        "SRC_SYS_CD",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "MDL_DOC_SK",
        "CERT_EFF_ON_GRP_RNWL_IN",
        "CERT_STTUS_ID",
        "CERT_TERM_DT"
    )
    .withColumn("MDL_DOC_ID", rpad(F.col("MDL_DOC_ID"), <...>, " "))
    .withColumn("POL_NO", rpad(F.col("POL_NO"), <...>, " "))
    .withColumn("CERT_ID", rpad(F.col("CERT_ID"), <...>, " "))
    .withColumn("SRC_SYS_CD", rpad(F.col("SRC_SYS_CD"), <...>, " "))
    .withColumn("CERT_EFF_ON_GRP_RNWL_IN", rpad(F.col("CERT_EFF_ON_GRP_RNWL_IN"), <...>, " "))
    .withColumn("CERT_STTUS_ID", rpad(F.col("CERT_STTUS_ID"), <...>, " "))
)
file_path_seq_MdlPolCertFKey = f"{adls_path}/load/MDLPOLCERT.{SrcSysCd}.Fkey.{RunID}.dat"
write_files(
    df_FN_MDLPOLCERT_write,
    file_path_seq_MdlPolCertFKey,
    delimiter="|",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)