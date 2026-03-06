# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:
# MAGIC 
# MAGIC PROCESSING:
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                   Project/Altiris #               Change Description                         Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------      -----------------------------------    ---------------------------------------------------------   ----------------------------------   ---------------------------------    -------------------------   
# MAGIC 
# MAGIC Raj Mangalampally        12/25/2013                 5114                        Original Programming                          EnterpriseWrhsDevl      Jag Yelavarthi             2014-01-29
# MAGIC                                                                                                          (Server to Parallel Conversion)

# MAGIC Code SK lookups for Denormalization
# MAGIC 
# MAGIC Lookup Keys:
# MAGIC CUST_SVC_TASK_NOTE_LOC_CD_SK
# MAGIC Write CUST_SVC_TASK_NOTE_D.dat Data into a Sequential file for Load Job IdsEdwCustSvcTaskNoteDLoad.
# MAGIC Read all the Data from IDS Cust Service Table CUST_SVC_TASK_NOTE and W_CUST_SVC_DRVR; Pull Records based on matching records on Column CUST_SVC_ID
# MAGIC Add Defaults and Null Handling.
# MAGIC Reference Code Mapping Data for Code SK lookups
# MAGIC Job Name: IdsEdwCustExtrSvcRaskNoteDExtr
# MAGIC Read all the Data from IDS APP User Table and
# MAGIC Lookup Keys:
# MAGIC 
# MAGIC AGNT_SK
# MAGIC AGNT_ID
# MAGIC Reference Code Mapping Data for Code SK lookups
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, when, trim, length, regexp_replace, rpad
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


# Retrieve all required job parameters
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
EDWRunCycle = get_widget_value('EDWRunCycle','')

# --------------------------------------------------------------------------------
# Stage: db2_CD_MPPNG_in (DB2ConnectorPX)
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = (
    "SELECT CD_MPPNG_SK,COALESCE(TRGT_CD,'UNK') TRGT_CD,COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM "
    f"from {IDSOwner}.CD_MPPNG"
)
df_db2_CD_MPPNG_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: db2_CUST_SVC_TASK_NOTE_D_in (DB2ConnectorPX)
extract_query = (
    "SELECT \n"
    "NOTE.CUST_SVC_TASK_NOTE_SK,\n"
    "COALESCE(CD.TRGT_CD,'UNK') SRC_SYS_CD,\n"
    "NOTE.CUST_SVC_ID,\n"
    "NOTE.TASK_SEQ_NO,\n"
    "NOTE.CUST_SVC_TASK_NOTE_LOC_CD_SK,\n"
    "NOTE.NOTE_SEQ_NO,\n"
    "NOTE.LAST_UPDT_DTM,\n"
    "NOTE.CUST_SVC_TASK_SK,\n"
    "NOTE.LAST_UPDT_USER_SK,\n"
    "NOTE.NOTE_SUM_TX \n"
    f"FROM {IDSOwner}.CUST_SVC_TASK_NOTE NOTE\n"
    f"LEFT OUTER JOIN {IDSOwner}.CD_MPPNG CD ON NOTE.SRC_SYS_CD_SK = CD.CD_MPPNG_SK,\n"
    f"{IDSOwner}.W_CUST_SVC_DRVR DRVR \n"
    "WHERE          \n"
    "NOTE.SRC_SYS_CD_SK = DRVR.SRC_SYS_CD_SK \n"
    "AND NOTE.CUST_SVC_ID       = DRVR.CUST_SVC_ID  "
    ";"
)
df_db2_CUST_SVC_TASK_NOTE_D_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: db2_APP_USER_in (DB2ConnectorPX)
extract_query = (
    "SELECT \n"
    "APP_USER.USER_SK,\n"
    "APP_USER.USER_ID\n"
    f" FROM {IDSOwner}.APP_USER APP_USER;"
)
df_db2_APP_USER_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: db2_CUST_SVC_TASK_NOTE_LN_in (DB2ConnectorPX)
extract_query = (
    "SELECT \n"
    "NOTE_LN.CUST_SVC_TASK_NOTE_SK,\n"
    "NOTE_LN.LN_TX,\n"
    "NOTE_LN.LN_SEQ_NO \n"
    f"FROM {IDSOwner}.CUST_SVC_TASK_NOTE_LN NOTE_LN,\n"
    f"{IDSOwner}.W_CUST_SVC_DRVR DRVR \n"
    "WHERE \n"
    "NOTE_LN.SRC_SYS_CD_SK = DRVR.SRC_SYS_CD_SK \n"
    "AND NOTE_LN.CUST_SVC_ID = DRVR.CUST_SVC_ID AND\n"
    "NOTE_LN.LN_TX IS \n"
    "NOT NULL"
)
df_db2_CUST_SVC_TASK_NOTE_LN_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: Transformer_96 (CTransformerStage)
# This transformer groups the rows by CUST_SVC_TASK_NOTE_SK, concatenates LN_TX, and outputs only once per group.
df_Transformer_96_agg = (
    df_db2_CUST_SVC_TASK_NOTE_LN_in
    .groupBy("CUST_SVC_TASK_NOTE_SK")
    .agg(F.concat_ws(" ", F.collect_list("LN_TX")).alias("SVLNTX"))
)

df_Transformer_96 = (
    df_Transformer_96_agg
    .select(
        col("CUST_SVC_TASK_NOTE_SK"),
        when(
            (length(col("SVLNTX")) > 4000) & (col("CUST_SVC_TASK_NOTE_SK") != lit(1)),
            lit("Y")
        )
        .when(col("CUST_SVC_TASK_NOTE_SK") == lit(1), lit("X"))
        .otherwise(lit("N")).alias("LN_IN"),
        when(length(col("SVLNTX")) > 4000, col("SVLNTX").substr(1, 4000))
        .otherwise(col("SVLNTX")).alias("LN_TX")
    )
)

# --------------------------------------------------------------------------------
# Stage: lkp_Codes1 (PxLookup)
# Primary link: df_db2_CUST_SVC_TASK_NOTE_D_in
# Lookup link: df_db2_APP_USER_in (left join)
# Lookup link: df_Transformer_96 (left join)
df_lkp_Codes1_primary = df_db2_CUST_SVC_TASK_NOTE_D_in.alias("primary")
df_lkp_Codes1 = (
    df_lkp_Codes1_primary
    .join(df_db2_APP_USER_in.alias("Ref_AgntLkup"),
          col("primary.LAST_UPDT_USER_SK") == col("Ref_AgntLkup.USER_SK"),
          "left")
    .join(df_Transformer_96.alias("DSLink101"),
          col("primary.CUST_SVC_TASK_NOTE_SK") == col("DSLink101.CUST_SVC_TASK_NOTE_SK"),
          "left")
)

df_lkp_Codes1 = df_lkp_Codes1.select(
    col("primary.CUST_SVC_TASK_NOTE_SK").alias("CUST_SVC_TASK_NOTE_SK"),
    col("primary.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("primary.CUST_SVC_ID").alias("CUST_SVC_ID"),
    col("primary.TASK_SEQ_NO").alias("TASK_SEQ_NO"),
    col("primary.CUST_SVC_TASK_NOTE_LOC_CD_SK").alias("CUST_SVC_TASK_NOTE_LOC_CD_SK"),
    col("primary.NOTE_SEQ_NO").alias("NOTE_SEQ_NO"),
    col("primary.LAST_UPDT_DTM").alias("LAST_UPDT_DTM"),
    col("primary.CUST_SVC_TASK_SK").alias("CUST_SVC_TASK_SK"),
    col("primary.LAST_UPDT_USER_SK").alias("LAST_UPDT_USER_SK"),
    col("DSLink101.LN_IN").alias("LN_IN"),
    col("DSLink101.LN_TX").alias("LN_TX"),
    col("DSLink101.CUST_SVC_TASK_NOTE_SK").alias("CUST_SVC_TASK_NOTE_SK_1"),
    col("primary.NOTE_SUM_TX").alias("NOTE_SUM_TX"),
    col("Ref_AgntLkup.USER_ID").alias("USER_ID")
)

# --------------------------------------------------------------------------------
# Stage: lkp_Codes (PxLookup)
# Primary link: df_lkp_Codes1
# Lookup link: df_db2_CD_MPPNG_in (left join)
df_lkp_Codes = (
    df_lkp_Codes1.alias("lnk_CustNote_Out")
    .join(
        df_db2_CD_MPPNG_in.alias("Ref_LobCd_Lkup"),
        col("lnk_CustNote_Out.CUST_SVC_TASK_NOTE_LOC_CD_SK") == col("Ref_LobCd_Lkup.CD_MPPNG_SK"),
        "left"
    )
)

df_lkp_Codes = df_lkp_Codes.select(
    col("lnk_CustNote_Out.CUST_SVC_TASK_NOTE_SK").alias("CUST_SVC_TASK_NOTE_SK"),
    col("lnk_CustNote_Out.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("lnk_CustNote_Out.CUST_SVC_ID").alias("CUST_SVC_ID"),
    col("lnk_CustNote_Out.TASK_SEQ_NO").alias("TASK_SEQ_NO"),
    col("lnk_CustNote_Out.CUST_SVC_TASK_NOTE_LOC_CD_SK").alias("CUST_SVC_TASK_NOTE_LOC_CD_SK"),
    col("lnk_CustNote_Out.NOTE_SEQ_NO").alias("NOTE_SEQ_NO"),
    col("lnk_CustNote_Out.LAST_UPDT_DTM").alias("LAST_UPDT_DTM"),
    col("lnk_CustNote_Out.CUST_SVC_TASK_SK").alias("CUST_SVC_TASK_SK"),
    col("lnk_CustNote_Out.LAST_UPDT_USER_SK").alias("LAST_UPDT_USER_SK"),
    col("lnk_CustNote_Out.LN_IN").alias("LN_IN"),
    col("lnk_CustNote_Out.LN_TX").alias("LN_TX"),
    col("lnk_CustNote_Out.CUST_SVC_TASK_NOTE_SK_1").alias("CUST_SVC_TASK_NOTE_SK_1"),
    col("Ref_LobCd_Lkup.TRGT_CD").alias("TRGT_CD"),
    col("Ref_LobCd_Lkup.TRGT_CD_NM").alias("TRGT_CD_NM"),
    col("lnk_CustNote_Out.NOTE_SUM_TX").alias("NOTE_SUM_TX"),
    col("lnk_CustNote_Out.USER_ID").alias("USER_ID")
)

# --------------------------------------------------------------------------------
# Stage: xfrm_BusinessLogic (CTransformerStage)
# Output 1: lnk_IdsEdwCustSvcTaskNoteDMainData_Out => constraint: CUST_SVC_TASK_NOTE_SK <> 0 and <>1
df_xfrm_BusinessLogic_main = (
    df_lkp_Codes
    .filter((col("CUST_SVC_TASK_NOTE_SK") != 0) & (col("CUST_SVC_TASK_NOTE_SK") != 1))
    .select(
        col("CUST_SVC_TASK_NOTE_SK").alias("CUST_SVC_TASK_NOTE_SK"),
        when(trim(col("SRC_SYS_CD")) == "", lit("UNK"))
        .otherwise(col("SRC_SYS_CD")).alias("SRC_SYS_CD"),
        col("CUST_SVC_ID").alias("CUST_SVC_ID"),
        col("TASK_SEQ_NO").alias("CUST_SVC_TASK_SEQ_NO"),
        when(trim(col("TRGT_CD")) == "", lit("UNK"))
        .otherwise(col("TRGT_CD")).alias("CUST_SVC_TASK_NOTE_LOC_CD"),
        col("NOTE_SEQ_NO").alias("CUST_SVC_TASK_NOTE_SEQ_NO"),
        col("LAST_UPDT_DTM").alias("CS_TASK_NOTE_LAST_UPDT_DTM"),
        lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        col("CUST_SVC_TASK_SK").alias("CUST_SVC_TASK_SK"),
        col("LAST_UPDT_USER_SK").alias("LAST_UPDT_USER_SK"),
        when(
            (col("CUST_SVC_TASK_NOTE_SK_1").isNull()) |
            (length(trim(col("CUST_SVC_TASK_NOTE_SK_1"))) == 0) |
            (length(trim(col("LN_IN"))) == 0),
            lit("N")
        ).otherwise(col("LN_IN")).alias("CUST_SVC_TASK_NOTE_CONT_IN"),
        col("CUST_SVC_TASK_NOTE_LOC_CD_SK").alias("CUST_SVC_TASK_NOTE_LOC_CD_SK"),
        when(trim(col("TRGT_CD_NM")) == "", lit("NA"))
        .otherwise(col("TRGT_CD_NM")).alias("CUST_SVC_TASK_NOTE_LOC_NM"),
        regexp_replace(col("NOTE_SUM_TX"), "\\^", " ").alias("CUST_SVC_TASK_NOTE_SUM_TX"),
        when(col("CUST_SVC_TASK_NOTE_SK_1").isNull(), lit("NA"))
        .otherwise(regexp_replace(col("LN_TX"), "\\^", " ")).alias("CUST_SVC_TASK_NOTE_TX"),
        when((col("USER_ID").isNull()) | (trim(col("USER_ID")) == ""), lit("NA"))
        .otherwise(col("USER_ID")).alias("LAST_UPDT_USER_ID"),
        lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
    )
)

# Output 2: NA => constraint: ((@INROWNUM - 1) * @NUMPARTITIONS + @PARTITIONNUM + 1) = 1
# Single row with specified columns
schema_na = StructType([
    StructField("CUST_SVC_TASK_NOTE_SK", IntegerType(), True),
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("CUST_SVC_ID", StringType(), True),
    StructField("CUST_SVC_TASK_SEQ_NO", IntegerType(), True),
    StructField("CUST_SVC_TASK_NOTE_LOC_CD", StringType(), True),
    StructField("CUST_SVC_TASK_NOTE_SEQ_NO", IntegerType(), True),
    StructField("CS_TASK_NOTE_LAST_UPDT_DTM", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
    StructField("CUST_SVC_TASK_SK", IntegerType(), True),
    StructField("LAST_UPDT_USER_SK", IntegerType(), True),
    StructField("CUST_SVC_TASK_NOTE_CONT_IN", StringType(), True),
    StructField("CUST_SVC_TASK_NOTE_LOC_CD_SK", IntegerType(), True),
    StructField("CUST_SVC_TASK_NOTE_LOC_NM", StringType(), True),
    StructField("CUST_SVC_TASK_NOTE_SUM_TX", StringType(), True),
    StructField("CUST_SVC_TASK_NOTE_TX", StringType(), True),
    StructField("LAST_UPDT_USER_ID", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True)
])
df_xfrm_BusinessLogic_na = spark.createDataFrame([
    (
        1,
        "NA",
        "NA",
        0,
        "NA",
        0,
        "1753-01-01 00:00:00.000000",
        "1753-01-01",
        "1753-01-01",
        1,
        1,
        "N",
        1,
        "NA",
        "",
        "",
        "NA",
        100,
        100
    )
], schema_na)

# Output 3: UNK => constraint: ((@INROWNUM - 1) * @NUMPARTITIONS + @PARTITIONNUM + 1) = 1
# Single row with specified columns
schema_unk = StructType([
    StructField("CUST_SVC_TASK_NOTE_SK", IntegerType(), True),
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("CUST_SVC_ID", StringType(), True),
    StructField("CUST_SVC_TASK_SEQ_NO", IntegerType(), True),
    StructField("CUST_SVC_TASK_NOTE_LOC_CD", StringType(), True),
    StructField("CUST_SVC_TASK_NOTE_SEQ_NO", IntegerType(), True),
    StructField("CS_TASK_NOTE_LAST_UPDT_DTM", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
    StructField("CUST_SVC_TASK_SK", IntegerType(), True),
    StructField("LAST_UPDT_USER_SK", IntegerType(), True),
    StructField("CUST_SVC_TASK_NOTE_CONT_IN", StringType(), True),
    StructField("CUST_SVC_TASK_NOTE_LOC_CD_SK", IntegerType(), True),
    StructField("CUST_SVC_TASK_NOTE_LOC_NM", StringType(), True),
    StructField("CUST_SVC_TASK_NOTE_SUM_TX", StringType(), True),
    StructField("CUST_SVC_TASK_NOTE_TX", StringType(), True),
    StructField("LAST_UPDT_USER_ID", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True)
])
df_xfrm_BusinessLogic_unk = spark.createDataFrame([
    (
        0,
        "UNK",
        "UNK",
        0,
        "UNK",
        0,
        "1753-01-01 00:00:00.000000",
        "1753-01-01",
        "1753-01-01",
        0,
        0,
        "N",
        0,
        "UNK",
        "",
        "",
        "UNK",
        100,
        100
    )
], schema_unk)

# --------------------------------------------------------------------------------
# Stage: fnl_NA_UNK (PxFunnel)
# Funnel in the order: main, NA, UNK
df_fnl_NA_UNK = (
    df_xfrm_BusinessLogic_main
    .unionByName(df_xfrm_BusinessLogic_na)
    .unionByName(df_xfrm_BusinessLogic_unk)
)

# --------------------------------------------------------------------------------
# Stage: CUST_SVC_TASK_NOTE_D (PxSequentialFile)
# Final select with rpad for known char columns
df_final = df_fnl_NA_UNK.select(
    col("CUST_SVC_TASK_NOTE_SK"),
    col("SRC_SYS_CD"),
    col("CUST_SVC_ID"),
    col("CUST_SVC_TASK_SEQ_NO"),
    col("CUST_SVC_TASK_NOTE_LOC_CD"),
    col("CUST_SVC_TASK_NOTE_SEQ_NO"),
    col("CS_TASK_NOTE_LAST_UPDT_DTM"),
    rpad(col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    rpad(col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("CUST_SVC_TASK_SK"),
    col("LAST_UPDT_USER_SK"),
    rpad(col("CUST_SVC_TASK_NOTE_CONT_IN"), 1, " ").alias("CUST_SVC_TASK_NOTE_CONT_IN"),
    col("CUST_SVC_TASK_NOTE_LOC_CD_SK"),
    col("CUST_SVC_TASK_NOTE_LOC_NM"),
    col("CUST_SVC_TASK_NOTE_SUM_TX"),
    col("CUST_SVC_TASK_NOTE_TX"),
    rpad(col("LAST_UPDT_USER_ID"), 10, " ").alias("LAST_UPDT_USER_ID"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

write_files(
    df_final,
    f"{adls_path}/load/CUST_SVC_TASK_NOTE_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)