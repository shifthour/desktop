# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC PROCESSING: It counts the rows from four different source lkup values. After counting those values then create a primary key process in order to assign primary keys.
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date               Project/Altiris #               Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------   -----------------------------------    ---------------------------------------------------------   ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Bhoomi Dasari                3/29/2007           CDS Sunset/3279           Originally Programmed                     devlEDW10                  Steph Goddard           4/6/07
# MAGIC Bhoomi Dasari                5/1/09             Prod Supp                       Removed check for clear files on        devlEDW                      Steph Goddard           05/15/2009
# MAGIC                                                                                                        primary key hash
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC Balkarrn Gill              12/26/2013        5114                              Create Load File for DM Table PRVCY_TRK_SUM_F         EnterpriseWrhsDevl                   Peter Marshall               1/7/2014

# MAGIC Write Data into a Dataset for Pky Job
# MAGIC Read all the Data from EDW W_COCC_DRVR Table;
# MAGIC Add Defaults and Null Handling.
# MAGIC Job Name: EdwEdwPrvcyTrkSumFExtr
# MAGIC Remove Duplicates stage takes care of getting rid of duplicate rows with the same  key.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
CurrRunCycleDate = get_widget_value('CurrRunCycleDate','')
CurrRunCycle = get_widget_value('CurrRunCycle','')

# db2_CD_MPPNG_Extr (DB2ConnectorPX) - IDS
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
extract_query_db2_CD_MPPNG_Extr = f"""
SELECT 
CD_MPPNG.SRC_DRVD_LKUP_VAL,
CD_MPPNG.TRGT_CD,
CD_MPPNG.TRGT_CD_NM,
CD_MPPNG.CD_MPPNG_SK
FROM {IDSOwner}.CD_MPPNG CD_MPPNG
WHERE
CD_MPPNG.SRC_DRVD_LKUP_VAL IN ('AUTH', 'CONFCOMM', 'MBRRELSHP', 'DSCLSR')
"""
df_db2_CD_MPPNG_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_CD_MPPNG_Extr.strip())
    .load()
)

# rdp_Cd (PxRemDup)
df_rdp_Cd = dedup_sort(
    df_db2_CD_MPPNG_Extr,
    partition_cols=["SRC_DRVD_LKUP_VAL"],
    sort_cols=[("SRC_DRVD_LKUP_VAL","A")]
).select(
    F.col("SRC_DRVD_LKUP_VAL"),
    F.col("CD_MPPNG_SK"),
    F.col("TRGT_CD"),
    F.col("TRGT_CD_NM")
)

# db2_PRVCY_AUTH_F_in (DB2ConnectorPX) - EDW
jdbc_url_edw, jdbc_props_edw = get_db_config(edw_secret_name)
extract_query_db2_PRVCY_AUTH_F_in = f"""
SELECT
AUTH.PRVCY_AUTH_SK,
AUTH.PRVCY_AUTH_CRT_DT_SK,
AUTH.PRVCY_AUTH_EFF_DT_SK,
AUTH.PRVCY_AUTH_TERM_DT_SK
FROM
{EDWOwner}.PRVCY_AUTH_F AUTH
ORDER BY
AUTH.PRVCY_AUTH_SK DESC
"""
df_db2_PRVCY_AUTH_F_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_edw)
    .options(**jdbc_props_edw)
    .option("query", extract_query_db2_PRVCY_AUTH_F_in.strip())
    .load()
)

# xfrm_Auth (CTransformerStage) - replicate stage-variable counting and constraint svCt=1 => single aggregated row
df_xfrm_Auth_agg = df_db2_PRVCY_AUTH_F_in.select(
    F.sum(
        F.when(
            (F.col("PRVCY_AUTH_EFF_DT_SK") <= CurrRunCycleDate) &
            (F.col("PRVCY_AUTH_TERM_DT_SK") >= CurrRunCycleDate),
            1
        ).otherwise(0)
    ).alias("ACTV_CT"),
    F.sum(
        F.when(
            (F.col("PRVCY_AUTH_TERM_DT_SK") <= CurrRunCycleDate),
            1
        ).otherwise(0)
    ).alias("CANC_CT"),
    F.sum(
        F.when(
            F.col("PRVCY_AUTH_CRT_DT_SK") == CurrRunCycleDate,
            1
        ).otherwise(0)
    ).alias("NEW_CT")
)
df_xfrm_Auth = df_xfrm_Auth_agg.withColumn("PRVCY_TRK_SUM_TYP_CD", F.lit("AUTH")) \
    .withColumn("SUM_DT", F.lit(CurrRunCycleDate))

# db2_PRVCY_CONF_COMM_F_in (DB2ConnectorPX) - EDW
extract_query_db2_PRVCY_CONF_COMM_F_in = f"""
SELECT
CONF.PRVCY_CONF_COMM_SK,
CONF.PRVCY_CONF_COMM_EFF_DT_SK,
CONF.PRVCY_CONF_COMM_RCVD_DT_SK,
CONF.PRVCY_CONF_COMM_TERM_DT_SK
FROM
{EDWOwner}.PRVCY_CONF_COMM_F CONF
ORDER BY
CONF.PRVCY_CONF_COMM_SK DESC
"""
df_db2_PRVCY_CONF_COMM_F_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_edw)
    .options(**jdbc_props_edw)
    .option("query", extract_query_db2_PRVCY_CONF_COMM_F_in.strip())
    .load()
)

# xfrm_Conf (CTransformerStage)
df_xfrm_Conf_agg = df_db2_PRVCY_CONF_COMM_F_in.select(
    F.sum(
        F.when(
            (F.col("PRVCY_CONF_COMM_EFF_DT_SK") <= CurrRunCycleDate) &
            (F.col("PRVCY_CONF_COMM_TERM_DT_SK") >= CurrRunCycleDate),
            1
        ).otherwise(0)
    ).alias("ACTV_CT"),
    F.sum(
        F.when(
            (F.col("PRVCY_CONF_COMM_TERM_DT_SK") <= CurrRunCycleDate),
            1
        ).otherwise(0)
    ).alias("CANC_CT"),
    F.sum(
        F.when(
            F.col("PRVCY_CONF_COMM_RCVD_DT_SK") == CurrRunCycleDate,
            1
        ).otherwise(0)
    ).alias("NEW_CT")
)
df_xfrm_Conf = df_xfrm_Conf_agg.withColumn("PRVCY_TRK_SUM_TYP_CD", F.lit("CONFCOMM")) \
    .withColumn("SUM_DT", F.lit(CurrRunCycleDate))

# db2_PRVCY_DSCLSUR_F_in (DB2ConnectorPX) - EDW
extract_query_db2_PRVCY_DSCLSUR_F_in = f"""
SELECT
DSCLSUR.PRVCY_DSCLSUR_SK,
DSCLSUR.PRVCY_DSCLSUR_DT_SK
FROM
{EDWOwner}.PRVCY_DSCLSUR_F DSCLSUR
ORDER BY
DSCLSUR.PRVCY_DSCLSUR_SK DESC
"""
df_db2_PRVCY_DSCLSUR_F_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_edw)
    .options(**jdbc_props_edw)
    .option("query", extract_query_db2_PRVCY_DSCLSUR_F_in.strip())
    .load()
)

# xfrm_Dsclsr (CTransformerStage)
df_xfrm_Dsclsr_agg = df_db2_PRVCY_DSCLSUR_F_in.select(
    F.lit(0).alias("ACTV_CT"),
    F.lit(0).alias("CANC_CT"),
    F.sum(
        F.when(
            F.col("PRVCY_DSCLSUR_DT_SK") == CurrRunCycleDate,
            1
        ).otherwise(0)
    ).alias("NEW_CT")
)
df_xfrm_Dsclsr = df_xfrm_Dsclsr_agg.withColumn("PRVCY_TRK_SUM_TYP_CD", F.lit("DSCLSR")) \
    .withColumn("SUM_DT", F.lit(CurrRunCycleDate))

# db2_PRVCY_MBR_RELSHP_F_in (DB2ConnectorPX) - EDW
extract_query_db2_PRVCY_MBR_RELSHP_F_in = f"""
SELECT
MBR.PRVCY_MBR_RELSHP_SK,
MBR.PRVCY_MBR_RELSHP_CRT_DT_SK,
MBR.PRVCY_MBR_RELSHP_EFF_DT_SK,
MBR.PRVCY_MBR_RELSHP_TERM_DT_SK
FROM
{EDWOwner}.PRVCY_MBR_RELSHP_F MBR
ORDER BY
MBR.PRVCY_MBR_RELSHP_SK DESC
"""
df_db2_PRVCY_MBR_RELSHP_F_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_edw)
    .options(**jdbc_props_edw)
    .option("query", extract_query_db2_PRVCY_MBR_RELSHP_F_in.strip())
    .load()
)

# xfrm_Relshp (CTransformerStage)
df_xfrm_Relshp_agg = df_db2_PRVCY_MBR_RELSHP_F_in.select(
    F.sum(
        F.when(
            (F.col("PRVCY_MBR_RELSHP_EFF_DT_SK") <= CurrRunCycleDate) &
            (F.col("PRVCY_MBR_RELSHP_TERM_DT_SK") >= CurrRunCycleDate),
            1
        ).otherwise(0)
    ).alias("ACTV_CT"),
    F.sum(
        F.when(
            (F.col("PRVCY_MBR_RELSHP_TERM_DT_SK") <= CurrRunCycleDate),
            1
        ).otherwise(0)
    ).alias("CANC_CT"),
    F.sum(
        F.when(
            (F.col("PRVCY_MBR_RELSHP_CRT_DT_SK") == CurrRunCycleDate),
            1
        ).otherwise(0)
    ).alias("NEW_CT")
)
df_xfrm_Relshp = df_xfrm_Relshp_agg.withColumn("PRVCY_TRK_SUM_TYP_CD", F.lit("MBRRELSHP")) \
    .withColumn("SUM_DT", F.lit(CurrRunCycleDate))

# fnl_data (PxFunnel): union of xfrm_Auth, xfrm_Conf, xfrm_Dsclsr, xfrm_Relshp
df_fnl_data = df_xfrm_Auth.select(
    "PRVCY_TRK_SUM_TYP_CD","ACTV_CT","CANC_CT","NEW_CT","SUM_DT"
).unionByName(
    df_xfrm_Conf.select("PRVCY_TRK_SUM_TYP_CD","ACTV_CT","CANC_CT","NEW_CT","SUM_DT")
).unionByName(
    df_xfrm_Dsclsr.select("PRVCY_TRK_SUM_TYP_CD","ACTV_CT","CANC_CT","NEW_CT","SUM_DT")
).unionByName(
    df_xfrm_Relshp.select("PRVCY_TRK_SUM_TYP_CD","ACTV_CT","CANC_CT","NEW_CT","SUM_DT")
)

# rdp_Sk (PxRemDup)
df_rdp_Sk = dedup_sort(
    df_fnl_data,
    partition_cols=["PRVCY_TRK_SUM_TYP_CD"],
    sort_cols=[("PRVCY_TRK_SUM_TYP_CD","A")]
).select(
    "PRVCY_TRK_SUM_TYP_CD",
    "ACTV_CT",
    "CANC_CT",
    "NEW_CT",
    "SUM_DT"
)

# lkp_Codes (PxLookup) - left join rdp_Sk with rdp_Cd
df_lkp_Codes = df_rdp_Sk.alias("lnk_rdp_out").join(
    df_rdp_Cd.alias("lkp_MajPrctcCatCd_ref"),
    on=[F.col("lnk_rdp_out.PRVCY_TRK_SUM_TYP_CD") == F.col("lkp_MajPrctcCatCd_ref.SRC_DRVD_LKUP_VAL")],
    how="left"
).select(
    F.col("lnk_rdp_out.PRVCY_TRK_SUM_TYP_CD").alias("PRVCY_TRK_SUM_TYP_CD"),
    F.col("lnk_rdp_out.ACTV_CT").alias("ACTV_CT"),
    F.col("lnk_rdp_out.CANC_CT").alias("CANC_CT"),
    F.col("lnk_rdp_out.NEW_CT").alias("NEW_CT"),
    F.col("lnk_rdp_out.SUM_DT").alias("SUM_DT"),
    F.col("lkp_MajPrctcCatCd_ref.CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("lkp_MajPrctcCatCd_ref.TRGT_CD").alias("TRGT_CD"),
    F.col("lkp_MajPrctcCatCd_ref.TRGT_CD_NM").alias("TRGT_CD_NM")
)

# xfrm_BusinessLogic (CTransformerStage) - three output links from the same input
df_main_data_in = (
    df_lkp_Codes
    .withColumn("SRC_SYS_CD", F.lit("FACETS"))
    .withColumn(
        "PRVCY_TRK_SUM_TYP_CD",
        F.when(
            F.col("TRGT_CD").isNull() | (F.length(trim(F.col("TRGT_CD"))) == 0),
            F.lit("NA")
        ).otherwise(F.col("TRGT_CD"))
    )
    .withColumn("SUM_DT_SK", F.col("SUM_DT"))
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", F.lit(CurrRunCycleDate))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.lit(CurrRunCycleDate))
    .withColumn("ACTV_CT", F.col("ACTV_CT"))
    .withColumn("CANC_CT", F.col("CANC_CT"))
    .withColumn(
        "PRVCY_TRK_SUM_TYP_NM",
        F.when(
            F.col("TRGT_CD_NM").isNull() | (F.length(trim(F.col("TRGT_CD_NM"))) == 0),
            F.lit("NA")
        ).otherwise(F.col("TRGT_CD_NM"))
    )
    .withColumn("NEW_CT", F.col("NEW_CT"))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(CurrRunCycle))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(CurrRunCycle))
    .withColumn(
        "PRVCY_TRK_SUM_TYP_CD_SK",
        F.when(
            F.col("CD_MPPNG_SK").isNull() | (F.length(trim(F.col("CD_MPPNG_SK"))) == 0),
            F.lit("NA")
        ).otherwise(F.col("CD_MPPNG_SK"))
    )
    .withColumn("PRVCY_TRK_SUM_SK", F.lit(0))
    .select(
        "SRC_SYS_CD",
        "PRVCY_TRK_SUM_TYP_CD",
        "SUM_DT_SK",
        "CRT_RUN_CYC_EXCTN_DT_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        "ACTV_CT",
        "CANC_CT",
        "PRVCY_TRK_SUM_TYP_NM",
        "NEW_CT",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "PRVCY_TRK_SUM_TYP_CD_SK",
        "PRVCY_TRK_SUM_SK"
    )
)

# lnk_UNK_out => single row
df_unk_out = spark.createDataFrame(
    [
        (
            "UNK",  # SRC_SYS_CD
            "UNK",  # PRVCY_TRK_SUM_TYP_CD
            "1753-01-01",  # SUM_DT_SK
            "1753-01-01",  # CRT_RUN_CYC_EXCTN_DT_SK
            "1753-01-01",  # LAST_UPDT_RUN_CYC_EXCTN_DT_SK
            0,             # ACTV_CT
            0,             # CANC_CT
            "UNK",         # PRVCY_TRK_SUM_TYP_NM
            0,             # NEW_CT
            100,           # CRT_RUN_CYC_EXCTN_SK
            100,           # LAST_UPDT_RUN_CYC_EXCTN_SK
            0,             # PRVCY_TRK_SUM_TYP_CD_SK
            0              # PRVCY_TRK_SUM_SK
        )
    ],
    [
        "SRC_SYS_CD",
        "PRVCY_TRK_SUM_TYP_CD",
        "SUM_DT_SK",
        "CRT_RUN_CYC_EXCTN_DT_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        "ACTV_CT",
        "CANC_CT",
        "PRVCY_TRK_SUM_TYP_NM",
        "NEW_CT",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "PRVCY_TRK_SUM_TYP_CD_SK",
        "PRVCY_TRK_SUM_SK"
    ]
)

# lnk_NA_out => single row
df_na_out = spark.createDataFrame(
    [
        (
            "NA",   # SRC_SYS_CD
            "NA",   # PRVCY_TRK_SUM_TYP_CD
            "1753-01-01",  # SUM_DT_SK
            "1753-01-01",  # CRT_RUN_CYC_EXCTN_DT_SK
            "1753-01-01",  # LAST_UPDT_RUN_CYC_EXCTN_DT_SK
            0,             # ACTV_CT
            0,             # CANC_CT
            "NA",          # PRVCY_TRK_SUM_TYP_NM
            0,             # NEW_CT
            100,           # CRT_RUN_CYC_EXCTN_SK
            100,           # LAST_UPDT_RUN_CYC_EXCTN_SK
            1,             # PRVCY_TRK_SUM_TYP_CD_SK
            1              # PRVCY_TRK_SUM_SK
        )
    ],
    [
        "SRC_SYS_CD",
        "PRVCY_TRK_SUM_TYP_CD",
        "SUM_DT_SK",
        "CRT_RUN_CYC_EXCTN_DT_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        "ACTV_CT",
        "CANC_CT",
        "PRVCY_TRK_SUM_TYP_NM",
        "NEW_CT",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "PRVCY_TRK_SUM_TYP_CD_SK",
        "PRVCY_TRK_SUM_SK"
    ]
)

# fnl_dataLinks (PxFunnel) => union of UNK_out, NA_out, Main_Data_in
df_fnl_dataLinks = df_unk_out.unionByName(df_na_out).unionByName(df_main_data_in)

# ds_PRVCY_TRK_SUM_F_Load (PxDataSet) => write to parquet
df_final = df_fnl_dataLinks.select(
    "PRVCY_TRK_SUM_SK",
    "SRC_SYS_CD",
    "PRVCY_TRK_SUM_TYP_CD",
    "SUM_DT_SK",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "ACTV_CT",
    "CANC_CT",
    "PRVCY_TRK_SUM_TYP_NM",
    "NEW_CT",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "PRVCY_TRK_SUM_TYP_CD_SK"
).withColumn(
    "SUM_DT_SK", F.rpad(F.col("SUM_DT_SK"), 10, " ")
).withColumn(
    "CRT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ")
).withColumn(
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ")
)

write_files(
    df_final,
    f"ds/PRVCY_TRK_SUM_F.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)