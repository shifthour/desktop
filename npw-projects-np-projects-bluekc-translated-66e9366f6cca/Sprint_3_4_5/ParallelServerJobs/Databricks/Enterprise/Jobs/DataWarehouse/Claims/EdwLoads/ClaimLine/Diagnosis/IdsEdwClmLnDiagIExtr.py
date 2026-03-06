# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2004 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC   Pulls data from (idsdevl.ClmLnDiag, idsdevl.DIAG_CD) and
# MAGIC                            edwdevl.ClmLnDiag_d
# MAGIC   Loads the data from EDW and IDS into two hash files for subsequent comparison.
# MAGIC   The comparison will determine rows that have changed and whether to generate a new history record.
# MAGIC 
# MAGIC INPUTS:
# MAGIC 	CLM_LN_DIAG
# MAGIC                 DIAG_CD
# MAGIC                 CLM_LN_DIAG_I
# MAGIC                 
# MAGIC                 /dev/null - just to init the compare hash-file
# MAGIC   
# MAGIC HASH FILES:
# MAGIC                 hf_cdma_codes           - Load the CDMA codes to get cd a nd cd_nm from cd_sk
# MAGIC                 hf_clm_ln_diag_lkup
# MAGIC 
# MAGIC TRANSFORMS:  
# MAGIC                   TRIM all the fields that are extracted or used in lookups
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC                   IDS - read from source, lookup all code SK values and get the natural codes
# MAGIC                   EDW - Read from source and dump to hf. Do nothing but trim the fields.
# MAGIC                   /dev/null - no records, just used to re-init the IUD hf
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC               Judy Reynolds 11/15/2004-   Originally Programmed
# MAGIC               Brent leland     03/24/2006    Changed parameters to evironment variables.
# MAGIC                                                              Took out trim() on SK values.
# MAGIC               Brent Leland    05/11/2006    Removed trim() on codes and IDs
# MAGIC               Brent Leland   07/21/2006     Changed SQL lookup to a hash lookup for DIAG_CD
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer          Date                 Project/Altiris #         Change Description                                                                                                Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------        --------------------     ------------------------        -----------------------------------------------------------------------------------------------------------------               --------------------------------       -------------------------------   ----------------------------       
# MAGIC Rick Henry        2012-05-08        4896                       Added DIAG_CD_TYP_CD, DIAG_CD_TYP_NM, DIAG_CD_TYPE_CD_SK         EnterpriseNewDevl
# MAGIC Ralph Tucker    2012-05-13        4896                       Added Standards on reference links because of null returns on 1st run                   EnterpriseNewDevl           2012-05-18                Sharon andrew
# MAGIC 
# MAGIC Pooja Sunkara  2013-10-01         5114                      Rewrite in Parallel                                                                                                     EnterpriseWrhsDevl       Peter Marshall             12/23/2013

# MAGIC Extracts all data from IDS reference table CD_MPPNG,
# MAGIC DIAG_CD
# MAGIC 
# MAGIC Code SK lookups for Denormalization
# MAGIC Lookup Keys:
# MAGIC CD_MPPNG_SK
# MAGIC DIAG_CD_SK
# MAGIC Add CRT_RUN_CYC_EXCTN_DT_SK ,LAST_UPDT_RUN_CYC_EXCTN_DT_SK,NA and UNK rows
# MAGIC Job name:
# MAGIC IdsEdwClmLnDiagDimExtr
# MAGIC EDW Claim Line Diagnosis extract from IDS
# MAGIC Write CLM_LN_DIAG_I Data into a Sequential file for Load Ready Job.
# MAGIC Read data from source tables CLM_LN_DIAG , 
# MAGIC W_EDW_ETL_DRVR
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType
)
# COMMAND ----------
# MAGIC %run ../../../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
EDWRunCycle = get_widget_value('EDWRunCycle','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query_db2_CLM_LN_DIAG_in = f"""
SELECT 
CLM_LN_DIAG_SK,
dg.CLM_ID,
CLM_LN_SEQ_NO,
dg.SRC_SYS_CD_SK,
CLM_LN_DIAG_ORDNL_CD_SK,
CLM_LN_SK,
DIAG_CD_SK
FROM
{IDSOwner}.CLM_LN_DIAG dg,
{IDSOwner}.W_EDW_ETL_DRVR dr
WHERE
dg.CLM_ID = dr.CLM_ID AND
dg.SRC_SYS_CD_SK = dr.SRC_SYS_CD_SK
"""
df_db2_CLM_LN_DIAG_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_CLM_LN_DIAG_in)
    .load()
)

extract_query_db2_DIAG_CD_in = f"""
SELECT
A.DIAG_CD_SK,
A.DIAG_CD,
A.DIAG_CD_TYP_CD,
A.DIAG_CD_TYP_CD_SK,
A.DIAG_CD_DESC,
B.TRGT_CD_NM
FROM
{IDSOwner}.DIAG_CD A,
{IDSOwner}.CD_MPPNG B
WHERE
A.DIAG_CD_TYP_CD_SK = B.CD_MPPNG_SK
"""
df_db2_DIAG_CD_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_DIAG_CD_in)
    .load()
)

extract_query_db2_CD_MPPNG2_in = f"""
SELECT 
CD_MPPNG_SK,
COALESCE(TRGT_CD,'UNK') AS TRGT_CD,
COALESCE(TRGT_CD_NM,'UNK') AS TRGT_CD_NM
FROM
{IDSOwner}.CD_MPPNG
"""
df_db2_CD_MPPNG2_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_CD_MPPNG2_in)
    .load()
)

df_ref_clmlndiagiordnl = df_db2_CD_MPPNG2_in.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_ref_SrcSysCd = df_db2_CD_MPPNG2_in.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_Lkp_CdmaCodes = (
    df_db2_CLM_LN_DIAG_in.alias("p")
    .join(df_ref_clmlndiagiordnl.alias("c1"), F.col("p.CLM_LN_DIAG_ORDNL_CD_SK") == F.col("c1.CD_MPPNG_SK"), "left")
    .join(df_ref_SrcSysCd.alias("c2"), F.col("p.SRC_SYS_CD_SK") == F.col("c2.CD_MPPNG_SK"), "left")
    .join(df_db2_DIAG_CD_in.alias("d"), F.col("p.DIAG_CD_SK") == F.col("d.DIAG_CD_SK"), "left")
    .select(
        F.col("p.CLM_LN_DIAG_SK").alias("CLM_LN_DIAG_SK"),
        F.col("p.CLM_ID").alias("CLM_ID"),
        F.col("p.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
        F.col("c1.TRGT_CD").alias("CLM_LN_DIAG_ORDNL_CD"),
        F.col("c2.TRGT_CD").alias("SRC_SYS_CD"),
        F.col("p.CLM_LN_DIAG_ORDNL_CD_SK").alias("CLM_LN_DIAG_ORDNL_CD_SK"),
        F.col("c1.TRGT_CD_NM").alias("CLM_LN_DIAG_ORDNL_NM"),
        F.col("p.CLM_LN_SK").alias("CLM_LN_SK"),
        F.col("p.DIAG_CD_SK").alias("DIAG_CD_SK"),
        F.col("d.DIAG_CD").alias("LkupDIAG_CD"),
        F.col("d.DIAG_CD_TYP_CD").alias("LkupDIAG_CD_TYP_CD"),
        F.col("d.DIAG_CD_TYP_CD_SK").alias("LkupDIAG_CD_TYP_CD_SK"),
        F.col("d.DIAG_CD_DESC").alias("LkupDIAG_CD_DESC"),
        F.col("d.TRGT_CD_NM").alias("LkupDIAG_CD_TYP_NM")
    )
)

df_Ink_IdsEdwClmLnDiagDimExtr_Main = (
    df_Lkp_CdmaCodes
    .filter((F.col("CLM_LN_DIAG_SK") != 0) & (F.col("CLM_LN_DIAG_SK") != 1))
    .withColumn(
        "SRC_SYS_CD",
        F.when(
            F.col("SRC_SYS_CD").isNull() | (trim(F.col("SRC_SYS_CD")) == ''),
            F.lit("UNK")
        ).otherwise(F.col("SRC_SYS_CD"))
    )
    .withColumn(
        "CLM_LN_DIAG_ORDNL_CD",
        F.when(
            F.col("CLM_LN_DIAG_ORDNL_CD").isNull() | (trim(F.col("CLM_LN_DIAG_ORDNL_CD")) == ''),
            F.lit("UNK")
        ).otherwise(F.col("CLM_LN_DIAG_ORDNL_CD"))
    )
    .withColumn(
        "CLM_LN_DIAG_ORDNL_NM",
        F.when(
            F.col("CLM_LN_DIAG_ORDNL_NM").isNull() | (trim(F.col("CLM_LN_DIAG_ORDNL_NM")) == ''),
            F.lit("UNKNOWN")
        ).otherwise(F.col("CLM_LN_DIAG_ORDNL_NM"))
    )
    .withColumn(
        "DIAG_CD",
        F.when(
            F.col("LkupDIAG_CD").isNull() | (trim(F.col("LkupDIAG_CD")) == ''),
            F.lit("UNK")
        ).otherwise(F.col("LkupDIAG_CD"))
    )
    .withColumn(
        "DIAG_CD_DESC",
        F.when(
            F.col("LkupDIAG_CD_DESC").isNull() | (trim(F.col("LkupDIAG_CD_DESC")) == ''),
            F.lit("UNK")
        ).otherwise(F.col("LkupDIAG_CD_DESC"))
    )
    .withColumn(
        "DIAG_CD_TYP_CD",
        F.when(
            F.col("LkupDIAG_CD_TYP_CD").isNull() | (trim(F.col("LkupDIAG_CD_TYP_CD")) == ''),
            F.lit("UNK")
        ).otherwise(F.col("LkupDIAG_CD_TYP_CD"))
    )
    .withColumn(
        "DIAG_CD_TYP_NM",
        F.when(
            F.col("LkupDIAG_CD_TYP_NM").isNull() | (trim(F.col("LkupDIAG_CD_TYP_NM")) == ''),
            F.lit("UNKNOWN")
        ).otherwise(F.col("LkupDIAG_CD_TYP_NM"))
    )
    .withColumn(
        "DIAG_CD_TYP_CD_SK",
        F.when(
            F.col("LkupDIAG_CD_TYP_CD_SK").isNull(),
            F.lit(0)
        ).otherwise(F.col("LkupDIAG_CD_TYP_CD_SK"))
    )
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", F.lit(EDWRunCycleDate))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.lit(EDWRunCycleDate))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(EDWRunCycle))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(EDWRunCycle))
    .select(
        F.col("CLM_LN_DIAG_SK"),
        F.col("SRC_SYS_CD"),
        F.col("CLM_ID"),
        F.col("CLM_LN_DIAG_ORDNL_CD"),
        F.col("CLM_LN_SEQ_NO"),
        F.col("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        F.col("CLM_LN_DIAG_ORDNL_NM"),
        F.col("DIAG_CD"),
        F.col("DIAG_CD_DESC"),
        F.col("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("CLM_LN_DIAG_ORDNL_CD_SK"),
        F.col("DIAG_CD_SK"),
        F.col("CLM_LN_SK"),
        F.col("DIAG_CD_TYP_CD"),
        F.col("DIAG_CD_TYP_NM"),
        F.col("DIAG_CD_TYP_CD_SK")
    )
)

df_UNKRow = spark.createDataFrame(
    [
        (
            0,
            'UNK',
            'UNK',
            'UNK',
            0,
            '1753-01-01',
            '1753-01-01',
            'UNK',
            'UNK',
            '',
            100,
            100,
            0,
            0,
            0,
            'UNK',
            'UNK',
            0
        )
    ],
    schema=StructType([
        StructField("CLM_LN_DIAG_SK", IntegerType(), True),
        StructField("SRC_SYS_CD", StringType(), True),
        StructField("CLM_ID", StringType(), True),
        StructField("CLM_LN_DIAG_ORDNL_CD", StringType(), True),
        StructField("CLM_LN_SEQ_NO", IntegerType(), True),
        StructField("CRT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
        StructField("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
        StructField("CLM_LN_DIAG_ORDNL_NM", StringType(), True),
        StructField("DIAG_CD", StringType(), True),
        StructField("DIAG_CD_DESC", StringType(), True),
        StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), True),
        StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True),
        StructField("CLM_LN_DIAG_ORDNL_CD_SK", IntegerType(), True),
        StructField("DIAG_CD_SK", IntegerType(), True),
        StructField("CLM_LN_SK", IntegerType(), True),
        StructField("DIAG_CD_TYP_CD", StringType(), True),
        StructField("DIAG_CD_TYP_NM", StringType(), True),
        StructField("DIAG_CD_TYP_CD_SK", IntegerType(), True)
    ])
)

df_NARow = spark.createDataFrame(
    [
        (
            1,
            'NA',
            'NA',
            'NA',
            0,
            '1753-01-01',
            '1753-01-01',
            'NA',
            'NA',
            '',
            100,
            100,
            1,
            1,
            1,
            'NA',
            'NA',
            1
        )
    ],
    schema=StructType([
        StructField("CLM_LN_DIAG_SK", IntegerType(), True),
        StructField("SRC_SYS_CD", StringType(), True),
        StructField("CLM_ID", StringType(), True),
        StructField("CLM_LN_DIAG_ORDNL_CD", StringType(), True),
        StructField("CLM_LN_SEQ_NO", IntegerType(), True),
        StructField("CRT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
        StructField("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", StringType(), True),
        StructField("CLM_LN_DIAG_ORDNL_NM", StringType(), True),
        StructField("DIAG_CD", StringType(), True),
        StructField("DIAG_CD_DESC", StringType(), True),
        StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), True),
        StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True),
        StructField("CLM_LN_DIAG_ORDNL_CD_SK", IntegerType(), True),
        StructField("DIAG_CD_SK", IntegerType(), True),
        StructField("CLM_LN_SK", IntegerType(), True),
        StructField("DIAG_CD_TYP_CD", StringType(), True),
        StructField("DIAG_CD_TYP_NM", StringType(), True),
        StructField("DIAG_CD_TYP_CD_SK", IntegerType(), True)
    ])
)

df_fnl_dataLinks = df_Ink_IdsEdwClmLnDiagDimExtr_Main.unionByName(df_UNKRow).unionByName(df_NARow)

df_final = (
    df_fnl_dataLinks
    .withColumn(
        "CRT_RUN_CYC_EXCTN_DT_SK",
        F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ")
    )
    .withColumn(
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ")
    )
    .select(
        "CLM_LN_DIAG_SK",
        "SRC_SYS_CD",
        "CLM_ID",
        "CLM_LN_DIAG_ORDNL_CD",
        "CLM_LN_SEQ_NO",
        "CRT_RUN_CYC_EXCTN_DT_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        "CLM_LN_DIAG_ORDNL_NM",
        "DIAG_CD",
        "DIAG_CD_DESC",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "CLM_LN_DIAG_ORDNL_CD_SK",
        "DIAG_CD_SK",
        "CLM_LN_SK",
        "DIAG_CD_TYP_CD",
        "DIAG_CD_TYP_NM",
        "DIAG_CD_TYP_CD_SK"
    )
)

write_files(
    df_final,
    f"{adls_path}/load/CLM_LN_DIAG_I.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)