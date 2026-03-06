# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2005, 2006, 2007, 2009, 2010 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC Called by: FctsIdsLfstylRateFctrCntl
# MAGIC                     
# MAGIC 
# MAGIC Modifications:                        
# MAGIC                                                      Project/                                                                                                                                                                     Code                   Date
# MAGIC Developer               Date              Altiris #                              Change Description                                              Environment                                       Reviewer            Reviewed
# MAGIC --------------------------     -------------------   -------------                            -----------------------------------------------------------------------------------------------------------------                       -------------------------  -------------------
# MAGIC Manasa Andru        2018-08-15    60037                              Initial Programming                                                IntegrateDev2\(9)\(9)  Abhiram Dasarathy\(9)  2018-08-24
# MAGIC                                                                                      Extracts data from the FACETS - CMC_CRFT_FCTR
# MAGIC                                                                                             table.
# MAGIC Prabhu ES             2022-03-01     S2S Remediation            MSSQL connection parameters added                  IntegrateDev5                                      Kalyan Neelam     2022-06-01

# MAGIC Job name: FctsIdsLfstylRateFctrExtrXfrm
# MAGIC Extract Data from Factes table CMC_CRTF_FCTR
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
import pyspark.sql.functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# Parameters
RunID = get_widget_value('RunID','')
RunIDTimeStamp = get_widget_value('RunIDTimeStamp','')
FacetsOwner = get_widget_value('FacetsOwner','')
IDSOwner = get_widget_value('IDSOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
ids_secret_name = get_widget_value('ids_secret_name','')

# Read from db2_CdMppng_Lkp (DB2ConnectorPX) - IDS
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
extract_query_db2_CdMppng_Lkp = f"""Select 
RTRIM(SUBSTR(LTRIM(RTRIM(SRC_CD)),1,4)) as SRC_CD,
TRGT_CD,
CD_MPPNG_SK
from {IDSOwner}.CD_MPPNG
where
SRC_SYS_CD = 'FACETS'
and SRC_CLCTN_CD = 'FACETS DBO'
and SRC_DOMAIN_NM = 'MEMBER LIFESTYLE BENEFIT'
and TRGT_CLCTN_CD = 'IDS'
and TRGT_DOMAIN_NM = 'MEMBER LIFESTYLE BENEFIT'"""
df_db2_CdMppng_Lkp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_CdMppng_Lkp)
    .load()
)

# Read from CMC_CRTF_FCTR_In (ODBCConnectorPX) - FacetsOwner
jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)
extract_query_CMC_CRTF_FCTR_In = f"""Select 
CRFT_ID, 
LTRIM(RTRIM(CRFT_MCTR_LSTY)) as SRC_CD, 
CRFT_EFF_DT, 
CRFT_TERM_DT, 
CRFT_FCTR
FROM {FacetsOwner}.CMC_CRFT_FCTR"""
df_CMC_CRTF_FCTR_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_CMC_CRTF_FCTR_In)
    .load()
)

# Join_91 (PxJoin) - inner join on SRC_CD
df_Join_91 = df_CMC_CRTF_FCTR_In.alias("Extract").join(
    df_db2_CdMppng_Lkp.alias("Lkup"),
    F.col("Extract.SRC_CD") == F.col("Lkup.SRC_CD"),
    "inner"
)

df_Join_91_out = df_Join_91.select(
    F.col("Extract.CRFT_ID").alias("CRFT_ID"),
    F.col("Extract.SRC_CD").alias("SRC_CD"),
    F.col("Extract.CRFT_EFF_DT").alias("CRFT_EFF_DT"),
    F.col("Extract.CRFT_TERM_DT").alias("CRFT_TERM_DT"),
    F.col("Extract.CRFT_FCTR").alias("CRFT_FCTR"),
    F.col("Lkup.TRGT_CD").alias("TRGT_CD"),
    F.col("Lkup.CD_MPPNG_SK").alias("CD_MPPNG_SK")
)

# BusinessRules (CTransformerStage)

# Output link → ds_Lfstyl_Rate_Fctr
df_BusinessRules_Output = (
    df_Join_91_out
    .withColumn(
        "PRI_NAT_KEY_STRING",
        F.lit("FACETS;")
        + F.col("CRFT_ID")
        + F.lit(";")
        + F.col("SRC_CD")
        + F.lit(";")
        + F.col("CRFT_EFF_DT")
    )
    .withColumn("FIRST_RECYC_TS", F.lit(RunIDTimeStamp))
    .withColumn("LFSTYL_RATE_FCTR_SK", F.lit(0))
    .withColumn("LFSTYL_RATE_FCTR_ID", F.col("CRFT_ID"))
    .withColumn(
        "MBR_LFSTYL_BNF_CD",
        F.when(
            F.col("TRGT_CD").isNull() | (F.col("TRGT_CD") == ""),
            F.lit("NA")
        ).otherwise(F.col("TRGT_CD"))
    )
    .withColumn("LFSTYL_RATE_FCTR_EFF_DT", F.col("CRFT_EFF_DT"))
    .withColumn("SRC_SYS_CD", F.lit("FACETS"))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(0))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(0))
    .withColumn(
        "MBR_LFSTYL_BNF_CD_SK",
        F.when(
            F.col("CD_MPPNG_SK").isNull() | (F.col("CD_MPPNG_SK") == ""),
            F.lit(1)
        ).otherwise(F.col("CD_MPPNG_SK"))
    )
    .withColumn("LFSTYL_RATE_FCTR_TERM_DT", F.col("CRFT_TERM_DT"))
    .withColumn(
        "LFSTYL_RATE_FCTR",
        F.when(F.col("CRFT_FCTR") == 1, F.lit(0))
        .otherwise(F.col("CRFT_FCTR") / F.lit(1000000.0))
    )
)

df_ds_Lfstyl_Rate_Fctr = df_BusinessRules_Output.select(
    "PRI_NAT_KEY_STRING",
    "FIRST_RECYC_TS",
    "LFSTYL_RATE_FCTR_SK",
    "LFSTYL_RATE_FCTR_ID",
    "MBR_LFSTYL_BNF_CD",
    "LFSTYL_RATE_FCTR_EFF_DT",
    "SRC_SYS_CD",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "MBR_LFSTYL_BNF_CD_SK",
    "LFSTYL_RATE_FCTR_TERM_DT",
    "LFSTYL_RATE_FCTR"
).withColumn(
    "LFSTYL_RATE_FCTR_ID",
    rpad(F.col("LFSTYL_RATE_FCTR_ID"), 4, " ")
)

# Output link → B_LFSTYL_RATE_FCTR (Snapshot)
df_BusinessRules_Snapshot = (
    df_Join_91_out
    .withColumn("LFSTYL_RATE_FCTR_ID", F.col("CRFT_ID"))
    .withColumn(
        "MBR_LFSTYL_BNF_CD",
        F.when(
            F.col("SRC_CD").isNull() | (F.col("SRC_CD") == ""),
            F.lit("NA")
        ).otherwise(F.col("SRC_CD"))
    )
    .withColumn("LFSTYL_RATE_FCTR_EFF_DT", F.col("CRFT_EFF_DT"))
    .withColumn("SRC_SYS_CD", F.lit("FACETS"))
)

df_B_LFSTYL_RATE_FCTR = df_BusinessRules_Snapshot.select(
    "LFSTYL_RATE_FCTR_ID",
    "MBR_LFSTYL_BNF_CD",
    "LFSTYL_RATE_FCTR_EFF_DT",
    "SRC_SYS_CD"
).withColumn(
    "LFSTYL_RATE_FCTR_ID",
    rpad(F.col("LFSTYL_RATE_FCTR_ID"), 4, " ")
)

# ds_Lfstyl_Rate_Fctr (PxDataSet) → write parquet
write_files(
    df_ds_Lfstyl_Rate_Fctr,
    f"{adls_path}/ds/LFSTYL_RATE_FCTR.LfstylRateFctr.extr.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

# B_LFSTYL_RATE_FCTR (PxSequentialFile) → write delimited
write_files(
    df_B_LFSTYL_RATE_FCTR,
    f"{adls_path}/load/B_LFSTYL_RATE_FCTR.FACETS.dat",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)