# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Calling Job:EAMFctsReconRptCntl
# MAGIC 
# MAGIC Description:This job loads the load file to EAM databse
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC ======================================================================================================================================
# MAGIC Developer                          Date                           Project/Altiris #                              Change Description                                                    Development Project               
# MAGIC ======================================================================================================================================
# MAGIC ReddySanam               2021-01-05                  US329820                                         Original Programming                                     IntegrateDev2      Kalyan Neelam    2021-01-14
# MAGIC ReddySanam               2021-01-27                  US329820                                         Added Billing and MedPln Files                      IntegrateDev2      Kalyan Neelam    2021-01-27
# MAGIC ReddySanam               2021-01-27                  US329820                                         Added MidMonth                                           IntegrateDev2      Jeyaprasanna    2021-02-02
# MAGIC ReddySanam               2021-02-03                US329820                                           Added BLMM                                                IntegrateDev2       Jeyaprasanna    2021-02-04
# MAGIC ReddySanam               2021-02-06                US329820                                           Added Dntl Pln File                                        IntegrateDev2       Jeyaprasanna    2021-02-08
# MAGIC ReddySanam               2021-02-16                US329820                                           Added EAM RxID TC 72 File                         IntegrateDev2       Jeyaprasanna    2021-02-17
# MAGIC ReddySanam               2021-02-22                US329820                                           Added AgentID file                                        IntegrateDev2       Jeyaprasanna    2021-02-23
# MAGIC                                                                                                                                   Added LICS file
# MAGIC JohnAbraham               2021-08-11                 US391328                Added EAMRPT Env variables and updated appropriate       IntegrateDev1
# MAGIC                                                                                                                 tables with these parameters
# MAGIC Arpitha V                     2024-03-20                        US 611997         AddedESRDSTRTDT_File,LISCOPAYLEVELCAT_File,          IntegrateDev2      Jeyaprasanna    2024-06-24
# MAGIC                                                                                                                    LISENDDATE_File
# MAGIC 
# MAGIC Arpitha V                     2024-07-03             US 611748,611749,       Added ESRDENDDT_File,KDTSPSTRTDT_File,                   IntegrateDev2      Jeyaprasanna    2024-08-02
# MAGIC                                                                             612321                               KDTSPSTRTDT_File, EFFDT_File

# MAGIC This Job Loads the New load file to EAM DISCREPANCY_ANALYSIS table. This job does only Inserts to the table
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../Utility_Integrate
# COMMAND ----------


EAMRPTOwner = get_widget_value('EAMRPTOwner','')
eamrpt_secret_name = get_widget_value('eamrpt_secret_name','')
runid = get_widget_value('RUNID','')

schema_data = StructType([
    StructField("GRGR_ID", StringType(), nullable=False),
    StructField("SBSB_ID", StringType(), nullable=False),
    StructField("MBI", StringType(), nullable=False),
    StructField("SOURCE1", StringType(), nullable=False),
    StructField("SOURCE2", StringType(), nullable=False),
    StructField("IDENTIFIEDBY", StringType(), nullable=False),
    StructField("IDENTIFIEDDATE", DateType(), nullable=False),
    StructField("RESOLVEDDATE", DateType(), nullable=True),
    StructField("STATUS", StringType(), nullable=False),
    StructField("DISCREPANCY", IntegerType(), nullable=False),
    StructField("NOTE", StringType(), nullable=True),
    StructField("PRIORITY", IntegerType(), nullable=False),
    StructField("AGE", IntegerType(), nullable=False),
    StructField("LEGACY", StringType(), nullable=True),
    StructField("LAST_UPDATED_DATE", DateType(), nullable=False)
])

df_Load_File = (
    spark.read.format("csv")
    .option("header", "true")
    .option("delimiter", "|")
    .option("quote", "\"")
    .option("nullValue", None)
    .schema(schema_data)
    .load(f"{adls_path}/load/FACETS_EAM_DISC_ANALYSIS_APPEND.{runid}.dat")
)

df_LBILL_Load_File = (
    spark.read.format("csv")
    .option("header", "true")
    .option("delimiter", "|")
    .option("quote", "\"")
    .option("nullValue", None)
    .schema(schema_data)
    .load(f"{adls_path}/load/FACETS_EAM_DISC_ANALYSIS_LBILL_APPEND.{runid}.dat")
)

df_MedPrdct_Load_File = (
    spark.read.format("csv")
    .option("header", "true")
    .option("delimiter", "|")
    .option("quote", "\"")
    .option("nullValue", None)
    .schema(schema_data)
    .load(f"{adls_path}/load/FACETS_EAM_DISC_ANALYSIS_MEDLPLN_APPEND.{runid}.dat")
)

df_MidMonth_Load_File = (
    spark.read.format("csv")
    .option("header", "true")
    .option("delimiter", "|")
    .option("quote", "\"")
    .option("nullValue", None)
    .schema(schema_data)
    .load(f"{adls_path}/load/FACETS_EAM_DISC_ANALYSIS_MIDM_APPEND.{runid}.dat")
)

df_BLMM_Load_File = (
    spark.read.format("csv")
    .option("header", "true")
    .option("delimiter", "|")
    .option("quote", "\"")
    .option("nullValue", None)
    .schema(schema_data)
    .load(f"{adls_path}/load/FACETS_EAM_DISC_ANALYSIS_BLMM_APPEND.{runid}.dat")
)

df_DntlPrdct_Load_File = (
    spark.read.format("csv")
    .option("header", "true")
    .option("delimiter", "|")
    .option("quote", "\"")
    .option("nullValue", None)
    .schema(schema_data)
    .load(f"{adls_path}/load/FACETS_EAM_DISC_ANALYSIS_DNTLPLN_APPEND.{runid}.dat")
)

df_RXIDTC72_File = (
    spark.read.format("csv")
    .option("header", "true")
    .option("delimiter", "|")
    .option("quote", "\"")
    .option("nullValue", None)
    .schema(schema_data)
    .load(f"{adls_path}/load/FACETS_EAM_DISC_ANALYSIS_RXIDTC72_APPEND.{runid}.dat")
)

df_AGNT_File = (
    spark.read.format("csv")
    .option("header", "true")
    .option("delimiter", "|")
    .option("quote", "\"")
    .option("nullValue", None)
    .schema(schema_data)
    .load(f"{adls_path}/load/FACETS_EAM_DISC_ANALYSIS_AGNT_APPEND.{runid}.dat")
)

df_LICS_File = (
    spark.read.format("csv")
    .option("header", "true")
    .option("delimiter", "|")
    .option("quote", "\"")
    .option("nullValue", None)
    .schema(schema_data)
    .load(f"{adls_path}/load/FACETS_EAM_DISC_ANALYSIS_LICS_APPEND.{runid}.dat")
)

df_ESRDSTRTDT_File = (
    spark.read.format("csv")
    .option("header", "true")
    .option("delimiter", "|")
    .option("quote", "\"")
    .option("nullValue", None)
    .schema(schema_data)
    .load(f"{adls_path}/load/FACETS_EAM_DISC_ANALYSIS_ESRDSTRTDT_APPEND.{runid}.dat")
)

df_ESRDENDDT_File = (
    spark.read.format("csv")
    .option("header", "true")
    .option("delimiter", "|")
    .option("quote", "\"")
    .option("nullValue", None)
    .schema(schema_data)
    .load(f"{adls_path}/load/FACETS_EAM_DISC_ANALYSIS_ESRDENDDT_APPEND.{runid}.dat")
)

df_EFFDT_File = (
    spark.read.format("csv")
    .option("header", "true")
    .option("delimiter", "|")
    .option("quote", "\"")
    .option("nullValue", None)
    .schema(schema_data)
    .load(f"{adls_path}/load/FACETS_EAM_DISC_ANALYSIS_EFFDT_APPEND.{runid}.dat")
)

df_KDTSPSTRTDT_File = (
    spark.read.format("csv")
    .option("header", "true")
    .option("delimiter", "|")
    .option("quote", "\"")
    .option("nullValue", None)
    .schema(schema_data)
    .load(f"{adls_path}/load/FACETS_EAM_DISC_ANALYSIS_KDTSPSTRTDT_APPEND.{runid}.dat")
)

df_KDTSPENDDT_File = (
    spark.read.format("csv")
    .option("header", "true")
    .option("delimiter", "|")
    .option("quote", "\"")
    .option("nullValue", None)
    .schema(schema_data)
    .load(f"{adls_path}/load/FACETS_EAM_DISC_ANALYSIS_KDTSPENDDT_APPEND.{runid}.dat")
)

df_LISCOPAYLEVELCAT_File = (
    spark.read.format("csv")
    .option("header", "true")
    .option("delimiter", "|")
    .option("quote", "\"")
    .option("nullValue", None)
    .schema(schema_data)
    .load(f"{adls_path}/load/FACETS_EAM_DISC_ANALYSIS_LISCOPAYLEVELCAT_APPEND.{runid}.dat")
)

df_LISENDDATE_File = (
    spark.read.format("csv")
    .option("header", "true")
    .option("delimiter", "|")
    .option("quote", "\"")
    .option("nullValue", None)
    .schema(schema_data)
    .load(f"{adls_path}/load/FACETS_EAM_DISC_ANALYSIS_LISENDDATE_APPEND.{runid}.dat")
)

df_Fnl = (
    df_MedPrdct_Load_File
    .unionByName(df_LBILL_Load_File)
    .unionByName(df_Load_File)
    .unionByName(df_MidMonth_Load_File)
    .unionByName(df_BLMM_Load_File)
    .unionByName(df_DntlPrdct_Load_File)
    .unionByName(df_RXIDTC72_File)
    .unionByName(df_AGNT_File)
    .unionByName(df_LICS_File)
    .unionByName(df_ESRDSTRTDT_File)
    .unionByName(df_ESRDENDDT_File)
    .unionByName(df_EFFDT_File)
    .unionByName(df_KDTSPSTRTDT_File)
    .unionByName(df_KDTSPENDDT_File)
    .unionByName(df_LISCOPAYLEVELCAT_File)
    .unionByName(df_LISENDDATE_File)
)

df_EAM = (
    df_Fnl
    .withColumn("LEGACY", F.rpad(F.col("LEGACY"), 1, " "))
    .select(
        "GRGR_ID",
        "SBSB_ID",
        "MBI",
        "SOURCE1",
        "SOURCE2",
        "IDENTIFIEDBY",
        "IDENTIFIEDDATE",
        "RESOLVEDDATE",
        "STATUS",
        "DISCREPANCY",
        "NOTE",
        "PRIORITY",
        "AGE",
        "LEGACY",
        "LAST_UPDATED_DATE"
    )
)

jdbc_url, jdbc_props = get_db_config(eamrpt_secret_name)

execute_dml(f"DROP TABLE IF EXISTS STAGING.FacetsEAMDiscAnalysisNewLoad_EAM_temp", jdbc_url, jdbc_props)

df_EAM.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.FacetsEAMDiscAnalysisNewLoad_EAM_temp") \
    .mode("append") \
    .save()

merge_sql = f"""
MERGE INTO {EAMRPTOwner}.DISCREPANCY_ANALYSIS AS T
USING STAGING.FacetsEAMDiscAnalysisNewLoad_EAM_temp AS S
ON 1=0
WHEN NOT MATCHED THEN INSERT
(
GRGR_ID,
SBSB_ID,
MBI,
SOURCE1,
SOURCE2,
IDENTIFIEDBY,
IDENTIFIEDDATE,
RESOLVEDDATE,
STATUS,
DISCREPANCY,
NOTE,
PRIORITY,
AGE,
LEGACY,
LAST_UPDATED_DATE
)
VALUES
(
S.GRGR_ID,
S.SBSB_ID,
S.MBI,
S.SOURCE1,
S.SOURCE2,
S.IDENTIFIEDBY,
S.IDENTIFIEDDATE,
S.RESOLVEDDATE,
S.STATUS,
S.DISCREPANCY,
S.NOTE,
S.PRIORITY,
S.AGE,
S.LEGACY,
S.LAST_UPDATED_DATE
);
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)