# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC Â© Copyright 2009 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC CALLED BY:  EdwWebProvSeq
# MAGIC 
# MAGIC PROCESSING:   Retrieves the data from EDW PROV_NTWK_D table and loads into the Web Provider Directory database
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                                   Date                 \(9)Project/Altiris #      \(9)Change Description                                        \(9)Development Project      Code Reviewer          Date Reviewed       
# MAGIC ---------------------------                        --------------------     \(9)------------------------      \(9)-----------------------------------------------------------------------         --------------------------------       -------------------------------   ----------------------------       
# MAGIC Bhoomi Dasari\(9)                   2009-05-29\(9)3500             \(9)Original Programming\(9)\(9)\(9)devlEDWnew                  Steph Goddard          06/03/2009
# MAGIC 
# MAGIC Steph Goddard                           10/28/2009             TTR624                  Change where clause to join two tables to join         devlEDWnew                  SAndrew                  2009-10-29
# MAGIC                                                                                                   on A.NTWK_ID = B.NTWK_ID instead of A to A
# MAGIC 
# MAGIC Bhoomi Dasari                             08/15/2012             PCMH-4837          Added new logic for PCMH indicator (PCMH_LKUP)  EnterpriseCurDevl             sharon andrew         2012-08-23       
# MAGIC 
# MAGIC Rajasekhar Mangalampally         06/07/2013              5114                    Original Programming  (Server to Parallel conversion)                                         Jag Yelavarthi          2013-08-25
# MAGIC Hugh Sisson                               2023-02-08           ProdSupp                Added to the primary query:
# MAGIC                                                                                                                  A.SRC_SYS_CD = B.SRC_SYS_CD AND

# MAGIC This is Source extract data from an EDW table
# MAGIC 
# MAGIC Job Name:EdwDMProvDirProvNtwkExtr
# MAGIC 
# MAGIC Table:
# MAGIC PROV_DIR_DM_PROV_NTWK
# MAGIC Read from source table PROV_NTWK_D from EDW. Apply Run Cycle filters to get just the needed rows forward
# MAGIC This is a load ready file conforms to the target table structure.
# MAGIC 
# MAGIC Write PROV_DIR_DM_PROV_NTWK Data into a Sequential file for Load Job.
# MAGIC Add Defaults and Null Handling
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType, StructType, StructField
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Enterprise
# COMMAND ----------


edw_secret_name = get_widget_value('edw_secret_name','')
EDWOwner = get_widget_value('EDWOwner','')
CurrDate = get_widget_value('CurrDate','')
LastUpdtDt = get_widget_value('LastUpdtDt','')

jdbc_url, jdbc_props = get_db_config(edw_secret_name)

df_db2_PCMH_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""
SELECT 
  PROV_NTWK_D.PROV_SK,
  PROV_NTWK_D.NTWK_ID
FROM 
{EDWOwner}.PROV_NTWK_D PROV_NTWK_D
WHERE
PROV_NTWK_D.PROV_NTWK_DIR_IN = 'Y'
AND PROV_NTWK_D.PROV_NTWK_EFF_DT_SK <= '{CurrDate}'
AND PROV_NTWK_D.PROV_NTWK_TERM_DT_SK > '{CurrDate}'
AND PROV_NTWK_D.PROV_NTWK_PFX_ID = 'PCMH'
"""
    )
    .load()
)

df_db2_PROV_NTWK_D_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""
SELECT
A.SRC_SYS_CD,
A.NTWK_ID,
A.PROV_ID,
A.PROV_NTWK_PFX_ID,
A.PROV_NTWK_EFF_DT_SK,
A.NTWK_NM,
A.NTWK_SH_NM,
A.PROV_NTWK_ACPTNG_MCAID_PATN_IN,
A.PROV_NTWK_ACPTNG_MCARE_PATN_IN,
A.PROV_NTWK_ACPTNG_PATN_IN,
A.PROV_NTWK_AUTO_PCP_ASGMT_IN,
A.PROV_NTWK_DIR_IN,
A.PROV_NTWK_GNDR_ACPTD_CD,
A.PROV_NTWK_GNDR_ACPTD_NM,
A.PROV_NTWK_MAX_PATN_AGE,
A.PROV_NTWK_MIN_PATN_AGE,
A.PROV_NTWK_PCP_IN,
A.PROV_NTWK_PROV_DIR_IN,
A.PROV_NTWK_TERM_DT_SK,
B.PROV_TERM_DT_SK,
A.PROV_SK
FROM 
{EDWOwner}.PROV_NTWK_D A, 
{EDWOwner}.PROV_D B, 
{EDWOwner}.NTWK_D C
WHERE 
A.PROV_ID = B.PROV_ID 
AND A.SRC_SYS_CD = B.SRC_SYS_CD
AND A.NTWK_ID = C.NTWK_ID
AND A.SRC_SYS_CD = 'FACETS'
AND C.NTWK_DIR_CD IN ('ALLDIR','WEBDIR')
AND A.PROV_NTWK_DIR_IN = 'Y'
AND A.PROV_NTWK_EFF_DT_SK <= '{CurrDate}'
AND A.PROV_NTWK_TERM_DT_SK > '{CurrDate}'
AND B.PROV_TERM_DT_SK > '{CurrDate}'
AND A.PROV_NTWK_SK NOT IN (0,1)
"""
    )
    .load()
)

df_db2_NTWK_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""
SELECT 
A.NTWK_ID,
A.PROV_ID,
A.PROV_NTWK_PFX_ID,
A.PROV_NTWK_EFF_DT_SK,
A.PROV_NTWK_TERM_DT_SK,
B.NTWK_TYP_CD
FROM 
{EDWOwner}.PROV_NTWK_D A,
{EDWOwner}.NTWK_D B
WHERE
A.NTWK_ID <> 'NA'
AND A.PROV_NTWK_EFF_DT_SK <= '{CurrDate}'
AND A.PROV_NTWK_TERM_DT_SK > '{CurrDate}'
AND A.NTWK_ID = B.NTWK_ID
AND B.NTWK_TYP_CD <> 'DFLT'
AND B.NTWK_TYP_CD <> 'INVLD'
"""
    )
    .load()
)

df_lkp_ntwk = (
    df_db2_PROV_NTWK_D_Extr.alias("lnk_EdwDMProvDirProvNtwk_InABC")
    .join(
        df_db2_NTWK_Extr.alias("Ref_ntwk"),
        (
            F.col("lnk_EdwDMProvDirProvNtwk_InABC.NTWK_ID") == F.col("Ref_ntwk.NTWK_ID")
        )
        & (
            F.col("lnk_EdwDMProvDirProvNtwk_InABC.PROV_ID") == F.col("Ref_ntwk.PROV_ID")
        )
        & (
            F.col("lnk_EdwDMProvDirProvNtwk_InABC.PROV_NTWK_PFX_ID")
            == F.col("Ref_ntwk.PROV_NTWK_PFX_ID")
        )
        & (
            F.col("lnk_EdwDMProvDirProvNtwk_InABC.PROV_NTWK_EFF_DT_SK")
            == F.col("Ref_ntwk.PROV_NTWK_EFF_DT_SK")
        ),
        "left",
    )
    .select(
        F.col("lnk_EdwDMProvDirProvNtwk_InABC.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("lnk_EdwDMProvDirProvNtwk_InABC.NTWK_ID").alias("NTWK_ID"),
        F.col("lnk_EdwDMProvDirProvNtwk_InABC.PROV_ID").alias("PROV_ID"),
        F.col("lnk_EdwDMProvDirProvNtwk_InABC.PROV_NTWK_PFX_ID").alias("PROV_NTWK_PFX_ID"),
        F.col("lnk_EdwDMProvDirProvNtwk_InABC.PROV_NTWK_EFF_DT_SK").alias("PROV_NTWK_EFF_DT_SK"),
        F.col("lnk_EdwDMProvDirProvNtwk_InABC.NTWK_NM").alias("NTWK_NM"),
        F.col("lnk_EdwDMProvDirProvNtwk_InABC.NTWK_SH_NM").alias("NTWK_SH_NM"),
        F.col("lnk_EdwDMProvDirProvNtwk_InABC.PROV_NTWK_ACPTNG_MCAID_PATN_IN").alias("PROV_NTWK_ACPTNG_MCAID_PATN_IN"),
        F.col("lnk_EdwDMProvDirProvNtwk_InABC.PROV_NTWK_ACPTNG_MCARE_PATN_IN").alias("PROV_NTWK_ACPTNG_MCARE_PATN_IN"),
        F.col("lnk_EdwDMProvDirProvNtwk_InABC.PROV_NTWK_ACPTNG_PATN_IN").alias("PROV_NTWK_ACPTNG_PATN_IN"),
        F.col("lnk_EdwDMProvDirProvNtwk_InABC.PROV_NTWK_AUTO_PCP_ASGMT_IN").alias("PROV_NTWK_AUTO_PCP_ASGMT_IN"),
        F.col("lnk_EdwDMProvDirProvNtwk_InABC.PROV_NTWK_DIR_IN").alias("PROV_NTWK_DIR_IN"),
        F.col("lnk_EdwDMProvDirProvNtwk_InABC.PROV_NTWK_GNDR_ACPTD_CD").alias("PROV_NTWK_GNDR_ACPTD_CD"),
        F.col("lnk_EdwDMProvDirProvNtwk_InABC.PROV_NTWK_GNDR_ACPTD_NM").alias("PROV_NTWK_GNDR_ACPTD_NM"),
        F.col("lnk_EdwDMProvDirProvNtwk_InABC.PROV_NTWK_MAX_PATN_AGE").alias("PROV_NTWK_MAX_PATN_AGE"),
        F.col("lnk_EdwDMProvDirProvNtwk_InABC.PROV_NTWK_MIN_PATN_AGE").alias("PROV_NTWK_MIN_PATN_AGE"),
        F.col("lnk_EdwDMProvDirProvNtwk_InABC.PROV_NTWK_PCP_IN").alias("PROV_NTWK_PCP_IN"),
        F.col("lnk_EdwDMProvDirProvNtwk_InABC.PROV_NTWK_PROV_DIR_IN").alias("PROV_NTWK_PROV_DIR_IN"),
        F.col("lnk_EdwDMProvDirProvNtwk_InABC.PROV_NTWK_TERM_DT_SK").alias("PROV_NTWK_TERM_DT_SK"),
        F.col("lnk_EdwDMProvDirProvNtwk_InABC.PROV_SK").alias("PROV_SK"),
        F.col("Ref_ntwk.NTWK_ID").alias("NTWK_ID_1"),
        F.col("Ref_ntwk.PROV_ID").alias("PROV_ID_1"),
        F.col("Ref_ntwk.PROV_NTWK_PFX_ID").alias("PROV_NTWK_PFX_ID_1"),
        F.col("Ref_ntwk.PROV_NTWK_EFF_DT_SK").alias("PROV_NTWK_EFF_DT_SK_1"),
    )
)

df_lkp_pcmh = (
    df_lkp_ntwk.alias("lnk_NtwkData_out")
    .join(
        df_db2_PCMH_Extr.alias("Ref_pcmh"),
        (
            F.col("lnk_NtwkData_out.NTWK_ID") == F.col("Ref_pcmh.NTWK_ID")
        )
        & (
            F.col("lnk_NtwkData_out.PROV_SK") == F.col("Ref_pcmh.PROV_SK")
        ),
        "left",
    )
    .select(
        F.col("lnk_NtwkData_out.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("lnk_NtwkData_out.NTWK_ID").alias("NTWK_ID"),
        F.col("lnk_NtwkData_out.PROV_ID").alias("PROV_ID"),
        F.col("lnk_NtwkData_out.PROV_NTWK_PFX_ID").alias("PROV_NTWK_PFX_ID"),
        F.col("lnk_NtwkData_out.PROV_NTWK_EFF_DT_SK").alias("PROV_NTWK_EFF_DT_SK"),
        F.col("lnk_NtwkData_out.NTWK_NM").alias("NTWK_NM"),
        F.col("lnk_NtwkData_out.NTWK_SH_NM").alias("NTWK_SH_NM"),
        F.col("lnk_NtwkData_out.PROV_NTWK_ACPTNG_MCAID_PATN_IN").alias("PROV_NTWK_ACPTNG_MCAID_PATN_IN"),
        F.col("lnk_NtwkData_out.PROV_NTWK_ACPTNG_MCARE_PATN_IN").alias("PROV_NTWK_ACPTNG_MCARE_PATN_IN"),
        F.col("lnk_NtwkData_out.PROV_NTWK_ACPTNG_PATN_IN").alias("PROV_NTWK_ACPTNG_PATN_IN"),
        F.col("lnk_NtwkData_out.PROV_NTWK_AUTO_PCP_ASGMT_IN").alias("PROV_NTWK_AUTO_PCP_ASGMT_IN"),
        F.col("lnk_NtwkData_out.PROV_NTWK_DIR_IN").alias("PROV_NTWK_DIR_IN"),
        F.col("lnk_NtwkData_out.PROV_NTWK_GNDR_ACPTD_CD").alias("PROV_NTWK_GNDR_ACPTD_CD"),
        F.col("lnk_NtwkData_out.PROV_NTWK_GNDR_ACPTD_NM").alias("PROV_NTWK_GNDR_ACPTD_NM"),
        F.col("lnk_NtwkData_out.PROV_NTWK_MAX_PATN_AGE").alias("PROV_NTWK_MAX_PATN_AGE"),
        F.col("lnk_NtwkData_out.PROV_NTWK_MIN_PATN_AGE").alias("PROV_NTWK_MIN_PATN_AGE"),
        F.col("lnk_NtwkData_out.PROV_NTWK_PCP_IN").alias("PROV_NTWK_PCP_IN"),
        F.col("lnk_NtwkData_out.PROV_NTWK_PROV_DIR_IN").alias("PROV_NTWK_PROV_DIR_IN"),
        F.col("lnk_NtwkData_out.PROV_NTWK_TERM_DT_SK").alias("PROV_NTWK_TERM_DT_SK"),
        F.col("lnk_NtwkData_out.PROV_SK").alias("PROV_SK"),
        F.col("lnk_NtwkData_out.NTWK_ID_1").alias("NTWK_ID_1"),
        F.col("lnk_NtwkData_out.PROV_ID_1").alias("PROV_ID_1"),
        F.col("lnk_NtwkData_out.PROV_NTWK_PFX_ID_1").alias("PROV_NTWK_PFX_ID_1"),
        F.col("lnk_NtwkData_out.PROV_NTWK_EFF_DT_SK_1").alias("PROV_NTWK_EFF_DT_SK_1"),
        F.col("Ref_pcmh.NTWK_ID").alias("NTWK_ID_2"),
        F.col("Ref_pcmh.PROV_SK").alias("PROV_SK_1"),
    )
)

df_xfm_BusinessLogic = (
    df_lkp_pcmh
    .withColumn(
        "svPcmhIn",
        F.when(
            F.col("PROV_SK_1").isNull() | F.col("NTWK_ID_2").isNull(),
            F.lit("N"),
        ).otherwise(F.lit("Y"))
    )
    .withColumn(
        "PROV_NTWK_EFF_DT",
        F.when(
            (trim(F.col("PROV_NTWK_EFF_DT_SK")) == "UNK")
            | (trim(F.col("PROV_NTWK_EFF_DT_SK")) == "NA")
            | (trim(F.col("PROV_NTWK_EFF_DT_SK")) == ""),
            FORMAT_DATE_EE(
                F.lit("1753-01-01"), F.lit("DATE"), F.lit("DATE"), F.lit("SYBTIMESTAMP")
            ),
        ).otherwise(
            FORMAT_DATE_EE(
                F.col("PROV_NTWK_EFF_DT_SK"), F.lit("DATE"), F.lit("DATE"), F.lit("SYBTIMESTAMP")
            )
        ),
    )
    .withColumn(
        "PROV_NTWK_TERM_DT",
        F.when(
            (trim(F.col("PROV_NTWK_TERM_DT_SK")) == "UNK")
            | (trim(F.col("PROV_NTWK_TERM_DT_SK")) == "NA")
            | (trim(F.col("PROV_NTWK_TERM_DT_SK")) == ""),
            FORMAT_DATE_EE(
                F.lit("2199-12-31"), F.lit("DATE"), F.lit("DATE"), F.lit("SYBTIMESTAMP")
            )
        ).otherwise(
            FORMAT_DATE_EE(
                F.col("PROV_NTWK_TERM_DT_SK"), F.lit("DATE"), F.lit("DATE"), F.lit("SYBTIMESTAMP")
            )
        ),
    )
    .withColumn(
        "PROV_NTWK_CNTR_IN",
        F.when(
            F.col("NTWK_ID_1").isNotNull()
            & F.col("PROV_ID_1").isNotNull()
            & F.col("PROV_NTWK_PFX_ID_1").isNotNull()
            & F.col("PROV_NTWK_EFF_DT_SK_1").isNotNull(),
            F.lit(1),
        ).otherwise(F.lit(0)),
    )
    .withColumn(
        "LAST_UPDT_DT",
        StringToTimestamp(F.lit(LastUpdtDt), F.lit("%yyyy-%mm-%dd"))
    )
    .withColumn(
        "PCMH_IN",
        F.when(
            F.col("svPcmhIn") == "Y",
            F.lit("Y"),
        ).otherwise(F.lit("N")),
    )
)

df_final = df_xfm_BusinessLogic.select(
    "NTWK_ID",
    "PROV_ID",
    "PROV_NTWK_PFX_ID",
    "PROV_NTWK_EFF_DT",
    "SRC_SYS_CD",
    "PROV_NTWK_ACPTNG_MCAID_PATN_IN",
    "PROV_NTWK_ACPTNG_MCARE_PATN_IN",
    "PROV_NTWK_AUTO_PCP_ASGMT_IN",
    "PROV_NTWK_DIR_IN",
    "PROV_NTWK_ACPTNG_PATN_IN",
    "PROV_NTWK_GNDR_ACPTD_CD",
    "PROV_NTWK_GNDR_ACPTD_NM",
    "PROV_NTWK_MAX_PATN_AGE",
    "PROV_NTWK_MIN_PATN_AGE",
    "PROV_NTWK_PCP_IN",
    "PROV_NTWK_PROV_DIR_IN",
    "NTWK_NM",
    "NTWK_SH_NM",
    "PROV_NTWK_TERM_DT",
    "PROV_NTWK_CNTR_IN",
    "LAST_UPDT_DT",
    "PCMH_IN",
)

df_final = (
    df_final
    .withColumn(
        "PROV_NTWK_ACPTNG_MCAID_PATN_IN",
        rpad(F.col("PROV_NTWK_ACPTNG_MCAID_PATN_IN"), 1, " ")
    )
    .withColumn(
        "PROV_NTWK_ACPTNG_MCARE_PATN_IN",
        rpad(F.col("PROV_NTWK_ACPTNG_MCARE_PATN_IN"), 1, " ")
    )
    .withColumn(
        "PROV_NTWK_AUTO_PCP_ASGMT_IN",
        rpad(F.col("PROV_NTWK_AUTO_PCP_ASGMT_IN"), 1, " ")
    )
    .withColumn(
        "PROV_NTWK_DIR_IN",
        rpad(F.col("PROV_NTWK_DIR_IN"), 1, " ")
    )
    .withColumn(
        "PROV_NTWK_ACPTNG_PATN_IN",
        rpad(F.col("PROV_NTWK_ACPTNG_PATN_IN"), 1, " ")
    )
    .withColumn(
        "PROV_NTWK_PCP_IN",
        rpad(F.col("PROV_NTWK_PCP_IN"), 1, " ")
    )
    .withColumn(
        "PROV_NTWK_PROV_DIR_IN",
        rpad(F.col("PROV_NTWK_PROV_DIR_IN"), 1, " ")
    )
)

write_files(
    df_final,
    f"{adls_path}/load/PROV_DIR_DM_PROV_NTWK.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)