# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2020  Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC ZENA JOBNAME:  EDW_ACTIVITY_FEE_ANALYSIS_EXTRACT_MONTHLY_000
# MAGIC 
# MAGIC CONTROL JOB:  EdwActivityFeeExtrCntl
# MAGIC 
# MAGIC JOB NAME:  ActivityFeeEdwExtr
# MAGIC 
# MAGIC Description:  Extract data from EDW on the 20th of the month for each line of business.  These files have active members counts and are compared against the Optum Activity Fee Invoice files in Activity Fee Report jobs.  Those jobs run on the 1st of the month.  Optum's cutoff date with the invoices is the 20th, but we don't get the invoice files until the 1st.  The data captured on the 20th helps with the accuracy of the reports.
# MAGIC 
# MAGIC Modifications:
# MAGIC 
# MAGIC Developer                  Date            User Story       Change Description                                                                             Development Project    Code Reviewer            Date Reviewed
# MAGIC -------------------------------   -----------------   ---------------------  ---------------------------------------------------------------------------------------------------------   ----------------------------------   ---------------------------------   -------------------------
# MAGIC Bill Schroeder            2022-01-24  US-486052      Initial Programming.                                                                             IntegrateDev2               Raja Gummadi             02/17/2022

# MAGIC Extract data from EDW on the 20th of the month for each line of business.  These files have active members counts and are compared against the Optum Activity Fee Invoice files in Activity Fee Report jobs.  Those jobs run on the 1st of the month.  Optum's cutoff date with the invoices is the 20th, but we don't get the invoice files until the 1st.  The data captured on the 20th helps with the accuracy of the reports.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../Utility_Integrate
# COMMAND ----------


EdwOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
CurrentDate = get_widget_value('CurrentDate','')
RunID = get_widget_value('RunID','')

jdbc_url, jdbc_props = get_db_config(edw_secret_name)

# EdwMedD_Extr
extract_query_EdwMedD_Extr = """SELECT Trim(GRP.GRP_ID) GRP_ID, GRP.GRP_NM, PC.PROD_ID, COUNT(*) CNT
  FROM
       #$EDWOwner#.MBR_D M
       INNER JOIN #$EDWOwner#.MBR_ENR_D ME    ON M.MBR_UNIQ_KEY = ME.MBR_UNIQ_KEY
       INNER JOIN #$EDWOwner#.GRP_D GRP       ON M.GRP_ID       = GRP.GRP_ID
       INNER JOIN #$EDWOwner#.PROD_D PD       ON ME.PROD_SK     = PD.PROD_SK
       INNER JOIN #$EDWOwner#.PROD_CMPNT_D PC ON PD.PROD_ID     = PC.PROD_ID
WHERE
       PC.PROD_CMPNT_EFF_DT_SK        <= TO_DATE('#CurrentDate#','YYYY-MM-DD')
   AND PC.PROD_CMPNT_TERM_DT_SK       >= TO_DATE('#CurrentDate#','YYYY-MM-DD')
   AND ME.MBR_ENR_EFF_DT_SK           <= TO_DATE('#CurrentDate#','YYYY-MM-DD')
   AND ME.MBR_ENR_TERM_DT_SK          >= TO_DATE('#CurrentDate#','YYYY-MM-DD')
   AND PD.PROD_TERM_DT_SK             >= TO_DATE('#CurrentDate#','YYYY-MM-DD')
   AND GRP.GRP_CLNT_ID                =  'MA'
   AND ME.MBR_ENR_CLS_PLN_PROD_CAT_CD =  'MED'
   AND ME.MBR_ENR_ELIG_IN             =  'Y'
   AND PC.PROD_CMPNT_PFX_ID           <> '0000'
   AND M.SUB_ID                       <> 'UNK'
   AND PC.PROD_CMPNT_TYP_CD           =  'BPL'
GROUP BY GRP.GRP_ID, GRP.GRP_NM, PC.PROD_ID
ORDER BY GRP.GRP_ID, GRP.GRP_NM, PC.PROD_ID
"""
extract_query_EdwMedD_Extr = (
    extract_query_EdwMedD_Extr
    .replace("#$EDWOwner#", EdwOwner)
    .replace("TO_DATE('#CurrentDate#','YYYY-MM-DD')", f"CAST('{CurrentDate}' as DATE)")
)

df_EdwMedD_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_EdwMedD_Extr)
    .load()
)

df_EdwMedD_Extr_final = df_EdwMedD_Extr.select(
    F.rpad(F.col("GRP_ID"), <...>, " ").alias("GRP_ID"),
    F.rpad(F.col("GRP_NM"), <...>, " ").alias("GRP_NM"),
    F.rpad(F.col("PROD_ID"), <...>, " ").alias("PROD_ID"),
    F.col("CNT").alias("CNT")
)
write_files(
    df_EdwMedD_Extr_final,
    f"{adls_path_raw}/landing/ActivityFeeEdwMedDExtr.txt",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)

# EdwACA_Extr
extract_query_EdwACA_Extr = """SELECT Trim(GRP.GRP_ID) GRP_ID, GRP.GRP_NM, PC.PROD_ID, COUNT(*) CNT
  FROM
       #$EDWOwner#.MBR_D M
       INNER JOIN #$EDWOwner#.MBR_ENR_D ME    ON M.MBR_UNIQ_KEY = ME.MBR_UNIQ_KEY
       INNER JOIN #$EDWOwner#.GRP_D GRP       ON M.GRP_ID       = GRP.GRP_ID
       INNER JOIN #$EDWOwner#.PROD_D PD       ON ME.PROD_SK     = PD.PROD_SK
       INNER JOIN #$EDWOwner#.PROD_CMPNT_D PC ON PD.PROD_ID     = PC.PROD_ID
WHERE 
       PC.PROD_CMPNT_EFF_DT_SK        <= TO_DATE('#CurrentDate#','YYYY-MM-DD')
   AND PC.PROD_CMPNT_TERM_DT_SK       >= TO_DATE('#CurrentDate#','YYYY-MM-DD')
   AND ME.MBR_ENR_EFF_DT_SK           <= TO_DATE('#CurrentDate#','YYYY-MM-DD')
   AND ME.MBR_ENR_TERM_DT_SK          >= TO_DATE('#CurrentDate#','YYYY-MM-DD')
   AND PD.PROD_TERM_DT_SK             >= TO_DATE('#CurrentDate#','YYYY-MM-DD')
   AND ME.MBR_ENR_CLS_PLN_PROD_CAT_CD =  'MED' 
   AND ME.MBR_ENR_ELIG_IN             =  'Y' 
   AND ME.GRP_ID                      =  '10001000' 
   AND SUBSTR(PC.PROD_CMPNT_PFX_ID,1,1) IN ('7','8') 
   AND M.SUB_ID                       <> 'UNK' 
   AND PC.PROD_CMPNT_TYP_CD           =  'BPL' 
 GROUP BY GRP.GRP_ID, GRP.GRP_NM, PC.PROD_ID
ORDER BY GRP.GRP_ID, GRP.GRP_NM, PC.PROD_ID
"""
extract_query_EdwACA_Extr = (
    extract_query_EdwACA_Extr
    .replace("#$EDWOwner#", EdwOwner)
    .replace("TO_DATE('#CurrentDate#','YYYY-MM-DD')", f"CAST('{CurrentDate}' as DATE)")
)

df_EdwACA_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_EdwACA_Extr)
    .load()
)

df_EdwACA_Extr_final = df_EdwACA_Extr.select(
    F.rpad(F.col("GRP_ID"), <...>, " ").alias("GRP_ID"),
    F.rpad(F.col("GRP_NM"), <...>, " ").alias("GRP_NM"),
    F.rpad(F.col("PROD_ID"), <...>, " ").alias("PROD_ID"),
    F.col("CNT").alias("CNT")
)
write_files(
    df_EdwACA_Extr_final,
    f"{adls_path_raw}/landing/ActivityFeeEdwACAExtr.txt",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)

# EdwComm_Extr
extract_query_EdwComm_Extr = """SELECT Trim(M.GRP_ID) GRP_ID, M.GRP_NM, PC.PROD_ID, COUNT(*) CNT
  FROM
       #$EDWOwner#.MBR_D M 
       INNER JOIN #$EDWOwner#.MBR_ENR_D ME    ON M.MBR_UNIQ_KEY = ME.MBR_UNIQ_KEY
       INNER JOIN #$EDWOwner#.PROD_D PD       ON ME.PROD_SK     = PD.PROD_SK
       INNER JOIN #$EDWOwner#.PROD_CMPNT_D PC ON PD.PROD_ID     = PC.PROD_ID
WHERE 
       PC.PROD_CMPNT_EFF_DT_SK        <= TO_DATE('#CurrentDate#','YYYY-MM-DD')
   AND PC.PROD_CMPNT_TERM_DT_SK       >= TO_DATE('#CurrentDate#','YYYY-MM-DD')
   AND ME.MBR_ENR_EFF_DT_SK           <= TO_DATE('#CurrentDate#','YYYY-MM-DD')
   AND ME.MBR_ENR_TERM_DT_SK          >= TO_DATE('#CurrentDate#','YYYY-MM-DD')
   AND PD.PROD_TERM_DT_SK             >= TO_DATE('#CurrentDate#','YYYY-MM-DD')
   AND ME.MBR_ENR_CLS_PLN_PROD_CAT_CD =  'MED'
   AND ME.MBR_ENR_ELIG_IN             =  'Y'
   AND PC.PROD_CMPNT_PFX_ID           <> '0000'
   AND M.SUB_ID                       <> 'UNK'
   AND PC.PROD_CMPNT_TYP_CD           =  'BPL'
   AND (M.GRP_ID, PC.PROD_CMPNT_PFX_ID) NOT IN  
       (SELECT DISTINCT ME.GRP_ID, PC.PROD_CMPNT_PFX_ID
          FROM #$EDWOwner#.MBR_ENR_D ME,
               #$EDWOwner#.PROD_CMPNT_D PC
         WHERE PC.PROD_ID   = ME.PROD_ID
           AND ME.GRP_ID    = '10001000' 
           AND SUBSTR(PC.PROD_CMPNT_PFX_ID,1,1) IN ('7','8'))
   AND (M.GRP_ID, PC.PROD_CMPNT_PFX_ID) NOT IN
       (SELECT DISTINCT GRP.GRP_ID, PROD_CMPNT_PFX_ID 
          FROM #$EDWOwner#.GRP_D GRP 
               INNER JOIN #$EDWOwner#.MBR_ENR_D ME    ON GRP.GRP_ID = ME.GRP_ID
               INNER JOIN #$EDWOwner#.PROD_CMPNT_D PC ON ME.PROD_ID = PC.PROD_ID
         WHERE PC.PROD_CMPNT_PFX_ID <> '0000' 
           AND GRP.GRP_CLNT_ID      =  'MA')
GROUP BY M.GRP_ID, M.GRP_NM, PC.PROD_ID
ORDER BY M.GRP_ID, M.GRP_NM, PC.PROD_ID
"""
extract_query_EdwComm_Extr = (
    extract_query_EdwComm_Extr
    .replace("#$EDWOwner#", EdwOwner)
    .replace("TO_DATE('#CurrentDate#','YYYY-MM-DD')", f"CAST('{CurrentDate}' as DATE)")
)

df_EdwComm_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_EdwComm_Extr)
    .load()
)

df_EdwComm_Extr_final = df_EdwComm_Extr.select(
    F.rpad(F.col("GRP_ID"), <...>, " ").alias("GRP_ID"),
    F.rpad(F.col("GRP_NM"), <...>, " ").alias("GRP_NM"),
    F.rpad(F.col("PROD_ID"), <...>, " ").alias("PROD_ID"),
    F.col("CNT").alias("CNT")
)
write_files(
    df_EdwComm_Extr_final,
    f"{adls_path_raw}/landing/ActivityFeeEdwCommExtr.txt",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)

# EdwStJoe_Extr
extract_query_EdwStJoe_Extr = """SELECT Trim(M.GRP_ID) GRP_ID, M.GRP_NM, PC.PROD_ID, COUNT(*) CNT
  FROM
       #$EDWOwner#.MBR_D M 
       INNER JOIN #$EDWOwner#.MBR_ENR_D ME    ON M.MBR_UNIQ_KEY = ME.MBR_UNIQ_KEY
       INNER JOIN #$EDWOwner#.PROD_D PD       ON ME.PROD_SK     = PD.PROD_SK
       INNER JOIN #$EDWOwner#.PROD_CMPNT_D PC ON PD.PROD_ID     = PC.PROD_ID
WHERE 
       PC.PROD_CMPNT_EFF_DT_SK        <= TO_DATE('#CurrentDate#','YYYY-MM-DD')
   AND PC.PROD_CMPNT_TERM_DT_SK       >= TO_DATE('#CurrentDate#','YYYY-MM-DD')
   AND ME.MBR_ENR_EFF_DT_SK           <= TO_DATE('#CurrentDate#','YYYY-MM-DD')
   AND ME.MBR_ENR_TERM_DT_SK          >= TO_DATE('#CurrentDate#','YYYY-MM-DD')
   AND PD.PROD_TERM_DT_SK             >= TO_DATE('#CurrentDate#','YYYY-MM-DD')
   AND ME.MBR_ENR_CLS_PLN_PROD_CAT_CD =  'MED'
   AND ME.MBR_ENR_ELIG_IN             =  'Y'
   AND PC.PROD_CMPNT_PFX_ID           <> '0000'
   AND M.SUB_ID                       <> 'UNK'
   AND PC.PROD_CMPNT_TYP_CD           =  'BPL'
   AND (M.GRP_ID, PC.PROD_CMPNT_PFX_ID) NOT IN  
       (SELECT DISTINCT ME.GRP_ID, PC.PROD_CMPNT_PFX_ID
          FROM #$EDWOwner#.MBR_ENR_D ME,
               #$EDWOwner#.PROD_CMPNT_D PC
         WHERE PC.PROD_ID   = ME.PROD_ID
           AND ME.GRP_ID    = '10001000' 
           AND SUBSTR(PC.PROD_CMPNT_PFX_ID,1,1) IN ('7','8'))
   AND (M.GRP_ID, PC.PROD_CMPNT_PFX_ID) NOT IN
       (SELECT DISTINCT GRP.GRP_ID, PROD_CMPNT_PFX_ID 
          FROM #$EDWOwner#.GRP_D GRP 
               INNER JOIN #$EDWOwner#.MBR_ENR_D ME    ON GRP.GRP_ID = ME.GRP_ID
               INNER JOIN #$EDWOwner#.PROD_CMPNT_D PC ON ME.PROD_ID = PC.PROD_ID
         WHERE PC.PROD_CMPNT_PFX_ID <> '0000' 
           AND GRP.GRP_CLNT_ID      =  'MA')
GROUP BY M.GRP_ID, M.GRP_NM, PC.PROD_ID
ORDER BY M.GRP_ID, M.GRP_NM, PC.PROD_ID
"""
extract_query_EdwStJoe_Extr = (
    extract_query_EdwStJoe_Extr
    .replace("#$EDWOwner#", EdwOwner)
    .replace("TO_DATE('#CurrentDate#','YYYY-MM-DD')", f"CAST('{CurrentDate}' as DATE)")
)

df_EdwStJoe_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_EdwStJoe_Extr)
    .load()
)

df_EdwStJoe_Extr_final = df_EdwStJoe_Extr.select(
    F.rpad(F.col("GRP_ID"), <...>, " ").alias("GRP_ID"),
    F.rpad(F.col("GRP_NM"), <...>, " ").alias("GRP_NM"),
    F.rpad(F.col("PROD_ID"), <...>, " ").alias("PROD_ID"),
    F.col("CNT").alias("CNT")
)
write_files(
    df_EdwStJoe_Extr_final,
    f"{adls_path_raw}/landing/ActivityFeeEdwStJoeExtr.txt",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)