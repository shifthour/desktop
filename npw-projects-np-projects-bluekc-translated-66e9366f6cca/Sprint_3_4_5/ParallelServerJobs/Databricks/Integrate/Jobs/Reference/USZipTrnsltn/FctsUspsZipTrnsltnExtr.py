# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC CALLED BY:  IdsReferenceSeq
# MAGIC 
# MAGIC PROCESSING:   	Extract County and State Fips Code information from Facets for IDS table USPS_ZIP_TRNSLTN.
# MAGIC 	
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                                  Date                 Project/Altiris #      	                            Change Description                                        	Development Project                 Code Reviewer          Date Reviewed       
# MAGIC ------------------                            -------------------             ------------------------      	-----------------------------------------------------------------------                                        --------------------------------                   ------------------------------   ----------------------------       
# MAGIC Karthik Chintalapani                  2012-04-25          Project 4784 		                Original Programming.                                             South Carolina Claims                  Bhoomi Dasari          07/19/2012  
# MAGIC 
# MAGIC Anoop Nair                              2022-03-08         S2S Remediation                   Added FACETS DSN Connection parameters                                   IntegrateDev5                          Jaideep Mankala     04/26/2022
# MAGIC 
# MAGIC 
# MAGIC Prabhu ES                               2022-06-13         515126                                 S2S Remediation                                                                                IntegrateDev5                                            Manasa Andru           2022-06-14

# MAGIC Extract job to load the USPS_ZIP_TRNSLTN table
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# Parameter parsing
FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
RunID = get_widget_value('RunID','')
CurrDate = get_widget_value('CurrDate','')
CurrRunCycle = get_widget_value('CurrRunCycle','')

# Read the dummy hashed file table (Scenario B)
jdbc_url_hf, jdbc_props_hf = get_db_config(facets_secret_name)
df_hf_usps_zip_trns_lkp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_hf)
    .options(**jdbc_props_hf)
    .option("query", "SELECT SRC_SYS_CD, ZIP_CD_5, ZIP_LN_ID, CRT_RUN_CYC_EXCTN_SK, USPS_ZIP_TRNSLTN_SK FROM dummy_hf_usps_zip_trns")
    .load()
)

# Read from ODBCConnector (CER_MCZT_ZIP_TRANS)
jdbc_url, jdbc_props = get_db_config(facets_secret_name)
extract_query = f"""SELECT 
MCZT_ZIP_CODE ZIP_CD_5,
MCZT_UPDATE_KEY_NO ZIP_LN_ID,
MCZT_REC_TYPE ZIP_RCRD_TYP_CD,
MCZT_FINANCE_NO ZIP_FNC_ID,
MCZT_LAST_LINE_NO PRFRD_ZIP_LN_ID,
MCZT_STATE_ABBREV ST_CD,
MCZT_CITY_NAME CITY_NM,
MCZT_CITY_ABBREV CITY_SH_NM,
MCZT_LAST_LN_NAME PRFRD_POSTAL_NM,
MCZT_COUNTY_NO CNTY_FIPS_NO,
MCZT_COUNTY_NAME CNTY_NAME,
MCZT_FACILITY_CODE USPS_FCLTY_CD
FROM {FacetsOwner}.CER_MCZT_ZIP_TRANS
"""
df_CER_MCZT_ZIP_TRANS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# Clean stage
df_Clean = (
    df_CER_MCZT_ZIP_TRANS
    .withColumn("ZIP_CD_5", trim(F.col("ZIP_CD_5")))
    .withColumn("ZIP_LN_ID", trim(F.col("ZIP_LN_ID")))
    .withColumn("ZIP_RCRD_TYP_CD", trim(F.col("ZIP_RCRD_TYP_CD")))
    .withColumn("ZIP_FNC_ID", trim(F.col("ZIP_FNC_ID")))
    .withColumn("PRFRD_ZIP_LN_ID", trim(F.col("PRFRD_ZIP_LN_ID")))
    .withColumn("ST_CD", trim(F.col("ST_CD")))
    .withColumn("CITY_NM", UpCase(trim(F.col("CITY_NM"))))
    .withColumn("CITY_SH_NM", UpCase(trim(F.col("CITY_SH_NM"))))
    .withColumn("PRFRD_POSTAL_NM", UpCase(trim(F.col("PRFRD_POSTAL_NM"))))
    .withColumn("CNTY_FIPS_NO", trim(F.col("CNTY_FIPS_NO")))
    .withColumn("CNTY_NM", UpCase(trim(F.col("CNTY_NAME"))))
    .withColumn("USPS_FCLTY_CD", trim(F.col("USPS_FCLTY_CD")))
)

# BusinessRules stage
df_BusinessRules = (
    df_Clean
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", F.lit(0))
    .withColumn("INSRT_UPDT_CD", F.lit("I"))
    .withColumn("DISCARD_IN", F.lit("N"))
    .withColumn("PASS_THRU_IN", F.lit("Y"))
    .withColumn("FIRST_RECYC_DT", F.lit(CurrDate))
    .withColumn("ERR_CT", F.lit(0))
    .withColumn("RECYCLE_CT", F.lit(0))
    .withColumn("SRC_SYS_CD", F.lit("FACETS"))
    .withColumn("PRI_KEY_STRING", F.concat(F.lit("FACETS;"), F.col("ZIP_CD_5"), F.lit(";"), F.col("ZIP_LN_ID")))
    .withColumn("USPS_ZIP_TRNSLTN_SK", F.lit(0))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(CurrRunCycle))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(CurrRunCycle))
    .withColumn("ST_CD", F.when(F.col("ST_CD").isNull() | (F.length(trim(F.col("ST_CD"))) == 0), F.lit("UNK")).otherwise(F.col("ST_CD")))
    .withColumn("USPS_FCLTY_CD", F.when(F.col("USPS_FCLTY_CD").isNull() | (F.length(trim(F.col("USPS_FCLTY_CD"))) == 0), F.lit("UNK")).otherwise(F.col("USPS_FCLTY_CD")))
    .withColumn("ZIP_RCRD_TYP_CD", F.when(F.col("ZIP_RCRD_TYP_CD").isNull() | (F.length(trim(F.col("ZIP_RCRD_TYP_CD"))) == 0), F.lit("G")).otherwise(F.col("ZIP_RCRD_TYP_CD")))
    .withColumn("CITY_NM", F.col("CITY_NM"))
    .withColumn("CITY_SH_NM", F.when(F.col("CITY_SH_NM").isNull() | (F.length(trim(F.col("CITY_SH_NM"))) == 0), F.lit("UNK")).otherwise(F.col("CITY_SH_NM")))
    .withColumn("CNTY_FIPS_NO", F.when(F.col("CNTY_FIPS_NO").isNull() | (F.length(trim(F.col("CNTY_FIPS_NO"))) == 0), F.lit("UNK")).otherwise(F.col("CNTY_FIPS_NO")))
    .withColumn("CNTY_NM", F.when(F.col("CNTY_NM").isNull() | (F.length(trim(F.col("CNTY_NM"))) == 0), F.lit("UNK")).otherwise(F.col("CNTY_NM")))
    .withColumn("PRFRD_POSTAL_NM", F.col("PRFRD_POSTAL_NM"))
    .withColumn("PRFRD_ZIP_LN_ID", F.col("PRFRD_ZIP_LN_ID"))
    .withColumn("ZIP_FNC_ID", F.col("ZIP_FNC_ID"))
)

# PK stage join (left join to hashed-file-lkp replaced by dummy table)
df_PK_joined = (
    df_BusinessRules.alias("Rules")
    .join(
        df_hf_usps_zip_trns_lkp.alias("lkup"),
        on=[
            F.col("Rules.SRC_SYS_CD") == F.col("lkup.SRC_SYS_CD"),
            F.col("Rules.ZIP_CD_5") == F.col("lkup.ZIP_CD_5"),
            F.col("Rules.ZIP_LN_ID") == F.col("lkup.ZIP_LN_ID")
        ],
        how="left"
    )
    .select(
        F.col("Rules.JOB_EXCTN_RCRD_ERR_SK"),
        F.col("Rules.INSRT_UPDT_CD"),
        F.col("Rules.DISCARD_IN"),
        F.col("Rules.PASS_THRU_IN"),
        F.col("Rules.FIRST_RECYC_DT"),
        F.col("Rules.ERR_CT"),
        F.col("Rules.RECYCLE_CT"),
        F.col("Rules.SRC_SYS_CD"),
        F.col("Rules.PRI_KEY_STRING"),
        F.col("Rules.ZIP_CD_5").alias("Rules_ZIP_CD_5"),
        F.col("Rules.ZIP_LN_ID").alias("Rules_ZIP_LN_ID"),
        F.col("Rules.CRT_RUN_CYC_EXCTN_SK").alias("Rules_CRT_RUN_CYC_EXCTN_SK"),
        F.col("Rules.LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("Rules.ST_CD"),
        F.col("Rules.USPS_FCLTY_CD"),
        F.col("Rules.ZIP_RCRD_TYP_CD"),
        F.col("Rules.CITY_NM"),
        F.col("Rules.CITY_SH_NM"),
        F.col("Rules.CNTY_FIPS_NO"),
        F.col("Rules.CNTY_NM"),
        F.col("Rules.PRFRD_POSTAL_NM"),
        F.col("Rules.PRFRD_ZIP_LN_ID"),
        F.col("Rules.ZIP_FNC_ID"),
        F.col("lkup.CRT_RUN_CYC_EXCTN_SK").alias("lkup_CRT_RUN_CYC_EXCTN_SK"),
        F.col("lkup.USPS_ZIP_TRNSLTN_SK").alias("lkup_USPS_ZIP_TRNSLTN_SK")
    )
)

# Stage variables logic
df_PK_vars = (
    df_PK_joined
    .withColumn(
        "temp_CRT_RUN_CYC_EXCTN_SK",
        F.when(F.col("lkup_USPS_ZIP_TRNSLTN_SK").isNull(), F.col("Rules_CRT_RUN_CYC_EXCTN_SK"))
         .otherwise(F.col("lkup_CRT_RUN_CYC_EXCTN_SK"))
    )
    .withColumn(
        "temp_USPS_ZIP_TRNSLTN_SK",
        F.when(F.col("lkup_USPS_ZIP_TRNSLTN_SK").isNull(), F.lit(None))
         .otherwise(F.col("lkup_USPS_ZIP_TRNSLTN_SK"))
    )
)

# Prepare for surrogate-key generation
df_enriched = (
    df_PK_vars
    .withColumn("USPS_ZIP_TRNSLTN_SK", F.col("temp_USPS_ZIP_TRNSLTN_SK"))
)

# Surrogate key assignment for USPS_ZIP_TRNSLTN_SK
df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"USPS_ZIP_TRNSLTN_SK",<schema>,<secret_name>)

# Build the "Key" link output (uspsziptrnsltn)
df_Key = (
    df_enriched
    .withColumn("ZIP_CD_5", F.col("Rules_ZIP_CD_5"))
    .withColumn("ZIP_LN_ID", F.col("Rules_ZIP_LN_ID"))
    .withColumn("CRT_RUN_CYC_EXCTN_SK",
                F.when(F.col("lkup_USPS_ZIP_TRNSLTN_SK").isNull(), F.col("temp_CRT_RUN_CYC_EXCTN_SK"))
                 .otherwise(F.col("temp_CRT_RUN_CYC_EXCTN_SK")))
    .select(
        F.col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        F.col("DISCARD_IN").alias("DISCARD_IN"),
        F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
        F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        F.col("ERR_CT").alias("ERR_CT"),
        F.col("RECYCLE_CT").alias("RECYCLE_CT"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        F.col("USPS_ZIP_TRNSLTN_SK").alias("USPS_ZIP_TRNSLTN_SK"),
        F.col("ZIP_CD_5").alias("ZIP_CD_5"),
        F.col("ZIP_LN_ID").alias("ZIP_LN_ID"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("ST_CD").alias("ST_CD"),
        F.col("USPS_FCLTY_CD").alias("USPS_FCLTY_CD"),
        F.col("ZIP_RCRD_TYP_CD").alias("ZIP_RCRD_TYP_CD"),
        F.col("CITY_NM").alias("CITY_NM"),
        F.col("CITY_SH_NM").alias("CITY_SH_NM"),
        F.col("CNTY_FIPS_NO").alias("CNTY_FIPS_NO"),
        F.col("CNTY_NM").alias("CNTY_NM"),
        F.col("PRFRD_POSTAL_NM").alias("PRFRD_POSTAL_NM"),
        F.col("PRFRD_ZIP_LN_ID").alias("PRFRD_ZIP_LN_ID"),
        F.col("ZIP_FNC_ID").alias("ZIP_FNC_ID")
    )
)

# Build the "updt" link output (only records with IsNull(lkup.USPS_ZIP_TRNSLTN_SK))
df_updt = (
    df_enriched
    .filter(F.col("lkup_USPS_ZIP_TRNSLTN_SK").isNull())
    .withColumn("ZIP_CD_5", F.col("Rules_ZIP_CD_5"))
    .withColumn("ZIP_LN_ID", F.col("Rules_ZIP_LN_ID"))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.col("temp_CRT_RUN_CYC_EXCTN_SK"))
    .select(
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("ZIP_CD_5").alias("ZIP_CD_5"),
        F.col("ZIP_LN_ID").alias("ZIP_LN_ID"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("USPS_ZIP_TRNSLTN_SK").alias("USPS_ZIP_TRNSLTN_SK")
    )
)

# Write to the dummy table for hashed file updates (Scenario B)
drop_temp_sql = "DROP TABLE IF EXISTS STAGING.FctsUspsZipTrnsltnExtr_hf_zip_cd_temp"
execute_dml(drop_temp_sql, jdbc_url_hf, jdbc_props_hf)

df_updt.write \
    .format("jdbc") \
    .mode("overwrite") \
    .option("url", jdbc_url_hf) \
    .options(**jdbc_props_hf) \
    .option("dbtable", "STAGING.FctsUspsZipTrnsltnExtr_hf_zip_cd_temp") \
    .save()

merge_sql = """
MERGE dummy_hf_usps_zip_trns AS T
USING STAGING.FctsUspsZipTrnsltnExtr_hf_zip_cd_temp AS S
ON
    T.SRC_SYS_CD = S.SRC_SYS_CD
    AND T.ZIP_CD_5 = S.ZIP_CD_5
    AND T.ZIP_LN_ID = S.ZIP_LN_ID
WHEN NOT MATCHED THEN
    INSERT (SRC_SYS_CD, ZIP_CD_5, ZIP_LN_ID, CRT_RUN_CYC_EXCTN_SK, USPS_ZIP_TRNSLTN_SK)
    VALUES (S.SRC_SYS_CD, S.ZIP_CD_5, S.ZIP_LN_ID, S.CRT_RUN_CYC_EXCTN_SK, S.USPS_ZIP_TRNSLTN_SK);
"""
execute_dml(merge_sql, jdbc_url_hf, jdbc_props_hf)

# Final output to sequential file (uspsziptrnsltn)
df_Key_final = df_Key.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("USPS_ZIP_TRNSLTN_SK"),
    F.rpad(F.col("ZIP_CD_5"), 5, " ").alias("ZIP_CD_5"),
    F.col("ZIP_LN_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("ST_CD"),
    F.col("USPS_FCLTY_CD"),
    F.col("ZIP_RCRD_TYP_CD"),
    F.col("CITY_NM"),
    F.col("CITY_SH_NM"),
    F.col("CNTY_FIPS_NO"),
    F.col("CNTY_NM"),
    F.col("PRFRD_POSTAL_NM"),
    F.col("PRFRD_ZIP_LN_ID"),
    F.col("ZIP_FNC_ID")
)

write_files(
    df_Key_final,
    f"{adls_path}/key/FctsUspsZipTrnsltnExtr.UspsZip.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)