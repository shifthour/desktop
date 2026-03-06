# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2005 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC JOB NAME:     IdsPCaeUniqKey
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC 	Gets INDV_BE_KEY values from the IDS MBR table to assign a new CAE_UNIQ_KEY value to.
# MAGIC 
# MAGIC INPUTS:
# MAGIC 	MBR.INDV_BE_KEY
# MAGIC   
# MAGIC HASH FILES:  hf_cae_uniq_key - This file should never get cleaned out.  This holds a master list of all the records that have been assigned a CAE_UNIQ_KEY.  If you have to do a rerun, the same CAE_UNIQ_KEY value will be assigned correctly.
# MAGIC 
# MAGIC TRANSFORMS:  
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC 	Output file is created for loading into IDS and EDW.
# MAGIC 
# MAGIC OUTPUTS: 
# MAGIC 	Sequential file P_CAE_UNIQ_KEY.dat is created.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 	Laurel Kindley - 10-03-2006 Originally Programmed
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #                            Change Description                                                             Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------                            -----------------------------------------------------------------------                    --------------------------------       -------------------------------   ----------------------------       
# MAGIC Jaideep Mankala     2017-03-13       5321 DataStage Upgrade            Modified SQL on MBR stage from Generate SQL to           IntegrateDev2                  Jag Yelavarthi           2017-03-17     
# MAGIC 							User defined sql

# MAGIC Pull all MBR_INDV_BE_KEY numbers that are not in the P_CAE_UNIQ_KEY table
# MAGIC Assign a CAE_UNIQ_KEY value to each one and create a load file.
# MAGIC If this process has to be rerun, we need to reassign the same CAE_UNIQ_KEY value to the records that came in already.  The INDV_BE_KEY value should always have the same cooresponding CAE_UNIQ_KEY value.  DO NOT CLEAR THE HASH FILE!!!
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


IdsOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query = f"SELECT MBR.INDV_BE_KEY FROM {IdsOwner}.mbr MBR WHERE MBR.MBR_SK <> 0 AND MBR.MBR_SK <> 1 AND MBR.INDV_BE_KEY <> 0 AND NOT EXISTS (SELECT CAE.INDV_BE_KEY FROM {IdsOwner}.p_cae_uniq_key CAE WHERE CAE.INDV_BE_KEY = MBR.INDV_BE_KEY)"

df_MBR = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_hf_cae_uniq_key_lkup = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", "SELECT INDV_BE_KEY, CAE_UNIQ_KEY, LAST_UPDT_DT FROM dummy_hf_cae_uniq_key")
    .load()
)

df_join = df_MBR.alias("m").join(
    df_hf_cae_uniq_key_lkup.alias("lkup"),
    F.col("m.INDV_BE_KEY") == F.col("lkup.INDV_BE_KEY"),
    "left"
)

df_enriched = df_join.select(
    F.col("m.INDV_BE_KEY").alias("mbr_indv_be_key"),
    F.col("lkup.INDV_BE_KEY").alias("lkup_INDV_BE_KEY"),
    F.col("lkup.CAE_UNIQ_KEY"),
    F.col("lkup.LAST_UPDT_DT")
).withColumn(
    "LAST_UPDT_DT",
    F.when(F.col("lkup_INDV_BE_KEY").isNull(), current_date()).otherwise(F.col("LAST_UPDT_DT"))
)

df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"CAE_UNIQ_KEY",<schema>,<secret_name>)

df_p_cae_uniq_key = df_enriched.select(
    F.col("mbr_indv_be_key").alias("INDV_BE_KEY"),
    F.col("CAE_UNIQ_KEY"),
    F.col("LAST_UPDT_DT")
)

df_updt = df_enriched.filter(
    F.col("lkup_INDV_BE_KEY").isNull()
).select(
    F.col("mbr_indv_be_key").alias("INDV_BE_KEY"),
    F.col("CAE_UNIQ_KEY"),
    F.col("LAST_UPDT_DT")
)

write_files(
    df_p_cae_uniq_key.select("INDV_BE_KEY","CAE_UNIQ_KEY","LAST_UPDT_DT"),
    f"{adls_path}/load/P_CAE_UNIQ_KEY.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

execute_dml(
    f"DROP TABLE IF EXISTS STAGING.IdsPCaeUniqKey_hf_cae_uniq_key_temp",
    jdbc_url,
    jdbc_props
)

df_updt.write.format("jdbc")\
    .option("url", jdbc_url)\
    .options(**jdbc_props)\
    .option("dbtable", "STAGING.IdsPCaeUniqKey_hf_cae_uniq_key_temp")\
    .mode("overwrite")\
    .save()

merge_sql = """
MERGE INTO dummy_hf_cae_uniq_key AS T
USING STAGING.IdsPCaeUniqKey_hf_cae_uniq_key_temp AS S
ON T.INDV_BE_KEY = S.INDV_BE_KEY
WHEN MATCHED THEN
  UPDATE SET T.CAE_UNIQ_KEY = S.CAE_UNIQ_KEY,
             T.LAST_UPDT_DT = S.LAST_UPDT_DT
WHEN NOT MATCHED THEN
  INSERT (INDV_BE_KEY, CAE_UNIQ_KEY, LAST_UPDT_DT)
  VALUES (S.INDV_BE_KEY, S.CAE_UNIQ_KEY, S.LAST_UPDT_DT);
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)