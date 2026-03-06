# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2005 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     EdwIncomeEarnedIncmFExtr
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC   Pulls from 3 EDW Fact tables:  Subscriber Premium Income Fact, Discretionary Income Fact, and Fee Discount Fact.  
# MAGIC   When pulls sums the premuim amounts by invoice and invoice created or due date.  
# MAGIC   Adds indicators so know which type of income it is: Sub, Discretionary or Fee
# MAGIC   Then gets more product short name, financial lob, experience category  from the EDW Prod D table
# MAGIC   Also get more Billing Entity information such as the Bill Unique key, Billing Level Code type from the EDW BILL_ENTTY_D
# MAGIC 
# MAGIC   Has it own primary key.   
# MAGIC       
# MAGIC 
# MAGIC INPUTS:
# MAGIC 	EDW:   SUB_PRM_INCM_F
# MAGIC                              DSCRTN_INCM_F
# MAGIC                              FEE_DSCNT_INCM_F
# MAGIC                              BLL_ENTTY_D
# MAGIC                              PROD_D
# MAGIC                              hf_ern_incm_f                 
# MAGIC 
# MAGIC 
# MAGIC TRANSFORMS:  
# MAGIC                   TRIM all the fields that are extracted or used in lookups
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC                   This job can only be ran after the 3 EDW tables have been loaded for the month.  
# MAGIC                   two of those 3 tables, Fee Discount and Discretionary income, can only be loaded after Member Recast is loaded.  Those 2 jobs use Member Recast data
# MAGIC 
# MAGIC OUTPUTS: 
# MAGIC                   An EDW load file.   Since no history, then the output is a load file to update the EDW table.
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                                                                                                                         Development          Code                        Date 
# MAGIC Developer            Date                   Project/Altiris #            Change Description                                                                                                                                                                                              Project                    Reviewer                  Reviewed       
# MAGIC ------------------          --------------------       ------------------------            ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------                    -----------------------        -------------------------       ---------------------------   
# MAGIC Sharon Andrew    02/08/2006  -                                          Originally Programmed               
# MAGIC 
# MAGIC Srikanth Mettpalli  2013-10-01          5114                            Original Programming                                                                                                                                                                                        EnterpriseWrhsDevl   Bhoomi Dasari        12/10/2013          
# MAGIC                                                                                              (Server to Parallel Conversion)

# MAGIC Job Name: IdsEdwWErnIncmDrvrExtr
# MAGIC Pulls from 3 EDW Fact tables, summarizes and loads to EDW ERN_INCM_F.  Provides Earned Income by Date.
# MAGIC Pull from 3 EDW Fact tables all invoices that have changed.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, lit, rpad
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


EDWOwner = get_widget_value("EDWOwner","")
edw_secret_name = get_widget_value("edw_secret_name","")
EDWIncomeBeginCycle = get_widget_value("EDWIncomeBeginCycle","")

jdbc_url, jdbc_props = get_db_config(edw_secret_name)

extract_query_db2_SUB_PRM_INCM_F_in = (
    f"SELECT DISTINCT SRC_SYS_CD, BILL_INVC_ID FROM {EDWOwner}.SUB_PRM_INCM_F "
    f"WHERE LAST_UPDT_RUN_CYC_EXCTN_SK >= {EDWIncomeBeginCycle} "
    "AND SUB_PRM_INCM_SK NOT IN (0,1)"
)
df_db2_SUB_PRM_INCM_F_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_SUB_PRM_INCM_F_in)
    .load()
)
df_xfrm_BusinessLogic1 = (
    df_db2_SUB_PRM_INCM_F_in
    .withColumn("SRC_SYS_CD", trim(col("SRC_SYS_CD")))
    .withColumn("BILL_INVC_ID", trim(col("BILL_INVC_ID")))
    .withColumn("ERN_INCM_TYP_CD", lit("SUB_PRM_INCM"))
)

extract_query_db2_FEE_DSCNT_INCM_F_in = (
    f"SELECT DISTINCT SRC_SYS_CD, BILL_INVC_ID FROM {EDWOwner}.FEE_DSCNT_INCM_F fee_dscnt "
    f"WHERE LAST_UPDT_RUN_CYC_EXCTN_SK >= {EDWIncomeBeginCycle} "
    "AND FEE_DSCNT_INCM_SK NOT IN (0,1)"
)
df_db2_FEE_DSCNT_INCM_F_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_FEE_DSCNT_INCM_F_in)
    .load()
)
df_xfrm_BusinessLogic3 = (
    df_db2_FEE_DSCNT_INCM_F_in
    .withColumn("SRC_SYS_CD", trim(col("SRC_SYS_CD")))
    .withColumn("BILL_INVC_ID", trim(col("BILL_INVC_ID")))
    .withColumn("ERN_INCM_TYP_CD", lit("FEE_DSCNT_INCM"))
)

extract_query_db2_DSCRTN_INCM_F_in = (
    f"SELECT DISTINCT SRC_SYS_CD, BILL_INVC_ID FROM {EDWOwner}.DSCRTN_INCM_F "
    f"WHERE LAST_UPDT_RUN_CYC_EXCTN_SK >= {EDWIncomeBeginCycle} "
    "AND DSCRTN_INCM_SK NOT IN (0,1)"
)
df_db2_DSCRTN_INCM_F_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_DSCRTN_INCM_F_in)
    .load()
)
df_xfrm_BusinessLogic2 = (
    df_db2_DSCRTN_INCM_F_in
    .withColumn("SRC_SYS_CD", trim(col("SRC_SYS_CD")))
    .withColumn("BILL_INVC_ID", trim(col("BILL_INVC_ID")))
    .withColumn("ERN_INCM_TYP_CD", lit("DSCRTN_INCM"))
)

df_fnl_Incm_Data_temp = (
    df_xfrm_BusinessLogic1
    .unionByName(df_xfrm_BusinessLogic2)
    .unionByName(df_xfrm_BusinessLogic3)
)
df_fnl_Incm_Data = df_fnl_Incm_Data_temp.dropDuplicates(["SRC_SYS_CD", "BILL_INVC_ID", "ERN_INCM_TYP_CD"])

df_seq_W_ERN_INCM_DRVR_csv_load = df_fnl_Incm_Data.select(
    rpad(col("SRC_SYS_CD"), <...>, " ").alias("SRC_SYS_CD"),
    rpad(col("BILL_INVC_ID"), <...>, " ").alias("BILL_INVC_ID"),
    rpad(col("ERN_INCM_TYP_CD"), <...>, " ").alias("ERN_INCM_TYP_CD")
)

write_files(
    df_seq_W_ERN_INCM_DRVR_csv_load,
    f"{adls_path}/load/W_ERN_INCM_DRVR.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)