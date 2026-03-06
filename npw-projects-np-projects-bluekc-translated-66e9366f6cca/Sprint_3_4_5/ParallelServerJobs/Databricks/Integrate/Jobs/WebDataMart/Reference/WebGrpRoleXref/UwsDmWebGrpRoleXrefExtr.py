# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC COPYRIGHT 2009 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:    UwsWebGrpRoleXrefExtr
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC                    Web Group Role Xref extract from UWS to Data Mart.   
# MAGIC 
# MAGIC              
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC                     Extracts from UWS and pulls to the Data Marts.  Perform no changes with the data.
# MAGIC 
# MAGIC 
# MAGIC OUTPUTS: 
# MAGIC                     Data Mart:  WEB_GRP_ROLE_XREF
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC                    
# MAGIC Developer                          Date                    Project/Altiris #                     Change Description                                                   Development Project               Code Reviewer                      Date Reviewed
# MAGIC ----------------------------------      -------------------      -----------------------------------           ---------------------------------------------------------                            ----------------------------------              ---------------------------------               -------------------------
# MAGIC Bhoomi Dasari                 2009-09-23             JAA - 3500                            Original Programming                                                  devlIDSnew                           Steph Goddard                       09/25/2009
# MAGIC 
# MAGIC 
# MAGIC Archana Palivela             03/30/14             5114                                    Original Programming(Server to Parallel)                         IntegrateWrhsDevl                Bhoomi Dasari                         4/8/2014

# MAGIC UWS Web Group CFG extract from UWS to Data Mart.
# MAGIC Write WEB_GRP_ROLE_XREF Data into a Sequential file for Load Job UwsDmWebGrpRoleXrefLoad
# MAGIC Read all the Data from IDS WEB_GRP_ROLE_XREF Table; Pull records based on LAST_UPDT_RUN_CYC_EXCTN_SK.
# MAGIC Add Defaults and Null Handling.
# MAGIC Job Name: UwsDmWebGrpRoleXrefExtr
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import *
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
DataMartRunCycle = get_widget_value('DataMartRunCycle','')
UWSOwner = get_widget_value('UWSOwner','')
uws_secret_name = get_widget_value('uws_secret_name','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = "SELECT \n\nGRP_ID,\nGRP_UNIQ_KEY \n\nFROM " + IDSOwner + ".GRP"
df_DB2_GRP_Uniq = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

jdbc_url, jdbc_props = get_db_config(uws_secret_name)
extract_query = "SELECT \n\nCAST(ROLE_ID AS VarChar(36)) as ROLE_ID,\nGRP_ID,\nUSER_ID,\nLAST_UPDT_DT_SK\n\nFROM WEB_GRP_ROLE_XREF"
df_UWS_WEB_GRP_ROLE_XREF = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_lkp_Codes = df_UWS_WEB_GRP_ROLE_XREF.alias("lnk_UwsDmWebGrpRoleXrefExtr_InABC").join(
    df_DB2_GRP_Uniq.alias("Lnk_GrpId"),
    F.col("lnk_UwsDmWebGrpRoleXrefExtr_InABC.GRP_ID") == F.col("Lnk_GrpId.GRP_ID"),
    how="left"
).select(
    F.col("lnk_UwsDmWebGrpRoleXrefExtr_InABC.ROLE_ID").alias("ROLE_ID"),
    F.col("lnk_UwsDmWebGrpRoleXrefExtr_InABC.GRP_ID").alias("GRP_ID"),
    F.col("Lnk_GrpId.GRP_UNIQ_KEY").alias("GRP_UNIQ_KEY"),
    F.col("lnk_UwsDmWebGrpRoleXrefExtr_InABC.USER_ID").alias("USER_ID"),
    F.col("lnk_UwsDmWebGrpRoleXrefExtr_InABC.LAST_UPDT_DT_SK").alias("LAST_UPDT_DT_SK")
)

df_xfrm_BusinessLogic = (
    df_lkp_Codes
    .withColumn(
        "GRP_UNIQ_KEY",
        F.when(F.col("GRP_UNIQ_KEY").isNull(), F.lit(" ")).otherwise(F.col("GRP_UNIQ_KEY"))
    )
    .withColumn(
        "LAST_UPDT_DT_SK",
        rpad("LAST_UPDT_DT_SK", 10, " ")
    )
)

write_files(
    df_xfrm_BusinessLogic.select(
        "ROLE_ID",
        "GRP_ID",
        "GRP_UNIQ_KEY",
        "USER_ID",
        "LAST_UPDT_DT_SK"
    ),
    f"{adls_path}/load/WEB_GRP_ROLE_XREF.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)