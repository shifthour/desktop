# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 03/10/09 09:25:21 Batch  15045_33943 INIT bckcetl ids20 dcg01 sa Bringing ALL Claim code down to devlIDS
# MAGIC ^1_2 02/10/09 11:16:45 Batch  15017_40611 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 04/10/08 15:02:15 Batch  14711_54139 PROMOTE bckcetl ids20 dsadm bls for hs
# MAGIC ^1_1 04/10/08 14:46:27 Batch  14711_53189 INIT bckcett testIDS dsadm bls for hs
# MAGIC ^1_1 03/31/08 07:55:19 Batch  14701_28532 PROMOTE bckcett testIDS u03651 steph for Hugh
# MAGIC ^1_1 03/31/08 07:43:11 Batch  14701_27794 INIT bckcett devlIDS u03651 steffy
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC COPYRIGHT 2011 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC JOB NAME:     FcltsClmFirstPassStoredProcUpdt
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC   Executes BCSP_CLM_MNTH_ACTV Sybase Stored Procedure to calculate First Pass Indicator    
# MAGIC   This stored procedure is also called by a monthly reporting process for Rubi reporting (FACMM010).
# MAGIC 
# MAGIC INPUTS:
# MAGIC 	START_DT
# MAGIC                 END_DT
# MAGIC   
# MAGIC HASH FILES:
# MAGIC                none
# MAGIC 
# MAGIC TRANSFORMS:  
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC                     Table in Facets is loaded
# MAGIC   
# MAGIC 
# MAGIC OUTPUTS: 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                           Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------   --------------------------------       -------------------------------   ----------------------------       
# MAGIC Oliver Nielsen          01/05/2008      Ops Dashboard     Originally Programmed                                        devlIDS30                     Steph Goddard           03/27/2008
# MAGIC Hugh Sisson            03/28/2008      3531                      Changed job name                                            devlIDS30
# MAGIC Judy Reynolds         03/16/2011      TTR_949              Modified to use BCBS Prod environmental         IntegrateNewDevl          Steph Goddard          03/16/2011
# MAGIC                                                                                        parameters
# MAGIC Judy Reynolds         03/30/2011      TTR_949              Modified to use BCBSPRDDSN instead of         IntegrateNewDevl          Steph Goddard          04/01/2011
# MAGIC                                                                                        BCBSPRDDB in data source name so that
# MAGIC                                                                                        stored procedure will run in production
# MAGIC Prabhu ES              2022-04-06           S2S                    MSSQL ODBC conn params added                   IntegrateDev5                Raja Gummadi      06/10/2022

# MAGIC Final disposition codes are linked by CLM_ID and loaded into has files in the FctsClmLnDispCd and FctsClmLnDntlExtr jobs, which run in the Pre-requisite step.
# MAGIC Collect rows after Disposition code has been assigned.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


BCBSPRDOwner = get_widget_value('BCBSPRDOwner','')
bcbs_secret_name = get_widget_value('bcbs_secret_name','')
BeginDate = get_widget_value('BeginDate','2022-06-04')
EndDate = get_widget_value('EndDate','2022-06-08')

jdbc_url, jdbc_props = get_db_config(bcbs_secret_name)

df_dummy = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", "SELECT GETDATE() as dummy")
    .load()
)

df_Transformer_243 = (
    df_dummy
    .withColumn("START_DT", F.lit(BeginDate))
    .withColumn("END_DT", F.lit(EndDate))
    .select("START_DT","END_DT")
)

valStart = df_Transformer_243.select("START_DT").first()[0]
valEnd = df_Transformer_243.select("END_DT").first()[0]

df_Stored_Procedure_237 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"EXEC {BCBSPRDOwner}.BCSP_CLM_MNTH_ACTV '{valStart}', '{valEnd}'")
    .load()
)

df_final = (
    df_Stored_Procedure_237
    .withColumn("ProcCode", F.rpad("ProcCode", <...>, " "))
    .select("ProcCode")
)

write_files(
    df_final,
    f"{adls_path}/dev/null",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)