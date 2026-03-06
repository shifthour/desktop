# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC Called by: IdsClmMartPrereqSeq
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #                                   Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------                                   -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Ralph Tucker          5/21/2009        Web Realign 3500                             New ETL                                                                                     devlIDSNew                    Steph Goddard           05/28/2009
# MAGIC 
# MAGIC  SAndrew               2009-08-18         Production Support                            Brought down from ids20 to testIDS for emergency prod fix         testIDS                    
# MAGIC                                                                                                                   Changed the SQLServer Update Mode from Clear table to Insert/Update
# MAGIC                                                                                                                     Added tbl to the delete process 
# MAGIC Kalyan Neelam       2010-04-14         4428                                                 Added 19 new fields at the end                                                      IntegrateWrhsDevl         Steph Goddard           04/19/2010
# MAGIC 
# MAGIC Bhoomi Dasari        2011-10-17        4673                                              Changed WAIST_CRCMFR_NO to Decimal(7,2) from Integer(10)     IntegrateWrhsDevl           SAndrew                  2011-10-21       
# MAGIC   
# MAGIC Archana Palivela             08/08/2013          5114                                 Original Programming(Server to Parallel)                                              IntegrateWrhsDevl           Jag Yelavarthi           2013-11-30
# MAGIC 
# MAGIC 
# MAGIC Manasa Andru          2014-01-02         TFS - 9821                                  Removed the Nasco Claim Source from the job                                IntegrateNewDevl       Kalyan Neelam            2015-01-06
# MAGIC 
# MAGIC Reddy Sanam           2020-09-14         US281070                                  Added parameters LumerisRunCycle,MedImpactRunCycle
# MAGIC                                                                                                                Added source clm extraction queries for Lumeris and Medimpact     IntegrateDev2              Kalyan Neelam            2020-09-15
# MAGIC                                                                                                                Sources
# MAGIC 
# MAGIC Reddy Sanam           2020-12-09         US329127                                  Added parameters EyeMedRunCycle,DentaQuestRunCycle
# MAGIC                                                                                                                Added source clm extraction queries for EyeMed and DentaQuest     IntegrateDev2              
# MAGIC                                                                                                                Sources
# MAGIC                                                                                                    
# MAGIC Goutham Kalidindi     2021-05-05       US-377804                                 Added parameter Lvnghlth (LIVONGO) RunCycle                               IntegrateDev2            Jeyaprasanna              2021-05-06
# MAGIC                                                                                                              and Clm Extract
# MAGIC                                                                                                    
# MAGIC Bhanu Shekar        2021-12-07      US471929                   Added parameter NATIONBFTSBBB/NATIONBFTSOTC/NATIONBFTSHRNG/
# MAGIC                                                                                             NATIONBFTSRWD) RunCycle  and Clm Extract                                                  IntegrateDev2             Jeyaprasanna            2022-01-03
# MAGIC 
# MAGIC Saranya                 2023-12-22        US 598810                                  Added job parameter for MARC, DOMINION and added new DB Stages    IntegrateDev1        Jeyaprasanna            2023-12-26
# MAGIC 
# MAGIC Arpitha V                2024-03-21        US 614485                    Added parameter NationBftsFLEXRunCycle and added new DB Stage              IntegrateDev2               Jeyaprasanna           2024-03-28
# MAGIC                                                                                                DB2_NationBftsFLEX_Clm_ln

# MAGIC Write W_WEBDM_ETL_DRVR Data into a Sequential file for Load Job
# MAGIC Since EyeMed is loaded with 1753-01-01 for clm status code, Extraction criteria is hard coded to pull on or after DOS value 2019-01-01
# MAGIC Write W_CLM_DEL Data into a Sequential file for Load Job
# MAGIC Read all the Data from IDS CLMTable;
# MAGIC Add Defaults and Null Handling.
# MAGIC Reference Code Mapping Data for Code SK lookups
# MAGIC Job Name: IdsDmMbrshDmMbrHlthScrnExtr
# MAGIC While the job can be designed to extract data for each source using the same db stage, Separate db stages design is chosen. This will help with stats for each source in case of debugging issues
# MAGIC Since DQ is loaded with 1753-01-01 for clm status code, Extraction criteria is hard coded to pull on or after DOS value 2020-01-01
# MAGIC Since DQ is loaded with 1753-01-01 for clm status code, Extraction criteria is hard coded to pull on or after DOS value 2021-01-01
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# Retrieve parameters
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
NascoRunCycle = get_widget_value('NascoRunCycle','')
TwoYearsAgo = get_widget_value('TwoYearsAgo','')
FctsRunCycle = get_widget_value('FctsRunCycle','')
LumerisRunCycle = get_widget_value('LumerisRunCycle','')
MedImpactRunCycle = get_widget_value('MedImpactRunCycle','')
EyeMedRunCycle = get_widget_value('EyeMedRunCycle','')
DentaQuestRunCycle = get_widget_value('DentaQuestRunCycle','')
LvnghlthRunCycle = get_widget_value('LvnghlthRunCycle','')
NationBftsBBBRunCycle = get_widget_value('NationBftsBBBRunCycle','')
NationBftsOTCRunCycle = get_widget_value('NationBftsOTCRunCycle','')
NationBftsHRNGRunCycle = get_widget_value('NationBftsHRNGRunCycle','')
NationBftsRWDRunCycle = get_widget_value('NationBftsRWDRunCycle','')
NationBftsFLEXRunCycle = get_widget_value('NationBftsFLEXRunCycle','')
MarcRunCycle = get_widget_value('MarcRunCycle','')
DominionRunCycle = get_widget_value('DominionRunCycle','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

# DB2_Facets_Clm_In
extract_query_DB2_Facets_Clm_In = (
    f"SELECT Distinct cl.CLM_SK, cl.CLM_ID, cl.LAST_UPDT_RUN_CYC_EXCTN_SK, cl.SRC_SYS_CD_SK, cd.TRGT_CD as SRC_SYS_CD "
    f"FROM {IDSOwner}.CLM cl, {IDSOwner}.CD_MPPNG cd "
    f"WHERE cl.SRC_SYS_CD_SK = cd.CD_MPPNG_SK "
    f"AND cd.TRGT_CD = 'FACETS' "
    f"AND (cl.LAST_UPDT_RUN_CYC_EXCTN_SK >= {FctsRunCycle} and cl.LAST_UPDT_RUN_CYC_EXCTN_SK < 999999) "
    f"and cl.sttus_dt_sk >= '{TwoYearsAgo}'"
)
df_DB2_Facets_Clm_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_DB2_Facets_Clm_In)
    .load()
)

# DB2_Lumeris_Clm_In
extract_query_DB2_Lumeris_Clm_In = (
    f"SELECT Distinct cl.CLM_SK, cl.CLM_ID, cl.LAST_UPDT_RUN_CYC_EXCTN_SK, cl.SRC_SYS_CD_SK, cd.TRGT_CD as SRC_SYS_CD "
    f"FROM {IDSOwner}.CLM cl, {IDSOwner}.CD_MPPNG cd "
    f"WHERE cl.SRC_SYS_CD_SK = cd.CD_MPPNG_SK "
    f"AND cd.TRGT_CD = 'LUMERIS' "
    f"AND (cl.LAST_UPDT_RUN_CYC_EXCTN_SK >= {LumerisRunCycle} and cl.LAST_UPDT_RUN_CYC_EXCTN_SK < 999999) "
    f"and cl.sttus_dt_sk >= '{TwoYearsAgo}'"
)
df_DB2_Lumeris_Clm_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_DB2_Lumeris_Clm_In)
    .load()
)

# DB2_MedImpact_Clm_In
extract_query_DB2_MedImpact_Clm_In = (
    f"SELECT Distinct cl.CLM_SK, cl.CLM_ID, cl.LAST_UPDT_RUN_CYC_EXCTN_SK, cl.SRC_SYS_CD_SK, cd.TRGT_CD as SRC_SYS_CD "
    f"FROM {IDSOwner}.CLM cl, {IDSOwner}.CD_MPPNG cd "
    f"WHERE cl.SRC_SYS_CD_SK = cd.CD_MPPNG_SK "
    f"AND cd.TRGT_CD = 'MEDIMPACT' "
    f"AND (cl.LAST_UPDT_RUN_CYC_EXCTN_SK >= {MedImpactRunCycle} and cl.LAST_UPDT_RUN_CYC_EXCTN_SK < 999999) "
    f"and cl.sttus_dt_sk >= '{TwoYearsAgo}'"
)
df_DB2_MedImpact_Clm_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_DB2_MedImpact_Clm_In)
    .load()
)

# DB2_EyeMed_Clm_In
extract_query_DB2_EyeMed_Clm_In = (
    f"SELECT Distinct cl.CLM_SK, cl.CLM_ID, cl.LAST_UPDT_RUN_CYC_EXCTN_SK, cl.SRC_SYS_CD_SK, cd.TRGT_CD as SRC_SYS_CD "
    f"FROM {IDSOwner}.CLM cl, {IDSOwner}.CD_MPPNG cd, {IDSOwner}.GRP GRP, {IDSOwner}.CLNT CLNT "
    f"WHERE cl.SRC_SYS_CD_SK = cd.CD_MPPNG_SK "
    f"AND cd.TRGT_CD = 'EYEMED' "
    f"AND (cl.LAST_UPDT_RUN_CYC_EXCTN_SK >= {EyeMedRunCycle} and cl.LAST_UPDT_RUN_CYC_EXCTN_SK < 999999) "
    f"AND cl.SVC_STRT_DT_SK >= '2019-01-01' "
    f"AND cl.GRP_SK = GRP.GRP_SK "
    f"AND GRP.CLNT_SK = CLNT.CLNT_SK "
    f"AND CLNT.CLNT_ID = 'MA'"
)
df_DB2_EyeMed_Clm_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_DB2_EyeMed_Clm_In)
    .load()
)

# DB2_DentaQuest_Clm_In
extract_query_DB2_DentaQuest_Clm_In = (
    f"SELECT Distinct cl.CLM_SK, cl.CLM_ID, cl.LAST_UPDT_RUN_CYC_EXCTN_SK, cl.SRC_SYS_CD_SK, cd.TRGT_CD as SRC_SYS_CD "
    f"FROM {IDSOwner}.CLM cl, {IDSOwner}.CD_MPPNG cd "
    f"WHERE cl.SRC_SYS_CD_SK = cd.CD_MPPNG_SK "
    f"AND cd.TRGT_CD = 'DENTAQUEST' "
    f"AND (cl.LAST_UPDT_RUN_CYC_EXCTN_SK >= {DentaQuestRunCycle} and cl.LAST_UPDT_RUN_CYC_EXCTN_SK < 999999) "
    f"and cl.SVC_STRT_DT_SK >= '2020-01-01'"
)
df_DB2_DentaQuest_Clm_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_DB2_DentaQuest_Clm_In)
    .load()
)

# DB2_LVNGHLTH_Clm_In
extract_query_DB2_LVNGHLTH_Clm_In = (
    f"SELECT Distinct cl.CLM_SK, cl.CLM_ID, cl.LAST_UPDT_RUN_CYC_EXCTN_SK, cl.SRC_SYS_CD_SK, cd.TRGT_CD as SRC_SYS_CD "
    f"FROM {IDSOwner}.CLM cl, {IDSOwner}.CD_MPPNG cd "
    f"WHERE cl.SRC_SYS_CD_SK = cd.CD_MPPNG_SK "
    f"AND cd.TRGT_CD = 'LVNGHLTH' "
    f"AND (cl.LAST_UPDT_RUN_CYC_EXCTN_SK >= {LvnghlthRunCycle} and cl.LAST_UPDT_RUN_CYC_EXCTN_SK < 999999) "
    f"and cl.SVC_STRT_DT_SK >= '2021-01-01'"
)
df_DB2_LVNGHLTH_Clm_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_DB2_LVNGHLTH_Clm_In)
    .load()
)

# DB2_NationBftsBBB_Clm_In
extract_query_DB2_NationBftsBBB_Clm_In = (
    f"SELECT Distinct cl.CLM_SK, cl.CLM_ID, cl.LAST_UPDT_RUN_CYC_EXCTN_SK, cl.SRC_SYS_CD_SK, cd.TRGT_CD as SRC_SYS_CD "
    f"FROM {IDSOwner}.CLM cl, {IDSOwner}.CD_MPPNG cd "
    f"WHERE cl.SRC_SYS_CD_SK = cd.CD_MPPNG_SK "
    f"AND cd.TRGT_CD = 'NATIONBFTSBBB' "
    f"AND (cl.LAST_UPDT_RUN_CYC_EXCTN_SK >= {NationBftsBBBRunCycle} and cl.LAST_UPDT_RUN_CYC_EXCTN_SK < 999999) "
    f"and cl.SVC_STRT_DT_SK >= '2021-01-01'"
)
df_DB2_NationBftsBBB_Clm_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_DB2_NationBftsBBB_Clm_In)
    .load()
)

# DB2_NATIONBFTSOTC_Clm_In
extract_query_DB2_NATIONBFTSOTC_Clm_In = (
    f"SELECT Distinct cl.CLM_SK, cl.CLM_ID, cl.LAST_UPDT_RUN_CYC_EXCTN_SK, cl.SRC_SYS_CD_SK, cd.TRGT_CD as SRC_SYS_CD "
    f"FROM {IDSOwner}.CLM cl, {IDSOwner}.CD_MPPNG cd "
    f"WHERE cl.SRC_SYS_CD_SK = cd.CD_MPPNG_SK "
    f"AND cd.TRGT_CD = 'NATIONBFTSOTC' "
    f"AND (cl.LAST_UPDT_RUN_CYC_EXCTN_SK >= {NationBftsOTCRunCycle} and cl.LAST_UPDT_RUN_CYC_EXCTN_SK < 999999) "
    f"and cl.SVC_STRT_DT_SK >= '2021-01-01'"
)
df_DB2_NATIONBFTSOTC_Clm_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_DB2_NATIONBFTSOTC_Clm_In)
    .load()
)

# DB2_NATIONBFTSHRNG_Clm_In
extract_query_DB2_NATIONBFTSHRNG_Clm_In = (
    f"SELECT Distinct cl.CLM_SK, cl.CLM_ID, cl.LAST_UPDT_RUN_CYC_EXCTN_SK, cl.SRC_SYS_CD_SK, cd.TRGT_CD as SRC_SYS_CD "
    f"FROM {IDSOwner}.CLM cl, {IDSOwner}.CD_MPPNG cd "
    f"WHERE cl.SRC_SYS_CD_SK = cd.CD_MPPNG_SK "
    f"AND cd.TRGT_CD = 'NATIONBFTSHRNG' "
    f"AND (cl.LAST_UPDT_RUN_CYC_EXCTN_SK >= {NationBftsHRNGRunCycle} and cl.LAST_UPDT_RUN_CYC_EXCTN_SK < 999999) "
    f"and cl.SVC_STRT_DT_SK >= '2021-01-01'"
)
df_DB2_NATIONBFTSHRNG_Clm_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_DB2_NATIONBFTSHRNG_Clm_In)
    .load()
)

# DB2_NATIONBFTSRWD_Clm_In
extract_query_DB2_NATIONBFTSRWD_Clm_In = (
    f"SELECT Distinct cl.CLM_SK, cl.CLM_ID, cl.LAST_UPDT_RUN_CYC_EXCTN_SK, cl.SRC_SYS_CD_SK, cd.TRGT_CD as SRC_SYS_CD "
    f"FROM {IDSOwner}.CLM cl, {IDSOwner}.CD_MPPNG cd "
    f"WHERE cl.SRC_SYS_CD_SK = cd.CD_MPPNG_SK "
    f"AND cd.TRGT_CD = 'NATIONBFTSRWD' "
    f"AND (cl.LAST_UPDT_RUN_CYC_EXCTN_SK >= {NationBftsRWDRunCycle} and cl.LAST_UPDT_RUN_CYC_EXCTN_SK < 999999) "
    f"and cl.SVC_STRT_DT_SK >= '2021-01-01'"
)
df_DB2_NATIONBFTSRWD_Clm_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_DB2_NATIONBFTSRWD_Clm_In)
    .load()
)

# DB2_MARC_Clm_In
extract_query_DB2_MARC_Clm_In = (
    f"SELECT Distinct cl.CLM_SK, cl.CLM_ID, cl.LAST_UPDT_RUN_CYC_EXCTN_SK, cl.SRC_SYS_CD_SK, cd.TRGT_CD as SRC_SYS_CD "
    f"FROM {IDSOwner}.CLM cl, {IDSOwner}.CD_MPPNG cd "
    f"WHERE cl.SRC_SYS_CD_SK = cd.CD_MPPNG_SK "
    f"AND cd.TRGT_CD = 'MARC' "
    f"AND (cl.LAST_UPDT_RUN_CYC_EXCTN_SK >= {MarcRunCycle} and cl.LAST_UPDT_RUN_CYC_EXCTN_SK < 999999)"
)
df_DB2_MARC_Clm_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_DB2_MARC_Clm_In)
    .load()
)

# DB2_DOMINION_Clm_In
extract_query_DB2_DOMINION_Clm_In = (
    f"SELECT Distinct cl.CLM_SK, cl.CLM_ID, cl.LAST_UPDT_RUN_CYC_EXCTN_SK, cl.SRC_SYS_CD_SK, cd.TRGT_CD as SRC_SYS_CD "
    f"FROM {IDSOwner}.CLM cl, {IDSOwner}.CD_MPPNG cd "
    f"WHERE cl.SRC_SYS_CD_SK = cd.CD_MPPNG_SK "
    f"AND cd.TRGT_CD = 'DOMINION' "
    f"AND (cl.LAST_UPDT_RUN_CYC_EXCTN_SK >= {DominionRunCycle} and cl.LAST_UPDT_RUN_CYC_EXCTN_SK < 999999)"
)
df_DB2_DOMINION_Clm_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_DB2_DOMINION_Clm_In)
    .load()
)

# DB2_NationBftsFLEX_Clm_ln
extract_query_DB2_NationBftsFLEX_Clm_ln = (
    f"SELECT Distinct cl.CLM_SK, cl.CLM_ID, cl.LAST_UPDT_RUN_CYC_EXCTN_SK, cl.SRC_SYS_CD_SK, cd.TRGT_CD as SRC_SYS_CD "
    f"FROM {IDSOwner}.CLM cl, {IDSOwner}.CD_MPPNG cd "
    f"WHERE cl.SRC_SYS_CD_SK = cd.CD_MPPNG_SK "
    f"AND cd.TRGT_CD = 'NATIONBFTSFLEX' "
    f"AND (cl.LAST_UPDT_RUN_CYC_EXCTN_SK >= {NationBftsFLEXRunCycle} and cl.LAST_UPDT_RUN_CYC_EXCTN_SK < 999999) "
    f"and cl.SVC_STRT_DT_SK >= '2021-01-01'"
)
df_DB2_NationBftsFLEX_Clm_ln = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_DB2_NationBftsFLEX_Clm_ln)
    .load()
)

# Funnel (PxFunnel) -> union all in the specified order
df_Fnl = (
    df_DB2_MedImpact_Clm_In.select("CLM_SK","CLM_ID","LAST_UPDT_RUN_CYC_EXCTN_SK","SRC_SYS_CD_SK","SRC_SYS_CD")
    .unionByName(
        df_DB2_Lumeris_Clm_In.select("CLM_SK","CLM_ID","LAST_UPDT_RUN_CYC_EXCTN_SK","SRC_SYS_CD_SK","SRC_SYS_CD")
    )
    .unionByName(
        df_DB2_Facets_Clm_In.select("CLM_SK","CLM_ID","LAST_UPDT_RUN_CYC_EXCTN_SK","SRC_SYS_CD_SK","SRC_SYS_CD")
    )
    .unionByName(
        df_DB2_EyeMed_Clm_In.select("CLM_SK","CLM_ID","LAST_UPDT_RUN_CYC_EXCTN_SK","SRC_SYS_CD_SK","SRC_SYS_CD")
    )
    .unionByName(
        df_DB2_DentaQuest_Clm_In.select("CLM_SK","CLM_ID","LAST_UPDT_RUN_CYC_EXCTN_SK","SRC_SYS_CD_SK","SRC_SYS_CD")
    )
    .unionByName(
        df_DB2_LVNGHLTH_Clm_In.select("CLM_SK","CLM_ID","LAST_UPDT_RUN_CYC_EXCTN_SK","SRC_SYS_CD_SK","SRC_SYS_CD")
    )
    .unionByName(
        df_DB2_NationBftsBBB_Clm_In.select("CLM_SK","CLM_ID","LAST_UPDT_RUN_CYC_EXCTN_SK","SRC_SYS_CD_SK","SRC_SYS_CD")
    )
    .unionByName(
        df_DB2_NATIONBFTSOTC_Clm_In.select("CLM_SK","CLM_ID","LAST_UPDT_RUN_CYC_EXCTN_SK","SRC_SYS_CD_SK","SRC_SYS_CD")
    )
    .unionByName(
        df_DB2_NATIONBFTSHRNG_Clm_In.select("CLM_SK","CLM_ID","LAST_UPDT_RUN_CYC_EXCTN_SK","SRC_SYS_CD_SK","SRC_SYS_CD")
    )
    .unionByName(
        df_DB2_NATIONBFTSRWD_Clm_In.select("CLM_SK","CLM_ID","LAST_UPDT_RUN_CYC_EXCTN_SK","SRC_SYS_CD_SK","SRC_SYS_CD")
    )
    .unionByName(
        df_DB2_MARC_Clm_In.select("CLM_SK","CLM_ID","LAST_UPDT_RUN_CYC_EXCTN_SK","SRC_SYS_CD_SK","SRC_SYS_CD")
    )
    .unionByName(
        df_DB2_DOMINION_Clm_In.select("CLM_SK","CLM_ID","LAST_UPDT_RUN_CYC_EXCTN_SK","SRC_SYS_CD_SK","SRC_SYS_CD")
    )
    .unionByName(
        df_DB2_NationBftsFLEX_Clm_ln.select("CLM_SK","CLM_ID","LAST_UPDT_RUN_CYC_EXCTN_SK","SRC_SYS_CD_SK","SRC_SYS_CD")
    )
)

# xfrm_BusinessLogic
df_xfrm_BusinessLogic_1 = df_Fnl.select(
    col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    col("CLM_ID").alias("CLM_ID"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

df_xfrm_BusinessLogic_2 = df_Fnl.select(
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("CLM_ID").alias("CLM_ID"),
    col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK")
)

# seq_W_CDM_ETL_DRVR_csv_load
write_files(
    df_xfrm_BusinessLogic_1.select("SRC_SYS_CD_SK","CLM_ID","SRC_SYS_CD","LAST_UPDT_RUN_CYC_EXCTN_SK"),
    f"{adls_path}/load/W_WEBDM_ETL_DRVR.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)

# Seq_W_CLM_DEL_Load
write_files(
    df_xfrm_BusinessLogic_2.select("SRC_SYS_CD","CLM_ID","SRC_SYS_CD_SK"),
    f"{adls_path}/load/W_CLM_DEL.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)