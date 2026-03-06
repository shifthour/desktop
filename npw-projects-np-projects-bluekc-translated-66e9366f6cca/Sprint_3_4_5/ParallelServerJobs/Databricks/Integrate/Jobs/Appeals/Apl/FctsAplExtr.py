# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:   Pulls data from CMC_APAP_APPEALS for loading into IDS APL.
# MAGIC 
# MAGIC PROCESSING:    Creates appeals data
# MAGIC       
# MAGIC Modifications:                        
# MAGIC                                               			Project/										Code		Date
# MAGIC Developer		Date		Altiris #		Change Description					Environment	Reviewer		Reviewed
# MAGIC -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# MAGIC Bhoomi Dasari    		07/01/2007                   	3028                	Initial program                                                           		devlIDS30              	Steph Goddard   	8/23/07
# MAGIC Bhoomi Dasari    		10/18/2007                	3028		Added Balancing Snapshot                                         		devlIDS30          	Steph Goddard           10/18/07
# MAGIC 
# MAGIC Jag Yelavarthi    		2015-01-19                  	TFS#8431       	Changed "IDS_SK" to "Table_Name_SK"                		IntegrateNewDevl      	Kalyan Neelam    	2015-01-27     
# MAGIC 
# MAGIC Manasa Andru    		2016-03-16                 	Project 5391	Updated the field names from APAP_END_DT,             		IntegrateDev1          	Kalyan Neelam     	2016-03-17
# MAGIC                                                              					APAP_INIT_DT and APAP_NEXT_REV_DT to
# MAGIC                                                    					APAP_END_DTM, APAP_INIT_DTM and APAP_NEXT_REV_DTM
# MAGIC 
# MAGIC Ravi Singh       		2018 -09-07               	MCAS-5796  	Updated with stage with Shared Container in last              	IntegrateDev1           	Kalyan Neelam     	2018-09-24
# MAGIC                                                           					to fetch Appeal Data for Evicore, Telligen and NDBH 
# MAGIC                                                           					process. And increase column length of PROD_SK,
# MAGIC                                                         					PROD_SH_NM_SK,APL_CAT_CD_SK,APL_CUR_DCSN_CD_SK,
# MAGIC                                                        					APL_CUR_STTUS_CD_SK,CLS_PLN_PROD_CAT_CD_SK,
# MAGIC                                                      					CUR_APL_LVL_CD_SK
# MAGIC Prabhu ES        		2022-02-25      	S2S 		Remediation-MSSQL connection parameters added                        IntegrateDev5	Ken Bradmon	2022-05-17

# MAGIC Adding the Balancing process
# MAGIC Strip Carriage Return, Line Feed, and Tab.
# MAGIC Extract Facets Appeals Data
# MAGIC Apply business logic
# MAGIC Assign primary surrogate key
# MAGIC Writing Sequential File to /pkey
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../shared_containers/PrimaryKey/AplMcasPkey
# COMMAND ----------

# Parameters (preserving original names and adding <database>_secret_name where required)
FacetsOwner = get_widget_value("FacetsOwner","")
facets_secret_name = get_widget_value("facets_secret_name","")
IDSOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")
CurrRunCycle = get_widget_value("CurrRunCycle","")
BeginDate = get_widget_value("BeginDate","")
EndDate = get_widget_value("EndDate","")
RunID = get_widget_value("RunID","")
RunDate = get_widget_value("RunDate","")

# IDS_PROD (DB2Connector)
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
df_IDS_PROD = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"SELECT PROD.PROD_ID as PROD_ID,PROD.PROD_SK as PROD_SK,PROD.PROD_SH_NM_SK as PROD_SH_NM_SK FROM {IDSOwner}.PROD PROD"
    )
    .load()
)
# Deduplicate for hf_apl_idsprod (Scenario A: key = PROD_ID)
df_hf_apl_idsprod = dedup_sort(df_IDS_PROD, ["PROD_ID"], [])

# FACETS (ODBCConnector)
jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)

# Pin: subgrpsk -> hf_apl_subgrp
df_subgrpsk = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option(
        "query",
        f"SELECT SGSG_CK,SGSG_ID FROM {FacetsOwner}.CMC_SGSG_SUB_GROUP"
    )
    .load()
)
df_hf_apl_subgrp = dedup_sort(df_subgrpsk, ["SGSG_CK"], [])

# Pin: Oprodsk -> hf_apl_prodAplk
df_oprodsk = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option(
        "query",
        f"""SELECT 
APAP.APAP_ID,
APLK.APLK_TYPE
FROM 
{FacetsOwner}.CMC_APAP_APPEALS APAP,
{FacetsOwner}.CMC_APLK_APP_LINK APLK
WHERE
APAP.APAP_ID = APLK.APAP_ID
AND APLK.APLK_TYPE = 'O'"""
    )
    .load()
)
df_oprodsk_dedup = dedup_sort(df_oprodsk, ["APAP_ID"], [])

# Pin: Dprodsk -> hf_apl_prodAplk
df_dprodsk = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option(
        "query",
        f"""SELECT 
APAP.APAP_ID,
APLK.APLK_TYPE
FROM 
{FacetsOwner}.CMC_APAP_APPEALS APAP,
{FacetsOwner}.CMC_APLK_APP_LINK APLK
WHERE
APAP.APAP_ID = APLK.APAP_ID
AND APLK.APLK_TYPE = 'D'"""
    )
    .load()
)
df_dprodsk_dedup = dedup_sort(df_dprodsk, ["APAP_ID"], [])

# Pin: Aprodsk -> hf_apl_prodAplk
df_aprodsk = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option(
        "query",
        f"""SELECT 
APAP.APAP_ID,
APLK.APLK_TYPE
FROM 
{FacetsOwner}.CMC_APAP_APPEALS APAP,
{FacetsOwner}.CMC_APLK_APP_LINK APLK
WHERE
APAP.APAP_ID = APLK.APAP_ID
AND APLK.APLK_TYPE = 'A'"""
    )
    .load()
)
df_aprodsk_dedup = dedup_sort(df_aprodsk, ["APAP_ID"], [])

# Pin: Vprodsk -> hf_apl_prodAplk
df_vprodsk = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option(
        "query",
        f"""SELECT 
APAP.APAP_ID,
APLK.APLK_TYPE
FROM 
{FacetsOwner}.CMC_APAP_APPEALS APAP,
{FacetsOwner}.CMC_APLK_APP_LINK APLK
WHERE
APAP.APAP_ID = APLK.APAP_ID
AND APLK.APLK_TYPE = 'V'"""
    )
    .load()
)
df_vprodsk_dedup = dedup_sort(df_vprodsk, ["APAP_ID"], [])

# Pin: Sprodsk -> hf_apl_prodAplk
df_sprodsk = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option(
        "query",
        f"""SELECT 
APAP.APAP_ID,
APLK.APLK_TYPE
FROM 
{FacetsOwner}.CMC_APAP_APPEALS APAP,
{FacetsOwner}.CMC_APLK_APP_LINK APLK
WHERE
APAP.APAP_ID = APLK.APAP_ID
AND APLK.APLK_TYPE = 'S'"""
    )
    .load()
)
df_sprodsk_dedup = dedup_sort(df_sprodsk, ["APAP_ID"], [])

# Pin: Uprodsk -> hf_apl_prodAplk
df_uprodsk = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option(
        "query",
        f"""SELECT 
APAP.APAP_ID,
APLK.APLK_TYPE
FROM 
{FacetsOwner}.CMC_APAP_APPEALS APAP,
{FacetsOwner}.CMC_APLK_APP_LINK APLK
WHERE
APAP.APAP_ID = APLK.APAP_ID
AND APLK.APLK_TYPE = 'U'"""
    )
    .load()
)
df_uprodsk_dedup = dedup_sort(df_uprodsk, ["APAP_ID"], [])

# Pin: Cprodsk -> hf_apl_prodAplk
df_cprodsk = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option(
        "query",
        f"""SELECT 
APAP.APAP_ID,
APLK.APLK_TYPE
FROM 
{FacetsOwner}.CMC_APAP_APPEALS APAP,
{FacetsOwner}.CMC_APLK_APP_LINK APLK
WHERE
APAP.APAP_ID = APLK.APAP_ID
AND APLK.APLK_TYPE = 'C'"""
    )
    .load()
)
df_cprodsk_dedup = dedup_sort(df_cprodsk, ["APAP_ID"], [])

# Pin: grpsk -> hf_apl_grp
df_grpsk = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option(
        "query",
        f"""SELECT GRGR_CK,GRGR_ID 
FROM {FacetsOwner}.CMC_GRGR_GROUP"""
    )
    .load()
)
df_hf_apl_grp = dedup_sort(df_grpsk, ["GRGR_CK"], [])

# Pin: ProdNot_sk -> hf_apl_prodnotlkup
df_ProdNot_sk = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option(
        "query",
        f"""SELECT 
APAP.APAP_ID APAP_ID_1,
APLK.APAP_ID,
PRCS.PDPD_ID
FROM 
{FacetsOwner}.CMC_APAP_APPEALS APAP LEFT JOIN {FacetsOwner}.CMC_APLK_APP_LINK APLK 
ON APAP.APAP_ID = APLK.APAP_ID,
{FacetsOwner}.CMC_MEPE_PRCS_ELIG PRCS
WHERE
APAP.MEME_CK = PRCS.MEME_CK
AND APAP.CSPD_CAT = PRCS.CSPD_CAT
AND APAP.APAP_INIT_DTM >= PRCS.MEPE_EFF_DT
AND APAP.APAP_INIT_DTM <= PRCS.MEPE_TERM_DT"""
    )
    .load()
)
df_hf_apl_prodnotlkup = dedup_sort(df_ProdNot_sk, ["APAP_ID_1"], [])

# Pin: ProdDO_sk -> hf_apl_proddo
df_ProdDO_sk = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option(
        "query",
        f"""SELECT 
APAP.APAP_ID,
APLK.APLK_TYPE,
PRCS.PDPD_ID
FROM 
{FacetsOwner}.CMC_APAP_APPEALS APAP,
{FacetsOwner}.CMC_APLK_APP_LINK APLK,
{FacetsOwner}.CMC_MEPE_PRCS_ELIG PRCS
WHERE
APAP.APAP_ID = APLK.APAP_ID
AND APLK.APLK_TYPE IN ('D', 'O')
AND APAP.MEME_CK = PRCS.MEME_CK
AND APAP.CSPD_CAT = PRCS.CSPD_CAT
AND APAP.APAP_INIT_DTM >= PRCS.MEPE_EFF_DT
AND APAP.APAP_INIT_DTM <= PRCS.MEPE_TERM_DT"""
    )
    .load()
)
df_hf_apl_proddo = dedup_sort(df_ProdDO_sk, ["APAP_ID"], [])

# Pin: ProdA_sk -> hf_apl_proda
df_ProdA_sk = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option(
        "query",
        f"""SELECT 
APAP.APAP_ID,
APLK.APLK_TYPE,
PRCS.PDPD_ID
FROM 
{FacetsOwner}.CMC_APAP_APPEALS APAP,
{FacetsOwner}.CMC_APLK_APP_LINK APLK,
{FacetsOwner}.CMC_MEPE_PRCS_ELIG PRCS
WHERE
APAP.APAP_ID = APLK.APAP_ID
AND APLK.APLK_TYPE = 'A'
AND APAP.MEME_CK = PRCS.MEME_CK
AND APAP.CSPD_CAT = PRCS.CSPD_CAT
AND APAP.APAP_INIT_DTM >= PRCS.MEPE_EFF_DT
AND APAP.APAP_INIT_DTM <= PRCS.MEPE_TERM_DT"""
    )
    .load()
)
df_hf_apl_proda = dedup_sort(df_ProdA_sk, ["APAP_ID"], [])

# Pin: ProdV_sk -> hf_apl_prodv
df_ProdV_sk = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option(
        "query",
        f"""SELECT 
APAP.APAP_ID,
APLK.APLK_TYPE,
MEPE.PDPD_ID
FROM 
{FacetsOwner}.CMC_APAP_APPEALS APAP,
{FacetsOwner}.CMC_APLK_APP_LINK APLK,
{FacetsOwner}.CMC_CSTK_TASK TASK,
{FacetsOwner}.CMC_MEPE_PRCS_ELIG MEPE
WHERE
APAP.APAP_ID = APLK.APAP_ID
AND APLK.APLK_TYPE = 'V'
AND APLK.APLK_ID = TASK.CSSC_ID
AND TASK.CSPD_CAT = MEPE.CSPD_CAT
AND TASK.MEME_CK = MEPE.MEME_CK
AND TASK.CSTK_SEQ_NO = 0
AND TASK.CSTK_RECD_DT >= MEPE.MEPE_EFF_DT
AND TASK.CSTK_RECD_DT <= MEPE.MEPE_TERM_DT"""
    )
    .load()
)
df_hf_apl_prodv = dedup_sort(df_ProdV_sk, ["APAP_ID"], [])

# Pin: ProdS_sk -> hf_apl_prods
df_ProdS_sk = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option(
        "query",
        f"""SELECT 
APAP.APAP_ID,
APLK.APLK_TYPE,
PRCS.PDPD_ID
FROM 
{FacetsOwner}.CMC_APAP_APPEALS APAP,
{FacetsOwner}.CMC_APLK_APP_LINK APLK,
{FacetsOwner}.CMC_CMCM_CASE_MGT CMCM,
{FacetsOwner}.CMC_MEPE_PRCS_ELIG PRCS
WHERE
APAP.APAP_ID = APLK.APAP_ID
AND APLK.APLK_TYPE = 'S'
AND APLK.APLK_ID = CMCM.CMCM_ID
AND CMCM.GRGR_CK = PRCS.GRGR_CK
AND CMCM.MEME_CK = PRCS.MEME_CK
AND PRCS.CSPD_CAT = 'M'
AND CMCM.CMCM_BEG_DT >= PRCS.MEPE_EFF_DT
AND CMCM.CMCM_BEG_DT <= PRCS.MEPE_TERM_DT"""
    )
    .load()
)
df_hf_apl_prods = dedup_sort(df_ProdS_sk, ["APAP_ID"], [])

# Pin: ProdU_sk -> hf_apl_produ
df_ProdU_sk = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option(
        "query",
        f"""SELECT 
APAP.APAP_ID,
APLK.APLK_TYPE,
UTIL.PDPD_ID
FROM 
{FacetsOwner}.CMC_APAP_APPEALS APAP,
{FacetsOwner}.CMC_APLK_APP_LINK APLK,
{FacetsOwner}.CMC_UMUM_UTIL_MGT UTIL
WHERE
APAP.APAP_ID = APLK.APAP_ID
AND APLK.APLK_TYPE = 'U'
AND APLK.APLK_ID = UTIL.UMUM_REF_ID"""
    )
    .load()
)
df_hf_apl_produ = dedup_sort(df_ProdU_sk, ["APAP_ID"], [])

# Pin: ProdC_sk -> hf_apl_prodc
df_ProdC_sk = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option(
        "query",
        f"""SELECT 
APAP.APAP_ID,
APLK.APLK_TYPE,
CLCL.PDPD_ID
FROM 
{FacetsOwner}.CMC_APAP_APPEALS APAP,
{FacetsOwner}.CMC_APLK_APP_LINK APLK,
{FacetsOwner}.CMC_CLCL_CLAIM CLCL
WHERE
APAP.APAP_ID = APLK.APAP_ID
AND APLK.APLK_TYPE = 'C'
AND APLK.APLK_ID = CLCL.CLCL_ID"""
    )
    .load()
)
df_hf_apl_prodc = dedup_sort(df_ProdC_sk, ["APAP_ID"], [])

# Pin: Extract -> Strip
df_Extract = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option(
        "query",
        f"""SELECT 
APAP.APAP_ID,
APAP.SBSB_CK,
APAP.GRGR_CK,
APAP.SGSG_CK,
APAP.CSPD_CAT,
APAP.APAP_ST,
APAP.APAP_PRPR_ID1,
APAP.APAP_MCTR_PRRE_1,
APAP.APAP_INIT_DTM,
APAP.APAP_END_DTM,
APAP.APAP_MCTR_METH,
APAP.APAP_CREATE_DTM,
APAP.APAP_CREATE_USID,
APAP.APAP_LAST_UPD_DTM,
APAP.APAP_LAST_UPD_USID,
APAP.APAP_MCTR_CAT,
APAP.APAP_MCTR_TYPE,
APAP.APAP_MCTR_STYP,
APAP.APAP_USID_PRI,
APAP.APAP_USID_SEC,
APAP.APAP_USID_TER,
APAP.APST_SEQ_NO,
APAP.APAP_CUR_STS,
APAP.APAP_CUR_STS_DTM,
APAP.APLV_SEQ_NO,
APAP.APAP_CUR_LEVEL,
APAP.APAP_CUR_EXP_IND,
APAP.APAP_DESC,
APAP.APAP_SUMMARY,
APAP.APAP_NEXT_REV_DTM,
APAP.APAP_NEXT_REV_INT,
APAP.APAP_CUR_DECISION,
APAP.MEME_CK
FROM 
{FacetsOwner}.CMC_APAP_APPEALS APAP
WHERE
APAP.APAP_LAST_UPD_DTM >= '{BeginDate}'
AND APAP.APAP_LAST_UPD_DTM <= '{EndDate}'"""
    )
    .load()
)

# Strip (CTransformerStage) - multiple left joins
df_Strip_joins = (
    df_Extract.alias("Extract")
    .join(df_hf_apl_prodc.alias("prodCsk"), F.col("Extract.APAP_ID")==F.col("prodCsk.APAP_ID"), "left")
    .join(df_hf_apl_produ.alias("prodUsk"), F.col("Extract.APAP_ID")==F.col("prodUsk.APAP_ID"), "left")
    .join(df_hf_apl_prods.alias("prodSsk"), F.col("Extract.APAP_ID")==F.col("prodSsk.APAP_ID"), "left")
    .join(df_hf_apl_prodv.alias("prodVsk"), F.col("Extract.APAP_ID")==F.col("prodVsk.APAP_ID"), "left")
    .join(df_hf_apl_proda.alias("prodAsk"), F.col("Extract.APAP_ID")==F.col("prodAsk.APAP_ID"), "left")
    .join(df_hf_apl_proddo.alias("prodDOsk"), F.col("Extract.APAP_ID")==F.col("prodDOsk.APAP_ID"), "left")
    .join(df_hf_apl_prodnotlkup.alias("prodnotsk"), F.col("Extract.APAP_ID")==F.col("prodnotsk.APAP_ID_1"), "left")
    .join(df_hf_apl_grp.alias("grpsklkup"), F.col("Extract.GRGR_CK")==F.col("grpsklkup.GRGR_CK"), "left")
    .join(df_hf_apl_subgrp.alias("subgrpsklkup"), F.col("Extract.SGSG_CK")==F.col("subgrpsklkup.SGSG_CK"), "left")
    .join(df_cprodsk_dedup.alias("Ccprodsk"), F.col("Extract.APAP_ID")==F.col("Ccprodsk.APAP_ID"), "left")
    .join(df_uprodsk_dedup.alias("Uuprodsk"), F.col("Extract.APAP_ID")==F.col("Uuprodsk.APAP_ID"), "left")
    .join(df_sprodsk_dedup.alias("Ssprodsk"), F.col("Extract.APAP_ID")==F.col("Ssprodsk.APAP_ID"), "left")
    .join(df_vprodsk_dedup.alias("Vvprodsk"), F.col("Extract.APAP_ID")==F.col("Vvprodsk.APAP_ID"), "left")
    .join(df_aprodsk_dedup.alias("Aaprodsk"), F.col("Extract.APAP_ID")==F.col("Aaprodsk.APAP_ID"), "left")
    .join(df_dprodsk_dedup.alias("Ddprodsk"), F.col("Extract.APAP_ID")==F.col("Ddprodsk.APAP_ID"), "left")
    .join(df_oprodsk_dedup.alias("Ooprodsk"), F.col("Extract.APAP_ID")==F.col("Ooprodsk.APAP_ID"), "left")
)

df_Strip = df_Strip_joins.select(
    F.regexp_replace(F.col("Extract.APAP_ID"), "[\r\n\t]", "").alias("APAP_ID"),
    F.col("Extract.SBSB_CK").alias("SBSB_CK"),
    F.when((F.length(F.col("grpsklkup.GRGR_ID"))==0)|F.col("grpsklkup.GRGR_ID").isNull(), F.lit("NA"))
     .otherwise(F.col("grpsklkup.GRGR_ID")).alias("GRGR_CK"),
    F.when((F.length(F.col("subgrpsklkup.SGSG_ID"))==0)|F.col("subgrpsklkup.SGSG_ID").isNull(), F.lit("NA"))
     .otherwise(F.col("subgrpsklkup.SGSG_ID")).alias("SGSG_CK"),
    F.regexp_replace(trim(F.col("Extract.CSPD_CAT")), "[\r\n\t]", "").alias("CSPD_CAT"),
    F.regexp_replace(trim(F.col("Extract.APAP_ST")), "[\r\n\t]", "").alias("APAP_ST"),
    F.regexp_replace(F.col("Extract.APAP_PRPR_ID1"), "[\r\n\t]", "").alias("APAP_PRPR_ID1"),
    F.regexp_replace(trim(F.col("Extract.APAP_MCTR_PRRE_1")), "[\r\n\t]", "").alias("APAP_MCTR_PRRE_1"),
    # APAP_INIT_DT
    F.date_format(F.col("Extract.APAP_INIT_DTM"), "yyyy-MM-dd").alias("APAP_INIT_DT"),
    # APAP_END_DT
    F.date_format(F.col("Extract.APAP_END_DTM"), "yyyy-MM-dd").alias("APAP_END_DT"),
    F.regexp_replace(trim(F.col("Extract.APAP_MCTR_METH")), "[\r\n\t]", "").alias("APAP_MCTR_METH"),
    # APAP_CREATE_DTM
    F.date_format(F.col("Extract.APAP_CREATE_DTM"), "yyyy-MM-dd HH:mm:ss").alias("APAP_CREATE_DTM"),
    F.regexp_replace(F.col("Extract.APAP_CREATE_USID"), "[\r\n\t]", "").alias("APAP_CREATE_USID"),
    # APAP_LAST_UPD_DTM
    F.date_format(F.col("Extract.APAP_LAST_UPD_DTM"), "yyyy-MM-dd HH:mm:ss").alias("APAP_LAST_UPD_DTM"),
    F.regexp_replace(F.col("Extract.APAP_LAST_UPD_USID"), "[\r\n\t]", "").alias("APAP_LAST_UPD_USID"),
    F.regexp_replace(trim(F.col("Extract.APAP_MCTR_CAT")), "[\r\n\t]", "").alias("APAP_MCTR_CAT"),
    F.regexp_replace(trim(F.col("Extract.APAP_MCTR_TYPE")), "[\r\n\t]", "").alias("APAP_MCTR_TYPE"),
    F.regexp_replace(trim(F.col("Extract.APAP_MCTR_STYP")), "[\r\n\t]", "").alias("APAP_MCTR_STYP"),
    F.regexp_replace(trim(F.col("Extract.APAP_USID_PRI")), "[\r\n\t]", "").alias("APAP_USID_PRI"),
    F.regexp_replace(trim(F.col("Extract.APAP_USID_SEC")), "[\r\n\t]", "").alias("APAP_USID_SEC"),
    F.regexp_replace(trim(F.col("Extract.APAP_USID_TER")), "[\r\n\t]", "").alias("APAP_USID_TER"),
    F.col("Extract.APST_SEQ_NO").alias("APST_SEQ_NO"),
    F.regexp_replace(trim(F.col("Extract.APAP_CUR_STS")), "[\r\n\t]", "").alias("APAP_CUR_STS"),
    F.date_format(F.col("Extract.APAP_CUR_STS_DTM"), "yyyy-MM-dd HH:mm:ss").alias("APAP_CUR_STS_DTM"),
    F.col("Extract.APLV_SEQ_NO").alias("APLV_SEQ_NO"),
    F.regexp_replace(trim(F.col("Extract.APAP_CUR_LEVEL")), "[\r\n\t]", "").alias("APAP_CUR_LEVEL"),
    F.regexp_replace(F.col("Extract.APAP_CUR_EXP_IND"), "[\r\n\t]", "").alias("APAP_CUR_EXP_IND"),
    F.regexp_replace(trim(F.col("Extract.APAP_DESC")), "[\r\n\t]", "").alias("APAP_DESC"),
    F.regexp_replace(trim(F.col("Extract.APAP_SUMMARY")), "[\r\n\t]", "").alias("APAP_SUMMARY"),
    F.date_format(F.col("Extract.APAP_NEXT_REV_DTM"), "yyyy-MM-dd").alias("APAP_NEXT_REV_DT"),
    F.col("Extract.APAP_NEXT_REV_INT").alias("APAP_NEXT_REV_INT"),
    F.regexp_replace(trim(F.col("Extract.APAP_CUR_DECISION")), "[\r\n\t]", "").alias("APAP_CUR_DECISION"),
    F.col("Extract.MEME_CK").alias("MEME_CK"),
    F.when(F.col("Ccprodsk.APAP_ID").isNotNull(), F.col("Ccprodsk.APLK_TYPE"))
     .when(F.col("Uuprodsk.APAP_ID").isNotNull(), F.col("Uuprodsk.APLK_TYPE"))
     .when(F.col("Ssprodsk.APAP_ID").isNotNull(), F.col("Ssprodsk.APLK_TYPE"))
     .when(F.col("Vvprodsk.APAP_ID").isNotNull(), F.col("Vvprodsk.APLK_TYPE"))
     .when(F.col("Aaprodsk.APAP_ID").isNotNull(), F.col("Aaprodsk.APLK_TYPE"))
     .when(F.col("Ddprodsk.APAP_ID").isNotNull(), F.col("Ddprodsk.APLK_TYPE"))
     .when(F.col("Ooprodsk.APAP_ID").isNotNull(), F.col("Ooprodsk.APLK_TYPE"))
     .otherwise(F.lit("NA"))
     .alias("APLK_TYPE"),
    F.when((F.length(F.regexp_replace(trim(F.col("prodCsk.PDPD_ID")), "[\r\n\t]", ""))==0)|F.regexp_replace(trim(F.col("prodCsk.PDPD_ID")), "[\r\n\t]", "").isNull(), F.lit("NA"))
     .otherwise(F.regexp_replace(trim(F.col("prodCsk.PDPD_ID")), "[\r\n\t]", ""))
     .alias("PDPD_ID"),
    F.when((F.length(F.regexp_replace(trim(F.col("prodUsk.PDPD_ID")), "[\r\n\t]", ""))==0)|F.regexp_replace(trim(F.col("prodUsk.PDPD_ID")), "[\r\n\t]", "").isNull(), F.lit("NA"))
     .otherwise(F.regexp_replace(trim(F.col("prodUsk.PDPD_ID")), "[\r\n\t]", ""))
     .alias("PDPD_ID_1"),
    F.when((F.length(F.regexp_replace(trim(F.col("prodSsk.PDPD_ID")), "[\r\n\t]", ""))==0)|F.regexp_replace(trim(F.col("prodSsk.PDPD_ID")), "[\r\n\t]", "").isNull(), F.lit("NA"))
     .otherwise(F.regexp_replace(trim(F.col("prodSsk.PDPD_ID")), "[\r\n\t]", ""))
     .alias("PDPD_ID_2"),
    F.when((F.length(F.regexp_replace(trim(F.col("prodVsk.PDPD_ID")), "[\r\n\t]", ""))==0)|F.regexp_replace(trim(F.col("prodVsk.PDPD_ID")), "[\r\n\t]", "").isNull(), F.lit("NA"))
     .otherwise(F.regexp_replace(trim(F.col("prodVsk.PDPD_ID")), "[\r\n\t]", ""))
     .alias("PDPD_ID_3"),
    F.when((F.length(F.regexp_replace(trim(F.col("prodAsk.PDPD_ID")), "[\r\n\t]", ""))==0)|F.regexp_replace(trim(F.col("prodAsk.PDPD_ID")), "[\r\n\t]", "").isNull(), F.lit("NA"))
     .otherwise(F.regexp_replace(trim(F.col("prodAsk.PDPD_ID")), "[\r\n\t]", ""))
     .alias("PDPD_ID_4"),
    F.when((F.length(F.regexp_replace(trim(F.col("prodDOsk.PDPD_ID")), "[\r\n\t]", ""))==0)|F.regexp_replace(trim(F.col("prodDOsk.PDPD_ID")), "[\r\n\t]", "").isNull(), F.lit("NA"))
     .otherwise(F.regexp_replace(trim(F.col("prodDOsk.PDPD_ID")), "[\r\n\t]", ""))
     .alias("PDPD_ID_5"),
    F.when((F.length(F.regexp_replace(trim(F.col("prodnotsk.APAP_ID_1")), "[\r\n\t]", ""))==0)|F.regexp_replace(trim(F.col("prodnotsk.APAP_ID_1")), "[\r\n\t]", "").isNull(), F.lit("NA"))
     .otherwise(F.regexp_replace(trim(F.col("prodnotsk.PDPD_ID")), "[\r\n\t]", ""))
     .alias("PDPD_ID_6")
)

# Trim (CTransformerStage)
# Define stage variables:
# RowPassThru = 'Y'
# svSrcSysCd = "FACETS"
# svAplId = trim(Strip.APAP_ID)
df_Trim_vars = df_Strip.withColumn("RowPassThru", F.lit("Y")) \
    .withColumn("svSrcSysCd", F.lit("FACETS")) \
    .withColumn("svAplId", trim(F.col("APAP_ID")))

df_Trim = df_Trim_vars.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.col("RowPassThru").alias("PASS_THRU_IN"),
    F.col("RunDate").alias("FIRST_RECYC_DT"),  # from the original logic => Expression: RunDate
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.col("svSrcSysCd").alias("SRC_SYS_CD"),
    F.concat(F.col("svSrcSysCd"), F.lit(";"), F.col("svAplId")).alias("PRI_KEY_STRING"),
    F.lit(0).alias("APL_SK"),
    F.col("svAplId").alias("APL_ID"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("APAP_CREATE_USID").alias("CRT_USER_SK"),
    F.col("APAP_USID_PRI").alias("CUR_PRI_USER_SK"),
    F.col("APAP_USID_SEC").alias("CUR_SEC_USER_SK"),
    F.col("APAP_USID_TER").alias("CUR_TRTY_USER_SK"),
    F.col("GRGR_CK").alias("GRP_SK"),
    F.col("APAP_LAST_UPD_USID").alias("LAST_UPDT_USER_SK"),
    F.col("MEME_CK").alias("MBR_SK"),
    F.col("APAP_PRPR_ID1").alias("REP_PROV_SK"),
    F.col("SGSG_CK").alias("SUBGRP_SK"),
    F.col("SBSB_CK").alias("SUB_SK"),
    F.col("APAP_MCTR_CAT").alias("APL_CAT_CD_SK"),
    F.col("APAP_CUR_DECISION").alias("APL_CUR_DCSN_CD_SK"),
    F.col("APAP_CUR_STS").alias("APL_CUR_STTUS_CD_SK"),
    F.col("APAP_MCTR_METH").alias("APL_INITN_METH_CD_SK"),
    F.col("APAP_ST").alias("APL_MBR_HOME_ADDR_ST_CD_SK"),
    F.col("APAP_MCTR_PRRE_1").alias("APL_PROV_RELSHP_RSN_CD_SK"),  # not used further in next stage's mapping, but keep
    F.col("APAP_MCTR_STYP").alias("APL_SUBTYP_CD_SK"),
    F.col("APAP_MCTR_TYPE").alias("APL_TYP_CD_SK"),
    F.col("CSPD_CAT").alias("CLS_PLN_PROD_CAT_CD_SK"),
    F.col("APAP_CUR_LEVEL").alias("CUR_APL_LVL_CD_SK"),
    trim(F.col("APAP_CUR_EXP_IND")).alias("CUR_APL_LVL_EXPDTD_IN"),
    F.col("APAP_CREATE_DTM").alias("CRT_DTM"),
    F.col("APAP_CUR_STS_DTM").alias("CUR_STTUS_DTM"),
    trim(F.col("APAP_END_DT")).alias("END_DT_SK"),
    trim(F.col("APAP_INIT_DT")).alias("INITN_DT_SK"),
    F.col("APAP_LAST_UPD_DTM").alias("LAST_UPDT_DTM"),
    trim(F.col("APAP_NEXT_REV_DT")).alias("NEXT_RVW_DT_SK"),
    F.col("APLV_SEQ_NO").alias("CUR_APL_LVL_SEQ_NO"),
    F.col("APST_SEQ_NO").alias("CUR_STTUS_SEQ_NO"),
    F.col("APAP_NEXT_REV_INT").alias("NEXT_RVW_INTRVL_NO"),
    trim(F.upper(F.col("APAP_DESC"))).alias("APL_DESC"),
    trim(F.upper(F.col("APAP_SUMMARY"))).alias("APL_SUM_DESC"),
    trim(F.col("APLK_TYPE")).alias("APLK_TYPE"),
    trim(F.col("PDPD_ID")).alias("PDPD_ID"),
    trim(F.col("PDPD_ID_1")).alias("PDPD_ID_1"),
    trim(F.col("PDPD_ID_2")).alias("PDPD_ID_2"),
    trim(F.col("PDPD_ID_3")).alias("PDPD_ID_3"),
    trim(F.col("PDPD_ID_4")).alias("PDPD_ID_4"),
    trim(F.col("PDPD_ID_5")).alias("PDPD_ID_5"),
    trim(F.col("PDPD_ID_6")).alias("PDPD_ID_6")
)

# BusinessRules (CTransformerStage) - multiple left joins with df_hf_apl_idsprod
df_BusinessRules_joins = (
    df_Trim.alias("Transform1")
    .join(df_hf_apl_idsprod.alias("Clkup"), F.col("Transform1.PDPD_ID")==F.col("Clkup.PROD_ID"), "left")
    .join(df_hf_apl_idsprod.alias("Ulkup"), F.col("Transform1.PDPD_ID_1")==F.col("Ulkup.PROD_ID"), "left")
    .join(df_hf_apl_idsprod.alias("Slkup"), F.col("Transform1.PDPD_ID_2")==F.col("Slkup.PROD_ID"), "left")
    .join(df_hf_apl_idsprod.alias("Vlkup"), F.col("Transform1.PDPD_ID_3")==F.col("Vlkup.PROD_ID"), "left")
    .join(df_hf_apl_idsprod.alias("Alkup"), F.col("Transform1.PDPD_ID_4")==F.col("Alkup.PROD_ID"), "left")
    .join(df_hf_apl_idsprod.alias("DOlkup"), F.col("Transform1.PDPD_ID_5")==F.col("DOlkup.PROD_ID"), "left")
    .join(df_hf_apl_idsprod.alias("Notlkup"), F.col("Transform1.PDPD_ID_6")==F.col("Notlkup.PROD_ID"), "left")
)

df_BusinessRules = df_BusinessRules_joins.select(
    F.col("Transform1.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("Transform1.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("Transform1.DISCARD_IN").alias("DISCARD_IN"),
    F.col("Transform1.PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("Transform1.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("Transform1.ERR_CT").alias("ERR_CT"),
    F.col("Transform1.RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("Transform1.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Transform1.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("Transform1.APL_SK").alias("APL_SK"),
    F.col("Transform1.APL_ID").alias("APL_ID"),
    F.col("Transform1.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("Transform1.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.when((F.length(F.col("Transform1.CRT_USER_SK"))==0)|F.col("Transform1.CRT_USER_SK").isNull(), F.lit("UNK")).otherwise(F.col("Transform1.CRT_USER_SK")).alias("CRT_USER_SK"),
    F.when((F.length(F.col("Transform1.CUR_PRI_USER_SK"))==0)|F.col("Transform1.CUR_PRI_USER_SK").isNull(), F.lit("NA")).otherwise(F.col("Transform1.CUR_PRI_USER_SK")).alias("CUR_PRI_USER_SK"),
    F.when((F.length(F.col("Transform1.CUR_SEC_USER_SK"))==0)|F.col("Transform1.CUR_SEC_USER_SK").isNull(), F.lit("NA")).otherwise(F.col("Transform1.CUR_SEC_USER_SK")).alias("CUR_SEC_USER_SK"),
    F.when((F.length(F.col("Transform1.CUR_TRTY_USER_SK"))==0)|F.col("Transform1.CUR_TRTY_USER_SK").isNull(), F.lit("NA")).otherwise(F.col("Transform1.CUR_TRTY_USER_SK")).alias("CUR_TRTY_USER_SK"),
    F.when((F.length(F.col("Transform1.GRP_SK"))==0)|F.col("Transform1.GRP_SK").isNull(), F.lit("UNK")).otherwise(F.col("Transform1.GRP_SK")).alias("GRP_SK"),
    F.when((F.length(F.col("Transform1.LAST_UPDT_USER_SK"))==0)|F.col("Transform1.LAST_UPDT_USER_SK").isNull(), F.lit("UNK")).otherwise(F.col("Transform1.LAST_UPDT_USER_SK")).alias("LAST_UPDT_USER_SK"),
    F.when((F.length(F.col("Transform1.MBR_SK"))==0)|F.col("Transform1.MBR_SK").isNull(), F.lit("UNK")).otherwise(F.col("Transform1.MBR_SK")).alias("MBR_SK"),
    F.when((F.length(F.col("Transform1.SUBGRP_SK"))==0)|F.col("Transform1.SUBGRP_SK").isNull(), F.lit("UNK")).otherwise(F.col("Transform1.SUBGRP_SK")).alias("SUBGRP_SK"),
    F.when((F.col("Transform1.APLK_TYPE")==F.lit("C"))&(F.col("Clkup.PROD_ID").isNotNull()), F.col("Clkup.PROD_SK"))
     .when((F.col("Transform1.APLK_TYPE")==F.lit("U"))&(F.col("Ulkup.PROD_ID").isNotNull()), F.col("Ulkup.PROD_SK"))
     .when((F.col("Transform1.APLK_TYPE")==F.lit("S"))&(F.col("Slkup.PROD_ID").isNotNull()), F.col("Slkup.PROD_SK"))
     .when((F.col("Transform1.APLK_TYPE")==F.lit("V"))&(F.col("Vlkup.PROD_ID").isNotNull()), F.col("Vlkup.PROD_SK"))
     .when((F.col("Transform1.APLK_TYPE")==F.lit("A"))&(F.col("Alkup.PROD_ID").isNotNull()), F.col("Alkup.PROD_SK"))
     .when((F.col("Transform1.APLK_TYPE")==F.lit("D"))&(F.col("DOlkup.PROD_ID").isNotNull()), F.col("DOlkup.PROD_SK"))
     .when((F.col("Transform1.APLK_TYPE")==F.lit("O"))&(F.col("DOlkup.PROD_ID").isNotNull()), F.col("DOlkup.PROD_SK"))
     .when(F.col("Notlkup.PROD_ID").isNotNull(), F.col("Notlkup.PROD_SK"))
     .otherwise(F.lit(0)).alias("PROD_SK"),
    F.when((F.col("Transform1.APLK_TYPE")==F.lit("C"))&(F.col("Clkup.PROD_ID").isNotNull()), F.col("Clkup.PROD_SH_NM_SK"))
     .when((F.col("Transform1.APLK_TYPE")==F.lit("U"))&(F.col("Ulkup.PROD_ID").isNotNull()), F.col("Ulkup.PROD_SH_NM_SK"))
     .when((F.col("Transform1.APLK_TYPE")==F.lit("S"))&(F.col("Slkup.PROD_ID").isNotNull()), F.col("Slkup.PROD_SH_NM_SK"))
     .when((F.col("Transform1.APLK_TYPE")==F.lit("V"))&(F.col("Vlkup.PROD_ID").isNotNull()), F.col("Vlkup.PROD_SH_NM_SK"))
     .when((F.col("Transform1.APLK_TYPE")==F.lit("A"))&(F.col("Alkup.PROD_ID").isNotNull()), F.col("Alkup.PROD_SH_NM_SK"))
     .when((F.col("Transform1.APLK_TYPE")==F.lit("D"))&(F.col("DOlkup.PROD_ID").isNotNull()), F.col("DOlkup.PROD_SH_NM_SK"))
     .when((F.col("Transform1.APLK_TYPE")==F.lit("O"))&(F.col("DOlkup.PROD_ID").isNotNull()), F.col("DOlkup.PROD_SH_NM_SK"))
     .when(F.col("Notlkup.PROD_ID").isNotNull(), F.col("Notlkup.PROD_SH_NM_SK"))
     .otherwise(F.lit(0)).alias("PROD_SH_NM_SK"),
    F.when((F.length(F.col("Transform1.SUB_SK"))==0)|F.col("Transform1.SUB_SK").isNull(), F.lit("UNK")).otherwise(F.col("Transform1.SUB_SK")).alias("SUB_SK"),
    F.when((F.length(F.col("Transform1.APL_CAT_CD_SK"))==0)|F.col("Transform1.APL_CAT_CD_SK").isNull(), F.lit("NA")).otherwise(F.col("Transform1.APL_CAT_CD_SK")).alias("APL_CAT_CD_SK"),
    F.when((F.length(F.col("Transform1.APL_CUR_DCSN_CD_SK"))==0)|F.col("Transform1.APL_CUR_DCSN_CD_SK").isNull(), F.lit("NA")).otherwise(F.col("Transform1.APL_CUR_DCSN_CD_SK")).alias("APL_CUR_DCSN_CD_SK"),
    F.when((F.length(F.col("Transform1.APL_CUR_STTUS_CD_SK"))==0)|F.col("Transform1.APL_CUR_STTUS_CD_SK").isNull(), F.lit("NA")).otherwise(F.col("Transform1.APL_CUR_STTUS_CD_SK")).alias("APL_CUR_STTUS_CD_SK"),
    F.when((F.length(F.col("Transform1.APL_INITN_METH_CD_SK"))==0)|F.col("Transform1.APL_INITN_METH_CD_SK").isNull(), F.lit("NA")).otherwise(F.col("Transform1.APL_INITN_METH_CD_SK")).alias("APL_INITN_METH_CD_SK"),
    F.when((F.length(F.col("Transform1.APL_MBR_HOME_ADDR_ST_CD_SK"))==0)|F.col("Transform1.APL_MBR_HOME_ADDR_ST_CD_SK").isNull(), F.lit("NA")).otherwise(F.col("Transform1.APL_MBR_HOME_ADDR_ST_CD_SK")).alias("APL_MBR_HOME_ADDR_ST_CD_SK"),
    F.when((F.length(F.col("Transform1.APL_SUBTYP_CD_SK"))==0)|F.col("Transform1.APL_SUBTYP_CD_SK").isNull(), F.lit("NA")).otherwise(F.col("Transform1.APL_SUBTYP_CD_SK")).alias("APL_SUBTYP_CD_SK"),
    F.when((F.length(F.col("Transform1.APL_TYP_CD_SK"))==0)|F.col("Transform1.APL_TYP_CD_SK").isNull(), F.lit("NA")).otherwise(F.col("Transform1.APL_TYP_CD_SK")).alias("APL_TYP_CD_SK"),
    F.when((F.length(F.col("Transform1.CLS_PLN_PROD_CAT_CD_SK"))==0)|F.col("Transform1.CLS_PLN_PROD_CAT_CD_SK").isNull(), F.lit("NA")).otherwise(F.col("Transform1.CLS_PLN_PROD_CAT_CD_SK")).alias("CLS_PLN_PROD_CAT_CD_SK"),
    F.when((F.length(F.col("Transform1.CUR_APL_LVL_CD_SK"))==0)|F.col("Transform1.CUR_APL_LVL_CD_SK").isNull(), F.lit("NA")).otherwise(F.col("Transform1.CUR_APL_LVL_CD_SK")).alias("CUR_APL_LVL_CD_SK"),
    F.when((F.length(F.col("Transform1.CUR_APL_LVL_EXPDTD_IN"))==0)|F.col("Transform1.CUR_APL_LVL_EXPDTD_IN").isNull(), F.lit("N")).otherwise(F.col("Transform1.CUR_APL_LVL_EXPDTD_IN")).alias("CUR_APL_LVL_EXPDTD_IN"),
    F.when((F.length(F.col("Transform1.CRT_DTM"))==0)|F.col("Transform1.CRT_DTM").isNull(), F.lit("UNK")).otherwise(F.col("Transform1.CRT_DTM")).alias("CRT_DTM"),
    F.when((F.length(F.col("Transform1.CUR_STTUS_DTM"))==0)|F.col("Transform1.CUR_STTUS_DTM").isNull(), F.lit("UNK")).otherwise(F.col("Transform1.CUR_STTUS_DTM")).alias("CUR_STTUS_DTM"),
    F.col("Transform1.END_DT_SK").alias("END_DT_SK"),
    F.col("Transform1.INITN_DT_SK").alias("INITN_DT_SK"),
    F.when((F.length(F.col("Transform1.LAST_UPDT_DTM"))==0)|F.col("Transform1.LAST_UPDT_DTM").isNull(), F.lit("UNK")).otherwise(F.col("Transform1.LAST_UPDT_DTM")).alias("LAST_UPDT_DTM"),
    F.col("Transform1.NEXT_RVW_DT_SK").alias("NEXT_RVW_DT_SK"),
    F.when((F.length(F.col("Transform1.CUR_APL_LVL_SEQ_NO"))==0)|F.col("Transform1.CUR_APL_LVL_SEQ_NO").isNull(), F.lit("UNK")).otherwise(F.col("Transform1.CUR_APL_LVL_SEQ_NO")).alias("CUR_APL_LVL_SEQ_NO"),
    F.when((F.length(F.col("Transform1.CUR_STTUS_SEQ_NO"))==0)|F.col("Transform1.CUR_STTUS_SEQ_NO").isNull(), F.lit("UNK")).otherwise(F.col("Transform1.CUR_STTUS_SEQ_NO")).alias("CUR_STTUS_SEQ_NO"),
    F.when((F.length(F.col("Transform1.NEXT_RVW_INTRVL_NO"))==0)|F.col("Transform1.NEXT_RVW_INTRVL_NO").isNull(), F.lit("UNK")).otherwise(F.col("Transform1.NEXT_RVW_INTRVL_NO")).alias("NEXT_RVW_INTRVL_NO"),
    F.col("Transform1.APL_DESC").alias("APL_DESC"),
    F.col("Transform1.APL_SUM_DESC").alias("APL_SUM_DESC")
)

# Snapshot (CTransformerStage)
df_Snapshot = df_BusinessRules.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("DISCARD_IN").alias("DISCARD_IN"),
    F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("ERR_CT").alias("ERR_CT"),
    F.col("RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("APL_SK").alias("APL_SK"),
    F.col("APL_ID").alias("APL_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CRT_USER_SK").alias("CRT_USER_SK"),
    F.col("CUR_PRI_USER_SK").alias("CUR_PRI_USER_SK"),
    F.col("CUR_SEC_USER_SK").alias("CUR_SEC_USER_SK"),
    F.col("CUR_TRTY_USER_SK").alias("CUR_TRTY_USER_SK"),
    F.col("GRP_SK").alias("GRP_SK"),
    F.col("LAST_UPDT_USER_SK").alias("LAST_UPDT_USER_SK"),
    F.col("MBR_SK").alias("MBR_SK"),
    F.col("SUBGRP_SK").alias("SUBGRP_SK"),
    F.col("PROD_SK").alias("PROD_SK"),
    F.col("PROD_SH_NM_SK").alias("PROD_SH_NM_SK"),
    F.col("SUB_SK").alias("SUB_SK"),
    F.col("APL_CAT_CD_SK").alias("APL_CAT_CD_SK"),
    F.col("APL_CUR_DCSN_CD_SK").alias("APL_CUR_DCSN_CD_SK"),
    F.col("APL_CUR_STTUS_CD_SK").alias("APL_CUR_STTUS_CD_SK"),
    F.col("APL_INITN_METH_CD_SK").alias("APL_INITN_METH_CD_SK"),
    F.col("APL_MBR_HOME_ADDR_ST_CD_SK").alias("APL_MBR_HOME_ADDR_ST_CD_SK"),
    F.col("APL_SUBTYP_CD_SK").alias("APL_SUBTYP_CD_SK"),
    F.col("APL_TYP_CD_SK").alias("APL_TYP_CD_SK"),
    F.col("CLS_PLN_PROD_CAT_CD_SK").alias("CLS_PLN_PROD_CAT_CD_SK"),
    F.col("CUR_APL_LVL_CD_SK").alias("CUR_APL_LVL_CD_SK"),
    F.col("CUR_APL_LVL_EXPDTD_IN").alias("CUR_APL_LVL_EXPDTD_IN"),
    F.col("CRT_DTM").alias("CRT_DTM"),
    F.col("CUR_STTUS_DTM").alias("CUR_STTUS_DTM"),
    F.col("END_DT_SK").alias("END_DT_SK"),
    F.col("INITN_DT_SK").alias("INITN_DT_SK"),
    F.col("LAST_UPDT_DTM").alias("LAST_UPDT_DTM"),
    F.col("NEXT_RVW_DT_SK").alias("NEXT_RVW_DT_SK"),
    F.col("CUR_APL_LVL_SEQ_NO").alias("CUR_APL_LVL_SEQ_NO"),
    F.col("CUR_STTUS_SEQ_NO").alias("CUR_STTUS_SEQ_NO"),
    F.col("NEXT_RVW_INTRVL_NO").alias("NEXT_RVW_INTRVL_NO"),
    F.col("APL_DESC").alias("APL_DESC"),
    F.col("APL_SUM_DESC").alias("APL_SUM_DESC")
)

# Next transformer ("Load" link in Snapshot) has columns (APL_ID primary, INITN_DT_SK, NEXT_RVW_DT_SK)
df_Snapshot_Load = df_Snapshot.select(
    F.col("APL_ID").alias("APL_ID"),
    F.col("INITN_DT_SK").alias("INITN_DT_SK"),
    F.col("NEXT_RVW_DT_SK").alias("NEXT_RVW_DT_SK")
)

# The other link "Transform" from Snapshot -> AplMcasPkey
df_Snapshot_Transform = df_Snapshot.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("DISCARD_IN").alias("DISCARD_IN"),
    F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("ERR_CT").alias("ERR_CT"),
    F.col("RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("APL_SK").alias("APL_SK"),
    F.col("APL_ID").alias("APL_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CRT_USER_SK").alias("CRT_USER_SK"),
    F.col("CUR_PRI_USER_SK").alias("CUR_PRI_USER_SK"),
    F.col("CUR_SEC_USER_SK").alias("CUR_SEC_USER_SK"),
    F.col("CUR_TRTY_USER_SK").alias("CUR_TRTY_USER_SK"),
    F.col("GRP_SK").alias("GRP_SK"),
    F.col("LAST_UPDT_USER_SK").alias("LAST_UPDT_USER_SK"),
    F.col("MBR_SK").alias("MBR_SK"),
    F.col("SUBGRP_SK").alias("SUBGRP_SK"),
    F.col("PROD_SK").alias("PROD_SK"),
    F.col("PROD_SH_NM_SK").alias("PROD_SH_NM_SK"),
    F.col("SUB_SK").alias("SUB_SK"),
    F.col("APL_CAT_CD_SK").alias("APL_CAT_CD_SK"),
    F.col("APL_CUR_DCSN_CD_SK").alias("APL_CUR_DCSN_CD_SK"),
    F.col("APL_CUR_STTUS_CD_SK").alias("APL_CUR_STTUS_CD_SK"),
    F.col("APL_INITN_METH_CD_SK").alias("APL_INITN_METH_CD_SK"),
    F.col("APL_MBR_HOME_ADDR_ST_CD_SK").alias("APL_MBR_HOME_ADDR_ST_CD_SK"),
    F.col("APL_SUBTYP_CD_SK").alias("APL_SUBTYP_CD_SK"),
    F.col("APL_TYP_CD_SK").alias("APL_TYP_CD_SK"),
    F.col("CLS_PLN_PROD_CAT_CD_SK").alias("CLS_PLN_PROD_CAT_CD_SK"),
    F.col("CUR_APL_LVL_CD_SK").alias("CUR_APL_LVL_CD_SK"),
    F.col("CUR_APL_LVL_EXPDTD_IN").alias("CUR_APL_LVL_EXPDTD_IN"),
    F.col("CRT_DTM").alias("CRT_DTM"),
    F.col("CUR_STTUS_DTM").alias("CUR_STTUS_DTM"),
    F.col("END_DT_SK").alias("END_DT_SK"),
    F.col("INITN_DT_SK").alias("INITN_DT_SK"),
    F.col("LAST_UPDT_DTM").alias("LAST_UPDT_DTM"),
    F.col("NEXT_RVW_DT_SK").alias("NEXT_RVW_DT_SK"),
    F.col("CUR_APL_LVL_SEQ_NO").alias("CUR_APL_LVL_SEQ_NO"),
    F.col("CUR_STTUS_SEQ_NO").alias("CUR_STTUS_SEQ_NO"),
    F.col("NEXT_RVW_INTRVL_NO").alias("NEXT_RVW_INTRVL_NO"),
    F.col("APL_DESC").alias("APL_DESC"),
    F.col("APL_SUM_DESC").alias("APL_SUM_DESC")
)

# Transformer stage after Snapshot -> "Transformer" (Load pin)
# Stage variable "svSrcSysCdSk" = GetFkeyCodes("IDS",1,"SOURCE SYSTEM","FACETS","X") is unknown user-defined call, assume it is available. 
# Its output used in next columns. We'll capture it in an additional column:
df_Transformer_vars = df_Snapshot_Load.withColumn(
    "SRC_SYS_CD_SK",
    F.lit(None)  # or <...> if we needed a placeholder, but the instructions say no placeholders if default is not known. We'll assume it's computed. 
)
# Output pins => "RowCount" => B_APL
df_Transformer = df_Transformer_vars.select(
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("APL_ID").alias("APL_ID"),
    F.col("INITN_DT_SK").alias("INITN_DT_SK"),
    F.col("NEXT_RVW_DT_SK").alias("NEXT_RVW_DT_SK")
)

# B_APL (CSeqFileStage) - write to load/B_APL.dat
write_files(
    df_Transformer.select(
        "SRC_SYS_CD_SK","APL_ID","INITN_DT_SK","NEXT_RVW_DT_SK"
    ),
    f"{adls_path}/load/B_APL.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

# AplMcasPkey (CContainerStage)
params = {
    "CurrRunCycle": CurrRunCycle
}
df_AplMcasPkey = AplMcasPkey(df_Snapshot_Transform, params)

# IdsAplExtr (CSeqFileStage) => write to key/FctsAplExtr.Apl.dat.#RunID#
write_files(
    df_AplMcasPkey.select(
        "JOB_EXCTN_RCRD_ERR_SK",
        "INSRT_UPDT_CD",
        "DISCARD_IN",
        "PASS_THRU_IN",
        "FIRST_RECYC_DT",
        "ERR_CT",
        "RECYCLE_CT",
        "SRC_SYS_CD",
        "PRI_KEY_STRING",
        "APL_SK",
        "APL_ID",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "CRT_USER_SK",
        "CUR_PRI_USER_SK",
        "CUR_SEC_USER_SK",
        "CUR_TRTY_USER_SK",
        "GRP_SK",
        "LAST_UPDT_USER_SK",
        "MBR_SK",
        "PROD_SK",
        "PROD_SH_NM_SK",
        "SUBGRP_SK",
        "SUB_SK",
        "APL_CAT_CD_SK",
        "APL_CUR_DCSN_CD_SK",
        "APL_CUR_STTUS_CD_SK",
        "APL_INITN_METH_CD_SK",
        "APL_MBR_HOME_ADDR_ST_CD_SK",
        "APL_SUBTYP_CD_SK",
        "APL_TYP_CD_SK",
        "CLS_PLN_PROD_CAT_CD_SK",
        "CUR_APL_LVL_CD_SK",
        "CUR_APL_LVL_EXPDTD_IN",
        "CRT_DTM",
        "CUR_STTUS_DTM",
        "END_DT_SK",
        "INITN_DT_SK",
        "LAST_UPDT_DTM",
        "NEXT_RVW_DT_SK",
        "CUR_APL_LVL_SEQ_NO",
        "CUR_STTUS_SEQ_NO",
        "NEXT_RVW_INTRVL_NO",
        "APL_DESC",
        "APL_SUM_DESC"
    ),
    f"{adls_path}/key/FctsAplExtr.Apl.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)