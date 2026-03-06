# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2005, 2006, 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC Calling Job Name :  CmsIdsProvTxnmyNtnlProvExtrSeq
# MAGIC 
# MAGIC                            
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                          Date                    Project/Altiris #                     Change Description                                                           Development Project               Code Reviewer                Date Reviewed
# MAGIC ----------------------------------      -------------------      -----------------------------------           ---------------------------------------------------------                                    ----------------------------------              ---------------------------------       -------------------------
# MAGIC Ravi Abburi                      2017-10-25            5781 HEDIS                       Initial Programming                                                                    IntegrateDev2                   Kalyan Neelam                 2018-01-30
# MAGIC 
# MAGIC Akash Parsha                  2017-02-26             201102                              Additional column has been added to the source file from        IntegarteDev1                   Jaideep Mankala              02/27/2020
# MAGIC                                                                                                                   the NPI Cms weekly files that are sourced into the
# MAGIC                                                                                                                   IDS -  - NTNL_PROV & NTNL_TXNMY and ENTY_RGSTRN

# MAGIC New Taxonomy Codes will be inserted in to the TXNMY_CD table. 
# MAGIC Data is used in the IdsTxnmyCdPkey .
# MAGIC Transformed Data will land into a Dataset for Primary Keying job.
# MAGIC 
# MAGIC Data is partitioned on Natural Key Columns.
# MAGIC JobName: CmsIdsNtnlProvTxnmyXfm
# MAGIC Sort on the Key Columns
# MAGIC Business Logic
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

# Retrieve job parameters
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
SrcSysCd = get_widget_value('SrcSysCd','')
RunID = get_widget_value('RunID','')
CMS_ProvTxnmyFile = get_widget_value('CMS_ProvTxnmyFile','')
IDSRunCycle = get_widget_value('IDSRunCycle','')

# Read from DB2ConnectorPX (db2_TXNMY_CD_Lkp)
jdbc_url_db2_TXNMY_CD_Lkp, jdbc_props_db2_TXNMY_CD_Lkp = get_db_config(ids_secret_name)
extract_query_db2_TXNMY_CD_Lkp = f"SELECT TXNMY_CD from {IDSOwner}.TXNMY_CD"
df_db2_TXNMY_CD_Lkp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_TXNMY_CD_Lkp)
    .options(**jdbc_props_db2_TXNMY_CD_Lkp)
    .option("query", extract_query_db2_TXNMY_CD_Lkp)
    .load()
)

# Read from DB2ConnectorPX (db2_NTNL_PROV_TXNMY_In)
jdbc_url_db2_NTNL_PROV_TXNMY_In, jdbc_props_db2_NTNL_PROV_TXNMY_In = get_db_config(ids_secret_name)
extract_query_db2_NTNL_PROV_TXNMY_In = f"""SELECT distinct
    NTNL_PROV_ID,
    max(LAST_UPDT_RUN_CYC_EXCTN_SK) LAST_UPDT_RUN_CYC_EXCTN_SK
FROM 
    {IDSOwner}.NTNL_PROV_TXNMY
GROUP BY NTNL_PROV_ID
"""
df_db2_NTNL_PROV_TXNMY_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_NTNL_PROV_TXNMY_In)
    .options(**jdbc_props_db2_NTNL_PROV_TXNMY_In)
    .option("query", extract_query_db2_NTNL_PROV_TXNMY_In)
    .load()
)

# Read from DB2ConnectorPX (db2_NPI_TXNMY_CD_In)
jdbc_url_db2_NPI_TXNMY_CD_In, jdbc_props_db2_NPI_TXNMY_CD_In = get_db_config(ids_secret_name)
extract_query_db2_NPI_TXNMY_CD_In = f"""SELECT 
    NPI.NTNL_PROV_ID,
    NPI.TXNMY_CD,
    NPI.ENTY_LIC_ST_CD,
    NPI.LIC_NO,
    TX.TXNMY_CD as HEALTHCARE_PROVIDER_TAXONOMY_GROUP,
    NPI.CUR_RCRD_IN,
    NPI.NTNL_PROV_TXNMY_PRI_IN
FROM 
    {IDSOwner}.NTNL_PROV_TXNMY NPI 
    left outer join {IDSOwner}.K_TXNMY_CD TX on NPI.GRP_TXNMY_CD_SK = TX.TXNMY_CD_SK
"""
df_db2_NPI_TXNMY_CD_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_NPI_TXNMY_CD_In)
    .options(**jdbc_props_db2_NPI_TXNMY_CD_In)
    .option("query", extract_query_db2_NPI_TXNMY_CD_In)
    .load()
)

# Read from PxSequentialFile (CMS_Prov_Txnmy)
schema_CMS_Prov_Txnmy = StructType([
    StructField("NPI", StringType(), True),
    StructField("ENTITY_TYPE_CODE", StringType(), True),
    StructField("REPLACEMENT_NPI", StringType(), True),
    StructField("EMPLOYER_IDENTIFICATION_NUMBER", StringType(), True),
    StructField("PROVIDER_ORGANIZATION_NAME", StringType(), True),
    StructField("PROV_LAST_NAME", StringType(), True),
    StructField("PROV_FIRST_NAME", StringType(), True),
    StructField("PROV_MIDDLE_NAME", StringType(), True),
    StructField("PROV_NAME_PREFIX_TEXT", StringType(), True),
    StructField("PROV_NAME_SUFFIX_TEXT", StringType(), True),
    StructField("PROV_CREDENTIAL_TEXT", StringType(), True),
    StructField("PROV_OTHER_ORGANIZATION_NAME", StringType(), True),
    StructField("PROV_OTHER_ORGANIZATION_NAME_TYPE_CODE", StringType(), True),
    StructField("PROV_OTHER_LAST_NAME", StringType(), True),
    StructField("PROV_OTHER_FIRST_NAME", StringType(), True),
    StructField("PROV_OTHER_MIDDLE_NAME", StringType(), True),
    StructField("PROV_OTHER_NAME_PREFIX_TEXT", StringType(), True),
    StructField("PROV_OTHER_NAME_SUFFIX_TEXT", StringType(), True),
    StructField("PROV_OTHER_CREDENTIAL_TEXT", StringType(), True),
    StructField("PROV_OTHER_LAST_NAME_TYPE_CODE", StringType(), True),
    StructField("PROV_FIRST_LINE_BUSINESS_MAILING_ADDR", StringType(), True),
    StructField("PROV_SECOND_LINE_BUSINESS_MAILING_ADDR", StringType(), True),
    StructField("PROV_BUS_MAILING_ADDR_CITY_NAME", StringType(), True),
    StructField("PROV_BUS_MAILING_ADDR_STATE_NAME", StringType(), True),
    StructField("PROV_BUS_MAILING_ADDR_POSTAL_CODE", StringType(), True),
    StructField("PROV_BUS_MAILING_ADDR_COUNTRY_CODE", StringType(), True),
    StructField("PROV_BUS_MAILING_ADDR_TELEPHONE_NUMBER", StringType(), True),
    StructField("PROV_BUS_MAILING_ADDR_FAX_NUMBER", StringType(), True),
    StructField("PROV_FIRST_LINE_BUS_PRACTICE_LOCA_ADDR", StringType(), True),
    StructField("PROV_SECOND_LINE_BUS_PRACTICE_LOC_ADDR", StringType(), True),
    StructField("PROV_BUS_PRACTICE_LOC_ADDR_CITY_NAME", StringType(), True),
    StructField("PROV_BUS_PRACTICE_LOC_ADDR_STATE_NAME", StringType(), True),
    StructField("PROV_BUS_PRACTICE_LOC_ADDR_POSTAL_CODE", StringType(), True),
    StructField("PROV_BUS_PRACTICE_LOC_ADDR_COUNTRY_CODE", StringType(), True),
    StructField("PROV_BUS_PRACTICE_LOC_ADDR_TEL_NUMBER", StringType(), True),
    StructField("PROV_BUS_PRACTICE_LOC_ADDR_FAX_NUMBER", StringType(), True),
    StructField("PROV_ENUMERATION_DATE", StringType(), True),
    StructField("LAST_UPDATE_DATE", StringType(), True),
    StructField("NPI_DEACTIVATION_REASON_CODE", StringType(), True),
    StructField("NPI_DEACTIVATION_DATE", StringType(), True),
    StructField("NPI_REACTIVATION_DATE", StringType(), True),
    StructField("PROVIDER_GENDER_CODE", StringType(), True),
    StructField("AUTHORIZED_OFFICIAL_LAST_NAME", StringType(), True),
    StructField("AUTHORIZED_OFFICIAL_FIRST_NAME", StringType(), True),
    StructField("AUTHORIZED_OFFICIAL_MIDDLE_NAME", StringType(), True),
    StructField("AUTHORIZED_OFFICIAL_TITLE_OR_POSITION", StringType(), True),
    StructField("AUTHORIZED_OFFICIAL_TELEPHONE_NUMBER", StringType(), True),
    StructField("HEALTHCARE_PROVIDER_TAXONOMY_CODE_1", StringType(), True),
    StructField("PROVIDER_LICENSE_NUMBER_1", StringType(), True),
    StructField("PROVIDER_LICENSE_NUMBER_STATE_CODE_1", StringType(), True),
    StructField("HLTHCARE_PROV_PRI_TAXONOMY_SWITCH_1", StringType(), True),
    StructField("HLTHCARE_PROV_TAXONOMY_CODE_2", StringType(), True),
    StructField("PROV_LICENSE_NUMBER_2", StringType(), True),
    StructField("PROV_LICENSE_NUMBER_STATE_CODE_2", StringType(), True),
    StructField("HLTHCARE_PROV_PRI_TAXONOMY_SWITCH_2", StringType(), True),
    StructField("HLTHCARE_PROV_TAXONOMY_CODE_3", StringType(), True),
    StructField("PROV_LICENSE_NUMBER_3", StringType(), True),
    StructField("PROV_LICENSE_NUMBER_STATE_CODE_3", StringType(), True),
    StructField("HLTHCARE_PROV_PRIM_TAXONOMY_SWITCH_3", StringType(), True),
    StructField("HLTHCARE_PROV_TAXONOMY_CODE_4", StringType(), True),
    StructField("PROV_LICENSE_NUMBER_4", StringType(), True),
    StructField("PROV_LICENSE_NUMBER_STATE_CODE_4", StringType(), True),
    StructField("HLTHCARE_PROV_PRIM_TAXONOMY_SWITCH_4", StringType(), True),
    StructField("HLTHCARE_PROV_TAXONOMY_CODE_5", StringType(), True),
    StructField("PROV_LICENSE_NUMBER_5", StringType(), True),
    StructField("PROV_LICENSE_NUMBER_STATE_CODE_5", StringType(), True),
    StructField("HLTHCARE_PROV_PRIM_TAXONOMY_SWITCH_5", StringType(), True),
    StructField("HLTHCARE_PROV_TAXONOMY_CODE_6", StringType(), True),
    StructField("PROV_LICENSE_NUMBER_6", StringType(), True),
    StructField("PROV_LICENSE_NUMBER_STATE_CODE_6", StringType(), True),
    StructField("HLTHCARE_PROV_PRI_TAXONOMY_SWITCH_6", StringType(), True),
    StructField("HLTHCARE_PROV_TAXONOMY_CODE_7", StringType(), True),
    StructField("PROV_LICENSE_NUMBER_7", StringType(), True),
    StructField("PROV_LICENSE_NUMBER_STATE_CODE_7", StringType(), True),
    StructField("HLTHCARE_PROV_PRI_TAXONOMY_SWITCH_7", StringType(), True),
    StructField("HLTHCARE_PROV_TAXONOMY_CODE_8", StringType(), True),
    StructField("PROV_LICENSE_NUMBER_8", StringType(), True),
    StructField("PROV_LICENSE_NUMBER_STATE_CODE_8", StringType(), True),
    StructField("HLTHCARE_PROV_PRIM_TAXONOMY_SWITCH_8", StringType(), True),
    StructField("HLTHCARE_PROV_TAXONOMY_CODE_9", StringType(), True),
    StructField("PROVIDER_LICENSE_NUMBER_9", StringType(), True),
    StructField("PROVIDER_LICENSE_NUMBER_STATE_CODE_9", StringType(), True),
    StructField("HLTHCARE_PROV_PRIM_TAXONOMY_SWITCH_9", StringType(), True),
    StructField("HLTHCARE_PROV_TAXONOMY_CODE_10", StringType(), True),
    StructField("PROVIDER_LICENSE_NUMBER_10", StringType(), True),
    StructField("PROVIDER_LICENSE_NUMBER_STATE_CODE_10", StringType(), True),
    StructField("HLTHCARE_PROV_PRIM_TAXONOMY_SWITCH_10", StringType(), True),
    StructField("HLTHCARE_PROV_TAXONOMY_CODE_11", StringType(), True),
    StructField("PROVIDER_LICENSE_NUMBER_11", StringType(), True),
    StructField("PROVIDER_LICENSE_NUMBER_STATE_CODE_11", StringType(), True),
    StructField("HLTHCARE_PROV_PRIM_TAXONOMY_SWITCH_11", StringType(), True),
    StructField("HLTHCARE_PROV_TAXONOMY_CODE_12", StringType(), True),
    StructField("PROVIDER_LICENSE_NUMBER_12", StringType(), True),
    StructField("PROVIDER_LICENSE_NUMBER_STATE_CODE_12", StringType(), True),
    StructField("HLTHCARE_PROV_PRIM_TAXONOMY_SWITCH_12", StringType(), True),
    StructField("HLTHCARE_PROV_TAXONOMY_CODE_13", StringType(), True),
    StructField("PROVIDER_LICENSE_NUMBER_13", StringType(), True),
    StructField("PROVIDER_LICENSE_NUMBER_STATE_CODE_13", StringType(), True),
    StructField("HLTHCARE_PROV_PRIM_TAXONOMY_SWITCH_13", StringType(), True),
    StructField("HLTHCARE_PROV_TAXONOMY_CODE_14", StringType(), True),
    StructField("PROVIDER_LICENSE_NUMBER_14", StringType(), True),
    StructField("PROVIDER_LICENSE_NUMBER_STATE_CODE_14", StringType(), True),
    StructField("HLTHCARE_PROV_PRIM_TAXONOMY_SWITCH_14", StringType(), True),
    StructField("HLTHCARE_PROV_TAXONOMY_CODE_15", StringType(), True),
    StructField("PROVIDER_LICENSE_NUMBER_15", StringType(), True),
    StructField("PROVIDER_LICENSE_NUMBER_STATE_CODE_15", StringType(), True),
    StructField("HLTHCARE_PROV_PRIM_TAXONOMY_SWITCH_15", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_1", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_TYPE_CODE_1", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_STATE_1", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_ISSUER_1", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_2", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_TYPE_CODE_2", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_STATE_2", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_ISSUER_2", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_3", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_TYPE_CODE_3", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_STATE_3", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_ISSUER_3", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_4", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_TYPE_CODE_4", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_STATE_4", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_ISSUER_4", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_5", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_TYPE_CODE_5", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_STATE_5", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_ISSUER_5", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_6", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_TYPE_CODE_6", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_STATE_6", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_ISSUER_6", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_7", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_TYPE_CODE_7", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_STATE_7", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_ISSUER_7", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_8", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_TYPE_CODE_8", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_STATE_8", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_ISSUER_8", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_9", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_TYPE_CODE_9", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_STATE_9", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_ISSUER_9", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_10", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_TYPE_CODE_10", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_STATE_10", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_ISSUER_10", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_11", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_TYPE_CODE_11", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_STATE_11", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_ISSUER_11", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_12", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_TYPE_CODE_12", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_STATE_12", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_ISSUER_12", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_13", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_TYPE_CODE_13", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_STATE_13", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_ISSUER_13", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_14", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_TYPE_CODE_14", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_STATE_14", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_ISSUER_14", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_15", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_TYPE_CODE_15", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_STATE_15", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_ISSUER_15", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_16", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_TYPE_CODE_16", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_STATE_16", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_ISSUER_16", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_17", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_TYPE_CODE_17", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_STATE_17", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_ISSUER_17", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_18", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_TYPE_CODE_18", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_STATE_18", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_ISSUER_18", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_19", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_TYPE_CODE_19", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_STATE_19", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_ISSUER_19", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_20", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_TYPE_CODE_20", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_STATE_20", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_ISSUER_20", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_21", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_TYPE_CODE_21", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_STATE_21", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_ISSUER_21", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_22", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_TYPE_CODE_22", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_STATE_22", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_ISSUER_22", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_23", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_TYPE_CODE_23", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_STATE_23", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_ISSUER_23", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_24", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_TYPE_CODE_24", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_STATE_24", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_ISSUER_24", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_25", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_TYPE_CODE_25", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_STATE_25", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_ISSUER_25", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_26", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_TYPE_CODE_26", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_STATE_26", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_ISSUER_26", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_27", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_TYPE_CODE_27", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_STATE_27", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_ISSUER_27", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_28", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_TYPE_CODE_28", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_STATE_28", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_ISSUER_28", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_29", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_TYPE_CODE_29", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_STATE_29", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_ISSUER_29", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_30", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_TYPE_CODE_30", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_STATE_30", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_ISSUER_30", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_31", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_TYPE_CODE_31", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_STATE_31", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_ISSUER_31", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_32", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_TYPE_CODE_32", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_STATE_32", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_ISSUER_32", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_33", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_TYPE_CODE_33", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_STATE_33", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_ISSUER_33", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_34", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_TYPE_CODE_34", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_STATE_34", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_ISSUER_34", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_35", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_TYPE_CODE_35", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_STATE_35", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_ISSUER_35", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_36", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_TYPE_CODE_36", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_STATE_36", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_ISSUER_36", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_37", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_TYPE_CODE_37", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_STATE_37", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_ISSUER_37", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_38", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_TYPE_CODE_38", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_STATE_38", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_ISSUER_38", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_39", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_TYPE_CODE_39", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_STATE_39", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_ISSUER_39", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_40", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_TYPE_CODE_40", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_STATE_40", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_ISSUER_40", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_41", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_TYPE_CODE_41", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_STATE_41", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_ISSUER_41", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_42", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_TYPE_CODE_42", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_STATE_42", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_ISSUER_42", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_43", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_TYPE_CODE_43", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_STATE_43", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_ISSUER_43", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_44", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_TYPE_CODE_44", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_STATE_44", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_ISSUER_44", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_45", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_TYPE_CODE_45", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_STATE_45", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_ISSUER_45", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_46", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_TYPE_CODE_46", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_STATE_46", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_ISSUER_46", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_47", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_TYPE_CODE_47", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_STATE_47", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_ISSUER_47", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_48", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_TYPE_CODE_48", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_STATE_48", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_ISSUER_48", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_49", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_TYPE_CODE_49", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_STATE_49", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_ISSUER_49", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_50", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_TYPE_CODE_50", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_STATE_50", StringType(), True),
    StructField("OTHER_PROVIDER_IDENTIFIER_ISSUER_50", StringType(), True),
    StructField("IS_SOLE_PROPRIETOR", StringType(), True),
    StructField("IS_ORGANIZATION_SUBPART", StringType(), True),
    StructField("PARENT_ORGANIZATION_LBN", StringType(), True),
    StructField("PARENT_ORGANIZATION_TIN", StringType(), True),
    StructField("AUTHORIZED_OFFICIAL_NAME_PREFIX_TEXT", StringType(), True),
    StructField("AUTHORIZED_OFFICIAL_NAME_SUFFIX_TEXT", StringType(), True),
    StructField("AUTHORIZED_OFFICIAL_CREDENTIAL_TEXT", StringType(), True),
    StructField("HEALTHCARE_PROVIDER_TAXONOMY_GROUP_1", StringType(), True),
    StructField("HEALTHCARE_PROVIDER_TAXONOMY_GROUP_2", StringType(), True),
    StructField("HEALTHCARE_PROVIDER_TAXONOMY_GROUP_3", StringType(), True),
    StructField("HEALTHCARE_PROVIDER_TAXONOMY_GROUP_4", StringType(), True),
    StructField("HEALTHCARE_PROVIDER_TAXONOMY_GROUP_5", StringType(), True),
    StructField("HEALTHCARE_PROVIDER_TAXONOMY_GROUP_6", StringType(), True),
    StructField("HEALTHCARE_PROVIDER_TAXONOMY_GROUP_7", StringType(), True),
    StructField("HEALTHCARE_PROVIDER_TAXONOMY_GROUP_8", StringType(), True),
    StructField("HEALTHCARE_PROVIDER_TAXONOMY_GROUP_9", StringType(), True),
    StructField("HEALTHCARE_PROVIDER_TAXONOMY_GROUP_10", StringType(), True),
    StructField("HEALTHCARE_PROVIDER_TAXONOMY_GROUP_11", StringType(), True),
    StructField("HEALTHCARE_PROVIDER_TAXONOMY_GROUP_12", StringType(), True),
    StructField("HEALTHCARE_PROVIDER_TAXONOMY_GROUP_13", StringType(), True),
    StructField("HEALTHCARE_PROVIDER_TAXONOMY_GROUP_14", StringType(), True),
    StructField("HEALTHCARE_PROVIDER_TAXONOMY_GROUP_15", StringType(), True),
    StructField("CREATION_DATE", StringType(), True)
])

df_CMS_Prov_Txnmy = (
    spark.read.format("csv")
    .schema(schema_CMS_Prov_Txnmy)
    .option("header", True)
    .option("sep", ",")
    .option("quote", '"')
    .option("nullValue", None)
    .load(f"{adls_path_raw}/landing/{CMS_ProvTxnmyFile}")
)

# Xfrm_ProvExtr (CTransformerStage)
df_Xfrm_ProvExtr = df_CMS_Prov_Txnmy.select(
    F.col("NPI").alias("NPI"),
    F.col("HEALTHCARE_PROVIDER_TAXONOMY_CODE_1").alias("HEALTHCARE_PROVIDER_TAXONOMY_CODE_1"),
    F.col("HLTHCARE_PROV_TAXONOMY_CODE_2").alias("HLTHCARE_PROV_TAXONOMY_CODE_2"),
    F.col("HLTHCARE_PROV_TAXONOMY_CODE_3").alias("HLTHCARE_PROV_TAXONOMY_CODE_3"),
    F.col("HLTHCARE_PROV_TAXONOMY_CODE_4").alias("HLTHCARE_PROV_TAXONOMY_CODE_4"),
    F.col("HLTHCARE_PROV_TAXONOMY_CODE_5").alias("HLTHCARE_PROV_TAXONOMY_CODE_5"),
    F.col("HLTHCARE_PROV_TAXONOMY_CODE_6").alias("HLTHCARE_PROV_TAXONOMY_CODE_6"),
    F.col("HLTHCARE_PROV_TAXONOMY_CODE_7").alias("HLTHCARE_PROV_TAXONOMY_CODE_7"),
    F.col("HLTHCARE_PROV_TAXONOMY_CODE_8").alias("HLTHCARE_PROV_TAXONOMY_CODE_8"),
    F.col("HLTHCARE_PROV_TAXONOMY_CODE_9").alias("HLTHCARE_PROV_TAXONOMY_CODE_9"),
    F.col("HLTHCARE_PROV_TAXONOMY_CODE_10").alias("HLTHCARE_PROV_TAXONOMY_CODE_10"),
    F.col("HLTHCARE_PROV_TAXONOMY_CODE_11").alias("HLTHCARE_PROV_TAXONOMY_CODE_11"),
    F.col("HLTHCARE_PROV_TAXONOMY_CODE_12").alias("HLTHCARE_PROV_TAXONOMY_CODE_12"),
    F.col("HLTHCARE_PROV_TAXONOMY_CODE_13").alias("HLTHCARE_PROV_TAXONOMY_CODE_13"),
    F.col("HLTHCARE_PROV_TAXONOMY_CODE_14").alias("HLTHCARE_PROV_TAXONOMY_CODE_14"),
    F.col("HLTHCARE_PROV_TAXONOMY_CODE_15").alias("HLTHCARE_PROV_TAXONOMY_CODE_15"),
    F.col("PROVIDER_LICENSE_NUMBER_STATE_CODE_1").alias("PROVIDER_LICENSE_NUMBER_STATE_CODE_1"),
    F.col("PROV_LICENSE_NUMBER_STATE_CODE_2").alias("PROV_LICENSE_NUMBER_STATE_CODE_2"),
    F.col("PROV_LICENSE_NUMBER_STATE_CODE_3").alias("PROV_LICENSE_NUMBER_STATE_CODE_3"),
    F.col("PROV_LICENSE_NUMBER_STATE_CODE_4").alias("PROV_LICENSE_NUMBER_STATE_CODE_4"),
    F.col("PROV_LICENSE_NUMBER_STATE_CODE_5").alias("PROV_LICENSE_NUMBER_STATE_CODE_5"),
    F.col("PROV_LICENSE_NUMBER_STATE_CODE_6").alias("PROV_LICENSE_NUMBER_STATE_CODE_6"),
    F.col("PROV_LICENSE_NUMBER_STATE_CODE_7").alias("PROV_LICENSE_NUMBER_STATE_CODE_7"),
    F.col("PROV_LICENSE_NUMBER_STATE_CODE_8").alias("PROV_LICENSE_NUMBER_STATE_CODE_8"),
    F.col("PROVIDER_LICENSE_NUMBER_STATE_CODE_9").alias("PROVIDER_LICENSE_NUMBER_STATE_CODE_9"),
    F.col("PROVIDER_LICENSE_NUMBER_STATE_CODE_10").alias("PROVIDER_LICENSE_NUMBER_STATE_CODE_10"),
    F.col("PROVIDER_LICENSE_NUMBER_STATE_CODE_11").alias("PROVIDER_LICENSE_NUMBER_STATE_CODE_11"),
    F.col("PROVIDER_LICENSE_NUMBER_STATE_CODE_12").alias("PROVIDER_LICENSE_NUMBER_STATE_CODE_12"),
    F.col("PROVIDER_LICENSE_NUMBER_STATE_CODE_13").alias("PROVIDER_LICENSE_NUMBER_STATE_CODE_13"),
    F.col("PROVIDER_LICENSE_NUMBER_STATE_CODE_14").alias("PROVIDER_LICENSE_NUMBER_STATE_CODE_14"),
    F.col("PROVIDER_LICENSE_NUMBER_STATE_CODE_15").alias("PROVIDER_LICENSE_NUMBER_STATE_CODE_15"),
    F.col("PROVIDER_LICENSE_NUMBER_1").alias("PROVIDER_LICENSE_NUMBER_1"),
    F.col("PROV_LICENSE_NUMBER_2").alias("PROV_LICENSE_NUMBER_2"),
    F.col("PROV_LICENSE_NUMBER_3").alias("PROV_LICENSE_NUMBER_3"),
    F.col("PROV_LICENSE_NUMBER_4").alias("PROV_LICENSE_NUMBER_4"),
    F.col("PROV_LICENSE_NUMBER_5").alias("PROV_LICENSE_NUMBER_5"),
    F.col("PROV_LICENSE_NUMBER_6").alias("PROV_LICENSE_NUMBER_6"),
    F.col("PROV_LICENSE_NUMBER_7").alias("PROV_LICENSE_NUMBER_7"),
    F.col("PROV_LICENSE_NUMBER_8").alias("PROV_LICENSE_NUMBER_8"),
    F.col("PROVIDER_LICENSE_NUMBER_9").alias("PROVIDER_LICENSE_NUMBER_9"),
    F.col("PROVIDER_LICENSE_NUMBER_10").alias("PROVIDER_LICENSE_NUMBER_10"),
    F.col("PROVIDER_LICENSE_NUMBER_11").alias("PROVIDER_LICENSE_NUMBER_11"),
    F.col("PROVIDER_LICENSE_NUMBER_12").alias("PROVIDER_LICENSE_NUMBER_12"),
    F.col("PROVIDER_LICENSE_NUMBER_13").alias("PROVIDER_LICENSE_NUMBER_13"),
    F.col("PROVIDER_LICENSE_NUMBER_14").alias("PROVIDER_LICENSE_NUMBER_14"),
    F.col("PROVIDER_LICENSE_NUMBER_15").alias("PROVIDER_LICENSE_NUMBER_15"),
    F.col("HEALTHCARE_PROVIDER_TAXONOMY_GROUP_1").alias("HEALTHCARE_PROVIDER_TAXONOMY_GROUP_1"),
    F.col("HEALTHCARE_PROVIDER_TAXONOMY_GROUP_2").alias("HEALTHCARE_PROVIDER_TAXONOMY_GROUP_2"),
    F.col("HEALTHCARE_PROVIDER_TAXONOMY_GROUP_3").alias("HEALTHCARE_PROVIDER_TAXONOMY_GROUP_3"),
    F.col("HEALTHCARE_PROVIDER_TAXONOMY_GROUP_4").alias("HEALTHCARE_PROVIDER_TAXONOMY_GROUP_4"),
    F.col("HEALTHCARE_PROVIDER_TAXONOMY_GROUP_5").alias("HEALTHCARE_PROVIDER_TAXONOMY_GROUP_5"),
    F.col("HEALTHCARE_PROVIDER_TAXONOMY_GROUP_6").alias("HEALTHCARE_PROVIDER_TAXONOMY_GROUP_6"),
    F.col("HEALTHCARE_PROVIDER_TAXONOMY_GROUP_7").alias("HEALTHCARE_PROVIDER_TAXONOMY_GROUP_7"),
    F.col("HEALTHCARE_PROVIDER_TAXONOMY_GROUP_8").alias("HEALTHCARE_PROVIDER_TAXONOMY_GROUP_8"),
    F.col("HEALTHCARE_PROVIDER_TAXONOMY_GROUP_9").alias("HEALTHCARE_PROVIDER_TAXONOMY_GROUP_9"),
    F.col("HEALTHCARE_PROVIDER_TAXONOMY_GROUP_10").alias("HEALTHCARE_PROVIDER_TAXONOMY_GROUP_10"),
    F.col("HEALTHCARE_PROVIDER_TAXONOMY_GROUP_11").alias("HEALTHCARE_PROVIDER_TAXONOMY_GROUP_11"),
    F.col("HEALTHCARE_PROVIDER_TAXONOMY_GROUP_12").alias("HEALTHCARE_PROVIDER_TAXONOMY_GROUP_12"),
    F.col("HEALTHCARE_PROVIDER_TAXONOMY_GROUP_13").alias("HEALTHCARE_PROVIDER_TAXONOMY_GROUP_13"),
    F.col("HEALTHCARE_PROVIDER_TAXONOMY_GROUP_14").alias("HEALTHCARE_PROVIDER_TAXONOMY_GROUP_14"),
    F.col("HEALTHCARE_PROVIDER_TAXONOMY_GROUP_15").alias("HEALTHCARE_PROVIDER_TAXONOMY_GROUP_15"),
    F.col("HLTHCARE_PROV_PRI_TAXONOMY_SWITCH_1").alias("HLTHCARE_PROV_PRI_TAXONOMY_SWITCH_1"),
    F.col("HLTHCARE_PROV_PRI_TAXONOMY_SWITCH_2").alias("HLTHCARE_PROV_PRI_TAXONOMY_SWITCH_2"),
    F.col("HLTHCARE_PROV_PRIM_TAXONOMY_SWITCH_3").alias("HLTHCARE_PROV_PRIM_TAXONOMY_SWITCH_3"),
    F.col("HLTHCARE_PROV_PRIM_TAXONOMY_SWITCH_4").alias("HLTHCARE_PROV_PRIM_TAXONOMY_SWITCH_4"),
    F.col("HLTHCARE_PROV_PRIM_TAXONOMY_SWITCH_5").alias("HLTHCARE_PROV_PRIM_TAXONOMY_SWITCH_5"),
    F.col("HLTHCARE_PROV_PRI_TAXONOMY_SWITCH_6").alias("HLTHCARE_PROV_PRI_TAXONOMY_SWITCH_6"),
    F.col("HLTHCARE_PROV_PRI_TAXONOMY_SWITCH_7").alias("HLTHCARE_PROV_PRI_TAXONOMY_SWITCH_7"),
    F.col("HLTHCARE_PROV_PRIM_TAXONOMY_SWITCH_8").alias("HLTHCARE_PROV_PRIM_TAXONOMY_SWITCH_8"),
    F.col("HLTHCARE_PROV_PRIM_TAXONOMY_SWITCH_9").alias("HLTHCARE_PROV_PRIM_TAXONOMY_SWITCH_9"),
    F.col("HLTHCARE_PROV_PRIM_TAXONOMY_SWITCH_10").alias("HLTHCARE_PROV_PRIM_TAXONOMY_SWITCH_10"),
    F.col("HLTHCARE_PROV_PRIM_TAXONOMY_SWITCH_11").alias("HLTHCARE_PROV_PRIM_TAXONOMY_SWITCH_11"),
    F.col("HLTHCARE_PROV_PRIM_TAXONOMY_SWITCH_12").alias("HLTHCARE_PROV_PRIM_TAXONOMY_SWITCH_12"),
    F.col("HLTHCARE_PROV_PRIM_TAXONOMY_SWITCH_13").alias("HLTHCARE_PROV_PRIM_TAXONOMY_SWITCH_13"),
    F.col("HLTHCARE_PROV_PRIM_TAXONOMY_SWITCH_14").alias("HLTHCARE_PROV_PRIM_TAXONOMY_SWITCH_14"),
    F.col("HLTHCARE_PROV_PRIM_TAXONOMY_SWITCH_15").alias("HLTHCARE_PROV_PRIM_TAXONOMY_SWITCH_15"),
    F.lit(SrcSysCd).alias("SrcSysCd")
)

# pivot_ColRws (PxPivot)
df_pivot_ColRws_pre = df_Xfrm_ProvExtr.withColumn(
    "hc_code_arr",
    F.array(
        "HEALTHCARE_PROVIDER_TAXONOMY_CODE_1","HLTHCARE_PROV_TAXONOMY_CODE_2","HLTHCARE_PROV_TAXONOMY_CODE_3","HLTHCARE_PROV_TAXONOMY_CODE_4","HLTHCARE_PROV_TAXONOMY_CODE_5","HLTHCARE_PROV_TAXONOMY_CODE_6","HLTHCARE_PROV_TAXONOMY_CODE_7","HLTHCARE_PROV_TAXONOMY_CODE_8","HLTHCARE_PROV_TAXONOMY_CODE_9","HLTHCARE_PROV_TAXONOMY_CODE_10","HLTHCARE_PROV_TAXONOMY_CODE_11","HLTHCARE_PROV_TAXONOMY_CODE_12","HLTHCARE_PROV_TAXONOMY_CODE_13","HLTHCARE_PROV_TAXONOMY_CODE_14","HLTHCARE_PROV_TAXONOMY_CODE_15"
    )
).withColumn(
    "state_arr",
    F.array(
        "PROVIDER_LICENSE_NUMBER_STATE_CODE_1","PROV_LICENSE_NUMBER_STATE_CODE_2","PROV_LICENSE_NUMBER_STATE_CODE_3","PROV_LICENSE_NUMBER_STATE_CODE_4","PROV_LICENSE_NUMBER_STATE_CODE_5","PROV_LICENSE_NUMBER_STATE_CODE_6","PROV_LICENSE_NUMBER_STATE_CODE_7","PROV_LICENSE_NUMBER_STATE_CODE_8","PROVIDER_LICENSE_NUMBER_STATE_CODE_9","PROVIDER_LICENSE_NUMBER_STATE_CODE_10","PROVIDER_LICENSE_NUMBER_STATE_CODE_11","PROVIDER_LICENSE_NUMBER_STATE_CODE_12","PROVIDER_LICENSE_NUMBER_STATE_CODE_13","PROVIDER_LICENSE_NUMBER_STATE_CODE_14","PROVIDER_LICENSE_NUMBER_STATE_CODE_15"
    )
).withColumn(
    "lic_no_arr",
    F.array(
        "PROVIDER_LICENSE_NUMBER_1","PROV_LICENSE_NUMBER_2","PROV_LICENSE_NUMBER_3","PROV_LICENSE_NUMBER_4","PROV_LICENSE_NUMBER_5","PROV_LICENSE_NUMBER_6","PROV_LICENSE_NUMBER_7","PROV_LICENSE_NUMBER_8","PROVIDER_LICENSE_NUMBER_9","PROVIDER_LICENSE_NUMBER_10","PROVIDER_LICENSE_NUMBER_11","PROVIDER_LICENSE_NUMBER_12","PROVIDER_LICENSE_NUMBER_13","PROVIDER_LICENSE_NUMBER_14","PROVIDER_LICENSE_NUMBER_15"
    )
).withColumn(
    "grp_arr",
    F.array(
        "HEALTHCARE_PROVIDER_TAXONOMY_GROUP_1","HEALTHCARE_PROVIDER_TAXONOMY_GROUP_2","HEALTHCARE_PROVIDER_TAXONOMY_GROUP_3","HEALTHCARE_PROVIDER_TAXONOMY_GROUP_4","HEALTHCARE_PROVIDER_TAXONOMY_GROUP_5","HEALTHCARE_PROVIDER_TAXONOMY_GROUP_6","HEALTHCARE_PROVIDER_TAXONOMY_GROUP_7","HEALTHCARE_PROVIDER_TAXONOMY_GROUP_8","HEALTHCARE_PROVIDER_TAXONOMY_GROUP_9","HEALTHCARE_PROVIDER_TAXONOMY_GROUP_10","HEALTHCARE_PROVIDER_TAXONOMY_GROUP_11","HEALTHCARE_PROVIDER_TAXONOMY_GROUP_12","HEALTHCARE_PROVIDER_TAXONOMY_GROUP_13","HEALTHCARE_PROVIDER_TAXONOMY_GROUP_14","HEALTHCARE_PROVIDER_TAXONOMY_GROUP_15"
    )
).withColumn(
    "swtch_arr",
    F.array(
        "HLTHCARE_PROV_PRI_TAXONOMY_SWITCH_1","HLTHCARE_PROV_PRI_TAXONOMY_SWITCH_2","HLTHCARE_PROV_PRIM_TAXONOMY_SWITCH_3","HLTHCARE_PROV_PRIM_TAXONOMY_SWITCH_4","HLTHCARE_PROV_PRIM_TAXONOMY_SWITCH_5","HLTHCARE_PROV_PRI_TAXONOMY_SWITCH_6","HLTHCARE_PROV_PRI_TAXONOMY_SWITCH_7","HLTHCARE_PROV_PRIM_TAXONOMY_SWITCH_8","HLTHCARE_PROV_PRIM_TAXONOMY_SWITCH_9","HLTHCARE_PROV_PRIM_TAXONOMY_SWITCH_10","HLTHCARE_PROV_PRIM_TAXONOMY_SWITCH_11","HLTHCARE_PROV_PRIM_TAXONOMY_SWITCH_12","HLTHCARE_PROV_PRIM_TAXONOMY_SWITCH_13","HLTHCARE_PROV_PRIM_TAXONOMY_SWITCH_14","HLTHCARE_PROV_PRIM_TAXONOMY_SWITCH_15"
    )
).withColumn(
    "zipped_arr",
    F.arrays_zip("hc_code_arr","state_arr","lic_no_arr","grp_arr","swtch_arr")
).select(
    "NPI",
    "SrcSysCd",
    F.explode("zipped_arr").alias("pivoted")
)

df_pivot_ColRws = df_pivot_ColRws_pre.select(
    F.col("NPI").alias("NPI"),
    F.col("pivoted.hc_code_arr").alias("HEALTHCARE_PROVIDER_TAXONOMY_CODE"),
    F.col("pivoted.state_arr").alias("PROVIDER_LICENSE_NUMBER_STATE_CODE"),
    F.col("pivoted.lic_no_arr").alias("PROVIDER_LICENSE_NUMBER"),
    F.col("pivoted.grp_arr").alias("HEALTHCARE_PROVIDER_TAXONOMY_GROUP"),
    F.col("pivoted.swtch_arr").alias("HLTHCARE_PROV_PRI_TAXONOMY_SWITCH"),
    "SrcSysCd"
)

# Xfrm_BusinessLogic
df_Xfrm_BusinessLogic = df_pivot_ColRws.withColumn(
    "svNPIChk",
    F.when(
        F.col("NPI").isNull() | (F.length(F.trim(F.col("NPI"))) < 10) | (F.col("NPI").rlike("\\D")),
        F.lit("N")
    ).otherwise(F.lit("Y"))
).withColumn(
    "svTxnmySwtchRnk",
    F.when(F.trim(F.col("HLTHCARE_PROV_PRI_TAXONOMY_SWITCH"))=="Y",F.lit(1))
     .when(F.trim(F.col("HLTHCARE_PROV_PRI_TAXONOMY_SWITCH"))=="N",F.lit(2))
     .otherwise(F.lit(3))
).withColumn(
    "svTxnmyCdCheck",
    F.when(F.col("HEALTHCARE_PROVIDER_TAXONOMY_CODE").isNull() | (F.trim(F.col("HEALTHCARE_PROVIDER_TAXONOMY_CODE"))==""),F.lit("N")).otherwise(F.lit("Y"))
)

df_SortData = df_Xfrm_BusinessLogic.filter(
    (F.col("svNPIChk") == "Y") & (F.col("svTxnmyCdCheck") == "Y")
).select(
    F.col("NPI"),
    F.when(F.col("HEALTHCARE_PROVIDER_TAXONOMY_CODE").isNull(),F.lit(" "))
     .otherwise(F.col("HEALTHCARE_PROVIDER_TAXONOMY_CODE")).alias("HEALTHCARE_PROVIDER_TAXONOMY_CODE"),
    F.when(
        F.col("PROVIDER_LICENSE_NUMBER_STATE_CODE").isNull() |
        (F.trim(F.col("PROVIDER_LICENSE_NUMBER_STATE_CODE"))=="") |
        (F.length(F.col("PROVIDER_LICENSE_NUMBER_STATE_CODE"))<2),
        F.lit("NA")
    ).otherwise(F.col("PROVIDER_LICENSE_NUMBER_STATE_CODE")).alias("PROVIDER_LICENSE_NUMBER_STATE_CODE"),
    F.when(
        (F.trim(F.col("PROVIDER_LICENSE_NUMBER")).isin("=========","0","EIN=========","IN PROCESS","N/A","NA","NONE","NONE NEEDED","NOT APPLICABLE","NOT REQUIRED","PENDING")) |
        (F.trim(F.col("PROVIDER_LICENSE_NUMBER")).startswith("--")) |
        (F.col("PROVIDER_LICENSE_NUMBER").isNull()),
        F.lit("UNK")
    ).otherwise(F.trim(F.col("PROVIDER_LICENSE_NUMBER"))).alias("PROVIDER_LICENSE_NUMBER"),
    F.col("SrcSysCd").alias("SRC_SYS_CD"),
    F.when(F.col("HEALTHCARE_PROVIDER_TAXONOMY_GROUP").isNull(),F.lit("UNK"))
     .otherwise(F.expr("substring(HEALTHCARE_PROVIDER_TAXONOMY_GROUP,1,10)")).alias("HEALTHCARE_PROVIDER_TAXONOMY_GROUP"),
    F.when(F.col("HLTHCARE_PROV_PRI_TAXONOMY_SWITCH").isNull(),F.lit("N"))
     .otherwise(F.col("HLTHCARE_PROV_PRI_TAXONOMY_SWITCH")).alias("HLTHCARE_PROV_PRI_TAXONOMY_SWITCH"),
    F.col("svTxnmySwtchRnk").alias("TXNMY_SWTCH_RNK")
)

df_Reject_Rec = df_Xfrm_BusinessLogic.filter(
    (F.col("svNPIChk") == "N") | (F.col("svTxnmyCdCheck") == "N")
).select(
    F.col("NPI"),
    F.col("HEALTHCARE_PROVIDER_TAXONOMY_CODE"),
    F.col("PROVIDER_LICENSE_NUMBER_STATE_CODE"),
    F.col("PROVIDER_LICENSE_NUMBER"),
    F.col("SrcSysCd").alias("SRC_SYS_CD"),
    F.col("HEALTHCARE_PROVIDER_TAXONOMY_GROUP"),
    F.col("HLTHCARE_PROV_PRI_TAXONOMY_SWITCH")
)

# Write reject records
reject_cols = df_Reject_Rec.columns
df_Reject_Rec_final = df_Reject_Rec
for c in reject_cols:
    df_Reject_Rec_final = df_Reject_Rec_final.withColumn(
        c,
        rpad(c,1," ") if c in ["NPI","HEALTHCARE_PROVIDER_TAXONOMY_CODE","PROVIDER_LICENSE_NUMBER_STATE_CODE","PROVIDER_LICENSE_NUMBER","SRC_SYS_CD","HEALTHCARE_PROVIDER_TAXONOMY_GROUP","HLTHCARE_PROV_PRI_TAXONOMY_SWITCH"] else F.col(c)
    )  # minimal rpad to demonstrate usage; length not specified

write_files(
    df_Reject_Rec_final.select(*reject_cols),
    f"{adls_path_raw}/landing/CmsIdsNtnlProvTxnmyExtr.{SrcSysCd}.{RunID}_Rej.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

# srt_OnKey (PxSort)
df_sort_OnKey = df_SortData.sort(
    F.col("NPI").asc(),
    F.col("HEALTHCARE_PROVIDER_TAXONOMY_CODE").asc(),
    F.col("PROVIDER_LICENSE_NUMBER_STATE_CODE").asc(),
    F.col("PROVIDER_LICENSE_NUMBER").asc(),
    F.col("TXNMY_SWTCH_RNK").asc()
)

# rmDups (PxRemDup)
df_rmDups = dedup_sort(
    df_sort_OnKey,
    partition_cols=["NPI","HEALTHCARE_PROVIDER_TAXONOMY_CODE","PROVIDER_LICENSE_NUMBER_STATE_CODE","PROVIDER_LICENSE_NUMBER"],
    sort_cols=[("TXNMY_SWTCH_RNK","A")]
)

df_rmDups = df_rmDups.select(
    "NPI",
    "HEALTHCARE_PROVIDER_TAXONOMY_CODE",
    "PROVIDER_LICENSE_NUMBER_STATE_CODE",
    "PROVIDER_LICENSE_NUMBER",
    "SRC_SYS_CD",
    "HEALTHCARE_PROVIDER_TAXONOMY_GROUP",
    "HLTHCARE_PROV_PRI_TAXONOMY_SWITCH",
    "TXNMY_SWTCH_RNK"
)

# Copy_of_cp_PKey
df_Copy_of_cp_PKey = df_rmDups.cache()

df_lkp_NtnlProvTxnmy = df_Copy_of_cp_PKey.select(
    F.col("NPI"),
    F.col("HEALTHCARE_PROVIDER_TAXONOMY_CODE"),
    F.col("PROVIDER_LICENSE_NUMBER_STATE_CODE"),
    F.col("PROVIDER_LICENSE_NUMBER"),
    F.col("SRC_SYS_CD"),
    F.col("HEALTHCARE_PROVIDER_TAXONOMY_GROUP"),
    F.col("HLTHCARE_PROV_PRI_TAXONOMY_SWITCH"),
    F.col("TXNMY_SWTCH_RNK")
)

df_Xfm_NewRecs = df_Copy_of_cp_PKey.select(
    F.col("NPI"),
    F.col("HEALTHCARE_PROVIDER_TAXONOMY_CODE"),
    F.col("PROVIDER_LICENSE_NUMBER_STATE_CODE"),
    F.col("PROVIDER_LICENSE_NUMBER"),
    F.col("SRC_SYS_CD"),
    F.col("HEALTHCARE_PROVIDER_TAXONOMY_GROUP"),
    F.col("HLTHCARE_PROV_PRI_TAXONOMY_SWITCH"),
    F.col("TXNMY_SWTCH_RNK")
)

# Xfm_NewRec
df_Xfm_NewRec = df_Xfm_NewRecs.select(
    F.col("NPI").alias("NTNL_PROV_ID"),
    F.col("HEALTHCARE_PROVIDER_TAXONOMY_CODE").alias("TXNMY_CD"),
    F.col("PROVIDER_LICENSE_NUMBER_STATE_CODE").alias("ENTY_LIC_ST_CD"),
    F.col("PROVIDER_LICENSE_NUMBER").alias("LIC_NO"),
    F.col("HEALTHCARE_PROVIDER_TAXONOMY_GROUP").alias("HEALTHCARE_PROVIDER_TAXONOMY_GROUP"),
    F.col("HLTHCARE_PROV_PRI_TAXONOMY_SWITCH").alias("HLTHCARE_PROV_PRI_TAXONOMY_SWITCH"),
    F.lit(SrcSysCd).alias("SRC_SYS_CD"),
    F.lit("Y").alias("CUR_RCRD_IN"),
    F.lit("Y").alias("NTNL_PROV_TXNMY_ACTV_IN"),
    F.when(
        F.col("HLTHCARE_PROV_PRI_TAXONOMY_SWITCH")=="X",
        F.lit("N")
    ).otherwise(F.col("HLTHCARE_PROV_PRI_TAXONOMY_SWITCH")).alias("NTNL_PROV_TXNMY_PRI_IN")
)

# lkp_NtnlProvTxnmy (PxLookup, left join with db2_NTNL_PROV_TXNMY_In)
df_lkp_join = df_db2_NTNL_PROV_TXNMY_In.alias("lnk_NtnlProvTxnmy_extr").join(
    df_lkp_NtnlProvTxnmy.alias("lkp_NpiTxnmy"),
    on=F.col("lkp_NpiTxnmy.NPI")==F.col("lnk_NtnlProvTxnmy_extr.NTNL_PROV_ID"),
    how="left"
)

df_UpdateRec = df_lkp_join.select(
    F.col("lkp_NpiTxnmy.NPI").alias("NPI"),
    F.col("lkp_NpiTxnmy.HEALTHCARE_PROVIDER_TAXONOMY_CODE").alias("HEALTHCARE_PROVIDER_TAXONOMY_CODE"),
    F.col("lkp_NpiTxnmy.PROVIDER_LICENSE_NUMBER_STATE_CODE").alias("PROVIDER_LICENSE_NUMBER_STATE_CODE"),
    F.col("lkp_NpiTxnmy.PROVIDER_LICENSE_NUMBER").alias("PROVIDER_LICENSE_NUMBER"),
    F.col("lkp_NpiTxnmy.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lkp_NpiTxnmy.HEALTHCARE_PROVIDER_TAXONOMY_GROUP").alias("HEALTHCARE_PROVIDER_TAXONOMY_GROUP"),
    F.col("lkp_NpiTxnmy.HLTHCARE_PROV_PRI_TAXONOMY_SWITCH").alias("HLTHCARE_PROV_PRI_TAXONOMY_SWITCH"),
    F.col("lnk_NtnlProvTxnmy_extr.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

# Xfm_UpdateRec
df_Xfm_UpdateRec = df_UpdateRec.select(
    F.col("NPI").alias("NTNL_PROV_ID"),
    F.col("HEALTHCARE_PROVIDER_TAXONOMY_CODE").alias("TXNMY_CD"),
    F.col("PROVIDER_LICENSE_NUMBER_STATE_CODE").alias("ENTY_LIC_ST_CD"),
    F.col("PROVIDER_LICENSE_NUMBER").alias("LIC_NO"),
    F.col("HEALTHCARE_PROVIDER_TAXONOMY_GROUP").alias("HEALTHCARE_PROVIDER_TAXONOMY_GROUP"),
    F.col("HLTHCARE_PROV_PRI_TAXONOMY_SWITCH").alias("HLTHCARE_PROV_PRI_TAXONOMY_SWITCH"),
    F.lit(SrcSysCd).alias("SRC_SYS_CD"),
    F.lit("N").alias("CUR_RCRD_IN"),
    F.lit("Y").alias("NTNL_PROV_TXNMY_ACTV_IN"),
    F.col("HLTHCARE_PROV_PRI_TAXONOMY_SWITCH").alias("NTNL_PROV_TXNMY_PRI_IN")
)

df_Xfm_UpdateRec_Before = df_UpdateRec.select(
    F.col("NPI").alias("NTNL_PROV_ID"),
    F.col("HEALTHCARE_PROVIDER_TAXONOMY_CODE").alias("TXNMY_CD"),
    F.col("PROVIDER_LICENSE_NUMBER_STATE_CODE").alias("ENTY_LIC_ST_CD"),
    F.col("PROVIDER_LICENSE_NUMBER").alias("LIC_NO"),
    F.col("HEALTHCARE_PROVIDER_TAXONOMY_GROUP").alias("HEALTHCARE_PROVIDER_TAXONOMY_GROUP"),
    F.col("HLTHCARE_PROV_PRI_TAXONOMY_SWITCH").alias("HLTHCARE_PROV_PRI_TAXONOMY_SWITCH"),
    F.lit("N").alias("CUR_RCRD_IN"),
    F.col("HLTHCARE_PROV_PRI_TAXONOMY_SWITCH").alias("NTNL_PROV_TXNMY_PRI_IN")
)

# CC_NPI_Rcrd (PxChangeCapture) - simulating a "before"/"after" comparison
# We'll join the "before" data (df_Xfm_UpdateRec_Before) with the "after" data (df_db2_NPI_TXNMY_CD_In) on keys, then produce a change_code.
df_CC_before = df_Xfm_UpdateRec_Before.select(
    F.col("NTNL_PROV_ID").alias("NTNL_PROV_ID_b"),
    F.col("TXNMY_CD").alias("TXNMY_CD_b"),
    F.col("ENTY_LIC_ST_CD").alias("ENTY_LIC_ST_CD_b"),
    F.col("LIC_NO").alias("LIC_NO_b"),
    F.col("HEALTHCARE_PROVIDER_TAXONOMY_GROUP").alias("HEALTHCARE_PROVIDER_TAXONOMY_GROUP_b"),
    F.col("CUR_RCRD_IN").alias("CUR_RCRD_IN_b"),
    F.col("NTNL_PROV_TXNMY_PRI_IN").alias("NTNL_PROV_TXNMY_PRI_IN_b"),
    F.col("HLTHCARE_PROV_PRI_TAXONOMY_SWITCH").alias("HLTHCARE_PROV_PRI_TAXONOMY_SWITCH_b")
)
df_CC_after = df_db2_NPI_TXNMY_CD_In.select(
    F.col("NTNL_PROV_ID").alias("NTNL_PROV_ID_a"),
    F.col("TXNMY_CD").alias("TXNMY_CD_a"),
    F.col("ENTY_LIC_ST_CD").alias("ENTY_LIC_ST_CD_a"),
    F.col("LIC_NO").alias("LIC_NO_a"),
    F.col("HEALTHCARE_PROVIDER_TAXONOMY_GROUP").alias("HEALTHCARE_PROVIDER_TAXONOMY_GROUP_a"),
    F.col("CUR_RCRD_IN").alias("CUR_RCRD_IN_a"),
    F.col("NTNL_PROV_TXNMY_PRI_IN").alias("NTNL_PROV_TXNMY_PRI_IN_a")
)

cond_cc = [
    df_CC_before["NTNL_PROV_ID_b"] == df_CC_after["NTNL_PROV_ID_a"],
    df_CC_before["TXNMY_CD_b"] == df_CC_after["TXNMY_CD_a"]
]

df_CC_joined = df_CC_before.join(df_CC_after, cond_cc, how="fullouter").withColumn(
    "change_code",
    ChangeCode() # assume a UDF that returns appropriate code
)

df_CC_output = df_CC_joined.select(
    F.coalesce(F.col("NTNL_PROV_ID_a"),F.col("NTNL_PROV_ID_b")).alias("NPI"),
    F.coalesce(F.col("TXNMY_CD_a"),F.col("TXNMY_CD_b")).alias("HEALTHCARE_PROVIDER_TAXONOMY_CODE"),
    F.col("ENTY_LIC_ST_CD_a").alias("PROVIDER_LICENSE_NUMBER_STATE_CODE"),
    F.col("LIC_NO_a").alias("PROVIDER_LICENSE_NUMBER"),
    F.col("HEALTHCARE_PROVIDER_TAXONOMY_GROUP_a").alias("HEALTHCARE_PROVIDER_TAXONOMY_GROUP"),
    F.col("CUR_RCRD_IN_a").alias("CUR_RCRD_IN"),
    F.col("NTNL_PROV_TXNMY_PRI_IN_a").alias("NTNL_PROV_TXNMY_PRI_IN"),
    F.col("change_code")
)

# Xfm_NtnlRec
df_Xfm_NtnlRec_updtInRec = df_CC_output.filter(F.col("change_code")==F.lit("2")).select(
    F.col("NPI").alias("NTNL_PROV_ID"),
    F.col("HEALTHCARE_PROVIDER_TAXONOMY_CODE").alias("TXNMY_CD"),
    F.col("PROVIDER_LICENSE_NUMBER_STATE_CODE").alias("ENTY_LIC_ST_CD"),
    F.col("PROVIDER_LICENSE_NUMBER").alias("LIC_NO"),
    F.when(F.col("HEALTHCARE_PROVIDER_TAXONOMY_GROUP").isNull(),F.lit("NA")).otherwise(F.col("HEALTHCARE_PROVIDER_TAXONOMY_GROUP")).alias("HEALTHCARE_PROVIDER_TAXONOMY_GROUP"),
    F.col("NTNL_PROV_TXNMY_PRI_IN").cast(StringType()).alias("HLTHCARE_PROV_PRI_TAXONOMY_SWITCH"),
    F.lit(SrcSysCd).alias("SRC_SYS_CD"),
    F.lit("N").alias("CUR_RCRD_IN"),
    F.lit("N").alias("NTNL_PROV_TXNMY_ACTV_IN"),
    F.col("NTNL_PROV_TXNMY_PRI_IN").alias("NTNL_PROV_TXNMY_PRI_IN")
)

df_fnl_All_NewRecOut = df_Xfm_NewRec
df_fnl_All_UpdateRecOut = df_Xfm_UpdateRec
df_fnl_All_UpdtInRec = df_Xfm_NtnlRec_updtInRec

# fnl_All (PxFunnel)
df_fnl_All_union = df_fnl_All_NewRecOut.select(
    "NTNL_PROV_ID","TXNMY_CD","ENTY_LIC_ST_CD","LIC_NO","HEALTHCARE_PROVIDER_TAXONOMY_GROUP","HLTHCARE_PROV_PRI_TAXONOMY_SWITCH","SRC_SYS_CD","CUR_RCRD_IN","NTNL_PROV_TXNMY_ACTV_IN","NTNL_PROV_TXNMY_PRI_IN"
).union(
    df_fnl_All_UpdateRecOut.select(
        "NTNL_PROV_ID","TXNMY_CD","ENTY_LIC_ST_CD","LIC_NO","HEALTHCARE_PROVIDER_TAXONOMY_GROUP","HLTHCARE_PROV_PRI_TAXONOMY_SWITCH","SRC_SYS_CD","CUR_RCRD_IN","NTNL_PROV_TXNMY_ACTV_IN","NTNL_PROV_TXNMY_PRI_IN"
    )
).union(
    df_fnl_All_UpdtInRec.select(
        "NTNL_PROV_ID","TXNMY_CD","ENTY_LIC_ST_CD","LIC_NO","HEALTHCARE_PROVIDER_TAXONOMY_GROUP","HLTHCARE_PROV_PRI_TAXONOMY_SWITCH","SRC_SYS_CD","CUR_RCRD_IN","NTNL_PROV_TXNMY_ACTV_IN","NTNL_PROV_TXNMY_PRI_IN"
    )
)

# rmDupsNpi (PxRemDup)
df_rmDupsNpi = dedup_sort(
    df_fnl_All_union,
    partition_cols=["NTNL_PROV_ID","TXNMY_CD","ENTY_LIC_ST_CD","LIC_NO"],
    sort_cols=[("NTNL_PROV_ID","A"),("TXNMY_CD","A"),("ENTY_LIC_ST_CD","A"),("LIC_NO","A")]
)
df_rmDupsNpi = df_rmDupsNpi.select(
    "NTNL_PROV_ID","TXNMY_CD","ENTY_LIC_ST_CD","LIC_NO","HEALTHCARE_PROVIDER_TAXONOMY_GROUP","HLTHCARE_PROV_PRI_TAXONOMY_SWITCH","SRC_SYS_CD","CUR_RCRD_IN","NTNL_PROV_TXNMY_ACTV_IN","NTNL_PROV_TXNMY_PRI_IN"
)

# cp_PKey
df_cp_PKey = df_rmDupsNpi.cache()
df_cp_PKey_out1 = df_cp_PKey.select(
    "NTNL_PROV_ID","TXNMY_CD","ENTY_LIC_ST_CD","LIC_NO","HEALTHCARE_PROVIDER_TAXONOMY_GROUP","HLTHCARE_PROV_PRI_TAXONOMY_SWITCH","SRC_SYS_CD","CUR_RCRD_IN","NTNL_PROV_TXNMY_ACTV_IN","NTNL_PROV_TXNMY_PRI_IN"
)
df_cp_PKey_out2 = df_cp_PKey.select(
    F.col("TXNMY_CD").alias("TXNMY_CD")
)

# ds_NTNL_PROV_TXNMY_Xfrm (PxDataSet => .ds => write parquet)
df_ds_NTNL_PROV_TXNMY_Xfrm_final = df_cp_PKey_out1
# rpad columns with char(1) if needed
df_ds_NTNL_PROV_TXNMY_Xfrm_final = df_ds_NTNL_PROV_TXNMY_Xfrm_final.withColumn(
    "CUR_RCRD_IN", rpad("CUR_RCRD_IN",1," ")
).withColumn(
    "NTNL_PROV_TXNMY_ACTV_IN", rpad("NTNL_PROV_TXNMY_ACTV_IN",1," ")
).withColumn(
    "NTNL_PROV_TXNMY_PRI_IN", rpad("NTNL_PROV_TXNMY_PRI_IN",1," ")
)

write_files(
    df_ds_NTNL_PROV_TXNMY_Xfrm_final.select(
        "NTNL_PROV_ID","TXNMY_CD","ENTY_LIC_ST_CD","LIC_NO","HEALTHCARE_PROVIDER_TAXONOMY_GROUP","HLTHCARE_PROV_PRI_TAXONOMY_SWITCH","SRC_SYS_CD","CUR_RCRD_IN","NTNL_PROV_TXNMY_ACTV_IN","NTNL_PROV_TXNMY_PRI_IN"
    ),
    f"NTNL_PROV_TXNMY.{SrcSysCd}.extr.{RunID}.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote='"',
    nullValue=None
)

# lkp_TxnmyCd (PxLookup with db2_TXNMY_CD_Lkp)
df_TxnmyChck = df_cp_PKey_out2.alias("TxnmyChck")
df_Ref_TxnmyCdSk = df_db2_TXNMY_CD_Lkp.alias("Ref_TxnmyCdSk")
df_lkp_TxnmyCd_matched = df_Ref_TxnmyCdSk.join(
    df_TxnmyChck,
    on=F.col("Ref_TxnmyCdSk.TXNMY_CD")==F.col("TxnmyChck.TXNMY_CD"),
    how="left"
)
df_lkp_TxnmyCd_matchedRec = df_lkp_TxnmyCd_matched.select(
    F.col("Ref_TxnmyCdSk.TXNMY_CD").alias("TXNMY_CD")
)
df_lkp_TxnmyCd_unmatched = df_TxnmyChck.join(
    df_Ref_TxnmyCdSk,
    on=F.col("TxnmyChck.TXNMY_CD")==F.col("Ref_TxnmyCdSk.TXNMY_CD"),
    how="left"
).filter(F.col("Ref_TxnmyCdSk.TXNMY_CD").isNull()).select(
    F.col("TxnmyChck.TXNMY_CD").alias("TXNMY_CD")
)

# seq_TxnmyCd (PxSequentialFile => "TxnmyExistingRec.txt")
df_seq_TxnmyCd_final = df_lkp_TxnmyCd_matchedRec
df_seq_TxnmyCd_final_out = df_seq_TxnmyCd_final.withColumn("TXNMY_CD", rpad("TXNMY_CD",1," "))
write_files(
    df_seq_TxnmyCd_final_out.select("TXNMY_CD"),
    f"{adls_path}/load/TxnmyExistingRec.txt",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

# Xfm_TxnmyCd
df_Xfm_TxnmyCd = df_lkp_TxnmyCd_unmatched.withColumn(
    "svTxnmyCdChck",
    F.when(F.length(F.col("TXNMY_CD"))==10,F.lit("Y")).otherwise(F.lit("N"))
)
df_NewTxnmyCds = df_Xfm_TxnmyCd.filter(F.col("svTxnmyCdChck")=="Y").select(
    F.col("TXNMY_CD"),
    F.lit("1753-01-01").alias("EFF_DT"),
    F.lit("2199-12-31").alias("DCTVTN_DT"),
    F.lit("2199-12-31").alias("LAST_UPDT_DT"),
    F.lit("NA").alias("TXNMY_DESC"),
    F.lit("NA").alias("TXNMY_PROV_TYP_CD"),
    F.lit("NA").alias("TXNMY_PROV_TYP_DESC"),
    F.lit("NA").alias("TXNMY_CLS_CD"),
    F.lit("NA").alias("TXNMY_CLS_DESC"),
    F.lit("NA").alias("TXNMY_SPCLIZATION_CD"),
    F.lit("NA").alias("TXNMY_SPCLIZATION_DESC"),
    F.lit("CMS").alias("CRT_SRC_SYS_CD"),
    F.lit("CMS").alias("LAST_UPDT_SRC_SYS_CD"),
    F.lit("NA").alias("PROV_SPEC_CD"),
    F.lit("NA").alias("PROV_FCLTY_TYP_CD")
)

# ds_NewTxnmyCds
df_ds_NewTxnmyCds = df_NewTxnmyCds
write_files(
    df_ds_NewTxnmyCds.select(
        "TXNMY_CD","EFF_DT","DCTVTN_DT","LAST_UPDT_DT","TXNMY_DESC","TXNMY_PROV_TYP_CD","TXNMY_PROV_TYP_DESC","TXNMY_CLS_CD","TXNMY_CLS_DESC","TXNMY_SPCLIZATION_CD","TXNMY_SPCLIZATION_DESC","CRT_SRC_SYS_CD","LAST_UPDT_SRC_SYS_CD","PROV_SPEC_CD","PROV_FCLTY_TYP_CD"
    ),
    f"TXNMY_CD.{SrcSysCd}.extr.{RunID}.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote='"',
    nullValue=None
)