# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC               Ralph Tucker 11/17/2004 -   Originally Programmed
# MAGIC               Brent Leland   08/30/2005 -   Changed column output order to match table.
# MAGIC               BJ Luce          11/21/2005  -   add DSALW_TYP_CAT_CD and DSALW_TYP_CAT_CD_SK, default to NA and 1
# MAGIC               BJ Luce          1/17/2006        pull DSALW_TYP_CAT_CD and DSALW_TYP_CAT_CD_SK from IDS 
# MAGIC               Brent Leland   04/04/2006      Changed parameters to environment parameters
# MAGIC                                                               Removed trim() off SK values.
# MAGIC               Brent Leland    05/11/2006    Removed trim() on codes and IDs
# MAGIC 
# MAGIC               Rama Kamjula  09/18/2013    Rewritten from Server to parallel version                                                                                  EnterpriseWrhsDevl   Jag Yelavarthi                 2013-12-22

# MAGIC JobName: IdsClmFactSubCatIPHash
# MAGIC Job creates dataset for CLM_F  in EDW
# MAGIC Creates dataset for  CLM_F extract Job - IdsEdwClmFExtr
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


# Parameters
IDSOwner = get_widget_value('IDSOwner','')
EDWRunDtCycle = get_widget_value('EDWRunDtCycle','')
EDWRunCycle = get_widget_value('EDWRunCycle','')
UWSOwner = get_widget_value('UWSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
uws_secret_name = get_widget_value('uws_secret_name','')

# Database configs
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
jdbc_url_uws, jdbc_props_uws = get_db_config(uws_secret_name)

# Stages: db2_IP_CLAIMS
extract_query_db2_IP_CLAIMS = f"""
SELECT 
DRVR.CLM_ID,
DRVR.SRC_SYS_CD

FROM {IDSOwner}.W_EDW_ETL_DRVR         DRVR ,
     {IDSOwner}.CLM                     CLM,
     {IDSOwner}.PROV                    PROV,
     {IDSOwner}.CD_MPPNG                MAP3

WHERE   DRVR.CLM_ID                     =  CLM.CLM_ID
AND     DRVR.SRC_SYS_CD_SK             = CLM.SRC_SYS_CD_SK

AND     PROV.PROV_ENTY_CD_SK = MAP3.CD_MPPNG_SK
AND     (MAP3.SRC_DOMAIN_NM='PROVIDER ENTITY' OR 
         MAP3.SRC_DOMAIN_NM='CUSTOM DOMAIN MAPPING')
AND     MAP3.TRGT_CD='FCLTY' 
AND PROV.PROV_SK =
          (select CLM_PROV.PROV_SK
             from {IDSOwner}.CLM_PROV CLM_PROV,
                  {IDSOwner}.W_EDW_ETL_DRVR DRV2,
                  {IDSOwner}.CD_MPPNG CM
             where DRV2.CLM_ID  = CLM_PROV.CLM_ID
               and DRV2.SRC_SYS_CD_SK = CLM_PROV.SRC_SYS_CD_SK
               and CLM_PROV.CLM_PROV_ROLE_TYP_CD_SK = CM.CD_MPPNG_SK 
               and (CM.SRC_DOMAIN_NM = 'CLAIM PROVIDER ROLE TYPE' OR 
                    CM.SRC_DOMAIN_NM='CUSTOM DOMAIN MAPPING')
               and CM.TRGT_CD='SVC'
               and CLM_PROV.CLM_ID  = CLM.CLM_ID
               and CLM_PROV.SRC_SYS_CD_SK = CLM.SRC_SYS_CD_SK )
AND CLM.CLM_ID = 
          (select CL.CLM_ID
             from {IDSOwner}.W_EDW_ETL_DRVR DRV3,
                  {IDSOwner}.CLM CL, 
                  {IDSOwner}.CD_MPPNG MAP1
            where DRV3.CLM_ID =  CL.CLM_ID
              and DRV3.SRC_SYS_CD_SK = CL.SRC_SYS_CD_SK
              and CLM.CLM_SUBTYP_CD_SK = MAP1.CD_MPPNG_SK
              and (MAP1.SRC_DOMAIN_NM = 'CLAIM SUBTYPE' OR
                  MAP1.SRC_DOMAIN_NM = 'CUSTOM DOMAIN MAPPING')
              and     MAP1.TRGT_CD='IP'
               and CL.CLM_ID  = CLM.CLM_ID
               and CL.SRC_SYS_CD_SK = CLM.SRC_SYS_CD_SK )
"""

df_db2_IP_CLAIMS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_IP_CLAIMS)
    .load()
)

# Stages: db2_SNF_RVNU
extract_query_db2_SNF_RVNU = f"""
SELECT 
DRVR.CLM_ID,
DRVR.SRC_SYS_CD,
'Y'  as  SNF_RVNU_IND

FROM {IDSOwner}.W_EDW_ETL_DRVR         DRVR ,
     {IDSOwner}.CLM                    CLM,
     {IDSOwner}.CLM_LN                 LINE, 
     {IDSOwner}.PROV                   PROV,
     {IDSOwner}.CD_MPPNG               MAP3,
     {IDSOwner}.FCLTY_TYP_CD           FCLTY_TYP,
     {IDSOwner}.RVNU_CD                RVNU

WHERE   DRVR.CLM_ID                     =  CLM.CLM_ID
AND     DRVR.SRC_SYS_CD_SK             = CLM.SRC_SYS_CD_SK

AND     PROV.PROV_ENTY_CD_SK = MAP3.CD_MPPNG_SK
AND     (MAP3.SRC_DOMAIN_NM='PROVIDER ENTITY' OR 
         MAP3.SRC_DOMAIN_NM='CUSTOM DOMAIN MAPPING')
AND     MAP3.TRGT_CD='FCLTY' 
AND PROV.PROV_SK =
          (select CLM_PROV.PROV_SK
             from {IDSOwner}.CLM_PROV CLM_PROV,
                  {IDSOwner}.W_EDW_ETL_DRVR DRV2,
                  {IDSOwner}.CD_MPPNG CM
             where DRV2.CLM_ID  = CLM_PROV.CLM_ID
               and DRV2.SRC_SYS_CD_SK = CLM_PROV.SRC_SYS_CD_SK
               and CLM_PROV.CLM_PROV_ROLE_TYP_CD_SK = CM.CD_MPPNG_SK 
               and (CM.SRC_DOMAIN_NM = 'CLAIM PROVIDER ROLE TYPE' OR 
                    CM.SRC_DOMAIN_NM='CUSTOM DOMAIN MAPPING')
               and CM.TRGT_CD='SVC'
               and CLM_PROV.CLM_ID  = CLM.CLM_ID
               and CLM_PROV.SRC_SYS_CD_SK = CLM.SRC_SYS_CD_SK )
AND CLM.CLM_ID = 
          (select CL.CLM_ID
             from {IDSOwner}.W_EDW_ETL_DRVR DRV3,
                  {IDSOwner}.CLM CL, 
                  {IDSOwner}.CD_MPPNG MAP1
            where DRV3.CLM_ID =  CL.CLM_ID
              and DRV3.SRC_SYS_CD_SK = CL.SRC_SYS_CD_SK
              and CLM.CLM_SUBTYP_CD_SK = MAP1.CD_MPPNG_SK
              and (MAP1.SRC_DOMAIN_NM = 'CLAIM SUBTYPE' OR
                  MAP1.SRC_DOMAIN_NM = 'CUSTOM DOMAIN MAPPING')
              and     MAP1.TRGT_CD='IP'
               and CL.CLM_ID  = CLM.CLM_ID
               and CL.SRC_SYS_CD_SK = CLM.SRC_SYS_CD_SK )

AND        PROV.PROV_FCLTY_TYP_CD_SK = FCLTY_TYP.FCLTY_TYP_CD_SK
AND        FCLTY_TYP.FCLTY_TYP_CD  <> '0007'

AND     DRVR.CLM_ID                =  LINE.CLM_ID                                    
AND     DRVR.SRC_SYS_CD_SK         =  LINE.SRC_SYS_CD_SK                    
AND     LINE.CLM_LN_RVNU_CD_SK     =  RVNU.RVNU_CD_SK              
AND     RVNU.RVNU_CD               >= '0190' 
AND     RVNU.RVNU_CD               <= '0199'
"""

df_db2_SNF_RVNU = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_SNF_RVNU)
    .load()
)

# Stages: db2_REHAB_FCLTY
extract_query_db2_REHAB_FCLTY = f"""
SELECT 
DRVR.CLM_ID,
DRVR.SRC_SYS_CD,
'Y'  as  REHAB_FCLTY_IND

FROM {IDSOwner}.W_EDW_ETL_DRVR      DRVR ,
     {IDSOwner}.CLM                 CLM,
     {IDSOwner}.PROV                PROV,
     {IDSOwner}.CD_MPPNG            MAP3,
     {IDSOwner}.FCLTY_TYP_CD        FCLTY_TYP

WHERE   DRVR.CLM_ID                     =  CLM.CLM_ID
AND     DRVR.SRC_SYS_CD_SK             = CLM.SRC_SYS_CD_SK

AND     PROV.PROV_ENTY_CD_SK = MAP3.CD_MPPNG_SK
AND     (MAP3.SRC_DOMAIN_NM='PROVIDER ENTITY' OR 
         MAP3.SRC_DOMAIN_NM='CUSTOM DOMAIN MAPPING')
AND     MAP3.TRGT_CD='FCLTY' 
AND PROV.PROV_SK =
          (select CLM_PROV.PROV_SK
             from {IDSOwner}.CLM_PROV CLM_PROV,
                  {IDSOwner}.W_EDW_ETL_DRVR DRV2,
                  {IDSOwner}.CD_MPPNG CM
             where DRV2.CLM_ID  = CLM_PROV.CLM_ID
               and DRV2.SRC_SYS_CD_SK = CLM_PROV.SRC_SYS_CD_SK
               and CLM_PROV.CLM_PROV_ROLE_TYP_CD_SK = CM.CD_MPPNG_SK 
               and (CM.SRC_DOMAIN_NM = 'CLAIM PROVIDER ROLE TYPE' OR 
                    CM.SRC_DOMAIN_NM='CUSTOM DOMAIN MAPPING')
               and CM.TRGT_CD='SVC'
               and CLM_PROV.CLM_ID  = CLM.CLM_ID
               and CLM_PROV.SRC_SYS_CD_SK = CLM.SRC_SYS_CD_SK )
AND CLM.CLM_ID = 
          (select CL.CLM_ID
             from {IDSOwner}.W_EDW_ETL_DRVR DRV3,
                  {IDSOwner}.CLM CL, 
                  {IDSOwner}.CD_MPPNG MAP1
            where DRV3.CLM_ID =  CL.CLM_ID
              and DRV3.SRC_SYS_CD_SK = CL.SRC_SYS_CD_SK
              and CLM.CLM_SUBTYP_CD_SK = MAP1.CD_MPPNG_SK
              and (MAP1.SRC_DOMAIN_NM = 'CLAIM SUBTYPE' OR
                  MAP1.SRC_DOMAIN_NM = 'CUSTOM DOMAIN MAPPING')
              and     MAP1.TRGT_CD='IP'
               and CL.CLM_ID  = CLM.CLM_ID
               and CL.SRC_SYS_CD_SK = CLM.SRC_SYS_CD_SK )

AND        PROV.PROV_FCLTY_TYP_CD_SK = FCLTY_TYP.FCLTY_TYP_CD_SK
AND        FCLTY_TYP.FCLTY_TYP_CD IN ('0013','0014','0026')
"""

df_db2_REHAB_FCLTY = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_REHAB_FCLTY)
    .load()
)

# Stages: db2_REHAB_RVNU
extract_query_db2_REHAB_RVNU = f"""
SELECT  DISTINCT
DRVR.CLM_ID,
DRVR.SRC_SYS_CD,
'Y'  as  REHAB_RVNU_IND

FROM {IDSOwner}.W_EDW_ETL_DRVR         DRVR ,
     {IDSOwner}.CLM                    CLM,
     {IDSOwner}.CLM_LN                 LINE, 
     {IDSOwner}.PROV                   PROV,
     {IDSOwner}.CD_MPPNG               MAP3,
     {IDSOwner}.FCLTY_TYP_CD           FCLTY_TYP,
     {IDSOwner}.RVNU_CD                RVNU

WHERE   DRVR.CLM_ID                    =  CLM.CLM_ID
AND     DRVR.SRC_SYS_CD_SK            =  CLM.SRC_SYS_CD_SK

AND     PROV.PROV_ENTY_CD_SK = MAP3.CD_MPPNG_SK
AND     (MAP3.SRC_DOMAIN_NM='PROVIDER ENTITY' OR 
         MAP3.SRC_DOMAIN_NM='CUSTOM DOMAIN MAPPING')
AND     MAP3.TRGT_CD='FCLTY' 
AND PROV.PROV_SK =
          (select CLM_PROV.PROV_SK
             from {IDSOwner}.CLM_PROV CLM_PROV,
                  {IDSOwner}.W_EDW_ETL_DRVR DRV2,
                  {IDSOwner}.CD_MPPNG CM
             where DRV2.CLM_ID  = CLM_PROV.CLM_ID
               and DRV2.SRC_SYS_CD_SK = CLM_PROV.SRC_SYS_CD_SK
               and CLM_PROV.CLM_PROV_ROLE_TYP_CD_SK = CM.CD_MPPNG_SK 
               and (CM.SRC_DOMAIN_NM = 'CLAIM PROVIDER ROLE TYPE' OR 
                    CM.SRC_DOMAIN_NM='CUSTOM DOMAIN MAPPING')
               and CM.TRGT_CD='SVC'
               and CLM_PROV.CLM_ID  = CLM.CLM_ID
               and CLM_PROV.SRC_SYS_CD_SK = CLM.SRC_SYS_CD_SK )
AND CLM.CLM_ID = 
          (select CL.CLM_ID
             from {IDSOwner}.W_EDW_ETL_DRVR DRV3,
                  {IDSOwner}.CLM CL, 
                  {IDSOwner}.CD_MPPNG MAP1
            where DRV3.CLM_ID =  CL.CLM_ID
              and DRV3.SRC_SYS_CD_SK = CL.SRC_SYS_CD_SK
              and CLM.CLM_SUBTYP_CD_SK = MAP1.CD_MPPNG_SK
              and (MAP1.SRC_DOMAIN_NM = 'CLAIM SUBTYPE' OR
                  MAP1.SRC_DOMAIN_NM = 'CUSTOM DOMAIN MAPPING')
              and     MAP1.TRGT_CD='IP'
               and CL.CLM_ID  = CLM.CLM_ID
               and CL.SRC_SYS_CD_SK = CLM.SRC_SYS_CD_SK )

AND        PROV.PROV_FCLTY_TYP_CD_SK = FCLTY_TYP.FCLTY_TYP_CD_SK
AND       ( FCLTY_TYP.FCLTY_TYP_CD   <> '0007'
AND        FCLTY_TYP.FCLTY_TYP_CD   <> '0013'
AND        FCLTY_TYP.FCLTY_TYP_CD   <> '0014'
AND        FCLTY_TYP.FCLTY_TYP_CD   <> '0026' )                                                      

AND     DRVR.CLM_ID                  =  LINE.CLM_ID                                    
AND     DRVR.SRC_SYS_CD_SK           =  LINE.SRC_SYS_CD_SK                    
AND     LINE.CLM_LN_RVNU_CD_SK       =  RVNU.RVNU_CD_SK              
AND     RVNU.RVNU_CD                 IN ('0118','0128','0138','0148','0158')
"""

df_db2_REHAB_RVNU = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_REHAB_RVNU)
    .load()
)

# Stages: db2_HOSPICE_RVNU
extract_query_db2_HOSPICE_RVNU = f"""
SELECT 
DRVR.CLM_ID,
DRVR.SRC_SYS_CD,
'Y'  as   HOSPICE_RVNU_IND

FROM {IDSOwner}.W_EDW_ETL_DRVR         DRVR ,
     {IDSOwner}.CLM                    CLM,
     {IDSOwner}.CLM_LN                 LINE, 
     {IDSOwner}.PROV                   PROV,
     {IDSOwner}.CD_MPPNG               MAP3,
     {IDSOwner}.FCLTY_TYP_CD           FCLTY_TYP,
     {IDSOwner}.RVNU_CD                RVNU

WHERE   DRVR.CLM_ID                    =  CLM.CLM_ID
AND     DRVR.SRC_SYS_CD_SK            =  CLM.SRC_SYS_CD_SK

AND     PROV.PROV_ENTY_CD_SK = MAP3.CD_MPPNG_SK
AND     (MAP3.SRC_DOMAIN_NM='PROVIDER ENTITY' OR 
         MAP3.SRC_DOMAIN_NM='CUSTOM DOMAIN MAPPING')
AND     MAP3.TRGT_CD='FCLTY' 
AND PROV.PROV_SK =
          (select CLM_PROV.PROV_SK
             from {IDSOwner}.CLM_PROV CLM_PROV,
                  {IDSOwner}.W_EDW_ETL_DRVR DRV2,
                  {IDSOwner}.CD_MPPNG CM
             where DRV2.CLM_ID  = CLM_PROV.CLM_ID
               and DRV2.SRC_SYS_CD_SK = CLM_PROV.SRC_SYS_CD_SK
               and CLM_PROV.CLM_PROV_ROLE_TYP_CD_SK = CM.CD_MPPNG_SK 
               and (CM.SRC_DOMAIN_NM = 'CLAIM PROVIDER ROLE TYPE' OR 
                    CM.SRC_DOMAIN_NM='CUSTOM DOMAIN MAPPING')
               and CM.TRGT_CD='SVC'
               and CLM_PROV.CLM_ID  = CLM.CLM_ID
               and CLM_PROV.SRC_SYS_CD_SK = CLM.SRC_SYS_CD_SK )
AND CLM.CLM_ID = 
          (select CL.CLM_ID
             from {IDSOwner}.W_EDW_ETL_DRVR DRV3,
                  {IDSOwner}.CLM CL, 
                  {IDSOwner}.CD_MPPNG MAP1
            where DRV3.CLM_ID =  CL.CLM_ID
              and DRV3.SRC_SYS_CD_SK = CL.SRC_SYS_CD_SK
              and CLM.CLM_SUBTYP_CD_SK = MAP1.CD_MPPNG_SK
              and (MAP1.SRC_DOMAIN_NM = 'CLAIM SUBTYPE' OR
                  MAP1.SRC_DOMAIN_NM = 'CUSTOM DOMAIN MAPPING')
              and     MAP1.TRGT_CD='IP'
               and CL.CLM_ID  = CLM.CLM_ID
               and CL.SRC_SYS_CD_SK = CLM.SRC_SYS_CD_SK )

AND        PROV.PROV_FCLTY_TYP_CD_SK = FCLTY_TYP.FCLTY_TYP_CD_SK
AND        FCLTY_TYP.FCLTY_TYP_CD   <> '0007'
AND        FCLTY_TYP.FCLTY_TYP_CD   <> '0010'
AND        FCLTY_TYP.FCLTY_TYP_CD   <> '0013'
AND        FCLTY_TYP.FCLTY_TYP_CD   <> '0014'
AND        FCLTY_TYP.FCLTY_TYP_CD   <> '0026'        

AND     DRVR.CLM_ID                  =  LINE.CLM_ID                                    
AND     DRVR.SRC_SYS_CD_SK           =  LINE.SRC_SYS_CD_SK                    
AND     LINE.CLM_LN_RVNU_CD_SK       =  RVNU.RVNU_CD_SK            
AND     RVNU.RVNU_CD                 IN ('0115','0125','0135','0145','0155','0650','0656','0658','0659')
"""

df_db2_HOSPICE_RVNU = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_HOSPICE_RVNU)
    .load()
)

# Stages: db2_RESPITE_RVNU
extract_query_db2_RESPITE_RVNU = f"""
SELECT 
DRVR.CLM_ID,
DRVR.SRC_SYS_CD,
'Y'   as   RESPITE_RVNU_IND

FROM {IDSOwner}.W_EDW_ETL_DRVR         DRVR ,
     {IDSOwner}.CLM                    CLM,
     {IDSOwner}.CLM_LN                 LINE, 
     {IDSOwner}.PROV                   PROV,
     {IDSOwner}.CD_MPPNG               MAP3,
     {IDSOwner}.FCLTY_TYP_CD           FCLTY_TYP,
     {IDSOwner}.RVNU_CD                RVNU

WHERE   DRVR.CLM_ID                    =  CLM.CLM_ID
AND     DRVR.SRC_SYS_CD_SK            =  CLM.SRC_SYS_CD_SK

AND     PROV.PROV_ENTY_CD_SK = MAP3.CD_MPPNG_SK
AND     (MAP3.SRC_DOMAIN_NM='PROVIDER ENTITY' OR 
         MAP3.SRC_DOMAIN_NM='CUSTOM DOMAIN MAPPING')
AND     MAP3.TRGT_CD='FCLTY' 
AND PROV.PROV_SK =
          (select CLM_PROV.PROV_SK
             from {IDSOwner}.CLM_PROV CLM_PROV,
                  {IDSOwner}.W_EDW_ETL_DRVR DRV2,
                  {IDSOwner}.CD_MPPNG CM
             where DRV2.CLM_ID  = CLM_PROV.CLM_ID
               and DRV2.SRC_SYS_CD_SK = CLM_PROV.SRC_SYS_CD_SK
               and CLM_PROV.CLM_PROV_ROLE_TYP_CD_SK = CM.CD_MPPNG_SK 
               and (CM.SRC_DOMAIN_NM = 'CLAIM PROVIDER ROLE TYPE' OR 
                    CM.SRC_DOMAIN_NM='CUSTOM DOMAIN MAPPING')
               and CM.TRGT_CD='SVC'
               and CLM_PROV.CLM_ID  = CLM.CLM_ID
               and CLM_PROV.SRC_SYS_CD_SK = CLM.SRC_SYS_CD_SK )
AND CLM.CLM_ID = 
          (select CL.CLM_ID
             from {IDSOwner}.W_EDW_ETL_DRVR DRV3,
                  {IDSOwner}.CLM CL, 
                  {IDSOwner}.CD_MPPNG MAP1
            where DRV3.CLM_ID =  CL.CLM_ID
              and DRV3.SRC_SYS_CD_SK = CL.SRC_SYS_CD_SK
              and CLM.CLM_SUBTYP_CD_SK = MAP1.CD_MPPNG_SK
              and (MAP1.SRC_DOMAIN_NM = 'CLAIM SUBTYPE' OR
                  MAP1.SRC_DOMAIN_NM = 'CUSTOM DOMAIN MAPPING')
              and     MAP1.TRGT_CD='IP'
               and CL.CLM_ID  = CLM.CLM_ID
               and CL.SRC_SYS_CD_SK = CLM.SRC_SYS_CD_SK )

AND        PROV.PROV_FCLTY_TYP_CD_SK = FCLTY_TYP.FCLTY_TYP_CD_SK
AND        FCLTY_TYP.FCLTY_TYP_CD  <>  '0010'      
AND        FCLTY_TYP.FCLTY_TYP_CD  <> '0007'
AND        FCLTY_TYP.FCLTY_TYP_CD  <> '0013'
AND        FCLTY_TYP.FCLTY_TYP_CD  <> '0014'
AND        FCLTY_TYP.FCLTY_TYP_CD  <> '0026'                                                             

AND     DRVR.CLM_ID                  =  LINE.CLM_ID                                    
AND     DRVR.SRC_SYS_CD_SK           =  LINE.SRC_SYS_CD_SK                    
AND     LINE.CLM_LN_RVNU_CD_SK       =  RVNU.RVNU_CD_SK              
AND     RVNU.RVNU_CD                 =   '0655'
"""

df_db2_RESPITE_RVNU = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_RESPITE_RVNU)
    .load()
)

# Stages: db2_UngroupAble_drg
extract_query_db2_UngroupAble_drg = f"""
SELECT 
DRVR.CLM_ID,
DRVR.SRC_SYS_CD,
DRG.DRG_CD,
'Y'   as    UngroupAble_Drg_Ind

FROM {IDSOwner}.W_EDW_ETL_DRVR      DRVR ,
     {IDSOwner}.CLM                 CLM,
     {IDSOwner}.PROV                PROV,
     {IDSOwner}.CD_MPPNG            MAP3,
     {IDSOwner}.FCLTY_TYP_CD        FCLTY_TYP,
     {IDSOwner}.FCLTY_CLM           FCLTY,
     {IDSOwner}.DRG                 DRG,
     {IDSOwner}.CD_MPPNG            MAP4

WHERE   DRVR.CLM_ID                     =  CLM.CLM_ID
AND     DRVR.SRC_SYS_CD_SK             =  CLM.SRC_SYS_CD_SK

AND     PROV.PROV_ENTY_CD_SK = MAP3.CD_MPPNG_SK
AND     (MAP3.SRC_DOMAIN_NM='PROVIDER ENTITY' OR 
         MAP3.SRC_DOMAIN_NM='CUSTOM DOMAIN MAPPING')
AND     MAP3.TRGT_CD='FCLTY' 
AND PROV.PROV_SK =
          (select CLM_PROV.PROV_SK
             from {IDSOwner}.CLM_PROV CLM_PROV,
                  {IDSOwner}.W_EDW_ETL_DRVR DRV2,
                  {IDSOwner}.CD_MPPNG CM
             where DRV2.CLM_ID  = CLM_PROV.CLM_ID
               and DRV2.SRC_SYS_CD_SK = CLM_PROV.SRC_SYS_CD_SK
               and CLM_PROV.CLM_PROV_ROLE_TYP_CD_SK = CM.CD_MPPNG_SK 
               and (CM.SRC_DOMAIN_NM = 'CLAIM PROVIDER ROLE TYPE' OR 
                    CM.SRC_DOMAIN_NM='CUSTOM DOMAIN MAPPING')
               and CM.TRGT_CD='SVC'
               and CLM_PROV.CLM_ID  = CLM.CLM_ID
               and CLM_PROV.SRC_SYS_CD_SK = CLM.SRC_SYS_CD_SK )
AND CLM.CLM_ID = 
          (select CL.CLM_ID
             from {IDSOwner}.W_EDW_ETL_DRVR DRV3,
                  {IDSOwner}.CLM CL, 
                  {IDSOwner}.CD_MPPNG MAP1
            where DRV3.CLM_ID =  CL.CLM_ID
              and DRV3.SRC_SYS_CD_SK = CL.SRC_SYS_CD_SK
              and CLM.CLM_SUBTYP_CD_SK = MAP1.CD_MPPNG_SK
              and (MAP1.SRC_DOMAIN_NM = 'CLAIM SUBTYPE' OR
                  MAP1.SRC_DOMAIN_NM = 'CUSTOM DOMAIN MAPPING')
              and     MAP1.TRGT_CD='IP'
               and CL.CLM_ID  = CLM.CLM_ID
               and CL.SRC_SYS_CD_SK = CLM.SRC_SYS_CD_SK )

AND        PROV.PROV_FCLTY_TYP_CD_SK = FCLTY_TYP.FCLTY_TYP_CD_SK
AND        FCLTY_TYP.FCLTY_TYP_CD  <> '0007'

AND          DRVR.CLM_ID                  =   FCLTY.CLM_ID
AND         DRVR.SRC_SYS_CD_SK            =   FCLTY.SRC_SYS_CD_SK             
AND         FCLTY.NRMTV_DRG_SK            =   DRG.DRG_SK
AND         DRG.DRG_TYP_CD_SK             =   MAP4.CD_MPPNG_SK
AND         MAP4.TRGT_CD                  =   ' UNGRPABL'
"""

df_db2_UngroupAble_drg = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_UngroupAble_drg)
    .load()
)

# Stages: db2_HOSPICE_FCLTY
extract_query_db2_HOSPICE_FCLTY = f"""
SELECT 
DRVR.CLM_ID,
DRVR.SRC_SYS_CD,
'Y'  as  HOSPICE_FCLTY_IND

FROM {IDSOwner}.W_EDW_ETL_DRVR         DRVR ,
     {IDSOwner}.CLM                    CLM,
     {IDSOwner}.PROV                   PROV,
     {IDSOwner}.CD_MPPNG               MAP3,
     {IDSOwner}.FCLTY_TYP_CD           FCLTY_TYP

WHERE   DRVR.CLM_ID                    =  CLM.CLM_ID
AND     DRVR.SRC_SYS_CD_SK            =  CLM.SRC_SYS_CD_SK

AND     PROV.PROV_ENTY_CD_SK = MAP3.CD_MPPNG_SK
AND     (MAP3.SRC_DOMAIN_NM='PROVIDER ENTITY' OR 
         MAP3.SRC_DOMAIN_NM='CUSTOM DOMAIN MAPPING')
AND     MAP3.TRGT_CD='FCLTY' 
AND PROV.PROV_SK =
          (select CLM_PROV.PROV_SK
             from {IDSOwner}.CLM_PROV CLM_PROV,
                  {IDSOwner}.W_EDW_ETL_DRVR DRV2,
                  {IDSOwner}.CD_MPPNG CM
             where DRV2.CLM_ID  = CLM_PROV.CLM_ID
               and DRV2.SRC_SYS_CD_SK = CLM_PROV.SRC_SYS_CD_SK
               and CLM_PROV.CLM_PROV_ROLE_TYP_CD_SK = CM.CD_MPPNG_SK 
               and (CM.SRC_DOMAIN_NM = 'CLAIM PROVIDER ROLE TYPE' OR 
                    CM.SRC_DOMAIN_NM='CUSTOM DOMAIN MAPPING')
               and CM.TRGT_CD='SVC'
               and CLM_PROV.CLM_ID  = CLM.CLM_ID
               and CLM_PROV.SRC_SYS_CD_SK = CLM.SRC_SYS_CD_SK )
AND CLM.CLM_ID = 
          (select CL.CLM_ID
             from {IDSOwner}.W_EDW_ETL_DRVR DRV3,
                  {IDSOwner}.CLM CL, 
                  {IDSOwner}.CD_MPPNG MAP1
            where DRV3.CLM_ID =  CL.CLM_ID
              and DRV3.SRC_SYS_CD_SK = CL.SRC_SYS_CD_SK
              and CLM.CLM_SUBTYP_CD_SK = MAP1.CD_MPPNG_SK
              and (MAP1.SRC_DOMAIN_NM = 'CLAIM SUBTYPE' OR
                  MAP1.SRC_DOMAIN_NM = 'CUSTOM DOMAIN MAPPING')
              and     MAP1.TRGT_CD='IP'
               and CL.CLM_ID  = CLM.CLM_ID
               and CL.SRC_SYS_CD_SK = CLM.SRC_SYS_CD_SK )

AND        PROV.PROV_FCLTY_TYP_CD_SK = FCLTY_TYP.FCLTY_TYP_CD_SK
AND        FCLTY_TYP.FCLTY_TYP_CD    = '0010'
"""

df_db2_HOSPICE_FCLTY = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_HOSPICE_FCLTY)
    .load()
)

# Stages: db2_SNF_FCLTY
extract_query_db2_SNF_FCLTY = f"""
SELECT 
DRVR.CLM_ID,
DRVR.SRC_SYS_CD,
'Y'  as   SNF_FCLTY_IND

FROM {IDSOwner}.W_EDW_ETL_DRVR         DRVR ,
     {IDSOwner}.CLM                    CLM,
     {IDSOwner}.PROV                   PROV,
     {IDSOwner}.CD_MPPNG               MAP3,
     {IDSOwner}.FCLTY_TYP_CD           FCLTY_TYP

WHERE   DRVR.CLM_ID                    =  CLM.CLM_ID
AND     DRVR.SRC_SYS_CD_SK            =  CLM.SRC_SYS_CD_SK

AND     PROV.PROV_ENTY_CD_SK = MAP3.CD_MPPNG_SK
AND     (MAP3.SRC_DOMAIN_NM='PROVIDER ENTITY' OR 
         MAP3.SRC_DOMAIN_NM='CUSTOM DOMAIN MAPPING')
AND     MAP3.TRGT_CD='FCLTY' 
AND PROV.PROV_SK =
          (select CLM_PROV.PROV_SK
             from {IDSOwner}.CLM_PROV CLM_PROV,
                  {IDSOwner}.W_EDW_ETL_DRVR DRV2,
                  {IDSOwner}.CD_MPPNG CM
             where DRV2.CLM_ID  = CLM_PROV.CLM_ID
               and DRV2.SRC_SYS_CD_SK = CLM_PROV.SRC_SYS_CD_SK
               and CLM_PROV.CLM_PROV_ROLE_TYP_CD_SK = CM.CD_MPPNG_SK 
               and (CM.SRC_DOMAIN_NM = 'CLAIM PROVIDER ROLE TYPE' OR 
                    CM.SRC_DOMAIN_NM='CUSTOM DOMAIN MAPPING')
               and CM.TRGT_CD='SVC'
               and CLM_PROV.CLM_ID  = CLM.CLM_ID
               and CLM_PROV.SRC_SYS_CD_SK = CLM.SRC_SYS_CD_SK )
AND CLM.CLM_ID = 
          (select CL.CLM_ID
             from {IDSOwner}.W_EDW_ETL_DRVR DRV3,
                  {IDSOwner}.CLM CL, 
                  {IDSOwner}.CD_MPPNG MAP1
            where DRV3.CLM_ID =  CL.CLM_ID
              and DRV3.SRC_SYS_CD_SK = CL.SRC_SYS_CD_SK
              and CLM.CLM_SUBTYP_CD_SK = MAP1.CD_MPPNG_SK
              and (MAP1.SRC_DOMAIN_NM = 'CLAIM SUBTYPE' OR
                  MAP1.SRC_DOMAIN_NM = 'CUSTOM DOMAIN MAPPING')
              and     MAP1.TRGT_CD='IP'
               and CL.CLM_ID  = CLM.CLM_ID
               and CL.SRC_SYS_CD_SK = CLM.SRC_SYS_CD_SK )

AND        PROV.PROV_FCLTY_TYP_CD_SK = FCLTY_TYP.FCLTY_TYP_CD_SK
AND        FCLTY_TYP.FCLTY_TYP_CD    ='0007'
"""

df_db2_SNF_FCLTY = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_SNF_FCLTY)
    .load()
)

# Stages: db2_REHAB_DRGS
extract_query_db2_REHAB_DRGS = f"""
SELECT 
DRVR.CLM_ID,
DRVR.SRC_SYS_CD,
DRG.DRG_CD

FROM {IDSOwner}.W_EDW_ETL_DRVR      DRVR ,
     {IDSOwner}.CLM                 CLM,
     {IDSOwner}.PROV                PROV,
     {IDSOwner}.CD_MPPNG            MAP3,
     {IDSOwner}.FCLTY_TYP_CD        FCLTY_TYP,
     {IDSOwner}.FCLTY_CLM           FCLTY,
     {IDSOwner}.DRG                 DRG

WHERE   DRVR.CLM_ID                     =  CLM.CLM_ID
AND     DRVR.SRC_SYS_CD_SK             =  CLM.SRC_SYS_CD_SK

AND     PROV.PROV_ENTY_CD_SK = MAP3.CD_MPPNG_SK
AND     (MAP3.SRC_DOMAIN_NM='PROVIDER ENTITY' OR 
         MAP3.SRC_DOMAIN_NM='CUSTOM DOMAIN MAPPING')
AND     MAP3.TRGT_CD='FCLTY' 
AND PROV.PROV_SK =
          (select CLM_PROV.PROV_SK
             from {IDSOwner}.CLM_PROV CLM_PROV,
                  {IDSOwner}.W_EDW_ETL_DRVR DRV2,
                  {IDSOwner}.CD_MPPNG CM
             where DRV2.CLM_ID  = CLM_PROV.CLM_ID
               and DRV2.SRC_SYS_CD_SK = CLM_PROV.SRC_SYS_CD_SK
               and CLM_PROV.CLM_PROV_ROLE_TYP_CD_SK = CM.CD_MPPNG_SK 
               and (CM.SRC_DOMAIN_NM = 'CLAIM PROVIDER ROLE TYPE' OR 
                    CM.SRC_DOMAIN_NM='CUSTOM DOMAIN MAPPING')
               and CM.TRGT_CD='SVC'
               and CLM_PROV.CLM_ID  = CLM.CLM_ID
               and CLM_PROV.SRC_SYS_CD_SK = CLM.SRC_SYS_CD_SK )
AND CLM.CLM_ID = 
          (select CL.CLM_ID
             from {IDSOwner}.W_EDW_ETL_DRVR DRV3,
                  {IDSOwner}.CLM CL, 
                  {IDSOwner}.CD_MPPNG MAP1
            where DRV3.CLM_ID =  CL.CLM_ID
              and DRV3.SRC_SYS_CD_SK = CL.SRC_SYS_CD_SK
              and CLM.CLM_SUBTYP_CD_SK = MAP1.CD_MPPNG_SK
              and (MAP1.SRC_DOMAIN_NM = 'CLAIM SUBTYPE' OR
                  MAP1.SRC_DOMAIN_NM = 'CUSTOM DOMAIN MAPPING')
              and     MAP1.TRGT_CD='IP'
               and CL.CLM_ID  = CLM.CLM_ID
               and CL.SRC_SYS_CD_SK = CLM.SRC_SYS_CD_SK )

AND        PROV.PROV_FCLTY_TYP_CD_SK = FCLTY_TYP.FCLTY_TYP_CD_SK
AND        FCLTY_TYP.FCLTY_TYP_CD    <> '0007'

AND        DRVR.CLM_ID               =   FCLTY.CLM_ID
AND        DRVR.SRC_SYS_CD_SK        =   FCLTY.SRC_SYS_CD_SK             
AND        FCLTY.NRMTV_DRG_SK        =   DRG.DRG_SK
"""

df_db2_REHAB_DRGS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_REHAB_DRGS)
    .load()
)

# Stages: db2_IP_CLMS_by_SERVICES
extract_query_db2_IP_CLMS_by_SERVICES = f"""
SELECT 
DRVR.CLM_ID,
DRVR.SRC_SYS_CD,
DRG.DRG_CD

FROM {IDSOwner}.W_EDW_ETL_DRVR      DRVR ,
     {IDSOwner}.CLM                 CLM,
     {IDSOwner}.PROV                PROV,
     {IDSOwner}.CD_MPPNG            MAP3,
     {IDSOwner}.FCLTY_TYP_CD        FCLTY_TYP,
     {IDSOwner}.FCLTY_CLM           FCLTY,
     {IDSOwner}.DRG                 DRG

WHERE   DRVR.CLM_ID                     =  CLM.CLM_ID
AND     DRVR.SRC_SYS_CD_SK             =  CLM.SRC_SYS_CD_SK

AND     PROV.PROV_ENTY_CD_SK = MAP3.CD_MPPNG_SK
AND     (MAP3.SRC_DOMAIN_NM='PROVIDER ENTITY' OR 
         MAP3.SRC_DOMAIN_NM='CUSTOM DOMAIN MAPPING')
AND     MAP3.TRGT_CD='FCLTY' 
AND PROV.PROV_SK =
          (select CLM_PROV.PROV_SK
             from {IDSOwner}.CLM_PROV CLM_PROV,
                  {IDSOwner}.W_EDW_ETL_DRVR DRV2,
                  {IDSOwner}.CD_MPPNG CM
             where DRV2.CLM_ID  = CLM_PROV.CLM_ID
               and DRV2.SRC_SYS_CD_SK = CLM_PROV.SRC_SYS_CD_SK
               and CLM_PROV.CLM_PROV_ROLE_TYP_CD_SK = CM.CD_MPPNG_SK 
               and (CM.SRC_DOMAIN_NM = 'CLAIM PROVIDER ROLE TYPE' OR 
                    CM.SRC_DOMAIN_NM='CUSTOM DOMAIN MAPPING')
               and CM.TRGT_CD='SVC'
               and CLM_PROV.CLM_ID  = CLM.CLM_ID
               and CLM_PROV.SRC_SYS_CD_SK = CLM.SRC_SYS_CD_SK )
AND CLM.CLM_ID = 
          (select CL.CLM_ID
             from {IDSOwner}.W_EDW_ETL_DRVR DRV3,
                  {IDSOwner}.CLM CL, 
                  {IDSOwner}.CD_MPPNG MAP1
            where DRV3.CLM_ID =  CL.CLM_ID
              and DRV3.SRC_SYS_CD_SK = CL.SRC_SYS_CD_SK
              and CLM.CLM_SUBTYP_CD_SK = MAP1.CD_MPPNG_SK
              and (MAP1.SRC_DOMAIN_NM = 'CLAIM SUBTYPE' OR
                  MAP1.SRC_DOMAIN_NM = 'CUSTOM DOMAIN MAPPING')
              and     MAP1.TRGT_CD='IP'
               and CL.CLM_ID  = CLM.CLM_ID
               and CL.SRC_SYS_CD_SK = CLM.SRC_SYS_CD_SK )

AND        PROV.PROV_FCLTY_TYP_CD_SK = FCLTY_TYP.FCLTY_TYP_CD_SK
AND        FCLTY_TYP.FCLTY_TYP_CD    <>  '0010'      
AND        FCLTY_TYP.FCLTY_TYP_CD    <> '0007'
AND        FCLTY_TYP.FCLTY_TYP_CD    <> '0013'
AND        FCLTY_TYP.FCLTY_TYP_CD    <> '0014'
AND        FCLTY_TYP.FCLTY_TYP_CD    <> '0026'

AND        DRVR.CLM_ID               =   FCLTY.CLM_ID
AND        DRVR.SRC_SYS_CD_SK        =   FCLTY.SRC_SYS_CD_SK             
AND        FCLTY.NRMTV_DRG_SK        =   DRG.DRG_SK
"""

df_db2_IP_CLMS_by_SERVICES = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_IP_CLMS_by_SERVICES)
    .load()
)

# Stages: odbc_UWS_EXP_SVC_CAT_BY_DRG
extract_query_odbc_UWS_EXP_SVC_CAT_BY_DRG = f"""
SELECT 
DRG.DRG_CD, 

SUB_CAT.EXP_SUB_CAT_CD, 
SUB_CAT.EXP_SUB_CAT_NM, 
SUB_CAT.EXP_SVC_CAT_CD, 
'Y' as  IND

 FROM {UWSOwner}.DRG_EXP_SUB_CAT  DRG, 
      {UWSOwner}.EXP_SUB_CAT      SUB_CAT

Where  {{ fn UCase ( DRG.EXP_SUB_CAT_CD) }}    =   {{ fn UCase ( SUB_CAT.EXP_SUB_CAT_CD )}}
"""

df_odbc_UWS_EXP_SVC_CAT_BY_DRG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_uws)
    .options(**jdbc_props_uws)
    .option("query", extract_query_odbc_UWS_EXP_SVC_CAT_BY_DRG)
    .load()
)

# Stages: Copy_303 -> produce two outputs
df_lnk_UWS_EXP_SVC_CAT_DRG = df_odbc_UWS_EXP_SVC_CAT_BY_DRG.select(
    F.col("DRG_CD").alias("DRG_CD"), 
    F.col("EXP_SUB_CAT_CD").alias("EXP_SUB_CAT_CD"),
    F.col("EXP_SUB_CAT_NM").alias("EXP_SUB_CAT_NM"),
    F.col("EXP_SVC_CAT_CD").alias("EXP_SVC_CAT_CD"),
    F.col("IND").alias("IND")
)

df_lnk_UWS_EXP_SVC_CAT = df_odbc_UWS_EXP_SVC_CAT_BY_DRG.select(
    F.col("DRG_CD").alias("DRG_CD"), 
    F.col("EXP_SUB_CAT_CD").alias("EXP_SUB_CAT_CD"),
    F.col("EXP_SUB_CAT_NM").alias("EXP_SUB_CAT_NM"),
    F.col("EXP_SVC_CAT_CD").alias("EXP_SVC_CAT_CD"),
    F.col("IND").alias("IND")
)

# Stages: lkp_Rehab_Drugs
df_lkp_Rehab_Drugs_join = df_db2_REHAB_DRGS.alias('lnk_REHAB_DRUGS').join(
    df_lnk_UWS_EXP_SVC_CAT.alias('lnk_UWS_EXP_SVC_CAT'),
    [
        F.col('lnk_REHAB_DRUGS.DRG_CD') == F.col('lnk_UWS_EXP_SVC_CAT.DRG_CD')
    ],
    how='left'
)
df_lkp_Rehab_Drugs = df_lkp_Rehab_Drugs_join.select(
    F.col('lnk_REHAB_DRUGS.CLM_ID').alias('CLM_ID'),
    F.col('lnk_REHAB_DRUGS.SRC_SYS_CD').alias('SRC_SYS_CD'),
    F.col('lnk_UWS_EXP_SVC_CAT.EXP_SUB_CAT_CD').alias('EXP_SUB_CAT_CD'),
    F.col('lnk_UWS_EXP_SVC_CAT.IND').alias('IND')
)

# Stages: xfm_REHAB_DRUGS
df_xfm_REHAB_DRUGS_filtered = df_lkp_Rehab_Drugs.filter(
    (F.col('IND').isNotNull()) &
    (F.col('IND') != '') &
    (F.upper(F.col('EXP_SUB_CAT_CD')) == 'REHABILITATION')
)
df_xfm_REHAB_DRUGS = df_xfm_REHAB_DRUGS_filtered.select(
    F.col('CLM_ID').alias('CLM_ID'),
    F.col('SRC_SYS_CD').alias('SRC_SYS_CD'),
    F.lit('Y').alias('REHAB_DRUG')
)

# Stages: lkp_IP_CLMS_BY_SERVICES
df_lkp_IP_CLMS_by_SERVICES_join = df_db2_IP_CLMS_by_SERVICES.alias('lnk_IP_CLMS_by_SERVICES').join(
    df_lnk_UWS_EXP_SVC_CAT_DRG.alias('lnk_UWS_EXP_SVC_CAT_DRG'),
    [
        F.col('lnk_IP_CLMS_by_SERVICES.DRG_CD') == F.col('lnk_UWS_EXP_SVC_CAT_DRG.DRG_CD')
    ],
    how='left'
)
df_lkp_IP_CLMS_by_SERVICES = df_lkp_IP_CLMS_by_SERVICES_join.select(
    F.col('lnk_IP_CLMS_by_SERVICES.CLM_ID').alias('CLM_ID'),
    F.col('lnk_IP_CLMS_by_SERVICES.SRC_SYS_CD').alias('SRC_SYS_CD'),
    F.col('lnk_UWS_EXP_SVC_CAT_DRG.EXP_SUB_CAT_CD').alias('EXP_SUB_CAT_CD'),
    F.col('lnk_UWS_EXP_SVC_CAT_DRG.EXP_SVC_CAT_CD').alias('EXP_SVC_CAT_CD'),
    F.col('lnk_UWS_EXP_SVC_CAT_DRG.IND').alias('IND')
)

# Stages: xfm_IP_CLAIMS_BY_SERVICES -> multiple output links
df_lnk_cmplx_chrnic = df_lkp_IP_CLMS_by_SERVICES.filter(
    (F.col('IND').isNotNull()) &
    (F.col('IND') != '') &
    (
       (F.upper(F.col('EXP_SUB_CAT_CD')) == 'CHRONIC CONDITION') |
       (F.upper(F.col('EXP_SUB_CAT_CD')) == 'COMPLEX NEWBORN')
    )
).select(
    F.col('CLM_ID').alias('CLM_ID'),
    F.col('SRC_SYS_CD').alias('SRC_SYS_CD'),
    F.col('EXP_SUB_CAT_CD').alias('EXP_SUB_CAT_CD'),
    F.lit('Y').alias('CMPLX_CHRNIC_IND')
)

df_lnk_maternity = df_lkp_IP_CLMS_by_SERVICES.filter(
    (F.col('IND').isNotNull()) &
    (F.col('IND') != '') &
    (F.upper(F.col('EXP_SVC_CAT_CD')) == 'MATERNITY')
).select(
    F.col('CLM_ID').alias('CLM_ID'),
    F.col('SRC_SYS_CD').alias('SRC_SYS_CD'),
    F.col('EXP_SUB_CAT_CD').alias('EXP_SUB_CAT_CD'),
    F.lit('Y').alias('MATERNITY_IND')
)

df_lnk_surgical = df_lkp_IP_CLMS_by_SERVICES.filter(
    (F.col('IND').isNotNull()) &
    (F.col('IND') != '') &
    (F.upper(F.col('EXP_SVC_CAT_CD')) == 'SURGICAL')
).select(
    F.col('CLM_ID').alias('CLM_ID'),
    F.col('SRC_SYS_CD').alias('SRC_SYS_CD'),
    F.col('EXP_SUB_CAT_CD').alias('EXP_SUB_CAT_CD'),
    F.lit('Y').alias('SURGICAL_IND')
)

df_lnk_mntl_hlth = df_lkp_IP_CLMS_by_SERVICES.filter(
    (F.col('IND').isNotNull()) &
    (F.col('IND') != '') &
    (F.upper(F.col('EXP_SVC_CAT_CD')) == 'MNTL HLTH SUB ABUSE')
).select(
    F.col('CLM_ID').alias('CLM_ID'),
    F.col('SRC_SYS_CD').alias('SRC_SYS_CD'),
    F.col('EXP_SUB_CAT_CD').alias('EXP_SUB_CAT_CD'),
    F.lit('Y').alias('MNTL_HLTH_IND')
)

df_lnk_other_med_stay = df_lkp_IP_CLMS_by_SERVICES.filter(
    (F.col('IND').isNotNull()) &
    (F.col('IND') != '') &
    (F.upper(F.col('EXP_SUB_CAT_CD')) == 'OTHER MEDICAL STAY')
).select(
    F.col('CLM_ID').alias('CLM_ID'),
    F.col('SRC_SYS_CD').alias('SRC_SYS_CD'),
    F.col('EXP_SUB_CAT_CD').alias('EXP_SUB_CAT_CD'),
    F.lit('Y').alias('OTHER_MED_STAY_IND')
)

# Now the big lkp_IP_CLAIMS
df_lkp_IP_CLAIMS_join = df_db2_IP_CLAIMS.alias('lnk_IP_CLAIMS') \
.join(
    df_db2_SNF_RVNU.alias('lnk_SNF_RVNU'),
    [
        F.col('lnk_IP_CLAIMS.CLM_ID')==F.col('lnk_SNF_RVNU.CLM_ID'),
        F.col('lnk_IP_CLAIMS.SRC_SYS_CD')==F.col('lnk_SNF_RVNU.SRC_SYS_CD')
    ],
    how='left'
).join(
    df_db2_REHAB_FCLTY.alias('lnk_REHAB_FCLTY'),
    [
        F.col('lnk_IP_CLAIMS.CLM_ID')==F.col('lnk_REHAB_FCLTY.CLM_ID'),
        F.col('lnk_IP_CLAIMS.SRC_SYS_CD')==F.col('lnk_REHAB_FCLTY.SRC_SYS_CD')
    ],
    how='left'
).join(
    df_db2_REHAB_RVNU.alias('lnk_REHAB_RVNU'),
    [
        F.col('lnk_IP_CLAIMS.CLM_ID')==F.col('lnk_REHAB_RVNU.CLM_ID'),
        F.col('lnk_IP_CLAIMS.SRC_SYS_CD')==F.col('lnk_REHAB_RVNU.SRC_SYS_CD')
    ],
    how='left'
).join(
    df_db2_HOSPICE_FCLTY.alias('lnk_HOSPICE_FCLTY'),
    [
        F.col('lnk_IP_CLAIMS.CLM_ID')==F.col('lnk_HOSPICE_FCLTY.CLM_ID'),
        F.col('lnk_IP_CLAIMS.SRC_SYS_CD')==F.col('lnk_HOSPICE_FCLTY.SRC_SYS_CD')
    ],
    how='left'
).join(
    df_db2_HOSPICE_RVNU.alias('lnk_HOSPICE_RVNU'),
    [
        F.col('lnk_IP_CLAIMS.CLM_ID')==F.col('lnk_HOSPICE_RVNU.CLM_ID'),
        F.col('lnk_IP_CLAIMS.SRC_SYS_CD')==F.col('lnk_HOSPICE_RVNU.SRC_SYS_CD')
    ],
    how='left'
).join(
    df_db2_RESPITE_RVNU.alias('lnk_RESPITE_RVNU'),
    [
        F.col('lnk_IP_CLAIMS.CLM_ID')==F.col('lnk_RESPITE_RVNU.CLM_ID'),
        F.col('lnk_IP_CLAIMS.SRC_SYS_CD')==F.col('lnk_RESPITE_RVNU.SRC_SYS_CD')
    ],
    how='left'
).join(
    df_db2_UngroupAble_drg.alias('lnk_UngroupAble_drg'),
    [
        F.col('lnk_IP_CLAIMS.CLM_ID')==F.col('lnk_UngroupAble_drg.CLM_ID'),
        F.col('lnk_IP_CLAIMS.SRC_SYS_CD')==F.col('lnk_UngroupAble_drg.SRC_SYS_CD')
    ],
    how='left'
).join(
    df_db2_SNF_FCLTY.alias('lnk_SNF_FCLTY'),
    [
        F.col('lnk_IP_CLAIMS.CLM_ID')==F.col('lnk_SNF_FCLTY.CLM_ID'),
        F.col('lnk_IP_CLAIMS.SRC_SYS_CD')==F.col('lnk_SNF_FCLTY.SRC_SYS_CD')
    ],
    how='left'
).join(
    df_xfm_REHAB_DRUGS.alias('lnk_REHAB_DRUG'),
    [
        F.col('lnk_IP_CLAIMS.CLM_ID')==F.col('lnk_REHAB_DRUG.CLM_ID'),
        F.col('lnk_IP_CLAIMS.SRC_SYS_CD')==F.col('lnk_REHAB_DRUG.SRC_SYS_CD')
    ],
    how='left'
).join(
    df_lnk_cmplx_chrnic.alias('lnk_cmplx_chrnic'),
    [
        F.col('lnk_IP_CLAIMS.CLM_ID')==F.col('lnk_cmplx_chrnic.CLM_ID'),
        F.col('lnk_IP_CLAIMS.SRC_SYS_CD')==F.col('lnk_cmplx_chrnic.SRC_SYS_CD')
    ],
    how='left'
).join(
    df_lnk_maternity.alias('lnk_maternity'),
    [
        F.col('lnk_IP_CLAIMS.CLM_ID')==F.col('lnk_maternity.CLM_ID'),
        F.col('lnk_IP_CLAIMS.SRC_SYS_CD')==F.col('lnk_maternity.SRC_SYS_CD')
    ],
    how='left'
).join(
    df_lnk_surgical.alias('lnk_surgical'),
    [
        F.col('lnk_IP_CLAIMS.CLM_ID')==F.col('lnk_surgical.CLM_ID'),
        F.col('lnk_IP_CLAIMS.SRC_SYS_CD')==F.col('lnk_surgical.SRC_SYS_CD')
    ],
    how='left'
).join(
    df_lnk_mntl_hlth.alias('lnk_mntl_hlth'),
    [
        F.col('lnk_IP_CLAIMS.CLM_ID')==F.col('lnk_mntl_hlth.CLM_ID'),
        F.col('lnk_IP_CLAIMS.SRC_SYS_CD')==F.col('lnk_mntl_hlth.SRC_SYS_CD')
    ],
    how='left'
).join(
    df_lnk_other_med_stay.alias('lnk_other_med_stay'),
    [
        F.col('lnk_IP_CLAIMS.CLM_ID')==F.col('lnk_other_med_stay.CLM_ID'),
        F.col('lnk_IP_CLAIMS.SRC_SYS_CD')==F.col('lnk_other_med_stay.SRC_SYS_CD')
    ],
    how='left'
)

df_lkp_IP_CLAIMS = df_lkp_IP_CLAIMS_join.select(
    F.col('lnk_IP_CLAIMS.CLM_ID').alias('CLM_ID'),
    F.col('lnk_IP_CLAIMS.SRC_SYS_CD').alias('SRC_SYS_CD'),
    F.col('lnk_SNF_RVNU.SNF_RVNU_IND').alias('SNF_RVNU_IND'),
    F.col('lnk_REHAB_FCLTY.REHAB_FCLTY_IND').alias('REHAB_FCLTY_IND'),
    F.col('lnk_REHAB_RVNU.REHAB_RVNU_IND').alias('REHAB_RVNU_IND'),
    F.col('lnk_HOSPICE_FCLTY.HOSPICE_FCLTY_IND').alias('HOSPICE_FCLTY_IND'),
    F.col('lnk_HOSPICE_RVNU.HOSPICE_RVNU_IND').alias('HOSPICE_RVNU_IND'),
    F.col('lnk_RESPITE_RVNU.RESPITE_RVNU_IND').alias('RESPITE_RVNU_IND'),
    F.col('lnk_UngroupAble_drg.UngroupAble_Drg_Ind').alias('UngroupAble_Drg_Ind'),
    F.col('lnk_SNF_FCLTY.SNF_FCLTY_IND').alias('SNF_FCLTY_IND'),
    F.col('lnk_REHAB_DRUG.REHAB_DRUG').alias('REHAB_DRUG'),
    F.col('lnk_cmplx_chrnic.CMPLX_CHRNIC_IND').alias('CMPLX_CHRNIC_IND'),
    F.col('lnk_maternity.MATERNITY_IND').alias('MATERNITY'),
    F.col('lnk_surgical.SURGICAL_IND').alias('SURGICAL'),
    F.col('lnk_mntl_hlth.MNTL_HLTH_IND').alias('MNTL_HLTH_IND'),
    F.col('lnk_other_med_stay.OTHER_MED_STAY_IND').alias('OTHER_MED_STAY_IND'),
    F.col('lnk_UngroupAble_drg.DRG_CD').alias('DRG_CD'),
    F.col('lnk_cmplx_chrnic.EXP_SUB_CAT_CD').alias('EXP_SUB_CAT_CD_Cmplx_Chrnic'),
    F.col('lnk_maternity.EXP_SUB_CAT_CD').alias('EXP_SUB_CAT_CD_Maternity'),
    F.col('lnk_surgical.EXP_SUB_CAT_CD').alias('EXP_SUB_CAT_CD_Surgical'),
    F.col('lnk_mntl_hlth.EXP_SUB_CAT_CD').alias('EXP_SUB_CAT_CD_Mntl_hlth'),
    F.col('lnk_other_med_stay.EXP_SUB_CAT_CD').alias('EXP_SUB_CAT_CD_Other_Med_Stay')
)

# Stages: xfm_BusinessLogic
df_xfm_BusinessLogic = df_lkp_IP_CLAIMS \
.withColumn("svSNFFacility",
    F.when((F.col("SNF_FCLTY_IND").isNull()) | (F.col("SNF_FCLTY_IND") == ''), "N").otherwise("Y")
).withColumn("svSNFRevenu",
    F.when((F.col("SNF_RVNU_IND").isNull()) | (F.col("SNF_RVNU_IND") == ''), "N").otherwise("Y")
).withColumn("svRehabDRG",
    F.when((F.col("REHAB_DRUG").isNull()) | (F.col("REHAB_DRUG") == ''), "N").otherwise("Y")
).withColumn("svRehabFacilty",
    F.when((F.col("REHAB_FCLTY_IND").isNull()) | (F.col("REHAB_FCLTY_IND") == ''), "N").otherwise("Y")
).withColumn("svRehabRevenu",
    F.when((F.col("REHAB_RVNU_IND").isNull()) | (F.col("REHAB_RVNU_IND") == ''), "N").otherwise("Y")
).withColumn("svHospiceFaciltiy",
    F.when((F.col("HOSPICE_FCLTY_IND").isNull()) | (F.col("HOSPICE_FCLTY_IND") == ''), "N").otherwise("Y")
).withColumn("svHospiceRevenue",
    F.when((F.col("HOSPICE_RVNU_IND").isNull()) | (F.col("HOSPICE_RVNU_IND") == ''), "N").otherwise("Y")
).withColumn("svRespiteRevenu",
    F.when((F.col("RESPITE_RVNU_IND").isNull()) | (F.col("RESPITE_RVNU_IND") == ''), "N").otherwise("Y")
).withColumn("svServiceMaternity",
    F.when((F.col("MATERNITY").isNull()) | (F.col("MATERNITY") == ''), "N").otherwise("Y")
).withColumn("svServiceMentalHealth",
    F.when((F.col("MNTL_HLTH_IND").isNull()) | (F.col("MNTL_HLTH_IND") == ''), "N").otherwise("Y")
).withColumn("svServiceSurgical",
    F.when((F.col("SURGICAL").isNull()) | (F.col("SURGICAL") == ''), "N").otherwise("Y")
).withColumn("svServiceChronic",
    F.when((F.col("CMPLX_CHRNIC_IND").isNull()) | (F.col("CMPLX_CHRNIC_IND") == ''), "N").otherwise("Y")
).withColumn("svOtherMedicalStayFacility",
    F.when((F.col("OTHER_MED_STAY_IND").isNull()) | (F.col("OTHER_MED_STAY_IND") == ''), "N").otherwise("Y")
).withColumn("svOtherMedicalStayNonGrpableDRG",
    F.when((F.col("UngroupAble_Drg_Ind").isNull()) | (F.col("UngroupAble_Drg_Ind") == ''), "N").otherwise("Y")
).withColumn("svSubCatCode",
    F.when(F.col("svSNFFacility")=="Y","SNF")
     .when(F.col("svSNFRevenu")=="Y","SNF")
     .when(F.col("svRehabDRG")=="Y","REHABILITATION")
     .when(F.col("svRehabFacilty")=="Y","REHABILITATION")
     .when(F.col("svRehabRevenu")=="Y","REHABILITATION")
     .when(F.col("svHospiceFaciltiy")=="Y","HOSPICE")
     .when(F.col("svHospiceRevenue")=="Y","HOSPICE")
     .when(F.col("svRespiteRevenu")=="Y","RESPITE")
     .when(F.col("svServiceMaternity")=="Y",F.col("EXP_SUB_CAT_CD_Maternity"))
     .when(F.col("svServiceMentalHealth")=="Y",F.col("EXP_SUB_CAT_CD_Mntl_hlth"))
     .when(F.col("svServiceSurgical")=="Y",F.col("EXP_SUB_CAT_CD_Surgical"))
     .when(F.col("svServiceChronic")=="Y",F.col("EXP_SUB_CAT_CD_Cmplx_Chrnic"))
     .when(F.col("svOtherMedicalStayFacility")=="Y",F.col("EXP_SUB_CAT_CD_Other_Med_Stay"))
     .when(F.col("svOtherMedicalStayNonGrpableDRG")=="Y","OTHER MEDICAL STAY")
     .otherwise("UNK")
)

df_xfm_BusinessLogic_select = df_xfm_BusinessLogic.select(
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("svSubCatCode").alias("EXP_SUB_CAT_CD")
)

# Stages: ds_clm_f_ip_clms_exp_sub_cat (Dataset -> Parquet)
# Column order is [CLM_ID, SRC_SYS_CD, EXP_SUB_CAT_CD].
df_ds_clm_f_ip_clms_exp_sub_cat = df_xfm_BusinessLogic_select.select(
    "CLM_ID",
    "SRC_SYS_CD",
    "EXP_SUB_CAT_CD"
)

write_files(
    df_ds_clm_f_ip_clms_exp_sub_cat,
    f"{adls_path}/ds/clm_f_ip_clms_exp_sub_cat.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)