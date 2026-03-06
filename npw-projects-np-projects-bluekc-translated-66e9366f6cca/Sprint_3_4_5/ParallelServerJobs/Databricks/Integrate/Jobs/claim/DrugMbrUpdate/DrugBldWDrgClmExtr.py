# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_2 03/10/09 09:25:21 Batch  15045_33943 INIT bckcetl ids20 dcg01 sa Bringing ALL Claim code down to devlIDS
# MAGIC ^1_5 02/10/09 11:16:45 Batch  15017_40611 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_4 11/21/08 10:38:45 Batch  14936_38329 PROMOTE bckcetl ids20 dsadm bls for sa
# MAGIC ^1_4 11/21/08 10:35:35 Batch  14936_38139 INIT bckcett testIDSnew dsadm bls for sa
# MAGIC ^1_3 11/21/08 09:31:24 Batch  14936_34286 INIT bckcett testIDSnew dsadm bls for sa
# MAGIC ^1_1 11/20/08 08:35:42 Batch  14935_30947 PROMOTE bckcett testIDSnew u03651 steph for Sharon
# MAGIC ^1_1 11/20/08 08:30:03 Batch  14935_30606 INIT bckcett devlIDSnew u03651 steffy
# MAGIC ^1_1 01/28/08 09:39:22 Batch  14638_34767 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 01/09/08 13:32:13 Batch  14619_48738 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 12/28/07 10:13:19 Batch  14607_36806 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_2 12/05/07 12:42:47 Batch  14584_45791 PROMOTE bckcetl ids20 dsadm rc for steph 
# MAGIC ^1_2 12/05/07 12:38:37 Batch  14584_45528 INIT bckcett testIDS30 dsadm rc for steph
# MAGIC ^1_3 12/04/07 10:16:02 Batch  14583_36968 PROMOTE bckcett testIDS30 u03651 Steph for Me
# MAGIC ^1_3 12/04/07 10:14:01 Batch  14583_36844 INIT bckcett devlIDS30 u03651 steffy
# MAGIC ^1_1 11/24/07 17:16:00 Batch  14573_62165 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 10/18/07 10:33:23 Batch  14536_38008 PROMOTE bckcetl ids20 dsadm bls for sg
# MAGIC ^1_1 10/18/07 10:16:25 Batch  14536_36992 INIT bckcett testIDSnew dsadm bls for sg
# MAGIC ^1_2 10/16/07 11:08:03 Batch  14534_40085 PROMOTE bckcett testIDSnew u03651 steffy
# MAGIC ^1_2 10/16/07 11:07:08 Batch  14534_40030 INIT bckcett devlIDS30 u03651 steffy
# MAGIC ^1_1 10/16/07 11:03:57 Batch  14534_39839 INIT bckcett devlIDS30 u03651 steffy
# MAGIC 
# MAGIC 
# MAGIC © Copyright 2007 Blue Cross/Blue Shield of Kansas City
# MAGIC © Copyright 2008 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC   looks for ARGUS or PCS drug claims (depending on parameter) without membership information.  Sometimes we get drug claims and can't match up to membership information because it hasn't been entered into the system yet.  If a claim has a paid date and missing member info, extract it and build driver table W_DRUG_CLM.
# MAGIC Program DrugClmFindMbrExtr will read the driver table and look for matching membership information
# MAGIC program DrugClmMbrDataUpdt will then update these claims with the matching membership information
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 \(9)Project/Altiris #      \(9)\(9)Change Description                                                                 Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     \(9)------------------------      \(9)\(9)-----------------------------------------------------------------------                         --------------------------------       -------------------------------   ----------------------------       
# MAGIC Steph Goddard        10/03/2007             Prod. Support                          Originally Programmed\(9)\(9)\(9)\(9)devlIDS30                       Brent Leland             10-08-2007  
# MAGIC Steph Goddard        11/30/2007             Prod. Support                          Added checking for 'NA' fields                                                 devlIDS30                       Brent Leland             12-04-2007
# MAGIC SAndrew                  2008-11-18              TTR394                                   Added criteria of  CLM.ALPHA_PFX_SK = 0                          devlIDSnew                    Steph Goddard           11/19/2008
# MAGIC                                                                                                                 Changed SQL to use source and claim ID to join 
# MAGIC                                                                                                                 instead of clm_sk.

# MAGIC Find Claims on IDS CLM table that meet criteria and load key fields to the W_DRUG_CLM load file.
# MAGIC Find claims missing membership information and load driver table W_DRUG_CLM with claim information
# MAGIC WHERE SRC_SYS_CD =  '#Source#'
# MAGIC AND        ( CLM.FNCL_LOB_SK = 0 
# MAGIC              OR CLM.PROD_SK = 0 
# MAGIC              OR CLM.CLS_SK = 0 
# MAGIC              OR  CLM.GRP_SK = 0  
# MAGIC              OR CLM.CLS_PLN_SK = 0 
# MAGIC              OR CLM.SUBGRP_SK = 0 
# MAGIC              OR CLM.MBR_SK =0  
# MAGIC              OR CLM.SUB_SK = 0 
# MAGIC              OR CLM.EXPRNC_CAT_SK= 0 
# MAGIC              OR CLM.FNCL_LOB_SK = 1 
# MAGIC              OR CLM.PROD_SK = 1
# MAGIC              OR CLM.CLS_SK = 1
# MAGIC              OR  CLM.GRP_SK = 1  
# MAGIC              OR CLM.CLS_PLN_SK = 1 
# MAGIC              OR CLM.SUBGRP_SK = 1 
# MAGIC              OR CLM.MBR_SK =1  
# MAGIC              OR CLM.SUB_SK = 1 
# MAGIC              OR CLM.EXPRNC_CAT_SK= 1 
# MAGIC              OR CLM.ALPHA_PFX_SK = 0 )
# MAGIC DrugBldWDrgClmExtr
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


Source = get_widget_value('Source','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query = f"""
SELECT
DRUG.DRUG_CLM_SK AS DRUG_CLM_SK,
CLM.SRC_SYS_CD_SK AS SRC_SYS_CD_SK,
CLM.CLM_SK        AS CLM_SK,
'{Source}'        AS SRC_SYS_CD,
DRUG.RX_NO        AS RX_NO,
DRUG.FILL_DT_SK   AS FILL_DT_SK,
0                 AS PROV_SK,
CLM.CLM_ID        AS CLM_ID,
'NA'              AS VNDR_CLM_NO
FROM {IDSOwner}.CLM CLM,
     {IDSOwner}.DRUG_CLM DRUG,
     {IDSOwner}.CD_MPPNG MAP1
WHERE CLM.SRC_SYS_CD_SK = MAP1.CD_MPPNG_SK
  AND MAP1.TRGT_CD = '{Source}'
  AND (
    CLM.FNCL_LOB_SK = 0
    OR CLM.PROD_SK = 0
    OR CLM.CLS_SK = 0
    OR CLM.GRP_SK = 0
    OR CLM.CLS_PLN_SK = 0
    OR CLM.SUBGRP_SK = 0
    OR CLM.MBR_SK = 0
    OR CLM.SUB_SK = 0
    OR CLM.EXPRNC_CAT_SK= 0
    OR CLM.FNCL_LOB_SK = 1
    OR CLM.PROD_SK = 1
    OR CLM.CLS_SK = 1
    OR CLM.GRP_SK = 1
    OR CLM.CLS_PLN_SK = 1
    OR CLM.SUBGRP_SK = 1
    OR CLM.MBR_SK = 1
    OR CLM.SUB_SK = 1
    OR CLM.EXPRNC_CAT_SK= 1
    OR CLM.ALPHA_PFX_SK = 0
  )
  AND CLM.SRC_SYS_CD_SK = DRUG.SRC_SYS_CD_SK
  AND CLM.CLM_ID = DRUG.CLM_ID
"""

df_IDSClaim = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_move_fields = df_IDSClaim.select(
    col("DRUG_CLM_SK"),
    col("SRC_SYS_CD_SK"),
    col("CLM_SK"),
    col("SRC_SYS_CD"),
    col("RX_NO"),
    col("FILL_DT_SK"),
    col("PROV_SK"),
    col("CLM_ID"),
    col("VNDR_CLM_NO")
)

df_W_DRUG_CLM = df_move_fields.withColumn("FILL_DT_SK", rpad("FILL_DT_SK", 10, " "))
df_W_DRUG_CLM = df_W_DRUG_CLM.select(
    "DRUG_CLM_SK",
    "SRC_SYS_CD_SK",
    "CLM_SK",
    "SRC_SYS_CD",
    "RX_NO",
    "FILL_DT_SK",
    "PROV_SK",
    "CLM_ID",
    "VNDR_CLM_NO"
)

write_files(
    df_W_DRUG_CLM,
    f"{adls_path}/load/W_DRUG_CLM.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)