# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:          
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   This is an extract from Product component table into the PROD_OOP_SUM_F table with summation logic added.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                             Development Project                             Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------      ----------------------------------                             ---------------------------------    -------------------------   
# MAGIC Parikshith Chada               12/14/2007          3044                              Originally Programmed                           devlEDW10               
# MAGIC  LEE    MOORE                 05/31/2013         5114                             UPDATE TO PARALLEL                        ENTERPRISEwarehouse                  Pete Marshall              2013-08-08

# MAGIC Code SK lookups for Denormalization
# MAGIC 
# MAGIC Once the SK is used for denormalization, do not carry oevr that information if it is not needed in the target table.
# MAGIC Read from source table;PROD_VRBLCMPNT and LMT_CMPNTand CODES 
# MAGIC Apply Run Cycle filters when applicable to get just the needed rows forward
# MAGIC Job:
# MAGIC ProdCopaySumFExtr
# MAGIC Table:
# MAGIC  PROD_OOP_SUM_F         
# MAGIC Pull the matched set of products fron IDS and move to EDW.
# MAGIC K table - Use K table when all we need from this look up is Natural key from FKey denormalization. 
# MAGIC 
# MAGIC Base table - Use Base table when we need more than Natural key values from FKey denormalization
# MAGIC 
# MAGIC Once the SK is used for denormalization, do not carry oevr that information if it is not needed in the target table.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner', '')
ids_secret_name = get_widget_value('ids_secret_name', '')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate', '')
EDWRunCycle = get_widget_value('EDWRunCycle', '')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
df_db2_ProdVrblCmpnt_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        """--used to remove dup--
SELECT DISTINCT
  SRC_SYS_CD,
SRC_SYS_CD_SK,
PROD_ID,
EFF_DT_SK,
PROD_SK,
TERM_DT_SK
FROM (


SELECT COALESCE(CD2.TRGT_CD,'NA') AS  SRC_SYS_CD,
PROD_VRBL_CMPNT.SRC_SYS_CD_SK,
PROD_VRBL_CMPNT.PROD_ID,
PROD_VRBL_CMPNT.EFF_DT_SK,
PROD_VRBL_CMPNT.PROD_SK,
PROD_VRBL_CMPNT.TERM_DT_SK
 FROM 
""" + f"{IDSOwner}" + """.PROD_VRBL_CMPNT PROD_VRBL_CMPNT
join  """ + f"{IDSOwner}" + """.LMT_CMPNT LMT_CMPNT
ON      
        PROD_VRBL_CMPNT.LMT_CMPNT_ID = LMT_CMPNT.LMT_CMPNT_ID 
join  """ + f"{IDSOwner}" + """.CD_MPPNG CD_MPPNG 
ON   
PROD_VRBL_CMPNT.PVC_TYP_CD_SK = CD_MPPNG.CD_MPPNG_SK 
 LEFT OUTER JOIN """ + f"{IDSOwner}" + """.CD_MPPNG  CD2
ON   PROD_VRBL_CMPNT.SRC_SYS_CD_SK = CD2.CD_MPPNG_SK 
WHERE 
 
PROD_VRBL_CMPNT.TIER_NO = 1 
AND 
PROD_VRBL_CMPNT.PREAUTH_NOT_RQRD_IN = 'Y' 
AND 
PROD_VRBL_CMPNT.RFRL_NOT_RQRD_IN = 'Y' 
AND 
CD_MPPNG.TRGT_CD = 'STD' 
AND 
(PROD_VRBL_CMPNT.IN_NTWK_PROV_BNF_LVL_IN = 'Y' OR PROD_VRBL_CMPNT.PAR_PROV_BNF_LVL_IN = 'Y') 
AND 
(LMT_CMPNT.ACCUM_NO = 1 OR LMT_CMPNT.ACCUM_NO = 2)

ORDER BY  
COALESCE(CD2.TRGT_CD,'UNK'),
PROD_VRBL_CMPNT.PROD_ID,
PROD_VRBL_CMPNT.EFF_DT_SK

)
--remove dup in this stage
"""
    )
    .load()
)

df_db2_InNtwkYAccum2Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        """SELECT  DISTINCT
PROD_VRBL_CMPNT.SRC_SYS_CD_SK,
PROD_VRBL_CMPNT.PROD_ID,
PROD_VRBL_CMPNT.EFF_DT_SK,
LMT_CMPNT.LMT_CMPNT_MAX_ACCUM_AMT
 FROM 
""" + f"{IDSOwner}" + """.PROD_VRBL_CMPNT PROD_VRBL_CMPNT,
""" + f"{IDSOwner}" + """.CD_MPPNG CD_MPPNG,
""" + f"{IDSOwner}" + """.LMT_CMPNT LMT_CMPNT
 WHERE
 PROD_VRBL_CMPNT.TIER_NO = 1
 AND 
PROD_VRBL_CMPNT.PREAUTH_NOT_RQRD_IN = 'Y' 
AND
 PROD_VRBL_CMPNT.RFRL_NOT_RQRD_IN = 'Y' 
AND 
PROD_VRBL_CMPNT.IN_NTWK_PROV_BNF_LVL_IN = 'Y' 
AND 
PROD_VRBL_CMPNT.PVC_TYP_CD_SK = CD_MPPNG.CD_MPPNG_SK
 AND
 CD_MPPNG.TRGT_CD = 'STD'
 AND 
PROD_VRBL_CMPNT.LMT_CMPNT_ID = LMT_CMPNT.LMT_CMPNT_ID 
AND
 LMT_CMPNT.ACCUM_NO = 2
"""
    )
    .load()
)

df_db2_InNtwkYAccum1Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        """SELECT DISTINCT
PROD_VRBL_CMPNT.SRC_SYS_CD_SK,
PROD_VRBL_CMPNT.PROD_ID,
PROD_VRBL_CMPNT.EFF_DT_SK,
LMT_CMPNT.LMT_CMPNT_MAX_ACCUM_AMT
 FROM
 """ + f"{IDSOwner}" + """.PROD_VRBL_CMPNT PROD_VRBL_CMPNT,
""" + f"{IDSOwner}" + """.CD_MPPNG CD_MPPNG,
""" + f"{IDSOwner}" + """.LMT_CMPNT LMT_CMPNT
 WHERE
 PROD_VRBL_CMPNT.TIER_NO = 1
 AND
 PROD_VRBL_CMPNT.PREAUTH_NOT_RQRD_IN = 'Y' 
AND
 PROD_VRBL_CMPNT.RFRL_NOT_RQRD_IN = 'Y'
 AND
 PROD_VRBL_CMPNT.IN_NTWK_PROV_BNF_LVL_IN = 'Y'
 AND 
PROD_VRBL_CMPNT.PVC_TYP_CD_SK = CD_MPPNG.CD_MPPNG_SK
 AND
 CD_MPPNG.TRGT_CD = 'STD' 
AND 
PROD_VRBL_CMPNT.LMT_CMPNT_ID = LMT_CMPNT.LMT_CMPNT_ID 
AND
 LMT_CMPNT.ACCUM_NO = 1
"""
    )
    .load()
)

df_db2_ParYAccum1Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        """SELECT DISTINCT
 PROD_VRBL_CMPNT.SRC_SYS_CD_SK,
PROD_VRBL_CMPNT.PROD_ID,
PROD_VRBL_CMPNT.EFF_DT_SK,
LMT_CMPNT.LMT_CMPNT_MAX_ACCUM_AMT
 FROM 
""" + f"{IDSOwner}" + """.PROD_VRBL_CMPNT PROD_VRBL_CMPNT,
""" + f"{IDSOwner}" + """.CD_MPPNG CD_MPPNG,
""" + f"{IDSOwner}" + """.LMT_CMPNT LMT_CMPNT 
WHERE 
PROD_VRBL_CMPNT.TIER_NO = 1
 AND 
PROD_VRBL_CMPNT.PREAUTH_NOT_RQRD_IN = 'Y' 
AND 
PROD_VRBL_CMPNT.RFRL_NOT_RQRD_IN = 'Y'
 AND
 PROD_VRBL_CMPNT.PAR_PROV_BNF_LVL_IN = 'Y' 
AND 
PROD_VRBL_CMPNT.PVC_TYP_CD_SK = CD_MPPNG.CD_MPPNG_SK 
AND 
CD_MPPNG.TRGT_CD = 'STD'
AND 
PROD_VRBL_CMPNT.LMT_CMPNT_ID = LMT_CMPNT.LMT_CMPNT_ID
 AND 
LMT_CMPNT.ACCUM_NO = 1
"""
    )
    .load()
)

df_db2_ParYAccum2Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        """SELECT DISTINCT
PROD_VRBL_CMPNT.SRC_SYS_CD_SK,
PROD_VRBL_CMPNT.PROD_ID,
PROD_VRBL_CMPNT.EFF_DT_SK,
LMT_CMPNT.LMT_CMPNT_MAX_ACCUM_AMT 
FROM 
""" + f"{IDSOwner}" + """.PROD_VRBL_CMPNT PROD_VRBL_CMPNT,
""" + f"{IDSOwner}" + """.CD_MPPNG CD_MPPNG,
""" + f"{IDSOwner}" + """.LMT_CMPNT LMT_CMPNT 
WHERE 
PROD_VRBL_CMPNT.TIER_NO = 1 
AND 
PROD_VRBL_CMPNT.PREAUTH_NOT_RQRD_IN = 'Y' 
AND 
PROD_VRBL_CMPNT.RFRL_NOT_RQRD_IN = 'Y' 
AND
 PROD_VRBL_CMPNT.PAR_PROV_BNF_LVL_IN = 'Y' 
AND 
PROD_VRBL_CMPNT.PVC_TYP_CD_SK = CD_MPPNG.CD_MPPNG_SK 
AND
 CD_MPPNG.TRGT_CD = 'STD' 
AND 
PROD_VRBL_CMPNT.LMT_CMPNT_ID = LMT_CMPNT.LMT_CMPNT_ID
 AND
 LMT_CMPNT.ACCUM_NO = 2
"""
    )
    .load()
)

df_lkp_prod_oop_sum_f = (
    df_db2_ProdVrblCmpnt_Extr.alias("main")
    .join(
        df_db2_InNtwkYAccum1Extr.alias("lkpInNtwk1"),
        [
            F.col("main.SRC_SYS_CD_SK") == F.col("lkpInNtwk1.SRC_SYS_CD_SK"),
            F.col("main.PROD_ID") == F.col("lkpInNtwk1.PROD_ID"),
            F.col("main.EFF_DT_SK") == F.col("lkpInNtwk1.EFF_DT_SK"),
        ],
        "left",
    )
    .join(
        df_db2_ParYAccum1Extr.alias("lkpPar1"),
        [
            F.col("main.SRC_SYS_CD_SK") == F.col("lkpPar1.SRC_SYS_CD_SK"),
            F.col("main.PROD_ID") == F.col("lkpPar1.PROD_ID"),
            F.col("main.EFF_DT_SK") == F.col("lkpPar1.EFF_DT_SK"),
        ],
        "left",
    )
    .join(
        df_db2_ParYAccum2Extr.alias("lkpPar2"),
        [
            F.col("main.SRC_SYS_CD_SK") == F.col("lkpPar2.SRC_SYS_CD_SK"),
            F.col("main.PROD_ID") == F.col("lkpPar2.PROD_ID"),
            F.col("main.EFF_DT_SK") == F.col("lkpPar2.EFF_DT_SK"),
        ],
        "left",
    )
    .join(
        df_db2_InNtwkYAccum2Extr.alias("lkpInNtwk2"),
        [
            F.col("main.SRC_SYS_CD_SK") == F.col("lkpInNtwk2.SRC_SYS_CD_SK"),
            F.col("main.PROD_ID") == F.col("lkpInNtwk2.PROD_ID"),
            F.col("main.EFF_DT_SK") == F.col("lkpInNtwk2.EFF_DT_SK"),
        ],
        "left",
    )
    .select(
        F.col("main.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("main.PROD_ID").alias("PROD_ID"),
        F.col("main.EFF_DT_SK").alias("EFF_DT_SK"),
        F.col("main.PROD_SK").alias("PROD_SK"),
        F.col("main.TERM_DT_SK").alias("TERM_DT_SK"),
        F.col("lkpInNtwk1.LMT_CMPNT_MAX_ACCUM_AMT").alias("OOP_INDV_IN_NTWK_MAX_AMT"),
        F.col("lkpPar1.LMT_CMPNT_MAX_ACCUM_AMT").alias("OOP_INDV_OUT_NTWK_MAX_AMT"),
        F.col("lkpPar2.LMT_CMPNT_MAX_ACCUM_AMT").alias("OOP_FMLY_OUT_NTWK_MAX_AMT"),
        F.col("lkpInNtwk2.LMT_CMPNT_MAX_ACCUM_AMT").alias("OOP_FMLY_IN_NTWK_MAX_AMT"),
    )
)

df_main_data_in = (
    df_lkp_prod_oop_sum_f.filter(
        (F.col("SRC_SYS_CD") != "UNK")
        & (F.col("PROD_ID") != "UNK")
        & (F.col("EFF_DT_SK") != "1753-01-01")
        & (F.col("SRC_SYS_CD") != "NA")
        & (F.col("PROD_ID") != "NA")
        & (F.col("EFF_DT_SK") != "1753-01-01")
    )
    .select(
        F.lit(0).alias("PROD_OOP_SUM_SK"),
        F.when(F.length(trim(F.col("SRC_SYS_CD"))) == 0, F.lit("NA")).otherwise(F.col("SRC_SYS_CD")).alias("SRC_SYS_CD"),
        F.col("PROD_ID").alias("PROD_ID"),
        F.col("EFF_DT_SK").alias("PVC_EFF_DT_SK"),
        F.col("TERM_DT_SK").alias("PVC_TERM_DT_SK"),
        F.when(
            (F.col("SRC_SYS_CD") == "NA")
            | (F.length(trim(F.col("PROD_ID"))) == 0)
            | (F.length(trim(F.col("EFF_DT_SK"))) == 0),
            F.lit(None),
        ).otherwise(F.col("OOP_INDV_IN_NTWK_MAX_AMT")).alias("OOP_INDV_IN_NTWK_MAX_AMT"),
        F.when(
            (F.col("SRC_SYS_CD") == "NA")
            | (F.length(trim(F.col("PROD_ID"))) == 0)
            | (F.length(trim(F.col("EFF_DT_SK"))) == 0),
            F.lit(None),
        ).otherwise(F.col("OOP_FMLY_IN_NTWK_MAX_AMT")).alias("OOP_FMLY_IN_NTWK_MAX_AMT"),
        F.when(
            (F.col("SRC_SYS_CD") == "NA")
            | (F.length(trim(F.col("PROD_ID"))) == 0)
            | (F.length(trim(F.col("EFF_DT_SK"))) == 0),
            F.lit(None),
        ).otherwise(F.col("OOP_INDV_OUT_NTWK_MAX_AMT")).alias("OOP_INDV_OUT_NTWK_MAX_AMT"),
        F.when(
            (F.col("SRC_SYS_CD") == "NA")
            | (F.length(trim(F.col("PROD_ID"))) == 0)
            | (F.length(trim(F.col("EFF_DT_SK"))) == 0),
            F.lit(None),
        ).otherwise(F.col("OOP_FMLY_OUT_NTWK_MAX_AMT")).alias("OOP_FMLY_OUT_NTWK_MAX_AMT"),
        F.col("PROD_SK").alias("PROD_SK"),
        F.lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    )
)

df_unk = (
    df_lkp_prod_oop_sum_f.limit(1)
    .select(
        F.lit(0).alias("PROD_OOP_SUM_SK"),
        F.lit("UNK").alias("SRC_SYS_CD"),
        F.lit("UNK").alias("PROD_ID"),
        F.lit("1753-01-01").alias("PVC_EFF_DT_SK"),
        F.lit("1753-01-01").alias("PVC_TERM_DT_SK"),
        F.lit(None).alias("OOP_INDV_IN_NTWK_MAX_AMT"),
        F.lit(None).alias("OOP_FMLY_IN_NTWK_MAX_AMT"),
        F.lit(None).alias("OOP_INDV_OUT_NTWK_MAX_AMT"),
        F.lit(None).alias("OOP_FMLY_OUT_NTWK_MAX_AMT"),
        F.lit(0).alias("PROD_SK"),
        F.lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    )
)

df_na = (
    df_lkp_prod_oop_sum_f.limit(1)
    .select(
        F.lit(1).alias("PROD_OOP_SUM_SK"),
        F.lit("NA").alias("SRC_SYS_CD"),
        F.lit("NA").alias("PROD_ID"),
        F.lit("1753-01-01").alias("PVC_EFF_DT_SK"),
        F.lit("1753-01-01").alias("PVC_TERM_DT_SK"),
        F.lit(None).alias("OOP_INDV_IN_NTWK_MAX_AMT"),
        F.lit(None).alias("OOP_FMLY_IN_NTWK_MAX_AMT"),
        F.lit(None).alias("OOP_INDV_OUT_NTWK_MAX_AMT"),
        F.lit(None).alias("OOP_FMLY_OUT_NTWK_MAX_AMT"),
        F.lit(1).alias("PROD_SK"),
        F.lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    )
)

cols = [
    "PROD_OOP_SUM_SK",
    "SRC_SYS_CD",
    "PROD_ID",
    "PVC_EFF_DT_SK",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "PVC_TERM_DT_SK",
    "OOP_INDV_IN_NTWK_MAX_AMT",
    "OOP_FMLY_IN_NTWK_MAX_AMT",
    "OOP_INDV_OUT_NTWK_MAX_AMT",
    "OOP_FMLY_OUT_NTWK_MAX_AMT",
    "PROD_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
]

df_fnl_NA_UNK = (
    df_unk.select(cols)
    .unionByName(df_na.select(cols))
    .unionByName(df_main_data_in.select(cols))
)

df_fnl_NA_UNK_final = df_fnl_NA_UNK.select(
    F.col("PROD_OOP_SUM_SK"),
    F.col("SRC_SYS_CD"),
    F.rpad(F.col("PROD_ID"), 8, " ").alias("PROD_ID"),
    F.rpad(F.col("PVC_EFF_DT_SK"), 10, " ").alias("PVC_EFF_DT_SK"),
    F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.rpad(F.col("PVC_TERM_DT_SK"), 10, " ").alias("PVC_TERM_DT_SK"),
    F.col("OOP_INDV_IN_NTWK_MAX_AMT"),
    F.col("OOP_FMLY_IN_NTWK_MAX_AMT"),
    F.col("OOP_INDV_OUT_NTWK_MAX_AMT"),
    F.col("OOP_FMLY_OUT_NTWK_MAX_AMT"),
    F.col("PROD_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
)

write_files(
    df_fnl_NA_UNK_final,
    f"{adls_path}/ds/PROD_OOP_SUM_F.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote='"',
    nullValue=None
)