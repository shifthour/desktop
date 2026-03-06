# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2004 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     EdwClmAltPayeDimExtr
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC   Pulls data from ids and edw  -  prepares files to compare and update edw claim alternate payee dimension table
# MAGIC   Loads the data from EDW and IDS into two hash files for subsequent comparison.
# MAGIC   The comparison will determine rows that have changed and whether to generate a new history record.
# MAGIC       
# MAGIC 
# MAGIC INPUTS:
# MAGIC 	IDS - CLM_ALT_PAYE
# MAGIC                 IDS - W_EDW_ETL_DRVR
# MAGIC                 EDW - CLM_ALT_PAYE_D
# MAGIC                 EDW - W_EDW_ETL_DRVR
# MAGIC 
# MAGIC 
# MAGIC   
# MAGIC HASH FILES:
# MAGIC                 hf_cdma_codes         - CDMA code table - resolve code lookups
# MAGIC                 hf_CLM_ALT_PAYE_D_ids - hash file from ids
# MAGIC                 hf_CLM_ALT_PAYE_D_edw - hash file from edw
# MAGIC                
# MAGIC 
# MAGIC TRANSFORMS:  
# MAGIC                   TRIM all the fields that are extracted or used in lookups
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC                   IDS - read from source, lookup all code SK values and get the natural codes
# MAGIC                   EDW - Read from source and dump to hf. Do nothing but trim the fields.
# MAGIC               
# MAGIC   
# MAGIC 
# MAGIC OUTPUTS: 
# MAGIC                     Each of the stream output hash files
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC DEVELOPER                         DATE                 PROJECT                                                DESCRIPTION                                            DATASTAGE                                            CODE                            REVIEW
# MAGIC                                                                                                                                                                                                               ENVIRONMENT                                       REVIEWER                   DATE                                              
# MAGIC ------------------------------------------   ----------------------      ------------------------------------------------                ---------------------------------------------------------------     ---------------------------------------------------              ------------------------------          -----------------
# MAGIC               Tom Harrocks 08/02/2004-                                                                               Originally Programmed
# MAGIC               Oliver Nielsen  12/06/2004                                                                                Parameterize Hash File Names
# MAGIC               Brent leland     03/24/2006                                                                               Changed parameters to evironment variables.
# MAGIC                                                                                                                                         Took out trim() on SK values.
# MAGIC               Steph Goddard 04/20/2006                                                                             Removed defaulted fields
# MAGIC               Brent Leland     08/25/2006                                                                             Added delete process to remove historical 
# MAGIC                                                                                                                                        records delete in source system that would 
# MAGIC                                                                                                                                       can conflict between primary keys and natural keys.
# MAGIC Leandrew Moore           2013-07-26               5114                                                          Rewrite in parallel                                           EnterpriseWrhsDevl                              Peter Marshall                 12/16/2013

# MAGIC Code SK lookups for Denormalization
# MAGIC Write CLM_COB_F Data into a Sequential file for Load Job IdsEdwCapFundDLoad.
# MAGIC Read all the Data from IDS CLM_COB Table;
# MAGIC Add Defaults and Null Handling.
# MAGIC Reference Code Mapping Data for Code SK lookups
# MAGIC Job Name: IdsEdwClmCobFExtr
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


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
EDWRunCycle = get_widget_value('EDWRunCycle','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query_db2_CLM_COB_F_in = """SELECT 
CLM_COB_SK,
COALESCE(CD.TRGT_CD, 'UNK')  SRC_SYS_CD,
COB.CLM_ID,
CLM_COB_TYP_CD_SK,
CLM_SK,
CLM_COB_LIAB_TYP_CD_SK,
ALW_AMT,
COPAY_AMT,
DEDCT_AMT,
DSALW_AMT,
MED_COINS_AMT,
MNTL_HLTH_COINS_AMT,
PD_AMT,
SANC_AMT,
COB_CAR_RSN_CD_TX,
COB_CAR_RSN_TX 

FROM
 #$IDSOwner#.CLM_COB COB    
    INNER JOIN   #$IDSOwner#.W_EDW_ETL_DRVR DRVR   
        ON   COB.SRC_SYS_CD_SK = DRVR.SRC_SYS_CD_SK  
             AND  COB.CLM_ID = DRVR.CLM_ID
    LEFT JOIN  #$IDSOwner#.CD_MPPNG  CD   
        ON   CD.CD_MPPNG_SK = DRVR.SRC_SYS_CD_SK
""".replace('#$IDSOwner#', IDSOwner)

df_db2_CLM_COB_F_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_CLM_COB_F_in)
    .load()
)

extract_query_db2_CD_MPPNG_in = f"""SELECT 
CD_MPPNG_SK,
COALESCE(TRGT_CD,'UNK') TRGT_CD,
TRGT_CD_NM

from {IDSOwner}.CD_MPPNG
"""

df_db2_CD_MPPNG_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_CD_MPPNG_in)
    .load()
)

df_lnk_refLiabTyp = df_db2_CD_MPPNG_in.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_lnk_refClmCobType = df_db2_CD_MPPNG_in.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_joined_lkp_Codes = (
    df_db2_CLM_COB_F_in.alias("lnk_IdsEdwClmCobFExtr_InABC")
    .join(
        df_lnk_refLiabTyp.alias("lnk_refLiabTyp"),
        F.col("lnk_IdsEdwClmCobFExtr_InABC.CLM_COB_LIAB_TYP_CD_SK")
        == F.col("lnk_refLiabTyp.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_lnk_refClmCobType.alias("lnk_refClmCobType"),
        F.col("lnk_IdsEdwClmCobFExtr_InABC.CLM_COB_TYP_CD_SK")
        == F.col("lnk_refClmCobType.CD_MPPNG_SK"),
        "left"
    )
)

df_lkp_Codes = df_joined_lkp_Codes.select(
    F.col("lnk_IdsEdwClmCobFExtr_InABC.CLM_COB_SK").alias("CLM_COB_SK"),
    F.col("lnk_IdsEdwClmCobFExtr_InABC.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnk_IdsEdwClmCobFExtr_InABC.CLM_ID").alias("CLM_ID"),
    F.col("lnk_IdsEdwClmCobFExtr_InABC.CLM_COB_TYP_CD_SK").alias("CLM_COB_TYP_CD_SK"),
    F.col("lnk_IdsEdwClmCobFExtr_InABC.CLM_SK").alias("CLM_SK"),
    F.col("lnk_IdsEdwClmCobFExtr_InABC.CLM_COB_LIAB_TYP_CD_SK").alias("CLM_COB_LIAB_TYP_CD_SK"),
    F.col("lnk_IdsEdwClmCobFExtr_InABC.ALW_AMT").alias("ALW_AMT"),
    F.col("lnk_IdsEdwClmCobFExtr_InABC.COPAY_AMT").alias("COPAY_AMT"),
    F.col("lnk_IdsEdwClmCobFExtr_InABC.DEDCT_AMT").alias("DEDCT_AMT"),
    F.col("lnk_IdsEdwClmCobFExtr_InABC.DSALW_AMT").alias("DSALW_AMT"),
    F.col("lnk_IdsEdwClmCobFExtr_InABC.MED_COINS_AMT").alias("MED_COINS_AMT"),
    F.col("lnk_IdsEdwClmCobFExtr_InABC.MNTL_HLTH_COINS_AMT").alias("MNTL_HLTH_COINS_AMT"),
    F.col("lnk_IdsEdwClmCobFExtr_InABC.PD_AMT").alias("PD_AMT"),
    F.col("lnk_IdsEdwClmCobFExtr_InABC.SANC_AMT").alias("SANC_AMT"),
    F.col("lnk_IdsEdwClmCobFExtr_InABC.COB_CAR_RSN_CD_TX").alias("COB_CAR_RSN_CD_TX"),
    F.col("lnk_IdsEdwClmCobFExtr_InABC.COB_CAR_RSN_TX").alias("COB_CAR_RSN_TX"),
    F.col("lnk_refLiabTyp.TRGT_CD").alias("CLM_COB_LIAB_TYP_CD"),
    F.col("lnk_refClmCobType.TRGT_CD").alias("CLM_COB_TYP_CD")
)

df_xfrm_BusinessLogic_lnk_Detail = df_lkp_Codes.filter(
    (F.col("CLM_COB_SK") != 0) & (F.col("CLM_COB_SK") != 1)
).select(
    F.col("CLM_COB_SK").alias("CLM_COB_SK"),
    F.when(F.col("SRC_SYS_CD").isNull(), 'UNK').otherwise(F.col("SRC_SYS_CD")).alias("SRC_SYS_CD"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.when(F.col("CLM_COB_TYP_CD").isNull(), ' ').otherwise(F.col("CLM_COB_TYP_CD")).alias("CLM_COB_TYP_CD"),
    F.lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("CLM_SK").alias("CLM_SK"),
    F.when(F.col("CLM_COB_LIAB_TYP_CD").isNull(), ' ').otherwise(F.col("CLM_COB_LIAB_TYP_CD")).alias("CLM_COB_LIAB_TYP_CD"),
    F.col("ALW_AMT").alias("CLM_COB_ALW_AMT"),
    F.col("COPAY_AMT").alias("CLM_COB_COPAY_AMT"),
    F.col("DEDCT_AMT").alias("CLM_COB_DEDCT_AMT"),
    F.col("DSALW_AMT").alias("CLM_COB_DSALW_AMT"),
    F.col("MED_COINS_AMT").alias("CLM_COB_MED_COINS_AMT"),
    F.col("MNTL_HLTH_COINS_AMT").alias("CLM_COB_MNTL_HLTH_COINS_AMT"),
    F.col("PD_AMT").alias("CLM_COB_PD_AMT"),
    F.col("SANC_AMT").alias("CLM_COB_SANC_AMT"),
    F.col("COB_CAR_RSN_CD_TX").alias("CLM_COB_CAR_RSN_CD_TX"),
    F.col("COB_CAR_RSN_TX").alias("CLM_COB_CAR_RSN_TX"),
    F.lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CLM_COB_TYP_CD_SK").alias("CLM_COB_TYP_CD_SK"),
    F.col("CLM_COB_LIAB_TYP_CD_SK").alias("CLM_COB_LIAB_TYP_CD_SK")
)

df_xfrm_BusinessLogic_lnk_Na = spark.createDataFrame(
    [
        (
            1,
            'NA',
            'NA',
            'NA',
            '1753-01-01',
            '1753-01-01',
            1,
            'NA',
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            None,
            None,
            100,
            100,
            1,
            1
        )
    ],
    [
        "CLM_COB_SK",
        "SRC_SYS_CD",
        "CLM_ID",
        "CLM_COB_TYP_CD",
        "CRT_RUN_CYC_EXCTN_DT_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        "CLM_SK",
        "CLM_COB_LIAB_TYP_CD",
        "CLM_COB_ALW_AMT",
        "CLM_COB_COPAY_AMT",
        "CLM_COB_DEDCT_AMT",
        "CLM_COB_DSALW_AMT",
        "CLM_COB_MED_COINS_AMT",
        "CLM_COB_MNTL_HLTH_COINS_AMT",
        "CLM_COB_PD_AMT",
        "CLM_COB_SANC_AMT",
        "CLM_COB_CAR_RSN_CD_TX",
        "CLM_COB_CAR_RSN_TX",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "CLM_COB_TYP_CD_SK",
        "CLM_COB_LIAB_TYP_CD_SK"
    ]
)

df_xfrm_BusinessLogic_lnk_Unk = spark.createDataFrame(
    [
        (
            0,
            'UNK',
            'UNK',
            'UNK',
            '1753-01-01',
            '1753-01-01',
            0,
            'UNK',
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            None,
            None,
            100,
            100,
            0,
            0
        )
    ],
    [
        "CLM_COB_SK",
        "SRC_SYS_CD",
        "CLM_ID",
        "CLM_COB_TYP_CD",
        "CRT_RUN_CYC_EXCTN_DT_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        "CLM_SK",
        "CLM_COB_LIAB_TYP_CD",
        "CLM_COB_ALW_AMT",
        "CLM_COB_COPAY_AMT",
        "CLM_COB_DEDCT_AMT",
        "CLM_COB_DSALW_AMT",
        "CLM_COB_MED_COINS_AMT",
        "CLM_COB_MNTL_HLTH_COINS_AMT",
        "CLM_COB_PD_AMT",
        "CLM_COB_SANC_AMT",
        "CLM_COB_CAR_RSN_CD_TX",
        "CLM_COB_CAR_RSN_TX",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "CLM_COB_TYP_CD_SK",
        "CLM_COB_LIAB_TYP_CD_SK"
    ]
)

commonCols = [
    "CLM_COB_SK",
    "SRC_SYS_CD",
    "CLM_ID",
    "CLM_COB_TYP_CD",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "CLM_SK",
    "CLM_COB_LIAB_TYP_CD",
    "CLM_COB_ALW_AMT",
    "CLM_COB_COPAY_AMT",
    "CLM_COB_DEDCT_AMT",
    "CLM_COB_DSALW_AMT",
    "CLM_COB_MED_COINS_AMT",
    "CLM_COB_MNTL_HLTH_COINS_AMT",
    "CLM_COB_PD_AMT",
    "CLM_COB_SANC_AMT",
    "CLM_COB_CAR_RSN_CD_TX",
    "CLM_COB_CAR_RSN_TX",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "CLM_COB_TYP_CD_SK",
    "CLM_COB_LIAB_TYP_CD_SK"
]

df_xfrm_BusinessLogic_lnk_Detail_sel = df_xfrm_BusinessLogic_lnk_Detail.select(commonCols)
df_xfrm_BusinessLogic_lnk_Na_sel = df_xfrm_BusinessLogic_lnk_Na.select(commonCols)
df_xfrm_BusinessLogic_lnk_Unk_sel = df_xfrm_BusinessLogic_lnk_Unk.select(commonCols)

df_Fnl_Clm_Cob_F = df_xfrm_BusinessLogic_lnk_Detail_sel.union(df_xfrm_BusinessLogic_lnk_Na_sel).union(df_xfrm_BusinessLogic_lnk_Unk_sel)

df_final = df_Fnl_Clm_Cob_F.select(
    F.col("CLM_COB_SK"),
    F.col("SRC_SYS_CD"),
    F.col("CLM_ID"),
    F.col("CLM_COB_TYP_CD"),
    F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("CLM_SK"),
    F.col("CLM_COB_LIAB_TYP_CD"),
    F.col("CLM_COB_ALW_AMT"),
    F.col("CLM_COB_COPAY_AMT"),
    F.col("CLM_COB_DEDCT_AMT"),
    F.col("CLM_COB_DSALW_AMT"),
    F.col("CLM_COB_MED_COINS_AMT"),
    F.col("CLM_COB_MNTL_HLTH_COINS_AMT"),
    F.col("CLM_COB_PD_AMT"),
    F.col("CLM_COB_SANC_AMT"),
    F.col("CLM_COB_CAR_RSN_CD_TX"),
    F.col("CLM_COB_CAR_RSN_TX"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CLM_COB_TYP_CD_SK"),
    F.col("CLM_COB_LIAB_TYP_CD_SK")
)

write_files(
    df_final,
    f"{adls_path}/load/CLM_COB_F.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)