# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2004 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     ProdExtr
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC   Pulls data from (idsdevl.prod, idsdevl.prod_sh_nm) and
# MAGIC                            edwdevl.prod_d
# MAGIC   Loads the data from EDW and IDS into two hash files for subsequent comparison.
# MAGIC   The comparison will determine rows that have changed and whether to generate a new history record.
# MAGIC       
# MAGIC 
# MAGIC INPUTS:
# MAGIC 	IDSDEVL.PROD
# MAGIC                 IDSDEVL.PROD_SH_NM
# MAGIC                 EDWDEVL.PROD_D
# MAGIC                 IDSDEVL.EXPRNC_CAT
# MAGIC                 /dev/null - just to init the compare hash-file
# MAGIC 
# MAGIC   
# MAGIC HASH FILES:
# MAGIC                 hf_cdma_codes         - Load the CDMA codes to get cd a nd cd_nm from cd_sk
# MAGIC                 hf_run_cycle              - Load the run cycles to match up for dates
# MAGIC                 hf_ids_prod                - Store the EDW/CRF PROD records extracted from IDS
# MAGIC                 hf_edw_prod              - Store the EDW/CRF PROD records extracted from EDW
# MAGIC                 hf_edw_prod_iud       - Stores the records that need to be loaded to EDW
# MAGIC 
# MAGIC TRANSFORMS:  
# MAGIC                   TRIM all the fields that are extracted or used in lookups
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC                   IDS - read from source, lookup all code SK values and get the natural codes
# MAGIC                   EDW - Read from source and dump to hf. Do nothing but trim the fields.
# MAGIC                   /dev/null - no records, just used to re-init the IUD hf
# MAGIC   
# MAGIC 
# MAGIC OUTPUTS: 
# MAGIC                     Each of the stream output hash files
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC               Tom Harrocks 08/02/2004-   Originally Programmed
# MAGIC               Brent leland     03/24/2006    Changed parameters to evironment variables.
# MAGIC                                                              Took out trim() on SK values.
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------     -----------------------------------    ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada               03/26/2008          3255                         Added ITS Home new logic to the job    devlEDWcur                 Steph Goddard            03/30/2008
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC Balkarn Gill               09/30/2013        5114                              Create Load File for EDW Table CLM_STTUS_AUDIT_F                   EnterpriseWhseDevl  Peter Marshall               12/20/2013

# MAGIC Reference Code Mapping Data for Code SK lookups
# MAGIC Job Name: IdsEdwClmSttusAuditFExtr
# MAGIC Read from source table CLM_STTUS_AUDIT .
# MAGIC Code SK lookups for Denormalization
# MAGIC 
# MAGIC Lookup Keys:
# MAGIC CLM_STTUS_CD_SK,
# MAGIC TRNSMSN_SRC_CD_SK,
# MAGIC CLM_STTUS_CHG_RSN_CD_SK.
# MAGIC Write CLM_STTUS_AUDIT_F Data into a Sequential file for Load Job IdsEdwClmSttusAuditFLoad.
# MAGIC Add Defaults and Null Handling.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
CurrRunCycleDate = get_widget_value('CurrRunCycleDate','')
CurrRunCycle = get_widget_value('CurrRunCycle','')

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

extract_query_db2_CLM_STTUS_AUDIT_in = f"""SELECT 
sa.CLM_STTUS_AUDIT_SK,
sa.CLM_ID,
sa.CLM_STTUS_AUDIT_SEQ_NO,
sa.CRT_RUN_CYC_EXCTN_SK,
sa.LAST_UPDT_RUN_CYC_EXCTN_SK,
sa.CLM_SK,
sa.CRT_BY_APP_USER_SK,
sa.RTE_TO_APP_USER_SK,
COALESCE(CD.TRGT_CD,'UNK') SRC_SYS_CD,
sa.CLM_STTUS_CD_SK,
sa.TRNSMSN_SRC_CD_SK,
sa.CLM_STTUS_CHG_RSN_CD_SK,
sa.CLM_STTUS_DT_SK,
sa.CLM_STTUS_DTM,
time(sa.CLM_STTUS_DTM)  CLM_STTUS_TM_OF_DAY
FROM {IDSOwner}.CLM_STTUS_AUDIT sa 
LEFT JOIN {IDSOwner}.CD_MPPNG CD
ON sa.SRC_SYS_CD_SK = CD.CD_MPPNG_SK ,
{IDSOwner}.W_EDW_ETL_DRVR dr
WHERE 
sa.SRC_SYS_CD_SK = dr.SRC_SYS_CD_SK 
AND 
sa.CLM_ID = dr.CLM_ID
"""

df_db2_CLM_STTUS_AUDIT_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_CLM_STTUS_AUDIT_in)
    .load()
)

extract_query_db2_EXCD_in = f"""SELECT 
EXCD_SK,
SRC_SYS_CD_SK,
EXCD_ID 
FROM {IDSOwner}.EXCD
"""

df_db2_EXCD_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_EXCD_in)
    .load()
)

extract_query_db2_CD_MPPNG_Extr = f"""SELECT CD_MPPNG_SK,COALESCE(TRGT_CD,'UNK') TRGT_CD,COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM from {IDSOwner}.CD_MPPNG
"""

df_db2_CD_MPPNG_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_CD_MPPNG_Extr)
    .load()
)

df_Cpy_Mppng_Cd_Ref_Status_Lkp = df_db2_CD_MPPNG_Extr.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_Cpy_Mppng_Cd_Ref_ClmStatusReason_Lkp = df_db2_CD_MPPNG_Extr.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_Cpy_Mppng_Cd_ref_ItsClm_lkp = df_db2_CD_MPPNG_Extr.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_lkp_Codes = (
    df_db2_CLM_STTUS_AUDIT_in.alias("lnk_IdsEdwClmSttusAuditFExtr_InAbc")
    .join(
        df_Cpy_Mppng_Cd_Ref_Status_Lkp.alias("Ref_Status_Lkp"),
        F.col("lnk_IdsEdwClmSttusAuditFExtr_InAbc.CLM_STTUS_CD_SK") == F.col("Ref_Status_Lkp.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_Cpy_Mppng_Cd_Ref_ClmStatusReason_Lkp.alias("Ref_ClmStatusReason_Lkp"),
        F.col("lnk_IdsEdwClmSttusAuditFExtr_InAbc.CLM_STTUS_CHG_RSN_CD_SK") == F.col("Ref_ClmStatusReason_Lkp.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_db2_EXCD_in.alias("lnk_Excd_out"),
        F.col("lnk_IdsEdwClmSttusAuditFExtr_InAbc.CLM_STTUS_CHG_RSN_CD_SK") == F.col("lnk_Excd_out.EXCD_SK"),
        "left"
    )
    .join(
        df_Cpy_Mppng_Cd_ref_ItsClm_lkp.alias("ref_ItsClm_lkp"),
        F.col("lnk_IdsEdwClmSttusAuditFExtr_InAbc.TRNSMSN_SRC_CD_SK") == F.col("ref_ItsClm_lkp.CD_MPPNG_SK"),
        "left"
    )
    .select(
        F.col("lnk_IdsEdwClmSttusAuditFExtr_InAbc.CLM_STTUS_AUDIT_SK").alias("CLM_STTUS_AUDIT_SK"),
        F.col("lnk_IdsEdwClmSttusAuditFExtr_InAbc.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("lnk_IdsEdwClmSttusAuditFExtr_InAbc.CLM_ID").alias("CLM_ID"),
        F.col("lnk_IdsEdwClmSttusAuditFExtr_InAbc.CLM_STTUS_AUDIT_SEQ_NO").alias("CLM_STTUS_AUDIT_SEQ_NO"),
        F.col("lnk_IdsEdwClmSttusAuditFExtr_InAbc.CRT_BY_APP_USER_SK").alias("CRT_BY_APP_USER_SK"),
        F.col("lnk_IdsEdwClmSttusAuditFExtr_InAbc.RTE_TO_APP_USER_SK").alias("RTE_TO_APP_USER_SK"),
        F.col("Ref_Status_Lkp.TRGT_CD").alias("CLM_STTUS_CD"),
        F.col("lnk_IdsEdwClmSttusAuditFExtr_InAbc.CLM_STTUS_DT_SK").alias("CLM_STTUS_AUDIT_DT_SK"),
        F.col("lnk_IdsEdwClmSttusAuditFExtr_InAbc.CLM_STTUS_TM_OF_DAY").alias("CLM_STTUS_AUDIT_TM_OF_DAY"),
        F.col("lnk_IdsEdwClmSttusAuditFExtr_InAbc.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("lnk_IdsEdwClmSttusAuditFExtr_InAbc.CLM_SK").alias("CLM_SK"),
        F.col("lnk_IdsEdwClmSttusAuditFExtr_InAbc.CLM_STTUS_CHG_RSN_CD_SK").alias("CLM_STTUS_CHG_RSN_CD_SK"),
        F.col("lnk_IdsEdwClmSttusAuditFExtr_InAbc.CLM_STTUS_CD_SK").alias("CLM_STTUS_CD_SK"),
        F.col("lnk_IdsEdwClmSttusAuditFExtr_InAbc.CLM_STTUS_DTM").alias("CLM_STTUS_DTM"),
        F.col("lnk_IdsEdwClmSttusAuditFExtr_InAbc.TRNSMSN_SRC_CD_SK").alias("CLM_TRNSMSN_SRC_CD_SK"),
        F.col("Ref_ClmStatusReason_Lkp.TRGT_CD").alias("CLM_STATUS_TRGT_CD"),
        F.col("lnk_Excd_out.EXCD_SK").alias("EXCD_SK"),
        F.col("lnk_Excd_out.EXCD_ID").alias("EXCD_ID"),
        F.col("ref_ItsClm_lkp.TRGT_CD").alias("CLM_TRNSMSN_STTUS_CD"),
        F.col("ref_ItsClm_lkp.TRGT_CD_NM").alias("CLM_TRNSMSN_STTUS_NM")
    )
)

df_xfm_BusinessLogic_stagevar = df_lkp_Codes.withColumn(
    "ClmStatusReasonCode",
    F.when(F.trim(F.col("SRC_SYS_CD")) == "NPS",
           F.when(F.col("EXCD_SK").isNotNull(), F.col("EXCD_ID"))
            .otherwise(F.lit("UNK"))
    ).otherwise(
        F.when(F.col("CLM_STATUS_TRGT_CD").isNotNull(), F.col("CLM_STATUS_TRGT_CD"))
        .otherwise(F.lit("UNK"))
    )
)

df_xfm_BusinessLogic = (
    df_xfm_BusinessLogic_stagevar
    .withColumn(
        "SRC_SYS_CD",
        F.when(
            (F.col("SRC_SYS_CD").isNull()) | (F.trim(F.col("SRC_SYS_CD")) == ""),
            F.lit("UNK")
        ).otherwise(F.col("SRC_SYS_CD"))
    )
    .withColumn(
        "CLM_STTUS_CD",
        F.when(
            (F.col("CLM_STTUS_CD").isNull()) | (F.trim(F.col("CLM_STTUS_CD")) == ""),
            F.lit("UNK")
        ).otherwise(F.col("CLM_STTUS_CD"))
    )
    .withColumn(
        "CLM_TRNSMSN_STTUS_CD",
        F.when(
            (F.col("CLM_TRNSMSN_STTUS_CD").isNull()) | (F.trim(F.col("CLM_TRNSMSN_STTUS_CD")) == ""),
            F.lit("UNK")
        ).otherwise(F.col("CLM_TRNSMSN_STTUS_CD"))
    )
    .withColumn(
        "CLM_TRNSMSN_STTUS_NM",
        F.when(
            (F.col("CLM_TRNSMSN_STTUS_NM").isNull()) | (F.trim(F.col("CLM_TRNSMSN_STTUS_NM")) == ""),
            F.lit("UNK")
        ).otherwise(F.col("CLM_TRNSMSN_STTUS_NM"))
    )
    .select(
        F.col("CLM_STTUS_AUDIT_SK"),
        F.col("SRC_SYS_CD"),
        F.col("CLM_ID"),
        F.col("CLM_STTUS_AUDIT_SEQ_NO"),
        F.lit(CurrRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.lit(CurrRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        F.col("CRT_BY_APP_USER_SK").alias("CRTD_BY_APP_USER_SK"),
        F.col("RTE_TO_APP_USER_SK"),
        F.col("ClmStatusReasonCode").alias("CLM_STTUS_CHG_RSN_CD"),
        F.col("CLM_STTUS_CD"),
        F.col("CLM_STTUS_AUDIT_DT_SK"),
        F.col("CLM_STTUS_AUDIT_TM_OF_DAY").alias("CLM_STTUS_AUDIT_TM_OF_DAY_SK"),
        F.lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("CLM_SK"),
        F.col("CLM_STTUS_CHG_RSN_CD_SK"),
        F.col("CLM_STTUS_CD_SK"),
        F.col("CLM_STTUS_DTM").alias("CLM_STTUS_AUDIT_DTM"),
        F.col("CLM_TRNSMSN_SRC_CD_SK").alias("CLM_TRNSMSN_STTUS_CD_SK"),
        F.col("CLM_TRNSMSN_STTUS_CD"),
        F.col("CLM_TRNSMSN_STTUS_NM")
    )
)

df_lnk_MainData_in = df_xfm_BusinessLogic.filter(
    (F.col("CLM_STTUS_AUDIT_SK") != 0) & (F.col("CLM_STTUS_AUDIT_SK") != 1)
)

df_lnk_UNK_out = spark.createDataFrame(
    [
        (
            0,
            "UNK",
            "UNK",
            0,
            "1753-01-01",
            "1753-01-01",
            0,
            0,
            "UNK",
            "UNK",
            "1753-01-01",
            "00:00:0000",
            100,
            100,
            0,
            0,
            0,
            "1753-01-01 00:00:0000",
            0,
            "UNK",
            "UNK"
        )
    ],
    [
        "CLM_STTUS_AUDIT_SK",
        "SRC_SYS_CD",
        "CLM_ID",
        "CLM_STTUS_AUDIT_SEQ_NO",
        "CRT_RUN_CYC_EXCTN_DT_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        "CRTD_BY_APP_USER_SK",
        "RTE_TO_APP_USER_SK",
        "CLM_STTUS_CHG_RSN_CD",
        "CLM_STTUS_CD",
        "CLM_STTUS_AUDIT_DT_SK",
        "CLM_STTUS_AUDIT_TM_OF_DAY_SK",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "CLM_SK",
        "CLM_STTUS_CHG_RSN_CD_SK",
        "CLM_STTUS_CD_SK",
        "CLM_STTUS_AUDIT_DTM",
        "CLM_TRNSMSN_STTUS_CD_SK",
        "CLM_TRNSMSN_STTUS_CD",
        "CLM_TRNSMSN_STTUS_NM"
    ]
)

df_lnk_NA_out = spark.createDataFrame(
    [
        (
            1,
            "NA",
            "NA",
            0,
            "1753-01-01",
            "1753-01-01",
            1,
            1,
            "NA",
            "NA",
            "1753-01-01",
            "00:00:0000",
            100,
            100,
            1,
            1,
            1,
            "1753-01-01 00:00:0000",
            1,
            "NA",
            "NA"
        )
    ],
    [
        "CLM_STTUS_AUDIT_SK",
        "SRC_SYS_CD",
        "CLM_ID",
        "CLM_STTUS_AUDIT_SEQ_NO",
        "CRT_RUN_CYC_EXCTN_DT_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        "CRTD_BY_APP_USER_SK",
        "RTE_TO_APP_USER_SK",
        "CLM_STTUS_CHG_RSN_CD",
        "CLM_STTUS_CD",
        "CLM_STTUS_AUDIT_DT_SK",
        "CLM_STTUS_AUDIT_TM_OF_DAY_SK",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "CLM_SK",
        "CLM_STTUS_CHG_RSN_CD_SK",
        "CLM_STTUS_CD_SK",
        "CLM_STTUS_AUDIT_DTM",
        "CLM_TRNSMSN_STTUS_CD_SK",
        "CLM_TRNSMSN_STTUS_CD",
        "CLM_TRNSMSN_STTUS_NM"
    ]
)

df_fnl_Cocc_F = (
    df_lnk_UNK_out.unionByName(df_lnk_NA_out)
    .unionByName(df_lnk_MainData_in)
)

df_fnl_Cocc_F_rpad = (
    df_fnl_Cocc_F
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
    .withColumn("CLM_STTUS_AUDIT_DT_SK", F.rpad(F.col("CLM_STTUS_AUDIT_DT_SK"), 10, " "))
    .withColumn("CLM_STTUS_AUDIT_TM_OF_DAY_SK", F.rpad(F.col("CLM_STTUS_AUDIT_TM_OF_DAY_SK"), 10, " "))
)

df_final_select = df_fnl_Cocc_F_rpad.select(
    "CLM_STTUS_AUDIT_SK",
    "SRC_SYS_CD",
    "CLM_ID",
    "CLM_STTUS_AUDIT_SEQ_NO",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "CRTD_BY_APP_USER_SK",
    "RTE_TO_APP_USER_SK",
    "CLM_STTUS_CHG_RSN_CD",
    "CLM_STTUS_CD",
    "CLM_STTUS_AUDIT_DT_SK",
    "CLM_STTUS_AUDIT_TM_OF_DAY_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "CLM_SK",
    "CLM_STTUS_CHG_RSN_CD_SK",
    "CLM_STTUS_CD_SK",
    "CLM_STTUS_AUDIT_DTM",
    "CLM_TRNSMSN_STTUS_CD_SK",
    "CLM_TRNSMSN_STTUS_CD",
    "CLM_TRNSMSN_STTUS_NM"
)

write_files(
    df_final_select,
    f"{adls_path}/load/CLM_STTUS_AUDIT_F.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)