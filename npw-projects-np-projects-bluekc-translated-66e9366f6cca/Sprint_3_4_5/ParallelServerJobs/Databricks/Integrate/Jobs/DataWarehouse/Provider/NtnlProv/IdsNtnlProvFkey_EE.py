# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC 
# MAGIC Ravi Abburi            2017-11-30              5781                             Original Programming                                                                              IntegrateDev2        Kalyan Neelam              2018-01-30

# MAGIC IdsNtnlProvFkey_EE
# MAGIC FKEY failures are written into this flat file.
# MAGIC This is a load ready file that will go into Load job
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


SrcSysCd = get_widget_value('SrcSysCd','CMS')
IDSRunCycle = get_widget_value('IDSRunCycle','100')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
RunID = get_widget_value('RunID','100')
PrefixFkeyFailedFileName = get_widget_value('PrefixFkeyFailedFileName','FKeysFailure.IdsNtnlProvTxnmyCntl')

schema_seq_NTNL_PROV_Pkey = StructType([
    StructField("NTNL_PROV_SK", IntegerType(), True),
    StructField("NTNL_PROV_ID", StringType(), True),
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("ENTITY_TYPE_CODE", StringType(), True),
    StructField("PROVIDER_GENDER_CODE", StringType(), True),
    StructField("IS_SOLE_PROPRIETOR", StringType(), True),
    StructField("NPI_DEACTIVATION_REASON_CODE", StringType(), True),
    StructField("NTNL_PROV_DCTVTN_DT", StringType(), True),
    StructField("NTNL_PROV_ENMRTN_DT", StringType(), True),
    StructField("NTNL_PROV_RCTVTN_DT", StringType(), True),
    StructField("NTNL_PROV_SRC_LAST_UPDT_DT", StringType(), True),
    StructField("NTNL_PROV_CRDTL_TX", StringType(), True),
    StructField("NTNL_PROV_FIRST_NM", StringType(), True),
    StructField("NTNL_PROV_LAST_NM", StringType(), True),
    StructField("NTNL_PROV_MID_NM", StringType(), True),
    StructField("NTNL_PROV_NM_PFX_TX", StringType(), True),
    StructField("NTNL_PROV_NM_SFX_TX", StringType(), True),
    StructField("NTNL_PROV_OTHR_CRDTL_TX", StringType(), True),
    StructField("NTNL_PROV_OTHR_FIRST_NM", StringType(), True),
    StructField("NTNL_PROV_OTHR_LAST_NM", StringType(), True),
    StructField("NTNL_PROV_OTHR_MID_NM", StringType(), True),
    StructField("NTNL_PROV_OTHR_NM_PFX_TX", StringType(), True),
    StructField("NTNL_PROV_OTHR_NM_SFX_TX", StringType(), True),
    StructField("PROV_OTHER_LAST_NAME_TYPE_CODE", StringType(), True),
    StructField("NTNL_PROV_ORG_LGL_BUS_NM", StringType(), True),
    StructField("NTNL_PROV_OTHR_ORG_NM", StringType(), True),
    StructField("PROV_OTHER_ORGANIZATION_NAME_TYPE_CODE", StringType(), True),
    StructField("IS_ORGANIZATION_SUBPART", StringType(), True),
    StructField("NTNL_PROV_ORG_PRNT_LGL_BUS_NM", StringType(), True),
    StructField("NTNL_PROV_ORG_PRNT_TAX_ID", StringType(), True),
    StructField("NTNL_PROV_MAIL_ADDR_LN_1", StringType(), True),
    StructField("NTNL_PROV_MAIL_ADDR_LN_2", StringType(), True),
    StructField("NTNL_PROV_MAIL_ADDR_CITY_NM", StringType(), True),
    StructField("PROV_BUS_MAILING_ADDR_STATE_NAME", StringType(), True),
    StructField("NTNL_PROV_MAIL_ADDR_POSTAL_CD", StringType(), True),
    StructField("PROV_BUS_MAILING_ADDR_COUNTRY_CODE", StringType(), True),
    StructField("NTNL_PROV_MAIL_ADDR_FAX_NO", StringType(), True),
    StructField("NTNL_PROV_MAIL_ADDR_TEL_NO", StringType(), True),
    StructField("NTNL_PROV_PRCTC_PRI_LOC_ADDR_LN_1", StringType(), True),
    StructField("NTNL_PROV_PRCTC_PRI_LOC_ADDR_LN_2", StringType(), True),
    StructField("NTNL_PROV_PRCTC_PRI_LOC_ADDR_CITY_NM", StringType(), True),
    StructField("PROV_BUS_PRACTICE_LOC_ADDR_STATE_NAME", StringType(), True),
    StructField("NTNL_PROV_PRCTC_PRI_LOC_ADDR_POSTAL_CD", StringType(), True),
    StructField("PROV_BUS_PRACTICE_LOC_ADDR_COUNTRY_CODE", StringType(), True),
    StructField("NTNL_PROV_PRCTC_PRI_LOC_ADDR_FAX_NO", StringType(), True),
    StructField("NTNL_PROV_PRCTC_PRI_LOC_ADDR_TEL_NO", StringType(), True),
    StructField("NTNL_PROV_ORG_EMPLR_ID_NO", StringType(), True),
    StructField("RPLMT_NTNL_PROV_ID", StringType(), True)
])

df_seq_NTNL_PROV_Pkey = (
    spark.read.option("sep", ",")
    .option("quote", "^")
    .option("header", False)
    .option("nullValue", None)
    .schema(schema_seq_NTNL_PROV_Pkey)
    .csv(f"{adls_path}/key/NTNL_PROV.{SrcSysCd}.pkey.{RunID}.dat")
)

df_seq_NTNL_PROV_Pkey = df_seq_NTNL_PROV_Pkey.withColumn(
    "lnk_IdsDmClmDmClmLnProcCdModExtr_InABC_CLM_LN_PROC_CD_MOD_ORDNL_CD_SK",
    F.lit(None).cast(IntegerType())
)

df_ds_CD_MPPNG_Data = spark.read.parquet(f"{adls_path}/ds/CD_MPPNG.parquet")

df_EntyTypCd_In = (
    df_ds_CD_MPPNG_Data.filter(
        "SRC_SYS_CD='CMS' and SRC_CLCTN_CD='CMS' and SRC_DOMAIN_NM='NATIONAL PROVIDER ENTITY' and TRGT_CLCTN_CD='IDS' and TRGT_DOMAIN_NM='NATIONAL PROVIDER ENTITY'"
    )
    .select(
        F.col("SRC_CD").alias("ENTITY_TYPE_CODE"),
        F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK")
    )
)

df_GndrCd_In = (
    df_ds_CD_MPPNG_Data.filter(
        "SRC_SYS_CD='FACETS' and SRC_CLCTN_CD='FACETS DBO' and SRC_DOMAIN_NM='GENDER' and TRGT_CLCTN_CD='IDS' and TRGT_DOMAIN_NM='GENDER'"
    )
    .select(
        F.col("SRC_CD").alias("PROVIDER_GENDER_CODE"),
        F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK")
    )
)

df_IsSoleProptr_In = (
    df_ds_CD_MPPNG_Data.filter(
        "SRC_SYS_CD='CMS' and SRC_CLCTN_CD='CMS' and SRC_DOMAIN_NM='PROVIDER SOLE PROPRIETOR' and TRGT_CLCTN_CD='IDS' and TRGT_DOMAIN_NM='PROVIDER SOLE PROPRIETOR'"
    )
    .select(
        F.col("SRC_CD").alias("IS_SOLE_PROPRIETOR"),
        F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK")
    )
)

df_DactRsnCd_In = (
    df_ds_CD_MPPNG_Data.filter(
        "SRC_SYS_CD='CMS' and SRC_CLCTN_CD='CMS' and SRC_DOMAIN_NM='DEACTIVATION REASON' and TRGT_CLCTN_CD='IDS' and TRGT_DOMAIN_NM='DEACTIVATION REASON'"
    )
    .select(
        F.col("SRC_CD").alias("NPI_DEACTIVATION_REASON_CODE"),
        F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK")
    )
)

df_LstNmTypCd_In = (
    df_ds_CD_MPPNG_Data.filter(
        "SRC_SYS_CD='CMS' and SRC_CLCTN_CD='CMS' and SRC_DOMAIN_NM='PROVIDER ORGINAZATOIN TYPE' and TRGT_CLCTN_CD='IDS' and TRGT_DOMAIN_NM='PROVIDER ORGINAZATOIN TYPE'"
    )
    .select(
        F.col("SRC_CD").alias("PROV_OTHER_LAST_NAME_TYPE_CODE"),
        F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK")
    )
)

df_OrgSubPrt_In = (
    df_ds_CD_MPPNG_Data.filter(
        "SRC_SYS_CD='CMS' and SRC_CLCTN_CD='CMS' and SRC_DOMAIN_NM='PROVIDER ORGINAZATION SUBPART' and TRGT_CLCTN_CD='IDS' and TRGT_DOMAIN_NM='PROVIDER ORGINAZATION SUBPART'"
    )
    .select(
        F.col("SRC_CD").alias("IS_ORGANIZATION_SUBPART"),
        F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK")
    )
)

df_StCd_In = (
    df_ds_CD_MPPNG_Data.filter(
        "SRC_SYS_CD='IDS' and SRC_CLCTN_CD='IDS' and SRC_DOMAIN_NM='STATE' and TRGT_CLCTN_CD='IDS' and TRGT_DOMAIN_NM='STATE'"
    )
    .select(
        F.col("SRC_CD").alias("PROV_BUS_MAILING_ADDR_STATE_NAME"),
        F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK")
    )
)

df_CntryCd_In = (
    df_ds_CD_MPPNG_Data.filter(
        "SRC_SYS_CD='FACETS' and SRC_CLCTN_CD='FACETS DBO' and SRC_DOMAIN_NM='COUNTRY' and TRGT_CLCTN_CD='IDS' and TRGT_DOMAIN_NM='COUNTRY'"
    )
    .select(
        F.col("SRC_CD").alias("PROV_BUS_MAILING_ADDR_COUNTRY_CODE"),
        F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK")
    )
)

df_LcStCd_In = (
    df_ds_CD_MPPNG_Data.filter(
        "SRC_SYS_CD='IDS' and SRC_CLCTN_CD='IDS' and SRC_DOMAIN_NM='STATE' and TRGT_CLCTN_CD='IDS' and TRGT_DOMAIN_NM='STATE'"
    )
    .select(
        F.col("SRC_CD").alias("PROV_BUS_PRACTICE_LOC_ADDR_STATE_NAME"),
        F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK")
    )
)

df_LcCntryCd_In = (
    df_ds_CD_MPPNG_Data.filter(
        "SRC_SYS_CD='FACETS' and SRC_CLCTN_CD='FACETS DBO' and SRC_DOMAIN_NM='COUNTRY' and TRGT_CLCTN_CD='IDS' and TRGT_DOMAIN_NM='COUNTRY'"
    )
    .select(
        F.col("SRC_CD").alias("PROV_BUS_PRACTICE_LOC_ADDR_COUNTRY_CODE"),
        F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK")
    )
)

df_OtrOrgNmTypCd_In = (
    df_ds_CD_MPPNG_Data.filter(
        "SRC_SYS_CD='CMS' and SRC_CLCTN_CD='CMS' and SRC_DOMAIN_NM='PROVIDER ORGINAZATOIN TYPE' and TRGT_CLCTN_CD='IDS' and TRGT_DOMAIN_NM='PROVIDER ORGINAZATOIN TYPE'"
    )
    .select(
        F.col("SRC_CD").alias("PROV_OTHER_ORGANIZATION_NAME_TYPE_CODE"),
        F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK")
    )
)

df_Lkup_Fkey = (
    df_seq_NTNL_PROV_Pkey.alias("Lnk_NtnlProvPkey_Out")
    .join(
        df_EntyTypCd_In.alias("EntyTypCd_In"),
        [
            F.col("Lnk_NtnlProvPkey_Out.ENTITY_TYPE_CODE") == F.col("EntyTypCd_In.ENTITY_TYPE_CODE"),
            F.col("Lnk_NtnlProvPkey_Out.lnk_IdsDmClmDmClmLnProcCdModExtr_InABC_CLM_LN_PROC_CD_MOD_ORDNL_CD_SK")
            == F.col("EntyTypCd_In.CD_MPPNG_SK")
        ],
        "left"
    )
    .join(
        df_IsSoleProptr_In.alias("IsSoleProptr_In"),
        [
            F.col("Lnk_NtnlProvPkey_Out.IS_SOLE_PROPRIETOR") == F.col("IsSoleProptr_In.IS_SOLE_PROPRIETOR"),
            F.col("Lnk_NtnlProvPkey_Out.lnk_IdsDmClmDmClmLnProcCdModExtr_InABC_CLM_LN_PROC_CD_MOD_ORDNL_CD_SK")
            == F.col("IsSoleProptr_In.CD_MPPNG_SK")
        ],
        "left"
    )
    .join(
        df_GndrCd_In.alias("GndrCd_In"),
        [
            F.col("Lnk_NtnlProvPkey_Out.PROVIDER_GENDER_CODE") == F.col("GndrCd_In.PROVIDER_GENDER_CODE"),
            F.col("Lnk_NtnlProvPkey_Out.lnk_IdsDmClmDmClmLnProcCdModExtr_InABC_CLM_LN_PROC_CD_MOD_ORDNL_CD_SK")
            == F.col("GndrCd_In.CD_MPPNG_SK")
        ],
        "left"
    )
    .join(
        df_DactRsnCd_In.alias("DactRsnCd_In"),
        [
            F.col("Lnk_NtnlProvPkey_Out.NPI_DEACTIVATION_REASON_CODE") == F.col("DactRsnCd_In.NPI_DEACTIVATION_REASON_CODE"),
            F.col("Lnk_NtnlProvPkey_Out.lnk_IdsDmClmDmClmLnProcCdModExtr_InABC_CLM_LN_PROC_CD_MOD_ORDNL_CD_SK")
            == F.col("DactRsnCd_In.CD_MPPNG_SK")
        ],
        "left"
    )
    .join(
        df_LstNmTypCd_In.alias("LstNmTypCd_In"),
        [
            F.col("Lnk_NtnlProvPkey_Out.PROV_OTHER_LAST_NAME_TYPE_CODE") == F.col("LstNmTypCd_In.PROV_OTHER_LAST_NAME_TYPE_CODE"),
            F.col("Lnk_NtnlProvPkey_Out.lnk_IdsDmClmDmClmLnProcCdModExtr_InABC_CLM_LN_PROC_CD_MOD_ORDNL_CD_SK")
            == F.col("LstNmTypCd_In.CD_MPPNG_SK")
        ],
        "left"
    )
    .join(
        df_OrgSubPrt_In.alias("OrgSubPrt_In"),
        [
            F.col("Lnk_NtnlProvPkey_Out.IS_ORGANIZATION_SUBPART") == F.col("OrgSubPrt_In.IS_ORGANIZATION_SUBPART"),
            F.col("Lnk_NtnlProvPkey_Out.lnk_IdsDmClmDmClmLnProcCdModExtr_InABC_CLM_LN_PROC_CD_MOD_ORDNL_CD_SK")
            == F.col("OrgSubPrt_In.CD_MPPNG_SK")
        ],
        "left"
    )
    .join(
        df_StCd_In.alias("StCd_In"),
        [
            F.col("Lnk_NtnlProvPkey_Out.PROV_BUS_MAILING_ADDR_STATE_NAME") == F.col("StCd_In.PROV_BUS_MAILING_ADDR_STATE_NAME"),
            F.col("Lnk_NtnlProvPkey_Out.lnk_IdsDmClmDmClmLnProcCdModExtr_InABC_CLM_LN_PROC_CD_MOD_ORDNL_CD_SK")
            == F.col("StCd_In.CD_MPPNG_SK")
        ],
        "left"
    )
    .join(
        df_CntryCd_In.alias("CntryCd_In"),
        [
            F.col("Lnk_NtnlProvPkey_Out.PROV_BUS_MAILING_ADDR_COUNTRY_CODE") == F.col("CntryCd_In.PROV_BUS_MAILING_ADDR_COUNTRY_CODE"),
            F.col("Lnk_NtnlProvPkey_Out.lnk_IdsDmClmDmClmLnProcCdModExtr_InABC_CLM_LN_PROC_CD_MOD_ORDNL_CD_SK")
            == F.col("CntryCd_In.CD_MPPNG_SK")
        ],
        "left"
    )
    .join(
        df_LcStCd_In.alias("LcStCd_In"),
        [
            F.col("Lnk_NtnlProvPkey_Out.PROV_BUS_PRACTICE_LOC_ADDR_STATE_NAME") == F.col("LcStCd_In.PROV_BUS_PRACTICE_LOC_ADDR_STATE_NAME"),
            F.col("Lnk_NtnlProvPkey_Out.lnk_IdsDmClmDmClmLnProcCdModExtr_InABC_CLM_LN_PROC_CD_MOD_ORDNL_CD_SK")
            == F.col("LcStCd_In.CD_MPPNG_SK")
        ],
        "left"
    )
    .join(
        df_LcCntryCd_In.alias("LcCntryCd_In"),
        [
            F.col("Lnk_NtnlProvPkey_Out.PROV_BUS_PRACTICE_LOC_ADDR_COUNTRY_CODE")
            == F.col("LcCntryCd_In.PROV_BUS_PRACTICE_LOC_ADDR_COUNTRY_CODE"),
            F.col("Lnk_NtnlProvPkey_Out.lnk_IdsDmClmDmClmLnProcCdModExtr_InABC_CLM_LN_PROC_CD_MOD_ORDNL_CD_SK")
            == F.col("LcCntryCd_In.CD_MPPNG_SK")
        ],
        "left"
    )
    .join(
        df_OtrOrgNmTypCd_In.alias("OtrOrgNmTypCd_In"),
        [
            F.col("Lnk_NtnlProvPkey_Out.PROV_OTHER_ORGANIZATION_NAME_TYPE_CODE")
            == F.col("OtrOrgNmTypCd_In.PROV_OTHER_ORGANIZATION_NAME_TYPE_CODE"),
            F.col("Lnk_NtnlProvPkey_Out.lnk_IdsDmClmDmClmLnProcCdModExtr_InABC_CLM_LN_PROC_CD_MOD_ORDNL_CD_SK")
            == F.col("OtrOrgNmTypCd_In.CD_MPPNG_SK")
        ],
        "left"
    )
)

df_OtrOrgNmTypCdSk = df_Lkup_Fkey.select(
    F.col("Lnk_NtnlProvPkey_Out.NTNL_PROV_SK").alias("NTNL_PROV_SK"),
    F.col("Lnk_NtnlProvPkey_Out.NTNL_PROV_ID").alias("NTNL_PROV_ID"),
    F.col("Lnk_NtnlProvPkey_Out.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Lnk_NtnlProvPkey_Out.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("Lnk_NtnlProvPkey_Out.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("Lnk_NtnlProvPkey_Out.ENTITY_TYPE_CODE").alias("ENTITY_TYPE_CODE"),
    F.col("Lnk_NtnlProvPkey_Out.PROVIDER_GENDER_CODE").alias("PROVIDER_GENDER_CODE"),
    F.col("Lnk_NtnlProvPkey_Out.IS_SOLE_PROPRIETOR").alias("IS_SOLE_PROPRIETOR"),
    F.col("Lnk_NtnlProvPkey_Out.NPI_DEACTIVATION_REASON_CODE").alias("NPI_DEACTIVATION_REASON_CODE"),
    F.col("Lnk_NtnlProvPkey_Out.NTNL_PROV_DCTVTN_DT").alias("NTNL_PROV_DCTVTN_DT"),
    F.col("Lnk_NtnlProvPkey_Out.NTNL_PROV_ENMRTN_DT").alias("NTNL_PROV_ENMRTN_DT"),
    F.col("Lnk_NtnlProvPkey_Out.NTNL_PROV_RCTVTN_DT").alias("NTNL_PROV_RCTVTN_DT"),
    F.col("Lnk_NtnlProvPkey_Out.NTNL_PROV_SRC_LAST_UPDT_DT").alias("NTNL_PROV_SRC_LAST_UPDT_DT"),
    F.col("Lnk_NtnlProvPkey_Out.NTNL_PROV_CRDTL_TX").alias("NTNL_PROV_CRDTL_TX"),
    F.col("Lnk_NtnlProvPkey_Out.NTNL_PROV_FIRST_NM").alias("NTNL_PROV_FIRST_NM"),
    F.col("Lnk_NtnlProvPkey_Out.NTNL_PROV_LAST_NM").alias("NTNL_PROV_LAST_NM"),
    F.col("Lnk_NtnlProvPkey_Out.NTNL_PROV_MID_NM").alias("NTNL_PROV_MID_NM"),
    F.col("Lnk_NtnlProvPkey_Out.NTNL_PROV_NM_PFX_TX").alias("NTNL_PROV_NM_PFX_TX"),
    F.col("Lnk_NtnlProvPkey_Out.NTNL_PROV_NM_SFX_TX").alias("NTNL_PROV_NM_SFX_TX"),
    F.col("Lnk_NtnlProvPkey_Out.NTNL_PROV_OTHR_CRDTL_TX").alias("NTNL_PROV_OTHR_CRDTL_TX"),
    F.col("Lnk_NtnlProvPkey_Out.NTNL_PROV_OTHR_FIRST_NM").alias("NTNL_PROV_OTHR_FIRST_NM"),
    F.col("Lnk_NtnlProvPkey_Out.NTNL_PROV_OTHR_LAST_NM").alias("NTNL_PROV_OTHR_LAST_NM"),
    F.col("Lnk_NtnlProvPkey_Out.NTNL_PROV_OTHR_MID_NM").alias("NTNL_PROV_OTHR_MID_NM"),
    F.col("Lnk_NtnlProvPkey_Out.NTNL_PROV_OTHR_NM_PFX_TX").alias("NTNL_PROV_OTHR_NM_PFX_TX"),
    F.col("Lnk_NtnlProvPkey_Out.NTNL_PROV_OTHR_NM_SFX_TX").alias("NTNL_PROV_OTHR_NM_SFX_TX"),
    F.col("Lnk_NtnlProvPkey_Out.PROV_OTHER_LAST_NAME_TYPE_CODE").alias("PROV_OTHER_LAST_NAME_TYPE_CODE"),
    F.col("Lnk_NtnlProvPkey_Out.NTNL_PROV_ORG_LGL_BUS_NM").alias("NTNL_PROV_ORG_LGL_BUS_NM"),
    F.col("Lnk_NtnlProvPkey_Out.NTNL_PROV_OTHR_ORG_NM").alias("NTNL_PROV_OTHR_ORG_NM"),
    F.col("Lnk_NtnlProvPkey_Out.PROV_OTHER_ORGANIZATION_NAME_TYPE_CODE").alias("PROV_OTHER_ORGANIZATION_NAME_TYPE_CODE"),
    F.col("Lnk_NtnlProvPkey_Out.IS_ORGANIZATION_SUBPART").alias("IS_ORGANIZATION_SUBPART"),
    F.col("Lnk_NtnlProvPkey_Out.NTNL_PROV_ORG_PRNT_LGL_BUS_NM").alias("NTNL_PROV_ORG_PRNT_LGL_BUS_NM"),
    F.col("Lnk_NtnlProvPkey_Out.NTNL_PROV_ORG_PRNT_TAX_ID").alias("NTNL_PROV_ORG_PRNT_TAX_ID"),
    F.col("Lnk_NtnlProvPkey_Out.NTNL_PROV_MAIL_ADDR_LN_1").alias("NTNL_PROV_MAIL_ADDR_LN_1"),
    F.col("Lnk_NtnlProvPkey_Out.NTNL_PROV_MAIL_ADDR_LN_2").alias("NTNL_PROV_MAIL_ADDR_LN_2"),
    F.col("Lnk_NtnlProvPkey_Out.NTNL_PROV_MAIL_ADDR_CITY_NM").alias("NTNL_PROV_MAIL_ADDR_CITY_NM"),
    F.col("Lnk_NtnlProvPkey_Out.PROV_BUS_MAILING_ADDR_STATE_NAME").alias("PROV_BUS_MAILING_ADDR_STATE_NAME"),
    F.col("Lnk_NtnlProvPkey_Out.NTNL_PROV_MAIL_ADDR_POSTAL_CD").alias("NTNL_PROV_MAIL_ADDR_POSTAL_CD"),
    F.col("Lnk_NtnlProvPkey_Out.PROV_BUS_MAILING_ADDR_COUNTRY_CODE").alias("PROV_BUS_MAILING_ADDR_COUNTRY_CODE"),
    F.col("Lnk_NtnlProvPkey_Out.NTNL_PROV_MAIL_ADDR_FAX_NO").alias("NTNL_PROV_MAIL_ADDR_FAX_NO"),
    F.col("Lnk_NtnlProvPkey_Out.NTNL_PROV_MAIL_ADDR_TEL_NO").alias("NTNL_PROV_MAIL_ADDR_TEL_NO"),
    F.col("Lnk_NtnlProvPkey_Out.NTNL_PROV_PRCTC_PRI_LOC_ADDR_LN_1").alias("NTNL_PROV_PRCTC_PRI_LOC_ADDR_LN_1"),
    F.col("Lnk_NtnlProvPkey_Out.NTNL_PROV_PRCTC_PRI_LOC_ADDR_LN_2").alias("NTNL_PROV_PRCTC_PRI_LOC_ADDR_LN_2"),
    F.col("Lnk_NtnlProvPkey_Out.NTNL_PROV_PRCTC_PRI_LOC_ADDR_CITY_NM").alias("NTNL_PROV_PRCTC_PRI_LOC_ADDR_CITY_NM"),
    F.col("Lnk_NtnlProvPkey_Out.PROV_BUS_PRACTICE_LOC_ADDR_STATE_NAME").alias("PROV_BUS_PRACTICE_LOC_ADDR_STATE_NAME"),
    F.col("Lnk_NtnlProvPkey_Out.NTNL_PROV_PRCTC_PRI_LOC_ADDR_POSTAL_CD").alias("NTNL_PROV_PRCTC_PRI_LOC_ADDR_POSTAL_CD"),
    F.col("Lnk_NtnlProvPkey_Out.PROV_BUS_PRACTICE_LOC_ADDR_COUNTRY_CODE").alias("PROV_BUS_PRACTICE_LOC_ADDR_COUNTRY_CODE"),
    F.col("Lnk_NtnlProvPkey_Out.NTNL_PROV_PRCTC_PRI_LOC_ADDR_FAX_NO").alias("NTNL_PROV_PRCTC_PRI_LOC_ADDR_FAX_NO"),
    F.col("Lnk_NtnlProvPkey_Out.NTNL_PROV_PRCTC_PRI_LOC_ADDR_TEL_NO").alias("NTNL_PROV_PRCTC_PRI_LOC_ADDR_TEL_NO"),
    F.col("Lnk_NtnlProvPkey_Out.NTNL_PROV_ORG_EMPLR_ID_NO").alias("NTNL_PROV_ORG_EMPLR_ID_NO"),
    F.col("Lnk_NtnlProvPkey_Out.RPLMT_NTNL_PROV_ID").alias("RPLMT_NTNL_PROV_ID"),
    F.col("EntyTypCd_In.CD_MPPNG_SK").alias("NTNL_PROV_ENTY_TYP_CD_SK"),
    F.col("GndrCd_In.CD_MPPNG_SK").alias("NTNL_PROV_GNDR_CD_SK"),
    F.col("IsSoleProptr_In.CD_MPPNG_SK").alias("NTNL_PROV_SOLE_PRPRTR_CD_SK"),
    F.col("DactRsnCd_In.CD_MPPNG_SK").alias("NTNL_PROV_DCTVTN_RSN_CD_SK"),
    F.col("LstNmTypCd_In.CD_MPPNG_SK").alias("NTNL_PROV_OTHR_LAST_NM_TYP_CD_SK"),
    F.col("OtrOrgNmTypCd_In.CD_MPPNG_SK").alias("NTNL_PROV_OTHR_ORG_NM_TYP_CD_SK"),
    F.col("OrgSubPrt_In.CD_MPPNG_SK").alias("NTNL_PROV_ORG_SUBPART_CD_SK"),
    F.col("StCd_In.CD_MPPNG_SK").alias("NTNL_PROV_MAIL_ADDR_ST_CD_SK"),
    F.col("CntryCd_In.CD_MPPNG_SK").alias("NTNL_PROV_MAIL_ADDR_CTRY_CD_SK"),
    F.col("LcStCd_In.CD_MPPNG_SK").alias("NTNL_PROV_PRCTC_PRI_LOC_ADDR_ST_CD_SK"),
    F.col("LcCntryCd_In.CD_MPPNG_SK").alias("NTNL_PROV_PRCTC_PRI_LOC_ADDR_CTRY_CD_SK")
)

df_xfm_vars = df_OtrOrgNmTypCdSk.withColumn(
    "svEntyTypCdSkLkupCheck",
    F.when(
        (F.col("NTNL_PROV_ENTY_TYP_CD_SK").isNull())
        & (F.col("NTNL_PROV_ENTY_TYP_CD_SK") != F.lit("NA")),
        "Y"
    ).otherwise("N")
).withColumn(
    "svIsSoleProptrSkLkupCheck",
    F.when(
        (F.col("NTNL_PROV_SOLE_PRPRTR_CD_SK").isNull())
        & (F.col("NTNL_PROV_SOLE_PRPRTR_CD_SK") != F.lit("NA")),
        "Y"
    ).otherwise("N")
).withColumn(
    "svGndrCdSkLkupCheck",
    F.when(
        (F.col("NTNL_PROV_GNDR_CD_SK").isNull())
        & (F.col("NTNL_PROV_GNDR_CD_SK") != F.lit("NA")),
        "Y"
    ).otherwise("N")
).withColumn(
    "svDectRsnCdSkLkupCheck",
    F.when(
        (F.col("NTNL_PROV_DCTVTN_RSN_CD_SK").isNull())
        & (F.col("NTNL_PROV_DCTVTN_RSN_CD_SK") != F.lit("NA")),
        "Y"
    ).otherwise("N")
).withColumn(
    "svLstNmTypCdSkLkupCheck",
    F.when(
        (F.col("NTNL_PROV_OTHR_LAST_NM_TYP_CD_SK").isNull())
        & (F.col("NTNL_PROV_OTHR_LAST_NM_TYP_CD_SK") != F.lit("NA")),
        "Y"
    ).otherwise("N")
).withColumn(
    "svOrgSubPrtSkLkupCheck",
    F.when(
        (F.col("NTNL_PROV_ORG_SUBPART_CD_SK").isNull())
        & (F.col("NTNL_PROV_ORG_SUBPART_CD_SK") != F.lit("NA")),
        "Y"
    ).otherwise("N")
).withColumn(
    "svStCdSkLkupCheck",
    F.when(
        (F.col("NTNL_PROV_MAIL_ADDR_ST_CD_SK").isNull())
        & (F.col("NTNL_PROV_MAIL_ADDR_ST_CD_SK") != F.lit("NA")),
        "Y"
    ).otherwise("N")
).withColumn(
    "svCntryCdSkLkupCheck",
    F.when(
        (F.col("NTNL_PROV_MAIL_ADDR_CTRY_CD_SK").isNull())
        & (F.col("NTNL_PROV_MAIL_ADDR_CTRY_CD_SK") != F.lit("NA")),
        "Y"
    ).otherwise("N")
).withColumn(
    "svLcStCdSkLkupCheck",
    F.when(
        (F.col("NTNL_PROV_PRCTC_PRI_LOC_ADDR_ST_CD_SK").isNull())
        & (F.col("NTNL_PROV_PRCTC_PRI_LOC_ADDR_ST_CD_SK") != F.lit("NA")),
        "Y"
    ).otherwise("N")
).withColumn(
    "svLcCntryCdSkLkupCheck",
    F.when(
        (F.col("NTNL_PROV_PRCTC_PRI_LOC_ADDR_CTRY_CD_SK").isNull())
        & (F.col("NTNL_PROV_PRCTC_PRI_LOC_ADDR_CTRY_CD_SK") != F.lit("NA")),
        "Y"
    ).otherwise("N")
).withColumn(
    "svOtrOrgNmTypCdSkLkupCheck",
    F.when(
        (F.col("NTNL_PROV_OTHR_ORG_NM_TYP_CD_SK").isNull())
        & (F.col("NTNL_PROV_OTHR_ORG_NM_TYP_CD_SK") != F.lit("NA")),
        "Y"
    ).otherwise("N")
)

window_row = F.row_number().over(
    (Window.orderBy(F.monotonically_increasing_id()))
)

df_with_rownum = df_xfm_vars.withColumn("ds_rownum", window_row)

df_Lnk_Main = df_with_rownum.select(
    F.when(
        F.col("NTNL_PROV_ENTY_TYP_CD_SK").isNull(), F.lit(0)
    ).otherwise(F.col("NTNL_PROV_ENTY_TYP_CD_SK")).alias("NTNL_PROV_ENTY_TYP_CD_SK"),
    F.when(
        F.col("NTNL_PROV_GNDR_CD_SK").isNull(), F.lit(0)
    ).otherwise(F.col("NTNL_PROV_GNDR_CD_SK")).alias("NTNL_PROV_GNDR_CD_SK"),
    F.when(
        F.col("NTNL_PROV_SOLE_PRPRTR_CD_SK").isNull(), F.lit(0)
    ).otherwise(F.col("NTNL_PROV_SOLE_PRPRTR_CD_SK")).alias("NTNL_PROV_SOLE_PRPRTR_CD_SK"),
    F.when(
        F.col("NTNL_PROV_DCTVTN_RSN_CD_SK").isNull(), F.lit(0)
    ).otherwise(F.col("NTNL_PROV_DCTVTN_RSN_CD_SK")).alias("NTNL_PROV_DCTVTN_RSN_CD_SK"),
    F.col("NTNL_PROV_DCTVTN_DT").alias("NTNL_PROV_DCTVTN_DT"),
    F.col("NTNL_PROV_ENMRTN_DT").alias("NTNL_PROV_ENMRTN_DT"),
    F.col("NTNL_PROV_RCTVTN_DT").alias("NTNL_PROV_RCTVTN_DT"),
    F.col("NTNL_PROV_SRC_LAST_UPDT_DT").alias("NTNL_PROV_SRC_LAST_UPDT_DT"),
    F.col("NTNL_PROV_CRDTL_TX").alias("NTNL_PROV_CRDTL_TX"),
    F.col("NTNL_PROV_FIRST_NM").alias("NTNL_PROV_FIRST_NM"),
    F.col("NTNL_PROV_LAST_NM").alias("NTNL_PROV_LAST_NM"),
    F.col("NTNL_PROV_MID_NM").alias("NTNL_PROV_MID_NM"),
    F.col("NTNL_PROV_NM_PFX_TX").alias("NTNL_PROV_NM_PFX_TX"),
    F.col("NTNL_PROV_NM_SFX_TX").alias("NTNL_PROV_NM_SFX_TX"),
    F.col("NTNL_PROV_OTHR_CRDTL_TX").alias("NTNL_PROV_OTHR_CRDTL_TX"),
    F.col("NTNL_PROV_OTHR_FIRST_NM").alias("NTNL_PROV_OTHR_FIRST_NM"),
    F.col("NTNL_PROV_OTHR_LAST_NM").alias("NTNL_PROV_OTHR_LAST_NM"),
    F.col("NTNL_PROV_OTHR_MID_NM").alias("NTNL_PROV_OTHR_MID_NM"),
    F.col("NTNL_PROV_OTHR_NM_PFX_TX").alias("NTNL_PROV_OTHR_NM_PFX_TX"),
    F.col("NTNL_PROV_OTHR_NM_SFX_TX").alias("NTNL_PROV_OTHR_NM_SFX_TX"),
    F.when(
        F.col("NTNL_PROV_OTHR_LAST_NM_TYP_CD_SK").isNull(), F.lit(0)
    ).otherwise(F.col("NTNL_PROV_OTHR_LAST_NM_TYP_CD_SK")).alias("NTNL_PROV_OTHR_LAST_NM_TYP_CD_SK"),
    F.col("NTNL_PROV_ORG_LGL_BUS_NM").alias("NTNL_PROV_ORG_LGL_BUS_NM"),
    F.col("NTNL_PROV_OTHR_ORG_NM").alias("NTNL_PROV_OTHR_ORG_NM"),
    F.when(
        F.col("NTNL_PROV_OTHR_ORG_NM_TYP_CD_SK").isNull(), F.lit(0)
    ).otherwise(F.col("NTNL_PROV_OTHR_ORG_NM_TYP_CD_SK")).alias("NTNL_PROV_OTHR_ORG_NM_TYP_CD_SK"),
    F.when(
        F.col("NTNL_PROV_ORG_SUBPART_CD_SK").isNull(), F.lit(0)
    ).otherwise(F.col("NTNL_PROV_ORG_SUBPART_CD_SK")).alias("NTNL_PROV_ORG_SUBPART_CD_SK"),
    F.col("NTNL_PROV_ORG_PRNT_LGL_BUS_NM").alias("NTNL_PROV_ORG_PRNT_LGL_BUS_NM"),
    F.col("NTNL_PROV_ORG_PRNT_TAX_ID").alias("NTNL_PROV_ORG_PRNT_TAX_ID"),
    F.col("NTNL_PROV_MAIL_ADDR_LN_1").alias("NTNL_PROV_MAIL_ADDR_LN_1"),
    F.col("NTNL_PROV_MAIL_ADDR_LN_2").alias("NTNL_PROV_MAIL_ADDR_LN_2"),
    F.col("NTNL_PROV_MAIL_ADDR_CITY_NM").alias("NTNL_PROV_MAIL_ADDR_CITY_NM"),
    F.when(
        F.col("PROV_BUS_MAILING_ADDR_STATE_NAME") == "~", F.lit(1)
    ).otherwise(
        F.when(F.col("NTNL_PROV_MAIL_ADDR_ST_CD_SK").isNull(), F.lit(0)).otherwise(F.col("NTNL_PROV_MAIL_ADDR_ST_CD_SK"))
    ).alias("NTNL_PROV_MAIL_ADDR_ST_CD_SK"),
    F.col("NTNL_PROV_MAIL_ADDR_POSTAL_CD").alias("NTNL_PROV_MAIL_ADDR_POSTAL_CD"),
    F.when(
        F.col("PROV_BUS_MAILING_ADDR_COUNTRY_CODE") == "~", F.lit(1)
    ).otherwise(
        F.when(F.col("NTNL_PROV_MAIL_ADDR_CTRY_CD_SK").isNull(), F.lit(0)).otherwise(F.col("NTNL_PROV_MAIL_ADDR_CTRY_CD_SK"))
    ).alias("NTNL_PROV_MAIL_ADDR_CTRY_CD_SK"),
    F.col("NTNL_PROV_MAIL_ADDR_FAX_NO").alias("NTNL_PROV_MAIL_ADDR_FAX_NO"),
    F.col("NTNL_PROV_MAIL_ADDR_TEL_NO").alias("NTNL_PROV_MAIL_ADDR_TEL_NO"),
    F.col("NTNL_PROV_PRCTC_PRI_LOC_ADDR_LN_1").alias("NTNL_PROV_PRCTC_PRI_LOC_ADDR_LN_1"),
    F.col("NTNL_PROV_PRCTC_PRI_LOC_ADDR_LN_2").alias("NTNL_PROV_PRCTC_PRI_LOC_ADDR_LN_2"),
    F.col("NTNL_PROV_PRCTC_PRI_LOC_ADDR_CITY_NM").alias("NTNL_PROV_PRCTC_PRI_LOC_ADDR_CITY_NM"),
    F.when(
        F.col("PROV_BUS_PRACTICE_LOC_ADDR_STATE_NAME") == "~", F.lit(1)
    ).otherwise(
        F.when(F.col("NTNL_PROV_PRCTC_PRI_LOC_ADDR_ST_CD_SK").isNull(), F.lit(0))
        .otherwise(F.col("NTNL_PROV_PRCTC_PRI_LOC_ADDR_ST_CD_SK"))
    ).alias("NTNL_PROV_PRCTC_PRI_LOC_ADDR_ST_CD_SK"),
    F.col("NTNL_PROV_PRCTC_PRI_LOC_ADDR_POSTAL_CD").alias("NTNL_PROV_PRCTC_PRI_LOC_ADDR_POSTAL_CD"),
    F.when(
        F.col("PROV_BUS_PRACTICE_LOC_ADDR_COUNTRY_CODE") == "~", F.lit(1)
    ).otherwise(
        F.when(F.col("NTNL_PROV_PRCTC_PRI_LOC_ADDR_CTRY_CD_SK").isNull(), F.lit(0))
        .otherwise(F.col("NTNL_PROV_PRCTC_PRI_LOC_ADDR_CTRY_CD_SK"))
    ).alias("NTNL_PROV_PRCTC_PRI_LOC_ADDR_CTRY_CD_SK"),
    F.col("NTNL_PROV_PRCTC_PRI_LOC_ADDR_FAX_NO").alias("NTNL_PROV_PRCTC_PRI_LOC_ADDR_FAX_NO"),
    F.col("NTNL_PROV_PRCTC_PRI_LOC_ADDR_TEL_NO").alias("NTNL_PROV_PRCTC_PRI_LOC_ADDR_TEL_NO"),
    F.col("NTNL_PROV_ORG_EMPLR_ID_NO").alias("NTNL_PROV_ORG_EMPLR_ID_NO"),
    F.col("RPLMT_NTNL_PROV_ID").alias("RPLMT_NTNL_PROV_ID"),
    F.col("NTNL_PROV_SK").alias("NTNL_PROV_SK"),
    F.col("NTNL_PROV_ID").alias("NTNL_PROV_ID"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("ds_rownum").alias("ds_rownum")
).where("1=1")

df_Lnk_UNK = df_with_rownum.filter(
    "((ds_rownum - 1)*1 + 1) = 1"
).select(
    F.lit(0).alias("NTNL_PROV_ENTY_TYP_CD_SK"),
    F.lit(0).alias("NTNL_PROV_GNDR_CD_SK"),
    F.lit(0).alias("NTNL_PROV_SOLE_PRPRTR_CD_SK"),
    F.lit(0).alias("NTNL_PROV_DCTVTN_RSN_CD_SK"),
    F.lit("2199-12-31").alias("NTNL_PROV_DCTVTN_DT"),
    F.lit("1753-01-01").alias("NTNL_PROV_ENMRTN_DT"),
    F.lit("1753-01-01").alias("NTNL_PROV_RCTVTN_DT"),
    F.lit("1753-01-01").alias("NTNL_PROV_SRC_LAST_UPDT_DT"),
    F.lit("UNK").alias("NTNL_PROV_CRDTL_TX"),
    F.lit("UNK").alias("NTNL_PROV_FIRST_NM"),
    F.lit("UNK").alias("NTNL_PROV_LAST_NM"),
    F.lit("UNK").alias("NTNL_PROV_MID_NM"),
    F.lit("UNK").alias("NTNL_PROV_NM_PFX_TX"),
    F.lit("UNK").alias("NTNL_PROV_NM_SFX_TX"),
    F.lit("UNK").alias("NTNL_PROV_OTHR_CRDTL_TX"),
    F.lit("UNK").alias("NTNL_PROV_OTHR_FIRST_NM"),
    F.lit("UNK").alias("NTNL_PROV_OTHR_LAST_NM"),
    F.lit("UNK").alias("NTNL_PROV_OTHR_MID_NM"),
    F.lit("UNK").alias("NTNL_PROV_OTHR_NM_PFX_TX"),
    F.lit("UNK").alias("NTNL_PROV_OTHR_NM_SFX_TX"),
    F.lit(0).alias("NTNL_PROV_OTHR_LAST_NM_TYP_CD_SK"),
    F.lit("UNK").alias("NTNL_PROV_ORG_LGL_BUS_NM"),
    F.lit("UNK").alias("NTNL_PROV_OTHR_ORG_NM"),
    F.lit(0).alias("NTNL_PROV_OTHR_ORG_NM_TYP_CD_SK"),
    F.lit(0).alias("NTNL_PROV_ORG_SUBPART_CD_SK"),
    F.lit("UNK").alias("NTNL_PROV_ORG_PRNT_LGL_BUS_NM"),
    F.lit("UNK").alias("NTNL_PROV_ORG_PRNT_TAX_ID"),
    F.lit("UNK").alias("NTNL_PROV_MAIL_ADDR_LN_1"),
    F.lit("UNK").alias("NTNL_PROV_MAIL_ADDR_LN_2"),
    F.lit("UNK").alias("NTNL_PROV_MAIL_ADDR_CITY_NM"),
    F.lit(0).alias("NTNL_PROV_MAIL_ADDR_ST_CD_SK"),
    F.lit("UNK").alias("NTNL_PROV_MAIL_ADDR_POSTAL_CD"),
    F.lit(0).alias("NTNL_PROV_MAIL_ADDR_CTRY_CD_SK"),
    F.lit("UNK").alias("NTNL_PROV_MAIL_ADDR_FAX_NO"),
    F.lit("UNK").alias("NTNL_PROV_MAIL_ADDR_TEL_NO"),
    F.lit("UNK").alias("NTNL_PROV_PRCTC_PRI_LOC_ADDR_LN_1"),
    F.lit("UNK").alias("NTNL_PROV_PRCTC_PRI_LOC_ADDR_LN_2"),
    F.lit("UNK").alias("NTNL_PROV_PRCTC_PRI_LOC_ADDR_CITY_NM"),
    F.lit(0).alias("NTNL_PROV_PRCTC_PRI_LOC_ADDR_ST_CD_SK"),
    F.lit("UNK").alias("NTNL_PROV_PRCTC_PRI_LOC_ADDR_POSTAL_CD"),
    F.lit(0).alias("NTNL_PROV_PRCTC_PRI_LOC_ADDR_CTRY_CD_SK"),
    F.lit("UNK").alias("NTNL_PROV_PRCTC_PRI_LOC_ADDR_FAX_NO"),
    F.lit("UNK").alias("NTNL_PROV_PRCTC_PRI_LOC_ADDR_TEL_NO"),
    F.lit("UNK").alias("NTNL_PROV_ORG_EMPLR_ID_NO"),
    F.lit("UNK").alias("RPLMT_NTNL_PROV_ID"),
    F.lit(0).alias("NTNL_PROV_SK"),
    F.lit("UNK").alias("NTNL_PROV_ID"),
    F.lit("0").alias("SRC_SYS_CD"),
    F.lit("100").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit("100").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("ds_rownum")
)

df_Lnk_NA = df_with_rownum.filter(
    "((ds_rownum - 1)*1 + 1) = 1"
).select(
    F.lit(1).alias("NTNL_PROV_ENTY_TYP_CD_SK"),
    F.lit(1).alias("NTNL_PROV_GNDR_CD_SK"),
    F.lit(1).alias("NTNL_PROV_SOLE_PRPRTR_CD_SK"),
    F.lit(1).alias("NTNL_PROV_DCTVTN_RSN_CD_SK"),
    F.lit("2199-12-31").alias("NTNL_PROV_DCTVTN_DT"),
    F.lit("1753-01-01").alias("NTNL_PROV_ENMRTN_DT"),
    F.lit("1753-01-01").alias("NTNL_PROV_RCTVTN_DT"),
    F.lit("1753-01-01").alias("NTNL_PROV_SRC_LAST_UPDT_DT"),
    F.lit("NA").alias("NTNL_PROV_CRDTL_TX"),
    F.lit("NA").alias("NTNL_PROV_FIRST_NM"),
    F.lit("NA").alias("NTNL_PROV_LAST_NM"),
    F.lit("NA").alias("NTNL_PROV_MID_NM"),
    F.lit("NA").alias("NTNL_PROV_NM_PFX_TX"),
    F.lit("NA").alias("NTNL_PROV_NM_SFX_TX"),
    F.lit("NA").alias("NTNL_PROV_OTHR_CRDTL_TX"),
    F.lit("NA").alias("NTNL_PROV_OTHR_FIRST_NM"),
    F.lit("NA").alias("NTNL_PROV_OTHR_LAST_NM"),
    F.lit("NA").alias("NTNL_PROV_OTHR_MID_NM"),
    F.lit("NA").alias("NTNL_PROV_OTHR_NM_PFX_TX"),
    F.lit("NA").alias("NTNL_PROV_OTHR_NM_SFX_TX"),
    F.lit(1).alias("NTNL_PROV_OTHR_LAST_NM_TYP_CD_SK"),
    F.lit("NA").alias("NTNL_PROV_ORG_LGL_BUS_NM"),
    F.lit("NA").alias("NTNL_PROV_OTHR_ORG_NM"),
    F.lit(1).alias("NTNL_PROV_OTHR_ORG_NM_TYP_CD_SK"),
    F.lit(1).alias("NTNL_PROV_ORG_SUBPART_CD_SK"),
    F.lit("NA").alias("NTNL_PROV_ORG_PRNT_LGL_BUS_NM"),
    F.lit("NA").alias("NTNL_PROV_ORG_PRNT_TAX_ID"),
    F.lit("NA").alias("NTNL_PROV_MAIL_ADDR_LN_1"),
    F.lit("NA").alias("NTNL_PROV_MAIL_ADDR_LN_2"),
    F.lit("NA").alias("NTNL_PROV_MAIL_ADDR_CITY_NM"),
    F.lit(1).alias("NTNL_PROV_MAIL_ADDR_ST_CD_SK"),
    F.lit("NA").alias("NTNL_PROV_MAIL_ADDR_POSTAL_CD"),
    F.lit(1).alias("NTNL_PROV_MAIL_ADDR_CTRY_CD_SK"),
    F.lit("NA").alias("NTNL_PROV_MAIL_ADDR_FAX_NO"),
    F.lit("NA").alias("NTNL_PROV_MAIL_ADDR_TEL_NO"),
    F.lit("NA").alias("NTNL_PROV_PRCTC_PRI_LOC_ADDR_LN_1"),
    F.lit("NA").alias("NTNL_PROV_PRCTC_PRI_LOC_ADDR_LN_2"),
    F.lit("NA").alias("NTNL_PROV_PRCTC_PRI_LOC_ADDR_CITY_NM"),
    F.lit(1).alias("NTNL_PROV_PRCTC_PRI_LOC_ADDR_ST_CD_SK"),
    F.lit("NA").alias("NTNL_PROV_PRCTC_PRI_LOC_ADDR_POSTAL_CD"),
    F.lit(1).alias("NTNL_PROV_PRCTC_PRI_LOC_ADDR_CTRY_CD_SK"),
    F.lit("NA").alias("NTNL_PROV_PRCTC_PRI_LOC_ADDR_FAX_NO"),
    F.lit("NA").alias("NTNL_PROV_PRCTC_PRI_LOC_ADDR_TEL_NO"),
    F.lit("NA").alias("NTNL_PROV_ORG_EMPLR_ID_NO"),
    F.lit("NA").alias("RPLMT_NTNL_PROV_ID"),
    F.lit(1).alias("NTNL_PROV_SK"),
    F.lit("NA").alias("NTNL_PROV_ID"),
    F.lit("1").alias("SRC_SYS_CD"),
    F.lit("100").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit("100").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("ds_rownum")
)

colsOrder = [
    "NTNL_PROV_SK","NTNL_PROV_ID","SRC_SYS_CD","CRT_RUN_CYC_EXCTN_SK","LAST_UPDT_RUN_CYC_EXCTN_SK",
    "NTNL_PROV_ENTY_TYP_CD_SK","NTNL_PROV_GNDR_CD_SK","NTNL_PROV_SOLE_PRPRTR_CD_SK","NTNL_PROV_DCTVTN_RSN_CD_SK",
    "NTNL_PROV_DCTVTN_DT","NTNL_PROV_ENMRTN_DT","NTNL_PROV_RCTVTN_DT","NTNL_PROV_SRC_LAST_UPDT_DT",
    "NTNL_PROV_CRDTL_TX","NTNL_PROV_FIRST_NM","NTNL_PROV_LAST_NM","NTNL_PROV_MID_NM","NTNL_PROV_NM_PFX_TX",
    "NTNL_PROV_NM_SFX_TX","NTNL_PROV_OTHR_CRDTL_TX","NTNL_PROV_OTHR_FIRST_NM","NTNL_PROV_OTHR_LAST_NM",
    "NTNL_PROV_OTHR_MID_NM","NTNL_PROV_OTHR_NM_PFX_TX","NTNL_PROV_OTHR_NM_SFX_TX","NTNL_PROV_OTHR_LAST_NM_TYP_CD_SK",
    "NTNL_PROV_ORG_LGL_BUS_NM","NTNL_PROV_OTHR_ORG_NM","NTNL_PROV_OTHR_ORG_NM_TYP_CD_SK","NTNL_PROV_ORG_SUBPART_CD_SK",
    "NTNL_PROV_ORG_PRNT_LGL_BUS_NM","NTNL_PROV_ORG_PRNT_TAX_ID","NTNL_PROV_MAIL_ADDR_LN_1","NTNL_PROV_MAIL_ADDR_LN_2",
    "NTNL_PROV_MAIL_ADDR_CITY_NM","NTNL_PROV_MAIL_ADDR_ST_CD_SK","NTNL_PROV_MAIL_ADDR_POSTAL_CD",
    "NTNL_PROV_MAIL_ADDR_CTRY_CD_SK","NTNL_PROV_MAIL_ADDR_FAX_NO","NTNL_PROV_MAIL_ADDR_TEL_NO",
    "NTNL_PROV_PRCTC_PRI_LOC_ADDR_LN_1","NTNL_PROV_PRCTC_PRI_LOC_ADDR_LN_2","NTNL_PROV_PRCTC_PRI_LOC_ADDR_CITY_NM",
    "NTNL_PROV_PRCTC_PRI_LOC_ADDR_ST_CD_SK","NTNL_PROV_PRCTC_PRI_LOC_ADDR_POSTAL_CD","NTNL_PROV_PRCTC_PRI_LOC_ADDR_CTRY_CD_SK",
    "NTNL_PROV_PRCTC_PRI_LOC_ADDR_FAX_NO","NTNL_PROV_PRCTC_PRI_LOC_ADDR_TEL_NO","NTNL_PROV_ORG_EMPLR_ID_NO","RPLMT_NTNL_PROV_ID"
]

df_main_out = df_Lnk_Main.select(
    F.col("NTNL_PROV_SK"),
    F.col("NTNL_PROV_ID"),
    F.col("SRC_SYS_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("NTNL_PROV_ENTY_TYP_CD_SK"),
    F.col("NTNL_PROV_GNDR_CD_SK"),
    F.col("NTNL_PROV_SOLE_PRPRTR_CD_SK"),
    F.col("NTNL_PROV_DCTVTN_RSN_CD_SK"),
    F.col("NTNL_PROV_DCTVTN_DT"),
    F.col("NTNL_PROV_ENMRTN_DT"),
    F.col("NTNL_PROV_RCTVTN_DT"),
    F.col("NTNL_PROV_SRC_LAST_UPDT_DT"),
    F.col("NTNL_PROV_CRDTL_TX"),
    F.col("NTNL_PROV_FIRST_NM"),
    F.col("NTNL_PROV_LAST_NM"),
    F.col("NTNL_PROV_MID_NM"),
    F.col("NTNL_PROV_NM_PFX_TX"),
    F.col("NTNL_PROV_NM_SFX_TX"),
    F.col("NTNL_PROV_OTHR_CRDTL_TX"),
    F.col("NTNL_PROV_OTHR_FIRST_NM"),
    F.col("NTNL_PROV_OTHR_LAST_NM"),
    F.col("NTNL_PROV_OTHR_MID_NM"),
    F.col("NTNL_PROV_OTHR_NM_PFX_TX"),
    F.col("NTNL_PROV_OTHR_NM_SFX_TX"),
    F.col("NTNL_PROV_OTHR_LAST_NM_TYP_CD_SK"),
    F.col("NTNL_PROV_ORG_LGL_BUS_NM"),
    F.col("NTNL_PROV_OTHR_ORG_NM"),
    F.col("NTNL_PROV_OTHR_ORG_NM_TYP_CD_SK"),
    F.col("NTNL_PROV_ORG_SUBPART_CD_SK"),
    F.col("NTNL_PROV_ORG_PRNT_LGL_BUS_NM"),
    F.col("NTNL_PROV_ORG_PRNT_TAX_ID"),
    F.col("NTNL_PROV_MAIL_ADDR_LN_1"),
    F.col("NTNL_PROV_MAIL_ADDR_LN_2"),
    F.col("NTNL_PROV_MAIL_ADDR_CITY_NM"),
    F.col("NTNL_PROV_MAIL_ADDR_ST_CD_SK"),
    F.col("NTNL_PROV_MAIL_ADDR_POSTAL_CD"),
    F.col("NTNL_PROV_MAIL_ADDR_CTRY_CD_SK"),
    F.col("NTNL_PROV_MAIL_ADDR_FAX_NO"),
    F.col("NTNL_PROV_MAIL_ADDR_TEL_NO"),
    F.col("NTNL_PROV_PRCTC_PRI_LOC_ADDR_LN_1"),
    F.col("NTNL_PROV_PRCTC_PRI_LOC_ADDR_LN_2"),
    F.col("NTNL_PROV_PRCTC_PRI_LOC_ADDR_CITY_NM"),
    F.col("NTNL_PROV_PRCTC_PRI_LOC_ADDR_ST_CD_SK"),
    F.col("NTNL_PROV_PRCTC_PRI_LOC_ADDR_POSTAL_CD"),
    F.col("NTNL_PROV_PRCTC_PRI_LOC_ADDR_CTRY_CD_SK"),
    F.col("NTNL_PROV_PRCTC_PRI_LOC_ADDR_FAX_NO"),
    F.col("NTNL_PROV_PRCTC_PRI_LOC_ADDR_TEL_NO"),
    F.col("NTNL_PROV_ORG_EMPLR_ID_NO"),
    F.col("RPLMT_NTNL_PROV_ID")
)

df_unk_out = df_Lnk_UNK.select(colsOrder)
df_na_out = df_Lnk_NA.select(colsOrder)

df_fnl_NA_UNK_Streams = df_main_out.union(df_unk_out).union(df_na_out)

write_files(
    df_fnl_NA_UNK_Streams.select(colsOrder),
    f"{adls_path}/load/NTNL_PROV.{SrcSysCd}.{RunID}.dat",
    ",",
    "overwrite",
    False,
    False,
    "^",
    None
)

df_fail = df_with_rownum.filter(
    "(svEntyTypCdSkLkupCheck = 'Y') or (svIsSoleProptrSkLkupCheck = 'Y') or (svGndrCdSkLkupCheck = 'Y') or (svDectRsnCdSkLkupCheck = 'Y') or (svLstNmTypCdSkLkupCheck = 'Y') or (svOrgSubPrtSkLkupCheck = 'Y') or (svStCdSkLkupCheck = 'Y') or (svCntryCdSkLkupCheck = 'Y') or (svLcStCdSkLkupCheck = 'Y') or (svLcCntryCdSkLkupCheck = 'Y') or (svOtrOrgNmTypCdSkLkupCheck = 'Y')"
).select(
    F.col("NTNL_PROV_SK").alias("PRI_SK"),
    F.col("NTNL_PROV_ID").alias("PRI_NAT_KEY_STRING"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD_SK"),
    F.lit("DSJobName").alias("JOB_NM"),
    F.lit("CDLOOKUP").alias("ERROR_TYP"),
    F.lit("CD_MPPNG").alias("PHYSCL_FILE_NM"),
    F.lit("").alias("FRGN_NAT_KEY_STRING"),
    F.lit("").alias("FIRST_RECYC_TS"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
)

write_files(
    df_fail,
    f"{adls_path}/fkey_failures/{PrefixFkeyFailedFileName}.IdsNtnlProvFkey_EE.dat",
    ",",
    "overwrite",
    False,
    False,
    "^",
    None
)