# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2005, 2006, 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC Job Name :  EyeMedIdsProvXfrm
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                                       Date                     Project/Altiris #                       Change Description                                                         Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                                ----------------------------------   ---------------------------------    -------------------------   
# MAGIC 
# MAGIC Goutham K                                       2020-09-23       US-261580              Original Programming.                                                        IntegrateDev2    Kalyan Neelam            2020-10-26      
# MAGIC 
# MAGIC Goutham K                                      2021-02-24          US-356916            Added Taxonomy field and added condition                      IntegrateDev2    Jeyaprasanna             2021-02-25
# MAGIC                                                                                                                   to do the look up ONLY if the taxomnomy code from 
# MAGIC                                                                                                                  source is null or blank
# MAGIC 
# MAGIC Goutham K                          2021-05-15         US-366403            New Provider file Change to include Loc and Svc loc id                  IntegrateDev1  Jeyaprasanna             2021-05-24

# MAGIC Transformed Data will land into a Dataset for Primary Keying job.
# MAGIC 
# MAGIC Data is partitioned on Natural Key Columns.
# MAGIC Attach taxonomy code based on NPI+ST If no match found, Match on NPI and retrieve the Code corresponding to Highest NPI SK value
# MAGIC JobName: EyeMedIdsProvXfrm
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType, TimestampType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


SrcSysCd = get_widget_value('SrcSysCd','')
RunID = get_widget_value('RunID','')
RunIDTimeStamp = get_widget_value('RunIDTimeStamp','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

# Read from db2_K_CMN_PRCT_Lkp
df_db2_K_CMN_PRCT_Lkp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", "SELECT CMN_PRCT_SK, SRC_SYS_CD, CMN_PRCT_ID FROM " + IDSOwner + ".K_CMN_PRCT")
    .load()
)

# Read from db2_CMN_PRCT
df_db2_CMN_PRCT = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", "SELECT NTNL_PROV_ID, MIN(CMN_PRCT_SK) AS CMN_PRCT_SK FROM " + IDSOwner + ".CMN_PRCT GROUP BY NTNL_PROV_ID")
    .load()
)

# Read from ds_PROV_EYEMED_CLM (converted .ds to .parquet)
df_ds_PROV_EYEMED_CLM = spark.read.parquet(f"{adls_path}/ds/PROV.{SrcSysCd}.extr.{RunID}.parquet")

# Xfrm_BusinessLogic
df_Xfrm_BusinessLogic_out = (
    df_ds_PROV_EYEMED_CLM
    .select(
        F.col("PROV_ID").alias("PROV_ID"),
        F.col("PROV_NPI").alias("PROV_NPI"),
        F.col("TAX_ENTY_NPI").alias("TAX_ENTY_NPI"),
        F.col("PROV_FIRST_NM").alias("PROV_FIRST_NM"),
        F.col("PROV_LAST_NM").alias("PROV_LAST_NM"),
        F.col("BUS_NM").alias("BUS_NM"),
        F.col("PROV_ADDR").alias("PROV_ADDR"),
        F.col("PROV_ADDR_2").alias("PROV_ADDR_2"),
        F.col("PROV_CITY").alias("PROV_CITY"),
        F.col("PROV_ST").alias("PROV_ST"),
        F.col("PROV_ZIP").alias("PROV_ZIP"),
        F.col("PROV_ZIP_PLUS_4").alias("PROV_ZIP_PLUS_4"),
        F.col("PROF_DSGTN").alias("PROF_DSGTN"),
        F.col("TAX_ENTY_ID").alias("TAX_ENTY_ID"),
        F.col("TXNMY_CD").alias("TXNMY_CD"),
        F.col("EFF_DT").alias("EFF_DT"),
        F.col("PROV_ADDR_ID").alias("PROV_ADDR_ID")
    )
    .withColumn("FCTS_SRC_SYS_CD", F.lit("FACETS"))
)

# Read ds_CD_MPPNG_Data (converted .ds to .parquet)
df_ds_CD_MPPNG_Data = spark.read.parquet(f"{adls_path}/ds/CD_MPPNG.parquet")

# fltr_Cd_MppngData
df_fltr_Cd_MppngData_tmp = df_ds_CD_MPPNG_Data.filter(
    (F.col("SRC_SYS_CD") == "EYEMED") &
    (F.col("SRC_CLCTN_CD") == "EYEMED") &
    (F.col("TRGT_CLCTN_CD") == "IDS") &
    (F.col("SRC_DOMAIN_NM") == "PROVIDER ENTITY") &
    (F.col("TRGT_DOMAIN_NM") == "PROVIDER ENTITY")
)
df_fltr_Cd_MppngData = df_fltr_Cd_MppngData_tmp.select(
    F.col("SRC_CD").alias("SRC_CD"),
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK")
)

# Lkup_Codes (PxLookup) - Additional columns needed for referencing nonexistent columns
df_Xfrm_BusinessLogic_out_tmp = df_Xfrm_BusinessLogic_out \
    .withColumn("lnk_IdsDmClmDmClmLnProcCdModExtr_InABC.CLM_LN_PROC_CD_MOD_ORDNL_CD_SK", F.lit(None)) \
    .withColumn("lnk_IdsProvFkey_EE_InAbc.CMN_PRCT", F.lit(None))

df_lkup_codes_join = df_Xfrm_BusinessLogic_out_tmp.alias("lnk_EyeMedProvLkup_in") \
    .join(
        df_db2_CMN_PRCT.alias("Ref_CmnPrctSk"),
        (
            (F.col("lnk_EyeMedProvLkup_in.CMN_PRCT_SK") == F.col("Ref_CmnPrctSk.CMN_PRCT_SK")) &
            (F.col("lnk_EyeMedProvLkup_in.FCTS_SRC_SYS_CD") == F.col("Ref_CmnPrctSk.SRC_SYS_CD")) &
            (F.col("lnk_IdsProvFkey_EE_InAbc.CMN_PRCT") == F.col("Ref_CmnPrctSk.CMN_PRCT_ID")) &
            (F.col("lnk_EyeMedProvLkup_in.PROV_NPI") == F.col("Ref_CmnPrctSk.NTNL_PROV_ID"))
        ),
        "left"
    ) \
    .join(
        df_fltr_Cd_MppngData.alias("ref_ProvEntyCdSK"),
        (
            (F.col("lnk_EyeMedProvLkup_in.PROF_DSGTN") == F.col("ref_ProvEntyCdSK.SRC_CD")) &
            (F.col("lnk_IdsDmClmDmClmLnProcCdModExtr_InABC.CLM_LN_PROC_CD_MOD_ORDNL_CD_SK") == F.col("ref_ProvEntyCdSK.CD_MPPNG_SK"))
        ),
        "left"
    )

df_lkup_codes = df_lkup_codes_join.select(
    F.col("lnk_EyeMedProvLkup_in.PROV_ID").alias("PROV_ID"),
    F.col("lnk_EyeMedProvLkup_in.PROV_NPI").alias("PROV_NPI"),
    F.col("lnk_EyeMedProvLkup_in.TAX_ENTY_NPI").alias("TAX_ENTY_NPI"),
    F.col("lnk_EyeMedProvLkup_in.PROV_FIRST_NM").alias("PROV_FIRST_NM"),
    F.col("lnk_EyeMedProvLkup_in.PROV_LAST_NM").alias("PROV_LAST_NM"),
    F.col("lnk_EyeMedProvLkup_in.BUS_NM").alias("BUS_NM"),
    F.col("lnk_EyeMedProvLkup_in.PROV_ADDR").alias("PROV_ADDR"),
    F.col("lnk_EyeMedProvLkup_in.PROV_ADDR_2").alias("PROV_ADDR_2"),
    F.col("lnk_EyeMedProvLkup_in.PROV_CITY").alias("PROV_CITY"),
    F.col("lnk_EyeMedProvLkup_in.PROV_ST").alias("PROV_ST"),
    F.col("lnk_EyeMedProvLkup_in.PROV_ZIP").alias("PROV_ZIP"),
    F.col("lnk_EyeMedProvLkup_in.PROV_ZIP_PLUS_4").alias("PROV_ZIP_PLUS_4"),
    F.col("lnk_EyeMedProvLkup_in.PROF_DSGTN").alias("PROF_DSGTN"),
    F.col("lnk_EyeMedProvLkup_in.TAX_ENTY_ID").alias("TAX_ENTY_ID"),
    F.col("lnk_EyeMedProvLkup_in.TXNMY_CD").alias("TXNMY_CD"),
    F.col("lnk_EyeMedProvLkup_in.FCTS_SRC_SYS_CD").alias("FCTS_SRC_SYS_CD"),
    F.col("Ref_CmnPrctSk.CMN_PRCT_SK").alias("CMN_PRCT_SK"),
    F.col("ref_ProvEntyCdSK.CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("lnk_EyeMedProvLkup_in.EFF_DT").alias("EFF_DT"),
    F.col("lnk_EyeMedProvLkup_in.PROV_ADDR_ID").alias("PROV_ADDR_ID")
)

# db2_TXNMY_Lkp
df_db2_TXNMY_Lkp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", "SELECT TXNMY_CD, NTNL_PROV_ID, ENTY_LIC_ST_CD, NTNL_PROV_TXNMY_SK FROM " + IDSOwner + ".NTNL_PROV_TXNMY GROUP BY TXNMY_CD, NTNL_PROV_ID, ENTY_LIC_ST_CD, NTNL_PROV_TXNMY_SK")
    .load()
)

# Cpy
df_cpy_npi_st = df_db2_TXNMY_Lkp.select(
    F.col("NTNL_PROV_TXNMY_SK").alias("NTNL_PROV_TXNMY_SK_ST"),
    F.col("NTNL_PROV_ID").alias("NTNL_PROV_ID"),
    F.col("TXNMY_CD").alias("TXNMY_CD"),
    F.col("ENTY_LIC_ST_CD").alias("ENTY_LIC_ST_CD")
)
df_cpy_npi = df_db2_TXNMY_Lkp.select(
    F.col("NTNL_PROV_TXNMY_SK").alias("NTNL_PROV_TXNMY_SK"),
    F.col("NTNL_PROV_ID").alias("NTNL_PROV_ID"),
    F.col("TXNMY_CD").alias("TXNMY_CD")
)

# RmDup_NPI_ST
df_rmd_npi_st_tmp = dedup_sort(
    df_cpy_npi_st,
    partition_cols=["NTNL_PROV_ID","ENTY_LIC_ST_CD"],
    sort_cols=[]
)
df_rmd_npi_st = df_rmd_npi_st_tmp.select(
    F.col("NTNL_PROV_ID").alias("NTNL_PROV_ID"),
    F.col("TXNMY_CD").alias("TXNMY_CD_ST"),
    F.col("ENTY_LIC_ST_CD").alias("ENTY_LIC_ST_CD")
)

# RmDup_NPI
df_rmd_npi_tmp = dedup_sort(
    df_cpy_npi,
    partition_cols=["NTNL_PROV_ID"],
    sort_cols=[("NTNL_PROV_ID","A"),("NTNL_PROV_TXNMY_SK","D")]
)
df_rmd_npi = df_rmd_npi_tmp.select(
    F.col("NTNL_PROV_ID").alias("NTNL_PROV_ID"),
    F.col("TXNMY_CD").alias("TXNMY_CD")
)

# Lkup_Fkey
df_lkup_fkey_in_tmp = df_lkup_codes \
    .withColumn("lnk_IdsProvFkey_EE_InAbc.CMN_PRCT", F.lit(None)) \
    .withColumn("lnk_IdsDmClmDmClmLnProcCdModExtr_InABC.CLM_LN_PROC_CD_MOD_ORDNL_CD_SK", F.lit(None))

df_lkup_fkey_join = df_lkup_fkey_in_tmp.alias("lnk_EyeMedProv_Lkup_In") \
    .join(
        df_db2_K_CMN_PRCT_Lkp.alias("Ref_CmnPrctSk"),
        (
            (F.col("lnk_EyeMedProv_Lkup_In.CMN_PRCT_SK") == F.col("Ref_CmnPrctSk.CMN_PRCT_SK")) &
            (F.col("lnk_EyeMedProv_Lkup_In.FCTS_SRC_SYS_CD") == F.col("Ref_CmnPrctSk.SRC_SYS_CD")) &
            (F.col("lnk_IdsProvFkey_EE_InAbc.CMN_PRCT") == F.col("Ref_CmnPrctSk.CMN_PRCT_ID")) &
            (F.col("lnk_EyeMedProv_Lkup_In.PROV_NPI") == F.col("Ref_CmnPrctSk.NTNL_PROV_ID"))
        ),
        "left"
    ) \
    .join(
        df_rmd_npi_st.alias("Ref_NpiSt_TxnmyCd"),
        (
            (F.col("lnk_EyeMedProv_Lkup_In.PROV_NPI") == F.col("Ref_NpiSt_TxnmyCd.NTNL_PROV_ID")) &
            (F.col("lnk_EyeMedProv_Lkup_In.PROV_ST") == F.col("Ref_NpiSt_TxnmyCd.ENTY_LIC_ST_CD"))
        ),
        "left"
    ) \
    .join(
        df_rmd_npi.alias("Ref_NPITxnmyCd"),
        (F.col("lnk_EyeMedProv_Lkup_In.PROV_NPI") == F.col("Ref_NPITxnmyCd.NTNL_PROV_ID")),
        "left"
    )

df_lkup_fkey_out = df_lkup_fkey_join.select(
    F.col("lnk_EyeMedProv_Lkup_In.PROV_ID").alias("PROV_ID"),
    F.col("lnk_EyeMedProv_Lkup_In.PROV_NPI").alias("PROV_NPI"),
    F.col("lnk_EyeMedProv_Lkup_In.TAX_ENTY_NPI").alias("TAX_ENTY_NPI"),
    F.col("lnk_EyeMedProv_Lkup_In.PROV_FIRST_NM").alias("PROV_FIRST_NM"),
    F.col("lnk_EyeMedProv_Lkup_In.PROV_LAST_NM").alias("PROV_LAST_NM"),
    F.col("lnk_EyeMedProv_Lkup_In.BUS_NM").alias("BUS_NM"),
    F.col("lnk_EyeMedProv_Lkup_In.PROV_ADDR").alias("PROV_ADDR"),
    F.col("lnk_EyeMedProv_Lkup_In.PROV_ADDR_2").alias("PROV_ADDR_2"),
    F.col("lnk_EyeMedProv_Lkup_In.PROV_CITY").alias("PROV_CITY"),
    F.col("lnk_EyeMedProv_Lkup_In.PROV_ST").alias("PROV_ST"),
    F.col("lnk_EyeMedProv_Lkup_In.PROV_ZIP").alias("PROV_ZIP"),
    F.col("lnk_EyeMedProv_Lkup_In.PROV_ZIP_PLUS_4").alias("PROV_ZIP_PLUS_4"),
    F.col("lnk_EyeMedProv_Lkup_In.PROF_DSGTN").alias("PROF_DSGTN"),
    F.col("lnk_EyeMedProv_Lkup_In.TAX_ENTY_ID").alias("TAX_ENTY_ID"),
    F.col("lnk_EyeMedProv_Lkup_In.CMN_PRCT_SK").alias("CMN_PRCT_SK"),
    F.col("lnk_EyeMedProv_Lkup_In.CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("Ref_CmnPrctSk.CMN_PRCT_ID").alias("CMN_PRCT_ID"),
    F.col("Ref_NpiSt_TxnmyCd.TXNMY_CD_ST").alias("TXNMY_CD_ST"),
    F.col("Ref_NPITxnmyCd.TXNMY_CD").alias("TXNMY_CD"),
    F.col("lnk_EyeMedProv_Lkup_In.TXNMY_CD").alias("TXNMY_CD_1"),
    F.col("lnk_EyeMedProv_Lkup_In.EFF_DT").alias("EFF_DT"),
    F.col("lnk_EyeMedProv_Lkup_In.PROV_ADDR_ID").alias("PROV_ADDR_ID")
)

# Xfrm_BusinessRules (transformer)
df_business_rules_stagevars = (
    df_lkup_fkey_out
    .withColumn(
        "svProvFirstNm",
        F.upper(F.trim(F.when(F.col("PROV_FIRST_NM").isNotNull(), F.col("PROV_FIRST_NM")).otherwise("")))
    )
    .withColumn(
        "svProvLastNm",
        F.upper(F.trim(F.when(F.col("PROV_LAST_NM").isNotNull(), F.col("PROV_LAST_NM")).otherwise("")))
    )
    .withColumn(
        "svBusNm",
        F.upper(F.trim(F.when(F.col("BUS_NM").isNotNull(), F.col("BUS_NM")).otherwise("")))
    )
    .withColumn(
        "svProvNm",
        F.concat_ws(",", 
            F.concat_ws(" ", F.col("svProvFirstNm"), F.col("svProvLastNm")), 
            F.col("svBusNm")
        )
    )
)

# Output link lnk_EyeMedProv_Out
df_business_rules_out = (
    df_business_rules_stagevars
    .select(
        F.concat_ws(";", F.col("PROV_ID"), F.lit(SrcSysCd)).alias("PRI_NAT_KEY_STRING"),
        F.lit(RunIDTimeStamp).alias("FIRST_RECYC_TS"),
        F.lit(0).alias("PROV_SK"),
        F.col("PROV_ID").alias("PROV_ID"),
        F.lit(SrcSysCd).alias("SRC_SYS_CD"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
        F.when(
            F.col("CMN_PRCT_ID").isNull(),
            F.lit("")
        ).otherwise(F.col("CMN_PRCT_ID")).alias("CMN_PRCT"),
        F.lit("NA").alias("REL_GRP_PROV"),
        F.lit("NA").alias("REL_IPA_PROV"),
        F.lit("NA").alias("PROV_CAP_PAYMT_EFT_METH_CD"),
        F.lit("NA").alias("PROV_CLM_PAYMT_EFT_METH_CD"),
        F.lit("NA").alias("PROV_CLM_PAYMT_METH_CD"),
        F.lit("PRCTR").alias("PROV_ENTY_CD"),
        F.lit("NA").alias("PROV_FCLTY_TYP_CD"),
        F.lit("NA").alias("PROV_PRCTC_TYP_CD"),
        F.lit("NA").alias("PROV_SVC_CAT_CD"),
        F.when(
            F.trim(F.col("PROF_DSGTN"), ".") == 'OPT', F.lit("0084")
        ).when(
            F.trim(F.col("PROF_DSGTN"), ".") == 'OD', F.lit("0083")
        ).when(
            F.trim(F.col("PROF_DSGTN"), ".") == 'MD', F.lit("0018")
        ).when(
            F.trim(F.col("PROF_DSGTN"), ".") == 'DO', F.lit("0018")
        ).otherwise(F.lit("UNK")).alias("PROV_SPEC_CD"),
        F.lit("NA").alias("PROV_STTUS_CD"),
        F.lit("NA").alias("PROV_TERM_RSN_CD"),
        F.lit("NA").alias("PROV_TYP_CD"),
        F.trim(F.col("EFF_DT")).alias("TERM_DT"),
        F.lit("2199-12-31").alias("PAYMT_HOLD_DT"),
        F.lit("NA").alias("CLRNGHOUSE_ID"),
        F.lit("NA").alias("EDI_DEST_ID"),
        F.lit("").alias("EDI_DEST_QUAL"),
        F.when(
            F.col("PROV_NPI").isNull() | (F.trim(F.col("PROV_NPI")) == ""), 
            F.lit("NA")
        ).otherwise(F.col("PROV_NPI")).alias("NTNL_PROV_ID"),
        F.col("PROV_ADDR_ID").alias("PROV_ADDR_ID"),
        F.when(
            F.rtrim(F.col("svProvNm")).endswith(","),
            F.trim(F.regexp_replace(F.col("svProvNm"), ",$", ""))
        ).when(
            F.rtrim(F.col("svProvNm")).startswith(","),
            F.trim(F.regexp_replace(F.col("svProvNm"), "^,", ""))
        ).when(
            F.col("svProvLastNm") == "",
            F.concat_ws(",", F.trim(F.split(F.col("svProvNm"), ",")[0]),
                             F.trim(F.split(F.col("svProvNm"), ",")[1])))
        .otherwise(F.trim(F.col("svProvNm")))
        .alias("PROV_NM"),
        F.when(
            (F.col("TAX_ENTY_ID").isNull()) | (F.trim(F.col("TAX_ENTY_ID")) == ""),
            F.lit("NA")
        ).otherwise(F.col("TAX_ENTY_ID")).alias("TAX_ID"),
        F.when(
            F.trim(F.col("TXNMY_CD_1")) == "",
            F.when(
                (F.col("TXNMY_CD_ST").isNotNull()) & (F.trim(F.col("TXNMY_CD_ST")) != ""),
                F.col("TXNMY_CD_ST")
            ).otherwise(
                F.when(
                    (F.col("TXNMY_CD").isNotNull()) & (F.trim(F.col("TXNMY_CD")) != ""),
                    F.col("TXNMY_CD")
                ).otherwise(F.lit("UNK"))
            )
        ).otherwise(F.col("TXNMY_CD_1")).alias("TXNMY_CD")
    )
)

# Output link lnk_EyeMedProvBProv_Out
df_business_rules_bprov = (
    df_business_rules_stagevars
    .select(
        F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
        F.col("PROV_ID").alias("PROV_ID"),
        F.when(
            (F.col("PROV_NPI").isNull()) | (F.trim(F.col("PROV_NPI")) == ""),
            F.lit(1)
        ).otherwise(
            F.when(F.col("CMN_PRCT_SK").isNull(), F.lit(0)).otherwise(F.col("CMN_PRCT_SK"))
        ).alias("CMN_PRCT_SK"),
        F.lit(1).alias("REL_GRP_PROV_SK"),
        F.lit(1).alias("REL_IPA_PROV_SK"),
        F.when(F.col("CD_MPPNG_SK").isNull(), F.lit(0)).otherwise(F.col("CD_MPPNG_SK")).alias("PROV_ENTY_CD_SK")
    )
)

# ds_PROV_Xfrm (converted .ds -> .parquet)
# We must apply rpad for columns with char(10)
df_ds_PROV_Xfrm_out = (
    df_business_rules_out
    .withColumn("TERM_DT", F.rpad(F.col("TERM_DT"), 10, " "))
    .withColumn("PAYMT_HOLD_DT", F.rpad(F.col("PAYMT_HOLD_DT"), 10, " "))
)

write_files(
    df_ds_PROV_Xfrm_out,
    f"{adls_path}/ds/PROV.{SrcSysCd}.xfrm.{RunID}.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

# seq_B_PROV_csv (PxSequentialFile)
write_files(
    df_business_rules_bprov,
    f"{adls_path}/load/B_PROV.{SrcSysCd}.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)