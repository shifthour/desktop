# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC CALLED BY : BCBSADrugClmExtrLoadSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   Reads the BCBSADrugClm_Land file from the extract program and adds the Recycle fields and business rules and reformats to the drug claim common format. Then runs shared container DrugClmPkey
# MAGIC                   Formats and processing primary key for DRUG_CLM
# MAGIC                   Also, creates new rows and gets primary key for new NDC codes, new DEA and new Pharmacies
# MAGIC                   
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC ================================================================================================================================================
# MAGIC Developer                      Date                 Project #            Change Description                          			Development Project  Code Reviewer           Date Reviewed  
# MAGIC ================================================================================================================================================
# MAGIC KChintalapani                2016-02-25        5212 PI              Original Program                                			IntegrateDev2             Kalyan Neelam            2016-05-11
# MAGIC Kaushik Kapoor             2018-03-05        TFS 20128        Adding NDC lookup to get Drug Legal status           	                IntegrateDev2             Kalyan Neelam            2018-03-05   
# MAGIC Manasa Andru               2018-03-22        TFS - 21219      Added IDS_PROV database stage and hashed file  	                IntegrateDev2             Hugh Sisson                2018-03-22
# MAGIC 			                                          to populate PROV_ID as per the mapping rule.
# MAGIC Sudhir Bomshetty         2018-05-01        5781 HEDIS       Updated logic for NTNL_PROV_ID                        	                IntegrateDev2             Kalyan Neelam            2018-05-10
# MAGIC 
# MAGIC Saikiran Subbagari         2019-01-15           5887         Added the TXNMY_CD column  in DrugClmPK Container                    IntegrateDev1                        Kalyan Neelam            2019-02-13
# MAGIC Giri Mallavaram    i         2020-04-06           5887         Added the SPEC_DRUG_IN  column in DrugClmPK Container            IntegrateDev2            Kalyan Neelam            2020-04-06
# MAGIC 
# MAGIC Velmani Kondappan      2020-08-28        6264-PBM Phase II - Government Programs           Added SUBMT_PROD_ID_QLFR,
# MAGIC                                                                                                                                          CNTNGNT_THER_FLAG,CNTNGNT_THER_SCHD,                                       IntegrateDev5  
# MAGIC                                                                                                                                          CLNT_PATN_PAY_ATRBD_PROD_AMT,
# MAGIC                                                                                                                                          CLNT_PATN_PAY_ATRBD_SLS_TAX_AMT,CLNT_PATN_PAY_ATRBD_SLS_TAX_AMT
# MAGIC                                                                                                                                          CLNT_PATN_PAY_ATRBD_PRCSR_FEE_AMT,CLNT_PATN_PAY_ATRBD_NTWK_AMT,GNRC_PROD_IN  fields

# MAGIC BCBSA Drug Claim Extract
# MAGIC Drug Claim Primary Key Shared Container
# MAGIC The file is created in the BCBSADrugClmPreProcExtr job
# MAGIC Writing Sequential File to /key
# MAGIC This container is used in:
# MAGIC PCSDrugClmExtr
# MAGIC ESIDrugClmExtr
# MAGIC WellDyneDurgClmExtr
# MAGIC MCSourceDrugClmExtr
# MAGIC Medicaid Drug Clm Extr
# MAGIC BCBSSCDrugClmExtr
# MAGIC BCADrugClmExtr
# MAGIC BCBSADrugClmExtr
# MAGIC BCBSKCCommDrugClmExtr
# MAGIC 
# MAGIC These programs need to be re-compiled when logic changes
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
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType
)
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../shared_containers/PrimaryKey/DrugClmPK
# COMMAND ----------

CactusOwner = get_widget_value('CactusOwner','')
IDSOwner = get_widget_value('IDSOwner','')
CurrDate = get_widget_value('CurrDate','')
RunCycle = get_widget_value('RunCycle','')
SrcSysCd = get_widget_value('SrcSysCd','')
RunID = get_widget_value('RunID','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
ProvRunCycle = get_widget_value('ProvRunCycle','')
ids_secret_name = get_widget_value('ids_secret_name','')
FilePath = get_widget_value('FilePath','')
CactusServer = get_widget_value('CactusServer','')
CactusAcct = get_widget_value('CactusAcct','')
CactusPW = get_widget_value('CactusPW','')
IDSDB = get_widget_value('IDSDB','')
IDSAcct = get_widget_value('IDSAcct','')
IDSPW = get_widget_value('IDSPW','')

schema_bcbsadrugclm_land = StructType([
    StructField("CLM_ID", StringType(), True),
    StructField("MBR_UNIQ_KEY", IntegerType(), True),
    StructField("NDW_HOME_PLN_ID", StringType(), True),
    StructField("CLM_LN_NO", StringType(), True),
    StructField("TRACEABILITY_FLD", StringType(), True),
    StructField("ADJ_SEQ_NO", StringType(), True),
    StructField("HOST_PLN_ID", StringType(), True),
    StructField("HOME_PLAN_PROD_ID_CD", StringType(), True),
    StructField("MBR_ID", StringType(), True),
    StructField("MBR_ZIP_CD_ON_CLM", StringType(), True),
    StructField("MBR_CTRY_ON_CLM", StringType(), True),
    StructField("NPI_REND_PROV_ID", StringType(), True),
    StructField("PRSCRB_PROV_ID", StringType(), True),
    StructField("NPI_PRSCRB_PROV_ID", StringType(), True),
    StructField("BNF_PAYMT_STTUS_CD", StringType(), True),
    StructField("PDX_CARVE_OUT_SUBMSN_IN", StringType(), True),
    StructField("CAT_OF_SVC", StringType(), True),
    StructField("CLM_PAYMT_STTUS", StringType(), True),
    StructField("DAW_CD", StringType(), True),
    StructField("DAYS_SUPL", StringType(), True),
    StructField("FRMLRY_IN", StringType(), True),
    StructField("PLN_SPEC_DRUG_IN", StringType(), True),
    StructField("PROD_SVC_ID", StringType(), True),
    StructField("QTY_DISPNS", StringType(), True),
    StructField("CLM_PD_DT", StringType(), True),
    StructField("DT_OF_SVC", StringType(), True),
    StructField("AVG_WHLSL_PRICE_SUBMT_AMT", StringType(), True),
    StructField("CONSIS_MBR_ID", StringType(), True),
    StructField("MMI_ID", StringType(), True),
    StructField("SUBGRP_SK", IntegerType(), True),
    StructField("SUBGRP_ID", StringType(), True),
    StructField("SUB_SK", IntegerType(), True),
    StructField("SUB_ID", StringType(), True),
    StructField("MBR_SFX_NO", StringType(), True),
    StructField("MBR_GNDR_CD_SK", IntegerType(), True),
    StructField("MBR_GNDR_CD", StringType(), True),
    StructField("BRTH_DT_SK", StringType(), True),
    StructField("SUB_UNIQ_KEY", IntegerType(), True),
    StructField("GRP_ID", StringType(), True)
])

df_BcbsaDrugData = (
    spark.read.format("csv")
    .option("header", False)
    .option("quote", '"')
    .option("escape", '"')
    .schema(schema_bcbsadrugclm_land)
    .load(f"{adls_path}/verified/BCBSADrugClm_Land.dat.{RunID}")
)

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

extract_query_ndc = (
    "SELECT NDC.NDC AS NDC, MAP1.SRC_CD AS NDC_DRUG_ABUSE_CTL_CD, "
    "NDC.DRUG_MNTN_IN AS DRUG_MNTN_IN "
    "FROM " + IDSOwner + ".NDC NDC, " + IDSOwner + ".CD_MPPNG MAP1 "
    "WHERE NDC.NDC_DRUG_ABUSE_CTL_CD_SK = MAP1.CD_MPPNG_SK"
)

df_ndc = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_ndc)
    .load()
)

df_ndc_dedup = dedup_sort(df_ndc, ["NDC"], [])

extract_query_prov = (
    "SELECT PROV1.NTNL_PROV_ID, PROV2.PROV_ID "
    "FROM (SELECT PROV.NTNL_PROV_ID, MIN(PROV.PROV_ID) PROV_ID "
    "      FROM " + IDSOwner + ".PROV PROV, " + IDSOwner + ".CD_MPPNG CDMP, " + IDSOwner + ".PROV_ADDR PA "
    "      WHERE PROV.PROV_ADDR_ID=PA.PROV_ADDR_ID "
    "        AND PA.PROV_ADDR_TYP_CD_SK=CDMP.CD_MPPNG_SK "
    "        AND CDMP.TRGT_CD='P' "
    "        AND PROV.NTNL_PROV_ID<>'NA' "
    "        AND PA.TERM_DT_SK>'" + CurrDate + "' "
    "      GROUP BY PROV.NTNL_PROV_ID) PROV1, "
    + IDSOwner + ".PROV PROV2 "
    "WHERE PROV1.PROV_ID = PROV2.PROV_ID"
)

df_IDS_PROV = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_prov)
    .load()
)

df_IDS_PROV_dedup = dedup_sort(df_IDS_PROV, ["NTNL_PROV_ID"], [])

df_bl = (
    df_BcbsaDrugData.alias("BcbsaDrugData")
    .join(df_ndc_dedup.alias("NdcLkup"),
          F.trim(F.col("BcbsaDrugData.PROD_SVC_ID")) == F.col("NdcLkup.NDC"),
          "left")
    .join(df_IDS_PROV_dedup.alias("provid6_lkup"),
          F.trim(F.col("BcbsaDrugData.NPI_REND_PROV_ID")) == F.col("provid6_lkup.NTNL_PROV_ID"),
          "left")
)

df_bl = df_bl.withColumn(
    "SvClmId",
    trim(
        Oconv(
            Iconv(F.col("BcbsaDrugData.DT_OF_SVC"), "D-YMD"),
            "DYJ[2,3]"
        ),
        " ",
        "A"
    )
)

df_Transform = (
    df_bl
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", F.lit(0))
    .withColumn("INSRT_UPDT_CD", F.lit("I"))
    .withColumn("DISCARD_IN", F.lit("N"))
    .withColumn("PASS_THRU_IN", F.lit("Y"))
    .withColumn("FIRST_RECYC_DT", F.lit(CurrDate))
    .withColumn("ERR_CT", F.lit(0))
    .withColumn("RECYCLE_CT", F.lit(0))
    .withColumn("SRC_SYS_CD", F.lit(SrcSysCd))
    .withColumn("PRI_KEY_STRING", F.concat(F.lit(SrcSysCd), F.lit(";"), F.col("BcbsaDrugData.CLM_ID")))
    .withColumn("DRUG_CLM_SK", F.lit(0))
    .withColumn("CLM_ID", F.col("BcbsaDrugData.CLM_ID"))
    .withColumn("CLM_STTUS", F.lit("1"))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(0))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(0))
    .withColumn(
        "NDC",
        F.when(
            F.col("BcbsaDrugData.PROD_SVC_ID").isNull() |
            (F.length(F.trim(F.col("BcbsaDrugData.PROD_SVC_ID"))) == 0),
            F.lit("UNK")
        ).otherwise(F.upper(F.trim(F.col("BcbsaDrugData.PROD_SVC_ID"))))
    )
    .withColumn("NDC_SK", F.lit(0))
    .withColumn(
        "PRSCRB_PROV_ID",
        F.when(
            F.length(F.col("BcbsaDrugData.PRSCRB_PROV_ID")) > 20,
            F.lit("NA")
        ).otherwise(F.col("BcbsaDrugData.PRSCRB_PROV_ID"))
    )
    .withColumn("PROV_DEA_SK", F.lit(0))
    .withColumn("DRUG_CLM_DAW_CD", F.lit("NA"))
    .withColumn(
        "DRUG_CLM_LGL_STTUS_CD",
        F.when(
            F.col("NdcLkup.NDC_DRUG_ABUSE_CTL_CD").isNotNull(),
            F.col("NdcLkup.NDC_DRUG_ABUSE_CTL_CD")
        ).otherwise(F.lit(""))
    )
    .withColumn("DRUG_CLM_TIER_CD", F.lit("NA"))
    .withColumn("DRUG_CLM_VNDR_STTUS_CD", F.lit("NA"))
    .withColumn("CMPND_IN", F.lit("N"))
    .withColumn("FRMLRY_IN", F.lit("N"))
    .withColumn("GNRC_DRUG_IN", F.lit("N"))
    .withColumn("MAIL_ORDER_IN", F.lit("N"))
    .withColumn("MNTN_IN", F.lit("N"))
    .withColumn("MAC_REDC_IN", F.lit("N"))
    .withColumn(
        "NON_FRMLRY_DRUG_IN",
        F.when(F.col("BcbsaDrugData.FRMLRY_IN") == F.lit("Y"), F.lit("N")).otherwise(F.lit("Y"))
    )
    .withColumn("SNGL_SRC_IN", F.lit("N"))
    .withColumn("ADJ_DT", F.lit("1753-01-01"))
    .withColumn("FILL_DT", F.col("BcbsaDrugData.DT_OF_SVC"))
    .withColumn("RECON_DT", F.lit(CurrDate))
    .withColumn("DISPNS_FEE_AMT", F.lit(0.0))
    .withColumn("HLTH_PLN_EXCL_AMT", F.lit(0.0))
    .withColumn("HLTH_PLN_PD_AMT", F.lit(0.0))
    .withColumn("INGR_CST_ALW_AMT", F.lit(0.0))
    .withColumn("INGR_CST_CHRGD_AMT", F.lit(0.0))
    .withColumn("INGR_SAV_AMT", F.lit(0.0))
    .withColumn("MBR_DEDCT_EXCL_AMT", F.lit(0.0))
    .withColumn("MBR_DIFF_PD_AMT", F.lit(0.0))
    .withColumn("MBR_OOP_AMT", F.lit(0.0))
    .withColumn("MBR_OOP_EXCL_AMT", F.lit(0.0))
    .withColumn("OTHR_SAV_AMT", F.lit(0.0))
    .withColumn("RX_ALW_QTY", F.col("BcbsaDrugData.QTY_DISPNS"))
    .withColumn("RX_SUBMT_QTY", F.col("BcbsaDrugData.QTY_DISPNS"))
    .withColumn("SLS_TAX_AMT", F.lit(0.00))
    .withColumn("RX_ALW_DAYS_SUPL_QTY", F.col("BcbsaDrugData.DAYS_SUPL"))
    .withColumn("RX_ORIG_DAYS_SUPL_QTY", F.col("BcbsaDrugData.DAYS_SUPL"))
    .withColumn("PDX_NTWK_ID", F.lit("NA"))
    .withColumn("RX_NO", F.lit(None))
    .withColumn("RFL_NO", F.lit(None))
    .withColumn("VNDR_CLM_NO", F.expr("substring(BcbsaDrugData.CLM_ID, 1, 20)"))
    .withColumn("VNDR_PREAUTH_ID", F.lit(None))
    .withColumn(
        "PROV_ID",
        F.when(
            F.col("provid6_lkup.PROV_ID").isNull() |
            (F.length(F.trim(F.col("provid6_lkup.PROV_ID"))) == 0),
            F.lit("UNK")
        ).otherwise(F.col("provid6_lkup.PROV_ID"))
    )
    .withColumn("NDC_LABEL_NM", F.lit(" "))
    .withColumn("PRSCRB_NAME", F.lit(" "))
    .withColumn("PHARMACY_NAME", F.lit("NA"))
    .withColumn("DRUG_CLM_BNF_FRMLRY_POL_CD", F.lit("NA"))
    .withColumn("DRUG_CLM_BNF_RSTRCT_CD", F.lit("NA"))
    .withColumn("DRUG_CLM_MCPA_RTD_COVDRUG_CD", F.lit("NA"))
    .withColumn("DRUG_CLM_PRAUTH_CD", F.lit("NA"))
    .withColumn("MNDTRY_MAIL_ORDER_IN", F.lit("N"))
    .withColumn("ADM_FEE_AMT", F.lit(0.00))
    .withColumn("DRUG_CLM_BILL_BSS_CD", F.lit("NA"))
    .withColumn(
        "AVG_WHLSL_PRICE_AMT",
        F.when(
            F.col("BcbsaDrugData.AVG_WHLSL_PRICE_SUBMT_AMT").isNull() |
            (F.length(F.trim(F.col("BcbsaDrugData.AVG_WHLSL_PRICE_SUBMT_AMT"))) == 0),
            F.lit(0.00)
        ).otherwise(F.col("BcbsaDrugData.AVG_WHLSL_PRICE_SUBMT_AMT").cast("double"))
    )
    .withColumn("NTNL_PROV_ID", F.col("BcbsaDrugData.NPI_PRSCRB_PROV_ID"))
    .withColumn("UCR_AMT", F.lit(0.00))
    .withColumn("SUBMT_PROD_ID_QLFR", F.lit(None))
    .withColumn("CNTNGNT_THER_FLAG", F.lit(None))
    .withColumn("CNTNGNT_THER_SCHD", F.lit("NA"))
    .withColumn("CLNT_PATN_PAY_ATRBD_PROD_AMT", F.lit(0.00))
    .withColumn("CLNT_PATN_PAY_ATRBD_SLS_TAX_AMT", F.lit(0.00))
    .withColumn("CLNT_PATN_PAY_ATRBD_PRCSR_FEE_AMT", F.lit(0.00))
    .withColumn("CLNT_PATN_PAY_ATRBD_NTWK_AMT", F.lit(0.00))
    .withColumn("GNRC_PROD_IN", F.lit("NA"))
)

df_Snapshot = df_Transform

df_Pkey = df_Snapshot.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("DISCARD_IN").alias("DISCARD_IN"),
    F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("ERR_CT").alias("ERR_CT"),
    F.col("RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("DRUG_CLM_SK").alias("DRUG_CLM_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("NDC").alias("NDC"),
    F.col("NDC_SK").alias("NDC_SK"),
    F.col("PRSCRB_PROV_ID").alias("PRSCRB_PROV_DEA"),
    F.col("DRUG_CLM_DAW_CD").alias("DRUG_CLM_DAW_CD"),
    F.col("DRUG_CLM_LGL_STTUS_CD").alias("DRUG_CLM_LGL_STTUS_CD"),
    F.col("DRUG_CLM_TIER_CD").alias("DRUG_CLM_TIER_CD"),
    F.col("DRUG_CLM_VNDR_STTUS_CD").alias("DRUG_CLM_VNDR_STTUS_CD"),
    F.col("CMPND_IN").alias("CMPND_IN"),
    F.col("FRMLRY_IN").alias("FRMLRY_IN"),
    F.col("GNRC_DRUG_IN").alias("GNRC_DRUG_IN"),
    F.col("MAIL_ORDER_IN").alias("MAIL_ORDER_IN"),
    F.col("MNTN_IN").alias("MNTN_IN"),
    F.col("MAC_REDC_IN").alias("MAC_REDC_IN"),
    F.col("NON_FRMLRY_DRUG_IN").alias("NON_FRMLRY_DRUG_IN"),
    F.col("SNGL_SRC_IN").alias("SNGL_SRC_IN"),
    F.col("ADJ_DT").alias("ADJ_DT"),
    F.col("FILL_DT").alias("FILL_DT"),
    F.col("RECON_DT").alias("RECON_DT"),
    F.col("DISPNS_FEE_AMT").alias("DISPNS_FEE_AMT"),
    F.col("HLTH_PLN_EXCL_AMT").alias("HLTH_PLN_EXCL_AMT"),
    F.col("HLTH_PLN_PD_AMT").alias("HLTH_PLN_PD_AMT"),
    F.col("INGR_CST_ALW_AMT").alias("INGR_CST_ALW_AMT"),
    F.col("INGR_CST_CHRGD_AMT").alias("INGR_CST_CHRGD_AMT"),
    F.col("INGR_SAV_AMT").alias("INGR_SAV_AMT"),
    F.col("MBR_DEDCT_EXCL_AMT").alias("MBR_DEDCT_EXCL_AMT"),
    F.col("MBR_DIFF_PD_AMT").alias("MBR_DIFF_PD_AMT"),
    F.col("MBR_OOP_AMT").alias("MBR_OOP_AMT"),
    F.col("MBR_OOP_EXCL_AMT").alias("MBR_OOP_EXCL_AMT"),
    F.col("OTHR_SAV_AMT").alias("OTHR_SAV_AMT"),
    F.col("RX_ALW_QTY").alias("RX_ALW_QTY"),
    F.col("RX_SUBMT_QTY").alias("RX_SUBMT_QTY"),
    F.col("SLS_TAX_AMT").alias("SLS_TAX_AMT"),
    F.col("RX_ALW_DAYS_SUPL_QTY").alias("RX_ALW_DAYS_SUPL_QTY"),
    F.col("RX_ORIG_DAYS_SUPL_QTY").alias("RX_ORIG_DAYS_SUPL_QTY"),
    F.col("PDX_NTWK_ID").alias("PDX_NTWK_ID"),
    F.col("RX_NO").alias("RX_NO"),
    F.col("RFL_NO").alias("RFL_NO"),
    F.col("VNDR_CLM_NO").alias("VNDR_CLM_NO"),
    F.col("VNDR_PREAUTH_ID").alias("VNDR_PREAUTH_ID"),
    F.col("CLM_STTUS").alias("CLM_STTUS_CD"),
    F.col("PROV_ID").alias("PROV_ID"),
    F.col("NDC_LABEL_NM").alias("NDC_LABEL_NM"),
    F.col("PRSCRB_NAME").alias("PRSCRB_NAME"),
    F.col("PHARMACY_NAME").alias("PHARMACY_NAME"),
    F.col("DRUG_CLM_BNF_FRMLRY_POL_CD").alias("DRUG_CLM_BNF_FRMLRY_POL_CD"),
    F.col("DRUG_CLM_BNF_RSTRCT_CD").alias("DRUG_CLM_BNF_RSTRCT_CD"),
    F.col("DRUG_CLM_MCPA_RTD_COVDRUG_CD").alias("DRUG_CLM_MCPARTD_COVDRUG_CD"),
    F.col("DRUG_CLM_PRAUTH_CD").alias("DRUG_CLM_PRAUTH_CD"),
    F.col("MNDTRY_MAIL_ORDER_IN").alias("MNDTRY_MAIL_ORDER_IN"),
    F.col("ADM_FEE_AMT").alias("ADM_FEE_AMT"),
    F.col("DRUG_CLM_BILL_BSS_CD").alias("DRUG_CLM_BILL_BSS_CD"),
    F.col("AVG_WHLSL_PRICE_AMT").alias("AVG_WHLSL_PRICE_AMT"),
    F.col("NTNL_PROV_ID").alias("NTNL_PROV_ID"),
    F.lit("NA").alias("DRUG_NM"),
    F.col("UCR_AMT").alias("UCR_AMT"),
    F.lit("NA").alias("PRTL_FILL_STTUS_CD"),
    F.lit("NA").alias("PDX_TYP"),
    F.lit(0.00).alias("INCNTV_FEE"),
    F.lit("").alias("PRSCRBR_NTNL_PROV_ID"),
    F.lit(None).alias("SPEC_DRUG_IN"),
    F.col("SUBMT_PROD_ID_QLFR").alias("SUBMT_PROD_ID_QLFR"),
    F.col("CNTNGNT_THER_FLAG").alias("CNTNGNT_THER_FLAG"),
    F.col("CNTNGNT_THER_SCHD").alias("CNTNGNT_THER_SCHD"),
    F.col("CLNT_PATN_PAY_ATRBD_PROD_AMT").alias("CLNT_PATN_PAY_ATRBD_PROD_AMT"),
    F.col("CLNT_PATN_PAY_ATRBD_SLS_TAX_AMT").alias("CLNT_PATN_PAY_ATRBD_SLS_TAX_AMT"),
    F.col("CLNT_PATN_PAY_ATRBD_PRCSR_FEE_AMT").alias("CLNT_PATN_PAY_ATRBD_PRCSR_FEE_AMT"),
    F.col("CLNT_PATN_PAY_ATRBD_NTWK_AMT").alias("CLNT_PATN_PAY_ATRBD_NTWK_AMT"),
    F.col("GNRC_PROD_IN").alias("GNRC_PROD_IN")
)

df_SnapshotToTransformer = df_Snapshot.select(
    F.col("CLM_ID").cast("string").alias("CLM_ID")
)

container_params = {
    "FilePath": FilePath,
    "RunCycle": RunCycle,
    "CactusServer": CactusServer,
    "CactusOwner": CactusOwner,
    "CactusAcct": CactusAcct,
    "CactusPW": CactusPW,
    "IDSDB": IDSDB,
    "IDSOwner": IDSOwner,
    "IDSAcct": IDSAcct,
    "IDSPW": IDSPW,
    "ProvRunCycle": ProvRunCycle
}
df_KeyDrugClm, df_KeyNDC, df_KeyDEA, df_KeyProv, df_KeyProvLoc = DrugClmPK(df_Pkey, container_params)

df_Transformer = df_SnapshotToTransformer

df_ToBDrugClm = df_Transformer.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").cast("string").alias("CLM_ID")
)

df_DrugProvLoc_final = df_KeyProvLoc.select(
    rpad(F.col("JOB_EXCTN_RCRD_ERR_SK").cast("string"), 0, "").alias("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    rpad(F.col("PROV_LOC_SK").cast("string"), 0, "").alias("PROV_LOC_SK"),
    rpad(F.col("SRC_SYS_CD_SK").cast("string"), 0, "").alias("SRC_SYS_CD_SK"),
    F.col("PROV_ID"),
    F.col("PROV_ADDR_ID"),
    rpad(F.col("PROV_ADDR_TYP_CD"), 12, " ").alias("PROV_ADDR_TYP_CD"),
    rpad(F.col("PROV_ADDR_EFF_DT"), 10, " ").alias("PROV_ADDR_EFF_DT"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("PROV_ADDR_SK"),
    F.col("PROV_SK"),
    rpad(F.col("PRI_ADDR_IN"), 1, " ").alias("PRI_ADDR_IN"),
    rpad(F.col("REMIT_ADDR_IN"), 1, " ").alias("REMIT_ADDR_IN")
)

write_files(
    df_DrugProvLoc_final,
    f"{adls_path}/key/BCBSADrugClmExtr.ProvLoc.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

df_DrugProv_final = df_KeyProv.select(
    rpad(F.col("JOB_EXCTN_RCRD_ERR_SK").cast("string"), 0, " ").alias("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    rpad(F.col("PROV_SK").cast("string"), 0, " ").alias("PROV_SK"),
    rpad(F.col("PROV_ID"), 12, " ").alias("PROV_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    rpad(F.col("CMN_PRCT"), 12, " ").alias("CMN_PRCT"),
    rpad(F.col("PROV_BILL_SVC_SK"), 10, " ").alias("PROV_BILL_SVC_SK"),
    rpad(F.col("REL_GRP_PROV"), 12, " ").alias("REL_GRP_PROV"),
    rpad(F.col("REL_IPA_PROV"), 12, " ").alias("REL_IPA_PROV"),
    rpad(F.col("PROV_CAP_PAYMT_EFT_METH_CD"), 1, " ").alias("PROV_CAP_PAYMT_EFT_METH_CD"),
    rpad(F.col("PROV_CLM_PAYMT_EFT_METH_CD"), 1, " ").alias("PROV_CLM_PAYMT_EFT_METH_CD"),
    rpad(F.col("PROV_CLM_PAYMT_METH_CD"), 1, " ").alias("PROV_CLM_PAYMT_METH_CD"),
    rpad(F.col("PROV_ENTY_CD"), 1, " ").alias("PROV_ENTY_CD"),
    rpad(F.col("PROV_FCLTY_TYP_CD"), 4, " ").alias("PROV_FCLTY_TYP_CD"),
    rpad(F.col("PROV_PRCTC_TYP_CD"), 4, " ").alias("PROV_PRCTC_TYP_CD"),
    rpad(F.col("PROV_SVC_CAT_CD"), 4, " ").alias("PROV_SVC_CAT_CD"),
    rpad(F.col("PROV_SPEC_CD"), 4, " ").alias("PROV_SPEC_CD"),
    rpad(F.col("PROV_STTUS_CD"), 2, " ").alias("PROV_STTUS_CD"),
    rpad(F.col("PROV_TERM_RSN_CD"), 4, " ").alias("PROV_TERM_RSN_CD"),
    rpad(F.col("PROV_TYP_CD"), 4, " ").alias("PROV_TYP_CD"),
    F.col("TERM_DT"),
    F.col("PAYMT_HOLD_DT"),
    rpad(F.col("CLRNGHOUSE_ID"), 30, " ").alias("CLRNGHOUSE_ID"),
    rpad(F.col("EDI_DEST_ID"), 15, " ").alias("EDI_DEST_ID"),
    rpad(F.col("EDI_DEST_QUAL"), 2, " ").alias("EDI_DEST_QUAL"),
    rpad(F.col("NTNL_PROV_ID"), 10, " ").alias("NTNL_PROV_ID"),
    rpad(F.col("PROV_ADDR_ID"), 12, " ").alias("PROV_ADDR_ID"),
    F.col("PROV_NM"),
    rpad(F.col("TAX_ID"), 9, " ").alias("TAX_ID"),
    F.col("TXNMY_CD")
)

write_files(
    df_DrugProv_final,
    f"{adls_path}/key/BCBSADrugClmExtr.DrugProv.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

df_DrugProvDea_final = df_KeyDEA.select(
    rpad(F.col("JOB_EXCTN_RCRD_ERR_SK").cast("string"), 0, " ").alias("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("PROV_DEA_SK"),
    F.col("DEA_NO"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    rpad(F.col("CMN_PRCT_CD"), 5, " ").alias("CMN_PRCT_CD"),
    rpad(F.col("CLM_TRANS_ADD_IN"), 1, " ").alias("CLM_TRANS_ADD_IN"),
    rpad(F.col("EFF_DT_SK"), 10, " ").alias("EFF_DT_SK"),
    rpad(F.col("TERM_DT_SK"), 10, " ").alias("TERM_DT_SK"),
    F.col("PROV_NM"),
    F.col("ADDR_LN_1"),
    F.col("ADDR_LN_2"),
    F.col("CITY_NM"),
    rpad(F.col("PROV_DEA_ST_CD"), 2, " ").alias("PROV_DEA_ST_CD"),
    rpad(F.col("POSTAL_CD"), 11, " ").alias("POSTAL_CD"),
    F.col("NTNL_PROV_ID"),
    F.col("NTNL_PROV_ID_SPEC_DESC"),
    F.col("NTNL_PROV_ID_PROV_TYP_DESC"),
    F.col("NTNL_PROV_ID_PROV_CLS_DESC")
)

write_files(
    df_DrugProvDea_final,
    f"{adls_path}/key/BCBSADrugClmExtr.DrugProvDea.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

df_DrugClm_final = df_KeyDrugClm.select(
    rpad(F.col("JOB_EXCTN_RCRD_ERR_SK").cast("string"), 0, " ").alias("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    rpad(F.col("DRUG_CLM_SK").cast("string"), 0, " ").alias("DRUG_CLM_SK"),
    F.col("CLM_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("NDC"),
    rpad(F.col("NDC_SK").cast("string"), 0, " ").alias("NDC_SK"),
    F.col("PRSCRB_PROV_DEA"),
    F.col("DRUG_CLM_DAW_CD"),
    F.col("DRUG_CLM_LGL_STTUS_CD"),
    F.col("DRUG_CLM_TIER_CD"),
    F.col("DRUG_CLM_VNDR_STTUS_CD"),
    rpad(F.col("CMPND_IN"), 1, " ").alias("CMPND_IN"),
    rpad(F.col("FRMLRY_IN"), 1, " ").alias("FRMLRY_IN"),
    rpad(F.col("GNRC_DRUG_IN"), 1, " ").alias("GNRC_DRUG_IN"),
    rpad(F.col("MAIL_ORDER_IN"), 1, " ").alias("MAIL_ORDER_IN"),
    rpad(F.col("MNTN_IN"), 1, " ").alias("MNTN_IN"),
    rpad(F.col("MAC_REDC_IN"), 1, " ").alias("MAC_REDC_IN"),
    rpad(F.col("NON_FRMLRY_DRUG_IN"), 1, " ").alias("NON_FRMLRY_DRUG_IN"),
    rpad(F.col("SNGL_SRC_IN"), 1, " ").alias("SNGL_SRC_IN"),
    rpad(F.col("ADJ_DT"), 10, " ").alias("ADJ_DT"),
    rpad(F.col("FILL_DT"), 10, " ").alias("FILL_DT"),
    rpad(F.col("RECON_DT"), 10, " ").alias("RECON_DT"),
    F.col("DISPNS_FEE_AMT"),
    F.col("HLTH_PLN_EXCL_AMT"),
    F.col("HLTH_PLN_PD_AMT"),
    F.col("INGR_CST_ALW_AMT"),
    F.col("INGR_CST_CHRGD_AMT"),
    F.col("INGR_SAV_AMT"),
    F.col("MBR_DEDCT_EXCL_AMT"),
    F.col("MBR_DIFF_PD_AMT"),
    F.col("MBR_OOP_AMT"),
    F.col("MBR_OOP_EXCL_AMT"),
    F.col("OTHR_SAV_AMT"),
    F.col("RX_ALW_QTY"),
    F.col("RX_SUBMT_QTY"),
    F.col("SLS_TAX_AMT"),
    F.col("RX_ALW_DAYS_SUPL_QTY"),
    F.col("RX_ORIG_DAYS_SUPL_QTY"),
    F.col("PDX_NTWK_ID"),
    F.col("RX_NO"),
    F.col("RFL_NO"),
    F.col("VNDR_CLM_NO"),
    F.col("VNDR_PREAUTH_ID"),
    F.col("CLM_STTUS_CD"),
    rpad(F.col("PROV_ID"), 12, " ").alias("PROV_ID"),
    F.col("NDC_LABEL_NM"),
    rpad(F.col("DRUG_CLM_BNF_FRMLRY_POL_CD"), 1, " ").alias("DRUG_CLM_BNF_FRMLRY_POL_CD"),
    rpad(F.col("DRUG_CLM_BNF_RSTRCT_CD"), 1, " ").alias("DRUG_CLM_BNF_RSTRCT_CD"),
    rpad(F.col("DRUG_CLM_MCPARTD_COVDRUG_CD"), 1, " ").alias("DRUG_CLM_MCPARTD_COVDRUG_CD"),
    rpad(F.col("DRUG_CLM_PRAUTH_CD"), 1, " ").alias("DRUG_CLM_PRAUTH_CD"),
    rpad(F.col("MNDTRY_MAIL_ORDER_IN"), 1, " ").alias("MNDTRY_MAIL_ORDER_IN"),
    F.col("ADM_FEE_AMT"),
    rpad(F.col("DRUG_CLM_BILL_BSS_CD"), 2, " ").alias("DRUG_CLM_BILL_BSS_CD"),
    F.col("AVG_WHLSL_PRICE_AMT"),
    F.col("UCR_AMT"),
    rpad(F.lit("NA"), 1, " ").alias("PRTL_FILL_STTUS_CD"),
    rpad(F.lit("NA"), 1, " ").alias("PDX_TYP"),
    F.col("INCNTV_FEE"),
    rpad(F.col("SPEC_DRUG_IN").cast("string"), 1, " ").alias("SPEC_DRUG_IN"),
    F.col("SUBMT_PROD_ID_QLFR"),
    F.col("CNTNGNT_THER_FLAG"),
    F.col("CNTNGNT_THER_SCHD"),
    F.col("CLNT_PATN_PAY_ATRBD_PROD_AMT"),
    F.col("CLNT_PATN_PAY_ATRBD_SLS_TAX_AMT"),
    F.col("CLNT_PATN_PAY_ATRBD_PRCSR_FEE_AMT"),
    F.col("CLNT_PATN_PAY_ATRBD_NTWK_AMT"),
    F.col("GNRC_PROD_IN")
)

write_files(
    df_DrugClm_final,
    f"{adls_path}/key/BCBSADrugClmExtr.DrugClmDrug.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

df_DrugNDC_final = df_KeyNDC.select(
    rpad(F.col("JOB_EXCTN_RCRD_ERR_SK").cast("string"), 0, " ").alias("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    rpad(F.col("NDC_SK").cast("string"), 0, " ").alias("NDC_SK"),
    F.col("NDC"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    rpad(F.lit(""), 10, " ").alias("AHFS_TCC_CD"),
    rpad(F.lit(""), 2, " ").alias("DOSE_FORM"),
    rpad(F.lit(""), 10, " ").alias("TCC_CD"),
    rpad(F.lit(""), 10, " ").alias("DSM_DRUG_TYP_CD"),
    rpad(F.col("NDC_DRUG_ABUSE_CTL_CD").cast("string"), 1, " ").alias("NDC_DRUG_ABUSE_CTL_CD"),
    rpad(F.lit(""), 1, " ").alias("NDC_DRUG_CLS_CD"),
    rpad(F.lit(""), 1, " ").alias("NDC_DRUG_FORM_CD"),
    rpad(F.lit(""), 1, " ").alias("NDC_FMT_CD"),
    rpad(F.lit(""), 1, " ").alias("NDC_GNRC_MNFCTR_CD"),
    rpad(F.lit(""), 1, " ").alias("NDC_GNRC_NMD_DRUG_CD"),
    rpad(F.lit(""), 1, " ").alias("NDC_GNRC_PRICE_CD"),
    rpad(F.lit(""), 1, " ").alias("NDC_GNRC_PRICE_SPREAD_CD"),
    rpad(F.lit(""), 1, " ").alias("NDC_ORANGE_BOOK_CD"),
    rpad(F.lit(""), 1, " ").alias("CLM_TRANS_ADD_IN"),
    rpad(F.lit(""), 1, " ").alias("DESI_DRUG_IN"),
    rpad(F.col("DRUG_MNTN_IN").cast("string"), 1, " ").alias("DRUG_MNTN_IN"),
    rpad(F.lit(""), 1, " ").alias("INNVTR_IN"),
    rpad(F.lit(""), 1, " ").alias("INSTUT_PROD_IN"),
    rpad(F.lit(""), 1, " ").alias("PRIV_LBLR_IN"),
    rpad(F.lit(""), 1, " ").alias("SNGL_SRC_IN"),
    rpad(F.lit(""), 1, " ").alias("UNIT_DOSE_IN"),
    rpad(F.lit(""), 1, " ").alias("UNIT_OF_USE_IN"),
    rpad(F.lit(""), 10, " ").alias("AVG_WHLSL_PRICE_CHG_DT_SK"),
    rpad(F.lit(""), 10, " ").alias("GNRC_PRICE_IN_CHG_DT_SK"),
    rpad(F.lit(""), 10, " ").alias("OBSLT_DT_SK"),
    rpad(F.lit(""), 10, " ").alias("SRC_NDC_CRT_DT_SK"),
    rpad(F.lit(""), 10, " ").alias("SRC_NDC_UPDT_DT_SK"),
    F.lit("").alias("BRND_NM"),
    F.lit("").alias("CORE_NINE_NO"),
    F.lit("").alias("DRUG_LABEL_NM"),
    F.lit("").alias("DRUG_STRG_DESC"),
    F.lit("").alias("GCN_CD_TX"),
    F.lit("").alias("GNRC_NM_SH_DESC"),
    F.lit("").alias("LBLR_NM"),
    F.lit("").alias("LBLR_NO"),
    F.lit("").alias("NEEDLE_GAUGE_VAL"),
    F.lit("").alias("NEEDLE_LGTH_VAL"),
    F.lit("").alias("PCKG_DESC"),
    F.lit("").alias("PCKG_SIZE_EQVLNT_NO"),
    F.lit("").alias("PCKG_SIZE_NO"),
    F.lit("").alias("PROD_NO"),
    F.lit("").alias("SYRNG_CPCT_VAL"),
    F.lit("").alias("NDC_RTE_TYP_CD")
)

write_files(
    df_DrugNDC_final,
    f"{adls_path}/key/BCBSADrugClmExtr.DrugNDC.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

df_B_DRUG_CLM_final = df_ToBDrugClm.select(
    rpad(F.col("SRC_SYS_CD_SK").cast("string"), 0, " ").alias("SRC_SYS_CD_SK"),
    rpad(F.col("CLM_ID"), 12, " ").alias("CLM_ID")
)

write_files(
    df_B_DRUG_CLM_final,
    f"{adls_path}/load/B_DRUG_CLM.BCBSA.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)