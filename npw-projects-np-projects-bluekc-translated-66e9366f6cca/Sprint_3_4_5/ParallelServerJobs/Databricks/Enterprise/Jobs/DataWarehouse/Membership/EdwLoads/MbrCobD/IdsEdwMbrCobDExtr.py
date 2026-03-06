# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2005 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC   Pulls data from ids and edw to compare and update edw
# MAGIC   Loads the data from EDW and IDS into two hash files for subsequent comparison.
# MAGIC   The comparison will determine rows that have changed and whether to generate a new history record.
# MAGIC       
# MAGIC TRANSFORMS:  
# MAGIC                   TRIM all the fields that are extracted or used in lookups
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC                   IDS - read from source, lookup all code SK values and get the natural codes
# MAGIC                   EDW - Read from source and dump to hf. Do nothing but trim the fields.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC               Tom Harrocks 08/02/2004-   Originally Programmed
# MAGIC               Steph Goddard 02/10/2005 - EDW 1.0 updates - source from CLS_PLN_DTL instead of CLS_PLN
# MAGIC               Suzanne Saylor  04/12/2006 - changed > #BeginCycle# to >=, added RunCycle parameter, set LAST_RUN_CYC_EXCTN_SK to RunCycle
# MAGIC               Sharon Andrew   07/06/2006    EDW Membership changed extractions to be pulled from W_MBR_DEL table.   Last Activity Run Cycle is not always indexed on all tables.  Helps expediate extractions.
# MAGIC 
# MAGIC MODIFICATIONS:                                                                                                                                                                                                                            DataStage                   Code                     Date 
# MAGIC Developer             Date                   Project/Altiris #     Change Description                                                                                                                                      Project                        Reviewer             Reviewed
# MAGIC -------------------------    -------------------       --------------------------    ----------------------------------------------------------------------------------------------------------------------------------------------                      ----------------------------        ----------------------    ------------------------- 
# MAGIC O. Nielsen             08/23/2008       Facets 4.5.1          Change OTHR_CAR_POL_ID to Varchar(40) in source query                                                                    devlIDSnew      
# MAGIC SAndrew               2010-07-14        TTR-665                Changed rule for  MCARE_PRI_IN from    f Trim(refMbrCobTypCd.TRGT_CD) = ( 'MCARE' )                    EnterpriseNewDevl      Steph Goddard   07/21/2010
# MAGIC                                                                                       To If Trim(refMbrCobTypCd.TRGT_CD) = ( 'MCARE' or 'MCARED')                                   
# MAGIC 
# MAGIC Srikanth Mettpalli    06/3/2013          5114                    Original Programming   (Server to Parallel Conversion)                                                                               EnterpriseWrhsDevl 
# MAGIC 
# MAGIC Venkata Y             07/09/2020       US#255921            Added 10 new fileds to the existing process                                                                                              Enterprisedev2            Kalyan Neelam    2020-07-23

# MAGIC Code SK lookups for Denormalization
# MAGIC 
# MAGIC Lookup Keys:
# MAGIC 
# MAGIC CD_MPPNG_SK
# MAGIC MBR_COB_PAYMT_PRTY_CD_SK
# MAGIC MBR_COB_TYP_CD_SK
# MAGIC MBR_COB_LAST_VER_METH_CD_SK
# MAGIC MBR_COB_OTHR_CAR_ID_CD_SK
# MAGIC MBR_COB_TERM_RSN_CD_SK
# MAGIC Write MBR_COB_D Data into a Sequential file for Load Job IdsEdwMbrCobDLoad.
# MAGIC Read all the Data from IDS MBR_COB Table;
# MAGIC Add Defaults and Null Handling.
# MAGIC Reference Code Mapping Data for Code SK lookups
# MAGIC Job Name: IdsEdwMbrCobDExtr
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
from pyspark.sql import types as T
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


# Parameter declarations
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
EDWRunCycle = get_widget_value('EDWRunCycle','')

# Acquire JDBC connection info
jdbc_url, jdbc_props = get_db_config(ids_secret_name)

# --------------------------------------------------------------------------------
# Stage: db2_MBR_COB_in (DB2ConnectorPX, Database=IDS)
# --------------------------------------------------------------------------------
extract_query_db2_MBR_COB_in = """
SELECT 
COB.MBR_COB_SK,
COALESCE(CD.TRGT_CD,'UNK') SRC_SYS_CD,
COB.MBR_UNIQ_KEY,
COB.MBR_COB_PAYMT_PRTY_CD_SK,
COB.MBR_COB_TYP_CD_SK,
COB.EFF_DT_SK,
COB.MBR_SK,
COB.MBR_COB_LAST_VER_METH_CD_SK,
COB.MBR_COB_OTHR_CAR_ID_CD_SK,
COB.MBR_COB_TERM_RSN_CD_SK,
COB.COB_LTR_TRGR_DT_SK,
COB.LACK_OF_COB_INFO_STRT_DT_SK,
COB.TERM_DT_SK,
COB.LAST_VERIFIER_TX,
COB.OTHR_CAR_POL_ID,
COB.SRC_SYS_LAST_UPDT_DT_SK,
COB.SRC_SYS_LAST_UPDT_USER_SK,
COB.MBR_COB_MCARE_SEC_PAYER_TYP_CD_SK,
COB.MBR_COB_RX_DRUG_COV_TYP_CD_SK,
COB.MBR_COB_SUPLMT_DRUG_TYP_CD_SK,
COB.RX_BIN_ID,
COB.RX_PCN_ID,
COB.RX_GRP_ID,
COB.RX_MBR_ID,
COB.MBR_COB_PRI_INSUR_ID,
COB.MBR_COB_PRI_INSUR_LAST_NM,
COB.MBR_COB_PRI_INSUR_FIRST_NM
FROM #$IDSOwner#.MBR_COB COB
LEFT JOIN #$IDSOwner#.CD_MPPNG CD
ON COB.SRC_SYS_CD_SK = CD.CD_MPPNG_SK
"""

df_db2_MBR_COB_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_MBR_COB_in)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: db2_CD_MPPNG_in (DB2ConnectorPX, Database=IDS)
# --------------------------------------------------------------------------------
extract_query_db2_CD_MPPNG_in = """
SELECT 
CD_MPPNG_SK,
COALESCE(TRGT_CD,'UNK') TRGT_CD,
COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM
FROM #$IDSOwner#.CD_MPPNG
"""

df_db2_CD_MPPNG_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_CD_MPPNG_in)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: cpy_cd_mppng (PxCopy). Create separate DataFrames for each output link.
# --------------------------------------------------------------------------------
df_Ref_MbrCobTypCd_Lkup = df_db2_CD_MPPNG_in.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_Ref_MbrCobOthrCarIdCd_Lkup = df_db2_CD_MPPNG_in.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_Ref_MbrCobTermRsnCd_Lkup = df_db2_CD_MPPNG_in.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_Ref_MbrCobLastVerMethCd_Lkup = df_db2_CD_MPPNG_in.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_Ref_MbrCobPaymtPrtyCd_Lkup = df_db2_CD_MPPNG_in.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_Ref_MbrCob_SecPYRTY = df_db2_CD_MPPNG_in.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_Ref_MbrCob_DRUGCOV_Lkp = df_db2_CD_MPPNG_in.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_Ref_MbrCob_spmnt_lkp = df_db2_CD_MPPNG_in.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

# --------------------------------------------------------------------------------
# Stage: lkp_Codes (PxLookup)
# Primary link: df_db2_MBR_COB_in as lnk_IdsEdwMbrCobDExtr_InABC
# Multiple lookup links, all left joins
# --------------------------------------------------------------------------------
df_lkp_Codes_joined = (
    df_db2_MBR_COB_in.alias("lnk_IdsEdwMbrCobDExtr_InABC")
    .join(
        df_Ref_MbrCobTypCd_Lkup.alias("Ref_MbrCobTypCd_Lkup"),
        F.col("lnk_IdsEdwMbrCobDExtr_InABC.MBR_COB_TYP_CD_SK") == F.col("Ref_MbrCobTypCd_Lkup.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_Ref_MbrCobOthrCarIdCd_Lkup.alias("Ref_MbrCobOthrCarIdCd_Lkup"),
        F.col("lnk_IdsEdwMbrCobDExtr_InABC.MBR_COB_OTHR_CAR_ID_CD_SK") == F.col("Ref_MbrCobOthrCarIdCd_Lkup.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_Ref_MbrCobTermRsnCd_Lkup.alias("Ref_MbrCobTermRsnCd_Lkup"),
        F.col("lnk_IdsEdwMbrCobDExtr_InABC.MBR_COB_TERM_RSN_CD_SK") == F.col("Ref_MbrCobTermRsnCd_Lkup.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_Ref_MbrCobLastVerMethCd_Lkup.alias("Ref_MbrCobLastVerMethCd_Lkup"),
        F.col("lnk_IdsEdwMbrCobDExtr_InABC.MBR_COB_LAST_VER_METH_CD_SK") == F.col("Ref_MbrCobLastVerMethCd_Lkup.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_Ref_MbrCobPaymtPrtyCd_Lkup.alias("Ref_MbrCobPaymtPrtyCd_Lkup"),
        F.col("lnk_IdsEdwMbrCobDExtr_InABC.MBR_COB_PAYMT_PRTY_CD_SK") == F.col("Ref_MbrCobPaymtPrtyCd_Lkup.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_Ref_MbrCob_SecPYRTY.alias("Ref_MbrCob_SecPYRTY"),
        F.col("lnk_IdsEdwMbrCobDExtr_InABC.MBR_COB_MCARE_SEC_PAYER_TYP_CD_SK") == F.col("Ref_MbrCob_SecPYRTY.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_Ref_MbrCob_DRUGCOV_Lkp.alias("Ref_MbrCob_DRUGCOV_Lkp"),
        F.col("lnk_IdsEdwMbrCobDExtr_InABC.MBR_COB_RX_DRUG_COV_TYP_CD_SK") == F.col("Ref_MbrCob_DRUGCOV_Lkp.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_Ref_MbrCob_spmnt_lkp.alias("Ref_MbrCob_spmnt_lkp"),
        F.col("lnk_IdsEdwMbrCobDExtr_InABC.MBR_COB_SUPLMT_DRUG_TYP_CD_SK") == F.col("Ref_MbrCob_spmnt_lkp.CD_MPPNG_SK"),
        "left"
    )
)

df_lkp_Codes = df_lkp_Codes_joined.select(
    F.col("lnk_IdsEdwMbrCobDExtr_InABC.MBR_COB_SK").alias("MBR_COB_SK"),
    F.col("lnk_IdsEdwMbrCobDExtr_InABC.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnk_IdsEdwMbrCobDExtr_InABC.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("Ref_MbrCobPaymtPrtyCd_Lkup.TRGT_CD").alias("MBR_COB_PAYMT_PRTY_CD"),
    F.col("Ref_MbrCobTypCd_Lkup.TRGT_CD").alias("MBR_COB_TYP_CD"),
    F.col("lnk_IdsEdwMbrCobDExtr_InABC.EFF_DT_SK").alias("MBR_COB_EFF_DT_SK"),
    F.col("lnk_IdsEdwMbrCobDExtr_InABC.MBR_SK").alias("MBR_SK"),
    F.col("lnk_IdsEdwMbrCobDExtr_InABC.LACK_OF_COB_INFO_STRT_DT_SK").alias("MBR_COB_LACK_COB_INFO_STRT_DT"),
    F.col("Ref_MbrCobLastVerMethCd_Lkup.TRGT_CD").alias("MBR_COB_LAST_VER_METH_CD"),
    F.col("Ref_MbrCobLastVerMethCd_Lkup.TRGT_CD_NM").alias("MBR_COB_LAST_VER_METH_NM"),
    F.col("lnk_IdsEdwMbrCobDExtr_InABC.LAST_VERIFIER_TX").alias("MBR_COB_LAST_VERIFIER_TX"),
    F.col("lnk_IdsEdwMbrCobDExtr_InABC.COB_LTR_TRGR_DT_SK").alias("MBR_COB_LTR_TRGR_DT_SK"),
    F.col("Ref_MbrCobOthrCarIdCd_Lkup.TRGT_CD").alias("MBR_COB_OTHR_CAR_ID_CD"),
    F.col("Ref_MbrCobOthrCarIdCd_Lkup.TRGT_CD_NM").alias("MBR_COB_OTHR_CAR_ID_NM"),
    F.col("lnk_IdsEdwMbrCobDExtr_InABC.OTHR_CAR_POL_ID").alias("MBR_COB_OTHR_CAR_POL_ID"),
    F.col("Ref_MbrCobPaymtPrtyCd_Lkup.TRGT_CD_NM").alias("MBR_COB_PAYMT_PRTY_NM"),
    F.col("Ref_MbrCobTermRsnCd_Lkup.TRGT_CD").alias("MBR_COB_TERM_RSN_CD"),
    F.col("Ref_MbrCobTermRsnCd_Lkup.TRGT_CD_NM").alias("MBR_COB_TERM_RSN_NM"),
    F.col("lnk_IdsEdwMbrCobDExtr_InABC.TERM_DT_SK").alias("MBR_COB_TERM_DT_SK"),
    F.col("Ref_MbrCobTypCd_Lkup.TRGT_CD_NM").alias("MBR_COB_TYP_NM"),
    F.col("lnk_IdsEdwMbrCobDExtr_InABC.MBR_COB_LAST_VER_METH_CD_SK").alias("MBR_COB_LAST_VER_METH_CD_SK"),
    F.col("lnk_IdsEdwMbrCobDExtr_InABC.MBR_COB_OTHR_CAR_ID_CD_SK").alias("MBR_COB_OTHR_CAR_ID_CD_SK"),
    F.col("lnk_IdsEdwMbrCobDExtr_InABC.MBR_COB_PAYMT_PRTY_CD_SK").alias("MBR_COB_PAYMT_PRTY_CD_SK"),
    F.col("lnk_IdsEdwMbrCobDExtr_InABC.MBR_COB_TERM_RSN_CD_SK").alias("MBR_COB_TERM_RSN_CD_SK"),
    F.col("lnk_IdsEdwMbrCobDExtr_InABC.MBR_COB_TYP_CD_SK").alias("MBR_COB_TYP_CD_SK"),
    F.col("lnk_IdsEdwMbrCobDExtr_InABC.MBR_COB_MCARE_SEC_PAYER_TYP_CD_SK").alias("MBR_COB_MCARE_SEC_PAYER_TYP_CD_SK"),
    F.col("Ref_MbrCob_SecPYRTY.TRGT_CD").alias("MBR_COB_MCARE_SEC_PAYER_TYP_CD"),
    F.col("Ref_MbrCob_SecPYRTY.TRGT_CD_NM").alias("MBR_COB_MCARE_SEC_PAYER_TYP_NM"),
    F.col("lnk_IdsEdwMbrCobDExtr_InABC.MBR_COB_RX_DRUG_COV_TYP_CD_SK").alias("MBR_COB_RX_DRUG_COV_TYP_CD_SK"),
    F.col("Ref_MbrCob_DRUGCOV_Lkp.TRGT_CD").alias("MBR_COB_RX_DRUG_COV_TYP_CD"),
    F.col("Ref_MbrCob_DRUGCOV_Lkp.TRGT_CD_NM").alias("MBR_COB_RX_DRUG_COV_TYP_NM"),
    F.col("lnk_IdsEdwMbrCobDExtr_InABC.MBR_COB_SUPLMT_DRUG_TYP_CD_SK").alias("MBR_COB_SUPLMT_DRUG_TYP_CD_SK"),
    F.col("Ref_MbrCob_spmnt_lkp.TRGT_CD").alias("MBR_COB_SUPLMT_DRUG_TYP_CD"),
    F.col("Ref_MbrCob_spmnt_lkp.TRGT_CD_NM").alias("MBR_COB_SUPLMT_DRUG_TYP_NM"),
    F.col("lnk_IdsEdwMbrCobDExtr_InABC.RX_BIN_ID").alias("RX_BIN_ID"),
    F.col("lnk_IdsEdwMbrCobDExtr_InABC.RX_PCN_ID").alias("RX_PCN_ID"),
    F.col("lnk_IdsEdwMbrCobDExtr_InABC.RX_GRP_ID").alias("RX_GRP_ID"),
    F.col("lnk_IdsEdwMbrCobDExtr_InABC.RX_MBR_ID").alias("RX_MBR_ID"),
    F.col("lnk_IdsEdwMbrCobDExtr_InABC.MBR_COB_PRI_INSUR_ID").alias("MBR_COB_PRI_INSUR_ID"),
    F.col("lnk_IdsEdwMbrCobDExtr_InABC.MBR_COB_PRI_INSUR_LAST_NM").alias("MBR_COB_PRI_INSUR_LAST_NM"),
    F.col("lnk_IdsEdwMbrCobDExtr_InABC.MBR_COB_PRI_INSUR_FIRST_NM").alias("MBR_COB_PRI_INSUR_FIRST_NM")
)

# --------------------------------------------------------------------------------
# Stage: xfrm_BusinessLogic (CTransformerStage)
# Define stage variables, then final output columns
# --------------------------------------------------------------------------------
df_xfrm_stagevars = (
    df_lkp_Codes
    .withColumn(
        "svMbrCobPaymtPrtyCd",
        F.when(F.trim(F.col("MBR_COB_PAYMT_PRTY_CD")) == "", F.lit("UNK"))
         .otherwise(F.trim(F.col("MBR_COB_PAYMT_PRTY_CD")))
    )
    .withColumn(
        "svMbrCobTypCd",
        F.when(F.trim(F.col("MBR_COB_TYP_CD")) == "", F.lit("UNK"))
         .otherwise(F.trim(F.col("MBR_COB_TYP_CD")))
    )
)

df_enriched = df_xfrm_stagevars.select(
    F.col("MBR_COB_SK").alias("MBR_COB_SK"),
    F.when(F.trim(F.col("SRC_SYS_CD")) == "", F.lit("UNK"))
     .otherwise(F.trim(F.col("SRC_SYS_CD"))).alias("SRC_SYS_CD"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("svMbrCobPaymtPrtyCd").alias("MBR_COB_PAYMT_PRTY_CD"),
    F.col("svMbrCobTypCd").alias("MBR_COB_TYP_CD"),
    # char(10)
    F.rpad(F.col("MBR_COB_EFF_DT_SK"), 10, " ").alias("MBR_COB_EFF_DT_SK"),
    # char(10)
    F.rpad(F.lit(EDWRunCycleDate), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    # char(10)
    F.rpad(F.lit(EDWRunCycleDate), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("MBR_SK").alias("MBR_SK"),
    # char(1)
    F.rpad(
        F.when(
            (F.lit(EDWRunCycleDate) >= F.col("MBR_COB_EFF_DT_SK")) &
            (F.lit(EDWRunCycleDate) <= F.col("MBR_COB_TERM_DT_SK")),
            F.lit("Y")
        ).otherwise(F.lit("N")),
        1, " "
    ).alias("ACTV_COB_IN"),
    # char(1)
    F.rpad(
        F.when(
            (F.col("svMbrCobTypCd").isin("MCARE", "MCARED")) &
            (F.col("svMbrCobPaymtPrtyCd") == "PRI"),
            F.lit("Y")
        ).otherwise(F.lit("N")),
        1, " "
    ).alias("MCARE_PRI_IN"),
    # char(10)
    F.rpad(F.col("MBR_COB_LACK_COB_INFO_STRT_DT"), 10, " ").alias("MBR_COB_LACK_COB_INFO_STRT_DT"),
    F.when(F.trim(F.col("MBR_COB_LAST_VER_METH_CD")) == "", F.lit("UNK"))
     .otherwise(F.trim(F.col("MBR_COB_LAST_VER_METH_CD"))).alias("MBR_COB_LAST_VER_METH_CD"),
    F.when(F.trim(F.col("MBR_COB_LAST_VER_METH_NM")) == "", F.lit("UNK"))
     .otherwise(F.trim(F.col("MBR_COB_LAST_VER_METH_NM"))).alias("MBR_COB_LAST_VER_METH_NM"),
    F.col("MBR_COB_LAST_VERIFIER_TX").alias("MBR_COB_LAST_VERIFIER_TX"),
    # char(10)
    F.rpad(F.col("MBR_COB_LTR_TRGR_DT_SK"), 10, " ").alias("MBR_COB_LTR_TRGR_DT_SK"),
    F.when(F.trim(F.col("MBR_COB_OTHR_CAR_ID_CD")) == "", F.lit("UNK"))
     .otherwise(F.trim(F.col("MBR_COB_OTHR_CAR_ID_CD"))).alias("MBR_COB_OTHR_CAR_ID_CD"),
    F.when(F.trim(F.col("MBR_COB_OTHR_CAR_ID_NM")) == "", F.lit("UNK"))
     .otherwise(F.trim(F.col("MBR_COB_OTHR_CAR_ID_NM"))).alias("MBR_COB_OTHR_CAR_ID_NM"),
    F.col("MBR_COB_OTHR_CAR_POL_ID").alias("MBR_COB_OTHR_CAR_POL_ID"),
    F.when(F.trim(F.col("MBR_COB_PAYMT_PRTY_NM")) == "", F.lit("UNK"))
     .otherwise(F.trim(F.col("MBR_COB_PAYMT_PRTY_NM"))).alias("MBR_COB_PAYMT_PRTY_NM"),
    F.when(F.trim(F.col("MBR_COB_TERM_RSN_CD")) == "", F.lit("UNK"))
     .otherwise(F.trim(F.col("MBR_COB_TERM_RSN_CD"))).alias("MBR_COB_TERM_RSN_CD"),
    F.when(F.trim(F.col("MBR_COB_TERM_RSN_NM")) == "", F.lit("UNK"))
     .otherwise(F.trim(F.col("MBR_COB_TERM_RSN_NM"))).alias("MBR_COB_TERM_RSN_NM"),
    # char(10)
    F.rpad(F.col("MBR_COB_TERM_DT_SK"), 10, " ").alias("MBR_COB_TERM_DT_SK"),
    F.when(F.trim(F.col("MBR_COB_TYP_NM")) == "", F.lit("UNK"))
     .otherwise(F.trim(F.col("MBR_COB_TYP_NM"))).alias("MBR_COB_TYP_NM"),
    F.lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("MBR_COB_LAST_VER_METH_CD_SK").alias("MBR_COB_LAST_VER_METH_CD_SK"),
    F.col("MBR_COB_OTHR_CAR_ID_CD_SK").alias("MBR_COB_OTHR_CAR_ID_CD_SK"),
    F.col("MBR_COB_PAYMT_PRTY_CD_SK").alias("MBR_COB_PAYMT_PRTY_CD_SK"),
    F.col("MBR_COB_TERM_RSN_CD_SK").alias("MBR_COB_TERM_RSN_CD_SK"),
    F.col("MBR_COB_TYP_CD_SK").alias("MBR_COB_TYP_CD_SK"),
    F.col("MBR_COB_MCARE_SEC_PAYER_TYP_CD_SK").alias("MBR_COB_MCARE_SEC_PAYER_TYP_CD_SK"),
    F.col("MBR_COB_MCARE_SEC_PAYER_TYP_CD").alias("MBR_COB_MCARE_SEC_PAYER_TYP_CD"),
    F.col("MBR_COB_MCARE_SEC_PAYER_TYP_NM").alias("MBR_COB_MCARE_SEC_PAYER_TYP_NM"),
    F.col("MBR_COB_RX_DRUG_COV_TYP_CD_SK").alias("MBR_COB_RX_DRUG_COV_TYP_CD_SK"),
    F.col("MBR_COB_RX_DRUG_COV_TYP_CD").alias("MBR_COB_RX_DRUG_COV_TYP_CD"),
    F.col("MBR_COB_RX_DRUG_COV_TYP_NM").alias("MBR_COB_RX_DRUG_COV_TYP_NM"),
    F.col("MBR_COB_SUPLMT_DRUG_TYP_CD_SK").alias("MBR_COB_SUPLMT_DRUG_TYP_CD_SK"),
    F.col("MBR_COB_SUPLMT_DRUG_TYP_CD").alias("MBR_COB_SUPLMT_DRUG_TYP_CD"),
    F.col("MBR_COB_SUPLMT_DRUG_TYP_NM").alias("MBR_COB_SUPLMT_DRUG_TYP_NM"),
    F.col("RX_BIN_ID").alias("RX_BIN_ID"),
    F.col("RX_PCN_ID").alias("RX_PCN_ID"),
    F.col("RX_GRP_ID").alias("RX_GRP_ID"),
    F.col("RX_MBR_ID").alias("RX_MBR_ID"),
    # substring [1,20]
    F.substring(F.col("MBR_COB_PRI_INSUR_ID"), 1, 20).alias("MBR_COB_PRI_INSUR_ID"),
    F.col("MBR_COB_PRI_INSUR_LAST_NM").alias("MBR_COB_PRI_INSUR_LAST_NM"),
    F.col("MBR_COB_PRI_INSUR_FIRST_NM").alias("MBR_COB_PRI_INSUR_FIRST_NM")
)

# According to the rules about KeyMgtGetNextValueConcurrent or SurrogateKeyGen,
# there is no mention of needing a surrogate key step here, so we do nothing special.

# --------------------------------------------------------------------------------
# Stage: seq_MBR_COB_D_csv_load (PxSequentialFile)
# Write out the final data
# --------------------------------------------------------------------------------
write_files(
    df_enriched,
    f"{adls_path}/load/MBR_COB_D.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)