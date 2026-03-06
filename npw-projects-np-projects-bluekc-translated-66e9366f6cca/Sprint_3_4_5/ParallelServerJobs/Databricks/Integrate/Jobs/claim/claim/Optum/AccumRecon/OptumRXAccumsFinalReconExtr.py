# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2020 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC 
# MAGIC CALLED BY :  GxOptumAccumReconSeq
# MAGIC 
# MAGIC DESCRIPTION:      Determining Inserts and Updates to process  amount calculations
# MAGIC                                
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                         Date                 Project/Altiris #                           Change Description                                                                                          Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------                      --------------------     ------------------------                            -----------------------------------------------------------------------                                                   --------------------------------       -------------------------------   ----------------------------      
# MAGIC Rekha Radhakrishna      10/01/2020    OPTUMRX Accum Exchange            Initial Programming                                                                            IntegrateDev2                   Jaideep Mankala        11/19/2020

# MAGIC Target lookup to determine Inserts/Updates before performing more calculations.
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
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

# Parameters
GxCAccumReconOwner = get_widget_value('GxCAccumReconOwner','')
gxcaccumrecon_secret_name = get_widget_value('gxcaccumrecon_secret_name','')
RxAccumExchangeOwner = get_widget_value('RxAccumExchangeOwner','')
rxaccumexchange_secret_name = get_widget_value('rxaccumexchange_secret_name','')

# Stage: RX_Accum_Exchange (ODBCConnectorPX) --> df_RX_Accum_Exchange
jdbc_url, jdbc_props = get_db_config(rxaccumexchange_secret_name)
extract_query_RX_Accum_Exchange = """
SELECT A.MBR_ID, 'Y' AS AE_ERR FROM 
       (SELECT TRANS_CLM_ID, MBR_ID, TRANS_TYP_CD FROM [dbo].[PBM_ACCUM_EXCH_TRANS] WHERE TRANS_STTUS_CD NOT IN ('200','202') AND MBR_ID IS NOT NULL GROUP BY TRANS_CLM_ID, MBR_ID, TRANS_TYP_CD) A
             LEFT JOIN 
       (SELECT TRANS_CLM_ID, MBR_ID, TRANS_TYP_CD FROM [dbo].[PBM_ACCUM_EXCH_TRANS] WHERE TRANS_STTUS_CD IN ('200','202') AND MBR_ID IS NOT NULL GROUP BY TRANS_CLM_ID, MBR_ID, TRANS_TYP_CD) B 
       ON A.TRANS_CLM_ID= B.TRANS_CLM_ID AND A.TRANS_TYP_CD = B.TRANS_TYP_CD
WHERE B.MBR_ID IS NULL
UNION
SELECT MT.MBR_ID, 'Y' AS AE_ERR
       FROM """ + RxAccumExchangeOwner + """.[PBM_ACCUM_EXCH_TRANS] MT
                    LEFT OUTER JOIN """ + RxAccumExchangeOwner + """.[PBM_ACCUM_EXCH_TRANS] MA 
         ON MT.TRANS_ID = MA.TRANS_ID
             WHERE MT.TRANS_TYP_CD in ('MedTran') 
                    and MT.TRANS_STTUS_CD IN ('200','202')
                    and MA.TRANS_TYP_CD in ('MedAck')
                    and MA.TRANS_STTUS_CD IN ('200','202')
                    and NULLIF(MA.TRANS_STTUS_CD,'') IS NULL
"""
df_RX_Accum_Exchange = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_RX_Accum_Exchange)
    .load()
)

# Stage: AccumReconExtrTrgLkup_ds (PxDataSet) --> df_AccumReconExtrTrgLkup_ds
df_AccumReconExtrTrgLkup_ds = spark.read.parquet(f"{adls_path}/datasets/AccumReconExtrTrgLkup.parquet")

# Stage: ds_AccumReconPreProcExtr (PxDataSet) --> df_ds_AccumReconPreProcExtr
df_ds_AccumReconPreProcExtr = spark.read.parquet(f"{adls_path}/datasets/AccumReconPreProcExtr.parquet")

# Stage: xfm_calc (CTransformerStage)
df_xfm_calc = df_ds_AccumReconPreProcExtr.select(
    F.col("GRP_TYP_CD").alias("GRP_TYP_CD"),
    F.col("GRP_ID").alias("GRP_ID"),
    F.col("GRP_NM").alias("GRP_NM"),
    F.col("PROD_ID").alias("PROD_ID"),
    F.col("PROD_BILL_CMPNT_PFX").alias("PROD_BILL_CMPNT_PFX"),
    F.col("SUB_ID").alias("SUB_ID"),
    trim(F.col("MBR_ID")).alias("MBR_ID"),  # Expression: Trim(inxfm_calc.MBR_ID)
    F.col("MBR_CK").alias("MBR_CK"),
    F.col("MBR_FIRST_NM").alias("MBR_FIRST_NM"),
    F.col("MBR_LAST_NM").alias("MBR_LAST_NM"),
    F.col("ACCUM_TYP_CD").alias("ACCUM_TYP_CD"),
    F.when(F.col("RECON_IN") == 'N', F.lit("<DSJobStartDate>"))
     .otherwise(F.lit("1900-01-01"))
     .alias("FIRST_DIFF_DT"),  # Expression: If RECON_IN='N' Then DSJobStartDate Else '1900-01-01'
    F.when(F.col("RECON_IN") == 'N', (F.col("MBR_FCTS_TOT_AMT") - F.col("MBR_OPTUMRX_TOT_AMT")))
     .otherwise(F.lit(0.00))
     .alias("FIRST_DIFF_AMT"),  # Expression: If RECON_IN='N' Then difference else 0
    F.col("FCTS_LAST_UPDT_DTM").alias("FCTS_LAST_UPDT_DTM"),
    F.col("MBR_PLN_LMT_NO").alias("MBR_PLN_LMT_NO"),
    F.col("MBR_PLN_TOT_PCT").alias("MBR_PLN_TOT_PCT"),
    F.col("MBR_OPTUMRX_TOT_AMT").alias("MBR_OPTUMRX_TOT_AMT"),
    F.col("MBR_FCTS_TOT_AMT").alias("MBR_FCTS_TOT_AMT"),
    F.col("MBR_DIFF_AMT").alias("MBR_DIFF_AMT"),
    F.col("FMLY_PLN_LMT_NO").alias("FMLY_PLN_LMT_NO"),
    F.col("FMLY_PLN_TOT_PCT").alias("FMLY_PLN_TOT_PCT"),
    F.col("FMLY_OPTUMRX_TOT_AMT").alias("FMLY_OPTUMRX_TOT_AMT"),
    F.col("FMLY_FCTS_TOT_AMT").alias("FMLY_FCTS_TOT_AMT"),
    F.col("FMLY_DIFF_AMT").alias("FMLY_DIFF_AMT"),
    F.col("LAST_UPDT_DTM").alias("LAST_UPDT_DTM"),
    F.col("RECON_IN").alias("RECON_IN"),
    F.col("CAR_ID").alias("CAR_ID"),
    F.col("MBR_ELIG_IN").alias("MBR_ELIG_IN"),
    F.col("MBR_EFF_DT").alias("MBR_EFF_DT"),
    F.col("MBR_TERM_DT").alias("MBR_TERM_DT"),
    F.col("PROD_CT").alias("PROD_CT"),
    F.concat(F.lit("BKC"), F.col("BPL_NBR")).alias("BPL_NBR")  # Expression: 'BKC' : inxfm_calc.BPL_NBR
)

# Stage: Lkup (PxLookup) with three input links: (primary) df_xfm_calc, (lookup) df_RX_Accum_Exchange, (lookup) df_AccumReconExtrTrgLkup_ds

# First, join df_xfm_calc (inlkup) with df_RX_Accum_Exchange (in_lkup_Accum_Xchang) (left join, no join condition → effectively cross join).
# The instructions forbid a cross join with F.lit(True), but DataStage job is specifying a left lookup with no conditions. We must replicate.
df_lkup_temp_1 = df_xfm_calc.alias("inlkup").join(
    df_RX_Accum_Exchange.alias("in_lkup_Accum_Xchang"),
    on=None,
    how="left"
)

# Next, join the result with df_AccumReconExtrTrgLkup_ds (in_lkup_Trg) on MBR_ID and ACCUM_TYP_CD (left join).
df_lkup_temp_2 = df_lkup_temp_1.alias("inlkup").join(
    df_AccumReconExtrTrgLkup_ds.alias("in_lkup_Trg"),
    (F.col("inlkup.MBR_ID") == F.col("in_lkup_Trg.MBR_ID")) & 
    (F.col("inlkup.ACCUM_TYP_CD") == F.col("in_lkup_Trg.ACCUM_TYP_CD")),
    how="left"
)

df_Lkup = df_lkup_temp_2.select(
    F.col("inlkup.GRP_TYP_CD").alias("GRP_TYP_CD"),
    F.col("inlkup.GRP_ID").alias("GRP_ID"),
    F.col("inlkup.GRP_NM").alias("GRP_NM"),
    F.col("inlkup.PROD_ID").alias("PROD_ID"),
    F.col("inlkup.PROD_BILL_CMPNT_PFX").alias("PROD_BILL_CMPNT_PFX"),
    F.col("inlkup.SUB_ID").alias("SUB_ID"),
    F.col("inlkup.MBR_ID").alias("MBR_ID"),
    F.col("inlkup.MBR_CK").alias("MBR_CK"),
    F.col("inlkup.MBR_FIRST_NM").alias("MBR_FIRST_NM"),
    F.col("inlkup.MBR_LAST_NM").alias("MBR_LAST_NM"),
    F.col("inlkup.ACCUM_TYP_CD").alias("ACCUM_TYP_CD"),
    F.col("inlkup.FIRST_DIFF_DT").alias("FIRST_DIFF_DT"),
    F.col("inlkup.FIRST_DIFF_AMT").alias("FIRST_DIFF_AMT"),
    F.col("inlkup.FCTS_LAST_UPDT_DTM").alias("FCTS_LAST_UPDT_DTM"),
    F.col("inlkup.MBR_PLN_LMT_NO").alias("MBR_PLN_LMT_NO"),
    F.col("inlkup.MBR_PLN_TOT_PCT").alias("MBR_PLN_TOT_PCT"),
    F.col("inlkup.MBR_OPTUMRX_TOT_AMT").alias("MBR_OPTUMRX_TOT_AMT"),
    F.col("inlkup.MBR_FCTS_TOT_AMT").alias("MBR_FCTS_TOT_AMT"),
    F.col("inlkup.MBR_DIFF_AMT").alias("MBR_DIFF_AMT"),
    F.col("inlkup.FMLY_PLN_LMT_NO").alias("FMLY_PLN_LMT_NO"),
    F.col("inlkup.FMLY_PLN_TOT_PCT").alias("FMLY_PLN_TOT_PCT"),
    F.col("inlkup.FMLY_OPTUMRX_TOT_AMT").alias("FMLY_OPTUMRX_TOT_AMT"),
    F.col("inlkup.FMLY_FCTS_TOT_AMT").alias("FMLY_FCTS_TOT_AMT"),
    F.col("inlkup.FMLY_DIFF_AMT").alias("FMLY_DIFF_AMT"),
    F.col("inlkup.LAST_UPDT_DTM").alias("LAST_UPDT_DTM"),
    F.col("inlkup.RECON_IN").alias("RECON_IN"),
    F.col("inlkup.CAR_ID").alias("CAR_ID"),
    F.col("inlkup.MBR_ELIG_IN").alias("MBR_ELIG_IN"),
    F.col("inlkup.MBR_EFF_DT").alias("MBR_EFF_DT"),
    F.col("inlkup.MBR_TERM_DT").alias("MBR_TERM_DT"),
    F.col("inlkup.PROD_CT").alias("PROD_CT"),
    F.col("in_lkup_Accum_Xchang.AE_ERR").alias("AE_ERR"),
    F.col("in_lkup_Trg.MBR_ID").alias("TRG_MBR_ID"),
    F.col("in_lkup_Trg.ACCUM_TYP_CD").alias("ACCUM_TYP_CD_1"),
    F.col("in_lkup_Trg.FIRST_DIFF_DT").alias("TRG_FIRST_DIFF_DT"),
    F.col("in_lkup_Trg.FIRST_DIFF_AMT").alias("TRG_FIRST_DIFF_AMT"),
    F.col("in_lkup_Trg.RECON_IN").alias("TRG_RECON_IN")
)

# Stage: xfm_insupd (CTransformerStage)
df_insupd_vars = (
    df_Lkup
    .withColumn(
        "NoUpdate", 
        F.when(
            (F.length(F.col("TRG_MBR_ID")) != 0) & (F.col("RECON_IN") == F.col("TRG_RECON_IN")),
            F.lit("N")
        ).otherwise(F.lit("Y"))
    )
    .withColumn(
        "Update",
        F.when(
            (F.length(F.col("TRG_MBR_ID")) != 0) & (F.col("RECON_IN") != F.col("TRG_RECON_IN")),
            F.lit("Y")
        ).otherwise(F.lit("N"))
    )
)

df_xfm_insupd = df_insupd_vars.select(
    F.col("GRP_TYP_CD").alias("GRP_TYP_CD"),
    F.col("GRP_ID").alias("GRP_ID"),
    F.col("GRP_NM").alias("GRP_NM"),
    F.col("PROD_ID").alias("PROD_ID"),
    F.col("PROD_BILL_CMPNT_PFX").alias("PROD_BILL_CMPNT_PFX"),
    F.col("SUB_ID").alias("SUB_ID"),
    F.col("MBR_ID").alias("MBR_ID"),
    F.col("MBR_CK").alias("MBR_CK"),
    F.col("MBR_FIRST_NM").alias("MBR_FIRST_NM"),
    F.col("MBR_LAST_NM").alias("MBR_LAST_NM"),
    F.col("ACCUM_TYP_CD").alias("ACCUM_TYP_CD"),
    F.when(
        (F.col("AE_ERR").isNull()) | (F.length(F.col("AE_ERR")) == 0),
        F.lit("N")
    ).otherwise(F.col("AE_ERR")).alias("ACCUM_EXCH_ERR"),  # Expression for ACCUM_EXCH_ERR
    F.when(
        F.length(F.col("TRG_MBR_ID")) == 0,
        F.col("FIRST_DIFF_DT")
    ).when(
        (F.col("Update") == 'Y') & (F.col("RECON_IN") == 'Y'),
        F.col("FIRST_DIFF_DT")
    ).when(
        (F.col("Update") == 'Y') & (F.col("RECON_IN") == 'N'),
        F.col("FIRST_DIFF_DT")
    ).when(
        (F.col("NoUpdate") == 'N') & (F.col("RECON_IN") == 'Y'),
        F.col("TRG_FIRST_DIFF_DT")
    ).when(
        (F.col("NoUpdate") == 'N') & (F.col("RECON_IN") == 'N'),
        F.col("TRG_FIRST_DIFF_DT")
    ).otherwise(F.lit("2199-12-31"))
    .alias("FIRST_DIFF_DT"),
    F.when(
        F.length(F.col("TRG_MBR_ID")) == 0,
        F.col("FIRST_DIFF_AMT")
    ).when(
        (F.col("Update") == 'Y') & (F.col("RECON_IN") == 'Y'),
        F.col("FIRST_DIFF_AMT")
    ).when(
        (F.col("Update") == 'Y') & (F.col("RECON_IN") == 'N'),
        F.col("FIRST_DIFF_AMT")
    ).when(
        (F.col("NoUpdate") == 'N') & (F.col("RECON_IN") == 'Y'),
        F.col("TRG_FIRST_DIFF_AMT")
    ).when(
        (F.col("NoUpdate") == 'N') & (F.col("RECON_IN") == 'N'),
        F.col("TRG_FIRST_DIFF_AMT")
    ).otherwise(F.lit("999999999.00"))
    .alias("FIRST_DIFF_AMT"),
    F.col("FCTS_LAST_UPDT_DTM").alias("FCTS_LAST_UPDT_DTM"),
    F.col("MBR_PLN_LMT_NO").alias("MBR_PLN_LMT_NO"),
    F.col("MBR_PLN_TOT_PCT").alias("MBR_PLN_TOT_PCT"),
    F.col("MBR_OPTUMRX_TOT_AMT").alias("MBR_OPTUMRX_TOT_AMT"),
    F.col("MBR_FCTS_TOT_AMT").alias("MBR_FCTS_TOT_AMT"),
    F.col("MBR_DIFF_AMT").alias("MBR_DIFF_AMT"),
    F.col("FMLY_PLN_LMT_NO").alias("FMLY_PLN_LMT_NO"),
    F.col("FMLY_PLN_TOT_PCT").alias("FMLY_PLN_TOT_PCT"),
    F.col("FMLY_OPTUMRX_TOT_AMT").alias("FMLY_OPTUMRX_TOT_AMT"),
    F.col("FMLY_FCTS_TOT_AMT").alias("FMLY_FCTS_TOT_AMT"),
    F.col("FMLY_DIFF_AMT").alias("FMLY_DIFF_AMT"),
    F.col("LAST_UPDT_DTM").alias("LAST_UPDT_DTM"),
    F.col("RECON_IN").alias("RECON_IN"),
    F.col("CAR_ID").alias("CAR_ID"),
    F.col("MBR_ELIG_IN").alias("MBR_ELIG_IN"),
    F.col("MBR_EFF_DT").alias("MBR_EFF_DT"),
    F.col("MBR_TERM_DT").alias("MBR_TERM_DT"),
    F.col("PROD_CT").alias("PROD_CT")
)

# Stage: AccumReconExtr (PxSequentialFile) - writing to AccumReconExtr.dat
# Apply rpad for char columns: ACCUM_EXCH_ERR (length 18), MBR_ELIG_IN (length 1)
df_final = (
    df_xfm_insupd
    .withColumn("ACCUM_EXCH_ERR", F.rpad(F.col("ACCUM_EXCH_ERR"), 18, " "))
    .withColumn("MBR_ELIG_IN", F.rpad(F.col("MBR_ELIG_IN"), 1, " "))
)

write_files(
    df_final,
    f"{adls_path}/load/AccumReconExtr.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)