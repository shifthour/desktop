# Databricks notebook source
# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def run_ITSClmPK(df_Transform: DataFrame, params: dict) -> DataFrame:
    """
    Shared Container: ITSClmPK
    Description:
        Takes the sorted file and applies the primary key, prepping the file for the foreign-key lookup process that follows.
    
    Annotations:
        ITS Claim Primary Key
        This job uses hf_clm as the primary key hash file since it has the same natural key as the CLM table
        This container is used in:
            ITSClmExtr
            NascoITSClmTrns
        These programs need to be re-compiled when logic changes
    """
    # -----------------------------------------------------------
    # Unpack runtime parameters (once only)
    CurrRunCycle = params["CurrRunCycle"]
    adls_path    = params["adls_path"]
    # -----------------------------------------------------------
    # Read lookup hash file (scenario c → parquet)
    lkup_path = f"{adls_path}/ITSClmPK_lkup.parquet"
    df_lkup = spark.read.parquet(lkup_path)
    # -----------------------------------------------------------
    # Join input stream with lookup
    join_cond = [
        F.col("t.SRC_SYS_CD") == F.col("lk.SRC_SYS_CD"),
        F.col("t.CLM_ID")     == F.col("lk.CLM_ID")
    ]
    df_joined = (
        df_Transform.alias("t")
        .join(df_lkup.alias("lk"), join_cond, "left")
    )
    # -----------------------------------------------------------
    # Derive columns
    df_enriched = (
        df_joined
        .withColumn(
            "ITS_CLM_SK",
            F.when(F.col("lk.CLM_SK").isNull(), F.lit(0)).otherwise(F.col("lk.CLM_SK"))
        )
        .withColumn(
            "CRT_RUN_CYC_EXCTN_SK",
            F.when(F.col("lk.CLM_SK").isNull(), F.lit(CurrRunCycle)).otherwise(F.col("lk.CRT_RUN_CYC_EXCTN_SK"))
        )
        .withColumn(
            "LAST_UPDT_RUN_CYC_EXCTN_SK",
            F.lit(CurrRunCycle)
        )
        .withColumn(
            "SCCF_NO",
            F.when(
                F.length(F.trim(F.col("t.SCCF_NO"))) == 0,
                F.lit(" ")
            ).otherwise(F.col("t.SCCF_NO"))
        )
    )
    # -----------------------------------------------------------
    # Surrogate-key generation
    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,'ITS_CLM_SK',<schema>,<secret_name>)
    # -----------------------------------------------------------
    # Select final output columns
    df_key = df_enriched.select(
        "JOB_EXCTN_RCRD_ERR_SK",
        "INSRT_UPDT_CD",
        "DISCARD_IN",
        "PASS_THRU_IN",
        "FIRST_RECYC_DT",
        "ERR_CT",
        "RECYCLE_CT",
        "SRC_SYS_CD",
        "PRI_KEY_STRING",
        "ITS_CLM_SK",
        "CLM_ID",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "TRNSMSN_SRC_CD",
        "CFA_DISP_PD_DT",
        "DISP_FMT_PD_DT_SK",
        "ACES_FEE_AMT",
        "ADM_FEE_AMT",
        "DRG_AMT",
        "SRCHRG_AMT",
        "SRPLS_AMT",
        "SCCF_NO",
        "CLMI_INVEST_IND",
        "CLMI_INVEST_DAYS",
        "CLMI_INVEST_BEG_DT",
        "CLMI_INVEST_END_DT",
        "SUPLMT_DSCNT_AMT"
    )
    return df_key