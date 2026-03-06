
# Databricks notebook source
# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from typing import Tuple, Dict


# COMMAND ----------
def run_DrugClmPK(
    df_DrugClmCrfIn: DataFrame,
    params: Dict
) -> Tuple[DataFrame, DataFrame, DataFrame, DataFrame]:
    """
    Shared-Container: DrugClmPK
    COPYRIGHT 2005–2020 BLUE CROSS/BLUE SHIELD OF KANSAS CITY

    PROCESSING:
        Primary keying for drug claims, NDC, DEA, pharmacy, provider, and provider-location.
        Generates new surrogate keys when look-ups against the hashed-file (now dummy-table)
        repositories miss, and returns the properly keyed records to the caller.

    MODIFICATIONS:  (see original DataStage job description for full audit history)
    """

    # ------------------------------------------------------------------
    # 1. Parameter unpacking
    # ------------------------------------------------------------------
    RunCycle         = params["RunCycle"]
    ProvRunCycle     = params["ProvRunCycle"]

    IDSOwner         = params["IDSOwner"]
    ids_secret_name  = params["ids_secret_name"]

    ids_jdbc_url     = params["ids_jdbc_url"]
    ids_jdbc_props   = params["ids_jdbc_props"]

    adls_path        = params["adls_path"]
    adls_path_raw    = params["adls_path_raw"]
    adls_path_publish= params["adls_path_publish"]

    # ------------------------------------------------------------------
    # 2. Dummy-table JDBC look-ups replacing read-modify-write hash files
    # ------------------------------------------------------------------
    #   hf_prov            -> dummy_hf_prov
    #   hf_prov_dea        -> dummy_hf_prov_dea
    #   hf_ndc             -> dummy_hf_ndc
    #   hf_prov_loc        -> dummy_hf_prov_loc
    #
    # ------------------------------------------------------------------
    def _read_dummy(table_nm: str) -> DataFrame:
        return (
            spark.read.format("jdbc")
            .option("url", ids_jdbc_url)
            .options(**ids_jdbc_props)
            .option("dbtable", table_nm)
            .load()
        )

    df_hf_prov      = _read_dummy("dummy_hf_prov")
    df_hf_prov_dea  = _read_dummy("dummy_hf_prov_dea")
    df_hf_ndc       = _read_dummy("dummy_hf_ndc")
    df_hf_prov_loc  = _read_dummy("dummy_hf_prov_loc")

    # ------------------------------------------------------------------
    # 3. Auxiliary reference sets that were formerly intermediate hash files
    # ------------------------------------------------------------------
    # CMN_PRCT (DB2)
    extract_query_cmn_prct = f"""
        SELECT CMN_PRCT_ID ,
               FIRST_NM ,
               LAST_NM ,
               MIDINIT
        FROM   {IDSOwner}.CMN_PRCT
    """
    df_cmn_prct = (
        spark.read.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("query", extract_query_cmn_prct)
        .load()
    )

    # CMN_PRCT_1 (DB2)
    extract_query_cmn_prct_1 = f"""
        SELECT  NTNL_PROV_ID ,
                MIN(CMN_PRCT_ID) AS CMN_PRCT_ID
        FROM    {IDSOwner}.CMN_PRCT
        WHERE   NTNL_PROV_ID NOT IN ('NA','UNK')
        GROUP BY NTNL_PROV_ID
    """
    df_cmn_prct_npi = (
        spark.read.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("query", extract_query_cmn_prct_1)
        .load()
    )

    # IDS_PROV  (used for NTNL_PROV_ID look-up)
    extract_query_ids_prov = f"""
        SELECT  PROV.NTNL_PROV_ID
        FROM    {IDSOwner}.PROV        PROV ,
                {IDSOwner}.CD_MPPNG   MPPNG
        WHERE   PROV.SRC_SYS_CD_SK = MPPNG.CD_MPPNG_SK
          AND   MPPNG.SRC_CD = 'NABP'
    """
    df_ids_prov = (
        spark.read.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("query", extract_query_ids_prov)
        .load()
    )

    # Visual Cactus (ODBC)  →  turned into pre-staged parquet
    # Here we assume a parquet holding the DEA→CMN_PRCT mapping already exists
    df_vcac_dea = spark.read.parquet(f"{adls_path}/DrugClmPK_vcac_dea.parquet")

    # ------------------------------------------------------------------
    # 4. Core enrichment logic (high-level translation of PkeyTrn & PkeyNDCDea)
    # ------------------------------------------------------------------
    df_enriched = (
        df_DrugClmCrfIn.alias("src")
        # ------------------------------------------------------------------
        # Existing claim surrogate-key / run-cycle look-up (hf_clm)
        # ------------------------------------------------------------------
        .join(
            spark.read.parquet(f"{adls_path}/DrugClmPK_hf_clm.parquet").alias("clm"),
            on=[
                F.trim(F.col("src.SRC_SYS_CD")) == F.col("clm.SRC_SYS_CD"),
                F.col("src.CLM_ID") == F.col("clm.CLM_ID")
            ],
            how="left"
        )
        # ------------------------------------------------------------------
        # Provider surrogate-key look-up
        # ------------------------------------------------------------------
        .join(
            df_hf_prov.alias("prov"),
            on=[
                F.trim(F.col("src.SRC_SYS_CD")) == F.col("prov.SRC_SYS_CD"),
                F.trim(F.col("src.PROV_ID"))    == F.col("prov.PROV_ID")
            ],
            how="left"
        )
        # ------------------------------------------------------------------
        # DEA surrogate-key look-up
        # ------------------------------------------------------------------
        .join(
            df_hf_prov_dea.alias("dea"),
            on=F.trim(F.col("src.PRSCRB_PROV_DEA")) == F.col("dea.DEA_NO"),
            how="left"
        )
        # ------------------------------------------------------------------
        # NDC surrogate-key look-up
        # ------------------------------------------------------------------
        .join(
            df_hf_ndc.alias("ndc"),
            on=F.trim(F.col("src.NDC")) == F.col("ndc.NDC_CD"),
            how="left"
        )
        # ------------------------------------------------------------------
        # Provider-Location surrogate-key look-up
        # ------------------------------------------------------------------
        .join(
            df_hf_prov_loc.alias("pl"),
            on=[
                F.lit("NABP")                   == F.col("pl.SRC_SYS_CD"),
                F.trim(F.col("src.PROV_ID"))    == F.col("pl.PROV_ID"),
                F.trim(F.col("src.PROV_ID"))    == F.col("pl.PROV_ADDR_ID"),
                F.lit("P")                      == F.col("pl.PROV_ADDR_TYP_CD"),
                F.lit("1753-01-01")             == F.col("pl.PROV_ADDR_EFF_DT")
            ],
            how="left"
        )
        # ------------------------------------------------------------------
        # bring along other reference-data look-ups
        # ------------------------------------------------------------------
        .join(df_cmn_prct.alias("cmp"), on=F.lit(True), how="left")          # placeholder
        .join(df_cmn_prct_npi.alias("npi"), on=F.lit(True), how="left")      # placeholder
        .join(df_ids_prov.alias("nprov"), on=F.lit(True), how="left")        # placeholder
    )

    # ------------------------------------------------------------------
    # 4.a  Derive surrogate keys using SurrogateKeyGen
    # ------------------------------------------------------------------
    df_enriched = SurrogateKeyGen(df_enriched, <DB sequence name>, "DRUG_CLM_SK", <schema>, <secret_name>)  # Drug-Claim
    df_enriched = SurrogateKeyGen(df_enriched, <DB sequence name>, "NDC_CD_SK",   <schema>, <secret_name>)  # NDC
    df_enriched = SurrogateKeyGen(df_enriched, <DB sequence name>, "PROV_DEA_SK", <schema>, <secret_name>)  # DEA
    df_enriched = SurrogateKeyGen(df_enriched, <DB sequence name>, "PROV_SK",     <schema>, <secret_name>)  # Provider
    df_enriched = SurrogateKeyGen(df_enriched, <DB sequence name>, "PROV_LOC_SK", <schema>, <secret_name>)  # Provider-Loc

    # ------------------------------------------------------------------
    # 4.b  Business-rule columns (partial translation; many columns default or pass-thru)
    # ------------------------------------------------------------------
    df_enriched = (
        df_enriched
        .withColumn("CRT_RUN_CYC_EXCTN_SK"      , F.lit(RunCycle))
        .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(RunCycle))
        # add additional withColumn(...) calls here to replicate every
        # DataStage derivation exactly as required
    )

    # ------------------------------------------------------------------
    # 5. Split into final output streams (DrugClm, NDC, Provider, PROV_LOC)
    # ------------------------------------------------------------------
    df_DrugClm_out = df_enriched.select([c for c in df_enriched.columns])  # placeholder-pass-thru
    df_NDC_out     = df_enriched.filter("NDC_CD_SK IS NOT NULL")           # placeholder-filter
    df_Prov_out    = df_enriched.filter("PROV_SK IS NOT NULL")
    df_ProvLoc_out = df_enriched.filter("PROV_LOC_SK IS NOT NULL")

    # ------------------------------------------------------------------
    # 6. Write-backs to dummy tables (formerly hash-file insert stages)
    # ------------------------------------------------------------------
    def _write_dummy(df: DataFrame, table_nm: str) -> None:
        (
            df.write
            .mode("append")
            .format("jdbc")
            .option("url", ids_jdbc_url)
            .options(**ids_jdbc_props)
            .option("dbtable", table_nm)
            .save()
        )

    _write_dummy(
        df_enriched
        .filter("PROV_SK IS NOT NULL AND prov.PROV_SK IS NULL")
        .select("SRC_SYS_CD", "PROV_ID", "CRT_RUN_CYC_EXCTN_SK", "PROV_SK"),
        "dummy_hf_prov"
    )

    _write_dummy(
        df_enriched
        .filter("PROV_DEA_SK IS NOT NULL AND dea.PROV_DEA_SK IS NULL")
        .select("DEA_NO", "CRT_RUN_CYC_EXCTN_SK", "PROV_DEA_SK"),
        "dummy_hf_prov_dea"
    )

    _write_dummy(
        df_enriched
        .filter("NDC_CD_SK IS NOT NULL AND ndc.NDC_CD_SK IS NULL")
        .select("NDC_CD", "CRT_RUN_CYC_EXCTN_SK", "NDC_CD_SK"),
        "dummy_hf_ndc"
    )

    _write_dummy(
        df_enriched
        .filter("PROV_LOC_SK IS NOT NULL AND pl.PROV_LOC_SK IS NULL")
        .select(
            F.lit("NABP"       ).alias("SRC_SYS_CD"),
            F.col("PROV_ID")   .alias("PROV_ID"),
            F.col("PROV_ID")   .alias("PROV_ADDR_ID"),
            F.lit("P")         .alias("PROV_ADDR_TYP_CD"),
            F.lit("1753-01-01").alias("PROV_ADDR_EFF_DT"),
            F.lit(ProvRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
            F.col("PROV_LOC_SK")
        ),
        "dummy_hf_prov_loc"
    )

    # ------------------------------------------------------------------
    # 7. Return final DataFrame(s) to the caller
    # ------------------------------------------------------------------
    return (
        df_DrugClm_out,     # DrugClm
        df_NDC_out,         # NDC
        df_Prov_out,        # Provider
        df_ProvLoc_out      # Provider-Location
    )
