# Databricks notebook source
# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

# COMMAND ----------
"""
***************************************************************************************************************************************************************
COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 


DESCRIPTION:   Shared container used for Primary Keying of Family Accumulator job

CALLED BY : FctsFmlyAccumExtr

PROCESSING:    
      


MODIFICATIONS:
Developer           Date                         Change Description                                                     Project/Altius #          Development Project          Code Reviewer          Date Reviewed  
-----------------------    ----------------------------     ----------------------------------------------------------------------------        ---------------------------     ------------------------------------       ----------------------------      -------------------------
Parik                 2008-08-27              Initial program                                                                             3567(Primary Key)               devlIDS          Steph Goddard          09/02/2008
Ralph Tucker   2008-12-16               Added two field (Carovr_amt)                                         3648(Labor Accts)     devlIDSnew  

Karthik Chintalapani        2016-11-11               5634      Added new columns PLN_YR_EFF_DT and  PLN_YR_END_DT   IntegrateDev2     Kalyan Neelam         2016-11-28
"""

# This container is used in:
# FctsFmlyAccumExtr
#
# Hash file (hf_fmly_accum_allcol) cleared in calling program
# join primary key info with table info
# update primary key table (K_FMLY_ACCUM) with new keys created today
# primary key hash file only contains current run keys and is cleared before writing
# SQL joins temp table with key table to assign known keys
# Temp table is tuncated before load and runstat done after load
# Load IDS temp. table

# COMMAND ----------
from typing import Tuple
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# COMMAND ----------
def run_FmlyAccumPK(
    df_AllCol: DataFrame,
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    """
    Executes the logic of the FmlyAccumPK shared container.

    Parameters
    ----------
    df_AllCol : DataFrame
        Input stream that corresponds to the 'AllCol' link.
    df_Transform : DataFrame
        Input stream that corresponds to the 'Transform' link.
    params : dict
        Runtime parameters and externalized configuration values.

    Returns
    -------
    DataFrame
        Output stream corresponding to the 'Key' link.
    """

    # ------------------------------------------------------------------
    # Unpack parameters exactly once
    # ------------------------------------------------------------------
    IDSOwner            = params["IDSOwner"]
    CurrRunCycle        = params["CurrRunCycle"]
    SrcSysCd            = params["SrcSysCd"]

    ids_jdbc_url        = params["ids_jdbc_url"]
    ids_jdbc_props      = params["ids_jdbc_props"]

    adls_path           = params["adls_path"]
    adls_path_raw       = params["adls_path_raw"]
    adls_path_publish   = params["adls_path_publish"]
    # ------------------------------------------------------------------
    # Stage: hf_fmly_accum_allcol  (Scenario a – intermediate hash file)
    # Replace by in-memory deduplication
    # ------------------------------------------------------------------
    key_cols_allcol = [
        "SRC_SYS_CD_SK",
        "SUB_UNIQ_KEY",
        "PROD_ACCUM_ID",
        "FMLY_ACCUM_TYP_CD",
        "ACCUM_NO",
        "YR_NO"
    ]
    df_AllCol_out = dedup_sort(
        df_AllCol,
        key_cols_allcol,
        []                      # no specific sort columns
    )

    # ------------------------------------------------------------------
    # Stage: K_FMLY_ACCUM_TEMP  (DB2 Connector)
    # Write df_Transform to temp table, then extract W_Extract
    # ------------------------------------------------------------------
    execute_dml(
        f"DELETE FROM {IDSOwner}.K_FMLY_ACCUM_TEMP",
        ids_jdbc_url,
        ids_jdbc_props
    )

    (
        df_Transform
        .write
        .format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("dbtable", f"{IDSOwner}.K_FMLY_ACCUM_TEMP")
        .mode("overwrite")
        .save()
    )

    execute_dml(
        f"CALL SYSPROC.ADMIN_CMD('runstats on table {IDSOwner}.K_FMLY_ACCUM_TEMP on key columns with distribution on key columns and detailed indexes all allow write access')",
        ids_jdbc_url,
        ids_jdbc_props
    )

    extract_query = f"""
        SELECT
               k.FMLY_ACCUM_SK,
               w.SRC_SYS_CD_SK,
               w.SUB_UNIQ_KEY,
               w.PROD_ACCUM_ID,
               w.FMLY_ACCUM_TYP_CD,
               w.ACCUM_NO,
               w.YR_NO,
               k.CRT_RUN_CYC_EXCTN_SK
        FROM {IDSOwner}.K_FMLY_ACCUM_TEMP w
        JOIN {IDSOwner}.K_FMLY_ACCUM k
          ON w.SRC_SYS_CD_SK     = k.SRC_SYS_CD_SK
         AND w.SUB_UNIQ_KEY      = k.SUB_UNIQ_KEY
         AND w.PROD_ACCUM_ID     = k.PROD_ACCUM_ID
         AND w.FMLY_ACCUM_TYP_CD = k.FMLY_ACCUM_TYP_CD
         AND w.ACCUM_NO          = k.ACCUM_NO
         AND w.YR_NO             = k.YR_NO
        UNION
        SELECT
               -1,
               w2.SRC_SYS_CD_SK,
               w2.SUB_UNIQ_KEY,
               w2.PROD_ACCUM_ID,
               w2.FMLY_ACCUM_TYP_CD,
               w2.ACCUM_NO,
               w2.YR_NO,
               {CurrRunCycle}
        FROM {IDSOwner}.K_FMLY_ACCUM_TEMP w2
        WHERE NOT EXISTS (
            SELECT 1
            FROM {IDSOwner}.K_FMLY_ACCUM k2
            WHERE w2.SRC_SYS_CD_SK     = k2.SRC_SYS_CD_SK
              AND w2.SUB_UNIQ_KEY      = k2.SUB_UNIQ_KEY
              AND w2.PROD_ACCUM_ID     = k2.PROD_ACCUM_ID
              AND w2.FMLY_ACCUM_TYP_CD = k2.FMLY_ACCUM_TYP_CD
              AND w2.ACCUM_NO          = k2.ACCUM_NO
              AND w2.YR_NO             = k2.YR_NO
        )
    """

    df_W_Extract = (
        spark.read.format("jdbc")
             .option("url", ids_jdbc_url)
             .options(**ids_jdbc_props)
             .option("query", extract_query)
             .load()
    )

    # ------------------------------------------------------------------
    # Stage: PrimaryKey (Transformer)
    # ------------------------------------------------------------------
    df_enriched = (
        df_W_Extract
        .withColumn(
            "svInstUpdt",
            F.when(F.col("FMLY_ACCUM_SK") == F.lit(-1), F.lit("I")).otherwise(F.lit("U"))
        )
        .withColumn("svSrcSysCd", F.lit(SrcSysCd))
        .withColumn(
            "svCrtRunCycExctnSk",
            F.when(F.col("svInstUpdt") == F.lit("I"), F.lit(CurrRunCycle))
             .otherwise(F.col("CRT_RUN_CYC_EXCTN_SK"))
        )
    )

    # SurrogateKeyGen replacement for KeyMgtGetNextValueConcurrent
    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"FMLY_ACCUM_SK",<schema>,<secret_name>)

    # Prepare outputs from PrimaryKey transformer
    df_updt = (
        df_enriched.select(
            F.col("svSrcSysCd").alias("SRC_SYS_CD"),
            "SUB_UNIQ_KEY",
            "PROD_ACCUM_ID",
            "FMLY_ACCUM_TYP_CD",
            "ACCUM_NO",
            "YR_NO",
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            "FMLY_ACCUM_SK"
        )
    )

    df_NewKeys = (
        df_enriched
        .filter(F.col("svInstUpdt") == F.lit("I"))
        .select(
            "SRC_SYS_CD_SK",
            "SUB_UNIQ_KEY",
            "PROD_ACCUM_ID",
            "FMLY_ACCUM_TYP_CD",
            "ACCUM_NO",
            "YR_NO",
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            "FMLY_ACCUM_SK"
        )
    )

    df_Keys = (
        df_enriched.select(
            "FMLY_ACCUM_SK",
            "SRC_SYS_CD_SK",
            F.col("svSrcSysCd").alias("SRC_SYS_CD"),
            "SUB_UNIQ_KEY",
            "PROD_ACCUM_ID",
            "FMLY_ACCUM_TYP_CD",
            "ACCUM_NO",
            "YR_NO",
            F.col("svInstUpdt").alias("INSRT_UPDT_CD"),
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK")
        )
    )

    # ------------------------------------------------------------------
    # Stage: hf_fmly_accum  (Scenario c – write parquet)
    # ------------------------------------------------------------------
    write_files(
        df_updt,
        f"{adls_path}/FmlyAccumPK_updt.parquet",
        delimiter=",",
        mode="overwrite",
        is_pqruet=True,
        header=True,
        quote="\"",
        nullValue=None
    )

    # ------------------------------------------------------------------
    # Stage: K_FMLY_ACCUM  (Sequential file)
    # ------------------------------------------------------------------
    write_files(
        df_NewKeys,
        f"{adls_path}/load/K_FMLY_ACCUM.dat",
        delimiter=",",
        mode="overwrite",
        is_pqruet=False,
        header=True,
        quote="\"",
        nullValue=None
    )

    # ------------------------------------------------------------------
    # Stage: Merge (Transformer)
    # ------------------------------------------------------------------
    join_expr = (
        (F.col("AllColOut.SRC_SYS_CD_SK")   == F.col("Keys.SRC_SYS_CD_SK"))   &
        (F.col("AllColOut.SUB_UNIQ_KEY")    == F.col("Keys.SUB_UNIQ_KEY"))    &
        (F.col("AllColOut.PROD_ACCUM_ID")   == F.col("Keys.PROD_ACCUM_ID"))   &
        (F.col("AllColOut.FMLY_ACCUM_TYP_CD") == F.col("Keys.FMLY_ACCUM_TYP_CD")) &
        (F.col("AllColOut.ACCUM_NO")        == F.col("Keys.ACCUM_NO"))        &
        (F.col("AllColOut.YR_NO")           == F.col("Keys.YR_NO"))
    )

    df_Key = (
        df_AllCol_out.alias("AllColOut")
        .join(df_Keys.alias("Keys"), join_expr, "left")
        .select(
            "AllColOut.JOB_EXCTN_RCRD_ERR_SK",
            "Keys.INSRT_UPDT_CD",
            "AllColOut.DISCARD_IN",
            "AllColOut.PASS_THRU_IN",
            "AllColOut.FIRST_RECYC_DT",
            "AllColOut.ERR_CT",
            "AllColOut.RECYCLE_CT",
            "Keys.SRC_SYS_CD",
            "AllColOut.PRI_KEY_STRING",
            "Keys.FMLY_ACCUM_SK",
            "AllColOut.SUB_UNIQ_KEY",
            "AllColOut.PROD_ACCUM_ID",
            "AllColOut.FMLY_ACCUM_TYP_CD",
            "AllColOut.ACCUM_NO",
            "AllColOut.YR_NO",
            "Keys.CRT_RUN_CYC_EXCTN_SK",
            F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            "AllColOut.GRGR_ID",
            "AllColOut.SUB_SK",
            "AllColOut.ACCUM_AMT",
            "AllColOut.CAROVR_AMT",
            "AllColOut.PLN_YR_EFF_DT",
            "AllColOut.PLN_YR_END_DT"
        )
    )

    # ------------------------------------------------------------------
    # Return container output
    # ------------------------------------------------------------------
    return df_Key

# COMMAND ----------