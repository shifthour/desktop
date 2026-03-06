# Databricks notebook source
# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

"""
JobName       : JrnlEntryTransPK
FolderPath    : Shared Containers/PrimaryKey
JobType       : Server Job
JobCategory   : DS_Integrate

DESCRIPTION:
Shared container used for Primary Keying of Journal Entry Transaction jobs

CALLED BY :
PSJrnlEntryCapExtr, PSJrnlEntryClmExtr, PSJrnlEntryComsnExtr,
PSJrnlEntryDrugExtr and PSJrnlEntryIncmExtr

PROCESSING NOTES:
  • join primary key info with table info
  • update primary key table (K_JRNL_ENTRY_TRANS) with new keys created today
  • primary key hash file only contains current run keys and is cleared before writing
  • SQL joins temp table with key table to assign known keys
  • Temp table is truncated before load and runstats done after load
  • Load IDS temp. table

ANNOTATIONS:
This container is used in:
  – PSJrnlEntryCapExtr
  – PSJrnlEntryClmExtr
  – PSJrnlEntryComsnExtr
  – PSJrnlEntryDrugExtr
  – PSJrnlEntryIncmExtr

Hash file (hf_jrnl_entry_trans_allcol) cleared in calling program
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, when


def run_JrnlEntryTransPK(
    df_AllCol: DataFrame,
    df_Transform: DataFrame,
    params: dict,
) -> DataFrame:
    """
    Executes the logic of the JrnlEntryTransPK shared container.

    Parameters
    ----------
    df_AllCol   : DataFrame
        Input stream corresponding to link “AllCol”.
    df_Transform: DataFrame
        Input stream corresponding to link “Transform”.
    params      : dict
        Runtime parameters and connection information.

    Returns
    -------
    DataFrame
        Output stream corresponding to link “Key”.
    """

    # ------------------------------------------------------------------
    # Un-pack required run-time parameters (exactly once)
    # ------------------------------------------------------------------
    IDSOwner = params["IDSOwner"]
    ids_secret_name = params["ids_secret_name"]
    CurrRunCycle = params["CurrRunCycle"]
    SrcSysCd = params["SrcSysCd"]

    ids_jdbc_url = params["ids_jdbc_url"]
    ids_jdbc_props = params["ids_jdbc_props"]

    adls_path = params["adls_path"]
    adls_path_raw = params["adls_path_raw"]
    adls_path_publish = params["adls_path_publish"]

    # ------------------------------------------------------------------
    # Hash-file “hf_jrnl_entry_trans_allcol” – scenario a (intermediate)
    # Replace with de-duplication on key columns
    # ------------------------------------------------------------------
    df_AllColOut = dedup_sort(
        df_AllCol,
        ["SRC_SYS_CD_SK", "SRC_TRANS_CK", "SRC_TRANS_TYP_CD", "ACCTG_DT_SK"],
        []
    )

    # ------------------------------------------------------------------
    # Stage: K_JRNL_ENTRY_TRANS_TEMP (write to temp table)
    # ------------------------------------------------------------------
    truncate_sql = f"TRUNCATE TABLE {IDSOwner}.K_JRNL_ENTRY_TRANS_TEMP"
    execute_dml(truncate_sql, ids_jdbc_url, ids_jdbc_props)

    (
        df_Transform.write.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("dbtable", f"{IDSOwner}.K_JRNL_ENTRY_TRANS_TEMP")
        .mode("append")
        .save()
    )

    runstats_sql = (
        f"CALL SYSPROC.ADMIN_CMD('runstats on table {IDSOwner}.K_JRNL_ENTRY_TRANS_TEMP "
        "on key columns with distribution on key columns and detailed indexes all allow write access')"
    )
    execute_dml(runstats_sql, ids_jdbc_url, ids_jdbc_props)

    # ------------------------------------------------------------------
    # Stage: K_JRNL_ENTRY_TRANS_TEMP (read W_Extract)
    # ------------------------------------------------------------------
    extract_query = f"""
        SELECT k.JRNL_ENTRY_TRANS_SK,
               w.SRC_SYS_CD_SK,
               w.SRC_TRANS_CK,
               w.SRC_TRANS_TYP_CD,
               w.ACCTG_DT_SK,
               k.CRT_RUN_CYC_EXCTN_SK
          FROM {IDSOwner}.K_JRNL_ENTRY_TRANS_TEMP w,
               {IDSOwner}.K_JRNL_ENTRY_TRANS      k
         WHERE w.SRC_SYS_CD_SK   = k.SRC_SYS_CD_SK
           AND w.SRC_TRANS_CK    = k.SRC_TRANS_CK
           AND w.SRC_TRANS_TYP_CD= k.SRC_TRANS_TYP_CD
           AND w.ACCTG_DT_SK     = k.ACCTG_DT_SK
        UNION
        SELECT -1,
               w2.SRC_SYS_CD_SK,
               w2.SRC_TRANS_CK,
               w2.SRC_TRANS_TYP_CD,
               w2.ACCTG_DT_SK,
               {CurrRunCycle}
          FROM {IDSOwner}.K_JRNL_ENTRY_TRANS_TEMP w2
         WHERE NOT EXISTS (
               SELECT 1
                 FROM {IDSOwner}.K_JRNL_ENTRY_TRANS k2
                WHERE w2.SRC_SYS_CD_SK    = k2.SRC_SYS_CD_SK
                  AND w2.SRC_TRANS_CK     = k2.SRC_TRANS_CK
                  AND w2.SRC_TRANS_TYP_CD = k2.SRC_TRANS_TYP_CD
                  AND w2.ACCTG_DT_SK      = k2.ACCTG_DT_SK )
    """

    df_W_Extract = (
        spark.read.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("query", extract_query)
        .load()
    )

    # ------------------------------------------------------------------
    # Transformer: PrimaryKey
    # ------------------------------------------------------------------
    df_enriched = (
        df_W_Extract
        .withColumn("svInstUpdt", when(col("JRNL_ENTRY_TRANS_SK") == -1, lit("I")).otherwise(lit("U")))
        .withColumn("svSrcSysCd", lit(SrcSysCd))
        .withColumn(
            "JRNL_ENTRY_TRANS_SK",
            when(col("svInstUpdt") == lit("I"), F.lit(None).cast("long")).otherwise(col("JRNL_ENTRY_TRANS_SK"))
        )
        .withColumn(
            "svCrtRunCycExctnSk",
            when(col("svInstUpdt") == lit("I"), lit(CurrRunCycle).cast("long")).otherwise(col("CRT_RUN_CYC_EXCTN_SK"))
        )
    )

    # SurrogateKeyGen invocation (per rules – parameters unresolved except key_col)
    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"JRNL_ENTRY_TRANS_SK",<schema>,<secret_name>)

    # “updt” link dataframe
    df_updt = (
        df_enriched.select(
            col("svSrcSysCd").alias("SRC_SYS_CD"),
            col("SRC_TRANS_CK"),
            col("SRC_TRANS_TYP_CD"),
            col("ACCTG_DT_SK"),
            col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            col("JRNL_ENTRY_TRANS_SK")
        )
    )

    # “NewKeys” link dataframe (only Inserts)
    df_NewKeys = (
        df_enriched.filter(col("svInstUpdt") == lit("I")).select(
            col("SRC_SYS_CD_SK"),
            col("SRC_TRANS_CK"),
            col("SRC_TRANS_TYP_CD"),
            col("ACCTG_DT_SK"),
            col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            col("JRNL_ENTRY_TRANS_SK")
        )
    )

    # “Keys” link dataframe
    df_Keys = (
        df_enriched.select(
            col("JRNL_ENTRY_TRANS_SK"),
            col("SRC_SYS_CD_SK"),
            col("svSrcSysCd").alias("SRC_SYS_CD"),
            col("SRC_TRANS_CK"),
            col("SRC_TRANS_TYP_CD"),
            col("ACCTG_DT_SK"),
            col("svInstUpdt").alias("INSRT_UPDT_CD"),
            col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK")
        )
    )

    # ------------------------------------------------------------------
    # Target: hf_jrnl_entry_trans (hashed file – scenario c → parquet)
    # ------------------------------------------------------------------
    write_files(
        df_updt,
        f"{adls_path}/JrnlEntryTransPK_updt.parquet",
        ",",
        "overwrite",
        True,
        True,
        "\"",
        None
    )

    # ------------------------------------------------------------------
    # Target: K_JRNL_ENTRY_TRANS (sequential file)
    # ------------------------------------------------------------------
    write_files(
        df_NewKeys,
        f"{adls_path}/load/K_JRNL_ENTRY_TRANS.dat",
        ",",
        "overwrite",
        False,
        False,
        "\"",
        None
    )

    # ------------------------------------------------------------------
    # Transformer: Merge
    # ------------------------------------------------------------------
    df_Key = (
        df_AllColOut.alias("AllColOut")
        .join(
            df_Keys.alias("Keys"),
            (col("AllColOut.SRC_SYS_CD_SK") == col("Keys.SRC_SYS_CD_SK")) &
            (col("AllColOut.SRC_TRANS_CK") == col("Keys.SRC_TRANS_CK")) &
            (col("AllColOut.SRC_TRANS_TYP_CD") == col("Keys.SRC_TRANS_TYP_CD")) &
            (col("AllColOut.ACCTG_DT_SK") == col("Keys.ACCTG_DT_SK")),
            "left"
        )
        .select(
            col("AllColOut.JOB_EXCTN_RCRD_ERR_SK"),
            col("Keys.INSRT_UPDT_CD"),
            col("AllColOut.DISCARD_IN"),
            col("AllColOut.PASS_THRU_IN"),
            col("AllColOut.FIRST_RECYC_DT"),
            col("AllColOut.ERR_CT"),
            col("AllColOut.RECYCLE_CT"),
            col("Keys.SRC_SYS_CD"),
            col("AllColOut.PRI_KEY_STRING"),
            col("Keys.JRNL_ENTRY_TRANS_SK"),
            col("AllColOut.SRC_TRANS_CK"),
            col("AllColOut.SRC_TRANS_TYP_CD"),
            col("AllColOut.ACCTG_DT_SK"),
            col("Keys.CRT_RUN_CYC_EXCTN_SK"),
            lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            col("AllColOut.TRANS_TBL_SK"),
            col("AllColOut.FNCL_LOB"),
            col("AllColOut.JRNL_ENTRY_TRANS_DR_CR_CD"),
            col("AllColOut.DIST_GL_IN"),
            col("AllColOut.JRNL_ENTRY_TRANS_AMT"),
            col("AllColOut.TRANS_LN_NO"),
            col("AllColOut.ACCT_NO"),
            col("AllColOut.AFFILIAT_NO"),
            col("AllColOut.APPL_JRNL_ID"),
            col("AllColOut.BUS_UNIT_GL_NO"),
            col("AllColOut.BUS_UNIT_NO"),
            col("AllColOut.CC_ID"),
            col("AllColOut.JRNL_LN_DESC"),
            col("AllColOut.JRNL_LN_REF_NO"),
            col("AllColOut.OPR_UNIT_NO"),
            col("AllColOut.SUB_ACCT_NO"),
            col("AllColOut.TRANS_ID"),
            col("AllColOut.PRCS_MAP_1_TX"),
            col("AllColOut.PRCS_MAP_2_TX"),
            col("AllColOut.PRCS_MAP_3_TX"),
            col("AllColOut.PRCS_MAP_4_TX"),
            col("AllColOut.PRCS_SUB_SRC_CD"),
            col("AllColOut.CLCL_ID"),
            col("AllColOut.WD_LOB_CD"),
            col("AllColOut.WD_BANK_ACCT_NM"),
            col("AllColOut.WD_RVNU_CAT_ID"),
            col("AllColOut.WD_SPEND_CAT_ID")
        )
    )

    # ------------------------------------------------------------------
    # Return the container output stream(s)
    # ------------------------------------------------------------------
    return df_Key