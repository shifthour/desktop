# COMMAND ----------
# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------
"""
Shared Container: ComsnSchdTierPK
Description:
* VC LOGS *
^1_1 02/20/09 11:01:04 Batch  15027_39667 PROMOTE bckcetl ids20 dsadm bls for sa
^1_1 02/20/09 10:38:04 Batch  15027_38287 INIT bckcett testIDS dsadm bls for sa
^1_2 02/19/09 15:36:15 Batch  15026_56186 PROMOTE bckcett testIDS u03651 steph for Sharon - primary key containers
^1_2 02/19/09 15:33:19 Batch  15026_56002 INIT bckcett devlIDS u03651 steffy
^1_1 01/13/09 08:41:23 Batch  14989_31287 INIT bckcett devlIDS u03651 steffy

***************************************************************************************************************************************************************
COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY


DESCRIPTION:   Shared container used for Primary Keying of Commision Scheduled Tier job

CALLED BY : FctsComsnSchdTierExtr

PROCESSING:    



MODIFICATIONS:
Developer           Date                         Change Description                                                     Project/Altius #          Development Project          Code Reviewer          Date Reviewed  
-----------------------    ----------------------------     ----------------------------------------------------------------------------        ---------------------------     ------------------------------------       ----------------------------      -------------------------
Bhoomi Dasari    2008-08-15               Initial program                                                               3567 Primary Key    devlIDS                                 Steph Goddard          09/22/2008

Annotations:
IDS Primary Key Container for Commission Schd Tier
Used by

FctsComsnSchdTierExtr
Hash file (hf_comsn_schd_tier_allcol) cleared in calling job
SQL joins temp table with key table to assign known keys
Temp table is tuncated before load and runstat done after load
Load IDS temp. table
join primary key info with table info
update primary key table (K_COMSN_SCHD_TIER) with new keys created today
primary key hash file only contains current run keys and is cleared before writing
Assign primary surrogate key
"""
from typing import Tuple
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# COMMAND ----------
def run_ComsnSchdTierPK(
    df_AllCol: DataFrame,
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    """
    Executes shared-container logic for ComsnSchdTierPK.

    Parameters
    ----------
    df_AllCol : DataFrame
        Incoming DataFrame for link 'AllCol'.
    df_Transform : DataFrame
        Incoming DataFrame for link 'Transform'.
    params : dict
        Runtime parameters and JDBC configuration.

    Returns
    -------
    DataFrame
        DataFrame corresponding to output link 'Key'.
    """
    # --------------------------------------------------
    # Unpack parameters exactly once
    CurrRunCycle        = params["CurrRunCycle"]
    SrcSysCd            = params["SrcSysCd"]
    IDSOwner            = params["IDSOwner"]
    ids_secret_name     = params["ids_secret_name"]
    ids_jdbc_url        = params["ids_jdbc_url"]
    ids_jdbc_props      = params["ids_jdbc_props"]
    # --------------------------------------------------
    # Scenario-a hashed file replacement: deduplicate AllCol
    partition_cols_allcol = [
        "SRC_SYS_CD_SK",
        "COMSN_SCHD_ID",
        "DURATN_EFF_DT_SK",
        "DURATN_STRT_PERD_NO",
        "PRM_FROM_THRS_HLD_AMT"
    ]
    df_AllCol_dedup = dedup_sort(
        df_AllCol,
        partition_cols_allcol,
        []
    )
    # --------------------------------------------------
    # Write df_Transform to IDS temp table
    table_temp = f"{IDSOwner}.K_COMSN_SCHD_TIER_TEMP"
    execute_dml(
        f"TRUNCATE TABLE {table_temp}",
        ids_jdbc_url,
        ids_jdbc_props
    )
    (
        df_Transform
        .write
        .mode("append")
        .format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("dbtable", table_temp)
        .save()
    )
    execute_dml(
        f"CALL SYSPROC.ADMIN_CMD('runstats on table {table_temp} on key columns with distribution on key columns and detailed indexes all allow write access')",
        ids_jdbc_url,
        ids_jdbc_props
    )
    # --------------------------------------------------
    # Read W_Extract via JDBC
    extract_query = f"""
    SELECT  k.COMSN_SCHD_TIER_SK,
            w.SRC_SYS_CD_SK,
            w.COMSN_SCHD_ID,
            w.DURATN_EFF_DT_SK,
            w.DURATN_STRT_PERD_NO,
            w.PRM_FROM_THRSHLD_AMT,
            k.CRT_RUN_CYC_EXCTN_SK
    FROM {IDSOwner}.K_COMSN_SCHD_TIER_TEMP w,
         {IDSOwner}.K_COMSN_SCHD_TIER k
    WHERE w.SRC_SYS_CD_SK          = k.SRC_SYS_CD_SK
      AND w.COMSN_SCHD_ID          = k.COMSN_SCHD_ID
      AND w.DURATN_EFF_DT_SK       = k.DURATN_EFF_DT_SK
      AND w.DURATN_STRT_PERD_NO    = k.DURATN_STRT_PERD_NO
      AND w.PRM_FROM_THRSHLD_AMT   = k.PRM_FROM_THRSHLD_AMT
    UNION
    SELECT -1,
           w2.SRC_SYS_CD_SK,
           w2.COMSN_SCHD_ID,
           w2.DURATN_EFF_DT_SK,
           w2.DURATN_STRT_PERD_NO,
           w2.PRM_FROM_THRSHLD_AMT,
           {CurrRunCycle}
    FROM {IDSOwner}.K_COMSN_SCHD_TIER_TEMP w2
    WHERE NOT EXISTS (
        SELECT k2.COMSN_SCHD_TIER_SK
        FROM {IDSOwner}.K_COMSN_SCHD_TIER k2
        WHERE w2.SRC_SYS_CD_SK        = k2.SRC_SYS_CD_SK
          AND w2.DURATN_EFF_DT_SK     = k2.DURATN_EFF_DT_SK
          AND w2.COMSN_SCHD_ID        = k2.COMSN_SCHD_ID
          AND w2.DURATN_STRT_PERD_NO  = k2.DURATN_STRT_PERD_NO
          AND w2.PRM_FROM_THRSHLD_AMT = k2.PRM_FROM_THRSHLD_AMT
    )
    """
    df_W_Extract = (
        spark.read.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("query", extract_query)
        .load()
    )
    # --------------------------------------------------
    # PrimaryKey transformer logic
    df_enriched = (
        df_W_Extract
        .withColumn(
            "svInstUpdt",
            F.when(F.col("COMSN_SCHD_TIER_SK") == -1, F.lit("I")).otherwise(F.lit("U"))
        )
        .withColumn("svComsnSchdId", F.trim(F.col("COMSN_SCHD_ID")))
        .withColumn(
            "svCrtRunCycExctnSk",
            F.when(F.col("svInstUpdt") == "I", F.lit(CurrRunCycle)).otherwise(F.col("CRT_RUN_CYC_EXCTN_SK"))
        )
        .withColumn(
            "COMSN_SCHD_TIER_SK",
            F.when(F.col("svInstUpdt") == "I", F.lit(None)).otherwise(F.col("COMSN_SCHD_TIER_SK"))
        )
        .withColumn("SRC_SYS_CD", F.lit(SrcSysCd))
    )
    df_enriched = SurrogateKeyGen(
        df_enriched,
        <DB sequence name>,
        "COMSN_SCHD_TIER_SK",
        <schema>,
        <secret_name>
    )
    # --------------------------------------------------
    # updt link (hashed file replaced with parquet)
    df_updt = (
        df_enriched
        .select(
            "SRC_SYS_CD",
            F.col("svComsnSchdId").alias("COMSN_SCHD_ID"),
            "DURATN_EFF_DT_SK",
            "DURATN_STRT_PERD_NO",
            "PRM_FROM_THRSHLD_AMT",
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            "COMSN_SCHD_TIER_SK"
        )
    )
    write_files(
        df_updt,
        f"{adls_path}/ComsnSchdTierPK_updt.parquet",
        delimiter=",",
        mode="overwrite",
        is_pqruet=True,
        header=True,
        quote="\"",
        nullValue=None
    )
    # --------------------------------------------------
    # NewKeys link -> sequential file
    df_newKeys = (
        df_enriched
        .filter(F.col("svInstUpdt") == "I")
        .select(
            "SRC_SYS_CD_SK",
            F.col("svComsnSchdId").alias("COMSN_SCHD_ID"),
            "DURATN_EFF_DT_SK",
            "DURATN_STRT_PERD_NO",
            "PRM_FROM_THRSHLD_AMT",
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            "COMSN_SCHD_TIER_SK"
        )
    )
    write_files(
        df_newKeys,
        f"{adls_path}/load/K_COMSN_SCHD_TIER.dat",
        delimiter=",",
        mode="overwrite",
        is_pqruet=False,
        header=False,
        quote="\"",
        nullValue=None
    )
    # --------------------------------------------------
    # Keys link for merge
    df_keys = (
        df_enriched
        .select(
            "COMSN_SCHD_TIER_SK",
            "SRC_SYS_CD_SK",
            F.col("svComsnSchdId").alias("COMSN_SCHD_ID"),
            "DURATN_EFF_DT_SK",
            "DURATN_STRT_PERD_NO",
            "PRM_FROM_THRSHLD_AMT",
            "SRC_SYS_CD",
            F.col("svInstUpdt").alias("INSRT_UPDT_CD"),
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK")
        )
    )
    # --------------------------------------------------
    # Merge transformer logic
    join_expr = [
        df_keys["SRC_SYS_CD_SK"]            == df_AllCol_dedup["SRC_SYS_CD_SK"],
        df_keys["COMSN_SCHD_ID"]            == df_AllCol_dedup["COMSN_SCHD_ID"],
        df_keys["DURATN_EFF_DT_SK"]         == df_AllCol_dedup["DURATN_EFF_DT_SK"],
        df_keys["DURATN_STRT_PERD_NO"]      == df_AllCol_dedup["DURATN_STRT_PERD_NO"],
        df_keys["PRM_FROM_THRSHLD_AMT"]     == df_AllCol_dedup["PRM_FROM_THRS_HLD_AMT"]
    ]
    df_key_output = (
        df_AllCol_dedup.alias("all")
        .join(df_keys.alias("k"), join_expr, "left")
        .select(
            F.col("all.JOB_EXCTN_RCRD_ERR_SK"),
            F.col("k.INSRT_UPDT_CD"),
            F.col("all.DISCARD_IN"),
            F.col("all.PASS_THRU_IN"),
            F.col("all.FIRST_RECYC_DT"),
            F.col("all.ERR_CT"),
            F.col("all.RECYCLE_CT"),
            F.col("k.SRC_SYS_CD"),
            F.col("all.PRI_KEY_STRING"),
            F.col("k.COMSN_SCHD_TIER_SK"),
            F.col("all.COMSN_SCHD_ID"),
            F.col("all.DURATN_EFF_DT_SK"),
            F.col("all.DURATN_STRT_PERD_NO"),
            F.col("all.PRM_FROM_THRS_HLD_AMT"),
            F.col("k.CRT_RUN_CYC_EXCTN_SK"),
            F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            F.lit(0).alias("COMSN_SCHD_SK"),
            F.col("all.COMSN_SCHD_TIER_CALC_METH_CD_SK"),
            F.col("all.PRM_THRU_THRESOLD_AMT"),
            F.col("all.TIER_AMT"),
            F.col("all.TIER_PCT"),
            F.col("all.TIER_UNIQ_KEY")
        )
    )
    # --------------------------------------------------
    return df_key_output
# COMMAND ----------