# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

"""
***************************************************************************************************************************************************************
COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 

DESCRIPTION:   Shared container used for Primary Keying of Claim Line Override job

CALLED BY : FctsClmLnOvrdExtr and NascoClmLnOverRideExtr

PROCESSING:    

MODIFICATIONS:
Developer           Date                         Change Description                                                     Project/Altius #          Development Project          Code Reviewer          Date Reviewed  
-----------------------    ----------------------------     ----------------------------------------------------------------------------        ---------------------------     ------------------------------------       ----------------------------      -------------------------
Parik                 2008-09-11                Initial program                                                                            3567(Primary Key)               devlIDS         Steph Goddard          09/12/2008
Ralph Tucker     2012-01-24               Removed Display and Element values for standards   TTR-1252                IntegrateCurDevl                   Sharon Andrew         2012-02-03
"""

# Hash file (hf_clm_ln_ovrd_allcol) cleared in calling program
# join primary key info with table info
# update primary key table (K_CLM_LN_OVRD) with new keys created today
# primary key hash file only contains current run keys and is cleared before writing
# SQL joins temp table with key table to assign known keys
# Temp table is tuncated before load and runstat done after load
# Load IDS temp. table
# This container is used in:
# FctsClmLnOvrdExtr
# NascoClmLnOverRideExtr
#
# These programs need to be re-compiled when logic changes

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, trim, when
from typing import Tuple

def run_ClmLnOvrdPK(
    df_AllCol: DataFrame,
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    """
    PySpark translation of DataStage shared container 'ClmLnOvrdPK'.
    
    Parameters
    ----------
    df_AllCol   : DataFrame
        Container input link `AllCol` (all columns hash-file feed).
    df_Transform: DataFrame
        Container input link `Transform` (rows to load into temp table).
    params      : dict
        Runtime parameters and JDBC configurations.
    
    Returns
    -------
    DataFrame
        Container output link `Key`.
    """

    # ------------------------------------------------------------
    # Unpack parameters (exactly once)
    # ------------------------------------------------------------
    IDSOwner         = params["IDSOwner"]
    ids_secret_name  = params["ids_secret_name"]
    CurrRunCycle     = params["CurrRunCycle"]
    SrcSysCd         = params["SrcSysCd"]
    ids_jdbc_url     = params["ids_jdbc_url"]
    ids_jdbc_props   = params["ids_jdbc_props"]
    adls_path        = params["adls_path"]
    adls_path_raw    = params["adls_path_raw"]
    adls_path_publish= params["adls_path_publish"]
    # ------------------------------------------------------------
    # Stage: hf_clm_ln_ovrd_allcol  (scenario a – intermediate hash file)
    # ------------------------------------------------------------
    df_AllCol_dedup = dedup_sort(
        df_AllCol,
        ["SRC_SYS_CD_SK", "CLM_ID", "CLM_LN_SEQ_NO", "CLM_LN_OVRD_ID"],
        [
            ("SRC_SYS_CD_SK", "A"),
            ("CLM_ID", "A"),
            ("CLM_LN_SEQ_NO", "A"),
            ("CLM_LN_OVRD_ID", "A")
        ]
    )
    # ------------------------------------------------------------
    # Stage: K_CLM_LN_OVRD_TEMP  (DB2Connector – load & extract)
    # ------------------------------------------------------------
    temp_table_fq = f"{IDSOwner}.K_CLM_LN_OVRD_TEMP"

    # Overwrite temp table with current data
    (
        df_Transform.write.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("dbtable", temp_table_fq)
        .mode("overwrite")
        .save()
    )

    # Execute runstats (After SQL)
    runstats_sql = (
        f"CALL SYSPROC.ADMIN_CMD("
        f"'runstats on table {IDSOwner}.K_CLM_LN_OVRD_TEMP "
        f"on key columns with distribution on key columns "
        f"and detailed indexes all allow write access')"
    )
    execute_dml(runstats_sql, ids_jdbc_url, ids_jdbc_props)

    # Build extract query (W_Extract)
    extract_query = f"""
    SELECT k.CLM_LN_OVRD_SK,
           w.SRC_SYS_CD_SK,
           w.CLM_ID,
           w.CLM_LN_SEQ_NO,
           w.CLM_LN_OVRD_ID,
           k.CRT_RUN_CYC_EXCTN_SK
    FROM {IDSOwner}.K_CLM_LN_OVRD_TEMP w
    JOIN {IDSOwner}.K_CLM_LN_OVRD k
      ON w.SRC_SYS_CD_SK   = k.SRC_SYS_CD_SK
     AND w.CLM_ID          = k.CLM_ID
     AND w.CLM_LN_SEQ_NO   = k.CLM_LN_SEQ_NO
     AND w.CLM_LN_OVRD_ID  = k.CLM_LN_OVRD_ID
    UNION
    SELECT -1,
           w2.SRC_SYS_CD_SK,
           w2.CLM_ID,
           w2.CLM_LN_SEQ_NO,
           w2.CLM_LN_OVRD_ID,
           {CurrRunCycle}
    FROM {IDSOwner}.K_CLM_LN_OVRD_TEMP w2
    WHERE NOT EXISTS (
        SELECT 1
        FROM {IDSOwner}.K_CLM_LN_OVRD k2
        WHERE w2.SRC_SYS_CD_SK  = k2.SRC_SYS_CD_SK
          AND w2.CLM_ID         = k2.CLM_ID
          AND w2.CLM_LN_SEQ_NO  = k2.CLM_LN_SEQ_NO
          AND w2.CLM_LN_OVRD_ID = k2.CLM_LN_OVRD_ID
    )
    """

    df_W_Extract = (
        spark.read.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("query", extract_query)
        .load()
    )
    # ------------------------------------------------------------
    # Stage: PrimaryKey  (Transformer logic)
    # ------------------------------------------------------------
    df_enriched = (
        df_W_Extract
        .withColumn("svInstUpdt", when(col("CLM_LN_OVRD_SK") == -1, lit("I")).otherwise(lit("U")))
        .withColumn("svSrcSysCd", lit(SrcSysCd))
        .withColumn("svClmId", trim(col("CLM_ID")))
        .withColumn(
            "svCrtRunCycExctnSk",
            when(col("svInstUpdt") == "I", lit(CurrRunCycle)).otherwise(col("CRT_RUN_CYC_EXCTN_SK"))
        )
        .withColumn(
            "svSK",
            when(col("svInstUpdt") == "I", lit(None)).otherwise(col("CLM_LN_OVRD_SK"))
        )
    )

    # Surrogate key generation (mandatory placeholder call)
    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"svSK",<schema>,<secret_name>)

    # updt link (to hf_clm_ln_ovrd)
    df_updt = (
        df_enriched.select(
            col("svSrcSysCd").alias("SRC_SYS_CD"),
            col("svClmId").alias("CLM_ID"),
            col("CLM_LN_SEQ_NO"),
            col("CLM_LN_OVRD_ID"),
            col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            col("svSK").alias("CLM_LN_OVRD_SK")
        )
    )

    # NewKeys link (for sequential file)
    df_NewKeys = (
        df_enriched.filter(col("svInstUpdt") == lit("I"))
        .select(
            col("SRC_SYS_CD_SK"),
            col("svClmId").alias("CLM_ID"),
            col("CLM_LN_SEQ_NO"),
            col("CLM_LN_OVRD_ID"),
            col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            col("svSK").alias("CLM_LN_OVRD_SK")
        )
    )

    # Keys link (to Merge stage)
    df_keys = (
        df_enriched.select(
            col("svSK").alias("CLM_LN_OVRD_SK"),
            col("SRC_SYS_CD_SK"),
            col("svSrcSysCd").alias("SRC_SYS_CD"),
            col("svClmId").alias("CLM_ID"),
            col("CLM_LN_SEQ_NO"),
            col("CLM_LN_OVRD_ID"),
            col("svInstUpdt").alias("INSRT_UPDT_CD"),
            col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK")
        )
    )
    # ------------------------------------------------------------
    # Write outputs of PrimaryKey stage
    # ------------------------------------------------------------
    write_files(
        df_updt,
        f"{adls_path}/ClmLnOvrdPK_updt.parquet",
        delimiter=",",
        mode="overwrite",
        is_pqruet=True,
        header=True,
        quote="\"",
        nullValue=None
    )

    write_files(
        df_NewKeys,
        f"{adls_path}/load/K_CLM_LN_OVRD.dat",
        delimiter=",",
        mode="overwrite",
        is_pqruet=False,
        header=False,
        quote="\"",
        nullValue=None
    )
    # ------------------------------------------------------------
    # Stage: Merge  (combine AllColOut & Keys)
    # ------------------------------------------------------------
    join_expr = (
        (col("a.SRC_SYS_CD_SK")   == col("k.SRC_SYS_CD_SK")) &
        (col("a.CLM_ID")          == col("k.CLM_ID")) &
        (col("a.CLM_LN_SEQ_NO")   == col("k.CLM_LN_SEQ_NO")) &
        (col("a.CLM_LN_OVRD_ID")  == col("k.CLM_LN_OVRD_ID"))
    )

    df_Key = (
        df_AllCol_dedup.alias("a")
        .join(df_keys.alias("k"), join_expr, "left")
        .select(
            col("a.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
            col("k.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
            col("a.DISCARD_IN").alias("DISCARD_IN"),
            col("a.PASS_THRU_IN").alias("PASS_THRU_IN"),
            col("a.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
            col("a.ERR_CT").alias("ERR_CT"),
            col("a.RECYCLE_CT").alias("RECYCLE_CT"),
            col("k.SRC_SYS_CD").alias("SRC_SYS_CD"),
            col("a.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
            col("k.CLM_LN_OVRD_SK").alias("CLM_LN_OVRD_SK"),
            col("a.CLM_ID").alias("CLM_ID"),
            col("a.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
            col("a.CLM_LN_OVRD_ID").alias("CLM_LN_OVRD_ID"),
            col("k.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
            lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            col("a.USER_ID").alias("USER_ID"),
            col("a.CLM_LN_OVRD_EXCD").alias("CLM_LN_OVRD_EXCD"),
            col("a.OVRD_DT").alias("OVRD_DT"),
            col("a.OVRD_AMT").alias("OVRD_AMT"),
            col("a.OVRD_VAL_DESC").alias("OVRD_VAL_DESC")
        )
    )
    # ------------------------------------------------------------
    # Return container output
    # ------------------------------------------------------------
    return df_Key