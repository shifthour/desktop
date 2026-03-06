# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

"""
***************************************************************************************************************************************************************
COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 

DESCRIPTION:   Shared container used for Primary Keying of Claim Line Savings

PROCESSING:    Called by FctsClmLnSavExtr



MODIFICATIONS:
Developer           Date                         Change Description                                                     Project/Altius #          Development Project          Code Reviewer          Date Reviewed  
-----------------------    ----------------------------     ----------------------------------------------------------------------------        ---------------------------     ------------------------------------       ----------------------------      -------------------------
Ralph Tucker     2008-08-14               Initial program                                                               3657 Primary Key    devlIDS                                 Steph Goddard          08/21/2008

Shanmugam A    2017-03-02            Modify Data type on Claim id field from Char to         5321                    IntegrateDev2                    Jag Yelavarthi           2017-03-07
				Varchar in stage K_CLM_LN_SAV_TEMP
"""

def run_ClmLnSavPK(
    df_AllCol: DataFrame,
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    # ------------------------------------------------------------------
    # Unpack parameters (exclude DB connection details except $FacetsDB)
    # ------------------------------------------------------------------
    CurrRunCycle       = params["CurrRunCycle"]
    IDSOwner           = params["IDSOwner"]
    ids_secret_name    = params["ids_secret_name"]
    ids_jdbc_url       = params["ids_jdbc_url"]
    ids_jdbc_props     = params["ids_jdbc_props"]
    adls_path          = params["adls_path"]
    adls_path_raw      = params["adls_path_raw"]
    adls_path_publish  = params["adls_path_publish"]
    SrcSysCd           = params["SrcSysCd"]

    # ------------------------------------------------------------------
    # Stage: K_CLM_LN_SAV_TEMP  (truncate → load → runstats)
    # ------------------------------------------------------------------
    execute_dml(f"TRUNCATE TABLE {IDSOwner}.K_CLM_LN_SAV_TEMP", ids_jdbc_url, ids_jdbc_props)

    (
        df_Transform.write.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("dbtable", f"{IDSOwner}.K_CLM_LN_SAV_TEMP")
        .mode("append")
        .save()
    )

    execute_dml(
        f"CALL SYSPROC.ADMIN_CMD('runstats on table {IDSOwner}.K_CLM_LN_SAV_TEMP on key columns with distribution on key columns and detailed indexes all allow write access')",
        ids_jdbc_url,
        ids_jdbc_props
    )

    # ------------------------------------------------------------------
    # Stage: K_CLM_LN_SAV_TEMP (W_Extract output)
    # ------------------------------------------------------------------
    extract_query = f"""
    SELECT k.CLM_LN_SAV_SK,
           w.SRC_SYS_CD_SK,
           w.CLM_ID,
           w.CLM_LN_SEQ_NO,
           w.CLM_LN_SAV_TYP_CD,
           k.CRT_RUN_CYC_EXCTN_SK
      FROM {IDSOwner}.K_CLM_LN_SAV_TEMP w
      JOIN {IDSOwner}.K_CLM_LN_SAV k
        ON w.SRC_SYS_CD_SK   = k.SRC_SYS_CD_SK
       AND w.CLM_ID          = k.CLM_ID
       AND w.CLM_LN_SEQ_NO   = k.CLM_LN_SEQ_NO
       AND w.CLM_LN_SAV_TYP_CD = k.CLM_LN_SAV_TYP_CD
    UNION
    SELECT -1,
           w2.SRC_SYS_CD_SK,
           w2.CLM_ID,
           w2.CLM_LN_SEQ_NO,
           w2.CLM_LN_SAV_TYP_CD,
           {CurrRunCycle}
      FROM {IDSOwner}.K_CLM_LN_SAV_TEMP w2
     WHERE NOT EXISTS (
           SELECT 1
             FROM {IDSOwner}.K_CLM_LN_SAV k2
            WHERE w2.SRC_SYS_CD_SK   = k2.SRC_SYS_CD_SK
              AND w2.CLM_ID          = k2.CLM_ID
              AND w2.CLM_LN_SEQ_NO   = k2.CLM_LN_SEQ_NO
              AND w2.CLM_LN_SAV_TYP_CD = k2.CLM_LN_SAV_TYP_CD
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
    # Stage: PrimaryKey (Transformer logic)
    # ------------------------------------------------------------------
    df_enriched = (
        df_W_Extract
        .withColumn("svInstUpdt",
                    F.when(F.col("CLM_LN_SAV_SK") == F.lit(-1), F.lit("I"))
                     .otherwise(F.lit("U")))
        .withColumn("svSK",
                    F.when(F.col("svInstUpdt") == F.lit("I"), F.lit(None))
                     .otherwise(F.col("CLM_LN_SAV_SK")))
        .withColumn("svCrtRunCycExctnSk",
                    F.when(F.col("svInstUpdt") == F.lit("I"), F.lit(CurrRunCycle))
                     .otherwise(F.col("CRT_RUN_CYC_EXCTN_SK")))
        .withColumn("SrcSysCd", F.lit(SrcSysCd))
    )

    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,'svSK',<schema>,<secret_name>)

    # updt link (to hf_clm_ln_sav parquet)
    df_updt = (
        df_enriched.select(
            F.col("SrcSysCd").alias("SRC_SYS_CD"),
            F.col("CLM_ID"),
            F.col("CLM_LN_SEQ_NO"),
            F.col("CLM_LN_SAV_TYP_CD"),
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.col("svSK").alias("PAYMT_SUM_SK")
        )
    )

    # NewKeys link (sequential file)
    df_newkeys = (
        df_enriched.filter(F.col("svInstUpdt") == F.lit("I"))
        .select(
            F.col("SRC_SYS_CD_SK"),
            F.col("CLM_ID"),
            F.col("CLM_LN_SEQ_NO"),
            F.col("CLM_LN_SAV_TYP_CD"),
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.col("svSK").alias("CLM_LN_SAV_SK")
        )
    )

    # Keys link (for merge)
    df_keys = (
        df_enriched.select(
            F.col("svSK").alias("CLM_LN_SAV_SK"),
            F.col("SRC_SYS_CD_SK"),
            F.col("SrcSysCd").alias("SRC_SYS_CD"),
            F.col("CLM_ID"),
            F.col("CLM_LN_SEQ_NO"),
            F.col("CLM_LN_SAV_TYP_CD"),
            F.col("svInstUpdt").alias("INSRT_UPDT_CD"),
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK")
        )
    )

    # ------------------------------------------------------------------
    # Stage: hf_clm_ln_sav (write parquet)
    # ------------------------------------------------------------------
    write_files(
        df_updt,
        f"{adls_path}/ClmLnSavPK_updt.parquet",
        delimiter=",",
        mode="overwrite",
        is_pqruet=True,
        header=True,
        quote="\"",
        nullValue=None
    )

    # ------------------------------------------------------------------
    # Stage: K_CLM_LN_SAV (write sequential file)
    # ------------------------------------------------------------------
    write_files(
        df_newkeys,
        f"{adls_path}/load/K_CLM_LN_SAV.dat",
        delimiter=",",
        mode="overwrite",
        is_pqruet=False,
        header=False,
        quote="\"",
        nullValue=None
    )

    # ------------------------------------------------------------------
    # Stage: hf_clm_ln_sav_allcol (intermediate hash file – dedup)
    # ------------------------------------------------------------------
    df_AllCol_dedup = dedup_sort(
        df_AllCol,
        ["SRC_SYS_CD_SK", "CLM_ID", "CLM_LN_SEQ_NO", "CLM_LN_SAV_TYP_CD"],
        []
    )

    # ------------------------------------------------------------------
    # Stage: Merge
    # ------------------------------------------------------------------
    join_expr = (
        (F.col("AllColOut.SRC_SYS_CD_SK")   == F.col("Keys.SRC_SYS_CD_SK")) &
        (F.col("AllColOut.CLM_ID")          == F.col("Keys.CLM_ID")) &
        (F.col("AllColOut.CLM_LN_SEQ_NO")   == F.col("Keys.CLM_LN_SEQ_NO")) &
        (F.col("AllColOut.CLM_LN_SAV_TYP_CD") == F.col("Keys.CLM_LN_SAV_TYP_CD"))
    )

    df_Key = (
        df_AllCol_dedup.alias("AllColOut")
        .join(df_keys.alias("Keys"), join_expr, "left")
        .select(
            F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
            F.col("Keys.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
            F.col("AllColOut.DISCARD_IN"),
            F.col("AllColOut.PASS_THRU_IN"),
            F.col("AllColOut.FIRST_RECYC_DT"),
            F.col("AllColOut.ERR_CT"),
            F.col("AllColOut.RECYCLE_CT"),
            F.col("Keys.SRC_SYS_CD"),
            F.col("AllColOut.PRI_KEY_STRING"),
            F.col("Keys.CLM_LN_SAV_SK"),
            F.col("AllColOut.SRC_SYS_CD_SK"),
            F.col("AllColOut.CLM_ID"),
            F.col("AllColOut.CLM_LN_SEQ_NO"),
            F.col("AllColOut.CLM_LN_SAV_TYP_CD"),
            F.col("Keys.CRT_RUN_CYC_EXCTN_SK"),
            F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            F.col("AllColOut.CLM_LN_SK"),
            F.col("AllColOut.SAV_AMT")
        )
    )

    return df_Key