# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

"""
Shared Container: MbrCaseDefnPK
Description:
* VC LOGS *
^1_1 02/05/09 11:41:41 Batch  15012_42105 PROMOTE bckcetl ids20 dsadm bls for sg
^1_1 02/05/09 10:22:42 Batch  15012_37366 INIT bckcett testIDS dsadm bls for sg
^1_2 01/29/09 10:10:07 Batch  15005_36616 PROMOTE bckcett testIDS u03651 steph - primary key/balancing
^1_2 01/29/09 09:38:37 Batch  15005_34720 INIT bckcett devlIDS u03651 steffy
^1_1 01/26/09 14:40:15 Batch  15002_52819 INIT bckcett devlIDS u03651 steffy

***************************************************************************************************************************************************************
COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 


DESCRIPTION:   Shared container used for Primary Keying of MbrCaseDefn job

CALLED BY : ImpProMbrCaseDefnExtr

PROCESSING:    



MODIFICATIONS:
Developer           Date                         Change Description                                                     Project/Altius #          Development Project          Code Reviewer          Date Reviewed  
-----------------------    ----------------------------     ----------------------------------------------------------------------------        ---------------------------     ------------------------------------       ----------------------------      -------------------------
Bhoomi Dasari    2008-08-26               Initial program                                                               3567 Primary Key    devlIDS                                 Steph Goddard         09/02/2008
"""

# COMMAND ----------
from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import col, when, lit, trim, coalesce

# COMMAND ----------
def run_MbrCaseDefnPK(
        df_AllCol: DataFrame,
        df_Transform: DataFrame,
        params: dict
    ) -> DataFrame:
    """
    Executes the logic of the MbrCaseDefnPK shared container.

    Parameters
    ----------
    df_AllCol : DataFrame
        DataFrame corresponding to the 'AllCol' link.
    df_Transform : DataFrame
        DataFrame corresponding to the 'Transform' link.
    params : dict
        Runtime parameters & configurations.

    Returns
    -------
    DataFrame
        DataFrame produced on the 'Key' output link.
    """

    # ------------------------------------------------------------------
    # Unpack runtime parameters (each only once)
    # ------------------------------------------------------------------
    CurrRunCycle          = params["CurrRunCycle"]
    SrcSysCd              = params["SrcSysCd"]
    IDSOwner              = params["IDSOwner"]
    ids_jdbc_url          = params["ids_jdbc_url"]
    ids_jdbc_props        = params["ids_jdbc_props"]
    adls_path             = params["adls_path"]
    adls_path_raw         = params["adls_path_raw"]
    adls_path_publish     = params["adls_path_publish"]
    ids_secret_name       = params["ids_secret_name"]

    # ------------------------------------------------------------------
    # Stage: hf_mbr_case_defn_allcol  (Scenario a – intermediate hash)
    # Replace with deduplication over the key columns
    # ------------------------------------------------------------------
    partition_cols_hash = ["SRC_SYS_CD_SK", "MBR_UNIQ_KEY", "PRCS_YR_MO_SK", "CASE_DEFN_ID"]
    df_allcol_dedup = df_AllCol.dropDuplicates(partition_cols_hash)

    # ------------------------------------------------------------------
    # Stage: K_MBR_CASE_DEFN  (Reference table read via JDBC)
    # ------------------------------------------------------------------
    extract_query_k = f"""
        SELECT
            MBR_CASE_DEFN_SK,
            SRC_SYS_CD_SK,
            MBR_UNIQ_KEY,
            PRCS_YR_MO_SK,
            CASE_DEFN_ID,
            CRT_RUN_CYC_EXCTN_SK
        FROM {IDSOwner}.K_MBR_CASE_DEFN
    """
    df_k_mbr_case_defn = (
        spark.read.format("jdbc")
            .option("url", ids_jdbc_url)
            .options(**ids_jdbc_props)
            .option("query", extract_query_k)
            .load()
    )

    # ------------------------------------------------------------------
    # Stage: W_Extract logic (equivalent to DB2 SQL UNION block)
    # ------------------------------------------------------------------
    join_expr = [
        df_Transform.SRC_SYS_CD_SK == df_k_mbr_case_defn.SRC_SYS_CD_SK,
        df_Transform.MBR_UNIQ_KEY  == df_k_mbr_case_defn.MBR_UNIQ_KEY,
        df_Transform.PRCS_YR_MO_SK == df_k_mbr_case_defn.PRCS_YR_MO_SK,
        df_Transform.CASE_DEFN_ID  == df_k_mbr_case_defn.CASE_DEFN_ID
    ]

    df_w_extract = (
        df_Transform.alias("w")
        .join(df_k_mbr_case_defn.alias("k"), join_expr, "left")
        .select(
            coalesce(col("k.MBR_CASE_DEFN_SK"), lit(-1)).alias("MBR_CASE_DEFN_SK"),
            col("w.SRC_SYS_CD_SK"),
            col("w.MBR_UNIQ_KEY"),
            col("w.PRCS_YR_MO_SK"),
            col("w.CASE_DEFN_ID"),
            when(col("k.MBR_CASE_DEFN_SK").isNotNull(),
                 col("k.CRT_RUN_CYC_EXCTN_SK")).otherwise(lit(CurrRunCycle)).alias("CRT_RUN_CYC_EXCTN_SK")
        )
    )

    # ------------------------------------------------------------------
    # Stage: PrimaryKey Transformer
    # ------------------------------------------------------------------
    df_enriched = (
        df_w_extract
        .withColumn("svInstUpdt",
                    when(col("MBR_CASE_DEFN_SK") == lit(-1), lit("I")).otherwise(lit("U")))
        .withColumn("svSrcSysCd", lit("FACETS"))
        .withColumn("svSK", lit(None))  # placeholder for surrogate key
        .withColumn("svCrtRunCycExctnSk",
                    when(col("svInstUpdt") == lit("I"), lit(CurrRunCycle))
                    .otherwise(col("CRT_RUN_CYC_EXCTN_SK")))
        .withColumn("svCaseDefnId", trim(col("CASE_DEFN_ID")))
    )

    # Surrogate key generation (as per mandatory format)
    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"svSK",<schema>,<secret_name>)

    # ------------------------------------------------------------------
    # updt link (to hf_mbr_case_defn parquet)
    # ------------------------------------------------------------------
    df_updt = df_enriched.select(
        lit(SrcSysCd).alias("SRC_SYS_CD"),
        "MBR_UNIQ_KEY",
        "PRCS_YR_MO_SK",
        col("svCaseDefnId").alias("CASE_DEFN_ID"),
        col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("svSK").alias("MBR_CASE_DEFN_SK")
    )

    write_files(
        df_updt,
        f"{adls_path}/MbrCaseDefnPK_updt.parquet",
        delimiter=",",
        mode="overwrite",
        is_pqruet=True,
        header=True,
        quote="\"",
        nullValue=None
    )

    # ------------------------------------------------------------------
    # NewKeys link  (constraint svInstUpdt == 'I')
    # ------------------------------------------------------------------
    df_newkeys = (
        df_enriched
        .filter(col("svInstUpdt") == lit("I"))
        .select(
            "SRC_SYS_CD_SK",
            "MBR_UNIQ_KEY",
            "PRCS_YR_MO_SK",
            col("svCaseDefnId").alias("CASE_DEFN_ID"),
            col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            col("svSK").alias("MBR_CASE_DEFN_SK")
        )
    )

    write_files(
        df_newkeys,
        f"{adls_path}/load/K_MBR_CASE_DEFN.dat",
        delimiter=",",
        mode="overwrite",
        is_pqruet=False,
        header=True,
        quote="\"",
        nullValue=None
    )

    # ------------------------------------------------------------------
    # Keys link  (all rows)
    # ------------------------------------------------------------------
    df_keys = (
        df_enriched.select(
            col("svSK").alias("MBR_CASE_DEFN_SK"),
            "SRC_SYS_CD_SK",
            lit(SrcSysCd).alias("SRC_SYS_CD"),
            "MBR_UNIQ_KEY",
            "PRCS_YR_MO_SK",
            col("svCaseDefnId").alias("CASE_DEFN_ID"),
            col("svInstUpdt").alias("INSRT_UPDT_CD"),
            col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK")
        )
    )

    # ------------------------------------------------------------------
    # Merge Transformer: join AllColOut (df_allcol_dedup) with Keys
    # ------------------------------------------------------------------
    merge_join_expr = [
        df_allcol_dedup.SRC_SYS_CD_SK == df_keys.SRC_SYS_CD_SK,
        df_allcol_dedup.MBR_UNIQ_KEY  == df_keys.MBR_UNIQ_KEY,
        df_allcol_dedup.PRCS_YR_MO_SK == df_keys.PRCS_YR_MO_SK,
        df_allcol_dedup.CASE_DEFN_ID  == df_keys.CASE_DEFN_ID
    ]

    df_key = (
        df_allcol_dedup.alias("all")
        .join(df_keys.alias("k"), merge_join_expr, "left")
        .select(
            col("all.JOB_EXCTN_RCRD_ERR_SK"),
            col("k.INSRT_UPDT_CD"),
            col("all.DISCARD_IN"),
            col("all.PASS_THRU_IN"),
            col("all.FIRST_RECYC_DT"),
            col("all.ERR_CT"),
            col("all.RECYCLE_CT"),
            col("k.SRC_SYS_CD"),
            col("all.PRI_KEY_STRING"),
            col("k.MBR_CASE_DEFN_SK"),
            col("all.MBR_UNIQ_KEY"),
            col("all.PRCS_YR_MO_SK"),
            col("all.CASE_DEFN_ID"),
            col("k.CRT_RUN_CYC_EXCTN_SK"),
            lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            col("all.CASE_DEFN_SK"),
            col("all.MBR_MED_MESRS_SK")
        )
    )

    # ------------------------------------------------------------------
    # Return the single output link
    # ------------------------------------------------------------------
    return df_key
# COMMAND ----------