# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

"""
ClmLnRemitDsalwLoadPK Shared Container

* VC LOGS *
^1_2 09/22/09 16:18:16 Batch  15241_58707 PROMOTE bckcett:31540 ids20 u10157 sa cuase this is third time trying
^1_2 09/22/09 16:15:40 Batch  15241_58542 PROMOTE bckcett:31540 testIDSnew u10157 sa
^1_2 09/22/09 16:12:55 Batch  15241_58378 INIT bckcett:31540 devlIDSnew u10157 sa
^1_1 06/29/09 16:46:57 Batch  15156_60447 INIT bckcett:31540 devlIDSnew u150906 3833-RemintAlternateCharge_Sharon_devlIDSnew      Maddy

Copyright 2009 Blue Cross/Blue Shield of Kansas City

CALLED BY:  FctsClmLnRemitDissalowPKExtr

PROCESSING:   Assign / Create primary SK for IDS claim line remit disallow tables with natural key of source system, claim ID, and sequence number.

MODIFICATIONS:
Developer                Date                 Project/Altiris #       Change Description                                                    Development Project  Code Reviewer        Date Reviewed
SAndrewd                 2009-06-10           Remit Alt Charge        Original Programming                                                  devlIDSnew

Annotations:
- Assign primary surrogate key
- Hash file lookup used for adjusted claims
- Claim Line Remit Disallow Primary Key Container
- Load IDS temp. table
- SQL joins temp table with key table to assign known keys
- Temp table is tuncated before load and runstat done after load
- primary key hash file only contains current run keys and is cleared before writing
- update primary key table (K_CLM_LN_REMIT_DSALW) with new keys created today
- File is appended to because of multiple iterations of this job.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def run_ClmLnRemitDsalwLoadPK(df_Transform: DataFrame, params: dict) -> DataFrame:
    """
    Executes the logic of the ClmLnRemitDsalwLoadPK shared container.

    Parameters
    ----------
    df_Transform : DataFrame
        Incoming DataFrame corresponding to the link "Transform".
    params : dict
        Runtime parameters passed into the shared container.

    Returns
    -------
    DataFrame
        Output DataFrame corresponding to the link "PK_ClmLnRemitDsalw".
    """

    # ------------------------------------------------------------------
    # Unpack runtime parameters (each exactly once)
    # ------------------------------------------------------------------
    CurrRunCycle        = params["CurrRunCycle"]
    IDSOwner            = params["IDSOwner"]
    ids_secret_name     = params["ids_secret_name"]
    SrcSysCd            = params["SrcSysCd"]
    adls_path           = params["adls_path"]
    ids_jdbc_url        = params["ids_jdbc_url"]
    ids_jdbc_props      = params["ids_jdbc_props"]

    # ------------------------------------------------------------------
    # Step 1: Truncate and load the temporary table K_CLM_LN_REMIT_DSALW_TEMP
    # ------------------------------------------------------------------
    execute_dml(
        f"TRUNCATE TABLE {IDSOwner}.K_CLM_LN_REMIT_DSALW_TEMP",
        ids_jdbc_url,
        ids_jdbc_props
    )

    (
        df_Transform.select(
            "SRC_SYS_CD_SK",
            "CLM_ID",
            "CLM_LN_SEQ_NO",
            "CLM_LN_DSALW_TYP_CD",
            "BYPS_IN"
        )
        .write.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("dbtable", f"{IDSOwner}.K_CLM_LN_REMIT_DSALW_TEMP")
        .mode("append")
        .save()
    )

    execute_dml(
        f"CALL SYSPROC.ADMIN_CMD('runstats on table {IDSOwner}.K_CLM_LN_REMIT_DSALW_TEMP on key columns with distribution on key columns and detailed indexes all allow write access')",
        ids_jdbc_url,
        ids_jdbc_props
    )

    # ------------------------------------------------------------------
    # Step 2: Extract data for further processing (W_Extract)
    # ------------------------------------------------------------------
    extract_query = f"""
    SELECT k.CLM_LN_REMIT_DSALW_SK,
           w.SRC_SYS_CD_SK,
           w.CLM_ID,
           w.CLM_LN_SEQ_NO,
           w.CLM_LN_DSALW_TYP_CD,
           w.BYPS_IN,
           k.CRT_RUN_CYC_EXCTN_SK
    FROM {IDSOwner}.K_CLM_LN_REMIT_DSALW_TEMP w
    JOIN {IDSOwner}.K_CLM_LN_REMIT_DSALW k
      ON w.SRC_SYS_CD_SK        = k.SRC_SYS_CD_SK
     AND w.CLM_ID               = k.CLM_ID
     AND w.CLM_LN_SEQ_NO        = k.CLM_LN_SEQ_NO
     AND w.CLM_LN_DSALW_TYP_CD  = k.CLM_LN_DSALW_TYP_CD
     AND w.BYPS_IN              = k.BYPS_IN

    UNION

    SELECT -1,
           w2.SRC_SYS_CD_SK,
           w2.CLM_ID,
           w2.CLM_LN_SEQ_NO,
           w2.CLM_LN_DSALW_TYP_CD,
           w2.BYPS_IN,
           {CurrRunCycle}
    FROM {IDSOwner}.K_CLM_LN_REMIT_DSALW_TEMP w2
    WHERE NOT EXISTS (
        SELECT 1
        FROM {IDSOwner}.K_CLM_LN_REMIT_DSALW k2
        WHERE w2.SRC_SYS_CD_SK       = k2.SRC_SYS_CD_SK
          AND w2.CLM_ID              = k2.CLM_ID
          AND w2.CLM_LN_SEQ_NO       = k2.CLM_LN_SEQ_NO
          AND w2.CLM_LN_DSALW_TYP_CD = k2.CLM_LN_DSALW_TYP_CD
          AND w2.BYPS_IN             = k2.BYPS_IN
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
    # Step 3: PrimaryKey Transformer logic
    # ------------------------------------------------------------------
    df_enriched = (
        df_W_Extract
        .withColumn(
            "svInstUpdt",
            F.when(F.col("CLM_LN_REMIT_DSALW_SK") == -1, F.lit("I")).otherwise(F.lit("U"))
        )
    )

    # Generate surrogate keys where needed
    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"CLM_LN_REMIT_DSALW_SK",<schema>,<secret_name>)

    # Determine correct CRT_RUN_CYC_EXCTN_SK
    df_enriched = (
        df_enriched
        .withColumn(
            "svCrtRunCycExctnSk",
            F.when(F.col("svInstUpdt") == "I", F.lit(CurrRunCycle)).otherwise(F.col("CRT_RUN_CYC_EXCTN_SK"))
        )
    )

    # ------------------------------------------------------------------
    # Step 4: Prepare 'updt' DataFrame for dummy table
    # ------------------------------------------------------------------
    df_updt = (
        df_enriched
        .withColumn("SRC_SYS_CD", F.lit(SrcSysCd))
        .select(
            "SRC_SYS_CD",
            "CLM_ID",
            "CLM_LN_SEQ_NO",
            "CLM_LN_DSALW_TYP_CD",
            "BYPS_IN",
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            "CLM_LN_REMIT_DSALW_SK"
        )
    )

    # ------------------------------------------------------------------
    # Step 5: Write to dummy table replacing hash file (hf_clm_ln_remit_dsalw)
    # ------------------------------------------------------------------
    dummy_table = "<dummy_hf_clm_ln_remit_dsalw>"
    execute_dml(f"DELETE FROM {dummy_table}", ids_jdbc_url, ids_jdbc_props)

    (
        df_updt
        .write.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("dbtable", dummy_table)
        .mode("append")
        .save()
    )

    # ------------------------------------------------------------------
    # Step 6: Prepare 'NewKeys' DataFrame and write sequential file
    # ------------------------------------------------------------------
    df_newkeys = (
        df_enriched
        .filter(F.col("svInstUpdt") == "I")
        .select(
            "SRC_SYS_CD_SK",
            "CLM_ID",
            "CLM_LN_SEQ_NO",
            "CLM_LN_DSALW_TYP_CD",
            "BYPS_IN",
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            "CLM_LN_REMIT_DSALW_SK"
        )
    )

    seq_file_path = f"{adls_path}/load/K_CLM_LN_REMIT_DSALW.dat"
    write_files(
        df_newkeys,
        seq_file_path,
        delimiter=',',
        mode='append',
        is_pqruet=False,
        header=False,
        quote='"',
        nullValue=None
    )

    # ------------------------------------------------------------------
    # Step 7: Return the final DataFrame for output link "PK_ClmLnRemitDsalw"
    # ------------------------------------------------------------------
    df_PK_ClmLnRemitDsalw = df_updt

    return df_PK_ClmLnRemitDsalw