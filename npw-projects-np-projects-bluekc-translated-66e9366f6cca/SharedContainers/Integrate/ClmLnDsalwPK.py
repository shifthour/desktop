# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

"""
Shared Container: ClmLnDsalwPK
Description:
* VC LOGS *
^1_2 03/10/09 11:34:35 Batch  15045_41695 PROMOTE bckcett devlIDS u10157 sa - Bringing ALL Claim code down from production
^1_2 03/10/09 09:25:21 Batch  15045_33943 INIT bckcetl ids20 dcg01 sa Bringing ALL Claim code down to devlIDS
^1_2 02/10/09 13:07:43 Batch  15017_47268 INIT bckcetl ids20 dsadm dsadm
^1_1 01/26/09 11:49:42 Batch  15002_42592 PROMOTE bckcetl ids20 dsadm bls for bl
^1_1 01/26/09 11:23:51 Batch  15002_41034 INIT bckcett testIDS dsadm BLS FOR BL
^1_1 12/16/08 10:00:18 Batch  14961_36031 PROMOTE bckcett testIDS u03651 steph for Brent - claim primary keying
^1_1 12/16/08 09:15:37 Batch  14961_33343 INIT bckcett devlIDS u03651 steffy

***************************************************************************************************************************************************************
COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 


DESCRIPTION:   Shared container used for Primary Keying of Claim Line COB job

CALLED BY : FctsClmLnDsalwExtr and NascoClmLnDsalwExtr

PROCESSING:    



MODIFICATIONS:
Developer           Date                         Change Description                                                     Project/Altius #          Development Project          Code Reviewer          Date Reviewed  
-----------------------    ----------------------------     ----------------------------------------------------------------------------        ---------------------------     ------------------------------------       ----------------------------      -------------------------
Parik                 2008-07-25               Initial program                                                                             3567(Primary Key)               devlIDS         Steph Goddard         07/29/2008

Annotations:
Hash file (hf_clm_ln_dsalw_allcol) cleared in calling program
join primary key info with table info
update primary key table (K_CLM_LN_DSALW) with new keys created today
primary key hash file only contains current run keys and is cleared before writing
SQL joins temp table with key table to assign known keys
Temp table is tuncated before load and runstat done after load
Load IDS temp. table
This container is used in:               FctsClmLnDsalwExtr
NascoClmLnDsalwExtr

These programs need to be re-compiled when logic changes
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, expr, trim


def run_ClmLnDsalwPK(
    df_AllCol: DataFrame,
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    """
    Executes shared-container logic for primary keying of Claim Line DSALW.
    Parameters
    ----------
    df_AllCol   : DataFrame
        AllCol link (hash file content supplied by calling job).
    df_Transform: DataFrame
        Transform link providing (SRC_SYS_CD_SK, CLM_ID, CLM_LN_SEQ_NO, CLM_LN_DSALW_TYP_CD).
    params      : dict
        Runtime parameters and JDBC configurations.
    Returns
    -------
    DataFrame
        InkOut link equivalent to DataStage container output.
    """

    # ------------------------------------------------------------------
    # Unpack required parameters (each exactly once)
    # ------------------------------------------------------------------
    IDSOwner            = params["IDSOwner"]
    ids_secret_name     = params["ids_secret_name"]
    ids_jdbc_url        = params["ids_jdbc_url"]
    ids_jdbc_props      = params["ids_jdbc_props"]
    CurrRunCycle        = params["CurrRunCycle"]
    SrcSysCd            = params["SrcSysCd"]
    adls_path           = params["adls_path"]
    adls_path_raw       = params["adls_path_raw"]
    adls_path_publish   = params["adls_path_publish"]

    # ------------------------------------------------------------------
    # Extract: read K_CLM_LN_DSALW from IDS
    # ------------------------------------------------------------------
    extract_query_key = f"""
        SELECT  SRC_SYS_CD_SK,
                CLM_ID,
                CLM_LN_SEQ_NO,
                CLM_LN_DSALW_TYP_CD,
                CLM_LN_DSALW_SK,
                CRT_RUN_CYC_EXCTN_SK
        FROM {IDSOwner}.K_CLM_LN_DSALW
    """

    df_key = (
        spark.read.format("jdbc")
            .option("url", ids_jdbc_url)
            .options(**ids_jdbc_props)
            .option("query", extract_query_key)
            .load()
    )

    # ------------------------------------------------------------------
    # Build W_Extract equivalent (matched + unmatched)
    # ------------------------------------------------------------------
    join_cond = [
        df_Transform.SRC_SYS_CD_SK == df_key.SRC_SYS_CD_SK,
        df_Transform.CLM_ID == df_key.CLM_ID,
        df_Transform.CLM_LN_SEQ_NO == df_key.CLM_LN_SEQ_NO,
        df_Transform.CLM_LN_DSALW_TYP_CD == df_key.CLM_LN_DSALW_TYP_CD
    ]

    df_matched = (
        df_Transform.alias("w")
        .join(df_key.alias("k"), join_cond, "inner")
        .select(
            col("k.CLM_LN_DSALW_SK"),
            col("w.SRC_SYS_CD_SK"),
            col("w.CLM_ID"),
            col("w.CLM_LN_SEQ_NO"),
            col("w.CLM_LN_DSALW_TYP_CD"),
            col("k.CRT_RUN_CYC_EXCTN_SK")
        )
    )

    df_unmatched = (
        df_Transform.alias("w2")
        .join(df_key.alias("k2"), join_cond, "left_anti")
        .select(
            lit(-1).alias("CLM_LN_DSALW_SK"),
            col("w2.SRC_SYS_CD_SK"),
            col("w2.CLM_ID"),
            col("w2.CLM_LN_SEQ_NO"),
            col("w2.CLM_LN_DSALW_TYP_CD"),
            lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK")
        )
    )

    df_W_Extract = df_matched.unionByName(df_unmatched)

    # ------------------------------------------------------------------
    # PrimaryKey transformer derivations
    # ------------------------------------------------------------------
    df_enriched = (
        df_W_Extract
        .withColumn("svInstUpdt", expr("case when CLM_LN_DSALW_SK = -1 then 'I' else 'U' end"))
        .withColumn("svSrcSysCd", lit(SrcSysCd))
        .withColumn("svClmId", trim(col("CLM_ID")))
        .withColumn("svClmLnDsalwTypCd", trim(col("CLM_LN_DSALW_TYP_CD")))
        .withColumn(
            "svCrtRunCycExctnSk",
            expr(f"case when svInstUpdt = 'I' then {CurrRunCycle} else CRT_RUN_CYC_EXCTN_SK end")
        )
    )

    # ------------------------------------------------------------------
    # Surrogate key generation (KeyMgtGetNextValueConcurrent replacement)
    # ------------------------------------------------------------------
    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"CLM_LN_DSALW_SK",<schema>,<secret_name>)

    # ------------------------------------------------------------------
    # Break-out DataFrames corresponding to output links
    # ------------------------------------------------------------------
    # updt link -> hash file replacement (parquet)
    df_updt = (
        df_enriched
        .select(
            col("svSrcSysCd").alias("SRC_SYS_CD"),
            col("svClmId").alias("CLM_ID"),
            col("CLM_LN_SEQ_NO"),
            col("svClmLnDsalwTypCd").alias("CLM_LN_DSALW_TYP_CD"),
            col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            col("CLM_LN_DSALW_SK")
        )
    )

    # NewKeys link -> sequential file
    df_NewKeys = (
        df_enriched
        .filter(col("svInstUpdt") == lit('I'))
        .select(
            col("SRC_SYS_CD_SK"),
            col("svClmId").alias("CLM_ID"),
            col("CLM_LN_SEQ_NO"),
            col("svClmLnDsalwTypCd").alias("CLM_LN_DSALW_TYP_CD"),
            col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            col("CLM_LN_DSALW_SK")
        )
    )

    # Keys link
    df_Keys = (
        df_enriched
        .select(
            col("CLM_LN_DSALW_SK"),
            col("SRC_SYS_CD_SK"),
            col("svSrcSysCd").alias("SRC_SYS_CD"),
            col("svClmId").alias("CLM_ID"),
            col("CLM_LN_SEQ_NO"),
            col("svClmLnDsalwTypCd").alias("CLM_LN_DSALW_TYP_CD"),
            col("svInstUpdt").alias("INSRT_UPDT_CD"),
            col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK")
        )
    )

    # ------------------------------------------------------------------
    # Merge stage logic
    # ------------------------------------------------------------------
    join_cond_merge = [
        df_AllCol.SRC_SYS_CD_SK == df_Keys.SRC_SYS_CD_SK,
        df_AllCol.CLM_ID == df_Keys.CLM_ID,
        df_AllCol.CLM_LN_SEQ_NO == df_Keys.CLM_LN_SEQ_NO,
        df_AllCol.CLM_LN_DSALW_TYP_CD == df_Keys.CLM_LN_DSALW_TYP_CD
    ]

    df_InkOut = (
        df_AllCol.alias("all")
        .join(df_Keys.alias("k"), join_cond_merge, "left")
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
            col("k.CLM_LN_DSALW_SK"),
            col("all.CLM_ID"),
            col("all.CLM_LN_SEQ_NO"),
            col("all.CLM_LN_DSALW_TYP_CD"),
            col("k.CRT_RUN_CYC_EXCTN_SK"),
            lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            col("all.CLM_LN_DSALW_EXCD"),
            col("all.DSALW_AMT")
        )
    )

    # ------------------------------------------------------------------
    # Egress: write hash-file replacement & sequential file
    # ------------------------------------------------------------------
    parquet_path = f"{adls_path}/ClmLnDsalwPK_updt.parquet"
    write_files(
        df_updt,
        parquet_path,
        delimiter=',',
        mode='overwrite',
        is_pqruet=True,
        header=True,
        quote='"',
        nullValue=None
    )

    seq_path = f"{adls_path}/load/K_CLM_LN_DSALW.dat"
    write_files(
        df_NewKeys,
        seq_path,
        delimiter=',',
        mode='overwrite',
        is_pqruet=False,
        header=False,
        quote='"',
        nullValue=None
    )

    # ------------------------------------------------------------------
    # Return container output
    # ------------------------------------------------------------------
    return df_InkOut