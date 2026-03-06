
# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

"""
ClmOverPaymtPK – Shared Container

* VC LOGS *
^1_2 03/10/09 09:25:21 Batch  15045_33943 INIT bckcetl ids20 dcg01 sa Bringing ALL Claim code down to devlIDS
^1_2 02/10/09 13:07:43 Batch  15017_47268 INIT bckcetl ids20 dsadm dsadm
^1_1 01/26/09 11:49:42 Batch  15002_42592 PROMOTE bckcetl ids20 dsadm bls for bl
^1_1 01/26/09 11:23:51 Batch  15002_41034 INIT bckcett testIDS dsadm BLS FOR BL
^1_1 12/16/08 10:00:18 Batch  14961_36031 PROMOTE bckcett testIDS u03651 steph for Brent - claim primary keying
^1_1 12/16/08 09:15:37 Batch  14961_33343 INIT bckcett devlIDS u03651 steffy
^1_1 07/24/08 13:21:35 Batch  14816_48098 INIT bckcett devlIDS u08717 Brent

***************************************************************************************************************************************************************
COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 

CALLED BY : FctsClmOvrPayExtr


PROCESSING:    Shared container used for Primary Keying of Claim Over payment job


MODIFICATIONS:
Developer           Date                         Change Description                                                     Project/Altius #          Development Project          Code Reviewer          Date Reviewed  
-----------------------    ----------------------------     ----------------------------------------------------------------------------        ---------------------------     ------------------------------------       ----------------------------      -------------------------

Bhoomi Dasari    2008-07-21               Initial program                                                               3567 Primary Key    devlIDS                               Steph Goddard          07-28-2008
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# COMMAND ----------

def run_ClmOverPaymtPK(
    df_AllColl: DataFrame,
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    """
    Executes the logic of the ClmOverPaymtPK shared container.

    Parameters
    ----------
    df_AllColl : DataFrame
        Incoming DataFrame that was previously routed to the AllColl link.
    df_Transform : DataFrame
        Incoming DataFrame that was previously routed to the Transform link.
    params : dict
        Runtime parameters, JDBC configs, secret names, ADLS paths, etc.

    Returns
    -------
    DataFrame
        Outgoing DataFrame corresponding to the Key link.
    """

    # --------------------------------------------------
    # Unpack parameters (only once)
    # --------------------------------------------------
    CurrRunCycle        = params["CurrRunCycle"]
    SrcSysCd            = params["SrcSysCd"]
    RunID               = params["RunID"]
    CurrentDate         = params["CurrentDate"]
    IDSOwner            = params["IDSOwner"]
    ids_secret_name     = params["ids_secret_name"]
    ids_jdbc_url        = params["ids_jdbc_url"]
    ids_jdbc_props      = params["ids_jdbc_props"]
    adls_path           = params["adls_path"]
    adls_path_raw       = params["adls_path_raw"]
    adls_path_publish   = params["adls_path_publish"]

    # --------------------------------------------------
    # STEP 1 : Replace intermediate hash file hf_clm_ovr_paymnt_allcol
    # --------------------------------------------------
    df_AllColl_dedup = dedup_sort(
        df_AllColl,
        partition_cols=[
            "SRC_SYS_CD_SK",
            "CLM_ID",
            "CLM_OVER_PAYMT_PAYE_IND",
            "LOB_NO"
        ],
        sort_cols=[]
    )

    # --------------------------------------------------
    # STEP 2 : Build W_Extract equivalent (join with K_CLM_OVER_PAYMT)
    # --------------------------------------------------
    extract_query = f"""
        SELECT
            CLM_OVER_PAYMT_SK,
            SRC_SYS_CD_SK,
            CLM_ID,
            CLM_OVER_PAYMT_PAYE_CD,
            LOB_NO,
            CRT_RUN_CYC_EXCTN_SK
        FROM {IDSOwner}.K_CLM_OVER_PAYMT
    """

    df_k_clm_over_paymt = (
        spark.read.format("jdbc")
            .option("url", ids_jdbc_url)
            .options(**ids_jdbc_props)
            .option("query", extract_query)
            .load()
    )

    join_expr = (
        (df_Transform.SRC_SYS_CD_SK == df_k_clm_over_paymt.SRC_SYS_CD_SK) &
        (df_Transform.CLM_ID == df_k_clm_over_paymt.CLM_ID) &
        (df_Transform.CLM_OVER_PAYMT_PAYE_CD == df_k_clm_over_paymt.CLM_OVER_PAYMT_PAYE_CD) &
        (df_Transform.LOB_NO == df_k_clm_over_paymt.LOB_NO)
    )

    df_W_Extract = (
        df_Transform.alias("w")
        .join(df_k_clm_over_paymt.alias("k"), join_expr, "left")
        .select(
            F.coalesce("k.CLM_OVER_PAYMT_SK", F.lit(-1)).alias("CLM_OVER_PAYMT_SK"),
            "w.SRC_SYS_CD_SK",
            "w.CLM_ID",
            "w.CLM_OVER_PAYMT_PAYE_CD",
            "w.LOB_NO",
            F.coalesce("k.CRT_RUN_CYC_EXCTN_SK", F.lit(CurrRunCycle)).alias("CRT_RUN_CYC_EXCTN_SK")
        )
    )

    # --------------------------------------------------
    # STEP 3 : PrimaryKey transformer logic
    # --------------------------------------------------
    df_enriched = (
        df_W_Extract
        .withColumn(
            "svInstUpdt",
            F.when(F.col("CLM_OVER_PAYMT_SK") == -1, F.lit("I")).otherwise(F.lit("U"))
        )
        .withColumn("svLobNo", F.trim(F.col("LOB_NO")))
    )

    # Surrogate key generation
    df_enriched = SurrogateKeyGen(
        df_enriched,
        <DB sequence name>,
        "CLM_OVER_PAYMT_SK",
        <schema>,
        <secret_name>
    )

    df_enriched = df_enriched.withColumn(
        "svCrtRunCycExctnSk",
        F.when(F.col("svInstUpdt") == "I", F.lit(CurrRunCycle)).otherwise(F.col("CRT_RUN_CYC_EXCTN_SK"))
    )

    # --------------------------------------------------
    # STEP 4 : Build transformer outputs
    # --------------------------------------------------
    # updt link
    df_updt = (
        df_enriched.select(
            F.lit(SrcSysCd).alias("SRC_SYS_CD"),
            "CLM_ID",
            F.col("CLM_OVER_PAYMT_PAYE_CD"),
            F.col("svLobNo").alias("LOB_NO"),
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.col("CLM_OVER_PAYMT_SK")
        )
    )

    # NewKeys link (insert-only)
    df_newKeys = (
        df_enriched.filter(F.col("svInstUpdt") == "I")
        .select(
            "SRC_SYS_CD_SK",
            "CLM_ID",
            "CLM_OVER_PAYMT_PAYE_CD",
            F.col("svLobNo").alias("LOB_NO"),
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            "CLM_OVER_PAYMT_SK"
        )
    )

    # Keys link
    df_keys = (
        df_enriched.select(
            "CLM_OVER_PAYMT_SK",
            "SRC_SYS_CD_SK",
            F.lit(SrcSysCd).alias("SRC_SYS_CD"),
            "CLM_ID",
            "CLM_OVER_PAYMT_PAYE_CD",
            F.col("svLobNo").alias("LOB_NO"),
            F.col("svInstUpdt").alias("INSRT_UPDT_CD"),
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK")
        )
    )

    # --------------------------------------------------
    # STEP 5 : Merge stage – combine AllColOut with Keys
    # --------------------------------------------------
    merge_join_expr = (
        (df_AllColl_dedup.CLM_ID == df_keys.CLM_ID) &
        (df_AllColl_dedup.LOB_NO == df_keys.LOB_NO)
    )

    df_Key = (
        df_AllColl_dedup.alias("all")
        .join(df_keys.alias("k"), merge_join_expr, "left")
        .select(
            "all.JOB_EXCTN_RCRD_ERR_SK",
            "k.INSRT_UPDT_CD",
            "all.DISCARD_IN",
            "all.PASS_THRU_IN",
            "all.FIRST_RECYC_DT",
            "all.ERR_CT",
            "all.RECYCLE_CT",
            "k.SRC_SYS_CD",
            "all.PRI_KEY_STRING",
            "k.CLM_OVER_PAYMT_SK",
            "all.CLM_ID",
            "all.CLM_OVER_PAYMT_PAYE_IND",
            "all.LOB_NO",
            F.col("k.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            "all.PAYMT_RDUCTN_ID",
            "all.ORIG_PAYE_PROV_ID",
            "all.ORIG_PAYE_SUB_CK",
            "all.ORIG_PAYMT_MBR_CK",
            "all.BYPS_AUTO_OVER_PAYMT_RDUCTN_IN",
            "all.ORIG_PCA_OVER_PAYMT_AMT",
            "all.OVER_PAYMT_AMT"
        )
    )

    # --------------------------------------------------
    # STEP 6 : Write sequential file K_CLM_OVER_PAYMT.dat
    # --------------------------------------------------
    seq_file_path = f"{adls_path}/load/K_CLM_OVER_PAYMT.dat"
    write_files(
        df_newKeys,
        seq_file_path,
        delimiter=",",
        mode="overwrite",
        is_pqruet=False,
        header=False,
        quote="\"",
        nullValue=None
    )

    # --------------------------------------------------
    # STEP 7 : Write parquet file for hf_clm_ovr_paymnt (scenario c)
    # --------------------------------------------------
    parquet_path = f"{adls_path}/ClmOverPaymtPK_updt.parquet"
    write_files(
        df_updt,
        parquet_path,
        delimiter=",",
        mode="overwrite",
        is_pqruet=True,
        header=True,
        quote="\"",
        nullValue=None
    )

    # --------------------------------------------------
    # Final output
    # --------------------------------------------------
    return df_Key
