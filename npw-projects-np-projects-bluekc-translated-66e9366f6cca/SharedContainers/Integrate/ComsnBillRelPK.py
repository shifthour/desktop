# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

"""
Shared Container : ComsnBillRelPK
Description      : Shared container used for Primary Keying of Comm Bill Rel job
Called By        : FctsComsnBillRelExtr

* VC LOGS *
^1_1 02/20/09 11:01:04 Batch  15027_39667 PROMOTE bckcetl ids20 dsadm bls for sa
^1_1 02/20/09 10:38:04 Batch  15027_38287 INIT bckcett testIDS dsadm bls for sa
^1_2 02/19/09 15:36:15 Batch  15026_56186 PROMOTE bckcett testIDS u03651 steph for Sharon - primary key containers
^1_2 02/19/09 15:33:19 Batch  15026_56002 INIT bckcett devlIDS u03651 steffy
^1_1 01/13/09 08:41:23 Batch  14989_31287 INIT bckcett devlIDS u03651 steffy

***************************************************************************************************************************************************************
COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 


Annotations:
IDS Primary Key Container for Commission Bill Comsn
Used by  FctsComsnBillRelExtr
primary key hash file only contains current run keys and is cleared before writing
update primary key table (K_COMSN_BILL_REL) with new keys created today
Assign primary surrogate key
Hash file (hf_comsn_bill_rel_allcol) cleared in calling job
join primary key info with table info
Load IDS temp. table
Temp table is tuncated before load and runstat done after load
SQL joins temp table with key table to assign known keys
"""

def run_ComsnBillRelPK(
    df_AllCol: DataFrame,
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    """
    Executes the logic of shared container ComsnBillRelPK.

    Parameters
    ----------
    df_AllCol : DataFrame
        Input stream carrying all commission bill relationship columns.
    df_Transform : DataFrame
        Input stream used to create/update primary keys.
    params : dict
        Runtime parameters and configurations.

    Returns
    -------
    DataFrame
        Output stream 'Key' with assigned surrogate keys and related columns.
    """

    # --------------------------------------------------
    # Unpack parameters (each exactly once)
    # --------------------------------------------------
    CurrRunCycle        = params["CurrRunCycle"]
    SrcSysCd            = params["SrcSysCd"]
    IDSOwner            = params["IDSOwner"]
    ids_secret_name     = params["ids_secret_name"]
    ids_jdbc_url        = params["ids_jdbc_url"]
    ids_jdbc_props      = params["ids_jdbc_props"]
    adls_path           = params["adls_path"]
    adls_path_raw       = params["adls_path_raw"]
    adls_path_publish   = params["adls_path_publish"]
    # --------------------------------------------------

    # --------------------------------------------------
    # Stage : hf_comsn_bill_rel_allcol  (scenario a – intermediate hash file)
    #          Replace with deduplicate logic
    # --------------------------------------------------
    df_AllColOut = dedup_sort(
        df_AllCol,
        partition_cols=["SRC_SYS_CD_SK", "COBL_CK"],
        sort_cols=[("SRC_SYS_CD_SK", "A")]
    )

    # --------------------------------------------------
    # Stage : K_COMSN_BILL_REL (database table lookup)
    # --------------------------------------------------
    extract_query_k = f"""
        SELECT COMSN_BILL_REL_SK,
               SRC_SYS_CD_SK,
               COMSN_BILL_REL_UNIQ_KEY,
               CRT_RUN_CYC_EXCTN_SK
        FROM {IDSOwner}.K_COMSN_BILL_REL
    """
    df_k_comsn_bill_rel = (
        spark.read.format("jdbc")
             .option("url", ids_jdbc_url)
             .options(**ids_jdbc_props)
             .option("query", extract_query_k)
             .load()
    )

    # --------------------------------------------------
    # Build W_Extract equivalent (union logic from DataStage)
    # --------------------------------------------------
    df_W_Extract = (
        df_Transform.alias("w")
        .join(
            df_k_comsn_bill_rel.alias("k"),
            on=[
                F.col("w.SRC_SYS_CD_SK") == F.col("k.SRC_SYS_CD_SK"),
                F.col("w.COMSN_BILL_REL_UNIQ_KEY") == F.col("k.COMSN_BILL_REL_UNIQ_KEY")
            ],
            how="left"
        )
        .select(
            F.coalesce(F.col("k.COMSN_BILL_REL_SK"), F.lit(-1)).alias("COMSN_BILL_REL_SK"),
            F.col("w.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
            F.col("w.COMSN_BILL_REL_UNIQ_KEY").alias("COMSN_BILL_REL_UNIQ_KEY"),
            F.coalesce(F.col("k.CRT_RUN_CYC_EXCTN_SK"), F.lit(CurrRunCycle)).alias("CRT_RUN_CYC_EXCTN_SK")
        )
    )

    # --------------------------------------------------
    # Transformer : PrimaryKey
    # --------------------------------------------------
    df_enriched = (
        df_W_Extract
        .withColumn(
            "INSRT_UPDT_CD",
            F.when(F.col("COMSN_BILL_REL_SK") == F.lit(-1), F.lit("I")).otherwise(F.lit("U"))
        )
        .withColumn("SRC_SYS_CD", F.lit(SrcSysCd))
    )

    # Derive runtime-dependent columns before surrogate-key generation
    df_enriched = df_enriched.withColumn(
        "CRT_RUN_CYC_EXCTN_SK",
        F.when(F.col("INSRT_UPDT_CD") == F.lit("I"), F.lit(CurrRunCycle)).otherwise(F.col("CRT_RUN_CYC_EXCTN_SK"))
    )

    # Set COMSN_BILL_REL_SK to null for rows that need a new surrogate key
    df_enriched = df_enriched.withColumn(
        "COMSN_BILL_REL_SK",
        F.when(F.col("INSRT_UPDT_CD") == F.lit("I"), F.lit(None)).otherwise(F.col("COMSN_BILL_REL_SK"))
    )

    # --------------------------------------------------
    # Surrogate Key Generation
    # --------------------------------------------------
    df_enriched = SurrogateKeyGen(
        df_enriched,
        <DB sequence name>,
        "COMSN_BILL_REL_SK",
        <schema>,
        <secret_name>
    )

    # --------------------------------------------------
    # Build ‘updt’ DataFrame for hf_comsn_bill_rel
    # --------------------------------------------------
    df_updt = df_enriched.select(
        F.col("SRC_SYS_CD"),
        F.col("COMSN_BILL_REL_UNIQ_KEY"),
        F.col("CRT_RUN_CYC_EXCTN_SK"),
        F.col("COMSN_BILL_REL_SK")
    )

    # --------------------------------------------------
    # Build ‘NewKeys’ DataFrame for sequential file
    # --------------------------------------------------
    df_newkeys = (
        df_enriched
        .filter(F.col("INSRT_UPDT_CD") == F.lit("I"))
        .select(
            F.col("SRC_SYS_CD_SK"),
            F.col("COMSN_BILL_REL_UNIQ_KEY"),
            F.col("CRT_RUN_CYC_EXCTN_SK"),
            F.col("COMSN_BILL_REL_SK")
        )
    )

    # --------------------------------------------------
    # Build ‘Keys’ DataFrame for merge
    # --------------------------------------------------
    df_keys = df_enriched.select(
        F.col("COMSN_BILL_REL_SK"),
        F.col("SRC_SYS_CD_SK"),
        F.col("COMSN_BILL_REL_UNIQ_KEY"),
        F.col("SRC_SYS_CD"),
        F.col("INSRT_UPDT_CD"),
        F.col("CRT_RUN_CYC_EXCTN_SK")
    )

    # --------------------------------------------------
    # Stage : Merge
    # --------------------------------------------------
    df_merge = (
        df_AllColOut.alias("all")
        .join(
            df_keys.alias("k"),
            on=[
                F.col("all.SRC_SYS_CD_SK") == F.col("k.SRC_SYS_CD_SK"),
                F.col("all.COBL_CK") == F.col("k.COMSN_BILL_REL_UNIQ_KEY")
            ],
            how="left"
        )
    )

    df_key_output = df_merge.select(
        F.col("all.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.col("k.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        F.col("all.DISCARD_IN").alias("DISCARD_IN"),
        F.col("all.PASS_THRU_IN").alias("PASS_THRU_IN"),
        F.col("all.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        F.col("all.ERR_CT").alias("ERR_CT"),
        F.col("all.RECYCLE_CT").alias("RECYCLE_CT"),
        F.col("k.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("all.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        F.col("k.COMSN_BILL_REL_SK").alias("COMSN_BILL_REL_SK"),
        F.col("all.COBL_CK").alias("COMSN_BILL_REL_UNIQ_KEY"),
        F.col("k.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("all.COCE_ID").alias("COCE_ID"),
        F.col("all.COAR_ID").alias("COAR_ID"),
        F.col("all.COAG_EFF_DT").alias("COAG_EFF_DT"),
        F.col("all.COSC_CALC_METH").alias("COSC_CALC_METH"),
        F.col("all.COBL_SOURCE_CD").alias("COBL_SOURCE_CD"),
        F.col("all.COBL_STS").alias("COBL_STS"),
        F.col("all.COBL_BASIS").alias("COMSN_BSS_AMT"),
        F.col("all.COBL_SOURCE_AMT").alias("INCM_AMT"),
        F.col("all.COBL_PER_BILL_CTR").alias("BILL_CT"),
        F.col("all.CORQ_CK").alias("COMSN_RELCALC_RQST_UNIQ_KEY"),
        F.col("all.COBL_SOURCE_CK").alias("INCM_UNIQ_KEY"),
        F.col("all.COCP_COMM_PER").alias("COMSN_PERD_NO")
    )

    # --------------------------------------------------
    # Write parquet for hf_comsn_bill_rel   (scenario c)
    # --------------------------------------------------
    write_files(
        df_updt,
        f"{adls_path}/ComsnBillRelPK_updt.parquet",
        delimiter=",",
        mode="overwrite",
        is_pqruet=True,
        header=True,
        quote="\"",
        nullValue=None
    )

    # --------------------------------------------------
    # Write sequential file for NewKeys
    # --------------------------------------------------
    write_files(
        df_newkeys,
        f"{adls_path}/load/K_COMSN_BILL_REL.dat",
        delimiter=",",
        mode="overwrite",
        is_pqruet=False,
        header=False,
        quote="\"",
        nullValue=None
    )

    # --------------------------------------------------
    # Return container output link(s)
    # --------------------------------------------------
    return df_key_output