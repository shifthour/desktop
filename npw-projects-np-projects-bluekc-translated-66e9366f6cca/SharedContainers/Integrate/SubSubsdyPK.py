# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ---------- 

"""
Shared Container: SubSubsdyPK
Folder Path     : Shared Containers/PrimaryKey
Description     :  Shared container used for Primary Keying of Subscriber PCA job

* VC LOGS *
^1_2 03/13/09 10:08:43 Batch  15048_36533 PROMOTE bckcetl ids20 dsadm bls for hs
^1_2 03/13/09 10:06:43 Batch  15048_36406 INIT bckcett testIDSnew dsadm bls for hs
^1_1 02/10/09 14:50:27 Batch  15017_53431 PROMOTE bckcetl ids20 dsadm bls for rt
^1_1 02/10/09 14:38:48 Batch  15017_52730 INIT bckcett testIDSnew dsadm bls for rt
^1_2 01/16/09 09:14:22 Batch  14992_33385 PROMOTE bckcett testIDSnew u06640 Ralph
^1_2 01/15/09 15:57:37 Batch  14991_57464 PROMOTE bckcett testIDSnew u06640 Ralph
^1_2 01/15/09 15:48:27 Batch  14991_56913 INIT bckcett devlIDS u06640 Ralph
^1_1 01/15/09 10:49:15 Batch  14991_38962 INIT bckcett devlIDS u06640 Ralph
^1_3 01/14/09 13:25:39 Batch  14990_48341 INIT bckcett devlIDS u03651 steffy
^1_2 01/06/09 09:21:07 Batch  14982_33669 INIT bckcett devlIDS u03651 steffy
^1_1 12/11/08 11:15:25 Batch  14956_40534 INIT bckcett devlIDS u03651 steffy

Annotations
-----------
update primary key table (K_SUB_PCA) with new keys created today
join primary key info with table info
Hash file (hf_sub_pca_allcol) cleared in calling program
This container is used in:
FctsSubSbsdyExtr

These programs need to be re-compiled when logic changes
Load IDS temp. table
Temp table is tuncated before load and runstat done after load
SQL joins temp table with key table to assign known keys
primary key hash file only contains current run keys
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def run_SubSubsdyPK(
    df_AllCol: DataFrame,
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    """
    Executes the SubSubsdyPK shared-container logic.
    Parameters
    ----------
    df_AllCol : DataFrame
        Input stream “AllCol”.
    df_Transform : DataFrame
        Input stream “Transform”.
    params : dict
        Runtime parameters and JDBC configurations.
    Returns
    -------
    DataFrame
        Output stream “Key”.
    """

    # --------------------------------------------------
    # Unpack parameters (each key exactly once)
    # --------------------------------------------------
    CurrRunCycle        = params["CurrRunCycle"]
    IDSOwner            = params["IDSOwner"]
    ids_secret_name     = params["ids_secret_name"]
    ids_jdbc_url        = params["ids_jdbc_url"]
    ids_jdbc_props      = params["ids_jdbc_props"]
    adls_path           = params["adls_path"]
    adls_path_raw       = params["adls_path_raw"]
    adls_path_publish   = params["adls_path_publish"]
    SrcSysCd            = params["SrcSysCd"]

    # --------------------------------------------------
    # 1. Replace intermediate hash file: hf_subsbsdy_pkey_allcol
    #    – deduplicate by natural keys
    # --------------------------------------------------
    hash_keys = [
        "SUB_UNIQ_KEY",
        "SUB_SBSDY_TYP_CD",
        "CLS_PLN_PROD_CAT_CD",
        "EFF_DT_SK",
        "SRC_SYS_CD_SK"
    ]
    df_AllCol_dedup = df_AllCol.dropDuplicates(hash_keys)

    # --------------------------------------------------
    # 2. Replace intermediate hash file: hf_subsbsdy_pkey_dedupe
    #    – deduplicate and stage to IDS temp table
    # --------------------------------------------------
    df_Transform_dedup = df_Transform.dropDuplicates(hash_keys)

    (
        df_Transform_dedup.write
        .mode("overwrite")
        .format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("dbtable", f"{IDSOwner}.K_SUB_SBSDY_TEMP")
        .save()
    )

    execute_dml(
        f"CALL SYSPROC.ADMIN_CMD('runstats on table {IDSOwner}.K_SUB_SBSDY_TEMP "
        "on key columns with distribution on key columns and detailed indexes all allow write access')",
        ids_jdbc_url,
        ids_jdbc_props
    )

    # --------------------------------------------------
    # 3. Extract W_Extract dataset from IDS
    # --------------------------------------------------
    extract_query = f"""
    SELECT 
           w.SUB_UNIQ_KEY,
           w.SUB_SBSDY_TYP_CD,
           w.CLS_PLN_PROD_CAT_CD,
           w.EFF_DT_SK,
           w.SRC_SYS_CD_SK,
           k.CRT_RUN_CYC_EXCTN_SK,
           k.SUB_SBSDY_SK
    FROM {IDSOwner}.K_SUB_SBSDY_TEMP w,
         {IDSOwner}.K_SUB_SBSDY k
    WHERE
           w.SUB_UNIQ_KEY        = k.SUB_UNIQ_KEY AND
           w.SUB_SBSDY_TYP_CD    = k.SUB_SBSDY_TYP_CD AND
           w.CLS_PLN_PROD_CAT_CD = k.CLS_PLN_PROD_CAT_CD AND
           w.EFF_DT_SK           = k.EFF_DT_SK AND
           w.SRC_SYS_CD_SK       = k.SRC_SYS_CD_SK
    UNION
    SELECT 
           w2.SUB_UNIQ_KEY,
           w2.SUB_SBSDY_TYP_CD,
           w2.CLS_PLN_PROD_CAT_CD,
           w2.EFF_DT_SK,
           w2.SRC_SYS_CD_SK,
           {CurrRunCycle},
           -1
    FROM {IDSOwner}.K_SUB_SBSDY_TEMP w2
    WHERE NOT EXISTS (
        SELECT k2.SUB_SBSDY_SK
        FROM {IDSOwner}.K_SUB_SBSDY k2
        WHERE  w2.SUB_UNIQ_KEY        = k2.SUB_UNIQ_KEY AND
               w2.SUB_SBSDY_TYP_CD    = k2.SUB_SBSDY_TYP_CD AND
               w2.CLS_PLN_PROD_CAT_CD = k2.CLS_PLN_PROD_CAT_CD AND
               w2.EFF_DT_SK           = k2.EFF_DT_SK AND
               w2.SRC_SYS_CD_SK       = k2.SRC_SYS_CD_SK
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
    # 4. PrimaryKey Transformer logic
    # --------------------------------------------------
    df_enriched = (
        df_W_Extract
        .withColumn(
            "svInstUpdt",
            F.when(F.col("SUB_SBSDY_SK") == -1, F.lit("I")).otherwise(F.lit("U"))
        )
    )

    # Surrogate key generation
    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,'SUB_SBSDY_SK',<schema>,<secret_name>)

    df_enriched = (
        df_enriched
        .withColumn("svSrcSysCd", F.lit(SrcSysCd))
        .withColumn("svSK", F.col("SUB_SBSDY_SK"))
        .withColumn(
            "svCrtRunCycExctnSk",
            F.when(F.col("svInstUpdt") == "I", F.lit(CurrRunCycle))
             .otherwise(F.col("CRT_RUN_CYC_EXCTN_SK"))
        )
    )

    # updt link (for hf_sub_sbsdy parquet write)
    df_updt = df_enriched.select(
        F.col("svSrcSysCd").alias("SRC_SYS_CD"),
        "SUB_UNIQ_KEY",
        "SUB_SBSDY_TYP_CD",
        "CLS_PLN_PROD_CAT_CD",
        "EFF_DT_SK",
        F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("svSK").alias("SUB_SBSDY_SK")
    )

    # NewKeys link (for sequential file write)
    df_NewKeys = (
        df_enriched
        .filter(F.col("svInstUpdt") == "I")
        .select(
            "SUB_UNIQ_KEY",
            "SUB_SBSDY_TYP_CD",
            "CLS_PLN_PROD_CAT_CD",
            "EFF_DT_SK",
            "SRC_SYS_CD_SK",
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.col("svSK").alias("SUB_SBSDY_SK")
        )
    )

    # Keys link (for merge transformer)
    df_keys = df_enriched.select(
        F.col("svSK").alias("SUB_SBSDY_SK"),
        F.col("svSrcSysCd").alias("SRC_SYS_CD"),
        "SUB_UNIQ_KEY",
        "SUB_SBSDY_TYP_CD",
        "CLS_PLN_PROD_CAT_CD",
        "EFF_DT_SK",
        "SRC_SYS_CD_SK",
        F.col("svInstUpdt").alias("INSRT_UPDT_CD"),
        F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK")
    )

    # --------------------------------------------------
    # 5. Merge Transformer logic
    # --------------------------------------------------
    df_Key = (
        df_AllCol_dedup.alias("AllColOut")
        .join(
            df_keys.alias("Keys"),
            (
                (F.col("AllColOut.SUB_UNIQ_KEY")        == F.col("Keys.SUB_UNIQ_KEY")) &
                (F.col("AllColOut.SUB_SBSDY_TYP_CD")    == F.col("Keys.SUB_SBSDY_TYP_CD")) &
                (F.col("AllColOut.CLS_PLN_PROD_CAT_CD") == F.col("Keys.CLS_PLN_PROD_CAT_CD")) &
                (F.col("AllColOut.EFF_DT_SK")           == F.col("Keys.EFF_DT_SK")) &
                (F.col("AllColOut.SRC_SYS_CD_SK")       == F.col("Keys.SRC_SYS_CD_SK"))
            ),
            "left"
        )
        .select(
            F.col("AllColOut.JOB_EXCTN_RCRD_ERR_SK"),
            F.col("Keys.INSRT_UPDT_CD"),
            F.col("AllColOut.DISCARD_IN"),
            F.col("AllColOut.PASS_THRU_IN"),
            F.col("AllColOut.FIRST_RECYC_DT"),
            F.col("AllColOut.ERR_CT"),
            F.col("AllColOut.RECYCLE_CT"),
            F.col("Keys.SRC_SYS_CD"),
            F.col("AllColOut.PRI_KEY_STRING"),
            F.col("Keys.SUB_SBSDY_SK"),
            F.col("AllColOut.SUB_UNIQ_KEY"),
            F.col("AllColOut.SUB_SBSDY_TYP_CD"),
            F.col("AllColOut.CLS_PLN_PROD_CAT_CD"),
            F.col("AllColOut.EFF_DT_SK"),
            F.col("AllColOut.SRC_SYS_CD_SK"),
            F.col("Keys.CRT_RUN_CYC_EXCTN_SK"),
            F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            F.col("AllColOut.TERM_DT_SK"),
            F.col("AllColOut.SBSDY_AMT"),
            F.col("AllColOut.MESU_MCTR_TYPE_SRC_CD"),
            F.col("AllColOut.CSPD_CAT")
        )
    )

    # --------------------------------------------------
    # 6. Output handling
    # --------------------------------------------------
    write_files(
        df_NewKeys,
        f"{adls_path}/load/K_SUB_SBSDY.dat",
        delimiter=",",
        mode="overwrite",
        is_pqruet=False,
        header=False
    )

    write_files(
        df_updt,
        f"{adls_path}/SubSubsdyPK_updt.parquet",
        mode="overwrite",
        is_pqruet=True
    )

    return df_Key