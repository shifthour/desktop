# COMMAND ----------
# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

"""
COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 

DESCRIPTION:   Shared container used for Primary Keying of Drug Transaction job

CALLED BY : PSDrugTransExtr

PROCESSING:    



MODIFICATIONS:
Developer           Date                         Change Description                                                     Project/Altius #          Development Project          Code Reviewer          Date Reviewed  
-----------------------    ----------------------------     ----------------------------------------------------------------------------        ---------------------------     ------------------------------------       ----------------------------      -------------------------
Parik                 2008-08-18               Initial program                                                                  3567(Primary Key)             devlIDS                      Steph Goddard         08/22/2008

Madhavan B      2017-10-18              Removed Data Elelments                                                 TFS 20094                IntegrateDev1                 Jag Yelavarthi            2017-10-18
"""

# This container is used in:
# PSDrugTransExtr
#
# Hash file (hf_drug_trans_allcol) cleared in calling program
# join primary key info with table info
# update primary key table (K_DRUG_TRANS) with new keys created today
# primary key hash file only contains current run keys and is cleared before writing
# SQL joins temp table with key table to assign known keys
# Temp table is truncated before load and runstat done after load
# Load IDS temp. table

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# COMMAND ----------

def run_DrugTransPK(
    df_AllCol: DataFrame,
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    """
    Executes the DrugTransPK shared-container logic.

    Parameters
    ----------
    df_AllCol : DataFrame
        Input stream from link 'AllCol'.
    df_Transform : DataFrame
        Input stream from link 'Transform'.
    params : dict
        Runtime parameters and JDBC configurations.

    Returns
    -------
    DataFrame
        Output stream for link 'Key'.
    """

    # --------------------------------------------------
    # Unpack parameters (exactly once)
    # --------------------------------------------------
    CurrRunCycle       = params["CurrRunCycle"]
    IDSOwner           = params["IDSOwner"]
    ids_secret_name    = params["ids_secret_name"]
    SrcSysCd           = params["SrcSysCd"]
    ids_jdbc_url       = params["ids_jdbc_url"]
    ids_jdbc_props     = params["ids_jdbc_props"]
    adls_path          = params["adls_path"]
    adls_path_raw      = params["adls_path_raw"]
    adls_path_publish  = params["adls_path_publish"]

    # --------------------------------------------------
    # Step 1 – Write df_Transform to {IDSOwner}.K_DRUG_TRANS_TEMP
    # --------------------------------------------------
    df_Transform.write.format("jdbc") \
        .option("url", ids_jdbc_url) \
        .options(**ids_jdbc_props) \
        .option("dbtable", f"{IDSOwner}.K_DRUG_TRANS_TEMP") \
        .mode("overwrite") \
        .save()

    after_sql = (
        f"CALL SYSPROC.ADMIN_CMD("
        f"'runstats on table {IDSOwner}.K_DRUG_TRANS_TEMP "
        f"on key columns with distribution on key columns and detailed indexes all allow write access')"
    )
    execute_dml(after_sql, ids_jdbc_url, ids_jdbc_props)

    # --------------------------------------------------
    # Step 2 – Extract W_Extract from database
    # --------------------------------------------------
    extract_query = f"""
        SELECT k.DRUG_TRANS_SK,
               w.SRC_SYS_CD_SK,
               w.DRUG_TRANS_CK,
               w.ACCTG_DT_SK,
               k.CRT_RUN_CYC_EXCTN_SK
        FROM {IDSOwner}.K_DRUG_TRANS_TEMP w,
             {IDSOwner}.K_DRUG_TRANS k
        WHERE w.SRC_SYS_CD_SK     = k.SRC_SYS_CD_SK
          AND w.DRUG_TRANS_CK     = k.DRUG_TRANS_CK
          AND w.ACCTG_DT_SK       = k.ACCTG_DT_SK

        UNION

        SELECT -1,
               w2.SRC_SYS_CD_SK,
               w2.DRUG_TRANS_CK,
               w2.ACCTG_DT_SK,
               {CurrRunCycle}
        FROM {IDSOwner}.K_DRUG_TRANS_TEMP w2
        WHERE NOT EXISTS (
              SELECT k2.DRUG_TRANS_SK
              FROM {IDSOwner}.K_DRUG_TRANS k2
              WHERE w2.SRC_SYS_CD_SK = k2.SRC_SYS_CD_SK
                AND w2.DRUG_TRANS_CK = k2.DRUG_TRANS_CK
                AND w2.ACCTG_DT_SK   = k2.ACCTG_DT_SK
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
    # Step 3 – PrimaryKey Transformer Logic
    # --------------------------------------------------
    df_enriched = (
        df_W_Extract
        .withColumn(
            "svInstUpdt",
            F.when(F.col("DRUG_TRANS_SK") == F.lit(-1), F.lit("I")).otherwise(F.lit("U"))
        )
        .withColumn("svSrcSysCd", F.lit(SrcSysCd))
        .withColumn(
            "svCrtRunCycExctnSk",
            F.when(F.col("svInstUpdt") == F.lit("I"), F.lit(CurrRunCycle)).otherwise(F.col("CRT_RUN_CYC_EXCTN_SK"))
        )
    )

    # Surrogate key generation
    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"DRUG_TRANS_SK",<schema>,<secret_name>)
    df_enriched = df_enriched.withColumn("svSK", F.col("DRUG_TRANS_SK"))

    # Output link: updt (to hf_drug_trans)
    df_updt = (
        df_enriched.select(
            F.col("svSrcSysCd").alias("SRC_SYS_CD"),
            "DRUG_TRANS_CK",
            "ACCTG_DT_SK",
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.col("svSK").alias("DRUG_TRANS_SK")
        )
    )

    # Output link: NewKeys (sequential file) – apply constraint svInstUpdt = 'I'
    df_NewKeys = (
        df_enriched
        .filter(F.col("svInstUpdt") == F.lit("I"))
        .select(
            "SRC_SYS_CD_SK",
            "DRUG_TRANS_CK",
            "ACCTG_DT_SK",
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.col("svSK").alias("DRUG_TRANS_SK")
        )
    )

    # Output link: Keys (to Merge)
    df_KeysForMerge = (
        df_enriched.select(
            F.col("svSK").alias("DRUG_TRANS_SK"),
            "SRC_SYS_CD_SK",
            F.col("svSrcSysCd").alias("SRC_SYS_CD"),
            "DRUG_TRANS_CK",
            "ACCTG_DT_SK",
            F.col("svInstUpdt").alias("INSRT_UPDT_CD"),
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK")
        )
    )

    # --------------------------------------------------
    # Step 4 – hf_drug_trans_allcol Hash Replace (dedup)
    # --------------------------------------------------
    df_AllCol_dedup = dedup_sort(
        df_AllCol,
        ["SRC_SYS_CD_SK", "DRUG_TRANS_CK", "ACCTG_DT_SK"],
        []
    )

    # --------------------------------------------------
    # Step 5 – Merge Transformer Logic
    # --------------------------------------------------
    join_condition = (
        (F.col("all.SRC_SYS_CD_SK") == F.col("k.SRC_SYS_CD_SK")) &
        (F.col("all.DRUG_TRANS_CK")  == F.col("k.DRUG_TRANS_CK")) &
        (F.col("all.ACCTG_DT_SK")    == F.col("k.ACCTG_DT_SK"))
    )

    df_Key = (
        df_AllCol_dedup.alias("all")
        .join(df_KeysForMerge.alias("k"), join_condition, "left")
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
            F.col("k.DRUG_TRANS_SK"),
            F.col("all.DRUG_TRANS_CK"),
            F.col("all.ACCTG_DT_SK"),
            F.col("k.CRT_RUN_CYC_EXCTN_SK"),
            F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            F.col("all.FNCL_LOB"),
            F.col("all.GRP"),
            F.col("all.PROD"),
            F.col("all.DRUG_TRANS_LOB_CD"),
            F.col("all.BILL_CMPNT_ID")
        )
    )

    # --------------------------------------------------
    # Step 6 – Write outputs (hash/parquet and sequential)
    # --------------------------------------------------
    # Write hf_drug_trans (as parquet)
    write_files(
        df_updt,
        f"{adls_path}/DrugTransPK_updt.parquet",
        delimiter=",",
        mode="overwrite",
        is_pqruet=True,
        header=True
    )

    # Write K_DRUG_TRANS.dat (sequential CSV)
    write_files(
        df_NewKeys,
        f"{adls_path}/load/K_DRUG_TRANS.dat",
        delimiter=",",
        mode="overwrite",
        is_pqruet=False,
        header=False
    )

    # Return container output link 'Key'
    return df_Key
# COMMAND ----------