
# COMMAND ----------
# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------
"""
Job Name       : PaymtRductnPK
Folder Path    : Shared Containers/PrimaryKey
Description    : ***************************************************************************************************************************************************************
COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 

CALLED BY: FctsClmPayRductnExtr

PROCESSING:    Shared container used for Primary Keying of Claim payment reduction job


MODIFICATIONS:
Developer           Date                         Change Description                                                     Project/Altius #          Development Project          Code Reviewer          Date Reviewed  
-----------------------    ----------------------------     ----------------------------------------------------------------------------        ---------------------------     ------------------------------------       ----------------------------      -------------------------
Bhoomi Dasari    2008-07-17               Initial program                                                               3567 Primary Key    devlIDS                               Steph Goddard          07-28-2008

Manasa Andru    2020-10-10               Changed the Update action in the Seq File        US - 294883              IntegrateDev1                   Jaideep Mankala       10/10/2020

Annotations:
- IDS Primary Key Container for Claims
- Used by  FctsClmPayRductnExtr
- join primary key info with table info
- Load IDS temp. table
- Assign primary surrogate key
- Temp table is tuncated before load and runstat done after load
- SQL joins temp table with key table to assign known keys
- Primary key hash file only contains current run keys.  Clear by <Source><Subject>PKHashClearJC at beginning of job stream.
- update primary key table (K_PAYMT_RDUCTN) with new keys created today
- Hash file (hf_paymt_reductn_allcol) cleared in the calling program - FctsClmPayRductnExtr
"""
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# COMMAND ----------
def run_PaymtRductnPK(
        df_AllColl: DataFrame,
        df_Transform: DataFrame,
        params: dict
    ) -> DataFrame:
    """
    Executes the logic of the PaymtRductnPK shared container.

    Parameters
    ----------
    df_AllColl : DataFrame
        Input DataFrame corresponding to link 'AllColl'.
    df_Transform : DataFrame
        Input DataFrame corresponding to link 'Transform'.
    params : dict
        Runtime parameters and JDBC configuration.

    Returns
    -------
    DataFrame
        Output DataFrame corresponding to link 'Key'.
    """

    # ------------------------------------------------------------------
    # Unpack parameters (exactly once)
    # ------------------------------------------------------------------
    CurrRunCycle        = params["CurrRunCycle"]
    CurrDate            = params["CurrDate"]
    SrcSysCdSk          = params["SrcSysCdSk"]
    SrcSysCd            = params["SrcSysCd"]
    RunID               = params["RunID"]
    IDSOwner            = params["IDSOwner"]
    ids_secret_name     = params["ids_secret_name"]
    ids_jdbc_url        = params["ids_jdbc_url"]
    ids_jdbc_props      = params["ids_jdbc_props"]
    FilePath            = params.get("FilePath", "")  # may or may not be present
    adls_path           = params["adls_path"]
    adls_path_raw       = params["adls_path_raw"]
    adls_path_publish   = params["adls_path_publish"]
    # ------------------------------------------------------------------
    # Scenario-a hash file: hf_paymt_reductn_allcol – deduplicate
    # ------------------------------------------------------------------
    df_AllColl_dedup = dedup_sort(
        df_AllColl,
        ["SRC_SYS_CD_SK", "PAYMT_RDUCTN_REF_ID", "PAYMT_RDUCTN_SUBTYP_CD"],
        [("SRC_SYS_CD_SK", "A")]
    )

    # ------------------------------------------------------------------
    # Stage: K_PAYMT_RDUCTN_TEMP  (DB2Connector – write, runstats, read)
    # ------------------------------------------------------------------
    # Clear/prepare temp table
    execute_dml(
        f"DELETE FROM {IDSOwner}.K_PAYMT_RDUCTN_TEMP",
        ids_jdbc_url,
        ids_jdbc_props
    )

    # Write incoming rows to the temp table
    (
        df_Transform.write
        .format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("dbtable", f"{IDSOwner}.K_PAYMT_RDUCTN_TEMP")
        .mode("append")
        .save()
    )

    # Run statistics (after SQL)
    execute_dml(
        f"CALL SYSPROC.ADMIN_CMD('runstats on table {IDSOwner}.K_PAYMT_RDUCTN_TEMP on key columns "
        f"with distribution on key columns and detailed indexes all allow write access')",
        ids_jdbc_url,
        ids_jdbc_props
    )

    # Extract query (Azure-SQL-compliant)
    extract_query = f"""
        SELECT k.PAYMT_RDUCTN_SK,
               w.SRC_SYS_CD_SK,
               w.PAYMT_RDUCTN_REF_ID,
               w.PAYMT_RDUCTN_SUBTYP_CD,
               k.CRT_RUN_CYC_EXCTN_SK
        FROM   {IDSOwner}.K_PAYMT_RDUCTN_TEMP w
        JOIN   {IDSOwner}.K_PAYMT_RDUCTN       k
          ON   w.SRC_SYS_CD_SK         = k.SRC_SYS_CD_SK
         AND   w.PAYMT_RDUCTN_REF_ID   = k.PAYMT_RDUCTN_REF_ID
         AND   w.PAYMT_RDUCTN_SUBTYP_CD= k.PAYMT_RDUCTN_SUBTYP_CD

        UNION

        SELECT -1,
               w2.SRC_SYS_CD_SK,
               w2.PAYMT_RDUCTN_REF_ID,
               w2.PAYMT_RDUCTN_SUBTYP_CD,
               {CurrRunCycle}
        FROM   {IDSOwner}.K_PAYMT_RDUCTN_TEMP w2
        WHERE  NOT EXISTS (
                SELECT 1
                FROM   {IDSOwner}.K_PAYMT_RDUCTN k2
                WHERE  w2.SRC_SYS_CD_SK          = k2.SRC_SYS_CD_SK
                  AND  w2.PAYMT_RDUCTN_REF_ID    = k2.PAYMT_RDUCTN_REF_ID
                  AND  w2.PAYMT_RDUCTN_SUBTYP_CD = k2.PAYMT_RDUCTN_SUBTYP_CD
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
    # Transformer: PrimaryKey
    # ------------------------------------------------------------------
    df_enriched = (
        df_W_Extract
        .withColumn("svInstUpdt", F.when(F.col("PAYMT_RDUCTN_SK") == F.lit(-1), F.lit("I")).otherwise(F.lit("U")))
        .withColumn("svSrcSysCd", F.lit("FACETS"))
        .withColumn("svCrtRunCycExctnSk",
                    F.when(F.col("svInstUpdt") == F.lit("I"), F.lit(CurrRunCycle))
                     .otherwise(F.col("CRT_RUN_CYC_EXCTN_SK")))
    )

    # Surrogate Key generation (mandatory placeholder values per requirements)
    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"PAYMT_RDUCTN_SK",<schema>,<secret_name>)

    # Preserve surrogate key for downstream use
    df_enriched = df_enriched.withColumn("svSK", F.col("PAYMT_RDUCTN_SK"))

    # ------------------------------------------------------------------
    # Outputs from PrimaryKey transformer
    # ------------------------------------------------------------------
    # updt (to hf_paymt_reductn – parquet)
    df_updt = (
        df_enriched.select(
            F.col("svSrcSysCd").alias("SRC_SYS_CD_SK"),
            "PAYMT_RDUCTN_REF_ID",
            "PAYMT_RDUCTN_SUBTYP_CD",
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.col("svSK").alias("PAYMT_RDUCTN_SK")
        )
    )

    # NewKeys (to K_PAYMT_RDUCTN – sequential file)
    df_NewKeys = (
        df_enriched
        .filter(F.col("svInstUpdt") == F.lit("I"))
        .select(
            "SRC_SYS_CD_SK",
            "PAYMT_RDUCTN_REF_ID",
            "PAYMT_RDUCTN_SUBTYP_CD",
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.col("svSK").alias("PAYMT_RDUCTN_SK")
        )
    )

    # Keys (to Merge transformer)
    df_Keys = (
        df_enriched.select(
            F.col("svSK").alias("PAYMT_RDUCTN_SK"),
            "SRC_SYS_CD_SK",
            F.col("svSrcSysCd").alias("SRC_SYS_CD"),
            "PAYMT_RDUCTN_REF_ID",
            "PAYMT_RDUCTN_SUBTYP_CD",
            F.col("svInstUpdt").alias("INSRT_UPDT_CD"),
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK")
        )
    )

    # ------------------------------------------------------------------
    # Transformer: Merge  (join AllColOut with Keys)
    # ------------------------------------------------------------------
    df_Key_out = (
        df_AllColl_dedup.alias("all")
        .join(
            df_Keys.alias("k"),
            (F.col("all.SRC_SYS_CD_SK") == F.col("k.SRC_SYS_CD_SK")) &
            (F.col("all.PAYMT_RDUCTN_REF_ID") == F.col("k.PAYMT_RDUCTN_REF_ID")) &
            (F.col("all.PAYMT_RDUCTN_SUBTYP_CD") == F.col("k.PAYMT_RDUCTN_SUBTYP_CD")),
            "left"
        )
        .select(
            F.col("all.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
            F.col("k.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
            F.col("all.DISCARD_IN").alias("DISCARD_IN"),
            F.col("all.PASS_THRU_IN").alias("PASS_THRU_IN"),
            F.col("all.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
            F.col("all.ERR_CT").alias("ERR_CT"),
            F.col("all.RECYCLE_CT").alias("RECYCLE_CT"),
            F.col("k.SRC_SYS_CD").alias("SRC_SYS_CD"),
            F.col("all.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
            F.col("k.PAYMT_RDUCTN_SK").alias("PAYMT_RDUCTN_SK"),
            F.col("all.PAYMT_RDUCTN_REF_ID").alias("PAYMT_RDUCTN_REF_ID"),
            F.col("all.PAYMT_RDUCTN_SUBTYP_CD").alias("PAYMT_RDUCTN_SUBTYP_CD"),
            F.col("k.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            F.col("all.PAYEE_PROVDER_ID").alias("PAYEE_PROVDER_ID"),
            F.col("all.USER_ID").alias("USER_ID"),
            F.col("all.PAYMT_RDUCTN_EXCD_ID").alias("PAYMT_RDUCTN_EXCD_ID"),
            F.col("all.PAYMT_RDUCTN_PAYE_TYP_CD").alias("PAYMT_RDUCTN_PAYE_TYP_CD"),
            F.col("all.PAYMT_RDUCTN_PRM_TYP_CD").alias("PAYMT_RDUCTN_PRM_TYP_CD"),
            F.col("all.PAYMT_RDUCTN_STTUS_CD").alias("PAYMT_RDUCTN_STTUS_CD"),
            F.col("all.PAYMT_RDUCTN_TYP_CD").alias("PAYMT_RDUCTN_TYP_CD"),
            F.col("all.AUTO_PAYMT_RDUCTN_IN").alias("AUTO_PAYMT_RDUCTN_IN"),
            F.col("all.CRT_DT").alias("CRT_DT"),
            F.col("all.PLN_YR_DT").alias("PLN_YR_DT"),
            F.col("all.ORIG_RDUCTN_AMT").alias("ORIG_RDUCTN_AMT"),
            F.col("all.PCA_OVERPD_NET_AMT").alias("PCA_OVERPD_NET_AMT"),
            F.col("all.RCVD_AMT").alias("RCVD_AMT"),
            F.col("all.RCVRED_AMT").alias("RCVRED_AMT"),
            F.col("all.REMN_NET_AMT").alias("REMN_NET_AMT"),
            F.col("all.WRT_OFF_AMT").alias("WRT_OFF_AMT"),
            F.col("all.RDUCTN_DESC").alias("RDUCTN_DESC"),
            F.col("all.TAX_YR").alias("TAX_YR")
        )
    )

    # ------------------------------------------------------------------
    # Write Outputs
    # ------------------------------------------------------------------
    # a) hf_paymt_reductn (parquet – scenario-c)
    write_files(
        df_updt,
        f"{adls_path}/PaymtRductnPK_updt.parquet",
        delimiter=",",
        mode="overwrite",
        is_pqruet=True,
        header=True,
        quote="\"",
        nullValue=None
    )

    # b) K_PAYMT_RDUCTN (sequential file)
    seq_file_path = f"{adls_path}/{{FilePath}}/load/K_PAYMT_RDUCTN.dat"
    write_files(
        df_NewKeys,
        seq_file_path,
        delimiter=",",
        mode="append",
        is_pqruet=False,
        header=False,
        quote="\"",
        nullValue=None
    )

    # ------------------------------------------------------------------
    # Return container output
    # ------------------------------------------------------------------
    return df_Key_out
# COMMAND ----------
