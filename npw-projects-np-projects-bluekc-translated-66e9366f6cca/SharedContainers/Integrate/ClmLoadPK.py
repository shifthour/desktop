# Databricks notebook source
# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

# Databricks notebook source
"""
JobName: ClmLoadPK
FolderPath: Shared Containers/PrimaryKey
JobType: Server Job
JobCategory: DS_Integrate

Description:
Copyright 2008 Blue Cross/Blue Shield of Kansas City

CALLED BY:  FctsClmExtr

PROCESSING:   Assign / Create primary SK for IDS claim tables with natural key of source system and claim ID.

MODIFICATIONS:
Developer                Date                 (9)Project/Altiris #(9)Change Description(9)(9)(9)(9)Development Project(9)Code Reviewer(9)Date Reviewed
------------------              --------------------     (9)------------------------(9)-----------------------------------------------------------------------(9)--------------------------------(9)-------------------------------(9)----------------------------
Brent Leland	2008-07-28	3567 Primary Key     Original Programming                                               devlIDS                                   Steph Goddard        10/03/2008
                                                                                                 Added hash file hf_fcts_rcrd_del
Kalyan Neelam        2010-12-24              4616                         Added documentation                                              IntegrateNewDevl                  Steph Goddard         12/28/2010

Rick Henry              2012-05-04              4896                         Removed Data Element and Display                       NewDevl                                Sandrew                    2012-05-20
"""

# Assign primary surrogate key
# Hash file lookup used for adjusted claims
# hf_clm hash file used to key tables
# CLM, CLM_PCA, CLM_EXTRNL_REF_DATA, CLM_EXTRNL_MBRSH, CLM_EXTRNL_PROV, FCLTY_CLM, ITS_CLM, CLM_REMIT_HIST, CLM_ALT_PAYE, DRUG_CLM, PSEUDO_CLM, CLM_REMIT_HIST
# Claim Primary Key Container
# File is deleted at beginning of calling job.  File is appended to due to multiple keying interations
# Load IDS temp. table
# SQL joins temp table with key table to assign known keys
# Temp table is tuncated before load and runstat done after load
# primary key hash file only contains current run keys and is cleared before writing
# update primary key table (K_CLM) with new keys created today
# This container is used in:
# ESIClmInvoicePKExtr, ESIClmPKExtr, FctsClmPKExtr, FctsPcaClmPKExtr, MCSourceClmPKExtr, MedicaidClmPKExtr, NascoClmExtr, NascoClmPKExtr, PcsClmPKExtr, WellDyneClmPKExtr, MedtrakClmPKExtr
# These programs need to be re-compiled when logic changes

# Databricks notebook source
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# Databricks notebook source
def run_ClmLoadPK(
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    """
    Shared-container logic translated from DataStage to PySpark.
    Parameters
    ----------
    df_Transform : DataFrame
        Input stream matching the 'Transform' link.
    params : dict
        Runtime parameters and secrets.

    Returns
    -------
    DataFrame
        Output stream corresponding to link 'PK_Clms'.
    """

    # --------------------------------------------------
    # unpack parameters (each exactly once)
    SrcSysCd               = params["SrcSysCd"]
    CurrRunCycle           = params["CurrRunCycle"]
    IDSOwner               = params["IDSOwner"]
    ids_secret_name        = params["ids_secret_name"]
    ids_jdbc_url           = params["ids_jdbc_url"]
    ids_jdbc_props         = params["ids_jdbc_props"]
    adls_path              = params["adls_path"]
    adls_path_raw          = params["adls_path_raw"]
    adls_path_publish      = params["adls_path_publish"]
    # --------------------------------------------------

    # --------------------------------------------------
    # Step 1: Manage temporary table IDSOwner.K_CLM_TEMP
    # --------------------------------------------------
    execute_dml(
        f"DROP TABLE {IDSOwner}.K_CLM_TEMP",
        ids_jdbc_url,
        ids_jdbc_props
    )

    execute_dml(
        f"""CREATE TABLE {IDSOwner}.K_CLM_TEMP (
            SRC_SYS_CD_SK INTEGER NOT NULL,
            CLM_ID VARCHAR(20) NOT NULL,
            PRIMARY KEY (SRC_SYS_CD_SK, CLM_ID)
        )""",
        ids_jdbc_url,
        ids_jdbc_props
    )

    # load incoming records into the temp table
    (
        df_Transform
        .select("SRC_SYS_CD_SK", "CLM_ID")
        .write
        .format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("dbtable", f"{IDSOwner}.K_CLM_TEMP")
        .mode("append")
        .save()
    )

    # run statistics on temp table
    execute_dml(
        f"""CALL SYSPROC.ADMIN_CMD(
            'runstats on table {IDSOwner}.K_CLM_TEMP
             on key columns with distribution on key columns
             and detailed indexes all allow write access'
        )""",
        ids_jdbc_url,
        ids_jdbc_props
    )

    # --------------------------------------------------
    # Step 2: Extract data for Transformer (W_Extract)
    # --------------------------------------------------
    extract_query = f"""
        SELECT k.CLM_SK,
               w.SRC_SYS_CD_SK,
               w.CLM_ID,
               k.CRT_RUN_CYC_EXCTN_SK
        FROM {IDSOwner}.K_CLM_TEMP w
        JOIN {IDSOwner}.K_CLM k
          ON w.SRC_SYS_CD_SK = k.SRC_SYS_CD_SK
         AND w.CLM_ID       = k.CLM_ID
        UNION
        SELECT -1,
               w2.SRC_SYS_CD_SK,
               w2.CLM_ID,
               {CurrRunCycle}
        FROM {IDSOwner}.K_CLM_TEMP w2
        WHERE NOT EXISTS (
            SELECT 1
              FROM {IDSOwner}.K_CLM k2
             WHERE w2.SRC_SYS_CD_SK = k2.SRC_SYS_CD_SK
               AND w2.CLM_ID       = k2.CLM_ID
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
    # Step 3: Transformer logic (PrimaryKey)
    # --------------------------------------------------
    df_enriched = (
        df_W_Extract
        .withColumn(
            "svInstUpdt",
            F.when(F.col("CLM_SK") == F.lit(-1), F.lit("I")).otherwise(F.lit("U"))
        )
        .withColumn(
            "svSK",
            F.when(F.col("svInstUpdt") == F.lit("I"), F.lit(None)).otherwise(F.col("CLM_SK"))
        )
    )

    # surrogate-key generation
    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,'svSK',<schema>,<secret_name>)

    df_enriched = (
        df_enriched
        .withColumn(
            "svCrtRunCycExctnSk",
            F.when(F.col("svInstUpdt") == F.lit("I"), F.lit(CurrRunCycle)).otherwise(F.col("CRT_RUN_CYC_EXCTN_SK"))
        )
    )

    # updt link (all rows)
    df_updt = (
        df_enriched
        .withColumn("SRC_SYS_CD", F.lit(SrcSysCd))
        .withColumnRenamed("CLM_ID", "CLM_ID")
        .withColumn("CRT_RUN_CYC_EXCTN_SK", F.col("svCrtRunCycExctnSk"))
        .withColumn("CLM_SK", F.col("svSK"))
        .select("SRC_SYS_CD", "CLM_ID", "CRT_RUN_CYC_EXCTN_SK", "CLM_SK")
    )

    # NewKeys link (svInstUpdt = 'I')
    df_newKeys = (
        df_enriched
        .filter(F.col("svInstUpdt") == F.lit("I"))
        .withColumn("SRC_SYS_CD_SK", F.col("SRC_SYS_CD_SK"))
        .withColumnRenamed("CLM_ID", "CLM_ID")
        .withColumn("CRT_RUN_CYC_EXCTN_SK", F.col("svCrtRunCycExctnSk"))
        .withColumn("CLM_SK", F.col("svSK"))
        .select("SRC_SYS_CD_SK", "CLM_ID", "CRT_RUN_CYC_EXCTN_SK", "CLM_SK")
    )

    # Existing_Clms link (svInstUpdt = 'U')
    df_existing_clms = (
        df_enriched
        .filter(F.col("svInstUpdt") == F.lit("U"))
        .withColumn("SRC_SYS_CD_SK", F.col("SRC_SYS_CD_SK"))
        .withColumnRenamed("CLM_ID", "CLM_ID")
        .withColumn("CRT_RUN_CYC_EXCTN_SK", F.col("svCrtRunCycExctnSk"))
        .withColumn("CLM_SK", F.col("svSK"))
        .select("SRC_SYS_CD_SK", "CLM_ID", "CRT_RUN_CYC_EXCTN_SK", "CLM_SK")
    )

    # --------------------------------------------------
    # Step 4: Replace hashed-file hf_clm (scenario a) with dedup logic
    # --------------------------------------------------
    df_PK_Clms = dedup_sort(
        df_updt,
        ["SRC_SYS_CD", "CLM_ID"],
        [("CRT_RUN_CYC_EXCTN_SK", "D")]
    )

    # --------------------------------------------------
    # Step 5: Write sequential file K_CLM.dat
    # --------------------------------------------------
    file_path_k_clm = f"{adls_path}/load/K_CLM.dat"
    write_files(
        df_newKeys,
        file_path_k_clm,
        delimiter=",",
        mode="append",
        is_pqruet=False,
        header=False,
        quote="\"",
        nullValue=None
    )

    # --------------------------------------------------
    # Step 6: Write parquet for hf_fcts_rcrd_del (scenario c)
    # --------------------------------------------------
    file_path_hf_fcts_rcrd_del = f"{adls_path}/ClmLoadPK_Existing_Clms.parquet"
    write_files(
        df_existing_clms,
        file_path_hf_fcts_rcrd_del,
        delimiter=",",
        mode="overwrite",
        is_pqruet=True,
        header=True,
        quote="\"",
        nullValue=None
    )

    # --------------------------------------------------
    # Step 7: Return output stream 'PK_Clms'
    # --------------------------------------------------
    return df_PK_Clms