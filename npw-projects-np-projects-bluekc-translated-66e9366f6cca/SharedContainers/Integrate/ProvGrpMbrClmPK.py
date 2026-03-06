# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

"""
COPYRIGHT 2011 BLUE CROSS/BLUE SHIELD OF KANSAS CITY

CALLED BY:    IdsPcmhProvGrpMbrClmExtr

PROCESSING:  Pulls data from the IDS updating associated K table and hash file.

MODIFICATIONS:
Developer                          Date                    Project/Altiris #                     Change Description                                                       Development Project        Code Reviewer               Date Reviewed
----------------------------------      -------------------      -----------------------------------           ---------------------------------------------------------                                 ----------------------------------       ---------------------------------        -------------------------

Ralph Tucker                  2011-05-12        4663 - PCMH                        Original Programming.                                                        IntegrateCurDevl

--------------------------------------------------------------------------------
IDS Primary Key Container for
PCMH_PROV_GRP_MBR_CLM

Used by

IdsPcmhProvGrpMbrClmExtr

SQL joins temp table with key table to assign known keys
Temp table is tuncated before load and runstat done after load
Load IDS temp. table
join primary key info with table info
Hashfile cleared everytime the job runs
update primary key table (PCMH_PROV_GRP_MBR_CLM) with new keys created today
primary key hash file only contains current run keys
Assign primary surrogate key
"""


from typing import Tuple, Union


def run_ProvGrpMbrClmPK(
    df_AllColl: DataFrame,
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    # ---------------------------------------------------------------------
    # unpack parameters (each exactly once)
    CurrRunCycle         = params["CurrRunCycle"]
    SrcSysCd             = params["SrcSysCd"]
    IDSOwner             = params["IDSOwner"]
    ids_secret_name      = params["ids_secret_name"]
    ids_jdbc_url         = params["ids_jdbc_url"]
    ids_jdbc_props       = params["ids_jdbc_props"]
    adls_path            = params["adls_path"]
    adls_path_raw        = params["adls_path_raw"]
    adls_path_publish    = params["adls_path_publish"]
    # ---------------------------------------------------------------------
    # 1. deduplicate AllColl (scenario a – intermediate hashed file)
    df_allcol_dedup = df_AllColl.dropDuplicates(
        ["SRC_SYS_CD_SK", "PROV_GRP_PROV_ID", "MBR_UNIQ_KEY"]
    )
    # ---------------------------------------------------------------------
    # 2. write rows from Transform link to IDS temp table
    execute_dml(
        f"CALL {IDSOwner}.BCSP_TRUNCATE('{IDSOwner}', 'K_PCMH_PROV_GRP_MBR_CLM_TEMP')",
        ids_jdbc_url,
        ids_jdbc_props
    )

    (
        df_Transform
        .select("SRC_SYS_CD_SK", "PROV_GRP_PROV_ID", "MBR_UNIQ_KEY")
        .write
        .format("jdbc")
        .option("url", ids_jdbc_url)
        .option("dbtable", f"{IDSOwner}.K_PCMH_PROV_GRP_MBR_CLM_TEMP")
        .options(**ids_jdbc_props)
        .mode("append")
        .save()
    )

    execute_dml(
        "CALL SYSPROC.ADMIN_CMD('runstats on table "
        f"{IDSOwner}.K_MED_MGT_NOTE_TX_TEMP on key columns with distribution on "
        "key columns and detailed indexes all allow write access')",
        ids_jdbc_url,
        ids_jdbc_props
    )
    # ---------------------------------------------------------------------
    # 3. extract W_Extract
    extract_query = f"""
    SELECT k.PCMH_PROV_GRP_MBR_CLM_SK,
           w.PROV_GRP_PROV_ID,
           w.MBR_UNIQ_KEY,
           w.SRC_SYS_CD_SK,
           k.CRT_RUN_CYC_EXCTN_SK
    FROM {IDSOwner}.K_PCMH_PROV_GRP_MBR_CLM_TEMP w
    JOIN {IDSOwner}.K_PCMH_PROV_GRP_MBR_CLM k
      ON w.SRC_SYS_CD_SK    = k.SRC_SYS_CD_SK
     AND w.PROV_GRP_PROV_ID = k.PROV_GRP_PROV_ID
     AND w.MBR_UNIQ_KEY     = k.MBR_UNIQ_KEY

    UNION

    SELECT -1,
           w2.PROV_GRP_PROV_ID,
           w2.MBR_UNIQ_KEY,
           w2.SRC_SYS_CD_SK,
           {CurrRunCycle}
    FROM {IDSOwner}.K_PCMH_PROV_GRP_MBR_CLM_TEMP w2
    WHERE NOT EXISTS (
          SELECT 1
          FROM {IDSOwner}.K_PCMH_PROV_GRP_MBR_CLM k2
          WHERE w2.SRC_SYS_CD_SK    = k2.SRC_SYS_CD_SK
            AND w2.PROV_GRP_PROV_ID = k2.PROV_GRP_PROV_ID
            AND w2.MBR_UNIQ_KEY     = k2.MBR_UNIQ_KEY
    )
    """
    df_W_Extract = (
        spark.read.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("query", extract_query)
        .load()
    )
    # ---------------------------------------------------------------------
    # 4. transformer PrimaryKey
    df_enriched = (
        df_W_Extract
        .withColumn(
            "svInstUpdt",
            F.when(F.col("PCMH_PROV_GRP_MBR_CLM_SK") == -1, F.lit("I")).otherwise(F.lit("U"))
        )
        .withColumn("svSrcSysCd", F.lit(SrcSysCd))
    )

    df_enriched = SurrogateKeyGen(
        df_enriched,
        <DB sequence name>,
        "PCMH_PROV_GRP_MBR_CLM_SK",
        <schema>,
        <secret_name>
    )

    df_enriched = (
        df_enriched
        .withColumn("svSK", F.col("PCMH_PROV_GRP_MBR_CLM_SK"))
        .withColumn(
            "svCrtRunCycExctnSk",
            F.when(F.col("svInstUpdt") == "I", F.lit(CurrRunCycle)).otherwise(F.col("CRT_RUN_CYC_EXCTN_SK"))
        )
    )
    # ---------------------------------------------------------------------
    # 4a. updt link
    df_updt = (
        df_enriched.select(
            F.col("svSrcSysCd").alias("SRC_SYS_CD"),
            "PROV_GRP_PROV_ID",
            "MBR_UNIQ_KEY",
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.col("svSK").alias("PCMH_PROV_GRP_MBR_CLM_SK")
        )
    )
    # write parquet (scenario c)
    write_files(
        df_updt,
        f"{adls_path}/ProvGrpMbrClmPK_updt.parquet",
        delimiter=",",
        mode="overwrite",
        is_pqruet=True,
        header=True,
        quote="\"",
        nullValue=None
    )
    # ---------------------------------------------------------------------
    # 4b. NewKeys link
    df_NewKeys = (
        df_enriched
        .filter(F.col("svInstUpdt") == "I")
        .select(
            "PROV_GRP_PROV_ID",
            "MBR_UNIQ_KEY",
            "SRC_SYS_CD_SK",
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.col("svSK").alias("PCMH_PROV_GRP_MBR_CLM_SK")
        )
    )
    write_files(
        df_NewKeys,
        f"{adls_path}/load/K_PCMH_PROV_GRP_MBR_CLM.dat",
        delimiter=",",
        mode="overwrite",
        is_pqruet=False,
        header=False,
        quote="\"",
        nullValue=None
    )
    # ---------------------------------------------------------------------
    # 4c. Keys link
    df_keys = (
        df_enriched.select(
            F.col("svSK").alias("PCMH_PROV_GRP_MBR_CLM_SK"),
            "SRC_SYS_CD_SK",
            F.col("svSrcSysCd").alias("SRC_SYS_CD"),
            "PROV_GRP_PROV_ID",
            "MBR_UNIQ_KEY",
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK")
        )
    )
    # ---------------------------------------------------------------------
    # 5. Merge transformer
    df_Key = (
        df_allcol_dedup.alias("all")
        .join(
            df_keys.alias("keys"),
            [
                F.col("all.SRC_SYS_CD_SK") == F.col("keys.SRC_SYS_CD_SK"),
                F.col("all.PROV_GRP_PROV_ID") == F.col("keys.PROV_GRP_PROV_ID"),
                F.col("all.MBR_UNIQ_KEY") == F.col("keys.MBR_UNIQ_KEY")
            ],
            "left"
        )
        .select(
            "all.JOB_EXCTN_RCRD_ERR_SK",
            "all.INSRT_UPDT_CD",
            "all.DISCARD_IN",
            "all.PASS_THRU_IN",
            "all.FIRST_RECYC_DT",
            "all.ERR_CT",
            "all.RECYCLE_CT",
            "keys.SRC_SYS_CD",
            "all.PRI_KEY_STRING",
            "keys.PCMH_PROV_GRP_MBR_CLM_SK",
            "keys.PROV_GRP_PROV_ID",
            "keys.MBR_UNIQ_KEY",
            "keys.CRT_RUN_CYC_EXCTN_SK",
            F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            "all.CLM_SK",
            "all.MBR_SK",
            "all.SVC_STRT_DT_SK",
            "all.CLM_ID"
        )
    )
    # ---------------------------------------------------------------------
    return df_Key