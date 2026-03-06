# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

"""
Job Name      : VbbPlnCmpntDpndcPK
Job Type      : Server Job
Category      : DS_Integrate
Folder Path   : Shared Containers/PrimaryKey

DESCRIPTION:
COPYRIGHT 2013 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 

Shared container used for Primary Keying of VBB Reward

MODIFICATIONS:
Developer                   Date        Change Description                     Project/Altius #           Development Project      Code Reviewer     Date Reviewed
-----------------------     ----------  ------------------------------------   ------------------------   -----------------------   ---------------   -------------
Karthik Chintalapani        2013-04-29  Initial Programming                    4963 VBB Phase III         IntegrateNewDevl         Bhoomi Dasari     2013-05-16
"""

# Annotation
# join primary key info with table info
# update primary key table (K_VBB_PLN) with new keys created today
# Assign primary surrogate key
# primary key hash file NOT cleared before routine. Used in Fkey routine GetFkeyVbbPln
# Temp table is tuncated before load and runstat done after load
# SQL joins temp table with key table to assign known keys
# Load IDS temp. table
# Hash file (hf_vbb_pln_cmpnt_dpndc allcol) cleared in the calling program - IdsVbbRwrdExtr
# Hashfile cleared in calling program
# This container is used in:
# IdsVbbRwrdExtr
# These programs need to be re-compiled when logic changes

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T


def run_VbbPlnCmpntDpndcPK(
    df_AllCol: DataFrame,
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    # --------------------------------------------------
    # Unpack parameters
    CurrRunCycle            = params["CurrRunCycle"]
    SrcSysCd                = params["SrcSysCd"]
    IDSOwner                = params["IDSOwner"]
    ids_secret_name         = params["ids_secret_name"]
    ids_jdbc_url            = params["ids_jdbc_url"]
    ids_jdbc_props          = params["ids_jdbc_props"]
    adls_path               = params["adls_path"]
    adls_path_raw           = params["adls_path_raw"]
    adls_path_publish       = params["adls_path_publish"]
    # --------------------------------------------------
    # Step 1: dedup AllCol (replacement for intermediate hashed file)
    df_allcol_dedup = dedup_sort(
        df_AllCol,
        ["VBB_PLN_UNIQ_KEY", "VBB_CMPNT_UNIQ_KEY", "RQRD_VBB_CMPNT_UNIQ_KEY", "SRC_SYS_CD_SK"],
        []
    )
    # --------------------------------------------------
    # Step 2: create, load, and analyze temporary table K_VBB_PLN_CMPNT_DPNDC_TEMP
    drop_sql = f"DROP TABLE IF EXISTS {IDSOwner}.K_VBB_PLN_CMPNT_DPNDC_TEMP"
    create_sql = f"""
    CREATE TABLE {IDSOwner}.K_VBB_PLN_CMPNT_DPNDC_TEMP (
        VBB_PLN_UNIQ_KEY INTEGER NOT NULL,
        VBB_CMPNT_UNIQ_KEY INTEGER NOT NULL,
        RQRD_VBB_CMPNT_UNIQ_KEY INTEGER NOT NULL,
        SRC_SYS_CD_SK INTEGER NOT NULL,
        PRIMARY KEY (VBB_PLN_UNIQ_KEY, VBB_CMPNT_UNIQ_KEY, RQRD_VBB_CMPNT_UNIQ_KEY, SRC_SYS_CD_SK)
    )
    """
    runstats_sql = (
        f"CALL SYSPROC.ADMIN_CMD("
        f"'runstats on table {IDSOwner}.K_VBB_PLN_CMPNT_DPNDC_TEMP "
        f"on key columns with distribution on key columns and detailed indexes all allow write access')"
    )
    execute_dml(drop_sql, ids_jdbc_url, ids_jdbc_props)
    execute_dml(create_sql, ids_jdbc_url, ids_jdbc_props)
    (
        df_Transform.write.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("dbtable", f"{IDSOwner}.K_VBB_PLN_CMPNT_DPNDC_TEMP")
        .mode("append")
        .save()
    )
    execute_dml(runstats_sql, ids_jdbc_url, ids_jdbc_props)
    # --------------------------------------------------
    # Step 3: extract W_Extract dataset
    extract_query = f"""
    SELECT k.VBB_PLN_CMPNT_DPNDC_SK,
           w.VBB_PLN_UNIQ_KEY,
           w.VBB_CMPNT_UNIQ_KEY,
           w.RQRD_VBB_CMPNT_UNIQ_KEY,
           w.SRC_SYS_CD_SK,
           k.CRT_RUN_CYC_EXCTN_SK
    FROM {IDSOwner}.K_VBB_PLN_CMPNT_DPNDC_TEMP w,
         {IDSOwner}.K_VBB_PLN_CMPNT_DPNDC k
    WHERE w.SRC_SYS_CD_SK             = k.SRC_SYS_CD_SK
      AND w.VBB_PLN_UNIQ_KEY          = k.VBB_PLN_UNIQ_KEY
      AND w.VBB_CMPNT_UNIQ_KEY        = k.VBB_CMPNT_UNIQ_KEY
      AND w.RQRD_VBB_CMPNT_UNIQ_KEY   = k.RQRD_VBB_CMPNT_UNIQ_KEY
    UNION
    SELECT -1,
           w2.VBB_PLN_UNIQ_KEY,
           w2.VBB_CMPNT_UNIQ_KEY,
           w2.RQRD_VBB_CMPNT_UNIQ_KEY,
           w2.SRC_SYS_CD_SK,
           {CurrRunCycle}
    FROM {IDSOwner}.K_VBB_PLN_CMPNT_DPNDC_TEMP w2
    WHERE NOT EXISTS (
          SELECT k2.VBB_PLN_CMPNT_DPNDC_SK
          FROM {IDSOwner}.K_VBB_PLN_CMPNT_DPNDC k2
          WHERE w2.SRC_SYS_CD_SK           = k2.SRC_SYS_CD_SK
            AND w2.VBB_PLN_UNIQ_KEY        = k2.VBB_PLN_UNIQ_KEY
            AND w2.VBB_CMPNT_UNIQ_KEY      = k2.VBB_CMPNT_UNIQ_KEY
            AND w2.RQRD_VBB_CMPNT_UNIQ_KEY = k2.RQRD_VBB_CMPNT_UNIQ_KEY
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
    # Step 4: primary key transformer logic
    df_enriched = (
        df_W_Extract
        .withColumn(
            "INSRT_UPDT_CD",
            F.when(F.col("VBB_PLN_CMPNT_DPNDC_SK") == F.lit(-1), F.lit("I")).otherwise(F.lit("U"))
        )
        .withColumn("SRC_SYS_CD", F.lit(SrcSysCd))
        .withColumn(
            "VBB_PLN_CMPNT_DPNDC_SK",
            F.when(F.col("VBB_PLN_CMPNT_DPNDC_SK") == F.lit(-1), F.lit(None).cast(T.IntegerType()))
             .otherwise(F.col("VBB_PLN_CMPNT_DPNDC_SK"))
        )
        .withColumn(
            "CRT_RUN_CYC_EXCTN_SK",
            F.when(F.col("VBB_PLN_CMPNT_DPNDC_SK").isNull(), F.lit(CurrRunCycle))
             .otherwise(F.col("CRT_RUN_CYC_EXCTN_SK"))
        )
    )
    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,'VBB_PLN_CMPNT_DPNDC_SK',<schema>,<secret_name>)
    # --------------------------------------------------
    # Step 5: new keys sequential file
    df_newkeys = (
        df_enriched
        .filter(F.col("INSRT_UPDT_CD") == F.lit("I"))
        .select(
            "VBB_PLN_UNIQ_KEY",
            "VBB_CMPNT_UNIQ_KEY",
            "RQRD_VBB_CMPNT_UNIQ_KEY",
            "SRC_SYS_CD_SK",
            "CRT_RUN_CYC_EXCTN_SK",
            "VBB_PLN_CMPNT_DPNDC_SK"
        )
    )
    newkeys_path = f"{adls_path}/load/K_VBB_PLN_CMPNT_DPNDC.dat"
    write_files(
        df_newkeys,
        newkeys_path,
        delimiter=",",
        mode="overwrite",
        is_pqruet=False,
        header=False,
        quote='"',
        nullValue=None
    )
    # --------------------------------------------------
    # Step 6: updt parquet file (hashed file replacement)
    df_updt = df_enriched.select(
        "VBB_PLN_UNIQ_KEY",
        "VBB_CMPNT_UNIQ_KEY",
        "RQRD_VBB_CMPNT_UNIQ_KEY",
        "SRC_SYS_CD",
        "CRT_RUN_CYC_EXCTN_SK",
        "VBB_PLN_CMPNT_DPNDC_SK"
    )
    updt_path = f"{adls_path}/VbbPlnCmpntDpndcPK_updt.parquet"
    write_files(
        df_updt,
        updt_path,
        delimiter=",",
        mode="overwrite",
        is_pqruet=True,
        header=True,
        quote='"',
        nullValue=None
    )
    # --------------------------------------------------
    # Step 7: merge logic to create final Key output
    join_cols = [
        "VBB_PLN_UNIQ_KEY",
        "VBB_CMPNT_UNIQ_KEY",
        "RQRD_VBB_CMPNT_UNIQ_KEY",
        "SRC_SYS_CD_SK"
    ]
    df_merge = (
        df_allcol_dedup.alias("all")
        .join(df_enriched.alias("k"), join_cols, "left")
    )
    df_Key = (
        df_merge.select(
            F.col("all.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
            F.col("k.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
            F.col("all.DISCARD_IN").alias("DISCARD_IN"),
            F.col("all.PASS_THRU_IN").alias("PASS_THRU_IN"),
            F.col("all.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
            F.col("all.ERR_CT").alias("ERR_CT"),
            F.col("all.RECYCLE_CT").alias("RECYCLE_CT"),
            F.col("k.SRC_SYS_CD").alias("SRC_SYS_CD"),
            F.col("all.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
            F.col("k.VBB_PLN_CMPNT_DPNDC_SK").alias("VBB_PLN_CMPNT_DPNDC_SK"),
            F.col("all.VBB_PLN_UNIQ_KEY").alias("VBB_PLN_UNIQ_KEY"),
            F.col("all.VBB_CMPNT_UNIQ_KEY").alias("VBB_CMPNT_UNIQ_KEY"),
            F.col("all.RQRD_VBB_CMPNT_UNIQ_KEY").alias("RQRD_VBB_CMPNT_UNIQ_KEY"),
            F.col("all.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
            F.col("k.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            F.col("all.INPR_ID_DEP").alias("INPR_ID_DEP"),
            F.col("all.INPR_ID").alias("INPR_ID"),
            F.col("all.HIPL_ID").alias("HIPL_ID"),
            F.col("all.MUTLLY_XCLSVE_DPNDC_IN").alias("MUTLLY_XCLSVE_DPNDC_IN"),
            F.col("all.SRC_SYS_CRT_DTM").alias("SRC_SYS_CRT_DTM"),
            F.col("all.SRC_SYS_UPDT_DTM").alias("SRC_SYS_UPDT_DTM"),
            F.col("all.WAIT_PERD_DAYS_NO").alias("WAIT_PERD_DAYS_NO")
        )
    )
    # --------------------------------------------------
    return df_Key