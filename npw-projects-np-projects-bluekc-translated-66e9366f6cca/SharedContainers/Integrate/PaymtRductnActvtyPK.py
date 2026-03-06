# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

"""
Shared Container: PaymtRductnActvtyPK
***************************************************************************************************************************************************************
COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 

CALLED BY: FctsClmPayRductnActvtyExtr

PROCESSING: Shared container used for Primary Keying of Claim payment reduction activity job

MODIFICATIONS:
Developer           Date            Change Description                                           Project/Altius #      Development Project     Code Reviewer        Date Reviewed  
------------------- ---------------- ------------------------------------------------------------------------------- ---------------------- ------------------- -----------------
Bhoomi Dasari       2008-07-17      Initial program                                              3567 Primary Key      devlIDS                Steph Goddard        07-28-2007
Manasa Andru        2020-10-10      Changed the Update action in the Seq File from 'Overwrite' to 'Append'            US - 294883            IntegrateDev1        10/10/2020
Reddy Sanam         2021-10-25      Column length-RCVD_CHK_NO changed from char(8) to char(10)                         US - 449722            IntegrateDev2

Annotations:
- IDS Primary Key Container for Claim Reduction Activity
- Used by FctsClmPayRductnActvtyExtr
- Hash file (hf_paymt_reductn_actvty_allcol) cleared in the calling program - FctsClmPayRductnActvtyExtr
- Assign primary surrogate key
- SQL joins temp table with key table to assign known keys
- Temp table is tuncated before load and runstat done after load
- Load IDS temp. table
- join primary key info with table info
- Primary key hash file only contains current run keys.  Clear by <Source><Subject>PKHashClearJC at beginning of job stream.
- update primary key table (K_PAYMT_RDUCTN_ACTVTY) with new keys created today
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when
from typing import Tuple

def run_PaymtRductnActvtyPK(
    df_AllColl: DataFrame,
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    """
    Executes the PaymtRductnActvtyPK shared container logic.

    Parameters
    ----------
    df_AllColl : DataFrame
        Input link `AllColl` from calling job.
    df_Transform : DataFrame
        Input link `Transform` from calling job.
    params : dict
        Runtime parameters.

    Returns
    -------
    DataFrame
        Output link `Key` back to calling job.
    """

    # --------------------------------------------------
    # Unpack runtime parameters
    # --------------------------------------------------
    CurrRunCycle          = params["CurrRunCycle"]
    CurrDate              = params["CurrDate"]
    SrcSysCdSk            = params["SrcSysCdSk"]
    SrcSysCd              = params["SrcSysCd"]
    RunID                 = params["RunID"]
    IDSOwner              = params["IDSOwner"]
    ids_secret_name       = params["ids_secret_name"]
    ids_jdbc_url          = params["ids_jdbc_url"]
    ids_jdbc_props        = params["ids_jdbc_props"]
    adls_path             = params["adls_path"]
    adls_path_raw         = params["adls_path_raw"]
    adls_path_publish     = params["adls_path_publish"]

    # --------------------------------------------------
    # Stage: hf_paymt_reductn_actvty_allcol  (scenario a)
    # Deduplicate incoming AllColl stream on primary key columns
    # --------------------------------------------------
    df_AllColOut = dedup_sort(
        df_AllColl,
        partition_cols=[
            "SRC_SYS_CD_SK",
            "PAYMT_RDUCTN_REF_ID",
            "PAYMT_RDUCTN_SUBTYP_CD",
            "PAYMT_RDUCTN_ACTVTY_SEQ_NO"
        ],
        sort_cols=[]
    )

    # --------------------------------------------------
    # Stage: K_PAYMT_RDUCTN_ACTVTY_TEMP  (write)
    # --------------------------------------------------
    execute_dml(
        f"TRUNCATE TABLE {IDSOwner}.K_PAYMT_RDUCTN_ACTVTY_TEMP",
        ids_jdbc_url,
        ids_jdbc_props
    )

    (
        df_Transform.write
        .format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("dbtable", f"{IDSOwner}.K_PAYMT_RDUCTN_ACTVTY_TEMP")
        .mode("append")
        .save()
    )

    execute_dml(
        f"CALL SYSPROC.ADMIN_CMD('runstats on table {IDSOwner}.K_PAYMT_RDUCTN_ACTVTY_TEMP on key columns with distribution on key columns and detailed indexes all allow write access')",
        ids_jdbc_url,
        ids_jdbc_props
    )

    # --------------------------------------------------
    # Stage: K_PAYMT_RDUCTN_ACTVTY_TEMP  (read - W_Extract)
    # --------------------------------------------------
    extract_query = f"""
        SELECT  k.PAYMT_RDUCTN_ACTVTY_SK,
                w.SRC_SYS_CD_SK,
                w.PAYMT_RDUCTN_REF_ID,
                w.PAYMT_RDUCTN_SUBTYP_CD,
                w.PAYMT_RDUCTN_ACTVTY_SEQ_NO,
                k.CRT_RUN_CYC_EXCTN_SK
        FROM {IDSOwner}.K_PAYMT_RDUCTN_ACTVTY_TEMP w,
             {IDSOwner}.K_PAYMT_RDUCTN_ACTVTY k
        WHERE w.SRC_SYS_CD_SK              = k.SRC_SYS_CD_SK
          AND w.PAYMT_RDUCTN_REF_ID        = k.PAYMT_RDUCTN_REF_ID
          AND w.PAYMT_RDUCTN_SUBTYP_CD     = k.PAYMT_RDUCTN_SUBTYP_CD
          AND w.PAYMT_RDUCTN_ACTVTY_SEQ_NO = k.PAYMT_RDUCTN_ACTVTY_SEQ_NO
        UNION
        SELECT -1,
               w2.SRC_SYS_CD_SK,
               w2.PAYMT_RDUCTN_REF_ID,
               w2.PAYMT_RDUCTN_SUBTYP_CD,
               w2.PAYMT_RDUCTN_ACTVTY_SEQ_NO,
               {CurrRunCycle}
        FROM {IDSOwner}.K_PAYMT_RDUCTN_ACTVTY_TEMP w2
        WHERE NOT EXISTS (
              SELECT 1
              FROM {IDSOwner}.K_PAYMT_RDUCTN_ACTVTY k2
              WHERE w2.SRC_SYS_CD_SK              = k2.SRC_SYS_CD_SK
                AND w2.PAYMT_RDUCTN_REF_ID        = k2.PAYMT_RDUCTN_REF_ID
                AND w2.PAYMT_RDUCTN_SUBTYP_CD     = k2.PAYMT_RDUCTN_SUBTYP_CD
                AND w2.PAYMT_RDUCTN_ACTVTY_SEQ_NO = k2.PAYMT_RDUCTN_ACTVTY_SEQ_NO
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
    # Stage: PrimaryKey  (Transformer)
    # --------------------------------------------------
    df_enriched = (
        df_W_Extract
        .withColumn("INSRT_UPDT_CD",
                    when(col("PAYMT_RDUCTN_ACTVTY_SK") == lit(-1), lit("I")).otherwise(lit("U")))
        .withColumn("SRC_SYS_CD", lit("FACETS"))
        .withColumn(
            "PAYMT_RDUCTN_ACTVTY_SK",
            when(col("PAYMT_RDUCTN_ACTVTY_SK") == lit(-1), lit(None)).otherwise(col("PAYMT_RDUCTN_ACTVTY_SK"))
        )
        .withColumn(
            "CRT_RUN_CYC_EXCTN_SK",
            when(col("INSRT_UPDT_CD") == lit("I"), lit(CurrRunCycle)).otherwise(col("CRT_RUN_CYC_EXCTN_SK"))
        )
    )

    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,'PAYMT_RDUCTN_ACTVTY_SK',<schema>,<secret_name>)

    # link: updt
    df_updt = df_enriched.select(
        lit("FACETS").alias("SRC_SYS_CD_SK"),
        "PAYMT_RDUCTN_REF_ID",
        "PAYMT_RDUCTN_SUBTYP_CD",
        "PAYMT_RDUCTN_ACTVTY_SEQ_NO",
        "CRT_RUN_CYC_EXCTN_SK",
        "PAYMT_RDUCTN_ACTVTY_SK"
    )

    # link: NewKeys (constraint INSRT_UPDT_CD == 'I')
    df_NewKeys = (
        df_enriched
        .filter(col("INSRT_UPDT_CD") == lit("I"))
        .select(
            "SRC_SYS_CD_SK",
            "PAYMT_RDUCTN_REF_ID",
            "PAYMT_RDUCTN_SUBTYP_CD",
            "PAYMT_RDUCTN_ACTVTY_SEQ_NO",
            "CRT_RUN_CYC_EXCTN_SK",
            "PAYMT_RDUCTN_ACTVTY_SK"
        )
    )

    # link: Keys
    df_Keys = df_enriched.select(
        "PAYMT_RDUCTN_ACTVTY_SK",
        "SRC_SYS_CD_SK",
        "SRC_SYS_CD",
        "PAYMT_RDUCTN_REF_ID",
        "PAYMT_RDUCTN_SUBTYP_CD",
        "PAYMT_RDUCTN_ACTVTY_SEQ_NO",
        "INSRT_UPDT_CD",
        "CRT_RUN_CYC_EXCTN_SK"
    )

    # --------------------------------------------------
    # Stage: hf_paymt_reductn_actvty (parquet write)
    # --------------------------------------------------
    write_files(
        df_updt,
        f"{adls_path}/PaymtRductnActvtyPK_updt.parquet",
        delimiter=",",
        mode="overwrite",
        is_pqruet=True,
        header=True,
        quote="\"",
        nullValue=None
    )

    # --------------------------------------------------
    # Stage: K_PAYMT_RDUCTN_ACTVTY (sequential file append)
    # --------------------------------------------------
    write_files(
        df_NewKeys,
        f"{adls_path}/load/K_PAYMT_RDUCTN_ACTVTY.dat",
        delimiter=",",
        mode="append",
        is_pqruet=False,
        header=False,
        quote="\"",
        nullValue=None
    )

    # --------------------------------------------------
    # Stage: Merge  (Transformer)
    # --------------------------------------------------
    join_expr = (
        (col("all.SRC_SYS_CD_SK")              == col("k.SRC_SYS_CD_SK")) &
        (col("all.PAYMT_RDUCTN_REF_ID")        == col("k.PAYMT_RDUCTN_REF_ID")) &
        (col("all.PAYMT_RDUCTN_SUBTYP_CD")     == col("k.PAYMT_RDUCTN_SUBTYP_CD")) &
        (col("all.PAYMT_RDUCTN_ACTVTY_SEQ_NO") == col("k.PAYMT_RDUCTN_ACTVTY_SEQ_NO"))
    )

    df_Key = (
        df_AllColOut.alias("all")
        .join(df_Keys.alias("k"), join_expr, "left")
        .select(
            col("all.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
            col("k.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
            col("all.DISCARD_IN").alias("DISCARD_IN"),
            col("all.PASS_THRU_IN").alias("PASS_THRU_IN"),
            col("all.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
            col("all.ERR_CT").alias("ERR_CT"),
            col("all.RECYCLE_CT").alias("RECYCLE_CT"),
            col("k.SRC_SYS_CD").alias("SRC_SYS_CD"),
            col("all.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
            col("k.PAYMT_RDUCTN_ACTVTY_SK").alias("PAYMT_RDUCTN_ACTVTY_SK"),
            col("all.PAYMT_RDUCTN_REF_ID").alias("PAYMT_RDUCTN_REF_ID"),
            col("all.PAYMT_RDUCTN_SUBTYP_CD").alias("PAYMT_RDUCTN_SUBTYP_CD"),
            col("all.PAYMT_RDUCTN_ACTVTY_SEQ_NO").alias("PAYMT_RDUCTN_ACTVTY_SEQ_NO"),
            col("k.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
            lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            col("all.PAYMT_RDUCTN_ID").alias("PAYMT_RDUCTN_ID"),
            col("all.USER_ID").alias("USER_ID"),
            col("all.PAYMT_RDUCTN_ACT_EVT_TYP_CD").alias("PAYMT_RDUCTN_ACT_EVT_TYP_CD"),
            col("all.PAYMT_RDUCTN_ACTVTY_RSN_CD").alias("PAYMT_RDUCTN_ACTVTY_RSN_CD"),
            col("all.ACCTG_PERD_END_DT").alias("ACCTG_PERD_END_DT"),
            col("all.CRT_DT").alias("CRT_DT"),
            col("all.RFND_RCVD_DT").alias("RFND_RCVD_DT"),
            col("all.PAYMT_RDUCTN_ACTVTY_AMT").alias("PAYMT_RDUCTN_ACTVTY_AMT"),
            col("all.PCA_RCVR_AMT").alias("PCA_RCVR_AMT"),
            col("all.PAYMT_REF_ID").alias("PAYMT_REF_ID"),
            col("all.RCVD_CHK_NO").alias("RCVD_CHK_NO")
        )
    )

    # --------------------------------------------------
    # Return container output
    # --------------------------------------------------
    return df_Key