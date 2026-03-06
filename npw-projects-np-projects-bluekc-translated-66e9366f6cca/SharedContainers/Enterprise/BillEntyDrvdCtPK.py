# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

"""
Shared-Container  : BillEntyDrvdCtPK
Description       : Copyright 2010 Blue Cross and Blue Shield of Kansas City

Called by:
    EdwBillEntyDrvdCtExtr
    
Processing:
    Assign new or lookup existing primary key

Control Job Rerun Information:
    Previous Run Successful : Restart, no other steps necessary
    Previous Run Aborted    : Restart, no other steps necessary

Modifications:
Developer        Date         Altiris #   Change Description                              Reviewer        Reviewed
---------------  -----------  ----------  ----------------------------------------------  --------------  ----------
Hugh Sisson      2010-09-23   3346        Original program                                Steph Goddard   09/28/2010

Annotations (DataStage):
    • Hashed file (hf_bill_enty_drvd_ct_f_allcol) cleared by calling job
    • Join primary key info with table info
    • Primary key hashed file contains only keys from rows in current run.  Hashed file is cleared before writing
    • Update primary key table (K_BILL_ENTY_DRVD_CT_F) with new primary key values created during current run
    • SQL joins temp table with key table to assign known keys
    • Load IDS temp table
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType


def run_BillEntyDrvdCtPK(
    df_AllCol: DataFrame,
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    """
    Executes the logic of the DataStage shared container `BillEntyDrvdCtPK`.

    Parameters
    ----------
    df_AllCol : DataFrame
        Container input link `AllCol`
    df_Transform : DataFrame
        Container input link `Transform`
    params : dict
        Runtime parameters and pre-resolved secrets / JDBC configs

    Returns
    -------
    DataFrame
        Container output link `Load`
    """

    # --------------------------------------------------
    # Un-pack runtime parameters (each exactly once)
    # --------------------------------------------------
    CurrentDate        = params["CurrentDate"]
    CurrRunCycle       = params["CurrRunCycle"]

    EDWOwner           = params["EDWOwner"]
    edw_secret_name    = params["edw_secret_name"]
    edw_jdbc_url       = params["edw_jdbc_url"]
    edw_jdbc_props     = params["edw_jdbc_props"]

    adls_path          = params["adls_path"]
    adls_path_raw      = params["adls_path_raw"]
    adls_path_publish  = params["adls_path_publish"]
    # --------------------------------------------------
    # Stage : hf_bill_enty_drvd_ct_f_allcol  (scenario a)
    #         Deduplicate by primary-key columns
    # --------------------------------------------------
    df_AllCol_dedup = df_AllCol.dropDuplicates(
        ["BILL_ENTY_UNIQ_KEY", "ACTVTY_YR_MO_SK", "SRC_SYS_CD"]
    )

    # --------------------------------------------------
    # Stage : K_BILL_ENTY_DRVD_CT_F_TEMP  (logical refactor)
    # --------------------------------------------------
    extract_query = f"""
        SELECT BILL_ENTY_DRVD_CT_SK,
               BILL_ENTY_UNIQ_KEY,
               ACTVTY_YR_MO_SK,
               SRC_SYS_CD,
               CRT_RUN_CYC_EXCTN_DT_SK
        FROM {EDWOwner}.K_BILL_ENTY_DRVD_CT_F
    """

    df_key_table = (
        spark.read.format("jdbc")
            .option("url", edw_jdbc_url)
            .options(**edw_jdbc_props)
            .option("query", extract_query)
            .load()
    )

    join_cond = [
        df_Transform.BILL_ENTY_UNIQ_KEY == df_key_table.BILL_ENTY_UNIQ_KEY,
        df_Transform.ACTVTY_YR_MO_SK   == df_key_table.ACTVTY_YR_MO_SK,
        df_Transform.SRC_SYS_CD        == df_key_table.SRC_SYS_CD
    ]

    df_W_Extract = (
        df_Transform.alias("w")
        .join(df_key_table.alias("k"), join_cond, "left")
        .select(
            F.coalesce("k.BILL_ENTY_DRVD_CT_SK", F.lit(-1)).alias("BILL_ENTY_DRVD_CT_SK"),
            F.col("w.BILL_ENTY_UNIQ_KEY"),
            F.col("w.ACTVTY_YR_MO_SK"),
            F.col("w.SRC_SYS_CD"),
            F.coalesce("k.CRT_RUN_CYC_EXCTN_DT_SK", F.lit(CurrentDate)).alias("CRT_RUN_CYC_EXCTN_DT_SK")
        )
    )

    # --------------------------------------------------
    # Stage : PrimaryKey (Transformer)
    # --------------------------------------------------
    df_enriched = (
        df_W_Extract
        .withColumn(
            "svInsrtUpdt",
            F.when(F.col("BILL_ENTY_DRVD_CT_SK") == -1, F.lit("I")).otherwise(F.lit("U"))
        )
        .withColumn(
            "svSK",
            F.when(F.col("svInsrtUpdt") == "I", F.lit(None).cast(IntegerType()))
             .otherwise(F.col("BILL_ENTY_DRVD_CT_SK"))
        )
    )

    # Populate surrogate keys
    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"svSK",<schema>,<secret_name>)

    df_enriched = df_enriched.withColumn(
        "svCrtRunCycExctnDtSk",
        F.when(F.col("svInsrtUpdt") == "I", F.lit(CurrentDate))
         .otherwise(F.col("CRT_RUN_CYC_EXCTN_DT_SK"))
    )

    # Output link : Updt  (to hf_bill_enty_drvd_ct_f)
    df_Updt = df_enriched.select(
        "BILL_ENTY_UNIQ_KEY",
        "ACTVTY_YR_MO_SK",
        "SRC_SYS_CD",
        F.col("svCrtRunCycExctnDtSk").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.col("svSK").alias("BILL_ENTY_DRVD_CT_SK")
    )

    updt_path = f"{adls_path}/BillEntyDrvdCtPK_Updt.parquet"
    write_files(
        df_Updt,
        updt_path,
        delimiter=",",
        mode="overwrite",
        is_pqruet=True,
        header=True,
        quote='"',
        nullValue=None
    )

    # Output link : NewKeys  (to sequential file)
    df_NewKeys = (
        df_enriched
        .filter(F.col("svInsrtUpdt") == "I")
        .select(
            "BILL_ENTY_UNIQ_KEY",
            "ACTVTY_YR_MO_SK",
            "SRC_SYS_CD",
            F.col("svCrtRunCycExctnDtSk").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
            F.col("svSK").alias("BILL_ENTY_DRVD_CT_SK")
        )
    )

    newkeys_path = f"{adls_path}/load/K_BILL_ENTY_DRVD_CT_F.dat"
    write_files(
        df_NewKeys,
        newkeys_path,
        delimiter=",",
        mode="overwrite",
        is_pqruet=False,
        header=False,
        quote='"',
        nullValue=None
    )

    # Output link : Keys  (to Merge)
    df_Keys = df_enriched.select(
        F.col("svSK").alias("BILL_ENTY_DRVD_CT_SK"),
        "BILL_ENTY_UNIQ_KEY",
        "ACTVTY_YR_MO_SK",
        "SRC_SYS_CD",
        F.col("svInsrtUpdt").alias("INSRT_UPDT_CD"),
        F.col("svCrtRunCycExctnDtSk").alias("CRT_RUN_CYC_EXCTN_DT_SK")
    )

    # --------------------------------------------------
    # Stage : Merge
    # --------------------------------------------------
    merge_cond = [
        df_Keys.BILL_ENTY_UNIQ_KEY == df_AllCol_dedup.BILL_ENTY_UNIQ_KEY,
        df_Keys.ACTVTY_YR_MO_SK   == df_AllCol_dedup.ACTVTY_YR_MO_SK,
        df_Keys.SRC_SYS_CD        == df_AllCol_dedup.SRC_SYS_CD
    ]

    df_Load = (
        df_Keys.alias("k")
        .join(df_AllCol_dedup.alias("a"), merge_cond, "left")
        .select(
            F.col("k.BILL_ENTY_DRVD_CT_SK"),
            F.col("k.BILL_ENTY_UNIQ_KEY"),
            F.col("k.ACTVTY_YR_MO_SK"),
            F.col("k.SRC_SYS_CD"),
            F.col("k.CRT_RUN_CYC_EXCTN_DT_SK"),
            F.lit(CurrentDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
            F.when(F.col("a.BILL_ENTY_SK").isNull(), F.lit(0)).otherwise(F.col("a.BILL_ENTY_SK")).alias("BILL_ENTY_SK"),
            F.when(F.col("a.GRP_SK").isNull(), F.lit(0)).otherwise(F.col("a.GRP_SK")).alias("GRP_SK"),
            F.when(F.col("a.SUBGRP_SK").isNull(), F.lit(0)).otherwise(F.col("a.SUBGRP_SK")).alias("SUBGRP_SK"),
            F.when(F.col("a.SUB_SK").isNull(), F.lit(0)).otherwise(F.col("a.SUB_SK")).alias("SUB_SK"),
            F.when(F.col("a.DRVD_CNTR_PRSN_CT").isNull(), F.lit(0)).otherwise(F.col("a.DRVD_CNTR_PRSN_CT")).alias("DRVD_CNTR_PRSN_CT"),
            F.when(F.col("a.DRVD_MBR_PRSN_CT").isNull(), F.lit(0)).otherwise(F.col("a.DRVD_MBR_PRSN_CT")).alias("DRVD_MBR_PRSN_CT"),
            F.when(F.col("a.GRP_ID").isNull(), F.lit("UNK")).otherwise(F.col("a.GRP_ID")).alias("GRP_ID"),
            F.when(F.col("a.SUBGRP_ID").isNull(), F.lit("UNK")).otherwise(F.col("a.SUBGRP_ID")).alias("SUBGRP_ID"),
            F.when(F.col("a.SUB_ID").isNull(), F.lit("UNK")).otherwise(F.col("a.SUB_ID")).alias("SUB_ID"),
            F.lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
            F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
        )
    )

    # --------------------------------------------------
    # Return container output
    # --------------------------------------------------
    return df_Load