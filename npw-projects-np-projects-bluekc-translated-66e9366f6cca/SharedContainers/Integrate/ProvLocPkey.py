# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------
"""
Job Name      : ProvLocPkey
Job Type      : Server Job
Job Category  : DS_Integrate
Folder Path   : Shared Containers

Description:
* VC LOGS *
^1_1 04/01/09 11:15:41 Batch  15067_40549 INIT bckcetl ids20 dsadm dsadm
^1_2 09/29/08 15:06:40 Batch  14883_54404 PROMOTE bckcetl ids20 dsadm bls for sa
^1_2 09/29/08 14:52:18 Batch  14883_53540 INIT bckcett testIDS dsadm bls for sa
^1_1 09/18/08 13:36:30 Batch  14872_48995 PROMOTE bckcett testIDS u03651 steph for Sharon
^1_1 09/18/08 13:26:42 Batch  14872_48429 INIT bckcett devlIDSnew u03651 steffy
^1_1 01/28/08 07:49:06 Batch  14638_28152 INIT bckcetl ids20 dsadm dsadm
^1_1 01/10/08 08:38:00 Batch  14620_31084 INIT bckcetl ids20 dsadm dsadm
^1_2 12/26/07 13:00:58 Batch  14605_46863 INIT bckcetl ids20 dsadm dsadm
^1_1 12/26/07 08:56:40 Batch  14605_32206 INIT bckcetl ids20 dsadm dsadm
^1_2 11/23/07 09:25:41 Batch  14572_33950 INIT bckcetl ids20 dsadm dsadm
^1_1 11/21/07 14:15:25 Batch  14570_51332 INIT bckcetl ids20 dsadm dsadm
^1_1 09/21/07 10:21:10 Batch  14509_37273 PROMOTE bckcetl ids20 dsadm bls for sa
^1_1 09/21/07 10:19:18 Batch  14509_37160 INIT bckcett testIDS30 dsadm bls for sa
^1_2 09/18/07 16:45:38 Batch  14506_60351 PROMOTE bckcett testIDS30 u10157 IAD 3rd Quarter Provider code moved to test
^1_2 09/18/07 15:05:48 Batch  14506_54384 INIT bckcett devlIDS30 u10157 moving IAD 3rd Quarter Provider changes to test
^1_2 09/14/06 13:22:08 Batch  14137_48134 INIT bckcett devlIDS30 u06640 Ralph
^1_1 09/08/06 12:32:44 Batch  14131_45168 INIT bckcett devlIDS30 u150129 Laurel
^1_1 04/13/06 07:36:31 Batch  13983_27397 INIT bckcett devlIDS30 u08717 Brent

MODIFICATIONS:
Developer : Manasa
Date      : 2012-03-12
Change    : Changed "IDS_SK" to "Table_Name_SK"  (TTR - 1098)

Annotation:
This Pkey used in IdsProvLocExtr, NABPProvLocExtr and ESIProvLocExtr
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# COMMAND ----------
def run_ProvLocPkey(df_Transform: DataFrame, params: dict) -> DataFrame:
    # unpack parameters
    CurrRunCycle      = params["CurrRunCycle"]
    ids_jdbc_url      = params["ids_jdbc_url"]
    ids_jdbc_props    = params["ids_jdbc_props"]

    # read dummy table that replaces hashed file hf_prov_loc
    extract_query = """
        SELECT
            SRC_SYS_CD            AS SRC_SYS_CD_lkp,
            PROV_ID               AS PROV_ID_lkp,
            PROV_ADDR_ID          AS PROV_ADDR_ID_lkp,
            PROV_ADDR_TYP_CD      AS PROV_ADDR_TYP_CD_lkp,
            PROV_ADDR_EFF_DT      AS PROV_ADDR_EFF_DT_lkp,
            CRT_RUN_CYC_EXCTN_SK  AS CRT_RUN_CYC_EXCTN_SK_lkp,
            PROV_LOC_SK           AS PROV_LOC_SK_lkp
        FROM dummy_hf_prov_loc
    """
    df_hf_prov_loc_lookup = (
        spark.read.format("jdbc")
            .option("url", ids_jdbc_url)
            .options(**ids_jdbc_props)
            .option("query", extract_query)
            .load()
    )

    # trim key columns on source dataframe
    key_cols = ["SRC_SYS_CD", "PROV_ID", "PROV_ADDR_ID",
                "PROV_ADDR_TYP_CD", "PROV_ADDR_EFF_DT"]
    df_src = df_Transform
    for col in key_cols:
        if col in df_src.columns:
            df_src = df_src.withColumn(col, F.trim(F.col(col)))

    # join with lookup
    join_cond = (
        (F.col("src.SRC_SYS_CD")       == F.col("lkp.SRC_SYS_CD_lkp")) &
        (F.col("src.PROV_ID")          == F.col("lkp.PROV_ID_lkp")) &
        (F.col("src.PROV_ADDR_ID")     == F.col("lkp.PROV_ADDR_ID_lkp")) &
        (F.col("src.PROV_ADDR_TYP_CD") == F.col("lkp.PROV_ADDR_TYP_CD_lkp")) &
        (F.col("src.PROV_ADDR_EFF_DT") == F.col("lkp.PROV_ADDR_EFF_DT_lkp"))
    )
    df_joined = (
        df_src.alias("src")
        .join(df_hf_prov_loc_lookup.alias("lkp"), join_cond, "left")
    )

    # enrich columns according to transformation logic
    df_enriched = (
        df_joined
        .withColumn(
            "PROV_LOC_SK",
            F.col("lkp.PROV_LOC_SK_lkp")
        )
        .withColumn(
            "CRT_RUN_CYC_EXCTN_SK",
            F.when(
                F.col("lkp.CRT_RUN_CYC_EXCTN_SK_lkp").isNotNull(),
                F.col("lkp.CRT_RUN_CYC_EXCTN_SK_lkp")
            ).otherwise(F.lit(CurrRunCycle))
        )
        .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(CurrRunCycle))
    )

    # surrogate-key generation for missing PROV_LOC_SK
    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,'PROV_LOC_SK',<schema>,<secret_name>)

    # prepare DataFrame for dummy table insert (only new records)
    df_insert = (
        df_enriched
        .filter(F.col("lkp.PROV_LOC_SK_lkp").isNull())
        .select(
            F.col("SRC_SYS_CD"),
            F.col("PROV_ID"),
            F.col("PROV_ADDR_ID"),
            F.col("PROV_ADDR_TYP_CD"),
            F.col("PROV_ADDR_EFF_DT"),
            F.col("CRT_RUN_CYC_EXCTN_SK"),
            F.col("PROV_LOC_SK")
        )
    )

    # write new rows back to dummy table
    (
        df_insert.write.format("jdbc")
            .option("url", ids_jdbc_url)
            .options(**ids_jdbc_props)
            .option("dbtable", "dummy_hf_prov_loc")
            .mode("append")
            .save()
    )

    # build output dataframe matching 'Key' link specification
    output_columns = [
        "JOB_EXCTN_RCRD_ERR_SK",
        "INSRT_UPDT_CD",
        "DISCARD_IN",
        "PASS_THRU_IN",
        "FIRST_RECYC_DT",
        "ERR_CT",
        "RECYCLE_CT",
        "SRC_SYS_CD",
        "PRI_KEY_STRING",
        "PROV_LOC_SK",
        "SRC_SYS_CD_SK",
        "PROV_ID",
        "PROV_ADDR_ID",
        "PROV_ADDR_TYP_CD",
        "PROV_ADDR_EFF_DT",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "PROV_ADDR_SK",
        "PROV_SK",
        "PRI_ADDR_IN",
        "REMIT_ADDR_IN"
    ]
    df_key = df_enriched.select([c for c in output_columns if c in df_enriched.columns])

    return df_key
# COMMAND ----------