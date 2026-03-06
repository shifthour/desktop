# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------
from pyspark.sql import DataFrame, functions as F
from pyspark.sql import Window


def run_MbrCommPK(
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    """
    PRIMARY KEY SHARED CONTAINER:  MbrCommPK
    
    DESCRIPTION:
        MBR_COMM Primary Key shared container originally implemented in IBM DataStage.
        Converted to PySpark for Databricks execution.
    
    DATA FLOW:
        1. Read dummy table <dummy_hf_mbr_comm> (replacement for hash-file hf_mbr_comm).
        2. Left-join incoming stream “Transform” with dummy table (lkup).
        3. Derive SK (MBR_COMM_SK) – generate when missing via SurrogateKeyGen.
        4. Derive NewCrtRunCycExtcnSk and other runtime columns.
        5. Split into:
           a. “Key” – main output stream returned by this function.
           b. “updt” – rows with new SKs, appended back to dummy table.
    """
    # ------------------------------------------------------------------
    # Unpack required runtime parameters
    CurrRunCycle        = params["CurrRunCycle"]
    IDSOwner            = params["IDSOwner"]
    ids_jdbc_url        = params["ids_jdbc_url"]
    ids_jdbc_props      = params["ids_jdbc_props"]
    # ------------------------------------------------------------------
    # Read dummy_hf_mbr_comm as lookup (scenario b hash-file replacement)
    extract_query = f"""
    SELECT
        SRC_SYS_CD            AS SRC_SYS_CD_lkp,
        MBR_UNIQ_KEY          AS MBR_UNIQ_KEY_lkp,
        CRT_RUN_CYC_EXCTN_SK  AS CRT_RUN_CYC_EXCTN_SK_lkp,
        MBR_COMM_SK           AS MBR_COMM_SK_lkp
    FROM {IDSOwner}.dummy_hf_mbr_comm
    """
    
    df_lkup = (
        spark.read.format("jdbc")
            .option("url", ids_jdbc_url)
            .options(**ids_jdbc_props)
            .option("query", extract_query)
            .load()
    )
    
    # ------------------------------------------------------------------
    # Join input with lookup
    df_joined = (
        df_Transform.alias("Transform")
        .join(
            df_lkup.alias("lkup"),
            (F.col("Transform.SRC_SYS_CD")  == F.col("lkup.SRC_SYS_CD_lkp")) &
            (F.col("Transform.MBR_UNIQ_KEY") == F.col("lkup.MBR_UNIQ_KEY_lkp")),
            "left"
        )
    )
    
    # ------------------------------------------------------------------
    # Derivations
    df_enriched = (
        df_joined
        .withColumn(
            "MBR_COMM_SK",
            F.when(
                F.col("lkup.MBR_COMM_SK_lkp").isNull(),
                F.lit(None)   # to be populated by SurrogateKeyGen
            ).otherwise(F.col("lkup.MBR_COMM_SK_lkp"))
        )
        .withColumn(
            "NewCrtRunCycExtcnSk",
            F.when(
                F.col("lkup.MBR_COMM_SK_lkp").isNull(),
                F.lit(CurrRunCycle)
            ).otherwise(F.col("lkup.CRT_RUN_CYC_EXCTN_SK_lkp"))
        )
        .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(CurrRunCycle))
    )
    
    # ------------------------------------------------------------------
    # Surrogate-key generation
    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"MBR_COMM_SK",<schema>,<secret_name>)
    
    # ------------------------------------------------------------------
    # Prepare “updt” stream (new rows for dummy table)
    df_updt = (
        df_enriched
        .filter(F.col("lkup.MBR_COMM_SK_lkp").isNull())
        .select(
            F.col("Transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
            F.col("Transform.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
            F.lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
            F.col("MBR_COMM_SK")
        )
    )
    
    # Append new rows to dummy table
    (
        df_updt.write
        .format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("dbtable", f"{IDSOwner}.dummy_hf_mbr_comm")
        .mode("append")
        .save()
    )
    
    # ------------------------------------------------------------------
    # Prepare “Key” output stream
    df_key = (
        df_enriched
        .select(
            F.col("Transform.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
            F.col("Transform.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
            F.col("Transform.DISCARD_IN").alias("DISCARD_IN"),
            F.col("Transform.PASS_THRU_IN").alias("PASS_THRU_IN"),
            F.col("Transform.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
            F.col("Transform.ERR_CT").alias("ERR_CT"),
            F.col("Transform.RECYCLE_CT").alias("RECYCLE_CT"),
            F.col("Transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
            F.col("Transform.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
            F.col("MBR_COMM_SK").alias("MBR_COMM_SK"),
            F.col("Transform.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
            F.col("NewCrtRunCycExtcnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            F.col("Transform.MYCLM_EOB_PRFRNC_CD").alias("MYCLM_EOB_PRFRNC_CD"),
            F.col("Transform.RCRD_STTUS_CD").alias("RCRD_STTUS_CD"),
            F.col("Transform.STTUS_RSN_CD").alias("STTUS_RSN_CD"),
            F.col("Transform.AUTO_PAY_NTFCTN_IN").alias("AUTO_PAY_NTFCTN_IN"),
            F.col("Transform.BLUELOOP_PARTCPN_IN").alias("BLUELOOP_PARTCPN_IN"),
            F.col("Transform.HLTH_WELNS_DO_NOT_SEND_IN").alias("HLTH_WELNS_DO_NOT_SEND_IN"),
            F.col("Transform.HLTH_WELNS_EMAIL_IN").alias("HLTH_WELNS_EMAIL_IN"),
            F.col("Transform.HLTH_WELNS_POSTAL_IN").alias("HLTH_WELNS_POSTAL_IN"),
            F.col("Transform.HLTH_WELNS_SMS_IN").alias("HLTH_WELNS_SMS_IN"),
            F.col("Transform.MBR_PRFRNCS_SET_IN").alias("MBR_PRFRNCS_SET_IN"),
            F.col("Transform.MYBLUEKC_ELTRNC_BILL_EMAIL_IN").alias("MYBLUEKC_ELTRNC_BILL_EMAIL_IN"),
            F.col("Transform.MYBLUEKC_ELTRNC_BILL_SMS_IN").alias("MYBLUEKC_ELTRNC_BILL_SMS_IN"),
            F.col("Transform.MYBLUEKC_PRT_BILL_EMAIL_IN").alias("MYBLUEKC_PRT_BILL_EMAIL_IN"),
            F.col("Transform.MYBLUEKC_PRT_BILL_POSTAL_IN").alias("MYBLUEKC_PRT_BILL_POSTAL_IN"),
            F.col("Transform.MYBLUEKC_PRT_BILL_SMS_IN").alias("MYBLUEKC_PRT_BILL_SMS_IN"),
            F.col("Transform.MYCLM_EMAIL_IN").alias("MYCLM_EMAIL_IN"),
            F.col("Transform.MYCLM_POSTAL_IN").alias("MYCLM_POSTAL_IN"),
            F.col("Transform.MYCLM_SMS_IN").alias("MYCLM_SMS_IN"),
            F.col("Transform.MYPLN_INFO_EMAIL_IN").alias("MYPLN_INFO_EMAIL_IN"),
            F.col("Transform.MYPLN_INFO_POSTAL_IN").alias("MYPLN_INFO_POSTAL_IN"),
            F.col("Transform.MYPLN_INFO_SMS_IN").alias("MYPLN_INFO_SMS_IN"),
            F.col("Transform.PROD_SVC_DO_NOT_SEND_IN").alias("PROD_SVC_DO_NOT_SEND_IN"),
            F.col("Transform.PROD_SVC_EMAIL_IN").alias("PROD_SVC_EMAIL_IN"),
            F.col("Transform.PROD_SVC_POSTAL_IN").alias("PROD_SVC_POSTAL_IN"),
            F.col("Transform.PROD_SVC_SMS_IN").alias("PROD_SVC_SMS_IN"),
            F.col("Transform.SRC_SYS_UPDT_DTM").alias("SRC_SYS_UPDT_DTM"),
            F.col("Transform.CELL_PHN_NO").alias("CELL_PHN_NO"),
            F.col("Transform.EMAIL_ADDR").alias("EMAIL_ADDR"),
            F.col("Transform.OTHR_EMAIL_ADDR").alias("OTHR_EMAIL_ADDR"),
            F.col("Transform.FCTS_EMAIL_ADDR").alias("FCTS_EMAIL_ADDR"),
            F.col("Transform.WEB_RGSTRN_EMAIL_ADDR").alias("WEB_RGSTRN_EMAIL_ADDR"),
            F.col("Transform.MEMBER_EDUCATIONAL_EMAIL_INDICATOR").alias("MEMBER_EDUCATIONAL_EMAIL_INDICATOR"),
            F.col("Transform.MEMBER_EDUCATIONAL_POSTAL_INDICATOR").alias("MEMBER_EDUCATIONAL_POSTAL_INDICATOR"),
            F.col("Transform.MEMBER_REQUIRED_EMAIL_INDICATOR").alias("MEMBER_REQUIRED_EMAIL_INDICATOR"),
            F.col("Transform.MEMBER_REQUIRED_POSTAL_INDICATOR").alias("MEMBER_REQUIRED_POSTAL_INDICATOR"),
            F.col("Transform.MEMBER_GENERIC_SMS_INDICATOR").alias("MEMBER_GENERIC_SMS_INDICATOR"),
            F.col("Transform.MEMBER_OPTIONAL_PHONE_CALL_INDICATOR").alias("MEMBER_OPTIONAL_PHONE_CALL_INDICATOR")
        )
    )
    
    # ------------------------------------------------------------------
    return df_key