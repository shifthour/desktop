# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2015 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC Job Name : FctsIdsEobAccumXfrm
# MAGIC 
# MAGIC                            
# MAGIC PROCESSING: Generates Primary Key for INTER_PLN_BILL_DTL table
# MAGIC     
# MAGIC 
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                         Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                    ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Kalyan Neelam                 2015-08-18         5212                              Initial Programming                                              IntegrateDev1                 Bhoomi Dasari          8/21/2015
# MAGIC 
# MAGIC 
# MAGIC Karthik Chintalapani         2016-02-01         5212                           Added new column   
# MAGIC                                                                                                        POST_AS_EXP_IN                                                 IntegrateDev1          Kalyan Neelam               2016-02-02
# MAGIC 
# MAGIC 
# MAGIC Karthik Chintalapani         2016-08-24         5212                           Added new column   
# MAGIC                                                                                                        PRCS_CYC_DT_SK                                                 IntegrateDev1         Kalyan Neelam              2016-08-25
# MAGIC 
# MAGIC K Chintalapani                    2017-04-09     5587                             Added new columns                                                  IntegrateDev1        Kalyan Neelam              2017-04-11
# MAGIC VAL_BASED_PGM_ID, DSPTD_RCRD_CD, HOME_PLN_DSPT_ACTN_CD, CMNT_TX, HOST_PLN_DSPT_ACTN_CD, LOCAL_PLN_CTL_NO, PRCS_SITE_CTL_NO
# MAGIC 
# MAGIC 
# MAGIC Manasa Andru                  2018-11-02      5726              Added BILL_DTL_RCRD_TYP_CD field at the end              IntegrateDev2            Kalyan Neelam              2018-12-18
# MAGIC                                                                18.5 Submission

# MAGIC New PKEYs are generated in this transformer. A database sequence will be used to create next PKEY value.
# MAGIC Insert Only the Newly generated Pkeys into the K Table
# MAGIC Read K_INTER_PLN_BILL_DTL Table to pull the Natural Keys and the Skey.
# MAGIC Inner Join primary key info with table info
# MAGIC Left Join on Natural Keys
# MAGIC Transformed Data will land into a Dataset for Primary Keying job.
# MAGIC 
# MAGIC Data is partitioned on Natural Key Columns.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# Retrieve parameter values
SrcSysCd = get_widget_value('SrcSysCd','BCBSA')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
IDSRunCycle = get_widget_value('IDSRunCycle','100')
RunID = get_widget_value('RunID','100')

# --------------------------------------------------------------------------------
# Read from ds_INTER_PLN_BILL_DTL_Xfm (PxDataSet) - translated to Parquet read
# --------------------------------------------------------------------------------
df_ds_INTER_PLN_BILL_DTL_Xfm_base = spark.read.parquet(f"{adls_path}/ds/INTER_PLN_BILL_DTL.{SrcSysCd}.xfrm.{RunID}.parquet")

df_ds_INTER_PLN_BILL_DTL_Xfm_padded = (
    df_ds_INTER_PLN_BILL_DTL_Xfm_base
    .withColumn("BTCH_CRT_DT_SK", F.rpad(F.col("BTCH_CRT_DT_SK"), 10, " "))
    .withColumn("CBF_SETL_STRT_DT_SK", F.rpad(F.col("CBF_SETL_STRT_DT_SK"), 10, " "))
    .withColumn("CBF_SETL_BILL_END_DT_SK", F.rpad(F.col("CBF_SETL_BILL_END_DT_SK"), 10, " "))
    .withColumn("CFA_DISP_DT_SK", F.rpad(F.col("CFA_DISP_DT_SK"), 10, " "))
    .withColumn("CFA_PRCS_DT_SK", F.rpad(F.col("CFA_PRCS_DT_SK"), 10, " "))
    .withColumn("HOME_AUTH_DENIAL_DT_SK", F.rpad(F.col("HOME_AUTH_DENIAL_DT_SK"), 10, " "))
    .withColumn("INCUR_PERD_STRT_DT_SK", F.rpad(F.col("INCUR_PERD_STRT_DT_SK"), 10, " "))
    .withColumn("INCUR_PERD_END_DT_SK", F.rpad(F.col("INCUR_PERD_END_DT_SK"), 10, " "))
    .withColumn("MSRMNT_PERD_STRT_DT_SK", F.rpad(F.col("MSRMNT_PERD_STRT_DT_SK"), 10, " "))
    .withColumn("MSRMNT_PERD_END_DT_SK", F.rpad(F.col("MSRMNT_PERD_END_DT_SK"), 10, " "))
    .withColumn("MBR_ELIG_STRT_DT_SK", F.rpad(F.col("MBR_ELIG_STRT_DT_SK"), 10, " "))
    .withColumn("MBR_ELIG_END_DT_SK", F.rpad(F.col("MBR_ELIG_END_DT_SK"), 10, " "))
    .withColumn("PRCS_CYC_YR_MO_SK", F.rpad(F.col("PRCS_CYC_YR_MO_SK"), 6, " "))
    .withColumn("SETL_DT_SK", F.rpad(F.col("SETL_DT_SK"), 10, " "))
    .withColumn("POST_AS_EXP_IN", F.rpad(F.col("POST_AS_EXP_IN"), 1, " "))
    .withColumn("PRCS_CYC_DT_SK", F.rpad(F.col("PRCS_CYC_DT_SK"), 10, " "))
    .withColumn("VAL_BASED_PGM_ID", F.rpad(F.col("VAL_BASED_PGM_ID"), 7, " "))
    .withColumn("BILL_DTL_RCRD_TYP_CD", F.rpad(F.col("BILL_DTL_RCRD_TYP_CD"), 1, " "))
)

df_ds_INTER_PLN_BILL_DTL_Xfm = df_ds_INTER_PLN_BILL_DTL_Xfm_padded.select(
    "PRI_NAT_KEY_STRING",
    "FIRST_RECYC_TS",
    "INTER_PLN_BILL_DTL_SK",
    "INTER_PLN_BILL_DTL_ID",
    "BUS_OWNER_ID",
    "SRC_SYS_CD",
    "SRC_SYS_CD_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "ATTRBTN_PGM_CD",
    "CFA_RMBRMT_TYP_CD",
    "CFA_TRANS_TYP_CD",
    "CTL_PLN_CD",
    "EPSD_TYP_CD",
    "HOME_CORP_PLN_CD",
    "HOME_PLN_CD",
    "HOST_CORP_PLN_CD",
    "HOST_PLN_CD",
    "LOCAL_PLN_CD",
    "VAL_BASED_PGM_CD",
    "VAL_BASED_PGM_PAYMT_TYP_CD",
    "BTCH_CRT_DT_SK",
    "CBF_SETL_STRT_DT_SK",
    "CBF_SETL_BILL_END_DT_SK",
    "CFA_DISP_DT_SK",
    "CFA_PRCS_DT_SK",
    "HOME_AUTH_DENIAL_DT_SK",
    "INCUR_PERD_STRT_DT_SK",
    "INCUR_PERD_END_DT_SK",
    "MSRMNT_PERD_STRT_DT_SK",
    "MSRMNT_PERD_END_DT_SK",
    "MBR_ELIG_STRT_DT_SK",
    "MBR_ELIG_END_DT_SK",
    "PRCS_CYC_YR_MO_SK",
    "SETL_DT_SK",
    "ACES_FEE_ADJ_AMT",
    "CBF_SETL_RATE_AMT",
    "MBR_LVL_CHRG_AMT",
    "ATRBD_PROV_ID",
    "ATRBD_PROV_NTNL_PROV_ID",
    "ATTRBTN_PROV_PGM_NM",
    "BCBSA_MMI_ID",
    "CONSIS_MBR_ID",
    "CBF_SCCF_NO",
    "GRP_ID",
    "HOME_PLN_MBR_ID",
    "ITS_SUB_ID",
    "ORIG_INTER_PLN_BILL_DTL_SRL_NO",
    "ORIG_ITS_CLM_SCCF_NO",
    "PROV_GRP_NM",
    "VRNC_ACCT_ID",
    "BTCH_STTUS_CD_TX",
    "STTUS_CD_TX",
    "ERR_CD_TX",
    "POST_AS_EXP_IN",
    "PRCS_CYC_DT_SK",
    "MBR_SK",
    "VAL_BASED_PGM_ID",
    "DSPTD_RCRD_CD",
    "HOME_PLN_DSPT_ACTN_CD",
    "CMNT_TX",
    "HOST_PLN_DSPT_ACTN_CD",
    "LOCAL_PLN_CTL_NO",
    "PRCS_SITE_CTL_NO",
    "BILL_DTL_RCRD_TYP_CD"
)

# --------------------------------------------------------------------------------
# cp_pk (PxCopy) -> produce two output dataframes
# --------------------------------------------------------------------------------
df_cp_pk_lnk_Transforms_Out = df_ds_INTER_PLN_BILL_DTL_Xfm.select(
    F.col("INTER_PLN_BILL_DTL_ID").alias("INTER_PLN_BILL_DTL_ID"),
    F.col("BUS_OWNER_ID").alias("BUS_OWNER_ID"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD")
)

df_cp_pk_Lnk_cp_Out = df_ds_INTER_PLN_BILL_DTL_Xfm.select(
    "PRI_NAT_KEY_STRING",
    "FIRST_RECYC_TS",
    "INTER_PLN_BILL_DTL_ID",
    "BUS_OWNER_ID",
    "SRC_SYS_CD",
    "SRC_SYS_CD_SK",
    "ATTRBTN_PGM_CD",
    "CFA_RMBRMT_TYP_CD",
    "CFA_TRANS_TYP_CD",
    "CTL_PLN_CD",
    "EPSD_TYP_CD",
    "HOME_CORP_PLN_CD",
    "HOME_PLN_CD",
    "HOST_CORP_PLN_CD",
    "HOST_PLN_CD",
    "LOCAL_PLN_CD",
    "VAL_BASED_PGM_CD",
    "VAL_BASED_PGM_PAYMT_TYP_CD",
    "BTCH_CRT_DT_SK",
    "CBF_SETL_STRT_DT_SK",
    "CBF_SETL_BILL_END_DT_SK",
    "CFA_DISP_DT_SK",
    "CFA_PRCS_DT_SK",
    "HOME_AUTH_DENIAL_DT_SK",
    "INCUR_PERD_STRT_DT_SK",
    "INCUR_PERD_END_DT_SK",
    "MSRMNT_PERD_STRT_DT_SK",
    "MSRMNT_PERD_END_DT_SK",
    "MBR_ELIG_STRT_DT_SK",
    "MBR_ELIG_END_DT_SK",
    "PRCS_CYC_YR_MO_SK",
    "SETL_DT_SK",
    "ACES_FEE_ADJ_AMT",
    "CBF_SETL_RATE_AMT",
    "MBR_LVL_CHRG_AMT",
    "ATRBD_PROV_ID",
    "ATRBD_PROV_NTNL_PROV_ID",
    "ATTRBTN_PROV_PGM_NM",
    "BCBSA_MMI_ID",
    "CONSIS_MBR_ID",
    "CBF_SCCF_NO",
    "GRP_ID",
    "HOME_PLN_MBR_ID",
    "ITS_SUB_ID",
    "ORIG_INTER_PLN_BILL_DTL_SRL_NO",
    "ORIG_ITS_CLM_SCCF_NO",
    "PROV_GRP_NM",
    "VRNC_ACCT_ID",
    "BTCH_STTUS_CD_TX",
    "STTUS_CD_TX",
    "ERR_CD_TX",
    "POST_AS_EXP_IN",
    "PRCS_CYC_DT_SK",
    "MBR_SK",
    "VAL_BASED_PGM_ID",
    "DSPTD_RCRD_CD",
    "HOME_PLN_DSPT_ACTN_CD",
    "CMNT_TX",
    "HOST_PLN_DSPT_ACTN_CD",
    "LOCAL_PLN_CTL_NO",
    "PRCS_SITE_CTL_NO",
    "BILL_DTL_RCRD_TYP_CD"
)

# --------------------------------------------------------------------------------
# rdup_Natural_Keys (PxRemDup)
# --------------------------------------------------------------------------------
df_rdup_Natural_Keys_temp = dedup_sort(
    df_cp_pk_lnk_Transforms_Out,
    partition_cols=["INTER_PLN_BILL_DTL_ID", "BUS_OWNER_ID", "SRC_SYS_CD"],
    sort_cols=[]
)
df_rdup_Natural_Keys_out = df_rdup_Natural_Keys_temp.select(
    "INTER_PLN_BILL_DTL_ID",
    "BUS_OWNER_ID",
    "SRC_SYS_CD"
)

# --------------------------------------------------------------------------------
# db2_K_INTER_PLN_BILL_DTL_in (DB2ConnectorPX)
# --------------------------------------------------------------------------------
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"SELECT INTER_PLN_BILL_DTL_ID, BUS_OWNER_ID, SRC_SYS_CD, CRT_RUN_CYC_EXCTN_SK, INTER_PLN_BILL_DTL_SK FROM {IDSOwner}.K_INTER_PLN_BILL_DTL"
df_db2_K_INTER_PLN_BILL_DTL_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# --------------------------------------------------------------------------------
# jn_InterPlnBillDtl (PxJoin) - left outer join
# --------------------------------------------------------------------------------
df_jn_InterPlnBillDtl_temp = df_rdup_Natural_Keys_out.alias("lnk_Natural_Keys_out").join(
    df_db2_K_INTER_PLN_BILL_DTL_in.alias("lnk_KInterPlnBillDtlPkey_out"),
    [
        F.col("lnk_Natural_Keys_out.INTER_PLN_BILL_DTL_ID") == F.col("lnk_KInterPlnBillDtlPkey_out.INTER_PLN_BILL_DTL_ID"),
        F.col("lnk_Natural_Keys_out.BUS_OWNER_ID") == F.col("lnk_KInterPlnBillDtlPkey_out.BUS_OWNER_ID"),
        F.col("lnk_Natural_Keys_out.SRC_SYS_CD") == F.col("lnk_KInterPlnBillDtlPkey_out.SRC_SYS_CD")
    ],
    how="left"
)

df_jn_InterPlnBillDtl_out = df_jn_InterPlnBillDtl_temp.select(
    F.col("lnk_Natural_Keys_out.INTER_PLN_BILL_DTL_ID").alias("INTER_PLN_BILL_DTL_ID"),
    F.col("lnk_Natural_Keys_out.BUS_OWNER_ID").alias("BUS_OWNER_ID"),
    F.col("lnk_Natural_Keys_out.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnk_KInterPlnBillDtlPkey_out.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("lnk_KInterPlnBillDtlPkey_out.INTER_PLN_BILL_DTL_SK").alias("INTER_PLN_BILL_DTL_SK")
)

# --------------------------------------------------------------------------------
# xfrm_PKEYgen (CTransformerStage)
# --------------------------------------------------------------------------------
df_xfrm_PKEYgen_in = df_jn_InterPlnBillDtl_out

df_xfrm_PKEYgen_temp = (
    df_xfrm_PKEYgen_in
    .withColumn("INTER_PLN_BILL_DTL_SK_orig", F.col("INTER_PLN_BILL_DTL_SK"))
    .withColumn("svRunCyle", F.when(F.isnull("INTER_PLN_BILL_DTL_SK_orig"), F.lit(IDSRunCycle))
                .otherwise(F.col("CRT_RUN_CYC_EXCTN_SK")))
    .withColumn("INTER_PLN_BILL_DTL_SK_temp", F.when(F.isnull("INTER_PLN_BILL_DTL_SK_orig"), F.lit(None))
                .otherwise(F.col("INTER_PLN_BILL_DTL_SK_orig")))
)

df_enriched = SurrogateKeyGen(df_xfrm_PKEYgen_temp,<DB sequence name>,"INTER_PLN_BILL_DTL_SK_temp",<schema>,<secret_name>)

df_xfrm_PKEYgen = df_enriched.withColumn("svInterPlnBillDtlSK", F.col("INTER_PLN_BILL_DTL_SK_temp"))

df_lnk_Pkey_out = df_xfrm_PKEYgen.select(
    F.col("INTER_PLN_BILL_DTL_ID").alias("INTER_PLN_BILL_DTL_ID"),
    F.col("BUS_OWNER_ID").alias("BUS_OWNER_ID"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("svRunCyle").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(IDSRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("svInterPlnBillDtlSK").alias("INTER_PLN_BILL_DTL_SK")
)

df_lnk_KInterPlnBillDtl_Out_temp = df_xfrm_PKEYgen.filter(F.isnull("INTER_PLN_BILL_DTL_SK_orig"))
df_lnk_KInterPlnBillDtl_Out = df_lnk_KInterPlnBillDtl_Out_temp.select(
    F.col("INTER_PLN_BILL_DTL_ID").alias("INTER_PLN_BILL_DTL_ID"),
    F.col("BUS_OWNER_ID").alias("BUS_OWNER_ID"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("svRunCyle").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("svInterPlnBillDtlSK").alias("INTER_PLN_BILL_DTL_SK")
)

# --------------------------------------------------------------------------------
# db2_K_INTER_PLN_BILL_DTL_Load (DB2ConnectorPX) - Write (Merge Upsert)
# --------------------------------------------------------------------------------
df_db2_out = df_lnk_KInterPlnBillDtl_Out

spark.sql(f"DROP TABLE IF EXISTS STAGING.IdsInterPlnBillDtlExtrPkey_db2_K_INTER_PLN_BILL_DTL_Load_temp")

df_db2_out.write.jdbc(
    url=jdbc_url,
    table="STAGING.IdsInterPlnBillDtlExtrPkey_db2_K_INTER_PLN_BILL_DTL_Load_temp",
    mode="overwrite",
    properties=jdbc_props
)

merge_sql = (
    f"MERGE {IDSOwner}.K_INTER_PLN_BILL_DTL AS T "
    f"USING STAGING.IdsInterPlnBillDtlExtrPkey_db2_K_INTER_PLN_BILL_DTL_Load_temp AS S "
    f"ON T.INTER_PLN_BILL_DTL_ID=S.INTER_PLN_BILL_DTL_ID AND T.BUS_OWNER_ID=S.BUS_OWNER_ID AND T.SRC_SYS_CD=S.SRC_SYS_CD "
    f"WHEN MATCHED THEN UPDATE SET "
    f"T.CRT_RUN_CYC_EXCTN_SK=S.CRT_RUN_CYC_EXCTN_SK, "
    f"T.INTER_PLN_BILL_DTL_SK=S.INTER_PLN_BILL_DTL_SK "
    f"WHEN NOT MATCHED THEN INSERT (INTER_PLN_BILL_DTL_ID, BUS_OWNER_ID, SRC_SYS_CD, CRT_RUN_CYC_EXCTN_SK, INTER_PLN_BILL_DTL_SK) "
    f"VALUES (S.INTER_PLN_BILL_DTL_ID, S.BUS_OWNER_ID, S.SRC_SYS_CD, S.CRT_RUN_CYC_EXCTN_SK, S.INTER_PLN_BILL_DTL_SK);"
)

execute_dml(merge_sql, jdbc_url, jdbc_props)

# --------------------------------------------------------------------------------
# jn_PKey (PxJoin) - inner join of Lnk_cp_Out and lnk_Pkey_out
# --------------------------------------------------------------------------------
df_jn_PKey_temp = df_cp_pk_Lnk_cp_Out.alias("Lnk_cp_Out").join(
    df_lnk_Pkey_out.alias("lnk_Pkey_out"),
    [
        F.col("Lnk_cp_Out.INTER_PLN_BILL_DTL_ID") == F.col("lnk_Pkey_out.INTER_PLN_BILL_DTL_ID"),
        F.col("Lnk_cp_Out.BUS_OWNER_ID") == F.col("lnk_Pkey_out.BUS_OWNER_ID"),
        F.col("Lnk_cp_Out.SRC_SYS_CD") == F.col("lnk_Pkey_out.SRC_SYS_CD")
    ],
    how="inner"
)

df_jn_PKey = df_jn_PKey_temp.select(
    F.col("Lnk_cp_Out.PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("Lnk_cp_Out.FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("lnk_Pkey_out.INTER_PLN_BILL_DTL_SK").alias("INTER_PLN_BILL_DTL_SK"),
    F.col("Lnk_cp_Out.INTER_PLN_BILL_DTL_ID").alias("INTER_PLN_BILL_DTL_ID"),
    F.col("Lnk_cp_Out.BUS_OWNER_ID").alias("BUS_OWNER_ID"),
    F.col("Lnk_cp_Out.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Lnk_cp_Out.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("lnk_Pkey_out.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("lnk_Pkey_out.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("Lnk_cp_Out.ATTRBTN_PGM_CD").alias("ATTRBTN_PGM_CD"),
    F.col("Lnk_cp_Out.CFA_RMBRMT_TYP_CD").alias("CFA_RMBRMT_TYP_CD"),
    F.col("Lnk_cp_Out.CFA_TRANS_TYP_CD").alias("CFA_TRANS_TYP_CD"),
    F.col("Lnk_cp_Out.CTL_PLN_CD").alias("CTL_PLN_CD"),
    F.col("Lnk_cp_Out.EPSD_TYP_CD").alias("EPSD_TYP_CD"),
    F.col("Lnk_cp_Out.HOME_CORP_PLN_CD").alias("HOME_CORP_PLN_CD"),
    F.col("Lnk_cp_Out.HOME_PLN_CD").alias("HOME_PLN_CD"),
    F.col("Lnk_cp_Out.HOST_CORP_PLN_CD").alias("HOST_CORP_PLN_CD"),
    F.col("Lnk_cp_Out.HOST_PLN_CD").alias("HOST_PLN_CD"),
    F.col("Lnk_cp_Out.LOCAL_PLN_CD").alias("LOCAL_PLN_CD"),
    F.col("Lnk_cp_Out.VAL_BASED_PGM_CD").alias("VAL_BASED_PGM_CD"),
    F.col("Lnk_cp_Out.VAL_BASED_PGM_PAYMT_TYP_CD").alias("VAL_BASED_PGM_PAYMT_TYP_CD"),
    F.col("Lnk_cp_Out.BTCH_CRT_DT_SK").alias("BTCH_CRT_DT_SK"),
    F.col("Lnk_cp_Out.CBF_SETL_STRT_DT_SK").alias("CBF_SETL_STRT_DT_SK"),
    F.col("Lnk_cp_Out.CBF_SETL_BILL_END_DT_SK").alias("CBF_SETL_BILL_END_DT_SK"),
    F.col("Lnk_cp_Out.CFA_DISP_DT_SK").alias("CFA_DISP_DT_SK"),
    F.col("Lnk_cp_Out.CFA_PRCS_DT_SK").alias("CFA_PRCS_DT_SK"),
    F.col("Lnk_cp_Out.HOME_AUTH_DENIAL_DT_SK").alias("HOME_AUTH_DENIAL_DT_SK"),
    F.col("Lnk_cp_Out.INCUR_PERD_STRT_DT_SK").alias("INCUR_PERD_STRT_DT_SK"),
    F.col("Lnk_cp_Out.INCUR_PERD_END_DT_SK").alias("INCUR_PERD_END_DT_SK"),
    F.col("Lnk_cp_Out.MSRMNT_PERD_STRT_DT_SK").alias("MSRMNT_PERD_STRT_DT_SK"),
    F.col("Lnk_cp_Out.MSRMNT_PERD_END_DT_SK").alias("MSRMNT_PERD_END_DT_SK"),
    F.col("Lnk_cp_Out.MBR_ELIG_STRT_DT_SK").alias("MBR_ELIG_STRT_DT_SK"),
    F.col("Lnk_cp_Out.MBR_ELIG_END_DT_SK").alias("MBR_ELIG_END_DT_SK"),
    F.col("Lnk_cp_Out.PRCS_CYC_YR_MO_SK").alias("PRCS_CYC_YR_MO_SK"),
    F.col("Lnk_cp_Out.SETL_DT_SK").alias("SETL_DT_SK"),
    F.col("Lnk_cp_Out.ACES_FEE_ADJ_AMT").alias("ACES_FEE_ADJ_AMT"),
    F.col("Lnk_cp_Out.CBF_SETL_RATE_AMT").alias("CBF_SETL_RATE_AMT"),
    F.col("Lnk_cp_Out.MBR_LVL_CHRG_AMT").alias("MBR_LVL_CHRG_AMT"),
    F.col("Lnk_cp_Out.ATRBD_PROV_ID").alias("ATRBD_PROV_ID"),
    F.col("Lnk_cp_Out.ATRBD_PROV_NTNL_PROV_ID").alias("ATRBD_PROV_NTNL_PROV_ID"),
    F.col("Lnk_cp_Out.ATTRBTN_PROV_PGM_NM").alias("ATTRBTN_PROV_PGM_NM"),
    F.col("Lnk_cp_Out.BCBSA_MMI_ID").alias("BCBSA_MMI_ID"),
    F.col("Lnk_cp_Out.CONSIS_MBR_ID").alias("CONSIS_MBR_ID"),
    F.col("Lnk_cp_Out.CBF_SCCF_NO").alias("CBF_SCCF_NO"),
    F.col("Lnk_cp_Out.GRP_ID").alias("GRP_ID"),
    F.col("Lnk_cp_Out.HOME_PLN_MBR_ID").alias("HOME_PLN_MBR_ID"),
    F.col("Lnk_cp_Out.ITS_SUB_ID").alias("ITS_SUB_ID"),
    F.col("Lnk_cp_Out.ORIG_INTER_PLN_BILL_DTL_SRL_NO").alias("ORIG_INTER_PLN_BILL_DTL_SRL_NO"),
    F.col("Lnk_cp_Out.ORIG_ITS_CLM_SCCF_NO").alias("ORIG_ITS_CLM_SCCF_NO"),
    F.col("Lnk_cp_Out.PROV_GRP_NM").alias("PROV_GRP_NM"),
    F.col("Lnk_cp_Out.VRNC_ACCT_ID").alias("VRNC_ACCT_ID"),
    F.col("Lnk_cp_Out.BTCH_STTUS_CD_TX").alias("BTCH_STTUS_CD_TX"),
    F.col("Lnk_cp_Out.STTUS_CD_TX").alias("STTUS_CD_TX"),
    F.col("Lnk_cp_Out.ERR_CD_TX").alias("ERR_CD_TX"),
    F.col("Lnk_cp_Out.POST_AS_EXP_IN").alias("POST_AS_EXP_IN"),
    F.col("Lnk_cp_Out.PRCS_CYC_DT_SK").alias("PRCS_CYC_DT_SK"),
    F.col("Lnk_cp_Out.MBR_SK").alias("MBR_SK"),
    F.col("Lnk_cp_Out.VAL_BASED_PGM_ID").alias("VAL_BASED_PGM_ID"),
    F.col("Lnk_cp_Out.DSPTD_RCRD_CD").alias("DSPTD_RCRD_CD"),
    F.col("Lnk_cp_Out.HOME_PLN_DSPT_ACTN_CD").alias("HOME_PLN_DSPT_ACTN_CD"),
    F.col("Lnk_cp_Out.CMNT_TX").alias("CMNT_TX"),
    F.col("Lnk_cp_Out.HOST_PLN_DSPT_ACTN_CD").alias("HOST_PLN_DSPT_ACTN_CD"),
    F.col("Lnk_cp_Out.LOCAL_PLN_CTL_NO").alias("LOCAL_PLN_CTL_NO"),
    F.col("Lnk_cp_Out.PRCS_SITE_CTL_NO").alias("PRCS_SITE_CTL_NO"),
    F.col("Lnk_cp_Out.BILL_DTL_RCRD_TYP_CD").alias("BILL_DTL_RCRD_TYP_CD")
)

# --------------------------------------------------------------------------------
# seq_INTER_PLN_BILL_DTL_PKEY (PxSequentialFile) - write to .dat file
# --------------------------------------------------------------------------------
df_seq_INTER_PLN_BILL_DTL_PKEY = df_jn_PKey

write_files(
    df_seq_INTER_PLN_BILL_DTL_PKEY,
    f"{adls_path}/key/INTER_PLN_BILL_DTL.{SrcSysCd}.pkey.{RunID}.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)