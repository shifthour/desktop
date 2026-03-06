# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC **********************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2014 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:  OPTUM Claims denied data is extracted from source file and loaded into the  MBR_PDX_DENIED_TRANS Table
# MAGIC 
# MAGIC 
# MAGIC Called By: OptumIdsMbrPdxDeniedTransLoadSeq
# MAGIC 
# MAGIC 
# MAGIC DEVELOPER             DATE                PROJECT                                                          DESCRIPTION                                                                                     ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------                                             -----------------------------------------------------------------------------------                           ------------------------------    ------------------------------       --------------------                                          
# MAGIC Peter Gichiri              2019-10-02           6131 - PBM REPLACEMENT  to OPTUMRX  Initial Devlopment                                                                                  Integrate2                     Kalyan Neelam             2019-11-21
# MAGIC Peter Gichiri              2020-01-09           6131 - PBM REPLACEMENT                          If NullToEmpty(lnk_Mbr_PdxDeniedJnData_out.MBR_PDX_DE-                                               Kalyan Neelam             2020-01-09
# MAGIC                                                                                                                                       NIED_TRANS_SK) ='' then IDSRunCycle else  lnk_Mbr_PdxDen-
# MAGIC                                                                                                                                       iedJnData_out.CRT_RUN_CYC_EXCTN_SK
# MAGIC Velmani Kondappan  20200-04-02      6131 - PBM REPLACEMEN                             Changed the sorting order within the rdup_Natural_Keys to remove        IntegrateDev2                     Kalyan Neelam            2020-04-09
# MAGIC                                                                                                                                     error warinings
# MAGIC 
# MAGIC Rekha Radhakrishna 2020-10-20        6343 - PhaseII governmanet program             Added Keys in Join to K_ table                                                                 IntegrateDev2

# MAGIC IdsMbrPdxDeniedTransPkey
# MAGIC Land into Seq File for the FKEY job
# MAGIC Insert Only the Newly generated Pkeys into the K Table
# MAGIC Read K_MBR_PDX_DENIED_TRANS Table to pull the Natural Keys and the Skey.
# MAGIC Triggered from :OptumIdsMbrPdxDeniedTransLoadSeq
# MAGIC Inner Join primary key info with table info
# MAGIC Left Join on Natural Keys
# MAGIC New PKEYs are generated in this transformer. A database sequence will be used to create next PKEY value.
# MAGIC Read data from MBR_PDX_DENIED_TRANS dataset.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark.sql import DataFrame
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# Retrieve parameter values
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
IDSRunCycle = get_widget_value('IDSRunCycle','')
SrcSysCd = get_widget_value('SrcSysCd','')
RunID = get_widget_value('RunID','')

# Get JDBC configuration
jdbc_url, jdbc_props = get_db_config(ids_secret_name)

# --------------------------------------------------------------------------------
# Stage: db2_K_MBR_PDX_DENIED_TRANS_in (DB2ConnectorPX) - READ from IDS
# --------------------------------------------------------------------------------
extract_query_db2_K_MBR_PDX_DENIED_TRANS_in = """
SELECT
MBR_UNIQ_KEY,
PDX_NTNL_PROV_ID,
TRANS_TYP_CD,
TRANS_DENIED_DT,
RX_NO,
PRCS_DT AS PRCS_DT,
SRC_SYS_CLM_RCVD_DT  AS SRC_SYS_CLM_RCVD_DT,
SRC_SYS_CLM_RCVD_TM AS SRC_SYS_CLM_RCVD_TM,
SRC_SYS_CD,
CRT_RUN_CYC_EXCTN_SK,
MBR_PDX_DENIED_TRANS_SK
FROM {owner}.K_MBR_PDX_DENIED_TRANS
""".format(owner=IDSOwner)

df_db2_K_MBR_PDX_DENIED_TRANS_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_K_MBR_PDX_DENIED_TRANS_in)
    .load()
)


# --------------------------------------------------------------------------------
# Stage: ds_MBR_PDX_DENIED_TRANS_xfm (PxDataSet) - READ from .ds => Parquet
# --------------------------------------------------------------------------------
df_ds_MBR_PDX_DENIED_TRANS_xfm = spark.read.parquet(
    f"{adls_path}/ds/MBR_PDX_DENIED_TRANS.{SrcSysCd}.xfrm.{RunID}.parquet"
)


# --------------------------------------------------------------------------------
# Stage: Cpy (PxCopy)
# --------------------------------------------------------------------------------
df_Cpy_in = df_ds_MBR_PDX_DENIED_TRANS_xfm

# Output link lnk_IdsMbr_PdxDeniedPkey_All (74 columns)
df_lnk_IdsMbr_PdxDeniedPkey_All = df_Cpy_in.select(
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("PDX_NTNL_PROV_ID").alias("PDX_NTNL_PROV_ID"),
    F.col("TRANS_TYP_CD").alias("TRANS_TYP_CD"),
    F.col("TRANS_DENIED_DT").alias("TRANS_DENIED_DT"),
    F.col("RX_NO").alias("RX_NO"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("GRP_SK").alias("GRP_SK"),
    F.col("MBR_SK").alias("MBR_SK"),
    F.col("NDC_SK").alias("NDC_SK"),
    F.col("PRSCRB_PROV_SK").alias("PRSCRB_PROV_SK"),
    F.col("PROV_SPEC_CD_SK").alias("PROV_SPEC_CD_SK"),
    F.col("SRV_PROV_SK").alias("SRV_PROV_SK"),
    F.col("SUB_SK").alias("SUB_SK"),
    F.col("MEDIA_TYP_CD_SK").alias("MEDIA_TYP_CD_SK"),
    F.col("MBR_GNDR_CD_SK").alias("MBR_GNDR_CD_SK"),
    F.col("MBR_RELSHP_CD_SK").alias("MBR_RELSHP_CD_SK"),
    F.col("PDX_RSPN_RSN_CD_SK").alias("PDX_RSPN_RSN_CD_SK"),
    F.col("PDX_RSPN_TYP_CD_SK").alias("PDX_RSPN_TYP_CD_SK"),
    F.col("TRANS_TYP_CD_SK").alias("TRANS_TYP_CD_SK"),
    F.col("MAIL_ORDER_IN").alias("MAIL_ORDER_IN"),
    F.col("MBR_BRTH_DT").alias("MBR_BRTH_DT"),
    F.col("SRC_SYS_CLM_RCVD_DT").alias("SRC_SYS_CLM_RCVD_DT"),
    F.col("SRC_SYS_CLM_RCVD_TM").alias("SRC_SYS_CLM_RCVD_TM"),
    F.col("SUB_BRTH_DT").alias("SUB_BRTH_DT"),
    F.col("BILL_RX_DISPENSE_FEE_AMT").alias("BILL_RX_DISPENSE_FEE_AMT"),
    F.col("BILL_RX_GROS_APRV_AMT").alias("BILL_RX_GROS_APRV_AMT"),
    F.col("BILL_RX_NET_CHK_AMT").alias("BILL_RX_NET_CHK_AMT"),
    F.col("BILL_RX_PATN_PAY_AMT").alias("BILL_RX_PATN_PAY_AMT"),
    F.col("INGR_CST_ALW_AMT").alias("INGR_CST_ALW_AMT"),
    F.col("TRANS_MO_NO").alias("TRANS_MO_NO"),
    F.col("TRANS_YR_NO").alias("TRANS_YR_NO"),
    F.col("MBR_ID").alias("MBR_ID"),
    F.col("MBR_SFX_NO").alias("MBR_SFX_NO"),
    F.col("MBR_FIRST_NM").alias("MBR_FIRST_NM"),
    F.col("MBR_LAST_NM").alias("MBR_LAST_NM"),
    F.col("MBR_SSN").alias("MBR_SSN"),
    F.col("PDX_NM").alias("PDX_NM"),
    F.col("PDX_PHN_NO").alias("PDX_PHN_NO"),
    F.col("PHYS_DEA_NO").alias("PHYS_DEA_NO"),
    F.col("PHYS_NTNL_PROV_ID").alias("PHYS_NTNL_PROV_ID"),
    F.col("PHYS_FIRST_NM").alias("PHYS_FIRST_NM"),
    F.col("PHYS_LAST_NM").alias("PHYS_LAST_NM"),
    F.col("PHYS_ST_ADDR_LN").alias("PHYS_ST_ADDR_LN"),
    F.col("PHYS_CITY_NM").alias("PHYS_CITY_NM"),
    F.col("PHYS_ST_CD").alias("PHYS_ST_CD"),
    F.col("PHYS_POSTAL_CD").alias("PHYS_POSTAL_CD"),
    F.col("RX_LABEL_TX").alias("RX_LABEL_TX"),
    F.col("SVC_PROV_NABP_NM").alias("SVC_PROV_NABP_NM"),
    F.col("SRC_SYS_CAR_ID").alias("SRC_SYS_CAR_ID"),
    F.col("SRC_SYS_CLNT_ORG_ID").alias("SRC_SYS_CLNT_ORG_ID"),
    F.col("SRC_SYS_CLNT_ORG_NM").alias("SRC_SYS_CLNT_ORG_NM"),
    F.col("SRC_SYS_CNTR_ID").alias("SRC_SYS_CNTR_ID"),
    F.col("SRC_SYS_GRP_ID").alias("SRC_SYS_GRP_ID"),
    F.col("SUB_ID").alias("SUB_ID"),
    F.col("SUB_FIRST_NM").alias("SUB_FIRST_NM"),
    F.col("SUB_LAST_NM").alias("SUB_LAST_NM"),
    F.col("CLNT_ELIG_MBRSH_ID").alias("CLNT_ELIG_MBRSH_ID"),
    F.col("PRSN_NO").alias("PRSN_NO"),
    F.col("RELSHP_CD").alias("RELSHP_CD"),
    F.col("GNDR_CD").alias("GNDR_CD"),
    F.col("CLM_RCVD_TM").alias("CLM_RCVD_TM"),
    F.col("MSG_TYP_CD").alias("MSG_TYP_CD"),
    F.col("MSG_TYP").alias("MSG_TYP"),
    F.col("MSG_TX").alias("MSG_TX"),
    F.col("CLNT_MBRSH_ID").alias("CLNT_MBRSH_ID"),
    F.col("MEDIA_TYP_CD").alias("MEDIA_TYP_CD"),
    F.col("RSPN_CD").alias("RSPN_CD"),
    F.col("DESC").alias("DESC"),
    F.col("CHAPTER_ID").alias("CHAPTER_ID"),
    F.col("CHAPTER_DESC").alias("CHAPTER_DESC"),
    F.col("PDX_NPI").alias("PDX_NPI"),
    F.col("NDC").alias("NDC")
)

# Output link lnk_FctsIdsMbr_PdxDeniedPkey_dedup (9 columns)
df_lnk_FctsIdsMbr_PdxDeniedPkey_dedup = df_Cpy_in.select(
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("PDX_NTNL_PROV_ID").alias("PDX_NTNL_PROV_ID"),
    F.col("TRANS_TYP_CD").alias("TRANS_TYP_CD"),
    F.col("TRANS_DENIED_DT").alias("TRANS_DENIED_DT"),
    F.col("RX_NO").alias("RX_NO"),
    F.col("PRCS_DT").alias("PRCS_DT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("SRC_SYS_CLM_RCVD_DT").alias("SRC_SYS_CLM_RCVD_DT"),
    F.col("SRC_SYS_CLM_RCVD_TM").alias("SRC_SYS_CLM_RCVD_TM")
)


# --------------------------------------------------------------------------------
# Stage: rdup_Natural_Keys (PxRemDup) - deduplicate
# --------------------------------------------------------------------------------
df_rdup_Natural_Keys_temp = dedup_sort(
    df_lnk_FctsIdsMbr_PdxDeniedPkey_dedup,
    partition_cols=[
        "MBR_UNIQ_KEY",
        "PDX_NTNL_PROV_ID",
        "TRANS_TYP_CD",
        "TRANS_DENIED_DT",
        "RX_NO",
        "PRCS_DT",
        "SRC_SYS_CLM_RCVD_DT",
        "SRC_SYS_CLM_RCVD_TM",
        "SRC_SYS_CD"
    ],
    sort_cols=[]
)

df_lnk_Natural_Keys_out = df_rdup_Natural_Keys_temp.select(
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("PDX_NTNL_PROV_ID").alias("PDX_NTNL_PROV_ID"),
    F.col("TRANS_TYP_CD").alias("TRANS_TYP_CD"),
    F.col("TRANS_DENIED_DT").alias("TRANS_DENIED_DT"),
    F.col("RX_NO").alias("RX_NO"),
    F.col("PRCS_DT").alias("PRCS_DT"),
    F.col("SRC_SYS_CLM_RCVD_DT").alias("SRC_SYS_CLM_RCVD_DT"),
    F.col("SRC_SYS_CLM_RCVD_TM").alias("SRC_SYS_CLM_RCVD_TM"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD")
)


# --------------------------------------------------------------------------------
# Stage: jn_Mbr_Pdx (PxJoin) - left outer join
# --------------------------------------------------------------------------------
join_keys_jn_Mbr_Pdx = [
    "MBR_UNIQ_KEY",
    "PDX_NTNL_PROV_ID",
    "TRANS_TYP_CD",
    "TRANS_DENIED_DT",
    "RX_NO",
    "SRC_SYS_CD",
    "PRCS_DT",
    "SRC_SYS_CLM_RCVD_DT",
    "SRC_SYS_CLM_RCVD_TM"
]

df_jn_Mbr_Pdx = (
    df_lnk_Natural_Keys_out.alias("lnk_Natural_Keys_out")
    .join(
        df_db2_K_MBR_PDX_DENIED_TRANS_in.alias("lnk_KMbrPdxDeniedTransPkey_extr"),
        on=join_keys_jn_Mbr_Pdx,
        how='left'
    )
    .select(
        F.col("lnk_Natural_Keys_out.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        F.col("lnk_Natural_Keys_out.PDX_NTNL_PROV_ID").alias("PDX_NTNL_PROV_ID"),
        F.col("lnk_Natural_Keys_out.TRANS_TYP_CD").alias("TRANS_TYP_CD"),
        F.col("lnk_Natural_Keys_out.TRANS_DENIED_DT").alias("TRANS_DENIED_DT"),
        F.col("lnk_Natural_Keys_out.RX_NO").alias("RX_NO"),
        F.col("lnk_Natural_Keys_out.PRCS_DT").alias("PRCS_DT"),
        F.col("lnk_Natural_Keys_out.SRC_SYS_CLM_RCVD_DT").alias("SRC_SYS_CLM_RCVD_DT"),
        F.col("lnk_Natural_Keys_out.SRC_SYS_CLM_RCVD_TM").alias("SRC_SYS_CLM_RCVD_TM"),
        F.col("lnk_Natural_Keys_out.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("lnk_KMbrPdxDeniedTransPkey_extr.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("lnk_KMbrPdxDeniedTransPkey_extr.MBR_PDX_DENIED_TRANS_SK").alias("MBR_PDX_DENIED_TRANS_SK")
    )
)


# --------------------------------------------------------------------------------
# Stage: xfrm_PKEYgen (CTransformerStage)
# --------------------------------------------------------------------------------
df_xfrm_PKEYgen_in = df_jn_Mbr_Pdx

# Separate new records vs existing records based on MBR_PDX_DENIED_TRANS_SK
df_new = df_xfrm_PKEYgen_in.filter(
    (F.col("MBR_PDX_DENIED_TRANS_SK").isNull()) | (F.col("MBR_PDX_DENIED_TRANS_SK") == 0)
)
df_existing = df_xfrm_PKEYgen_in.filter(
    ~((F.col("MBR_PDX_DENIED_TRANS_SK").isNull()) | (F.col("MBR_PDX_DENIED_TRANS_SK") == 0))
)

# For new records, set the SK column to None so SurrogateKeyGen fills it
df_new_temp = df_new.withColumn("MBR_PDX_DENIED_TRANS_SK", F.lit(None).cast(StringType()))
df_new_temp = SurrogateKeyGen(
    df_new_temp,
    <DB sequence name>,
    "MBR_PDX_DENIED_TRANS_SK",
    <schema>,
    <secret_name>
)

# Output link lnk_KMbrPdxDeniedTrans_new: columns with transforms
df_lnk_KMbrPdxDeniedTrans_new = df_new_temp.select(
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("PDX_NTNL_PROV_ID").alias("PDX_NTNL_PROV_ID"),
    F.col("TRANS_TYP_CD").alias("TRANS_TYP_CD"),
    F.col("TRANS_DENIED_DT").alias("TRANS_DENIED_DT"),
    F.col("RX_NO").alias("RX_NO"),
    current_date().alias("PRCS_DT"),  # from xfrm
    F.col("SRC_SYS_CLM_RCVD_DT").alias("SRC_SYS_CLM_RCVD_DT"),
    F.col("SRC_SYS_CLM_RCVD_TM").alias("SRC_SYS_CLM_RCVD_TM"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.lit(IDSRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("MBR_PDX_DENIED_TRANS_SK").alias("MBR_PDX_DENIED_TRANS_SK")
)

# Output link lnk_Pkey_out: all rows, but with transformations
# Per expression:
#  PRCS_DT => if MBR_PDX_DENIED_TRANS_SK is null/0 => current_date() else keep PRCS_DT
#  CRT_RUN_CYC_EXCTN_SK => if MBR_PDX_DENIED_TRANS_SK is null/0 => IDSRunCycle else keep old
#  LAST_UPDT_RUN_CYC_EXCTN_SK => IDSRunCycle always
df_lnk_Pkey_out_all = df_xfrm_PKEYgen_in.withColumn(
    "temp_is_new",
    F.when(
        (F.col("MBR_PDX_DENIED_TRANS_SK").isNull()) | (F.col("MBR_PDX_DENIED_TRANS_SK") == 0),
        F.lit(True)
    ).otherwise(F.lit(False))
).withColumn(
    "PRCS_DT",
    F.when(F.col("temp_is_new"), current_date()).otherwise(F.col("PRCS_DT"))
).withColumn(
    "CRT_RUN_CYC_EXCTN_SK",
    F.when(F.col("temp_is_new"), F.lit(IDSRunCycle)).otherwise(F.col("CRT_RUN_CYC_EXCTN_SK"))
).withColumn(
    "MBR_PDX_DENIED_TRANS_SK",
    F.when(
        (F.col("MBR_PDX_DENIED_TRANS_SK").isNull()) | (F.col("MBR_PDX_DENIED_TRANS_SK") == 0),
        F.lit(None).cast(StringType())
    ).otherwise(F.col("MBR_PDX_DENIED_TRANS_SK"))
)

df_lnk_Pkey_out_all = SurrogateKeyGen(
    df_lnk_Pkey_out_all,
    <DB sequence name>,
    "MBR_PDX_DENIED_TRANS_SK",
    <schema>,
    <secret_name>
)

df_lnk_Pkey_out = df_lnk_Pkey_out_all.withColumn(
    "LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(IDSRunCycle)
).select(
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("PDX_NTNL_PROV_ID").alias("PDX_NTNL_PROV_ID"),
    F.col("TRANS_TYP_CD").alias("TRANS_TYP_CD"),
    F.col("TRANS_DENIED_DT").alias("TRANS_DENIED_DT"),
    F.col("RX_NO").alias("RX_NO"),
    F.col("PRCS_DT").alias("PRCS_DT"),
    F.col("SRC_SYS_CLM_RCVD_DT").alias("SRC_SYS_CLM_RCVD_DT"),
    F.col("SRC_SYS_CLM_RCVD_TM").alias("SRC_SYS_CLM_RCVD_TM"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("MBR_PDX_DENIED_TRANS_SK").alias("MBR_PDX_DENIED_TRANS_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)


# --------------------------------------------------------------------------------
# Stage: db2_K_MBR_PDX_DENIED_TRANS_load (DB2ConnectorPX) - WRITE to IDS
#   (Insert-only logic)
# --------------------------------------------------------------------------------
df_db2_K_MBR_PDX_DENIED_TRANS_load = df_lnk_KMbrPdxDeniedTrans_new

# Create a staging table
temp_table_name = "STAGING.IdsMbrPdxDeniedClmPkey_db2_K_MBR_PDX_DENIED_TRANS_load_temp"
execute_dml(f"DROP TABLE IF EXISTS {temp_table_name}", jdbc_url, jdbc_props)

(
    df_db2_K_MBR_PDX_DENIED_TRANS_load
    .write
    .format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("dbtable", temp_table_name)
    .mode("overwrite")
    .save()
)

all_cols_load = [
    "MBR_UNIQ_KEY",
    "PDX_NTNL_PROV_ID",
    "TRANS_TYP_CD",
    "TRANS_DENIED_DT",
    "RX_NO",
    "PRCS_DT",
    "SRC_SYS_CLM_RCVD_DT",
    "SRC_SYS_CLM_RCVD_TM",
    "SRC_SYS_CD",
    "CRT_RUN_CYC_EXCTN_SK",
    "MBR_PDX_DENIED_TRANS_SK"
]

merge_sql_load = f"""
MERGE INTO {IDSOwner}.K_MBR_PDX_DENIED_TRANS AS T
USING {temp_table_name} AS S
ON (
   T.MBR_UNIQ_KEY = S.MBR_UNIQ_KEY
   AND T.PDX_NTNL_PROV_ID = S.PDX_NTNL_PROV_ID
   AND T.TRANS_TYP_CD = S.TRANS_TYP_CD
   AND T.TRANS_DENIED_DT = S.TRANS_DENIED_DT
   AND T.RX_NO = S.RX_NO
   AND T.PRCS_DT = S.PRCS_DT
   AND T.SRC_SYS_CLM_RCVD_DT = S.SRC_SYS_CLM_RCVD_DT
   AND T.SRC_SYS_CLM_RCVD_TM = S.SRC_SYS_CLM_RCVD_TM
   AND T.SRC_SYS_CD = S.SRC_SYS_CD
)
WHEN NOT MATCHED THEN
INSERT (
   MBR_UNIQ_KEY,
   PDX_NTNL_PROV_ID,
   TRANS_TYP_CD,
   TRANS_DENIED_DT,
   RX_NO,
   PRCS_DT,
   SRC_SYS_CLM_RCVD_DT,
   SRC_SYS_CLM_RCVD_TM,
   SRC_SYS_CD,
   CRT_RUN_CYC_EXCTN_SK,
   MBR_PDX_DENIED_TRANS_SK
)
VALUES (
   S.MBR_UNIQ_KEY,
   S.PDX_NTNL_PROV_ID,
   S.TRANS_TYP_CD,
   S.TRANS_DENIED_DT,
   S.RX_NO,
   S.PRCS_DT,
   S.SRC_SYS_CLM_RCVD_DT,
   S.SRC_SYS_CLM_RCVD_TM,
   S.SRC_SYS_CD,
   S.CRT_RUN_CYC_EXCTN_SK,
   S.MBR_PDX_DENIED_TRANS_SK
);
"""
execute_dml(merge_sql_load, jdbc_url, jdbc_props)


# --------------------------------------------------------------------------------
# Stage: jn_PKey (PxJoin) - inner join
# --------------------------------------------------------------------------------
join_keys_jn_PKey = [
    "MBR_UNIQ_KEY",
    "PDX_NTNL_PROV_ID",
    "TRANS_TYP_CD",
    "TRANS_DENIED_DT",
    "RX_NO",
    "SRC_SYS_CLM_RCVD_DT",
    "SRC_SYS_CLM_RCVD_TM",
    "SRC_SYS_CD"
]

df_jn_PKey = (
    df_lnk_Pkey_out.alias("lnk_Pkey_out")
    .join(
        df_lnk_IdsMbr_PdxDeniedPkey_All.alias("lnk_IdsMbr_PdxDeniedPkey_All"),
        on=join_keys_jn_PKey,
        how='inner'
    )
    .select(
        F.col("lnk_Pkey_out.MBR_PDX_DENIED_TRANS_SK").alias("MBR_PDX_DENIED_TRANS_SK"),
        F.col("lnk_Pkey_out.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        F.col("lnk_Pkey_out.PDX_NTNL_PROV_ID").alias("PDX_NTNL_PROV_ID"),
        F.col("lnk_Pkey_out.TRANS_TYP_CD").alias("TRANS_TYP_CD"),
        F.col("lnk_Pkey_out.TRANS_DENIED_DT").alias("TRANS_DENIED_DT"),
        F.col("lnk_Pkey_out.RX_NO").alias("RX_NO"),
        F.col("lnk_Pkey_out.PRCS_DT").alias("PRCS_DT"),
        F.col("lnk_Pkey_out.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("lnk_Pkey_out.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("lnk_Pkey_out.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("lnk_IdsMbr_PdxDeniedPkey_All.GRP_SK").alias("GRP_SK"),
        F.col("lnk_IdsMbr_PdxDeniedPkey_All.MBR_SK").alias("MBR_SK"),
        F.col("lnk_IdsMbr_PdxDeniedPkey_All.NDC_SK").alias("NDC_SK"),
        F.col("lnk_IdsMbr_PdxDeniedPkey_All.PRSCRB_PROV_SK").alias("PRSCRB_PROV_SK"),
        F.col("lnk_IdsMbr_PdxDeniedPkey_All.PROV_SPEC_CD_SK").alias("PROV_SPEC_CD_SK"),
        F.col("lnk_IdsMbr_PdxDeniedPkey_All.SRV_PROV_SK").alias("SRV_PROV_SK"),
        F.col("lnk_IdsMbr_PdxDeniedPkey_All.SUB_SK").alias("SUB_SK"),
        F.col("lnk_IdsMbr_PdxDeniedPkey_All.MEDIA_TYP_CD_SK").alias("MEDIA_TYP_CD_SK"),
        F.col("lnk_IdsMbr_PdxDeniedPkey_All.MBR_GNDR_CD_SK").alias("MBR_GNDR_CD_SK"),
        F.col("lnk_IdsMbr_PdxDeniedPkey_All.MBR_RELSHP_CD_SK").alias("MBR_RELSHP_CD_SK"),
        F.col("lnk_IdsMbr_PdxDeniedPkey_All.PDX_RSPN_RSN_CD_SK").alias("PDX_RSPN_RSN_CD_SK"),
        F.col("lnk_IdsMbr_PdxDeniedPkey_All.PDX_RSPN_TYP_CD_SK").alias("PDX_RSPN_TYP_CD_SK"),
        F.col("lnk_IdsMbr_PdxDeniedPkey_All.TRANS_TYP_CD_SK").alias("TRANS_TYP_CD_SK"),
        F.col("lnk_IdsMbr_PdxDeniedPkey_All.MAIL_ORDER_IN").alias("MAIL_ORDER_IN"),
        F.col("lnk_IdsMbr_PdxDeniedPkey_All.MBR_BRTH_DT").alias("MBR_BRTH_DT"),
        F.col("lnk_Pkey_out.SRC_SYS_CLM_RCVD_DT").alias("SRC_SYS_CLM_RCVD_DT"),
        F.col("lnk_Pkey_out.SRC_SYS_CLM_RCVD_TM").alias("SRC_SYS_CLM_RCVD_TM"),
        F.col("lnk_IdsMbr_PdxDeniedPkey_All.SUB_BRTH_DT").alias("SUB_BRTH_DT"),
        F.col("lnk_IdsMbr_PdxDeniedPkey_All.BILL_RX_DISPENSE_FEE_AMT").alias("BILL_RX_DISPENSE_FEE_AMT"),
        F.col("lnk_IdsMbr_PdxDeniedPkey_All.BILL_RX_GROS_APRV_AMT").alias("BILL_RX_GROS_APRV_AMT"),
        F.col("lnk_IdsMbr_PdxDeniedPkey_All.BILL_RX_NET_CHK_AMT").alias("BILL_RX_NET_CHK_AMT"),
        F.col("lnk_IdsMbr_PdxDeniedPkey_All.BILL_RX_PATN_PAY_AMT").alias("BILL_RX_PATN_PAY_AMT"),
        F.col("lnk_IdsMbr_PdxDeniedPkey_All.INGR_CST_ALW_AMT").alias("INGR_CST_ALW_AMT"),
        F.col("lnk_IdsMbr_PdxDeniedPkey_All.TRANS_MO_NO").alias("TRANS_MO_NO"),
        F.col("lnk_IdsMbr_PdxDeniedPkey_All.TRANS_YR_NO").alias("TRANS_YR_NO"),
        F.col("lnk_IdsMbr_PdxDeniedPkey_All.MBR_ID").alias("MBR_ID"),
        F.col("lnk_IdsMbr_PdxDeniedPkey_All.MBR_SFX_NO").alias("MBR_SFX_NO"),
        F.col("lnk_IdsMbr_PdxDeniedPkey_All.MBR_FIRST_NM").alias("MBR_FIRST_NM"),
        F.col("lnk_IdsMbr_PdxDeniedPkey_All.MBR_LAST_NM").alias("MBR_LAST_NM"),
        F.col("lnk_IdsMbr_PdxDeniedPkey_All.MBR_SSN").alias("MBR_SSN"),
        F.col("lnk_IdsMbr_PdxDeniedPkey_All.PDX_NM").alias("PDX_NM"),
        F.col("lnk_IdsMbr_PdxDeniedPkey_All.PDX_PHN_NO").alias("PDX_PHN_NO"),
        F.col("lnk_IdsMbr_PdxDeniedPkey_All.PHYS_DEA_NO").alias("PHYS_DEA_NO"),
        F.col("lnk_IdsMbr_PdxDeniedPkey_All.PHYS_NTNL_PROV_ID").alias("PHYS_NTNL_PROV_ID"),
        F.col("lnk_IdsMbr_PdxDeniedPkey_All.PHYS_FIRST_NM").alias("PHYS_FIRST_NM"),
        F.col("lnk_IdsMbr_PdxDeniedPkey_All.PHYS_LAST_NM").alias("PHYS_LAST_NM"),
        F.col("lnk_IdsMbr_PdxDeniedPkey_All.PHYS_ST_ADDR_LN").alias("PHYS_ST_ADDR_LN"),
        F.col("lnk_IdsMbr_PdxDeniedPkey_All.PHYS_CITY_NM").alias("PHYS_CITY_NM"),
        F.col("lnk_IdsMbr_PdxDeniedPkey_All.PHYS_ST_CD").alias("PHYS_ST_CD"),
        F.col("lnk_IdsMbr_PdxDeniedPkey_All.PHYS_POSTAL_CD").alias("PHYS_POSTAL_CD"),
        F.col("lnk_IdsMbr_PdxDeniedPkey_All.RX_LABEL_TX").alias("RX_LABEL_TX"),
        F.col("lnk_IdsMbr_PdxDeniedPkey_All.SVC_PROV_NABP_NM").alias("SVC_PROV_NABP_NM"),
        F.col("lnk_IdsMbr_PdxDeniedPkey_All.SRC_SYS_CAR_ID").alias("SRC_SYS_CAR_ID"),
        F.col("lnk_IdsMbr_PdxDeniedPkey_All.SRC_SYS_CLNT_ORG_ID").alias("SRC_SYS_CLNT_ORG_ID"),
        F.col("lnk_IdsMbr_PdxDeniedPkey_All.SRC_SYS_CLNT_ORG_NM").alias("SRC_SYS_CLNT_ORG_NM"),
        F.col("lnk_IdsMbr_PdxDeniedPkey_All.SRC_SYS_CNTR_ID").alias("SRC_SYS_CNTR_ID"),
        F.col("lnk_IdsMbr_PdxDeniedPkey_All.SRC_SYS_GRP_ID").alias("SRC_SYS_GRP_ID"),
        F.col("lnk_IdsMbr_PdxDeniedPkey_All.SUB_ID").alias("SUB_ID"),
        F.col("lnk_IdsMbr_PdxDeniedPkey_All.SUB_FIRST_NM").alias("SUB_FIRST_NM"),
        F.col("lnk_IdsMbr_PdxDeniedPkey_All.SUB_LAST_NM").alias("SUB_LAST_NM"),
        F.col("lnk_IdsMbr_PdxDeniedPkey_All.CLNT_ELIG_MBRSH_ID").alias("CLNT_ELIG_MBRSH_ID"),
        F.col("lnk_IdsMbr_PdxDeniedPkey_All.PRSN_NO").alias("PRSN_NO"),
        F.col("lnk_IdsMbr_PdxDeniedPkey_All.RELSHP_CD").alias("RELSHP_CD"),
        F.col("lnk_IdsMbr_PdxDeniedPkey_All.GNDR_CD").alias("GNDR_CD"),
        F.col("lnk_IdsMbr_PdxDeniedPkey_All.CLM_RCVD_TM").alias("CLM_RCVD_TM"),
        F.col("lnk_IdsMbr_PdxDeniedPkey_All.MSG_TYP_CD").alias("MSG_TYP_CD"),
        F.col("lnk_IdsMbr_PdxDeniedPkey_All.MSG_TYP").alias("MSG_TYP"),
        F.col("lnk_IdsMbr_PdxDeniedPkey_All.MSG_TX").alias("MSG_TX"),
        F.col("lnk_IdsMbr_PdxDeniedPkey_All.CLNT_MBRSH_ID").alias("CLNT_MBRSH_ID"),
        F.col("lnk_IdsMbr_PdxDeniedPkey_All.MEDIA_TYP_CD").alias("MEDIA_TYP_CD"),
        F.col("lnk_IdsMbr_PdxDeniedPkey_All.RSPN_CD").alias("RSPN_CD"),
        F.col("lnk_IdsMbr_PdxDeniedPkey_All.DESC").alias("DESC"),
        F.col("lnk_IdsMbr_PdxDeniedPkey_All.CHAPTER_ID").alias("CHAPTER_ID"),
        F.col("lnk_IdsMbr_PdxDeniedPkey_All.CHAPTER_DESC").alias("CHAPTER_DESC"),
        F.col("lnk_IdsMbr_PdxDeniedPkey_All.PDX_NPI").alias("PDX_NPI"),
        F.col("lnk_IdsMbr_PdxDeniedPkey_All.NDC").alias("NDC")
    )
)

# --------------------------------------------------------------------------------
# Stage: seq_MBR_PDX_DENIED_TRANS_Pkey (PxSequentialFile) - WRITE delimited
# --------------------------------------------------------------------------------
# Apply rpad for char columns (MAIL_ORDER_IN length=1, MBR_SFX_NO length=2)
df_seq_MBR_PDX_DENIED_TRANS_Pkey = df_jn_PKey.select(
    F.col("MBR_PDX_DENIED_TRANS_SK"),
    F.col("MBR_UNIQ_KEY"),
    F.col("PDX_NTNL_PROV_ID"),
    F.col("TRANS_TYP_CD"),
    F.col("TRANS_DENIED_DT"),
    F.col("RX_NO"),
    F.col("PRCS_DT"),
    F.col("SRC_SYS_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("GRP_SK"),
    F.col("MBR_SK"),
    F.col("NDC_SK"),
    F.col("PRSCRB_PROV_SK"),
    F.col("PROV_SPEC_CD_SK"),
    F.col("SRV_PROV_SK"),
    F.col("SUB_SK"),
    F.col("MEDIA_TYP_CD_SK"),
    F.col("MBR_GNDR_CD_SK"),
    F.col("MBR_RELSHP_CD_SK"),
    F.col("PDX_RSPN_RSN_CD_SK"),
    F.col("PDX_RSPN_TYP_CD_SK"),
    F.col("TRANS_TYP_CD_SK"),
    F.rpad(F.col("MAIL_ORDER_IN"), 1, " ").alias("MAIL_ORDER_IN"),
    F.col("MBR_BRTH_DT"),
    F.col("SRC_SYS_CLM_RCVD_DT"),
    F.col("SRC_SYS_CLM_RCVD_TM"),
    F.col("SUB_BRTH_DT"),
    F.col("BILL_RX_DISPENSE_FEE_AMT"),
    F.col("BILL_RX_GROS_APRV_AMT"),
    F.col("BILL_RX_NET_CHK_AMT"),
    F.col("BILL_RX_PATN_PAY_AMT"),
    F.col("INGR_CST_ALW_AMT"),
    F.col("TRANS_MO_NO"),
    F.col("TRANS_YR_NO"),
    F.col("MBR_ID"),
    F.rpad(F.col("MBR_SFX_NO"), 2, " ").alias("MBR_SFX_NO"),
    F.col("MBR_FIRST_NM"),
    F.col("MBR_LAST_NM"),
    F.col("MBR_SSN"),
    F.col("PDX_NM"),
    F.col("PDX_PHN_NO"),
    F.col("PHYS_DEA_NO"),
    F.col("PHYS_NTNL_PROV_ID"),
    F.col("PHYS_FIRST_NM"),
    F.col("PHYS_LAST_NM"),
    F.col("PHYS_ST_ADDR_LN"),
    F.col("PHYS_CITY_NM"),
    F.col("PHYS_ST_CD"),
    F.col("PHYS_POSTAL_CD"),
    F.col("RX_LABEL_TX"),
    F.col("SVC_PROV_NABP_NM"),
    F.col("SRC_SYS_CAR_ID"),
    F.col("SRC_SYS_CLNT_ORG_ID"),
    F.col("SRC_SYS_CLNT_ORG_NM"),
    F.col("SRC_SYS_CNTR_ID"),
    F.col("SRC_SYS_GRP_ID"),
    F.col("SUB_ID"),
    F.col("SUB_FIRST_NM"),
    F.col("SUB_LAST_NM"),
    F.col("CLNT_ELIG_MBRSH_ID"),
    F.col("PRSN_NO"),
    F.col("RELSHP_CD"),
    F.col("GNDR_CD"),
    F.col("CLM_RCVD_TM"),
    F.col("MSG_TYP_CD"),
    F.col("MSG_TYP"),
    F.col("MSG_TX"),
    F.col("CLNT_MBRSH_ID"),
    F.col("MEDIA_TYP_CD"),
    F.col("RSPN_CD"),
    F.col("DESC"),
    F.col("CHAPTER_ID"),
    F.col("CHAPTER_DESC"),
    F.col("PDX_NPI"),
    F.col("NDC")
)

write_files(
    df_seq_MBR_PDX_DENIED_TRANS_Pkey,
    f"{adls_path}/key/MBR_PDX_DENIED_TRANS.{SrcSysCd}.pkey.{RunID}.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)