# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2019 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:  OPTUMRX  Claims denied data is extracted from source file and loaded into the  MBR_PDX_DENIED_TRANS Table
# MAGIC 
# MAGIC 
# MAGIC Called By: OptumIdsMbrPdxDeniedTransLoadSeq
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                 Date                  Project/Altiris #                                                     Change Description                                                      Development Project           Code Reviewer                
# MAGIC ------------------              --------------------     ------------------------                                                    --------------------------------------------------------------                      --------------------------------------         --------------------------------
# MAGIC Peter Gichiri              2019-10-27      6131 - PBM REPLACEMENT  to OPTUMRX     Original Devlopment Integrate                                       IntegrateDev2                         Kalyan Neelam        2019-11-21
# MAGIC 
# MAGIC Rekha Radhakrishna 2020-10-12  Phase II Government Programs                             TRANS_MO_NO derivation changed                           IntegrateDev2
# MAGIC                                                                                                                                       to show two digit month
# MAGIC Rekha Radhakrishna 2020-11-05 Phase II Government Programs                               Changed Member Matching logic for Med-D                IntegrateDev2                        Kalyan Neelam         2020-12-10

# MAGIC Business Logic appied in this xfm
# MAGIC Remove Duplicates based on natural Keys
# MAGIC Job Name: OptumIdsMbrPdxDeniedTransExtr
# MAGIC 
# MAGIC Description: OptumRxClaims denied data is extracted from source file and loaded into the  MBR_PDX_DENIED_TRANS Table
# MAGIC Triggered from: OptumIdsMbrPdxDeniedTransLoadSeq
# MAGIC Read data from the OPTUMRX Denied file from OptumClmLandExtr job
# MAGIC Loads data into MBR_PDX_DENIED_TRANS dataset.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DecimalType,
    IntegerType,
    DateType
)
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
IDSRunCycle = get_widget_value('IDSRunCycle','')
SrcSysCd = get_widget_value('SrcSysCd','')
RunID = get_widget_value('RunID','')
CurrDate = get_widget_value('CurrDate','')

#--------------------------------------------------------------------------------
# Stage: OptumRxDeniedInputFile (PxSequentialFile)
#--------------------------------------------------------------------------------
schema_OptumRxDeniedInputFile = StructType([
    StructField("MEMBERID", StringType(), True),
    StructField("ACCOUNTID", StringType(), True),
    StructField("NPIPROV", StringType(), True),
    StructField("DENIALDTE", StringType(), True),
    StructField("RXNUMBER", StringType(), True),
    StructField("DATESBM", DateType(), True),
    StructField("TIMESBM", StringType(), True),
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("MBRSEX", StringType(), True),
    StructField("MBRRELCDE", StringType(), True),
    StructField("REJCDE1", StringType(), True),
    StructField("REJCDE2", StringType(), True),
    StructField("RXNETWRKQL", StringType(), True),
    StructField("MBRBIRTH", DateType(), True),
    StructField("PHRDISPFEE", DecimalType(38,10), True),
    StructField("CLTDUEAMT", DecimalType(38,10), True),
    StructField("CLT2DUEAMT", DecimalType(38,10), True),
    StructField("CLTPATPAY", DecimalType(38,10), True),
    StructField("CLTINGRCST", DecimalType(38,10), True),
    StructField("MBRFSTNME", StringType(), True),
    StructField("MBRLSTNME", StringType(), True),
    StructField("SOCSECNBR", StringType(), True),
    StructField("SRVPROVNME", StringType(), True),
    StructField("PRESCDEAID", StringType(), True),
    StructField("NPIPRESCR", StringType(), True),
    StructField("PPRSFSTNME", StringType(), True),
    StructField("PPRSLSTNME", StringType(), True),
    StructField("PPRSTATE", StringType(), True),
    StructField("LABELNAME", StringType(), True),
    StructField("SRVPROVID", StringType(), True),
    StructField("CARID", StringType(), True),
    StructField("GROUPID", StringType(), True),
    StructField("CLMORIGIN", StringType(), True),
    StructField("RXCLMNBR", DecimalType(38,10), True),
    StructField("PRODUCTID", StringType(), True),
    StructField("MBR_UNIQ_KEY", IntegerType(), False),
    StructField("GRP_ID", StringType(), False),
    StructField("LOB_IN", StringType(), False)
])

df_OptumRxDeniedInputFile = (
    spark.read.format("csv")
    .option("delimiter", ",")
    .option("quote", "^")
    .option("header", "false")
    .schema(schema_OptumRxDeniedInputFile)
    .load(f"{adls_path}/verified/{SrcSysCd}_DeniedClaims_Landing.dat.{RunID}")
)

#--------------------------------------------------------------------------------
# Stage: xfmDataClean (CTransformerStage)
#--------------------------------------------------------------------------------
df_xfmDataClean = df_OptumRxDeniedInputFile.select(
    F.lit("").alias("MBR_PDX_DENIED_TRANS_SK"),

    trim(F.col("MBR_UNIQ_KEY")).alias("MBR_UNIQ_KEY"),
    trim(F.col("ACCOUNTID")).alias("ACCOUNTID"),
    F.col("NPIPROV").alias("PDX_NTNL_PROV_ID"),
    F.lit("31").alias("TRANS_TYP_CD"),

    F.col("DATESBM").alias("TRANS_DENIED_DT"),
    F.col("RXNUMBER").alias("RX_NO"),
    F.lit(CurrDate).alias("PRCS_DT"),
    F.col("DATESBM").alias("SRC_SYS_CLM_RCVD_DT"),
    F.concat(
        F.col("TIMESBM").substr(F.lit(1), F.lit(2)),
        F.lit(":"),
        F.col("TIMESBM").substr(F.lit(3), F.lit(2)),
        F.lit(":"),
        F.col("TIMESBM").substr(F.lit(5), F.lit(2))
    ).alias("SRC_SYS_CLM_RCVD_TM"),

    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("GRP_SK"),
    F.lit(0).alias("MBR_SK"),
    F.lit(0).alias("NDC_SK"),
    F.lit(0).alias("PRSCRB_PROV_SK"),
    F.lit(0).alias("PROV_SPEC_CD_SK"),
    F.lit(0).alias("SRV_PROV_SK"),
    F.lit(0).alias("SUB_SK"),
    F.lit(0).alias("MEDIA_TYP_CD_SK"),

    F.col("MBRSEX").alias("MBR_GNDR_CD_SK"),
    F.col("MBRRELCDE").alias("MBR_RELSHP_CD_SK"),
    F.col("REJCDE1").alias("PDX_RSPN_RSN_CD_SK"),
    F.lit(0).alias("PDX_RSPN_TYP_CD_SK"),
    F.lit("").alias("TRANS_TYP_CD_SK"),
    F.col("RXNETWRKQL").alias("MAIL_ORDER_IN"),
    F.col("MBRBIRTH").alias("MBR_BRTH_DT"),
    F.col("PHRDISPFEE").alias("BILL_RX_DISPENSE_FEE_AMT"),
    F.col("CLTDUEAMT").alias("BILL_RX_GROS_APRV_AMT"),
    F.col("CLT2DUEAMT").alias("BILL_RX_NET_CHK_AMT"),
    F.col("CLTPATPAY").alias("BILL_RX_PATN_PAY_AMT"),
    F.col("CLTINGRCST").alias("INGR_CST_ALW_AMT"),
    F.lit(0).alias("TRANS_MO_NO"),
    F.lit(0).alias("TRANS_YR_NO"),

    # MBR_ID
    F.when(trim(F.col("LOB_IN")) == F.lit("MCARE"),
           trim(F.col("MEMBERID").substr(F.lit(4), 9999)).concat(F.lit("00")))
    .otherwise(trim("MEMBERID")).alias("MBR_ID"),

    # MBR_SFX_NO
    F.when(F.col("LOB_IN") == F.lit("MCARE"), F.lit("00"))
    .otherwise(F.col("MEMBERID").substr(F.lit(10), 2)).alias("MBR_SFX_NO"),

    F.col("MBRFSTNME").alias("MBR_FIRST_NM"),
    F.col("MBRLSTNME").alias("MBR_LAST_NM"),
    F.col("SOCSECNBR").alias("MBR_SSN"),
    F.col("SRVPROVNME").alias("PDX_NM"),
    F.lit("UNK").alias("PDX_PHN_NO"),

    F.when(F.col("PRESCDEAID") == F.lit("UNK"), F.lit("NA"))
    .otherwise(F.col("PRESCDEAID")).alias("PHYS_DEA_NO"),

    F.col("NPIPRESCR").alias("PHYS_NTNL_PROV_ID"),
    F.col("PPRSFSTNME").alias("PHYS_FIRST_NM"),
    F.col("PPRSLSTNME").alias("PHYS_LAST_NM"),
    F.lit("UNK").alias("PHYS_ST_ADDR_LN"),
    F.lit("UNK").alias("PHYS_CITY_NM"),
    F.col("PPRSTATE").alias("PHYS_ST_CD"),
    F.lit("UNK").alias("PHYS_POSTAL_CD"),

    F.col("LABELNAME").alias("RX_LABEL_TX"),
    trim(F.col("SRVPROVID")).alias("SVC_PROV_NABP_NM"),
    F.col("CARID").alias("SRC_SYS_CAR_ID"),
    F.lit("20165").alias("SRC_SYS_CLNT_ORG_ID"),
    F.lit("BCBS OF KANSAS CITY").alias("SRC_SYS_CLNT_ORG_NM"),
    F.col("MEMBERID").alias("SRC_SYS_CNTR_ID"),
    F.col("ACCOUNTID").alias("SRC_SYS_GRP_ID"),

    # SUB_ID
    F.when(trim(F.col("LOB_IN")) == F.lit("MCARE"),
           trim(F.col("MEMBERID").substr(F.lit(4), 9999)))
    .otherwise(trim("MEMBERID")).alias("SUB_ID"),

    F.lit("").alias("SUB_FIRST_NM"),
    F.lit("").alias("SUB_LAST_NM"),
    trim(F.col("PRODUCTID")).alias("NDC"),
    F.col("MEMBERID").alias("MEMBERID"),
    F.col("GRP_ID").alias("GRP_ID"),
    F.col("CLMORIGIN").alias("CLMORIGIN"),
    F.col("LOB_IN").alias("LOB_IN")
)

#--------------------------------------------------------------------------------
# Stage: xfm_optumrx (CTransformerStage) - Split into two outputs
#   1) Lnk_OptumRx_mbr_denied  (where all stagevars = 'N')
#   2) Lnk_reject              (where any stagevar = 'Y')
#--------------------------------------------------------------------------------

# Evaluate stage variables as columns:
cond_pdx_ntnl_prov_id_null_or_empty = ((F.col("PDX_NTNL_PROV_ID").isNull()) | (trim(F.col("PDX_NTNL_PROV_ID")) == ""))
svPdxNtnlProvId = F.when(cond_pdx_ntnl_prov_id_null_or_empty, "Y").otherwise("N")

cond_trans_denied_dt_equals_1753 = (F.col("TRANS_DENIED_DT") == F.lit("1753-01-01"))
svTransDeniedDt = F.when(cond_trans_denied_dt_equals_1753, "Y").otherwise("N")

cond_rx_no_null_or_empty = ((F.col("RX_NO").isNull()) | (trim(F.col("RX_NO")) == ""))
svRxNo = F.when(cond_rx_no_null_or_empty, "Y").otherwise("N")

cond_mbr_uniq_key_null_or_zero = (
    (F.col("MBR_UNIQ_KEY").isNull()) |
    (trim(F.col("MBR_UNIQ_KEY")) == "") |
    (F.col("MBR_UNIQ_KEY") == 0)
)
svMbrUniqKey = F.when(cond_mbr_uniq_key_null_or_zero, "Y").otherwise("N")

# They repeated condition for srcSysClmRcdDt = '1753-01-01' also
cond_src_sys_clm_rcvd_dt_1753 = (F.col("TRANS_DENIED_DT") == F.lit("1753-01-01"))
svSrcSysClmRcdDt = F.when(cond_src_sys_clm_rcvd_dt_1753, "Y").otherwise("N")

cond_src_sys_clm_rcvd_tm_null_or_empty = (
    (F.col("SRC_SYS_CLM_RCVD_TM").isNull()) |
    (trim(F.col("SRC_SYS_CLM_RCVD_TM")) == "")
)
svSrcSysClmRcdTm = F.when(cond_src_sys_clm_rcvd_tm_null_or_empty, "Y").otherwise("N")

df_with_flags = (
    df_xfmDataClean
    .withColumn("svPdxNtnlProvId", svPdxNtnlProvId)
    .withColumn("svTransDeniedDt", svTransDeniedDt)
    .withColumn("svRxNo", svRxNo)
    .withColumn("svMbrUniqKey", svMbrUniqKey)
    .withColumn("svSrcSysClmRcdDt", svSrcSysClmRcdDt)
    .withColumn("svSrcSysClmRcdTm", svSrcSysClmRcdTm)
)

df_OptumRx_mbr_denied = df_with_flags.filter(
    (F.col("svPdxNtnlProvId") == "N") &
    (F.col("svTransDeniedDt") == "N") &
    (F.col("svRxNo") == "N") &
    (F.col("svMbrUniqKey") == "N") &
    (F.col("svSrcSysClmRcdDt") == "N") &
    (F.col("svSrcSysClmRcdTm") == "N")
)

df_reject = df_with_flags.filter(
    (F.col("svPdxNtnlProvId") == "Y") |
    (F.col("svTransDeniedDt") == "Y") |
    (F.col("svRxNo") == "Y") |
    (F.col("svMbrUniqKey") == "Y") |
    (F.col("svSrcSysClmRcdDt") == "Y") |
    (F.col("svSrcSysClmRcdTm") == "Y")
)

#--------------------------------------------------------------------------------
# Stage: Seq_error (PxSequentialFile) - Write the reject dataframe
#--------------------------------------------------------------------------------
df_reject_out = df_reject.select(
    trim(F.col("MBR_ID")).alias("CLNT_ID"),
    trim(F.col("PDX_NTNL_PROV_ID")).alias("PDX_NTNL_PROV_ID"),
    trim(F.col("TRANS_TYP_CD")).alias("TRANS_TYP_CD"),
    F.col("TRANS_DENIED_DT").alias("TRANS_DENIED_DT"),
    trim(F.col("RX_NO")).alias("RX_NO"),
    F.col("PRCS_DT").alias("PRCS_DT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    trim(F.col("PHYS_DEA_NO")).alias("DEA_NO"),
    trim(F.col("PHYS_NTNL_PROV_ID")).alias("NPI_NO")
)

write_files(
    df_reject_out,
    f"{adls_path}/load/MBR_PDX_DENIED_TRANS_ERROR.{CurrDate}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote='"',
    nullValue=None
)

#--------------------------------------------------------------------------------
# Stage: db2_PROV (DB2ConnectorPX) - Read from IDS
#--------------------------------------------------------------------------------
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
query_db2_PROV = f"""
SELECT
       PROV.PROV_SK AS PROV_SK_SRV_PROV_SK,
       PROV.SRC_SYS_CD_SK AS SRC_SYS_CD_SK_SRV_PROV_SK,
       PROV.PROV_ID AS SVC_PROV_NABP_NM
FROM {IDSOwner}.PROV PROV
INNER JOIN {IDSOwner}.CD_MPPNG CD_MPPNG
  ON PROV.SRC_SYS_CD_SK=CD_MPPNG.CD_MPPNG_SK
WHERE CD_MPPNG.TRGT_CD = 'NABP'
ORDER BY PROV_ID ASC
"""
df_db2_PROV = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", query_db2_PROV.replace(" with ur",""))
    .load()
)

#--------------------------------------------------------------------------------
# Stage: db2_MBR (DB2ConnectorPX) - Read from IDS
#--------------------------------------------------------------------------------
query_db2_MBR = f"""
SELECT
S.SUB_ID||M.MBR_SFX_NO AS MBR_ID,
M.MBR_SK AS MBR_SK,
M.FIRST_NM AS SUB_FIRST_NM,
M.LAST_NM AS SUB_LAST_NM,
M.BRTH_DT_SK AS SUB_BRTH_DT,
S.GRP_SK AS GRP_SK,
S.SUB_SK AS SUB_SK,
G.GRP_ID as ACCOUNTID,
M.MBR_GNDR_CD_SK MBR_GNDR_CD_SK,
M.MBR_RELSHP_CD_SK MBR_RELSHP_CD_SK
FROM {IDSOwner}.MBR M, {IDSOwner}.SUB S, {IDSOwner}.GRP G
WHERE
S.SUB_SK = M.SUB_SK
AND
S.GRP_SK = G.GRP_SK
ORDER BY  S.SUB_ID||M.MBR_SFX_NO, G.GRP_ID
"""
df_db2_MBR = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", query_db2_MBR.replace(" with ur",""))
    .load()
)

#--------------------------------------------------------------------------------
# We join df_OptumRx_mbr_denied to df_db2_MBR (PxJoin: Jn_Mbr) - inner join on MBR_ID, ACCOUNTID
#--------------------------------------------------------------------------------
df_join_mbr = (
    df_OptumRx_mbr_denied.alias("Lnk_OptumRx_mbr_denied")
    .join(
        df_db2_MBR.alias("lnk_mbr_extr"),
        on=[
            F.col("Lnk_OptumRx_mbr_denied.MBR_ID") == F.col("lnk_mbr_extr.MBR_ID"),
            F.col("Lnk_OptumRx_mbr_denied.ACCOUNTID") == F.col("lnk_mbr_extr.ACCOUNTID")
        ],
        how="inner"
    )
)

df_Jn_Mbr = df_join_mbr.select(
    F.col("Lnk_OptumRx_mbr_denied.CLNT_ORG_OPTRNAL_ID"),
    F.col("Lnk_OptumRx_mbr_denied.CLNT_ORG_NM"),
    F.col("Lnk_OptumRx_mbr_denied.MAIL_ORDER_IN"),
    F.col("Lnk_OptumRx_mbr_denied.SRC_SYS_CAR_ID"),
    F.col("Lnk_OptumRx_mbr_denied.TRANS_TYP_CD"),
    F.col("Lnk_OptumRx_mbr_denied.CLNT_MBRSH_ID"),
    F.col("Lnk_OptumRx_mbr_denied.PATN_ID"),
    F.col("Lnk_OptumRx_mbr_denied.CLNT_ID"),
    F.col("Lnk_OptumRx_mbr_denied.CLNT_ELIG_MBRSH_ID"),
    F.col("Lnk_OptumRx_mbr_denied.MBR_SSN"),
    F.col("Lnk_OptumRx_mbr_denied.PRSN_NO"),
    F.col("Lnk_OptumRx_mbr_denied.RELSHP_CD"),
    F.col("Lnk_OptumRx_mbr_denied.GNDR_CD"),
    F.col("lnk_mbr_extr.SUB_BRTH_DT").alias("SUB_BRTH_DT"),
    F.col("Lnk_OptumRx_mbr_denied.MBR_BRTH_DT"),
    F.col("Lnk_OptumRx_mbr_denied.MBR_FIRST_NM"),
    F.col("Lnk_OptumRx_mbr_denied.MBR_LAST_NM"),
    F.col("lnk_mbr_extr.SUB_FIRST_NM").alias("SUBMT_SUB_FIRST_NM"),
    F.col("lnk_mbr_extr.SUB_LAST_NM").alias("SUBMT_SUB_LAST_NM"),
    F.col("Lnk_OptumRx_mbr_denied.RX_LABEL_TX"),
    F.col("Lnk_OptumRx_mbr_denied.SVC_DT"),
    F.col("Lnk_OptumRx_mbr_denied.SRC_SYS_GRP_ID"),
    F.col("Lnk_OptumRx_mbr_denied.SRC_SYS_CLM_RCVD_DT"),
    F.col("Lnk_OptumRx_mbr_denied.SRC_SYS_CLM_RCVD_TM"),
    F.col("Lnk_OptumRx_mbr_denied.MSG_TYP_CD"),
    F.col("Lnk_OptumRx_mbr_denied.MSG_TYP"),
    F.col("Lnk_OptumRx_mbr_denied.MSG_TX"),
    F.col("Lnk_OptumRx_mbr_denied.INGR_CST_ALW_AMT"),
    F.col("Lnk_OptumRx_mbr_denied.BILL_RX_GROS_APRV_AMT"),
    F.col("Lnk_OptumRx_mbr_denied.BILL_RX_NET_CHK_AMT"),
    F.col("Lnk_OptumRx_mbr_denied.BILL_RX_PATN_PAY_AMT"),
    F.col("Lnk_OptumRx_mbr_denied.BILL_RX_DISPENSE_FEE_AMT"),
    F.col("Lnk_OptumRx_mbr_denied.MEDIA_TYP_CD"),
    F.col("Lnk_OptumRx_mbr_denied.RSPN_CD"),
    F.col("Lnk_OptumRx_mbr_denied.DESC"),
    F.col("Lnk_OptumRx_mbr_denied.CHAPTER_ID"),
    F.col("Lnk_OptumRx_mbr_denied.CHAPTER_DESC"),
    F.col("Lnk_OptumRx_mbr_denied.PHYS_DEA_NO"),
    F.col("Lnk_OptumRx_mbr_denied.PHYS_NTNL_PROV_ID"),
    F.col("Lnk_OptumRx_mbr_denied.PHYS_FIRST_NM"),
    F.col("Lnk_OptumRx_mbr_denied.PHYS_LAST_NM"),
    F.col("Lnk_OptumRx_mbr_denied.PHYS_ST_ADDR_LN"),
    F.col("Lnk_OptumRx_mbr_denied.PHYS_CITY_NM"),
    F.col("Lnk_OptumRx_mbr_denied.PHYS_ST_CD"),
    F.col("Lnk_OptumRx_mbr_denied.PHYS_POSTAL_CD"),
    F.col("Lnk_OptumRx_mbr_denied.PDX_NTNL_PROV_ID"),
    F.col("Lnk_OptumRx_mbr_denied.SVC_PROV_NABP_NM"),
    F.col("Lnk_OptumRx_mbr_denied.PDX_NM"),
    F.col("Lnk_OptumRx_mbr_denied.PDX_PHN_NO"),
    F.col("Lnk_OptumRx_mbr_denied.NDC"),
    F.col("Lnk_OptumRx_mbr_denied.MBR_UNIQ_KEY"),
    F.col("lnk_mbr_extr.MBR_SK").alias("MBR_SK"),
    F.col("lnk_mbr_extr.GRP_SK").alias("GRP_SK"),
    F.col("lnk_mbr_extr.SUB_SK").alias("SUB_SK"),
    F.col("lnk_mbr_extr.MBR_GNDR_CD_SK").alias("MBR_GNDR_CD_SK"),
    F.col("lnk_mbr_extr.MBR_RELSHP_CD_SK").alias("MBR_RELSHP_CD_SK"),
    F.col("Lnk_OptumRx_mbr_denied.MBR_SFX_NO"),
    F.col("Lnk_OptumRx_mbr_denied.SRC_SYS_CD"),
    F.col("Lnk_OptumRx_mbr_denied.RX_NO"),
    F.col("Lnk_OptumRx_mbr_denied.MBR_ID"),
    F.col("Lnk_OptumRx_mbr_denied.SRC_SYS_CNTR_ID"),
    F.col("Lnk_OptumRx_mbr_denied.GRP_ID"),
    F.col("Lnk_OptumRx_mbr_denied.CLMORIGIN"),
    F.col("Lnk_OptumRx_mbr_denied.PDX_RSPN_RSN_CD_SK"),
    F.col("Lnk_OptumRx_mbr_denied.PRCS_DT"),
    F.col("Lnk_OptumRx_mbr_denied.LOB_IN")
)

#--------------------------------------------------------------------------------
# Stage: db2_prov_dea (DB2ConnectorPX) used as a reference link in Lkp_prov,
#   with range conditions on SVC_DT, PHYS_DEA_NO, PHYS_NTNL_PROV_ID, etc.
#   We must stage the primary link (df_Jn_Mbr) and do one big JDBC query join.
#--------------------------------------------------------------------------------
temp_table_db2_prov_dea = "STAGING.OptumIdsMbrPdxDeniedClmXfm_db2_prov_dea_temp"

execute_dml(f"DROP TABLE IF EXISTS {temp_table_db2_prov_dea}", jdbc_url_ids, jdbc_props_ids)

df_Jn_Mbr.write.jdbc(
    url=jdbc_url_ids,
    table=temp_table_db2_prov_dea,
    mode="overwrite",
    properties=jdbc_props_ids
)

query_db2_prov_dea = f"""
SELECT
       MAX(PROV.PROV_SK) AS PROV_SK,
       PROV_DEA.PROV_DEA_SK,
       PROV.PROV_SPEC_CD_SK,
       PROV_DEA.NTNL_PROV_ID AS NPI_NO,
       PROV_DEA.EFF_DT_SK,
       PROV_DEA.TERM_DT_SK,
       st.PHYS_DEA_NO AS PHYS_DEA_NO_joinkey,
       st.PHYS_NTNL_PROV_ID AS PHYS_NTNL_PROV_ID_joinkey,
       st.SVC_DT AS SVC_DT_joinkey
FROM {IDSOwner}.PROV_DEA PROV_DEA
INNER JOIN {IDSOwner}.PROV PROV
  ON PROV_DEA.CMN_PRCT_SK=PROV.CMN_PRCT_SK
  AND PROV_DEA.NTNL_PROV_ID=PROV.NTNL_PROV_ID
INNER JOIN (
  SELECT *
  FROM {temp_table_db2_prov_dea}
) st
  ON PROV_DEA.DEA_NO=st.PHYS_DEA_NO
  AND PROV_DEA.NTNL_PROV_ID=st.PHYS_NTNL_PROV_ID
WHERE
  PROV_DEA.CMN_PRCT_SK NOT IN ('0','1')
  AND PROV.TERM_DT_SK >= st.SVC_DT
  AND PROV_DEA.EFF_DT_SK <= st.SVC_DT
  AND PROV_DEA.TERM_DT_SK >= st.SVC_DT
GROUP BY
  PROV_DEA.PROV_DEA_SK,
  PROV.PROV_SPEC_CD_SK,
  PROV_DEA.DEA_NO,
  PROV_DEA.NTNL_PROV_ID,
  PROV_DEA.EFF_DT_SK,
  PROV_DEA.TERM_DT_SK,
  st.PHYS_DEA_NO,
  st.PHYS_NTNL_PROV_ID,
  st.SVC_DT
"""

df_db2_prov_dea = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", query_db2_prov_dea.replace(" with ur",""))
    .load()
)

# We rename columns to line up with the usage in the next stage:
df_db2_prov_dea_renamed = df_db2_prov_dea.select(
    F.col("PROV_SK").alias("PROV_SK"),
    F.col("PROV_DEA_SK").alias("PROV_DEA_SK"),
    F.col("PROV_SPEC_CD_SK").alias("PROV_SPEC_CD_SK"),
    F.col("NPI_NO").alias("NPI_NO"),
    F.col("EFF_DT_SK").alias("EFF_DT_SK"),
    F.col("TERM_DT_SK").alias("TERM_DT_SK"),
    F.col("PHYS_DEA_NO_joinkey").alias("PHYS_DEA_NO_joinkey"),
    F.col("PHYS_NTNL_PROV_ID_joinkey").alias("PHYS_NTNL_PROV_ID_joinkey"),
    F.col("SVC_DT_joinkey").alias("SVC_DT_joinkey")
)

#--------------------------------------------------------------------------------
# Stage: Lkp_prov (PxLookup) - left join df_Jn_Mbr with df_db2_prov_dea_renamed
#   Join keys: PHYS_DEA_NO -> PHYS_DEA_NO_joinkey
#              PHYS_NTNL_PROV_ID -> PHYS_NTNL_PROV_ID_joinkey
#              SVC_DT -> SVC_DT_joinkey
#--------------------------------------------------------------------------------
df_Lkp_prov = (
    df_Jn_Mbr.alias("Lnk_mbr")
    .join(
        df_db2_prov_dea_renamed.alias("lnk_prov_extr"),
        on=[
            F.col("Lnk_mbr.PHYS_DEA_NO") == F.col("lnk_prov_extr.PHYS_DEA_NO_joinkey"),
            F.col("Lnk_mbr.PHYS_NTNL_PROV_ID") == F.col("lnk_prov_extr.PHYS_NTNL_PROV_ID_joinkey"),
            F.col("Lnk_mbr.SVC_DT") == F.col("lnk_prov_extr.SVC_DT_joinkey")
        ],
        how="left"
    )
)

#--------------------------------------------------------------------------------
# Stage: Jn_ndc => next step is the output pin to next join, but effectively
#   we pass forward columns. The JSON calls it "Jn_ndc",
#   but physically it's just the primary link output from Lkp_prov.
#--------------------------------------------------------------------------------
df_Jn_ndc = df_Lkp_prov.select(
    F.col("Lnk_mbr.CLNT_ORG_OPTRNAL_ID"),
    F.col("Lnk_mbr.CLNT_ORG_NM"),
    F.col("Lnk_mbr.MAIL_ORDER_IN"),
    F.col("Lnk_mbr.SRC_SYS_CAR_ID"),
    F.col("Lnk_mbr.TRANS_TYP_CD"),
    F.col("Lnk_mbr.CLNT_MBRSH_ID"),
    F.col("Lnk_mbr.PATN_ID"),
    F.col("Lnk_mbr.CLNT_ID"),
    F.col("Lnk_mbr.CLNT_ELIG_MBRSH_ID"),
    F.col("Lnk_mbr.MBR_SSN"),
    F.col("Lnk_mbr.PRSN_NO"),
    F.col("Lnk_mbr.RELSHP_CD"),
    F.col("Lnk_mbr.GNDR_CD"),
    F.col("Lnk_mbr.SUB_BRTH_DT"),
    F.col("Lnk_mbr.MBR_BRTH_DT"),
    F.col("Lnk_mbr.MBR_FIRST_NM"),
    F.col("Lnk_mbr.MBR_LAST_NM"),
    F.col("Lnk_mbr.SUBMT_SUB_FIRST_NM"),
    F.col("Lnk_mbr.SUBMT_SUB_LAST_NM"),
    F.col("Lnk_mbr.RX_LABEL_TX"),
    F.col("Lnk_mbr.SVC_DT"),
    F.col("Lnk_mbr.SRC_SYS_GRP_ID"),
    F.col("Lnk_mbr.SRC_SYS_CLM_RCVD_DT"),
    F.col("Lnk_mbr.SRC_SYS_CLM_RCVD_TM"),
    F.col("Lnk_mbr.MSG_TYP_CD"),
    F.col("Lnk_mbr.MSG_TYP"),
    F.col("Lnk_mbr.MSG_TX"),
    F.col("Lnk_mbr.INGR_CST_ALW_AMT"),
    F.col("Lnk_mbr.BILL_RX_GROS_APRV_AMT"),
    F.col("Lnk_mbr.BILL_RX_NET_CHK_AMT"),
    F.col("Lnk_mbr.BILL_RX_PATN_PAY_AMT"),
    F.col("Lnk_mbr.BILL_RX_DISPENSE_FEE_AMT"),
    F.col("Lnk_mbr.MEDIA_TYP_CD"),
    F.col("Lnk_mbr.RSPN_CD"),
    F.col("Lnk_mbr.DESC"),
    F.col("Lnk_mbr.CHAPTER_ID"),
    F.col("Lnk_mbr.CHAPTER_DESC"),
    F.col("Lnk_mbr.PHYS_DEA_NO"),
    F.col("Lnk_mbr.PHYS_NTNL_PROV_ID"),
    F.col("Lnk_mbr.PHYS_FIRST_NM"),
    F.col("Lnk_mbr.PHYS_LAST_NM"),
    F.col("Lnk_mbr.PHYS_ST_ADDR_LN"),
    F.col("Lnk_mbr.PHYS_CITY_NM"),
    F.col("Lnk_mbr.PHYS_ST_CD"),
    F.col("Lnk_mbr.PHYS_POSTAL_CD"),
    F.col("Lnk_mbr.PDX_NTNL_PROV_ID"),
    F.col("Lnk_mbr.SVC_PROV_NABP_NM"),
    F.col("Lnk_mbr.PDX_NM"),
    F.col("Lnk_mbr.PDX_PHN_NO"),
    F.col("Lnk_mbr.NDC"),
    F.col("Lnk_mbr.MBR_UNIQ_KEY"),
    F.col("Lnk_mbr.MBR_SK"),
    F.col("Lnk_mbr.GRP_SK"),
    F.col("Lnk_mbr.SUB_SK"),
    F.col("Lnk_mbr.MBR_GNDR_CD_SK"),
    F.col("Lnk_mbr.MBR_RELSHP_CD_SK"),
    F.col("Lnk_mbr.MBR_SFX_NO"),
    F.col("lnk_prov_extr.PROV_SK").alias("PROV_SK"),
    F.col("lnk_prov_extr.PROV_SPEC_CD_SK").alias("PROV_SPEC_CD_SK"),
    F.col("lnk_prov_extr.PROV_DEA_SK").alias("PROV_DEA_SK"),
    F.col("lnk_prov_extr.EFF_DT_SK").alias("EFF_DT_SK"),
    F.col("lnk_prov_extr.TERM_DT_SK").alias("TERM_DT_SK"),
    F.col("Lnk_mbr.SRC_SYS_CD"),
    F.col("Lnk_mbr.CLMORIGIN"),
    F.col("Lnk_mbr.RX_NO"),
    F.col("Lnk_mbr.MBR_ID"),
    F.col("Lnk_mbr.SRC_SYS_CNTR_ID"),
    F.col("Lnk_mbr.GRP_ID"),
    F.col("Lnk_mbr.PDX_RSPN_RSN_CD_SK"),
    F.col("Lnk_mbr.PRCS_DT"),
    F.col("Lnk_mbr.LOB_IN")
)

#--------------------------------------------------------------------------------
# Stage: Jn_NABP => next join with df_db2_PROV (PxJoin leftouter by SVC_PROV_NABP_NM).
#--------------------------------------------------------------------------------
df_Jn_NABP = (
    df_Jn_ndc.alias("Jn_ndc")
    .join(
        df_db2_PROV.alias("lnk_SRV_PROV_SK"),
        on=[
            F.col("Jn_ndc.SVC_PROV_NABP_NM") == F.col("lnk_SRV_PROV_SK.SVC_PROV_NABP_NM")
        ],
        how="left"
    )
)

df_Lnk_OptumRx_mbr_denied_out = df_Jn_NABP.select(
    F.col("Jn_ndc.CLNT_ORG_OPTRNAL_ID"),
    F.col("Jn_ndc.CLNT_ORG_NM"),
    F.col("Jn_ndc.MAIL_ORDER_IN"),
    F.col("Jn_ndc.SRC_SYS_CAR_ID"),
    F.col("Jn_ndc.TRANS_TYP_CD"),
    F.col("Jn_ndc.CLNT_MBRSH_ID"),
    F.col("Jn_ndc.PATN_ID"),
    F.col("Jn_ndc.CLNT_ID"),
    F.col("Jn_ndc.CLNT_ELIG_MBRSH_ID"),
    F.col("Jn_ndc.MBR_SSN"),
    F.col("Jn_ndc.PRSN_NO"),
    F.col("Jn_ndc.RELSHP_CD"),
    F.col("Jn_ndc.GNDR_CD"),
    F.col("Jn_ndc.SUB_BRTH_DT"),
    F.col("Jn_ndc.MBR_BRTH_DT"),
    F.col("Jn_ndc.MBR_FIRST_NM"),
    F.col("Jn_ndc.MBR_LAST_NM"),
    F.col("Jn_ndc.SUBMT_SUB_FIRST_NM"),
    F.col("Jn_ndc.SUBMT_SUB_LAST_NM"),
    F.col("Jn_ndc.RX_LABEL_TX"),
    F.col("Jn_ndc.SVC_DT"),
    F.col("Jn_ndc.SRC_SYS_GRP_ID"),
    F.col("Jn_ndc.SRC_SYS_CLM_RCVD_DT"),
    F.col("Jn_ndc.SRC_SYS_CLM_RCVD_TM"),
    F.col("Jn_ndc.MSG_TYP_CD"),
    F.col("Jn_ndc.MSG_TYP"),
    F.col("Jn_ndc.MSG_TX"),
    F.col("Jn_ndc.INGR_CST_ALW_AMT"),
    F.col("Jn_ndc.BILL_RX_GROS_APRV_AMT"),
    F.col("Jn_ndc.BILL_RX_NET_CHK_AMT"),
    F.col("Jn_ndc.BILL_RX_PATN_PAY_AMT"),
    F.col("Jn_ndc.BILL_RX_DISPENSE_FEE_AMT"),
    F.col("Jn_ndc.MEDIA_TYP_CD"),
    F.col("Jn_ndc.RSPN_CD"),
    F.col("Jn_ndc.DESC"),
    F.col("Jn_ndc.CHAPTER_ID"),
    F.col("Jn_ndc.CHAPTER_DESC"),
    F.col("Jn_ndc.PHYS_DEA_NO"),
    F.col("Jn_ndc.PHYS_NTNL_PROV_ID"),
    F.col("Jn_ndc.PHYS_FIRST_NM"),
    F.col("Jn_ndc.PHYS_LAST_NM"),
    F.col("Jn_ndc.PHYS_ST_ADDR_LN"),
    F.col("Jn_ndc.PHYS_CITY_NM"),
    F.col("Jn_ndc.PHYS_ST_CD"),
    F.col("Jn_ndc.PHYS_POSTAL_CD"),
    F.col("Jn_ndc.PDX_NTNL_PROV_ID"),
    F.col("Jn_ndc.SVC_PROV_NABP_NM"),
    F.col("Jn_ndc.PDX_NM"),
    F.col("Jn_ndc.PDX_PHN_NO"),
    F.col("Jn_ndc.NDC"),
    F.col("Jn_ndc.MBR_UNIQ_KEY"),
    F.col("Jn_ndc.MBR_SK"),
    F.col("Jn_ndc.GRP_SK"),
    F.col("Jn_ndc.SUB_SK"),
    F.col("Jn_ndc.MBR_GNDR_CD_SK"),
    F.col("Jn_ndc.MBR_RELSHP_CD_SK"),
    F.col("Jn_ndc.MBR_SFX_NO"),
    F.col("Jn_ndc.PROV_SK"),
    F.col("Jn_ndc.PROV_SPEC_CD_SK"),
    F.col("Jn_ndc.PROV_DEA_SK"),
    F.col("Jn_ndc.EFF_DT_SK"),
    F.col("Jn_ndc.TERM_DT_SK"),
    F.col("lnk_SRV_PROV_SK.PROV_SK_SRV_PROV_SK").alias("SRV_PROV_SK"),
    F.col("lnk_SRV_PROV_SK.SRC_SYS_CD_SK_SRV_PROV_SK").alias("SRC_SYS_CD_SK_SRV_PROV_SK"),
    F.col("Jn_ndc.SRC_SYS_CD"),
    F.col("Jn_ndc.RX_NO"),
    F.col("Jn_ndc.MBR_ID"),
    F.col("Jn_ndc.SRC_SYS_CNTR_ID"),
    F.col("Jn_ndc.GRP_ID"),
    F.col("Jn_ndc.CLMORIGIN"),
    F.col("Jn_ndc.PDX_RSPN_RSN_CD_SK"),
    F.col("Jn_ndc.PRCS_DT"),
    F.col("Jn_ndc.LOB_IN")
)

#--------------------------------------------------------------------------------
# Stage: xfm_OptumRx_pdx (CTransformerStage)
#--------------------------------------------------------------------------------
df_enriched = df_Lnk_OptumRx_mbr_denied_out.select(
    F.lit(0).alias("MBR_PDX_DENIED_TRANS_SK"),  # PK with default 0
    F.col("MBR_UNIQ_KEY"),
    F.col("PDX_NTNL_PROV_ID"),
    trim(F.upper(F.col("TRANS_TYP_CD"))).alias("TRANS_TYP_CD"),
    F.col("SVC_DT").alias("TRANS_DENIED_DT"),
    F.col("RX_NO"),
    current_date(),  # PRCS_DT => currentdate() in DS means current_date() here
    trim(F.upper(F.col("SRC_SYS_CD"))).alias("SRC_SYS_CD"),
    F.lit(IDSRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(IDSRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.when(
        (F.col("GRP_SK").isNull()) | (F.length(F.col("GRP_SK")) == 0),
        F.lit(0)
    ).otherwise(F.col("GRP_SK")).alias("GRP_SK"),
    F.when(
        (F.col("MBR_SK").isNull()) | (F.length(F.col("MBR_SK")) == 0),
        F.lit(0)
    ).otherwise(F.col("MBR_SK")).alias("MBR_SK"),
    F.lit("0").alias("NDC_SK"),
    F.when(
        (F.col("PROV_SK").isNull()) | (F.length(F.col("PROV_SK")) == 0),
        F.lit(0)
    ).otherwise(F.col("PROV_SK")).alias("PRSCRB_PROV_SK"),
    F.when(
        (F.col("PROV_SPEC_CD_SK").isNull()) | (F.length(F.col("PROV_SPEC_CD_SK")) == 0),
        F.lit(0)
    ).otherwise(F.col("PROV_SPEC_CD_SK")).alias("PROV_SPEC_CD_SK"),
    F.when(
        (F.col("SRV_PROV_SK").isNull()) | (F.length(F.col("SRV_PROV_SK")) == 0),
        F.lit(0)
    ).otherwise(F.col("SRV_PROV_SK")).alias("SRV_PROV_SK"),
    F.when(
        (F.col("SUB_SK").isNull()) | (F.length(F.col("SUB_SK")) == 0),
        F.lit(0)
    ).otherwise(F.col("SUB_SK")).alias("SUB_SK"),
    F.col("CLMORIGIN").alias("MEDIA_TYP_CD_SK"),
    F.when(
        (F.col("MBR_GNDR_CD_SK").isNull()) | (F.length(F.col("MBR_GNDR_CD_SK")) == 0),
        F.lit(0)
    ).otherwise(F.col("MBR_GNDR_CD_SK")).alias("MBR_GNDR_CD_SK"),
    F.when(
        (F.col("MBR_RELSHP_CD_SK").isNull()) | (F.length(F.col("MBR_RELSHP_CD_SK")) == 0),
        F.lit(0)
    ).otherwise(F.col("MBR_RELSHP_CD_SK")).alias("MBR_RELSHP_CD_SK"),
    trim(F.col("PDX_RSPN_RSN_CD_SK")).alias("PDX_RSPN_RSN_CD_SK"),
    F.lit(0).alias("PDX_RSPN_TYP_CD_SK"),
    F.lit(0).alias("TRANS_TYP_CD_SK"),
    F.when(
        (F.col("MAIL_ORDER_IN").isNull()) | (F.length(trim(F.col("MAIL_ORDER_IN"))) == 0),
        F.lit("R")
    ).otherwise(F.col("MAIL_ORDER_IN")).alias("MAIL_ORDER_IN"),
    F.when(
        (F.col("MBR_BRTH_DT").isNull()) | (F.length(trim(F.col("MBR_BRTH_DT"))) == 0),
        F.lit("1753-01-01")
    ).otherwise(F.col("MBR_BRTH_DT")).alias("MBR_BRTH_DT"),
    F.col("SRC_SYS_CLM_RCVD_DT"),
    F.col("SRC_SYS_CLM_RCVD_TM"),
    F.when(
        (F.col("SUB_BRTH_DT").isNull()) | (F.length(trim(F.col("SUB_BRTH_DT"))) == 0),
        F.lit("1753-01-01")
    ).otherwise(F.col("SUB_BRTH_DT")).alias("SUB_BRTH_DT"),
    F.when(
        (F.col("BILL_RX_DISPENSE_FEE_AMT").isNull()) | (F.length(trim(F.col("BILL_RX_DISPENSE_FEE_AMT"))) == 0),
        F.lit("0.00 ")
    ).otherwise(F.col("BILL_RX_DISPENSE_FEE_AMT")).alias("BILL_RX_DISPENSE_FEE_AMT"),
    F.when(
        (F.col("BILL_RX_GROS_APRV_AMT").isNull()) | (F.length(trim(F.col("BILL_RX_GROS_APRV_AMT"))) == 0),
        F.lit("0.00 ")
    ).otherwise(F.col("BILL_RX_GROS_APRV_AMT")).alias("BILL_RX_GROS_APRV_AMT"),
    F.when(
        (F.col("BILL_RX_NET_CHK_AMT").isNull()) | (F.length(trim(F.col("BILL_RX_NET_CHK_AMT"))) == 0),
        F.lit("0.00 ")
    ).otherwise(F.col("BILL_RX_NET_CHK_AMT")).alias("BILL_RX_NET_CHK_AMT"),
    F.when(
        (F.col("BILL_RX_PATN_PAY_AMT").isNull()) | (F.length(trim(F.col("BILL_RX_PATN_PAY_AMT"))) == 0),
        F.lit("0.00 ")
    ).otherwise(F.col("BILL_RX_PATN_PAY_AMT")).alias("BILL_RX_PATN_PAY_AMT"),
    F.when(
        (F.col("INGR_CST_ALW_AMT").isNull()) | (F.length(trim(F.col("INGR_CST_ALW_AMT"))) == 0),
        F.lit("0.00 ")
    ).otherwise(F.col("INGR_CST_ALW_AMT")).alias("INGR_CST_ALW_AMT"),
    F.when(
        F.length(F.month(current_date())) == 1,
        F.concat(F.lit("0"), F.month(current_date()).cast(StringType()))
    ).otherwise(F.month(current_date()).cast(StringType())).alias("TRANS_MO_NO"),
    F.year(current_date()).alias("TRANS_YR_NO"),
    F.when(
        F.col("LOB_IN") == F.lit("MCARE"),
        F.col("SRC_SYS_CNTR_ID")
    ).otherwise(F.col("MBR_ID").substr(F.lit(1), F.lit(20))).alias("MBR_ID"),
    F.when(
        (F.col("MBR_SFX_NO").isNull()) | (F.length(trim(F.col("MBR_SFX_NO"))) == 0),
        F.lit("00")
    ).otherwise(trim("MBR_SFX_NO")).alias("MBR_SFX_NO"),
    F.when(
        (F.col("MBR_FIRST_NM").isNull()) | (F.length(trim(F.col("MBR_FIRST_NM"))) == 0),
        F.lit("UNK")
    ).otherwise(trim(F.upper("MBR_FIRST_NM"))).alias("MBR_FIRST_NM"),
    F.when(
        (F.col("MBR_LAST_NM").isNull()) | (F.length(trim(F.col("MBR_LAST_NM"))) == 0),
        F.lit("UNK")
    ).otherwise(trim(F.upper("MBR_LAST_NM"))).alias("MBR_LAST_NM"),
    F.when(
        (F.col("MBR_SSN").isNull()) | (F.length(F.col("MBR_SSN")) == 0),
        F.lit("UNK")
    ).otherwise(F.col("MBR_SSN")).alias("MBR_SSN"),
    F.when(
        (F.col("PDX_NM").isNull()) | (F.length(trim(F.col("PDX_NM"))) == 0),
        F.lit("")
    ).otherwise(trim(F.upper("PDX_NM"))).alias("PDX_NM"),
    F.col("PDX_PHN_NO"),
    F.col("PHYS_DEA_NO"),
    F.when(
        (F.col("PHYS_NTNL_PROV_ID").isNull()) | (F.length(trim(F.col("PHYS_NTNL_PROV_ID"))) == 0)
         | (F.col("PHYS_NTNL_PROV_ID") == F.lit("0")) | (F.col("PHYS_NTNL_PROV_ID") == F.lit("NA")),
        F.lit("UNK")
    ).otherwise(trim(F.upper("PHYS_NTNL_PROV_ID"))).alias("PHYS_NTNL_PROV_ID"),
    F.when(
        (F.col("PHYS_FIRST_NM").isNull()) | (F.length(trim(F.col("PHYS_FIRST_NM"))) == 0),
        F.lit("UNK")
    ).otherwise(trim(F.upper("PHYS_FIRST_NM"))).alias("PHYS_FIRST_NM"),
    F.when(
        (F.col("PHYS_LAST_NM").isNull()) | (F.length(trim(F.col("PHYS_LAST_NM"))) == 0),
        F.lit("UNK")
    ).otherwise(trim(F.upper("PHYS_LAST_NM"))).alias("PHYS_LAST_NM"),
    F.col("PHYS_ST_ADDR_LN"),
    F.col("PHYS_CITY_NM"),
    F.when(
        (F.col("PHYS_ST_CD").isNull()) | (F.length(trim(F.col("PHYS_ST_CD"))) == 0),
        F.lit("UNK")
    ).otherwise(trim(F.upper("PHYS_ST_CD"))).alias("PHYS_ST_CD"),
    F.col("PHYS_POSTAL_CD"),
    F.when(
        (F.col("RX_LABEL_TX").isNull()) | (F.length(trim(F.col("RX_LABEL_TX"))) == 0),
        F.lit("UNK")
    ).otherwise(trim(F.upper("RX_LABEL_TX"))).alias("RX_LABEL_TX"),
    F.when(
        (F.col("SVC_PROV_NABP_NM").isNull()) | (F.length(trim(F.col("SVC_PROV_NABP_NM"))) == 0),
        F.lit("UNK")
    ).otherwise(trim(F.upper("SVC_PROV_NABP_NM"))).alias("SVC_PROV_NABP_NM"),
    F.when(
        (F.col("SRC_SYS_CAR_ID").isNull()) | (F.length(trim(F.col("SRC_SYS_CAR_ID"))) == 0),
        F.lit("UNK")
    ).otherwise(trim(F.upper("SRC_SYS_CAR_ID"))).alias("SRC_SYS_CAR_ID"),
    F.lit("BCBSKC").alias("SRC_SYS_CLNT_ORG_ID"),
    F.lit("BCBS OF KANSAS CITY").alias("SRC_SYS_CLNT_ORG_NM"),
    F.when(
        (F.col("SRC_SYS_CNTR_ID").isNull()) | (F.length(trim(F.col("SRC_SYS_CNTR_ID"))) == 0),
        F.lit("UNK")
    ).otherwise(trim(F.upper("SRC_SYS_CNTR_ID"))).alias("SRC_SYS_CNTR_ID"),
    F.when(
        (F.col("SRC_SYS_GRP_ID").isNull()) | (F.length(trim(F.col("SRC_SYS_GRP_ID"))) == 0),
        F.lit("UNK")
    ).otherwise(trim(F.upper("SRC_SYS_GRP_ID"))).alias("SRC_SYS_GRP_ID"),
    F.when(
        (F.col("CLNT_ID").isNull()) | (F.length(trim(F.col("CLNT_ID"))) == 0),
        F.lit("UNK")
    ).otherwise(trim(F.upper("CLNT_ID"))).alias("SUB_ID"),
    trim(F.upper("SUBMT_SUB_FIRST_NM")).alias("SUB_FIRST_NM"),
    trim(F.upper("SUBMT_SUB_LAST_NM")).alias("SUB_LAST_NM"),
    F.col("CLNT_ELIG_MBRSH_ID"),
    trim(F.upper("PRSN_NO")).alias("PRSN_NO"),
    trim(F.upper("RELSHP_CD")).alias("RELSHP_CD"),
    trim(F.upper("GNDR_CD")).alias("GNDR_CD"),
    F.col("SRC_SYS_CLM_RCVD_TM").alias("CLM_RCVD_TM"),
    trim(F.upper("MSG_TYP_CD")).alias("MSG_TYP_CD"),
    trim(F.upper("MSG_TYP")).alias("MSG_TYP"),
    trim(F.upper("MSG_TX")).alias("MSG_TX"),
    trim(F.upper("CLNT_MBRSH_ID")).alias("CLNT_MBRSH_ID"),
    trim(F.upper("MEDIA_TYP_CD")).alias("MEDIA_TYP_CD"),
    trim(F.upper("RSPN_CD")).alias("RSPN_CD"),
    trim(F.upper("DESC")).alias("DESC"),
    trim(F.upper("CHAPTER_ID")).alias("CHAPTER_ID"),
    trim(F.upper("CHAPTER_DESC")).alias("CHAPTER_DESC"),
    F.col("PDX_NTNL_PROV_ID").alias("PDX_NPI"),
    F.col("NDC")
)

#--------------------------------------------------------------------------------
# Stage: Rv_Natural_Keys (PxRemDup)
#   Keys: MBR_UNIQ_KEY, PDX_NTNL_PROV_ID, TRANS_TYP_CD, TRANS_DENIED_DT, RX_NO,
#         PRCS_DT, SRC_SYS_CLM_RCVD_DT, SRC_SYS_CLM_RCVD_TM, SRC_SYS_CD
#   Deduplicate retaining first record
#--------------------------------------------------------------------------------
df_deduped = dedup_sort(
    df_enriched,
    partition_cols=[
        "MBR_UNIQ_KEY","PDX_NTNL_PROV_ID","TRANS_TYP_CD","TRANS_DENIED_DT","RX_NO",
        "PRCS_DT","SRC_SYS_CLM_RCVD_DT","SRC_SYS_CLM_RCVD_TM","SRC_SYS_CD"
    ],
    sort_cols=[]
)

#--------------------------------------------------------------------------------
# Stage: ds_MBR_PDX_DENIED_TRANS_xfm (PxDataSet) => must become Parquet
#   The path is "ds/MBR_PDX_DENIED_TRANS.#SrcSysCd#.xfrm.#RunID#.ds"
#   Remove the .ds, add .parquet
#--------------------------------------------------------------------------------
df_final = df_deduped.select(
    "MBR_PDX_DENIED_TRANS_SK",
    "MBR_UNIQ_KEY",
    "PDX_NTNL_PROV_ID",
    "TRANS_TYP_CD",
    "TRANS_DENIED_DT",
    "RX_NO",
    "PRCS_DT",
    "SRC_SYS_CD",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "GRP_SK",
    "MBR_SK",
    "NDC_SK",
    "PRSCRB_PROV_SK",
    "PROV_SPEC_CD_SK",
    "SRV_PROV_SK",
    "SUB_SK",
    "MEDIA_TYP_CD_SK",
    "MBR_GNDR_CD_SK",
    "MBR_RELSHP_CD_SK",
    "PDX_RSPN_RSN_CD_SK",
    "PDX_RSPN_TYP_CD_SK",
    "TRANS_TYP_CD_SK",
    "MAIL_ORDER_IN",
    "MBR_BRTH_DT",
    "SRC_SYS_CLM_RCVD_DT",
    "SRC_SYS_CLM_RCVD_TM",
    "SUB_BRTH_DT",
    "BILL_RX_DISPENSE_FEE_AMT",
    "BILL_RX_GROS_APRV_AMT",
    "BILL_RX_NET_CHK_AMT",
    "BILL_RX_PATN_PAY_AMT",
    "INGR_CST_ALW_AMT",
    "TRANS_MO_NO",
    "TRANS_YR_NO",
    "MBR_ID",
    "MBR_SFX_NO",
    "MBR_FIRST_NM",
    "MBR_LAST_NM",
    "MBR_SSN",
    "PDX_NM",
    "PDX_PHN_NO",
    "PHYS_DEA_NO",
    "PHYS_NTNL_PROV_ID",
    "PHYS_FIRST_NM",
    "PHYS_LAST_NM",
    "PHYS_ST_ADDR_LN",
    "PHYS_CITY_NM",
    "PHYS_ST_CD",
    "PHYS_POSTAL_CD",
    "RX_LABEL_TX",
    "SVC_PROV_NABP_NM",
    "SRC_SYS_CAR_ID",
    "SRC_SYS_CLNT_ORG_ID",
    "SRC_SYS_CLNT_ORG_NM",
    "SRC_SYS_CNTR_ID",
    "SRC_SYS_GRP_ID",
    "SUB_ID",
    "SUB_FIRST_NM",
    "SUB_LAST_NM",
    "CLNT_ELIG_MBRSH_ID",
    "PRSN_NO",
    "RELSHP_CD",
    "GNDR_CD",
    "CLM_RCVD_TM",
    "MSG_TYP_CD",
    "MSG_TYP",
    "MSG_TX",
    "CLNT_MBRSH_ID",
    "MEDIA_TYP_CD",
    "RSPN_CD",
    "DESC",
    "CHAPTER_ID",
    "CHAPTER_DESC",
    "PDX_NPI",
    "NDC"
)

write_files(
    df_final,
    f"{adls_path}/ds/MBR_PDX_DENIED_TRANS.{SrcSysCd}.xfrm.{RunID}.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote='"',
    nullValue=None
)

# End of generated script