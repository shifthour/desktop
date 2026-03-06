# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2009 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #               Change Description                                                           Development Project      Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------     ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Sravya Gorla                    2019-04-17        Spira reporting                               Originally Programmed                                             IntegrateDev2          Kalyan Neelam             2019-04-18
# MAGIC Sunny Mandadi               2019-06-27        Spira Reporting Changes           Handled nulls in SVC_PROV_SK field                 IntegrateDev1         Kalyan Neelam             2019-06-28
# MAGIC                                                                                                                                    and removed aftre-job subroutine

# MAGIC CLM_LN lookup
# MAGIC MR_LN_SVC_CAT lookup
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    TimestampType
)
from pyspark.sql.functions import col, lit, when, concat, isnull, coalesce, row_number
from pyspark.sql import Window
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------

################################################################################
# Parameter Definition
################################################################################
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
SrcSysCd = get_widget_value('SrcSysCd','CAP')
RunID = get_widget_value('RunID','20190402000000')
PrefixFkeyFailedFileName = get_widget_value('PrefixFkeyFailedFileName','')

# We will use the job name "IdsMbrVstFkey" per the JSON
DSJobName = "IdsMbrVstFkey"

################################################################################
# Read from K_PROV (DB2ConnectorPX -> IDS)
# Columns: PROV_ID, PROV_SK, SRC_SYS_CD_SK
################################################################################
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = (
    "SELECT \n"
    "PROV_ID,\n"
    "PROV_SK,\n"
    "SRC_SYS_CD_SK \n"
    f"from {IDSOwner}.prov"
)
df_K_PROV = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

################################################################################
# Copy_136 (PxCopy)
# We produce three outputs from df_K_PROV, each identical,
# with columns PROV_SK, PROV_ID, SRC_SYS_CD_SK
################################################################################
df_Copy_136P3 = df_K_PROV.select(
    col("PROV_SK").alias("PROV_SK"),
    col("PROV_ID").alias("PROV_ID"),
    col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK")
)
df_Copy_136P4 = df_K_PROV.select(
    col("PROV_SK").alias("PROV_SK"),
    col("PROV_ID").alias("PROV_ID"),
    col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK")
)
df_Copy_136P5 = df_K_PROV.select(
    col("PROV_SK").alias("PROV_SK"),
    col("PROV_ID").alias("PROV_ID"),
    col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK")
)

################################################################################
# Read from db2_MBR (DB2ConnectorPX -> IDS)
# Columns: MBR_UNIQ_KEY, MBR_SK
################################################################################
extract_query = (
    "SELECT \n"
    "MBR_UNIQ_KEY,\n"
    "MBR_SK\n"
    f"FROM {IDSOwner}.MBR"
)
df_db2_MBR = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

################################################################################
# MBRVST_Pkey (PxSequentialFile) - read .dat file
################################################################################
schema_MBRVST_Pkey = StructType([
    StructField("PRI_NAT_KEY_STRING", StringType(), nullable=False),
    StructField("FIRST_RECYC_TS", TimestampType(), nullable=False),
    StructField("MBR_VST_SK", IntegerType(), nullable=False),
    StructField("MBR_UNIQ_KEY", IntegerType(), nullable=False),
    StructField("VST_ID", StringType(), nullable=False),
    StructField("SRC_SYS_CD_SK", StringType(), nullable=False),
    StructField("CRT_RUN_EXCNT_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCNT_SK", IntegerType(), nullable=False),
    StructField("PRI_POL_ID", StringType(), nullable=True),
    StructField("SEC_POL_ID", StringType(), nullable=True),
    StructField("PATN_EXTRNL_VNDR_ID", StringType(), nullable=False),
    StructField("VST_DT_SK", StringType(), nullable=False),
    StructField("VST_STTUS_CD", StringType(), nullable=True),
    StructField("VST_CLSD_DT_SK", StringType(), nullable=True),
    StructField("BILL_RVWED_DT_SK", StringType(), nullable=True),
    StructField("BILL_RVWED_BY_NM", StringType(), nullable=True),
    StructField("PROV_EXTRNL_VNDR_ID", StringType(), nullable=True),
    StructField("PROV_NTNL_PROV_ID", StringType(), nullable=False),
    StructField("prov_id_carectr", StringType(), nullable=True),
    StructField("PROV_ID_Practioner_lkp", StringType(), nullable=True),
    StructField("src_sys_cd_sk_prov_match", StringType(), nullable=True),
    StructField("PROV_ID_ipa_lkp", StringType(), nullable=True),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False)
])
df_MBRVST_Pkey = (
    spark.read
    .option("sep", ",")
    .option("quote", "^")
    .option("header", "false")
    .csv(f"{adls_path}/key/MBR_VST.pkey.{RunID}.dat", schema=schema_MBRVST_Pkey)
)

################################################################################
# Transformer_94
# Pass-through of columns
################################################################################
df_Transformer_94 = df_MBRVST_Pkey.select(
    col("PRI_NAT_KEY_STRING"),
    col("FIRST_RECYC_TS"),
    col("MBR_VST_SK"),
    col("MBR_UNIQ_KEY"),
    col("VST_ID"),
    col("SRC_SYS_CD_SK"),
    col("CRT_RUN_EXCNT_SK"),
    col("LAST_UPDT_RUN_CYC_EXCNT_SK"),
    col("PRI_POL_ID"),
    col("SEC_POL_ID"),
    col("PATN_EXTRNL_VNDR_ID"),
    col("VST_DT_SK"),
    col("VST_STTUS_CD"),
    col("VST_CLSD_DT_SK"),
    col("BILL_RVWED_DT_SK"),
    col("BILL_RVWED_BY_NM"),
    col("PROV_EXTRNL_VNDR_ID"),
    col("PROV_NTNL_PROV_ID"),
    col("prov_id_carectr"),
    col("PROV_ID_Practioner_lkp"),
    col("src_sys_cd_sk_prov_match"),
    col("PROV_ID_ipa_lkp"),
    col("CRT_RUN_CYC_EXCTN_SK")
)

################################################################################
# LkupFkey (PxLookup) => left join with df_db2_MBR on MBR_UNIQ_KEY
################################################################################
df_LkupFkey = (
    df_Transformer_94.alias("DSLink95")
    .join(
        df_db2_MBR.alias("MbrLkup"),
        (col("DSLink95.MBR_UNIQ_KEY") == col("MbrLkup.MBR_UNIQ_KEY")),
        "left"
    )
)

df_LkupFkey_out = df_LkupFkey.select(
    col("DSLink95.PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    col("DSLink95.FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    col("DSLink95.MBR_VST_SK").alias("MBR_VST_SK"),
    col("DSLink95.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    col("DSLink95.VST_ID").alias("VST_ID"),
    col("DSLink95.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    col("DSLink95.CRT_RUN_EXCNT_SK").alias("CRT_RUN_EXCNT_SK"),
    col("DSLink95.LAST_UPDT_RUN_CYC_EXCNT_SK").alias("LAST_UPDT_RUN_CYC_EXCNT_SK"),
    col("DSLink95.PRI_POL_ID").alias("PRI_POL_ID"),
    col("DSLink95.SEC_POL_ID").alias("SEC_POL_ID"),
    col("DSLink95.PATN_EXTRNL_VNDR_ID").alias("PATN_EXTRNL_VNDR_ID"),
    col("DSLink95.VST_DT_SK").alias("VST_DT_SK"),
    col("DSLink95.VST_STTUS_CD").alias("VST_STTUS_CD"),
    col("DSLink95.VST_CLSD_DT_SK").alias("VST_CLSD_DT_SK"),
    col("DSLink95.BILL_RVWED_DT_SK").alias("BILL_RVWED_DT_SK"),
    col("DSLink95.BILL_RVWED_BY_NM").alias("BILL_RVWED_BY_NM"),
    col("DSLink95.PROV_EXTRNL_VNDR_ID").alias("PROV_EXTRNL_VNDR_ID"),
    col("DSLink95.PROV_NTNL_PROV_ID").alias("PROV_NTNL_PROV_ID"),
    col("DSLink95.prov_id_carectr").alias("prov_id_carectr"),
    col("DSLink95.PROV_ID_Practioner_lkp").alias("PROV_ID_Practioner_lkp"),
    col("DSLink95.src_sys_cd_sk_prov_match").alias("src_sys_cd_sk_prov_match"),
    col("DSLink95.PROV_ID_ipa_lkp").alias("PROV_ID_ipa_lkp"),
    col("DSLink95.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("MbrLkup.MBR_SK").alias("MBR_SK")
)

################################################################################
# Transformer_102
# Pass-through
################################################################################
df_Transformer_102 = df_LkupFkey_out.select(
    col("PRI_NAT_KEY_STRING"),
    col("FIRST_RECYC_TS"),
    col("MBR_VST_SK"),
    col("MBR_UNIQ_KEY"),
    col("VST_ID"),
    col("SRC_SYS_CD_SK"),
    col("CRT_RUN_EXCNT_SK"),
    col("LAST_UPDT_RUN_CYC_EXCNT_SK"),
    col("PRI_POL_ID"),
    col("SEC_POL_ID"),
    col("PATN_EXTRNL_VNDR_ID"),
    col("VST_DT_SK"),
    col("VST_STTUS_CD"),
    col("VST_CLSD_DT_SK"),
    col("BILL_RVWED_DT_SK"),
    col("BILL_RVWED_BY_NM"),
    col("PROV_EXTRNL_VNDR_ID"),
    col("PROV_NTNL_PROV_ID"),
    col("prov_id_carectr"),
    col("PROV_ID_Practioner_lkp"),
    col("src_sys_cd_sk_prov_match"),
    col("PROV_ID_ipa_lkp"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("MBR_SK")
)

################################################################################
# Lookup_97 (PxLookup) => multiple left joins with df_Copy_136P3/4/5
#   Join #1: df_Copy_136P3 on (prov_id_carectr, src_sys_cd_sk_prov_match)
#   Join #2: df_Copy_136P4 on (PROV_ID_Practioner_lkp, src_sys_cd_sk_prov_match)
#   Join #3: df_Copy_136P5 on (PROV_ID_ipa_lkp, src_sys_cd_sk_prov_match)
################################################################################
df_Lookup_97_step1 = (
    df_Transformer_102.alias("DSLink109")
    .join(
        df_Copy_136P3.alias("DSLink139"),
        ((col("DSLink109.prov_id_carectr") == col("DSLink139.PROV_ID")) &
         (col("DSLink109.src_sys_cd_sk_prov_match") == col("DSLink139.SRC_SYS_CD_SK"))),
        "left"
    )
)
df_Lookup_97_step2 = df_Lookup_97_step1.join(
    df_Copy_136P4.alias("DSLink140"),
    ((col("DSLink109.PROV_ID_Practioner_lkp") == col("DSLink140.PROV_ID")) &
     (col("DSLink109.src_sys_cd_sk_prov_match") == col("DSLink140.SRC_SYS_CD_SK"))),
    "left"
)
df_Lookup_97 = df_Lookup_97_step2.join(
    df_Copy_136P5.alias("DSLink141"),
    ((col("DSLink109.PROV_ID_ipa_lkp") == col("DSLink141.PROV_ID")) &
     (col("DSLink109.src_sys_cd_sk_prov_match") == col("DSLink141.SRC_SYS_CD_SK"))),
    "left"
)

df_Lookup_97_out = df_Lookup_97.select(
    col("DSLink109.PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    col("DSLink109.FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    col("DSLink109.MBR_VST_SK").alias("MBR_VST_SK"),
    col("DSLink109.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    col("DSLink109.VST_ID").alias("VST_ID"),
    col("DSLink109.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    col("DSLink109.CRT_RUN_EXCNT_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("DSLink109.LAST_UPDT_RUN_CYC_EXCNT_SK").alias("LAST_UPDT_RUN_CYC_EXCNT_SK"),
    col("DSLink109.PRI_POL_ID").alias("PRI_POL_ID"),
    col("DSLink109.SEC_POL_ID").alias("SEC_POL_ID"),
    col("DSLink109.PATN_EXTRNL_VNDR_ID").alias("PATN_EXTRNL_VNDR_ID"),
    col("DSLink109.VST_DT_SK").alias("VST_DT_SK"),
    col("DSLink109.VST_STTUS_CD").alias("VST_STTUS_CD"),
    col("DSLink109.VST_CLSD_DT_SK").alias("VST_CLSD_DT_SK"),
    col("DSLink109.BILL_RVWED_DT_SK").alias("BILL_RVWED_DT_SK"),
    col("DSLink109.BILL_RVWED_BY_NM").alias("BILL_RVWED_BY_NM"),
    col("DSLink109.PROV_EXTRNL_VNDR_ID").alias("PROV_EXTRNL_VNDR_ID"),
    col("DSLink109.PROV_NTNL_PROV_ID").alias("SVC_PROV_NTNL_PROV_ID"),
    col("DSLink140.PROV_SK").alias("SVC_PROV_SK"),
    col("DSLink139.PROV_SK").alias("REL_GRP_PROV_SK"),
    col("DSLink141.PROV_SK").alias("IPA_PROV_SK"),
    col("DSLink109.MBR_SK").alias("MBR_SK"),
    col("DSLink109.prov_id_carectr").alias("prov_id_carectr"),
    col("DSLink109.PROV_ID_Practioner_lkp").alias("PROV_ID_Practioner_lkp"),
    col("DSLink109.PROV_ID_ipa_lkp").alias("PROV_ID_ipa_lkp"),
    col("DSLink109.src_sys_cd_sk_prov_match").alias("src_sys_cd_sk_prov_match")
)

################################################################################
# Transformer_130
# StageVariables:
#   svmbrskfkeylkpcheck => if IsNull(MBR_SK) then 'Y' else 'N'
#   svrelgrpprovskfkeylkpcheck => if IsNull(REL_GRP_PROV_SK) then 'Y' else 'N'
#   svsvcprovskfkeylkpcheck => if IsNull(SVC_PROV_SK) then 'Y' else 'N'
#   svipaprovskfKeylkpcheck => if IsNull(IPA_PROV_SK) then 'Y' else 'N'
# Outputs:
#   1) Lnk_PrmRate_Main
#   2) Lnk_PrmRateUNK
#   3) Lnk_PrmRateNA
#   4) lnkTierModLkpFail, lnkPrmTypLkupFail, lnkMethCdLkupFail, lnkTierClmLkpFail  => FKey fail constraints
################################################################################

# Add columns for stage variables
df_Transformer_130_stagevars = (
    df_Lookup_97_out
    .withColumn(
        "svmbrskfkeylkpcheck",
        when(col("MBR_SK").isNull(), lit("Y")).otherwise(lit("N"))
    )
    .withColumn(
        "svrelgrpprovskfkeylkpcheck",
        when(col("REL_GRP_PROV_SK").isNull(), lit("Y")).otherwise(lit("N"))
    )
    .withColumn(
        "svsvcprovskfkeylkpcheck",
        when(col("SVC_PROV_SK").isNull(), lit("Y")).otherwise(lit("N"))
    )
    .withColumn(
        "svipaprovskfKeylkpcheck",
        when(col("IPA_PROV_SK").isNull(), lit("Y")).otherwise(lit("N"))
    )
)

# Create a window to mimic @INROWNUM and produce row_number
# We have no partitioning info, so we do a single partition.
w = Window.orderBy(lit(1))
df_Transformer_130_enriched = df_Transformer_130_stagevars.withColumn("ds_row_num", row_number().over(w))

###############################################################################
# Lnk_PrmRate_Main (no constraint on row, the entire set). 
# Apply the output column transformations (If IsNull() then 0, else original).
###############################################################################
df_Lnk_PrmRate_Main = df_Transformer_130_enriched.select(
    col("MBR_VST_SK"),
    col("MBR_UNIQ_KEY"),
    col("VST_ID"),
    col("SRC_SYS_CD_SK"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("PRI_POL_ID"),
    col("SEC_POL_ID"),
    col("PATN_EXTRNL_VNDR_ID"),
    col("VST_STTUS_CD"),
    col("VST_CLSD_DT_SK"),
    col("BILL_RVWED_DT_SK"),
    col("BILL_RVWED_BY_NM"),
    col("PROV_EXTRNL_VNDR_ID"),
    col("SVC_PROV_NTNL_PROV_ID"),
    col("VST_DT_SK"),
    when(col("SVC_PROV_SK").isNull(), lit(0)).otherwise(col("SVC_PROV_SK")).alias("SVC_PROV_SK"),
    when(col("REL_GRP_PROV_SK").isNull(), lit(0)).otherwise(col("REL_GRP_PROV_SK")).alias("REL_GRP_PROV_SK"),
    when(col("IPA_PROV_SK").isNull(), lit(0)).otherwise(col("IPA_PROV_SK")).alias("REL_IPA_PROV_SK"),
    when(col("MBR_SK").isNull(), lit(0)).otherwise(col("MBR_SK")).alias("MBR_SK")
)

###############################################################################
# Lnk_PrmRateUNK (constraint: ((@INROWNUM - 1) * @NUMPARTITIONS + @PARTITIONNUM + 1) = 1)
# We'll approximate by ds_row_num = 1
###############################################################################
df_Lnk_PrmRateUNK = df_Transformer_130_enriched.filter(col("ds_row_num") == lit(1)).select(
    lit(0).alias("MBR_VST_SK"),
    lit(0).alias("MBR_UNIQ_KEY"),
    lit("UNK").alias("VST_ID"),
    lit(0).alias("SRC_SYS_CD_SK"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit("UNK").alias("PRI_POL_ID"),
    lit("UNK").alias("SEC_POL_ID"),
    lit("UNK").alias("PATN_EXTRNL_VNDR_ID"),
    lit("UNK").alias("VST_STTUS_CD"),
    lit("2199-12-31").alias("VST_CLSD_DT_SK"),
    lit("1753-01-01").alias("BILL_RVWED_DT_SK"),
    lit("UNK").alias("BILL_RVWED_BY_NM"),
    lit("UNK").alias("PROV_EXTRNL_VNDR_ID"),
    lit("UNK").alias("SVC_PROV_NTNL_PROV_ID"),
    lit("1753-01-01").alias("VST_DT_SK"),
    lit(0).alias("SVC_PROV_SK"),
    lit(0).alias("REL_GRP_PROV_SK"),
    lit(0).alias("REL_IPA_PROV_SK"),
    lit(0).alias("MBR_SK")
)

###############################################################################
# Lnk_PrmRateNA (constraint: ((@INROWNUM - 1) * @NUMPARTITIONS + @PARTITIONNUM + 1) = 1)
# We'll approximate by ds_row_num = 1
###############################################################################
df_Lnk_PrmRateNA = df_Transformer_130_enriched.filter(col("ds_row_num") == lit(1)).select(
    lit(1).alias("MBR_VST_SK"),
    lit(1).alias("MBR_UNIQ_KEY"),
    lit("NA").alias("VST_ID"),
    lit(1).alias("SRC_SYS_CD_SK"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit("NA").alias("PRI_POL_ID"),
    lit("NA").alias("SEC_POL_ID"),
    lit("NA").alias("PATN_EXTRNL_VNDR_ID"),
    lit("NA").alias("VST_STTUS_CD"),
    lit("2199-12-31").alias("VST_CLSD_DT_SK"),
    lit("1753-01-01").alias("BILL_RVWED_DT_SK"),
    lit("NA").alias("BILL_RVWED_BY_NM"),
    lit("NA").alias("PROV_EXTRNL_VNDR_ID"),
    lit("NA").alias("SVC_PROV_NTNL_PROV_ID"),
    lit("1753-01-01").alias("VST_DT_SK"),
    lit(1).alias("SVC_PROV_SK"),
    lit(1).alias("REL_GRP_PROV_SK"),
    lit(1).alias("REL_IPA_PROV_SK"),
    lit(1).alias("MBR_SK")
)

###############################################################################
# The FKey Fail constraints
#   lnkTierModLkpFail => svmbrskfkeylkpcheck='Y'
#   lnkPrmTypLkupFail => svrelgrpprovskfkeylkpcheck='Y'
#   lnkMethCdLkupFail => svsvcprovskfkeylkpcheck='Y'
#   lnkTierClmLkpFail => svipaprovskfKeylkpcheck='Y'
###############################################################################

# We define a small helper to select columns for the FKey fail outputs
def select_fkey_fail(df, constraint):
    return (
        df.filter(constraint)
        .select(
            col("MBR_VST_SK").alias("PRI_SK"),
            col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
            col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
            lit(DSJobName).alias("JOB_NM"),
            lit("FKLOOKUP").alias("ERROR_TYP"),
            # PHYSCL_FILE_NM differs for MBR vs PROV depending on link
            # We'll handle that outside since each link sets a different literal
            col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
            col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK"),
            col("src_sys_cd_sk_prov_match").alias("__TEMP_SRC_SYS_CD_SK_PROV"),
            col("prov_id_carectr").alias("__TEMP_PROV_ID_CARECTR"),
            col("PROV_ID_Practioner_lkp").alias("__TEMP_PROV_ID_Practioner_lkp"),
            col("PROV_ID_ipa_lkp").alias("__TEMP_PROV_ID_ipa_lkp"),
            col("MBR_UNIQ_KEY").alias("__TEMP_MBR_UNIQ_KEY")
        )
    )

# 1) lnkTierModLkpFail => MBR => constraint: svmbrskfkeylkpcheck='Y'
df_tierModLkpFail = select_fkey_fail(df_Transformer_130_enriched, col("svmbrskfkeylkpcheck") == lit("Y")).withColumn(
    "PHYSCL_FILE_NM", lit("MBR")
).withColumn(
    "FRGN_NAT_KEY_STRING",
    concat(lit(SrcSysCd), lit(";"), col("__TEMP_MBR_UNIQ_KEY"))
)

# 2) lnkPrmTypLkupFail => PROV => constraint: svrelgrpprovskfkeylkpcheck='Y'
#   FRGN_NAT_KEY_STRING => if empty => 'UNK' else ...
df_prmTypLkupFail = select_fkey_fail(df_Transformer_130_enriched, col("svrelgrpprovskfkeylkpcheck") == lit("Y"))
df_prmTypLkupFail = (
    df_prmTypLkupFail
    .withColumn("PHYSCL_FILE_NM", lit("PROV"))
    .withColumn(
        "FRGN_NAT_KEY_STRING",
        when(
            (coalesce(col("__TEMP_SRC_SYS_CD_SK_PROV"), lit("")) +
             coalesce(col("__TEMP_PROV_ID_CARECTR"), lit(""))) == lit(""),
            lit("UNK")
        ).otherwise(
            coalesce(col("__TEMP_SRC_SYS_CD_SK_PROV"), lit("")) +
            coalesce(col("__TEMP_PROV_ID_CARECTR"), lit(""))
        )
    )
)

# 3) lnkMethCdLkupFail => PROV => constraint: svsvcprovskfkeylkpcheck='Y'
#   FRGN_NAT_KEY_STRING => if empty => 'UNK' else ...
df_methCdLkupFail = select_fkey_fail(df_Transformer_130_enriched, col("svsvcprovskfkeylkpcheck") == lit("Y"))
df_methCdLkupFail = (
    df_methCdLkupFail
    .withColumn("PHYSCL_FILE_NM", lit("PROV"))
    .withColumn(
        "FRGN_NAT_KEY_STRING",
        when(
            (coalesce(col("__TEMP_SRC_SYS_CD_SK_PROV"), lit("")) +
             coalesce(col("__TEMP_PROV_ID_Practioner_lkp"), lit(""))) == lit(""),
            lit("UNK")
        ).otherwise(
            coalesce(col("__TEMP_SRC_SYS_CD_SK_PROV"), lit("")) +
            coalesce(col("__TEMP_PROV_ID_Practioner_lkp"), lit(""))
        )
    )
)

# 4) lnkTierClmLkpFail => PROV => constraint: svipaprovskfKeylkpcheck='Y'
#   FRGN_NAT_KEY_STRING => if empty => 'UNK' else ...
df_tierClmLkpFail = select_fkey_fail(df_Transformer_130_enriched, col("svipaprovskfKeylkpcheck") == lit("Y"))
df_tierClmLkpFail = (
    df_tierClmLkpFail
    .withColumn("PHYSCL_FILE_NM", lit("PROV"))
    .withColumn(
        "FRGN_NAT_KEY_STRING",
        when(
            (coalesce(col("__TEMP_SRC_SYS_CD_SK_PROV"), lit("")) +
             coalesce(col("__TEMP_PROV_ID_ipa_lkp"), lit(""))) == lit(""),
            lit("UNK")
        ).otherwise(
            coalesce(col("__TEMP_SRC_SYS_CD_SK_PROV"), lit("")) +
            coalesce(col("__TEMP_PROV_ID_ipa_lkp"), lit(""))
        )
    )
)

# Final selection of columns for each fail set
common_fkey_fail_cols = [
    col("PRI_SK"),
    col("PRI_NAT_KEY_STRING"),
    col("SRC_SYS_CD_SK"),
    col("JOB_NM"),
    col("ERROR_TYP"),
    col("PHYSCL_FILE_NM"),
    col("FRGN_NAT_KEY_STRING"),
    col("FIRST_RECYC_TS"),
    col("JOB_EXCTN_SK")
]
df_tierModLkpFail_final = df_tierModLkpFail.select(common_fkey_fail_cols)
df_prmTypLkupFail_final = df_prmTypLkupFail.select(common_fkey_fail_cols)
df_methCdLkupFail_final = df_methCdLkupFail.select(common_fkey_fail_cols)
df_tierClmLkpFail_final = df_tierClmLkpFail.select(common_fkey_fail_cols)

################################################################################
# FnlFkeyFailures (PxFunnel)
# Union of the four fail dataframes
################################################################################
df_FnlFkeyFailures = df_tierModLkpFail_final.unionByName(df_tierClmLkpFail_final) \
    .unionByName(df_prmTypLkupFail_final) \
    .unionByName(df_methCdLkupFail_final)

################################################################################
# seq_FkeyFailedFile (PxSequentialFile) => write .dat
#   File path: f"{adls_path}/fkey_failures/{PrefixFkeyFailedFileName}.{DSJobName}.dat"
#   delimiter=",", quote="^"
################################################################################
# No "SqlType" with explicit lengths in the funnel for these columns, so we do not rpad.
write_files(
    df_FnlFkeyFailures.select(
        "PRI_SK",
        "PRI_NAT_KEY_STRING",
        "SRC_SYS_CD_SK",
        "JOB_NM",
        "ERROR_TYP",
        "PHYSCL_FILE_NM",
        "FRGN_NAT_KEY_STRING",
        "FIRST_RECYC_TS",
        "JOB_EXCTN_SK"
    ),
    f"{adls_path}/fkey_failures/{PrefixFkeyFailedFileName}.{DSJobName}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)

################################################################################
# fnl_NA_UNK_Streams (PxFunnel)
# Union of Lnk_PrmRate_Main, Lnk_PrmRateUNK, Lnk_PrmRateNA
################################################################################
df_fnl_NA_UNK_Streams = df_Lnk_PrmRate_Main.unionByName(df_Lnk_PrmRateUNK).unionByName(df_Lnk_PrmRateNA)

################################################################################
# Seq_MBRVST_FKEY (PxSequentialFile) => write MBR_VST.dat
#   path: f"{adls_path}/load/MBR_VST.dat"
#   delimiter=",", quote="\""
#   We see "SqlType=char(10)" on VST_CLSD_DT_SK, BILL_RVWED_DT_SK, VST_DT_SK => rpad to length=10
################################################################################
df_fnl_NA_UNK_Streams_to_write = df_fnl_NA_UNK_Streams.select(
    col("MBR_VST_SK"),
    col("MBR_UNIQ_KEY"),
    col("VST_ID"),
    col("SRC_SYS_CD_SK"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("PRI_POL_ID"),
    col("SEC_POL_ID"),
    col("PATN_EXTRNL_VNDR_ID"),
    col("VST_STTUS_CD"),
    when(col("VST_CLSD_DT_SK").isNull(), lit("")).otherwise(col("VST_CLSD_DT_SK")).alias("VST_CLSD_DT_SK"),
    when(col("BILL_RVWED_DT_SK").isNull(), lit("")).otherwise(col("BILL_RVWED_DT_SK")).alias("BILL_RVWED_DT_SK"),
    col("BILL_RVWED_BY_NM"),
    col("PROV_EXTRNL_VNDR_ID"),
    col("SVC_PROV_NTNL_PROV_ID"),
    when(col("VST_DT_SK").isNull(), lit("")).otherwise(col("VST_DT_SK")).alias("VST_DT_SK"),
    col("SVC_PROV_SK"),
    col("REL_GRP_PROV_SK"),
    col("REL_IPA_PROV_SK"),
    col("MBR_SK")
)

# Apply rpad for the char(10) columns specifically
df_fnl_NA_UNK_Streams_char_padded = (
    df_fnl_NA_UNK_Streams_to_write
    .withColumn("VST_CLSD_DT_SK", when(col("VST_CLSD_DT_SK")=="", lit("")).otherwise(col("VST_CLSD_DT_SK")))
    .withColumn("VST_CLSD_DT_SK", rpad(col("VST_CLSD_DT_SK"), 10, " "))
    .withColumn("BILL_RVWED_DT_SK", when(col("BILL_RVWED_DT_SK")=="", lit("")).otherwise(col("BILL_RVWED_DT_SK")))
    .withColumn("BILL_RVWED_DT_SK", rpad(col("BILL_RVWED_DT_SK"), 10, " "))
    .withColumn("VST_DT_SK", when(col("VST_DT_SK")=="", lit("")).otherwise(col("VST_DT_SK")))
    .withColumn("VST_DT_SK", rpad(col("VST_DT_SK"), 10, " "))
)

write_files(
    df_fnl_NA_UNK_Streams_char_padded,
    f"{adls_path}/load/MBR_VST.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)