# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC CALLED BY : BCAFEPMedClmExtrSeq
# MAGIC 
# MAGIC PROCESSING : Reads the BCA Fep Med Land file and puts the data into the claim  provider common record format and runs through primary key using Shared container ClmProvPkey
# MAGIC 
# MAGIC Developer                    Date         Project #                     Change Description                                        Development Project          Code Reviewer               Date Reviewed  
# MAGIC ------------------           ------------------      ----------------                  --------------------------------------------------------------         ------------------------------------       ----------------------------            ----------------
# MAGIC Sudhir Bomshetty    2017-09-28    5781 - HEDIS            Initial Programming                                          IntegrateDev2                     Kalyan Neelam               2017-10-18
# MAGIC 
# MAGIC Sudhir Bomshetty    2018-03-28    5781 - HEDIS            Changed logic for PROV_ID, TAX_ID            IntegrateDev2                     Jaideep Mankala            04/02/2018
# MAGIC 
# MAGIC Sudhir Bomshetty    2018-04-27    5781 - HEDIS            Added the NTNL_PROV_ID                          IntegrateDev2                      Kalyan Neelam               2018-05-01
# MAGIC 
# MAGIC Karthik Chintalapani   2020-04-08   US168193           Changed the logic for the NTNL_PROV_ID     IntegrateDev1                 Jaideep Mankala               2020-04-08
# MAGIC                                                                                                   to include the min(PROV_ID) for BCA.

# MAGIC BCA FEP Claim Provider Extract
# MAGIC This container is used in:
# MAGIC ArgusClmProvExtr
# MAGIC PCSClmProvExtr
# MAGIC NascoClmProvExtr
# MAGIC FctsClmProvExtr
# MAGIC WellDyneClmProvExtr
# MAGIC MCSourceClmProvExtr
# MAGIC MedicaidClmProvExtr
# MAGIC MedtrakClmProvExtr
# MAGIC BCBSSCClmProvExtr
# MAGIC BCBSSCMedClmProvExtr
# MAGIC BCAClmProvExtr
# MAGIC 
# MAGIC These programs need to be re-compiled when logic changes
# MAGIC Writing Sequential File to /key
# MAGIC The file is created in the BCAFEPMedClmPreProcExtr job
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
    IntegerType,
    DecimalType
)
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
RunID = get_widget_value('RunID','')
CurrDate = get_widget_value('CurrDate','2018-05-03')
SrcSysCd = get_widget_value('SrcSysCd','BCA')
CurrRunCycle = get_widget_value('CurrRunCycle','205')
SrcSysCdSk = get_widget_value('SrcSysCdSk','-2022470479')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

# MAGIC %run ../../../../../shared_containers/PrimaryKey/ClmProvPK
# COMMAND ----------

schema_BCAFepClmLand = StructType([
    StructField("CLM_ID", StringType(), False),
    StructField("CLM_LN_NO", IntegerType(), False),
    StructField("ADJ_FROM_CLM_ID", StringType(), False),
    StructField("ADJ_TO_CLM_ID", StringType(), False),
    StructField("CLM_STTUS_CD", StringType(), False),
    StructField("MBR_UNIQ_KEY", IntegerType(), False),
    StructField("SUB_UNIQ_KEY", IntegerType(), False),
    StructField("GRP_ID", StringType(), False),
    StructField("DOB", StringType(), True),
    StructField("GNDR_CD", StringType(), True),
    StructField("SUB_ID", StringType(), False),
    StructField("MBR_SFX_NO", StringType(), True),
    StructField("SRC_SYS", StringType(), False),
    StructField("RCRD_ID", StringType(), False),
    StructField("ADJ_NO", StringType(), True),
    StructField("PERFORMING_PROV_ID", StringType(), True),
    StructField("FEP_PROD", StringType(), True),
    StructField("MBR_ID", StringType(), True),
    StructField("BILL_TYP__CD", StringType(), True),
    StructField("FEP_SVC_LOC_CD", StringType(), True),
    StructField("IP_CLM_TYP_IN", StringType(), True),
    StructField("DRG_VRSN_IN", StringType(), True),
    StructField("DRG_CD", StringType(), True),
    StructField("PATN_STTUS_CD", StringType(), True),
    StructField("CLM_CLS_IN", StringType(), True),
    StructField("CLM_DENIED_FLAG", StringType(), True),
    StructField("ILNS_DT", StringType(), True),
    StructField("IP_CLM_BEG_DT", StringType(), True),
    StructField("IP_CLM_DSCHG_DT", StringType(), True),
    StructField("CLM_SVC_DT_BEG", StringType(), True),
    StructField("CLM_SVC_DT_END", StringType(), True),
    StructField("FCLTY_CLM_STMNT_BEG_DT", StringType(), True),
    StructField("FCLTY_CLM_STMNT_END_DT", StringType(), True),
    StructField("CLM_PRCS_DT", StringType(), True),
    StructField("IP_ADMS_CT", StringType(), True),
    StructField("NO_COV_DAYS", IntegerType(), True),
    StructField("DIAG_CDNG_TYP", StringType(), True),
    StructField("PRI_DIAG_CD", StringType(), True),
    StructField("PRI_DIAG_POA_IN", StringType(), True),
    StructField("ADM_DIAG_CD", StringType(), True),
    StructField("ADM_DIAG_POA_IN", StringType(), True),
    StructField("OTHR_DIAG_CD_1", StringType(), True),
    StructField("OTHR_DIAG_CD_1_POA_IN", StringType(), True),
    StructField("OTHR_DIAG_CD_2", StringType(), True),
    StructField("OTHR_DIAG_CD_2_POA_IN", StringType(), True),
    StructField("OTHR_DIAG_CD_3", StringType(), True),
    StructField("OTHR_DIAG_CD_3_POA_IN", StringType(), True),
    StructField("OTHR_DIAG_CD_4", StringType(), True),
    StructField("OTHR_DIAG_CD_4_POA_IN", StringType(), True),
    StructField("OTHR_DIAG_CD_5", StringType(), True),
    StructField("OTHR_DIAG_CD_5_POA_IN", StringType(), True),
    StructField("OTHR_DIAG_CD_6", StringType(), True),
    StructField("OTHR_DIAG_CD_6_POA_IN", StringType(), True),
    StructField("OTHR_DIAG_CD_7", StringType(), True),
    StructField("OTHR_DIAG_CD_7_POA_IN", StringType(), True),
    StructField("OTHR_DIAG_CD_8", StringType(), True),
    StructField("OTHR_DIAG_CD_8_POA_IN", StringType(), True),
    StructField("OTHR_DIAG_CD_9", StringType(), True),
    StructField("OTHR_DIAG_CD_9_POA_IN", StringType(), True),
    StructField("OTHR_DIAG_CD_10", StringType(), True),
    StructField("OTHR_DIAG_CD_10_POA_IN", StringType(), True),
    StructField("OTHR_DIAG_CD_11", StringType(), True),
    StructField("OTHR_DIAG_CD_11_POA_IN", StringType(), True),
    StructField("OTHR_DIAG_CD_12", StringType(), True),
    StructField("OTHR_DIAG_CD_12_POA_IN", StringType(), True),
    StructField("OTHR_DIAG_CD_13", StringType(), True),
    StructField("OTHR_DIAG_CD_13_POA_IN", StringType(), True),
    StructField("OTHR_DIAG_CD_14", StringType(), True),
    StructField("OTHR_DIAG_CD_14_POA_IN", StringType(), True),
    StructField("OTHR_DIAG_CD_15", StringType(), True),
    StructField("OTHR_DIAG_CD_15_POA_IN", StringType(), True),
    StructField("OTHR_DIAG_CD_16", StringType(), True),
    StructField("OTHR_DIAG_CD_16_POA_IN", StringType(), True),
    StructField("OTHR_DIAG_CD_17", StringType(), True),
    StructField("OTHR_DIAG_CD_17_POA_IN", StringType(), True),
    StructField("OTHR_DIAG_CD_18", StringType(), True),
    StructField("OTHR_DIAG_CD_18_POA_IN", StringType(), True),
    StructField("OTHR_DIAG_CD_19", StringType(), True),
    StructField("OTHR_DIAG_CD_19_POA_IN", StringType(), True),
    StructField("OTHR_DIAG_CD_20", StringType(), True),
    StructField("OTHR_DIAG_CD_20_POA_IN", StringType(), True),
    StructField("OTHR_DIAG_CD_21", StringType(), True),
    StructField("OTHR_DIAG_CD_21_POA_IN", StringType(), True),
    StructField("OTHR_DIAG_CD_22", StringType(), True),
    StructField("OTHR_DIAG_CD_22_POA_IN", StringType(), True),
    StructField("OTHR_DIAG_CD_23", StringType(), True),
    StructField("OTHR_DIAG_CD_23_POA_IN", StringType(), True),
    StructField("OTHR_DIAG_CD_24", StringType(), True),
    StructField("OTHR_DIAG_CD_24_POA_IN", StringType(), True),
    StructField("PROC_CDNG_TYP", StringType(), True),
    StructField("PRINCIPLE_PROC_CD", StringType(), True),
    StructField("PRINCIPLE_PROC_CD_DT", StringType(), True),
    StructField("OTHR_PROC_CD_1", StringType(), True),
    StructField("OTHR_PROC_CD_1_DT", StringType(), True),
    StructField("OTHR_PROC_CD_2", StringType(), True),
    StructField("OTHR_PROC_CD_2_DT", StringType(), True),
    StructField("OTHR_PROC_CD_3", StringType(), True),
    StructField("OTHR_PROC_CD_3_DT", StringType(), True),
    StructField("OTHR_PROC_CD_4", StringType(), True),
    StructField("OTHR_PROC_CD_4_DT", StringType(), True),
    StructField("OTHR_PROC_CD_5", StringType(), True),
    StructField("OTHR_PROC_CD_5_DT", StringType(), True),
    StructField("OTHR_PROC_CD_6", StringType(), True),
    StructField("OTHR_PROC_CD_6_DT", StringType(), True),
    StructField("OTHR_PROC_CD_7", StringType(), True),
    StructField("OTHR_PROC_CD_7_DT", StringType(), True),
    StructField("OTHR_PROC_CD_8", StringType(), True),
    StructField("OTHR_PROC_CD_8_DT", StringType(), True),
    StructField("OTHR_PROC_CD_9", StringType(), True),
    StructField("OTHR_PROC_CD_9_DT", StringType(), True),
    StructField("OTHR_PROC_CD_10", StringType(), True),
    StructField("OTHR_PROC_CD_10_DT", StringType(), True),
    StructField("OTHR_PROC_CD_11", StringType(), True),
    StructField("OTHR_PROC_CD_11_DT", StringType(), True),
    StructField("OTHR_PROC_CD_12", StringType(), True),
    StructField("OTHR_PROC_CD_12_DT", StringType(), True),
    StructField("OTHR_PROC_CD_13", StringType(), True),
    StructField("OTHR_PROC_CD_13_DT", StringType(), True),
    StructField("OTHR_PROC_CD_14", StringType(), True),
    StructField("OTHR_PROC_CD_14_DT", StringType(), True),
    StructField("OTHR_PROC_CD_15", StringType(), True),
    StructField("OTHR_PROC_CD_15_DT", StringType(), True),
    StructField("OTHR_PROC_CD_16", StringType(), True),
    StructField("OTHR_PROC_CD_16_DT", StringType(), True),
    StructField("OTHR_PROC_CD_17", StringType(), True),
    StructField("OTHR_PROC_CD_17_DT", StringType(), True),
    StructField("OTHR_PROC_CD_18", StringType(), True),
    StructField("OTHR_PROC_CD_18_DT", StringType(), True),
    StructField("OTHR_PROC_CD_19", StringType(), True),
    StructField("OTHR_PROC_CD_19_DT", StringType(), True),
    StructField("OTHR_PROC_CD_20", StringType(), True),
    StructField("OTHR_PROC_CD_20_DT", StringType(), True),
    StructField("OTHR_PROC_CD_21", StringType(), True),
    StructField("OTHR_PROC_CD_21_DT", StringType(), True),
    StructField("OTHR_PROC_CD_22", StringType(), True),
    StructField("OTHR_PROC_CD_22_DT", StringType(), True),
    StructField("OTHR_PROC_CD_23", StringType(), True),
    StructField("OTHR_PROC_CD_23_DT", StringType(), True),
    StructField("OTHR_PROC_CD_24", StringType(), True),
    StructField("OTHR_PROC_CD_24_DT", StringType(), True),
    StructField("RVNU_CD", StringType(), True),
    StructField("PROC_CD_NON_ICD", StringType(), True),
    StructField("PROC_CD_MOD_1", StringType(), True),
    StructField("PROC_CD_MOD_2", StringType(), True),
    StructField("PROC_CD_MOD_3", StringType(), True),
    StructField("PROC_CD_MOD_4", StringType(), True),
    StructField("CLM_UNIT", DecimalType(38,10), True),
    StructField("CLM_LN_TOT_ALL_SVC_CHRG_AMT", DecimalType(38,10), True),
    StructField("CLM_LN_PD_AMT", DecimalType(38,10), True),
    StructField("CLM_ALT_AMT_PD_1", DecimalType(38,10), True),
    StructField("CLM_ALT_AMT_PD_2", DecimalType(38,10), True),
    StructField("LOINC_CD", StringType(), True),
    StructField("CLM_TST_RSLT", StringType(), True),
    StructField("ALT_CLM_TST_RSLT", StringType(), True),
    StructField("DRUG_CD", StringType(), True),
    StructField("DRUG_CLM_INCUR_DT", StringType(), True),
    StructField("CLM_TREAT_DURATN", StringType(), True),
    StructField("DATA_SRC", StringType(), True),
    StructField("SUPLMT_DATA_SRC_TYP", StringType(), True),
    StructField("ADTR_APRV_STTUS", StringType(), True),
    StructField("RUN_DT", StringType(), True),
    StructField("PD_DAYS", IntegerType(), True),
    StructField("PERFORMING_NTNL_PROV_ID", StringType(), True)
])

df_BCAFepClmLand = (
    spark.read.format("csv")
    .option("header", "false")
    .option("quote", "\"")
    .schema(schema_BCAFepClmLand)
    .load(f"{adls_path}/verified/BCAFEPCMedClm_ClmLanding.dat.{RunID}")
)

df_Transformer_34 = df_BCAFepClmLand.select(
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_LN_NO").alias("CLM_LN_NO"),
    F.col("PERFORMING_PROV_ID").alias("PROV_ID"),
    F.col("PERFORMING_NTNL_PROV_ID").alias("PERFORMING_NTNL_PROV_ID")
)

df_IDS_PROV_Prov = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"SELECT PROV_ID, TAX_ID, NTNL_PROV_ID FROM {IDSOwner}.PROV PROV, {IDSOwner}.CD_MPPNG CD_MPPNG WHERE PROV.SRC_SYS_CD_SK = CD_MPPNG.CD_MPPNG_SK AND TRGT_CD = 'BCA' ORDER BY PROV_SK DESC"
    )
    .load()
)

df_IDS_PROV_ntnl_provid = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"SELECT PROV2.NTNL_PROV_ID, PROV2.PROV_ID, PROV2.TAX_ID FROM (SELECT PROV.NTNL_PROV_ID, MIN(PROV.PROV_ID) PROV_ID, CD2.TRGT_CD FROM {IDSOwner}.PROV PROV, {IDSOwner}.CD_MPPNG CD2 WHERE PROV.NTNL_PROV_ID<>'NA' AND PROV.SRC_SYS_CD_SK = CD2.CD_MPPNG_SK AND PROV.TERM_DT_SK > '#CurrentDate#' GROUP BY PROV.NTNL_PROV_ID, CD2.TRGT_CD) PROV1, {IDSOwner}.CD_MPPNG CD, {IDSOwner}.PROV PROV2 WHERE PROV2.PROV_ID = PROV1.PROV_ID AND PROV2.SRC_SYS_CD_SK = CD.CD_MPPNG_SK AND CD.TRGT_CD= 'BCA'"
    )
    .load()
)

df_hf_prov_intermediate = df_IDS_PROV_Prov
df_hf_ntnl_provid_intermediate = df_IDS_PROV_ntnl_provid

df_hf_prov_dedup = dedup_sort(
    df_hf_prov_intermediate,
    partition_cols=["PROV_ID"],
    sort_cols=[("PROV_ID", "A")]
)
df_hf_prov = df_hf_prov_dedup.select(
    "PROV_ID",
    "TAX_ID",
    "NTNL_PROV_ID"
)

df_hf_ntnl_provid_dedup = dedup_sort(
    df_hf_ntnl_provid_intermediate,
    partition_cols=["NTNL_PROV_ID"],
    sort_cols=[("NTNL_PROV_ID", "A")]
)
df_hf_ntnl_provid = df_hf_ntnl_provid_dedup.select(
    "NTNL_PROV_ID",
    "PROV_ID",
    "TAX_ID"
)

df_BusinessRules_joined = (
    df_Transformer_34.alias("BcaFepData")
    .join(df_hf_prov.alias("hf_prov"), F.col("BcaFepData.PROV_ID") == F.col("hf_prov.PROV_ID"), "left")
    .join(df_hf_ntnl_provid.alias("hf_ntnl_provid"), F.col("BcaFepData.PERFORMING_NTNL_PROV_ID") == F.col("hf_ntnl_provid.NTNL_PROV_ID"), "left")
)

is_not_numeric = ~F.col("BcaFepData.PERFORMING_NTNL_PROV_ID").substr(F.lit(1), F.lit(1)).rlike("^[0-9]$")
prov_id_expr = F.when(
    F.col("BcaFepData.PERFORMING_NTNL_PROV_ID").isNotNull(),
    F.when(
        is_not_numeric,
        F.when(F.col("hf_prov.PROV_ID").isNull(), F.lit("UNK")).otherwise(F.col("hf_prov.PROV_ID"))
    ).otherwise(
        F.when(F.col("hf_ntnl_provid.PROV_ID").isNull(), F.lit("UNK")).otherwise(F.col("hf_ntnl_provid.PROV_ID"))
    )
).otherwise(F.lit("NA"))

tax_id_expr = F.when(
    F.col("BcaFepData.PERFORMING_NTNL_PROV_ID").isNotNull(),
    F.when(
        is_not_numeric,
        F.when(F.col("hf_prov.TAX_ID").isNotNull(), F.col("hf_prov.TAX_ID")).otherwise(F.lit("NA"))
    ).otherwise(
        F.when(F.col("hf_ntnl_provid.TAX_ID").isNotNull(), F.col("hf_ntnl_provid.TAX_ID")).otherwise(F.lit("NA"))
    )
).otherwise(F.lit("NA"))

ntnl_prov_id_expr = F.when(
    F.col("BcaFepData.PERFORMING_NTNL_PROV_ID").isNotNull(),
    F.when(
        is_not_numeric,
        F.when(F.col("hf_prov.NTNL_PROV_ID").isNull(), F.lit("UNK")).otherwise(F.col("hf_prov.NTNL_PROV_ID"))
    ).otherwise(
        F.col("BcaFepData.PERFORMING_NTNL_PROV_ID")
    )
).otherwise(F.lit("NA"))

df_BusinessRules = (
    df_BusinessRules_joined
    .withColumn("CLM_PROV_SK", F.lit(0))
    .withColumn("CLM_ID", F.col("BcaFepData.CLM_ID"))
    .withColumn("CLM_PROV_ROLE_TYP_CD", F.lit("SVC"))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(0))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(0))
    .withColumn("PROV_ID", prov_id_expr)
    .withColumn("TAX_ID", tax_id_expr)
    .withColumn("SVC_FCLTY_LOC_NTNL_PROV_ID", F.lit("NA"))
    .withColumn("NTNL_PROV_ID", ntnl_prov_id_expr)
    .select(
        "CLM_PROV_SK",
        "CLM_ID",
        "CLM_PROV_ROLE_TYP_CD",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "PROV_ID",
        "TAX_ID",
        "SVC_FCLTY_LOC_NTNL_PROV_ID",
        "NTNL_PROV_ID"
    )
)

df_SnapshotTemp = (
    df_BusinessRules
    .withColumn("SRC_SYS_CD_SK", F.lit(SrcSysCdSk))
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", F.lit(0))
    .withColumn("INSRT_UPDT_CD", F.lit("I"))
    .withColumn("DISCARD_IN", F.lit("N"))
    .withColumn("PASS_THRU_IN", F.lit("Y"))
    .withColumn("FIRST_RECYC_DT", F.lit(CurrDate))
    .withColumn("ERR_CT", F.lit(0))
    .withColumn("RECYCLE_CT", F.lit(0))
    .withColumn("SRC_SYS_CD", F.lit(SrcSysCd))
    .withColumn("PRI_KEY_STRING", F.concat(F.lit(SrcSysCd), F.lit(";"), F.col("CLM_ID"), F.lit(";"), F.col("CLM_PROV_ROLE_TYP_CD")))
    .withColumn(
        "NTNL_PROV_ID",
        F.when(
            F.col("NTNL_PROV_ID").isNull() | (F.length(F.trim(F.col("NTNL_PROV_ID"))) == 0),
            F.lit("UNK")
        ).otherwise(F.col("NTNL_PROV_ID"))
    )
)

df_AllCol = df_SnapshotTemp.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD"),
    F.col("DISCARD_IN"),
    F.col("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("CLM_PROV_SK"),
    F.col("SRC_SYS_CD_SK"),
    F.col("CLM_ID"),
    F.col("CLM_PROV_ROLE_TYP_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("PROV_ID"),
    F.col("TAX_ID"),
    F.col("SVC_FCLTY_LOC_NTNL_PROV_ID"),
    F.col("NTNL_PROV_ID")
)

df_Transform = df_SnapshotTemp.select(
    F.col("SRC_SYS_CD_SK"),
    F.col("CLM_ID"),
    F.col("CLM_PROV_ROLE_TYP_CD")
)

df_SnapShot = df_SnapshotTemp.select(
    F.col("CLM_ID").cast(StringType()).alias("CLM_ID"),
    F.col("CLM_PROV_ROLE_TYP_CD").cast(StringType()).alias("CLM_PROV_ROLE_TYP_CD"),
    F.col("PROV_ID").cast(StringType()).alias("PROV_ID")
)

params_ClmProvPK = {
    "CurrRunCycle": CurrRunCycle,
    "SrcSysCd": SrcSysCd,
    "$IDSOwner": IDSOwner
}
df_ClmProvPK_Key = ClmProvPK(df_AllCol, df_Transform, params_ClmProvPK)

df_BCAFepClmProvExtr = df_ClmProvPK_Key.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "CLM_PROV_SK",
    "SRC_SYS_CD_SK",
    "CLM_ID",
    "CLM_PROV_ROLE_TYP_CD",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "PROV_ID",
    "TAX_ID",
    "SVC_FCLTY_LOC_NTNL_PROV_ID",
    "NTNL_PROV_ID"
)

df_BCAFepClmProvExtr_rpad = (
    df_BCAFepClmProvExtr
    .withColumn("INSRT_UPDT_CD", F.rpad("INSRT_UPDT_CD", 10, " "))
    .withColumn("DISCARD_IN", F.rpad("DISCARD_IN", 1, " "))
    .withColumn("PASS_THRU_IN", F.rpad("PASS_THRU_IN", 1, " "))
    .withColumn("TAX_ID", F.rpad("TAX_ID", 9, " "))
    .withColumn("NTNL_PROV_ID", F.rpad("NTNL_PROV_ID", F.length("NTNL_PROV_ID") + 0, " "))
    .withColumn("SRC_SYS_CD", F.rpad("SRC_SYS_CD", F.length("SRC_SYS_CD") + 0, " "))
    .withColumn("PROV_ID", F.rpad("PROV_ID", F.length("PROV_ID") + 0, " "))
    .withColumn("SVC_FCLTY_LOC_NTNL_PROV_ID", F.rpad("SVC_FCLTY_LOC_NTNL_PROV_ID", F.length("SVC_FCLTY_LOC_NTNL_PROV_ID") + 0, " "))
    .withColumn("CLM_ID", F.rpad("CLM_ID", F.length("CLM_ID") + 0, " "))
    .withColumn("CLM_PROV_ROLE_TYP_CD", F.rpad("CLM_PROV_ROLE_TYP_CD", F.length("CLM_PROV_ROLE_TYP_CD") + 0, " "))
)

write_files(
    df_BCAFepClmProvExtr_rpad,
    f"{adls_path}/key/BCAFEPMedClmProvExtr.BCAFEPMedClmProv.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)

df_Transformer_in = df_SnapShot.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID"),
    F.col("CLM_PROV_ROLE_TYP_CD"),
    F.col("PROV_ID")
)

df_Transformer_out = (
    df_Transformer_in
    .withColumn(
        "CLM_PROV_ROLE_TYP_CD_SK",
        GetFkeyCodes(
            SrcSysCd,
            F.lit(0),
            F.lit("CLAIM PROVIDER ROLE TYPE"),
            F.col("CLM_PROV_ROLE_TYP_CD"),
            F.lit("X")
        )
    )
)

df_B_CLM_PROV = df_Transformer_out.select(
    F.col("SRC_SYS_CD_SK"),
    F.col("CLM_ID").cast(StringType()).alias("CLM_ID"),
    F.col("CLM_PROV_ROLE_TYP_CD_SK"),
    F.col("PROV_ID").cast(StringType()).alias("PROV_ID")
)

df_B_CLM_PROV_rpad = (
    df_B_CLM_PROV
    .withColumn("CLM_ID", F.rpad("CLM_ID", 18, " "))
    .withColumn("PROV_ID", F.rpad("PROV_ID", 12, " "))
)

write_files(
    df_B_CLM_PROV_rpad,
    f"{adls_path}/load/B_CLM_PROV.{SrcSysCd}.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)