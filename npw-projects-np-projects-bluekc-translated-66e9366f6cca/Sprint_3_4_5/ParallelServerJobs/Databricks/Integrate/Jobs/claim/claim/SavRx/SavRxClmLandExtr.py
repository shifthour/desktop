# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC Â© Copyright 2010 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC 
# MAGIC CALLED BY : 
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:      SavRx Drug Claim Landing Extract. Looks up against the MBR_ENR table to get the right member unique key for a Member for a Claim
# MAGIC                                
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                                          Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                                                                                            --------------------------------       -------------------------------   ----------------------------      
# MAGIC Kaushik Kapoor                                5828                      Initial Programming                                                                                                                                      IntegrateDev2                 Kalyan Neelam           2018-02-26 
# MAGIC Kaushik Kapoor       2018-03-16        5828                     Removing Cd_mppng query to get CLM_STTUS_CD
# MAGIC                                                                                        and updated remove UNK assign logic from PDX_NTNL_PROV_ID column                                             IntegrateDev2                 Jaideep Mankala        03/21/2018
# MAGIC Kaushik Kapoor       2018-04-12       5828                      Adding P_PBM_GRP_XREF lookup for assigning GRP_ID having UNK                                                   IntegrateDev2                 Kalyan Neelam           2018-04-13
# MAGIC Ramu Avula            2020-08-28        6264-                     Added SUBMT_PROD_ID_QLFR,                                                                                                            IntegrateDev5
# MAGIC                                                           PBM Phase II              CNTNGNT_THER_FLAG,CNTNGNT_THER_SCHD,                                      
# MAGIC                                                           - Government Programs  CLNT_PATN_PAY_ATRBD_PROD_AMT,
# MAGIC                                                                                                 CLNT_PATN_PAY_ATRBD_SLS_TAX_AMT,CLNT_PATN_PAY_ATRBD_SLS_TAX_AMT
# MAGIC                                                                                                  CLNT_PATN_PAY_ATRBD_PRCSR_FEE_AMT,CLNT_PATN_PAY_ATRBD_NTWK_AMT,
# MAGIC                                                                                                   LOB_IN  fields

# MAGIC SavRx Claim Landing Extract
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType
)
from pyspark.sql.functions import (
    col, lit, when, length, upper, rpad
)
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
RunID = get_widget_value('RunID','')
SrcSysCd = get_widget_value('SrcSysCd','')
RunDate = get_widget_value('RunDate','')

# 1) Stage: P_PBM_GRP_XREF (DB2Connector) --> df_P_PBM_GRP_XREF
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query_P_PBM_GRP_XREF = (
    f"SELECT DISTINCT\n"
    f"PBM_GRP_ID,\n"
    f"GRP_ID\n"
    f"FROM {IDSOwner}.P_PBM_GRP_XREF XREF\n"
    f"WHERE UPPER(XREF.SRC_SYS_CD) = '{SrcSysCd.upper()}'\n"
    f"AND '{RunDate}' BETWEEN DATE(XREF.EFF_DT) AND DATE(XREF.TERM_DT)"
)
df_P_PBM_GRP_XREF = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_P_PBM_GRP_XREF)
    .load()
)

# 2) Stage: hf_SavRx_clm_land_grpxref (CHashedFileStage, scenario A deduplicate on PBM_GRP_ID)
df_hf_SavRx_clm_land_grpxref = dedup_sort(
    df_P_PBM_GRP_XREF,
    partition_cols=["PBM_GRP_ID"],
    sort_cols=[]
)

# 3) Stage: SavRxPreProcExtr (CSeqFileStage) --> df_SavRxPreProcExtr
schema_SavRxPreProcExtr = StructType([
    StructField("PDX_NO", StringType(), True),
    StructField("RX_NO", StringType(), True),
    StructField("FILL_DT", StringType(), True),
    StructField("NDC_NO", StringType(), True),
    StructField("NEW_OR_RFL_CD", StringType(), True),
    StructField("METRIC_QTY", StringType(), True),
    StructField("DAYS_SUPL_AMT", StringType(), True),
    StructField("INGR_CST_AMT", StringType(), True),
    StructField("DISPNS_FEE_AMT", StringType(), True),
    StructField("COPAY_AMT", StringType(), True),
    StructField("SLS_TAX_AMT", StringType(), True),
    StructField("BILL_AMT", StringType(), True),
    StructField("PATN_FIRST_NM", StringType(), True),
    StructField("PATN_LAST_NM", StringType(), True),
    StructField("BRTH_DT", StringType(), True),
    StructField("CARDHLDR_ID_NO", StringType(), True),
    StructField("GRP_NO", StringType(), True),
    StructField("PRSCRBR_ID", StringType(), True),
    StructField("CARDHLDR_FIRST_NM", StringType(), True),
    StructField("CARDHLDR_LAST_NM", StringType(), True),
    StructField("PRAUTH_NO", StringType(), True),
    StructField("DISPENSE_AS_WRTN_PROD_SEL_CD", StringType(), True),
    StructField("OTHR_COV_CD", StringType(), True),
    StructField("CMPND_CD", StringType(), True),
    StructField("DRUG_TYP", StringType(), True),
    StructField("PRSCRBR_LAST_NM", StringType(), True),
    StructField("OTHR_PAYOR_AMT", StringType(), True),
    StructField("CLM_TYP", StringType(), True),
    StructField("ADJDCT_DT", StringType(), True),
    StructField("INCNTV_FEE_AMT", StringType(), True),
    StructField("FRMLRY_FLAG", StringType(), True),
    StructField("BILL_BSS_CD", StringType(), True),
    StructField("USL_AND_CUST_CHRG_AMT", StringType(), True),
    StructField("PD_DT", StringType(), True),
    StructField("REF_NO", StringType(), True),
    StructField("DEDCT_AMT", StringType(), True),
    StructField("PRSCRBR_NTNL_PROV_ID", StringType(), True),
    StructField("PDX_NTNL_PROV_ID", StringType(), True),
    StructField("PATN_SSN", StringType(), True),
    StructField("CARDHLDR_SSN", StringType(), True),
    StructField("CARDHLDR_BRTH_DT", StringType(), True),
    StructField("PAR_PDX_IN", StringType(), True),
    StructField("GNDR_CD", StringType(), True),
    StructField("MBR_UNIQ_KEY", IntegerType(), False),
    StructField("GRP_ID", StringType(), True),
    StructField("CLAIM_ID", StringType(), False)
])
df_SavRxPreProcExtr = (
    spark.read.format("csv")
    .option("header", "false")
    .option("sep", ",")
    .option("quote", "\"")
    .schema(schema_SavRxPreProcExtr)
    .load(f"{adls_path}/verified/{SrcSysCd}_Clm_PreProc.dat.{RunID}")
)

# 4) Stage: KeyCol (CTransformerStage)
df_KeyCol = df_SavRxPreProcExtr.select(
    when(col("CLAIM_ID").isNotNull(), col("CLAIM_ID")).alias("CLAIM_ID"),
    when(col("MBR_UNIQ_KEY").isNotNull(), col("MBR_UNIQ_KEY")).alias("MBR_UNIQ_KEY"),
    when(col("FILL_DT").isNotNull(), col("FILL_DT")).alias("FILLED_DT"),
    lit("SAVRX").alias("SRC_SYS_CD"),                       # WhereExpression => 'SAVRX'
    col("CLAIM_ID").alias("CLAIM_ID_2"),
    col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY_2"),
    col("FILL_DT").alias("FILL_DT_2"),
    col("PDX_NO"),
    col("RX_NO"),
    col("NDC_NO"),
    col("NEW_OR_RFL_CD"),
    col("METRIC_QTY"),
    col("DAYS_SUPL_AMT"),
    col("INGR_CST_AMT"),
    col("DISPNS_FEE_AMT"),
    col("COPAY_AMT"),
    col("SLS_TAX_AMT"),
    col("BILL_AMT"),
    col("PATN_FIRST_NM"),
    col("PATN_LAST_NM"),
    col("BRTH_DT"),
    col("CARDHLDR_ID_NO"),
    col("GRP_NO"),
    col("PRSCRBR_ID"),
    col("CARDHLDR_FIRST_NM"),
    col("CARDHLDR_LAST_NM"),
    col("PRAUTH_NO"),
    col("DISPENSE_AS_WRTN_PROD_SEL_CD"),
    col("OTHR_COV_CD"),
    col("CMPND_CD"),
    col("DRUG_TYP"),
    col("PRSCRBR_LAST_NM"),
    col("OTHR_PAYOR_AMT"),
    col("CLM_TYP"),
    col("ADJDCT_DT"),
    col("INCNTV_FEE_AMT"),
    col("FRMLRY_FLAG"),
    col("BILL_BSS_CD"),
    col("USL_AND_CUST_CHRG_AMT"),
    col("PD_DT"),
    col("REF_NO"),
    col("DEDCT_AMT"),
    col("PRSCRBR_NTNL_PROV_ID"),
    col("PDX_NTNL_PROV_ID"),
    col("PATN_SSN"),
    col("CARDHLDR_SSN"),
    col("CARDHLDR_BRTH_DT"),
    col("PAR_PDX_IN"),
    col("GNDR_CD"),
    col("GRP_ID").alias("GRP_ID_2"),
    when(
        col("CLM_TYP").isNull() | (length(trim(col("CLM_TYP"))) == 0),
        "0"
    ).otherwise(upper(trim(col("CLM_TYP")))).alias("CLM_STTUS_CD")
)

# Rename final columns to match exactly the output pin definition from KeyCol->Dedup
df_KeyCol_renamed = df_KeyCol.select(
    col("CLAIM_ID").alias("CLM_ID"),
    col("MBR_UNIQ_KEY").alias("MBR_UNQ_KEY"),
    col("FILLED_DT").alias("FILLED_DT"),
    col("SRC_SYS_CD"),
    col("CLAIM_ID_2").alias("CLAIM_ID"),
    col("MBR_UNIQ_KEY_2").alias("MBR_UNIQ_KEY"),
    col("FILL_DT_2").alias("FILL_DT"),
    col("PDX_NO"),
    col("RX_NO"),
    col("NDC_NO"),
    col("NEW_OR_RFL_CD"),
    col("METRIC_QTY"),
    col("DAYS_SUPL_AMT"),
    col("INGR_CST_AMT"),
    col("DISPNS_FEE_AMT"),
    col("COPAY_AMT"),
    col("SLS_TAX_AMT"),
    col("BILL_AMT"),
    col("PATN_FIRST_NM"),
    col("PATN_LAST_NM"),
    col("BRTH_DT"),
    col("CARDHLDR_ID_NO"),
    col("GRP_NO"),
    col("PRSCRBR_ID"),
    col("CARDHLDR_FIRST_NM"),
    col("CARDHLDR_LAST_NM"),
    col("PRAUTH_NO"),
    col("DISPENSE_AS_WRTN_PROD_SEL_CD"),
    col("OTHR_COV_CD"),
    col("CMPND_CD"),
    col("DRUG_TYP"),
    col("PRSCRBR_LAST_NM"),
    col("OTHR_PAYOR_AMT"),
    col("CLM_TYP"),
    col("ADJDCT_DT"),
    col("INCNTV_FEE_AMT"),
    col("FRMLRY_FLAG"),
    col("BILL_BSS_CD"),
    col("USL_AND_CUST_CHRG_AMT"),
    col("PD_DT"),
    col("REF_NO"),
    col("DEDCT_AMT"),
    col("PRSCRBR_NTNL_PROV_ID"),
    col("PDX_NTNL_PROV_ID"),
    col("PATN_SSN"),
    col("CARDHLDR_SSN"),
    col("CARDHLDR_BRTH_DT"),
    col("PAR_PDX_IN"),
    col("GNDR_CD"),
    col("GRP_ID_2").alias("GRP_ID"),
    col("CLM_STTUS_CD")
)

# 5) Stage: hf_SavRx_clm_land_filededupe (CHashedFileStage, scenario A deduplicate)
df_hf_SavRx_clm_land_filededupe = dedup_sort(
    df_KeyCol_renamed,
    partition_cols=["CLM_ID", "MBR_UNQ_KEY", "FILLED_DT"],
    sort_cols=[]
)

# 6) Stage: IDS_MBR (DB2Connector) --> df_IDS_MBR
extract_query_IDS_MBR = (
    f"SELECT\n"
    f"       CLM_ID,\n"
    f"       MBR_UNIQ_KEY,\n"
    f"       Order\n"
    f"FROM (\n"
    f"(SELECT \n"
    f"      DRUG.CLM_ID,\n"
    f"      ENR.MBR_UNIQ_KEY,\n"
    f"      MAX(ENR.TERM_DT_SK) TERM_DT_SK,\n"
    f"      MAX(ENR.EFF_DT_SK) EFF_DT_SK,\n"
    f"      4 as Order\n"
    f"FROM {IDSOwner}.MBR_ENR ENR,\n"
    f"     {IDSOwner}.CD_MPPNG CD1,\n"
    f"     {IDSOwner}.W_DRUG_ENR_MATCH DRUG\n"
    f"WHERE ENR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK = CD1.CD_MPPNG_SK AND\n"
    f"      CD1.TRGT_CD = 'MED' AND\n"
    f"      ENR.ELIG_IN = 'Y' AND\n"
    f"      ENR.MBR_UNIQ_KEY = DRUG.MBR_UNIQ_KEY AND\n"
    f"      ENR.EFF_DT_SK <= DRUG.FILL_DT_SK AND\n"
    f"      ENR.TERM_DT_SK >= DRUG.FILL_DT_SK\n"
    f"GROUP BY DRUG.CLM_ID, ENR.MBR_UNIQ_KEY)\n"
    f"UNION\n"
    f"(SELECT \n"
    f"      DRUG.CLM_ID,\n"
    f"      ENR.MBR_UNIQ_KEY,\n"
    f"      MAX(ENR.TERM_DT_SK) TERM_DT_SK,\n"
    f"      MAX(ENR.EFF_DT_SK) EFF_DT_SK,\n"
    f"      3 as Order\n"
    f"FROM {IDSOwner}.MBR_ENR ENR,\n"
    f"     {IDSOwner}.CD_MPPNG CD1,\n"
    f"     {IDSOwner}.W_DRUG_ENR_MATCH DRUG\n"
    f"WHERE ENR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK = CD1.CD_MPPNG_SK AND\n"
    f"      CD1.TRGT_CD = 'MED' AND\n"
    f"      ENR.ELIG_IN = 'Y' AND\n"
    f"      ENR.MBR_UNIQ_KEY = DRUG.MBR_UNIQ_KEY\n"
    f"GROUP BY DRUG.CLM_ID, ENR.MBR_UNIQ_KEY)\n"
    f"UNION\n"
    f"(SELECT \n"
    f"      DRUG.CLM_ID,\n"
    f"      ENR.MBR_UNIQ_KEY,\n"
    f"      MAX(ENR.TERM_DT_SK) TERM_DT_SK,\n"
    f"      MAX(ENR.EFF_DT_SK) EFF_DT_SK,\n"
    f"      2 as Order\n"
    f"FROM {IDSOwner}.MBR_ENR ENR,\n"
    f"     {IDSOwner}.CD_MPPNG CD1,\n"
    f"     {IDSOwner}.W_DRUG_ENR_MATCH DRUG\n"
    f"WHERE ENR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK = CD1.CD_MPPNG_SK AND\n"
    f"      CD1.TRGT_CD = 'MED' AND\n"
    f"      ENR.ELIG_IN = 'N' AND\n"
    f"      ENR.MBR_UNIQ_KEY = DRUG.MBR_UNIQ_KEY AND\n"
    f"      ENR.EFF_DT_SK <= DRUG.FILL_DT_SK AND\n"
    f"      ENR.TERM_DT_SK >= DRUG.FILL_DT_SK\n"
    f"GROUP BY DRUG.CLM_ID, ENR.MBR_UNIQ_KEY)\n"
    f"UNION\n"
    f"(SELECT \n"
    f"      DRUG.CLM_ID,\n"
    f"      ENR.MBR_UNIQ_KEY,\n"
    f"      MAX(ENR.TERM_DT_SK) TERM_DT_SK,\n"
    f"      MAX(ENR.EFF_DT_SK) EFF_DT_SK,\n"
    f"      1 as Order\n"
    f"FROM {IDSOwner}.MBR_ENR ENR,\n"
    f"     {IDSOwner}.CD_MPPNG CD1,\n"
    f"     {IDSOwner}.W_DRUG_ENR_MATCH DRUG\n"
    f"WHERE ENR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK = CD1.CD_MPPNG_SK AND\n"
    f"      CD1.TRGT_CD = 'MED' AND\n"
    f"      ENR.MBR_UNIQ_KEY = DRUG.MBR_UNIQ_KEY\n"
    f"GROUP BY DRUG.CLM_ID, ENR.MBR_UNIQ_KEY)\n"
    f")\n"
    f"ORDER BY CLM_ID, Order, TERM_DT_SK, EFF_DT_SK, MBR_UNIQ_KEY"
)
df_IDS_MBR = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_IDS_MBR)
    .load()
)

# 7) Stage: hf_SavRx_clm_land_clmid_dedupe (CHashedFileStage, scenario A deduplicate on CLM_ID)
df_hf_SavRx_clm_land_clmid_dedupe = dedup_sort(
    df_IDS_MBR,
    partition_cols=["CLM_ID"],
    sort_cols=[]
)

# 8) Stage: ReKey (CTransformerStage)
df_ReKey = df_hf_SavRx_clm_land_clmid_dedupe

# "Clm_Mbr" link
df_ReKey_Clm_Mbr = df_ReKey.select(
    col("CLM_ID").alias("CLM_ID"),
    col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY")
)

# "ClmIdforErr" link
df_ReKey_ClmIdforErr = df_ReKey.select(
    col("CLM_ID").alias("CLM_ID")
)

# 9) Stage: hf_SavRx_clm_land_clmidlkup_err (CHashedFileStage, scenario A deduplicate on CLM_ID)
df_hf_SavRx_clm_land_clmidlkup_err = dedup_sort(
    df_ReKey_ClmIdforErr,
    partition_cols=["CLM_ID"],
    sort_cols=[]
)

# 10) Stage: hf_SavRx_clm_land_clmid_lkup (CHashedFileStage, scenario A deduplicate on CLM_ID,MBR_UNIQ_KEY)
df_hf_SavRx_clm_land_clmid_lkup = dedup_sort(
    df_ReKey_Clm_Mbr,
    partition_cols=["CLM_ID", "MBR_UNIQ_KEY"],
    sort_cols=[]
)

# 11) Stage: MemberLkp (CTransformerStage)
#   Primary link: df_hf_SavRx_clm_land_filededupe as "SavRx"
#   Lookup link1: df_hf_SavRx_clm_land_clmid_lkup as "Clm_Mbr_Lkp" (left join on CLM_ID & MBR_UNIQ_KEY)
#   Lookup link2: df_hf_SavRx_clm_land_clmidlkup_err as "ErrProc_Lkp" (left join on CLM_ID)
#   Lookup link3: df_hf_SavRx_clm_land_grpxref as "Grp_Xref" (left join on PBM_GRP_ID -> SavRx.GRP_NO)
SavRx = df_hf_SavRx_clm_land_filededupe.alias("SavRx")
Clm_Mbr_Lkp = df_hf_SavRx_clm_land_clmid_lkup.alias("Clm_Mbr_Lkp")
ErrProc_Lkp = df_hf_SavRx_clm_land_clmidlkup_err.alias("ErrProc_Lkp")
Grp_Xref = df_hf_SavRx_clm_land_grpxref.alias("Grp_Xref")

df_MemberLkp_join1 = SavRx.join(
    Clm_Mbr_Lkp,
    on=[
        SavRx["CLAIM_ID"] == Clm_Mbr_Lkp["CLM_ID"],
        SavRx["MBR_UNIQ_KEY"] == Clm_Mbr_Lkp["MBR_UNIQ_KEY"]
    ],
    how="left"
).alias("Joined1")

df_MemberLkp_join2 = df_MemberLkp_join1.join(
    ErrProc_Lkp,
    on=[
        df_MemberLkp_join1["SavRx.CLAIM_ID"] == ErrProc_Lkp["CLM_ID"]
    ],
    how="left"
).alias("Joined2")

df_MemberLkp_join3 = df_MemberLkp_join2.join(
    Grp_Xref,
    on=[df_MemberLkp_join2["SavRx.GRP_NO"] == Grp_Xref["PBM_GRP_ID"]],
    how="left"
).alias("Joined3")

# Stage Variables (performed as columns)
df_MemberLkp_stagevars = df_MemberLkp_join3.withColumn(
    "svClmTyp",
    when(
        col("SavRx.CLM_TYP").isNull() | (length(trim(col("SavRx.CLM_TYP"))) == 0),
        "0"
    ).otherwise(upper(trim(col("SavRx.CLM_TYP"))))
).withColumn(
    "svPersonCd",
    when(
        col("SavRx.PATN_SSN").isNull() | (length(trim(col("SavRx.PATN_SSN"))) == 0),
        "UNK"
    ).otherwise(
        col("SavRx.PATN_SSN").substr(length(col("SavRx.PATN_SSN")) - 1, 2)
    )
).withColumn(
    "svAdjdctDt",
    trim(col("SavRx.ADJDCT_DT"))  # Then date-format conversions left as an example
).withColumn(
    "svPaidDate",
    trim(col("SavRx.PD_DT"))
).withColumn(
    "svPrscrbrNtnlProvId",
    when(
        col("SavRx.PRSCRBR_NTNL_PROV_ID").isNull(),
        trim(col("SavRx.PRSCRBR_ID"))
    ).otherwise(trim(col("SavRx.PRSCRBR_NTNL_PROV_ID")))
).withColumn(
    "svPatnSSN",
    when(
        col("SavRx.PATN_SSN").isNull() | (length(trim(col("SavRx.PATN_SSN"))) == 0),
        "UNK"
    ).otherwise(trim(col("SavRx.PATN_SSN")).substr(1, 9))
).withColumn(
    "svAdjFrmClm",
    when(
        col("svClmTyp") == "X",
        trim(col("SavRx.CLAIM_ID")).substr(
            lit(1),
            length(trim(col("SavRx.CLAIM_ID"))) - lit(1)
        )
    ).otherwise("NA")
).withColumn(
    "svXrefGrp",
    when(
        col("Grp_Xref.GRP_ID").isNull() | (length(trim(col("Grp_Xref.GRP_ID"))) == 0),
        "UNK"
    ).otherwise(trim(col("Grp_Xref.GRP_ID")))
)

# Now produce the multiple output links from MemberLkp

# Output link: "WriteToFile" => PDX_CLM_STD_INPT_Land
df_WriteToFile = df_MemberLkp_stagevars.select(
    lit("SAVRX").alias("SRC_SYS_CD"),
    when(
        col("SavRx.PD_DT").isNull() | (length(trim(col("SavRx.PD_DT"))) == 0),
        "1753-01-01"
    ).otherwise(col("svPaidDate")).alias("FILE_RCVD_DT"),
    lit(None).alias("RCRD_ID"),
    lit(None).alias("PRCSR_NO"),
    lit(None).alias("BTCH_NO"),
    when(
        col("SavRx.PDX_NO").isNull() | (length(trim(col("SavRx.PDX_NO"))) == 0),
        "UNK"
    ).otherwise(trim(col("SavRx.PDX_NO"))).alias("PDX_NO"),
    when(
        col("SavRx.RX_NO").isNull() | (length(trim(col("SavRx.RX_NO"))) == 0),
        "UNK"
    ).otherwise(trim(col("SavRx.RX_NO"))).alias("RX_NO"),
    col("SavRx.FILL_DT").alias("FILL_DT"),
    when(
        col("SavRx.NDC_NO").isNull() | (length(trim(col("SavRx.NDC_NO"))) == 0),
        "UNK"
    ).otherwise(trim(col("SavRx.NDC_NO"))).alias("NDC"),
    lit(None).alias("DRUG_DESC"),
    when(
        col("SavRx.NEW_OR_RFL_CD").isNull() | (length(trim(col("SavRx.NEW_OR_RFL_CD"))) == 0),
        "UNK"
    ).otherwise(trim(col("SavRx.NEW_OR_RFL_CD"))).alias("NEW_OR_RFL_CD"),
    when(
        col("SavRx.METRIC_QTY").isNull() | (length(trim(col("SavRx.METRIC_QTY"))) == 0),
        "0"
    ).otherwise(trim(col("SavRx.METRIC_QTY"))).alias("METRIC_QTY"),
    when(
        col("SavRx.DAYS_SUPL_AMT").isNull() | (length(trim(col("SavRx.DAYS_SUPL_AMT"))) == 0),
        "0"
    ).otherwise(trim(col("SavRx.DAYS_SUPL_AMT"))).alias("DAYS_SUPL"),
    lit(None).alias("BSS_OF_CST_DTRM"),
    when(
        col("SavRx.INGR_CST_AMT").isNull() | (length(trim(col("SavRx.INGR_CST_AMT"))) == 0),
        "0"
    ).otherwise(trim(col("SavRx.INGR_CST_AMT"))).alias("INGR_CST_AMT"),
    when(
        col("SavRx.DISPNS_FEE_AMT").isNull() | (length(trim(col("SavRx.DISPNS_FEE_AMT"))) == 0),
        "0"
    ).otherwise(trim(col("SavRx.DISPNS_FEE_AMT"))).alias("DISPNS_FEE_AMT"),
    when(
        col("SavRx.COPAY_AMT").isNull() | (length(trim(col("SavRx.COPAY_AMT"))) == 0),
        "0"
    ).otherwise(trim(col("SavRx.COPAY_AMT"))).alias("COPAY_AMT"),
    when(
        col("SavRx.SLS_TAX_AMT").isNull() | (length(trim(col("SavRx.SLS_TAX_AMT"))) == 0),
        "0"
    ).otherwise(trim(col("SavRx.SLS_TAX_AMT"))).alias("SLS_TAX_AMT"),
    when(
        col("SavRx.BILL_AMT").isNull() | (length(trim(col("SavRx.BILL_AMT"))) == 0),
        "0"
    ).otherwise(trim(col("SavRx.BILL_AMT"))).alias("BILL_AMT"),
    col("SavRx.PATN_FIRST_NM").alias("PATN_FIRST_NM"),
    col("SavRx.PATN_LAST_NM").alias("PATN_LAST_NM"),
    when(
        col("SavRx.BRTH_DT").isNull(),
        "1753-01-01"
    ).otherwise(col("SavRx.BRTH_DT")).alias("BRTH_DT"),
    col("SavRx.GNDR_CD").alias("SEX_CD"),
    when(
        col("SavRx.CARDHLDR_ID_NO").isNull() | (length(trim(col("SavRx.CARDHLDR_ID_NO"))) == 0),
        "UNK"
    ).otherwise(trim(col("SavRx.CARDHLDR_ID_NO"))).alias("CARDHLDR_ID_NO"),
    lit(None).alias("RELSHP_CD"),
    when(
        col("SavRx.GRP_ID").isNull() | (length(trim(col("SavRx.GRP_ID"))) == 0) | (trim(col("SavRx.GRP_ID")) == "UNK") | (col("SavRx.GRP_ID") != col("Grp_Xref.GRP_ID")),
        col("svXrefGrp")
    ).otherwise(trim(col("SavRx.GRP_ID"))).alias("GRP_NO"),
    lit(None).alias("HOME_PLN"),
    lit(None).alias("HOST_PLN"),
    when(
        col("SavRx.PRSCRBR_ID").isNull() | (length(trim(col("SavRx.PRSCRBR_ID"))) == 0),
        "UNK"
    ).otherwise(trim(col("SavRx.PRSCRBR_ID"))).alias("PRSCRBR_ID"),
    lit(None).alias("DIAG_CD"),
    col("SavRx.CARDHLDR_FIRST_NM").alias("CARDHLDR_FIRST_NM"),
    col("SavRx.CARDHLDR_LAST_NM").alias("CARDHLDR_LAST_NM"),
    col("SavRx.PRAUTH_NO").alias("PRAUTH_NO"),
    lit(None).alias("PA_MC_SC_NO"),
    lit(None).alias("CUST_LOC"),
    lit(None).alias("RESUB_CYC_CT"),
    lit("1753-01-01").alias("RX_DT"),
    when(
        col("SavRx.DISPENSE_AS_WRTN_PROD_SEL_CD").isNull() | (length(trim(col("SavRx.DISPENSE_AS_WRTN_PROD_SEL_CD"))) == 0),
        "UNK"
    ).otherwise(trim(col("SavRx.DISPENSE_AS_WRTN_PROD_SEL_CD"))).alias("DISPENSE_AS_WRTN_PROD_SEL_CD"),
    col("svPersonCd").alias("PRSN_CD"),
    when(
        col("SavRx.OTHR_COV_CD").isNull() | (length(trim(col("SavRx.OTHR_COV_CD"))) == 0),
        "0"
    ).otherwise(trim(col("SavRx.OTHR_COV_CD"))).alias("OTHR_COV_CD"),
    lit(None).alias("ELIG_CLRFCTN_CD"),
    when(
        col("SavRx.CMPND_CD").isNull() | (length(trim(col("SavRx.CMPND_CD"))) == 0),
        "0"
    ).otherwise(trim(col("SavRx.CMPND_CD"))).alias("CMPND_CD"),
    lit(None).alias("NO_OF_RFLS_AUTH"),
    lit(None).alias("LVL_OF_SVC"),
    lit(None).alias("RX_ORIG_CD"),
    lit(None).alias("RX_DENIAL_CLRFCTN"),
    lit(None).alias("PRI_PRSCRBR"),
    lit(None).alias("CLNC_ID_NO"),
    col("SavRx.DRUG_TYP").alias("DRUG_TYP"),
    col("SavRx.PRSCRBR_LAST_NM").alias("PRSCRBR_LAST_NM"),
    lit("0.00").alias("POSTAGE_AMT"),
    lit(None).alias("UNIT_DOSE_IN"),
    when(
        col("SavRx.OTHR_PAYOR_AMT").isNull(),
        "0.00"
    ).otherwise(col("SavRx.OTHR_PAYOR_AMT")).alias("OTHR_PAYOR_AMT"),
    lit(None).alias("BSS_OF_DAYS_SUPL_DTRM"),
    lit("0.00").alias("FULL_AVG_WHLSL_PRICE"),
    lit(None).alias("EXPNSN_AREA"),
    lit(None).alias("MSTR_CAR"),
    lit(None).alias("SUBCAR"),
    when(
        col("svClmTyp") == "X",
        "R"
    ).otherwise(col("svClmTyp")).alias("CLM_TYP"),
    lit(None).alias("SUBGRP"),
    lit(None).alias("PLN_DSGNR"),
    when(
        col("SavRx.ADJDCT_DT").isNull() | (length(trim(col("SavRx.ADJDCT_DT"))) == 0),
        "UNK"
    ).otherwise(col("svAdjdctDt")).alias("ADJDCT_DT"),
    lit("0.00").alias("ADMIN_FEE_AMT"),
    lit("0.00").alias("CAP_AMT"),
    lit("0.00").alias("INGR_CST_SUB_AMT"),
    lit("0.00").alias("MBR_NON_COPAY_AMT"),
    lit(None).alias("MBR_PAY_CD"),
    when(
        col("SavRx.INCNTV_FEE_AMT").isNull(),
        "0.00"
    ).otherwise(col("SavRx.INCNTV_FEE_AMT")).alias("INCNTV_FEE_AMT"),
    lit("0.00").alias("CLM_ADJ_AMT"),
    lit(None).alias("CLM_ADJ_CD"),
    when(col("SavRx.FRMLRY_FLAG") == "1", "Y").otherwise("N").alias("FRMLRY_FLAG"),
    lit(None).alias("GNRC_CLS_NO"),
    lit(None).alias("THRPTC_CLS_AHFS"),
    when(
        col("SavRx.PDX_NO") == "2623735",
        "Y"
    ).otherwise("N").alias("PDX_TYP"),
    when(
        col("SavRx.BILL_BSS_CD").isNull() | (length(trim(col("SavRx.BILL_BSS_CD"))) == 0),
        "0"
    ).otherwise(trim(col("SavRx.BILL_BSS_CD"))).alias("BILL_BSS_CD"),
    when(
        col("SavRx.USL_AND_CUST_CHRG_AMT").isNull(),
        "0.00"
    ).otherwise(col("SavRx.USL_AND_CUST_CHRG_AMT")).alias("USL_AND_CUST_CHRG_AMT"),
    when(
        col("SavRx.PD_DT").isNull() | (length(trim(col("SavRx.PD_DT"))) == 0),
        "1753-01-01"
    ).otherwise(col("svPaidDate")).alias("PD_DT"),
    lit(None).alias("BNF_CD"),
    lit(None).alias("DRUG_STRG"),
    col("SavRx.MBR_UNIQ_KEY").alias("ORIG_MBR"),
    lit(None).alias("INJRY_DT"),
    lit("0.00").alias("FEE_AMT"),
    when(
        col("SavRx.REF_NO").isNull() | (length(trim(col("SavRx.REF_NO"))) == 0),
        "UNK"
    ).otherwise(trim(col("SavRx.REF_NO"))).alias("REF_NO"),
    lit(None).alias("CLNT_CUST_ID"),
    lit(None).alias("PLN_TYP"),
    lit(None).alias("ADJDCT_REF_NO"),
    lit("0.00").alias("ANCLRY_AMT"),
    lit(None).alias("CLNT_GNRL_PRPS_AREA"),
    lit(None).alias("PRTL_FILL_STTUS_CD"),
    lit("1753-01-01").alias("BILL_DT"),
    lit(None).alias("FSA_VNDR_CD"),
    lit(None).alias("PICA_DRUG_CD"),
    lit("0.00").alias("CLM_AMT"),
    lit("0.00").alias("DSALW_AMT"),
    lit(None).alias("FED_DRUG_CLS_CD"),
    when(
        col("SavRx.DEDCT_AMT").isNull(),
        "0.00"
    ).otherwise(col("SavRx.DEDCT_AMT")).alias("DEDCT_AMT"),
    lit(None).alias("BNF_COPAY_100"),
    lit(None).alias("CLM_PRCS_TYP"),
    lit(None).alias("INDEM_HIER_TIER_NO"),
    lit(None).alias("MCARE_D_COV_DRUG"),
    lit(None).alias("RETRO_LICS_CD"),
    lit("0.00").alias("RETRO_LICS_AMT"),
    lit("0.00").alias("LICS_SBSDY_AMT"),
    lit(None).alias("MCARE_B_DRUG"),
    lit(None).alias("MCARE_B_CLM"),
    lit(None).alias("PRSCRBR_QLFR"),
    col("svPrscrbrNtnlProvId").alias("PRSCRBR_NTNL_PROV_ID"),
    lit(None).alias("PDX_QLFR"),
    trim(col("SavRx.PDX_NTNL_PROV_ID")).alias("PDX_NTNL_PROV_ID"),
    lit("0.00").alias("HLTH_RMBRMT_ARGMT_APLD_AMT"),
    lit(None).alias("THER_CLS"),
    lit(None).alias("HIC_NO"),
    lit(None).alias("HLTH_RMBRMT_ARGMT_FLAG"),
    lit(None).alias("DOSE_CD"),
    lit(None).alias("LOW_INCM"),
    lit(None).alias("RTE_OF_ADMIN"),
    lit(None).alias("DEA_SCHD"),
    lit(None).alias("COPAY_BNF_OPT"),
    lit(None).alias("GNRC_PROD_IN"),
    lit(None).alias("PRSCRBR_SPEC"),
    lit(None).alias("VAL_CD"),
    lit(None).alias("PRI_CARE_PDX"),
    lit(None).alias("OFC_OF_INSPECTOR_GNRL"),
    col("svPatnSSN").alias("PATN_SSN"),
    when(
        col("SavRx.CARDHLDR_SSN").isNull() | (length(trim(col("SavRx.CARDHLDR_SSN"))) == 0),
        "UNK"
    ).otherwise(trim(col("SavRx.CARDHLDR_SSN"))).alias("CARDHLDR_SSN"),
    when(
        col("SavRx.CARDHLDR_BRTH_DT").isNull() | (length(trim(col("SavRx.CARDHLDR_BRTH_DT"))) == 0),
        "1753-01-01"
    ).otherwise(trim(col("SavRx.CARDHLDR_BRTH_DT"))).alias("CARDHLDR_BRTH_DT"),
    lit(None).alias("CARDHLDR_ADDR"),
    lit(None).alias("CARDHLDR_CITY"),
    lit(None).alias("CHADHLDR_ST"),
    lit(None).alias("CARDHLDR_ZIP_CD"),
    lit("0.00").alias("PSL_FMLY_MET_AMT"),
    lit("0.00").alias("PSL_MBR_MET_AMT"),
    lit("0.00").alias("PSL_FMLY_AMT"),
    lit("0.00").alias("DEDCT_FMLY_MET_AMT"),
    lit("0.00").alias("DEDCT_FMLY_AMT"),
    lit("0.00").alias("MOPS_FMLY_AMT"),
    lit("0.00").alias("MOPS_FMLY_MET_AMT"),
    lit("0.00").alias("MOPS_MBR_MET_AMT"),
    lit("0.00").alias("DEDCT_MBR_MET_AMT"),
    lit("0.00").alias("PSL_APLD_AMT"),
    lit("0.00").alias("MOPS_APLD_AMT"),
    when(
        col("SavRx.PAR_PDX_IN").isNull() | (length(trim(col("SavRx.PAR_PDX_IN"))) == 0),
        "N"
    ).otherwise(trim(col("SavRx.PAR_PDX_IN"))).alias("PAR_PDX_IN"),
    lit("0.00").alias("COPAY_PCT_AMT"),
    lit("0.00").alias("COPAY_FLAT_AMT"),
    lit(None).alias("CLM_TRNSMSN_METH"),
    lit(None).alias("RX_NO_2012"),
    trim(col("SavRx.CLAIM_ID")).alias("CLM_ID"),
    upper(trim(col("SavRx.CLM_TYP"))).alias("CLM_STTUS_CD"),
    col("svAdjFrmClm").alias("ADJ_FROM_CLM_ID"),
    lit("NA").alias("ADJ_TO_CLM_ID"),
    lit(None).alias("SUBMT_PROD_ID_QLFR"),
    lit(None).alias("CNTNGNT_THER_FLAG"),
    lit("NA").alias("CNTNGNT_THER_SCHD"),
    lit("0.00").alias("CLNT_PATN_PAY_ATRBD_PROD_AMT"),
    lit("0.00").alias("CLNT_PATN_PAY_ATRBD_SLS_TAX_AMT"),
    lit("0.00").alias("CLNT_PATN_PAY_ATRBD_PRCSR_FEE_AMT"),
    lit("0.00").alias("CLNT_PATN_PAY_ATRBD_NTWK_AMT"),
    lit("NA").alias("LOB_IN")
)

# Before writing, apply rpad for char/varchar columns
# (Length seen in the JSON; if missing length for varchar, use <...>)
# This link has many columns, so we will do them all in a chain:
df_PDX_CLM_STD_INPT_Land = df_WriteToFile \
    .withColumn("FILE_RCVD_DT", rpad(col("FILE_RCVD_DT"), 10, " ")) \
    .withColumn("FILL_DT", rpad(col("FILL_DT"), 10, " ")) \
    .withColumn("BRTH_DT", rpad(col("BRTH_DT"), 10, " ")) \
    .withColumn("RX_DT", rpad(col("RX_DT"), 10, " ")) \
    .withColumn("DISPENSE_AS_WRTN_PROD_SEL_CD", rpad(col("DISPENSE_AS_WRTN_PROD_SEL_CD"), 1, " ")) \
    .withColumn("CLM_TYP", rpad(col("CLM_TYP"), 1, " ")) \
    .withColumn("ADJDCT_DT", rpad(col("ADJDCT_DT"), 10, " ")) \
    .withColumn("FRMLRY_FLAG", rpad(col("FRMLRY_FLAG"), 1, " ")) \
    .withColumn("PDX_TYP", rpad(col("PDX_TYP"), 1, " ")) \
    .withColumn("BILL_BSS_CD", rpad(col("BILL_BSS_CD"), 2, " ")) \
    .withColumn("PD_DT", rpad(col("PD_DT"), 10, " ")) \
    .withColumn("CARDHLDR_BRTH_DT", rpad(col("CARDHLDR_BRTH_DT"), 10, " ")) \
    .withColumn("PICA_DRUG_CD", rpad(col("PICA_DRUG_CD"), 1, " ")) \
    .withColumn("DEDCT_AMT", col("DEDCT_AMT")) \
    .withColumn("PRSCRBR_QLFR", rpad(col("PRSCRBR_QLFR"), 2, " ")) \
    .withColumn("PDX_QLFR", rpad(col("PDX_QLFR"), 2, " ")) \
    .withColumn("LOW_INCM", rpad(col("LOW_INCM"), 1, " ")) \
    .withColumn("RTE_OF_ADMIN", rpad(col("RTE_OF_ADMIN"), 2, " ")) \
    .withColumn("MCARE_D_COV_DRUG", rpad(col("MCARE_D_COV_DRUG"), 1, " ")) \
    .withColumn("RETRO_LICS_CD", rpad(col("RETRO_LICS_CD"), 1, " ")) \
    .withColumn("MCARE_B_DRUG", rpad(col("MCARE_B_DRUG"), 1, " ")) \
    .withColumn("MCARE_B_CLM", rpad(col("MCARE_B_CLM"), 1, " ")) \
    .withColumn("PLN_DSGNR", rpad(col("PLN_DSGNR"), 1, " ")) \
    .withColumn("GNDR_CD", rpad(col("SEX_CD"), 1, " ")) \
    .withColumn("PAR_PDX_IN", rpad(col("PAR_PDX_IN"), 1, " ")) \
    .withColumn("DEA_SCHD", col("DEA_SCHD")) \
    .withColumn("OFC_OF_INSPECTOR_GNRL", rpad(col("OFC_OF_INSPECTOR_GNRL"), 1, " ")) \
    .withColumn("CLM_STTUS_CD", rpad(col("CLM_STTUS_CD"), 1, " ")) \
    .select(*[c for c in df_WriteToFile.columns])  # keep final columns in original order

write_files(
    df_PDX_CLM_STD_INPT_Land,
    f"{adls_path}/verified/PDX_CLM_STD_INPT_Land_{SrcSysCd}.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# Output link: "Load" => W_DRUG_ENR (CSeqFileStage)
df_Load = df_MemberLkp_stagevars.select(
    trim(col("SavRx.CLAIM_ID")).alias("CLM_ID"),
    col("SavRx.FILL_DT").alias("FILL_DT_SK"),
    col("SavRx.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY")
)
# rpad char columns
df_Load_rpad = df_Load.withColumn("FILL_DT_SK", rpad(col("FILL_DT_SK"), 10, " "))
write_files(
    df_Load_rpad.select("CLM_ID","FILL_DT_SK","MBR_UNIQ_KEY"),
    f"{adls_path}/load/W_DRUG_ENR.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# Output link: "MbrFillDt" => hf_SavRx_clm_land_mbr_fill_dt (CHashedFileStage, scenario A deduplicate on MBR_UNIQ_KEY,FILL_DT_SK)
df_MbrFillDt = df_MemberLkp_stagevars.select(
    col("SavRx.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    col("SavRx.FILL_DT").alias("FILL_DT_SK")
)
df_hf_SavRx_clm_land_mbr_fill_dt = dedup_sort(
    df_MbrFillDt,
    partition_cols=["MBR_UNIQ_KEY", "FILL_DT_SK"],
    sort_cols=[]
)

# Next stage: W_DRUG_CLM_PCP (CSeqFileStage)
df_FillDtLoad = df_hf_SavRx_clm_land_mbr_fill_dt.withColumn(
    "FILL_DT_SK", rpad(col("FILL_DT_SK"), 10, " ")
).select(
    "MBR_UNIQ_KEY",
    "FILL_DT_SK"
)
write_files(
    df_FillDtLoad,
    f"{adls_path}/load/W_DRUG_CLM_PCP.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# Output link: "Reject" => hf_SavRx_clm_land_reject (CHashedFileStage, scenario A deduplicate on CLM_ID)
df_Reject = df_MemberLkp_stagevars.where(col("ErrProc_Lkp.CLM_ID").isNull()).select(
    col("SavRx.CLAIM_ID").alias("CLM_ID"),
    col("SavRx.GRP_NO").alias("GRP_NO"),
    col("SavRx.REF_NO").alias("REF_NO"),
    col("SavRx.RX_NO").alias("RX_NO"),
    when(col("SavRx.FILL_DT") == "1753-01-01", lit("")).otherwise(col("SavRx.FILL_DT")).alias("FILL_DT"),
    col("SavRx.PATN_FIRST_NM").alias("PATN_FIRST_NM"),
    col("SavRx.PATN_LAST_NM").alias("PATN_LAST_NM"),
    when(col("SavRx.BRTH_DT") == "1753-01-01", lit("")).otherwise(col("SavRx.BRTH_DT")).alias("BRTH_DT"),
    col("SavRx.PATN_SSN").alias("PATN_SSN"),
    col("SavRx.CARDHLDR_ID_NO").alias("CARDHLDR_ID_NO"),
    col("SavRx.CARDHLDR_FIRST_NM").alias("CARDHLDR_FIRST_NM"),
    col("SavRx.CARDHLDR_LAST_NM").alias("CARDHLDR_LAST_NM"),
    when(col("SavRx.CARDHLDR_BRTH_DT") == "1753-01-01", lit("")).otherwise(col("SavRx.CARDHLDR_BRTH_DT")).alias("CARDHLDR_BRTH_DT"),
    col("SavRx.CARDHLDR_SSN").alias("CARDHLDR_SSN"),
    col("SavRx.GNDR_CD").alias("GNDR_CD")
)
df_hf_SavRx_clm_land_reject = dedup_sort(
    df_Reject,
    partition_cols=["CLM_ID"],
    sort_cols=[]
)

# 12) Stage: DropField (CTransformerStage)
df_DropField = df_hf_SavRx_clm_land_reject.select(
    col("REF_NO").alias("REF_NO"),
    col("GRP_NO").alias("GRP_NO"),
    col("FILL_DT").alias("FILL_DT"),
    col("PATN_FIRST_NM").alias("PATN_FIRST_NM"),
    col("PATN_LAST_NM").alias("PATN_LAST_NM"),
    col("BRTH_DT").alias("BRTH_DT"),
    col("PATN_SSN").alias("PATN_SSN"),
    col("CARDHLDR_ID_NO").alias("CARDHLDR_ID_NO"),
    col("CARDHLDR_FIRST_NM").alias("CARDHLDR_FIRST_NM"),
    col("CARDHLDR_LAST_NM").alias("CARDHLDR_LAST_NM"),
    col("CARDHLDR_BRTH_DT").alias("CARDHLDR_BRTH_DT"),
    col("CARDHLDR_SSN").alias("CARDHLDR_SSN"),
    col("GNDR_CD").alias("GNDR_CD"),
    lit("FAILED ELIGIBILITY").alias("ERR_RSN_STRING")
)

# 13) Stage: MbrEnrNotFound (CSeqFileStage)
# rpad char/varchar columns
df_MbrEnrNotFound = df_DropField.withColumn(
    "FILL_DT", rpad(col("FILL_DT"), 10, " ")
).withColumn(
    "BRTH_DT", rpad(col("BRTH_DT"), 10, " ")
).withColumn(
    "CARDHLDR_BRTH_DT", rpad(col("CARDHLDR_BRTH_DT"), 10, " ")
).withColumn(
    "GNDR_CD", rpad(col("GNDR_CD"), 1, " ")
).withColumn(
    "ERR_RSN_STRING", rpad(col("ERR_RSN_STRING"), 100, " ")
)

write_files(
    df_MbrEnrNotFound.select(
        "REF_NO", "GRP_NO", "FILL_DT", "PATN_FIRST_NM", "PATN_LAST_NM",
        "BRTH_DT", "PATN_SSN", "CARDHLDR_ID_NO", "CARDHLDR_FIRST_NM",
        "CARDHLDR_LAST_NM", "CARDHLDR_BRTH_DT", "CARDHLDR_SSN",
        "GNDR_CD", "ERR_RSN_STRING"
    ),
    f"{adls_path_publish}/external/SavRxDrugClm_NoMbrMatchRecs.csv",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)