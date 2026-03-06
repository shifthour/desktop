# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2023 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC Job Name :  DominionIdsProvXfrm
# MAGIC Calling Job:  DominionProvExtrSeq
# MAGIC 
# MAGIC                            
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                        Date                Project/Altiris #                Change Description                                                        Development Project     Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                                ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Deepika C                       2023-11-24      US 600544                     Initial Programming                                                           IntegrateSITF                  Jeyaprasanna              2024-01-18

# MAGIC Transformed Data will land into a Dataset for Primary Keying job.
# MAGIC 
# MAGIC Data is partitioned on Natural Key Columns.
# MAGIC JobName: DominionIdsProvXfrm
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, lit, when, length, concat_ws, rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# Retrieve parameters
SrcSysCd = get_widget_value("SrcSysCd","")
SrcSysCdSk = get_widget_value("SrcSysCdSk","")
IDSOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")
RunID = get_widget_value("RunID","")
RunIDTimeStamp = get_widget_value("RunIDTimeStamp","")

# Stage: db2_K_CMN_PRCT_Lkp (DB2ConnectorPX)
jdbc_url_db2_K_CMN_PRCT_Lkp, jdbc_props_db2_K_CMN_PRCT_Lkp = get_db_config(ids_secret_name)
extract_query_db2_K_CMN_PRCT_Lkp = f"SELECT CMN_PRCT_SK, SRC_SYS_CD, CMN_PRCT_ID FROM {IDSOwner}.K_CMN_PRCT"
df_db2_K_CMN_PRCT_Lkp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_K_CMN_PRCT_Lkp)
    .options(**jdbc_props_db2_K_CMN_PRCT_Lkp)
    .option("query", extract_query_db2_K_CMN_PRCT_Lkp)
    .load()
)

# Stage: db2_CMN_PRCT (DB2ConnectorPX)
jdbc_url_db2_CMN_PRCT, jdbc_props_db2_CMN_PRCT = get_db_config(ids_secret_name)
extract_query_db2_CMN_PRCT = f"SELECT NTNL_PROV_ID, MIN(CMN_PRCT_SK) AS CMN_PRCT_SK FROM {IDSOwner}.CMN_PRCT GROUP BY NTNL_PROV_ID"
df_db2_CMN_PRCT = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_CMN_PRCT)
    .options(**jdbc_props_db2_CMN_PRCT)
    .option("query", extract_query_db2_CMN_PRCT)
    .load()
)

# Stage: ds_CD_MPPNG_Data (PxDataSet) => Read from parquet
df_ds_CD_MPPNG_Data = spark.read.parquet(f"{adls_path}/ds/CD_MPPNG.parquet")

# Stage: fltr_Cd_MppngData (PxFilter)
df_fltr_Cd_MppngData_filtered = df_ds_CD_MPPNG_Data.filter(
    (col("SRC_SYS_CD") == SrcSysCd)
    & (col("SRC_CLCTN_CD") == "IDS")
    & (col("TRGT_CLCTN_CD") == "IDS")
    & (col("SRC_DOMAIN_NM") == "PROVIDER ENTITY")
    & (col("TRGT_DOMAIN_NM") == "PROVIDER ENTITY")
)
df_fltr_Cd_MppngData_sel = df_fltr_Cd_MppngData_filtered.select(
    col("SRC_CD").alias("SRC_CD"),
    col("CD_MPPNG_SK").alias("CD_MPPNG_SK")
)

# Stage: ds_PROV_Dominion (PxDataSet) => Read from parquet
df_ds_PROV_Dominion = spark.read.parquet(
    f"{adls_path}/ds/PROV.{SrcSysCd}.extr.{RunID}.parquet"
)

# Stage: Xfrm_BusinessLogic (CTransformerStage)
df_Xfrm_BusinessLogic = df_ds_PROV_Dominion.select(
    col("Last_Name").alias("Last_Name"),
    col("First_Name").alias("First_Name"),
    col("Middle_Name").alias("Middle_Name"),
    col("Suffix").alias("Suffix"),
    col("Specialty").alias("Specialty"),
    col("Board_Certified_Indicator").alias("Board_Certified_Indicator"),
    col("National_Provider_Identifer").alias("National_Provider_Identifer"),
    col("Prescriber_Flag").alias("Prescriber_Flag"),
    col("Provider_DEA").alias("Provider_DEA"),
    col("Provider_Identifier").alias("Provider_Identifier"),
    col("Network_Identifier").alias("Network_Identifier"),
    col("Provider_Status").alias("Provider_Status"),
    col("Effective_Date").alias("Effective_Date"),
    col("Term_Date").alias("Term_Date"),
    lit("FACETS").alias("FCTS_SRC_SYS_CD"),
    lit("PRCTR").alias("PROV_ENTY_CD_B"),
    col("State").alias("State"),
    col("Provider_Address_Identifier").alias("Provider_Address_Identifier"),
    col("Provider_Tax_Identifier").alias("Provider_Tax_Identifier"),
    col("Provider_Taxonomy_Code").alias("Provider_Taxonomy_Code")
)

# Stage: Lkup_Codes (PxLookup) with multiple lookup links

# Left join with db2_CMN_PRCT (Ref_CmnPrctSk)
df_Lkup_Codes_join1 = df_Xfrm_BusinessLogic.alias("lnk_Dominion_ProvLkup_in").join(
    df_db2_CMN_PRCT.alias("Ref_CmnPrctSk"),
    (
        (col("lnk_Dominion_ProvLkup_in.CMN_PRCT_SK") == col("Ref_CmnPrctSk.CMN_PRCT_SK"))
        & (col("lnk_Dominion_ProvLkup_in.FCTS_SRC_SYS_CD") == col("Ref_CmnPrctSk.SRC_SYS_CD"))
        & (lit(None).cast("string") == col("Ref_CmnPrctSk.CMN_PRCT_ID"))
        & (col("lnk_Dominion_ProvLkup_in.National_Provider_Identifer") == col("Ref_CmnPrctSk.NTNL_PROV_ID"))
    ),
    how="left"
)

# Left join with fltr_Cd_MppngData_sel (ref_ProvEntyCdSK)
df_Lkup_Codes = df_Lkup_Codes_join1.join(
    df_fltr_Cd_MppngData_sel.alias("ref_ProvEntyCdSK"),
    (
        (col("lnk_Dominion_ProvLkup_in.PROV_ENTY_CD_B") == col("ref_ProvEntyCdSK.SRC_CD"))
        & (lit(None).cast("string") == col("ref_ProvEntyCdSK.CD_MPPNG_SK"))
    ),
    how="left"
)

df_Lkup_Codes_final = df_Lkup_Codes.select(
    col("lnk_Dominion_ProvLkup_in.Last_Name").alias("Last_Name"),
    col("lnk_Dominion_ProvLkup_in.First_Name").alias("First_Name"),
    col("lnk_Dominion_ProvLkup_in.Middle_Name").alias("Middle_Name"),
    col("lnk_Dominion_ProvLkup_in.Suffix").alias("Suffix"),
    col("lnk_Dominion_ProvLkup_in.Specialty").alias("Specialty"),
    col("lnk_Dominion_ProvLkup_in.Board_Certified_Indicator").alias("Board_Certified_Indicator"),
    col("lnk_Dominion_ProvLkup_in.National_Provider_Identifer").alias("National_Provider_Identifer"),
    col("lnk_Dominion_ProvLkup_in.Prescriber_Flag").alias("Prescriber_Flag"),
    col("lnk_Dominion_ProvLkup_in.Provider_DEA").alias("Provider_DEA"),
    col("lnk_Dominion_ProvLkup_in.Provider_Identifier").alias("Provider_Identifier"),
    col("lnk_Dominion_ProvLkup_in.Network_Identifier").alias("Network_Identifier"),
    col("lnk_Dominion_ProvLkup_in.Provider_Status").alias("Provider_Status"),
    col("lnk_Dominion_ProvLkup_in.Effective_Date").alias("Effective_Date"),
    col("lnk_Dominion_ProvLkup_in.Term_Date").alias("Term_Date"),
    col("lnk_Dominion_ProvLkup_in.State").alias("State"),
    col("lnk_Dominion_ProvLkup_in.Provider_Address_Identifier").alias("Provider_Address_Identifier"),
    col("lnk_Dominion_ProvLkup_in.Provider_Tax_Identifier").alias("Provider_Tax_Identifier"),
    col("lnk_Dominion_ProvLkup_in.Provider_Taxonomy_Code").alias("Provider_Taxonomy_Code"),
    col("lnk_Dominion_ProvLkup_in.FCTS_SRC_SYS_CD").alias("FCTS_SRC_SYS_CD"),
    col("Ref_CmnPrctSk.CMN_PRCT_SK").alias("CMN_PRCT_SK"),
    col("ref_ProvEntyCdSK.CD_MPPNG_SK").alias("CD_MPPNG_SK")
)

# Stage: Lkup_Fkey (PxLookup) with primary link = df_Lkup_Codes_final, lookup link = df_db2_K_CMN_PRCT_Lkp
df_Lkup_Fkey_join = df_Lkup_Codes_final.alias("lnk_Dominion_Lkup_In").join(
    df_db2_K_CMN_PRCT_Lkp.alias("Ref_CmnPrctSk"),
    (
        (col("lnk_Dominion_Lkup_In.CMN_PRCT_SK") == col("Ref_CmnPrctSk.CMN_PRCT_SK"))
        & (col("lnk_Dominion_Lkup_In.FCTS_SRC_SYS_CD") == col("Ref_CmnPrctSk.SRC_SYS_CD"))
        & (lit(None).cast("string") == col("Ref_CmnPrctSk.CMN_PRCT_ID"))
        & (col("lnk_Dominion_Lkup_In.National_Provider_Identifer") == col("Ref_CmnPrctSk.NTNL_PROV_ID"))
    ),
    how="left"
)

df_Lkup_Fkey_final = df_Lkup_Fkey_join.select(
    col("lnk_Dominion_Lkup_In.Last_Name").alias("Last_Name"),
    col("lnk_Dominion_Lkup_In.First_Name").alias("First_Name"),
    col("lnk_Dominion_Lkup_In.Middle_Name").alias("Middle_Name"),
    col("lnk_Dominion_Lkup_In.Suffix").alias("Suffix"),
    col("lnk_Dominion_Lkup_In.Specialty").alias("Specialty"),
    col("lnk_Dominion_Lkup_In.Board_Certified_Indicator").alias("Board_Certified_Indicator"),
    col("lnk_Dominion_Lkup_In.National_Provider_Identifer").alias("National_Provider_Identifer"),
    col("lnk_Dominion_Lkup_In.Prescriber_Flag").alias("Prescriber_Flag"),
    col("lnk_Dominion_Lkup_In.Provider_DEA").alias("Provider_DEA"),
    col("lnk_Dominion_Lkup_In.Provider_Identifier").alias("Provider_Identifier"),
    col("lnk_Dominion_Lkup_In.Network_Identifier").alias("Network_Identifier"),
    col("lnk_Dominion_Lkup_In.Provider_Status").alias("Provider_Status"),
    col("lnk_Dominion_Lkup_In.Effective_Date").alias("Effective_Date"),
    col("lnk_Dominion_Lkup_In.Term_Date").alias("Term_Date"),
    col("lnk_Dominion_Lkup_In.Provider_Address_Identifier").alias("Provider_Address_Identifier"),
    col("lnk_Dominion_Lkup_In.Provider_Tax_Identifier").alias("Provider_Tax_Identifier"),
    col("lnk_Dominion_Lkup_In.Provider_Taxonomy_Code").alias("Provider_Taxonomy_Code"),
    col("lnk_Dominion_Lkup_In.FCTS_SRC_SYS_CD").alias("FCTS_SRC_SYS_CD"),
    col("lnk_Dominion_Lkup_In.CMN_PRCT_SK").alias("CMN_PRCT_SK"),
    col("lnk_Dominion_Lkup_In.CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    col("Ref_CmnPrctSk.CMN_PRCT_ID").alias("CMN_PRCT_ID")
)

# Stage: Xfrm_BusinessRules (CTransformerStage) with Stage Variables and two outputs

# Prepare Stage Variables
df_Xfrm_BusinessRules_sv = (
    df_Lkup_Fkey_final
    .withColumn(
        "svProvFirstNm",
        UpCase(trim(when(col("First_Name").isNotNull(), col("First_Name")).otherwise(lit(""))))
    )
    .withColumn(
        "svProvLastNm",
        UpCase(trim(when(col("Last_Name").isNotNull(), col("Last_Name")).otherwise(lit(""))))
    )
    .withColumn(
        "svProvMidNm",
        UpCase(trim(when(col("Middle_Name").isNotNull(), col("Middle_Name")).otherwise(lit(""))))
    )
    .withColumn(
        "svProvNm",
        when(
            (col("svProvMidNm").isNull()) | (length(col("svProvMidNm")) == 0),
            concat_ws(",", col("svProvFirstNm"), col("svProvLastNm"))
        ).otherwise(
            concat_ws(",", col("svProvFirstNm"), col("svProvLastNm"), col("svProvMidNm"))
        )
    )
    .withColumn(
        "svSpecialty",
        UpCase(trim(when(col("Specialty").isNotNull(), col("Specialty")).otherwise(lit(""))))
    )
)

# Output link lnk_DominionProv_Out => ds_PROV_Xfrm
df_Xfrm_BusinessRules_linkA = df_Xfrm_BusinessRules_sv.select(
    (
        col("Provider_Identifier").cast("string") 
        + lit(";") 
        + lit(SrcSysCd)
    ).alias("PRI_NAT_KEY_STRING"),
    lit(RunIDTimeStamp).alias("FIRST_RECYC_TS"),
    lit(0).alias("PROV_SK"),
    col("Provider_Identifier").alias("PROV_ID"),
    lit(SrcSysCd).alias("SRC_SYS_CD"),
    lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    when(col("CMN_PRCT_ID").isNull(), lit("")).otherwise(col("CMN_PRCT_ID")).alias("CMN_PRCT"),
    lit("NA").alias("REL_GRP_PROV"),
    lit("NA").alias("REL_IPA_PROV"),
    lit("NA").alias("PROV_CAP_PAYMT_EFT_METH_CD"),
    lit("NA").alias("PROV_CLM_PAYMT_EFT_METH_CD"),
    lit("NA").alias("PROV_CLM_PAYMT_METH_CD"),
    lit("PRCTR").alias("PROV_ENTY_CD"),
    lit("NA").alias("PROV_FCLTY_TYP_CD"),
    lit("NA").alias("PROV_PRCTC_TYP_CD"),
    lit("NA").alias("PROV_SVC_CAT_CD"),
    when(
        col("svSpecialty") == "ANESTHESIOLOGIST", lit("0005")
    ).when(
        col("svSpecialty") == "ANY SPECIALTY", lit("0041")
    ).when(
        col("svSpecialty") == "DENTURIST", lit("0041")
    ).when(
        col("svSpecialty") == "ENDODONTIST", lit("0047")
    ).when(
        col("svSpecialty") == "GENERAL PRACTITIONER", lit("0041")
    ).when(
        col("svSpecialty") == "GP WITH PEDIATRIC DENTIST", lit("0044")
    ).when(
        col("svSpecialty") == "GP WITH PUBLIC HEALTH FOC", lit("0041")
    ).when(
        col("svSpecialty") == "HYGIENIST", lit("0164")
    ).when(
        col("svSpecialty") == "ORAL SURGEON", lit("0019")
    ).when(
        col("svSpecialty") == "ORTHODONTIST", lit("0042")
    ).otherwise(lit("UNK")).alias("PROV_SPEC_CD"),
    lit("NA").alias("PROV_STTUS_CD"),
    lit("NA").alias("PROV_TERM_RSN_CD"),
    lit("NA").alias("PROV_TYP_CD"),
    rpad(
        when(
            trim(when(col("Term_Date").isNotNull(), col("Term_Date")).otherwise(lit(""))) == lit("9999-12-31"),
            lit("2199-12-31")
        ).otherwise(col("Term_Date")),
        10, " "
    ).alias("TERM_DT"),
    rpad(lit("2199-12-31"), 10, " ").alias("PAYMT_HOLD_DT"),
    lit("NA").alias("CLRNGHOUSE_ID"),
    lit("NA").alias("EDI_DEST_ID"),
    lit("").alias("EDI_DEST_QUAL"),
    when(
        (col("National_Provider_Identifer").isNull())
        | (trim(col("National_Provider_Identifer")) == lit("")),
        lit("NA")
    ).otherwise(col("National_Provider_Identifer")).alias("NTNL_PROV_ID"),
    col("Provider_Address_Identifier").alias("PROV_ADDR_ID"),
    trim(col("svProvNm")).alias("PROV_NM"),
    col("Provider_Tax_Identifier").alias("TAX_ID"),
    col("Provider_Taxonomy_Code").alias("TXNMY_CD")
)

# Output link lnk_DominionProvBProv_Out => seq_B_PROV_csv
df_Xfrm_BusinessRules_linkB = df_Xfrm_BusinessRules_sv.select(
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    col("Provider_Identifier").alias("PROV_ID"),
    when(
        (col("National_Provider_Identifer").isNull())
        | (trim(col("National_Provider_Identifer")) == lit("")),
        lit(1)
    ).otherwise(
        when(col("CMN_PRCT_SK").isNull(), lit(0)).otherwise(col("CMN_PRCT_SK"))
    ).alias("CMN_PRCT_SK"),
    lit(1).alias("REL_GRP_PROV_SK"),
    lit(1).alias("REL_IPA_PROV_SK"),
    when(col("CD_MPPNG_SK").isNull(), lit(0)).otherwise(col("CD_MPPNG_SK")).alias("PROV_ENTY_CD_SK")
)

# Stage: ds_PROV_Xfrm (PxDataSet) => Write as parquet
# Maintain column order exactly as in the output pins
write_files(
    df_Xfrm_BusinessRules_linkA.select(
        "PRI_NAT_KEY_STRING",
        "FIRST_RECYC_TS",
        "PROV_SK",
        "PROV_ID",
        "SRC_SYS_CD",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "SRC_SYS_CD_SK",
        "CMN_PRCT",
        "REL_GRP_PROV",
        "REL_IPA_PROV",
        "PROV_CAP_PAYMT_EFT_METH_CD",
        "PROV_CLM_PAYMT_EFT_METH_CD",
        "PROV_CLM_PAYMT_METH_CD",
        "PROV_ENTY_CD",
        "PROV_FCLTY_TYP_CD",
        "PROV_PRCTC_TYP_CD",
        "PROV_SVC_CAT_CD",
        "PROV_SPEC_CD",
        "PROV_STTUS_CD",
        "PROV_TERM_RSN_CD",
        "PROV_TYP_CD",
        "TERM_DT",
        "PAYMT_HOLD_DT",
        "CLRNGHOUSE_ID",
        "EDI_DEST_ID",
        "EDI_DEST_QUAL",
        "NTNL_PROV_ID",
        "PROV_ADDR_ID",
        "PROV_NM",
        "TAX_ID",
        "TXNMY_CD"
    ),
    f"{adls_path}/ds/PROV.{SrcSysCd}.xfrm.{RunID}.parquet",
    delimiter=",",
    mode="overwrite",
    is_parquet=True,
    header=True,
    quote='"',
    nullValue=None
)

# Stage: seq_B_PROV_csv (PxSequentialFile) => Write delimited file
write_files(
    df_Xfrm_BusinessRules_linkB.select(
        "SRC_SYS_CD_SK",
        "PROV_ID",
        "CMN_PRCT_SK",
        "REL_GRP_PROV_SK",
        "REL_IPA_PROV_SK",
        "PROV_ENTY_CD_SK"
    ),
    f"{adls_path}/load/B_PROV.{SrcSysCd}.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)