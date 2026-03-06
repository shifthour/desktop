# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2010 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC CALLED BY:  MedtrakDrugExtrSeq
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:     Reads the ESIDrugFile.dat created in  ESIClmLand  job and puts the data into the claim  provider common record format and runs through primary key using Shared container ClmProvPkey
# MAGIC     
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Kalyan Neelam         2010-12-22       4616                     Initial Programming                                                                       IntegrateNewDevl          Steph Goddard           12/23/2010
# MAGIC Kalyan Neelam         2011-02-08       4616                     Trimming the PROV_ID for the three o/p links                             IntegrateNewDevl          Steph Goddard           02/15/2011
# MAGIC                                                                                        in BusinessRules Transformer
# MAGIC Raja Gummadi        2012-07-23         TTR 1330            Changed RX_NO field size from 9 to 20 in input file                     IntegrateWrhsDevl          Bhoomi Dasari            08/08/2012
# MAGIC Dan Long                2014-04-18        5082                     Changed lenghth of CLM_ID go into
# MAGIC                                                                                        contaner ClmProvPK from 18 to 20 and                                      IntegrateNewDevl     
# MAGIC                                                                                        type to VAR, Changed CLM_PROV_ROLE_TYPE_CD
# MAGIC                                                                                        length to 20.
# MAGIC                                                                                        Changed PROV_ID type to VARCHAR.
# MAGIC 
# MAGIC Manasa Andru          2016-04-18    TFS - 12505            Modified the extract SQL in the DeaNo and MinDea links by      IntegrateDev2                Kalyan Neelam            2016-04-20
# MAGIC                                                                                           removing the join against the CMN_PRCT table.
# MAGIC 
# MAGIC Madhavan B	2017-06-20   5788 - BHI Updates    Added new column                                                                    IntegrateDev2                Kalyan Neelam            2017-07-07
# MAGIC                                                                                         SVC_FCLTY_LOC_NTNL_PROV_ID
# MAGIC 
# MAGIC Ravi Abburi             2018-01-29    5781 - HEDIS            Added the NTNL_PROV_ID                                                       IntegrateDev2               Kalyan Neelam            2018-04-23

# MAGIC Read the Medtrak file created from MedtrakClmLand
# MAGIC Driver Table data created in MedtrakClmDailyLand
# MAGIC This container is used in:
# MAGIC ESIClmProvExtr
# MAGIC FctsClmProvExtr
# MAGIC MCSourceClmProvExtr
# MAGIC MedicaidClmProvExtr
# MAGIC NascoClmProvExtr
# MAGIC PcsClmProvExtr
# MAGIC WellDyneClmProvExtr
# MAGIC MedtrakClmProvExtr
# MAGIC 
# MAGIC These programs need to be re-compiled when logic changes
# MAGIC Assign primary surrogate key
# MAGIC Writing Sequential File to /key
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
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DecimalType, LongType, DateType
)
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../shared_containers/PrimaryKey/ClmProvPK
# COMMAND ----------

IDSOwner = get_widget_value("IDSOwner", "")
ids_secret_name = get_widget_value("ids_secret_name", "")
SrcSysCd = get_widget_value("SrcSysCd","MEDTRAK")
SrcSysCdSk = get_widget_value("SrcSysCdSk","")
CurrentDate = get_widget_value("CurrentDate","2007-09-11")
RunID = get_widget_value("RunID","100")
RunCycle = get_widget_value("RunCycle","100")

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

# -------------------------------------------------------
# ProvExtr DB2Connector (IDS)
# -------------------------------------------------------
df_ProvExtr_ProvId = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"SELECT PROV.PROV_ID, PROV.TAX_ID FROM {IDSOwner}.PROV PROV")
    .load()
)

df_ProvExtr_NtnlProvId = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"SELECT PROV.NTNL_PROV_ID, PROV.TAX_ID, PROV.PROV_ID, PROV.TERM_DT_SK FROM {IDSOwner}.PROV PROV")
    .load()
)

df_ProvExtr_DeaNo = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", 
        f"SELECT DEA.DEA_NO, PROV.PROV_ID, PROV.TAX_ID "
        f"FROM {IDSOwner}.PROV_DEA DEA, {IDSOwner}.PROV PROV "
        f"WHERE DEA.CMN_PRCT_SK = PROV.CMN_PRCT_SK "
        f"AND PROV.PROV_ID not in ('UNK','NA') "
        f"AND DEA.CMN_PRCT_SK not in (0,1) "
        f"ORDER BY DEA.DEA_NO, PROV.PROV_ID DESC"
    )
    .load()
)

df_ProvExtr_PrscrbProv = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", 
        f"SELECT PROV.NTNL_PROV_ID, PROV.TAX_ID, PROV.PROV_ID "
        f"FROM {IDSOwner}.PROV PROV "
        f"ORDER BY PROV.NTNL_PROV_ID, PROV.PROV_ID DESC"
    )
    .load()
)

df_ProvExtr_MinProvId = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", 
        f"SELECT PROV.NTNL_PROV_ID, PROV.TERM_DT_SK, min(PROV.PROV_ID) "
        f"FROM {IDSOwner}.PROV PROV "
        f"WHERE PROV.NTNL_PROV_ID=? AND PROV.TERM_DT_SK>=? AND PROV.TERM_DT_SK=( "
        f"SELECT MIN(PROV2.TERM_DT_SK) FROM {IDSOwner}.PROV PROV2 "
        f"WHERE PROV2.NTNL_PROV_ID=PROV.NTNL_PROV_ID ) "
        f"GROUP BY PROV.NTNL_PROV_ID, PROV.TERM_DT_SK"
    )
    .load()
)

df_ProvExtr_MinDea = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query",
        f"SELECT DEA.DEA_NO, min(PROV.PROV_ID) "
        f"FROM {IDSOwner}.PROV_DEA DEA, {IDSOwner}.PROV PROV "
        f"WHERE DEA.CMN_PRCT_SK=PROV.CMN_PRCT_SK "
        f"AND PROV.PROV_ID NOT IN('UNK','NA') "
        f"AND DEA.CMN_PRCT_SK not in (0,1) "
        f"GROUP BY DEA.DEA_NO "
        f"HAVING min(PROV.PROV_ID) NOT IN('UNK','NA')"
    )
    .load()
)

df_ProvExtr_MinPrscrb = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query",
        f"SELECT PROV.NTNL_PROV_ID, min(PROV.PROV_ID) "
        f"FROM {IDSOwner}.PROV PROV "
        f"GROUP BY PROV.NTNL_PROV_ID"
    )
    .load()
)

df_ProvExtr_NtnlProv = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", 
        f"SELECT PROV.PROV_ID, PROV.NTNL_PROV_ID "
        f"FROM {IDSOwner}.PROV PROV, {IDSOwner}.CD_MPPNG CDMP "
        f"WHERE CDMP.CD_MPPNG_SK=PROV.SRC_SYS_CD_SK AND SRC_CD='NABP'"
    )
    .load()
)

df_ProvExtr_DeaNtnlId = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"SELECT DEA.DEA_NO, DEA.NTNL_PROV_ID FROM {IDSOwner}.PROV_DEA DEA")
    .load()
)

# -------------------------------------------------------
# Convert each to CHashedFileStage scenario A => deduplicate on key columns
# -------------------------------------------------------
df_hf_medtrak_clm_prov_lkup = dedup_sort(
    df_ProvExtr_ProvId,
    partition_cols=["PROV_ID"],
    sort_cols=[]
)

df_hf_medtrak_clm_provnpi_lkup = dedup_sort(
    df_ProvExtr_NtnlProvId,
    partition_cols=["NTNL_PROV_ID"],
    sort_cols=[]
)

df_hf_medtrak_prov_dea_lkup = dedup_sort(
    df_ProvExtr_DeaNo,
    partition_cols=["DEA_NO"],
    sort_cols=[]
)

df_hf_medtrak_prscrb_prov_npi_lkup = dedup_sort(
    df_ProvExtr_PrscrbProv,
    partition_cols=["NTNL_PROV_ID"],
    sort_cols=[]
)

df_hf_medtrak_min_dea_prov = dedup_sort(
    df_ProvExtr_MinDea,
    partition_cols=["DEA_NO"],
    sort_cols=[]
)

df_hf_min_medtrak_clm_prov_prscrb = dedup_sort(
    df_ProvExtr_MinPrscrb,
    partition_cols=["NTNL_PROV_ID"],
    sort_cols=[]
)

df_hf_medtrak_clm_prov_ntnl_lkup = dedup_sort(
    df_ProvExtr_NtnlProv,
    partition_cols=["PROV_ID"],
    sort_cols=[]
)

df_hf_medtrak_provdea_ntnllkup = dedup_sort(
    df_ProvExtr_DeaNtnlId,
    partition_cols=["DEA_NO"],
    sort_cols=[]
)

# -------------------------------------------------------
# MbrHmoExtr DB2Connector (IDS)
# -------------------------------------------------------
df_MbrHmoExtr_HmoExtr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query",
        f"SELECT DRVR.MBR_UNIQ_KEY, DRVR.DRUG_FILL_DT_SK, MAX(MBR_ENR.EFF_DT_SK) "
        f"FROM {IDSOwner}.W_DRUG_CLM_PCP DRVR, {IDSOwner}.MBR_ENR MBR_ENR, {IDSOwner}.CD_MPPNG MAP1, "
        f"{IDSOwner}.PROD PROD, {IDSOwner}.PROD_SH_NM PRODSH, {IDSOwner}.CD_MPPNG MAP2 "
        f"WHERE DRVR.MBR_UNIQ_KEY=MBR_ENR.MBR_UNIQ_KEY "
        f"AND MBR_ENR.SRC_SYS_CD_SK=MAP1.CD_MPPNG_SK "
        f"AND MAP1.TRGT_CD='FACETS' "
        f"AND DRVR.DRUG_FILL_DT_SK BETWEEN MBR_ENR.EFF_DT_SK AND MBR_ENR.TERM_DT_SK "
        f"AND MBR_ENR.ELIG_IN='Y' "
        f"AND MBR_ENR.PROD_SK=PROD.PROD_SK "
        f"AND PROD.PROD_SH_NM_SK=PRODSH.PROD_SH_NM_SK "
        f"AND PRODSH.PROD_SH_NM_DLVRY_METH_CD_SK=MAP2.CD_MPPNG_SK "
        f"AND MAP2.TRGT_CD='HMO' "
        f"GROUP BY DRVR.MBR_UNIQ_KEY, DRVR.DRUG_FILL_DT_SK"
    )
    .load()
)

df_MbrHmoExtr_MbrPcpEffDt = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query",
        f"SELECT DRVR.MBR_UNIQ_KEY, DRVR.DRUG_FILL_DT_SK, MAX(MBR_PCP.EFF_DT_SK), "
        f"PROV.PROV_ID, PROV.TAX_ID, PROV.NTNL_PROV_ID, MPPNG.TRGT_CD "
        f"FROM {IDSOwner}.W_DRUG_CLM_PCP DRVR, {IDSOwner}.MBR_PCP MBR_PCP, {IDSOwner}.CD_MPPNG MPPNG, {IDSOwner}.PROV PROV "
        f"WHERE DRVR.MBR_UNIQ_KEY=MBR_PCP.MBR_UNIQ_KEY "
        f"AND DRVR.DRUG_FILL_DT_SK BETWEEN MBR_PCP.EFF_DT_SK AND MBR_PCP.TERM_DT_SK "
        f"AND MBR_PCP.MBR_PCP_TYP_CD_SK=MPPNG.CD_MPPNG_SK "
        f"AND MBR_PCP.PROV_SK=PROV.PROV_SK "
        f"GROUP BY DRVR.MBR_UNIQ_KEY, DRVR.DRUG_FILL_DT_SK, PROV.PROV_ID, PROV.TAX_ID, PROV.NTNL_PROV_ID, MPPNG.TRGT_CD"
    )
    .load()
)

# Deduplicate for hf_esi_clm_mbr_pcp
df_hf_esi_clm_mbr_pcp = dedup_sort(
    df_MbrHmoExtr_MbrPcpEffDt,
    partition_cols=["MBR_UNIQ_KEY","DRUG_FILL_DT_SK"],
    sort_cols=[]
)

# -------------------------------------------------------
# Transform (CTransformerStage) merges HmoExtr + MbrPcpLkup
# -------------------------------------------------------
df_join_Transform = (
    df_MbrHmoExtr_HmoExtr.alias("HmoExtr")
    .join(
        df_hf_esi_clm_mbr_pcp.alias("MbrPcpLkup"),
        [
            F.col("HmoExtr.MBR_UNIQ_KEY") == F.col("MbrPcpLkup.MBR_UNIQ_KEY"),
            F.col("HmoExtr.DRUG_FILL_DT_SK") == F.col("MbrPcpLkup.DRUG_FILL_DT_SK"),
            F.col("HmoExtr['MAX(MBR_ENR.EFF_DT_SK')".replace("['",".").replace("']","")) 
             == F.col("MbrPcpLkup['MAX(MBR_PCP.EFF_DT_SK')".replace("['",".")) # slight trick for the "MAX()" alias
        ],
        how="left"
    )
)

df_Transform_PCP = df_join_Transform.select(
    F.col("HmoExtr.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("HmoExtr.DRUG_FILL_DT_SK").alias("ARGUS_FILL_DT_SK"),
    F.when(
        (F.col("MbrPcpLkup.PROV_ID").isNull()) | (F.length(F.col("MbrPcpLkup.PROV_ID")) == 0),
        F.lit("UNK")
    ).otherwise(F.col("MbrPcpLkup.PROV_ID")).alias("PROV_ID"),
    F.when(
        (F.col("MbrPcpLkup.TAX_ID").isNull()) | (F.length(F.col("MbrPcpLkup.TAX_ID")) == 0),
        F.lit("NA")
    ).otherwise(F.col("MbrPcpLkup.TAX_ID")).alias("TAX_ID"),
    F.when(
        (F.col("MbrPcpLkup.PROV_ID") == "0") | (F.col("MbrPcpLkup.PROV_ID") == "1"),
        F.col("MbrPcpLkup.PROV_ID")
    ).otherwise(
        F.when(
            (F.col("MbrPcpLkup.NTNL_PROV_ID").isNull())
            | (F.length(F.col("MbrPcpLkup.NTNL_PROV_ID")) == 0)
            | (F.col("MbrPcpLkup.NTNL_PROV_ID").isin("NA","UNK")),
            F.lit("UNK")
        ).otherwise(F.col("MbrPcpLkup.NTNL_PROV_ID"))
    ).alias("NTNL_PROV_ID"),
    F.when(
        (F.col("MbrPcpLkup.TRGT_CD").isNull())
        | (F.length(F.col("MbrPcpLkup.TRGT_CD")) == 0)
        | (F.col("MbrPcpLkup.TRGT_CD") != "PRI"),
        F.lit("UNK")
    ).otherwise(F.lit("PCP")).alias("TRGT_CD")
)

# Deduplicate for hf_medtrak_clm_hmo_mbr_pcp
df_hf_medtrak_clm_hmo_mbr_pcp = dedup_sort(
    df_Transform_PCP,
    partition_cols=["MBR_UNIQ_KEY","ARGUS_FILL_DT_SK"],
    sort_cols=[]
)

# -------------------------------------------------------
# MedtrakClmLand (CSeqFileStage) => read the .dat
# -------------------------------------------------------
schema_MedtrakClmLand = StructType([
    StructField("RCRD_ID", DecimalType(38,10), nullable=False),
    StructField("CLAIM_ID", StringType(), nullable=False),
    StructField("PRCSR_NO", DecimalType(38,10), nullable=False),
    StructField("MEM_CK_KEY", IntegerType(), nullable=False),
    StructField("BTCH_NO", DecimalType(38,10), nullable=False),
    StructField("PDX_NO", StringType(), nullable=False),
    StructField("RX_NO", DecimalType(38,10), nullable=False),
    StructField("DT_FILLED", StringType(), nullable=False),
    StructField("NDC_NO", DecimalType(38,10), nullable=False),
    StructField("DRUG_DESC", StringType(), nullable=False),
    StructField("NEW_RFL_CD", DecimalType(38,10), nullable=False),
    StructField("METRIC_QTY", DecimalType(38,10), nullable=False),
    StructField("DAYS_SUPL", DecimalType(38,10), nullable=False),
    StructField("BSS_OF_CST_DTRM", StringType(), nullable=False),
    StructField("INGR_CST", DecimalType(38,10), nullable=False),
    StructField("DISPNS_FEE", DecimalType(38,10), nullable=False),
    StructField("COPAY_AMT", DecimalType(38,10), nullable=False),
    StructField("SLS_TAX", DecimalType(38,10), nullable=False),
    StructField("AMT_BILL", DecimalType(38,10), nullable=False),
    StructField("PATN_FIRST_NM", StringType(), nullable=False),
    StructField("PATN_LAST_NM", StringType(), nullable=False),
    StructField("DOB", DecimalType(38,10), nullable=False),
    StructField("SEX_CD", DecimalType(38,10), nullable=False),
    StructField("CARDHLDR_ID_NO", StringType(), nullable=False),
    StructField("RELSHP_CD", DecimalType(38,10), nullable=False),
    StructField("GRP_NO", StringType(), nullable=False),
    StructField("HOME_PLN", StringType(), nullable=False),
    StructField("HOST_PLN", DecimalType(38,10), nullable=False),
    StructField("PRESCRIBER_ID", StringType(), nullable=False),
    StructField("DIAG_CD", StringType(), nullable=False),
    StructField("CARDHLDR_FIRST_NM", StringType(), nullable=False),
    StructField("CARDHLDR_LAST_NM", StringType(), nullable=False),
    StructField("PRAUTH_NO", DecimalType(38,10), nullable=False),
    StructField("PA_MC_SC_NO", StringType(), nullable=False),
    StructField("CUST_LOC", DecimalType(38,10), nullable=False),
    StructField("RESUB_CYC_CT", DecimalType(38,10), nullable=False),
    StructField("DT_RX_WRTN", DecimalType(38,10), nullable=False),
    StructField("DISPENSE_AS_WRTN_PROD_SEL_CD", StringType(), nullable=False),
    StructField("PRSN_CD", StringType(), nullable=False),
    StructField("OTHR_COV_CD", DecimalType(38,10), nullable=False),
    StructField("ELIG_CLRFCTN_CD", DecimalType(38,10), nullable=False),
    StructField("CMPND_CD", DecimalType(38,10), nullable=False),
    StructField("NO_OF_RFLS_AUTH", DecimalType(38,10), nullable=False),
    StructField("LVL_OF_SVC", DecimalType(38,10), nullable=False),
    StructField("RX_ORIG_CD", DecimalType(38,10), nullable=False),
    StructField("RX_DENIAL_CLRFCTN", DecimalType(38,10), nullable=False),
    StructField("PRI_PRESCRIBER", StringType(), nullable=False),
    StructField("CLNC_ID_NO", DecimalType(38,10), nullable=False),
    StructField("DRUG_TYP", DecimalType(38,10), nullable=False),
    StructField("PRESCRIBER_LAST_NM", StringType(), nullable=False),
    StructField("POSTAGE_AMT_CLMED", DecimalType(38,10), nullable=False),
    StructField("UNIT_DOSE_IN", DecimalType(38,10), nullable=False),
    StructField("OTHR_PAYOR_AMT", DecimalType(38,10), nullable=False),
    StructField("BSS_OF_DAYS_SUPL_DTRM", DecimalType(38,10), nullable=False),
    StructField("FULL_AWP", DecimalType(38,10), nullable=False),
    StructField("EXPNSN_AREA", StringType(), nullable=False),
    StructField("MSTR_CAR", StringType(), nullable=False),
    StructField("SUB_CAR", StringType(), nullable=False),
    StructField("CLM_TYP", StringType(), nullable=False),
    StructField("ESI_SUB_GRP", StringType(), nullable=False),
    StructField("PLN_DSGNR", StringType(), nullable=False),
    StructField("ADJDCT_DT", StringType(), nullable=False),
    StructField("ADMIN_FEE", DecimalType(38,10), nullable=False),
    StructField("CAP_AMT", DecimalType(38,10), nullable=False),
    StructField("INGR_CST_SUB", DecimalType(38,10), nullable=False),
    StructField("MBR_NON_COPAY_AMT", DecimalType(38,10), nullable=False),
    StructField("MBR_PAY_CD", StringType(), nullable=False),
    StructField("INCNTV_FEE", DecimalType(38,10), nullable=False),
    StructField("CLM_ADJ_AMT", DecimalType(38,10), nullable=False),
    StructField("CLM_ADJ_CD", StringType(), nullable=False),
    StructField("FRMLRY_FLAG", StringType(), nullable=False),
    StructField("GNRC_CLS_NO", StringType(), nullable=False),
    StructField("THRPTC_CLS_AHFS", StringType(), nullable=False),
    StructField("PDX_TYP", StringType(), nullable=False),
    StructField("BILL_BSS_CD", StringType(), nullable=False),
    StructField("USL_AND_CUST_CHRG", DecimalType(38,10), nullable=False),
    StructField("PD_DT", StringType(), nullable=False),
    StructField("BNF_CD", StringType(), nullable=False),
    StructField("DRUG_STRG", StringType(), nullable=False),
    StructField("ORIG_MBR", StringType(), nullable=False),
    StructField("DT_OF_INJURY", DecimalType(38,10), nullable=False),
    StructField("FEE_AMT", DecimalType(38,10), nullable=False),
    StructField("ESI_REF_NO", StringType(), nullable=False),
    StructField("CLNT_CUST_ID", StringType(), nullable=False),
    StructField("PLN_TYP", StringType(), nullable=False),
    StructField("ESI_ADJDCT_REF_NO", DecimalType(38,10), nullable=False),
    StructField("ESI_ANCLRY_AMT", DecimalType(38,10), nullable=False),
    StructField("ESI_CLNT_GNRL_PRPS_AREA", StringType(), nullable=True),
    StructField("GRP_ID", StringType(), nullable=False),
    StructField("SUBGRP_ID", StringType(), nullable=False),
    StructField("CLS_PLN_ID", StringType(), nullable=False),
    StructField("PAID_DATE", StringType(), nullable=False),
    StructField("PRTL_FILL_STTUS_CD", StringType(), nullable=False),
    StructField("ESI_BILL_DT", DecimalType(38,10), nullable=False),
    StructField("FSA_VNDR_CD", StringType(), nullable=False),
    StructField("PICA_DRUG_CD", StringType(), nullable=False),
    StructField("AMT_CLMED", DecimalType(38,10), nullable=False),
    StructField("AMT_DSALW", DecimalType(38,10), nullable=False),
    StructField("FED_DRUG_CLS_CD", StringType(), nullable=False),
    StructField("DEDCT_AMT", DecimalType(38,10), nullable=False),
    StructField("BNF_COPAY_100", StringType(), nullable=False),
    StructField("CLM_PRCS_TYP", StringType(), nullable=False),
    StructField("INDEM_HIER_TIER_NO", DecimalType(38,10), nullable=False),
    StructField("FLR", StringType(), nullable=False),
    StructField("MCARE_D_COV_DRUG", StringType(), nullable=False),
    StructField("RETRO_LICS_CD", StringType(), nullable=False),
    StructField("RETRO_LICS_AMT", DecimalType(38,10), nullable=False),
    StructField("LICS_SBSDY_AMT", DecimalType(38,10), nullable=False),
    StructField("MED_B_DRUG", StringType(), nullable=False),
    StructField("MED_B_CLM", StringType(), nullable=False),
    StructField("PRESCRIBER_QLFR", StringType(), nullable=False),
    StructField("PRESCRIBER_ID_NPI", StringType(), nullable=False),
    StructField("PDX_QLFR", StringType(), nullable=False),
    StructField("PDX_ID_NPI", StringType(), nullable=False),
    StructField("HRA_APLD_AMT", DecimalType(38,10), nullable=False),
    StructField("ESI_THER_CLS", DecimalType(38,10), nullable=False),
    StructField("HIC_NO", StringType(), nullable=False),
    StructField("HRA_FLAG", StringType(), nullable=False),
    StructField("DOSE_CD", DecimalType(38,10), nullable=False),
    StructField("LOW_INCM", StringType(), nullable=False),
    StructField("RTE_OF_ADMIN", StringType(), nullable=False),
    StructField("DEA_SCHD", DecimalType(38,10), nullable=False),
    StructField("COPAY_BNF_OPT", DecimalType(38,10), nullable=False),
    StructField("GNRC_PROD_IN_GPI", DecimalType(38,10), nullable=False),
    StructField("PRESCRIBER_SPEC", StringType(), nullable=False),
    StructField("VAL_CD", StringType(), nullable=False),
    StructField("PRI_CARE_PDX", StringType(), nullable=False),
    StructField("OFC_OF_INSPECTOR_GNRL_OIG", StringType(), nullable=False),
    StructField("FLR3", StringType(), nullable=False),
    StructField("PSL_FMLY_MET_AMT", DecimalType(38,10), nullable=False),
    StructField("PSL_MBR_MET_AMT", DecimalType(38,10), nullable=False),
    StructField("PSL_FMLY_AMT", DecimalType(38,10), nullable=False),
    StructField("DED_FMLY_MET_AMT", DecimalType(38,10), nullable=False),
    StructField("DED_FMLY_AMT", DecimalType(38,10), nullable=False),
    StructField("MOPS_FMLY_AMT", DecimalType(38,10), nullable=False),
    StructField("MOPS_FMLY_MET_AMT", DecimalType(38,10), nullable=False),
    StructField("MOPS_MBR_MET_AMT", DecimalType(38,10), nullable=False),
    StructField("DED_MBR_MET_AMT", DecimalType(38,10), nullable=False),
    StructField("PSL_APLD_AMT", DecimalType(38,10), nullable=False),
    StructField("MOPS_APLD_AMT", DecimalType(38,10), nullable=False),
    StructField("PAR_PDX_IND", StringType(), nullable=False),
    StructField("COPAY_PCT_AMT", DecimalType(38,10), nullable=False),
    StructField("COPAY_FLAT_AMT", DecimalType(38,10), nullable=False),
    StructField("CLM_TRANSMITTAL_METH", StringType(), nullable=False),
    StructField("FLR4", StringType(), nullable=False),
])

df_MedtrakClmLand = spark.read.format("csv") \
    .option("delimiter", ",") \
    .option("quote", "\"") \
    .option("header", "false") \
    .schema(schema_MedtrakClmLand) \
    .load(f"{adls_path_raw}/verified/MedtrakDrugClm_Land.dat.{RunID}")

# -------------------------------------------------------
# BusinessRules (CTransformerStage)
#    Primary link: Extract (df_MedtrakClmLand)
#    Many lookups: PCPLkup, ProvIdLkup, NtnlProvIdLkup, DeaNbrLkup, PrscrbProvLkup, ...
# -------------------------------------------------------
df_Br = df_MedtrakClmLand.alias("Extract")

df_Br = df_Br.join(
    df_hf_medtrak_clm_hmo_mbr_pcp.alias("PCPLkup"),
    [
        F.col("Extract.MEM_CK_KEY") == F.col("PCPLkup.MBR_UNIQ_KEY"),
        F.col("Extract.DT_FILLED") == F.col("PCPLkup.ARGUS_FILL_DT_SK")
    ],
    how="left"
).join(
    df_hf_medtrak_clm_prov_lkup.alias("ProvIdLkup"),
    F.col("Extract.PDX_NO") == F.col("ProvIdLkup.PROV_ID"),
    how="left"
).join(
    df_hf_medtrak_clm_provnpi_lkup.alias("NtnlProvIdLkup"),
    F.col("Extract.PDX_ID_NPI") == F.col("NtnlProvIdLkup.NTNL_PROV_ID"),
    how="left"
).join(
    df_hf_medtrak_prov_dea_lkup.alias("DeaNbrLkup"),
    F.col("Extract.PRESCRIBER_ID") == F.col("DeaNbrLkup.DEA_NO"),
    how="left"
).join(
    df_hf_medtrak_prscrb_prov_npi_lkup.alias("PrscrbProvLkup"),
    F.col("Extract.PRESCRIBER_ID_NPI") == F.col("PrscrbProvLkup.NTNL_PROV_ID"),
    how="left"
).join(
    df_ProvExtr_MinProvId.alias("MinProvId"),
    [
        F.col("Extract.PDX_ID_NPI") == F.col("MinProvId.NTNL_PROV_ID"),
        F.col("Extract.DT_FILLED") == F.col("MinProvId.TERM_DT_SK")
    ],
    how="left"
).join(
    df_hf_medtrak_min_dea_prov.alias("MinDeaLkup"),
    F.col("Extract.PRESCRIBER_ID") == F.col("MinDeaLkup.DEA_NO"),
    how="left"
).join(
    df_hf_min_medtrak_clm_prov_prscrb.alias("MinPrscrbLkup"),
    F.col("Extract.PRESCRIBER_ID_NPI") == F.col("MinPrscrbLkup.NTNL_PROV_ID"),
    how="left"
).join(
    df_hf_medtrak_clm_prov_ntnl_lkup.alias("NtnlProvSkLkp"),
    F.col("Extract.PDX_NO") == F.col("NtnlProvSkLkp.PROV_ID"),
    how="left"
).join(
    df_hf_medtrak_provdea_ntnllkup.alias("DeaNtnlLkp"),
    F.col("Extract.PRESCRIBER_ID") == F.col("DeaNtnlLkp.DEA_NO"),
    how="left"
)

df_Br = df_Br.withColumn("SvcProv",
    F.when(
        ~F.col("ProvIdLkup.PROV_ID").isNull(),
        F.col("Extract.PDX_NO")
    ).otherwise(
        F.when(
            ~F.col("NtnlProvIdLkup.PROV_ID").isNull(),
            trim(F.col("NtnlProvIdLkup.PROV_ID"))
        ).otherwise(F.lit("UNK"))
    )
)

df_Br = df_Br.withColumn("ClmId", F.col("Extract.CLAIM_ID"))

df_Br = df_Br.withColumn("PkString", F.concat_ws(";", F.lit(SrcSysCd), F.col("ClmId")))

df_Br = df_Br.withColumn("CurDateTime", current_date())

df_Br = df_Br.withColumn("PassThru", F.lit("Y"))

# We now generate 3 output streams from df_Br: SVC, PCP, PRSCRB
# We'll create them as separate DataFrames. Then we will union them in a collector.

# SVC
df_SVC = df_Br.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.col("PassThru").alias("PASS_THRU_IN"),
    F.col("CurDateTime").alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit(SrcSysCd).alias("SRC_SYS_CD"),
    F.concat_ws(";", F.col("PkString"), F.lit("SVC")).alias("PRI_KEY_STRING"),
    F.lit(0).alias("CLM_PROV_SK"),
    F.rpad(F.col("ClmId"), 18, " ").alias("CLM_ID"),
    F.rpad(F.lit("SVC"), 10, " ").alias("CLM_PROV_ROLE_TYP_CD"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("SvcProv").alias("PROV_ID"),
    F.rpad(
        F.when(
            (~F.col("ProvIdLkup.TAX_ID").isNull())
            & (~F.col("MinProvId.NTNL_PROV_ID").isNull()),
            F.col("ProvIdLkup.TAX_ID")
        ).otherwise(
            F.when(
                (~F.col("NtnlProvIdLkup.TAX_ID").isNull())
                & (~F.col("MinProvId.NTNL_PROV_ID").isNull()),
                F.col("NtnlProvIdLkup.TAX_ID")
            ).otherwise(F.lit("NA"))
        ), 9, " "
    ).alias("TAX_ID"),
    F.when(
        ( ~F.col("Extract.PDX_NO").isNull() | ~F.col("Extract.PDX_ID_NPI").isNull() ),
        F.when(
            F.col("NtnlProvSkLkp.NTNL_PROV_ID").isNull()
            | (F.col("NtnlProvSkLkp.NTNL_PROV_ID") == "NA"),
            F.when(~F.col("Extract.PDX_ID_NPI").isNull(), F.col("Extract.PDX_ID_NPI")).otherwise(F.lit("NA"))
        ).otherwise(F.col("NtnlProvSkLkp.NTNL_PROV_ID"))
    ).otherwise(F.lit("NA")).alias("NTNL_PROV_ID")
)

# PCP
df_PCP = df_Br.filter(
    (~F.col("PCPLkup.MBR_UNIQ_KEY").isNull()) & (F.length(trim(F.col("PCPLkup.MBR_UNIQ_KEY"))) > 0)
).select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.col("PassThru").alias("PASS_THRU_IN"),
    F.col("CurDateTime").alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit(SrcSysCd).alias("SRC_SYS_CD"),
    F.concat_ws(";", F.col("PkString"), F.lit("PCP")).alias("PRI_KEY_STRING"),
    F.lit(0).alias("CLM_PROV_SK"),
    F.rpad(F.col("ClmId"), 18, " ").alias("CLM_ID"),
    F.rpad(F.lit("PCP"), 10, " ").alias("CLM_PROV_ROLE_TYP_CD"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.when(
        trim(F.col("PCPLkup.PROV_ID")).isNull(),
        F.lit("UNK")
    ).otherwise(trim(F.col("PCPLkup.PROV_ID"))).alias("PROV_ID"),
    F.rpad(
        F.when(trim(F.col("PCPLkup.TAX_ID")).isNull(), F.lit("UNK"))
         .otherwise(trim(F.col("PCPLkup.TAX_ID"))),
        9, " "
    ).alias("TAX_ID"),
    F.when(trim(F.col("PCPLkup.NTNL_PROV_ID")).isNull(), F.lit("UNK"))
     .otherwise(trim(F.col("PCPLkup.NTNL_PROV_ID"))).alias("NTNL_PROV_ID")
)

# PRSCRB
df_PRSCRB = df_Br.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.col("PassThru").alias("PASS_THRU_IN"),
    F.col("CurDateTime").alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit(SrcSysCd).alias("SRC_SYS_CD"),
    F.concat_ws(";", F.col("PkString"), F.lit("PRSCRB")).alias("PRI_KEY_STRING"),
    F.lit(0).alias("CLM_PROV_SK"),
    F.rpad(F.col("ClmId"), 18, " ").alias("CLM_ID"),
    F.rpad(F.lit("PRSCRB"), 10, " ").alias("CLM_PROV_ROLE_TYP_CD"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.when(
        (~F.col("DeaNbrLkup.PROV_ID").isNull()) & (~F.col("MinDeaLkup.PROV_ID").isNull()),
        trim(F.col("DeaNbrLkup.PROV_ID"))
    ).otherwise(
        F.when(
            (~F.col("PrscrbProvLkup.PROV_ID").isNull()) & (~F.col("MinPrscrbLkup.PROV_ID").isNull()),
            trim(F.col("PrscrbProvLkup.PROV_ID"))
        ).otherwise(F.lit("UNK"))
    ).alias("PROV_ID"),
    F.rpad(
        F.when(
            (~F.col("DeaNbrLkup.TAX_ID").isNull()) & (~F.col("MinDeaLkup.DEA_NO").isNull()),
            F.col("DeaNbrLkup.TAX_ID")
        ).otherwise(
            F.when(
                (~F.col("PrscrbProvLkup.TAX_ID").isNull()) & (~F.col("MinPrscrbLkup.NTNL_PROV_ID").isNull()),
                F.col("PrscrbProvLkup.TAX_ID")
            ).otherwise(F.lit("UNK"))
        ),
        9, " "
    ).alias("TAX_ID"),
    F.when(
        (~F.col("Extract.PRESCRIBER_ID").isNull()) | (~F.col("Extract.PRESCRIBER_ID_NPI").isNull()),
        F.when(
            F.col("DeaNtnlLkp.NTNL_PROV_ID").isNull() | (F.col("DeaNtnlLkp.NTNL_PROV_ID")=="NA"),
            F.when(~F.col("Extract.PRESCRIBER_ID_NPI").isNull(), F.col("Extract.PRESCRIBER_ID_NPI")).otherwise(F.lit("NA"))
        ).otherwise(F.col("DeaNtnlLkp.NTNL_PROV_ID"))
    ).otherwise(F.lit("NA")).alias("NTNL_PROV_ID")
)

# -------------------------------------------------------
# Link_Collector => union all SVC, PCP, PRSCRB => Trans
# -------------------------------------------------------
common_cols = [
    "JOB_EXCTN_RCRD_ERR_SK","INSRT_UPDT_CD","DISCARD_IN","PASS_THRU_IN","FIRST_RECYC_DT",
    "ERR_CT","RECYCLE_CT","SRC_SYS_CD","PRI_KEY_STRING","CLM_PROV_SK","CLM_ID",
    "CLM_PROV_ROLE_TYP_CD","CRT_RUN_CYC_EXCTN_SK","LAST_UPDT_RUN_CYC_EXCTN_SK","PROV_ID","TAX_ID","NTNL_PROV_ID"
]

df_Trans = df_SVC.select(common_cols).unionByName(
    df_PCP.select(common_cols)
).unionByName(
    df_PRSCRB.select(common_cols)
)

# -------------------------------------------------------
# Snapshot (CTransformerStage) => outputs SnapShot and AllCol, Transform
# -------------------------------------------------------
df_SnapShot = df_Trans.select(
    F.rpad(F.col("CLM_ID"),18," ").alias("CLM_ID"),
    F.rpad(F.col("CLM_PROV_ROLE_TYP_CD"),10," ").alias("CLM_PROV_ROLE_TYP_CD"),
    F.rpad(F.col("PROV_ID"),12," ").alias("PROV_ID")
)

df_AllCol = df_Trans.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID"),
    F.col("CLM_PROV_ROLE_TYP_CD"),
    "JOB_EXCTN_RCRD_ERR_SK","INSRT_UPDT_CD","DISCARD_IN","PASS_THRU_IN","FIRST_RECYC_DT",
    "ERR_CT","RECYCLE_CT","SRC_SYS_CD","PRI_KEY_STRING","CLM_PROV_SK",
    "CRT_RUN_CYC_EXCTN_SK","LAST_UPDT_RUN_CYC_EXCTN_SK",
    F.expr("trim(PROV_ID)").alias("PROV_ID"),
    "TAX_ID",
    F.lit("NA").alias("SVC_FCLTY_LOC_NTNL_PROV_ID"),
    F.when(F.col("NTNL_PROV_ID").isNull(),F.lit("UNK")).otherwise(F.col("NTNL_PROV_ID")).alias("NTNL_PROV_ID")
)

df_Transform_3Cols = df_Trans.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    "CLM_ID",
    "CLM_PROV_ROLE_TYP_CD"
)

# -------------------------------------------------------
# Next Transformer => uses SnapShot => outputs RowCount to B_CLM_PROV
# -------------------------------------------------------
# Stage variable: svClmProvRole = GetFkeyCodes('MEDTRAK', 0, "CLAIM PROVIDER ROLE TYPE", SnapShot.CLM_PROV_ROLE_TYP_CD, 'X')
# We assume "GetFkeyCodes" is a user-defined function available.

df_SnapShot_alias = df_SnapShot.alias("SnapShot")
df_Transformer = df_SnapShot_alias.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("SnapShot.CLM_ID").alias("CLM_ID"),
    GetFkeyCodes("MEDTRAK", F.lit(0), "CLAIM PROVIDER ROLE TYPE",
                 F.col("SnapShot.CLM_PROV_ROLE_TYP_CD"), "X").alias("CLM_PROV_ROLE_TYP_CD_SK"),
    F.rpad(F.col("SnapShot.PROV_ID"),12," ").alias("PROV_ID")
).alias("RowCount")

# -------------------------------------------------------
# B_CLM_PROV => CSeqFileStage => write
# -------------------------------------------------------
df_B_CLM_PROV = df_Transformer.select(
    F.col("SRC_SYS_CD_SK"),
    F.rpad(F.col("CLM_ID"),18," ").alias("CLM_ID"),
    F.rpad(F.col("CLM_PROV_ROLE_TYP_CD_SK"),10," ").alias("CLM_PROV_ROLE_TYP_CD_SK"),
    F.col("PROV_ID")
)

write_files(
    df_B_CLM_PROV,
    f"{adls_path}/load/B_CLM_PROV.{SrcSysCd}.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# -------------------------------------------------------
# ClmProvPK container => takes AllCol, Transform => outputs Key
# -------------------------------------------------------
params_ClmProvPK = {
    "CurrRunCycle": RunCycle,
    "SrcSysCd": SrcSysCd,
    "IDSOwner": IDSOwner
}
df_ClmProvPK_Key = ClmProvPK(df_AllCol, df_Transform_3Cols, params_ClmProvPK)

# -------------------------------------------------------
# MedtrakClmProvExtr => CSeqFileStage => final write
# -------------------------------------------------------
df_MedtrakClmProvExtr = df_ClmProvPK_Key.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("INSRT_UPDT_CD"),10," ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("DISCARD_IN"),1," ").alias("DISCARD_IN"),
    F.rpad(F.col("PASS_THRU_IN"),1," ").alias("PASS_THRU_IN"),
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
    F.rpad(F.col("TAX_ID"),9," ").alias("TAX_ID"),
    F.col("SVC_FCLTY_LOC_NTNL_PROV_ID"),
    F.when(F.col("NTNL_PROV_ID").isNull(),F.lit("UNK")).otherwise(F.col("NTNL_PROV_ID")).alias("NTNL_PROV_ID")
)

write_files(
    df_MedtrakClmProvExtr,
    f"{adls_path}/key/MedtrakClmProvExtr.DrugClmProv.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)