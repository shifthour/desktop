# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2012 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  BCAFEPMedClmLnExtrSeq
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC     Reads the BCAFEPMedClm_Landing.dat file and runs through primary key using shared container ClmLnPkey
# MAGIC     
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                           Date                 	Project/Altiris #	       Change Description			                  Development Project	  Code Reviewer	    Date Reviewed       
# MAGIC ------------------                       --------------------     	------------------------	-----------------------------------------------------------------------	                 --------------------------------	-------------------------------	    ---------------------------       
# MAGIC Sudhir Bomshetty              2017-09-28                    5781 - HEDIS                 Initial Programming                                                                IntegrateDev2                        Kalyan Neelam           2017-10-18
# MAGIC 
# MAGIC Sudhir Bomshetty              2018-01-11                    5781 - HEDIS                 Changing rule for CLM_LN_SVC_LOC_TYP_CD_SK           IntegrateDev2                        Kalyan Neelam           2018-01-16
# MAGIC 
# MAGIC Madhavan B                     2018-02-06                    5792 	                     Changed the datatype of the column                                      IntegrateDev1                       Kalyan Neelam           2018-02-08
# MAGIC                                                       		                                     NDC_UNIT_CT from SMALLINT to DECIMAL (21,10)
# MAGIC 
# MAGIC Saikiran Mahenderker      2018-03-28                     5781 HEDIS                 Changing logic for SVC_PROV_ID                                         IntegrateDev2                       Jaideep Mankala        04/02/2018
# MAGIC 
# MAGIC 
# MAGIC Karthik Chintalapani    2020-03-23                    5781 HEDIS        Changed logic for Procedure code field as per the mapping changes.       IntegrateDev1     Jaideep Mankala        03/29/2020           
# MAGIC 
# MAGIC Sayam Sagar                 2020-08-28                  6264-PBM                        Added APC_ID,APC_STTUS_ID fileds                                      IntegrateDev5                     Kalyan Neelam            2020-12-10
# MAGIC                                                                           Phase II - Government Programs 
# MAGIC Prabhu ES                      2022-02-26                 S2S Remediation               MSSQL connection parameters added                                     IntegrateDev5
# MAGIC 
# MAGIC Amritha A J                      2023-07-31                US 589700           Added two new fields SNOMED_CT_CD,CVX_VCCN_CD with a default   IntegrateDevB	Harsha Ravuri	2023-08-31
# MAGIC                                                                                                     value in BusinessRules stage and mapped it till target

# MAGIC Added new field MED_PDX_IND to the common layout file that is sent to IdsClmLnFkey job
# MAGIC which indicates whether the claim is Medical or Pharmacy
# MAGIC Indicator used :
# MAGIC MED - medical
# MAGIC PDX - Pharmacy
# MAGIC This container is used in:
# MAGIC ESIClmLnExtr
# MAGIC FctsClmLnDntlTrns
# MAGIC FctsClmLnMedTrns
# MAGIC MCSourceClmLnExtr
# MAGIC MedicaidClmLnExtr
# MAGIC NascoClmLnExtr
# MAGIC PcsClmLnExtr
# MAGIC PCTAClmLnExtr
# MAGIC WellDyneClmLnExtr
# MAGIC MedtrakClmLnExtr
# MAGIC BCBSSCClmLnExtr
# MAGIC BCBSSCMedClmLnExtr
# MAGIC BCCAFEPMedClmLnExtr
# MAGIC 
# MAGIC These programs need to be re-compiled when logic changes
# MAGIC Writing Sequential File to /key
# MAGIC Read the BCA FEP Med Clm file created from BCAClmLnLand
# MAGIC Assign primary surrogate key
# MAGIC Apply business logic
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DecimalType
)
# COMMAND ----------
# MAGIC %run ../../../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../../../shared_containers/PrimaryKey/ClmLnPK
# COMMAND ----------

# Retrieve all parameter values
CurrentDate = get_widget_value('CurrentDate','')
SrcSysCd = get_widget_value('SrcSysCd','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
RunID = get_widget_value('RunID','')
RunCycle = get_widget_value('RunCycle','')
IDSOwner = get_widget_value('IDSOwner','')
FacetsOwner = get_widget_value('FacetsOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
facets_secret_name = get_widget_value('facets_secret_name','')

# ----------------------------------------------------------------------------
# Stage: PROC_CD (DB2Connector)
# ----------------------------------------------------------------------------
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
extract_query_proc_cd = (
    f"SELECT PROC_CD,\n"
    f"       PROC_CD_TYP_CD,\n"
    f"       PROC_CD_CAT_CD\n"
    f"FROM {IDSOwner}.PROC_CD"
)
df_PROC_CD = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_proc_cd)
    .load()
)

# ----------------------------------------------------------------------------
# Stage: hf_ClmLn_ProcCdTypCd (CHashedFileStage) - Scenario A dedup
# ----------------------------------------------------------------------------
df_hf_ClmLn_ProcCdTypCd = df_PROC_CD.dropDuplicates(["PROC_CD", "PROC_CD_TYP_CD", "PROC_CD_CAT_CD"])

# ----------------------------------------------------------------------------
# Stage: BCAFEPMedClmLnLanding (CSeqFileStage) - Read
# ----------------------------------------------------------------------------
schema_BCAFEPMedClmLnLanding = StructType([
    StructField("CLM_ID", StringType(), True),
    StructField("CLM_LN_NO", IntegerType(), True),
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
    StructField("PERFORMING_NTNL_PROV_ID", StringType(), True),
])

df_Extract = (
    spark.read.format("csv")
    .option("header", "false")
    .option("quote", '"')
    .option("escape", '"')
    .schema(schema_BCAFEPMedClmLnLanding)
    .load(f"{adls_path}/verified/BCAFEPCMedClm_ClmLnlanding.dat.{RunID}")
)

# ----------------------------------------------------------------------------
# Stage: Facets (ODBCConnector)
# ----------------------------------------------------------------------------
jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)
df_Facets = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("dbtable", f"{FacetsOwner}.CMC_PSCD_POS_DESC")
    .load()
)

# ----------------------------------------------------------------------------
# Stage: IDS_PROV (DB2Connector)
# ----------------------------------------------------------------------------
extract_query_prov = (
    f"SELECT PROV.PROV_ID\n"
    f"FROM {IDSOwner}.CD_MPPNG CD,\n"
    f"     {IDSOwner}.PROV PROV\n"
    f"WHERE PROV.SRC_SYS_CD_SK = CD.CD_MPPNG_SK\n"
    f"  AND CD.TRGT_CD= 'BCA'"
)
df_Prov = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_prov)
    .load()
)

extract_query_ntnl_prov = (
    f"SELECT PROV2.NTNL_PROV_ID, PROV2.PROV_ID\n"
    f"FROM (\n"
    f"  SELECT PROV.NTNL_PROV_ID, MIN(PROV.PROV_ID) PROV_ID, CD2.TRGT_CD\n"
    f"  FROM {IDSOwner}.PROV PROV, {IDSOwner}.CD_MPPNG CD2\n"
    f"  WHERE PROV.NTNL_PROV_ID<>'NA'\n"
    f"    AND PROV.SRC_SYS_CD_SK = CD2.CD_MPPNG_SK\n"
    f"    AND PROV.TERM_DT_SK  >  '{CurrentDate}'\n"
    f"  GROUP BY PROV.NTNL_PROV_ID, CD2.TRGT_CD\n"
    f") PROV1,\n"
    f"{IDSOwner}.CD_MPPNG CD,\n"
    f"{IDSOwner}.PROV PROV2\n"
    f"WHERE PROV2.PROV_ID = PROV1.PROV_ID\n"
    f"  AND PROV2.SRC_SYS_CD_SK = CD.CD_MPPNG_SK\n"
    f"  AND CD.TRGT_CD= 'BCA'"
)
df_Ntnl_ProvId = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_ntnl_prov)
    .load()
)

# ----------------------------------------------------------------------------
# Stage: hf_ProvId (CHashedFileStage) - Scenario A dedup (two links)
# ----------------------------------------------------------------------------
# Link "Prov" => "ProvId": primary key is PROV_ID
df_ProvId = df_Prov.dropDuplicates(["PROV_ID"])

# Link "Ntnl_ProvId" => "NtnlProvId": primary key is NTNL_PROV_ID
df_NtnlProvId = df_Ntnl_ProvId.dropDuplicates(["NTNL_PROV_ID"])

# ----------------------------------------------------------------------------
# Stage: BusinessRules (CTransformerStage)
# ----------------------------------------------------------------------------
# We must combine df_Extract (primary) with df_Facets (left, no join condition => cross join),
# plus df_ProvId (left join on PERFORMING_PROV_ID=PROV_ID),
# plus df_NtnlProvId (left join on PERFORMING_PROV_ID=NTNL_PROV_ID).
# Then apply the stage variable logic.
# Then produce columns from the output pin "Transform".

# Cross join with df_Facets (since no conditions were given).
df_brs = df_Extract.crossJoin(df_Facets.alias("CMC_PSCD_POS_DESC"))

# Join ProvId (left)
df_brs = df_brs.join(
    df_ProvId.alias("ProvId"),
    on=(df_brs["PERFORMING_PROV_ID"] == F.col("ProvId.PROV_ID")),
    how="left"
)

# Join NtnlProvId (left)
df_brs = df_brs.join(
    df_NtnlProvId.alias("NtnlProvId"),
    on=(df_brs["PERFORMING_PROV_ID"] == F.col("NtnlProvId.NTNL_PROV_ID")),
    how="left"
)

# Stage variables in Transformer:
# svProcCd = Left(Trim(Extract.OTHR_PROC_CD_1), 5)
df_brs = df_brs.withColumn(
    "svProcCd",
    F.substring(trim(F.col("OTHR_PROC_CD_1")), 1, 5)
)
# svProcCdCatCd = 'MED'
df_brs = df_brs.withColumn("svProcCdCatCd", F.lit("MED"))

# svProcCdTypCd = If Num(svProcCd[1,1]) Then 'CPT4' Else 'HCPCS'
# We'll interpret "Num(x[1,1])" as checking if the first character is digit
df_brs = df_brs.withColumn(
    "svProcCdTypCd",
    F.when(
        F.col("svProcCd").substr(F.lit(1), F.lit(1)).rlike("^[0-9]$"),
        F.lit("CPT4")
    ).otherwise(F.lit("HCPCS"))
)

# svProcCdTypCd2 = same logic
df_brs = df_brs.withColumn(
    "svProcCdTypCd2",
    F.when(
        F.col("svProcCd").substr(F.lit(1), F.lit(1)).rlike("^[0-9]$"),
        F.lit("CPT4")
    ).otherwise(F.lit("HCPCS"))
)

# Output columns with expressions

df_brs = df_brs.withColumn("JOB_EXCTN_RCRD_ERR_SK", F.lit(0)) \
    .withColumn("INSRT_UPDT_CD", F.lit("I")) \
    .withColumn("DISCARD_IN", F.lit("N")) \
    .withColumn("PASS_THRU_IN", F.lit("Y")) \
    .withColumn("FIRST_RECYC_DT", current_date()) \
    .withColumn("ERR_CT", F.lit(0)) \
    .withColumn("RECYCLE_CT", F.lit(0)) \
    .withColumn("SRC_SYS_CD", F.lit(SrcSysCd)) \
    .withColumn("PRI_KEY_STRING",
                F.concat_ws(";", F.lit(SrcSysCd), F.col("CLM_ID"), F.col("CLM_LN_NO"))) \
    .withColumn("CLM_LN_SK", F.lit(0)) \
    .withColumn("CLM_ID", F.col("CLM_ID")) \
    .withColumn("CLM_LN_SEQ_NO", F.col("CLM_LN_NO")) \
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(RunCycle)) \
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(RunCycle)) \
    .withColumn("PROC_CD", F.col("svProcCd")) \
    .withColumn(
        "SVC_PROV_ID",
        F.when(
            F.col("PERFORMING_NTNL_PROV_ID").isNotNull(),
            F.when(
                ~F.col("PERFORMING_NTNL_PROV_ID").substr(F.lit(1), F.lit(1)).rlike("^[0-9]$"),
                F.when(F.col("ProvId.PROV_ID").isNotNull(), F.col("ProvId.PROV_ID")).otherwise(F.lit("UNK"))
            ).otherwise(
                F.when(F.col("NtnlProvId.PROV_ID").isNotNull(), F.col("NtnlProvId.PROV_ID")).otherwise(F.lit("UNK"))
            )
        ).otherwise(F.lit("NA"))
    ) \
    .withColumn("CLM_LN_DSALW_EXCD", F.lit("NA")) \
    .withColumn("CLM_LN_EOB_EXCD", F.lit("NA")) \
    .withColumn(
        "CLM_LN_FINL_DISP_CD",
        F.when(F.col("CLM_DENIED_FLAG") == "Y", F.lit("DENIEDREJ")).otherwise(F.lit("ACPTD"))
    ) \
    .withColumn("CLM_LN_LOB_CD", F.lit("NA")) \
    .withColumn(
        "CLM_LN_POS_CD",
        F.when(F.col("FEP_SVC_LOC_CD").isNull() | (F.length(F.col("FEP_SVC_LOC_CD")) == 0), F.lit("NA"))
        .otherwise(F.col("FEP_SVC_LOC_CD"))
    ) \
    .withColumn("CLM_LN_PREAUTH_CD", F.lit("NA")) \
    .withColumn("CLM_LN_PREAUTH_SRC_CD", F.lit("NA")) \
    .withColumn("CLM_LN_PRICE_SRC_CD", F.lit("NA")) \
    .withColumn("CLM_LN_RFRL_CD", F.lit("NA")) \
    .withColumn(
        "CLM_LN_RVNU_CD",
        F.when(F.col("RVNU_CD").isNull() | (F.length(F.col("RVNU_CD")) == 0), F.lit("NA")).otherwise(F.col("RVNU_CD"))
    ) \
    .withColumn("CLM_LN_ROOM_PRICE_METH_CD", F.lit("NA")) \
    .withColumn("CLM_LN_ROOM_TYP_CD", F.lit("NA")) \
    .withColumn("CLM_LN_TOS_CD", F.lit("NA")) \
    .withColumn("CLM_LN_UNIT_TYP_CD", F.lit("NA")) \
    .withColumn("CAP_LN_IN", F.lit("N")) \
    .withColumn("PRI_LOB_IN", F.lit("N")) \
    .withColumn(
        "SVC_END_DT",
        F.when(
            F.col("CLM_SVC_DT_END").isNull() | (F.length(F.col("CLM_SVC_DT_END")) == 0),
            F.lit("2199-12-31")
        ).otherwise(F.col("CLM_SVC_DT_END"))
    ) \
    .withColumn(
        "SVC_STRT_DT",
        F.when(
            F.col("CLM_SVC_DT_BEG").isNull() | (F.length(F.col("CLM_SVC_DT_BEG")) == 0),
            F.lit("1753-01-01")
        ).otherwise(F.col("CLM_SVC_DT_BEG"))
    ) \
    .withColumn("AGMNT_PRICE_AMT", F.lit("0.00")) \
    .withColumn("ALW_AMT", F.lit("0.00")) \
    .withColumn(
        "CHRG_AMT",
        F.when(F.col("CLM_LN_TOT_ALL_SVC_CHRG_AMT").isNull(), F.lit("0.00"))
        .otherwise(F.col("CLM_LN_TOT_ALL_SVC_CHRG_AMT").cast(StringType()))
    ) \
    .withColumn("COINS_AMT", F.lit("0.00")) \
    .withColumn("CNSD_CHRG_AMT", F.lit("0.00")) \
    .withColumn("COPAY_AMT", F.lit("0.00")) \
    .withColumn("DEDCT_AMT", F.lit("0.00")) \
    .withColumn("DSALW_AMT", F.lit("0.00")) \
    .withColumn("ITS_HOME_DSCNT_AMT", F.lit("0.00")) \
    .withColumn("NO_RESP_AMT", F.lit("0.00")) \
    .withColumn("MBR_LIAB_BSS_AMT", F.lit("0.00")) \
    .withColumn("PATN_RESP_AMT", F.lit("0.00")) \
    .withColumn("PAYBL_AMT", F.lit("0.00")) \
    .withColumn("PAYBL_TO_PROV_AMT", F.lit("0.00")) \
    .withColumn("PAYBL_TO_SUB_AMT", F.lit("0.00")) \
    .withColumn("PROC_TBL_PRICE_AMT", F.lit("0.00")) \
    .withColumn("PROFL_PRICE_AMT", F.lit("0.00")) \
    .withColumn("PROV_WRT_OFF_AMT", F.lit("0.00")) \
    .withColumn("RISK_WTHLD_AMT", F.lit("0.00")) \
    .withColumn("SVC_PRICE_AMT", F.lit("0.00")) \
    .withColumn("SUPLMT_DSCNT_AMT", F.lit("0.00")) \
    .withColumn("ALW_PRICE_UNIT_CT", F.lit(0)) \
    .withColumn("UNIT_CT", F.lit(0)) \
    .withColumn("DEDCT_AMT_ACCUM_ID", F.lit("NA")) \
    .withColumn("PREAUTH_SVC_SEQ_NO", F.lit("NA")) \
    .withColumn("RFRL_SVC_SEQ_NO", F.lit("NA")) \
    .withColumn("LMT_PFX_ID", F.lit("NA")) \
    .withColumn("PREAUTH_ID", F.lit("NA")) \
    .withColumn("PROD_CMPNT_DEDCT_PFX_ID", F.lit("NA")) \
    .withColumn("PROD_CMPNT_SVC_PAYMT_ID", F.lit("NA")) \
    .withColumn("RFRL_ID_TX", F.lit("NA")) \
    .withColumn(
        "SVC_ID",
        F.when(
            F.col("PRINCIPLE_PROC_CD").isNull() | (F.length(trim(F.col("PRINCIPLE_PROC_CD"))) == 0),
            F.lit("NA")
        ).otherwise(F.col("PRINCIPLE_PROC_CD"))
    ) \
    .withColumn("SVC_PRICE_RULE_ID", F.lit("NA")) \
    .withColumn("SVC_RULE_TYP_TX", F.lit("NA")) \
    .withColumn(
        "SVC_LOC_TYP_CD",
        F.when(
            F.col("IP_CLM_TYP_IN") == "I",
            F.lit("IP")
        ).otherwise(
            F.when(
                F.col("IP_CLM_TYP_IN").isNull() | (F.length(trim(F.col("IP_CLM_TYP_IN"))) == 0),
                F.when(
                    F.col("PATN_STTUS_CD").isNull() & (F.col("CLM_CLS_IN") == "F"),
                    F.lit("OP")
                ).otherwise(
                    F.when(
                        F.col("PATN_STTUS_CD").isNull() & (F.col("CLM_CLS_IN") == "M"),
                        F.when(F.col("CMC_PSCD_POS_DESC.TPCT_MCTR_SETG") == "I", F.lit("IP"))
                         .when(F.col("CMC_PSCD_POS_DESC.TPCT_MCTR_SETG") == "O", F.lit("OP"))
                         .otherwise(F.lit("NA"))
                    ).otherwise(F.lit("NA"))
                )
            ).otherwise(F.lit("NA"))
        )
    ) \
    .withColumn("NON_PAR_SAV_AMT", F.lit("0.00")) \
    .withColumn("VBB_RULE_ID", F.lit("NA")) \
    .withColumn("VBB_EXCD_ID", F.lit("NA")) \
    .withColumn("CLM_LN_VBB_IN", F.lit("N")) \
    .withColumn("ITS_SUPLMT_DSCNT_AMT", F.lit("0.00")) \
    .withColumn("ITS_SRCHRG_AMT", F.lit("0.00")) \
    .withColumn("PROC_CD_TYP_CD", F.col("svProcCdTypCd")) \
    .withColumn("PROC_CD_TYP_CD2", F.col("svProcCdTypCd2")) \
    .withColumn("PROC_CD_CAT_CD", F.col("svProcCdCatCd")) \
    .withColumn("SNOMED_CT_CD", F.lit("NA")) \
    .withColumn("CVX_VCCN_CD", F.lit("NA"))

df_BusinessRules = df_brs

# ----------------------------------------------------------------------------
# Stage: Snapshot (CTransformerStage)
# ----------------------------------------------------------------------------
# We take the primary link from BusinessRules => "Transform"
# Then do left joins with df_hf_ClmLn_ProcCdTypCd for "ProcCd1" and "ProcCd2" on different join conditions.

df_snap = df_BusinessRules \
    .join(
        df_hf_ClmLn_ProcCdTypCd.alias("ProcCd1"),
        on=[
            df_BusinessRules["PROC_CD"] == F.col("ProcCd1.PROC_CD"),
            df_BusinessRules["PROC_CD_TYP_CD"] == F.col("ProcCd1.PROC_CD_TYP_CD"),
            df_BusinessRules["PROC_CD_CAT_CD"] == F.col("ProcCd1.PROC_CD_CAT_CD")
        ],
        how="left"
    ) \
    .join(
        df_hf_ClmLn_ProcCdTypCd.alias("ProcCd2"),
        on=[
            df_BusinessRules["PROC_CD"] == F.col("ProcCd2.PROC_CD"),
            df_BusinessRules["PROC_CD_TYP_CD2"] == F.col("ProcCd2.PROC_CD_TYP_CD"),
            df_BusinessRules["PROC_CD_CAT_CD"] == F.col("ProcCd2.PROC_CD_CAT_CD")
        ],
        how="left"
    )

# Snapshot stage variables:
# svProcCd = If IsNull(ProcCd1.PROC_CD)=FALSE => ProcCd1.PROC_CD else if IsNull(ProcCd2.PROC_CD)=FALSE => ...
df_snap = df_snap.withColumn(
    "svProcCd",
    F.when(
        (F.col("ProcCd1.PROC_CD").isNotNull()) & (F.length(trim(F.col("ProcCd1.PROC_CD"))) != 0),
        F.col("ProcCd1.PROC_CD")
    ).otherwise(
        F.when(
            (F.col("ProcCd2.PROC_CD").isNotNull()) & (F.length(trim(F.col("ProcCd2.PROC_CD"))) != 0),
            F.col("ProcCd2.PROC_CD")
        ).otherwise(F.lit("UNK"))
    )
)
# svProcCdTypCd = similar logic
df_snap = df_snap.withColumn(
    "svProcCdTypCd",
    F.when(
        (F.col("ProcCd1.PROC_CD").isNotNull()) & (F.length(trim(F.col("ProcCd1.PROC_CD"))) != 0),
        F.col("ProcCd1.PROC_CD_TYP_CD")
    ).otherwise(
        F.when(
            (F.col("ProcCd2.PROC_CD").isNotNull()) & (F.length(trim(F.col("ProcCd2.PROC_CD"))) != 0),
            F.col("ProcCd2.PROC_CD_TYP_CD")
        ).otherwise(F.lit("UNK"))
    )
)

# Two output pins:
# 1) PKey => goes to ClmLnPK
df_pkey = df_snap.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("DISCARD_IN").alias("DISCARD_IN"),
    F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("ERR_CT").alias("ERR_CT"),
    F.col("RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("CLM_LN_SK").alias("CLM_LN_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("svProcCd").alias("PROC_CD"),
    F.col("SVC_PROV_ID").alias("SVC_PROV_ID"),
    F.col("CLM_LN_DSALW_EXCD").alias("CLM_LN_DSALW_EXCD"),
    F.col("CLM_LN_EOB_EXCD").alias("CLM_LN_EOB_EXCD"),
    F.col("CLM_LN_FINL_DISP_CD").alias("CLM_LN_FINL_DISP_CD"),
    F.col("CLM_LN_LOB_CD").alias("CLM_LN_LOB_CD"),
    F.col("CLM_LN_POS_CD").alias("CLM_LN_POS_CD"),
    F.col("CLM_LN_PREAUTH_CD").alias("CLM_LN_PREAUTH_CD"),
    F.col("CLM_LN_PREAUTH_SRC_CD").alias("CLM_LN_PREAUTH_SRC_CD"),
    F.col("CLM_LN_PRICE_SRC_CD").alias("CLM_LN_PRICE_SRC_CD"),
    F.col("CLM_LN_RFRL_CD").alias("CLM_LN_RFRL_CD"),
    F.col("CLM_LN_RVNU_CD").alias("CLM_LN_RVNU_CD"),
    F.col("CLM_LN_ROOM_PRICE_METH_CD").alias("CLM_LN_ROOM_PRICE_METH_CD"),
    F.col("CLM_LN_ROOM_TYP_CD").alias("CLM_LN_ROOM_TYP_CD"),
    F.col("CLM_LN_TOS_CD").alias("CLM_LN_TOS_CD"),
    F.col("CLM_LN_UNIT_TYP_CD").alias("CLM_LN_UNIT_TYP_CD"),
    F.col("CAP_LN_IN").alias("CAP_LN_IN"),
    F.col("PRI_LOB_IN").alias("PRI_LOB_IN"),
    F.col("SVC_END_DT").alias("SVC_END_DT"),
    F.col("SVC_STRT_DT").alias("SVC_STRT_DT"),
    F.col("AGMNT_PRICE_AMT").alias("AGMNT_PRICE_AMT"),
    F.col("ALW_AMT").alias("ALW_AMT"),
    F.col("CHRG_AMT").alias("CHRG_AMT"),
    F.col("COINS_AMT").alias("COINS_AMT"),
    F.col("CNSD_CHRG_AMT").alias("CNSD_CHRG_AMT"),
    F.col("COPAY_AMT").alias("COPAY_AMT"),
    F.col("DEDCT_AMT").alias("DEDCT_AMT"),
    F.col("DSALW_AMT").alias("DSALW_AMT"),
    F.col("ITS_HOME_DSCNT_AMT").alias("ITS_HOME_DSCNT_AMT"),
    F.col("NO_RESP_AMT").alias("NO_RESP_AMT"),
    F.col("MBR_LIAB_BSS_AMT").alias("MBR_LIAB_BSS_AMT"),
    F.col("PATN_RESP_AMT").alias("PATN_RESP_AMT"),
    F.col("PAYBL_AMT").alias("PAYBL_AMT"),
    F.col("PAYBL_TO_PROV_AMT").alias("PAYBL_TO_PROV_AMT"),
    F.col("PAYBL_TO_SUB_AMT").alias("PAYBL_TO_SUB_AMT"),
    F.col("PROC_TBL_PRICE_AMT").alias("PROC_TBL_PRICE_AMT"),
    F.col("PROFL_PRICE_AMT").alias("PROFL_PRICE_AMT"),
    F.col("PROV_WRT_OFF_AMT").alias("PROV_WRT_OFF_AMT"),
    F.col("RISK_WTHLD_AMT").alias("RISK_WTHLD_AMT"),
    F.col("SVC_PRICE_AMT").alias("SVC_PRICE_AMT"),
    F.col("SUPLMT_DSCNT_AMT").alias("SUPLMT_DSCNT_AMT"),
    F.col("ALW_PRICE_UNIT_CT").alias("ALW_PRICE_UNIT_CT"),
    F.col("UNIT_CT").alias("UNIT_CT"),
    F.col("DEDCT_AMT_ACCUM_ID").alias("DEDCT_AMT_ACCUM_ID"),
    F.col("PREAUTH_SVC_SEQ_NO").alias("PREAUTH_SVC_SEQ_NO"),
    F.col("RFRL_SVC_SEQ_NO").alias("RFRL_SVC_SEQ_NO"),
    F.col("LMT_PFX_ID").alias("LMT_PFX_ID"),
    F.col("PREAUTH_ID").alias("PREAUTH_ID"),
    F.col("PROD_CMPNT_DEDCT_PFX_ID").alias("PROD_CMPNT_DEDCT_PFX_ID"),
    F.col("PROD_CMPNT_SVC_PAYMT_ID").alias("PROD_CMPNT_SVC_PAYMT_ID"),
    F.col("RFRL_ID_TX").alias("RFRL_ID_TX"),
    F.col("SVC_ID").alias("SVC_ID"),
    F.col("SVC_PRICE_RULE_ID").alias("SVC_PRICE_RULE_ID"),
    F.col("SVC_RULE_TYP_TX").alias("SVC_RULE_TYP_TX"),
    F.col("SVC_LOC_TYP_CD").alias("SVC_LOC_TYP_CD"),
    F.col("NON_PAR_SAV_AMT").alias("NON_PAR_SAV_AMT"),
    F.col("svProcCdTypCd").alias("PROC_CD_TYP_CD"),
    F.col("PROC_CD_CAT_CD").alias("PROC_CD_CAT_CD"),
    F.col("VBB_RULE_ID").alias("VBB_RULE_ID"),
    F.col("VBB_EXCD_ID").alias("VBB_EXCD_ID"),
    F.col("CLM_LN_VBB_IN").alias("CLM_LN_VBB_IN"),
    F.col("ITS_SUPLMT_DSCNT_AMT").alias("ITS_SUPLMT_DSCNT_AMT"),
    F.col("ITS_SRCHRG_AMT").alias("ITS_SRCHRG_AMT"),
    F.lit("NA").alias("NDC"),
    F.lit("NA").alias("NDC_DRUG_FORM_CD"),
    F.lit(None).alias("NDC_UNIT_CT"),
    F.lit("MED").alias("MED_PDX_IND"),
    F.lit(None).alias("APC_ID"),
    F.lit(None).alias("APC_STTUS_ID"),
    F.col("SNOMED_CT_CD").alias("SNOMED_CT_CD"),
    F.col("CVX_VCCN_CD").alias("CVX_VCCN_CD"),
)

# 2) Snapshot => "Snapshot" link => goes to Transformer
df_snapshot2 = df_snap.select(
    F.col("CLM_ID").alias("CLCL_ID"),
    F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("svProcCd").alias("PROC_CD"),
    F.col("CLM_LN_RVNU_CD").alias("CLM_LN_RVNU_CD"),
    F.col("ALW_AMT").alias("ALW_AMT"),
    F.col("CHRG_AMT").alias("CHRG_AMT"),
    F.col("PAYBL_AMT").alias("PAYBL_AMT"),
    F.col("svProcCdTypCd").alias("PROC_CD_TYP_CD"),
    F.col("PROC_CD_CAT_CD").alias("PROC_CD_CAT_CD"),
)

# ----------------------------------------------------------------------------
# Stage: ClmLnPK (CContainerStage) - shared container
# ----------------------------------------------------------------------------
params_ClmLnPK = {
    "CurrRunCycle": RunCycle
}
df_pkey_out = ClmLnPK(df_pkey, params_ClmLnPK)

# ----------------------------------------------------------------------------
# Stage: BCAFEPMedClmLnExtr (CSeqFileStage) - final file
# ----------------------------------------------------------------------------
# We must rpad all char/varchar columns in the final select to match lengths from the metadata.
# The metadata is from the link "Key" => we see many columns with char / varchar. We apply rpad where appropriate.

df_final_bcafep = df_pkey_out.select(
    F.rpad(F.col("JOB_EXCTN_RCRD_ERR_SK").cast(StringType()), 1, " ").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.rpad(F.col("ERR_CT").cast(StringType()), 1, " ").alias("ERR_CT"),
    F.rpad(F.col("RECYCLE_CT").cast(StringType()), 1, " ").alias("RECYCLE_CT"),
    F.rpad(F.col("SRC_SYS_CD"), len(SrcSysCd), " ").alias("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.rpad(F.col("CLM_LN_SK").cast(StringType()), 1, " ").alias("CLM_LN_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.rpad(F.col("CLM_LN_SEQ_NO").cast(StringType()), 1, " ").alias("CLM_LN_SEQ_NO"),
    F.rpad(F.col("CRT_RUN_CYC_EXCTN_SK").cast(StringType()), len(RunCycle), " ").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").cast(StringType()), len(RunCycle), " ").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("PROC_CD").alias("PROC_CD"),
    F.col("SVC_PROV_ID").alias("SVC_PROV_ID"),
    F.col("CLM_LN_DSALW_EXCD").alias("CLM_LN_DSALW_EXCD"),
    F.col("CLM_LN_EOB_EXCD").alias("CLM_LN_EOB_EXCD"),
    F.col("CLM_LN_FINL_DISP_CD").alias("CLM_LN_FINL_DISP_CD"),
    F.col("CLM_LN_LOB_CD").alias("CLM_LN_LOB_CD"),
    F.col("CLM_LN_POS_CD").alias("CLM_LN_POS_CD"),
    F.col("CLM_LN_PREAUTH_CD").alias("CLM_LN_PREAUTH_CD"),
    F.col("CLM_LN_PREAUTH_SRC_CD").alias("CLM_LN_PREAUTH_SRC_CD"),
    F.col("CLM_LN_PRICE_SRC_CD").alias("CLM_LN_PRICE_SRC_CD"),
    F.col("CLM_LN_RFRL_CD").alias("CLM_LN_RFRL_CD"),
    F.col("CLM_LN_RVNU_CD").alias("CLM_LN_RVNU_CD"),
    F.rpad(F.col("CLM_LN_ROOM_PRICE_METH_CD"), 2, " ").alias("CLM_LN_ROOM_PRICE_METH_CD"),
    F.col("CLM_LN_ROOM_TYP_CD").alias("CLM_LN_ROOM_TYP_CD"),
    F.col("CLM_LN_TOS_CD").alias("CLM_LN_TOS_CD"),
    F.col("CLM_LN_UNIT_TYP_CD").alias("CLM_LN_UNIT_TYP_CD"),
    F.rpad(F.col("CAP_LN_IN"), 1, " ").alias("CAP_LN_IN"),
    F.rpad(F.col("PRI_LOB_IN"), 1, " ").alias("PRI_LOB_IN"),
    F.col("SVC_END_DT").alias("SVC_END_DT"),
    F.col("SVC_STRT_DT").alias("SVC_STRT_DT"),
    F.col("AGMNT_PRICE_AMT").alias("AGMNT_PRICE_AMT"),
    F.col("ALW_AMT").alias("ALW_AMT"),
    F.col("CHRG_AMT").alias("CHRG_AMT"),
    F.col("COINS_AMT").alias("COINS_AMT"),
    F.col("CNSD_CHRG_AMT").alias("CNSD_CHRG_AMT"),
    F.col("COPAY_AMT").alias("COPAY_AMT"),
    F.col("DEDCT_AMT").alias("DEDCT_AMT"),
    F.col("DSALW_AMT").alias("DSALW_AMT"),
    F.col("ITS_HOME_DSCNT_AMT").alias("ITS_HOME_DSCNT_AMT"),
    F.col("NO_RESP_AMT").alias("NO_RESP_AMT"),
    F.col("MBR_LIAB_BSS_AMT").alias("MBR_LIAB_BSS_AMT"),
    F.col("PATN_RESP_AMT").alias("PATN_RESP_AMT"),
    F.col("PAYBL_AMT").alias("PAYBL_AMT"),
    F.col("PAYBL_TO_PROV_AMT").alias("PAYBL_TO_PROV_AMT"),
    F.col("PAYBL_TO_SUB_AMT").alias("PAYBL_TO_SUB_AMT"),
    F.col("PROC_TBL_PRICE_AMT").alias("PROC_TBL_PRICE_AMT"),
    F.col("PROFL_PRICE_AMT").alias("PROFL_PRICE_AMT"),
    F.col("PROV_WRT_OFF_AMT").alias("PROV_WRT_OFF_AMT"),
    F.col("RISK_WTHLD_AMT").alias("RISK_WTHLD_AMT"),
    F.col("SVC_PRICE_AMT").alias("SVC_PRICE_AMT"),
    F.col("SUPLMT_DSCNT_AMT").alias("SUPLMT_DSCNT_AMT"),
    F.col("ALW_PRICE_UNIT_CT").alias("ALW_PRICE_UNIT_CT"),
    F.col("UNIT_CT").alias("UNIT_CT"),
    F.rpad(F.col("DEDCT_AMT_ACCUM_ID"), 4, " ").alias("DEDCT_AMT_ACCUM_ID"),
    F.rpad(F.col("PREAUTH_SVC_SEQ_NO"), 4, " ").alias("PREAUTH_SVC_SEQ_NO"),
    F.rpad(F.col("RFRL_SVC_SEQ_NO"), 4, " ").alias("RFRL_SVC_SEQ_NO"),
    F.rpad(F.col("LMT_PFX_ID"), 4, " ").alias("LMT_PFX_ID"),
    F.rpad(F.col("PREAUTH_ID"), 9, " ").alias("PREAUTH_ID"),
    F.rpad(F.col("PROD_CMPNT_DEDCT_PFX_ID"), 4, " ").alias("PROD_CMPNT_DEDCT_PFX_ID"),
    F.rpad(F.col("PROD_CMPNT_SVC_PAYMT_ID"), 4, " ").alias("PROD_CMPNT_SVC_PAYMT_ID"),
    F.rpad(F.col("RFRL_ID_TX"), 9, " ").alias("RFRL_ID_TX"),
    F.rpad(F.col("SVC_ID"), 4, " ").alias("SVC_ID"),
    F.rpad(F.col("SVC_PRICE_RULE_ID"), 4, " ").alias("SVC_PRICE_RULE_ID"),
    F.rpad(F.col("SVC_RULE_TYP_TX"), 3, " ").alias("SVC_RULE_TYP_TX"),
    F.rpad(F.col("SVC_LOC_TYP_CD"), 20, " ").alias("SVC_LOC_TYP_CD"),
    F.col("NON_PAR_SAV_AMT").alias("NON_PAR_SAV_AMT"),
    F.col("PROC_CD_TYP_CD").alias("PROC_CD_TYP_CD"),
    F.col("PROC_CD_CAT_CD").alias("PROC_CD_CAT_CD"),
    F.col("VBB_RULE_ID").alias("VBB_RULE_ID"),
    F.col("VBB_EXCD_ID").alias("VBB_EXCD_ID"),
    F.rpad(F.col("CLM_LN_VBB_IN"), 1, " ").alias("CLM_LN_VBB_IN"),
    F.col("ITS_SUPLMT_DSCNT_AMT").alias("ITS_SUPLMT_DSCNT_AMT"),
    F.col("ITS_SRCHRG_AMT").alias("ITS_SRCHRG_AMT"),
    F.col("NDC").alias("NDC"),
    F.col("NDC_DRUG_FORM_CD").alias("NDC_DRUG_FORM_CD"),
    F.col("NDC_UNIT_CT").alias("NDC_UNIT_CT"),
    F.rpad(F.col("MED_PDX_IND"), 3, " ").alias("MED_PDX_IND"),
    F.col("APC_ID").alias("APC_ID"),
    F.col("APC_STTUS_ID").alias("APC_STTUS_ID"),
    F.col("SNOMED_CT_CD").alias("SNOMED_CT_CD"),
    F.col("CVX_VCCN_CD").alias("CVX_VCCN_CD")
)

write_files(
    df_final_bcafep,
    f"{adls_path}/key/BCAFEPMedClmLnExtr.BCAFEPMedClmLn.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

# ----------------------------------------------------------------------------
# Stage: Transformer (CTransformerStage) => "Snapshot" => "B_CLM_LN"
# ----------------------------------------------------------------------------
# We have df_snapshot2 => input. We apply stage variables:
#   ProcCdSk = GetFkeyProcCd("FACETS", 0, Snapshot.PROC_CD[1, 5], ...)
#   ClmLnRvnuCdSk = GetFkeyRvnu("FACETS", 0, Snapshot.CLM_LN_RVNU_CD, 'N')
# These are user-defined functions. We'll just call them directly.

df_enriched = df_snapshot2.withColumn(
    "PROC_CD_SK",
    GetFkeyProcCd("FACETS", F.lit(0), F.substring(F.col("PROC_CD"),1,5), F.col("PROC_CD_TYP_CD"), F.col("PROC_CD_CAT_CD"), F.lit("N"))
).withColumn(
    "CLM_LN_RVNU_CD_SK",
    GetFkeyRvnu("FACETS", F.lit(0), F.col("CLM_LN_RVNU_CD"), F.lit("N"))
)

# Output columns => link "RowCount" => "B_CLM_LN"
df_b_clm_ln = df_enriched.select(
    F.col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
    F.col("CLCL_ID").alias("CLM_ID"),
    F.col("CLM_LN_SEQ_NO"),
    F.col("PROC_CD_SK"),
    F.col("CLM_LN_RVNU_CD_SK"),
    F.col("ALW_AMT"),
    F.col("CHRG_AMT"),
    F.col("PAYBL_AMT")
)

# ----------------------------------------------------------------------------
# Stage: B_CLM_LN (CSeqFileStage) => final output
# ----------------------------------------------------------------------------
# The file path => #$FilePath#/load/B_CLM_LN.#SrcSysCd#.dat.#RunID#
# => f"{adls_path}/load/B_CLM_LN.{SrcSysCd}.dat.{RunID}"

# Apply rpad for char columns if needed:
# CLM_ID is char(12)? We do rpad
df_b_clm_ln_final = df_b_clm_ln.select(
    F.col("SRC_SYS_CD_SK"),
    F.rpad(F.col("CLM_ID"), 12, " ").alias("CLM_ID"),
    F.col("CLM_LN_SEQ_NO"),
    F.col("PROC_CD_SK"),
    F.col("CLM_LN_RVNU_CD_SK"),
    F.col("ALW_AMT"),
    F.col("CHRG_AMT"),
    F.col("PAYBL_AMT")
)

write_files(
    df_b_clm_ln_final,
    f"{adls_path}/load/B_CLM_LN.{SrcSysCd}.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)