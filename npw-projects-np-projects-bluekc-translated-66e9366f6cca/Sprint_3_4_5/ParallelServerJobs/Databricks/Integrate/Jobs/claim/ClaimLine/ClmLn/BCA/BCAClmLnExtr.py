# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC CALLED BY:   BCADrugExtrSeq
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC     Reads the BCADrugClm_Land.dat created in BCADrugClmPreProcExtr job and runs through primary key using shared container ClmLnPK
# MAGIC   
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer             Date                 Project/Altiris #              Change Description                                                                                Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------           --------------------     ------------------------           -----------------------------------------------------------------------                                           --------------------------------       -------------------------------   ----------------------------       
# MAGIC Kalyan Neelam     2013-10-29      5056 FEP Claims             Initial Programming                                                                                  IntegrateNewDevl          Bhoomi Dasari           11/30/2013
# MAGIC 
# MAGIC Manasa Andru      2014-10-17                                       Added 2 new fields(ITS_SUPLMT_DSCNT_AMT and                                 IntegrateCurDevl             Kalyan Neelam           2014-10-22
# MAGIC                                                                                                       ITS_SRCHRG_AMT) at the end.
# MAGIC Abhiram Dasarathy	2016-11-07    5568 - HEDIS	Changed the business rules based on the mapping			           IntegrateDev2	   Kalyan Neelam           2016-11-15	
# MAGIC 						changes
# MAGIC Abhiram Dasarathy	2017-02-27   5568 - HEDIS	       Changed the transformation rule for 				           IntegrateDev2	   Kalyan Neelam           2017-02-28	
# MAGIC 					       DISPNS_PROV_ID
# MAGIC 
# MAGIC Hari Pinnaka          2017-08-07      5792                      Added 3 new fields (NDC_SK ,NDC_DRUG_FORM_CD_SK,                        IntegrateDev1                Kalyan Neelam           2017-09-05
# MAGIC                                                                                     NDC_UNIT_CT) at the end
# MAGIC Jaideep Mankala   2017-11-20      5828 		    Added new field to identify MED / PDX claim when passing to Fkey job         IntegrateDev2                Kalyan Neelam          2017-11-20
# MAGIC 
# MAGIC Madhavan B          2018-02-06        5792 	     Changed the datatype of the column                                                              IntegrateDev1                Kalyan Neelam          2018-02-08
# MAGIC                                                       		     NDC_UNIT_CT from SMALLINT to DECIMAL (21,10)
# MAGIC 
# MAGIC Sudhir Bomshetty   2018-03-16  5781 HEDIS           Updated logic for SVC_PROV_ID                                                                     IntegrateDev2                Kalyan Neelam          2018-03-20    
# MAGIC 
# MAGIC Sudhir Bomshetty   2018-03-29  5781 HEDIS           Added last 4 columns in 'BCADrugClm_Land' stage                                           IntegrateDev2                Jaideep Mankala      04/04/2018
# MAGIC 
# MAGIC 
# MAGIC Karthik Chintalapani         2019-08-27        5884       Added new columns to BCAClmLand  file stage                                                IntegrateDev1                Kalyan Neelam          2019-09-05	                
# MAGIC                                                                                  CLM_STTUS_CD,ADJ_FROM_CLM_ID,ADJ_TO_CLM_ID    
# MAGIC                                                                                  for implementing reversals logic
# MAGIC                                                                                                    
# MAGIC Sagar S                         2020-08-28        6264-PBM Phase II     Added APC_ID, APC_STTUS_ID                                                    IntegrateDev5               Kalyan Neelam          2020-12-10
# MAGIC                                                                (Government Programs) 
# MAGIC Amritha A J                  2023-07-31         US 589700       Added two new fields SNOMED_CT_CD,CVX_VCCN_CD with a default   IntegrateDevB	Harsha Ravuri	2023-08-31
# MAGIC                                                                                          value in BusinessRules stage and mapped it till target

# MAGIC Assign primary surrogate key
# MAGIC Writing Sequential File to /key
# MAGIC This container is used in:
# MAGIC ArgusClmLnExtr
# MAGIC ESIClmLnExtr
# MAGIC PCSClmLnExtr
# MAGIC PseudoClmLnPkey
# MAGIC FctsClmLnDntlTrns
# MAGIC FctsClmLnExtr
# MAGIC NascoClmLnExtr
# MAGIC WellDyneClmLnExtr
# MAGIC MCSourceClmLnExtr
# MAGIC 
# MAGIC These programs need to be re-compiled when logic changes
# MAGIC The file is created in the BCADrugClmPreProcExtr job
# MAGIC BCA Claim Line Extract
# MAGIC Added new field MED_PDX_IND to the common layout file that is sent to IdsClmLnFkey job
# MAGIC which indicates whether the claim is Medical or Pharmacy
# MAGIC Indicator used :
# MAGIC MED - medical
# MAGIC PDX - Pharmacy
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../../shared_containers/PrimaryKey/ClmLnPK

# ----------------------------------------------------------------
# Retrieve Parameter Values
# ----------------------------------------------------------------
IDSOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")
RunID = get_widget_value("RunID","")
CurrentDate = get_widget_value("CurrentDate","")
SrcSysCd = get_widget_value("SrcSysCd","")
RunCycle = get_widget_value("RunCycle","")
SrcSysCdSk = get_widget_value("SrcSysCdSk","")

# ----------------------------------------------------------------
# Read Stage: BCADrugClm_Land (CSeqFileStage)
# ----------------------------------------------------------------
schema_BCADrugClm_Land = StructType([
    StructField("CLM_ID", StringType(), True),
    StructField("CLM_STTUS_CD", StringType(), True),
    StructField("ADJ_FROM_CLM_ID", StringType(), True),
    StructField("ADJ_TO_CLM_ID", StringType(), True),
    StructField("MBR_UNIQ_KEY", IntegerType(), True),
    StructField("ALLOCD_CRS_PLN_CD", StringType(), True),
    StructField("ALLOCD_SHIELD_PLN_CD", StringType(), True),
    StructField("PDX_SVC_ID", DecimalType(38,10), True),
    StructField("CLM_LN_NO", DecimalType(38,10), True),
    StructField("RX_FILLED_DT", StringType(), True),
    StructField("CONSIS_MBR_ID", DecimalType(38,10), True),
    StructField("FEP_CNTR_ID", StringType(), True),
    StructField("LGCY_MBR_ID", StringType(), True),
    StructField("DOB", StringType(), True),
    StructField("PATN_AGE", DecimalType(38,10), True),
    StructField("GNDR_CD", StringType(), True),
    StructField("PDX_NTNL_PROV_ID", StringType(), True),
    StructField("LGCY_SRC_CD", StringType(), True),
    StructField("PRSCRB_PROV_NTNL_PROV_ID", StringType(), True),
    StructField("PRSCRB_NTWK_CD", StringType(), True),
    StructField("PRSCRB_PROV_PLN_CD", StringType(), True),
    StructField("NDC", StringType(), True),
    StructField("BRND_NM", StringType(), True),
    StructField("LABEL_NM", StringType(), True),
    StructField("THRPTC_CAT_DESC", StringType(), True),
    StructField("GNRC_NM_DRUG_IN", StringType(), True),
    StructField("METH_DRUG_ADM", StringType(), True),
    StructField("RX_CST_EQVLNT", DecimalType(38,10), True),
    StructField("METRIC_UNIT", DecimalType(38,10), True),
    StructField("NON_METRIC_UNIT", DecimalType(38,10), True),
    StructField("DAYS_SUPPLIED", DecimalType(38,10), True),
    StructField("CLM_PD_DT", StringType(), True),
    StructField("CLM_LOAD_DT", StringType(), True),
    StructField("SUB_UNIQ_KEY", IntegerType(), True),
    StructField("GRP_ID", StringType(), True),
    StructField("SUB_ID", StringType(), True),
    StructField("MBR_SFX_NO", StringType(), True),
    StructField("DISPNS_DRUG_TYP", StringType(), True),
    StructField("DISPNS_AS_WRTN_STTUS_CD", StringType(), True),
    StructField("ADJ_CD", StringType(), True),
    StructField("PD_DT", StringType(), True)
])

file_path_BCADrugClm_Land = f"{adls_path}/verified/BCADrugClm_Land.dat.{RunID}"
df_BCADrugClm_Land = (
    spark.read
    .option("header", False)
    .schema(schema_BCADrugClm_Land)
    .option("delimiter", ",")
    .option("quote", "\"")
    .csv(file_path_BCADrugClm_Land)
)

# ----------------------------------------------------------------
# Transformer_34 (CTransformerStage)
# ----------------------------------------------------------------
df_Transformer_34 = df_BCADrugClm_Land.withColumn(
    "svDispnsProvId",
    F.instr(F.col("PDX_NTNL_PROV_ID"), "NBP")
)

df_BcaData = df_Transformer_34.select(
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_STTUS_CD").alias("CLM_STTUS_CD"),
    F.col("ADJ_FROM_CLM_ID").alias("ADJ_FROM_CLM_ID"),
    F.col("ADJ_TO_CLM_ID").alias("ADJ_TO_CLM_ID"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("ALLOCD_CRS_PLN_CD").alias("ALLOCD_CRS_PLN_CD"),
    F.col("ALLOCD_SHIELD_PLN_CD").alias("ALLOCD_SHIELD_PLN_CD"),
    F.col("PDX_SVC_ID").alias("PDX_SVC_ID"),
    F.col("CLM_LN_NO").alias("CLM_LN_NO"),
    F.col("RX_FILLED_DT").alias("RX_FILLED_DT"),
    F.col("CONSIS_MBR_ID").alias("CONSIS_MBR_ID"),
    F.col("FEP_CNTR_ID").alias("FEP_CNTR_ID"),
    F.col("LGCY_MBR_ID").alias("LGCY_MBR_ID"),
    F.col("DOB").alias("DOB"),
    F.col("PATN_AGE").alias("PATN_AGE"),
    F.col("GNDR_CD").alias("GNDR_CD"),
    F.col("PDX_NTNL_PROV_ID").alias("PDX_NTNL_PROV_ID"),
    F.col("LGCY_SRC_CD").alias("LGCY_SRC_CD"),
    F.col("PRSCRB_PROV_NTNL_PROV_ID").alias("PRSCRB_PROV_ID"),
    F.col("PRSCRB_NTWK_CD").alias("PRSCRB_NTWK_CD"),
    F.col("PRSCRB_PROV_PLN_CD").alias("PRSCRB_PROV_PLN_CD"),
    F.col("NDC").alias("NDC"),
    F.col("BRND_NM").alias("BRND_NM"),
    F.col("LABEL_NM").alias("LABEL_NM"),
    F.col("THRPTC_CAT_DESC").alias("THRPTC_CAT_DESC"),
    F.col("GNRC_NM_DRUG_IN").alias("GNRC_NM_DRUG_IN"),
    F.col("METH_DRUG_ADM").alias("METH_DRUG_ADM"),
    F.col("RX_CST_EQVLNT").alias("RX_CST_EQVLNT"),
    F.col("METRIC_UNIT").alias("METRIC_UNIT"),
    F.col("NON_METRIC_UNIT").alias("NON_METRIC_UNIT"),
    F.col("DAYS_SUPPLIED").alias("DAYS_SUPPLIED"),
    F.col("CLM_PD_DT").alias("CLM_PD_DT"),
    F.col("CLM_LOAD_DT").alias("CLM_LOAD_DT"),
    F.col("SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    F.col("GRP_ID").alias("GRP_ID"),
    F.col("SUB_ID").alias("SUB_ID"),
    F.col("MBR_SFX_NO").alias("MBR_SFX_NO")
)

# ----------------------------------------------------------------
# IDS_PROV (DB2Connector) -> df_IDS_PROV
# ----------------------------------------------------------------
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = (
    f"SELECT PROV.NTNL_PROV_ID, MPPNG.SRC_CD, MIN(PROV_ID) as PROV_ID "
    f"FROM {IDSOwner}.PROV PROV, {IDSOwner}.CD_MPPNG MPPNG "
    f"WHERE PROV.SRC_SYS_CD_SK = MPPNG.CD_MPPNG_SK "
    f"GROUP BY PROV.NTNL_PROV_ID, MPPNG.SRC_CD "
    f"ORDER BY PROV.NTNL_PROV_ID"
)

df_IDS_PROV = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# ----------------------------------------------------------------
# hf_bca_clm_ln_prov (CHashedFileStage) - Scenario A Dedup
# ----------------------------------------------------------------
df_hf_bca_clm_ln_prov_temp = dedup_sort(df_IDS_PROV, ["NTNL_PROV_ID", "SRC_CD"], [])
df_hf_bca_clm_ln_prov = df_hf_bca_clm_ln_prov_temp

# ----------------------------------------------------------------
# BusinessRules (CTransformerStage)
# ----------------------------------------------------------------
df_BusinessRules = (
    df_BcaData.alias("BcaData")
    .join(
        df_hf_bca_clm_ln_prov.alias("ntnlprovid_nabp"),
        (
            (F.col("BcaData.PDX_NTNL_PROV_ID") == F.col("ntnlprovid_nabp.NTNL_PROV_ID"))
            & (F.lit("NABP") == F.col("ntnlprovid_nabp.SRC_CD"))
        ),
        "left"
    )
    .join(
        df_hf_bca_clm_ln_prov.alias("ntnlprovid_fcets"),
        (
            (F.col("BcaData.PDX_NTNL_PROV_ID") == F.col("ntnlprovid_fcets.NTNL_PROV_ID"))
            & (F.lit("FACETS") == F.col("ntnlprovid_fcets.SRC_CD"))
        ),
        "left"
    )
    .join(
        df_hf_bca_clm_ln_prov.alias("ntnlprovid_bca"),
        (
            (F.col("BcaData.PDX_NTNL_PROV_ID") == F.col("ntnlprovid_bca.NTNL_PROV_ID"))
            & (F.lit("BCA") == F.col("ntnlprovid_bca.SRC_CD"))
        ),
        "left"
    )
)

df_BusinessRules = df_BusinessRules.withColumn(
    "svSvcProvID",
    F.when(
        (F.col("BcaData.PDX_NTNL_PROV_ID").isNotNull())
        & (F.length("BcaData.PDX_NTNL_PROV_ID") == 10),
        F.when(F.col("ntnlprovid_nabp.PROV_ID").isNotNull(), F.col("ntnlprovid_nabp.PROV_ID"))
        .otherwise(
            F.when(F.col("ntnlprovid_fcets.PROV_ID").isNotNull(), F.col("ntnlprovid_fcets.PROV_ID"))
            .otherwise(
                F.when(F.col("ntnlprovid_bca.PROV_ID").isNotNull(), F.col("ntnlprovid_bca.PROV_ID"))
                .otherwise('UNK')
            )
        )
    ).otherwise('NA')
)

df_Transform = df_BusinessRules.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    current_date().alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.col("SrcSysCd").alias("SRC_SYS_CD"),
    F.expr("SrcSysCd || ';' || BcaData.CLM_ID || ';' || '1'").alias("PRI_KEY_STRING"),
    F.lit(0).alias("CLM_LN_SK"),
    F.col("BcaData.CLM_ID").alias("CLM_ID"),
    F.lit(1).alias("CLM_LN_SEQ_NO"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit("NA").alias("PROC_CD"),
    F.col("svSvcProvID").alias("SVC_PROV_ID"),
    F.lit("NA").alias("CLM_LN_DSALW_EXCD"),
    F.lit("NA").alias("CLM_LN_EOB_EXCD"),
    F.lit("ACPTD").alias("CLM_LN_FINL_DISP_CD"),
    F.lit("NA").alias("CLM_LN_LOB_CD"),
    F.lit("NA").alias("CLM_LN_POS_CD"),
    F.lit("NA").alias("CLM_LN_PREAUTH_CD"),
    F.lit("NA").alias("CLM_LN_PREAUTH_SRC_CD"),
    F.lit("NA").alias("CLM_LN_PRICE_SRC_CD"),
    F.lit("NA").alias("CLM_LN_RFRL_CD"),
    F.lit("NA").alias("CLM_LN_RVNU_CD"),
    F.lit("NA").alias("CLM_LN_ROOM_PRICE_METH_CD"),
    F.lit("NA").alias("CLM_LN_ROOM_TYP_CD"),
    F.lit("NA").alias("CLM_LN_TOS_CD"),
    F.lit("NA").alias("CLM_LN_UNIT_TYP_CD"),
    F.lit("N").alias("CAP_LN_IN"),
    F.lit("N").alias("PRI_LOB_IN"),
    F.col("BcaData.RX_FILLED_DT").alias("SVC_END_DT"),
    F.col("BcaData.RX_FILLED_DT").alias("SVC_STRT_DT"),
    F.lit(0.00).alias("AGMNT_PRICE_AMT"),
    F.when(
        (F.col("BcaData.RX_CST_EQVLNT").isNull())
        | (F.length(trim("BcaData.RX_CST_EQVLNT")) == 0),
        F.lit(0.00)
    ).otherwise(F.col("BcaData.RX_CST_EQVLNT")).alias("ALW_AMT"),
    F.when(
        (F.col("BcaData.RX_CST_EQVLNT").isNull())
        | (F.length(trim("BcaData.RX_CST_EQVLNT")) == 0),
        F.lit(0.00)
    ).otherwise(F.col("BcaData.RX_CST_EQVLNT")).alias("CHRG_AMT"),
    F.lit(0.00).alias("COINS_AMT"),
    F.when(
        (F.col("BcaData.RX_CST_EQVLNT").isNull())
        | (F.length(trim("BcaData.RX_CST_EQVLNT")) == 0),
        F.lit(0.00)
    ).otherwise(F.col("BcaData.RX_CST_EQVLNT")).alias("CNSD_CHRG_AMT"),
    F.lit(0.00).alias("COPAY_AMT"),
    F.lit(0.00).alias("DEDCT_AMT"),
    F.lit(0.00).alias("DSALW_AMT"),
    F.lit(0.00).alias("ITS_HOME_DSCNT_AMT"),
    F.lit(0.00).alias("NO_RESP_AMT"),
    F.lit(0.00).alias("MBR_LIAB_BSS_AMT"),
    F.lit(0.00).alias("PATN_RESP_AMT"),
    F.lit(0.00).alias("PAYBL_AMT"),
    F.lit(0.00).alias("PAYBL_TO_PROV_AMT"),
    F.lit(0.00).alias("PAYBL_TO_SUB_AMT"),
    F.lit(0.00).alias("PROC_TBL_PRICE_AMT"),
    F.lit(0.00).alias("PROFL_PRICE_AMT"),
    F.lit(0.00).alias("PROV_WRT_OFF_AMT"),
    F.lit(0.00).alias("RISK_WTHLD_AMT"),
    F.lit(0.00).alias("SVC_PRICE_AMT"),
    F.lit(0.00).alias("SUPLMT_DSCNT_AMT"),
    F.lit(0).alias("ALW_PRICE_UNIT_CT"),
    F.lit(0).alias("UNIT_CT"),
    F.lit("NA").alias("DEDCT_AMT_ACCUM_ID"),
    F.lit("NA").alias("PREAUTH_SVC_SEQ_NO"),
    F.lit("NA").alias("RFRL_SVC_SEQ_NO"),
    F.lit("NA").alias("LMT_PFX_ID"),
    F.lit("NA").alias("PREAUTH_ID"),
    F.lit("NA").alias("PROD_CMPNT_DEDCT_PFX_ID"),
    F.lit("NA").alias("PROD_CMPNT_SVC_PAYMT_ID"),
    F.lit("NA").alias("RFRL_ID_TX"),
    F.lit("NA").alias("SVC_ID"),
    F.lit("NA").alias("SVC_PRICE_RULE_ID"),
    F.lit("NA").alias("SVC_RULE_TYP_TX"),
    F.lit("NA").alias("SVC_LOC_TYP_CD"),
    F.lit(0.00).alias("NON_PAR_SAV_AMT"),
    F.lit("NA").alias("PROC_CD_TYP_CD"),
    F.lit("NA").alias("PROC_CD_CAT_CD"),
    F.lit("NA").alias("SNOMED_CT_CD"),
    F.lit("NA").alias("CVX_VCCN_CD")
)

df_Snapshot_PKey = df_Transform.select(
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
    F.col("PROC_CD_TYP_CD").alias("PROC_CD_TYP_CD"),
    F.col("PROC_CD_CAT_CD").alias("PROC_CD_CAT_CD"),
    F.lit("NA").alias("VBB_RULE_ID"),
    F.lit("NA").alias("VBB_EXCD_ID"),
    F.lit("N").alias("CLM_LN_VBB_IN"),
    F.lit(0.00).alias("ITS_SUPLMT_DSCNT_AMT"),
    F.lit(0.00).alias("ITS_SRCHRG_AMT"),
    F.lit("NA").alias("NDC"),
    F.lit("NA").alias("NDC_DRUG_FORM_CD"),
    F.lit(None).alias("NDC_UNIT_CT"),
    F.lit("PDX").alias("MED_PDX_IND"),
    F.lit(None).alias("APC_ID"),
    F.lit(None).alias("APC_STTUS_ID"),
    F.col("SNOMED_CT_CD").alias("SNOMED_CT_CD"),
    F.col("CVX_VCCN_CD").alias("CVX_VCCN_CD")
)

df_Snapshot_Snapshot = df_Transform.select(
    F.col("CLM_ID").alias("CLCL_ID"),
    F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("PROC_CD").alias("PROC_CD"),
    F.col("CLM_LN_RVNU_CD").alias("CLM_LN_RVNU_CD"),
    F.col("ALW_AMT").alias("ALW_AMT"),
    F.col("CHRG_AMT").alias("CHRG_AMT"),
    F.col("PAYBL_AMT").alias("PAYBL_AMT"),
    F.col("PROC_CD_TYP_CD").alias("PROC_CD_TYP_CD"),
    F.col("PROC_CD_CAT_CD").alias("PROC_CD_CAT_CD")
)

# ----------------------------------------------------------------
# ClmLnPK (CContainerStage)
# ----------------------------------------------------------------
params_ClmLnPK = {
    "CurrRunCycle": RunCycle
}
df_key = ClmLnPK(df_Snapshot_PKey, params_ClmLnPK)

# ----------------------------------------------------------------
# BCAClmLnExtr (CSeqFileStage)
# ----------------------------------------------------------------
# Final select preserving order and applying rpad for char/varchar with known length
df_BCAClmLnExtr = df_key.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("INSRT_UPDT_CD"),10," ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("DISCARD_IN"),1," ").alias("DISCARD_IN"),
    F.rpad(F.col("PASS_THRU_IN"),1," ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("CLM_LN_SK"),
    F.col("CLM_ID"),
    F.col("CLM_LN_SEQ_NO"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("PROC_CD"),
    F.col("SVC_PROV_ID"),
    F.col("CLM_LN_DSALW_EXCD"),
    F.col("CLM_LN_EOB_EXCD"),
    F.col("CLM_LN_FINL_DISP_CD"),
    F.col("CLM_LN_LOB_CD"),
    F.col("CLM_LN_POS_CD"),
    F.col("CLM_LN_PREAUTH_CD"),
    F.col("CLM_LN_PREAUTH_SRC_CD"),
    F.col("CLM_LN_PRICE_SRC_CD"),
    F.col("CLM_LN_RFRL_CD"),
    F.col("CLM_LN_RVNU_CD"),
    F.rpad(F.col("CLM_LN_ROOM_PRICE_METH_CD"),2," ").alias("CLM_LN_ROOM_PRICE_METH_CD"),
    F.col("CLM_LN_ROOM_TYP_CD"),
    F.col("CLM_LN_TOS_CD"),
    F.col("CLM_LN_UNIT_TYP_CD"),
    F.rpad(F.col("CAP_LN_IN"),1," ").alias("CAP_LN_IN"),
    F.rpad(F.col("PRI_LOB_IN"),1," ").alias("PRI_LOB_IN"),
    F.col("SVC_END_DT"),
    F.col("SVC_STRT_DT"),
    F.col("AGMNT_PRICE_AMT"),
    F.col("ALW_AMT"),
    F.col("CHRG_AMT"),
    F.col("COINS_AMT"),
    F.col("CNSD_CHRG_AMT"),
    F.col("COPAY_AMT"),
    F.col("DEDCT_AMT"),
    F.col("DSALW_AMT"),
    F.col("ITS_HOME_DSCNT_AMT"),
    F.col("NO_RESP_AMT"),
    F.col("MBR_LIAB_BSS_AMT"),
    F.col("PATN_RESP_AMT"),
    F.col("PAYBL_AMT"),
    F.col("PAYBL_TO_PROV_AMT"),
    F.col("PAYBL_TO_SUB_AMT"),
    F.col("PROC_TBL_PRICE_AMT"),
    F.col("PROFL_PRICE_AMT"),
    F.col("PROV_WRT_OFF_AMT"),
    F.col("RISK_WTHLD_AMT"),
    F.col("SVC_PRICE_AMT"),
    F.col("SUPLMT_DSCNT_AMT"),
    F.col("ALW_PRICE_UNIT_CT"),
    F.col("UNIT_CT"),
    F.rpad(F.col("DEDCT_AMT_ACCUM_ID"),4," ").alias("DEDCT_AMT_ACCUM_ID"),
    F.rpad(F.col("PREAUTH_SVC_SEQ_NO"),4," ").alias("PREAUTH_SVC_SEQ_NO"),
    F.rpad(F.col("RFRL_SVC_SEQ_NO"),4," ").alias("RFRL_SVC_SEQ_NO"),
    F.rpad(F.col("LMT_PFX_ID"),4," ").alias("LMT_PFX_ID"),
    F.rpad(F.col("PREAUTH_ID"),9," ").alias("PREAUTH_ID"),
    F.rpad(F.col("PROD_CMPNT_DEDCT_PFX_ID"),4," ").alias("PROD_CMPNT_DEDCT_PFX_ID"),
    F.rpad(F.col("PROD_CMPNT_SVC_PAYMT_ID"),4," ").alias("PROD_CMPNT_SVC_PAYMT_ID"),
    F.rpad(F.col("RFRL_ID_TX"),9," ").alias("RFRL_ID_TX"),
    F.rpad(F.col("SVC_ID"),4," ").alias("SVC_ID"),
    F.rpad(F.col("SVC_PRICE_RULE_ID"),4," ").alias("SVC_PRICE_RULE_ID"),
    F.rpad(F.col("SVC_RULE_TYP_TX"),3," ").alias("SVC_RULE_TYP_TX"),
    F.rpad(F.col("SVC_LOC_TYP_CD"),20," ").alias("SVC_LOC_TYP_CD"),
    F.col("NON_PAR_SAV_AMT"),
    F.col("PROC_CD_TYP_CD"),
    F.col("PROC_CD_CAT_CD"),
    F.col("VBB_RULE_ID"),
    F.col("VBB_EXCD_ID"),
    F.rpad(F.col("CLM_LN_VBB_IN"),1," ").alias("CLM_LN_VBB_IN"),
    F.col("ITS_SUPLMT_DSCNT_AMT"),
    F.col("ITS_SRCHRG_AMT"),
    F.col("NDC"),
    F.col("NDC_DRUG_FORM_CD"),
    F.col("NDC_UNIT_CT"),
    F.rpad(F.col("MED_PDX_IND"),3," ").alias("MED_PDX_IND"),
    F.col("APC_ID"),
    F.col("APC_STTUS_ID"),
    F.col("SNOMED_CT_CD"),
    F.col("CVX_VCCN_CD")
)

write_files(
    df_BCAClmLnExtr,
    f"{adls_path}/key/BCAClmLnExtr.DrugClmLn.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# ----------------------------------------------------------------
# Transformer (CTransformerStage) -> from Snapshot to B_CLM_LN
# ----------------------------------------------------------------
df_Transformer = df_Snapshot_Snapshot.withColumn(
    "ProcCdSk",
    GetFkeyProcCd("FACETS", F.lit(0), F.expr("substring(CLCL_ID.PROC_CD,1,5)"), F.col("PROC_CD_TYP_CD"), F.col("PROC_CD_CAT_CD"), F.lit("N"))
).withColumn(
    "ClmLnRvnuCdSk",
    GetFkeyRvnu("FACETS", F.lit(0), F.col("CLM_LN_RVNU_CD"), F.lit("N"))
)

df_RowCount = df_Transformer.select(
    F.col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
    F.col("CLCL_ID").alias("CLM_ID"),
    F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("ProcCdSk").alias("PROC_CD_SK"),
    F.col("ClmLnRvnuCdSk").alias("CLM_LN_RVNU_CD_SK"),
    F.col("ALW_AMT").alias("ALW_AMT"),
    F.col("CHRG_AMT").alias("CHRG_AMT"),
    F.col("PAYBL_AMT").alias("PAYBL_AMT")
)

# ----------------------------------------------------------------
# B_CLM_LN (CSeqFileStage)
# ----------------------------------------------------------------
df_B_CLM_LN = df_RowCount.select(
    F.col("SRC_SYS_CD_SK"),
    F.col("CLM_ID"),
    F.col("CLM_LN_SEQ_NO"),
    F.col("PROC_CD_SK"),
    F.col("CLM_LN_RVNU_CD_SK"),
    F.col("ALW_AMT"),
    F.col("CHRG_AMT"),
    F.col("PAYBL_AMT")
)

write_files(
    df_B_CLM_LN,
    f"{adls_path}/load/B_CLM_LN.{SrcSysCd}.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)