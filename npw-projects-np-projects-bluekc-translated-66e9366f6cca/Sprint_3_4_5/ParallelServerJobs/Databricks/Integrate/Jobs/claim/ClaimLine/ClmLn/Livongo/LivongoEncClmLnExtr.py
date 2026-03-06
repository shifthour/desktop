# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2018 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC CALLED BY: LivongoEncClmLnExtrSeq
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:  Reads the Livongo_Monthly_Billing_Claims.*.dat  file and puts the data into the claim common record format and takes it through primary key process using Shared Container ClmlnPkey
# MAGIC     
# MAGIC     
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                               Date                 Project/Altiris #       Change Description                                                                   Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------                             --------------------     ------------------------       -----------------------------------------------------------------------------------------          --------------------------------       -------------------------------   ----------------------------       
# MAGIC Mrudula Kodali                       2020-03-10        311337                  Initial Programming                                                                         IntegrateDev2                      Manasa Andru            2021-03-17   
# MAGIC 
# MAGIC Revathi Boojireddy                 2023-08-01       US 589700            Added two new fields SNOMED_CT_CD,CVX_VCCN_CD            IntegrateDevB	Harsha Ravuri	2023-08-31
# MAGIC                                                                                                       with a default value in Snapshot  stage and
# MAGIC 	                                                                                      mapped it till target LivongoEncounterClmLnExtr file stage

# MAGIC Read the LivongoEncounter file created
# MAGIC Writing Sequential File to /key
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
# MAGIC BCBSKCCommClmLnExtr
# MAGIC 
# MAGIC These programs need to be re-compiled when logic changes
# MAGIC Apply business logic
# MAGIC Assign primary surrogate key
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


# Retrieve job parameters (including database-secret parameters):
IDSOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")
CurrentDate = get_widget_value("CurrentDate","")
SrcSysCd = get_widget_value("SrcSysCd","")
SrcSysCdSk = get_widget_value("SrcSysCdSk","")
RunID = get_widget_value("RunID","")
RunCycle = get_widget_value("RunCycle","")
InFile_F = get_widget_value("InFile_F","")

# MAGIC %run ../../../../../../shared_containers/PrimaryKey/ClmLnPK
# COMMAND ----------

# --------------------------------------------------------------------------------
# Stage: IDS_NPI (DB2Connector) - Database = IDS
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

# Output pin "Npi"
extract_query_npi = (
    f"SELECT '1' AS FLAG, PROV.PROV_ID "
    f"FROM {IDSOwner}.PROV PROV "
    f"INNER JOIN {IDSOwner}.P_SRC_DOMAIN_TRNSLTN SRC_DOMAIN "
    f"ON SRC_DOMAIN.TRGT_DOMAIN_TX = PROV.NTNL_PROV_ID "
    f"WHERE SRC_DOMAIN.SRC_SYS_CD = 'LVNGHLTH' "
    f"AND SRC_DOMAIN.DOMAIN_ID = 'LIVONGO_NPI' "
    f"AND PROV.SRC_SYS_CD_SK IN (SELECT DISTINCT SRC_CD_SK FROM {IDSOwner}.CD_MPPNG CD WHERE CD.SRC_SYS_CD = 'FACETS')"
)
df_IDS_NPI_npi = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_npi)
    .load()
)

# Output pin "lob"
extract_query_lob = (
    f"SELECT CONCAT(PFX.ALPHA_PFX_CD,SUB.SUB_ID) AS MEMBER_ID,"
    f"       ENR.EFF_DT_SK AS EFF_DT_SK,"
    f"       ENR.TERM_DT_SK AS TERM_DT_SK,"
    f"       PROD.LOB_NO AS LOB_NO "
    f"FROM {IDSOwner}.MBR MBR "
    f"INNER JOIN {IDSOwner}.SUB SUB "
    f"  ON MBR.SUB_SK = SUB.SUB_SK "
    f"INNER JOIN {IDSOwner}.ALPHA_PFX PFX "
    f"  ON SUB.ALPHA_PFX_SK = PFX.ALPHA_PFX_SK "
    f"INNER JOIN {IDSOwner}.MBR_ENR ENR "
    f"  ON MBR.MBR_SK = ENR.MBR_SK "
    f"INNER JOIN {IDSOwner}.PROD PROD "
    f"  ON ENR.PROD_SK = PROD.PROD_SK "
    f"INNER JOIN {IDSOwner}.CD_MPPNG CD "
    f"  ON CD.SRC_CD = PROD.LOB_NO "
    f"WHERE ENR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK = 1949 "
    f"  AND SRC_SYS_CD = 'FACETS' "
    f"  AND SRC_CLCTN_CD = 'FACETS DBO' "
    f"  AND SRC_DOMAIN_NM = 'LOB' "
    f"  AND TRGT_CLCTN_CD = 'IDS' "
    f"  AND TRGT_DOMAIN_NM = 'CLAIM LINE LOB' "
    f"  AND ENR.TERM_DT_SK >= CAST(dbo.GetDateCST() AS DATE)"
)
df_IDS_NPI_lob = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_lob)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: hf_livongo_prov (CHashedFileStage) - Scenario A (intermediate hashed file)
# We deduplicate on key columns for each link separately, then pass along.

# For "natln_prov" output, key is FLAG
df_hf_livongo_prov_natln_prov = df_IDS_NPI_npi.dropDuplicates(["FLAG"])

# For "lnk_lob" output, key is MEMBER_ID
df_hf_livongo_prov_lnk_lob = df_IDS_NPI_lob.dropDuplicates(["MEMBER_ID"])

# --------------------------------------------------------------------------------
# Stage: LivEncClmLnLanding (CSeqFileStage) - reading from:
# directory = "verified", filename = "#InFile_F#"
schema_LivEncClmLnLanding = StructType([
    StructField("livongo_id", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("birth_date", StringType(), True),
    StructField("client_name", StringType(), True),
    StructField("member_id", StringType(), True),
    StructField("claim_code", StringType(), True),
    StructField("service_date", StringType(), True),
    StructField("quantity", StringType(), True)
])

df_LivEncClmLnLanding = (
    spark.read
    .option("quote", "\"")
    .option("sep", ",")
    .option("header", False)
    .schema(schema_LivEncClmLnLanding)
    .csv(f"{adls_path}/verified/{InFile_F}")
)

# --------------------------------------------------------------------------------
# Stage: Sort_liv (sort)
# Sort on SORTSPEC = "livongo_id a"
df_Sort_liv = df_LivEncClmLnLanding.orderBy(F.col("livongo_id").asc())

# --------------------------------------------------------------------------------
# Stage: BusinessRules (CTransformerStage)
# Primary link: "Extract" => df_Sort_liv
# Lookup link: "lnk_lob" => df_hf_livongo_prov_lnk_lob (left join on Extract.member_id = lnk_lob.MEMBER_ID)

df_BusinessRules_join = df_Sort_liv.alias("Extract").join(
    df_hf_livongo_prov_lnk_lob.alias("lnk_lob"),
    F.col("Extract.member_id") == F.col("lnk_lob.MEMBER_ID"),
    how="left"
)

# Emulate stage variables:
#  - SvlvgID = Extract.livongo_id
#  - SvOldClaimID increments for consecutive rows with same livongo_id. We handle via window partition.
df_BusinessRules_var = (
    df_BusinessRules_join
    .withColumn("SvlvgID", F.col("Extract.livongo_id"))
    .withColumn(
        "Svcdt",
        F.when(
            F.col("Extract.service_date").isNull() | (F.length(F.col("Extract.service_date")) == 0),
            F.lit("1753-01-01")
        ).otherwise(
            # Assume user-defined function FormatDate(...) is available in Python
            FormatDate(F.col("Extract.service_date"), 'ACCESS','TIMESTAMP','CCYY-MM-DD')
        )
    )
    .withColumn(
        "Svcendt",
        F.when(
            F.col("Extract.service_date").isNull() | (F.length(F.col("Extract.service_date")) == 0),
            F.lit("2199-12-31")
        ).otherwise(
            FormatDate(F.col("Extract.service_date"), 'ACCESS','TIMESTAMP','CCYY-MM-DD')
        )
    )
)

# Window for incremental claim line numbering within each livongo_id
w_claim = Window.partitionBy(F.trim(F.col("Extract.livongo_id"))).orderBy(F.col("Extract.livongo_id"))
df_BusinessRules = df_BusinessRules_var.withColumn(
    "SvOldClaimID",
    F.row_number().over(w_claim)
)

# Now produce the output link "Transform" (V0S20P23) columns via select in correct order.
df_BusinessRules_out = df_BusinessRules.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    # INSRT_UPDT_CD: char(10) => "I"
    F.rpad(F.lit("I"), 10, " ").alias("INSRT_UPDT_CD"),
    # DISCARD_IN: char(1) => "N"
    F.rpad(F.lit("N"), 1, " ").alias("DISCARD_IN"),
    # PASS_THRU_IN: char(1) => "Y"
    F.rpad(F.lit("Y"), 1, " ").alias("PASS_THRU_IN"),
    # FIRST_RECYC_DT => expression: CurrentDate param
    F.lit(CurrentDate).alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit(SrcSysCd).alias("SRC_SYS_CD"),
    # PRI_KEY_STRING => SrcSysCd : ";" : Extract.livongo_id : ";" : SvOldClaimID
    F.concat_ws(";", F.lit(SrcSysCd), F.col("Extract.livongo_id"), F.col("SvOldClaimID")).alias("PRI_KEY_STRING"),
    F.lit(0).alias("CLM_LN_SK"),
    F.col("Extract.livongo_id").alias("CLM_ID"),
    F.col("SvOldClaimID").alias("CLM_LN_SEQ_NO"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("Extract.claim_code").alias("PROC_CD"),
    # SVC_PROV_ID => "'1'"
    F.lit("1").alias("SVC_PROV_ID"),
    F.lit("NA").alias("CLM_LN_DSALW_EXCD"),
    F.lit("XFE").alias("CLM_LN_EOB_EXCD"),
    F.lit("ACPTD").alias("CLM_LN_FINL_DISP_CD"),
    # CLM_LN_LOB_CD => If Extract.service_date >= lnk_lob.EFF_DT_SK and Extract.service_date <= lnk_lob.TERM_DT_SK Then lnk_lob.LOB_NO Else 'UNK'
    F.when(
        (F.col("Extract.service_date") >= F.col("lnk_lob.EFF_DT_SK")) &
        (F.col("Extract.service_date") <= F.col("lnk_lob.TERM_DT_SK")),
        F.col("lnk_lob.LOB_NO")
    ).otherwise(F.lit("UNK")).alias("CLM_LN_LOB_CD"),
    F.lit("12").alias("CLM_LN_POS_CD"),
    F.lit("NA").alias("CLM_LN_PREAUTH_CD"),
    F.lit("NA").alias("CLM_LN_PREAUTH_SRC_CD"),
    F.lit("NA").alias("CLM_LN_PRICE_SRC_CD"),
    F.lit("NA").alias("CLM_LN_RFRL_CD"),
    F.lit("NA").alias("CLM_LN_RVNU_CD"),
    # CLM_LN_ROOM_PRICE_METH_CD => char(2) => "NA"
    F.rpad(F.lit("NA"), 2, " ").alias("CLM_LN_ROOM_PRICE_METH_CD"),
    F.lit("NA").alias("CLM_LN_ROOM_TYP_CD"),
    F.lit("NA").alias("CLM_LN_TOS_CD"),
    F.lit("UN").alias("CLM_LN_UNIT_TYP_CD"),
    # CAP_LN_IN => char(1) => "N"
    F.rpad(F.lit("N"), 1, " ").alias("CAP_LN_IN"),
    # PRI_LOB_IN => char(1) => "N"
    F.rpad(F.lit("N"), 1, " ").alias("PRI_LOB_IN"),
    # SVC_END_DT => Extract.service_date
    F.col("Extract.service_date").alias("SVC_END_DT"),
    # SVC_STRT_DT => Extract.service_date
    F.col("Extract.service_date").alias("SVC_STRT_DT"),
    F.lit("0.00").alias("AGMNT_PRICE_AMT"),
    F.lit("0.00").alias("ALW_AMT"),
    F.lit("0.00").alias("CHRG_AMT"),
    F.lit("0.00").alias("COINS_AMT"),
    F.lit("0.00").alias("CNSD_CHRG_AMT"),
    F.lit("0.00").alias("COPAY_AMT"),
    F.lit("0.00").alias("DEDCT_AMT"),
    F.lit("0.00").alias("DSALW_AMT"),
    F.lit("0.00").alias("ITS_HOME_DSCNT_AMT"),
    F.lit("0.00").alias("NO_RESP_AMT"),
    F.lit("0.00").alias("MBR_LIAB_BSS_AMT"),
    F.lit("0.00").alias("PATN_RESP_AMT"),
    F.lit("0.00").alias("PAYBL_AMT"),
    F.lit("0.00").alias("PAYBL_TO_PROV_AMT"),
    F.lit("0.00").alias("PAYBL_TO_SUB_AMT"),
    F.lit("0.00").alias("PROC_TBL_PRICE_AMT"),
    F.lit("0.00").alias("PROFL_PRICE_AMT"),
    F.lit("0.00").alias("PROV_WRT_OFF_AMT"),
    F.lit("0.00").alias("RISK_WTHLD_AMT"),
    F.lit("0.00").alias("SVC_PRICE_AMT"),
    F.lit("0.00").alias("SUPLMT_DSCNT_AMT"),
    F.lit(0).alias("ALW_PRICE_UNIT_CT"),
    F.col("Extract.quantity").alias("UNIT_CT"),
    # DEDCT_AMT_ACCUM_ID => char(4) => "NA"
    F.rpad(F.lit("NA"), 4, " ").alias("DEDCT_AMT_ACCUM_ID"),
    # PREAUTH_SVC_SEQ_NO => char(4) => "NA"
    F.rpad(F.lit("NA"), 4, " ").alias("PREAUTH_SVC_SEQ_NO"),
    # RFRL_SVC_SEQ_NO => char(4) => "NA"
    F.rpad(F.lit("NA"), 4, " ").alias("RFRL_SVC_SEQ_NO"),
    # LMT_PFX_ID => char(4) => "NA"
    F.rpad(F.lit("NA"), 4, " ").alias("LMT_PFX_ID"),
    # PREAUTH_ID => char(9) => "NA"
    F.rpad(F.lit("NA"), 9, " ").alias("PREAUTH_ID"),
    # PROD_CMPNT_DEDCT_PFX_ID => char(4) => "NA"
    F.rpad(F.lit("NA"), 4, " ").alias("PROD_CMPNT_DEDCT_PFX_ID"),
    # PROD_CMPNT_SVC_PAYMT_ID => char(4) => "NA"
    F.rpad(F.lit("NA"), 4, " ").alias("PROD_CMPNT_SVC_PAYMT_ID"),
    # RFRL_ID_TX => char(9) => "NA"
    F.rpad(F.lit("NA"), 9, " ").alias("RFRL_ID_TX"),
    # SVC_ID => char(4) => "NA"
    F.rpad(F.lit("NA"), 4, " ").alias("SVC_ID"),
    # SVC_PRICE_RULE_ID => char(4) => "NA"
    F.rpad(F.lit("NA"), 4, " ").alias("SVC_PRICE_RULE_ID"),
    # SVC_RULE_TYP_TX => char(3) => "NA"
    F.rpad(F.lit("NA"), 3, " ").alias("SVC_RULE_TYP_TX"),
    # CLM_TYPE => char(1) => "M"
    F.rpad(F.lit("M"), 1, " ").alias("CLM_TYPE"),
    # SVC_LOC_TYP_CD => char(20) => "NA"
    F.rpad(F.lit("NA"), 20, " ").alias("SVC_LOC_TYP_CD"),
    F.lit("0.00").alias("NON_PAR_SAV_AMT"),
    F.lit("HCPCS").alias("PROC_CD_TYP_CD"),
    F.lit("MED").alias("PROC_CD_CAT_CD"),
    F.lit("NA").alias("NDC"),
    F.lit("NA").alias("APC_ID"),
    F.lit("NA").alias("APC_STTUS_ID")
)

# --------------------------------------------------------------------------------
# Stage: Snapshot (CTransformerStage)
# Input pins: "Transform" (primary => df_BusinessRules_out), "natln_prov" (lookup => df_hf_livongo_prov_natln_prov)
# join on Transform.SVC_PROV_ID == natln_prov.FLAG (left join)

df_Snapshot_join = df_BusinessRules_out.alias("Transform").join(
    df_hf_livongo_prov_natln_prov.alias("natln_prov"),
    F.col("Transform.SVC_PROV_ID") == F.col("natln_prov.FLAG"),
    how="left"
)

# Output pin "PKey" (C106P2 -> ClmLnPK) => big list of columns
df_Snapshot_out_PKey = df_Snapshot_join.select(
    F.col("Transform.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("Transform.INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("Transform.DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.col("Transform.PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    F.col("Transform.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("Transform.ERR_CT").alias("ERR_CT"),
    F.col("Transform.RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("Transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Transform.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("Transform.CLM_LN_SK").alias("CLM_LN_SK"),
    F.col("Transform.CLM_ID").alias("CLM_ID"),
    F.col("Transform.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("Transform.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("Transform.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("Transform.PROC_CD").alias("PROC_CD"),
    F.col("natln_prov.PROV_ID").alias("SVC_PROV_ID"),
    F.col("Transform.CLM_LN_DSALW_EXCD").alias("CLM_LN_DSALW_EXCD"),
    F.col("Transform.CLM_LN_EOB_EXCD").alias("CLM_LN_EOB_EXCD"),
    F.col("Transform.CLM_LN_FINL_DISP_CD").alias("CLM_LN_FINL_DISP_CD"),
    F.col("Transform.CLM_LN_LOB_CD").alias("CLM_LN_LOB_CD"),
    F.col("Transform.CLM_LN_POS_CD").alias("CLM_LN_POS_CD"),
    F.col("Transform.CLM_LN_PREAUTH_CD").alias("CLM_LN_PREAUTH_CD"),
    F.col("Transform.CLM_LN_PREAUTH_SRC_CD").alias("CLM_LN_PREAUTH_SRC_CD"),
    F.col("Transform.CLM_LN_PRICE_SRC_CD").alias("CLM_LN_PRICE_SRC_CD"),
    F.col("Transform.CLM_LN_RFRL_CD").alias("CLM_LN_RFRL_CD"),
    F.col("Transform.CLM_LN_RVNU_CD").alias("CLM_LN_RVNU_CD"),
    F.rpad(F.col("Transform.CLM_LN_ROOM_PRICE_METH_CD"), 2, " ").alias("CLM_LN_ROOM_PRICE_METH_CD"),
    F.col("Transform.CLM_LN_ROOM_TYP_CD").alias("CLM_LN_ROOM_TYP_CD"),
    F.col("Transform.CLM_LN_TOS_CD").alias("CLM_LN_TOS_CD"),
    F.col("Transform.CLM_LN_UNIT_TYP_CD").alias("CLM_LN_UNIT_TYP_CD"),
    F.rpad(F.col("Transform.CAP_LN_IN"), 1, " ").alias("CAP_LN_IN"),
    F.rpad(F.col("Transform.PRI_LOB_IN"), 1, " ").alias("PRI_LOB_IN"),
    F.col("Transform.SVC_END_DT").alias("SVC_END_DT"),
    F.col("Transform.SVC_STRT_DT").alias("SVC_STRT_DT"),
    F.col("Transform.AGMNT_PRICE_AMT").alias("AGMNT_PRICE_AMT"),
    F.col("Transform.ALW_AMT").alias("ALW_AMT"),
    F.col("Transform.CHRG_AMT").alias("CHRG_AMT"),
    F.col("Transform.COINS_AMT").alias("COINS_AMT"),
    F.col("Transform.CNSD_CHRG_AMT").alias("CNSD_CHRG_AMT"),
    F.col("Transform.COPAY_AMT").alias("COPAY_AMT"),
    F.col("Transform.DEDCT_AMT").alias("DEDCT_AMT"),
    F.col("Transform.DSALW_AMT").alias("DSALW_AMT"),
    F.col("Transform.ITS_HOME_DSCNT_AMT").alias("ITS_HOME_DSCNT_AMT"),
    F.col("Transform.NO_RESP_AMT").alias("NO_RESP_AMT"),
    F.col("Transform.MBR_LIAB_BSS_AMT").alias("MBR_LIAB_BSS_AMT"),
    F.col("Transform.PATN_RESP_AMT").alias("PATN_RESP_AMT"),
    F.col("Transform.PAYBL_AMT").alias("PAYBL_AMT"),
    F.col("Transform.PAYBL_TO_PROV_AMT").alias("PAYBL_TO_PROV_AMT"),
    F.col("Transform.PAYBL_TO_SUB_AMT").alias("PAYBL_TO_SUB_AMT"),
    F.col("Transform.PROC_TBL_PRICE_AMT").alias("PROC_TBL_PRICE_AMT"),
    F.col("Transform.PROFL_PRICE_AMT").alias("PROFL_PRICE_AMT"),
    F.col("Transform.PROV_WRT_OFF_AMT").alias("PROV_WRT_OFF_AMT"),
    F.col("Transform.RISK_WTHLD_AMT").alias("RISK_WTHLD_AMT"),
    F.col("Transform.SVC_PRICE_AMT").alias("SVC_PRICE_AMT"),
    F.col("Transform.SUPLMT_DSCNT_AMT").alias("SUPLMT_DSCNT_AMT"),
    F.col("Transform.ALW_PRICE_UNIT_CT").alias("ALW_PRICE_UNIT_CT"),
    F.col("Transform.UNIT_CT").alias("UNIT_CT"),
    F.rpad(F.col("Transform.DEDCT_AMT_ACCUM_ID"), 4, " ").alias("DEDCT_AMT_ACCUM_ID"),
    F.rpad(F.col("Transform.PREAUTH_SVC_SEQ_NO"), 4, " ").alias("PREAUTH_SVC_SEQ_NO"),
    F.rpad(F.col("Transform.RFRL_SVC_SEQ_NO"), 4, " ").alias("RFRL_SVC_SEQ_NO"),
    F.rpad(F.col("Transform.LMT_PFX_ID"), 4, " ").alias("LMT_PFX_ID"),
    F.rpad(F.col("Transform.PREAUTH_ID"), 9, " ").alias("PREAUTH_ID"),
    F.rpad(F.col("Transform.PROD_CMPNT_DEDCT_PFX_ID"), 4, " ").alias("PROD_CMPNT_DEDCT_PFX_ID"),
    F.rpad(F.col("Transform.PROD_CMPNT_SVC_PAYMT_ID"), 4, " ").alias("PROD_CMPNT_SVC_PAYMT_ID"),
    F.rpad(F.col("Transform.RFRL_ID_TX"), 9, " ").alias("RFRL_ID_TX"),
    F.rpad(F.col("Transform.SVC_ID"), 4, " ").alias("SVC_ID"),
    F.rpad(F.col("Transform.SVC_PRICE_RULE_ID"), 4, " ").alias("SVC_PRICE_RULE_ID"),
    F.rpad(F.col("Transform.SVC_RULE_TYP_TX"), 3, " ").alias("SVC_RULE_TYP_TX"),
    F.rpad(F.col("Transform.SVC_LOC_TYP_CD"), 20, " ").alias("SVC_LOC_TYP_CD"),
    F.col("Transform.NON_PAR_SAV_AMT").alias("NON_PAR_SAV_AMT"),
    F.col("Transform.PROC_CD_TYP_CD").alias("PROC_CD_TYP_CD"),
    F.col("Transform.PROC_CD_CAT_CD").alias("PROC_CD_CAT_CD"),
    F.lit("NA").alias("VBB_RULE_ID"),
    F.lit("NA").alias("VBB_EXCD_ID"),
    F.rpad(F.lit("N"), 1, " ").alias("CLM_LN_VBB_IN"),
    F.lit("0.00").alias("ITS_SUPLMT_DSCNT_AMT"),
    F.lit("0.00").alias("ITS_SRCHRG_AMT"),
    F.col("Transform.NDC").alias("NDC"),
    F.lit("NA").alias("NDC_DRUG_FORM_CD"),
    F.lit(None).alias("NDC_UNIT_CT"),
    F.rpad(F.lit("PDX"), 3, " ").alias("MED_PDX_IND"),
    F.col("Transform.APC_ID").alias("APC_ID"),
    F.col("Transform.APC_STTUS_ID").alias("APC_STTUS_ID"),
    F.lit("NA").alias("SNOMED_CT_CD"),
    F.lit("NA").alias("CVX_VCCN_CD")
)

# Output pin "Snapshot" (V0S133P3) -> "Transformer"
df_Snapshot_out_Snapshot = df_Snapshot_join.select(
    F.col("Transform.CLM_ID").alias("CLCL_ID"),
    F.col("Transform.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("Transform.PROC_CD").alias("PROC_CD"),
    F.col("Transform.CLM_LN_RVNU_CD").alias("CLM_LN_RVNU_CD"),
    F.col("Transform.ALW_AMT").alias("ALW_AMT"),
    F.col("Transform.CHRG_AMT").alias("CHRG_AMT"),
    F.col("Transform.PAYBL_AMT").alias("PAYBL_AMT"),
    F.rpad(F.col("Transform.CLM_TYPE"), 1, " ").alias("CLM_TYPE"),
    F.col("Transform.PROC_CD_TYP_CD").alias("PROC_CD_TYP_CD"),
    F.col("Transform.PROC_CD_CAT_CD").alias("PROC_CD_CAT_CD")
)

# --------------------------------------------------------------------------------
# Stage: Transformer (CTransformerStage)
# Input from df_Snapshot_out_Snapshot, output => B_CLM_LN
# Stage variables:
#   ProcCdSk = GetFkeyProcCd("FACETS", 0, Snapshot.PROC_CD[1,5], Snapshot.PROC_CD_TYP_CD, Snapshot.PROC_CD_CAT_CD, 'X')
#   ClmLnRvnuCdSk = GetFkeyRvnu("FACETS", 0, Snapshot.CLM_LN_RVNU_CD, 'X')

df_Transformer_var = (
    df_Snapshot_out_Snapshot
    .withColumn(
        "ProcCdSk",
        GetFkeyProcCd("FACETS", F.lit(0),
                      F.expr("substring(PROC_CD, 1, 5)"),
                      F.col("PROC_CD_TYP_CD"),
                      F.col("PROC_CD_CAT_CD"),
                      F.lit('X'))
    )
    .withColumn(
        "ClmLnRvnuCdSk",
        GetFkeyRvnu("FACETS",
                    F.lit(0),
                    F.col("CLM_LN_RVNU_CD"),
                    F.lit('X'))
    )
)

df_Transformer_out = df_Transformer_var.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("CLCL_ID").alias("CLM_ID"),
    F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("ProcCdSk").alias("PROC_CD_SK"),
    F.col("ClmLnRvnuCdSk").alias("CLM_LN_RVNU_CD_SK"),
    F.col("ALW_AMT").alias("ALW_AMT"),
    F.col("CHRG_AMT").alias("CHRG_AMT"),
    F.col("PAYBL_AMT").alias("PAYBL_AMT")
)

# --------------------------------------------------------------------------------
# Stage: B_CLM_LN (CSeqFileStage) - write to "load/B_CLM_LN.#SrcSysCd#.dat.#RunID#"
file_B_CLM_LN = f"{adls_path}/load/B_CLM_LN.{SrcSysCd}.dat.{RunID}"
write_files(
    df_Transformer_out,
    file_B_CLM_LN,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# --------------------------------------------------------------------------------
# Stage: ClmLnPK (CContainerStage)
params_ClmLnPK = {
    "CurrRunCycle": RunCycle
}
df_ClmLnPK_out = ClmLnPK(df_Snapshot_out_PKey, params_ClmLnPK)

# --------------------------------------------------------------------------------
# Stage: LivongoEncounterClmLnExtr (CSeqFileStage)
# Write "df_ClmLnPK_out" to "key/LivongoEncClmLnExtr_#SrcSysCd#.dat.#RunID#"
file_LivongoEncounterClmLnExtr = f"{adls_path}/key/LivongoEncClmLnExtr_{SrcSysCd}.dat.{RunID}"
write_files(
    df_ClmLnPK_out,
    file_LivongoEncounterClmLnExtr,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)