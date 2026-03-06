# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2018, 2023 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC Job Name:  EmrProcedureClmLnExtr
# MAGIC CALLED BY:EmrProcedureClmLnExtrSeq
# MAGIC 
# MAGIC DESCRIPTION:  Reads the #PROVIDERNAME#.PROCEDURE.timestamp.TXTt  file and puts the data into the claim common record format and takes it through primary key process using Shared Container ClmlnPkey
# MAGIC     
# MAGIC     
# MAGIC 
# MAGIC Developer		Date		Project/Altiris #	Change Description								Development Project	Code Reviewer	Date Reviewed       
# MAGIC =======================================================================================================================================================================================       
# MAGIC Mrudula Kodali                      	2021-02-22       	US356530	Initial Programming								IntegrateDev2       
# MAGIC 
# MAGIC 
# MAGIC Venkata Y                            	2021-11-09        	US413785             	Handled Nonstandard vendor data and proc_cd_sk				IntegrateDev2    		Harsha Ravuri	2021-11-11
# MAGIC                                                                                                     	in snapshot transformer
# MAGIC 
# MAGIC Revathi Boojireddy                	2023-02-07       	US 575983           	Added SNOMED_CT_CD,CVX_VCCN_CD  fields and				IntegrateDevB		Harsha Ravuri	2023-06-14
# MAGIC 							ProcedurerClmLnExtr file.
# MAGIC Ken Bradmon		2023-03-13	us542805		Harsha copied the job from the offshore environment for me.				IntegrateDev2		Harsha Ravuri	2023-06-14
# MAGIC 
# MAGIC Revathi Boojireddy                  2023-08-01            US 589700               Removed ProcedurerClmLnExtr file and Added  default value to the fields                               IntegrateDevB		Harsha Ravuri	2023-08-31
# MAGIC                                                                                                                  SNOMED_CT_CD,CVX_VCCN_CD  in Trans_Logic
# MAGIC 	                                                                                                 stage and mapped it till target EmrProcedurerClmLnExtr file stage
# MAGIC Ken Bradmon		2023-12-11	us603888		Added logic to the stage called "Trans_Logic" for the PROC_CD column, related		IntegrateDev2		Harsha Ravuri	2023-12-15
# MAGIC 							to changes for switching to file version 5.0 of the Procedure file.  Also changed the
# MAGIC 							SQL in the "db2_src_domain" stage, because we no longer want the SQL to reference
# MAGIC 							records in the P_SRC_DOMAIN_TRNSLTN table.
# MAGIC Ken Bradmon		2024-09-06	us609108		In the stage called "Trans_Logic" the default value for the column called 		IntegrateDev1                          Jeyaprasanna        2024-10-10
# MAGIC 							SVC_PRICE_RULE_ID has been changed from a space to "NA."  Now this
# MAGIC 							column will be set to "NA" instead of "UNK" in the EDW table.

# MAGIC Added another query in reference  to extract and handle non stand supplmnt data in  Trans_logic, snapshot transformer.
# MAGIC Apply Business Logic
# MAGIC Default the value of the PROC_CD column to "UNK" and the PROC_CD_SK to zero if the reference to the PROC_CD table doesn't find a row.  Then the dowstream process won't abort.
# MAGIC Read the file created by the EmrProcedureClmExtrFormatData job.
# MAGIC /ids/prod/verified/#InFile_F#
# MAGIC Writing Sequential File to /key
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
# MAGIC Assign primary surrogate key
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType
)
from pyspark.sql.functions import (
    col,
    lit,
    when,
    length,
    rpad,
    substring
)
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../../shared_containers/PrimaryKey/ClmLnPK
# COMMAND ----------

# -------------------------------------------------------------------------
# Retrieve Parameter Values
# -------------------------------------------------------------------------
IDSOwner = get_widget_value('IDSOwner', '')
ids_secret_name = get_widget_value('ids_secret_name', '')
CurrentDate = get_widget_value('CurrentDate', '')
SrcSysCd = get_widget_value('SrcSysCd', '')
SrcSysCdSk = get_widget_value('SrcSysCdSk', '')
RunID = get_widget_value('RunID', '')
RunCycle = get_widget_value('RunCycle', '')
PROVIDERNAME = get_widget_value('PROVIDERNAME', '')
InFile_F = get_widget_value('InFile_F', '')
Nonstnd = get_widget_value('Nonstnd', '')

# -------------------------------------------------------------------------
# db2_src_domain: DB2Connector (IDS)
# -------------------------------------------------------------------------
jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query_prov = (
    "SELECT NTNL_PROV_ID,PROV_ID "
    "FROM " + IDSOwner + ".PROV "
    "WHERE NTNL_PROV_ID is not null and length(trim(NTNL_PROV_ID)) > 1 "
    "and (NTNL_PROV_ID, ABS(PROV_SK)) IN (SELECT NTNL_PROV_ID, PROV_SK FROM "
    "(SELECT NTNL_PROV_ID,ABS(PROV_SK) PROV_SK, ROW_NUMBER() OVER (PARTITION BY NTNL_PROV_ID ORDER BY ABS(PROV_SK) DESC) AS NUM  FROM "
    + IDSOwner + ".PROV)A WHERE A.NUM=1) AND NTNL_PROV_ID IS NOT NULL AND LENGTH(TRIM(NTNL_PROV_ID)) > 1 "
    "GROUP BY NTNL_PROV_ID,PROV_ID"
)

df_db2_src_domain_prov = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_prov)
    .load()
)

extract_query_src_domain = (
    "SELECT pc.PROC_CD, pc.PROC_CD_TYP_CD, pc.PROC_CD_CAT_CD "
    "FROM " + IDSOwner + ".PROC_CD pc "
    "WHERE pc.PROC_CD_CAT_CD = 'MED' "
    "AND " + Nonstnd + " = 1"
)

df_db2_src_domain_src_domain = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_src_domain)
    .load()
)

extract_query_Nonstd = (
    "SELECT PROC_CD  AS PROC_CD_ST, "
    "PROC_CD_SK AS PROC_CD_SK_ST, "
    "PROC_CD_TYP_CD AS PROC_CD_TYP_CD_ST, "
    "PROC_CD_CAT_CD AS PROC_CD_CAT_CD_ST "
    " FROM " + IDSOwner + ".PROC_CD "
    "WHERE " + Nonstnd + " = 0"
)

df_db2_src_domain_Nonstd = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_Nonstd)
    .load()
)

# -------------------------------------------------------------------------
# hf_src_domain: CHashedFileStage (Scenario A - intermediate hashed file)
# We remove the hashed file stage. Deduplicate by key columns and pass along.
# -------------------------------------------------------------------------
df_lnk_prov = df_db2_src_domain_prov.dropDuplicates(["NTNL_PROV_ID"])
df_lnk_src_domain = df_db2_src_domain_src_domain.dropDuplicates(["PROC_CD"])
df_NonstdExt = df_db2_src_domain_Nonstd.dropDuplicates(["PROC_CD_ST"])

# -------------------------------------------------------------------------
# EmrProecClmLanding: CSeqFileStage (read .dat file)
# -------------------------------------------------------------------------
schema_EmrProc = StructType([
    StructField("CLM_ID", StringType(), True),
    StructField("POL_NO", StringType(), True),
    StructField("PATN_LAST_NM", StringType(), True),
    StructField("PATN_FIRST_NM", StringType(), True),
    StructField("PATN_MID_NM", StringType(), True),
    StructField("MBI", StringType(), True),
    StructField("DOB", StringType(), True),
    StructField("GNDR", StringType(), True),
    StructField("PROC_TYPE", StringType(), True),
    StructField("DT_OF_SVC", StringType(), True),
    StructField("RSLT_VAL", StringType(), True),
    StructField("RNDR_NTNL_PROV_ID", StringType(), True),
    StructField("RNDR_PROV_TYP", StringType(), True),
    StructField("PROC_CD_CPT", StringType(), True),
    StructField("PROC_CD_CPT_MOD_1", StringType(), True),
    StructField("PROC_CD_CPT_MOD_2", StringType(), True),
    StructField("PROC_CD_CPTII_MOD_1", StringType(), True),
    StructField("PROC_CD_CPTII_MOD_2", StringType(), True),
    StructField("SNOMED_CT_CD", StringType(), True),
    StructField("CVX_VCCN_CD", StringType(), True),
    StructField("SOURCE_ID", StringType(), True),
    StructField("MBR_UNIQ_KEY", StringType(), True),
    StructField("CLM_LN_SEQ", StringType(), True)
])

df_EmrProc = (
    spark.read.format("csv")
    .option("header", "false")
    .option("inferschema", "false")
    .option("sep", ",")
    .option("quote", "\u0000")  # "quoteChar": "000" means no real quotes
    .schema(schema_EmrProc)
    .load(f"{adls_path}/verified/{InFile_F}")
)

# -------------------------------------------------------------------------
# Trans_Logic: CTransformerStage
# Primary link = df_EmrProc
# Lookup link 1 (left join) = df_lnk_prov on "RNDR_NTNL_PROV_ID" = "NTNL_PROV_ID"
# Lookup link 2 (left join) = df_lnk_src_domain on "PROC_CD_CPT" = "PROC_CD"
# Lookup link 3 (left join) = df_NonstdExt on "PROC_TYPE" = "PROC_CD_ST"
# Then produce output "lnk_tx_out"
# -------------------------------------------------------------------------
df_trans_logic = (
    df_EmrProc.alias("EmrProc")
    .join(df_lnk_prov.alias("lnk_prov"),
          col("EmrProc.RNDR_NTNL_PROV_ID") == col("lnk_prov.NTNL_PROV_ID"),
          "left")
    .join(df_lnk_src_domain.alias("lnk_src_domain"),
          col("EmrProc.PROC_CD_CPT") == col("lnk_src_domain.PROC_CD"),
          "left")
    .join(df_NonstdExt.alias("NonstdExt"),
          col("EmrProc.PROC_TYPE") == col("NonstdExt.PROC_CD_ST"),
          "left")
)

# Build the final output columns for link "lnk_tx_out"
df_lnk_tx_out = df_trans_logic.select(
    # 1) CLM_LN_SK => always 0
    lit(0).alias("CLM_LN_SK"),

    # 2) SRC_SYS_CD_SK => from parameter: SrcSysCdSk
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),

    # 3) CLM_ID => EmrProc.CLM_ID
    col("EmrProc.CLM_ID").alias("CLM_ID"),

    # 4) CLM_LN_SEQ_NO => always 1
    lit(1).alias("CLM_LN_SEQ_NO"),

    # 5) CRT_RUN_CYC_EXCTN_SK => 0
    lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),

    # 6) LAST_UPDT_RUN_CYC_EXCTN_SK => 0
    lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),

    # 7) CLM_SK => 0
    lit(0).alias("CLM_SK"),

    # 8) PROC_CD => conditional
    when(
        (lit(SrcSysCd) == lit("NONSTDSUPLMTDATA")) & (length(col("EmrProc.PROC_TYPE")) > 0),
        col("EmrProc.PROC_TYPE")
    ).otherwise(
        when(
            (lit(SrcSysCd) == lit("NONSTDSUPLMTDATA")) & (length(col("EmrProc.PROC_TYPE")) == 0),
            lit("UNK")
        ).otherwise(
            when(length(col("lnk_src_domain.PROC_CD")) > 0, col("lnk_src_domain.PROC_CD")).otherwise(lit("UNK"))
        )
    ).alias("PROC_CD"),

    # 9) PROC_CD_TYP_CD => conditional
    when(
        (lit(SrcSysCd) == lit("NONSTDSUPLMTDATA")) & (length(col("NonstdExt.PROC_CD_TYP_CD_ST")) > 0),
        col("NonstdExt.PROC_CD_TYP_CD_ST")
    ).otherwise(
        when(
            (lit(SrcSysCd) == lit("NONSTDSUPLMTDATA")) & (length(col("NonstdExt.PROC_CD_TYP_CD_ST")) == 0),
            lit("UNK")
        ).otherwise(
            when(length(col("lnk_src_domain.PROC_CD_TYP_CD")) > 0, col("lnk_src_domain.PROC_CD_TYP_CD")).otherwise(lit("UNK"))
        )
    ).alias("PROC_CD_TYP_CD"),

    # 10) PROC_CD_CAT_CD => conditional
    when(
        (lit(SrcSysCd) == lit("NONSTDSUPLMTDATA")) & (length(col("NonstdExt.PROC_CD_CAT_CD_ST")) > 0),
        col("NonstdExt.PROC_CD_CAT_CD_ST")
    ).otherwise(
        when(
            (lit(SrcSysCd) == lit("NONSTDSUPLMTDATA")) & (length(col("NonstdExt.PROC_CD_CAT_CD_ST")) == 0),
            lit("UNK")
        ).otherwise(
            when(length(col("lnk_src_domain.PROC_CD_CAT_CD")) > 0, col("lnk_src_domain.PROC_CD_CAT_CD")).otherwise(lit("UNK"))
        )
    ).alias("PROC_CD_CAT_CD"),

    # 11) SVC_PROV_SK => if null(lnk_prov.PROV_ID) then "NA" else lnk_prov.PROV_ID
    when(col("lnk_prov.PROV_ID").isNull(), lit("NA")).otherwise(col("lnk_prov.PROV_ID")).alias("SVC_PROV_SK"),

    # 12) CLM_LN_DSALW_EXCD => 'NA'
    lit("NA").alias("CLM_LN_DSALW_EXCD"),

    # 13) CLM_LN_EOB_EXCD => 'NA'
    lit("NA").alias("CLM_LN_EOB_EXCD"),

    # 14) CLM_LN_FINL_DISP_CD => 'NA'
    lit("NA").alias("CLM_LN_FINL_DISP_CD"),

    # 15) CLM_LN_LOB_CD => 'NA'
    lit("NA").alias("CLM_LN_LOB_CD"),

    # 16) CLM_LN_POS_CD_SK => 11
    lit(11).alias("CLM_LN_POS_CD_SK"),

    # 17) CLM_LN_PREAUTH_CD => 'NA'
    lit("NA").alias("CLM_LN_PREAUTH_CD"),

    # 18) CLM_LN_PREAUTH_SRC_CD => 'NA'
    lit("NA").alias("CLM_LN_PREAUTH_SRC_CD"),

    # 19) CLM_LN_PRICE_SRC_CD => 'NA'
    lit("NA").alias("CLM_LN_PRICE_SRC_CD"),

    # 20) CLM_LN_RFRL_CD => 'NA'
    lit("NA").alias("CLM_LN_RFRL_CD"),

    # 21) CLM_LN_RVNU_CD => 'NA'
    lit("NA").alias("CLM_LN_RVNU_CD"),

    # 22) CLM_LN_ROOM_PRICE_METH_CD => 'NA'
    lit("NA").alias("CLM_LN_ROOM_PRICE_METH_CD"),

    # 23) CLM_LN_ROOM_TYP_CD => 'NA'
    lit("NA").alias("CLM_LN_ROOM_TYP_CD"),

    # 24) CLM_LN_TOS_CD => 'NA'
    lit("NA").alias("CLM_LN_TOS_CD"),

    # 25) CLM_LN_UNIT_TYP_CD_SK => 'UN'
    lit("UN").alias("CLM_LN_UNIT_TYP_CD_SK"),

    # 26) CAP_LN_IN => 'N'
    lit("N").alias("CAP_LN_IN"),

    # 27) PRI_LOB_IN => 'N'
    lit("N").alias("PRI_LOB_IN"),

    # 28) SVC_END_DT_SK => EmrProc.DT_OF_SVC[1,4] : '-' : [5,2] : '-' : [7,2]
    when(
        length(col("EmrProc.DT_OF_SVC")) >= 8,
        col("EmrProc.DT_OF_SVC").substr(1,4).concat(lit("-"))
        .concat(col("EmrProc.DT_OF_SVC").substr(5,2)).concat(lit("-"))
        .concat(col("EmrProc.DT_OF_SVC").substr(7,2))
    ).otherwise(lit("")).alias("SVC_END_DT_SK"),

    # 29) SVC_STRT_DT_SK => same expression
    when(
        length(col("EmrProc.DT_OF_SVC")) >= 8,
        col("EmrProc.DT_OF_SVC").substr(1,4).concat(lit("-"))
        .concat(col("EmrProc.DT_OF_SVC").substr(5,2)).concat(lit("-"))
        .concat(col("EmrProc.DT_OF_SVC").substr(7,2))
    ).otherwise(lit("")).alias("SVC_STRT_DT_SK"),

    # 30) AGMNT_PRICE_AMT => 0.00
    lit("0.00").alias("AGMNT_PRICE_AMT"),

    # 31) ALW_AMT => 0.00
    lit("0.00").alias("ALW_AMT"),

    # 32) CHRG_AMT => 0.00
    lit("0.00").alias("CHRG_AMT"),

    # 33) COINS_AMT => 0.00
    lit("0.00").alias("COINS_AMT"),

    # 34) CNSD_CHRG_AMT => 0.00
    lit("0.00").alias("CNSD_CHRG_AMT"),

    # 35) COPAY_AMT => 0.00
    lit("0.00").alias("COPAY_AMT"),

    # 36) DEDCT_AMT => 0.00
    lit("0.00").alias("DEDCT_AMT"),

    # 37) DSALW_AMT => 0.00
    lit("0.00").alias("DSALW_AMT"),

    # 38) ITS_HOME_DSCNT_AMT => 0.00
    lit("0.00").alias("ITS_HOME_DSCNT_AMT"),

    # 39) NO_RESP_AMT => 0.00
    lit("0.00").alias("NO_RESP_AMT"),

    # 40) MBR_LIAB_BSS_AMT => 0.00
    lit("0.00").alias("MBR_LIAB_BSS_AMT"),

    # 41) PATN_RESP_AMT => 0.00
    lit("0.00").alias("PATN_RESP_AMT"),

    # 42) PAYBL_AMT => 0.00
    lit("0.00").alias("PAYBL_AMT"),

    # 43) PAYBL_TO_PROV_AMT => 0.00
    lit("0.00").alias("PAYBL_TO_PROV_AMT"),

    # 44) PAYBL_TO_SUB_AMT => 0.00
    lit("0.00").alias("PAYBL_TO_SUB_AMT"),

    # 45) PROC_TBL_PRICE_AMT => 0.00
    lit("0.00").alias("PROC_TBL_PRICE_AMT"),

    # 46) PROFL_PRICE_AMT => 0.00
    lit("0.00").alias("PROFL_PRICE_AMT"),

    # 47) PROV_WRT_OFF_AMT => 0.00
    lit("0.00").alias("PROV_WRT_OFF_AMT"),

    # 48) RISK_WTHLD_AMT => 0.00
    lit("0.00").alias("RISK_WTHLD_AMT"),

    # 49) SVC_PRICE_AMT => 0.00
    lit("0.00").alias("SVC_PRICE_AMT"),

    # 50) SUPLMT_DSCNT_AMT => 0.00
    lit("0.00").alias("SUPLMT_DSCNT_AMT"),

    # 51) ALW_PRICE_UNIT_CT => 0
    lit(0).alias("ALW_PRICE_UNIT_CT"),

    # 52) UNIT_CT => 1
    lit(1).alias("UNIT_CT"),

    # 53) DEDCT_AMT_ACCUM_ID => '  '
    lit("  ").alias("DEDCT_AMT_ACCUM_ID"),

    # 54) PREAUTH_SVC_SEQ_NO => 'NA'
    lit("NA").alias("PREAUTH_SVC_SEQ_NO"),

    # 55) RFRL_SVC_SEQ_NO => 'NA'
    lit("NA").alias("RFRL_SVC_SEQ_NO"),

    # 56) LMT_PFX_ID => '  '
    lit("  ").alias("LMT_PFX_ID"),

    # 57) PREAUTH_ID => '  '
    lit("  ").alias("PREAUTH_ID"),

    # 58) PROD_CMPNT_DEDCT_PFX_ID => '  '
    lit("  ").alias("PROD_CMPNT_DEDCT_PFX_ID"),

    # 59) PROD_CMPNT_SVC_PAYMT_ID => '  '
    lit("  ").alias("PROD_CMPNT_SVC_PAYMT_ID"),

    # 60) RFRL_ID_TX => 'NA'
    lit("NA").alias("RFRL_ID_TX"),

    # 61) SVC_ID => '  '
    lit("  ").alias("SVC_ID"),

    # 62) SVC_PRICE_RULE_ID => 'NA'
    lit("NA").alias("SVC_PRICE_RULE_ID"),

    # 63) SVC_RULE_TYP_TX => 'NA'
    lit("NA").alias("SVC_RULE_TYP_TX"),

    # 64) CLM_LN_SVC_LOC_TYP_CD => 'NA'
    lit("NA").alias("CLM_LN_SVC_LOC_TYP_CD"),

    # 65) CLM_LN_SVC_PRICE_RULE_CD => 'NA'
    lit("NA").alias("CLM_LN_SVC_PRICE_RULE_CD"),

    # 66) NON_PAR_SAV_AMT => 0.00
    lit("0.00").alias("NON_PAR_SAV_AMT"),

    # 67) VBB_RULE => 'NA'
    lit("NA").alias("VBB_RULE"),

    # 68) VBB_EXCD => 'NA'
    lit("NA").alias("VBB_EXCD"),

    # 69) CLM_LN_VBB_IN => 'N'
    lit("N").alias("CLM_LN_VBB_IN"),

    # 70) ITS_SUPLMT_DSCNT_AMT => 0.00
    lit("0.00").alias("ITS_SUPLMT_DSCNT_AMT"),

    # 71) ITS_SRCHRG_AMT => 0.00
    lit("0.00").alias("ITS_SRCHRG_AMT"),

    # 72) NDC => 'NA'
    lit("NA").alias("NDC"),

    # 73) NDC_DRUG_FORM_CD => 'NA'
    lit("NA").alias("NDC_DRUG_FORM_CD"),

    # 74) NDC_UNIT_CT => ''
    lit("").alias("NDC_UNIT_CT"),

    # 75) APC_ID => 'NA'
    lit("NA").alias("APC_ID"),

    # 76) APC_STTUS_ID => 'NA'
    lit("NA").alias("APC_STTUS_ID"),

    # 77) PROC_CD_SK_ST => IF Len(NonstdExt.PROC_CD_SK_ST) > 0 THEN NonstdExt.PROC_CD_SK_ST ELSE 0
    when(length(col("NonstdExt.PROC_CD_SK_ST")) > 0, col("NonstdExt.PROC_CD_SK_ST")).otherwise(lit("0")).alias("PROC_CD_SK_ST"),

    # 78) SNOMED_CT_CD => EmrProc.SNOMED_CT_CD
    col("EmrProc.SNOMED_CT_CD").alias("SNOMED_CT_CD"),

    # 79) CVX_VCCN_CD => EmrProc.CVX_VCCN_CD
    col("EmrProc.CVX_VCCN_CD").alias("CVX_VCCN_CD")
)

# -------------------------------------------------------------------------
# Snapshot: CTransformerStage
# Input: df_lnk_tx_out (Primary)
# Output: PKey, Snapshot
# -------------------------------------------------------------------------
df_PKey = df_lnk_tx_out.select(
    lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(lit("|"), 10, " ").alias("INSRT_UPDT_CD"),  # char(10)
    rpad(lit("N"), 1, " ").alias("DISCARD_IN"),      # char(1)
    rpad(lit("Y"), 1, " ").alias("PASS_THRU_IN"),    # char(1)
    lit(current_date()).alias("FIRST_RECYC_DT"),
    lit(0).alias("ERR_CT"),
    lit(0).alias("RECYCLE_CT"),
    lit(SrcSysCd).alias("SRC_SYS_CD"),
    (lit(SrcSysCd) + lit(";") + col("CLM_ID") + lit(";") + lit("1")).alias("PRI_KEY_STRING"),
    col("CLM_LN_SK").alias("CLM_LN_SK"),
    col("CLM_ID").alias("CLM_ID"),
    col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("PROC_CD").alias("PROC_CD"),
    col("SVC_PROV_SK").alias("SVC_PROV_ID"),
    col("CLM_LN_DSALW_EXCD").alias("CLM_LN_DSALW_EXCD"),
    col("CLM_LN_EOB_EXCD").alias("CLM_LN_EOB_EXCD"),
    col("CLM_LN_FINL_DISP_CD").alias("CLM_LN_FINL_DISP_CD"),
    col("CLM_LN_LOB_CD").alias("CLM_LN_LOB_CD"),
    col("CLM_LN_POS_CD_SK").alias("CLM_LN_POS_CD"),
    col("CLM_LN_PREAUTH_CD").alias("CLM_LN_PREAUTH_CD"),
    col("CLM_LN_PREAUTH_SRC_CD").alias("CLM_LN_PREAUTH_SRC_CD"),
    col("CLM_LN_PRICE_SRC_CD").alias("CLM_LN_PRICE_SRC_CD"),
    col("CLM_LN_RFRL_CD").alias("CLM_LN_RFRL_CD"),
    col("CLM_LN_RVNU_CD").alias("CLM_LN_RVNU_CD"),
    rpad(col("CLM_LN_ROOM_PRICE_METH_CD"), 2, " ").alias("CLM_LN_ROOM_PRICE_METH_CD"),  # char(2)
    col("CLM_LN_ROOM_TYP_CD").alias("CLM_LN_ROOM_TYP_CD"),
    col("CLM_LN_TOS_CD").alias("CLM_LN_TOS_CD"),
    col("CLM_LN_UNIT_TYP_CD_SK").alias("CLM_LN_UNIT_TYP_CD"),
    rpad(col("CAP_LN_IN"), 1, " ").alias("CAP_LN_IN"),   # char(1)
    rpad(col("PRI_LOB_IN"), 1, " ").alias("PRI_LOB_IN"), # char(1)
    col("SVC_END_DT_SK").alias("SVC_END_DT"),
    col("SVC_STRT_DT_SK").alias("SVC_STRT_DT"),
    col("AGMNT_PRICE_AMT").alias("AGMNT_PRICE_AMT"),
    col("ALW_AMT").alias("ALW_AMT"),
    col("CHRG_AMT").alias("CHRG_AMT"),
    col("COINS_AMT").alias("COINS_AMT"),
    col("CNSD_CHRG_AMT").alias("CNSD_CHRG_AMT"),
    col("COPAY_AMT").alias("COPAY_AMT"),
    col("DEDCT_AMT").alias("DEDCT_AMT"),
    col("DSALW_AMT").alias("DSALW_AMT"),
    col("ITS_HOME_DSCNT_AMT").alias("ITS_HOME_DSCNT_AMT"),
    col("NO_RESP_AMT").alias("NO_RESP_AMT"),
    col("MBR_LIAB_BSS_AMT").alias("MBR_LIAB_BSS_AMT"),
    col("PATN_RESP_AMT").alias("PATN_RESP_AMT"),
    col("PAYBL_AMT").alias("PAYBL_AMT"),
    col("PAYBL_TO_PROV_AMT").alias("PAYBL_TO_PROV_AMT"),
    col("PAYBL_TO_SUB_AMT").alias("PAYBL_TO_SUB_AMT"),
    col("PROC_TBL_PRICE_AMT").alias("PROC_TBL_PRICE_AMT"),
    col("PROFL_PRICE_AMT").alias("PROFL_PRICE_AMT"),
    col("PROV_WRT_OFF_AMT").alias("PROV_WRT_OFF_AMT"),
    col("RISK_WTHLD_AMT").alias("RISK_WTHLD_AMT"),
    col("SVC_PRICE_AMT").alias("SVC_PRICE_AMT"),
    col("SUPLMT_DSCNT_AMT").alias("SUPLMT_DSCNT_AMT"),
    col("ALW_PRICE_UNIT_CT").alias("ALW_PRICE_UNIT_CT"),
    col("UNIT_CT").alias("UNIT_CT"),
    rpad(col("DEDCT_AMT_ACCUM_ID"), 4, " ").alias("DEDCT_AMT_ACCUM_ID"),       # char(4)
    rpad(col("PREAUTH_SVC_SEQ_NO"), 4, " ").alias("PREAUTH_SVC_SEQ_NO"),       # char(4)
    rpad(col("RFRL_SVC_SEQ_NO"), 4, " ").alias("RFRL_SVC_SEQ_NO"),             # char(4)
    rpad(col("LMT_PFX_ID"), 4, " ").alias("LMT_PFX_ID"),                       # char(4)
    rpad(col("PREAUTH_ID"), 9, " ").alias("PREAUTH_ID"),                       # char(9)
    rpad(col("PROD_CMPNT_DEDCT_PFX_ID"), 4, " ").alias("PROD_CMPNT_DEDCT_PFX_ID"),    # char(4)
    rpad(col("PROD_CMPNT_SVC_PAYMT_ID"), 4, " ").alias("PROD_CMPNT_SVC_PAYMT_ID"),    # char(4)
    rpad(col("RFRL_ID_TX"), 9, " ").alias("RFRL_ID_TX"),                             # char(9)
    rpad(col("SVC_ID"), 4, " ").alias("SVC_ID"),                                     # char(4)
    rpad(col("SVC_PRICE_RULE_ID"), 4, " ").alias("SVC_PRICE_RULE_ID"),               # char(4)
    rpad(col("SVC_RULE_TYP_TX"), 3, " ").alias("SVC_RULE_TYP_TX"),                   # char(3)
    rpad(col("CLM_LN_SVC_LOC_TYP_CD"), 20, " ").alias("SVC_LOC_TYP_CD"),             # char(20)
    col("NON_PAR_SAV_AMT").alias("NON_PAR_SAV_AMT"),
    col("PROC_CD_TYP_CD").alias("PROC_CD_TYP_CD"),
    col("PROC_CD_CAT_CD").alias("PROC_CD_CAT_CD"),
    lit("NA").alias("VBB_RULE_ID"),
    lit("NA").alias("VBB_EXCD_ID"),
    rpad(col("CLM_LN_VBB_IN"), 1, " ").alias("CLM_LN_VBB_IN"),                  # char(1)
    col("ITS_SUPLMT_DSCNT_AMT").alias("ITS_SUPLMT_DSCNT_AMT"),
    col("ITS_SRCHRG_AMT").alias("ITS_SRCHRG_AMT"),
    col("NDC").alias("NDC"),
    col("NDC_DRUG_FORM_CD").alias("NDC_DRUG_FORM_CD"),
    col("NDC_UNIT_CT").alias("NDC_UNIT_CT"),
    rpad(lit("PDX"), 3, " ").alias("MED_PDX_IND"),                              # char(3)
    col("APC_ID").alias("APC_ID"),
    col("APC_STTUS_ID").alias("APC_STTUS_ID"),
    col("SNOMED_CT_CD").alias("SNOMED_CT_CD"),
    col("CVX_VCCN_CD").alias("CVX_VCCN_CD")
)

df_Snapshot = df_lnk_tx_out.select(
    col("CLM_ID").alias("CLCL_ID"),
    col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    col("PROC_CD").alias("PROC_CD"),
    col("CLM_LN_RVNU_CD").alias("CLM_LN_RVNU_CD"),
    col("ALW_AMT").alias("ALW_AMT"),
    col("CHRG_AMT").alias("CHRG_AMT"),
    col("PAYBL_AMT").alias("PAYBL_AMT"),
    rpad(lit("M"), 1, " ").alias("CLM_TYPE"),            # char(1)
    col("PROC_CD_TYP_CD").alias("PROC_CD_TYP_CD"),
    col("PROC_CD_CAT_CD").alias("PROC_CD_CAT_CD"),
    col("PROC_CD_SK_ST").alias("PROC_CD_SK_ST")
)

# -------------------------------------------------------------------------
# ClmLnPK: Shared Container
# Input: df_PKey
# Output: df_Key
# -------------------------------------------------------------------------
params = {
    "CurrRunCycle": RunCycle
}
df_Key = ClmLnPK(df_PKey, params)

# -------------------------------------------------------------------------
# EmrProcedurerClmLnExtr: CSeqFileStage (write)
# Input: df_Key
# -------------------------------------------------------------------------
# Ensure correct order and apply rpad for char/varchar, but they are already set above.
df_EmrProcedurerClmLnExtr = df_Key  # Already has columns in final rpad form

write_files(
    df_EmrProcedurerClmLnExtr,
    f"{adls_path}/key/EmrProcedureClmLnExtr_{SrcSysCd}.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# -------------------------------------------------------------------------
# Transformer (StageName="Transformer"): CTransformerStage
# Input: df_Snapshot
# Stage Variables: 
#   ProcCdSk = GetFkeyProcCd("FACETS", 0, Snapshot.PROC_CD[1,5], Snapshot.PROC_CD_TYP_CD, Snapshot.PROC_CD_CAT_CD, 'X')
#   ClmLnRvnuCdSk = GetFkeyRvnu("FACETS", 0, Snapshot.CLM_LN_RVNU_CD, 'X')
# Output: RowCount
# -------------------------------------------------------------------------
df_Transformer = (
    df_Snapshot
    .withColumn(
        "ProcCdSk",
        GetFkeyProcCd(
            "FACETS",
            lit(0),
            col("PROC_CD").substr(1,5),
            col("PROC_CD_TYP_CD"),
            col("PROC_CD_CAT_CD"),
            'X'
        )
    )
    .withColumn(
        "ClmLnRvnuCdSk",
        GetFkeyRvnu(
            "FACETS",
            lit(0),
            col("CLM_LN_RVNU_CD"),
            'X'
        )
    )
)

df_RowCount = df_Transformer.select(
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    col("CLCL_ID").alias("CLM_ID"),
    col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    when(
        lit(SrcSysCd) == lit("NONSTDSUPLMTDATA"),
        col("PROC_CD_SK_ST")
    ).otherwise(col("ProcCdSk")).alias("PROC_CD_SK"),
    col("ClmLnRvnuCdSk").alias("CLM_LN_RVNU_CD_SK"),
    col("ALW_AMT").alias("ALW_AMT"),
    col("CHRG_AMT").alias("CHRG_AMT"),
    col("PAYBL_AMT").alias("PAYBL_AMT")
)

# -------------------------------------------------------------------------
# B_CLM_LN: CSeqFileStage (write)
# Input: df_RowCount
# -------------------------------------------------------------------------
# The job does not specify char/varchar lengths for these columns, so we assume none require rpad here.
df_B_CLM_LN = df_RowCount.select(
    "SRC_SYS_CD_SK",
    "CLM_ID",
    "CLM_LN_SEQ_NO",
    "PROC_CD_SK",
    "CLM_LN_RVNU_CD_SK",
    "ALW_AMT",
    "CHRG_AMT",
    "PAYBL_AMT"
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