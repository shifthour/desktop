# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2018 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC CALLED BY:  
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:  Reads the EyeMedClm_ClaimsLanding.dat file and puts the data into the claim common record format and takes it through primary key process using Shared Container ClmPkey
# MAGIC     
# MAGIC     
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                               Date                 Project/Altiris #       Change Description                                                                   Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------                             --------------------     ------------------------       -----------------------------------------------------------------------------------------          --------------------------------       -------------------------------   ----------------------------       
# MAGIC Sethuraman Rajendran           2018-03-01        5744                     Initial Programming                                                                    IntegrateDev2                    Kalyan Neelam          2018-04-04
# MAGIC Sethuraman Rajendran           2019-05-01        111088                 Cdde Fix on Proc Cd column mapping                                      IntegrateDev2    	   Jaideep Mankala      05/06/2019               
# MAGIC 
# MAGIC Velmani Kondappan               2020-08-28         6264-PBM Phase II - Government Programs      Added APC_ID, APC_STTUS_ID  IntegrateDev5                 Kalyan Neelam          2020-12-10
# MAGIC 
# MAGIC Goutham K                            2021-06-02         US-366403            New Provider file Change to include Loc and Svc loc id             IntegrateDev1                 Jeyaprasanna            2021-06-08
# MAGIC 
# MAGIC Arpitha V                               2023-07-31         US 589700         Added two new fields SNOMED_CT_CD,CVX_VCCN_CD with          IntegrateDevB	Harsha Ravuri	2023-08-31
# MAGIC                                                                                                    a default value in BusinessRules stage and mapped it till target

# MAGIC Read the EYEMED Claim Line file created
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
# MAGIC BCBSSCClmLnExtr
# MAGIC BCBSSCMedClmLnExtr
# MAGIC EyeMedClmLnExtr
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
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DecimalType
)
from pyspark.sql.functions import (
    col,
    lit,
    when,
    length,
    concat,
    concat_ws,
    substring,
    rpad
)
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
CurrentDate = get_widget_value('CurrentDate','')
SrcSysCd = get_widget_value('SrcSysCd','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
RunID = get_widget_value('RunID','')
RunCycle = get_widget_value('RunCycle','')

# MAGIC %run ../../../../../../shared_containers/PrimaryKey/ClmLnPK
# COMMAND ----------

# --------------------------------------------------------------------------------
# EYEMEDClmLnLanding (CSeqFileStage) - Read from file with defined schema
# --------------------------------------------------------------------------------
schema_EYEMEDClmLnLanding = StructType([
    StructField("CLM_ID", StringType(), True),
    StructField("CLM_LN_NO", DecimalType(10, 0), True),
    StructField("MBR_UNIQ_KEY", IntegerType(), True),
    StructField("SUB_UNIQ_KEY", IntegerType(), True),
    StructField("GRP_ID", StringType(), True),
    StructField("DOB", StringType(), True),
    StructField("GNDR_CD", StringType(), True),
    StructField("SUB_ID_IDS", StringType(), True),
    StructField("MBR_SFX_NO", StringType(), True),
    StructField("RCRD_TYP", StringType(), True),
    StructField("ADJ_VOID_FLAG", StringType(), True),
    StructField("EYEMED_GRP_ID", StringType(), True),
    StructField("EYEMED_SUBGRP_ID", StringType(), True),
    StructField("BILL_TYP_IN", StringType(), True),
    StructField("CLM_NO", StringType(), True),
    StructField("LN_CTR", StringType(), True),
    StructField("DT_OF_SVC", StringType(), True),
    StructField("INVC_NO", StringType(), True),
    StructField("INVC_DT", StringType(), True),
    StructField("BILL_AMT", StringType(), True),
    StructField("FFS_ADM_FEE", StringType(), True),
    StructField("RTL_AMT", StringType(), True),
    StructField("MBR_OOP", StringType(), True),
    StructField("3RD_PARTY_DSCNT", StringType(), True),
    StructField("COPAY_AMT", StringType(), True),
    StructField("COV_AMT", StringType(), True),
    StructField("FLR_1", StringType(), True),
    StructField("FLR_2", StringType(), True),
    StructField("NTWK_IN", StringType(), True),
    StructField("SVC_CD", StringType(), True),
    StructField("SVC_DESC", StringType(), True),
    StructField("MOD_CD_1", StringType(), True),
    StructField("MOD_CD_2", StringType(), True),
    StructField("MOD_CD_3", StringType(), True),
    StructField("MOD_CD_4", StringType(), True),
    StructField("MOD_CD_5", StringType(), True),
    StructField("MOD_CD_6", StringType(), True),
    StructField("MOD_CD_7", StringType(), True),
    StructField("MOD_CD_8", StringType(), True),
    StructField("ICD_CD_SET", StringType(), True),
    StructField("DIAG_CD_1", StringType(), True),
    StructField("DIAG_CD_2", StringType(), True),
    StructField("DIAG_CD_3", StringType(), True),
    StructField("DIAG_CD_4", StringType(), True),
    StructField("DIAG_CD_5", StringType(), True),
    StructField("DIAG_CD_6", StringType(), True),
    StructField("DIAG_CD_7", StringType(), True),
    StructField("DIAG_CD_8", StringType(), True),
    StructField("PATN_ID", StringType(), True),
    StructField("PATN_SSN", StringType(), True),
    StructField("PATN_FIRST_NM", StringType(), True),
    StructField("PATN_MIDINIT", StringType(), True),
    StructField("PATN_LAST_NM", StringType(), True),
    StructField("PATN_GNDR", StringType(), True),
    StructField("PATN_FMLY_RELSHP", StringType(), True),
    StructField("PATN_DOB", StringType(), True),
    StructField("PATN_ADDR", StringType(), True),
    StructField("PATN_ADDR_2", StringType(), True),
    StructField("PATN_CITY", StringType(), True),
    StructField("PATN_ST", StringType(), True),
    StructField("PATN_ZIP", StringType(), True),
    StructField("PATN_ZIP4", StringType(), True),
    StructField("CLNT_GRP_NO", StringType(), True),
    StructField("CO_CD", StringType(), True),
    StructField("DIV_CD", StringType(), True),
    StructField("LOC_CD", StringType(), True),
    StructField("CLNT_RPTNG_1", StringType(), True),
    StructField("CLNT_RPTNG_2", StringType(), True),
    StructField("CLNT_RPTNG_3", StringType(), True),
    StructField("CLNT_RPTNG_4", StringType(), True),
    StructField("CLNT_RPTNG_5", StringType(), True),
    StructField("CLS_PLN_ID", StringType(), True),
    StructField("SUB_ID", StringType(), True),
    StructField("SUB_SSN", StringType(), True),
    StructField("SUB_FIRST_NM", StringType(), True),
    StructField("SUB_MIDINIT", StringType(), True),
    StructField("SUB_LAST_NM", StringType(), True),
    StructField("SUB_GNDR", StringType(), True),
    StructField("SUB_DOB", StringType(), True),
    StructField("SUB_ADDR", StringType(), True),
    StructField("SUB_ADDR_2", StringType(), True),
    StructField("SUB_CITY", StringType(), True),
    StructField("SUB_ST", StringType(), True),
    StructField("SUB_ZIP", StringType(), True),
    StructField("SUB_ZIP_PLUS_4", StringType(), True),
    StructField("PROV_ID", StringType(), True),
    StructField("PROV_NPI", StringType(), True),
    StructField("TAX_ENTY_NPI", StringType(), True),
    StructField("PROV_FIRST_NM", StringType(), True),
    StructField("PROV_LAST_NM", StringType(), True),
    StructField("BUS_NM", StringType(), True),
    StructField("PROV_ADDR", StringType(), True),
    StructField("PROV_ADDR_2", StringType(), True),
    StructField("PROV_CITY", StringType(), True),
    StructField("PROV_ST", StringType(), True),
    StructField("PROV_ZIP", StringType(), True),
    StructField("PROV_ZIP_PLUS_4", StringType(), True),
    StructField("PROF_DSGTN", StringType(), True),
    StructField("TAX_ENTY_ID", StringType(), True),
    StructField("TXNMY_CD", StringType(), True),
    StructField("CLM_RCVD_DT", StringType(), True),
    StructField("ADJDCT_DT", StringType(), True),
    StructField("CHK_DT", StringType(), True),
    StructField("DENIAL_RSN_CD", StringType(), True),
    StructField("SVC_TYP", StringType(), True),
    StructField("UNIT_OF_SVC", StringType(), True),
    StructField("FLR", StringType(), True)
])

df_EYEMEDClmLnLanding = (
    spark.read.format("csv")
    .option("header", "false")
    .option("quote", "\"")
    .option("escape", "\"")
    .schema(schema_EYEMEDClmLnLanding)
    .load(f"{adls_path}/verified/EyeMedClm_ClmLnlanding.dat.{RunID}")
)

# --------------------------------------------------------------------------------
# BusinessRules (CTransformerStage)
# --------------------------------------------------------------------------------
df_BusinessRules_intermediate = (
    df_EYEMEDClmLnLanding
    .withColumn("svProcCd", substring(trim(col("SVC_CD")), 1, 5))
    .withColumn("svProcCdCatCd", lit("MED"))
    .withColumn("svProcCdTypCd", lit(""))
    .withColumn("svProcCdTypCd2", lit(""))
)

df_BusinessRules = (
    df_BusinessRules_intermediate
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", lit(0))
    .withColumn("INSRT_UPDT_CD", rpad(lit("I"), 10, " "))
    .withColumn("DISCARD_IN", rpad(lit("N"), 1, " "))
    .withColumn("PASS_THRU_IN", rpad(lit("Y"), 1, " "))
    .withColumn("FIRST_RECYC_DT", current_date())
    .withColumn("ERR_CT", lit(0))
    .withColumn("RECYCLE_CT", lit(0))
    .withColumn("SRC_SYS_CD", lit(SrcSysCd))
    .withColumn("PRI_KEY_STRING", concat(
        lit(SrcSysCd), lit(";"), col("CLM_ID"), lit(";"), col("CLM_LN_NO")
    ))
    .withColumn("CLM_LN_SK", lit(0))
    .withColumn("CLM_ID", col("CLM_ID"))
    .withColumn("CLM_LN_SEQ_NO", col("LN_CTR"))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", lit(RunCycle))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", lit(RunCycle))
    .withColumn("PROC_CD", col("svProcCd"))
    .withColumn("SVC_PROV_ID", col("PROV_ID"))
    .withColumn("CLM_LN_DSALW_EXCD", lit("NA"))
    .withColumn("CLM_LN_EOB_EXCD", lit("NA"))
    .withColumn(
        "CLM_LN_FINL_DISP_CD",
        when(
            (col("ADJ_VOID_FLAG").isNull()) | (length(trim(col("ADJ_VOID_FLAG"))) == 0),
            lit("NULL")
        ).otherwise(col("ADJ_VOID_FLAG"))
    )
    .withColumn("CLM_LN_LOB_CD", lit("NA"))
    .withColumn("CLM_LN_POS_CD", lit("NA"))
    .withColumn("CLM_LN_PREAUTH_CD", lit("NA"))
    .withColumn("CLM_LN_PREAUTH_SRC_CD", lit("NA"))
    .withColumn("CLM_LN_PRICE_SRC_CD", lit("NA"))
    .withColumn("CLM_LN_RFRL_CD", lit("NA"))
    .withColumn("CLM_LN_RVNU_CD", lit("NA"))
    .withColumn("CLM_LN_ROOM_PRICE_METH_CD", rpad(lit("NA"), 2, " "))
    .withColumn("CLM_LN_ROOM_TYP_CD", lit("NA"))
    .withColumn("CLM_LN_TOS_CD", lit("NA"))
    .withColumn("CLM_LN_UNIT_TYP_CD", lit("NA"))
    .withColumn("CAP_LN_IN", rpad(lit("N"), 1, " "))
    .withColumn("PRI_LOB_IN", rpad(lit("N"), 1, " "))
    .withColumn(
        "SVC_END_DT",
        when(
            (col("DT_OF_SVC").isNull()) | (length(col("DT_OF_SVC")) == 0),
            lit("2199-12-31")
        ).otherwise(
            concat(
                substring(col("DT_OF_SVC"), 1, 4), lit("-"),
                substring(col("DT_OF_SVC"), 5, 2), lit("-"),
                substring(col("DT_OF_SVC"), 7, 2)
            )
        )
    )
    .withColumn(
        "SVC_STRT_DT",
        when(
            (col("DT_OF_SVC").isNull()) | (length(col("DT_OF_SVC")) == 0),
            lit("2199-12-31")
        ).otherwise(
            concat(
                substring(col("DT_OF_SVC"), 1, 4), lit("-"),
                substring(col("DT_OF_SVC"), 5, 2), lit("-"),
                substring(col("DT_OF_SVC"), 7, 2)
            )
        )
    )
    .withColumn("AGMNT_PRICE_AMT", lit("0.00"))
    .withColumn("ALW_AMT", col("COV_AMT"))
    .withColumn(
        "CHRG_AMT",
        when(
            (col("RTL_AMT").isNull()) | (length(col("RTL_AMT")) == 0),
            lit("0.00")
        ).otherwise(col("RTL_AMT"))
    )
    .withColumn("COINS_AMT", lit("0.00"))
    .withColumn("CNSD_CHRG_AMT", lit("0.00"))
    .withColumn("COPAY_AMT", col("COPAY_AMT"))
    .withColumn("DEDCT_AMT", lit("0.00"))
    .withColumn("DSALW_AMT", col("3RD_PARTY_DSCNT"))
    .withColumn("ITS_HOME_DSCNT_AMT", lit(0))
    .withColumn("NO_RESP_AMT", lit("0.00"))
    .withColumn("MBR_LIAB_BSS_AMT", lit("0.00"))
    .withColumn("PATN_RESP_AMT", col("MBR_OOP"))
    .withColumn("PAYBL_AMT", lit("0.00"))
    .withColumn("PAYBL_TO_PROV_AMT", lit("0.00"))
    .withColumn("PAYBL_TO_SUB_AMT", lit("0.00"))
    .withColumn("PROC_TBL_PRICE_AMT", lit("0.00"))
    .withColumn("PROFL_PRICE_AMT", lit("0.00"))
    .withColumn("PROV_WRT_OFF_AMT", lit("0.00"))
    .withColumn("RISK_WTHLD_AMT", lit("0.00"))
    .withColumn("SVC_PRICE_AMT", lit("0.00"))
    .withColumn("SUPLMT_DSCNT_AMT", lit("0.00"))
    .withColumn("ALW_PRICE_UNIT_CT", lit(0))
    .withColumn("UNIT_CT", col("UNIT_OF_SVC"))
    .withColumn("DEDCT_AMT_ACCUM_ID", rpad(lit("NA"), 4, " "))
    .withColumn("PREAUTH_SVC_SEQ_NO", rpad(lit("NA"), 4, " "))
    .withColumn("RFRL_SVC_SEQ_NO", rpad(lit("NA"), 4, " "))
    .withColumn("LMT_PFX_ID", rpad(lit("NA"), 4, " "))
    .withColumn("PREAUTH_ID", rpad(lit("NA"), 9, " "))
    .withColumn("PROD_CMPNT_DEDCT_PFX_ID", rpad(lit("NA"), 4, " "))
    .withColumn("PROD_CMPNT_SVC_PAYMT_ID", rpad(lit("NA"), 4, " "))
    .withColumn("RFRL_ID_TX", rpad(lit("NA"), 9, " "))
    .withColumn("SVC_ID", rpad(lit("NA"), 4, " "))
    .withColumn("SVC_PRICE_RULE_ID", rpad(lit("NA"), 4, " "))
    .withColumn("SVC_RULE_TYP_TX", rpad(lit("NA"), 3, " "))
    .withColumn("SVC_LOC_TYP_CD", rpad(lit("NA"), 20, " "))
    .withColumn("NON_PAR_SAV_AMT", lit("0.00"))
    .withColumn("PROC_CD_TYP_CD", col("svProcCdTypCd"))
    .withColumn("PROC_CD_CAT_CD", col("svProcCdCatCd"))
    .withColumn("APC_ID", lit(None))
    .withColumn("APC_STTUS_ID", lit(None))
    .withColumn("SNOMED_CT_CD", lit("NA"))
    .withColumn("CVX_VCCN_CD", lit("NA"))
)

# --------------------------------------------------------------------------------
# PROC_CD (DB2Connector) - Read from DB
# --------------------------------------------------------------------------------
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
df_PROC_CD = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", f"SELECT PROC_CD,\n PROC_CD_TYP_CD,\n PROC_CD_CAT_CD\nFROM {IDSOwner}.PROC_CD")
    .load()
)

# --------------------------------------------------------------------------------
# hf_ClmLn_ProcCdTypCd (CHashedFileStage) - Scenario A deduplicate
# --------------------------------------------------------------------------------
df_hf_ClmLn_ProcCdTypCd = dedup_sort(
    df_PROC_CD,
    partition_cols=["PROC_CD", "PROC_CD_CAT_CD"],
    sort_cols=[]
)

# --------------------------------------------------------------------------------
# Snapshot (CTransformerStage) - Joins to df_hf_ClmLn_ProcCdTypCd (twice) and sets columns
# --------------------------------------------------------------------------------
df_Snapshot_join_ProcCd1 = df_BusinessRules.alias("Transform").join(
    df_hf_ClmLn_ProcCdTypCd.alias("ProcCd1"),
    (
        (col("Transform.PROC_CD") == col("ProcCd1.PROC_CD"))
        & (col("Transform.PROC_CD_CAT_CD") == col("ProcCd1.PROC_CD_CAT_CD"))
    ),
    how="left"
)

df_Snapshot_join_ProcCd2 = df_Snapshot_join_ProcCd1.alias("Temp").join(
    df_hf_ClmLn_ProcCdTypCd.alias("ProcCd2"),
    (
        (col("Temp.PROC_CD") == col("ProcCd2.PROC_CD"))
        & (col("Temp.PROC_CD_CAT_CD") == col("ProcCd2.PROC_CD_CAT_CD"))
    ),
    how="left"
)

df_Snapshot_vars = (
    df_Snapshot_join_ProcCd2
    .withColumn(
        "svProcCd",
        when(
            (col("ProcCd1.PROC_CD").isNotNull()) & (length(trim(col("ProcCd1.PROC_CD"))) != 0),
            col("ProcCd1.PROC_CD")
        ).otherwise(
            when(
                (col("ProcCd2.PROC_CD").isNotNull()) & (length(trim(col("ProcCd2.PROC_CD"))) != 0),
                col("ProcCd2.PROC_CD")
            ).otherwise(lit("UNK"))
        )
    )
    .withColumn(
        "svProcCdTypCd",
        when(
            (col("ProcCd1.PROC_CD").isNotNull()) & (length(trim(col("ProcCd1.PROC_CD"))) != 0),
            col("ProcCd1.PROC_CD_TYP_CD")
        ).otherwise(
            when(
                (col("ProcCd2.PROC_CD").isNotNull()) & (length(trim(col("ProcCd2.PROC_CD"))) != 0),
                col("ProcCd2.PROC_CD_TYP_CD")
            ).otherwise(lit("UNK"))
        )
    )
)

df_PKey = df_Snapshot_vars.select(
    col("Transform.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    col("Transform.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    col("Transform.DISCARD_IN").alias("DISCARD_IN"),
    col("Transform.PASS_THRU_IN").alias("PASS_THRU_IN"),
    col("Transform.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    col("Transform.ERR_CT").alias("ERR_CT"),
    col("Transform.RECYCLE_CT").alias("RECYCLE_CT"),
    col("Transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("Transform.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    col("Transform.CLM_LN_SK").alias("CLM_LN_SK"),
    col("Transform.CLM_ID").alias("CLM_ID"),
    col("Transform.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    col("Transform.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("Transform.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("svProcCd").alias("PROC_CD"),
    col("Transform.SVC_PROV_ID").alias("SVC_PROV_ID"),
    col("Transform.CLM_LN_DSALW_EXCD").alias("CLM_LN_DSALW_EXCD"),
    col("Transform.CLM_LN_EOB_EXCD").alias("CLM_LN_EOB_EXCD"),
    col("Transform.CLM_LN_FINL_DISP_CD").alias("CLM_LN_FINL_DISP_CD"),
    col("Transform.CLM_LN_LOB_CD").alias("CLM_LN_LOB_CD"),
    col("Transform.CLM_LN_POS_CD").alias("CLM_LN_POS_CD"),
    col("Transform.CLM_LN_PREAUTH_CD").alias("CLM_LN_PREAUTH_CD"),
    col("Transform.CLM_LN_PREAUTH_SRC_CD").alias("CLM_LN_PREAUTH_SRC_CD"),
    col("Transform.CLM_LN_PRICE_SRC_CD").alias("CLM_LN_PRICE_SRC_CD"),
    col("Transform.CLM_LN_RFRL_CD").alias("CLM_LN_RFRL_CD"),
    col("Transform.CLM_LN_RVNU_CD").alias("CLM_LN_RVNU_CD"),
    col("Transform.CLM_LN_ROOM_PRICE_METH_CD").alias("CLM_LN_ROOM_PRICE_METH_CD"),
    col("Transform.CLM_LN_ROOM_TYP_CD").alias("CLM_LN_ROOM_TYP_CD"),
    col("Transform.CLM_LN_TOS_CD").alias("CLM_LN_TOS_CD"),
    col("Transform.CLM_LN_UNIT_TYP_CD").alias("CLM_LN_UNIT_TYP_CD"),
    col("Transform.CAP_LN_IN").alias("CAP_LN_IN"),
    col("Transform.PRI_LOB_IN").alias("PRI_LOB_IN"),
    col("Transform.SVC_END_DT").alias("SVC_END_DT"),
    col("Transform.SVC_STRT_DT").alias("SVC_STRT_DT"),
    col("Transform.AGMNT_PRICE_AMT").alias("AGMNT_PRICE_AMT"),
    col("Transform.ALW_AMT").alias("ALW_AMT"),
    col("Transform.CHRG_AMT").alias("CHRG_AMT"),
    col("Transform.COINS_AMT").alias("COINS_AMT"),
    col("Transform.CNSD_CHRG_AMT").alias("CNSD_CHRG_AMT"),
    col("Transform.COPAY_AMT").alias("COPAY_AMT"),
    col("Transform.DEDCT_AMT").alias("DEDCT_AMT"),
    col("Transform.DSALW_AMT").alias("DSALW_AMT"),
    col("Transform.ITS_HOME_DSCNT_AMT").alias("ITS_HOME_DSCNT_AMT"),
    col("Transform.NO_RESP_AMT").alias("NO_RESP_AMT"),
    col("Transform.MBR_LIAB_BSS_AMT").alias("MBR_LIAB_BSS_AMT"),
    col("Transform.PATN_RESP_AMT").alias("PATN_RESP_AMT"),
    col("Transform.PAYBL_AMT").alias("PAYBL_AMT"),
    col("Transform.PAYBL_TO_PROV_AMT").alias("PAYBL_TO_PROV_AMT"),
    col("Transform.PAYBL_TO_SUB_AMT").alias("PAYBL_TO_SUB_AMT"),
    col("Transform.PROC_TBL_PRICE_AMT").alias("PROC_TBL_PRICE_AMT"),
    col("Transform.PROFL_PRICE_AMT").alias("PROFL_PRICE_AMT"),
    col("Transform.PROV_WRT_OFF_AMT").alias("PROV_WRT_OFF_AMT"),
    col("Transform.RISK_WTHLD_AMT").alias("RISK_WTHLD_AMT"),
    col("Transform.SVC_PRICE_AMT").alias("SVC_PRICE_AMT"),
    col("Transform.SUPLMT_DSCNT_AMT").alias("SUPLMT_DSCNT_AMT"),
    col("Transform.ALW_PRICE_UNIT_CT").alias("ALW_PRICE_UNIT_CT"),
    col("Transform.UNIT_CT").alias("UNIT_CT"),
    col("Transform.DEDCT_AMT_ACCUM_ID").alias("DEDCT_AMT_ACCUM_ID"),
    col("Transform.PREAUTH_SVC_SEQ_NO").alias("PREAUTH_SVC_SEQ_NO"),
    col("Transform.RFRL_SVC_SEQ_NO").alias("RFRL_SVC_SEQ_NO"),
    col("Transform.LMT_PFX_ID").alias("LMT_PFX_ID"),
    col("Transform.PREAUTH_ID").alias("PREAUTH_ID"),
    col("Transform.PROD_CMPNT_DEDCT_PFX_ID").alias("PROD_CMPNT_DEDCT_PFX_ID"),
    col("Transform.PROD_CMPNT_SVC_PAYMT_ID").alias("PROD_CMPNT_SVC_PAYMT_ID"),
    col("Transform.RFRL_ID_TX").alias("RFRL_ID_TX"),
    col("Transform.SVC_ID").alias("SVC_ID"),
    col("Transform.SVC_PRICE_RULE_ID").alias("SVC_PRICE_RULE_ID"),
    col("Transform.SVC_RULE_TYP_TX").alias("SVC_RULE_TYP_TX"),
    col("Transform.SVC_LOC_TYP_CD").alias("SVC_LOC_TYP_CD"),
    col("Transform.NON_PAR_SAV_AMT").alias("NON_PAR_SAV_AMT"),
    col("svProcCdTypCd").alias("PROC_CD_TYP_CD"),
    col("Transform.PROC_CD_CAT_CD").alias("PROC_CD_CAT_CD"),
    lit("NA").alias("VBB_RULE_ID"),
    lit("NA").alias("VBB_EXCD_ID"),
    rpad(lit("N"), 1, " ").alias("CLM_LN_VBB_IN"),
    lit("0.00").alias("ITS_SUPLMT_DSCNT_AMT"),
    lit("0.00").alias("ITS_SRCHRG_AMT"),
    lit("NA").alias("NDC"),
    lit("NA").alias("NDC_DRUG_FORM_CD"),
    lit(None).alias("NDC_UNIT_CT"),
    rpad(lit("VSN"), 3, " ").alias("MED_PDX_IND"),
    col("Transform.APC_ID").alias("APC_ID"),
    col("Transform.APC_STTUS_ID").alias("APC_STTUS_ID"),
    col("Transform.SNOMED_CT_CD").alias("SNOMED_CT_CD"),
    col("Transform.CVX_VCCN_CD").alias("CVX_VCCN_CD")
)

df_SnapshotOut = df_Snapshot_vars.select(
    col("Transform.CLM_ID").alias("CLCL_ID"),
    col("Transform.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    col("svProcCd").alias("PROC_CD"),
    col("Transform.CLM_LN_RVNU_CD").alias("CLM_LN_RVNU_CD"),
    col("Transform.ALW_AMT").alias("ALW_AMT"),
    col("Transform.CHRG_AMT").alias("CHRG_AMT"),
    col("Transform.PAYBL_AMT").alias("PAYBL_AMT"),
    col("svProcCdTypCd").alias("PROC_CD_TYP_CD"),
    col("Transform.PROC_CD_CAT_CD").alias("PROC_CD_CAT_CD")
)

# --------------------------------------------------------------------------------
# Transformer (CTransformerStage)
# --------------------------------------------------------------------------------
df_Transformer_intermediate = (
    df_SnapshotOut
    .withColumn("ProcCdSk", GetFkeyProcCd(
        lit("EYEMED"),
        lit(0),
        substring(col("PROC_CD"), 1, 5),
        col("PROC_CD_TYP_CD"),
        col("PROC_CD_CAT_CD"),
        lit("N")
    ))
    .withColumn("ClmLnRvnuCdSk", GetFkeyRvnu(
        lit("EYEMED"),
        lit(0),
        col("CLM_LN_RVNU_CD"),
        lit("N")
    ))
)

df_Transformer = df_Transformer_intermediate.select(
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    col("CLCL_ID").alias("CLM_ID"),
    col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    col("ProcCdSk").alias("PROC_CD_SK"),
    col("ClmLnRvnuCdSk").alias("CLM_LN_RVNU_CD_SK"),
    col("ALW_AMT").alias("ALW_AMT"),
    col("CHRG_AMT").alias("CHRG_AMT"),
    col("PAYBL_AMT").alias("PAYBL_AMT")
)

# --------------------------------------------------------------------------------
# B_CLM_LN (CSeqFileStage) - Write to file
# --------------------------------------------------------------------------------
df_B_CLM_LN_reorder = df_Transformer.select(
    rpad(col("SRC_SYS_CD_SK").cast(StringType()), 0, " ").alias("SRC_SYS_CD_SK"),
    rpad(col("CLM_ID"), 0, " ").alias("CLM_ID"),
    rpad(col("CLM_LN_SEQ_NO"), 0, " ").alias("CLM_LN_SEQ_NO"),
    rpad(col("PROC_CD_SK").cast(StringType()), 0, " ").alias("PROC_CD_SK"),
    rpad(col("CLM_LN_RVNU_CD_SK").cast(StringType()), 0, " ").alias("CLM_LN_RVNU_CD_SK"),
    rpad(col("ALW_AMT"), 0, " ").alias("ALW_AMT"),
    rpad(col("CHRG_AMT"), 0, " ").alias("CHRG_AMT"),
    rpad(col("PAYBL_AMT"), 0, " ").alias("PAYBL_AMT")
)

write_files(
    df_B_CLM_LN_reorder,
    f"{adls_path}/load/B_CLM_LN.{SrcSysCd}.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# --------------------------------------------------------------------------------
# ClmLnPK (CContainerStage) - Shared Container
# --------------------------------------------------------------------------------
params_ClmLnPK = {
    "CurrRunCycle": RunCycle
}
df_ClmLnPK = ClmLnPK(df_PKey, params_ClmLnPK)

# --------------------------------------------------------------------------------
# EyeMedClmLnExtr (CSeqFileStage) - Write final file
# --------------------------------------------------------------------------------
df_EyeMedClmLnExtr_reorder = df_ClmLnPK.select(
    [rpad(col(x.ColumnName), int(x.Length) if ("Length" in x and x.Length) else 0, " ").alias(x.ColumnName)
     if (("SqlType" in x and (x.SqlType == "char" or x.SqlType == "varchar")) and ("ColumnName" in x and x.ColumnName))
     else col(x.ColumnName).alias(x.ColumnName)
     for x in [
        # Reconstructing the output schema from JSON
        # Note: If length not specified for a char/varchar, using 0 in rpad
        # The JSON lists them below:
        StructField("JOB_EXCTN_RCRD_ERR_SK", StringType()),
        StructField("INSRT_UPDT_CD", StringType(), metadata={"Length":"10","SqlType":"char"}),
        StructField("DISCARD_IN", StringType(), metadata={"Length":"1","SqlType":"char"}),
        StructField("PASS_THRU_IN", StringType(), metadata={"Length":"1","SqlType":"char"}),
        StructField("FIRST_RECYC_DT", StringType()),
        StructField("ERR_CT", StringType()),
        StructField("RECYCLE_CT", StringType()),
        StructField("SRC_SYS_CD", StringType()),
        StructField("PRI_KEY_STRING", StringType()),
        StructField("CLM_LN_SK", StringType()),
        StructField("CLM_ID", StringType()),
        StructField("CLM_LN_SEQ_NO", StringType()),
        StructField("CRT_RUN_CYC_EXCTN_SK", StringType()),
        StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", StringType()),
        StructField("PROC_CD", StringType()),
        StructField("SVC_PROV_ID", StringType()),
        StructField("CLM_LN_DSALW_EXCD", StringType()),
        StructField("CLM_LN_EOB_EXCD", StringType()),
        StructField("CLM_LN_FINL_DISP_CD", StringType()),
        StructField("CLM_LN_LOB_CD", StringType()),
        StructField("CLM_LN_POS_CD", StringType()),
        StructField("CLM_LN_PREAUTH_CD", StringType()),
        StructField("CLM_LN_PREAUTH_SRC_CD", StringType()),
        StructField("CLM_LN_PRICE_SRC_CD", StringType()),
        StructField("CLM_LN_RFRL_CD", StringType()),
        StructField("CLM_LN_RVNU_CD", StringType()),
        StructField("CLM_LN_ROOM_PRICE_METH_CD", StringType(), metadata={"Length":"2","SqlType":"char"}),
        StructField("CLM_LN_ROOM_TYP_CD", StringType()),
        StructField("CLM_LN_TOS_CD", StringType()),
        StructField("CLM_LN_UNIT_TYP_CD", StringType()),
        StructField("CAP_LN_IN", StringType(), metadata={"Length":"1","SqlType":"char"}),
        StructField("PRI_LOB_IN", StringType(), metadata={"Length":"1","SqlType":"char"}),
        StructField("SVC_END_DT", StringType()),
        StructField("SVC_STRT_DT", StringType()),
        StructField("AGMNT_PRICE_AMT", StringType()),
        StructField("ALW_AMT", StringType()),
        StructField("CHRG_AMT", StringType()),
        StructField("COINS_AMT", StringType()),
        StructField("CNSD_CHRG_AMT", StringType()),
        StructField("COPAY_AMT", StringType()),
        StructField("DEDCT_AMT", StringType()),
        StructField("DSALW_AMT", StringType()),
        StructField("ITS_HOME_DSCNT_AMT", StringType()),
        StructField("NO_RESP_AMT", StringType()),
        StructField("MBR_LIAB_BSS_AMT", StringType()),
        StructField("PATN_RESP_AMT", StringType()),
        StructField("PAYBL_AMT", StringType()),
        StructField("PAYBL_TO_PROV_AMT", StringType()),
        StructField("PAYBL_TO_SUB_AMT", StringType()),
        StructField("PROC_TBL_PRICE_AMT", StringType()),
        StructField("PROFL_PRICE_AMT", StringType()),
        StructField("PROV_WRT_OFF_AMT", StringType()),
        StructField("RISK_WTHLD_AMT", StringType()),
        StructField("SVC_PRICE_AMT", StringType()),
        StructField("SUPLMT_DSCNT_AMT", StringType()),
        StructField("ALW_PRICE_UNIT_CT", StringType()),
        StructField("UNIT_CT", StringType()),
        StructField("DEDCT_AMT_ACCUM_ID", StringType(), metadata={"Length":"4","SqlType":"char"}),
        StructField("PREAUTH_SVC_SEQ_NO", StringType(), metadata={"Length":"4","SqlType":"char"}),
        StructField("RFRL_SVC_SEQ_NO", StringType(), metadata={"Length":"4","SqlType":"char"}),
        StructField("LMT_PFX_ID", StringType(), metadata={"Length":"4","SqlType":"char"}),
        StructField("PREAUTH_ID", StringType(), metadata={"Length":"9","SqlType":"char"}),
        StructField("PROD_CMPNT_DEDCT_PFX_ID", StringType(), metadata={"Length":"4","SqlType":"char"}),
        StructField("PROD_CMPNT_SVC_PAYMT_ID", StringType(), metadata={"Length":"4","SqlType":"char"}),
        StructField("RFRL_ID_TX", StringType(), metadata={"Length":"9","SqlType":"char"}),
        StructField("SVC_ID", StringType(), metadata={"Length":"4","SqlType":"char"}),
        StructField("SVC_PRICE_RULE_ID", StringType(), metadata={"Length":"4","SqlType":"char"}),
        StructField("SVC_RULE_TYP_TX", StringType(), metadata={"Length":"3","SqlType":"char"}),
        StructField("SVC_LOC_TYP_CD", StringType(), metadata={"Length":"20","SqlType":"char"}),
        StructField("NON_PAR_SAV_AMT", StringType()),
        StructField("PROC_CD_TYP_CD", StringType()),
        StructField("PROC_CD_CAT_CD", StringType()),
        StructField("VBB_RULE_ID", StringType()),
        StructField("VBB_EXCD_ID", StringType()),
        StructField("CLM_LN_VBB_IN", StringType(), metadata={"Length":"1","SqlType":"char"}),
        StructField("ITS_SUPLMT_DSCNT_AMT", StringType()),
        StructField("ITS_SRCHRG_AMT", StringType()),
        StructField("NDC", StringType()),
        StructField("NDC_DRUG_FORM_CD", StringType()),
        StructField("NDC_UNIT_CT", StringType()),
        StructField("MED_PDX_IND", StringType(), metadata={"Length":"3","SqlType":"char"}),
        StructField("APC_ID", StringType()),
        StructField("APC_STTUS_ID", StringType()),
        StructField("SNOMED_CT_CD", StringType()),
        StructField("CVX_VCCN_CD", StringType())
     ]
    ]
)

write_files(
    df_EyeMedClmLnExtr_reorder,
    f"{adls_path}/key/EyeMedClmLnExtr.EyeMedClmLn.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)