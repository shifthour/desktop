# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_3 11/12/08 10:10:08 Batch  14927_36617 PROMOTE bckcetl ids20 dsadm rc for brent 
# MAGIC ^1_3 11/12/08 10:04:14 Batch  14927_36261 INIT bckcett testIDS dsadm rc for brent 
# MAGIC ^1_1 11/06/08 08:57:33 Batch  14921_32258 PROMOTE bckcett testIDS u03651 steph for Brent
# MAGIC ^1_1 11/06/08 08:52:23 Batch  14921_31946 INIT bckcett devlIDS u03651 steffy
# MAGIC ^1_2 07/24/08 10:56:42 Batch  14816_39411 PROMOTE bckcetl ids20 dsadm rc for brent 
# MAGIC ^1_2 07/24/08 10:41:03 Batch  14816_38477 INIT bckcett testIDS dsadm rc fro brent 
# MAGIC ^1_2 07/22/08 08:32:54 Batch  14814_30778 PROMOTE bckcett testIDS u08717 Brent
# MAGIC ^1_2 07/22/08 08:21:54 Batch  14814_30118 INIT bckcett devlIDS u08717 Brent
# MAGIC ^1_1 07/07/08 11:47:11 Batch  14799_42473 INIT bckcett devlIDS u11141 has
# MAGIC ^1_1 10/24/07 09:59:24 Batch  14542_35967 PROMOTE bckcetl ids20 dsadm bls for on
# MAGIC ^1_1 10/24/07 09:54:15 Batch  14542_35658 INIT bckcett testIDS30 dsadm bls for on
# MAGIC ^1_2 10/17/07 13:59:14 Batch  14535_50357 PROMOTE bckcett testIDS30 u03651 steffy
# MAGIC ^1_2 10/17/07 13:54:06 Batch  14535_50077 INIT bckcett devlIDS30 u03651 steffy
# MAGIC ^1_1 10/16/07 09:44:13 Batch  14534_35065 INIT bckcett devlIDS30 u03651 steffy
# MAGIC ^1_8 09/28/05 09:15:12 Batch  13786_33322 INIT bckcett devlIDS30 u06640 Ralph
# MAGIC ^1_7 09/28/05 08:42:15 Batch  13786_31339 INIT bckcett devlIDS30 u06640 Ralph
# MAGIC ^1_2 07/29/05 14:39:17 Batch  13725_52764 INIT bckcetl ids20 dsadm Brent
# MAGIC ^1_1 07/29/05 13:02:36 Batch  13725_46962 INIT bckcetl ids20 dsadm Brent
# MAGIC ^1_1 07/20/05 07:35:13 Batch  13716_27318 PROMOTE bckcetl ids20 dsadm Gina Parr
# MAGIC ^1_1 07/20/05 07:29:40 Batch  13716_26986 INIT bckcett testIDS30 dsadm Gina Parr
# MAGIC ^1_6 06/22/05 09:10:14 Batch  13688_33020 PROMOTE bckcett VERSION u06640 Ralph
# MAGIC ^1_6 06/22/05 09:08:48 Batch  13688_32937 INIT bckcett devlIDS30 u06640 Ralph
# MAGIC ^1_5 06/20/05 13:59:07 Batch  13686_50351 INIT bckcett devlIDS30 u06640 Ralph
# MAGIC ^1_4 06/17/05 10:29:55 Batch  13683_37806 INIT bckcett devlIDS30 u06640 Ralph
# MAGIC ^1_3 06/17/05 10:28:29 Batch  13683_37715 INIT bckcett devlIDS30 u06640 Ralph
# MAGIC ^1_2 06/15/05 08:54:19 Batch  13681_32063 INIT bckcett devlIDS30 u06640 Ralph
# MAGIC ^1_1 06/09/05 13:18:55 Batch  13675_47941 INIT bckcett devlIDS30 u06640 Ralph
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2005, 2006, 2007, 2008, 2022 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC PROCESSING:   Pulls data from CMC_CRFD_FUND_DEFN for loading into IDS.
# MAGIC       
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer 	Date   		Project/Altiris #		Change Description					Development Project		Code Reviewer	Date Reviewed       
# MAGIC =========================================================================================================================================================================================
# MAGIC Parikshith Chada	4/20/2007          	3264                             		Added Balancing process to the overall                 		devlIDS30                		Steph Goddard    	09/19/2007
# MAGIC                                                                                                       			job that takes a snapshot of the source data                
# MAGIC Brent Leland      	06-26-2008     	3567 Primary Key            	Added output for paymnt sum foriegn key lookup   		devlIDS                        		Steph Goddard     	07/03/2008
# MAGIC                                                                                                       			used in IdsProvCapFkey job.              
# MAGIC Parik                    	2008-09-08      	3567 Primary Key            	added the new primary key process to the job       		devlIDS                         		Steph Goddard    	09/10/2008
# MAGIC Ralph Tucker        	2011-06-02      	TTR-1058                       	Changed default for PAYMT_SUM_CD                 		IntegrateCurDevl           	SAnderw                   	2011-06-13 
# MAGIC Prabhu ES           	2022-02-25       	S2S Remediation            	MSSQL connection parameters added                 		IntegrateDev5		Ken Bradmon	2022-06-07

# MAGIC Remove Carriage Return, Line Feed, and  Tab in fields
# MAGIC Extract Provider Capitation Data
# MAGIC Writing Sequential File to /key
# MAGIC Apply Business Logic to common record format
# MAGIC Hash file (hf_prov_cap_allcol) cleared in calling program
# MAGIC This container is used in:
# MAGIC FctsProvCapExtr
# MAGIC 
# MAGIC These programs need to be re-compiled when logic changes
# MAGIC Balancing snapshot of source table
# MAGIC Used in IdsProvCapFkey to foreign key payment summary value
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType, DecimalType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# Retrieve parameter values
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
RunID = get_widget_value('RunID','')
CurrDate = get_widget_value('CurrDate','')
SrcSysCd = get_widget_value('SrcSysCd','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')

# MAGIC %run ../../../../shared_containers/PrimaryKey/ProvCapPK
# COMMAND ----------

# Stage: CMC_CRPR_PROV_SUM (ODBCConnector)
jdbc_url_CMC_CRPR_PROV_SUM, jdbc_props_CMC_CRPR_PROV_SUM = get_db_config(facets_secret_name)
sql_stmt_CMC_CRPR_PROV_SUM = f"""
SELECT 
  CRME_PAY_THRU_DT,
  LOBD_ID,
  CRME_PAYEE_PR_ID,
  CRME_CR_PR_TYPE,
  CRME_PYMT_METH_IND,
  CRME_CR_PR_ID,
  CRPR_CURR_AMT,
  CRPR_AUTO_ADJ_AMT,
  CRPR_MAN_ADJ_AMT,
  CRPR_NET_AMT,
  CRPR_CURR_ME_M,
  CRPR_AUTO_ADJ_ME_M,
  CRPR_MAN_ADJ_ME_M,
  CKPY_REF_ID
FROM {FacetsOwner}.CMC_CRPR_PROV_SUM
WHERE CRME_PAY_THRU_DT >= '2000-01-01'
"""
df_CMC_CRPR_PROV_SUM = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_CMC_CRPR_PROV_SUM)
    .options(**jdbc_props_CMC_CRPR_PROV_SUM)
    .option("query", sql_stmt_CMC_CRPR_PROV_SUM)
    .load()
)

# Stage: TrnsStripField (CTransformerStage)
df_TrnsStripField = df_CMC_CRPR_PROV_SUM.select(
    F.col("CRME_CR_PR_ID").alias("CRME_CR_PR_ID"),
    F.col("CRME_PAY_THRU_DT").alias("CRME_PAY_THRU_DT"),
    F.col("LOBD_ID").alias("LOBD_ID"),
    F.col("CRME_PAYEE_PR_ID").alias("CRME_PAYEE_PR_ID"),
    F.col("CRME_CR_PR_TYPE").alias("CRME_CR_PR_TYPE"),
    F.col("CRME_PYMT_METH_IND").alias("CRME_PYMT_METH_IND"),
    F.col("CRPR_CURR_AMT").alias("CRPR_CURR_AMT"),
    F.col("CRPR_AUTO_ADJ_AMT").alias("CRPR_AUTO_ADJ_AMT"),
    F.col("CRPR_MAN_ADJ_AMT").alias("CRPR_MAN_ADJ_AMT"),
    F.col("CRPR_NET_AMT").alias("CRPR_NET_AMT"),
    F.col("CRPR_CURR_ME_M").alias("CRPR_CURR_ME_M"),
    F.col("CRPR_AUTO_ADJ_ME_M").alias("CRPR_AUTO_ADJ_ME_M"),
    F.col("CRPR_MAN_ADJ_ME_M").alias("CRPR_MAN_ADJ_ME_M"),
    F.col("CKPY_REF_ID").alias("CKPY_REF_ID")
)

# Stage: TrnsCommRec (CTransformerStage)
# Stage variables
df_TrnsCommRec = df_TrnsStripField.withColumn("RowPassThru", F.lit("Y"))
df_TrnsCommRec = df_TrnsCommRec.withColumn(
    "PaidDt",
    F.date_format(F.col("CRME_PAY_THRU_DT"), "yyyy-MM-dd")
)

# Output link "AllCol"
df_AllCol = df_TrnsCommRec.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("PaidDt").alias("PD_DT_SK"),
    F.when(
        F.length(trim(strip_field(F.col("CRME_CR_PR_ID")))) == 0,
        F.lit("UNK")
    ).otherwise(trim(strip_field(F.col("CRME_CR_PR_ID")))).alias("CAP_PROV_ID"),
    F.when(
        F.length(trim(strip_field(F.col("CRME_PAYEE_PR_ID")))) == 0,
        F.lit("UNK")
    ).otherwise(trim(strip_field(F.col("CRME_PAYEE_PR_ID")))).alias("PD_PROV_ID"),
    trim(F.col("LOBD_ID")).alias("PROV_CAP_PAYMT_LOB_CD"),
    trim(F.col("CRME_PYMT_METH_IND")).alias("PROV_CAP_PAYMT_METH_CD"),
    trim(F.col("CRME_CR_PR_TYPE")).alias("PROV_CAP_PAYMT_CAP_TYP_CD"),
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.col("RowPassThru").alias("PASS_THRU_IN"),
    F.lit(CurrDate).alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit(SrcSysCd).alias("SRC_SYS_CD"),
    F.concat_ws(
        ";",
        F.lit(SrcSysCd),
        F.col("PaidDt"),
        trim(F.col("CRME_CR_PR_ID")),
        trim(F.col("CRME_PAYEE_PR_ID")),
        trim(F.col("LOBD_ID")),
        trim(F.col("CRME_PYMT_METH_IND")),
        trim(F.col("CRME_CR_PR_TYPE"))
    ).alias("PRI_KEY_STRING"),
    F.lit(0).alias("PROV_CAP_SK"),
    F.lit(0).alias("CRT_RUN_CYC_EXTCN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXTCN_SK"),
    strip_field(F.col("CKPY_REF_ID")).alias("PAYMT_SUM_CD"),
    F.col("CRPR_AUTO_ADJ_AMT").alias("AUTO_ADJ_AMT"),
    F.col("CRPR_CURR_AMT").alias("CUR_CAP_AMT"),
    F.col("CRPR_MAN_ADJ_AMT").alias("MAN_ADJ_AMT"),
    F.col("CRPR_NET_AMT").alias("NET_AMT"),
    F.col("CRPR_AUTO_ADJ_ME_M").alias("AUTO_ADJ_MBR_MO_CT"),
    F.col("CRPR_CURR_ME_M").alias("CUR_MBR_MO_CT"),
    F.col("CRPR_MAN_ADJ_ME_M").alias("MNL_ADJ_MBR_MO_CT")
)

# Output link "Transform"
df_Transform = df_TrnsCommRec.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("PaidDt").alias("PD_DT_SK"),
    F.when(
        F.length(trim(strip_field(F.col("CRME_CR_PR_ID")))) == 0,
        F.lit("UNK")
    ).otherwise(trim(strip_field(F.col("CRME_CR_PR_ID")))).alias("CAP_PROV_ID"),
    F.when(
        F.length(trim(strip_field(F.col("CRME_PAYEE_PR_ID")))) == 0,
        F.lit("UNK")
    ).otherwise(trim(strip_field(F.col("CRME_PAYEE_PR_ID")))).alias("PD_PROV_ID"),
    trim(F.col("LOBD_ID")).alias("PROV_CAP_PAYMT_LOB_CD"),
    trim(F.col("CRME_PYMT_METH_IND")).alias("PROV_CAP_PAYMT_METH_CD"),
    trim(F.col("CRME_CR_PR_TYPE")).alias("PROV_CAP_PAYMT_CAP_TYP_CD")
)

# Stage: ProvCapPK (CContainerStage)
params_ProvCapPK = {
    "IDSOwner": IDSOwner,
    "CurrRunCycle": CurrRunCycle,
    "SrcSysCd": SrcSysCd,
    "SrcSysCdSk": SrcSysCdSk
}
df_ProvCapPK_lnkOut, df_ProvCapPK_PaymntSum = ProvCapPK(df_AllCol, df_Transform, params_ProvCapPK)

# Stage: IdsProvCapPkey (CSeqFileStage)
df_IdsProvCapPkey = df_ProvCapPK_lnkOut.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "PROV_CAP_SK",
    rpad(F.col("PD_DT_SK"), 10, " ").alias("PD_DT_SK"),
    "CAP_PROV_ID",
    "PD_PROV_ID",
    "PROV_CAP_PAYMT_LOB_CD",
    "PROV_CAP_PAYMT_METH_CD",
    "PROV_CAP_PAYMT_CAP_TYP_CD",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    rpad(F.col("PAYMT_SUM_CD"), 16, " ").alias("PAYMT_SUM_CD"),
    "AUTO_ADJ_AMT",
    "CUR_CAP_AMT",
    "MAN_ADJ_AMT",
    "NET_AMT",
    "AUTO_ADJ_MBR_MO_CT",
    "CUR_MBR_MO_CT",
    "MNL_ADJ_MBR_MO_CT"
)
write_files(
    df_IdsProvCapPkey,
    f"{adls_path}/key/FctsProvCapExtr.IdsProvCap.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# Stage: W_PAYMT_SUM (CSeqFileStage)
df_W_PAYMT_SUM = df_ProvCapPK_PaymntSum.select(
    "SRC_SYS_CD_SK",
    rpad(F.col("PAYMT_SUM_CD"), 16, " ").alias("PAYMT_SUM_CD"),
    "PROV_CAP_PAYMT_LOB_CD"
)
write_files(
    df_W_PAYMT_SUM,
    f"{adls_path}/load/W_PAYMT_SUM.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# Stage: Facets_Source (ODBCConnector)
jdbc_url_Facets_Source, jdbc_props_Facets_Source = get_db_config(facets_secret_name)
sql_stmt_Facets_Source = f"""
SELECT
  CRME_PAY_THRU_DT,
  LOBD_ID,
  CRME_PAYEE_PR_ID,
  CRME_CR_PR_TYPE,
  CRME_PYMT_METH_IND,
  CRME_CR_PR_ID
FROM {FacetsOwner}.CMC_CRPR_PROV_SUM
WHERE CRME_PAY_THRU_DT >= '2000-01-01'
"""
df_Facets_Source = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_Facets_Source)
    .options(**jdbc_props_Facets_Source)
    .option("query", sql_stmt_Facets_Source)
    .load()
)

# Stage: Transform (CTransformerStage) for Facets_Source path
df_TransformSnapshot = df_Facets_Source
df_TransformSnapshot = df_TransformSnapshot.withColumn(
    "svProvCapLobCdSk",
    GetFkeyCodes("FACETS", 100, "CLAIM LINE LOB", trim(F.col("LOBD_ID")), "X")
).withColumn(
    "svProvCapPaymtMethCdSk",
    GetFkeyCodes("FACETS", 101, "CAPITATION FUND PAYMENT METHOD", trim(F.col("CRME_PYMT_METH_IND")), "X")
).withColumn(
    "svProvCapTypCdSk",
    GetFkeyCodes("FACETS", 102, "CAPITATION ADJUSTMENT TYPE", trim(F.col("CRME_CR_PR_TYPE")), "X")
)

df_Transform2 = df_TransformSnapshot.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.date_format(F.col("CRME_PAY_THRU_DT"), "yyyy-MM-dd").alias("PD_DT_SK"),
    F.when(
        F.length(trim(strip_field(F.col("CRME_CR_PR_ID")))) == 0,
        F.lit("UNK")
    ).otherwise(trim(strip_field(F.col("CRME_CR_PR_ID")))).alias("CAP_PROV_ID"),
    F.when(
        F.length(trim(strip_field(F.col("CRME_PAYEE_PR_ID")))) == 0,
        F.lit("UNK")
    ).otherwise(trim(strip_field(F.col("CRME_PAYEE_PR_ID")))).alias("PD_PROV_ID"),
    F.col("svProvCapLobCdSk").alias("PROV_CAP_PAYMT_LOB_CD_SK"),
    F.col("svProvCapPaymtMethCdSk").alias("PROV_CAP_PAYMT_METH_CD_SK"),
    F.col("svProvCapTypCdSk").alias("PROV_CAP_PAYMT_CAP_TYP_CD_SK")
)

# Stage: Snapshot_File (CSeqFileStage)
df_Snapshot_File = df_Transform2.select(
    "SRC_SYS_CD_SK",
    "PD_DT_SK",
    "CAP_PROV_ID",
    "PD_PROV_ID",
    "PROV_CAP_PAYMT_LOB_CD_SK",
    "PROV_CAP_PAYMT_METH_CD_SK",
    "PROV_CAP_PAYMT_CAP_TYP_CD_SK"
)
write_files(
    df_Snapshot_File,
    f"{adls_path}/load/B_PROV_CAP.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)