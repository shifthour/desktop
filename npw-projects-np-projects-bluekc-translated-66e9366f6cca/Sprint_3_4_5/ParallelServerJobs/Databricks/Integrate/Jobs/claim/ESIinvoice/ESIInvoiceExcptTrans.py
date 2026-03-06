# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_3 06/22/09 10:48:20 Batch  15149_38904 PROMOTE bckcetl:31540 ids20 dsadm bls for sg
# MAGIC ^1_3 06/22/09 10:45:28 Batch  15149_38733 INIT bckcett:31540 testIDS dsadm bls for sg
# MAGIC ^1_2 06/05/09 11:02:32 Batch  15132_39796 INIT bckcett:31540 testIDS u150906 TTR519_Maddy_testIDS      maddy
# MAGIC ^1_1 05/22/09 09:55:27 Batch  15118_35732 PROMOTE bckcett:31540 testIDS u03651 steph for Maddy
# MAGIC ^1_1 05/22/09 09:54:27 Batch  15118_35669 INIT bckcett:31540 devlIDS u03651 steffy
# MAGIC ^1_2 03/10/09 11:34:35 Batch  15045_41695 PROMOTE bckcett devlIDS u10157 sa - Bringing ALL Claim code down from production
# MAGIC ^1_2 03/10/09 09:25:21 Batch  15045_33943 INIT bckcetl ids20 dcg01 sa Bringing ALL Claim code down to devlIDS
# MAGIC ^1_2 02/10/09 11:16:45 Batch  15017_40611 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 02/02/09 09:52:49 Batch  15009_35578 PROMOTE bckcetl ids20 dsadm bls for brent
# MAGIC ^1_1 01/26/09 13:03:15 Batch  15002_46999 INIT bckcett testIDS dsadm bls for bl
# MAGIC ^1_1 12/16/08 10:00:18 Batch  14961_36031 PROMOTE bckcett testIDS u03651 steph for Brent - claim primary keying
# MAGIC ^1_1 12/16/08 09:15:37 Batch  14961_33343 INIT bckcett devlIDS u03651 steffy
# MAGIC 
# MAGIC 
# MAGIC COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC PROCESSING:   Set primary key for ESI exception table.
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                                    Date                        Change Description                                                                 Project #                  Development Project          Code Reviewer               Date Reviewed  
# MAGIC ------------------                          ----------------------------        ----------------------------------------------------------------------------------                    ----------------               ------------------------------------       ----------------------------           -------------
# MAGIC Brent Leland                            11-25-2008                  Original programming                                                                3567 Primary Key    devlIDS
# MAGIC                                                        
# MAGIC Madhav Myanmpati                  05-05-2009                 SQL Change and change in Logic                                            TTR-519                 devlIDS                                Steph Goddard                05/20/2009

# MAGIC Primary Key ESI Invoice Exception Records
# MAGIC Input file created in ESIIdsUpd
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


SrcSysCd = get_widget_value("SrcSysCd", "ESI")
RunID = get_widget_value("RunID", "100")
RunCycle = get_widget_value("RunCycle", "100")
CurrentDate = get_widget_value("CurrentDate", "")
IDSOwner = get_widget_value("IDSOwner", "")
ids_secret_name = get_widget_value("ids_secret_name", "")

schema_ESI_Invoice_Unmatched = StructType([
    StructField("ESI_INVC_EXCPT_SK", IntegerType(), False),
    StructField("SRC_SYS_CD_SK", IntegerType(), False),
    StructField("CLM_ID", StringType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("FNCL_LOB_CD", StringType(), False),
    StructField("GRP_ID", StringType(), False),
    StructField("MBR_UNIQ_KEY", IntegerType(), False),
    StructField("PROD_ID", StringType(), False),
    StructField("PD_DT_SK", StringType(), False),
    StructField("PRCS_DT_SK", StringType(), False),
    StructField("ACTL_PD_AMT", DecimalType(18, 2), False),
    StructField("DT_FILLED", StringType(), False)
])

df_ESI_Invoice_Unmatched = (
    spark.read.format("csv")
    .option("delimiter", ",")
    .option("quote", "\"")
    .option("header", "false")
    .schema(schema_ESI_Invoice_Unmatched)
    .load(f"{adls_path}/key/ESI_Invoice_Unmatched.dat.{RunID}")
)

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
extract_sql_ids = """SELECT 
MBR.MBR_UNIQ_KEY,
DRUG.FILL_DT_SK,
CLS.CLS_ID,
PLN.CLS_PLN_ID,
SUBGRP.SUBGRP_ID,
CAT.EXPRNC_CAT_CD,
LOB.FNCL_LOB_CD,
CMPNT.PROD_ID 
FROM #$IDSOwner#.W_DRUG_ENR DRUG,
#$IDSOwner#.MBR_ENR MBR, 
#$IDSOwner#.CD_MPPNG MAP1,
#$IDSOwner#.CLS CLS, 
#$IDSOwner#.SUBGRP SUBGRP,
#$IDSOwner#.CLS_PLN PLN, 
#$IDSOwner#.CD_MPPNG MAP2, 
#$IDSOwner#.PROD_CMPNT CMPNT,
#$IDSOwner#.PROD_BILL_CMPNT BILL_CMPNT,
#$IDSOwner#.EXPRNC_CAT CAT,
#$IDSOwner#.FNCL_LOB LOB 
WHERE 
DRUG.MBR_UNIQ_KEY = MBR.MBR_UNIQ_KEY
and DRUG.FILL_DT_SK between MBR.EFF_DT_SK and MBR.TERM_DT_SK
and MBR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK = MAP1.CD_MPPNG_SK
and MAP1.TRGT_CD = 'MED'
and MBR.CLS_SK = CLS.CLS_SK
and MBR.CLS_PLN_SK = PLN.CLS_PLN_SK
and MBR.SUBGRP_SK = SUBGRP.SUBGRP_SK
AND MBR.PROD_SK = CMPNT.PROD_SK
AND CMPNT.PROD_CMPNT_TYP_CD_SK = MAP2.CD_MPPNG_SK
AND MAP2.TRGT_CD= 'PDBL'
AND DRUG.FILL_DT_SK between CMPNT.PROD_CMPNT_EFF_DT_SK AND CMPNT.PROD_CMPNT_TERM_DT_SK
AND CMPNT.PROD_CMPNT_EFF_DT_SK= (SELECT MAX ( CMPNT2.PROD_CMPNT_EFF_DT_SK ) FROM  #$IDSOwner#.PROD_CMPNT   CMPNT2
                                 WHERE CMPNT.PROD_SK = CMPNT2.PROD_SK
                                 AND CMPNT.PROD_CMPNT_TYP_CD_SK =  CMPNT2.PROD_CMPNT_TYP_CD_SK
                                 AND DRUG.FILL_DT_SK  BETWEEN CMPNT2.PROD_CMPNT_EFF_DT_SK  AND  CMPNT2.PROD_CMPNT_TERM_DT_SK)
AND CMPNT.PROD_CMPNT_PFX_ID = BILL_CMPNT.PROD_CMPNT_PFX_ID
AND DRUG.FILL_DT_SK between BILL_CMPNT.PROD_BILL_CMPNT_EFF_DT_SK  AND BILL_CMPNT.PROD_BILL_CMPNT_TERM_DT_SK
AND BILL_CMPNT.PROD_BILL_CMPNT_ID IN ('MED', 'MED1')
AND BILL_CMPNT.PROD_BILL_CMPNT_EFF_DT_SK= (SELECT MAX ( BILL_CMPNT2.PROD_BILL_CMPNT_EFF_DT_SK ) FROM  #$IDSOwner#.PROD_BILL_CMPNT   BILL_CMPNT2
                                          WHERE BILL_CMPNT.PROD_CMPNT_PFX_ID = BILL_CMPNT2.PROD_CMPNT_PFX_ID
                                          AND CMPNT.PROD_CMPNT_PFX_ID = BILL_CMPNT2.PROD_CMPNT_PFX_ID
                                          AND BILL_CMPNT.PROD_BILL_CMPNT_ID = BILL_CMPNT2.PROD_BILL_CMPNT_ID
                                          AND BILL_CMPNT2.PROD_BILL_CMPNT_ID IN ('MED', 'MED1')
                                          AND DRUG.FILL_DT_SK  BETWEEN BILL_CMPNT2.PROD_BILL_CMPNT_EFF_DT_SK  AND BILL_CMPNT2.PROD_BILL_CMPNT_TERM_DT_SK)
AND BILL_CMPNT.EXPRNC_CAT_SK = CAT.EXPRNC_CAT_SK
AND BILL_CMPNT.FNCL_LOB_SK = LOB.FNCL_LOB_SK
#$IDSOwner#.W_DRUG_ENR DRUG,#$IDSOwner#.MBR_ENR MBR, #$IDSOwner#.CD_MPPNG NG1,#$IDSOwner#.CLS CLS, #$IDSOwner#.SUBGRP SUBGRP, #$IDSOwner#.CLS_PLN PLN, #$IDSOwner#.CD_MPPNG NG2, #$IDSOwner#.PROD PROD,#$IDSOwner#.EXPRNC_CAT CAT,#$IDSOwner#.FNCL_LOB LOB
"""
df_ids_raw = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_sql_ids)
    .load()
)

df_hf_esi_invoice_fncl_lob = df_ids_raw.withColumnRenamed("FILL_DT_SK","DT_FILLED")
df_hf_esi_invoice_fncl_lob = df_hf_esi_invoice_fncl_lob.select(
    F.col("MBR_UNIQ_KEY"),
    F.col("DT_FILLED"),
    F.col("CLS_ID"),
    F.col("CLS_PLN_ID"),
    F.col("SUBGRP_ID"),
    F.col("EXPRNC_CAT_CD"),
    F.col("FNCL_LOB_CD"),
    F.col("PROD_ID")
)
df_hf_esi_invoice_fncl_lob_dedup = dedup_sort(df_hf_esi_invoice_fncl_lob, ["MBR_UNIQ_KEY","DT_FILLED"], [])

extract_sql_esi_invc_ecpt = """SELECT SRC_SYS_CD_SK,CLM_ID,ESI_INVC_EXCPT_SK,CRT_RUN_CYC_EXCTN_SK FROM #$IDSOwner#.ESI_INVC_EXCPT
#$IDSOwner#.ESI_INVC_EXCPT
"""
df_esi_invc_ecpt_raw = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_sql_esi_invc_ecpt)
    .load()
)

df_hf_esi_invc_excpt_sk = df_esi_invc_ecpt_raw.select(
    F.col("SRC_SYS_CD_SK"),
    F.col("CLM_ID"),
    F.col("ESI_INVC_EXCPT_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK")
)
df_hf_esi_invc_excpt_sk_dedup = dedup_sort(df_hf_esi_invc_excpt_sk, ["SRC_SYS_CD_SK","CLM_ID"], [])

df_trns2_joined = (
    df_ESI_Invoice_Unmatched.alias("Input")
    .join(df_hf_esi_invoice_fncl_lob_dedup.alias("Fncl_LOB"), [(F.col("Input.MBR_UNIQ_KEY")==F.col("Fncl_LOB.MBR_UNIQ_KEY")), (F.col("Input.DT_FILLED")==F.col("Fncl_LOB.DT_FILLED"))], "left")
    .join(df_hf_esi_invc_excpt_sk_dedup.alias("Excpt_Sk"), [(F.col("Input.SRC_SYS_CD_SK")==F.col("Excpt_Sk.SRC_SYS_CD_SK")), (F.col("Input.CLM_ID")==F.col("Excpt_Sk.CLM_ID"))], "left")
)

df_trns2_stagevars = (
    df_trns2_joined
    .withColumn(
        "CRT_RUN_CYC_EXCTN_SK",
        F.when(F.col("Excpt_Sk.ESI_INVC_EXCPT_SK").isNull(), F.lit(RunCycle))
        .otherwise(F.col("Excpt_Sk.CRT_RUN_CYC_EXCTN_SK"))
    )
    .withColumn(
        "ESI_INVC_EXCPT_SK",
        F.col("Excpt_Sk.ESI_INVC_EXCPT_SK")
    )
    .withColumn(
        "FNCL_LOB_CD",
        F.when(F.col("Fncl_LOB.FNCL_LOB_CD").isNull(), F.lit("UNK"))
        .otherwise(F.col("Fncl_LOB.FNCL_LOB_CD"))
    )
    .withColumn(
        "PROD_ID",
        F.when(F.col("Input.PROD_ID")=="HMS00000", F.col("Fncl_LOB.PROD_ID"))
        .otherwise(F.col("Input.PROD_ID"))
    )
)

df_enriched = df_trns2_stagevars
df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"ESI_INVC_EXCPT_SK",<schema>,<secret_name>)

df_trns2_final = df_enriched.select(
    F.col("ESI_INVC_EXCPT_SK").alias("ESI_INVC_EXCPT_SK"),
    F.col("Input.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("Input.CLM_ID").alias("CLM_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("Input.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("FNCL_LOB_CD").alias("FNCL_LOB_CD"),
    F.col("Input.GRP_ID").alias("GRP_ID"),
    F.col("Input.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("PROD_ID").alias("PROD_ID"),
    F.col("Input.PD_DT_SK").alias("PD_DT_SK"),
    F.col("Input.PRCS_DT_SK").alias("PRCS_DT_SK"),
    F.col("Input.ACTL_PD_AMT").alias("ACTL_PD_AMT"),
    F.col("Input.DT_FILLED").alias("DT_FILLED")
)

df_trns2_final_char_adjusted = (
    df_trns2_final
    .withColumn("GRP_ID", F.rpad(F.col("GRP_ID"), 8, " "))
    .withColumn("PROD_ID", F.rpad(F.col("PROD_ID"), 18, " "))
    .withColumn("PD_DT_SK", F.rpad(F.col("PD_DT_SK"), 10, " "))
    .withColumn("PRCS_DT_SK", F.rpad(F.col("PRCS_DT_SK"), 10, " "))
    .withColumn("DT_FILLED", F.rpad(F.col("DT_FILLED"), 10, " "))
)

write_files(
    df_trns2_final_char_adjusted,
    f"{adls_path}/key/ESI_Invoice_trans.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)