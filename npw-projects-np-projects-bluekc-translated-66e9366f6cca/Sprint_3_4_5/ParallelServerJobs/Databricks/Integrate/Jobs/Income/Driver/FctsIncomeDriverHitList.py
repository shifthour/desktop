# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2006 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC JOB NAME:  FctsIncomeDriverHitList
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:  Read list of invoice  IDs from FctsInvoiceHitList.dat in ../update directory.  These invoices are added to the driver table for claim processing.
# MAGIC PROCESSING:  Invoice IDs in FctsIncomeHitList.dat are looked up against facets for validation of existance and then, if exist, written to the TMP_IDS_INCOME tmp table
# MAGIC                            If Invoice ID does not exist , then it is written to a sequential file which is later read and an email is sent out if invoices where not found.
# MAGIC            
# MAGIC   
# MAGIC MODIFICATIONS:
# MAGIC             Sharon Andrew  05/01/2006-   Originally Programmed
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #            Change Description                                                                                                                                               Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------------------------------------------------------------------------------------------                                   ----------------------------------   ---------------------------------    -------------------------   
# MAGIC SAndrew                         2009-07-29        TTR- 578                      changed location of hit list in FctsIncomePrereqSeq from /ids/update to /ids/***/update                                             devlIDS                  Steph Goddard            07/30/2009
# MAGIC 
# MAGIC SAndrew                         2014-04-01         Prod Outage                 removed HASH.CLEAR of  hf_hit_list_build_clm_ids
# MAGIC                                                                                                        Put in check to see if the Invoice already exists on the TmpTble as the extract program would have placed it there.
# MAGIC                                                                                                       we did this because this program is abending when the BLIV_ID was extracted for changed reasons AND it is on the Hit list.                                                                                                        
# MAGIC 					When the hit list tries to load it to the temp table, it crashes.
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC Goutham Kalidindi  2021-03-24 358186                                                 Changed Datatype length for field BLIV_ID                                                                                                                           IntegrateDev2    Kalyan Neelam   2021-03-31
# MAGIC                                                                                                                  char(12) to Varchar(15)
# MAGIC Prabhu ES                 2022-03-02            S2S Remediation                MSSQL ODBC conn added and other param changes                                                                                            IntegrateDev5	Ken Bradmon	2022-06-15

# MAGIC Load list of invoices from the hit list into the tmp_ids_income_drvr temp table.   All income extracts drive from this temp table.
# MAGIC Read Facets Billing Invoice IDs from file
# MAGIC Lookup claim in Facets for other information
# MAGIC If Hist LIst Invoice is not on CDS_INID_INVOICE, write out to file.   File is later checked for any records and an email will be mailed.
# MAGIC Insert records into driver table into BILL_INVC_ID, the only field on the table TMP_IDS_INCOME #RunId#
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType
import pyspark.sql.functions as psf
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


DriverTable = get_widget_value('DriverTable','')
FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
tempdb_secret_name = get_widget_value('tempdb_secret_name','')

jdbc_url_tempdb, jdbc_props_tempdb = get_db_config(tempdb_secret_name)
jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)

schema_FctsIncomeHitList = StructType([
    StructField("BILL_INVC_ID", StringType(), True)
])

df_Invoice = (
    spark.read
    .option("header", False)
    .option("quote", "\"")
    .schema(schema_FctsIncomeHitList)
    .csv(f"{adls_path}/update/FctsIncomeHitList.dat")
)

df_fctsTempTable = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_tempdb)
    .options(**jdbc_props_tempdb)
    .option("query", f"SELECT * FROM tempdb..{DriverTable}")
    .load()
)

df_CDS_INID_INVOICE = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", f"SELECT INVC.BLIV_ID AS BLIV_ID, INVC.BLBL_SPCL_BL_IND AS BLBL_SPCL_BL_IND FROM {FacetsOwner}.CDS_INID_INVOICE INVC WHERE INVC.BLBL_SPCL_BL_IND IN ('N','F','P','C')")
    .load()
)

df_InvcLkup = dedup_sort(
    df_CDS_INID_INVOICE,
    ["BLIV_ID"],
    [("BLIV_ID", "A")]
)

df_InvoicePrep = (
    df_Invoice.alias("Invoice")
    .withColumn("trimmed_BILL_INVC_ID", trim(psf.col("Invoice.BILL_INVC_ID")))
    .withColumn(
        "BLIV_key",
        psf.when(
            psf.length(psf.col("trimmed_BILL_INVC_ID")) > 12,
            psf.col("trimmed_BILL_INVC_ID").substr(psf.lit(1), psf.lit(2))
        ).otherwise(psf.col("trimmed_BILL_INVC_ID"))
    )
)

df_join1 = (
    df_InvoicePrep
    .join(
        df_fctsTempTable.alias("fctsTempTable"),
        psf.col("Invoice.BILL_INVC_ID") == psf.col("fctsTempTable.BILL_INVC_ID"),
        "left"
    )
)

df_join2 = (
    df_join1
    .join(
        df_InvcLkup.alias("InvcLkup"),
        psf.col("BLIV_key") == psf.col("InvcLkup.BLIV_ID"),
        "left"
    )
)

df_lnkInvoiceOut = (
    df_join2
    .filter(
        (psf.col("InvcLkup.BLIV_ID").isNotNull())
        & (psf.col("fctsTempTable.BILL_INVC_ID").isNull())
    )
    .withColumn("BILL_INVC_ID", trim(psf.col("trimmed_BILL_INVC_ID")))
    .withColumn("BILL_INVC_ID", psf.rpad(psf.col("BILL_INVC_ID"), psf.lit(<...>), psf.lit(" ")))
    .select("BILL_INVC_ID")
)

df_not_found = (
    df_join2
    .filter(psf.col("InvcLkup.BLIV_ID").isNull())
    .withColumn("BILL_INVC_ID", trim(psf.col("trimmed_BILL_INVC_ID")))
    .withColumn("BILL_INVC_ID", psf.rpad(psf.col("BILL_INVC_ID"), psf.lit(<...>), psf.lit(" ")))
    .select("BILL_INVC_ID")
)

write_files(
    df_not_found,
    f"{adls_path}/verified/IdsIncomeHitListNotFound.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

execute_dml(
    f"DROP TABLE IF EXISTS tempdb.FctsIncomeDriverHitList_TMP_IDS_INCOME_temp",
    jdbc_url_tempdb,
    jdbc_props_tempdb
)

df_lnkInvoiceOut.write.format("jdbc") \
    .option("url", jdbc_url_tempdb) \
    .options(**jdbc_props_tempdb) \
    .option("dbtable", "tempdb.FctsIncomeDriverHitList_TMP_IDS_INCOME_temp") \
    .mode("overwrite") \
    .save()

merge_sql = f"""
MERGE tempdb..{DriverTable} AS T
USING tempdb.FctsIncomeDriverHitList_TMP_IDS_INCOME_temp AS S
ON T.BILL_INVC_ID = S.BILL_INVC_ID
WHEN NOT MATCHED THEN
  INSERT (BILL_INVC_ID)
  VALUES (S.BILL_INVC_ID);
"""

execute_dml(merge_sql, jdbc_url_tempdb, jdbc_props_tempdb)