# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Copyright 2009 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC 
# MAGIC CALLED BY : DeaProvDeaCntl
# MAGIC                                
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                  Date                 Project/Altiris #      Change Description                                                                                                      Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                                                                 --------------------------------       -------------------------------   ----------------------------       
# MAGIC Kalyan Neelam         2009-10-28            TTR-525                 Original Programming                                                                                                devlIDS                          Steph Goddard        10/29/2009 
# MAGIC 
# MAGIC Manasa Andru         2014-02-21           TFS - 4011         Updated the EFF_DT_SK and TERM_DT_SK                                                               IntegrateCurDevl            Bhoomi Dasari          2/26/2014
# MAGIC                                                                                   fields as per the mapping rules in the Transformer_3 stage
# MAGIC Kalyan Neelam         2015-08-25            5403               Updated logic for CMN_PRCT_ID in Transformer_30                                                       IntegrateDev1                 Bhoomi Dasari          8/26/2015 
# MAGIC 
# MAGIC Kalyan Neelam         2015-10-30      5403/TFS1048     Added 2 new columns on end -                                                                                         IntegrateDev1                Bhoomi Dasari          11/9/2015 
# MAGIC                                                                                  NTNL_PROV_ID_PROV_TYP_DESC and NTNL_PROV_ID_PROV_TYP_DESC
# MAGIC 
# MAGIC Ravi Singh               2018-05-25      TFS - 21354      Added hash file (hf_enclarity_proddea_cmnprct_lkup1) in Input                                            IntegrateDev1                Abhiram Dasarathy    2018-06-19
# MAGIC                                                                                   Value. Earlier it was mentioned wrong name so job does not clear 
# MAGIC                                                                                   the hf_enclarity_proddea_cmnprct_lkup1 hashed file and throwing 
# MAGIC                                                                                   warning message.

# MAGIC IDS Enclarity Prov Dea Extract
# MAGIC Prov Dea Primary Key
# MAGIC Creates file in /key
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
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType, DecimalType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../shared_containers/PrimaryKey/ProvDeaPkey
# COMMAND ----------

IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
CactusOwner = get_widget_value('CactusOwner','')
cactus_secret_name = get_widget_value('cactus_secret_name','')
CurrDate = get_widget_value('CurrDate','')
RunCycle = get_widget_value('RunCycle','')
RunID = get_widget_value('RunID','')

schema_ENCLARITY_DEA = StructType([
    StructField("GRP_KEY", StringType(), True),
    StructField("DEA_NO", StringType(), True),
    StructField("DEA_NO_EXPRTN_DT", StringType(), True),
    StructField("BUS_ACTVTY_CD", StringType(), True)
])
path_ENCLARITY_DEA = f"{adls_path_raw}/landing/Enclarity_DEA_dea_idv.txt"
df_ENCLARITY_DEA = (
    spark.read
    .option("header", "false")
    .option("quote", "000")
    .schema(schema_ENCLARITY_DEA)
    .csv(path_ENCLARITY_DEA)
)
df_Extract = df_ENCLARITY_DEA

IDSOwner_value = IDSOwner
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

df_CMN_PRCT_PROV_CMN_PRCT = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"SELECT CMN_PRCT.NTNL_PROV_ID, MIN(CMN_PRCT.CMN_PRCT_ID) FROM {IDSOwner_value}.CMN_PRCT CMN_PRCT GROUP BY CMN_PRCT.NTNL_PROV_ID"
    )
    .load()
)

df_CMN_PRCT_PROV_PROV = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"SELECT PROV.NTNL_PROV_ID, MIN(CP.CMN_PRCT_ID) FROM {IDSOwner_value}.PROV PROV, {IDSOwner_value}.CMN_PRCT CP WHERE PROV.CMN_PRCT_SK = CP.CMN_PRCT_SK GROUP BY PROV.NTNL_PROV_ID"
    )
    .load()
)

df_CMN_PRCT_PROV_CMN_PRCT = df_CMN_PRCT_PROV_CMN_PRCT.select(
    F.col("NTNL_PROV_ID"),
    F.col("MIN(CMN_PRCT.CMN_PRCT_ID)").alias("CMN_PRCT_ID")
)
df_CMN_PRCT_PROV_PROV = df_CMN_PRCT_PROV_PROV.select(
    F.col("NTNL_PROV_ID"),
    F.col("MIN(CP.CMN_PRCT_ID)").alias("CMN_PRCT_ID")
)

df_CMN_PRCT_lkup = df_CMN_PRCT_PROV_CMN_PRCT.dropDuplicates(["NTNL_PROV_ID"])
df_Prov_lkup = df_CMN_PRCT_PROV_PROV.dropDuplicates(["NTNL_PROV_ID"])

schema_LIC = StructType([
    StructField("GRP_KEY", StringType(), True),
    StructField("LIC_NO_INPT", StringType(), True),
    StructField("LIC_ST", StringType(), True),
    StructField("LIC_NO", StringType(), True),
    StructField("LIC_TYP", StringType(), True),
    StructField("LIC_STTUS", StringType(), True),
    StructField("LIC_BEG_DT", StringType(), True),
    StructField("LIC_END_DT", StringType(), True)
])
path_LIC = f"{adls_path_raw}/landing/Enclarity_DEA_license_idv.txt"
df_LIC = (
    spark.read
    .option("header", "false")
    .option("quote", "000")
    .schema(schema_LIC)
    .csv(path_LIC)
)

schema_LIC_1 = StructType([
    StructField("GRP_KEY", StringType(), True),
    StructField("LIC_NO_INPT", StringType(), True),
    StructField("LIC_ST", StringType(), True),
    StructField("LIC_NO", StringType(), True),
    StructField("LIC_TYP", StringType(), True),
    StructField("LIC_STTUS", StringType(), True),
    StructField("LIC_BEG_DT", StringType(), True),
    StructField("LIC_END_DT", StringType(), True)
])
df_LIC_1 = (
    spark.read
    .option("header", "false")
    .option("quote", "000")
    .schema(schema_LIC_1)
    .csv(path_LIC)
)

schema_NPI = StructType([
    StructField("GRP_KEY", StringType(), True),
    StructField("NTNL_PROV_ID", StringType(), True),
    StructField("TAXONOMY", StringType(), True),
    StructField("PROV_TYP", StringType(), True),
    StructField("PROV_CLS", StringType(), True),
    StructField("SPCLIZATION", StringType(), True)
])
path_NPI = f"{adls_path_raw}/landing/Enclarity_DEA_npi_idv.txt"
df_NPI = (
    spark.read
    .option("header", "false")
    .option("quote", "000")
    .schema(schema_NPI)
    .csv(path_NPI)
)

schema_INDVS = StructType([
    StructField("GRP_KEY", StringType(), True),
    StructField("NM_PFX", StringType(), True),
    StructField("FIRST_NM", StringType(), True),
    StructField("MID_NM", StringType(), True),
    StructField("LAST_NM", StringType(), True),
    StructField("NM_SFX", StringType(), True),
    StructField("OTHR_SFX", StringType(), True)
])
path_INDVS = f"{adls_path_raw}/landing/Enclarity_DEA_master_idv.txt"
df_INDVS = (
    spark.read
    .option("header", "false")
    .option("quote", "000")
    .schema(schema_INDVS)
    .csv(path_INDVS)
)

df_LIC_1_sorted = df_LIC_1.orderBy(F.col("GRP_KEY").asc(), F.col("LIC_END_DT").asc())

cactusOwner_value = CactusOwner
jdbc_url_cactus, jdbc_props_cactus = get_db_config(cactus_secret_name)
df_VC_PROV = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_cactus)
    .options(**jdbc_props_cactus)
    .option(
        "query",
        f"SELECT PROV.NPI, PROV.ID FROM {cactusOwner_value}.PROVIDERS PROV WHERE PROV.ID <> '00000'"
    )
    .load()
)

df_Trim_VC_Prov = df_VC_PROV.select(
    trim(F.col("NPI")).alias("NPI"),
    trim(F.col("ID")).alias("ID")
)

df_hf_enclarity_vc_prov_npi_id = df_Trim_VC_Prov.dropDuplicates(["NPI"])

df_VISUAL_CACTUS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_cactus)
    .options(**jdbc_props_cactus)
    .option(
        "query",
        f"SELECT LIC.LICENSENUMBER, PROV.ID FROM {cactusOwner_value}.PROVIDERLICENSES LIC, {cactusOwner_value}.PROVIDERS PROV WHERE LIC.PROVIDER_K = PROV.PROVIDER_K AND LIC.LICENSE_RTK = 'NPDBDEAxxx' AND PROV.ID <> '00000'"
    )
    .load()
)
df_Trim = df_VISUAL_CACTUS.select(
    trim(F.col("LICENSENUMBER")).alias("LICENSENUMBER"),
    trim(F.col("ID")).alias("ID")
)

schema_ENCLARITY_ADDR = StructType([
    StructField("GRP_KEY", StringType(), True),
    StructField("ADDR_KEY", StringType(), True),
    StructField("ADDR_LN_1", StringType(), True),
    StructField("ADDR_LN_2", StringType(), True),
    StructField("CITY", StringType(), True),
    StructField("ST", StringType(), True),
    StructField("ZIP5", StringType(), True),
    StructField("ZIP4", StringType(), True),
    StructField("FIPS_CD", StringType(), True),
    StructField("PRCTC_ADDR_IN", StringType(), True),
    StructField("MAIL_BILL_ADDR_IN", StringType(), True),
    StructField("PHN_NO_1", StringType(), True),
    StructField("PHN_1_PRCTC_IN", StringType(), True),
    StructField("PHN_1_MAIL_BILL_IN", StringType(), True),
    StructField("PHN_NO_2", StringType(), True),
    StructField("PHN_2_PRCTC_IN", StringType(), True),
    StructField("PHN_2_MAIL_BILL_IN", StringType(), True),
    StructField("PHN_NO_3", StringType(), True),
    StructField("PHN_3_PRCTC_IN", StringType(), True),
    StructField("PHN_3_MAIL_BILL_IN", StringType(), True),
    StructField("FAX_NO_1", StringType(), True),
    StructField("FAX_1_PRCTC_IN", StringType(), True),
    StructField("FAX_1_MAIL_BILL_IN", StringType(), True),
    StructField("FAX_NO_2", StringType(), True),
    StructField("FAX_2_PRCTC_IN", StringType(), True),
    StructField("FAX_2_MAIL_BILL_IN", StringType(), True),
    StructField("FAX_NO_3", StringType(), True),
    StructField("FAX_3_PRCTC_IN", StringType(), True),
    StructField("FAX_3_MAIL_BILL_IN", StringType(), True),
    StructField("CO_NM", StringType(), True),
    StructField("LAST_VER_DT", StringType(), True),
    StructField("ADDR_CNFDNC_SCORE", StringType(), True)
])
path_ENCLARITY_ADDR = f"{adls_path_raw}/landing/Enclarity_DEA_addrphonefax_idv.txt"
df_ENCLARITY_ADDR = (
    spark.read
    .option("header", "false")
    .option("quote", "000")
    .schema(schema_ENCLARITY_ADDR)
    .csv(path_ENCLARITY_ADDR)
)

df_addrin_y = df_ENCLARITY_ADDR.filter(F.col("PRCTC_ADDR_IN") == 'Y').select(
    F.col("GRP_KEY"),
    F.col("ADDR_CNFDNC_SCORE"),
    F.col("ADDR_KEY"),
    F.col("ADDR_LN_1"),
    F.col("ADDR_LN_2"),
    F.col("CITY"),
    F.col("ST"),
    F.col("ZIP5")
)

df_addrin_null = df_ENCLARITY_ADDR.filter(F.col("PRCTC_ADDR_IN").isNull()).select(
    F.col("GRP_KEY"),
    F.col("ADDR_CNFDNC_SCORE"),
    F.col("ADDR_KEY"),
    F.col("ADDR_LN_1"),
    F.col("ADDR_LN_2"),
    F.col("CITY"),
    F.col("ST"),
    F.col("ZIP5")
)

df_addrin_y_sorted = df_addrin_y.orderBy(
    F.col("GRP_KEY").asc(),
    F.col("ADDR_CNFDNC_SCORE").asc(),
    F.col("ADDR_KEY").desc()
)

df_addrin_null_sorted = df_addrin_null.orderBy(
    F.col("GRP_KEY").asc(),
    F.col("ADDR_CNFDNC_SCORE").asc(),
    F.col("ADDR_KEY").desc()
)

df_hf_enclarity_provdea_id_addr_id = df_Trim.dropDuplicates(["LICENSENUMBER"])
df_hf_enclarity_provdea_id_addr_addr_y = df_addrin_y_sorted.dropDuplicates(["GRP_KEY"])
df_hf_enclarity_provdea_id_addr_addr_null = df_addrin_null_sorted.dropDuplicates(["GRP_KEY"])

df_LIC_agg = df_LIC.groupBy("GRP_KEY").agg(F.count("GRP_KEY").alias("Count"))
df_hf_enclarity_prov_dea_cnt_agg = df_LIC_agg.dropDuplicates(["GRP_KEY"])

df_LIC_join = df_LIC.alias("LIC")
df_cnt_lkup = df_hf_enclarity_prov_dea_cnt_agg.alias("cnt_lkup")

df_Transformer_101_joined = (
    df_LIC_join.join(
        df_cnt_lkup, F.col("LIC.GRP_KEY") == F.col("cnt_lkup.GRP_KEY"), how="left"
    )
    .withColumn(
        "svRecInd",
        F.when(F.col("cnt_lkup.Count") > 1,
               F.when(F.col("LIC.LIC_BEG_DT") == "0000-00-00", F.lit("N")).otherwise(F.lit("Y")))
        .otherwise(
            F.when(F.col("cnt_lkup.Count") == 1, "Y").otherwise("N")
        )
    )
)
df_DSLink111 = df_Transformer_101_joined.filter(F.col("svRecInd") == "Y").select(
    F.col("LIC.GRP_KEY").alias("GRP_KEY"),
    F.col("LIC.LIC_NO_INPT").alias("LIC_NO_INPT"),
    F.col("LIC.LIC_ST").alias("LIC_ST"),
    F.col("LIC.LIC_NO").alias("LIC_NO"),
    F.col("LIC.LIC_TYP").alias("LIC_TYP"),
    F.col("LIC.LIC_STTUS").alias("LIC_STTUS"),
    F.when(F.col("LIC.LIC_BEG_DT") == "0000-00-00","1753-01-01").otherwise(F.col("LIC.LIC_BEG_DT")).alias("LIC_BEG_DT"),
    F.col("LIC.LIC_END_DT").alias("LIC_END_DT")
)

df_sort_83 = df_DSLink111.orderBy(
    F.col("GRP_KEY").asc(),
    F.col("LIC_BEG_DT").desc()
)

df_NPI_dedup = df_NPI.dropDuplicates(["GRP_KEY"])
df_INDVS_dedup = df_INDVS.dropDuplicates(["GRP_KEY"])
df_LIC_beg_dedup = df_sort_83.dropDuplicates(["GRP_KEY"])
df_LIC_end_dedup = df_LIC_1_sorted.dropDuplicates(["GRP_KEY"])

df_hf_lic_npi_indvs_NPI = df_NPI_dedup
df_hf_lic_npi_indvs_INDVS = df_INDVS_dedup
df_hf_lic_npi_indvs_beg = df_LIC_beg_dedup
df_hf_lic_npi_indvs_end = df_LIC_1_sorted.dropDuplicates(["GRP_KEY"])

df_hf_lic_npi_indvs_beg_out = df_hf_lic_npi_indvs_beg.select(
    F.col("GRP_KEY"),
    F.col("LIC_NO_INPT"),
    F.col("LIC_ST"),
    F.col("LIC_NO"),
    F.col("LIC_TYP"),
    F.col("LIC_STTUS"),
    F.col("LIC_BEG_DT"),
    F.col("LIC_END_DT")
)

df_hf_lic_npi_indvs_end_out = df_hf_lic_npi_indvs_end.select(
    F.col("GRP_KEY"),
    F.col("LIC_NO_INPT"),
    F.col("LIC_ST"),
    F.col("LIC_NO"),
    F.col("LIC_TYP"),
    F.col("LIC_STTUS"),
    F.col("LIC_BEG_DT"),
    F.col("LIC_END_DT")
)

df_hf_lic_npi_indvs_npi_out = df_hf_lic_npi_indvs_NPI.select(
    F.col("GRP_KEY"),
    F.col("NTNL_PROV_ID"),
    F.col("TAXONOMY"),
    F.col("PROV_TYP"),
    F.col("PROV_CLS"),
    F.col("SPCLIZATION")
)

df_hf_lic_npi_indvs_indvs_out = df_hf_lic_npi_indvs_INDVS.select(
    F.col("GRP_KEY"),
    F.col("NM_PFX"),
    F.col("FIRST_NM"),
    F.col("MID_NM"),
    F.col("LAST_NM"),
    F.col("NM_SFX"),
    F.col("OTHR_SFX")
)

df_Transformer_3_join = df_Extract.alias("Extract")
df_lkup_id = df_hf_enclarity_provdea_id_addr_id.alias("lkup_id")
df_lkup_addr_y = df_hf_enclarity_provdea_id_addr_addr_y.alias("lkup_addr_y")
df_lkup_addr_null = df_hf_enclarity_provdea_id_addr_addr_null.alias("lkup_addr_null")
df_lic_beg = df_hf_lic_npi_indvs_beg_out.alias("LIC_BegDt")
df_lic_end = df_hf_lic_npi_indvs_end_out.alias("LIC_EndDt")
df_lkup_npi = df_hf_lic_npi_indvs_npi_out.alias("lkup_NPI")
df_lkup_indvs = df_hf_lic_npi_indvs_indvs_out.alias("lkup_INDVS")

df_Transformer_3_combined = (
    df_Transformer_3_join
    .join(
        df_lkup_id, F.col("Extract.DEA_NO") == F.col("lkup_id.LICENSENUMBER"), how="left"
    )
    .join(
        df_lic_beg, F.col("Extract.GRP_KEY") == F.col("LIC_BegDt.GRP_KEY"), how="left"
    )
    .join(
        df_lic_end, F.col("Extract.GRP_KEY") == F.col("LIC_EndDt.GRP_KEY"), how="left"
    )
    .join(
        df_lkup_npi, F.col("Extract.GRP_KEY") == F.col("lkup_NPI.GRP_KEY"), how="left"
    )
    .join(
        df_lkup_indvs, F.col("Extract.GRP_KEY") == F.col("lkup_INDVS.GRP_KEY"), how="left"
    )
    .join(
        df_lkup_addr_y, F.col("Extract.GRP_KEY") == F.col("lkup_addr_y.GRP_KEY"), how="left"
    )
    .join(
        df_lkup_addr_null, F.col("Extract.GRP_KEY") == F.col("lkup_addr_null.GRP_KEY"), how="left"
    )
    .withColumn(
        "svProvNm",
        F.when(
            F.col("lkup_INDVS.MID_NM").isNull(),
            F.upper(F.col("lkup_INDVS.LAST_NM")) + F.lit(" ") + F.upper(F.col("lkup_INDVS.FIRST_NM"))
        ).otherwise(
            F.upper(F.col("lkup_INDVS.LAST_NM")) + F.lit(" ") + F.upper(F.col("lkup_INDVS.FIRST_NM")) + F.lit(" ") + F.upper(F.substring(F.col("lkup_INDVS.MID_NM"),1,1))
        )
    )
)

df_dea_data = df_Transformer_3_combined.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.lit("I"),10," ").alias("INSRT_UPDT_CD"),
    F.rpad(F.lit("N"),1," ").alias("DISCARD_IN"),
    F.rpad(F.lit("Y"),1," ").alias("PASS_THRU_IN"),
    F.col("CurrDate").alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit("NA").alias("SRC_SYS_CD"),
    F.col("Extract.DEA_NO").alias("PRI_KEY_STRING"),
    F.lit(0).alias("PROV_DEA_SK"),
    trim(F.upper(F.col("Extract.DEA_NO"))).alias("DEA_NO"),
    F.col("RunCycle").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("RunCycle").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.rpad(F.lit("N"),1," ").alias("CLM_TRANS_ADD_IN"),
    F.when(
        F.col("LIC_BegDt.LIC_BEG_DT").isNull()|
        (F.length(F.trim(F.col("LIC_BegDt.LIC_BEG_DT")))==0)|
        (F.trim(F.col("LIC_BegDt.LIC_BEG_DT"))=="NA")|
        (F.trim(F.col("LIC_BegDt.LIC_BEG_DT"))=="UNK"),
        F.lit("1753-01-01")
    ).otherwise(F.col("LIC_BegDt.LIC_BEG_DT")).alias("EFF_DT_SK"),
    F.when(
        F.col("LIC_EndDt.LIC_END_DT").isNull()|
        (F.length(F.trim(F.col("LIC_EndDt.LIC_END_DT")))==0)|
        (F.trim(F.col("LIC_EndDt.LIC_END_DT"))=="NA")|
        (F.trim(F.col("LIC_EndDt.LIC_END_DT"))=="UNK"),
        F.lit("2199-12-31")
    ).otherwise(
        F.when(F.trim(F.col("LIC_EndDt.LIC_END_DT"))=="0000-00-00", F.lit("2199-12-31"))
         .otherwise(F.col("LIC_EndDt.LIC_END_DT"))
    ).alias("TERM_DT_SK"),
    F.when(
        F.col("svProvNm").isNull(), F.lit(None)
    ).otherwise(F.col("svProvNm")).alias("PROV_NM"),
    F.when(
        F.col("lkup_addr_y.ADDR_LN_1").isNull(),
        F.when(
            F.col("lkup_addr_null.ADDR_LN_1").isNull(), F.lit(None)
        ).otherwise(trim(F.upper(F.col("lkup_addr_null.ADDR_LN_1"))))
    ).otherwise(trim(F.upper(F.col("lkup_addr_y.ADDR_LN_1")))).alias("ADDR_LN_1"),
    F.when(
        F.col("lkup_addr_y.ADDR_LN_1").isNull(),
        F.when(
            F.col("lkup_addr_null.ADDR_LN_2").isNull(), F.lit(None)
        ).otherwise(trim(F.upper(F.col("lkup_addr_null.ADDR_LN_2"))))
    ).otherwise(trim(F.upper(F.col("lkup_addr_y.ADDR_LN_2")))).alias("ADDR_LN_2"),
    F.when(
        F.col("lkup_addr_y.ADDR_LN_1").isNull(),
        F.when(
            F.col("lkup_addr_null.CITY").isNull(), F.lit(None)
        ).otherwise(trim(F.upper(F.col("lkup_addr_null.CITY"))))
    ).otherwise(trim(F.upper(F.col("lkup_addr_y.CITY")))).alias("CITY_NM"),
    F.when(
        F.col("lkup_addr_y.ADDR_LN_1").isNull(),
        F.when(
            F.col("lkup_addr_null.ST").isNull(), F.lit(None)
        ).otherwise(trim(F.upper(F.col("lkup_addr_null.ST"))))
    ).otherwise(trim(F.upper(F.col("lkup_addr_y.ST")))).alias("PROV_DEA_ST_CD"),
    F.when(
        F.col("lkup_addr_y.ADDR_LN_1").isNull(),
        F.when(
            F.col("lkup_addr_null.ZIP5").isNull(), F.lit(None)
        ).otherwise(trim(F.upper(F.col("lkup_addr_null.ZIP5"))))
    ).otherwise(trim(F.upper(F.col("lkup_addr_y.ZIP5")))).alias("POSTAL_CD"),
    F.when(
        F.col("lkup_NPI.NTNL_PROV_ID").isNull(), F.lit("NA")
    ).otherwise(F.col("lkup_NPI.NTNL_PROV_ID")).alias("NTNL_PROV_ID"),
    F.when(
        F.col("lkup_NPI.SPCLIZATION").isNull(),
        F.lit(None)
    ).otherwise(
        trim(F.upper(F.regexp_replace(F.col("lkup_NPI.SPCLIZATION"), r"\.", "&")))
    ).alias("NTNL_PROV_ID_SPEC_DESC"),
    F.when(
        F.col("lkup_id.ID").isNull(),
        F.lit(0)
    ).otherwise(F.col("lkup_id.ID")).alias("ID"),
    F.col("lkup_NPI.PROV_TYP").isNull().when(
        F.col("lkup_NPI.PROV_TYP").isNull(), F.lit(None)
    ).otherwise(F.upper(F.col("lkup_NPI.PROV_TYP"))).alias("NTNL_PROV_ID_PROV_TYP_DESC"),
    F.col("lkup_NPI.PROV_CLS").isNull().when(
        F.col("lkup_NPI.PROV_CLS").isNull(), F.lit(None)
    ).otherwise(F.upper(F.col("lkup_NPI.PROV_CLS"))).alias("NTNL_PROV_ID_PROV_CLS_DESC")
)

df_hf_enclarity_vc_prov_npi_id_alias = df_hf_enclarity_vc_prov_npi_id.alias("DSLink124")
df_CmnPrct_lkup_alias = df_CMN_PRCT_lkup.alias("CmnPrct_lkup")
df_Prov_lkup_alias = df_Prov_lkup.alias("Prov_lkup")

df_Transformer_33_joined = (
    df_dea_data.alias("dea_data")
    .join(
        df_hf_enclarity_vc_prov_npi_id_alias,
        F.col("dea_data.NTNL_PROV_ID") == F.col("DSLink124.NPI"),
        how="left"
    )
    .join(
        df_CmnPrct_lkup_alias,
        F.col("dea_data.NTNL_PROV_ID") == F.col("CmnPrct_lkup.NTNL_PROV_ID"),
        how="left"
    )
    .join(
        df_Prov_lkup_alias,
        F.col("dea_data.NTNL_PROV_ID") == F.col("Prov_lkup.NTNL_PROV_ID"),
        how="left"
    )
    .select(
        F.col("dea_data.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.col("dea_data.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        F.col("dea_data.DISCARD_IN").alias("DISCARD_IN"),
        F.col("dea_data.PASS_THRU_IN").alias("PASS_THRU_IN"),
        F.col("dea_data.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        F.col("dea_data.ERR_CT").alias("ERR_CT"),
        F.col("dea_data.RECYCLE_CT").alias("RECYCLE_CT"),
        F.col("dea_data.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("dea_data.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        F.col("dea_data.PROV_DEA_SK").alias("PROV_DEA_SK"),
        F.col("dea_data.DEA_NO").alias("DEA_NO"),
        F.col("dea_data.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("dea_data.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.when(
            F.col("dea_data.ID") == 0,
            F.when(
                F.col("DSLink124.ID").isNull(),
                F.when(
                    F.col("CmnPrct_lkup.CMN_PRCT_ID").isNull(),
                    F.when(
                        F.col("Prov_lkup.CMN_PRCT_ID").isNull(), F.lit(0)
                    ).otherwise(F.col("Prov_lkup.CMN_PRCT_ID"))
                ).otherwise(F.col("CmnPrct_lkup.CMN_PRCT_ID"))
            ).otherwise(F.col("DSLink124.ID"))
        ).otherwise(F.col("dea_data.ID")).alias("CMN_PRCT_CD"),
        F.col("dea_data.CLM_TRANS_ADD_IN").alias("CLM_TRANS_ADD_IN"),
        F.col("dea_data.EFF_DT_SK").alias("EFF_DT_SK"),
        F.col("dea_data.TERM_DT_SK").alias("TERM_DT_SK"),
        F.when(
            F.when(
                F.col("dea_data.PROV_NM").isNull()|
                (F.length(F.trim(F.col("dea_data.PROV_NM")))==0),
                F.lit("UNK")
            ).otherwise(F.trim(F.col("dea_data.PROV_NM"))) == "UNK",
            F.lit(" ")
        ).otherwise(F.col("dea_data.PROV_NM")).alias("PROV_NM"),
        F.col("dea_data.ADDR_LN_1").alias("ADDR_LN_1"),
        F.col("dea_data.ADDR_LN_2").alias("ADDR_LN_2"),
        F.col("dea_data.CITY_NM").alias("CITY_NM"),
        F.col("dea_data.PROV_DEA_ST_CD").alias("PROV_DEA_ST_CD"),
        F.col("dea_data.POSTAL_CD").alias("POSTAL_CD"),
        F.when(
            F.when(
                F.col("dea_data.NTNL_PROV_ID").isNull()|
                (F.length(F.trim(F.col("dea_data.NTNL_PROV_ID")))==0),
                F.lit("UNK")
            ).otherwise(F.trim(F.col("dea_data.NTNL_PROV_ID"))) == "UNK",
            F.lit("NA")
        ).otherwise(F.col("dea_data.NTNL_PROV_ID")).alias("NTNL_PROV_ID"),
        F.col("dea_data.NTNL_PROV_ID_SPEC_DESC").alias("NTNL_PROV_ID_SPEC_DESC"),
        F.col("dea_data.NTNL_PROV_ID_PROV_TYP_DESC").alias("NTNL_PROV_ID_PROV_TYP_DESC"),
        F.col("dea_data.NTNL_PROV_ID_PROV_CLS_DESC").alias("NTNL_PROV_ID_PROV_CLS_DESC")
    )
)

params_ProvDeaPkey = {
    "RunCycle": RunCycle
}
df_Transform = df_Transformer_33_joined
df_PROV_DEA = ProvDeaPkey(df_Transform, params_ProvDeaPkey)

df_PROV_DEA_final = df_PROV_DEA.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(F.col("INSRT_UPDT_CD"),10," ").alias("INSRT_UPDT_CD"),
    rpad(F.col("DISCARD_IN"),1," ").alias("DISCARD_IN"),
    rpad(F.col("PASS_THRU_IN"),1," ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("PROV_DEA_SK"),
    rpad(F.col("DEA_NO"),9," ").alias("DEA_NO"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    rpad(F.col("CMN_PRCT_CD"),5," ").alias("CMN_PRCT_CD"),
    rpad(F.col("CLM_TRANS_ADD_IN"),1," ").alias("CLM_TRANS_ADD_IN"),
    rpad(F.col("EFF_DT_SK"),10," ").alias("EFF_DT_SK"),
    rpad(F.col("TERM_DT_SK"),10," ").alias("TERM_DT_SK"),
    F.col("PROV_NM"),
    F.col("ADDR_LN_1"),
    F.col("ADDR_LN_2"),
    F.col("CITY_NM"),
    rpad(F.col("PROV_DEA_ST_CD"),2," ").alias("PROV_DEA_ST_CD"),
    rpad(F.col("POSTAL_CD"),11," ").alias("POSTAL_CD"),
    F.col("NTNL_PROV_ID"),
    F.col("NTNL_PROV_ID_SPEC_DESC"),
    F.col("NTNL_PROV_ID_PROV_TYP_DESC"),
    F.col("NTNL_PROV_ID_PROV_CLS_DESC")
)

output_path_PROV_DEA = f"{adls_path}/key/IdsEnclarityProvDEAExtr.ProvDEA.dat.{RunID}"
write_files(
    df_PROV_DEA_final,
    output_path_PROV_DEA,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)