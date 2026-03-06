# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Called By: FctsMedMgtCntl
# MAGIC 
# MAGIC 
# MAGIC Process : Case Mgt Status Pkey process which is processed and loaded to CASE_MGT_CNTCT table
# MAGIC 
# MAGIC 
# MAGIC Modifications:                        
# MAGIC                                              		  Project/                                                                                                    	Code                  Date
# MAGIC Developer         	Date             	 Altiris #     	Change Description                                                   	Reviewer            Reviewed
# MAGIC -----------------------  	-------------------   	-------------   	-----------------------------------------------------------------------------   	-------------------------  -------------------
# MAGIC Akash Parsha        2019-09-03                 US1401675             Case Mgt Cost PKey Process         			Jaideep Mankala  10/10/2019

# MAGIC New PKEYs are generated in this transformer. A database sequence will be used to create next PKEY value.
# MAGIC Insert Only the Newly generated Pkeys into the K Table
# MAGIC Read K_CASE_MGT_CONTACT Table to pull the Natural Keys and the Skey.
# MAGIC Inner Join primary key info with table info
# MAGIC Left Join on Natural Keys
# MAGIC Remove Duplicates stage takes care of getting rid of duplicate rows with the same Natural key get into the PKEYing step.
# MAGIC JobName: IdsProdQhpPkey
# MAGIC 
# MAGIC Strip field function and Tranforamtion rules are applied on the data Extracted from SBC
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, lit, when, isnull, rpad
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


SrcSysCd = get_widget_value('SrcSysCd','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
IDSRunCycle = get_widget_value('IDSRunCycle','')
RunID = get_widget_value('RunID','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query_db2_K_CASE_MGT_CONTACT_in = f"Select CASE_MGT_ID, CNTCT_SEQ_NO, SRC_SYS_CD, CRT_RUN_CYC_EXCTN_SK, CASE_MGT_CNTCT_SK from {IDSOwner}.K_CASE_MGT_CNTCT"
df_db2_K_CASE_MGT_CONTACT_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_K_CASE_MGT_CONTACT_in)
    .load()
)

df_CMCS_CONTACT_Extr = spark.read.parquet(
    f"{adls_path}/ds/CASE_MGT_CONTACT.{SrcSysCd}.extr.{RunID}.parquet"
)

df_Cp_Pk_out_CP_Out = df_CMCS_CONTACT_Extr.select(
    col("PRI_NAT_KEY_STRING"),
    col("FIRST_RECYC_TS"),
    col("CNTCT_SEQ_NO"),
    col("CASE_MGT_ID"),
    col("SRC_SYS_CD"),
    col("CNTCT_FIRST_NM"),
    col("CNTCT_LAST_NM"),
    col("CNTCT_MIDINIT"),
    col("CNTCT_TTL_NM"),
    col("CNTCT_ADDR_LN_1"),
    col("CNTCT_ADDR_LN_2"),
    col("CNTCT_ADDR_LN_3"),
    col("CMCT_CITY"),
    col("CMCT_ZIP"),
    col("CNTCT_CNTY_NM"),
    col("CNTCT_FAX_NO"),
    col("CNTCT_FAX_EXT_NO"),
    col("CNTCT_PHN_NO"),
    col("CNTCT_PHN_EXT_NO"),
    col("CNTCT_EMAIL_ADDR"),
    col("CMCT_CTRY_CD"),
    col("CMCT_STATE"),
    col("CMCT_MCTR_LANG")
)

df_Cp_Pk_out_lnk_Transforms_Out = df_CMCS_CONTACT_Extr.select(
    col("CASE_MGT_ID"),
    col("CNTCT_SEQ_NO"),
    col("SRC_SYS_CD")
)

df_rdup_Natural_Keys = dedup_sort(
    df_Cp_Pk_out_lnk_Transforms_Out,
    ["CASE_MGT_ID", "CNTCT_SEQ_NO", "SRC_SYS_CD"],
    [("CASE_MGT_ID", "A"), ("CNTCT_SEQ_NO", "A"), ("SRC_SYS_CD", "A")]
)

df_jn_CMCT_joined = df_rdup_Natural_Keys.alias("lnk_Natural_Keys_out").join(
    df_db2_K_CASE_MGT_CONTACT_in.alias("LnkCaseMgtContactPk"),
    on=[
        df_rdup_Natural_Keys["CASE_MGT_ID"] == df_db2_K_CASE_MGT_CONTACT_in["CASE_MGT_ID"],
        df_rdup_Natural_Keys["CNTCT_SEQ_NO"] == df_db2_K_CASE_MGT_CONTACT_in["CNTCT_SEQ_NO"],
        df_rdup_Natural_Keys["SRC_SYS_CD"] == df_db2_K_CASE_MGT_CONTACT_in["SRC_SYS_CD"]
    ],
    how="left"
)

df_jn_CMCT = df_jn_CMCT_joined.select(
    col("lnk_Natural_Keys_out.CASE_MGT_ID").alias("CASE_MGT_ID"),
    col("lnk_Natural_Keys_out.CNTCT_SEQ_NO").alias("CNTCT_SEQ_NO"),
    col("lnk_Natural_Keys_out.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("LnkCaseMgtContactPk.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LnkCaseMgtContactPk.CASE_MGT_CNTCT_SK").alias("CASE_MGT_CNTCT_SK")
)

df_in = df_jn_CMCT
df_enriched = SurrogateKeyGen(df_in, <DB sequence name>, "CASE_MGT_CNTCT_SK", <schema>, <secret_name>)

df_xfrm_PKEYgen_lnk_Pkey_out = (
    df_enriched
    .withColumn(
        "CRT_RUN_CYC_EXCTN_SK",
        when(isnull(df_in["CASE_MGT_CNTCT_SK"]), lit(IDSRunCycle)).otherwise(df_in["CRT_RUN_CYC_EXCTN_SK"])
    )
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", lit(IDSRunCycle))
    .select(
        df_in["CASE_MGT_ID"].alias("CASE_MGT_ID"),
        df_in["CNTCT_SEQ_NO"].alias("CNTCT_SEQ_NO"),
        df_in["SRC_SYS_CD"].alias("SRC_SYS_CD"),
        col("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        df_enriched["CASE_MGT_CNTCT_SK"].alias("CASE_MGT_CNTCT_SK")
    )
)

df_xfrm_PKEYgen_preNew = df_in.filter(isnull(df_in["CASE_MGT_CNTCT_SK"]))
df_xfrm_PKEYgen_lnk_KCmSt_New = (
    df_xfrm_PKEYgen_preNew.alias("orig")
    .join(
        df_enriched.alias("enr"),
        on=[
            df_xfrm_PKEYgen_preNew["CASE_MGT_ID"] == df_enriched["CASE_MGT_ID"],
            df_xfrm_PKEYgen_preNew["CNTCT_SEQ_NO"] == df_enriched["CNTCT_SEQ_NO"],
            df_xfrm_PKEYgen_preNew["SRC_SYS_CD"] == df_enriched["SRC_SYS_CD"]
        ],
        how="inner"
    )
    .select(
        col("orig.CASE_MGT_ID").alias("CASE_MGT_ID"),
        col("orig.CNTCT_SEQ_NO").alias("CNTCT_SEQ_NO"),
        col("orig.SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("enr.CASE_MGT_CNTCT_SK").alias("CASE_MGT_CNTCT_SK"),
        lit(IDSRunCycle).alias("CRT_RUN_CYC_EXCTN_SK")
    )
)

spark.sql(f"DROP TABLE IF EXISTS STAGING.FctsCaseMgtContactPKey_DB2_K_CASE_MGT_CONTACT_Load_temp")

(
    df_xfrm_PKEYgen_lnk_KCmSt_New.write
    .format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("dbtable", "STAGING.FctsCaseMgtContactPKey_DB2_K_CASE_MGT_CONTACT_Load_temp")
    .mode("overwrite")
    .save()
)

merge_sql_DB2_K_CASE_MGT_CONTACT_Load = f"""
MERGE {IDSOwner}.K_CASE_MGT_CNTCT AS T
USING STAGING.FctsCaseMgtContactPKey_DB2_K_CASE_MGT_CONTACT_Load_temp AS S
ON T.CASE_MGT_ID=S.CASE_MGT_ID AND T.CNTCT_SEQ_NO=S.CNTCT_SEQ_NO AND T.SRC_SYS_CD=S.SRC_SYS_CD
WHEN MATCHED THEN
  UPDATE SET
    T.CASE_MGT_CNTCT_SK = S.CASE_MGT_CNTCT_SK,
    T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK
WHEN NOT MATCHED THEN
  INSERT
  (
    CASE_MGT_ID,
    CNTCT_SEQ_NO,
    SRC_SYS_CD,
    CASE_MGT_CNTCT_SK,
    CRT_RUN_CYC_EXCTN_SK
  )
  VALUES
  (
    S.CASE_MGT_ID,
    S.CNTCT_SEQ_NO,
    S.SRC_SYS_CD,
    S.CASE_MGT_CNTCT_SK,
    S.CRT_RUN_CYC_EXCTN_SK
  )
;
"""

execute_dml(merge_sql_DB2_K_CASE_MGT_CONTACT_Load, jdbc_url, jdbc_props)

df_jn_PKey_joined = df_xfrm_PKEYgen_lnk_Pkey_out.alias("lnk_Pkey_out").join(
    df_Cp_Pk_out_CP_Out.alias("CP_Out"),
    on=[
        df_xfrm_PKEYgen_lnk_Pkey_out["CASE_MGT_ID"] == df_Cp_Pk_out_CP_Out["CASE_MGT_ID"],
        df_xfrm_PKEYgen_lnk_Pkey_out["CNTCT_SEQ_NO"] == df_Cp_Pk_out_CP_Out["CNTCT_SEQ_NO"],
        df_xfrm_PKEYgen_lnk_Pkey_out["SRC_SYS_CD"] == df_Cp_Pk_out_CP_Out["SRC_SYS_CD"]
    ],
    how="inner"
)

df_jn_PKey = df_jn_PKey_joined.select(
    col("lnk_Pkey_out.CASE_MGT_ID").alias("CASE_MGT_ID"),
    col("lnk_Pkey_out.CNTCT_SEQ_NO").alias("CNTCT_SEQ_NO"),
    col("lnk_Pkey_out.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("lnk_Pkey_out.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("lnk_Pkey_out.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("lnk_Pkey_out.CASE_MGT_CNTCT_SK").alias("CASE_MGT_CNTCT_SK"),
    col("CP_Out.PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    col("CP_Out.FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    col("CP_Out.CNTCT_FIRST_NM").alias("CNTCT_FIRST_NM"),
    col("CP_Out.CNTCT_LAST_NM").alias("CNTCT_LAST_NM"),
    col("CP_Out.CNTCT_MIDINIT").alias("CNTCT_MIDINIT"),
    col("CP_Out.CNTCT_TTL_NM").alias("CNTCT_TTL_NM"),
    col("CP_Out.CNTCT_ADDR_LN_1").alias("CNTCT_ADDR_LN_1"),
    col("CP_Out.CNTCT_ADDR_LN_2").alias("CNTCT_ADDR_LN_2"),
    col("CP_Out.CNTCT_ADDR_LN_3").alias("CNTCT_ADDR_LN_3"),
    col("CP_Out.CMCT_CITY").alias("CMCT_CITY"),
    col("CP_Out.CMCT_ZIP").alias("CMCT_ZIP"),
    col("CP_Out.CNTCT_CNTY_NM").alias("CNTCT_CNTY_NM"),
    col("CP_Out.CNTCT_FAX_NO").alias("CNTCT_FAX_NO"),
    col("CP_Out.CNTCT_FAX_EXT_NO").alias("CNTCT_FAX_EXT_NO"),
    col("CP_Out.CNTCT_PHN_NO").alias("CNTCT_PHN_NO"),
    col("CP_Out.CNTCT_PHN_EXT_NO").alias("CNTCT_PHN_EXT_NO"),
    col("CP_Out.CNTCT_EMAIL_ADDR").alias("CNTCT_EMAIL_ADDR"),
    col("CP_Out.CMCT_CTRY_CD").alias("CMCT_CTRY_CD"),
    col("CP_Out.CMCT_STATE").alias("CMCT_STATE"),
    col("CP_Out.CMCT_MCTR_LANG").alias("CMCT_MCTR_LANG")
)

df_seq_CASE_MGT_CONTACT_PKEY = (
    df_jn_PKey
    .withColumn("SRC_SYS_CD", rpad(col("SRC_SYS_CD"), <...>, " "))
    .withColumn("PRI_NAT_KEY_STRING", rpad(col("PRI_NAT_KEY_STRING"), <...>, " "))
    .withColumn("CNTCT_FIRST_NM", rpad(col("CNTCT_FIRST_NM"), <...>, " "))
    .withColumn("CNTCT_LAST_NM", rpad(col("CNTCT_LAST_NM"), <...>, " "))
    .withColumn("CNTCT_MIDINIT", rpad(col("CNTCT_MIDINIT"), <...>, " "))
    .withColumn("CNTCT_TTL_NM", rpad(col("CNTCT_TTL_NM"), <...>, " "))
    .withColumn("CNTCT_ADDR_LN_1", rpad(col("CNTCT_ADDR_LN_1"), <...>, " "))
    .withColumn("CNTCT_ADDR_LN_2", rpad(col("CNTCT_ADDR_LN_2"), <...>, " "))
    .withColumn("CNTCT_ADDR_LN_3", rpad(col("CNTCT_ADDR_LN_3"), <...>, " "))
    .withColumn("CMCT_CITY", rpad(col("CMCT_CITY"), <...>, " "))
    .withColumn("CMCT_ZIP", rpad(col("CMCT_ZIP"), <...>, " "))
    .withColumn("CNTCT_CNTY_NM", rpad(col("CNTCT_CNTY_NM"), <...>, " "))
    .withColumn("CNTCT_FAX_NO", rpad(col("CNTCT_FAX_NO"), <...>, " "))
    .withColumn("CNTCT_FAX_EXT_NO", rpad(col("CNTCT_FAX_EXT_NO"), <...>, " "))
    .withColumn("CNTCT_PHN_NO", rpad(col("CNTCT_PHN_NO"), <...>, " "))
    .withColumn("CNTCT_PHN_EXT_NO", rpad(col("CNTCT_PHN_EXT_NO"), <...>, " "))
    .withColumn("CNTCT_EMAIL_ADDR", rpad(col("CNTCT_EMAIL_ADDR"), <...>, " "))
    .withColumn("CMCT_CTRY_CD", rpad(col("CMCT_CTRY_CD"), <...>, " "))
    .withColumn("CMCT_STATE", rpad(col("CMCT_STATE"), <...>, " "))
    .withColumn("CMCT_MCTR_LANG", rpad(col("CMCT_MCTR_LANG"), <...>, " "))
    .select(
        "CASE_MGT_ID",
        "CNTCT_SEQ_NO",
        "SRC_SYS_CD",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "CASE_MGT_CNTCT_SK",
        "PRI_NAT_KEY_STRING",
        "FIRST_RECYC_TS",
        "CNTCT_FIRST_NM",
        "CNTCT_LAST_NM",
        "CNTCT_MIDINIT",
        "CNTCT_TTL_NM",
        "CNTCT_ADDR_LN_1",
        "CNTCT_ADDR_LN_2",
        "CNTCT_ADDR_LN_3",
        "CMCT_CITY",
        "CMCT_ZIP",
        "CNTCT_CNTY_NM",
        "CNTCT_FAX_NO",
        "CNTCT_FAX_EXT_NO",
        "CNTCT_PHN_NO",
        "CNTCT_PHN_EXT_NO",
        "CNTCT_EMAIL_ADDR",
        "CMCT_CTRY_CD",
        "CMCT_STATE",
        "CMCT_MCTR_LANG"
    )
)

write_files(
    df_seq_CASE_MGT_CONTACT_PKEY,
    f"{adls_path}/key/CASE_MGT_CNT.{SrcSysCd}.pkey.{RunID}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)