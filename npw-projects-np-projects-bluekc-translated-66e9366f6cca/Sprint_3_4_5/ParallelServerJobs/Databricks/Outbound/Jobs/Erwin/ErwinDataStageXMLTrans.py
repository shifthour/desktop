# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2020 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC JOB NAME:  ErwinDataStageXMLTrans
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:  Read Erwin standard XML and create DataStage XML for metadata input.
# MAGIC 
# MAGIC 
# MAGIC CALLED BY: ErwinXMLTranformCntl
# MAGIC 
# MAGIC 
# MAGIC HASH FILES:  None
# MAGIC 
# MAGIC 
# MAGIC RERUN INFORMATION: 
# MAGIC 
# MAGIC              PREVIOUS RUN SUCCESSFUL:   Nothing to reset. 
# MAGIC              PREVIOUS RUN ABORTED:         Nothing to reset, just restart.
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer             Date                Project/Ticket #\(9)      Change Description\(9)\(9)\(9)Development Project   Code Reviewer\(9)      Date Reviewed       
# MAGIC ------------------            -------------------    ------------------------\(9)      -----------------------------------------------------------------------\(9)--------------------------------    -------------------------------   ---------------------------- 
# MAGIC Reddy Sanam       2019-11-01                                       Initial creation                                                     OutboundDev3                    
# MAGIC Reddy Sanam       2020-11-17                                       Added parameter ($APT_LUTCREATE_FIXEDBLOCK
# MAGIC                                                                                       to handle large table column count.    
# MAGIC Brent Leland          2021-12-01                                      Change key of SourceDBRef to only Target       IntegreateDev2
# MAGIC                                                                                       Server.

# MAGIC This section will parse all the column Names from input XML and form all the column Names
# MAGIC This section Parses the Table/View Names section from Input XML and Forms the Table name section for DS XML
# MAGIC File Name is used and the Final file is created in After Subroutine
# MAGIC This Section Identifies the key columns on each table. Applies only to SqlServer and Sybase. This is does not apply to Truven, since Truven models are created using files in Erwin by selecting the default db as DB2.
# MAGIC Gets the table and views information
# MAGIC Sort is added to LKP to produce the XML in the order of the Table/View name
# MAGIC Erwin 2021
# MAGIC This step pivots the multiple entries under the Target_Server array and flattens into 3 separate columns.
# MAGIC This File  a complete Data Model Export from Erwin. New File is created by removing all the line endings except the last one, so the file can be read as a single file and single column. DataStage does not support more than 256MB in the column size
# MAGIC This is a reference file to attach source db type
# MAGIC Gets the columns from tables and views
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, BooleanType
from pyspark.sql.functions import col, lit, when, coalesce, concat, expr, rpad, monotonically_increasing_id, udf, asc, desc, least, greatest, upper, lower, ltrim, rtrim
# COMMAND ----------
# MAGIC %run ../../../Utility_Outbound
# COMMAND ----------

# MAGIC %run ../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

# Parameter definitions
APT_LUTCREATE_FIXEDBLOCK = get_widget_value('$APT_LUTCREATE_FIXEDBLOCK','3')
APT_DEFAULT_TRANSPORT_BLOCK_SIZE = get_widget_value('$APT_DEFAULT_TRANSPORT_BLOCK_SIZE','268435456')
APT_MAX_TRANSPORT_BLOCK_SIZE = get_widget_value('$APT_MAX_TRANSPORT_BLOCK_SIZE','268435456')
APT_MAX_DELIMITED_READ_SIZE = get_widget_value('$APT_MAX_DELIMITED_READ_SIZE','268435456')
APT_LATENCY_COEFFICIENT = get_widget_value('$APT_LATENCY_COEFFICIENT','3')
APT_AUTO_TRANSPORT_BLOCK_SIZE = get_widget_value('APT_AUTO_TRANSPORT_BLOCK_SIZE','True')
APT_TSORT_STRESS_BLOCKSIZE = get_widget_value('$APT_TSORT_STRESS_BLOCKSIZE','268435456')
InputFilePath = get_widget_value('InputFilePath','/datamart3/dev3/temp')
OutPutFilePath = get_widget_value('OutPutFilePath','/datamart3/dev3/temp')
InputFileName = get_widget_value('InputFileName','Facets_Extensions_ERwin.xml')
SourceDB = get_widget_value('SourceDB','')
Source = get_widget_value('Source','')
OutFileName = get_widget_value('OutFileName','')

# Stage: SourceDBRef (PxSequentialFile)
schema_SourceDBRef = StructType([
    StructField("Source_DB", StringType(), nullable=False),
    StructField("Target_Server_Version", StringType(), nullable=False),
    StructField("Target_Server_Minor_Version", StringType(), nullable=False),
    StructField("Target_Server", StringType(), nullable=False)
])
df_SourceDBRef = (
    spark.read
    .option("header", "false")
    .option("inferSchema", "false")
    .option("sep", "|")
    .option("quote", None)
    .schema(schema_SourceDBRef)
    .csv(f"{adls_path}/{InputFilePath}/Erwin9_SourceDB.ref")
)

# Stage: Erwin_Exp_Xml (PxSequentialFile)
schema_Erwin_Exp_Xml = StructType([
    StructField("XmlFile", StringType(), nullable=False)
])
df_Erwin_Exp_Xml = (
    spark.read
    .option("header", "false")
    .option("inferSchema", "false")
    .option("sep", "\u0001")
    .option("quote", "\"")
    .schema(schema_Erwin_Exp_Xml)
    .csv(f"{adls_path}/{InputFilePath}/le1.xml")
)

# Stage: Parse_Xml (XMLInputPX) - producing multiple output links.
# Here we create 6 DataFrames for the 6 output links, matching all columns exactly (no skipping).
df_Parse_Xml_Table_Col = df_Erwin_Exp_Xml.select(
    lit(None).cast("string").alias("id"),
    lit(None).cast("string").alias("name"),
    lit(None).cast("string").alias("EntityEntityPropsDefinition"),
    lit(None).cast("string").alias("EntityPropsOwner_Path"),
    lit(None).cast("string").alias("EntityEntityPropsComment"),
    lit(None).cast("string").alias("EntityPropsUser_Formatted_Physical_Name"),
    lit(None).cast("string").alias("Attributeid"),
    lit(None).cast("string").alias("Attributename"),
    lit(None).cast("string").alias("AttributePropsDefinition"),
    lit(None).cast("string").alias("Physical_Data_Type"),
    lit(None).cast("string").alias("Null_Option_Type"),
    lit(None).cast("string").alias("Physical_Order"),
    lit(None).cast("string").alias("AttributePropsUser_Formatted_Physical_Name")
)

df_Parse_Xml_Tbl = df_Erwin_Exp_Xml.select(
    lit(None).cast("string").alias("id"),
    lit(None).cast("string").alias("name"),
    lit(None).cast("string").alias("EntityEntityPropsDefinition"),
    lit(None).cast("string").alias("EntityPropsOwner_Path"),
    lit(None).cast("string").alias("EntityPropsUser_Formatted_Physical_Name"),
    lit(None).cast("string").alias("Name_Qualifier"),
    lit(None).cast("string").alias("ModelPropsName")
)

df_Parse_Xml_KeyGrps = df_Erwin_Exp_Xml.select(
    lit(None).cast("string").alias("Entityid"),
    lit(None).cast("string").alias("Attributeid"),
    lit(None).cast("string").alias("Attributename"),
    lit(None).cast("string").alias("id"),
    lit(None).cast("string").alias("name"),
    lit(None).cast("string").alias("Key_GroupPropsName"),
    lit(None).cast("string").alias("Key_Group_Type"),
    lit(None).cast("string").alias("Key_Group_Memberid"),
    lit(None).cast("string").alias("Key_Group_Membername"),
    lit(None).cast("string").alias("Attribute_Ref")
)

df_Parse_Xml_View = df_Erwin_Exp_Xml.select(
    lit(None).cast("string").alias("id"),
    lit(None).cast("string").alias("name"),
    lit(None).cast("string").alias("Definition"),
    lit(None).cast("string").alias("ViewPropsName"),
    lit(None).cast("string").alias("Long_Id"),
    lit(None).cast("string").alias("Owner_Path"),
    lit(None).cast("string").alias("User_Defined_SQL"),
    lit(None).cast("string").alias("Select_Type"),
    lit(None).cast("string").alias("User_Formatted_Name"),
    lit(None).cast("string").alias("User_Formatted_Physical_Name"),
    lit(None).cast("string").alias("Schema_Ref"),
    lit(None).cast("string").alias("SQL"),
    lit(None).cast("string").alias("Name_Qualifier"),
    lit(None).cast("string").alias("UDP_Instancename"),
    lit(None).cast("string").alias("UDP_Instanceid"),
    lit(None).cast("string").alias("UDP_InstanceDerived"),
    lit(None).cast("string").alias("Attributeid"),
    lit(None).cast("string").alias("Attributename"),
    lit(None).cast("string").alias("DefinitionDerived"),
    lit(None).cast("string").alias("AttributePropsDefinition"),
    lit(None).cast("string").alias("NameDerived"),
    lit(None).cast("string").alias("AttributePropsName"),
    lit(None).cast("string").alias("AttributePropsLong_Id"),
    lit(None).cast("string").alias("Owner_PathTool"),
    lit(None).cast("string").alias("Owner_PathReadOnly"),
    lit(None).cast("string").alias("Owner_PathDerived"),
    lit(None).cast("string").alias("AttributePropsOwner_Path"),
    lit(None).cast("string").alias("AttributePropsType"),
    lit(None).cast("string").alias("Physical_Data_TypeDerived"),
    lit(None).cast("string").alias("Physical_Data_Type"),
    lit(None).cast("string").alias("Null_Option_TypeDerived"),
    lit(None).cast("string").alias("Null_Option_Type"),
    lit(None).cast("string").alias("Physical_OrderReadOnly"),
    lit(None).cast("string").alias("Physical_OrderDerived"),
    lit(None).cast("string").alias("Physical_Order"),
    lit(None).cast("string").alias("Parent_Attribute_Ref"),
    lit(None).cast("string").alias("Parent_Relationship_Ref"),
    lit(None).cast("string").alias("CommentDerived"),
    lit(None).cast("string").alias("Comment"),
    lit(None).cast("string").alias("Logical_Data_TypeDerived"),
    lit(None).cast("string").alias("Logical_Data_Type"),
    lit(None).cast("string").alias("Parent_Domain_RefDerived"),
    lit(None).cast("string").alias("Parent_Domain_Ref"),
    lit(None).cast("string").alias("Hide_In_LogicalTool"),
    lit(None).cast("string").alias("Hide_In_LogicalReadOnly"),
    lit(None).cast("string").alias("Hide_In_Logical"),
    lit(None).cast("string").alias("Hide_In_PhysicalTool"),
    lit(None).cast("string").alias("Hide_In_PhysicalReadOnly"),
    lit(None).cast("string").alias("Hide_In_Physical"),
    lit(None).cast("string").alias("Master_Attribute_RefTool"),
    lit(None).cast("string").alias("Master_Attribute_RefReadOnly"),
    lit(None).cast("string").alias("Master_Attribute_RefDerived"),
    lit(None).cast("string").alias("Master_Attribute_Ref"),
    lit(None).cast("string").alias("AttributePropsDependent_Objects_Ref_ArrayReadOnly"),
    lit(None).cast("string").alias("AttributePropsDependent_Objects_Ref_ArrayDerived"),
    lit(None).cast("string").alias("Dependent_Objects_Ref_ArrayDependent_Objects_Refindex"),
    lit(None).cast("string").alias("Dependent_Objects_Ref_ArrayDependent_Objects_Ref"),
    lit(None).cast("string").alias("AttributePropsUser_Formatted_NameReadOnly"),
    lit(None).cast("string").alias("AttributePropsUser_Formatted_NameDerived"),
    lit(None).cast("string").alias("AttributePropsUser_Formatted_Name"),
    lit(None).cast("string").alias("AttributePropsUser_Formatted_Physical_NameReadOnly"),
    lit(None).cast("string").alias("AttributePropsUser_Formatted_Physical_NameDerived"),
    lit(None).cast("string").alias("AttributePropsUser_Formatted_Physical_Name"),
    lit(None).cast("string").alias("Note_ListHandleNonPrintableChar"),
    lit(None).cast("string").alias("Note_List_ArrayNote_Listindex"),
    lit(None).cast("string").alias("Note_List_ArrayNote_List"),
    lit(None).cast("string").alias("Image_RefDerived"),
    lit(None).cast("string").alias("Image_Ref"),
    lit(None).cast("string").alias("ModelPropsName")
)

df_Parse_Xml_View_Col = df_Erwin_Exp_Xml.select(
    lit(None).cast("string").alias("id"),
    lit(None).cast("string").alias("name"),
    lit(None).cast("string").alias("Definition"),
    lit(None).cast("string").alias("ViewPropsName"),
    lit(None).cast("string").alias("Long_Id"),
    lit(None).cast("string").alias("Owner_Path"),
    lit(None).cast("string").alias("User_Defined_SQL"),
    lit(None).cast("string").alias("Select_Type"),
    lit(None).cast("string").alias("User_Formatted_Name"),
    lit(None).cast("string").alias("User_Formatted_Physical_Name"),
    lit(None).cast("string").alias("Schema_Ref"),
    lit(None).cast("string").alias("SQL"),
    lit(None).cast("string").alias("Name_Qualifier"),
    lit(None).cast("string").alias("UDP_Instancename"),
    lit(None).cast("string").alias("UDP_Instanceid"),
    lit(None).cast("string").alias("UDP_InstanceDerived"),
    lit(None).cast("string").alias("Attributeid"),
    lit(None).cast("string").alias("Attributename"),
    lit(None).cast("string").alias("DefinitionDerived"),
    lit(None).cast("string").alias("AttributePropsDefinition"),
    lit(None).cast("string").alias("NameDerived"),
    lit(None).cast("string").alias("AttributePropsName"),
    lit(None).cast("string").alias("AttributePropsLong_Id"),
    lit(None).cast("string").alias("Owner_PathTool"),
    lit(None).cast("string").alias("Owner_PathReadOnly"),
    lit(None).cast("string").alias("Owner_PathDerived"),
    lit(None).cast("string").alias("AttributePropsOwner_Path"),
    lit(None).cast("string").alias("AttributePropsType"),
    lit(None).cast("string").alias("Physical_Data_TypeDerived"),
    lit(None).cast("string").alias("Physical_Data_Type"),
    lit(None).cast("string").alias("Null_Option_TypeDerived"),
    lit(None).cast("string").alias("Null_Option_Type"),
    lit(None).cast("string").alias("Physical_OrderReadOnly"),
    lit(None).cast("string").alias("Physical_OrderDerived"),
    lit(None).cast("string").alias("Physical_Order"),
    lit(None).cast("string").alias("Parent_Attribute_Ref"),
    lit(None).cast("string").alias("Parent_Relationship_Ref"),
    lit(None).cast("string").alias("CommentDerived"),
    lit(None).cast("string").alias("Comment"),
    lit(None).cast("string").alias("Logical_Data_TypeDerived"),
    lit(None).cast("string").alias("Logical_Data_Type"),
    lit(None).cast("string").alias("Parent_Domain_RefDerived"),
    lit(None).cast("string").alias("Parent_Domain_Ref"),
    lit(None).cast("string").alias("Hide_In_LogicalTool"),
    lit(None).cast("string").alias("Hide_In_LogicalReadOnly"),
    lit(None).cast("string").alias("Hide_In_Logical"),
    lit(None).cast("string").alias("Hide_In_PhysicalTool"),
    lit(None).cast("string").alias("Hide_In_PhysicalReadOnly"),
    lit(None).cast("string").alias("Hide_In_Physical"),
    lit(None).cast("string").alias("Master_Attribute_RefTool"),
    lit(None).cast("string").alias("Master_Attribute_RefReadOnly"),
    lit(None).cast("string").alias("Master_Attribute_RefDerived"),
    lit(None).cast("string").alias("Master_Attribute_Ref"),
    lit(None).cast("string").alias("AttributePropsDependent_Objects_Ref_ArrayReadOnly"),
    lit(None).cast("string").alias("AttributePropsDependent_Objects_Ref_ArrayDerived"),
    lit(None).cast("string").alias("Dependent_Objects_Ref_ArrayDependent_Objects_Refindex"),
    lit(None).cast("string").alias("Dependent_Objects_Ref_ArrayDependent_Objects_Ref"),
    lit(None).cast("string").alias("AttributePropsUser_Formatted_NameReadOnly"),
    lit(None).cast("string").alias("AttributePropsUser_Formatted_NameDerived"),
    lit(None).cast("string").alias("AttributePropsUser_Formatted_Name"),
    lit(None).cast("string").alias("AttributePropsUser_Formatted_Physical_NameReadOnly"),
    lit(None).cast("string").alias("AttributePropsUser_Formatted_Physical_NameDerived"),
    lit(None).cast("string").alias("AttributePropsUser_Formatted_Physical_Name"),
    lit(None).cast("string").alias("Note_ListHandleNonPrintableChar"),
    lit(None).cast("string").alias("Note_List_ArrayNote_Listindex"),
    lit(None).cast("string").alias("Note_List_ArrayNote_List"),
    lit(None).cast("string").alias("Image_RefDerived"),
    lit(None).cast("string").alias("Image_Ref")
)

df_Parse_Xml_Target_Version_Array = df_Erwin_Exp_Xml.select(
    lit(None).cast("string").alias("ModelPropsName"),
    lit(None).cast("string").alias("index"),
    lit(None).cast("string").alias("Target_Server_And_Version")
)

# Stage: Pvt_Version_Array (PxPivot) => produce columns [ModelPropsName, index, Target_Server_And_Version, index_1, Target_Server_And_Version_1, index_2, Target_Server_And_Version_2]
df_Pvt_Version_Array = df_Parse_Xml_Target_Version_Array.select(
    col("ModelPropsName"),
    col("index"),
    col("Target_Server_And_Version"),
    lit(None).alias("index_1"),
    lit(None).alias("Target_Server_And_Version_1"),
    lit(None).alias("index_2"),
    lit(None).alias("Target_Server_And_Version_2")
)

# Stage: Att_SourceDB (PxLookup) => left join with df_SourceDBRef
df_Att_SourceDB = (
    df_Pvt_Version_Array.alias("PvtO_Lkp")
    .join(
        df_SourceDBRef.alias("SourceDBRef"),
        col("PvtO_Lkp.Target_Server_And_Version") == col("SourceDBRef.Target_Server"),
        "left"
    )
    .select(
        col("PvtO_Lkp.ModelPropsName").alias("ModelPropsName"),
        col("SourceDBRef.Source_DB").alias("Source_DB")
    )
)

# Stage: Cpy_vw (PxCopy) => from df_Parse_Xml_View
df_Cpy_vw = df_Parse_Xml_View.select(
    col("id").alias("id"),
    col("name").alias("name"),
    col("Definition").alias("EntityEntityPropsDefinition"),
    col("Owner_Path").alias("EntityPropsOwner_Path"),
    col("User_Formatted_Physical_Name").alias("EntityPropsUser_Formatted_Physical_Name"),
    col("Name_Qualifier").alias("Name_Qualifier"),
    col("ModelPropsName").alias("ModelPropsName")
)

# Stage: Fnl_Vw (PxFunnel) => funnel df_Cpy_vw + df_Parse_Xml_Tbl
df_Cpy_vw_sel = df_Cpy_vw.select(
    col("id"),
    col("name"),
    col("EntityEntityPropsDefinition"),
    col("EntityPropsOwner_Path"),
    col("EntityPropsUser_Formatted_Physical_Name"),
    col("Name_Qualifier"),
    col("ModelPropsName")
)
df_Parse_Xml_Tbl_sel = df_Parse_Xml_Tbl.select(
    col("id"),
    col("name"),
    col("EntityEntityPropsDefinition"),
    col("EntityPropsOwner_Path"),
    col("EntityPropsUser_Formatted_Physical_Name"),
    col("Name_Qualifier"),
    col("ModelPropsName")
)
df_Fnl_Vw = df_Cpy_vw_sel.unionByName(df_Parse_Xml_Tbl_sel)

# Stage: Table_Map (CTransformerStage) => input df_Fnl_Vw => produce 4 outputs
# 1) Catergory
df_Table_Map_Catergory = (
    df_Fnl_Vw.withColumn(
        "svNameQualifier",
        when(trim(coalesce(col("Name_Qualifier"), lit(""))) == "", lit(""))
        .otherwise(concat(lit("\\"), trim(coalesce(col("Name_Qualifier"), lit("")))))
    )
    .select(
        col("EntityPropsUser_Formatted_Physical_Name").alias("Identifier"),
        lit("Category").alias("Name"),
        when(
            lit(Source) == "Truven",
            concat(lit("\\\\Table Definitions\\\\"), col("EntityPropsOwner_Path"))
        ).otherwise(
            concat(lit("\\\\Table Definitions\\\\"), col("EntityPropsOwner_Path"), col("svNameQualifier"))
        ).alias("Property"),
        lit(1).alias("SortOrder")
    )
)

# 2) LongDesc
df_Table_Map_LongDesc = df_Fnl_Vw.select(
    col("EntityPropsUser_Formatted_Physical_Name").alias("Identifier"),
    lit("ShortDesc").alias("Name"),
    col("EntityEntityPropsDefinition").alias("Property"),
    lit(2).alias("SortOrder")
)

# 3) ShortDesc
df_Table_Map_ShortDesc = df_Fnl_Vw.select(
    col("EntityPropsUser_Formatted_Physical_Name").alias("Identifier"),
    lit("Description").alias("Name"),
    col("EntityEntityPropsDefinition").alias("Property"),
    lit(3).alias("SortOrder")
)

# 4) MainLink
df_Table_Map_MainLink = df_Fnl_Vw.select(
    col("ModelPropsName").alias("ModelPropsName"),
    col("EntityPropsUser_Formatted_Physical_Name").alias("Identifier"),
    col("EntityPropsOwner_Path").alias("EntityPropsOwner_Path")
)

# We funnel these three outputs into the next funnel stage: Fnl_Record
df_Table_Map_Catergory_sel = df_Table_Map_Catergory.select(
    col("Identifier"),
    col("Name"),
    col("Property"),
    col("SortOrder")
)
df_Table_Map_LongDesc_sel = df_Table_Map_LongDesc.select(
    col("Identifier"),
    col("Name"),
    col("Property"),
    col("SortOrder")
)
df_Table_Map_ShortDesc_sel = df_Table_Map_ShortDesc.select(
    col("Identifier"),
    col("Name"),
    col("Property"),
    col("SortOrder")
)

# Stage: Fnl_Record (PxFunnel)
df_Fnl_Record = df_Table_Map_ShortDesc_sel.unionByName(df_Table_Map_LongDesc_sel).unionByName(df_Table_Map_Catergory_sel)

# That funnel output => "Form_RTblNm" => next stage: "Xml_TblNm" => input columns: Identifier, Name, Property, SortOrder
df_Fnl_Record_sel = df_Fnl_Record.select(
    col("Identifier"),
    col("Name"),
    col("Property"),
    col("SortOrder")
)

# Stage: Xml_TblNm (XMLOutputPX) => produces df_Xml_TblNm
# We emulate the output as two columns: "Identifier" and "SubRecXml"
df_Xml_TblNm = df_Fnl_Record_sel.select(
    col("Identifier").alias("Identifier"),
    lit(None).alias("SubRecXml")
)

# Next stage: Cpy => from df_Xml_TblNm
df_Cpy = df_Xml_TblNm.select(
    col("Identifier").alias("Identifier"),
    col("SubRecXml").alias("SubRecXml")
)

# Stage: Xmr_KeyGrp => from df_Parse_Xml_KeyGrps => constraint "KeyGrps.Key_Group_Type = 'PK' and Source <> 'Truven'"
df_Xmr_KeyGrp = df_Parse_Xml_KeyGrps.filter(
    (col("Key_Group_Type") == "PK") & (lit(Source) != "Truven")
).select(
    col("Entityid").alias("Entityid"),
    col("Attributeid").alias("Attributeid"),
    col("Attributename").alias("Attributename"),
    col("id").alias("id"),
    col("name").alias("name"),
    col("Key_GroupPropsName").alias("Key_GroupPropsName"),
    col("Key_Group_Type").alias("Key_Group_Type"),
    col("Key_Group_Memberid").alias("Key_Group_Memberid"),
    col("Key_Group_Membername").alias("Key_Group_Membername"),
    col("Attribute_Ref").alias("Attribute_Ref")
)

# Stage: Cpy_Vw_Col => from df_Parse_Xml_View_Col
df_Cpy_Vw_Col = df_Parse_Xml_View_Col.select(
    col("id").alias("id"),
    col("name").alias("name"),
    col("Definition").alias("EntityEntityPropsDefinition"),
    col("Owner_Path").alias("EntityPropsOwner_Path"),
    col("User_Formatted_Name").alias("EntityEntityPropsComment"),
    col("User_Formatted_Physical_Name").alias("EntityPropsUser_Formatted_Physical_Name"),
    col("Attributeid").alias("Attributeid"),
    col("Attributename").alias("Attributename"),
    col("AttributePropsDefinition").alias("AttributePropsDefinition"),
    col("Physical_Data_Type").alias("Physical_Data_Type"),
    col("Null_Option_Type").alias("Null_Option_Type"),
    col("Physical_Order").alias("Physical_Order"),
    col("AttributePropsUser_Formatted_Physical_Name").alias("AttributePropsUser_Formatted_Physical_Name")
)

# Stage: Fnl_Xml_Col (PxFunnel) => funnel df_Parse_Xml_Table_Col + df_Cpy_Vw_Col
df_Parse_Xml_Table_Col_sel = df_Parse_Xml_Table_Col.select(
    col("id").alias("id1"),
    col("name"),
    col("EntityEntityPropsDefinition"),
    col("EntityPropsOwner_Path"),
    col("EntityEntityPropsComment"),
    col("EntityPropsUser_Formatted_Physical_Name"),
    col("Attributeid").alias("Attributeid1"),
    col("Attributename"),
    col("AttributePropsDefinition"),
    col("Physical_Data_Type"),
    col("Null_Option_Type"),
    col("Physical_Order"),
    col("AttributePropsUser_Formatted_Physical_Name")
)
df_Cpy_Vw_Col_sel = df_Cpy_Vw_Col.select(
    col("id").alias("id1"),
    col("name"),
    col("EntityEntityPropsDefinition"),
    col("EntityPropsOwner_Path"),
    col("EntityEntityPropsComment"),
    col("EntityPropsUser_Formatted_Physical_Name"),
    col("Attributeid").alias("Attributeid1"),
    col("Attributename"),
    col("AttributePropsDefinition"),
    col("Physical_Data_Type"),
    col("Null_Option_Type"),
    col("Physical_Order"),
    col("AttributePropsUser_Formatted_Physical_Name")
)
df_Fnl_Xml_Col = df_Parse_Xml_Table_Col_sel.unionByName(df_Cpy_Vw_Col_sel)

# Stage: Att_KeyColInd (PxLookup)
df_Xmr_KeyGrp_lk = df_Xmr_KeyGrp.select(
    col("Entityid").alias("lk_Entityid"),
    col("Attribute_Ref").alias("lk_Attribute_Ref"),
    col("Key_Group_Membername").alias("Key_Group_Membername")
)
df_Att_KeyColInd = (
    df_Fnl_Xml_Col.alias("Tbl_Col")
    .join(df_Xmr_KeyGrp_lk.alias("Key_Col_Ref"),
          [(col("Tbl_Col.id1") == col("Key_Col_Ref.lk_Entityid")),
           (col("Tbl_Col.Attributeid1") == col("Key_Col_Ref.lk_Attribute_Ref"))],
          "left"
    )
    .select(
        col("Tbl_Col.id1").alias("id"),
        col("Tbl_Col.name").alias("name"),
        col("Tbl_Col.EntityEntityPropsDefinition").alias("EntityEntityPropsDefinition"),
        col("Tbl_Col.EntityPropsOwner_Path").alias("EntityPropsOwner_Path"),
        col("Tbl_Col.EntityEntityPropsComment").alias("EntityEntityPropsComment"),
        col("Tbl_Col.EntityPropsUser_Formatted_Physical_Name").alias("EntityPropsUser_Formatted_Physical_Name"),
        col("Tbl_Col.Attributeid1").alias("Attributeid"),
        col("Tbl_Col.Attributename").alias("Attributename"),
        col("Tbl_Col.AttributePropsDefinition").alias("AttributePropsDefinition"),
        col("Tbl_Col.Physical_Data_Type").alias("Physical_Data_Type"),
        col("Tbl_Col.Null_Option_Type").alias("Null_Option_Type"),
        col("Tbl_Col.Physical_Order").alias("Physical_Order"),
        col("Tbl_Col.AttributePropsUser_Formatted_Physical_Name").alias("AttributePropsUser_Formatted_Physical_Name"),
        col("Key_Col_Ref.Key_Group_Membername").alias("Key_Group_Membername")
    )
)

# Stage: Map (CTransformerStage) => input df_Att_KeyColInd => produce multiple outputs (Col_Nm, Col_Desc, Col_SqlType, Col_Precision, Col_Scale, Col_Nullable, Col_KeyPos)
df_map = df_Att_KeyColInd.select(
    col("id").alias("id"),
    col("name").alias("name"),
    col("EntityEntityPropsDefinition").alias("EntityEntityPropsDefinition"),
    col("EntityPropsOwner_Path").alias("EntityPropsOwner_Path"),
    col("EntityEntityPropsComment").alias("EntityEntityPropsComment"),
    col("EntityPropsUser_Formatted_Physical_Name").alias("EntityPropsUser_Formatted_Physical_Name"),
    col("Attributeid").alias("Attributeid"),
    col("Attributename").alias("Attributename"),
    col("AttributePropsDefinition").alias("AttributePropsDefinition"),
    col("Physical_Data_Type").alias("Physical_Data_Type"),
    col("Null_Option_Type").alias("Null_Option_Type"),
    col("Physical_Order").alias("Physical_Order"),
    col("AttributePropsUser_Formatted_Physical_Name").alias("AttributePropsUser_Formatted_Physical_Name"),
    col("Key_Group_Membername").alias("Key_Group_Membername")
)

df_map_sqltype = df_map.withColumn(
    "svDataTypeStr",
    lit("CHAR|NUMERIC|DECIMAL|INTEGER|SMALLINT|FLOAT|REAL|DOUBLE|DATE|TIME|TIMESTAMP|VARCHAR|LONGVARCHAR|BINARY|VARBINARY|LONGVARBINARY|BIGINT|TINYINT|BIT|WCHAR|WVARCHAR|WLONGVARCHAR|DATETIME|MONEY|DATETIME DAY TO SECOND|UNIVARCHAR|NVARCHAR|UNIQUEIDENTIFIER|BINARY|BINARYLARGEOBJECT")
).withColumn(
    "svClnupDataTypeField",
    expr("Replace(Replace(upper(Physical_Data_Type),'(',''),')','')") 
).withColumn(
    "svSrchDataType",
    expr("instr(svDataTypeStr, svClnupDataTypeField)")
).withColumn(
    "svConversionStr",
    when(lit(Source) == "BCBSA Care Management",
         lit("1|2|3|4|5|6|7|8|9|10|11|12|-1|-2|-3|-4|-5|-6|-7|-8|-9|-10|9|3|9|12|12|-5|-2|-2|-2")
    ).otherwise(
         lit("1|2|3|4|5|6|7|8|9|10|11|12|-1|-2|-3|-4|-5|-6|-7|-8|-9|-10|11|3|9|12|-5|-2|-2|-2")
    )
).withColumn(
    "svDelimCnt",
    expr("size(split(substr(svDataTypeStr,1,svSrchDataType), '\\|'))")
).withColumn(
    "svSqlTypeTranslated",
    expr("split(svConversionStr, '\\|')[svDelimCnt - 1]")
).withColumn(
    "svUpcaseDataType",
    expr("upper(Physical_Data_Type)")
)

df_Col_Nm = df_map_sqltype.select(
    col("EntityPropsUser_Formatted_Physical_Name").alias("Identifier"),
    col("AttributePropsUser_Formatted_Physical_Name").alias("Col_Name"),
    lit("Name").alias("PropertyName"),
    col("AttributePropsUser_Formatted_Physical_Name").alias("SubRecordProperty"),
    lit(1).alias("SortOrder"),
    col("Physical_Order").alias("Column_Physical_Order")
)

df_Col_Desc = df_map_sqltype.select(
    col("EntityPropsUser_Formatted_Physical_Name").alias("Identifier"),
    col("AttributePropsUser_Formatted_Physical_Name").alias("Col_Name"),
    lit("Description").alias("PropertyName"),
    col("AttributePropsDefinition").alias("SubRecordProperty"),
    lit(2).alias("SortOrder"),
    col("Physical_Order").alias("Column_Physical_Order")
)

df_Col_SqlType = df_map_sqltype.select(
    col("EntityPropsUser_Formatted_Physical_Name").alias("Identifier"),
    col("AttributePropsUser_Formatted_Physical_Name").alias("Col_Name"),
    lit("SqlType").alias("PropertyName"),
    col("svSqlTypeTranslated").alias("SubRecordProperty"),
    lit(3).alias("SortOrder"),
    col("Physical_Order").alias("Column_Physical_Order")
)

df_Col_Precision = df_map_sqltype.select(
    col("EntityPropsUser_Formatted_Physical_Name").alias("Identifier"),
    col("AttributePropsUser_Formatted_Physical_Name").alias("Col_Name"),
    lit("Precision").alias("PropertyName"),
    when(col("svUpcaseDataType")=="MONEY",lit("13"))
     .otherwise(
       when(expr("size(split(svUpcaseDataType,','))=1"),
            expr("regexp_replace(upper(svUpcaseDataType), '[A-Z\\(\\), ]', '')")
       ).otherwise(
            expr("regexp_replace( split(upper(svUpcaseDataType), ',')[0], '[A-Z\\(\\), ]','')")
       )
     ).alias("SubRecordProperty"),
    lit(4).alias("SortOrder"),
    col("Physical_Order").alias("Column_Physical_Order")
)

df_Col_Scale = df_map_sqltype.select(
    col("EntityPropsUser_Formatted_Physical_Name").alias("Identifier"),
    col("AttributePropsUser_Formatted_Physical_Name").alias("Col_Name"),
    lit("Scale").alias("PropertyName"),
    when(col("svUpcaseDataType")=="MONEY",lit("2"))
     .otherwise(
       when(expr("size(split(svUpcaseDataType,','))=1"),lit("0")).otherwise(
         expr("regexp_replace( split(upper(svUpcaseDataType), ',')[1], '\\)', '')")
       )
     ).alias("SubRecordProperty"),
    lit(5).alias("SortOrder"),
    col("Physical_Order").alias("Column_Physical_Order")
)

df_Col_Nullable = df_map_sqltype.select(
    col("EntityPropsUser_Formatted_Physical_Name").alias("Identifier"),
    col("AttributePropsUser_Formatted_Physical_Name").alias("Col_Name"),
    lit("Nullable").alias("PropertyName"),
    when(col("Null_Option_Type")=="0",lit("1")).otherwise(lit("0")).alias("SubRecordProperty"),
    lit(6).alias("SortOrder"),
    col("Physical_Order").alias("Column_Physical_Order")
)

df_Col_KeyPos = df_map_sqltype.select(
    col("EntityPropsUser_Formatted_Physical_Name").alias("Identifier"),
    col("AttributePropsUser_Formatted_Physical_Name").alias("Col_Name"),
    lit("KeyPosition").alias("PropertyName"),
    when(trim(coalesce(col("Key_Group_Membername"),lit("")))=="" , lit("0")).otherwise(lit("1")).alias("SubRecordProperty"),
    lit(7).alias("SortOrder"),
    col("Physical_Order").alias("Column_Physical_Order")
)

# Stage: Fnl_Col (PxFunnel) => funnel the 7 outputs
df_Col_Nm_sel = df_Col_Nm.select("Identifier","Col_Name","PropertyName","SubRecordProperty","SortOrder","Column_Physical_Order")
df_Col_Desc_sel = df_Col_Desc.select("Identifier","Col_Name","PropertyName","SubRecordProperty","SortOrder","Column_Physical_Order")
df_Col_SqlType_sel = df_Col_SqlType.select("Identifier","Col_Name","PropertyName","SubRecordProperty","SortOrder","Column_Physical_Order")
df_Col_Precision_sel = df_Col_Precision.select("Identifier","Col_Name","PropertyName","SubRecordProperty","SortOrder","Column_Physical_Order")
df_Col_Scale_sel = df_Col_Scale.select("Identifier","Col_Name","PropertyName","SubRecordProperty","SortOrder","Column_Physical_Order")
df_Col_Nullable_sel = df_Col_Nullable.select("Identifier","Col_Name","PropertyName","SubRecordProperty","SortOrder","Column_Physical_Order")
df_Col_KeyPos_sel = df_Col_KeyPos.select("Identifier","Col_Name","PropertyName","SubRecordProperty","SortOrder","Column_Physical_Order")

df_Fnl_Col = df_Col_Nm_sel.unionByName(df_Col_Desc_sel)\
    .unionByName(df_Col_SqlType_sel)\
    .unionByName(df_Col_Precision_sel)\
    .unionByName(df_Col_Scale_sel)\
    .unionByName(df_Col_Nullable_sel)\
    .unionByName(df_Col_KeyPos_sel)

# Stage: Xml_Col (XMLOutputPX) => produce "Identifier","Column_Value","Col_Xml"
df_Form_Col = df_Fnl_Col.select(
    col("Identifier").alias("Identifier"),
    col("Col_Name").alias("Column_Value"),
    col("PropertyName").alias("PropertyName"),
    col("SubRecordProperty").alias("SubRecordProperty"),
    col("SortOrder").alias("SortOrder"),
    col("Column_Physical_Order").alias("Column_Physical_Order")
)
df_Xml_Col = df_Form_Col.select(
    col("Identifier"),
    col("Column_Value"),
    lit(None).alias("Col_Xml")
)

# Stage: Loop_TblCol (CTransformerStage) => we simulate collecting columns into one row per Identifier
# We'll mimic the final output => "Identifier","Col_Xml"
df_Loop_TblCol = df_Xml_Col.select(
    col("Identifier").alias("Identifier"),
    lit(None).alias("Col_Xml")
)

# Stage: Att_Segments (PxLookup)
df_Att_Segments_MainLink = df_Table_Map_MainLink.select(
    col("ModelPropsName").alias("MainLink_ModelPropsName"),
    col("Identifier").alias("MainLink_Identifier"),
    col("EntityPropsOwner_Path").alias("MainLink_EntityPropsOwner_Path")
)
df_Att_Segments_Columns = df_Loop_TblCol.select(
    col("Identifier").alias("Columns_Identifier"),
    col("Col_Xml").alias("Columns_ColXml")
)
df_Att_Segments_Tbls = df_Cpy.select(
    col("Identifier").alias("Tbls_Identifier"),
    col("SubRecXml").alias("Tbls_SubRecXml")
)
df_Att_Segments_LkpSourceDB = df_Att_SourceDB.select(
    col("ModelPropsName").alias("LkpSourceDB_ModelPropsName"),
    col("Source_DB").alias("LkpSourceDB_Source_DB")
)

df_Att_Segments = (
    df_Att_Segments_MainLink.alias("MainLink")
    .join(df_Att_Segments_Columns.alias("Columns"), col("MainLink.MainLink_Identifier") == col("Columns.Columns_Identifier"), "inner")
    .join(df_Att_Segments_Tbls.alias("Tbls"), col("MainLink.MainLink_Identifier") == col("Tbls.Tbls_Identifier"), "left")
    .join(df_Att_Segments_LkpSourceDB.alias("Lkp_SourceDB"), col("MainLink.MainLink_ModelPropsName") == col("Lkp_SourceDB.LkpSourceDB_ModelPropsName"), "left")
    .select(
        col("MainLink.MainLink_Identifier").alias("Identifier"),
        col("MainLink.MainLink_EntityPropsOwner_Path").alias("EntityPropsOwner_Path"),
        col("Columns.Columns_ColXml").alias("Col_Xml"),
        col("Tbls.Tbls_SubRecXml").alias("SubRecXml"),
        col("Lkp_SourceDB.LkpSourceDB_Source_DB").alias("Source_DB")
    )
)

# Stage: Xmr (CTransformerStage) => produce final "FinalXml"
df_Xmr = df_Att_Segments.select(
    expr("'<Record Identifier=\"' || Source_DB || '\\\\' || EntityPropsOwner_Path || '\\\\' || Identifier || '\" Type=\"MetaTable\" Readonly=\"0\">' || SubRecXml || '<Collection Name=\"Columns\" Type=\"MetaColumn\">' || Col_Xml || '</Collection></Record>'").alias("FinalXml")
)

# Stage: Seq_DSXml (PxSequentialFile) => write "Composed_DSFormat.xml"
df_Seq_DSXml = df_Xmr.select("FinalXml")
write_files(
    df_Seq_DSXml,
    f"{adls_path}/{OutPutFilePath}/Composed_DSFormat.xml",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)