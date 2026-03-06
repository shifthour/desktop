#!/usr/bin/python3

from npadf import *

def IdsDMDrugClmAccumImpctExtrActivities(ctx):
  def dfIdsDMDrugClmAccumImpctExtr():
    def DB2CDMPPNGCouponNm():
       ds_ls_db = LinkedServiceReference(type='LinkedServiceReference',reference_name=ctx.sqldb_linked_service)
       df_sources_inline = DataFlowSource(name='DB2CDMPPNGCouponNm',linked_service=ds_ls_db)
       return DataflowSegment([df_sources_inline])
    def DRUGCLMACCUMIMPCT():
       ds_ls_db = LinkedServiceReference(type='LinkedServiceReference',reference_name=ctx.sqldb_linked_service)
       df_sources_inline = DataFlowSource(name='DRUGCLMACCUMIMPCT',linked_service=ds_ls_db)
       return DataflowSegment([df_sources_inline])
    def TransCoupon():
       return DataflowSegment([
         Transformation(name='TransCouponDerived'),
         Transformation(name='TransCoupon')])
    def DB2CDMPPNGSrcCd():
       ds_ls_db = LinkedServiceReference(type='LinkedServiceReference',reference_name=ctx.sqldb_linked_service)
       df_sources_inline = DataFlowSource(name='DB2CDMPPNGSrcCd',linked_service=ds_ls_db)
       return DataflowSegment([df_sources_inline])
    def LkupCodes():
       return DataflowSegment([
         Transformation(name="LkupCodes"),
         Transformation(name="LkupCodesJoin1"),Transformation(name="LkupCodesDerived1"),
         Transformation(name="LkupCodesJoin2"),Transformation(name="LkupCodesDerived2")])
    def TransSrcSys():
       return DataflowSegment([
         Transformation(name='TransSrcSysDerived'),
         Transformation(name='TransSrcSys')])
    def CLMDMDRUGCLMACCUMIMPCTextract():
       ds_ls_blob = LinkedServiceReference(type='LinkedServiceReference',reference_name=ctx.file_linked_service)
       df_sinks_inline = DataFlowSink(name='CLMDMDRUGCLMACCUMIMPCTextract', linked_service=ds_ls_blob)
       return DataflowSegment([df_sinks_inline])
    dataflowName = "IdsDMDrugClmAccumImpctExtr"
    buffer = DataflowBuffer()
    buffer.append(DB2CDMPPNGCouponNm())
    buffer.append(DRUGCLMACCUMIMPCT())
    buffer.append(TransCoupon())
    buffer.append(DB2CDMPPNGSrcCd())
    buffer.append(LkupCodes())
    buffer.append(TransSrcSys())
    buffer.append(CLMDMDRUGCLMACCUMIMPCTextract())
    dataflow = MappingDataFlow(
      folder = DataFlowFolder(name="WebDataMart/Claim/Claim"),
      sources = buffer.sources,
      sinks = buffer.sinks,
      transformations = buffer.transformations,
      script = f"""parameters{{
        IDSAcct as string ,
        IDSDB as string ("{ctx.IDSDB}"),
        FilePath as string ("{ctx.FilePath}"),
        IDSPW as string ,
        IDSOwner as string ("{ctx.IDSOwner}"),
        RunCycle as string 
      }}
    source(output(
        SRC_CD as integer,
        TRGT_CD_NM as string,
        TRGT_CD as string),
      allowSchemaDrift: true,
      validateSchema: false,
      format: 'query',
      store: 'sqlserver',
      query: ("SELECT CD_MPPNG_SK as SRC_CD,TRGT_CD_NM,TRGT_CD\\n  FROM " + toString($IDSOwner) + ".CD_MPPNG \\n WHERE SRC_CLCTN_CD   = 'OPTUMRX' \\n   AND SRC_SYS_CD     = 'OPTUMRX' \\n   AND SRC_DOMAIN_NM  = 'NCP COUPON TYPE' \\n   AND TRGT_CLCTN_CD  = 'IDS' \\n   AND TRGT_DOMAIN_NM = 'NCP COUPON TYPE' \\nunion\\nSELECT \\n CD_MPPNG_SK as SRC_CD,TRGT_CD_NM,TRGT_CD\\n  FROM " + toString($IDSOwner) + ".CD_MPPNG where  CD_MPPNG_SK='1'\\n"),
      schemaName: '',
      tableName: '',
      isolationLevel: 'READ_UNCOMMITTED') ~> DB2CDMPPNGCouponNm
    source(output(
        DRUG_CLM_SK as integer,
        SRC_SYS_CD_SK as integer,
        CLM_ID as string,
        CRT_RUN_CYC_EXCTN_SK as integer,
        LAST_UPDT_RUN_CYC_EXCTN_SK as integer,
        NCP_COUPON_TYP_CD_SK as integer,
        CCAA_APLD_IN as string,
        CCAA_COUPON_AMT as decimal(13, 2),
        FMLY_ACCUM_DEDCT_AMT as decimal(13, 2),
        FMLY_ACCUM_OOP_AMT as decimal(13, 2),
        INDV_ACCUM_DEDCT_AMT as decimal(13, 2),
        INDV_ACCUM_OOP_AMT as decimal(13, 2),
        INDV_APLD_DEDCT_AMT as decimal(13, 2),
        INDV_APLD_OOP_AMT as decimal(13, 2)),
      allowSchemaDrift: true,
      validateSchema: false,
      format: 'query',
      store: 'sqlserver',
      query: ("SELECT \\n	CLM.DRUG_CLM_SK, \\n	CLM.SRC_SYS_CD_SK, \\n	CLM.CLM_ID, \\n	CLM.CRT_RUN_CYC_EXCTN_SK, \\n	CLM.LAST_UPDT_RUN_CYC_EXCTN_SK, \\n	CLM.NCP_COUPON_TYP_CD_SK, \\n	CLM.CCAA_APLD_IN, \\n	CLM.CCAA_COUPON_AMT, \\n	CLM.FMLY_ACCUM_DEDCT_AMT, \\n	CLM.FMLY_ACCUM_OOP_AMT, \\n                CLM.INDV_ACCUM_DEDCT_AMT, \\n	CLM.INDV_ACCUM_OOP_AMT, \\n	CLM.INDV_APLD_DEDCT_AMT, \\n	CLM.INDV_APLD_OOP_AMT\\nFROM\\n    " + toString($IDSOwner) + ".DRUG_CLM_ACCUM_IMPCT CLM,\\n " + toString($IDSOwner) + ".W_WEBDM_ETL_DRVR DRVR\\nWHERE  \\nDRVR.SRC_SYS_CD_SK = CLM.SRC_SYS_CD_SK  AND\\nDRVR.CLM_ID = CLM.CLM_ID \\n"),
      schemaName: '',
      tableName: '',
      isolationLevel: 'READ_UNCOMMITTED') ~> DRUGCLMACCUMIMPCT
    DRUGCLMACCUMIMPCT
        derive(
              SRC_SYS_CD_SK = SRC_SYS_CD_SK,
              CLM_ID = CLM_ID,
              CCAA_APLD_IN = CCAA_APLD_IN,
              CCAA_COUPON_AMT = CCAA_COUPON_AMT,
              FMLY_ACCUM_DEDCT_AMT = FMLY_ACCUM_DEDCT_AMT,
              FMLY_ACCUM_OOP_AMT = FMLY_ACCUM_OOP_AMT,
              INDV_ACCUM_DEDCT_AMT = INDV_ACCUM_DEDCT_AMT,
              INDV_ACCUM_OOP_AMT = INDV_ACCUM_OOP_AMT,
              INDV_APLD_DEDCT_AMT = INDV_APLD_DEDCT_AMT,
              INDV_APLD_OOP_AMT = INDV_APLD_OOP_AMT,
              NCP_COUPON_TYP_CD = NCP_COUPON_TYP_CD_SK) ~> TransCouponDerived
        TransCouponDerived
        select(mapColumn(
            SRC_SYS_CD_SK,
            CLM_ID,
            CCAA_APLD_IN,
            CCAA_COUPON_AMT,
            FMLY_ACCUM_DEDCT_AMT,
            FMLY_ACCUM_OOP_AMT,
            INDV_ACCUM_DEDCT_AMT,
            INDV_ACCUM_OOP_AMT,
            INDV_APLD_DEDCT_AMT,
            INDV_APLD_OOP_AMT,
            NCP_COUPON_TYP_CD
        ),
        skipDuplicateMapInputs: true,
        skipDuplicateMapOutputs: true) ~> TransCoupon
    source(output(
        CD_MPPNG_SK as integer,
        TRGT_CD as string),
      allowSchemaDrift: true,
      validateSchema: false,
      format: 'query',
      store: 'sqlserver',
      query: ("SELECT \\nCD_MPPNG_SK,\\nTRGT_CD\\nFROM " + toString($IDSOwner) + ".CD_MPPNG \\nWHERE\\nTRGT_DOMAIN_NM = 'SOURCE SYSTEM'\\n"),
      schemaName: '',
      tableName: '',
      isolationLevel: 'READ_UNCOMMITTED') ~> DB2CDMPPNGSrcCd
    {ctx.joinScript(joinType='left',
      master=['TransCoupon', 'SRC_SYS_CD_SK,CLM_ID,CCAA_APLD_IN,CCAA_COUPON_AMT,FMLY_ACCUM_DEDCT_AMT,FMLY_ACCUM_OOP_AMT,INDV_ACCUM_DEDCT_AMT,INDV_ACCUM_OOP_AMT,INDV_APLD_DEDCT_AMT,INDV_APLD_OOP_AMT,NCP_COUPON_TYP_CD'],
      transformationName='LkupCodes',
      references=[['DB2CDMPPNGCouponNm', ctx.joinConditionForLookup(master={'name':'TransCoupon','alias':'Lnk_Trans_Extr','types':{'SRC_SYS_CD_SK':'integer','CLM_ID':'string','CCAA_APLD_IN':'string','CCAA_COUPON_AMT':'decimal(13, 2)','FMLY_ACCUM_DEDCT_AMT':'decimal(13, 2)','FMLY_ACCUM_OOP_AMT':'decimal(13, 2)','INDV_ACCUM_DEDCT_AMT':'decimal(13, 2)','INDV_ACCUM_OOP_AMT':'decimal(13, 2)','INDV_APLD_DEDCT_AMT':'decimal(13, 2)','INDV_APLD_OOP_AMT':'decimal(13, 2)','NCP_COUPON_TYP_CD':'integer'}}, reference={'name':'DB2CDMPPNGCouponNm','alias':'Lnk_Lkup_Coupon_Nm','types':{'SRC_CD':'integer','TRGT_CD_NM':'string','TRGT_CD':'string'}}, joinFields=[{'master':'Lnk_Trans_Extr.NCP_COUPON_TYP_CD','reference':'Lnk_Lkup_Coupon_Nm.SRC_CD', 'operator':'=='}]), 'SRC_CD,TRGT_CD_NM,TRGT_CD'], 
    ['DB2CDMPPNGSrcCd', ctx.joinConditionForLookup(master={'name':'TransCoupon','alias':'Lnk_Trans_Extr','types':{'SRC_SYS_CD_SK':'integer','CLM_ID':'string','CCAA_APLD_IN':'string','CCAA_COUPON_AMT':'decimal(13, 2)','FMLY_ACCUM_DEDCT_AMT':'decimal(13, 2)','FMLY_ACCUM_OOP_AMT':'decimal(13, 2)','INDV_ACCUM_DEDCT_AMT':'decimal(13, 2)','INDV_ACCUM_OOP_AMT':'decimal(13, 2)','INDV_APLD_DEDCT_AMT':'decimal(13, 2)','INDV_APLD_OOP_AMT':'decimal(13, 2)','NCP_COUPON_TYP_CD':'integer'}}, reference={'name':'DB2CDMPPNGSrcCd','alias':'Lnk_Lkup_Src_Cd','types':{'CD_MPPNG_SK':'integer','TRGT_CD':'string'}}, joinFields=[{'master':'Lnk_Trans_Extr.SRC_SYS_CD_SK','reference':'Lnk_Lkup_Src_Cd.CD_MPPNG_SK', 'operator':'=='}]), 'CD_MPPNG_SK,TRGT_CD']],
      schema="CLM_ID,CCAA_APLD_IN,CCAA_COUPON_AMT,FMLY_ACCUM_DEDCT_AMT,FMLY_ACCUM_OOP_AMT,INDV_ACCUM_DEDCT_AMT,INDV_ACCUM_OOP_AMT,INDV_APLD_DEDCT_AMT,INDV_APLD_OOP_AMT,CD_MPPNG_SK_Src_Sys,TRGT_CD_Src_Sys,NCP_COUPON_TYP_CD,NCP_COUPON_TYP_NM",
      map=['Lnk_Trans_Extr.CLM_ID','Lnk_Trans_Extr.CCAA_APLD_IN','Lnk_Trans_Extr.CCAA_COUPON_AMT','Lnk_Trans_Extr.FMLY_ACCUM_DEDCT_AMT','Lnk_Trans_Extr.FMLY_ACCUM_OOP_AMT','Lnk_Trans_Extr.INDV_ACCUM_DEDCT_AMT','Lnk_Trans_Extr.INDV_ACCUM_OOP_AMT','Lnk_Trans_Extr.INDV_APLD_DEDCT_AMT','Lnk_Trans_Extr.INDV_APLD_OOP_AMT','Lnk_Lkup_Src_Cd.CD_MPPNG_SK','Lnk_Lkup_Src_Cd.TRGT_CD','Lnk_Lkup_Coupon_Nm.TRGT_CD','Lnk_Lkup_Coupon_Nm.TRGT_CD_NM'],
      aliasFrom="Lnk_Trans_Extr,Lnk_Lkup_Coupon_Nm,Lnk_Lkup_Src_Cd",
      aliasTo="TransCoupon,DB2CDMPPNGCouponNm,DB2CDMPPNGSrcCd")}
    LkupCodes
        derive(
              SRC_SYS_CD =  case(isNull(CD_MPPNG_SK_Src_Sys), " ", TRGT_CD_Src_Sys),
              CLM_ID = CLM_ID,
              NCP_COUPON_TYP_CD =  case(isNull(NCP_COUPON_TYP_CD), " ", NCP_COUPON_TYP_CD),
              NCP_COUPON_TYP_NM =  case(isNull(NCP_COUPON_TYP_NM), " ", NCP_COUPON_TYP_NM),
              CCAA_APLD_IN = CCAA_APLD_IN,
              CCAA_COUPON_AMT = CCAA_COUPON_AMT,
              FMLY_ACCUM_DEDCT_AMT = FMLY_ACCUM_DEDCT_AMT,
              FMLY_ACCUM_OOP_AMT = FMLY_ACCUM_OOP_AMT,
              INDV_ACCUM_DEDCT_AMT = INDV_ACCUM_DEDCT_AMT,
              INDV_ACCUM_OOP_AMT = INDV_ACCUM_OOP_AMT,
              INDV_APLD_DEDCT_AMT = INDV_APLD_DEDCT_AMT,
              INDV_APLD_OOP_AMT = INDV_APLD_OOP_AMT,
              LAST_UPDT_RUN_CYC_NO = $RunCycle) ~> TransSrcSysDerived
        TransSrcSysDerived
        select(mapColumn(
            SRC_SYS_CD,
            CLM_ID,
            NCP_COUPON_TYP_CD,
            NCP_COUPON_TYP_NM,
            CCAA_APLD_IN,
            CCAA_COUPON_AMT,
            FMLY_ACCUM_DEDCT_AMT,
            FMLY_ACCUM_OOP_AMT,
            INDV_ACCUM_DEDCT_AMT,
            INDV_ACCUM_OOP_AMT,
            INDV_APLD_DEDCT_AMT,
            INDV_APLD_OOP_AMT,
            LAST_UPDT_RUN_CYC_NO
        ),
        skipDuplicateMapInputs: true,
        skipDuplicateMapOutputs: true) ~> TransSrcSys
    TransSrcSys sink(
      allowSchemaDrift: true,
      validateSchema: false,
      partitionFileNames:[(""" + ctx.getFileName("""$FilePath + '/load/CLM_DM_DRUG_CLM_ACCUM_IMPCT.dat'""") + f""")],
      partitionBy('hash', 1),
      skipDuplicateMapInputs: true,
      skipDuplicateMapOutputs: true,
      format: 'delimited',
      fileSystem: '{ctx.file_container}',
      folderPath: (""" + ctx.getFolderPath("""$FilePath + '/load/CLM_DM_DRUG_CLM_ACCUM_IMPCT.dat'""") + f""") ,
      columnDelimiter: ',',
      escapeChar:""" + '\\\\' + f""",
      rowDelimiter: '\\n',
      quoteChar: '\\"',
      columnNamesAsHeader:false) ~> CLMDMDRUGCLMACCUMIMPCTextract""", type='MappingDataFlow')
    artifacts = buffer.artifacts + [(dataflowName, dataflow)]
    return artifacts, dataflowName
  artifacts, dataflowName = dfIdsDMDrugClmAccumImpctExtr()
  activityName = "IdsDMDrugClmAccumImpctExtr"
  activityParameters = {"IDSAcct":  {"value": "'@{pipeline().parameters.IDSAcct}'","type": "Expression"},
  "IDSDB":  {"value": "'@{pipeline().parameters.IDSDB}'","type": "Expression"},
  "FilePath":  {"value": "'@{pipeline().parameters.FilePath}'","type": "Expression"},
  "IDSPW":  {"value": "'@{pipeline().parameters.IDSPW}'","type": "Expression"},
  "IDSOwner":  {"value": "'@{pipeline().parameters.IDSOwner}'","type": "Expression"},
  "RunCycle":  {"value": "'@{pipeline().parameters.RunCycle}'","type": "Expression"}}
  activity = ExecuteDataFlowActivity(
    name = activityName,
    data_flow = DataFlowReference(reference_name=dataflowName,
      parameters=activityParameters, type='DataFlowReference'))
  return artifacts, [activity]

def IdsDMDrugClmAccumImpctExtr(ctx):
  name = "IdsDMDrugClmAccumImpctExtr"
  artifacts, activities = IdsDMDrugClmAccumImpctExtrActivities(ctx)
  pipeline = PipelineResource(
    folder = PipelineFolder(name="WebDataMart/Claim/Claim"),
    activities = activities,
    description = """
      Extract from IDS table DRUG_CLM_ACCUM_IMPCT to create input file for DataMart table CLM_DM_DRUG_CLM_ACCUM_IMPCT.
      COPYRIGHT 2023 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 

      CALLED BY :  OptumDrugCntl
       
      DESCRIPTION:  Extract from IDS table DRUG_CLM_ACCUM_IMPCT to create input file forDataMart table CLM_DM_DRUG_CLM_ACCUM_IMPCT.


      MODIFICATIONS:

      Developer                     Date               Project                 Change Description                                                                                                   Development Project    Code Reviewer               Date Reviewed
      --------------------------          --------------------    ----------------------      ------------------------------------------------------------------------------------------------------------------------------    ---------------------------------    ------------------------------------    ----------------------------              
      Bill Schroeder              07/27/2023     US-586570          Original Programming                                                                                                 IntegrateDev2                Jeyaprasanna                2023-08-15
      Parameters:
      -----------
      IDSAcct:
        IDS Account
      IDSDB:
        IDS Database
      FilePath:
        IDS File Path
      IDSPW:
        IDS Password
      IDSOwner:
        IDS Table Owner
      RunCycle:
        RunCycle""",
    parameters = {
      "IDSAcct": ParameterSpecification(type="String"),
      "IDSDB": ParameterSpecification(type="String", default_value=ctx.IDSDB),
      "FilePath": ParameterSpecification(type="String", default_value=ctx.FilePath),
      "IDSPW": ParameterSpecification(type="String"),
      "IDSOwner": ParameterSpecification(type="String", default_value=ctx.IDSOwner),
      "RunCycle": ParameterSpecification(type="String")})
  return artifacts + [(name, pipeline)]

def main():
  ctx = TranslationContext()
  artifacts = (IdsDMDrugClmAccumImpctExtr(ctx))
  if ctx.validateArtifacts(artifacts):
    ctx.deployArtifacts(artifacts)

if __name__ == '__main__':
  main()
