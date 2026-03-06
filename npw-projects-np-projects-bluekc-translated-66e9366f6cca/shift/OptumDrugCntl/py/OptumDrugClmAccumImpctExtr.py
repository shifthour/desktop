#!/usr/bin/python3

from npadf import *

def OptumDrugClmAccumImpctExtrActivities(ctx):
  def dfOptumDrugClmAccumImpctExtr():
    def db2KCLM():
       ds_ls_db = LinkedServiceReference(type='LinkedServiceReference',reference_name=ctx.sqldb_linked_service)
       df_sources_inline = DataFlowSource(name='db2KCLM',linked_service=ds_ls_db)
       return DataflowSegment([df_sources_inline])
    def db2CDMPPNG():
       ds_ls_db = LinkedServiceReference(type='LinkedServiceReference',reference_name=ctx.sqldb_linked_service)
       df_sources_inline = DataFlowSource(name='db2CDMPPNG',linked_service=ds_ls_db)
       return DataflowSegment([df_sources_inline])
    def DRUGCLMACCUMIMPCTprep():
       ds_ls_blob = LinkedServiceReference(type='LinkedServiceReference',reference_name=ctx.file_linked_service)
       df_sources_inline = DataFlowSource(name='DRUGCLMACCUMIMPCTprep', linked_service=ds_ls_blob)
       return DataflowSegment([df_sources_inline])
    def LkpKCLM():
       return DataflowSegment([
         Transformation(name="LkpKCLM"),
         Transformation(name="LkpKCLMJoin1"),Transformation(name="LkpKCLMDerived1")])
    def LkpSrcCd():
       return DataflowSegment([
         Transformation(name="LkpSrcCd"),
         Transformation(name="LkpSrcCdJoin1"),Transformation(name="LkpSrcCdDerived1")])
    def TransAccum():
       return DataflowSegment([
         Transformation(name='TransAccumDerived'),
         Transformation(name='TransAccum')])
    def DRUGCLMACCUMIMPCT():
       ds_ls_blob = LinkedServiceReference(type='LinkedServiceReference',reference_name=ctx.file_linked_service)
       df_sinks_inline = DataFlowSink(name='DRUGCLMACCUMIMPCT', linked_service=ds_ls_blob)
       return DataflowSegment([df_sinks_inline])
    dataflowName = "OptumDrugClmAccumImpctExtr"
    buffer = DataflowBuffer()
    buffer.append(db2KCLM())
    buffer.append(db2CDMPPNG())
    buffer.append(DRUGCLMACCUMIMPCTprep())
    buffer.append(LkpKCLM())
    buffer.append(LkpSrcCd())
    buffer.append(TransAccum())
    buffer.append(DRUGCLMACCUMIMPCT())
    dataflow = MappingDataFlow(
      folder = DataFlowFolder(name="claim/claim/Optum"),
      sources = buffer.sources,
      sinks = buffer.sinks,
      transformations = buffer.transformations,
      script = f"""parameters{{
        FilePath as string ("{ctx.FilePath}"),
        SrcSysCd as string ,
        SrcSysCdSK as string ,
        CurrRunCycle as string ,
        RunID as string ,
        IDSDB as string ("{ctx.IDSDB}"),
        IDSOwner as string ("{ctx.IDSOwner}"),
        IDSAcct as string ,
        IDSPW as string 
      }}
    source(output(
        SRC_SYS_CD_SK as integer,
        CLM_ID as string,
        CRT_RUN_CYC_EXCTN_SK as integer,
        CLM_SK as integer),
      allowSchemaDrift: true,
      validateSchema: false,
      format: 'query',
      store: 'sqlserver',
      query: ("SELECT SRC_SYS_CD_SK, CLM_ID, CRT_RUN_CYC_EXCTN_SK, CLM_SK\\nFROM " + toString($IDSOwner) + ".K_CLM where SRC_SYS_CD_SK=" + toString($SrcSysCdSK)),
      schemaName: '',
      tableName: '',
      isolationLevel: 'READ_UNCOMMITTED') ~> db2KCLM
    source(output(
        SRC_CD as string,
        TRGT_CD_NM as string,
        SRC_CD_SK as integer),
      allowSchemaDrift: true,
      validateSchema: false,
      format: 'query',
      store: 'sqlserver',
      query: ("SELECT \\nSRC_CD, TRGT_CD_NM ,CD_MPPNG_SK as SRC_CD_SK\\n  FROM " + toString($IDSOwner) + ".CD_MPPNG \\n WHERE SRC_CLCTN_CD   = 'OPTUMRX' \\n   AND SRC_SYS_CD     = 'OPTUMRX' \\n   AND SRC_DOMAIN_NM  = 'NCP COUPON TYPE' \\n   AND TRGT_CLCTN_CD  = 'IDS' \\n   AND TRGT_DOMAIN_NM = 'NCP COUPON TYPE'\\nunion\\n SELECT \\nSRC_CD, TRGT_CD_NM ,cast(CD_MPPNG_SK as integer) as SRC_CD_SK\\n  FROM " + toString($IDSOwner) + ".CD_MPPNG where  CD_MPPNG_SK='1'\\n"),
      schemaName: '',
      tableName: '',
      isolationLevel: 'READ_UNCOMMITTED') ~> db2CDMPPNG
    source(output(
        SRC_SYS_CD_SK as integer,
        CLM_ID as string,
        LAST_UPDT_RUN_CYC_EXCTN_SK as integer,
        CLIENTDEF3 as string,
        CCAA_COUPON_AMT as decimal(13, 2),
        FMLY_ACCUM_DEDCT_AMT as decimal(13, 2),
        FMLY_ACCUM_OOP_AMT as decimal(13, 2),
        INDV_ACCUM_DEDCT_AMT as decimal(13, 2),
        INDV_ACCUM_OOP_AMT as decimal(13, 2),
        INDV_APLD_DEDCT_AMT as decimal(13, 2),
        INDV_APLD_OOP_AMT as decimal(13, 2)),
      allowSchemaDrift: true,
      validateSchema: false,
      ignoreNoFilesFound: false,
      format: 'delimited',
      fileSystem: '{ctx.file_container}',
      folderPath: ({ctx.getFolderPath("$FilePath + '/load/DRUG_CLM_ACCUM_IMPCT_prep.dat'")}),
      fileName: ({ctx.getFileName("$FilePath + '/load/DRUG_CLM_ACCUM_IMPCT_prep.dat'")}) ,
      columnDelimiter: ',',
      rowDelimiter: '\\n',
      columnNamesAsHeader:false) ~> DRUGCLMACCUMIMPCTprep
    {ctx.joinScript(joinType='inner',
      master=['DRUGCLMACCUMIMPCTprep', 'SRC_SYS_CD_SK,CLM_ID,LAST_UPDT_RUN_CYC_EXCTN_SK,CLIENTDEF3,CCAA_COUPON_AMT,FMLY_ACCUM_DEDCT_AMT,FMLY_ACCUM_OOP_AMT,INDV_ACCUM_DEDCT_AMT,INDV_ACCUM_OOP_AMT,INDV_APLD_DEDCT_AMT,INDV_APLD_OOP_AMT'],
      transformationName='LkpKCLM',
      references=[['db2KCLM', ctx.joinConditionForLookup(master={'name':'DRUGCLMACCUMIMPCTprep','alias':'Lnk_Accum_extr','types':{'SRC_SYS_CD_SK':'integer','CLM_ID':'string','LAST_UPDT_RUN_CYC_EXCTN_SK':'integer','CLIENTDEF3':'string','CCAA_COUPON_AMT':'decimal(13, 2)','FMLY_ACCUM_DEDCT_AMT':'decimal(13, 2)','FMLY_ACCUM_OOP_AMT':'decimal(13, 2)','INDV_ACCUM_DEDCT_AMT':'decimal(13, 2)','INDV_ACCUM_OOP_AMT':'decimal(13, 2)','INDV_APLD_DEDCT_AMT':'decimal(13, 2)','INDV_APLD_OOP_AMT':'decimal(13, 2)'}}, reference={'name':'db2KCLM','alias':'Ref_db2_K_CLM','types':{'SRC_SYS_CD_SK':'integer','CLM_ID':'string','CRT_RUN_CYC_EXCTN_SK':'integer','CLM_SK':'integer'}}, joinFields=[{'master':'Lnk_Accum_extr.SRC_SYS_CD_SK','reference':'Ref_db2_K_CLM.SRC_SYS_CD_SK', 'operator':'=='},{'master':'Lnk_Accum_extr.CLM_ID','reference':'Ref_db2_K_CLM.CLM_ID', 'operator':'=='}]), 'SRC_SYS_CD_SK,CLM_ID,CRT_RUN_CYC_EXCTN_SK,CLM_SK']],
      schema="DRUG_CLM_SK,SRC_SYS_CD_SK,CLM_ID,CRT_RUN_CYC_EXCTN_SK,LAST_UPDT_RUN_CYC_EXCTN_SK,CCAA_COUPON_AMT,FMLY_ACCUM_DEDCT_AMT,FMLY_ACCUM_OOP_AMT,INDV_ACCUM_DEDCT_AMT,INDV_ACCUM_OOP_AMT,INDV_APLD_DEDCT_AMT,INDV_APLD_OOP_AMT,CLIENTDEF3",
      map=['Ref_db2_K_CLM.CLM_SK','Lnk_Accum_extr.SRC_SYS_CD_SK','Lnk_Accum_extr.CLM_ID','Ref_db2_K_CLM.CRT_RUN_CYC_EXCTN_SK','Lnk_Accum_extr.LAST_UPDT_RUN_CYC_EXCTN_SK','Lnk_Accum_extr.CCAA_COUPON_AMT','Lnk_Accum_extr.FMLY_ACCUM_DEDCT_AMT','Lnk_Accum_extr.FMLY_ACCUM_OOP_AMT','Lnk_Accum_extr.INDV_ACCUM_DEDCT_AMT','Lnk_Accum_extr.INDV_ACCUM_OOP_AMT','Lnk_Accum_extr.INDV_APLD_DEDCT_AMT','Lnk_Accum_extr.INDV_APLD_OOP_AMT','Lnk_Accum_extr.CLIENTDEF3'],
      aliasFrom="Lnk_Accum_extr,Ref_db2_K_CLM",
      aliasTo="DRUGCLMACCUMIMPCTprep,db2KCLM")}
    {ctx.joinScript(joinType='left',
      master=['LkpKCLM', 'DRUG_CLM_SK,SRC_SYS_CD_SK,CLM_ID,CRT_RUN_CYC_EXCTN_SK,LAST_UPDT_RUN_CYC_EXCTN_SK,CCAA_COUPON_AMT,FMLY_ACCUM_DEDCT_AMT,FMLY_ACCUM_OOP_AMT,INDV_ACCUM_DEDCT_AMT,INDV_ACCUM_OOP_AMT,INDV_APLD_DEDCT_AMT,INDV_APLD_OOP_AMT,CLIENTDEF3'],
      transformationName='LkpSrcCd',
      references=[['db2CDMPPNG', ctx.joinConditionForLookup(master={'name':'LkpKCLM','alias':'Lnk_Accum1','types':{'DRUG_CLM_SK':'integer','SRC_SYS_CD_SK':'integer','CLM_ID':'string','CRT_RUN_CYC_EXCTN_SK':'integer','LAST_UPDT_RUN_CYC_EXCTN_SK':'integer','CCAA_COUPON_AMT':'decimal(13, 2)','FMLY_ACCUM_DEDCT_AMT':'decimal(13, 2)','FMLY_ACCUM_OOP_AMT':'decimal(13, 2)','INDV_ACCUM_DEDCT_AMT':'decimal(13, 2)','INDV_ACCUM_OOP_AMT':'decimal(13, 2)','INDV_APLD_DEDCT_AMT':'decimal(13, 2)','INDV_APLD_OOP_AMT':'decimal(13, 2)','CLIENTDEF3':'string'}}, reference={'name':'db2CDMPPNG','alias':'Ref_db2_CD_MPPNG','types':{'SRC_CD':'string','TRGT_CD_NM':'string','SRC_CD_SK':'integer'}}, joinFields=[{'master':'Lnk_Accum1.CLIENTDEF3','reference':'Ref_db2_CD_MPPNG.SRC_CD', 'operator':'=='}]), 'SRC_CD,TRGT_CD_NM,SRC_CD_SK']],
      schema="DRUG_CLM_SK,SRC_SYS_CD_SK,CLM_ID,CRT_RUN_CYC_EXCTN_SK,LAST_UPDT_RUN_CYC_EXCTN_SK,SRC_CD,TRGT_CD_NM,CCAA_COUPON_AMT,FMLY_ACCUM_DEDCT_AMT,FMLY_ACCUM_OOP_AMT,INDV_ACCUM_DEDCT_AMT,INDV_ACCUM_OOP_AMT,INDV_APLD_DEDCT_AMT,INDV_APLD_OOP_AMT,SRC_CD_SK",
      map=['Lnk_Accum1.DRUG_CLM_SK','Lnk_Accum1.SRC_SYS_CD_SK','Lnk_Accum1.CLM_ID','Lnk_Accum1.CRT_RUN_CYC_EXCTN_SK','Lnk_Accum1.LAST_UPDT_RUN_CYC_EXCTN_SK','Ref_db2_CD_MPPNG.SRC_CD','Ref_db2_CD_MPPNG.TRGT_CD_NM','Lnk_Accum1.CCAA_COUPON_AMT','Lnk_Accum1.FMLY_ACCUM_DEDCT_AMT','Lnk_Accum1.FMLY_ACCUM_OOP_AMT','Lnk_Accum1.INDV_ACCUM_DEDCT_AMT','Lnk_Accum1.INDV_ACCUM_OOP_AMT','Lnk_Accum1.INDV_APLD_DEDCT_AMT','Lnk_Accum1.INDV_APLD_OOP_AMT','Ref_db2_CD_MPPNG.SRC_CD_SK'],
      aliasFrom="Lnk_Accum1,Ref_db2_CD_MPPNG",
      aliasTo="LkpKCLM,db2CDMPPNG")}
    LkpSrcCd
        derive(
              DRUG_CLM_SK = DRUG_CLM_SK,
              SRC_SYS_CD_SK = SRC_SYS_CD_SK,
              CLM_ID = CLM_ID,
              CRT_RUN_CYC_EXCTN_SK = CRT_RUN_CYC_EXCTN_SK,
              LAST_UPDT_RUN_CYC_EXCTN_SK = LAST_UPDT_RUN_CYC_EXCTN_SK,
              NCP_COUPON_TYP_CD_SK = npw_DS_TRIM_1(SRC_CD_SK),
              CCAA_APLD_IN =  case(npw_DS_TRIM_1(SRC_CD) == "99" && CCAA_COUPON_AMT > 0, "Y", "N"),
              CCAA_COUPON_AMT = CCAA_COUPON_AMT,
              FMLY_ACCUM_DEDCT_AMT = FMLY_ACCUM_DEDCT_AMT,
              FMLY_ACCUM_OOP_AMT = FMLY_ACCUM_OOP_AMT,
              INDV_ACCUM_DEDCT_AMT = INDV_ACCUM_DEDCT_AMT,
              INDV_ACCUM_OOP_AMT = INDV_ACCUM_OOP_AMT,
              INDV_APLD_DEDCT_AMT = INDV_APLD_DEDCT_AMT,
              INDV_APLD_OOP_AMT = INDV_APLD_OOP_AMT) ~> TransAccumDerived
        TransAccumDerived
        select(mapColumn(
            DRUG_CLM_SK,
            SRC_SYS_CD_SK,
            CLM_ID,
            CRT_RUN_CYC_EXCTN_SK,
            LAST_UPDT_RUN_CYC_EXCTN_SK,
            NCP_COUPON_TYP_CD_SK,
            CCAA_APLD_IN,
            CCAA_COUPON_AMT,
            FMLY_ACCUM_DEDCT_AMT,
            FMLY_ACCUM_OOP_AMT,
            INDV_ACCUM_DEDCT_AMT,
            INDV_ACCUM_OOP_AMT,
            INDV_APLD_DEDCT_AMT,
            INDV_APLD_OOP_AMT
        ),
        skipDuplicateMapInputs: true,
        skipDuplicateMapOutputs: true) ~> TransAccum
    TransAccum sink(
      allowSchemaDrift: true,
      validateSchema: false,
      partitionFileNames:[(""" + ctx.getFileName("""$FilePath + '/load/DRUG_CLM_ACCUM_IMPCT.dat'""") + f""")],
      partitionBy('hash', 1),
      skipDuplicateMapInputs: true,
      skipDuplicateMapOutputs: true,
      format: 'delimited',
      fileSystem: '{ctx.file_container}',
      folderPath: (""" + ctx.getFolderPath("""$FilePath + '/load/DRUG_CLM_ACCUM_IMPCT.dat'""") + f""") ,
      columnDelimiter: ',',
      escapeChar:""" + '\\\\' + f""",
      rowDelimiter: '\\n',
      quoteChar: '',
      columnNamesAsHeader:false) ~> DRUGCLMACCUMIMPCT""", type='MappingDataFlow')
    artifacts = buffer.artifacts + [(dataflowName, dataflow)]
    return artifacts, dataflowName
  artifacts, dataflowName = dfOptumDrugClmAccumImpctExtr()
  activityName = "OptumDrugClmAccumImpctExtr"
  activityParameters = {"FilePath":  {"value": "'@{pipeline().parameters.FilePath}'","type": "Expression"},
  "SrcSysCd":  {"value": "'@{pipeline().parameters.SrcSysCd}'","type": "Expression"},
  "SrcSysCdSK":  {"value": "'@{pipeline().parameters.SrcSysCdSK}'","type": "Expression"},
  "CurrRunCycle":  {"value": "'@{pipeline().parameters.CurrRunCycle}'","type": "Expression"},
  "RunID":  {"value": "'@{pipeline().parameters.RunID}'","type": "Expression"},
  "IDSDB":  {"value": "'@{pipeline().parameters.IDSDB}'","type": "Expression"},
  "IDSOwner":  {"value": "'@{pipeline().parameters.IDSOwner}'","type": "Expression"},
  "IDSAcct":  {"value": "'@{pipeline().parameters.IDSAcct}'","type": "Expression"},
  "IDSPW":  {"value": "'@{pipeline().parameters.IDSPW}'","type": "Expression"}}
  activity = ExecuteDataFlowActivity(
    name = activityName,
    data_flow = DataFlowReference(reference_name=dataflowName,
      parameters=activityParameters, type='DataFlowReference'))
  return artifacts, [activity]

def OptumDrugClmAccumImpctExtr(ctx):
  name = "OptumDrugClmAccumImpctExtr"
  artifacts, activities = OptumDrugClmAccumImpctExtrActivities(ctx)
  pipeline = PipelineResource(
    folder = PipelineFolder(name="claim/claim/Optum"),
    activities = activities,
    description = """
      Extract from Optum daily claims file for loading into table DRUG_CLM_ACCUM_IMPCT
      © Copyright 2020 Blue Cross/Blue Shield of Kansas City

      CALLED BY :  OptumDrugLandSeq
       
      DESCRIPTION:  Join extract file DRUG_CLM_ACCUM_IMPCT_prep.dat (created in job OptumClmLandExtr that extracts from the daily claims file) to table K_CLM (already created for table CLM in OptumDrugLandSeq) to get the surrogate key values for columns DRUG_CLM_SK and CRT_RUN_CYC_EXCTN_SK and write to file DRUG_CLM_ACCUM_IMPCT.dat.

      MODIFICATIONS:

      Developer                     Date               Project                 Change Description                                                                                                   Development Project    Code Reviewer               Date Reviewed
      --------------------------          --------------------    ----------------------      ------------------------------------------------------------------------------------------------------------------------------    ---------------------------------    ------------------------------------    ----------------------------              
      Bill Schroeder              07/14/2023     US-586570          Original Programming                                                                                                 IntegrateDev2               Jeyaprasanna                  2023-08-14
      Parameters:
      -----------
      FilePath:
        File Path
      SrcSysCd:
        SrcSysCd
      SrcSysCdSK:
        SrcSysCdSK
      CurrRunCycle:
        RunCycle
      RunID:
        RunID
      IDSDB:
        IDS Database
      IDSOwner:
        IDS Table Owner
      IDSAcct:
        IDS Account
      IDSPW:
        IDS Password""",
    parameters = {
      "FilePath": ParameterSpecification(type="String", default_value=ctx.FilePath),
      "SrcSysCd": ParameterSpecification(type="String"),
      "SrcSysCdSK": ParameterSpecification(type="String"),
      "CurrRunCycle": ParameterSpecification(type="String"),
      "RunID": ParameterSpecification(type="String"),
      "IDSDB": ParameterSpecification(type="String", default_value=ctx.IDSDB),
      "IDSOwner": ParameterSpecification(type="String", default_value=ctx.IDSOwner),
      "IDSAcct": ParameterSpecification(type="String"),
      "IDSPW": ParameterSpecification(type="String")})
  return artifacts + [(name, pipeline)]

def main():
  ctx = TranslationContext()
  artifacts = (OptumDrugClmAccumImpctExtr(ctx))
  if ctx.validateArtifacts(artifacts):
    ctx.deployArtifacts(artifacts)

if __name__ == '__main__':
  main()
