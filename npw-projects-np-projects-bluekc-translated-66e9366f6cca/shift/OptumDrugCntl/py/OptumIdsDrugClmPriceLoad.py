#!/usr/bin/python3

from npadf import *

def OptumIdsDrugClmPriceLoadActivities(ctx):
  def dfOptumIdsDrugClmPriceLoad():
    def seqDRUGCLMPRICE():
       ds_ls_blob = LinkedServiceReference(type='LinkedServiceReference',reference_name=ctx.file_linked_service)
       df_sources_inline = DataFlowSource(name='seqDRUGCLMPRICE', linked_service=ds_ls_blob)
       return DataflowSegment([df_sources_inline])
    def cpyforBuffer():
       return DataflowSegment([
         Transformation(name='cpyforBufferDerived'),
         Transformation(name='cpyforBuffer')])
    def Db2LoadDRUGCLMPRICE():
       ds_ls_db = LinkedServiceReference(type='LinkedServiceReference',reference_name=ctx.sqldb_linked_service)
       df_sinks_inline = DataFlowSink(name='Db2LoadDRUGCLMPRICE',linked_service=ds_ls_db)
       df_alterrows = Transformation(name='Db2LoadDRUGCLMPRICEAlterRows')
       return DataflowSegment([df_sinks_inline, df_alterrows])
    dataflowName = "OptumIdsDrugClmPriceLoad"
    buffer = DataflowBuffer()
    buffer.append(seqDRUGCLMPRICE())
    buffer.append(cpyforBuffer())
    buffer.append(Db2LoadDRUGCLMPRICE())
    dataflow = MappingDataFlow(
      folder = DataFlowFolder(name="claim/DrugClmPrice"),
      sources = buffer.sources,
      sinks = buffer.sinks,
      transformations = buffer.transformations,
      script = f"""parameters{{
        FilePath as string ("{ctx.FilePath}"),
        IDSInstance as string ("{ctx.IDSInstance}"),
        IDSAcct as string ,
        IDSPW as string ,
        IDSDB as string ("{ctx.IDSDB}"),
        IDSDB2ArraySize as string ("{ctx.IDSDB2ArraySize}"),
        IDSDB2RecordCount as string ("{ctx.IDSDB2RecordCount}"),
        IDSOwner as string ("{ctx.IDSOwner}")
      }}
    source(output(
        DRUG_CLM_SK as integer,
        SRC_SYS_CD_SK as integer,
        CLM_ID as string,
        CRT_RUN_CYC_EXCTN_SK as integer,
        LAST_UPDT_RUN_CYC_EXCTN_SK as integer,
        BUY_CST_SRC_CD_SK as integer,
        GNRC_OVRD_CD_SK as integer,
        PDX_NTWK_QLFR_CD_SK as integer,
        FRMLRY_PROTOCOL_IN as string,
        RECON_IN as string,
        SPEC_PGM_IN as string,
        FINL_PLN_EFF_DT_SK as string,
        ORIG_PD_TRANS_SUBMT_DT_SK as string,
        PRORTD_DISPNS_QTY as decimal(11, 3),
        SUBMT_DISPNS_METRIC_QTY as integer,
        AVG_WHLSL_PRICE_UNIT_CST_AMT as decimal(13, 5),
        WHLSL_ACQSTN_CST_UNIT_CST_AMT as decimal(13, 5),
        BUY_DISPNS_FEE_AMT as decimal(13, 2),
        BUY_INGR_CST_AMT as decimal(13, 2),
        BUY_RATE_PCT as decimal(7, 2),
        BUY_SLS_TAX_AMT as decimal(13, 2),
        BUY_TOT_DUE_AMT as decimal(13, 2),
        BUY_TOT_OTHR_AMT as decimal(13, 2),
        CST_TYP_UNIT_CST_AMT as decimal(13, 5),
        INVC_TOT_DUE_AMT as decimal(13, 2),
        SELL_CST_TYP_UNIT_CST_AMT as decimal(13, 5),
        SELL_RATE_PCT as decimal(7, 2),
        SPREAD_DISPNS_FEE_AMT as decimal(13, 2),
        SPREAD_INGR_CST_AMT as decimal(13, 2),
        SPREAD_SLS_TAX_AMT as decimal(13, 2),
        BUY_CST_TYP_ID as string,
        BUY_PRICE_TYP_ID as string,
        CST_TYP_ID as string,
        DRUG_TYP_ID as string,
        FINL_PLN_ID as string,
        GNRC_PROD_ID as string,
        SELL_PRICE_TYP_ID as string,
        SPEC_PGM_ID as string,
        BUY_INCNTV_FEE_AMT as decimal(13, 2),
        SELL_INCNTV_FEE_AMT as decimal(13, 2),
        SPREAD_INCNTV_FEE_AMT as decimal(13, 2),
        SELL_OTHR_AMT as decimal(13, 2),
        SELL_OTHR_PAYOR_AMT as decimal(13, 2),
        FRMLRY_SK as integer,
        TIER_ID as string,
        DRUG_TYP_CD_SK as integer,
        PLN_DRUG_STTUS_CD_SK as integer,
        DRUG_PLN_TYP_ID as string),
      allowSchemaDrift: true,
      validateSchema: false,
      ignoreNoFilesFound: false,
      format: 'delimited',
      fileSystem: '{ctx.file_container}',
      folderPath: ({ctx.getFolderPath("$FilePath + '/load/DRUG_CLM_PRICE.dat'")}),
      fileName: ({ctx.getFileName("$FilePath + '/load/DRUG_CLM_PRICE.dat'")}) ,
      columnDelimiter: ',',
      rowDelimiter: '\\n',
      quoteChar: '',
      columnNamesAsHeader:false) ~> seqDRUGCLMPRICE
    seqDRUGCLMPRICE
        derive(
              DRUG_CLM_SK = DRUG_CLM_SK,
              SRC_SYS_CD_SK = SRC_SYS_CD_SK,
              CLM_ID = CLM_ID,
              CRT_RUN_CYC_EXCTN_SK = CRT_RUN_CYC_EXCTN_SK,
              LAST_UPDT_RUN_CYC_EXCTN_SK = LAST_UPDT_RUN_CYC_EXCTN_SK,
              BUY_CST_SRC_CD_SK = BUY_CST_SRC_CD_SK,
              GNRC_OVRD_CD_SK = GNRC_OVRD_CD_SK,
              PDX_NTWK_QLFR_CD_SK = PDX_NTWK_QLFR_CD_SK,
              FRMLRY_PROTOCOL_IN = FRMLRY_PROTOCOL_IN,
              RECON_IN = RECON_IN,
              SPEC_PGM_IN = SPEC_PGM_IN,
              FINL_PLN_EFF_DT_SK = FINL_PLN_EFF_DT_SK,
              ORIG_PD_TRANS_SUBMT_DT_SK = ORIG_PD_TRANS_SUBMT_DT_SK,
              PRORTD_DISPNS_QTY = PRORTD_DISPNS_QTY,
              SUBMT_DISPNS_METRIC_QTY = SUBMT_DISPNS_METRIC_QTY,
              AVG_WHLSL_PRICE_UNIT_CST_AMT = AVG_WHLSL_PRICE_UNIT_CST_AMT,
              WHLSL_ACQSTN_CST_UNIT_CST_AMT = WHLSL_ACQSTN_CST_UNIT_CST_AMT,
              BUY_DISPNS_FEE_AMT = BUY_DISPNS_FEE_AMT,
              BUY_INGR_CST_AMT = BUY_INGR_CST_AMT,
              BUY_RATE_PCT = BUY_RATE_PCT,
              BUY_SLS_TAX_AMT = BUY_SLS_TAX_AMT,
              BUY_TOT_DUE_AMT = BUY_TOT_DUE_AMT,
              BUY_TOT_OTHR_AMT = BUY_TOT_OTHR_AMT,
              CST_TYP_UNIT_CST_AMT = CST_TYP_UNIT_CST_AMT,
              INVC_TOT_DUE_AMT = INVC_TOT_DUE_AMT,
              SELL_CST_TYP_UNIT_CST_AMT = SELL_CST_TYP_UNIT_CST_AMT,
              SELL_RATE_PCT = SELL_RATE_PCT,
              SPREAD_DISPNS_FEE_AMT = SPREAD_DISPNS_FEE_AMT,
              SPREAD_INGR_CST_AMT = SPREAD_INGR_CST_AMT,
              SPREAD_SLS_TAX_AMT = SPREAD_SLS_TAX_AMT,
              BUY_CST_TYP_ID = BUY_CST_TYP_ID,
              BUY_PRICE_TYP_ID = BUY_PRICE_TYP_ID,
              CST_TYP_ID = CST_TYP_ID,
              DRUG_TYP_ID = DRUG_TYP_ID,
              FINL_PLN_ID = FINL_PLN_ID,
              GNRC_PROD_ID = GNRC_PROD_ID,
              SELL_PRICE_TYP_ID = SELL_PRICE_TYP_ID,
              SPEC_PGM_ID = SPEC_PGM_ID,
              BUY_INCNTV_FEE_AMT = BUY_INCNTV_FEE_AMT,
              SELL_INCNTV_FEE_AMT = SELL_INCNTV_FEE_AMT,
              SPREAD_INCNTV_FEE_AMT = SPREAD_INCNTV_FEE_AMT,
              SELL_OTHR_AMT = SELL_OTHR_AMT,
              SELL_OTHR_PAYER_AMT = SELL_OTHR_PAYOR_AMT,
              FRMLRY_SK = FRMLRY_SK,
              TIER_ID = TIER_ID,
              DRUG_TYP_CD_SK = DRUG_TYP_CD_SK,
              PLN_DRUG_STTUS_CD_SK = PLN_DRUG_STTUS_CD_SK,
              DRUG_PLN_TYP_ID = DRUG_PLN_TYP_ID) ~> cpyforBufferDerived
        cpyforBufferDerived
        select(mapColumn(
            DRUG_CLM_SK,
            SRC_SYS_CD_SK,
            CLM_ID,
            CRT_RUN_CYC_EXCTN_SK,
            LAST_UPDT_RUN_CYC_EXCTN_SK,
            BUY_CST_SRC_CD_SK,
            GNRC_OVRD_CD_SK,
            PDX_NTWK_QLFR_CD_SK,
            FRMLRY_PROTOCOL_IN,
            RECON_IN,
            SPEC_PGM_IN,
            FINL_PLN_EFF_DT_SK,
            ORIG_PD_TRANS_SUBMT_DT_SK,
            PRORTD_DISPNS_QTY,
            SUBMT_DISPNS_METRIC_QTY,
            AVG_WHLSL_PRICE_UNIT_CST_AMT,
            WHLSL_ACQSTN_CST_UNIT_CST_AMT,
            BUY_DISPNS_FEE_AMT,
            BUY_INGR_CST_AMT,
            BUY_RATE_PCT,
            BUY_SLS_TAX_AMT,
            BUY_TOT_DUE_AMT,
            BUY_TOT_OTHR_AMT,
            CST_TYP_UNIT_CST_AMT,
            INVC_TOT_DUE_AMT,
            SELL_CST_TYP_UNIT_CST_AMT,
            SELL_RATE_PCT,
            SPREAD_DISPNS_FEE_AMT,
            SPREAD_INGR_CST_AMT,
            SPREAD_SLS_TAX_AMT,
            BUY_CST_TYP_ID,
            BUY_PRICE_TYP_ID,
            CST_TYP_ID,
            DRUG_TYP_ID,
            FINL_PLN_ID,
            GNRC_PROD_ID,
            SELL_PRICE_TYP_ID,
            SPEC_PGM_ID,
            BUY_INCNTV_FEE_AMT,
            SELL_INCNTV_FEE_AMT,
            SPREAD_INCNTV_FEE_AMT,
            SELL_OTHR_AMT,
            SELL_OTHR_PAYER_AMT,
            FRMLRY_SK,
            TIER_ID,
            DRUG_TYP_CD_SK,
            PLN_DRUG_STTUS_CD_SK,
            DRUG_PLN_TYP_ID
        ),
        skipDuplicateMapInputs: true,
        skipDuplicateMapOutputs: true) ~> cpyforBuffer
    cpyforBuffer alterRow(upsertIf(1==1)) ~> Db2LoadDRUGCLMPRICEAlterRows
    Db2LoadDRUGCLMPRICEAlterRows sink(
      allowSchemaDrift: true,
      validateSchema: false,
      format: 'table',
      store: 'sqlserver',
      schemaName: (""" + ctx.getSchemaName("""$IDSOwner + '.DRUG_CLM_PRICE'""") + f"""),
      tableName: (""" + ctx.getTableName("""$IDSOwner + '.DRUG_CLM_PRICE'""") + f"""),
      insertable:{'true' if 'upsertIf' == 'insertIf' else 'false'},
      updateable:{'true' if 'upsertIf' == 'updateIf' else 'false'},
      deletable:{'true' if 'upsertIf' == 'deleteIf' else 'false'},
      upsertable:{'true' if 'upsertIf' == 'upsertIf' else 'false'},
      keys:['DRUG_CLM_SK'],
      preSQLs: [],
      postSQLs: [],
      skipDuplicateMapInputs: true,
      skipDuplicateMapOutputs: true,
      errorHandlingOption: 'stopOnFirstError') ~> Db2LoadDRUGCLMPRICE""", type='MappingDataFlow')
    artifacts = buffer.artifacts + [(dataflowName, dataflow)]
    return artifacts, dataflowName
  artifacts, dataflowName = dfOptumIdsDrugClmPriceLoad()
  activityName = "OptumIdsDrugClmPriceLoad"
  activityParameters = {"FilePath":  {"value": "'@{pipeline().parameters.FilePath}'","type": "Expression"},
  "IDSInstance":  {"value": "'@{pipeline().parameters.IDSInstance}'","type": "Expression"},
  "IDSAcct":  {"value": "'@{pipeline().parameters.IDSAcct}'","type": "Expression"},
  "IDSPW":  {"value": "'@{pipeline().parameters.IDSPW}'","type": "Expression"},
  "IDSDB":  {"value": "'@{pipeline().parameters.IDSDB}'","type": "Expression"},
  "IDSDB2ArraySize":  {"value": "'@{pipeline().parameters.IDSDB2ArraySize}'","type": "Expression"},
  "IDSDB2RecordCount":  {"value": "'@{pipeline().parameters.IDSDB2RecordCount}'","type": "Expression"},
  "IDSOwner":  {"value": "'@{pipeline().parameters.IDSOwner}'","type": "Expression"}}
  activity = ExecuteDataFlowActivity(
    name = activityName,
    data_flow = DataFlowReference(reference_name=dataflowName,
      parameters=activityParameters, type='DataFlowReference'))
  return artifacts, [activity,
    CustomActivity(
      name = ctx.removeAlias("DSU.UnixSH"),
      command = ctx.getRoutineCommand(ctx.removeAlias("DSU.UnixSH"), isSubroutine=True),
      linked_service_name = LinkedServiceReference(type='LinkedServiceReference',reference_name=ctx.batch_linked_service),
      depends_on = [ActivityDependency( activity = activityName,
        dependency_conditions = [DependencyCondition.COMPLETED])]),
    FailActivity(
      name = "Fail",
      message = "Dataflow execution failed",
      error_code = "1",
      depends_on = [ActivityDependency(activity = activityName,
        dependency_conditions = [DependencyCondition.FAILED]),
        ActivityDependency(activity = ctx.removeAlias("DSU.UnixSH"),
        dependency_conditions = [DependencyCondition.COMPLETED])])]

def OptumIdsDrugClmPriceLoad(ctx):
  name = "OptumIdsDrugClmPriceLoad"
  artifacts, activities = OptumIdsDrugClmPriceLoadActivities(ctx)
  pipeline = PipelineResource(
    folder = PipelineFolder(name="claim/DrugClmPrice"),
    activities = activities,
    description = """
      This Job Loads Data from Sequential File created in Extract Job and loads into IDS DRUG_CLM_PRICE table And this job is called in OptumIdsDrugClmPriceLoadSeq sequence job
      MODIFICATIONS:
                                                                                                                                                                                                                                                                                    			DATASTAGE	CODE		DATE
      DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                                                                                    		ENVIRONMENT	REVIEWER	REVIEW
      ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
      Sri Nannapaneni        10-22-2019        Spread price              This Job Loads Data from Sequential File created in fkey Job and loads into IDS DRUG_CLM_PRICE table    IntegrateDev2	Kalyan Neelam	2019-11-26
      Rekha Radhakrishna  2020-02-05                                             Mapped new fields SELL_INCNTV_FEE_AMT,BUY_INCNTV_FEE_AMT                                                                       IntegrateDev2	Kalyan Neelam	2020-02-06
                                                                                                                    ,SELL_OTHR_PAYOR_AMT,SELL_OTHR_AMT and SPREAD_INCNTV_ FEE_AMT
      Geetanjali Rajendran    2021-06-03   PBM PhaseII              Mapped new fields FRMLRY_SK, DRUG_TYP_CD_SK and TIER_ID                                                                                IntegrateDev2	ABhiram Dasarathy	2021-06-23
                        
      Arpitha V                      2023-11-07     US 600305               Mapped new field PLN_DRUG_STTUS_CD_SK  to DRUG_CLM_PRICE table                                                                  IntegrateDevB              Jeyaprasanna           2024-01-01

      Ashok kumar B            2024-02-01        US 608682                        Added  PLAN_TYPE  to the DrugClmPrice Landing file                                                                                             IntegrateDev2              Jeyaprasanna           2024-03-14
      Parameters:
      -----------
      FilePath:
        EDW File Path
      IDSInstance:
        IDS Instance
      IDSAcct:
        IDS Account
      IDSPW:
        IDS Password
      IDSDB:
        IDS Database
      IDSDB2ArraySize:
        IDS DB2 Array Size for Load
      IDSDB2RecordCount:
        IDS DB2 Record Count for Load
      IDSOwner:
        IDS Table Owner""",
    parameters = {
      "FilePath": ParameterSpecification(type="String", default_value=ctx.FilePath),
      "IDSInstance": ParameterSpecification(type="String", default_value=ctx.IDSInstance),
      "IDSAcct": ParameterSpecification(type="String"),
      "IDSPW": ParameterSpecification(type="String"),
      "IDSDB": ParameterSpecification(type="String", default_value=ctx.IDSDB),
      "IDSDB2ArraySize": ParameterSpecification(type="String", default_value=ctx.IDSDB2ArraySize),
      "IDSDB2RecordCount": ParameterSpecification(type="String", default_value=ctx.IDSDB2RecordCount),
      "IDSOwner": ParameterSpecification(type="String", default_value=ctx.IDSOwner)})
  return artifacts + [(name, pipeline)]

def main():
  ctx = TranslationContext()
  artifacts = (OptumIdsDrugClmPriceLoad(ctx))
  if ctx.validateArtifacts(artifacts):
    ctx.deployArtifacts(artifacts)

if __name__ == '__main__':
  main()
