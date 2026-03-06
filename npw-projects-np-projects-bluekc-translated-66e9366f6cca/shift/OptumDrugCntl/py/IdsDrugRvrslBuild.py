#!/usr/bin/python3

from npadf import *

def IdsDrugRvrslBuildActivities(ctx):
  def dfIdsDrugRvrslBuild():
    def ids():
       ds_ls_db = LinkedServiceReference(type='LinkedServiceReference',reference_name=ctx.sqldb_linked_service)
       df_sources_inline = DataFlowSource(name='ids',linked_service=ds_ls_db)
       return DataflowSegment([df_sources_inline])
    def WCLMRVRSL():
       ds_ls_blob = LinkedServiceReference(type='LinkedServiceReference',reference_name=ctx.file_linked_service)
       df_sinks_inline = DataFlowSink(name='WCLMRVRSL', linked_service=ds_ls_blob)
       return DataflowSegment([df_sinks_inline])
    dataflowName = "IdsDrugRvrslBuild"
    buffer = DataflowBuffer()
    buffer.append(ids())
    buffer.append(WCLMRVRSL())
    dataflow = MappingDataFlow(
      folder = DataFlowFolder(name="claim/DrugReversal"),
      sources = buffer.sources,
      sinks = buffer.sinks,
      transformations = buffer.transformations,
      script = f"""parameters{{
        FilePath as string ("{ctx.FilePath}"),
        Source as string ,
        IDSInstance as string ("{ctx.IDSInstance}"),
        IDSDB as string ("{ctx.IDSDB}"),
        IDSOwner as string ("{ctx.IDSOwner}"),
        IDSAcct as string ,
        IDSPW as string 
      }}
    source(output(
        ORIG_CLM_SK as integer,
        SRC_SYS_CD_SK as integer,
        ORIG_CLM_ID as string,
        RVRSL_CLM_SK as integer,
        RVRSL_CLM_ID as string),
      allowSchemaDrift: true,
      validateSchema: false,
      format: 'query',
      store: 'sqlserver',
      query: ("SELECT \\nA02_CLM.CLM_SK,  \\nA02_CLM.SRC_SYS_CD_SK, \\nA02_CLM.CLM_ID, \\nA08_CLM.CLM_SK, \\nA08_CLM.CLM_ID\\nFROM  " + toString($IDSOwner) + ".W_DRUG_CLM         	ADJ_DRVR,\\n          " + toString($IDSOwner) + ".DRUG_CLM   	     	  A02_DRUG_CLM, \\n      	" + toString($IDSOwner) + ".DRUG_CLM             	 A08_DRUG_CLM,\\n      	" + toString($IDSOwner) + ".CLM        	      	 A02_CLM,\\n      	" + toString($IDSOwner) + ".CLM        	     	  A08_CLM,\\n     	 " + toString($IDSOwner) + ".CD_MPPNG   	  MAP1,\\n     	 " + toString($IDSOwner) + ".CD_MPPNG   	  MAP2\\nWHERE     	   ADJ_DRVR.SRC_SYS_CD       IN ( 'MEDIMPACT')\\n      	AND    ADJ_DRVR.SRC_SYS_CD_SK   =  A08_DRUG_CLM.SRC_SYS_CD_SK\\n      	AND    ADJ_DRVR.DRUG_CLM_SK       =  A08_DRUG_CLM.DRUG_CLM_SK\\n                AND    ADJ_DRVR.DRUG_CLM_SK       =  A08_CLM.CLM_SK  \\n      	AND    A08_CLM.CLM_STTUS_CD_SK  =  MAP2.CD_MPPNG_SK\\n      	AND    MAP2.TRGT_CD	                  = 'A08'\\n\\n         AND trim (A08_DRUG_CLM.VNDR_CLM_NO) 	= trim(A02_DRUG_CLM.VNDR_CLM_NO)\\n         AND trim (A08_DRUG_CLM.RX_NO )          = trim(A02_DRUG_CLM.RX_NO)\\n         and trim (A08_DRUG_CLM.RFL_NO)          = TRIM(A02_DRUG_CLM.RFL_NO)\\n     	AND A08_DRUG_CLM.FILL_DT_SK 		    = A02_DRUG_CLM.FILL_DT_SK\\n         AND A08_DRUG_CLM.FILL_DT_SK             = A02_CLM.SVC_STRT_DT_SK\\n         AND trim (A08_CLM.SUB_ID)           	= trim (A02_CLM.SUB_ID)\\n         AND trim (A08_CLM.MBR_SFX_NO)           = trim (A02_CLM.MBR_SFX_NO)\\n\\n         AND ADJ_DRVR.SRC_SYS_CD_SK              =  A02_CLM.SRC_SYS_CD_SK\\n         AND A08_DRUG_CLM.CLM_SK       	   <> A02_DRUG_CLM.CLM_SK \\n         AND A02_DRUG_CLM.CLM_SK      	       =  A02_CLM.CLM_SK\\n         AND A02_CLM.CLM_STTUS_CD_SK          =  MAP1.CD_MPPNG_SK\\n         AND SUBSTRING(ADJ_DRVR.CLM_ID, 1, LENGTH (ADJ_DRVR.CLM_ID)-1)  = TRIM(A02_CLM.CLM_ID )       \\n         AND MAP1.TRGT_CD  IN ( 'A02', 'A09' )\\n\\nUNION \\n\\nSELECT \\nA02_CLM.CLM_SK,  \\nA02_CLM.SRC_SYS_CD_SK, \\nA02_CLM.CLM_ID, \\nA08_CLM.CLM_SK, \\nA08_CLM.CLM_ID\\nFROM  " + toString($IDSOwner) + ".W_DRUG_CLM         	ADJ_DRVR,\\n          " + toString($IDSOwner) + ".DRUG_CLM   	     	  A02_DRUG_CLM, \\n      	" + toString($IDSOwner) + ".DRUG_CLM             	 A08_DRUG_CLM,\\n      	" + toString($IDSOwner) + ".CLM        	      	 A02_CLM,\\n      	" + toString($IDSOwner) + ".CLM        	     	  A08_CLM,\\n     	 " + toString($IDSOwner) + ".CD_MPPNG   	  MAP1,\\n     	 " + toString($IDSOwner) + ".CD_MPPNG   	  MAP2\\nWHERE     	   ADJ_DRVR.SRC_SYS_CD       IN ( 'OPTUMRX')\\n      	AND    ADJ_DRVR.SRC_SYS_CD_SK   =  A08_DRUG_CLM.SRC_SYS_CD_SK\\n      	AND    ADJ_DRVR.DRUG_CLM_SK       =  A08_DRUG_CLM.DRUG_CLM_SK\\n                AND    ADJ_DRVR.DRUG_CLM_SK       =  A08_CLM.CLM_SK  \\n      	AND    A08_CLM.CLM_STTUS_CD_SK  =  MAP2.CD_MPPNG_SK\\n      	AND    MAP2.TRGT_CD	                  = 'A08'\\n\\n         AND trim (A08_DRUG_CLM.VNDR_CLM_NO) 	= trim(A02_DRUG_CLM.VNDR_CLM_NO)\\n         AND trim (A08_DRUG_CLM.RX_NO )          = trim(A02_DRUG_CLM.RX_NO)\\n         and trim (A08_DRUG_CLM.RFL_NO)          = TRIM(A02_DRUG_CLM.RFL_NO)\\n     	AND A08_DRUG_CLM.FILL_DT_SK 		    = A02_DRUG_CLM.FILL_DT_SK\\n         AND A08_DRUG_CLM.FILL_DT_SK             = A02_CLM.SVC_STRT_DT_SK\\n         AND trim (A08_CLM.SUB_ID)           	= trim (A02_CLM.SUB_ID)\\n         AND trim (A08_CLM.MBR_SFX_NO)           = trim (A02_CLM.MBR_SFX_NO)\\n\\n         AND ADJ_DRVR.SRC_SYS_CD_SK              =  A02_CLM.SRC_SYS_CD_SK\\n         AND A08_DRUG_CLM.CLM_SK       	   <> A02_DRUG_CLM.CLM_SK \\n         AND A02_DRUG_CLM.CLM_SK      	       =  A02_CLM.CLM_SK\\n         AND A02_CLM.CLM_STTUS_CD_SK          =  MAP1.CD_MPPNG_SK\\n         AND SUBSTRING(ADJ_DRVR.CLM_ID, 1, LENGTH (ADJ_DRVR.CLM_ID)-1)  = TRIM(A02_CLM.CLM_ID )       \\n         AND MAP1.TRGT_CD  IN ( 'A02', 'A09' )\\n      \\n\\nunion\\nSELECT D.CLM_SK,ADJ.SRC_SYS_CD_SK,D.CLM_ID,ADJ.CLM_SK,ADJ.CLM_ID\\n FROM " + toString($IDSOwner) + ".W_DRUG_CLM    ADJ,\\n            " + toString($IDSOwner) + ".DRUG_CLM           D \\nWHERE ADJ.SRC_SYS_CD            = 'PCS' \\n      AND ADJ.SRC_SYS_CD_SK      = D.SRC_SYS_CD_SK \\n      AND ADJ.RX_NO                       = D.RX_NO \\n      AND ADJ.FILL_DT_SK               = D.FILL_DT_SK\\n      AND ADJ.CLM_SK                    <> D.CLM_SK\\n\\nUNION\\nSELECT D.CLM_SK,ADJ.SRC_SYS_CD_SK,D.CLM_ID,ADJ.CLM_SK,ADJ.CLM_ID\\nFROM   " + toString($IDSOwner) + ".W_DRUG_CLM ADJ,\\n             " + toString($IDSOwner) + ".DRUG_CLM D \\nWHERE ADJ.SRC_SYS_CD = 'WELLDYNERX' \\n      AND ADJ.SRC_SYS_CD_SK = D.SRC_SYS_CD_SK \\n      AND ADJ.VNDR_CLM_NO = D.VNDR_CLM_NO\\n      AND ADJ.CLM_SK <> D.CLM_SK\\n\\nUNION\\nSELECT D.CLM_SK,ADJ.SRC_SYS_CD_SK,D.CLM_ID,ADJ.CLM_SK,ADJ.CLM_ID\\n FROM " + toString($IDSOwner) + ".W_DRUG_CLM ADJ,\\n            " + toString($IDSOwner) + ".DRUG_CLM D \\nWHERE ADJ.SRC_SYS_CD = 'MEDTRAK' \\n      AND ADJ.SRC_SYS_CD_SK = D.SRC_SYS_CD_SK \\n      AND ADJ.VNDR_CLM_NO = D.VNDR_CLM_NO\\n      /*AND ADJ.FILL_DT_SK = D.FILL_DT_SK*/\\n      AND ADJ.CLM_SK <> D.CLM_SK\\n\\nUNION\\nSELECT D.CLM_SK,ADJ.SRC_SYS_CD_SK,D.CLM_ID,ADJ.CLM_SK,ADJ.CLM_ID\\n FROM " + toString($IDSOwner) + ".W_DRUG_CLM ADJ,\\n            " + toString($IDSOwner) + ".DRUG_CLM D \\nWHERE ADJ.SRC_SYS_CD = 'BCBSSC' \\n      AND ADJ.SRC_SYS_CD_SK = D.SRC_SYS_CD_SK \\n      AND ADJ.VNDR_CLM_NO = D.VNDR_CLM_NO\\n      AND ADJ.CLM_SK <> D.CLM_SK\\n\\n\\nUNION\\nSELECT \\nA02_CLM.CLM_SK,  \\nA02_CLM.SRC_SYS_CD_SK, \\nA02_CLM.CLM_ID, \\nA08_CLM.CLM_SK, \\nA08_CLM.CLM_ID\\n\\nFROM   " + toString($IDSOwner) + ".W_DRUG_CLM    ADJ_DRVR,\\n           " + toString($IDSOwner) + ".DRUG_CLM   	  A02_DRUG_CLM, \\n      	" + toString($IDSOwner) + ".DRUG_CLM         A08_DRUG_CLM,\\n      	" + toString($IDSOwner) + ".CLM        	  A02_CLM,\\n      	" + toString($IDSOwner) + ".CLM        	  A08_CLM,\\n     	" + toString($IDSOwner) + ".CD_MPPNG   	  MAP1,\\n     	" + toString($IDSOwner) + ".CD_MPPNG   	  MAP2\\n\\nWHERE     	ADJ_DRVR.SRC_SYS_CD                	IN ('ESI')\\n      	AND    ADJ_DRVR.SRC_SYS_CD_SK 	 	=  A08_DRUG_CLM.SRC_SYS_CD_SK\\n      	AND    ADJ_DRVR.DRUG_CLM_SK                	=  A08_DRUG_CLM.DRUG_CLM_SK  \\n                AND A08_DRUG_CLM.CLM_SK      	                = A08_CLM.CLM_SK\\n     	AND A08_CLM.PAYMT_REF_ID                     	<> 'NA'\\n      	AND A08_CLM.CLM_STTUS_CD_SK 		=  MAP2.CD_MPPNG_SK\\n      	AND MAP2.TRGT_CD		              = 'A08'\\n\\n                 AND A08_DRUG_CLM.VNDR_CLM_NO	=  A02_DRUG_CLM.VNDR_CLM_NO      \\n                AND A08_DRUG_CLM.RX_NO              	=  A02_DRUG_CLM.RX_NO\\n      	AND A08_DRUG_CLM.FILL_DT_SK 		=  A02_DRUG_CLM.FILL_DT_SK\\n      	AND A08_DRUG_CLM.CLM_SK      		<> A02_DRUG_CLM.CLM_SK \\n               AND  A08_CLM.SUB_ID                		= A02_CLM.SUB_ID\\n                AND A08_CLM.MBR_SFX_NO                               = A02_CLM.MBR_SFX_NO\\n      	\\n                AND A02_DRUG_CLM.CLM_SK      	                = A02_CLM.CLM_SK\\n      	AND A02_CLM.CLM_STTUS_CD_SK 		=  MAP1.CD_MPPNG_SK\\n      	AND MAP1.TRGT_CD			= 'A02'\\n      	AND A02_CLM.ADJ_TO_CLM_ID	 	= 'NA'\\n                AND (\\n                  ( A02_CLM.RCVD_DT_SK < A08_CLM.RCVD_DT_SK  )\\n                  OR \\n                  ( A02_CLM.RCVD_DT_SK = A08_CLM.RCVD_DT_SK\\n                    AND NOT EXISTS 	(SELECT NO_PRIOR_A02_CLM.CLM_SK \\n     	 			FROM  	" + toString($IDSOwner) + ".DRUG_CLM                            NO_PRIOR_A02_DRUG_CLM, \\n     	 			            " + toString($IDSOwner) + ".CLM                                      NO_PRIOR_A02_CLM,\\n     	 			            " + toString($IDSOwner) + ".CD_MPPNG                            MAP3\\n				WHERE   NO_PRIOR_A02_DRUG_CLM.VNDR_CLM_NO 	=  A08_DRUG_CLM.VNDR_CLM_NO\\n      				AND 	NO_PRIOR_A02_DRUG_CLM.RX_NO        	=  A08_DRUG_CLM.RX_NO\\n     				AND 	NO_PRIOR_A02_DRUG_CLM.FILL_DT_SK   	=  A08_DRUG_CLM.FILL_DT_SK\\n      				AND 	NO_PRIOR_A02_DRUG_CLM.CLM_SK       	<> A08_DRUG_CLM.CLM_SK\\n      				AND        NO_PRIOR_A02_DRUG_CLM.CLM_SK                =  NO_PRIOR_A02_CLM.CLM_SK\\n      				AND        NO_PRIOR_A02_CLM.SUB_ID                               = A08_CLM.SUB_ID\\n      				 AND        NO_PRIOR_A02_CLM.MBR_SFX_NO                   =  A08_CLM.MBR_SFX_NO  \\n     				AND 	NO_PRIOR_A02_CLM.CLM_STTUS_CD_SK 	=  MAP3.CD_MPPNG_SK\\n    				AND 	MAP3.TRGT_CD				= 'A02'\\n      				AND 	NO_PRIOR_A02_CLM.ADJ_TO_CLM_ID	= 'NA'\\n      				 AND       NO_PRIOR_A02_CLM.CLM_SK          	               <>  A02_CLM.CLM_SK\\n				AND	NO_PRIOR_A02_CLM.RCVD_DT_SK                  < A02_CLM.RCVD_DT_SK)\\n              )\\n            )\\n\\nUNION\\nSELECT D.CLM_SK,ADJ.SRC_SYS_CD_SK,D.CLM_ID,ADJ.CLM_SK,ADJ.CLM_ID\\n FROM " + toString($IDSOwner) + ".W_DRUG_CLM ADJ,\\n            " + toString($IDSOwner) + ".DRUG_CLM D \\nWHERE ADJ.SRC_SYS_CD = 'SAVRX' \\n      AND ADJ.SRC_SYS_CD_SK = D.SRC_SYS_CD_SK \\n      AND ADJ.VNDR_CLM_NO = D.VNDR_CLM_NO\\n      AND ADJ.CLM_SK <> D.CLM_SK\\n\\n\\nUNION\\nSELECT D.CLM_SK,ADJ.SRC_SYS_CD_SK,D.CLM_ID,ADJ.CLM_SK,ADJ.CLM_ID\\n FROM " + toString($IDSOwner) + ".W_DRUG_CLM ADJ,\\n            " + toString($IDSOwner) + ".DRUG_CLM D \\nWHERE ADJ.SRC_SYS_CD = 'LDI' \\n      AND ADJ.SRC_SYS_CD_SK = D.SRC_SYS_CD_SK \\n      AND ADJ.VNDR_CLM_NO = D.VNDR_CLM_NO\\n      AND ADJ.CLM_SK <> D.CLM_SK\\n\\nUNION\\nSELECT D.CLM_SK,ADJ.SRC_SYS_CD_SK,D.CLM_ID,ADJ.CLM_SK,ADJ.CLM_ID\\n FROM " + toString($IDSOwner) + ".W_DRUG_CLM ADJ,\\n            " + toString($IDSOwner) + ".DRUG_CLM D \\nWHERE ADJ.SRC_SYS_CD = 'CVS' \\n      AND ADJ.SRC_SYS_CD_SK = D.SRC_SYS_CD_SK \\n      AND ADJ.VNDR_CLM_NO = D.VNDR_CLM_NO\\n      AND ADJ.CLM_SK <> D.CLM_SK\\n"),
      schemaName: '',
      tableName: '',
      isolationLevel: 'READ_UNCOMMITTED') ~> ids
    ids sink(
      allowSchemaDrift: true,
      validateSchema: false,
      partitionFileNames:[(""" + ctx.getFileName("""$FilePath + '/load/W_CLM_RVRSL.' + $Source + '.dat'""") + f""")],
      partitionBy('hash', 1),
      skipDuplicateMapInputs: true,
      skipDuplicateMapOutputs: true,
      format: 'delimited',
      fileSystem: '{ctx.file_container}',
      folderPath: (""" + ctx.getFolderPath("""$FilePath + '/load/W_CLM_RVRSL.' + $Source + '.dat'""") + f""") ,
      columnDelimiter: ',',
      escapeChar:""" + '\\\\' + f""",
      quoteChar: '\\"',
      columnNamesAsHeader:false) ~> WCLMRVRSL""", type='MappingDataFlow')
    artifacts = buffer.artifacts + [(dataflowName, dataflow)]
    return artifacts, dataflowName
  artifacts, dataflowName = dfIdsDrugRvrslBuild()
  activityName = "IdsDrugRvrslBuild"
  activityParameters = {"FilePath":  {"value": "'@{pipeline().parameters.FilePath}'","type": "Expression"},
  "Source":  {"value": "'@{pipeline().parameters.Source}'","type": "Expression"},
  "IDSInstance":  {"value": "'@{pipeline().parameters.IDSInstance}'","type": "Expression"},
  "IDSDB":  {"value": "'@{pipeline().parameters.IDSDB}'","type": "Expression"},
  "IDSOwner":  {"value": "'@{pipeline().parameters.IDSOwner}'","type": "Expression"},
  "IDSAcct":  {"value": "'@{pipeline().parameters.IDSAcct}'","type": "Expression"},
  "IDSPW":  {"value": "'@{pipeline().parameters.IDSPW}'","type": "Expression"}}
  activity = ExecuteDataFlowActivity(
    name = activityName,
    data_flow = DataFlowReference(reference_name=dataflowName,
      parameters=activityParameters, type='DataFlowReference'))
  return artifacts, [activity]

def IdsDrugRvrslBuild(ctx):
  name = "IdsDrugRvrslBuild"
  artifacts, activities = IdsDrugRvrslBuildActivities(ctx)
  pipeline = PipelineResource(
    folder = PipelineFolder(name="claim/DrugReversal"),
    activities = activities,
    description = """
      IDS Drug adjustments - Reversal process
      COPYRIGHT 2005 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
      COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 

      DESCRIPTION:     Build P_CLM_RVRSL to use in IdsDrugReversal

      PROCESSING: build file with adjusted to and adjusted from ids for reversals to use in IdsDrugReversal. associate the reversal claim with the original claim

      OUTPUTS:  W_CLM_RVRSL used in IdsDrugReversal
      MODIFICATIONS:
                  BJ Luce               2005/05           originally programmed
                  Steph Goddard    2006/04/17     changed where statement to cast fill_dt to match fill_dt_sk 
                  Brent Leland        2006-04-20      Change DB2 insert to sequential file.
                                                                      Changed parameters to envrionment values.
      MODIFICATIONS:
      Developer         Date               Project/Altiris #      Change Description                                                                    Development Project          Code Reviewer           Date Reviewed
      ------------------       ----------------       ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
      Sandrew          2008-10-03       3784(PBM)           Added extraction criteria to include ESI drug claims                     devlIDSnew                    Steph Goddard          10/28/2008
      KNeelam         2010-12-21        4616                    Added extraction criteria for new source MEDTRAK                    IntegrateNewDevl          Steph Goddard          12/23/2010
      KNeelam         2012-01-17        4784                    Added extraction criteria for new source BCBSSC                        IntegrateCurDevl            Brent Leland               02-09-2012
      SAndrew         2014-07-21        ServiceNowINC0107293                                                                                                                                   Kalyan Neelam          2014-09-08
      The ESI data has changed due to Anchor to F14 ESI System conversion.    The input file.field ESI_ADJ_REF_NO is mapped to the IDS.DRUG_CLM.VNDR_CLM_ID.  This used to be unquie for every claim and its finacial adjustments.   Example, f a claim was entered into ESI System, it received value of 100 for ESI_ADJ_REF_NO.   If they adjusted the claim, they would send the same claim, with same ESI_ADJ_REF_NO but the claim would have a different claim status.   Then if ESI decided to then pay the claim again (they corrected Deduct Amt, COB Amounts, the member ended up purchasing the claim after the first filled version was shelved while waiting fo rmember to pick up and then ultimiately purchased the drug, the claim data would be sent with a diffrent ESI_ADJ_REF_NO.  This is important to understand becuase the ESI_ADJ_REF_NO -> DRUG_CLM.CLM_VNDR_NO was used to tie the IDS.CLM.ESI.CLM_ID"R" of A08  back to the original claim IDS.CLM.ESI.CLM of A02 that it was to adjust.
      Now ESI is populating the  ESI_ADJ_REF_NO with the same value for every tweek to the claim.   There needed to be a business rule updates made in order to identify the origina claim, the IDS.CLM.ESI.CLM of A02, the Adjusting claim record is to negate out, the IDS.CLM.ESI.CLM_ID"R" of A08.   The criteria used to only be:
                                                                                           UNION
                                                                                           SELECT D.CLM_SK,ADJ.SRC_SYS_CD_SK,D.CLM_ID,ADJ.CLM_SK,ADJ.CLM_ID
                                                                                           FROM   #$IDSOwner#.W_DRUG_CLM ADJ,   #$IDSOwner#.DRUG_CLM        D,    #$IDSOwner#.CLM                    ORIG_CLM,  #$IDSOwner#.CLM                    ADJ_CLM,  #$IDSOwner#.CD_MPPNG       MAP1
                                                                                           WHERE ADJ.SRC_SYS_CD = 'ESI'
                                                                                                  AND ADJ.SRC_SYS_CD_SK = D.SRC_SYS_CD_SK 
                                                                                                 AND ADJ.VNDR_CLM_NO = D.VNDR_CLM_NO
                                                                                                 AND ADJ.CLM_SK <> D.CLM_SK
                                                                                                 AND ADJ.CLM_SK = ADJ_CLM.CLM_SK
                                                                                                 AND D.CLM_SK   = ORIG_CLM.CLM_SK
      Then it became 
                                                                                           UNION
                                                                                           SELECT D.CLM_SK,ADJ.SRC_SYS_CD_SK,D.CLM_ID,ADJ.CLM_SK,ADJ.CLM_ID
                                                                                           FROM   #$IDSOwner#.W_DRUG_CLM ADJ,   #$IDSOwner#.DRUG_CLM        D,   #$IDSOwner#.CLM                    ORIG_CLM,   #$IDSOwner#.CLM                    ADJ_CLM,   #$IDSOwner#.CD_MPPNG       MAP1
                                                                                           WHERE ADJ.SRC_SYS_CD = 'ESI'
                                                                                                  AND ADJ.SRC_SYS_CD_SK = D.SRC_SYS_CD_SK 
                                                                                                 AND ADJ.VNDR_CLM_NO = D.VNDR_CLM_NO
                                                                                                 AND ADJ.CLM_SK <> D.CLM_SK
                                                                                                 AND ADJ.CLM_SK = ADJ_CLM.CLM_SK
                                                                                                 AND D.CLM_SK   = ORIG_CLM.CLM_SK
                                                                                                 AND ADJ_CLM.PAYMT_REF_ID > ORIG_CLM.PAYMT_REF_ID
                                                                                                 AND ORIG_CLM.CLM_STTUS_CD_SK = MAP1.CD_MPPNG_SK
                                                                                                 AND MAP1.TRGT_CD='A02'
                                                                                                 AND ORIG_CLM.PAYMT_REF_ID = 
                                                                                                                     (SELECT MAX( MAX_CLM.PAYMT_REF_ID) 
                                                                                                                      FROM #$IDSOwner#.CLM MAX_CLM,    #$IDSOwner#.DRUG_CLM MAX_DRUG
                                                                                                                     WHERE MAX_CLM.CLM_SK = MAX_DRUG.CLM_SK
                                                                                                                            AND MAX_DRUG.VNDR_CLM_NO =  ADJ.VNDR_CLM_NO
                                                                                                                            AND MAX_CLM.PAYMT_REF_ID < ADJ_CLM.PAYMT_REF_ID)
      It now makes sure it looks for a paid claim with an A02 that was loaded to the system from a prior run before it looks at any A02 that was loaded as part of this batch run.   the key difference is the claims input date.


      KapoorK               2018-01-24        5828              Added Query for SAVRX                                                                   IntegrateDev2                   Kalyan Neelam                                       2018-02-26 
      KapoorK               2018-03-15        5828              Added Query for LDI                                                                         IntegrateDev2                   Jaideep Mankala                                    03/20/2018
      KapoorK               2018-09-20        5828              Added Query for CVS                                                                        IntegrateDev2                   Kalyan Neelam                                       2018-10-01

      Rekha Radhakrishna 2019-11-01    6131           Added Query for OPTUMRX                                                             IntegrateDev2                    KalyanNeelam                                        2019-11-20

      Peter Gichiri               2020 -05-27    6131          Added the filter criteria 'A09'  to the OPTUMRX SQL                        IntegrateDev2             Jaideep Mankala                            05/29/2020      

      Goutham Kalidindi     2020-10-22   US283560   Added Query for MEDIMPACT                                                           IntegrateDev2                    Kalyan Neelam                                       2020-11-03
      Parameters:
      -----------
      FilePath:
        IDS File Path
      Source:
        Source System
      IDSInstance:
        IDS Instance
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
      "Source": ParameterSpecification(type="String"),
      "IDSInstance": ParameterSpecification(type="String", default_value=ctx.IDSInstance),
      "IDSDB": ParameterSpecification(type="String", default_value=ctx.IDSDB),
      "IDSOwner": ParameterSpecification(type="String", default_value=ctx.IDSOwner),
      "IDSAcct": ParameterSpecification(type="String"),
      "IDSPW": ParameterSpecification(type="String")})
  return artifacts + [(name, pipeline)]

def main():
  ctx = TranslationContext()
  artifacts = (IdsDrugRvrslBuild(ctx))
  if ctx.validateArtifacts(artifacts):
    ctx.deployArtifacts(artifacts)

if __name__ == '__main__':
  main()
