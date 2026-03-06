model:
  config:
    origin: datastage
  jobs:
    - name: OptumDrugCntl
      origin: datastage
      type: controlflow
      folder: claim
      checkpoint: true
      description: "Extract OPTUMRX claims and load to IDS called via CLAIMS_OPTUMRX_DLY_WRHS_000"
      parameters:
        - name: APT_CONFIG_FILE
          type: string
          description: "Configuration file"
          longDescription: "The Parallel job configuration file."
          defaultType: project
        - name: FilePath
          type: string
          description: "File Path"
          defaultType: project
        - name: EDWFilePath
          type: string
          description: "EDW File Path"
          defaultType: project
        - name: EmailSupport
          type: string
          description: "Warehouse Support Email"
          defaultType: project
        - name: IDSInstance
          type: string
          description: "IDS DB Instance"
          defaultType: project
        - name: IDSDSN
          type: string
          description: "IDS DSN"
          defaultType: project
        - name: IDSDB
          type: string
          description: "IDS Database"
          defaultType: project
        - name: IDSOwner
          type: string
          description: "IDS DB Owner"
          defaultType: project
        - name: IDSAcct
          type: string
          description: "IDS DB Account"
        - name: IDSPW
          type: encrypted
          description: "IDS DB Password"
        - name: CactusServer
          type: string
          description: "Cactus Server"
          defaultType: project
        - name: CactusDB
          type: string
          description: "Cactus Database"
          defaultType: project
        - name: CactusOwner
          type: string
          description: "Cactus Table Owner"
          defaultType: project
        - name: CactusAcct
          type: string
          description: "Cactus Account"
        - name: CactusPW
          type: encrypted
          description: "Cactus Password"
        - name: ClmMartAcct
          type: string
          description: "Claim Mart Account"
        - name: ClmMartPW
          type: encrypted
          description: "Claim Mart Password"
        - name: ClmMartServer
          type: string
          description: "Claim Mart Server"
          defaultType: project
        - name: ClmMartDB
          type: string
          description: "Claim Mart Database"
          defaultType: project
        - name: ClmMartOwner
          type: string
          description: "Claim Mart Table Owner"
          defaultType: project
        - name: UWSDB
          type: string
          description: "UWS Database"
          defaultType: project
        - name: UWSOwner
          type: string
          description: "UWS Table Owner"
          defaultType: project
        - name: UWSPW
          type: encrypted
          description: "UWS Password"
        - name: UWSAcct
          type: string
          description: "UWS Account"
        - name: LANAcct
          type: string
          description: "LAN Account"
        - name: LANPW
          type: encrypted
          description: "LAN Password"
        - name: IDSDB2ArraySize
          type: string
          description: "IDS DB2 Array Size for Load"
          defaultType: project
        - name: IDSDB2RecordCount
          type: string
          description: "IDS DB2 Record Count for Load"
          defaultType: project
        - name: VendorEmail
          type: string
          description: VendorEmail
        - name: Env
          type: string
          description: Env
          defaultType: expr
          defaultValue: T
        - name: MbrTermDate
          type: string
          description: MbrTermDate
          longDescription: "Used in IDS Member Extract for matching back to Optum claims.  Should not be receiving claims for termed members.  To avoid all members for past 20 years."
        - name: LOB
          type: string
          description: LOB
      activities:
        - name: BCBSKCClmLoadSeq
          type: dataflow
          executionType: reset then run
          jobname: BCBSKCCommClmLoadSeq
          parameters:
            - name: "FilePath"
              expression: "$FilePath"
            - name: "IDSDSN"
              expression: "$IDSDSN"
            - name: "IDSInstance"
              expression: "$IDSInstance"
            - name: "IDSDB"
              expression: "$IDSDB"
            - name: "IDSOwner"
              expression: "$IDSOwner"
            - name: "IDSAcct"
              expression: "$IDSAcct"
            - name: "IDSPW"
              expression: "$IDSPW"
            - name: "Logging"
              expression: "LoopVariables.Logging"
            - name: "RunCycle"
              expression: "Run_Cycle.RunCycle"
            - name: "RunID"
              expression: "LoopVariables.RunID"
            - name: "ExclusionList"
              expression: "LinkCount.ExclusionList"
            - name: "SrcSysCd"
              expression: "Initial.Source"
            - name: "SrcSysCdSk"
              expression: "Variables.SrcSysCdSk"
            - name: "Source"
              expression: "Initial.Source"
          triggers: 
            - activity: Or_1B
        - name: ListFiles
          type: userVars
          parameters:
              - name: FileList
                type: string
                expression: "LIST.FILES($FilePath:'/landing', 'OPTUMRX.PBM.DrugClm_COMM*')"
          triggers: 
            - activity: If_1A
        - name: Any5
          type: sequencer
          sequencerType: all
          triggers: 
            - activity: Any6
        - name: Any6
          type: sequencer
          sequencerType: all
          triggers: 
            - activity: StartLoop
        - name: StartLoop
          type: startLoop
          loopType: list
          loopList: "#ListFiles.FileList#"
          delimiter: ","
          triggers: 
            - activity: Run_Cycle
        - name: EndLoop
          type: endLoop
          startID: StartLoop
          triggers: 
            - activity: MvSrcFile
        - name: Any8
          type: sequencer
          sequencerType: any
          triggers: 
            - activity: If_1E
        - name: CpSrcFile
          type: shell
          path: cd
          command: "#$FilePath#/landing; cp -f #StartLoop.$Counter# processed/#StartLoop.$Counter#.#LoopVariables.RunID#; compress -f processed/#StartLoop.$Counter#.#LoopVariables.RunID# ;"
          triggers: 
            - activity: MvControlFile
        - name: MvSrcFile
          type: shell
          path: cd
          command: "#$FilePath#/landing; rm -f OPTUMRX.PBM.DrugClm_COMM*.* "
          triggers: 
            - activity: Success
        - name: OptumDrugLandSeq
          type: dataflow
          executionType: reset then run
          jobname: OptumDrugLandSeq
          parameters:
            - name: "CurrentDate"
              expression: "LoopVariables.RunCycleDate"
            - name: "FilePath"
              expression: "$FilePath"
            - name: "RunCycle"
              expression: "Run_Cycle.RunCycle"
            - name: "ExclusionList"
              expression: "LinkCount.ExclusionList"
            - name: "RunID"
              expression: "LoopVariables.RunID"
            - name: "SrcSysCd"
              expression: "Initial.Source"
            - name: "SrcSysCdSK"
              expression: "Variables.SrcSysCdSk"
            - name: "IDSInstance"
              expression: "$IDSInstance"
            - name: "IDSDB"
              expression: "$IDSDB"
            - name: "IDSOwner"
              expression: "$IDSOwner"
            - name: "IDSDSN"
              expression: "$IDSDSN"
            - name: "IDSAcct"
              expression: "$IDSAcct"
            - name: "IDSPW"
              expression: "$IDSPW"
            - name: "CactusServer"
              expression: "$CactusServer"
            - name: "CactusDB"
              expression: "$CactusDB"
            - name: "CactusOwner"
              expression: "$CactusOwner"
            - name: "CactusAcct"
              expression: "$CactusAcct"
            - name: "CactusPW"
              expression: "$CactusPW"
            - name: "Logging"
              expression: "LoopVariables.Logging"
            - name: "VendorEmail"
              expression: VendorEmail
            - name: "LANAcct"
              expression: "$LANAcct"
            - name: "LANPW"
              expression: "$LANPW"
            - name: "ProvRunCycle"
              expression: "Run_Cycle.ProvRunCycle"
            - name: "InFile"
              expression: "StartLoop.$Counter"
            - name: "Env"
              expression: Env
            - name: "MbrTermDate"
              expression: MbrTermDate
          triggers: 
            - activity: Or_2A
        - name: OptumIdsMbrPdxDeniedTransCntlSeq
          type: dataflow
          executionType: reset then run
          jobname: OptumIdsMbrPdxDeniedTransLoadSeq
          parameters:
            - name: "APT_CONFIG_FILE"
              expression: "$APT_CONFIG_FILE"
            - name: "IDSInstance"
              expression: "$IDSInstance"
            - name: "IDSDSN"
              expression: "$IDSDSN"
            - name: "IDSAcct"
              expression: "$IDSAcct"
            - name: "IDSDB"
              expression: "$IDSDB"
            - name: "IDSPW"
              expression: "$IDSPW"
            - name: "IDSOwner"
              expression: "$IDSOwner"
            - name: "FilePath"
              expression: "$FilePath"
            - name: "SrcSysCd"
              expression: "Initial.Source"
            - name: "RunID"
              expression: "LoopVariables.RunID"
            - name: "IDSRunCycle"
              expression: "Run_Cycle.MbrDndPdxRunCycle"
            - name: "IDSDB2ArraySize"
              expression: "$IDSDB2ArraySize"
            - name: "IDSDB2RecordCount"
              expression: "$IDSDB2RecordCount"
            - name: "ExclusionList"
              expression: "LinkCount.ExclusionList"
            - name: "PrefixFkeyFailedFileName"
              expression: "\"PrefixFkeyFailedFileName\""
            - name: "CurrDate"
              expression: "LoopVariables.RunCycleDate"
          triggers: 
            - activity: Any8
        - name: ProcessCheckExtr
          type: dataflow
          executionType: run
          jobname: ProcessCheckExtr
          parameters:
            - name: "IDSDB"
              expression: "$IDSDB"
            - name: "IDSOwner"
              expression: "$IDSOwner"
            - name: "IDSAcct"
              expression: "$IDSAcct"
            - name: "IDSPW"
              expression: "$IDSPW"
            - name: "Subject"
              expression: "Initial.Subject"
            - name: "TargetSys"
              expression: "Initial.Target"
            - name: "SourceSys"
              expression: "Initial.Source"
          triggers: 
            - activity: LinkCount
        - name: CountChk
          type: switch
          triggers: 
            - activity: Any1
              conditionType: Otherwise
            - activity: Email_DAT
              conditionType: Custom
              condition: "LinkCount.LinkCount <> 0"
        - name: Initial
          type: userVars
          parameters:
              - name: JobName
                type: string
                expression: DSJobName
              - name: GroupName
                type: string
                expression: "\"IDS_OPTUMRX_DRUG\""
              - name: Source
                type: string
                expression: "\"OPTUMRX\""
              - name: Target
                type: string
                expression: "\"IDS\""
              - name: Subject
                type: string
                expression: "\"CLAIM\""
              - name: Frequency
                type: string
                expression: "\"DAILY\""
          triggers: 
            - activity: ProcessCheckExtr
        - name: Email_DAT
          type: sendEmail
          mailServer: "mail.bcbskc.com"
          to: "#$EmailSupport#"
          from: "_DATA_WAREHOUSE_SUPPORT@bcbskc.com"
          subject: "Optum RX Claim Process Abort"
          messageSource: "The Optum RX Claim processing cannot run while IDS claim is being updated or extracted from.\n\nSource:  Optum RX\nTarget:    IDS\nSubject:  Claim"
          triggers: 
            - activity: Failed
        - name: Failed
          type: shell
          path: echo
          command: "   CANNOT RUN WHILE OTHER #Initial.Subject# JOB IS RUNNING / ABORTED.       failed > #$FilePath#/scripts/log/#Initial.JobName#.log"
        - name: Any1
          type: sequencer
          sequencerType: any
          triggers: 
            - activity: Any2
        - name: Any2
          type: sequencer
          sequencerType: any
          triggers: 
            - activity: ListFiles
        - name: LinkCount
          type: userVars
          parameters:
              - name: LinkCount
                type: string
                expression: "JobLinkCount('ProcessCheckExtr.':Initial.JobName, 'Trans1', 'Output')"
              - name: ExclusionList
                type: string
                expression: "GetExclusionList(Initial.JobName)"
          triggers: 
            - activity: CountChk
        - name: LogRunCycle
          type: routine
          skipCheckpoint: true
          routinename: "WRITE.LOG"
          parameters:
            - name: "Message"
              expression: "\"Current Run Cycle: \":Run_Cycle.RunCycle"
            - name: "LogType"
              expression: "\"INFO\""
          triggers: 
            - activity: LogProvRunCycle
        - name: Run_Cycle
          type: userVars
          parameters:
              - name: RunCycle
                type: string
                expression: "OpenRunCycle($IDSInstance, $IDSOwner, $IDSDSN, $IDSAcct, $IDSPW, Initial.Source, Initial.Target, Initial.Subject, Initial.Frequency)"
              - name: ProvRunCycle
                type: string
                expression: "OpenRunCycle($IDSInstance, $IDSOwner, $IDSDSN, $IDSAcct, $IDSPW, \"NABP\", \"IDS\", \"PROVIDERS\", 'DAILY')"
              - name: MbrDndPdxRunCycle
                type: string
                expression: "OpenRunCycle($IDSInstance, $IDSOwner, $IDSDSN, $IDSAcct, $IDSPW, Initial.Source, Initial.Target, 'DENIED_CLAIMS', Initial.Frequency)"
          triggers: 
            - activity: LogRunCycle
        - name: If_6A
          type: switch
          triggers: 
            - activity: Or_3A
              conditionType: Otherwise
            - activity: IDSDrugDailyCleanupSeq
              conditionType: Custom
              condition: "Count (LinkCount.ExclusionList, \";\": \"IDSDrugDailyCleanupSeq\" :\";\") = 0"
        - name: Or_3A
          type: sequencer
          sequencerType: any
          triggers: 
            - activity: Loop14
        - name: ProcessRptExtr
          type: dataflow
          executionType: reset then run
          jobname: ProcessRptExtr
          parameters:
            - name: "IDSFilePath"
              expression: "$FilePath"
            - name: "RunCycle"
              expression: "Run_Cycle.RunCycle"
            - name: "Subject"
              expression: "Initial.Subject"
            - name: "SourceSys"
              expression: "Initial.Source"
            - name: "TargetSys"
              expression: "Initial.Target"
            - name: "RowCount"
              expression: "RptLinkCount.LinkCount"
            - name: "IDSInstance"
              expression: "$IDSInstance"
            - name: "IDSDB"
              expression: "$IDSDB"
            - name: "IDSOwner"
              expression: "$IDSOwner"
            - name: "IDSAcct"
              expression: "$IDSAcct"
            - name: "IDSPW"
              expression: "$IDSPW"
          triggers: 
            - activity: If_4B
        - name: If_4A
          type: switch
          triggers: 
            - activity: ProcessRptExtr
              conditionType: Custom
              condition: "Count (LinkCount.ExclusionList, \";\": \"ProcessRptExtr\" :\";\") = 0"
            - activity: If_4B
              conditionType: Otherwise
        - name: If_4B
          type: sequencer
          sequencerType: any
          triggers: 
            - activity: CpSrcFile
        - name: RptLinkCount
          type: userVars
          parameters:
              - name: LinkCount
                type: string
                expression: "JobLinkCount('IdsClmFkey', 'Link_Collector_237', 'LoadFile')"
              - name: CntlFileName
                type: string
                expression: "Ereplace(Exec_CntrlFilePick.$CommandOutput,@FM,'' )"
          triggers: 
            - activity: If_4A
        - name: CloseRunCycle
          type: routine
          routinename: CloseRunCycle
          parameters:
            - name: "DBInstance"
              expression: "$IDSInstance"
            - name: "DBOwner"
              expression: "$IDSOwner"
            - name: "DBDsn"
              expression: "$IDSDSN"
            - name: "DBAcct"
              expression: "$IDSAcct"
            - name: "DBPW"
              expression: "$IDSPW"
            - name: "SrcSystem"
              expression: "Initial.Source"
            - name: "TrgtSystem"
              expression: "Initial.Target"
            - name: "SubjCode"
              expression: "Initial.Subject"
            - name: "FreqCode"
              expression: "Initial.Frequency"
            - name: "RunCycle"
              expression: "Run_Cycle.RunCycle"
          triggers: 
            - activity: CloseProvRunCycle
        - name: IDSDrugDailyCleanupSeq
          type: dataflow
          executionType: reset then run
          jobname: IDSDrugDailyCleanupSeq
          parameters:
            - name: "FilePath"
              expression: "$FilePath"
            - name: "ExclusionList"
              expression: "LinkCount.ExclusionList"
            - name: "RunID"
              expression: "LoopVariables.RunID"
            - name: "IDSAcct"
              expression: "$IDSAcct"
            - name: "IDSPW"
              expression: "$IDSPW"
            - name: "IDSOwner"
              expression: "$IDSOwner"
            - name: "IDSInstance"
              expression: "$IDSInstance"
            - name: "IDSDB"
              expression: "$IDSDB"
            - name: "IDSDSN"
              expression: "$IDSDSN"
            - name: "SrcSysCd"
              expression: "Initial.Source"
          triggers: 
            - activity: Or_3A
        - name: Success
          type: shell
          path: echo
          command: "success > #$FilePath#/scripts/log/#Initial.JobName#.log"
        - name: CloseProvRunCycle
          type: routine
          routinename: CloseRunCycle
          parameters:
            - name: "DBInstance"
              expression: "$IDSInstance"
            - name: "DBOwner"
              expression: "$IDSOwner"
            - name: "DBDsn"
              expression: "$IDSDSN"
            - name: "DBAcct"
              expression: "$IDSAcct"
            - name: "DBPW"
              expression: "$IDSPW"
            - name: "SrcSystem"
              expression: "'NABP'"
            - name: "TrgtSystem"
              expression: "'IDS'"
            - name: "SubjCode"
              expression: "'PROVIDERS'"
            - name: "FreqCode"
              expression: "'DAILY'"
            - name: "RunCycle"
              expression: "Run_Cycle.ProvRunCycle"
          triggers: 
            - activity: CloseDeniedRunCycle
        - name: SuccessButNoFile
          type: shell
          path: echo
          command: "success > #$FilePath#/scripts/log/#Initial.JobName#.log"
        - name: If_1A
          type: switch
          triggers: 
            - activity: EmailNoFile
              conditionType: Custom
              condition: "Len(Trim( ListFiles.FileList )) = 0"
            - activity: ExclusionListExtr
              conditionType: Otherwise
        - name: ExclusionListExtr
          type: dataflow
          skipCheckpoint: true
          executionType: reset then run
          jobname: ExclusionListExtr
          parameters:
            - name: "IDSDB"
              expression: "$IDSDB"
            - name: "IDSOwner"
              expression: "$IDSOwner"
            - name: "IDSAcct"
              expression: "$IDSAcct"
            - name: "IDSPW"
              expression: "$IDSPW"
            - name: "Subject"
              expression: "Initial.JobName"
          triggers: 
            - activity: Variables
        - name: Variables
          type: userVars
          parameters:
              - name: Logging
                type: string
                expression: "'Y'"
              - name: SrcSysCdSk
                type: string
                expression: "GetFkeyCodes(\"IDS\", \"1\",  \"SOURCE SYSTEM\",  Initial.Source , \"X\") "
          triggers: 
            - activity: ExclusionListDelete
        - name: ExclusionListDelete
          type: dataflow
          skipCheckpoint: true
          executionType: reset then run
          jobname: ExclusionListDelete
          parameters:
            - name: "IDSDB"
              expression: "$IDSDB"
            - name: "IDSOwner"
              expression: "$IDSOwner"
            - name: "IDSAcct"
              expression: "$IDSAcct"
            - name: "IDSPW"
              expression: "$IDSPW"
            - name: "Subject"
              expression: "Initial.JobName"
          triggers: 
            - activity: Any5
        - name: EmailNoFile
          type: sendEmail
          mailServer: "mail.bcbskc.com"
          to: "#$EmailSupport#"
          from: "_DATA_WAREHOUSE_SUPPORT@bcbskc.com"
          subject: "OPTUMRX.PBM.DrugClm_*.dat files not found"
          messageSource: "The OPTUMRX.PBM.DrugClm_*.dat file was not found for OPTUM processing. The job has ended."
          triggers: 
            - activity: SuccessButNoFile
        - name: LoopVariables
          type: userVars
          parameters:
              - name: RunID
                type: string
                expression: "GetRunInfo(Initial.JobName:\"RunID\")"
              - name: RunCycleDate
                type: string
                expression: "GetRunCycleDate($IDSInstance, $IDSOwner, $IDSDSN, $IDSAcct, $IDSPW, Initial.Subject, Initial.Source, Initial.Target, Run_Cycle.RunCycle)"
              - name: Logging
                type: string
                expression: "'N'"
              - name: CurrRunCycle
                type: string
                expression: "Run_Cycle.RunCycle"
              - name: FillDtMin
                type: string
                expression: "GetRunInfo(\"IdsClmMartDrugCntlFillDtMin\")"
          triggers: 
            - activity: Loop1
        - name: Or_5C
          type: sequencer
          sequencerType: any
          triggers: 
            - activity: If_5D
        - name: If_5A
          type: switch
          triggers: 
            - activity: IdsWebDMDrugPrereqSeq
              conditionType: Custom
              condition: "Count (LinkCount.ExclusionList, \";\": \"IdsWebDMDrugPrereqSeq\" :\";\") = 0"
            - activity: Or_5A
              conditionType: Otherwise
        - name: If_5B
          type: switch
          triggers: 
            - activity: IdsWebDMDrugLoadSeq
              conditionType: Custom
              condition: "Count (LinkCount.ExclusionList, \";\": \"IdsWebDMDrugLoadSeq\" :\";\") = 0"
            - activity: Or_5B
              conditionType: Otherwise
        - name: If_5C
          type: switch
          triggers: 
            - activity: WebDMLastUpd
              conditionType: Custom
              condition: "Count (LinkCount.ExclusionList, \";\": \"WebDMLastUpd\" :\";\") = 0"
            - activity: Or_5C
              conditionType: Otherwise
        - name: WebDMLastUpd
          type: dataflow
          executionType: reset then run
          jobname: WebDMLastUpd
          parameters:
            - name: "IDSInstance"
              expression: "$IDSInstance"
            - name: "IDSDB"
              expression: "$IDSDB"
            - name: "IDSAcct"
              expression: "$IDSAcct"
            - name: "IDSPW"
              expression: "$IDSPW"
            - name: "ClmMartServer"
              expression: "$ClmMartServer"
            - name: "ClmMartDB"
              expression: "$ClmMartDB"
            - name: "ClmMartOwner"
              expression: "$ClmMartOwner"
            - name: "ClmMartAcct"
              expression: "$ClmMartAcct"
            - name: "ClmMartPW"
              expression: "$ClmMartPW"
            - name: "SubjectName"
              expression: "'OPTUMRX CLAIM'"
            - name: "UpdateDate"
              expression: "FORMAT.DATE (@DATE, \"DATE\", \"CURRENT\", \"SYBTIMESTAMP\")"
          triggers: 
            - activity: Or_5C
        - name: IdsWebDMDrugPrereqSeq
          type: dataflow
          executionType: reset then run
          jobname: IdsWebDMDrugPrereqSeq
          parameters:
            - name: "FilePath"
              expression: "$FilePath"
            - name: "ExclusionList"
              expression: "LinkCount.ExclusionList"
            - name: "RunID"
              expression: "LoopVariables.RunID"
            - name: "IDSInstance"
              expression: "$IDSInstance"
            - name: "IDSOwner"
              expression: "$IDSOwner"
            - name: "IDSDB"
              expression: "$IDSDB"
            - name: "IDSDSN"
              expression: "$IDSDSN"
            - name: "IDSAcct"
              expression: "$IDSAcct"
            - name: "IDSPW"
              expression: "$IDSPW"
          triggers: 
            - activity: Or_5A
        - name: IdsWebDMDrugLoadSeq
          type: dataflow
          executionType: reset then run
          jobname: IdsWebDMDrugLoadSeq
          parameters:
            - name: "FilePath"
              expression: "$FilePath"
            - name: "ExclusionList"
              expression: "LinkCount.ExclusionList"
            - name: "RunID"
              expression: "LoopVariables.RunID"
            - name: "RunCycle"
              expression: "Run_Cycle.RunCycle"
            - name: "IDSInstance"
              expression: "$IDSInstance"
            - name: "IDSDB"
              expression: "$IDSDB"
            - name: "IDSDSN"
              expression: "$IDSDSN"
            - name: "IDSOwner"
              expression: "$IDSOwner"
            - name: "IDSAcct"
              expression: "$IDSAcct"
            - name: "IDSPW"
              expression: "$IDSPW"
            - name: "ClmMartServer"
              expression: "$ClmMartServer"
            - name: "ClmMartDB"
              expression: "$ClmMartDB"
            - name: "ClmMartOwner"
              expression: "$ClmMartOwner"
            - name: "ClmMartAcct"
              expression: "$ClmMartAcct"
            - name: "ClmMartPW"
              expression: "$ClmMartPW"
          triggers: 
            - activity: Or_5B
        - name: Or_5B
          type: sequencer
          sequencerType: any
          triggers: 
            - activity: If_5C
        - name: Or_5A
          type: sequencer
          sequencerType: any
          triggers: 
            - activity: If_5B
        - name: If_1D
          type: switch
          triggers: 
            - activity: OptumIdsMbrPdxDeniedTransCntlSeq
              conditionType: Custom
              condition: "Count (LinkCount.ExclusionList, \";\": \"OptumIdsMbrPdxDeniedTransLoadSeq\" :\";\") = 0"
            - activity: Any8
              conditionType: Otherwise
        - name: LogProvRunCycle
          type: routine
          skipCheckpoint: true
          routinename: "WRITE.LOG"
          parameters:
            - name: "Message"
              expression: "\"Providers to Prov and Prov_DEA witll be loaded with OptumRX Claim Run Cycle: \" : Run_Cycle.ProvRunCycle"
            - name: "LogType"
              expression: "\"INFO\""
          triggers: 
            - activity: LogMbrTermDtUsed
        - name: LogMbrTermDtUsed
          type: routine
          skipCheckpoint: true
          routinename: "WRITE.LOG"
          parameters:
            - name: "Message"
              expression: "\"Member lookup queries will pull members with a MBR.TERM_DT_SK >= : \" :  MbrTermDate"
            - name: "LogType"
              expression: "\"INFO\""
          triggers: 
            - activity: LogDeniedRunCycle
        - name: If_1E
          type: switch
          triggers: 
            - activity: OptumIdsDrugClmPriceLoadSeq
              conditionType: Custom
              condition: "Count (LinkCount.ExclusionList, \";\": \"OptumIdsDrugClmPriceLoadSeq\" :\";\") = 0"
            - activity: Any9
              conditionType: Otherwise
        - name: OptumIdsDrugClmPriceLoadSeq
          type: dataflow
          executionType: reset then run
          jobname: OptumIdsDrugClmPriceLoadSeq
          parameters:
            - name: "APT_CONFIG_FILE"
              expression: "$APT_CONFIG_FILE"
            - name: "IDSInstance"
              expression: "$IDSInstance"
            - name: "IDSDSN"
              expression: "$IDSDSN"
            - name: "IDSAcct"
              expression: "$IDSAcct"
            - name: "IDSDB"
              expression: "$IDSDB"
            - name: "IDSPW"
              expression: "$IDSPW"
            - name: "IDSOwner"
              expression: "$IDSOwner"
            - name: "FilePath"
              expression: "$FilePath"
            - name: "SrcSysCd"
              expression: "Initial.Source"
            - name: "SrcSysCdSK"
              expression: "Variables.SrcSysCdSk"
            - name: "RunID"
              expression: "LoopVariables.RunID"
            - name: "IDSRunCycle"
              expression: "Run_Cycle.RunCycle"
            - name: "IDSDB2ArraySize"
              expression: "$IDSDB2ArraySize"
            - name: "IDSDB2RecordCount"
              expression: "$IDSDB2RecordCount"
            - name: "ExclusionList"
              expression: "LinkCount.ExclusionList"
            - name: "UWSAcct"
              expression: "$UWSAcct"
            - name: "UWSPW"
              expression: "$UWSPW"
            - name: "UWSOwner"
              expression: "$UWSOwner"
            - name: "UWSDB"
              expression: "$UWSDB"
            - name: "Current_Dt"
              expression: "LoopVariables.RunCycleDate"
          triggers: 
            - activity: Any9
        - name: Any9
          type: sequencer
          sequencerType: any
          triggers: 
            - activity: If_1C
        - name: If_2A
          type: switch
          triggers: 
            - activity: OptumDrugLandSeq
              conditionType: Custom
              condition: "Count (LinkCount.ExclusionList, \";\": \"OptumDrugLandSeq\" :\";\") = 0"
            - activity: Or_2A
              conditionType: Otherwise
        - name: LogDeniedRunCycle
          type: routine
          skipCheckpoint: true
          routinename: "WRITE.LOG"
          parameters:
            - name: "Message"
              expression: "\"Denied Reject Claims written to MBR_CLM_DENIED_REJECTED with Run Cycle: \" :  Run_Cycle.MbrDndPdxRunCycle"
            - name: "LogType"
              expression: "\"INFO\""
          triggers: 
            - activity: WriteRunID
        - name: Loop1
          type: sequencer
          sequencerType: all
          triggers: 
            - activity: Exec_CntrlFilePick
        - name: Loop2
          type: sequencer
          sequencerType: all
          triggers: 
            - activity: If_2A
        - name: Loop10
          type: sequencer
          sequencerType: any
          triggers: 
            - activity: Loop11
        - name: Loop11
          type: sequencer
          sequencerType: any
          triggers: 
            - activity: If_5A
        - name: CloseDeniedRunCycle
          type: routine
          routinename: CloseRunCycle
          parameters:
            - name: "DBInstance"
              expression: "$IDSInstance"
            - name: "DBOwner"
              expression: "$IDSOwner"
            - name: "DBDsn"
              expression: "$IDSDSN"
            - name: "DBAcct"
              expression: "$IDSAcct"
            - name: "DBPW"
              expression: "$IDSPW"
            - name: "SrcSystem"
              expression: "Initial.Source"
            - name: "TrgtSystem"
              expression: "'IDS'"
            - name: "SubjCode"
              expression: "'DENIED_CLAIMS'"
            - name: "FreqCode"
              expression: "'DAILY'"
            - name: "RunCycle"
              expression: "Run_Cycle.MbrDndPdxRunCycle"
          triggers: 
            - activity: RptLinkCount
        - name: Loop15
          type: sequencer
          sequencerType: any
          triggers: 
            - activity: CloseRunCycle
        - name: Or_2A
          type: sequencer
          sequencerType: any
          triggers: 
            - activity: If_1B
        - name: Loop14
          type: sequencer
          sequencerType: any
          triggers: 
            - activity: Loop15
        - name: Or_1C
          type: sequencer
          sequencerType: any
          triggers: 
            - activity: Loop10
        - name: If_1C
          type: switch
          triggers: 
            - activity: IdsDrugRvrslSeq
              conditionType: Custom
              condition: "Count (LinkCount.ExclusionList, \";\": \"IdsDrugRvrslSeq\" :\";\") = 0"
            - activity: Or_1C
              conditionType: Otherwise
        - name: IdsDrugRvrslSeq
          type: dataflow
          executionType: reset then run
          jobname: IdsDrugRvrslSeq
          parameters:
            - name: "FilePath"
              expression: "$FilePath"
            - name: "Logging"
              expression: "LoopVariables.Logging"
            - name: "RunCycle"
              expression: "Run_Cycle.RunCycle"
            - name: "RunID"
              expression: "LoopVariables.RunID"
            - name: "ExclusionList"
              expression: "LinkCount.ExclusionList"
            - name: "Source"
              expression: "Initial.Source"
            - name: "IDSInstance"
              expression: "$IDSInstance"
            - name: "IDSDB"
              expression: "$IDSDB"
            - name: "IDSDSN"
              expression: "$IDSDSN"
            - name: "IDSOwner"
              expression: "$IDSOwner"
            - name: "IDSAcct"
              expression: "$IDSAcct"
            - name: "IDSPW"
              expression: "$IDSPW"
          triggers: 
            - activity: Or_1C
        - name: If_1B
          type: switch
          triggers: 
            - activity: BCBSKCClmLoadSeq
              conditionType: Custom
              condition: "Count (LinkCount.ExclusionList, \";\": \"BCBSKCClmLoadSeq\" :\";\") = 0"
            - activity: Or_1B
              conditionType: Otherwise
        - name: Or_1B
          type: sequencer
          sequencerType: any
          triggers: 
            - activity: If_1D
        - name: Exec_CntrlFilePick
          type: shell
          path: ls
          command: "#$FilePath#/landing/OPTUMRX.PBM.DrugClmControl*COMM*`echo #StartLoop.$Counter# | awk -F\"_\" '{print $NF}' | awk -F\".\" '{print $1}' | cut -c 1-8`*.dat* | awk -F\"/\" '{print $NF}'"
          triggers: 
            - activity: If_11A
        - name: OptumControlReconExtr
          type: dataflow
          executionType: reset then run
          jobname: OptumControlReconExtr
          parameters:
            - name: "FilePath"
              expression: "$FilePath"
            - name: "RunID"
              expression: "LoopVariables.RunID"
            - name: "SrcSysCd"
              expression: "Initial.Source"
            - name: "SrcSysCdSK"
              expression: "Variables.SrcSysCdSk"
            - name: "RunDate"
              expression: "LoopVariables.RunCycleDate"
            - name: "InFile"
              expression: "StartLoop.$Counter"
            - name: "ControlInFile"
              expression: "Ereplace(Exec_CntrlFilePick.$CommandOutput,@FM,'' )"
            - name: "MbrTermDate"
              expression: MbrTermDate
          triggers: 
            - activity: Or_11A
        - name: If_11A
          type: switch
          triggers: 
            - activity: Or_11A
              conditionType: Otherwise
            - activity: OptumControlReconExtr
              conditionType: Custom
              condition: "Count (LinkCount.ExclusionList, \";\": \"OptumControlReconExtr\" :\";\") = 0"
        - name: Or_11A
          type: sequencer
          sequencerType: any
          triggers: 
            - activity: Loop2
        - name: MvControlFile
          type: shell
          path: cd
          command: "#$FilePath#/landing; mv -f #RptLinkCount.CntlFileName# processed/#RptLinkCount.CntlFileName#.#LoopVariables.RunID# ; compress -f processed/#RptLinkCount.CntlFileName#.#LoopVariables.RunID#"
          triggers: 
            - activity: EndLoop
        - name: WriteRunID
          type: routine
          routinename: WriteRunID
          parameters:
            - name: "EntryName"
              expression: "Initial.JobName:\"RunID\""
          triggers: 
            - activity: LoopVariables
        - name: OptumDrugClmSrcDtlExtr
          type: dataflow
          executionType: reset then run
          jobname: OptumDrugClmSrcDtlExtr
          parameters:
            - name: "IDSDB"
              expression: "$IDSDB"
            - name: "IDSOwner"
              expression: "$IDSOwner"
            - name: "IDSPW"
              expression: "$IDSPW"
            - name: "IDSAcct"
              expression: "$IDSAcct"
            - name: "FilePath"
              expression: "$FilePath"
            - name: "SrcSysCd"
              expression: "Initial.Source"
            - name: "RunID"
              expression: "LoopVariables.RunID"
            - name: "IDSRunCycle"
              expression: "Run_Cycle.RunCycle"
            - name: "SrcSysCdSK"
              expression: "Variables.SrcSysCdSk"
            - name: "LOB"
              expression: LOB
          triggers: 
            - activity: Or_5D
            - activity: If_5E
        - name: Or_5D
          type: sequencer
          sequencerType: any
          triggers: 
            - activity: If_6A
        - name: If_5D
          type: switch
          triggers: 
            - activity: OptumDrugClmSrcDtlExtr
              conditionType: Custom
              condition: "Count (LinkCount.ExclusionList, \";\": \"OptumDrugClmSrcDtlExtr\" :\";\") = 0"
            - activity: Or_5D
              conditionType: Otherwise
        - name: LoadClmSrcDtl
          type: routine
          routinename: "DB.LOAD"
          parameters:
            - name: "Arg1"
              expression: "\"CLM_SRC_DTL\""
            - name: "Arg2"
              expression: "\"UPDATE\""
            - name: "Arg3"
              expression: "$IDSInstance"
            - name: "Arg4"
              expression: "$IDSDB"
            - name: "Arg5"
              expression: "$IDSAcct"
            - name: "Arg6"
              expression: "$IDSPW"
            - name: "Arg7"
              expression: "$FilePath"
            - name: "Arg8"
              expression: "\"CLM_SRC_DTL_\":Initial.Source:\".dat\":\".\":LoopVariables.RunID"
            - name: "Arg9"
              expression: "\"N\""
            - name: "Arg10"
              expression: "$IDSOwner"
          triggers: 
            - activity: MvClmLandExtr
        - name: If_5E
          type: switch
          triggers: 
            - activity: LoadClmSrcDtl
              conditionType: Custom
              condition: "Count (LinkCount.ExclusionList,\";\": \"LoadClmSrcDtl\" :\";\") = 0"
        - name: MvClmLandExtr
          type: shell
          path: cd
          command: "#$FilePath#/verified; mv -f PDX_CLM_STD_INPT_Land_#Initial.Source#.dat.#LoopVariables.RunID#   processed/;    compress -f   processed/PDX_CLM_STD_INPT_Land_#Initial.Source#.dat.#LoopVariables.RunID#"


