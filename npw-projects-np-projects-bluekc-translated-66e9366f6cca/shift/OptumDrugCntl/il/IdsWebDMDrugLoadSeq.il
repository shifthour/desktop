model:
  config:
    origin: datastage
  jobs:
    - name: IdsWebDMDrugLoadSeq
      origin: datastage
      type: controlflow
      folder: "claim/SeqDrug"
      checkpoint: true
      description: "Pull Argus data from IDS for Web Data Mart CLM, INIT_CLM and CLM_LN"
      parameters:
        - name: FilePath
          type: string
          description: "File Path"
          defaultType: project
        - name: ExclusionList
          type: string
          description: "Exclusion List"
          defaultType: expr
          defaultValue: "\"\""
        - name: RunID
          type: string
          description: RunID
        - name: RunCycle
          type: string
          description: "Run Cycle"
        - name: IDSInstance
          type: string
          description: "IDS DB Instance"
          defaultType: project
        - name: IDSDB
          type: string
          description: "IDS Database"
          defaultType: project
        - name: IDSDSN
          type: string
          description: "IDS DSN"
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
        - name: ClmMartAcct
          type: string
          description: "Claim Mart Account"
        - name: ClmMartPW
          type: encrypted
          description: "Claim Mart Password"
      activities:
        - name: Any1
          type: sequencer
          sequencerType: any
          triggers: 
            - activity: Any2
        - name: Any2
          type: sequencer
          sequencerType: any
          triggers: 
            - activity: All
        - name: EndLoop
          type: endLoop
          startID: StartLoop
          triggers: 
            - activity: Any1
        - name: If_2B
          type: switch
          triggers: 
            - activity: Or_2B
              conditionType: Otherwise
            - activity: IdsDMDrugClmAccumImpctExtr
              conditionType: Custom
              condition: "Count (ExclusionList, \";\": \"IdsDMDrugClmAccumImpctExtr\" :\";\") = 0"
        - name: IdsDMDrugClmAccumImpctExtr
          type: dataflow
          executionType: reset then run
          jobname: IdsDMDrugClmAccumImpctExtr
          parameters:
            - name: "IDSAcct"
              expression: "$IDSAcct"
            - name: "IDSDB"
              expression: "$IDSDB"
            - name: "FilePath"
              expression: "$FilePath"
            - name: "IDSPW"
              expression: "$IDSPW"
            - name: "IDSOwner"
              expression: "$IDSOwner"
            - name: "RunCycle"
              expression: RunCycle
          triggers: 
            - activity: Or_2B
        - name: Or_2B
          type: sequencer
          sequencerType: any
          triggers: 
            - activity: If_2C
        - name: If_2C
          type: switch
          triggers: 
            - activity: Or_2C
              conditionType: Otherwise
            - activity: IdsDMDrugClmAccumImpctLoad
              conditionType: Custom
              condition: "Count (ExclusionList, \";\": \"IdsDMDrugClmAccumImpctLoad\" :\";\") = 0"
        - name: IdsDMDrugClmAccumImpctLoad
          type: dataflow
          executionType: reset then run
          jobname: IdsDMDrugClmAccumImpctLoad
          parameters:
            - name: "FilePath"
              expression: "$FilePath"
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
            - name: "ClmMartDSN"
              expression: "$ClmMartServer"
            - name: "ClmMartArraySize"
              expression: $ClmMartArraySize
            - name: "ClmMartRecordCount"
              expression: $ClmMartRecordCount
          triggers: 
            - activity: Or_2C
        - name: Or_2C
          type: sequencer
          sequencerType: any
          triggers: 
            - activity: If_3A
        - name: WebDmLoadRunCycUpd
          type: dataflow
          executionType: reset then run
          jobname: WebDmLoadRunCycUpd
          parameters:
            - name: "FilePath"
              expression: "$FilePath"
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
            - activity: Or_3A
        - name: If_3A
          type: switch
          triggers: 
            - activity: WebDmLoadRunCycUpd
              conditionType: Custom
              condition: "Count (ExclusionList, \";\": \"WebDmLoadRunCycUpd\" :\";\") = 0"
            - activity: Or_3A
              conditionType: Otherwise
        - name: Or_3A
          type: sequencer
          sequencerType: any
          triggers: 
            - activity: If_3B
        - name: If_1B
          type: switch
          triggers: 
            - activity: StartLoop
              conditionType: Custom
              condition: "LinkCount.LinkCount > 0"
            - activity: Any1
              conditionType: Custom
              condition: "LinkCount.LinkCount = 0"
        - name: LinkCount
          type: userVars
          parameters:
              - name: LinkCount
                type: string
                expression: "JobLinkCount('IdsClmMartClmExtr', 'Trans2', 'Count')"
          triggers: 
            - activity: If_1B
        - name: IdsClmMartClmInitUpd
          type: dataflow
          executionType: run
          jobname: IdsClmMartClmInitUpd
          parameters:
            - name: "FilePath"
              expression: "$FilePath"
            - name: "CommitCnt"
              expression: "StartLoop.$Counter"
            - name: "CommitCnt2"
              expression: "StartLoop.$Counter + 1"
            - name: "CommitCnt3"
              expression: "StartLoop.$Counter + 2"
            - name: "CommitCnt4"
              expression: "StartLoop.$Counter + 3"
            - name: "ClmMartServer"
              expression: "$ClmMartServer"
            - name: "RunCycle"
              expression: RunCycle
            - name: "ClmMartDB"
              expression: "$ClmMartDB"
            - name: "ClmMartOwner"
              expression: "$ClmMartOwner"
            - name: "ClmMartAcct"
              expression: "$ClmMartAcct"
            - name: "ClmMartPW"
              expression: "$ClmMartPW"
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
            - activity: EndLoop
        - name: StartLoop
          type: startLoop
          loopType: numeric
          startNumber: "1"
          increment: "4"
          endNumber: "#LinkCount.LinkCount#"
          triggers: 
            - activity: IdsClmMartClmInitUpd
        - name: IdsClmMartClmExtr
          type: dataflow
          executionType: reset then run
          jobname: IdsClmMartClmExtr
          parameters:
            - name: "FilePath"
              expression: "$FilePath"
            - name: "CommitPoint"
              expression: "5000"
            - name: "RunCycle"
              expression: RunCycle
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
            - activity: Or_1A
        - name: If_1A
          type: switch
          triggers: 
            - activity: IdsClmMartClmExtr
              conditionType: Custom
              condition: "Count (ExclusionList, \";\": \"IdsClmMartClmExtr\" :\";\") = 0"
            - activity: Or_1A
              conditionType: Otherwise
        - name: IdsClmMartClmLnExtr
          type: dataflow
          executionType: reset then run
          jobname: IdsClmMartClmLnExtr
          parameters:
            - name: "FilePath"
              expression: "$FilePath"
            - name: "RunCycle"
              expression: RunCycle
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
            - activity: Or_2A
        - name: If_2A
          type: switch
          triggers: 
            - activity: IdsClmMartClmLnExtr
              conditionType: Custom
              condition: "Count (ExclusionList, \";\": \"IdsClmMartClmLnExtr\" :\";\") = 0"
            - activity: Or_2A
              conditionType: Otherwise
        - name: If_3B
          type: switch
          triggers: 
            - activity: TruncateW_WEBDM_ETL_DRVR
              conditionType: Custom
              condition: "Count (ExclusionList, \";\": \"TruncateW_WEBDM_ETL_DRVR\" :\";\") = 0"
        - name: TruncateW_WEBDM_ETL_DRVR
          type: routine
          routinename: "DB.TRUNCATE"
          parameters:
            - name: "DBInstance"
              expression: "$IDSInstance"
            - name: "DBName"
              expression: "$IDSDB"
            - name: "DBOwner"
              expression: "$IDSOwner"
            - name: "DBAcct"
              expression: "$IDSAcct"
            - name: "DBPW"
              expression: "$IDSPW"
            - name: "FilePath"
              expression: "$FilePath"
            - name: "DBTable"
              expression: "\"W_WEBDM_ETL_DRVR\""
        - name: All
          type: sequencer
          sequencerType: all
          triggers: 
            - activity: If_2B
        - name: Or_2A
          type: sequencer
          sequencerType: any
          triggers: 
            - activity: All
        - name: Or_1A
          type: sequencer
          sequencerType: any
          triggers: 
            - activity: LinkCount


