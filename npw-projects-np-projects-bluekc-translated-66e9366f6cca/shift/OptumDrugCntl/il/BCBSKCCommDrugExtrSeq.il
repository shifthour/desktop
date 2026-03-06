model:
  config:
    origin: datastage
  jobs:
    - name: BCBSKCCommDrugExtrSeq
      origin: datastage
      type: controlflow
      folder: "claim/SeqBCBSKCCommon"
      checkpoint: true
      parameters:
        - name: FilePath
          type: string
          description: "File Path"
          defaultType: project
        - name: IDSDSN
          type: string
          description: "IDS ODBC DSN"
          defaultType: project
        - name: IDSInstance
          type: string
          description: "IDS Instance"
          defaultType: project
        - name: IDSDB
          type: string
          description: "IDS Database"
          defaultType: project
        - name: IDSOwner
          type: string
          description: "IDS Table Owner"
          defaultType: project
        - name: IDSAcct
          type: string
          description: "IDS Account"
        - name: IDSPW
          type: encrypted
          description: "IDS Password"
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
        - name: CurrentDate
          type: string
          description: CurrentDate
        - name: RunCycle
          type: string
          description: RunCycle
        - name: RunID
          type: string
          description: RunID
        - name: ExclusionList
          type: string
          description: ExclusionList
          defaultType: expr
          defaultValue: "\"\""
        - name: SrcSysCd
          type: string
          description: SourceSys
          defaultType: expr
          defaultValue: SAVRX
        - name: SrcSysCdSk
          type: string
          description: SrcSysCdSk
        - name: Logging
          type: string
          description: Logging
        - name: ProvRunCycle
          type: integer
          description: ProvRunCycle
      activities:
        - name: BCBSKCCommClmExtr
          type: dataflow
          executionType: reset then run
          jobname: BCBSKCCommClmExtr
          parameters:
            - name: "CurrentDate"
              expression: CurrentDate
            - name: "SrcSysCd"
              expression: SrcSysCd
            - name: "SrcSysCdSk"
              expression: SrcSysCdSk
            - name: "FilePath"
              expression: "$FilePath"
            - name: "RunCycle"
              expression: RunCycle
            - name: "RunID"
              expression: RunID
            - name: "IDSAcct"
              expression: "$IDSAcct"
            - name: "IDSPW"
              expression: "$IDSPW"
            - name: "IDSInstance"
              expression: "$IDSInstance"
            - name: "IDSDB"
              expression: "$IDSDB"
            - name: "IDSOwner"
              expression: "$IDSOwner"
          triggers: 
            - activity: Or_2A
        - name: BCBSKCCommClmProvExtr
          type: dataflow
          executionType: reset then run
          jobname: BCBSKCCommClmProvExtr
          parameters:
            - name: "SrcSysCd"
              expression: SrcSysCd
            - name: "SrcSysCdSk"
              expression: SrcSysCdSk
            - name: "CurrentDate"
              expression: CurrentDate
            - name: "FilePath"
              expression: "$FilePath"
            - name: "RunID"
              expression: RunID
            - name: "RunCycle"
              expression: RunCycle
            - name: "IDSAcct"
              expression: "$IDSAcct"
            - name: "IDSPW"
              expression: "$IDSPW"
            - name: "IDSDB"
              expression: "$IDSDB"
            - name: "IDSInstance"
              expression: "$IDSInstance"
            - name: "IDSOwner"
              expression: "$IDSOwner"
          triggers: 
            - activity: Or_4A
        - name: BCBSKCCommClmLnExtr
          type: dataflow
          executionType: reset then run
          jobname: BCBSKCCommClmLnExtr
          parameters:
            - name: "FilePath"
              expression: "$FilePath"
            - name: "CurrentDate"
              expression: CurrentDate
            - name: "SrcSysCd"
              expression: SrcSysCd
            - name: "SrcSysCdSk"
              expression: SrcSysCdSk
            - name: "RunID"
              expression: RunID
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
          triggers: 
            - activity: Or_3A
        - name: BCBSKCCommDrugClmExtrLoadSeq
          type: dataflow
          executionType: reset then run
          jobname: BCBSKCCommDrugClmExtrLoadSeq
          parameters:
            - name: "CurrentDate"
              expression: CurrentDate
            - name: "FilePath"
              expression: "$FilePath"
            - name: "Logging"
              expression: Logging
            - name: "RunCycle"
              expression: RunCycle
            - name: "RunID"
              expression: RunID
            - name: "ExclusionList"
              expression: ExclusionList
            - name: "SrcSysCd"
              expression: SrcSysCd
            - name: "SrcSysCdSK"
              expression: SrcSysCdSk
            - name: "IDSAcct"
              expression: "$IDSAcct"
            - name: "IDSPW"
              expression: "$IDSPW"
            - name: "IDSInstance"
              expression: "$IDSInstance"
            - name: "IDSOwner"
              expression: "$IDSOwner"
            - name: "IDSDB"
              expression: "$IDSDB"
            - name: "IDSDSN"
              expression: "$IDSDSN"
            - name: "CactusAcct"
              expression: "$CactusAcct"
            - name: "CactusPW"
              expression: "$CactusPW"
            - name: "CactusDB"
              expression: "$CactusDB"
            - name: "CactusServer"
              expression: "$CactusServer"
            - name: "CactusOwner"
              expression: "$CactusOwner"
            - name: "ProvRunCycle"
              expression: ProvRunCycle
          triggers: 
            - activity: Or_5B
        - name: Any9
          type: sequencer
          sequencerType: any
          triggers: 
            - activity: Any10
        - name: Any10
          type: sequencer
          sequencerType: any
          triggers: 
            - activity: If_1A
        - name: Or_1B
          type: sequencer
          sequencerType: any
        - name: BCBSKCCommClmRemitHistExtr
          type: dataflow
          executionType: reset then run
          jobname: BCBSKCCommClmRemitHistExtr
          parameters:
            - name: "FilePath"
              expression: "$FilePath"
            - name: "CurrentDate"
              expression: CurrentDate
            - name: "RunCycle"
              expression: RunCycle
            - name: "RunID"
              expression: RunID
            - name: "SrcSysCd"
              expression: SrcSysCd
            - name: "SrcSysCdSk"
              expression: SrcSysCdSk
          triggers: 
            - activity: Or_1B
        - name: Any8
          type: sequencer
          sequencerType: any
          triggers: 
            - activity: If_4A
        - name: If_2A
          type: switch
          triggers: 
            - activity: BCBSKCCommClmExtr
              conditionType: Custom
              condition: "Count (ExclusionList, \";\": \"BCBSKCCommClmExtr\" :\";\") = 0"
            - activity: Or_2A
              conditionType: Otherwise
        - name: If_3A
          type: switch
          triggers: 
            - activity: BCBSKCCommClmLnExtr
              conditionType: Custom
              condition: "Count (ExclusionList, \";\": \"BCBSKCCommClmLnExtr\" :\";\") = 0"
            - activity: Or_3A
              conditionType: Otherwise
        - name: If_4A
          type: switch
          triggers: 
            - activity: BCBSKCCommClmProvExtr
              conditionType: Custom
              condition: "Count (ExclusionList, \";\": \"BCBSKCCommClmProvExtr\" :\";\") = 0"
            - activity: Or_4A
              conditionType: Otherwise
        - name: Any7
          type: sequencer
          sequencerType: any
          triggers: 
            - activity: Any8
        - name: Or_4A
          type: sequencer
          sequencerType: any
          triggers: 
            - activity: If_3A
        - name: Or_3A
          type: sequencer
          sequencerType: any
          triggers: 
            - activity: Any9
        - name: Or_2A
          type: sequencer
          sequencerType: any
          triggers: 
            - activity: If_5A
        - name: If_5A
          type: switch
          triggers: 
            - activity: BCBSKCCommDrugClmExtrLoadSeq
              conditionType: Custom
              condition: "Count (ExclusionList, \";\": \"BCBSKCCommDrugClmExtrLoadSeq\" :\";\") = 0"
            - activity: Or_5B
              conditionType: Otherwise
        - name: Or_5B
          type: sequencer
          sequencerType: any
          triggers: 
            - activity: Any7
        - name: If_1A
          type: switch
          triggers: 
            - activity: BCBSKCCommClmRemitHistExtr
              conditionType: Custom
              condition: "Count (ExclusionList, \";\": \"BCBSKCCommClmRemitHistExtr\" :\";\")  = 0 and SrcSysCd = 'OPTUMRX'"
            - activity: Or_1B
              conditionType: Otherwise


