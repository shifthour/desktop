model:
  config:
    origin: datastage
  jobs:
    - name: IdsDrugRvrslSeq
      origin: datastage
      type: controlflow
      folder: "claim/SeqDrug"
      checkpoint: true
      description: "IDS Drug Claim Reversal Process"
      parameters:
        - name: FilePath
          type: string
          description: "File Path"
          defaultType: project
        - name: Logging
          type: string
          description: "Error Log Indicator"
          defaultType: expr
          defaultValue: Y
        - name: RunCycle
          type: integer
          description: "Run Cycle"
        - name: RunID
          type: string
          description: RunID
        - name: ExclusionList
          type: string
          description: "Exclusion List"
          defaultType: expr
          defaultValue: "\"\""
        - name: Source
          type: string
          description: "Source System"
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
      activities:
        - name: IdsDrugRvrslBuild
          type: dataflow
          executionType: reset then run
          jobname: IdsDrugRvrslBuild
          parameters:
            - name: "FilePath"
              expression: "$FilePath"
            - name: "Source"
              expression: Source
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
            - activity: IdsDrugRvrslBuild
              conditionType: Custom
              condition: "Count (ExclusionList, \";\": \"IdsDrugRvrslBuild\" :\";\") = 0"
            - activity: Or_1A
              conditionType: Otherwise
        - name: Or_1A
          type: sequencer
          sequencerType: any
          triggers: 
            - activity: If_1B
        - name: Or_1C
          type: sequencer
          sequencerType: any
          triggers: 
            - activity: If_3C
            - activity: If_2C
            - activity: If_4C
            - activity: If_1D
            - activity: If_5C
            - activity: If_6C
        - name: If_1C
          type: switch
          triggers: 
            - activity: IdsDrugRvrslExtr
              conditionType: Custom
              condition: "Count (ExclusionList, \";\": \"IdsDrugRvrslExtr\" :\";\") = 0"
            - activity: Or_1C
              conditionType: Otherwise
        - name: IdsDrugRvrslExtr
          type: dataflow
          executionType: reset then run
          jobname: IdsDrugRvrslExtr
          parameters:
            - name: "FilePath"
              expression: "$FilePath"
            - name: "RunID"
              expression: RunID
            - name: "Source"
              expression: Source
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
            - activity: Or_1C
        - name: If_4C
          type: switch
          triggers: 
            - activity: LoadDrugClmReversal
              conditionType: Custom
              condition: "Count (ExclusionList, \";\": \"LoadDrugClmReversal\" :\";\") = 0"
        - name: LoadDrugClmReversal
          type: routine
          routinename: "DB.LOAD"
          parameters:
            - name: "Arg1"
              expression: "\"DRUG_CLM\""
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
              expression: "\"DRUG_CLM.\":Source:\".dat\""
            - name: "Arg9"
              expression: "\"N\""
            - name: "Arg10"
              expression: "$IDSOwner"
        - name: LoadClmRemitHistReversal
          type: routine
          routinename: "DB.LOAD"
          parameters:
            - name: "Arg1"
              expression: "\"CLM_REMIT_HIST\""
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
              expression: "\"CLM_REMIT_HIST.\":Source:\".dat\""
            - name: "Arg9"
              expression: "\"N\""
            - name: "Arg10"
              expression: "$IDSOwner"
        - name: If_3C
          type: switch
          triggers: 
            - activity: LoadClmRemitHistReversal
              conditionType: Custom
              condition: "Count (ExclusionList, \";\": \"LoadClmRemitHistReversal\" :\";\") = 0"
        - name: If_2C
          type: switch
          triggers: 
            - activity: LoadClmLnReversal
              conditionType: Custom
              condition: "Count (ExclusionList, \";\": \"LoadClmLnReversal\" :\";\") = 0"
        - name: LoadClmLnReversal
          type: routine
          routinename: "DB.LOAD"
          parameters:
            - name: "Arg1"
              expression: "\"CLM_LN\""
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
              expression: "\"CLM_LN.\":Source:\".dat\""
            - name: "Arg9"
              expression: "\"N\""
            - name: "Arg10"
              expression: "$IDSOwner"
        - name: ConcatClmReversal
          type: shell
          path: cat
          command: "#$FilePath#/load/CLM_REVERSAL_DRUG*  > #$FilePath#/load/CLM.#Source#.dat"
          triggers: 
            - activity: Or_1D
              conditionType: OK
        - name: LoadClm
          type: routine
          routinename: "DB.LOAD"
          parameters:
            - name: "Arg1"
              expression: "\"CLM\""
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
              expression: "\"CLM.\":Source:\".dat\""
            - name: "Arg9"
              expression: "\"N\""
            - name: "Arg10"
              expression: "$IDSOwner"
          triggers: 
            - activity: Or_1E
              conditionType: OK
        - name: If_1E
          type: switch
          triggers: 
            - activity: LoadClm
              conditionType: Custom
              condition: "Count (ExclusionList, \";\": \"LoadClm\" :\";\") = 0"
            - activity: Or_1E
              conditionType: Otherwise
        - name: If_1D
          type: switch
          triggers: 
            - activity: ConcatClmReversal
              conditionType: Custom
              condition: "Count (ExclusionList, \";\": \"ConcatClmReversal\" :\";\") = 0"
            - activity: Or_1D
              conditionType: Otherwise
        - name: Or_1D
          type: sequencer
          sequencerType: any
          triggers: 
            - activity: If_1E
        - name: RmClmReversal
          type: shell
          path: rm
          command: " -f #$FilePath#/load/CLM_REVERSAL_DRUG*"
        - name: Or_1E
          type: sequencer
          sequencerType: any
          triggers: 
            - activity: If_1F
        - name: If_1F
          type: switch
          triggers: 
            - activity: RmClmReversal
              conditionType: Custom
              condition: "Count (ExclusionList, \";\": \"RmClmReversal\" :\";\") = 0"
        - name: LoadDrugClmPriceReversal
          type: routine
          routinename: "DB.LOAD"
          parameters:
            - name: "Arg1"
              expression: "\"DRUG_CLM_PRICE\""
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
              expression: "\"DRUG_CLM_PRICE.\":Source:\".dat\""
            - name: "Arg9"
              expression: "\"N\""
            - name: "Arg10"
              expression: "$IDSOwner"
        - name: If_6C
          type: switch
          triggers: 
            - activity: LoadDrugClmPriceReversal
              conditionType: Custom
              condition: "Count (ExclusionList, \";\": \"LoadDrugClmPriceReversal\" :\";\") = 0"
        - name: If_5C
          type: switch
          triggers: 
            - activity: LoadClmCobReversal
              conditionType: Custom
              condition: "Count (ExclusionList, \";\": \"LoadClmCobReversal\" :\";\") = 0"
        - name: LoadClmCobReversal
          type: routine
          routinename: "DB.LOAD"
          parameters:
            - name: "Arg1"
              expression: "\"CLM_COB\""
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
              expression: "\"CLM_COB.\":Source:\".dat\""
            - name: "Arg9"
              expression: "\"N\""
            - name: "Arg10"
              expression: "$IDSOwner"
        - name: Or_1B
          type: sequencer
          sequencerType: any
          triggers: 
            - activity: If_1C
        - name: If_1B
          type: switch
          triggers: 
            - activity: LoadW_CLM_RVRSL
              conditionType: Custom
              condition: "Count (ExclusionList, \";\": \"LoadW_CLM_RVRSL\" :\";\") = 0"
            - activity: Or_1B
              conditionType: Otherwise
        - name: LoadW_CLM_RVRSL
          type: routine
          routinename: "DB.LOAD"
          parameters:
            - name: "Arg1"
              expression: "\"W_CLM_RVRSL\""
            - name: "Arg2"
              expression: "\"REPLACE\""
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
              expression: "\"W_CLM_RVRSL.\":Source:\".dat\""
            - name: "Arg9"
              expression: "\"Y\""
            - name: "Arg10"
              expression: "$IDSOwner"
          triggers: 
            - activity: Or_1B
              conditionType: OK


