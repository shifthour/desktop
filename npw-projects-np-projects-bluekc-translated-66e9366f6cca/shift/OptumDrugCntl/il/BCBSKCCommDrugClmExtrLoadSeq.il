model:
  config:
    origin: datastage
  jobs:
    - name: BCBSKCCommDrugClmExtrLoadSeq
      origin: datastage
      type: controlflow
      folder: "claim/SeqBCBSKCCommon"
      checkpoint: true
      parameters:
        - name: CurrentDate
          type: string
          description: CurrentDate
          defaultType: expr
          defaultValue: "2008-10-01"
        - name: FilePath
          type: string
          description: "File Path"
          defaultType: project
        - name: Logging
          type: string
          description: "Error Log Indicator"
          defaultType: expr
          defaultValue: N
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
        - name: SrcSysCd
          type: string
          description: "source system"
        - name: SrcSysCdSK
          type: integer
          description: SrcSysCdSK
        - name: IDSAcct
          type: string
          description: "IDS DB Account"
        - name: IDSPW
          type: encrypted
          description: "IDS DB Password"
        - name: IDSInstance
          type: string
          description: "IDS DB Instance"
          defaultType: project
        - name: IDSOwner
          type: string
          description: "IDS DB Owner"
          defaultType: project
        - name: IDSDB
          type: string
          description: "IDS Database"
          defaultType: project
        - name: IDSDSN
          type: string
          description: "IDS DSN"
          defaultType: project
        - name: CactusAcct
          type: string
          description: "Cactus Account"
        - name: CactusPW
          type: encrypted
          description: "Cactus Password"
        - name: CactusDB
          type: string
          description: "Cactus Database"
          defaultType: project
        - name: CactusServer
          type: string
          description: "Cactus Server"
          defaultType: project
        - name: CactusOwner
          type: string
          description: "Cactus Table Owner"
          defaultType: project
        - name: ProvRunCycle
          type: integer
          description: ProvRunCycle
      activities:
        - name: Sequencer_209
          type: sequencer
          sequencerType: all
          triggers: 
            - activity: If_5A
            - activity: If_7E
            - activity: If_7A
            - activity: If_6A
            - activity: If_2A
        - name: BCBSKCCommDrugClmExtr
          type: dataflow
          executionType: reset then run
          jobname: BCBSKCCommDrugClmExtr
          parameters:
            - name: "CurrentDate"
              expression: CurrentDate
            - name: "SrcSysCd"
              expression: SrcSysCd
            - name: "SrcSysCdSk"
              expression: SrcSysCdSK
            - name: "FilePath"
              expression: "$FilePath"
            - name: "RunCycle"
              expression: RunCycle
            - name: "ProvRunCycle"
              expression: ProvRunCycle
            - name: "RunID"
              expression: RunID
            - name: "IDSAcct"
              expression: "$IDSAcct"
            - name: "IDSPW"
              expression: "$IDSPW"
            - name: "IDSDSN"
              expression: "$IDSDSN"
            - name: "IDSDB"
              expression: "$IDSDB"
            - name: "IDSOwner"
              expression: "$IDSOwner"
            - name: "CactusAcct"
              expression: "$CactusAcct"
            - name: "CactusPW"
              expression: "$CactusPW"
            - name: "CactusOwner"
              expression: "$CactusOwner"
            - name: "CactusServer"
              expression: "$CactusServer"
            - name: "CactusDB"
              expression: "$CactusDB"
          triggers: 
            - activity: Or_5B1
        - name: ConcatProvLoc
          type: shell
          path: cd
          command: "#$FilePath#/scripts; prepare_files.ksh #$FilePath#/key *.ProvLoc*  IdsProvLocExtr.ProvLoc '-k9,9 -k5,5r'  '-k9,9'"
          triggers: 
            - activity: Or_7A
        - name: RmProvLoc
          type: shell
          path: rm
          command: " -f \"#$FilePath#/key/IdsProvLocExtr.ProvLoc.uniq\" "
          triggers: 
            - activity: Or_7C
        - name: IdsProvLocFkey
          type: dataflow
          executionType: reset then run
          jobname: IdsProvLocFkey
          parameters:
            - name: "InFile"
              expression: "\"IdsProvLocExtr.ProvLoc.uniq\""
            - name: "Logging"
              expression: Logging
            - name: "FilePath"
              expression: "$FilePath"
          triggers: 
            - activity: Or_7B
        - name: Or_7C
          type: sequencer
          sequencerType: any
          triggers: 
            - activity: If_7D
        - name: If_7C
          type: switch
          triggers: 
            - activity: RmProvLoc
              conditionType: Custom
              condition: "Count (ExclusionList, \";\": \"RmProvLoc\" :\";\") = 0"
            - activity: Or_7C
              conditionType: Otherwise
        - name: If_7D
          type: switch
          triggers: 
            - activity: LoadProvLoc
              conditionType: Custom
              condition: "Count (ExclusionList, \";\": \"LoadProvLoc\" :\";\") = 0"
        - name: Or_7B
          type: sequencer
          sequencerType: any
          triggers: 
            - activity: If_7C
        - name: If_7B
          type: switch
          triggers: 
            - activity: IdsProvLocFkey
              conditionType: Custom
              condition: "Count (ExclusionList, \";\": \"IdsProvLocFkey\" :\";\") = 0"
            - activity: Or_7B
              conditionType: Otherwise
        - name: Or_7A
          type: sequencer
          sequencerType: any
          triggers: 
            - activity: If_7B
        - name: If_7A
          type: switch
          triggers: 
            - activity: ConcatProvLoc
              conditionType: Custom
              condition: "Count (ExclusionList, \";\": \"ConcatProvLoc\" :\";\") = 0"
            - activity: Or_7A
              conditionType: Otherwise
        - name: LoadProvLoc
          type: routine
          routinename: "DB.LOAD"
          parameters:
            - name: "Arg1"
              expression: "\"PROV_LOC\""
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
              expression: "\"PROV_LOC.dat\""
            - name: "Arg9"
              expression: "\"Y\""
            - name: "Arg10"
              expression: "$IDSOwner"
        - name: If_7E
          type: switch
          triggers: 
            - activity: ConcatProvDrug
              conditionType: Custom
              condition: "Count (ExclusionList, \";\": \"ConcatProvDrug\" :\";\") = 0"
            - activity: Or_7E
              conditionType: Otherwise
        - name: LoadProv
          type: routine
          routinename: "DB.LOAD"
          parameters:
            - name: "Arg1"
              expression: "\"PROV\""
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
              expression: "\"PROV.dat\""
            - name: "Arg9"
              expression: "\"N\""
            - name: "Arg10"
              expression: "$IDSOwner"
        - name: ConcatProvDrug
          type: shell
          path: cd
          command: "#$FilePath#/scripts; prepare_files.ksh #$FilePath#/key *.DrugProv*  IdsProvExtr.DrugProv '-k9,9 -k5,5r'  '-k9,9'"
          triggers: 
            - activity: Or_7E
        - name: RmProv
          type: shell
          path: rm
          command: " -f \"#$FilePath#/key/IdsProvExtr.DrugProv.uniq\" "
          triggers: 
            - activity: Or_7G
        - name: IdsProvFkey
          type: dataflow
          executionType: reset then run
          jobname: IdsProvFkey
          parameters:
            - name: "FilePath"
              expression: "$FilePath"
            - name: "InFile"
              expression: "\"IdsProvExtr.DrugProv.uniq\""
            - name: "Logging"
              expression: "'X'"
          triggers: 
            - activity: Or_7F
        - name: IdsProvDEAFkey
          type: dataflow
          executionType: reset then run
          jobname: IdsProvDEAFkey
          parameters:
            - name: "FilePath"
              expression: "$FilePath"
            - name: "Logging"
              expression: "'X'"
          triggers: 
            - activity: Or_2B
        - name: RmProvDea
          type: shell
          path: rm
          command: " -f \"#$FilePath#/key/IdsProvDEAExtr.ProvDEA.uniq\" "
          triggers: 
            - activity: Or_2C
        - name: ConcatDEADrug
          type: shell
          path: cd
          command: "#$FilePath#/scripts; prepare_files.ksh #$FilePath#/key *.DrugProvDea*  IdsProvDEAExtr.ProvDEA '-k9,9 -k5,5r'  '-k9,9'"
          triggers: 
            - activity: Or_2A
        - name: LoadDEA
          type: routine
          routinename: "DB.LOAD"
          parameters:
            - name: "Arg1"
              expression: "\"PROV_DEA\""
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
              expression: "\"PROV_DEA.dat\""
            - name: "Arg9"
              expression: "\"N\""
            - name: "Arg10"
              expression: "$IDSOwner"
        - name: If_2A
          type: switch
          triggers: 
            - activity: ConcatDEADrug
              conditionType: Custom
              condition: "Count (ExclusionList, \";\": \"ConcatDEADrug\" :\";\") = 0"
            - activity: Or_2A
              conditionType: Otherwise
        - name: Or_2A
          type: sequencer
          sequencerType: any
          triggers: 
            - activity: If_2B
        - name: If_2B
          type: switch
          triggers: 
            - activity: IdsProvDEAFkey
              conditionType: Custom
              condition: "Count (ExclusionList, \";\": \"IdsProvDEAFkey\" :\";\") = 0"
            - activity: Or_2B
              conditionType: Otherwise
        - name: Or_2B
          type: sequencer
          sequencerType: any
          triggers: 
            - activity: If_2C
        - name: If_2D
          type: switch
          triggers: 
            - activity: LoadDEA
              conditionType: Custom
              condition: "Count (ExclusionList, \";\": \"LoadDEA\" :\";\") = 0"
        - name: If_2C
          type: switch
          triggers: 
            - activity: RmProvDea
              conditionType: Custom
              condition: "Count (ExclusionList, \";\": \"RmProvDea\" :\";\") = 0"
            - activity: Or_2C
              conditionType: Otherwise
        - name: Or_2C
          type: sequencer
          sequencerType: any
          triggers: 
            - activity: If_2D
        - name: Or_6C
          type: sequencer
          sequencerType: any
          triggers: 
            - activity: If_6D
        - name: If_6C
          type: switch
          triggers: 
            - activity: RmNDC
              conditionType: Custom
              condition: "Count (ExclusionList, \";\": \"RmNDC\" :\";\") = 0"
            - activity: Or_6C
              conditionType: Otherwise
        - name: If_6D
          type: switch
          triggers: 
            - activity: LoadNDC
              conditionType: Custom
              condition: "Count (ExclusionList, \";\": \"LoadNDC\" :\";\") = 0"
        - name: Or_6B
          type: sequencer
          sequencerType: any
          triggers: 
            - activity: If_6C
        - name: If_6B
          type: switch
          triggers: 
            - activity: IdsNdcFkey
              conditionType: Custom
              condition: "Count (ExclusionList, \";\": \"IdsNdcFkey\" :\";\") = 0"
            - activity: Or_6B
              conditionType: Otherwise
        - name: Or_6A
          type: sequencer
          sequencerType: any
          triggers: 
            - activity: If_6B
        - name: If_6A
          type: switch
          triggers: 
            - activity: ConcatNDCDrug
              conditionType: Custom
              condition: "Count (ExclusionList, \";\": \"ConcatNDCDrug\" :\";\") = 0"
            - activity: Or_6A
              conditionType: Otherwise
        - name: LoadNDC
          type: routine
          routinename: "DB.LOAD"
          parameters:
            - name: "Arg1"
              expression: "\"NDC\""
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
              expression: "\"NDC.\": SrcSysCd :\".dat\""
            - name: "Arg9"
              expression: "\"N\""
            - name: "Arg10"
              expression: "$IDSOwner"
        - name: ConcatNDCDrug
          type: shell
          path: cd
          command: "#$FilePath#/scripts; prepare_files.ksh #$FilePath#/key *.DrugNDC* IdsNdcDrugExtr.DrugNDC '-k9,9 -k5,5r'  '-k9,9'"
          triggers: 
            - activity: Or_6A
        - name: RmNDC
          type: shell
          path: rm
          command: " -f \"#$FilePath#/key/IdsNdcDrugExtr.DrugNDC.uniq\" "
          triggers: 
            - activity: Or_6C
        - name: IdsNdcFkey
          type: dataflow
          executionType: reset then run
          jobname: IdsNdcFkey
          parameters:
            - name: "FilePath"
              expression: "$FilePath"
            - name: "InFile"
              expression: "\"IdsNdcDrugExtr.DrugNDC.uniq\""
            - name: "Logging"
              expression: "'X'"
            - name: "Source"
              expression: SrcSysCd
          triggers: 
            - activity: Or_6B
        - name: LoadW_DRUG_CLM
          type: routine
          routinename: "DB.LOAD"
          parameters:
            - name: "Arg1"
              expression: "\"W_DRUG_CLM\""
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
              expression: "\"W_DRUG_CLM.dat\""
            - name: "Arg9"
              expression: "\"Y\""
            - name: "Arg10"
              expression: "$IDSOwner"
        - name: Or_7G
          type: sequencer
          sequencerType: any
          triggers: 
            - activity: If_7H
        - name: If_5E
          type: switch
          triggers: 
            - activity: LoadW_DRUG_CLM
              conditionType: Custom
              condition: "Count (ExclusionList, \";\": \"LoadW_DRUG_CLM\" :\";\") = 0"
        - name: IdsDrugClmFkey
          type: dataflow
          executionType: reset then run
          jobname: IdsDrugClmFkey
          parameters:
            - name: "IDSDB"
              expression: "$IDSDB"
            - name: "Source"
              expression: SrcSysCd
            - name: "SrcSysCdSk"
              expression: SrcSysCdSK
            - name: "RunCycle"
              expression: RunCycle
            - name: "Logging"
              expression: Logging
            - name: "RunID"
              expression: RunID
            - name: "FilePath"
              expression: "$FilePath"
          triggers: 
            - activity: Or_5B
        - name: RmDrugClm
          type: shell
          path: rm
          command: " -f \"#$FilePath#/key/IdsDrugClmDrugExtr.DrugClmDrug.uniq\" "
          triggers: 
            - activity: Or_5C
        - name: ConcatDrugClmDrug
          type: shell
          path: cd
          command: "#$FilePath#/scripts; prepare_files.ksh #$FilePath#/key *.DrugClmDrug*  IdsDrugClmDrugExtr.DrugClmDrug '-k9,9 -k5,5r'  '-k9,9' "
          triggers: 
            - activity: Or_5A
        - name: LoadDrugClm
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
              expression: "\"DRUG_CLM.\": SrcSysCd :\".dat\""
            - name: "Arg9"
              expression: "\"N\""
            - name: "Arg10"
              expression: "$IDSOwner"
        - name: If_5A
          type: switch
          triggers: 
            - activity: ConcatDrugClmDrug
              conditionType: Custom
              condition: "Count (ExclusionList, \";\": \"ConcatDrugClmDrug\" :\";\") = 0"
            - activity: Or_5A
              conditionType: Otherwise
        - name: Or_5A
          type: sequencer
          sequencerType: any
          triggers: 
            - activity: If_5B
        - name: If_5B
          type: switch
          triggers: 
            - activity: IdsDrugClmFkey
              conditionType: Custom
              condition: "Count (ExclusionList, \";\": \"IdsDrugClmFkey\" :\";\") = 0"
            - activity: Or_5B
              conditionType: Otherwise
        - name: Or_5B
          type: sequencer
          sequencerType: any
          triggers: 
            - activity: If_5C
        - name: If_5D
          type: switch
          triggers: 
            - activity: LoadDrugClm
              conditionType: Custom
              condition: "Count (ExclusionList, \";\": \"LoadDrugClm\" :\";\") = 0"
        - name: If_7G
          type: switch
          triggers: 
            - activity: RmProv
              conditionType: Custom
              condition: "Count (ExclusionList, \";\": \"RmProv\" :\";\") = 0"
            - activity: Or_7G
              conditionType: Otherwise
        - name: If_5C
          type: switch
          triggers: 
            - activity: RmDrugClm
              conditionType: Custom
              condition: "Count (ExclusionList, \";\": \"RmDrugClm\" :\";\") = 0"
            - activity: Or_5C
              conditionType: Otherwise
        - name: Or_5C
          type: sequencer
          sequencerType: any
          triggers: 
            - activity: If_5D
            - activity: If_5E
            - activity: If_5F
        - name: If_7H
          type: switch
          triggers: 
            - activity: LoadProv
              conditionType: Custom
              condition: "Count (ExclusionList, \";\": \"LoadProv\" :\";\") = 0"
        - name: Or_7F
          type: sequencer
          sequencerType: any
          triggers: 
            - activity: If_7G
        - name: If_7F
          type: switch
          triggers: 
            - activity: IdsProvFkey
              conditionType: Custom
              condition: "Count (ExclusionList, \";\": \"IdsProvFkey\" :\";\") = 0"
            - activity: Or_7F
              conditionType: Otherwise
        - name: Or_7E
          type: sequencer
          sequencerType: any
          triggers: 
            - activity: If_7F
        - name: LoadP_CLM_PDX
          type: routine
          routinename: "DB.LOAD"
          parameters:
            - name: "Arg1"
              expression: "\"P_CLM_PDX\""
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
              expression: "\"P_CLM_PDX.dat\""
            - name: "Arg9"
              expression: "\"Y\""
            - name: "Arg10"
              expression: "$IDSOwner"
        - name: If_5F
          type: switch
          triggers: 
            - activity: LoadP_CLM_PDX
              conditionType: Custom
              condition: "Count (ExclusionList, \";\": \"LoadP_CLM_PDX\" :\";\") = 0"
        - name: If_5A1
          type: switch
          triggers: 
            - activity: BCBSKCCommDrugClmExtr
              conditionType: Custom
              condition: "Count (ExclusionList, \";\": \"BCBSKCCommDrugClmExtr\" :\";\") = 0"
            - activity: Or_5B1
              conditionType: Otherwise
        - name: Or_5B1
          type: sequencer
          sequencerType: any
          triggers: 
            - activity: Sequencer_209


