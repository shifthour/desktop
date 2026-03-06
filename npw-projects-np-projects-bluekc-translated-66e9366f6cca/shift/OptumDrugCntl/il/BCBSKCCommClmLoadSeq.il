model:
  config:
    origin: datastage
  jobs:
    - name: BCBSKCCommClmLoadSeq
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
        - name: Logging
          type: string
          description: Logging
          defaultType: expr
          defaultValue: X
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
        - name: SrcSysCdSk
          type: integer
          description: SrcSysCdSk
      activities:
        - name: IdsClmRemitHistFkey
          type: dataflow
          executionType: run
          jobname: IdsClmRemitHistFkey
          parameters:
            - name: "FilePath"
              expression: "$FilePath"
            - name: "Source"
              expression: SrcSysCd
            - name: "SrcSysCdSk"
              expression: SrcSysCdSk
            - name: "InFile"
              expression: "\"IdsClmRemitHistDrugExtr.DrugClmRemitHist.uniq\""
            - name: "Logging"
              expression: Logging
          triggers: 
            - activity: Or_4B
        - name: IdsClmProvFkey
          type: dataflow
          executionType: reset then run
          jobname: IdsClmProvFkey
          parameters:
            - name: "FilePath"
              expression: "$FilePath"
            - name: "Logging"
              expression: Logging
            - name: "Source"
              expression: SrcSysCd
            - name: "InFile"
              expression: "\"IdsClmProvDrugExtr.DrugClmProv.uniq\""
          triggers: 
            - activity: Or_3B
        - name: RmClmProv
          type: shell
          path: rm
          command: " -f \"#$FilePath#/key/IdsClmProvDrugExtr.DrugClmProv.uniq\" "
          triggers: 
            - activity: Or_3C
        - name: ConcatClmProvDrug
          type: shell
          path: cd
          command: "#$FilePath#/scripts; prepare_files.ksh #$FilePath#/key  *.DrugClmProv*  IdsClmProvDrugExtr.DrugClmProv '-k9,9 -k5,5r'  '-k9,9'"
          triggers: 
            - activity: Or_3A
        - name: LoadClmProv
          type: routine
          routinename: "DB.LOAD"
          parameters:
            - name: "Arg1"
              expression: "\"CLM_PROV\""
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
              expression: "\"CLM_PROV.\": SrcSysCd :\".dat\""
            - name: "Arg9"
              expression: "\"N\""
            - name: "Arg10"
              expression: "$IDSOwner"
        - name: If_3A
          type: switch
          triggers: 
            - activity: ConcatClmProvDrug
              conditionType: Custom
              condition: "Count (ExclusionList, \";\": \"ConcatClmProvDrug\" :\";\") = 0"
            - activity: Or_3A
              conditionType: Otherwise
        - name: Or_3A
          type: sequencer
          sequencerType: any
          triggers: 
            - activity: If_3B
        - name: If_3B
          type: switch
          triggers: 
            - activity: IdsClmProvFkey
              conditionType: Custom
              condition: "Count (ExclusionList, \";\": \"IdsClmProvFkey\" :\";\") = 0"
            - activity: Or_3B
              conditionType: Otherwise
        - name: Or_3B
          type: sequencer
          sequencerType: any
          triggers: 
            - activity: If_3C
        - name: If_3D
          type: switch
          triggers: 
            - activity: LoadClmProv
              conditionType: Custom
              condition: "Count (ExclusionList, \";\": \"LoadClmProv\" :\";\") = 0"
            - activity: K_CLM_PROV
              conditionType: Custom
              condition: "Count (ExclusionList, \";\": \"K_CLM_PROV\" :\";\") = 0"
        - name: If_3C
          type: switch
          triggers: 
            - activity: RmClmProv
              conditionType: Custom
              condition: "Count (ExclusionList, \";\": \"RmClmProv\" :\";\") = 0"
            - activity: Or_3C
              conditionType: Otherwise
        - name: Or_3C
          type: sequencer
          sequencerType: any
          triggers: 
            - activity: If_3D
        - name: K_CLM_PROV
          type: routine
          routinename: "DB.LOAD"
          parameters:
            - name: "Arg1"
              expression: "\"K_CLM_PROV\""
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
              expression: "\"K_CLM_PROV.dat\""
            - name: "Arg9"
              expression: "\"N\""
            - name: "Arg10"
              expression: "$IDSOwner"
        - name: Or_2C
          type: sequencer
          sequencerType: any
          triggers: 
            - activity: If_2D
        - name: If_2C
          type: switch
          triggers: 
            - activity: RmClmLn
              conditionType: Custom
              condition: "Count (ExclusionList, \";\": \"RmClmLn\" :\";\") = 0"
            - activity: Or_2C
              conditionType: Otherwise
        - name: If_2D
          type: switch
          triggers: 
            - activity: LoadClmLn
              conditionType: Custom
              condition: "Count (ExclusionList, \";\": \"LoadClmLn\" :\";\") = 0"
        - name: Or_2B
          type: sequencer
          sequencerType: any
          triggers: 
            - activity: If_2C
        - name: If_2B
          type: switch
          triggers: 
            - activity: IdsClmLnFkey
              conditionType: Custom
              condition: "Count (ExclusionList, \";\": \"IdsClmLnFkey\" :\";\") = 0"
            - activity: Or_2B
              conditionType: Otherwise
        - name: Or_2A
          type: sequencer
          sequencerType: any
          triggers: 
            - activity: If_2B
        - name: If_2A
          type: switch
          triggers: 
            - activity: ConcatClmLnDrug
              conditionType: Custom
              condition: "Count (ExclusionList, \";\": \"ConcatClmLnDrug\" :\";\") = 0"
            - activity: Or_2A
              conditionType: Otherwise
        - name: LoadClmLn
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
              expression: "\"CLM_LN.\": SrcSysCd :\".dat\""
            - name: "Arg9"
              expression: "\"N\""
            - name: "Arg10"
              expression: "$IDSOwner"
        - name: ConcatClmLnDrug
          type: shell
          path: cd
          command: "#$FilePath#/scripts; prepare_files.ksh #$FilePath#/key *.DrugClmLn*  IdsClmLnDrugExtr.DrugClmLn '-k9,9 -k5,5r'  '-k9,9'"
          triggers: 
            - activity: Or_2A
        - name: RmClmLn
          type: shell
          path: rm
          command: " -f \"#$FilePath#/key/IdsClmLnDrugExtr.DrugClmLn.uniq\" "
          triggers: 
            - activity: Or_2C
        - name: IdsClmLnFkey
          type: dataflow
          executionType: reset then run
          jobname: IdsClmLnFkey
          parameters:
            - name: "FilePath"
              expression: "$FilePath"
            - name: "RunCycle"
              expression: RunCycle
            - name: "Logging"
              expression: Logging
            - name: "Source"
              expression: SrcSysCd
            - name: "InFile"
              expression: "\"IdsClmLnDrugExtr.DrugClmLn.uniq\""
          triggers: 
            - activity: Or_2B
        - name: IdsClmFkey
          type: dataflow
          executionType: reset then run
          jobname: IdsClmFkey
          parameters:
            - name: "FilePath"
              expression: "$FilePath"
            - name: "Source"
              expression: SrcSysCd
            - name: "SrcSysCdSk"
              expression: SrcSysCdSk
            - name: "InFile"
              expression: "\"IdsClmDrugExtr.DrugClm.uniq\""
            - name: "Logging"
              expression: Logging
            - name: "RunCycle"
              expression: RunCycle
            - name: "RunID"
              expression: RunID
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
            - activity: Or_1B
        - name: RmClm
          type: shell
          path: rm
          command: " -f \"#$FilePath#/key/IdsClmDrugExtr.DrugClm.uniq\" "
          triggers: 
            - activity: Or_1C
        - name: ConcatClmDrug
          type: shell
          path: cd
          command: "#$FilePath#/scripts; prepare_files.ksh #$FilePath#/key *.DrugClm.* IdsClmDrugExtr.DrugClm '-k9,9 -k5,5r'  '-k9,9'"
          triggers: 
            - activity: Or_1A
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
              expression: "\"CLM.\": SrcSysCd :\".dat\""
            - name: "Arg9"
              expression: "\"N\""
            - name: "Arg10"
              expression: "$IDSOwner"
        - name: If_1A
          type: switch
          triggers: 
            - activity: ConcatClmDrug
              conditionType: Custom
              condition: "Count (ExclusionList, \";\": \"ConcatClmDrug\" :\";\") = 0"
            - activity: Or_1A
              conditionType: Otherwise
        - name: Or_1A
          type: sequencer
          sequencerType: any
          triggers: 
            - activity: If_1B
        - name: If_1B
          type: switch
          triggers: 
            - activity: IdsClmFkey
              conditionType: Custom
              condition: "Count (ExclusionList, \";\": \"IdsClmFkey\" :\";\") = 0"
            - activity: Or_1B
              conditionType: Otherwise
        - name: Or_1B
          type: sequencer
          sequencerType: any
          triggers: 
            - activity: If_1C
        - name: If_1D
          type: switch
          triggers: 
            - activity: LoadClm
              conditionType: Custom
              condition: "Count (ExclusionList, \";\": \"LoadClm\" :\";\") = 0"
        - name: If_1C
          type: switch
          triggers: 
            - activity: RmClm
              conditionType: Custom
              condition: "Count (ExclusionList, \";\": \"RmClm\" :\";\") = 0"
            - activity: Or_1C
              conditionType: Otherwise
        - name: Or_1C
          type: sequencer
          sequencerType: any
          triggers: 
            - activity: If_1D
        - name: Or_4D
          type: sequencer
          sequencerType: any
        - name: ConcatClmRemitHistDrug
          type: shell
          path: cd
          command: "#$FilePath#/scripts; prepare_files.ksh #$FilePath#/key *.DrugClmRemitHist  IdsClmRemitHistDrugExtr.DrugClmRemitHist '-k9,9 -k5,5r'  '-k9,9'"
          triggers: 
            - activity: Or_4A
              conditionType: OK
        - name: RmClmRemitHist
          type: shell
          path: rm
          command: " -f \"#$FilePath#/key/IdsClmRemitHistDrugExtr.DrugClmRemitHist.uniq\" "
          triggers: 
            - activity: Or_4C
              conditionType: OK
        - name: Or_4C
          type: sequencer
          sequencerType: any
          triggers: 
            - activity: If_4D
        - name: If_4C
          type: switch
          triggers: 
            - activity: RmClmRemitHist
              conditionType: Custom
              condition: "Count (ExclusionList, \";\": \"RmClmRemitHist\" :\";\") = 0"
            - activity: Or_4C
              conditionType: Otherwise
        - name: If_4D
          type: switch
          triggers: 
            - activity: LoadClmRemitHist
              conditionType: Custom
              condition: "Count (ExclusionList, \";\": \"LoadClmRemitHist\" :\";\") = 0"
            - activity: Or_4D
              conditionType: Otherwise
        - name: Or_4B
          type: sequencer
          sequencerType: any
          triggers: 
            - activity: If_4C
        - name: If_4B
          type: switch
          triggers: 
            - activity: IdsClmRemitHistFkey
              conditionType: Custom
              condition: "Count (ExclusionList, \";\": \"IdsClmRemitHistFkey\" :\";\") = 0"
            - activity: Or_4B
              conditionType: Otherwise
        - name: Or_4A
          type: sequencer
          sequencerType: any
          triggers: 
            - activity: If_4B
        - name: If_4A
          type: switch
          triggers: 
            - activity: ConcatClmRemitHistDrug
              conditionType: Custom
              condition: "Count (ExclusionList, \";\": \"ConcatClmRemitHistDrug\" :\";\") = 0"
            - activity: Or_4A
              conditionType: Otherwise
        - name: LoadClmRemitHist
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
              expression: "\"CLM_REMIT_HIST.\":SrcSysCd:\".dat\""
            - name: "Arg9"
              expression: "\"N\""
            - name: "Arg10"
              expression: "$IDSOwner"
          triggers: 
            - activity: Or_4D
              conditionType: OK


