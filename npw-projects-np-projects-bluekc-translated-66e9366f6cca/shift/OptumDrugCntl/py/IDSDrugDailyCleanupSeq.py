#!/usr/bin/python3

from npadf import *

def IDSDrugDailyCleanupSeqActivities(ctx):
  def If_3B():
    def WEB_DRVR_RevOrig():
      return [], [WaitActivity(
        name = "WEB_DRVR_RevOrig",
        description = "Placeholder for wait",
        wait_time_in_seconds = 1)]
    def Mv_WebdrvrRevsOrig():
      activities = [CustomActivity(
        name = "Mv_WebdrvrRevsOrig",
        command = ctx.formatShellCommand('cd' toString($FilePath) + '/load/processed; mv -f  ../W_WEBDM_ETL_DRVR.dat.DrugReversalOrig.' + toString($RunID) + ' . '),
        depends_on = [
          ActivityDependency(
            activity = "WEB_DRVR_RevOrig",
            dependency_conditions = [DependencyCondition.SUCCEEDED])],
        linked_service_name = LinkedServiceReference(type='LinkedServiceReference',reference_name=ctx.batch_linked_service))]
      return [], activities
    artifacts = []
    activities = [IfConditionActivity(name = "If_3B",
      expression = Expression(type="Expression", value="@equals(int(string(sub(length(array(split(pipeline().parameters.ExclusionList,concat(';', concat('MvFiles', ';'))))),1))),0)"),
      if_true_activities = collect(artifacts,
        WEB_DRVR_RevOrig(),
        Mv_WebdrvrRevsOrig()))]
    return artifacts, activities
  def If_2B():
    def WEB_DRVR_RevAdj():
      return [], [WaitActivity(
        name = "WEB_DRVR_RevAdj",
        description = "Placeholder for wait",
        wait_time_in_seconds = 1)]
    def Mv_WebdrvrRevAdj():
      activities = [CustomActivity(
        name = "Mv_WebdrvrRevAdj",
        command = ctx.formatShellCommand('cd' toString($FilePath) + '/load/processed; mv -f  ../W_WEBDM_ETL_DRVR.dat.DrugReversalAdj.' + toString($RunID) + ' . '),
        depends_on = [
          ActivityDependency(
            activity = "WEB_DRVR_RevAdj",
            dependency_conditions = [DependencyCondition.SUCCEEDED])],
        linked_service_name = LinkedServiceReference(type='LinkedServiceReference',reference_name=ctx.batch_linked_service))]
      return [], activities
    artifacts = []
    activities = [IfConditionActivity(name = "If_2B",
      expression = Expression(type="Expression", value="@equals(int(string(sub(length(array(split(pipeline().parameters.ExclusionList,concat(';', concat('MvFiles', ';'))))),1))),0)"),
      if_true_activities = collect(artifacts,
        WEB_DRVR_RevAdj(),
        Mv_WebdrvrRevAdj()))]
    return artifacts, activities
  def If_1A():
    def NascoFile():
      return [], [WaitActivity(
        name = "NascoFile",
        description = "Placeholder for wait",
        wait_time_in_seconds = 1)]
    def MvFiles():
      activities = [CustomActivity(
        name = "MvFiles",
        command = ctx.formatShellCommand('cd' toString($FilePath) + '/load/processed;  mv -f  ../ClmRvversalUpdates.' + toString($RunID) + ' . ; mv -f ../CLM_PCA_UPDT.dat .'),
        depends_on = [
          ActivityDependency(
            activity = "NascoFile",
            dependency_conditions = [DependencyCondition.SUCCEEDED])],
        linked_service_name = LinkedServiceReference(type='LinkedServiceReference',reference_name=ctx.batch_linked_service))]
      return [], activities
    artifacts = []
    activities = [IfConditionActivity(name = "If_1A",
      expression = Expression(type="Expression", value="@equals(int(string(sub(length(array(split(pipeline().parameters.ExclusionList,concat(';', concat('MvFiles', ';'))))),1))),0)"),
      if_true_activities = collect(artifacts,
        NascoFile(),
        MvFiles()))]
    return artifacts, activities
  def If_1B():
    def WEB_DRVR():
      return [], [WaitActivity(
        name = "WEB_DRVR",
        description = "Placeholder for wait",
        wait_time_in_seconds = 1)]
    def Mv_Webdrvr():
      activities = [CustomActivity(
        name = "Mv_Webdrvr",
        command = ctx.formatShellCommand('cd' toString($FilePath) + '/load/processed; mv -f  ../W_WEBDM_ETL_DRVR.dat.Drug.' + toString($RunID) + ' . '),
        depends_on = [
          ActivityDependency(
            activity = "WEB_DRVR",
            dependency_conditions = [DependencyCondition.SUCCEEDED])],
        linked_service_name = LinkedServiceReference(type='LinkedServiceReference',reference_name=ctx.batch_linked_service))]
      return [], activities
    artifacts = []
    activities = [IfConditionActivity(name = "If_1B",
      expression = Expression(type="Expression", value="@equals(int(string(sub(length(array(split(pipeline().parameters.ExclusionList,concat(';', concat('MvFiles', ';'))))),1))),0)"),
      if_true_activities = collect(artifacts,
        WEB_DRVR(),
        Mv_Webdrvr()))]
    return artifacts, activities
  return merge(
    If_1B(),
    If_1A(),
    If_2B(),
    If_3B())

def IDSDrugDailyCleanupSeq(ctx):
  name = "IDSDrugDailyCleanupSeq"
  artifacts, activities = IDSDrugDailyCleanupSeqActivities(ctx)
  pipeline = PipelineResource(
    folder = PipelineFolder(name="claim/SeqDrug"),
    activities = activities,
    description = """
      IDS Drug Cleanup
      Parameters:
      -----------
      FilePath:
        File Path
      ExclusionList:
        ExclusionList
      RunID:
        Run ID
      IDSAcct:
        IDS DB Account
      IDSPW:
        IDS DB Password
      IDSOwner:
        IDS DB Owner
      IDSInstance:
        IDS DB Instance
      IDSDB:
        IDS Database
      IDSDSN:
        IDS DSN""",
    parameters = {
      "FilePath": ParameterSpecification(type="String", default_value=ctx.FilePath),
      "ExclusionList": ParameterSpecification(type="String", default_value="\"\""),
      "RunID": ParameterSpecification(type="Int"),
      "IDSAcct": ParameterSpecification(type="String", default_value=" "),
      "IDSPW": ParameterSpecification(type="String"),
      "IDSOwner": ParameterSpecification(type="String", default_value=ctx.IDSOwner),
      "IDSInstance": ParameterSpecification(type="String", default_value=ctx.IDSInstance),
      "IDSDB": ParameterSpecification(type="String", default_value=ctx.IDSDB),
      "IDSDSN": ParameterSpecification(type="String", default_value=ctx.IDSDSN)},
    variables = {
      "If_1A_fired": VariableSpecification(type="String", default_value="n"),
      "If_1B_fired": VariableSpecification(type="String", default_value="n"),
      "If_2B_fired": VariableSpecification(type="String", default_value="n"),
      "If_3B_fired": VariableSpecification(type="String", default_value="n")})
  return artifacts + [(name, pipeline)]

def main():
  ctx = TranslationContext()
  artifacts = (IDSDrugDailyCleanupSeq(ctx))
  if ctx.validateArtifacts(artifacts):
    ctx.deployArtifacts(artifacts)

if __name__ == '__main__':
  main()
