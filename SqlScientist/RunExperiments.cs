namespace SqlScientist
{
  public interface RunExperiments
  {
    void Execute(RunExperimentsInput input, ExperimentProgressHandler handler);
  }

  class RunExperimentsImpl : RunExperiments
  {
    public void Execute(RunExperimentsInput input, ExperimentProgressHandler handler)
    {
      throw new System.NotImplementedException();
    }
  }

  public interface ExperimentProgressHandler
  {
    void ExperimentsCompleted(bool outputsWereIdentical);
  }

  public class RunExperimentsInput
  {
    public string ConnectionString;
    public string OldCode;
    public string NewCode;
    public string DataQueryCode;
  }

}