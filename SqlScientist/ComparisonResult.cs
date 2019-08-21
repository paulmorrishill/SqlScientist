namespace SqlScientist
{
  public class ComparisonResult
  {
    public QueryOutput Output1 { get; set; }
    public QueryOutput Output2 { get; set; }
    public ComparisonSummary ComparisonSummary { get; set; }

    public ComparisonResult()
    {
    }
  }
}