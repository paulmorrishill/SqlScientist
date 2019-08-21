using System.Collections.Generic;

namespace SqlScientist
{
  public class ComparisonSummary
  {
    public bool ResultsAreIdentical { get; set; }
    public bool ColumnsAreSame { get; set; }
    public List<ColumnDifference> ColumnDifferences { get; } = new List<ColumnDifference>();
    public bool ColumnCountMismatch { get; set; }
  }
}