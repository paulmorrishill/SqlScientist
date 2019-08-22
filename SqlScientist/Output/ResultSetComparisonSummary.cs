using System;
using System.Collections.Generic;

namespace SqlScientist
{
  public class ResultSetComparisonSummary
  {
    public bool ResultsAreIdentical { get; set; }
    public bool ColumnsAreSame { get; set; }
    public List<ColumnDifference> ColumnDifferences { get; } = new List<ColumnDifference>();
    public bool ColumnCountMismatch { get; set; }
    public List<RowDifference> DataDifferences { get; set; } = new List<RowDifference>();
  }
}