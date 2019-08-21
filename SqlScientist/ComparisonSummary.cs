using System;
using System.Collections.Generic;

namespace SqlScientist
{
  public class ComparisonSummary
  {
    public bool ResultsAreIdentical { get; set; }
    public bool ColumnsAreSame { get; set; }
    public List<ColumnDifference> ColumnDifferences { get; } = new List<ColumnDifference>();
    public bool ColumnCountMismatch { get; set; }
    public List<RowDifference> DataDifferences { get; set; } = new List<RowDifference>();
  }

  public class RowDifference
  {
    public int RowIndex { get; set; }
    public List<CellDifference> CellDifferences { get; } = new List<CellDifference>();
  }

  public class CellDifference
  {
    /*public string ColumnName { get; set; }*/
    public int ColumnIndex { get; set; }
    /* public decimal? NumericDifference { get; set; }
    public TimeSpan? DurationDifference { get; set; } */
  }
}