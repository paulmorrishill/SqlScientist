using System.Collections.Generic;

namespace SqlScientist
{
  public class RowDifference
  {
    public int RowIndex { get; set; }
    public List<CellDifference> CellDifferences { get; } = new List<CellDifference>();
  }
}