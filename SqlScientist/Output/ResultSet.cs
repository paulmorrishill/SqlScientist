using System.Collections.Generic;

namespace SqlScientist
{
  public class ResultSet
  {
    public List<QueryRow> Rows = new List<QueryRow>();
    public List<ColumnInfo> Columns { get; set; } = new List<ColumnInfo>();
  }
}