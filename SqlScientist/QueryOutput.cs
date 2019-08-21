using System.Collections.Generic;

namespace SqlScientist
{
  public class QueryOutput
  {
    public List<QueryRow> Rows = new List<QueryRow>();
    public List<ColumnInfo> Columns { get; set; }
  }
}