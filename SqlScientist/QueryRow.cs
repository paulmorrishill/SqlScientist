using System.Collections.Generic;

namespace SqlScientist
{
  public class QueryRow
  {
    public readonly List<object> Values;

    public QueryRow(List<object> columnValues)
    {
      Values = columnValues;
    }
  }
}