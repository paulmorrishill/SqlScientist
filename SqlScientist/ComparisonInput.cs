using System.Collections.Generic;

namespace SqlScientist
{
  public class ComparisonInput
  {
    public ComparisonInput(string query1, string query2)
    {
      Query1 = query1;
      Query2 = query2;
    }

    public string Query1 { get; private set; }
    public string Query2 { get; private set; }
    public List<ComparisonParameter> QueryParameters { get; set; } = new List<ComparisonParameter>();
  }
}