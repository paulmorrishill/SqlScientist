using System.Collections.Generic;

namespace SqlScientist
{
  public class ComparisonInput
  {
    public ComparisonInput(string query1, string query2): this(query1, query2, new List<ComparisonParameter>())
    {
    }

    public ComparisonInput(string query1, string query2, List<ComparisonParameter> parameters)
    {
      Query1 = query1;
      Query2 = query2;
      QueryParameters = parameters;
    }

    public string Query1 { get; private set; }
    public string Query2 { get; private set; }
    public List<ComparisonParameter> QueryParameters { get; private set; }
  }
}