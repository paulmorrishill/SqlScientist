using System.Collections.Generic;
using System.Data.SqlClient;

namespace SqlScientist.Parameterization
{
  public class ComparisonParameterCollection
  {
    public ComparisonParameterCollection(List<ComparisonParameter> parameters)
    {
      Parameters = parameters;
    }

    public ComparisonParameterCollection() : this(new List<ComparisonParameter>())
    {
      
    }
    
    public List<ComparisonParameter> Parameters { get; set; }
  }
}