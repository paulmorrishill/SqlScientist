using System;
using System.Data;

namespace SqlScientist.Parameterization
{
  public class SqlParameterSource
  {
    public SqlParameterSource(IDbConnection connection)
    {
      
    }
    
    public ComparisonParameterCollection GetParametersFromQuery(string query)
    {
      throw new NotImplementedException();
    }
  }
}