using System.Data;

namespace SqlScientist
{
  public interface ISqlCommandFactory
  {
    IDbCommand CreateCommand(string query);
  }
}