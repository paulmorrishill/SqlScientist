using System.Data;

namespace SqlScientist
{
  public interface ISqlCommandFactory
  {
    IDbCommand CreateCommand(string query);
    object CreateCommandParameter(string name, object value);
  }
}