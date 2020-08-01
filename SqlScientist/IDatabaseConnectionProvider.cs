using System.Data;

namespace SqlScientist
{
  public interface IDatabaseConnectionProvider
  {
    IDbConnection GetConnection1();
  }
}