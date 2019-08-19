using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Net.Http.Headers;
using System.Reflection;
using DapperExtensions;
using NUnit.Framework;
using Should;

namespace SqlScientistTests
{
  public interface ISqlCommandFactory
  {
    IDbCommand CreateCommand(string query);
  }
  
  public class SqlComparatorTests : ISqlCommandFactory
  {
    private SqlComparator _comparator;
    private SqlConnection _connection;

    [SetUp]
    public void SetUp()
    {
      _connection = new SqlConnection(@"Server=(localdb)\test;Integrated Security=true");
      _connection.Open();
      RunSql("DROP DATABASE IF EXISTS sql_scientist_test");
      RunSql("CREATE DATABASE sql_scientist_test");
      RunSql("USE sql_scientist_test");
      _comparator = new SqlComparator(_connection, this);
    }

    [TearDown]
    public void Teardown()
    {
      _connection.Close();
      _connection.Dispose();
    }
    
    [Test]
    public void CanCompareSingleIntegerSelectsWhenTheyAreIdentical()
    {
      AreSame("SELECT 1", "SELECT 1").ShouldBeTrue();
    }

    [Test]
    public void CanCompareSingleIntegerSelectsWhenTheyProduceDifferentResults()
    {
      AreSame("SELECT 1", "SELECT 2").ShouldBeFalse();
    }

    [Test]
    public void CanCompareSingleIntegerQueriesThatAreDifferentButProduceTheSameResult()
    {
      AreSame("SELECT 2", "SELECT 1 + 1").ShouldBeTrue();
    }
    
    [Test]
    public void CanCompareTwoIdenticalStringValues()
    {
      AreSame("SELECT 'Test1'", "SELECT 'Test1'").ShouldBeTrue();
    }
    
    [Test]
    public void CanCompareTwoDifferentStringValues()
    {
      AreSame("SELECT 'Test1'", "SELECT 'Test2'").ShouldBeFalse();
    }
    
    [Test]
    public void CanCompareAStringAndAnInteger()
    {
      AreSame("SELECT 'Test1'", "SELECT 123").ShouldBeFalse();
    }

    [Test]
    public void CanCompareMultipleColumnsOfIntegers()
    {
      AreSame("SELECT 1, 2", "SELECT 1, 2").ShouldBeTrue();
      AreSame("SELECT 1, 2", "SELECT 1, 1").ShouldBeFalse();
    }
    
    [Test]
    public void CanCompareMultipleRowsOfMultipleColumnsOfIntegers()
    {
      AreSame("SELECT 1, 1 UNION ALL SELECT 1, 1", "SELECT 1, 1 UNION ALL SELECT 1, 1").ShouldBeTrue();
      AreSame("SELECT 1, 1 UNION ALL SELECT 1, 1", "SELECT 1, 1 UNION ALL SELECT 1, 2").ShouldBeFalse();
    }

    [Test]
    public void CanReadQueryOutputFromSimpleSelect()
    {
      var comparison = _comparator.AreQueryOutputsIdentical("SELECT 'test1'", "SELECT 'test2'");
      comparison.Output1.Rows.Count.ShouldEqual(1);
      comparison.Output1.Rows[0].Values.Count.ShouldEqual(1);
      comparison.Output1.Rows[0].Values[0].ShouldEqual("test1");
      comparison.Output2.Rows.Count.ShouldEqual(1);
      comparison.Output2.Rows[0].Values.Count.ShouldEqual(1);
      comparison.Output2.Rows[0].Values[0].ShouldEqual("test2");
    }
    
    [Test]
    public void CanReadQueryOutputFromIdenticalSimpleSelect()
    {
      var comparison = _comparator.AreQueryOutputsIdentical("SELECT 'test1'", "SELECT 'test1'");
      comparison.Output1.Rows.Count.ShouldEqual(1);
      comparison.Output1.Rows[0].Values.Count.ShouldEqual(1);
      comparison.Output1.Rows[0].Values[0].ShouldEqual("test1");
      comparison.Output2.Rows.Count.ShouldEqual(1);
      comparison.Output2.Rows[0].Values.Count.ShouldEqual(1);
      comparison.Output2.Rows[0].Values[0].ShouldEqual("test1");
    }

    [Test]
    public void CanReadColumnHeadersFromQuery()
    {
      var q = "SELECT 'test1' as 'Col1', 'test2'";
      var comparison = _comparator.AreQueryOutputsIdentical(q, q);
      comparison.Output1.Columns[0].Name.ShouldEqual("Col1");
      comparison.Output1.Columns[0].DataType.ShouldEqual(typeof(string));
      comparison.Output1.Columns[1].Name.ShouldEqual("");
    }
    
    // TODO: Row count mismatch show missing rows from query 1
    // TODO: Row count mistmatch show missing rows from query 2
    // TODO: 
    private bool AreSame(string q1, string q2)
    {
      return _comparator.AreQueryOutputsIdentical(q1, q2).AreSame;
    }

    private void RunSql(string query)
    {
      using (var c = new SqlCommand(query, _connection))
      {
        c.ExecuteNonQuery();
      }
    }
    
    public IDbCommand CreateCommand(string query)
    {
      return new SqlCommand(query);
    }
  }

  public class ComparisonResult
  {
    public QueryOutput Output1 { get; set; }
    public QueryOutput Output2 { get; set; }
    public ComparisonSummary ComparisonSummary { get; set; }
    public bool AreSame { get; set; }

    public ComparisonResult()
    {
    }
  }

  public class ComparisonSummary
  {
  }

  public class SqlComparator
  {
    private IDbConnection _connection;
    private ISqlCommandFactory _commandFactory;

    public SqlComparator(IDbConnection connection, ISqlCommandFactory commandFactory)
    {
      _commandFactory = commandFactory;
      _connection = connection;
    }
    
    public ComparisonResult AreQueryOutputsIdentical(string query1, string query2)
    {
      using(var command1 = _commandFactory.CreateCommand(query1))
      using (var command2 = _commandFactory.CreateCommand(query2))
      {
        const string returnParam = "@returnValue";
        var returnValueParam1 = new SqlParameter(returnParam, SqlDbType.Int);
        returnValueParam1.Direction = ParameterDirection.ReturnValue;
        var returnValueParam2 = new SqlParameter(returnParam, SqlDbType.Int);
        returnValueParam2.Direction = ParameterDirection.ReturnValue;

        command1.Connection = _connection;
        command2.Connection = _connection;

        command1.Parameters.Add(returnValueParam1);
        command2.Parameters.Add(returnValueParam2);

        var command1Output = ReadOutput(command1);
        var command2Output = ReadOutput(command2);
        
        var comparisonOutput = new ComparisonResult
        {
          AreSame = false,
          Output1 = command1Output,
          Output2 = command2Output
        };
        
        //var result1 = returnValueParam1.Value;
        //var result2 = returnValueParam2.Value;
        for (var r = 0; r < command1Output.Rows.Count; r++)
        {
          var row1 = command1Output.Rows[r];
          var row2 = command2Output.Rows[r];
          for(var i = 0; i < row1.Values.Count; i++)
          {
            var result1 = row1.Values[i];
            var result2 = row2.Values[i];
            var valuesAreSame = AreOutputsSame(result1, result2);
            if (!valuesAreSame)
            {
              comparisonOutput.AreSame = false;
              return comparisonOutput;
            }
          }
        }

        comparisonOutput.AreSame = true;
        return comparisonOutput;
      }
    }

    private static bool AreOutputsSame(object result1, object result2)
    {
      if (result1.GetType() != result2.GetType())
        return false;

      if (result1 is string)
      {
        return (string) result1 == (string) result2;
      }

      return (int) result1 == (int) result2;
    }

    private static QueryOutput ReadOutput(IDbCommand command1)
    {
      var rows = new List<QueryRow>();
      var headers = new List<ColumnInfo>();
      using (var reader1 = command1.ExecuteReader())
      {
        var headerTable = reader1.GetSchemaTable();
        for (var headerIndex = 0; headerIndex < headerTable.Rows.Count; headerIndex++)
        {
          var column = headerTable.Rows[headerIndex];
          var columnName = column["ColumnName"].ToString();
          headers.Add(new ColumnInfo
          {
            Name = columnName,
            DataType = Assembly.GetAssembly(typeof(string)).GetType(column["DataType"].ToString())
          });
        }
        
        while (reader1.Read())
        {
          var result1 = new List<object>();
          for (var column = 0; column < reader1.FieldCount; column++)
          {
            result1.Add(reader1.GetValue(column));
          }
          rows.Add(new QueryRow(result1));
        }
      }

      return new QueryOutput
      {
        Rows = rows,
        Columns = headers
      };
    }
  }
  
  public class QueryOutput
  {
    public List<QueryRow> Rows = new List<QueryRow>();
    public List<ColumnInfo> Columns { get; set; }
  }

  public class ColumnInfo
  {
    public string Name { get; set; }
    public Type DataType { get; set; }
    public Type SqlDataType { get; set; }
  }

  public class QueryRow
  {
    public readonly List<object> Values;

    public QueryRow(List<object> columnValues)
    {
      Values = columnValues;
    }
  }
}