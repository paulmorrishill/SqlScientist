using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Reflection;

namespace SqlScientist
{
  public interface IDatabaseConnectionProvider
  {
    IDbConnection GetConnection1();
    IDbConnection GetConnection2();
  }
  
  public class SqlComparator
  {
    private IDatabaseConnectionProvider _connectionProvider;
    private ISqlCommandFactory _commandFactory;

    public SqlComparator(IDatabaseConnectionProvider connectionProviderProvider, ISqlCommandFactory commandFactory)
    {
      _commandFactory = commandFactory;
      _connectionProvider = connectionProviderProvider;
    }
    
    public QueryComparison CompareQueryOutputs(ComparisonInput comparisonInput)
    {
      var resultSetComparisons = new List<ResultSetComparisonResult>();
      using(var command1 = _commandFactory.CreateCommand(comparisonInput.Query1))
      using (var command2 = _commandFactory.CreateCommand(comparisonInput.Query2))
      {
        ConfigureCommand(command1, comparisonInput.QueryParameters);
        ConfigureCommand(command2, comparisonInput.QueryParameters);

        var command1ResultSets = ReadOutput(command1);
        var command2ResultSets = ReadOutput(command2);

        var leastResultSets = Math.Min(command1ResultSets.Count, command2ResultSets.Count);
        
        for (var set = 0; set < leastResultSets; set++)
        {
          var command1ResultSet = command1ResultSets[set];
          var command2ResultSet = command2ResultSets[set];
          
          var summary = new ResultSetComparisonSummary
          {
            ColumnsAreSame = true,
            ResultsAreIdentical = true
          };
        
          var comparisonOutput = new ResultSetComparisonResult
          {
            Output1 = command1ResultSet,
            Output2 = command2ResultSet,
            ResultSetSummary = summary
          };

          CompareColumnHeadersAndApplyToSummary(command1ResultSet, command2ResultSet, summary);
          CompareDataAndApplyToSummary(command1ResultSet, command2ResultSet, summary);
          
          resultSetComparisons.Add(comparisonOutput);
        }

        var allResultSetsIdentical = resultSetComparisons.TrueForAll(r => r.ResultSetSummary.ResultsAreIdentical);
        var resultSetCountMismatch = command1ResultSets.Count != command2ResultSets.Count;
        
        return new QueryComparison
        {
          ResultsAreIdentical = allResultSetsIdentical && !resultSetCountMismatch,
          ResultSetComparisons = resultSetComparisons,
          ResultSetCountsAreNotSame = resultSetCountMismatch
        };
      }
    }

    private void ConfigureCommand(IDbCommand command1,
      List<ComparisonParameter> comparisonInputQuery1Parameters)
    {
      command1.Connection = _connectionProvider.GetConnection1();
      foreach (var param in comparisonInputQuery1Parameters)
      {
        command1.Parameters.Add(_commandFactory.CreateCommandParameter(param.Name, param.Value));
      }
    }

    private static void CompareDataAndApplyToSummary(ResultSet command1Output, ResultSet command2Output,
      ResultSetComparisonSummary summary)
    {
      var smallestRows = Math.Min(command1Output.Rows.Count, command2Output.Rows.Count);
      for (var rowIndex = 0; rowIndex < smallestRows; rowIndex++)
      {
        var row1 = command1Output.Rows[rowIndex];
        var row2 = command2Output.Rows[rowIndex];
        for (var columnIndex = 0; columnIndex < row1.Values.Count; columnIndex++)
        {
          var result1 = row1.Values[columnIndex];
          var result2 = row2.Values[columnIndex];
          var valuesAreSame = AreOutputsSame(result1, result2);
          if (!valuesAreSame)
          {
            summary.ResultsAreIdentical = false;
            var rowDifference = new RowDifference
            {
              RowIndex = rowIndex
            };
            rowDifference.CellDifferences.Add(new CellDifference
            {
              ColumnIndex = columnIndex
            });
            summary.DataDifferences.Add(rowDifference);
          }
        }

        if (command1Output.Rows.Count != command2Output.Rows.Count)
        {
          summary.ResultsAreIdentical = false;
          summary.RowCountMismatch = true;
        }
      }
    }

    private static void CompareColumnHeadersAndApplyToSummary(ResultSet command1Output, ResultSet command2Output,
      ResultSetComparisonSummary summary)
    {
      var command1ColumnCount = command1Output.Columns.Count;
      var command2ColumnCount = command2Output.Columns.Count;
      var fewestColumns = Math.Min(command1ColumnCount, command2ColumnCount);
      for (var c = 0; c < fewestColumns; c++)
      {
        var command1ColumnHeader = command1Output.Columns[c];
        var command2ColumnHeader = command2Output.Columns[c];
        var columnNameIsDifferent = command1ColumnHeader.Name != command2ColumnHeader.Name;
        var columnDataTypeIsDifferent = command1ColumnHeader.SqlDataType != command2ColumnHeader.SqlDataType;
        var columnNullabilityIsDifferent = command1ColumnHeader.IsNullable != command2ColumnHeader.IsNullable;
        var sizeIsDifferent = command1ColumnHeader.Size != command2ColumnHeader.Size;
        
        if (columnNameIsDifferent || 
            columnDataTypeIsDifferent || 
            columnNullabilityIsDifferent || 
            sizeIsDifferent)
        {
          summary.ResultsAreIdentical = false;
          summary.ColumnsAreSame = false;
          summary.ColumnDifferences.Add(new ColumnDifference
          {
            Index = c,
            NameIsDifferent = columnNameIsDifferent,
            TypeIsDifferent = columnDataTypeIsDifferent,
            NullabilityIsDifferent = columnNullabilityIsDifferent,
            SizeIsDifferent = sizeIsDifferent
          });
        }
      }

      if (command1ColumnCount != command2ColumnCount)
      {
        summary.ResultsAreIdentical = false;
        summary.ColumnsAreSame = false;
        summary.ColumnCountMismatch = true;
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

    private static List<ResultSet> ReadOutput(IDbCommand command1)
    {
      var outputSets = new List<ResultSet>();
      using (var reader = command1.ExecuteReader())
      {
        do
        {
          var resultSet = new ResultSet();
          PopulateHeadersFromReader(reader, resultSet.Columns);
          while (reader.Read())
          {
            var result1 = new List<object>();
            for (var column = 0; column < reader.FieldCount; column++)
            {
              result1.Add(reader.GetValue(column));
            }

            resultSet.Rows.Add(new QueryRow(result1));
          }
          outputSets.Add(resultSet);
        } while (reader.NextResult());
      }

      return outputSets;
    }

    private static void PopulateHeadersFromReader(IDataReader reader, List<ColumnInfo> headers)
    {
      var headerTable = reader.GetSchemaTable();
      if (headerTable == null)
        return;
      for (var headerIndex = 0; headerIndex < headerTable.Rows.Count; headerIndex++)
      {
        var column = headerTable.Rows[headerIndex];
        var columnName = column["ColumnName"].ToString();
        var allowsNull = (bool) column["AllowDBNull"];
        var type = reader.GetDataTypeName(headerIndex);
        var columnSize = (int) column["ColumnSize"];

        headers.Add(new ColumnInfo
        {
          Name = columnName,
          SqlDataType = type,
          IsNullable = allowsNull,
          Size = columnSize,
          DataType = Assembly.GetAssembly(typeof(string)).GetType(column["DataType"].ToString())
        });
      }
    }
  }

  public class QueryComparison
  {
    public List<ResultSetComparisonResult> ResultSetComparisons { get; set; }
    public bool ResultsAreIdentical { get; set; }
    public bool ResultSetCountsAreNotSame { get; set; }
  }
}