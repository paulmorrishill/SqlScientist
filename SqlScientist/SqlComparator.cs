using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Reflection;

namespace SqlScientist
{
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

        var summary = new ComparisonSummary
        {
          ColumnsAreSame = true,
          ResultsAreIdentical = true
        };
        
        var comparisonOutput = new ComparisonResult
        {
          Output1 = command1Output,
          Output2 = command2Output,
          ComparisonSummary = summary
        };

        CompareColumnHeadersAndApplyToSummary(command1Output, command2Output, summary);
        //var result1 = returnValueParam1.Value;
        //var result2 = returnValueParam2.Value;
        CompareDataAndApplyToSummary(command1Output, command2Output, summary);

        return comparisonOutput;
      }
    }

    private static void CompareDataAndApplyToSummary(QueryOutput command1Output, QueryOutput command2Output,
      ComparisonSummary summary)
    {
      for (var r = 0; r < command1Output.Rows.Count; r++)
      {
        var row1 = command1Output.Rows[r];
        var row2 = command2Output.Rows[r];
        for (var i = 0; i < row1.Values.Count; i++)
        {
          var result1 = row1.Values[i];
          var result2 = row2.Values[i];
          var valuesAreSame = AreOutputsSame(result1, result2);
          if (!valuesAreSame)
          {
            summary.ResultsAreIdentical = false;
          }
        }
      }
    }

    private static void CompareColumnHeadersAndApplyToSummary(QueryOutput command1Output, QueryOutput command2Output,
      ComparisonSummary summary)
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

    private static QueryOutput ReadOutput(IDbCommand command1)
    {
      var rows = new List<QueryRow>();
      var headers = new List<ColumnInfo>();
      using (var reader = command1.ExecuteReader())
      {
        var headerTable = reader.GetSchemaTable();
        for (var headerIndex = 0; headerIndex < headerTable.Rows.Count; headerIndex++)
        {
          var column = headerTable.Rows[headerIndex];
          var columnName = column["ColumnName"].ToString();
          var allowsNull = (bool)column["AllowDBNull"];
          var type = reader.GetDataTypeName(headerIndex);
          var columnSize = (int)column["ColumnSize"];
          
          headers.Add(new ColumnInfo
          {
            Name = columnName,
            SqlDataType = type,
            IsNullable = allowsNull,
            Size = columnSize,
            DataType = Assembly.GetAssembly(typeof(string)).GetType(column["DataType"].ToString())
          });
        }
        
        while (reader.Read())
        {
          var result1 = new List<object>();
          for (var column = 0; column < reader.FieldCount; column++)
          {
            result1.Add(reader.GetValue(column));
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
}