using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Net.Http.Headers;
using DapperExtensions;
using NUnit.Framework;
using Should;
using SqlScientist;

namespace SqlScientistTests
{
  public class SqlComparatorTests : ISqlCommandFactory, IDatabaseConnectionProvider
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
      _comparator = new SqlComparator(this, this);
    }

    [TearDown]
    public void Teardown()
    {
      _connection.Close();
      _connection.Dispose();
    }
    
    #region Simple comparisons
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

    #endregion

    #region Outputting collected query data

    [Test]
    public void CanReadQueryOutputFromSimpleSelect()
    {
      var comparison = RunComparison("SELECT 'test1'", "SELECT 'test2'");
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
      var comparison = RunComparison("SELECT 'test1'", "SELECT 'test1'");
      comparison.Output1.Rows.Count.ShouldEqual(1);
      comparison.Output1.Rows[0].Values.Count.ShouldEqual(1);
      comparison.Output1.Rows[0].Values[0].ShouldEqual("test1");
      comparison.Output2.Rows.Count.ShouldEqual(1);
      comparison.Output2.Rows[0].Values.Count.ShouldEqual(1);
      comparison.Output2.Rows[0].Values[0].ShouldEqual("test1");
    }

    #endregion

    #region Column headers
    
    [Test]
    public void CanReadColumnHeadersFromQuery()
    {
      const string q = "SELECT 'test1' as 'Col1', 1";
      var comparison = RunComparison(q, q);
      comparison.Output1.Columns[0].Name.ShouldEqual("Col1");
      comparison.Output1.Columns[0].DataType.ShouldEqual(typeof(string));
      comparison.Output1.Columns[0].SqlDataType.ShouldEqual("varchar");
      comparison.Output1.Columns[0].IsNullable.ShouldBeFalse();
      comparison.Output1.Columns[1].Name.ShouldEqual("");
      comparison.Output1.Columns[1].DataType.ShouldEqual(typeof(int));
      comparison.Output1.Columns[1].SqlDataType.ShouldEqual("int");      
      comparison.Output1.Columns[1].IsNullable.ShouldBeFalse();
    }

    [Test]
    public void CanGetNullableColumns()
    {
      RunSql("CREATE TABLE TestEntity (IntColNullable int NULL)");
      const string q = "SELECT * FROM TestEntity";
      var comparison = RunComparison(q, q);
      comparison.Output1.Columns[0].DataType.ShouldEqual(typeof(int));
      comparison.Output1.Columns[0].IsNullable.ShouldBeTrue();
    }
    
    [Test]
    public void CanGetColumnLengthsOfVaryingSizes()
    {
      RunSql("CREATE TABLE TestEntity (VarCharCol varchar(50))");
      const string q = "SELECT * FROM TestEntity";
      var comparison = RunComparison(q, q);
      comparison.Output1.Columns[0].DataType.ShouldEqual(typeof(string));
      comparison.Output1.Columns[0].SqlDataType.ShouldEqual("varchar");
      comparison.Output1.Columns[0].Size.ShouldEqual(50);
    }
    
    [Test]
    public void CanGetColumnLengthWhenColumnIsMaxLength()
    {
      RunSql("CREATE TABLE TestEntity (VarCharCol varchar(max))");
      const string q = "SELECT * FROM TestEntity";
      var comparison = RunComparison(q, q);
      comparison.Output1.Columns[0].DataType.ShouldEqual(typeof(string));
      comparison.Output1.Columns[0].SqlDataType.ShouldEqual("varchar");
      comparison.Output1.Columns[0].Size.ShouldEqual(2147483647);
    }

    [Test]
    public void GivenColumnsAreSameItReturnsColumnsSame()
    {
      const string q = "SELECT '' as 'C1'";
      var comparison = RunComparison(q, q);
      comparison.ResultSetSummary.ResultsAreIdentical.ShouldBeTrue();
      comparison.ResultSetSummary.ColumnsAreSame.ShouldBeTrue();
      comparison.ResultSetSummary.ColumnDifferences.Count.ShouldEqual(0);
    }
    
    [Test]
    public void CanOutputWhenColumnHeadersAreDifferent()
    {
      const string q1 = "SELECT '' as 'C1'";
      const string q2 = "SELECT '' as 'C2'";
      var comparison = RunComparison(q1, q2);
      comparison.ResultSetSummary.ResultsAreIdentical.ShouldBeFalse();
      comparison.ResultSetSummary.ColumnsAreSame.ShouldBeFalse();
      comparison.ResultSetSummary.ColumnDifferences.Count.ShouldEqual(1);
      comparison.ResultSetSummary.ColumnDifferences[0].Index.ShouldEqual(0);
      comparison.ResultSetSummary.ColumnDifferences[0].NameIsDifferent.ShouldBeTrue();
      comparison.ResultSetSummary.ColumnDifferences[0].TypeIsDifferent.ShouldBeFalse();
      comparison.ResultSetSummary.ColumnDifferences[0].NullabilityIsDifferent.ShouldBeFalse();
    }

    [Test]
    public void CanOutputWhenColumnHeadersAreDifferentAfterTheFirst()
    {
      const string q1 = "SELECT '' as 'C1', '' AS 'C2'";
      const string q2 = "SELECT '' as 'C1', '' AS 'C3'";
      var comparison = RunComparison(q1, q2);
      comparison.ResultSetSummary.ResultsAreIdentical.ShouldBeFalse();
      comparison.ResultSetSummary.ColumnsAreSame.ShouldBeFalse();      
      comparison.ResultSetSummary.ColumnDifferences.Count.ShouldEqual(1);
      comparison.ResultSetSummary.ColumnDifferences[0].Index.ShouldEqual(1);
      comparison.ResultSetSummary.ColumnDifferences[0].NameIsDifferent.ShouldBeTrue();
      comparison.ResultSetSummary.ColumnDifferences[0].TypeIsDifferent.ShouldBeFalse();
    }
    
    [Test]
    public void CanOutputWhenColumnHeadersAreDifferentInType()
    {
      const string q1 = "SELECT '0' as 'C1'";
      const string q2 = "SELECT 0 as 'C1'";
      var comparison = RunComparison(q1, q2);
      comparison.ResultSetSummary.ResultsAreIdentical.ShouldBeFalse();
      comparison.ResultSetSummary.ColumnsAreSame.ShouldBeFalse();      
      comparison.ResultSetSummary.ColumnDifferences.Count.ShouldEqual(1);
      comparison.ResultSetSummary.ColumnDifferences[0].Index.ShouldEqual(0);
      comparison.ResultSetSummary.ColumnDifferences[0].NameIsDifferent.ShouldBeFalse();
      comparison.ResultSetSummary.ColumnDifferences[0].TypeIsDifferent.ShouldBeTrue();
    }
    
    [Test]
    public void CanOutputWhenColumnHeadersAreDifferentInNameAndType()
    {
      const string q1 = "SELECT '0' as 'C1'";
      const string q2 = "SELECT 0 as 'C2'";
      var comparison = RunComparison(q1, q2);
      comparison.ResultSetSummary.ResultsAreIdentical.ShouldBeFalse();
      comparison.ResultSetSummary.ColumnsAreSame.ShouldBeFalse();      
      comparison.ResultSetSummary.ColumnDifferences.Count.ShouldEqual(1);
      comparison.ResultSetSummary.ColumnDifferences[0].Index.ShouldEqual(0);
      comparison.ResultSetSummary.ColumnDifferences[0].NameIsDifferent.ShouldBeTrue();
      comparison.ResultSetSummary.ColumnDifferences[0].TypeIsDifferent.ShouldBeTrue();
    }

    [Test]
    public void CanOutputWhenColumnsHaveDifferentNullability()
    {
      var comparison = CompareTablesWithTheseColumns(
        "IntColNullable int NULL", 
        "IntColNullable int NOT NULL"
      );
      
      comparison.ResultSetSummary.ColumnsAreSame.ShouldBeFalse();
      comparison.ResultSetSummary.ColumnDifferences.Count.ShouldEqual(1);
      comparison.ResultSetSummary.ColumnDifferences[0].NullabilityIsDifferent.ShouldBeTrue();
    }
    
    [Test]
    public void CanOutputWhenColumnsHaveDifferentSize()
    {
      var comparison = CompareTablesWithTheseColumns(
        "VarCharCol nvarchar(100) NULL", 
        "VarCharCol nvarchar(110) NULL"
      );
      
      comparison.ResultSetSummary.ColumnsAreSame.ShouldBeFalse();
      comparison.ResultSetSummary.ColumnDifferences.Count.ShouldEqual(1);
      comparison.ResultSetSummary.ColumnDifferences[0].SizeIsDifferent.ShouldBeTrue();
    }
    
    [Test]
    public void CanOutputWhenColumnsHaveDifferentSqlDataTypeButSameDotNetDataType()
    {
      var comparison = CompareTablesWithTheseColumns(
        "VarCharCol nvarchar(100)", 
        "VarCharCol varchar(100)"
      );
      
      comparison.ResultSetSummary.ColumnsAreSame.ShouldBeFalse();
      comparison.ResultSetSummary.ColumnDifferences.Count.ShouldEqual(1);
      comparison.ResultSetSummary.ColumnDifferences[0].TypeIsDifferent.ShouldBeTrue();
    }
    
    [Test]
    public void CanOutputWhenFirstQueryHasMoreColumns()
    {
      var comparison = CompareTablesWithTheseColumns(
        "VarCharCol varchar(100), IntCol int", 
        "VarCharCol varchar(100)"
      );
      
      comparison.ResultSetSummary.ResultsAreIdentical.ShouldBeFalse();
      comparison.ResultSetSummary.ColumnsAreSame.ShouldBeFalse();
      comparison.ResultSetSummary.ColumnCountMismatch.ShouldBeTrue();
      comparison.ResultSetSummary.ColumnDifferences.Count.ShouldEqual(0);
    }
    
    [Test]
    public void CanOutputWhenSecondQueryHasMoreColumns()
    {
      var comparison = CompareTablesWithTheseColumns(
        "VarCharCol varchar(100)", 
        "VarCharCol varchar(100), IntCol int"
      );
      
      comparison.ResultSetSummary.ResultsAreIdentical.ShouldBeFalse();
      comparison.ResultSetSummary.ColumnsAreSame.ShouldBeFalse();
      comparison.ResultSetSummary.ColumnCountMismatch.ShouldBeTrue();
      comparison.ResultSetSummary.ColumnDifferences.Count.ShouldEqual(0);
    }

    private ResultSetComparisonResult CompareTablesWithTheseColumns(string col1, string col2)
    {
      RunSql($"CREATE TABLE TestEntity1 ({col1})");
      RunSql($"CREATE TABLE TestEntity2 ({col2})");
      const string q1 = "SELECT * FROM TestEntity1";
      const string q2 = "SELECT * FROM TestEntity2";

      var comparison = RunComparison(q1, q2);
      return comparison;
    }

    #endregion

    #region Data Comparisons

    [Test]
    public void OutputsCellInformationForIncorrectData()
    {
      var comparison = RunComparison("SELECT 'test1' as 'Col1'", "SELECT 'test2' as 'Col1'");
      comparison.ResultSetSummary.DataDifferences.Count.ShouldEqual(1);
      var firstRowDifference = comparison.ResultSetSummary.DataDifferences[0];
      firstRowDifference.RowIndex.ShouldEqual(0);
      var firstRowCellDifferences = firstRowDifference.CellDifferences;
      firstRowCellDifferences.Count.ShouldEqual(1);
      firstRowCellDifferences[0].ColumnIndex.ShouldEqual(0);
    }
    
    [Test]
    public void OutputsCellInformationForIncorrectDataInNonZeroIndexes()
    {
      var comparison = RunComparison(
        "SELECT 'V1' as 'Col1', 'V2' as 'Col2' UNION ALL " +
        "SELECT 'V3' as 'Col1', 'V4' as 'Col2'", 
        "SELECT 'V1' as 'Col1', 'V2' as 'Col2' UNION ALL " +
        "SELECT 'V3' as 'Col1', 'WRONG' as 'Col2'");
      comparison.ResultSetSummary.DataDifferences.Count.ShouldEqual(1);
      var firstRowDifference = comparison.ResultSetSummary.DataDifferences[0];
      firstRowDifference.RowIndex.ShouldEqual(1);
      var firstRowCellDifferences = firstRowDifference.CellDifferences;
      firstRowCellDifferences.Count.ShouldEqual(1);
      firstRowCellDifferences[0].ColumnIndex.ShouldEqual(1);
    }

    [Test]
    public void OutputsMissingRowsWhenResultCountsMismatch()
    {
      var comparison = RunComparison(
        "SELECT 'V1' as 'Col1' UNION ALL SELECT 'V2'",
        "SELECT 'V1' as 'Col1'"
      );
      comparison.ResultSetSummary.ResultsAreIdentical.ShouldBeFalse();
      comparison.ResultSetSummary.RowCountMismatch.ShouldBeTrue();
      comparison.ResultSetSummary.DataDifferences.Count.ShouldEqual(0);
    }
    
    [Test]
    public void WhenRowsSameFlagsAreFalse()
    {
      var comparison = RunComparison(
        "SELECT 'V1'",
        "SELECT 'V1'"
      );
      comparison.ResultSetSummary.ResultsAreIdentical.ShouldBeTrue();
      comparison.ResultSetSummary.RowCountMismatch.ShouldBeFalse();
      comparison.ResultSetSummary.DataDifferences.Count.ShouldEqual(0);
    }
    
    // TODO: Primary key matching
    
    #endregion

    #region Parameterisation

    [Test]
    public void CanParameterizeAQuery()
    {
      var comparison = _comparator.CompareQueryOutputs(new ComparisonInput(
        "SELECT @param1", 
        "SELECT @param1 + '2'",
        new List<ComparisonParameter>
      {
        new ComparisonParameter("param1", "Test1")
      })).ResultSetComparisons[0];
      
      comparison.ResultSetSummary.ResultsAreIdentical.ShouldBeFalse();
      comparison.Output1.Rows[0].Values[0].ShouldEqual("Test1");
      comparison.Output2.Rows[0].Values[0].ShouldEqual("Test12");
    }

    #endregion

    #region Multiple result sets

    [Test]
    public void CanFailOnSecondResultSet()
    {
      var comparison = _comparator.CompareQueryOutputs(new ComparisonInput(
        "SELECT 'test'; SELECT 'test'", 
        "SELECT 'test'; SELECT 'test2'"
        )
      );
      
      comparison.ResultsAreIdentical.ShouldBeFalse();
      comparison.ResultSetComparisons.Count.ShouldEqual(2);
      comparison.ResultSetComparisons[0].ResultSetSummary.ResultsAreIdentical.ShouldBeTrue();
      comparison.ResultSetComparisons[1].ResultSetSummary.ResultsAreIdentical.ShouldBeFalse();
      comparison.ResultSetComparisons[1].Output2.Rows[0].Values[0].ShouldEqual("test2");
    }

    [Test]
    public void CanCompareMultipleIdenticalResultSets()
    {
      var comparison = _comparator.CompareQueryOutputs(new ComparisonInput(
          "SELECT 'test'; SELECT 'test'", 
          "SELECT 'test'; SELECT 'test'"
        )
      );
      
      comparison.ResultsAreIdentical.ShouldBeTrue();
      comparison.ResultSetComparisons.Count.ShouldEqual(2);
      comparison.ResultSetComparisons[0].ResultSetSummary.ResultsAreIdentical.ShouldBeTrue();
      comparison.ResultSetComparisons[1].ResultSetSummary.ResultsAreIdentical.ShouldBeTrue();
      comparison.ResultSetCountsAreNotSame.ShouldBeFalse();
    }

    [Test]
    public void OneQueryProducesMoreResultSetsThanTheOther()
    {
      var comparison = _comparator.CompareQueryOutputs(new ComparisonInput(
          "SELECT 'test'; SELECT 'test'", 
          "SELECT 'test';"
        )
      );
      
      comparison.ResultsAreIdentical.ShouldBeFalse();
      comparison.ResultSetComparisons.Count.ShouldEqual(1);
      comparison.ResultSetCountsAreNotSame.ShouldBeTrue();
    }
    
    #endregion
    
    // TODO: Precision
    private bool AreSame(string q1, string q2)
    {
      var comparisonSummary = RunComparison(q1, q2).ResultSetSummary;
      return comparisonSummary.ResultsAreIdentical;
    }

    private ResultSetComparisonResult RunComparison(string q1, string q2)
    {
      return _comparator.CompareQueryOutputs(new ComparisonInput(q1, q2)).ResultSetComparisons[0];
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

    public object CreateCommandParameter(string name, object value)
    {
      return new SqlParameter(name, value);
    }

    public IDbConnection GetConnection1()
    {
      return _connection;
    }

    public IDbConnection GetConnection2()
    {
      return _connection;
    }
  }
}