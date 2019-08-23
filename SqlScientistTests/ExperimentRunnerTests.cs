using System.Collections.Generic;
using NSubstitute;
using NUnit.Framework;
using Should;
using SqlScientist;
using SqlScientist.Parameterization;

namespace SqlScientistTests
{
  public class ExperimentRunnerTests
  {
    private ISqlComparator _comparitorMock;
    private ExperimentRunner _experimentRunner;

    [SetUp]
    public void SetUp()
    {
      _comparitorMock = Substitute.For<ISqlComparator>();
      _experimentRunner = new ExperimentRunner(_comparitorMock);
    }
    
    [Test]
    public void CanRunExperimentSynchronouslyWithEmptyParameterSets()
    {
      var queryResult = new QueryComparison();
      _comparitorMock.CompareQueryOutputs(new ComparisonInput("q1", "q2").Equivalent())
        .Returns(queryResult);
      
      var results = _experimentRunner.RunExperiment("q1", "q2", new List<ComparisonParameterCollection>
      {
        new ComparisonParameterCollection()
      });
      
      results.QueryComparisons.Count.ShouldEqual(1);
      results.QueryComparisons[0].ShouldEqual(queryResult);
    }

    [Test]
    public void CanRunExperimentSynchronouslyWithMultipleDataSets()
    {
      var queryResult1 = new QueryComparison();
      var queryResult2 = new QueryComparison();
      
      var parameterSet1 = new List<ComparisonParameter>{ new ComparisonParameter("p1", "1") };
      var parameterSet2 = new List<ComparisonParameter>{ new ComparisonParameter("p1", "2") };

      _comparitorMock.CompareQueryOutputs(new ComparisonInput("q1", "q2", parameterSet1).Equivalent())
        .Returns(queryResult1);
      
      _comparitorMock.CompareQueryOutputs(new ComparisonInput("q1", "q2", parameterSet2).Equivalent())
        .Returns(queryResult2);
      
      var results = _experimentRunner.RunExperiment("q1", "q2", new List<ComparisonParameterCollection>
      {
        new ComparisonParameterCollection(parameterSet1),
        new ComparisonParameterCollection(parameterSet2)
      });
      
      results.QueryComparisons.Count.ShouldEqual(2);
      results.QueryComparisons[0].ShouldEqual(queryResult1);
      results.QueryComparisons[1].ShouldEqual(queryResult2);
    }
  }

}