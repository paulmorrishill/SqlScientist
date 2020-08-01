using System;
using System.Collections.Generic;
using SqlScientist.Parameterization;

namespace SqlScientist
{
  public interface IExperimentRunner
  {
    ExperimentResult RunExperiment(string query1, string query2, List<ComparisonParameterCollection> parameterCollections);
  }

  public class ExperimentRunner : IExperimentRunner
  {
    private ISqlComparator _comparator;

    public ExperimentRunner(ISqlComparator comparator)
    {
      _comparator = comparator;
    }

    public ExperimentResult RunExperiment(string query1, string query2, List<ComparisonParameterCollection> parameterCollections)
    {
      var queryOutputs = new List<QueryComparison>();

      foreach (var parameterCollection in parameterCollections)
      {
        var output = _comparator.CompareQueryOutputs(new ComparisonInput(query1, query2, parameterCollection.Parameters));
        queryOutputs.Add(output);
      }

      return new ExperimentResult
      {
        QueryComparisons = queryOutputs
      };  
    }
  }

  public class ExperimentResult
  {
    public List<QueryComparison> QueryComparisons { get; set; }
  }
}