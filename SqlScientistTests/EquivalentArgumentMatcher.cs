using System;
using FluentAssertions;
using NSubstitute.Core;
using NSubstitute.Core.Arguments;

namespace SqlScientistTests
{
  public class EquivalentArgumentMatcher<T> : IArgumentMatcher, IDescribeNonMatches
  {
    private static readonly ArgumentFormatter DefaultArgumentFormatter = new ArgumentFormatter();
    private readonly object _expected;

    public EquivalentArgumentMatcher(object expected)
    {
      _expected = expected;
    }

    public override string ToString()
    {
      return DefaultArgumentFormatter.Format(_expected, false);
    }

    public string DescribeFor(object argument)
    {
      try
      {
        ((T)argument).Should().BeEquivalentTo(argument);
        return string.Empty;
      }
      catch (Exception ex)
      {
        return ex.Message;
      }
    }

    public bool IsSatisfiedBy(object argument)
    {
      try
      {
        ((T)argument).Should()
          .BeEquivalentTo(_expected, options => options.IncludingAllDeclaredProperties());
        return true;
      }
      catch
      {
        return false;
      }
    }
  }
}