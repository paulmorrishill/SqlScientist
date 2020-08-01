using NSubstitute.Core;
using NSubstitute.Core.Arguments;

namespace SqlScientistTests
{
  public static class NSubstituteExtensions
  {
    public static T Equivalent<T>(this object obj)
    {
      SubstitutionContext.Current.ThreadContext.EnqueueArgumentSpecification(new ArgumentSpecification(typeof(T), new EquivalentArgumentMatcher<T>(obj)));
      return default(T);
    }

    public static T Equivalent<T>(this T obj)
    {
      return Equivalent<T>((object)obj);
    }
  }
}