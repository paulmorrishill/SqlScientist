namespace SqlScientist
{
  public class ComparisonParameter
  {
    public string Name { get; set; }
    public object Value { get; set; }

    public ComparisonParameter(string name, object value)
    {
      Value = value;
      Name = name;
    }
  }
}