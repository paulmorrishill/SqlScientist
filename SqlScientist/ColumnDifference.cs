namespace SqlScientist
{
  public class ColumnDifference
  {
    public int Index { get; set; }
    public bool NameIsDifferent { get; set; }
    public bool TypeIsDifferent { get; set; }
    public bool NullabilityIsDifferent { get; set; }
    public bool SizeIsDifferent { get; set; }
  }
}