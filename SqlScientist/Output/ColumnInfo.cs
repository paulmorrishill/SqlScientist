using System;

namespace SqlScientist
{
  public class ColumnInfo
  {
    public string Name { get; set; }
    public Type DataType { get; set; }
    public string SqlDataType { get; set; }
    public bool IsNullable { get; set; }
    public int Size { get; set; }
  }
}