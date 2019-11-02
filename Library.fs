namespace Microsoft.Spark.FSharp

open Microsoft.Spark.Sql

module SparkCol =
    let Col(name: string) = Functions.Col(name)
    let Split (pattern: string) (column: Column): Column = Functions.Split(column, pattern)
    let Alias (name: string) (col: Column) = col.Alias(name)
    let SplitAlias (pattern: string) (name: string) (nameCol: string) = Split pattern (Col nameCol) |> Alias name
    let Explode(nameCol: string) = Col nameCol |> Functions.Explode


module SparkDF =
    let Alias (name: string) (df: DataFrame) = name |> df.Alias
    let As (name: string) (df: DataFrame) = name |> df.As

    let Checkpoint (eager: bool) (df: DataFrame): DataFrame = df.Checkpoint eager

    let Coalesce (number: int) (df: DataFrame) = number |> df.Coalesce

    let Select (col: list<Column>) (df: DataFrame): DataFrame = df.Select(Array.ofList col)


// let x (df:DataFrame) =
//     df.Col
//     0
module SparkFunc =
    let Col(name: string) = Functions.Col name
    let CollectList(col: Column) = Functions.CollectList col
    let CollectListByName(colName: string) = Col colName |> CollectList
    let CollectSet(col: Column) = Functions.CollectSet col
    let CollectSetByName(colName: string) = Col colName |> CollectSet
    let Column = Col
    let Concat(cols: list<Column>) = Functions.Concat(Array.ofList cols)
    let ConcatWs (sep: string) (cols: list<Column>) = Functions.ConcatWs(sep, Array.ofList cols)
    let Conv (fromBase: int) (toBase: int) (col: Column) = Functions.Conv(col, fromBase, toBase)
    let ConvByName (fromBase: int) (toBase: int) (col: string) = Functions.Conv(Col col, fromBase, toBase)
    let Corr (col1: Column) (col2: Column) = Functions.Corr(col1, col2)
    let Cos(col: Column) = Functions.Cos col
    let Cosh(col: Column) = Functions.Cosh col
    let Count(colName: string) = Functions.Count colName

    let CountDistinct(cols: list<Column>) =
        if List.length cols <= 1 then Functions.CountDistinct cols.[0]
        else Functions.CountDistinct(cols.[0], Array.ofList cols.[1..])

    let CovarPop (col1: Column) (col2: Column) = Functions.CovarPop(col1, col2)
    
    let CovarSamp (col1: Column) (col2: Column) = Functions.CovarSamp(col1, col2)

    let Crc32 (col:Column) = Functions.Crc32 col

    let CumeDist = Functions.CumeDist
    let CurrentDate = Functions.CurrentDate
    let CurrentRow = Functions.CurrentRow
    let CurrentTimestamp = Functions.CurrentTimestamp
    let DateAdd (start:Column) (days:int) =  Functions.DateAdd(start,days)
    let DateDiff (col1:Column) (col2:Column) = Functions.DateDiff(col1,col2)
    let DateFormat (col:Column) (format:string)  = Functions.DateFormat(col,format)
    let DateSub (col:Column) (days:int) = Functions.DateSub(col,days)
    let DateTrunc (col:Column) (format:string) = Functions.DateTrunc(format,col)
    let DayOfMonth (col:Column) = Functions.DayOfMonth col
    let DayOfWeek (col:Column) = Functions.DayOfWeek col
    let DayOfYear (col:Column) = Functions.DayOfYear col
    let Decode (col:Column) (charset:string) = Functions.Decode(col,charset)
    let Degrees (col:Column) = Functions.Degrees col
    let DenseRank = Functions.DenseRank
    let Desc (columnName:string) = Functions.Desc columnName
    let DescNullsFirst (columnName:string) = Functions.DescNullsFirst columnName
    let DescNullsLast (columnName:string) = Functions.DescNullsLast columnName
    let ElementAt (column:Column) (value:obj)= Functions.ElementAt(column,value)
    let Encode (col:Column) (charset:string) = Functions.Encode(col,charset)
    let Exp (col:Column) = Functions.Exp col
    let Explode (col:Column) = Functions.Explode col
    let ExplodeOuter (col:Column) = Functions.ExplodeOuter col