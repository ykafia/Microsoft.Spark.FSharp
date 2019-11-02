namespace Microsoft.Spark.FSharp
open Microsoft.Spark.Sql

module SparkCol =
    let Col (name:string)  = 
        Functions.Col(name)
    let Split (pattern : string) (column: Column) : Column =
        Functions.Split(column,pattern)
    let Alias (name:string) (col:Column) =
        col.Alias(name)
    let SplitAlias (pattern : string) (name:string) (nameCol:string) =
        Split pattern (Col nameCol)
            |> Alias name
    let Explode (nameCol : string)  =
        Col nameCol
            |> Functions.Explode
    

module SparkDF = 
    let Alias (name:string) (df:DataFrame)=
        name |>
            df.Alias
    let As (name:string) (df:DataFrame) =
        name |> df.As

    let Checkpoint (eager:bool) (df:DataFrame) : DataFrame = 
        df.Checkpoint eager

    let Coalesce (number:int) (df:DataFrame) =
        number |> df.Coalesce

    let Select (col:list<Column>) (df:DataFrame) : DataFrame= 
        df.Select(Array.ofList col)
    

    // let x (df:DataFrame) = 
    //     df.Col
    //     0
module SparkFunc = 
    let Col (name:string) = Functions.Col name
    let CollectList (col:Column) = Functions.CollectList col
    let CollectListByName (colName:string) = 
        Col colName 
            |> CollectList
    let CollectSet (col:Column) = Functions.CollectSet col
    let CollectSetByName (colName:string) = 
        Col colName 
            |> CollectSet
    let Column = Col

    // let x = Functions.
    