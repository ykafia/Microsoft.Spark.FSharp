namespace Microsoft.Spark.FSharp
open Microsoft.Spark.Sql

module Column =
    let Col (name:string) = 
        Functions.Col(name)
    let Split (pattern : string) (column: Column) : Column =
        Functions.Split(column,pattern)
    let Alias (name:string) (col:Column) =
        col.Alias(name)
    let SplitAlias (pattern : string) (name:string) (nameCol:string) =
        Split pattern (Col nameCol)
            |> Alias name

module DF = 
    let Select (col:list<Column>) (df:DataFrame) : DataFrame= 
        df.Select(Array.ofList col)