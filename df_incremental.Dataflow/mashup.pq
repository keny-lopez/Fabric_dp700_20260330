[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[IncrementalRefreshSettings = [FilterColumnName = "time", WindowSize = 4, WindowUnit = "Year", BucketUnit = "Year", BookmarkColumnName = "time", ConsiderOnlyConcludedPeriods = true, RequireIncrementalRefreshQueryToFold = false], DataDestinations = {[Definition = [Kind = "Reference", QueryName = "minciencia_DataDestination", IsNewTarget = false], Settings = [Kind = "Manual", AllowCreation = false, ColumnSettings = [Mappings = {[SourceColumnName = "time", DestinationColumnName = "time"], [SourceColumnName = "dd_Valor", DestinationColumnName = "dd_Valor"], [SourceColumnName = "ff_Valor", DestinationColumnName = "ff_Valor"], [SourceColumnName = "VRB_Valor", DestinationColumnName = "VRB_Valor"], [SourceColumnName = "CodigoNacional", DestinationColumnName = "CodigoNacional"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared minciencia = let
  Origen = Lakehouse.Contents([HierarchicalNavigation = null, EnableVorder = true, OutputMetadataRefresh = true]),
  #"Navegación 1" = Origen{[workspaceId = "f2f1a2a4-28bd-4388-aea2-989b8d2479ff"]}[Data],
  #"Navegación 2" = #"Navegación 1"{[lakehouseId = "f2f5ef99-fa5e-4802-b052-60d3674c2af2"]}[Data],
  #"Navegación 3" = #"Navegación 2"{[Id = "minciencia", ItemKind = "Table"]}[Data],
  #"Columnas quitadas" = Table.RemoveColumns(#"Navegación 3", {"latitud", "longitud", "nombreEstacion"}),
  #"Filas filtradas" = Table.SelectRows(#"Columnas quitadas", each [time] >= RangeStart and [time] <= RangeEnd)
in
  #"Filas filtradas";
shared RangeStart = null meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type datetime];
shared RangeEnd = null meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type datetime];
shared minciencia_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = null, EnableVorder = true, OutputMetadataRefresh = true]),
  Navigation_1 = Pattern{[workspaceId = "f2f1a2a4-28bd-4388-aea2-989b8d2479ff"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "c3abadd9-077d-48dd-a313-40970c5853b4"]}[Data],
  TableNavigation = Navigation_2{[Id = "minciencia_incremental", ItemKind = "Table"]}[Data]
in
  TableNavigation;
