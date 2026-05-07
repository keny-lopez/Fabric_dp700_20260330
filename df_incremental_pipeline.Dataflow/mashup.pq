[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "minciencia_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared minciencia = let
  Origen = Lakehouse.Contents([HierarchicalNavigation = null, EnableVorder = true, OutputMetadataRefresh = true]),
  #"Navegación 1" = Origen{[workspaceId = "f2f1a2a4-28bd-4388-aea2-989b8d2479ff"]}[Data],
  #"Navegación 2" = #"Navegación 1"{[lakehouseId = "f2f5ef99-fa5e-4802-b052-60d3674c2af2"]}[Data],
  #"Navegación 3" = #"Navegación 2"{[Id = "minciencia", ItemKind = "Table"]}[Data],
  #"Columnas quitadas" = Table.RemoveColumns(#"Navegación 3", {"latitud", "longitud", "nombreEstacion"}),
  #"Filas filtradas 1" = Table.SelectRows(#"Columnas quitadas", each [time] > RangeStart and [time] <= RangeEnd)
in
  #"Filas filtradas 1";
shared RangeStart = null meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type datetime];
shared RangeEnd = null meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type datetime];
shared minciencia_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = null, EnableVorder = true, OutputMetadataRefresh = true]),
  Navigation_1 = Pattern{[workspaceId = "f2f1a2a4-28bd-4388-aea2-989b8d2479ff"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "c3abadd9-077d-48dd-a313-40970c5853b4"]}[Data],
  TableNavigation = Navigation_2{[Id = "minciencia_incremental_pipeline", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
