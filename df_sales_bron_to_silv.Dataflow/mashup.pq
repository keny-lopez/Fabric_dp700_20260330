[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "ft_sales_dia_orden_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared ft_sales_dia_orden = let
  Origen = Lakehouse.Contents(null),
  Navegación = Origen{[workspaceId = "f2f1a2a4-28bd-4388-aea2-989b8d2479ff"]}[Data],
  #"Navegación 1" = Navegación{[lakehouseId = "f2f5ef99-fa5e-4802-b052-60d3674c2af2"]}[Data],
  #"Navegación 2" = #"Navegación 1"{[Id = "sales_salesorderheadersalesreason", ItemKind = "Table"]}[Data],
  #"Columnas con nombre cambiado" = Table.RenameColumns(#"Navegación 2", {{"modifieddate", "sale_date"}})
in
  #"Columnas con nombre cambiado";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "dim_salesreason_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared dim_salesreason = let
  Origen = Lakehouse.Contents(null),
  Navegación = Origen{[workspaceId = "f2f1a2a4-28bd-4388-aea2-989b8d2479ff"]}[Data],
  #"Navegación 1" = Navegación{[lakehouseId = "f2f5ef99-fa5e-4802-b052-60d3674c2af2"]}[Data],
  #"Navegación 2" = #"Navegación 1"{[Id = "sales_salesreason", ItemKind = "Table"]}[Data],
  #"Columnas quitadas" = Table.RemoveColumns(#"Navegación 2", {"modifieddate"})
in
  #"Columnas quitadas";
shared sales_shoppingcartitem = let
  Origen = Lakehouse.Contents(null),
  Navegación = Origen{[workspaceId = "f2f1a2a4-28bd-4388-aea2-989b8d2479ff"]}[Data],
  #"Navegación 1" = Navegación{[lakehouseId = "f2f5ef99-fa5e-4802-b052-60d3674c2af2"]}[Data],
  #"Navegación 2" = #"Navegación 1"{[Id = "sales_shoppingcartitem", ItemKind = "Table"]}[Data]
in
  #"Navegación 2";
shared sales_countryregioncurrency = let
  Origen = Lakehouse.Contents(null),
  Navegación = Origen{[workspaceId = "f2f1a2a4-28bd-4388-aea2-989b8d2479ff"]}[Data],
  #"Navegación 1" = Navegación{[lakehouseId = "f2f5ef99-fa5e-4802-b052-60d3674c2af2"]}[Data],
  #"Navegación 2" = #"Navegación 1"{[Id = "sales_countryregioncurrency", ItemKind = "Table"]}[Data],
  #"Columnas quitadas" = Table.RemoveColumns(#"Navegación 2", {"modifieddate"})
in
  #"Columnas quitadas";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "dim_tarjeta_credito_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared dim_tarjeta_credito = let
  Origen = Lakehouse.Contents(null),
  Navegación = Origen{[workspaceId = "f2f1a2a4-28bd-4388-aea2-989b8d2479ff"]}[Data],
  #"Navegación 1" = Navegación{[lakehouseId = "f2f5ef99-fa5e-4802-b052-60d3674c2af2"]}[Data],
  #"Navegación 2" = #"Navegación 1"{[Id = "sales_creditcard", ItemKind = "Table"]}[Data],
  #"Columnas quitadas" = Table.RemoveColumns(#"Navegación 2", {"modifieddate"})
in
  #"Columnas quitadas";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "dim_monedas_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared dim_monedas = let
  Origen = Lakehouse.Contents(null),
  Navegación = Origen{[workspaceId = "f2f1a2a4-28bd-4388-aea2-989b8d2479ff"]}[Data],
  #"Navegación 1" = Navegación{[lakehouseId = "f2f5ef99-fa5e-4802-b052-60d3674c2af2"]}[Data],
  #"Navegación 2" = #"Navegación 1"{[Id = "sales_currency", ItemKind = "Table"]}[Data],
  #"Columnas quitadas" = Table.RemoveColumns(#"Navegación 2", {"modifieddate"}),
  #"Consultas combinadas" = Table.NestedJoin(#"Columnas quitadas", {"currencycode"}, sales_countryregioncurrency, {"currencycode"}, "sales_countryregioncurrency", JoinKind.LeftOuter),
  #"sales_countryregioncurrency expandido" = Table.ExpandTableColumn(#"Consultas combinadas", "sales_countryregioncurrency", {"countryregioncode"}, {"countryregioncode"})
in
  #"sales_countryregioncurrency expandido";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "ft_tipo_cambio_dia_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared ft_tipo_cambio_dia = let
  Origen = Lakehouse.Contents(null),
  Navegación = Origen{[workspaceId = "f2f1a2a4-28bd-4388-aea2-989b8d2479ff"]}[Data],
  #"Navegación 1" = Navegación{[lakehouseId = "f2f5ef99-fa5e-4802-b052-60d3674c2af2"]}[Data],
  #"Navegación 2" = #"Navegación 1"{[Id = "sales_currencyrate", ItemKind = "Table"]}[Data],
  #"Columnas quitadas" = Table.RemoveColumns(#"Navegación 2", {"_ingestion_ts"})
in
  #"Columnas quitadas";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "dim_cliente_tcredito_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared dim_cliente_tcredito = let
  Origen = Lakehouse.Contents(null),
  Navegación = Origen{[workspaceId = "f2f1a2a4-28bd-4388-aea2-989b8d2479ff"]}[Data],
  #"Navegación 1" = Navegación{[lakehouseId = "f2f5ef99-fa5e-4802-b052-60d3674c2af2"]}[Data],
  #"Navegación 2" = #"Navegación 1"{[Id = "sales_personcreditcard", ItemKind = "Table"]}[Data],
  #"Columnas quitadas" = Table.RemoveColumns(#"Navegación 2", {"modifieddate"}),
  #"Duplicados quitados" = Table.Distinct(#"Columnas quitadas", {"businessentityid"})
in
  #"Duplicados quitados";
shared sales_customer = let
  Origen = Lakehouse.Contents(null),
  Navegación = Origen{[workspaceId = "f2f1a2a4-28bd-4388-aea2-989b8d2479ff"]}[Data],
  #"Navegación 1" = Navegación{[lakehouseId = "f2f5ef99-fa5e-4802-b052-60d3674c2af2"]}[Data],
  #"Navegación 2" = #"Navegación 1"{[Id = "sales_customer", ItemKind = "Table"]}[Data],
  #"Columnas quitadas" = Table.RemoveColumns(#"Navegación 2", {"modifieddate"})
in
  #"Columnas quitadas";
shared sales_specialoffer = let
  Origen = Lakehouse.Contents(null),
  Navegación = Origen{[workspaceId = "f2f1a2a4-28bd-4388-aea2-989b8d2479ff"]}[Data],
  #"Navegación 1" = Navegación{[lakehouseId = "f2f5ef99-fa5e-4802-b052-60d3674c2af2"]}[Data],
  #"Navegación 2" = #"Navegación 1"{[Id = "sales_specialoffer", ItemKind = "Table"]}[Data]
in
  #"Navegación 2";
shared ft_sales_dia_orden_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "f2f1a2a4-28bd-4388-aea2-989b8d2479ff"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "c3abadd9-077d-48dd-a313-40970c5853b4"]}[Data],
  TableNavigation = Navigation_2{[Id = "ft_sales_dia_orden", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared dim_salesreason_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = null]),
  Navigation_1 = Pattern{[workspaceId = "f2f1a2a4-28bd-4388-aea2-989b8d2479ff"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "c3abadd9-077d-48dd-a313-40970c5853b4"]}[Data],
  TableNavigation = Navigation_2{[Id = "dim_salesreason", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared dim_tarjeta_credito_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = null]),
  Navigation_1 = Pattern{[workspaceId = "f2f1a2a4-28bd-4388-aea2-989b8d2479ff"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "c3abadd9-077d-48dd-a313-40970c5853b4"]}[Data],
  TableNavigation = Navigation_2{[Id = "dim_tarjeta_credito", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared dim_monedas_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = null]),
  Navigation_1 = Pattern{[workspaceId = "f2f1a2a4-28bd-4388-aea2-989b8d2479ff"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "c3abadd9-077d-48dd-a313-40970c5853b4"]}[Data],
  TableNavigation = Navigation_2{[Id = "dim_monedas", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared ft_tipo_cambio_dia_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = null]),
  Navigation_1 = Pattern{[workspaceId = "f2f1a2a4-28bd-4388-aea2-989b8d2479ff"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "c3abadd9-077d-48dd-a313-40970c5853b4"]}[Data],
  TableNavigation = Navigation_2{[Id = "ft_tipo_cambio_dia", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared dim_cliente_tcredito_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = null]),
  Navigation_1 = Pattern{[workspaceId = "f2f1a2a4-28bd-4388-aea2-989b8d2479ff"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "c3abadd9-077d-48dd-a313-40970c5853b4"]}[Data],
  TableNavigation = Navigation_2{[Id = "dim_cliente_tcredito", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
