CREATE TABLE [dbo].[FactSalesOrder] (

	[SalesOrderKey] int NOT NULL, 
	[SalesOrderDateKey] int NOT NULL, 
	[ProductKey] int NOT NULL, 
	[CustomerKey] int NOT NULL, 
	[Quantity] int NULL, 
	[SalesTotal] decimal(18,0) NULL
);


GO
ALTER TABLE [dbo].[FactSalesOrder] ADD CONSTRAINT FK_d0fdd31c_fa9e_467c_b95f_e99d90569071 FOREIGN KEY ([CustomerKey]) REFERENCES [dbo].[DimCustomer]([CustomerKey]);
GO
ALTER TABLE [dbo].[FactSalesOrder] ADD CONSTRAINT FK_eae912e1_e1bc_4f13_95f5_95cc8a685f37 FOREIGN KEY ([SalesOrderDateKey]) REFERENCES [dbo].[DimDate]([DateKey]);