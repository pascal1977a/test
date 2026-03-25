CREATE TABLE [dbo].[DimProduct] (

	[ProductKey] int NOT NULL, 
	[ProductAltKey] varchar(25) NULL, 
	[ProductName] varchar(50) NOT NULL, 
	[Category] varchar(50) NULL, 
	[ListPrice] decimal(18,0) NULL
);


GO
ALTER TABLE [dbo].[DimProduct] ADD CONSTRAINT UQ_f100b53f_2d49_461a_9ce9_b5e3d33094d8 unique NONCLUSTERED ([ProductAltKey]);