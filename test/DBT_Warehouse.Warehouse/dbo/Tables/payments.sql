CREATE TABLE [dbo].[payments] (

	[ID] int NULL, 
	[ORDERID] int NULL, 
	[PAYMENTMETHOD] varchar(8000) NULL, 
	[STATUS] varchar(8000) NULL, 
	[AMOUNT] int NULL, 
	[CREATED] date NULL
);