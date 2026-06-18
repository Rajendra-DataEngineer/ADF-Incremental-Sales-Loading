-- Watermark table for incremental load tracking (run once in Azure SQL)
CREATE TABLE dbo.WatermarkTable (
    TableName       VARCHAR(100) NOT NULL PRIMARY KEY,
    WatermarkValue  DATETIME     NOT NULL,
    LastUpdated     DATETIME     NOT NULL DEFAULT GETUTCDATE()
);

-- Target table for sales incremental load
CREATE TABLE dbo.Sales_Target (
    SaleID            INT            NOT NULL PRIMARY KEY,
    Product           VARCHAR(100)   NOT NULL,
    Amount            DECIMAL(10, 2) NOT NULL,
    LastModifiedDate  DATETIME       NOT NULL
);

-- Stored procedure: update watermark after successful load
CREATE OR ALTER PROCEDURE dbo.UpdateWatermarkValue
    @Tablename        VARCHAR(100),
    @Lastmodifieddate DATETIME
AS
BEGIN
    SET NOCOUNT ON;

    MERGE dbo.WatermarkTable AS target
    USING (SELECT @Tablename AS TableName, @Lastmodifieddate AS WatermarkValue) AS source
    ON target.TableName = source.TableName
    WHEN MATCHED THEN
        UPDATE SET
            WatermarkValue = source.WatermarkValue,
            LastUpdated    = GETUTCDATE()
    WHEN NOT MATCHED THEN
        INSERT (TableName, WatermarkValue, LastUpdated)
        VALUES (source.TableName, source.WatermarkValue, GETUTCDATE());
END;
GO

-- Stored procedure: MERGE incremental sales (SQL equivalent of PySpark Delta MERGE)
CREATE OR ALTER PROCEDURE dbo.usp_MergeSalesIncremental
    @WatermarkValue DATETIME
AS
BEGIN
    SET NOCOUNT ON;

    MERGE dbo.Sales_Target AS target
    USING (
        SELECT
            SaleID,
            Product,
            Amount,
            LastModifiedDate
        FROM dbo.Sales_Staging
        WHERE LastModifiedDate > @WatermarkValue
    ) AS source
    ON target.SaleID = source.SaleID
    WHEN MATCHED AND source.LastModifiedDate > target.LastModifiedDate THEN
        UPDATE SET
            Product          = source.Product,
            Amount           = source.Amount,
            LastModifiedDate = source.LastModifiedDate
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (SaleID, Product, Amount, LastModifiedDate)
        VALUES (source.SaleID, source.Product, source.Amount, source.LastModifiedDate);
END;
GO
