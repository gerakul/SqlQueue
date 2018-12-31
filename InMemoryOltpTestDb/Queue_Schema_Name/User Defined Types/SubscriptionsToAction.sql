CREATE TYPE [Queue_Schema_Name].[SubscriptionsToAction] AS TABLE (
    [RowID]                  INT IDENTITY (1, 1) NOT NULL,
    [ID]                     INT NOT NULL,
    [ActionOnLimitExceeding] INT NOT NULL,
    PRIMARY KEY NONCLUSTERED HASH ([RowID]) WITH (BUCKET_COUNT = 256))
    WITH (MEMORY_OPTIMIZED = ON);

