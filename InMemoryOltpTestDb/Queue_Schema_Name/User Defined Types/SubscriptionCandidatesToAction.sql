CREATE TYPE [Queue_Schema_Name].[SubscriptionCandidatesToAction] AS TABLE (
    [ID]                     INT    NOT NULL,
    [NextToCompletedID]      BIGINT NOT NULL,
    [MaxIdleIntervalSeconds] INT    NOT NULL,
    [ActionOnLimitExceeding] INT    NOT NULL,
    PRIMARY KEY NONCLUSTERED HASH ([ID]) WITH (BUCKET_COUNT = 256))
    WITH (MEMORY_OPTIMIZED = ON);

