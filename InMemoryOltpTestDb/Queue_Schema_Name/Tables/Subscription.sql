CREATE TABLE [Queue_Schema_Name].[Subscription] (
    [ID]                     INT              IDENTITY (1, 1) NOT NULL,
    [Name]                   NVARCHAR (255)   NOT NULL,
    [LastCompletedID]        BIGINT           NOT NULL,
    [LastCompletedTime]      DATETIME2 (7)    NOT NULL,
    [LockTime]               DATETIME2 (7)    NULL,
    [LockToken]              UNIQUEIDENTIFIER NULL,
    [Disabled]               BIT              NOT NULL,
    [MaxIdleIntervalSeconds] INT              NULL,
    [MaxUncompletedMessages] INT              NULL,
    [ActionOnLimitExceeding] INT              NULL,
    CONSTRAINT [PK_Subscription] PRIMARY KEY NONCLUSTERED HASH ([ID]) WITH (BUCKET_COUNT = 256),
    CONSTRAINT [IX_Subscription_Name] UNIQUE NONCLUSTERED HASH ([Name]) WITH (BUCKET_COUNT = 256)
)
WITH (MEMORY_OPTIMIZED = ON);



