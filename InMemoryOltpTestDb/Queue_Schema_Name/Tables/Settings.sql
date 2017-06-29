CREATE TABLE [Queue_Schema_Name].[Settings] (
    [ID]          BIGINT NOT NULL,
    [MinNum]      INT    NOT NULL,
    [TresholdNum] INT    NOT NULL,
    PRIMARY KEY NONCLUSTERED HASH ([ID]) WITH (BUCKET_COUNT = 1)
)
WITH (MEMORY_OPTIMIZED = ON);

