CREATE TABLE [Queue_Schema_Name].[State] (
    [ID]            BIGINT        NOT NULL,
    [Modified]      DATETIME2 (7) NOT NULL,
    [MinID1]        BIGINT        NOT NULL,
    [MaxID1]        BIGINT        NOT NULL,
    [Num1]          INT           NOT NULL,
    [NeedClean1]    BIT           NOT NULL,
    [MinID2]        BIGINT        NOT NULL,
    [MaxID2]        BIGINT        NOT NULL,
    [Num2]          INT           NOT NULL,
    [NeedClean2]    BIT           NOT NULL,
    [IsFirstActive] BIT           NOT NULL,
    [MinNum]        INT           NOT NULL,
    [TresholdNum]   INT           NOT NULL,
    PRIMARY KEY NONCLUSTERED HASH ([ID]) WITH (BUCKET_COUNT = 1)
)
WITH (DURABILITY = SCHEMA_ONLY, MEMORY_OPTIMIZED = ON);

