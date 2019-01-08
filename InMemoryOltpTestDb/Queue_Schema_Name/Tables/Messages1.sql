CREATE TABLE [Queue_Schema_Name].[Messages1] (
    [ID]      BIGINT           NOT NULL,
    [Created] DATETIME2 (7)    NOT NULL,
    [Body]    VARBINARY (8000) NOT NULL,
    PRIMARY KEY NONCLUSTERED ([ID] ASC)
)
WITH (MEMORY_OPTIMIZED = ON);



