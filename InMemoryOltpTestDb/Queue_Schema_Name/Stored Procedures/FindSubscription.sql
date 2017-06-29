



CREATE PROCEDURE [Queue_Schema_Name].[FindSubscription]
  @name nvarchar(255)
  WITH NATIVE_COMPILATION, SCHEMABINDING, EXECUTE AS OWNER
  AS 
  BEGIN ATOMIC 
  WITH (TRANSACTION ISOLATION LEVEL = SNAPSHOT, LANGUAGE = N'us_english')

select [ID], [Name], [LastCompletedID], [LastCompletedTime], [LockTime], [LockToken], [Disabled]
from [Queue_Schema_Name].[Subscription]
where [Name] = @name

END