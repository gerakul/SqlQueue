




CREATE PROCEDURE [Queue_Schema_Name].[Complete]
  @subscriptionID int,
  @id bigint,
  @currentLockToken uniqueidentifier
  WITH NATIVE_COMPILATION, SCHEMABINDING, EXECUTE AS OWNER
  AS 
  BEGIN ATOMIC 
  WITH (TRANSACTION ISOLATION LEVEL = SNAPSHOT, LANGUAGE = N'us_english')

declare @LockToken uniqueidentifier
declare @Disabled bit

select top 1 @LockToken = LockToken, @Disabled = [Disabled]
from [Queue_Schema_Name].[Subscription]
where ID = @subscriptionID

if (@Disabled = 1)
	throw 50002, 'Subscription is disabled', 1;


if (@LockToken = @currentLockToken)
    update [Queue_Schema_Name].[Subscription]
    set LastCompletedID = @id, LastCompletedTime = sysutcdatetime(), LockTime = null, LockToken = null
    where ID = @subscriptionID;
else
    throw 50001, 'Sent LockToken don''t equals stored LockToken', 1;

END