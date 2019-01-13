
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

declare @descriptionString nvarchar(1024) = 'Queue: Queue_Schema_Name, SubscriptionID: ' + cast(@subscriptionID as varchar(10))
declare @errStr nvarchar(2048)

select top 1 @LockToken = LockToken, @Disabled = [Disabled]
from [Queue_Schema_Name].[Subscription]
where ID = @subscriptionID

if (@Disabled is null)
begin
	set @errStr = 'Subscription does not exist. ' + @descriptionString;
	throw 50004, @errStr, 1;
end

if (@Disabled = 1)
begin
	set @errStr = 'Subscription is disabled. ' + @descriptionString;
	throw 50002, @errStr, 1;
end


if (@LockToken = @currentLockToken)
    update [Queue_Schema_Name].[Subscription]
    set LastCompletedID = @id, LastCompletedTime = sysutcdatetime(), LockTime = null, LockToken = null
    where ID = @subscriptionID;
else
begin
	set @errStr = 'Sent LockToken ' + isnull(cast(@currentLockToken as nvarchar(50)), 'NULL') 
	    + ' doesn''t equal stored LockToken ' + isnull(cast(@LockToken as nvarchar(50)), 'NULL')
		+ '. ' + @descriptionString;
    throw 50001, @errStr, 1;
end

END