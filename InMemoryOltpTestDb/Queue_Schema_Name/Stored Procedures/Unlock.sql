
CREATE PROCEDURE [Queue_Schema_Name].[Unlock]
  @subscriptionID int,
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

if (@LockToken is null)
	return;

if (@LockToken = @currentLockToken)
    update [Queue_Schema_Name].[Subscription]
    set LockTime = null, LockToken = null
    where ID = @subscriptionID;
else
begin
	declare @errStr nvarchar(1000) = 'Sent LockToken ' + isnull(cast(@currentLockToken as nvarchar(50)), 'NULL') + ' doesn''t equal stored LockToken ' + isnull(cast(@LockToken as nvarchar(50)), 'NULL');
    throw 50001, @errStr, 1;
end


END