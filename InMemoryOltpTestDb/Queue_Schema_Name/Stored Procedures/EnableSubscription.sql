
CREATE PROCEDURE [Queue_Schema_Name].[EnableSubscription] 
  @subscriptionID int
  WITH NATIVE_COMPILATION, SCHEMABINDING, EXECUTE AS OWNER
  AS 
  BEGIN ATOMIC 
  WITH (TRANSACTION ISOLATION LEVEL = SNAPSHOT, LANGUAGE = N'us_english')


declare @MaxID1 bigint
declare @MaxID2 bigint
declare @IsFirstActive bit

select top 1 @MaxID1 = MaxID1, @MaxID2 = MaxID2, @IsFirstActive = IsFirstActive
from [Queue_Schema_Name].[State]

if (@MaxID1 is null)
begin
    exec [Queue_Schema_Name].[RestoreState]

    select top 1 @MaxID1 = MaxID1, @MaxID2 = MaxID2, @IsFirstActive = IsFirstActive
    from [Queue_Schema_Name].[State]
end


if (@IsFirstActive = 1)
    update [Queue_Schema_Name].[Subscription]
    set LastCompletedID = @MaxID1, LastCompletedTime = sysutcdatetime(), LockTime = null, LockToken = null, [Disabled] = 0
    where ID = @subscriptionID and [Disabled] = 1
else
    update [Queue_Schema_Name].[Subscription]
    set LastCompletedID = @MaxID2, LastCompletedTime = sysutcdatetime(), LockTime = null, LockToken = null, [Disabled] = 0
    where ID = @subscriptionID and [Disabled] = 1

END