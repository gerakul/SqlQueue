
CREATE PROCEDURE [Queue_Schema_Name].[GetSubscriptionInfo] 
  @subscriptionID int
  WITH NATIVE_COMPILATION, SCHEMABINDING, EXECUTE AS OWNER
  AS 
  BEGIN ATOMIC 
  WITH (TRANSACTION ISOLATION LEVEL = SNAPSHOT, LANGUAGE = N'us_english')


declare @Modified datetime2(7)
declare @IsFirstActive bit
declare @MaxID1 bigint
declare @MaxID2 bigint

select top 1 @Modified = Modified, @IsFirstActive = IsFirstActive, @MaxID1 = MaxID1, @MaxID2 = MaxID2
from [Queue_Schema_Name].[State]

if (@MaxID1 is null)
begin
    exec [Queue_Schema_Name].[RestoreState]

    select top 1 @Modified = Modified, @IsFirstActive = IsFirstActive, @MaxID1 = MaxID1, @MaxID2 = MaxID2
    from [Queue_Schema_Name].[State]
end

declare @maxID bigint

if (@IsFirstActive = 1)
	set @maxID = @MaxID1
else 
	set @maxID = @MaxID2

select [ID], [Name], [LastCompletedID], [LastCompletedTime], [LockTime], [Disabled],
	[MaxIdleIntervalSeconds], [MaxUncompletedMessages], [ActionOnLimitExceeding],
	@maxID - [LastCompletedID] as UncompletedMessages, 
	datediff(second, [LastCompletedTime], @Modified) as IdleIntervalSeconds
from [Queue_Schema_Name].[Subscription]
where ID = @subscriptionID

END