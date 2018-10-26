
CREATE PROCEDURE [Queue_Schema_Name].[Clean]
  WITH NATIVE_COMPILATION, SCHEMABINDING, EXECUTE AS OWNER
  AS 
  BEGIN ATOMIC 
  WITH (TRANSACTION ISOLATION LEVEL = SNAPSHOT, LANGUAGE = N'us_english')

declare @Modified datetime2(7)
declare @IsFirstActive bit
declare @MaxID1 bigint
declare @NeedClean1 bit
declare @MaxID2 bigint
declare @NeedClean2 bit

select top 1 @Modified = Modified, @IsFirstActive = IsFirstActive,
	@MaxID1 = MaxID1, @NeedClean1 = NeedClean1, @MaxID2 = MaxID2, @NeedClean2 = NeedClean2
from [Queue_Schema_Name].[State]

if (@MaxID1 is null)
begin
    exec [Queue_Schema_Name].[RestoreState]

    select top 1 @Modified = Modified, @IsFirstActive = IsFirstActive,
		@MaxID1 = MaxID1, @NeedClean1 = NeedClean1, @MaxID2 = MaxID2, @NeedClean2 = NeedClean2
    from [Queue_Schema_Name].[State]
end

declare @maxID bigint

if (@IsFirstActive = 1)
	set @maxID = @MaxID1
else 
	set @maxID = @MaxID2

delete from [Queue_Schema_Name].Subscription
where ActionOnLimitExceeding = 1 
	and ((@maxID - LastCompletedID) > MaxUncompletedMessages 
		or datediff(second, LastCompletedTime, @Modified) > MaxIdleIntervalSeconds)

update [Queue_Schema_Name].Subscription
set LockTime = null, LockToken = null, [Disabled] = 1
where ActionOnLimitExceeding = 2 and [Disabled] = 0 
	and ((@maxID - LastCompletedID) > MaxUncompletedMessages 
		or datediff(second, LastCompletedTime, @Modified) > MaxIdleIntervalSeconds)

if (@NeedClean1 = 1)
begin
    declare @SubscriptionExists1 bit

    select top 1 @SubscriptionExists1 = 1 
    from [Queue_Schema_Name].Subscription 
    where [Disabled] = 0 and LastCompletedID < @MaxID1   
    
    if (@SubscriptionExists1 is null) 
    begin
        delete from [Queue_Schema_Name].[Messages1] -- заменить на truncate в следующем релизе sql server

        update [Queue_Schema_Name].[State]
        set Modified = sysutcdatetime(), MinID1 = 0, MaxID1 = 0, Num1 = 0, NeedClean1 = 0
    end
end

if (@NeedClean2 = 1)
begin
    declare @SubscriptionExists2 bit

    select top 1 @SubscriptionExists2 = 1 
    from [Queue_Schema_Name].Subscription 
    where [Disabled] = 0 and LastCompletedID < @MaxID2  
    
    if (@SubscriptionExists2 is null) 
    begin
        delete from [Queue_Schema_Name].[Messages2] -- заменить на truncate в следующем релизе sql server

        update [Queue_Schema_Name].[State]
        set Modified = sysutcdatetime(), MinID2 = 0, MaxID2 = 0, Num2 = 0, NeedClean2 = 0
    end
end


END