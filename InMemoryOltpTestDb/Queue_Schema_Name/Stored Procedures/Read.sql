
CREATE PROCEDURE [Queue_Schema_Name].[Read] 
  @subscriptionID int,
  @num int,
  @checkLockSeconds int,
  @peek bit,
  @newLockToken uniqueidentifier out
  WITH NATIVE_COMPILATION, SCHEMABINDING, EXECUTE AS OWNER
  AS 
  BEGIN ATOMIC 
  WITH (TRANSACTION ISOLATION LEVEL = SNAPSHOT, LANGUAGE = N'us_english')

declare @time datetime2(7) = sysutcdatetime()

declare @LastCompletedID bigint
declare @LockTime datetime2(7)
declare @Disabled bit

declare @descriptionString nvarchar(1024) = 'Queue: Queue_Schema_Name, SubscriptionID: ' + cast(@subscriptionID as varchar(10))
declare @errStr nvarchar(2048)

select top 1 @LastCompletedID = LastCompletedID, @LockTime = LockTime, @Disabled = [Disabled]
from [Queue_Schema_Name].[Subscription]
where ID = @subscriptionID

if (@Disabled is null)
begin
	set @errStr = 'Subscription does not exist. ' + @descriptionString;
	throw 50004, @errStr, 1;
end

if (@Disabled = 1)
begin
    set @newLockToken = null;
	set @errStr = 'Subscription is disabled. ' + @descriptionString;
	throw 50002, @errStr, 1;
end

-- проверяем заблокирована ли подписка
if (isnull(@peek, 0) = 0 and @LockTime is not null)
begin
    -- проверяем актуальна ли блокировка
    if (DATEDIFF(second, @LockTime, @time) <  @checkLockSeconds)
    begin
        set @newLockToken = null;
	    set @errStr = 'Subscription is locked. ' + @descriptionString;
	    throw 50003, @errStr, 1;
    end
end

declare @Num1 int
declare @MinID1 bigint
declare @MaxID1 bigint
declare @NeedClean1 bit
declare @Num2 int
declare @MinID2 bigint
declare @MaxID2 bigint
declare @NeedClean2 bit
declare @IsFirstActive bit

-- определяем откуда читать и есть ли что читать
select top 1 @Num1 = Num1, @MinID1 = MinID1, @MaxID1 = MaxID1, 
	@Num2 = Num2, @MinID2 = MinID2, @MaxID2 = MaxID2, 
	@IsFirstActive = IsFirstActive
from [Queue_Schema_Name].[State]


if (@MaxID1 is null)
begin
    exec [Queue_Schema_Name].[RestoreState]

    select top 1 @Num1 = Num1, @MinID1 = MinID1, @MaxID1 = MaxID1, 
	    @Num2 = Num2, @MinID2 = MinID2, @MaxID2 = MaxID2, 
	    @IsFirstActive = IsFirstActive
    from [Queue_Schema_Name].[State]
end


if (@LastCompletedID >= @MaxID1 and @LastCompletedID >= @MaxID2)
begin
    -- если нечего читать, то выходим
    set @newLockToken = null

	select ID, Created, Body
    from [Queue_Schema_Name].Messages0
end
else
begin
    if (isnull(@peek, 0) = 0)
    begin
        set @newLockToken = newid()

        -- лочим
        update [Queue_Schema_Name].[Subscription]
        set LockTime = @time, LockToken = @newLockToken
        where ID = @subscriptionID
    end
    else
        set @newLockToken = null


    if (@IsFirstActive = 1 and @LastCompletedID >= @MaxID2)
        if (@num > 0)
            select top(@num) ID, Created, Body
            from [Queue_Schema_Name].Messages1
            where ID > @LastCompletedID
            order by ID
        else
            select ID, Created, Body
            from [Queue_Schema_Name].Messages1
            where ID > @LastCompletedID
            order by ID
    else if (@IsFirstActive = 0 and @LastCompletedID < @MaxID1)
        if (@num > 0) 
	        if (@num <= @MaxID1 - @LastCompletedID)
	            select top(@num) ID, Created, Body
	            from [Queue_Schema_Name].Messages1
	            where ID > @LastCompletedID
	            order by ID
	        else 
                select top(@num) ID, Created, Body
                from
                (
	                select ID, Created, Body
	                from [Queue_Schema_Name].Messages1
	                where ID > @LastCompletedID
	                union all
	                select ID, Created, Body
	                from [Queue_Schema_Name].Messages2
                ) T
                order by ID
        else
	        select ID, Created, Body
	        from [Queue_Schema_Name].Messages1
	        where ID > @LastCompletedID
	        union all
	        select ID, Created, Body
	        from [Queue_Schema_Name].Messages2
	        order by ID	      
    else if (@IsFirstActive = 0 and @LastCompletedID >= @MaxID1)
        if (@num > 0)
            select top(@num) ID, Created, Body
            from [Queue_Schema_Name].Messages2
            where ID > @LastCompletedID
            order by ID
        else
            select ID, Created, Body
            from [Queue_Schema_Name].Messages2
            where ID > @LastCompletedID
            order by ID
    else if (@IsFirstActive = 1 and @LastCompletedID < @MaxID2)
        if (@num > 0) 
	        if (@num <= @MaxID2 - @LastCompletedID)
	            select top(@num) ID, Created, Body
	            from [Queue_Schema_Name].Messages2
	            where ID > @LastCompletedID
	            order by ID
	        else 
                select top(@num) ID, Created, Body
                from 
                (
	                select ID, Created, Body
	                from [Queue_Schema_Name].Messages2
	                where ID > @LastCompletedID
	                union all
	                select ID, Created, Body
	                from [Queue_Schema_Name].Messages1
                ) T
                order by ID
        else
	        select ID, Created, Body
	        from [Queue_Schema_Name].Messages2
	        where ID > @LastCompletedID
	        union all
	        select ID, Created, Body
	        from [Queue_Schema_Name].Messages1
	        order by ID	      
end    


END