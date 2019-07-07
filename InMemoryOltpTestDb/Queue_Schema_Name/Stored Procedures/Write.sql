
CREATE PROCEDURE [Queue_Schema_Name].[Write] 
  @body varbinary(8000),
  @id bigint out
  WITH NATIVE_COMPILATION, SCHEMABINDING, EXECUTE AS OWNER
  AS 
  BEGIN ATOMIC 
  WITH (TRANSACTION ISOLATION LEVEL = SNAPSHOT, LANGUAGE = N'us_english')

declare @date datetime2(7) = sysutcdatetime()
declare @stateUpdated bit = 0

declare @IsFirstActive bit
declare @MaxID1 bigint
declare @MaxID2 bigint
declare @Num1 int
declare @Num2 int
declare @NeedClean1 bit
declare @NeedClean2 bit
declare @MinNum int
declare @TresholdNum int
declare @ForceCleanInAction bit

declare @descriptionString nvarchar(1024) = 'Queue: Queue_Schema_Name'
declare @errStr nvarchar(2048)

select top 1 @IsFirstActive = IsFirstActive, @MaxID1 = MaxID1, @MaxID2 = MaxID2,
    @Num1 = Num1, @Num2 = Num2, @NeedClean1 = NeedClean1, @NeedClean2 = NeedClean2, 
    @MinNum = MinNum, @TresholdNum = TresholdNum, @ForceCleanInAction = ForceCleanInAction
from [Queue_Schema_Name].[State]

if (@MaxID1 is null)
begin
    exec [Queue_Schema_Name].[RestoreState]

    select top 1 @IsFirstActive = IsFirstActive, @MaxID1 = MaxID1, @MaxID2 = MaxID2,
        @Num1 = Num1, @Num2 = Num2, @NeedClean1 = NeedClean1, @NeedClean2 = NeedClean2, 
        @MinNum = MinNum, @TresholdNum = TresholdNum, @ForceCleanInAction = ForceCleanInAction
    from [Queue_Schema_Name].[State]
end

if (@ForceCleanInAction = 1)
begin
	set @errStr = 'Force Clean in action. ' + @descriptionString;
	throw 50005, @errStr, 1;
end

-- всегда оставляем последнее сообщение
if (@MinNum < 1)
    set @MinNum = 1

-- если можем очистить другую таблицу, то помечаем для очистки
if (@IsFirstActive = 1 and @Num1 >= @MinNum and @MaxID2 > 0 and @NeedClean2 = 0)
begin
    update [Queue_Schema_Name].[State]
    set Modified = @date, NeedClean2 = 1
end
else if (@IsFirstActive = 0 and @Num2 >= @MinNum and @MaxID1 > 0 and @NeedClean1 = 0)
begin
    update [Queue_Schema_Name].[State]
    set Modified = @date, NeedClean1 = 1
end


-- если превысили количество сообщений и другая таблица свободна, то переключаемся
if (@IsFirstActive = 1 and @Num1 >= @TresholdNum and @MaxID2 = 0)
begin
    set @IsFirstActive = 0
    set @id = @MaxID1 + 1

    update [Queue_Schema_Name].[State]
    set Modified = @date, LastWrite = @date, MinID2 = @id, MaxID2 = @id, Num2 = 1, IsFirstActive = 0

    set @stateUpdated = 1
end
else if (@IsFirstActive = 0 and @Num2 >= @TresholdNum and @MaxID1 = 0)
begin
    set @IsFirstActive = 1
    set @id = @MaxID2 + 1

    update [Queue_Schema_Name].[State]
    set Modified = @date, LastWrite = @date, MinID1 = @id, MaxID1 = @id, Num1 = 1, IsFirstActive = 1

    set @stateUpdated = 1
end
  

if (@IsFirstActive = 1)
begin
    if (@stateUpdated = 0)
    begin
        set @id = @MaxID1 + 1

        update [Queue_Schema_Name].[State]
	    set Modified = @date, LastWrite = @date, MaxID1 = @id, Num1 = @Num1 + 1
    end

    insert into [Queue_Schema_Name].Messages1 (ID, Created, Body)
    values (@id, @date, @body)
end
else
begin
    if (@stateUpdated = 0)
    begin
        set @id = @MaxID2 + 1

        update [Queue_Schema_Name].[State]
        set Modified = @date, LastWrite = @date, MaxID2 = @id, Num2 = @Num2 + 1
    end

    insert into [Queue_Schema_Name].Messages2 (ID, Created, Body)
    values (@id, @date, @body)
end

END