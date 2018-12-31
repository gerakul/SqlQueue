
CREATE PROCEDURE [Queue_Schema_Name].[RestoreState] 
  WITH NATIVE_COMPILATION, SCHEMABINDING, EXECUTE AS OWNER
  AS 
  BEGIN ATOMIC 
  WITH (TRANSACTION ISOLATION LEVEL = SNAPSHOT, LANGUAGE = N'us_english')

declare @MinID1 bigint
declare @MaxID1 bigint
declare @Num1 int
declare @LastWrite1 datetime2(7)
declare @NeedClean1 bit = 0
declare @MinID2 bigint
declare @MaxID2 bigint
declare @Num2 int
declare @LastWrite2 datetime2(7)
declare @NeedClean2 bit = 0
declare @IsFirstActive bit
declare @LastWrite datetime2(7)

declare @MinNum int
declare @TresholdNum int

declare @time datetime2(7) = sysutcdatetime()

select top 1 @MinNum = MinNum, @TresholdNum = TresholdNum
from [Queue_Schema_Name].[Settings]

select @MinID1 = min(ID), @MaxID1 = max(ID), @Num1 = count(*), @LastWrite1 = max(Created)
from [Queue_Schema_Name].Messages1

if (@Num1 = 0)
begin
    set @MinID1 = 0
    set @MaxID1 = 0
end

select @MinID2 = min(ID), @MaxID2 = max(ID), @Num2 = count(*), @LastWrite2 = max(Created)
from [Queue_Schema_Name].Messages2

if (@Num2 = 0)
begin
    set @MinID2 = 0
    set @MaxID2 = 0
end

if (@MaxID1 >= @MaxID2)
begin
    set @IsFirstActive = 1
	set @LastWrite = @LastWrite1
end
else 
begin
    set @IsFirstActive = 0
	set @LastWrite = @LastWrite2
end

if (@LastWrite is null)
	set @LastWrite = @time

-- если можем очистить другую таблицу, то помечаем для очистки
if (@IsFirstActive = 1 and @Num1 >= @MinNum and @MaxID2 > 0)
    set @NeedClean2 = 1
else if (@IsFirstActive = 0 and @Num2 >= @MinNum and @MaxID1 > 0)
    set @NeedClean1 = 1

delete from [Queue_Schema_Name].[State]

insert into [Queue_Schema_Name].[State] (ID, Modified, LastWrite, MinID1, MaxID1, Num1, NeedClean1, MinID2, MaxID2,
    Num2, NeedClean2, IsFirstActive, MinNum, TresholdNum)
values (1, @time, @LastWrite, @MinID1, @MaxID1, @Num1, @NeedClean1, @MinID2, @MaxID2,
    @Num2, @NeedClean2, @IsFirstActive, @MinNum, @TresholdNum)

END