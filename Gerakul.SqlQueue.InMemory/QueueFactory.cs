using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;

namespace Gerakul.SqlQueue.InMemory
{
    public sealed class QueueFactory
    {
        public const string Version = "1.4.1";

        private string connectionString { get; }

        public QueueFactory(string connectionString)
        {
            this.connectionString = connectionString;
        }

        public QueueClient CreateQueue(string name, int minMessNum = 10000, int tresholdMessNumBeforeClean = 100000)
        {
            using (var conn = new SqlConnection(connectionString))
            {
                conn.Open();

                var preparationCmd = conn.CreateCommand();
                preparationCmd.CommandText = "ALTER DATABASE CURRENT SET DELAYED_DURABILITY = DISABLED";
                preparationCmd.ExecuteNonQuery();

                Helper.ExecuteBatches(conn, GetCreationScript(name));

                GetPostCommand(conn, name, minMessNum, tresholdMessNumBeforeClean).ExecuteNonQuery();

                return new QueueClient(connectionString, name);
            }
        }

        public void DeleteQueue(string name)
        {
            using (var conn = new SqlConnection(connectionString))
            {
                conn.Open();


                Helper.ExecuteBatches(conn, GetDeletionScript(name));
            }
        }

        public bool IsQueueExsists(string name)
        {
            using (var conn = new SqlConnection(connectionString))
            {
                conn.Open();

                var cmd = conn.CreateCommand();

                cmd.CommandType = System.Data.CommandType.Text;
                cmd.CommandText = @"select top 1 1
                    from sys.tables t
                        join sys.schemas s on t.schema_id = s.schema_id
                            and s.name = @name
                    where t.name = 'Subscription'";
                cmd.Parameters.AddWithValue("@name", name);

                using (var r = cmd.ExecuteReader())
                {
                    if (r.Read())
                    {
                        return true;
                    }
                    else
                    {
                        return false;
                    }
                }
            }
        }

        private SqlCommand GetPostCommand(SqlConnection conn, string name, int minMessNum, int tresholdMessNumBeforeClean)
        {
            var cmd = new SqlCommand($@"
INSERT INTO [{name}].[Settings] ([ID], [MinNum], [TresholdNum])
VALUES (1, @MinNum, @TresholdNum)

INSERT INTO [{name}].[Global] ([ID], [Version])
VALUES (1, @Version)
", conn);

            cmd.Parameters.AddWithValue("MinNum", minMessNum);
            cmd.Parameters.AddWithValue("TresholdNum", tresholdMessNumBeforeClean);
            cmd.Parameters.AddWithValue("Version", Version);

            return cmd;
        }

        internal static string GetDeletionScript(string name)
        {
            return GetProceduresDeletionScript(name) + GetObjectsDeletionScript(name) + $@"

IF EXISTS (SELECT TOP 1 1 FROM INFORMATION_SCHEMA.SCHEMATA WHERE [SCHEMA_NAME] = '{name}')
    DROP SCHEMA [{name}]
";
        }

        internal static string GetCreationScript(string name)
        {
            return $@"

CREATE SCHEMA [{name}]
    AUTHORIZATION [dbo];

GO

" + GetObjectsCreationScript(name) + GetProceduresCreationScript(name);
        }

        internal static string GetObjectsDeletionScript(string name)
        {
            return $@"

IF EXISTS (SELECT TOP 1 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'Global' AND TABLE_SCHEMA = '{name}')
    DROP TABLE [{name}].[Global]
GO

IF EXISTS (SELECT TOP 1 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'Messages0' AND TABLE_SCHEMA = '{name}')
    DROP TABLE [{name}].[Messages0]
GO

IF EXISTS (SELECT TOP 1 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'Messages1' AND TABLE_SCHEMA = '{name}')
    DROP TABLE [{name}].[Messages1]
GO

IF EXISTS (SELECT TOP 1 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'Messages2' AND TABLE_SCHEMA = '{name}')
    DROP TABLE [{name}].[Messages2]
GO

IF EXISTS (SELECT TOP 1 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'Settings' AND TABLE_SCHEMA = '{name}')
    DROP TABLE [{name}].[Settings]
GO

IF EXISTS (SELECT TOP 1 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'State' AND TABLE_SCHEMA = '{name}')
    DROP TABLE [{name}].[State]
GO

IF EXISTS (SELECT TOP 1 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'Subscription' AND TABLE_SCHEMA = '{name}')
    DROP TABLE [{name}].[Subscription]
GO

IF EXISTS (SELECT TOP 1 1 FROM INFORMATION_SCHEMA.DOMAINS WHERE DOMAIN_NAME = 'MessageList' AND DOMAIN_SCHEMA = '{name}' AND DATA_TYPE = 'table type')
    DROP TYPE [{name}].[MessageList]
GO

IF EXISTS (SELECT TOP 1 1 FROM INFORMATION_SCHEMA.DOMAINS WHERE DOMAIN_NAME = 'SubscriptionCandidatesToAction' AND DOMAIN_SCHEMA = '{name}' AND DATA_TYPE = 'table type')
    DROP TYPE [{name}].[SubscriptionCandidatesToAction]
GO

IF EXISTS (SELECT TOP 1 1 FROM INFORMATION_SCHEMA.DOMAINS WHERE DOMAIN_NAME = 'SubscriptionsToAction' AND DOMAIN_SCHEMA = '{name}' AND DATA_TYPE = 'table type')
    DROP TYPE [{name}].[SubscriptionsToAction]
GO

";
        }

        internal static string GetObjectsCreationScript(string name)
        {
            return $@"

CREATE TYPE [{name}].[MessageList] AS TABLE(
	[ID] [int] NOT NULL,
	[Body] [varbinary](8000) NOT NULL,
	 PRIMARY KEY NONCLUSTERED HASH 
(
	[ID]
)WITH ( BUCKET_COUNT = 1024)
)
WITH ( MEMORY_OPTIMIZED = ON )

GO


CREATE TYPE [{name}].[SubscriptionCandidatesToAction] AS TABLE(
	[ID] [int] NOT NULL,
	[NextToCompletedID] [bigint] NOT NULL,
	[MaxIdleIntervalSeconds] [int] NOT NULL,
	[ActionOnLimitExceeding] [int] NOT NULL,
	 PRIMARY KEY NONCLUSTERED HASH 
(
	[ID]
)WITH ( BUCKET_COUNT = 256)
)
WITH ( MEMORY_OPTIMIZED = ON )

GO


CREATE TYPE [{name}].[SubscriptionsToAction] AS TABLE(
	[RowID] [int] IDENTITY(1,1) NOT NULL,
	[ID] [int] NOT NULL,
	[ActionOnLimitExceeding] [int] NOT NULL,
	 PRIMARY KEY NONCLUSTERED HASH 
(
	[RowID]
)WITH ( BUCKET_COUNT = 256)
)
WITH ( MEMORY_OPTIMIZED = ON )

GO


CREATE TABLE [{name}].[Subscription] (
    [ID]                     INT              IDENTITY (1, 1) NOT NULL,
    [Name]                   NVARCHAR (255)   NOT NULL,
    [LastCompletedID]        BIGINT           NOT NULL,
    [LastCompletedTime]      DATETIME2 (7)    NOT NULL,
    [LockTime]               DATETIME2 (7)    NULL,
    [LockToken]              UNIQUEIDENTIFIER NULL,
    [Disabled]               BIT              NOT NULL,
    [MaxIdleIntervalSeconds] INT              NULL,
    [MaxUncompletedMessages] INT              NULL,
    [ActionOnLimitExceeding] INT              NULL,
    CONSTRAINT [PK_Subscription] PRIMARY KEY NONCLUSTERED HASH ([ID]) WITH (BUCKET_COUNT = 256),
    CONSTRAINT [IX_Subscription_Name] UNIQUE NONCLUSTERED HASH ([Name]) WITH (BUCKET_COUNT = 256)
)
WITH (MEMORY_OPTIMIZED = ON);


GO

CREATE TABLE [{name}].[State] (
    [ID]            BIGINT        NOT NULL,
    [Modified]      DATETIME2 (7) NOT NULL,
	[LastWrite]     DATETIME2 (7) NOT NULL,
    [MinID1]        BIGINT        NOT NULL,
    [MaxID1]        BIGINT        NOT NULL,
    [Num1]          INT           NOT NULL,
    [NeedClean1]    BIT           NOT NULL,
    [MinID2]        BIGINT        NOT NULL,
    [MaxID2]        BIGINT        NOT NULL,
    [Num2]          INT           NOT NULL,
    [NeedClean2]    BIT           NOT NULL,
    [IsFirstActive] BIT           NOT NULL,
    [MinNum]        INT           NOT NULL,
    [TresholdNum]   INT           NOT NULL,
    PRIMARY KEY NONCLUSTERED HASH ([ID]) WITH (BUCKET_COUNT = 1)
)
WITH (DURABILITY = SCHEMA_ONLY, MEMORY_OPTIMIZED = ON);


GO

CREATE TABLE [{name}].[Settings] (
    [ID]          BIGINT NOT NULL,
    [MinNum]      INT    NOT NULL,
    [TresholdNum] INT    NOT NULL,
    PRIMARY KEY NONCLUSTERED HASH ([ID]) WITH (BUCKET_COUNT = 1)
)
WITH (MEMORY_OPTIMIZED = ON);


GO

CREATE TABLE [{name}].[Messages2] (
    [ID]      BIGINT           NOT NULL,
    [Created] DATETIME2 (7)    NOT NULL,
    [Body]    VARBINARY (8000) NOT NULL,
    PRIMARY KEY NONCLUSTERED ([ID] ASC) 
)
WITH (MEMORY_OPTIMIZED = ON);


GO

CREATE TABLE [{name}].[Messages1] (
    [ID]      BIGINT           NOT NULL,
    [Created] DATETIME2 (7)    NOT NULL,
    [Body]    VARBINARY (8000) NOT NULL,
    PRIMARY KEY NONCLUSTERED ([ID] ASC) 
)
WITH (MEMORY_OPTIMIZED = ON);


GO

CREATE TABLE [{name}].[Messages0] (
    [ID]      BIGINT           NOT NULL,
    [Created] DATETIME2 (7)    NOT NULL,
    [Body]    VARBINARY (8000) NOT NULL,
    PRIMARY KEY NONCLUSTERED HASH ([ID]) WITH (BUCKET_COUNT = 1)
)
WITH (DURABILITY = SCHEMA_ONLY, MEMORY_OPTIMIZED = ON);

GO

CREATE TABLE [{name}].[Global] (
    [ID]      BIGINT        NOT NULL,
    [Version] NVARCHAR (32) NOT NULL,
    PRIMARY KEY NONCLUSTERED HASH ([ID]) WITH (BUCKET_COUNT = 1)
)
WITH (MEMORY_OPTIMIZED = ON);

GO

";
        }

        internal static string GetProceduresDeletionScript(string name)
        {
            return $@"

IF EXISTS (SELECT TOP 1 1 FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_TYPE = 'PROCEDURE' AND ROUTINE_NAME = 'CreateSubscription' AND ROUTINE_SCHEMA = '{name}')
    DROP PROCEDURE [{name}].[CreateSubscription]
GO

IF EXISTS (SELECT TOP 1 1 FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_TYPE = 'PROCEDURE' AND ROUTINE_NAME = 'EnableSubscription' AND ROUTINE_SCHEMA = '{name}')
    DROP PROCEDURE [{name}].[EnableSubscription] 
GO

IF EXISTS (SELECT TOP 1 1 FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_TYPE = 'PROCEDURE' AND ROUTINE_NAME = 'DeleteSubscription' AND ROUTINE_SCHEMA = '{name}')
    DROP PROCEDURE [{name}].[DeleteSubscription]
GO

IF EXISTS (SELECT TOP 1 1 FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_TYPE = 'PROCEDURE' AND ROUTINE_NAME = 'DisableSubscription' AND ROUTINE_SCHEMA = '{name}')
    DROP PROCEDURE [{name}].[DisableSubscription] 
GO

IF EXISTS (SELECT TOP 1 1 FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_TYPE = 'PROCEDURE' AND ROUTINE_NAME = 'FindSubscription' AND ROUTINE_SCHEMA = '{name}')
    DROP PROCEDURE [{name}].[FindSubscription]
GO
 
IF EXISTS (SELECT TOP 1 1 FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_TYPE = 'PROCEDURE' AND ROUTINE_NAME = 'GetSubscription' AND ROUTINE_SCHEMA = '{name}')
    DROP PROCEDURE [{name}].[GetSubscription]
GO

IF EXISTS (SELECT TOP 1 1 FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_TYPE = 'PROCEDURE' AND ROUTINE_NAME = 'GetSubscriptionInfo' AND ROUTINE_SCHEMA = '{name}')
    DROP PROCEDURE [{name}].[GetSubscriptionInfo] 
GO

IF EXISTS (SELECT TOP 1 1 FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_TYPE = 'PROCEDURE' AND ROUTINE_NAME = 'GetAllSubscriptionsInfo' AND ROUTINE_SCHEMA = '{name}')
    DROP PROCEDURE [{name}].[GetAllSubscriptionsInfo] 
GO

IF EXISTS (SELECT TOP 1 1 FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_TYPE = 'PROCEDURE' AND ROUTINE_NAME = 'SetSubscriptionSettings' AND ROUTINE_SCHEMA = '{name}')
    DROP PROCEDURE [{name}].[SetSubscriptionSettings] 
GO

IF EXISTS (SELECT TOP 1 1 FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_TYPE = 'PROCEDURE' AND ROUTINE_NAME = 'Write' AND ROUTINE_SCHEMA = '{name}')
    DROP PROCEDURE [{name}].[Write]
GO

IF EXISTS (SELECT TOP 1 1 FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_TYPE = 'PROCEDURE' AND ROUTINE_NAME = 'WriteMany' AND ROUTINE_SCHEMA = '{name}')
    DROP PROCEDURE [{name}].[WriteMany]
GO

IF EXISTS (SELECT TOP 1 1 FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_TYPE = 'PROCEDURE' AND ROUTINE_NAME = 'Clean' AND ROUTINE_SCHEMA = '{name}')
    DROP PROCEDURE [{name}].[Clean]
GO

IF EXISTS (SELECT TOP 1 1 FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_TYPE = 'PROCEDURE' AND ROUTINE_NAME = 'Read' AND ROUTINE_SCHEMA = '{name}')
    DROP PROCEDURE [{name}].[Read] 
GO

IF EXISTS (SELECT TOP 1 1 FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_TYPE = 'PROCEDURE' AND ROUTINE_NAME = 'Complete' AND ROUTINE_SCHEMA = '{name}')
    DROP PROCEDURE [{name}].[Complete]
GO
 
IF EXISTS (SELECT TOP 1 1 FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_TYPE = 'PROCEDURE' AND ROUTINE_NAME = 'Relock' AND ROUTINE_SCHEMA = '{name}')
    DROP PROCEDURE [{name}].[Relock]
GO
 
IF EXISTS (SELECT TOP 1 1 FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_TYPE = 'PROCEDURE' AND ROUTINE_NAME = 'Unlock' AND ROUTINE_SCHEMA = '{name}')
    DROP PROCEDURE [{name}].[Unlock]
GO

IF EXISTS (SELECT TOP 1 1 FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_TYPE = 'PROCEDURE' AND ROUTINE_NAME = 'RestoreState' AND ROUTINE_SCHEMA = '{name}')
    DROP PROCEDURE [{name}].[RestoreState]
GO
";
        }

        internal static string GetProceduresCreationScript(string name)
        {
            return $@"

CREATE PROCEDURE [{name}].[RestoreState] 
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
from [{name}].[Settings]

select @MinID1 = min(ID), @MaxID1 = max(ID), @Num1 = count(*), @LastWrite1 = max(Created)
from [{name}].Messages1

if (@Num1 = 0)
begin
    set @MinID1 = 0
    set @MaxID1 = 0
end

select @MinID2 = min(ID), @MaxID2 = max(ID), @Num2 = count(*), @LastWrite2 = max(Created)
from [{name}].Messages2

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

delete from [{name}].[State]

insert into [{name}].[State] (ID, Modified, LastWrite, MinID1, MaxID1, Num1, NeedClean1, MinID2, MaxID2,
    Num2, NeedClean2, IsFirstActive, MinNum, TresholdNum)
values (1, @time, @LastWrite, @MinID1, @MaxID1, @Num1, @NeedClean1, @MinID2, @MaxID2,
    @Num2, @NeedClean2, @IsFirstActive, @MinNum, @TresholdNum)

END

GO

" + GetMainProceduresScript(name, false);
        }

        internal static string GetMainProceduresScript(string name, bool alter)
        {
            return $@"

{(alter ? "ALTER" : "CREATE")} PROCEDURE [{name}].[Unlock]
  @subscriptionID int,
  @currentLockToken uniqueidentifier
  WITH NATIVE_COMPILATION, SCHEMABINDING, EXECUTE AS OWNER
  AS 
  BEGIN ATOMIC 
  WITH (TRANSACTION ISOLATION LEVEL = SNAPSHOT, LANGUAGE = N'us_english')

declare @LockToken uniqueidentifier
declare @Disabled bit

declare @descriptionString nvarchar(1024) = 'Queue: {name}, SubscriptionID: ' + cast(@subscriptionID as varchar(10))
declare @errStr nvarchar(2048)

select top 1 @LockToken = LockToken, @Disabled = [Disabled]
from [{name}].[Subscription]
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

if (@LockToken is null)
	return;

if (@LockToken = @currentLockToken)
    update [{name}].[Subscription]
    set LockTime = null, LockToken = null
    where ID = @subscriptionID;
else
begin
	set @errStr = 'Sent LockToken ' + isnull(cast(@currentLockToken as nvarchar(50)), 'NULL') 
	    + ' doesn''t equal stored LockToken ' + isnull(cast(@LockToken as nvarchar(50)), 'NULL')
		+ '. ' + @descriptionString;
    throw 50001, @errStr, 1;
end


END

GO


{(alter ? "ALTER" : "CREATE")} PROCEDURE [{name}].[Relock]
  @subscriptionID int,
  @currentLockToken uniqueidentifier
  WITH NATIVE_COMPILATION, SCHEMABINDING, EXECUTE AS OWNER
  AS 
  BEGIN ATOMIC 
  WITH (TRANSACTION ISOLATION LEVEL = SNAPSHOT, LANGUAGE = N'us_english')

declare @LockToken uniqueidentifier
declare @Disabled bit

declare @descriptionString nvarchar(1024) = 'Queue: {name}, SubscriptionID: ' + cast(@subscriptionID as varchar(10))
declare @errStr nvarchar(2048)

select top 1 @LockToken = LockToken, @Disabled = [Disabled]
from [{name}].[Subscription]
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
    update [{name}].[Subscription]
    set LockTime = sysutcdatetime()
    where ID = @subscriptionID;
else
begin
	set @errStr = 'Sent LockToken ' + isnull(cast(@currentLockToken as nvarchar(50)), 'NULL') 
	    + ' doesn''t equal stored LockToken ' + isnull(cast(@LockToken as nvarchar(50)), 'NULL')
		+ '. ' + @descriptionString;
    throw 50001, @errStr, 1;
end

END

GO


{(alter ? "ALTER" : "CREATE")} PROCEDURE [{name}].[Complete]
  @subscriptionID int,
  @id bigint,
  @currentLockToken uniqueidentifier
  WITH NATIVE_COMPILATION, SCHEMABINDING, EXECUTE AS OWNER
  AS 
  BEGIN ATOMIC 
  WITH (TRANSACTION ISOLATION LEVEL = SNAPSHOT, LANGUAGE = N'us_english')

declare @LockToken uniqueidentifier
declare @Disabled bit

declare @descriptionString nvarchar(1024) = 'Queue: {name}, SubscriptionID: ' + cast(@subscriptionID as varchar(10))
declare @errStr nvarchar(2048)

select top 1 @LockToken = LockToken, @Disabled = [Disabled]
from [{name}].[Subscription]
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
    update [{name}].[Subscription]
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

GO


{(alter ? "ALTER" : "CREATE")} PROCEDURE [{name}].[Read] 
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

declare @descriptionString nvarchar(1024) = 'Queue: {name}, SubscriptionID: ' + cast(@subscriptionID as varchar(10))
declare @errStr nvarchar(2048)

select top 1 @LastCompletedID = LastCompletedID, @LockTime = LockTime, @Disabled = [Disabled]
from [{name}].[Subscription]
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
declare @MinID1 int
declare @MaxID1 int
declare @NeedClean1 bit
declare @Num2 int
declare @MinID2 int
declare @MaxID2 int
declare @NeedClean2 bit
declare @IsFirstActive bit

-- определяем откуда читать и есть ли что читать
select top 1 @Num1 = Num1, @MinID1 = MinID1, @MaxID1 = MaxID1, 
	@Num2 = Num2, @MinID2 = MinID2, @MaxID2 = MaxID2, 
	@IsFirstActive = IsFirstActive
from [{name}].[State]


if (@MaxID1 is null)
begin
    exec [{name}].[RestoreState]

    select top 1 @Num1 = Num1, @MinID1 = MinID1, @MaxID1 = MaxID1, 
	    @Num2 = Num2, @MinID2 = MinID2, @MaxID2 = MaxID2, 
	    @IsFirstActive = IsFirstActive
    from [{name}].[State]
end


if (@LastCompletedID >= @MaxID1 and @LastCompletedID >= @MaxID2)
begin
    -- если нечего читать, то выходим
    set @newLockToken = null

	select ID, Created, Body
    from [{name}].Messages0
end
else
begin
    if (isnull(@peek, 0) = 0)
    begin
        set @newLockToken = newid()

        -- лочим
        update [{name}].[Subscription]
        set LockTime = @time, LockToken = @newLockToken
        where ID = @subscriptionID
    end
    else
        set @newLockToken = null


    if (@IsFirstActive = 1 and @LastCompletedID >= @MaxID2)
        if (@num > 0)
            select top(@num) ID, Created, Body
            from [{name}].Messages1
            where ID > @LastCompletedID
            order by ID
        else
            select ID, Created, Body
            from [{name}].Messages1
            where ID > @LastCompletedID
            order by ID
    else if (@IsFirstActive = 0 and @LastCompletedID < @MaxID1)
        if (@num > 0) 
	        if (@num <= @MaxID1 - @LastCompletedID)
	            select top(@num) ID, Created, Body
	            from [{name}].Messages1
	            where ID > @LastCompletedID
	            order by ID
	        else 
                select top(@num) ID, Created, Body
                from
                (
	                select ID, Created, Body
	                from [{name}].Messages1
	                where ID > @LastCompletedID
	                union all
	                select ID, Created, Body
	                from [{name}].Messages2
                ) T
                order by ID
        else
	        select ID, Created, Body
	        from [{name}].Messages1
	        where ID > @LastCompletedID
	        union all
	        select ID, Created, Body
	        from [{name}].Messages2
	        order by ID	      
    else if (@IsFirstActive = 0 and @LastCompletedID >= @MaxID1)
        if (@num > 0)
            select top(@num) ID, Created, Body
            from [{name}].Messages2
            where ID > @LastCompletedID
            order by ID
        else
            select ID, Created, Body
            from [{name}].Messages2
            where ID > @LastCompletedID
            order by ID
    else if (@IsFirstActive = 1 and @LastCompletedID < @MaxID2)
        if (@num > 0) 
	        if (@num <= @MaxID2 - @LastCompletedID)
	            select top(@num) ID, Created, Body
	            from [{name}].Messages2
	            where ID > @LastCompletedID
	            order by ID
	        else 
                select top(@num) ID, Created, Body
                from 
                (
	                select ID, Created, Body
	                from [{name}].Messages2
	                where ID > @LastCompletedID
	                union all
	                select ID, Created, Body
	                from [{name}].Messages1
                ) T
                order by ID
        else
	        select ID, Created, Body
	        from [{name}].Messages2
	        where ID > @LastCompletedID
	        union all
	        select ID, Created, Body
	        from [{name}].Messages1
	        order by ID	      
end    


END

GO


{(alter ? "ALTER" : "CREATE")} PROCEDURE [{name}].[Clean]
  WITH NATIVE_COMPILATION, SCHEMABINDING, EXECUTE AS OWNER
  AS 
  BEGIN ATOMIC 
  WITH (TRANSACTION ISOLATION LEVEL = SNAPSHOT, LANGUAGE = N'us_english')

declare @LastWrite datetime2(7)
declare @IsFirstActive bit
declare @MaxID1 bigint
declare @NeedClean1 bit
declare @MaxID2 bigint
declare @NeedClean2 bit

select top 1 @LastWrite = LastWrite, @IsFirstActive = IsFirstActive,
	@MaxID1 = MaxID1, @NeedClean1 = NeedClean1, @MaxID2 = MaxID2, @NeedClean2 = NeedClean2
from [{name}].[State]

if (@MaxID1 is null)
begin
    exec [{name}].[RestoreState]

    select top 1 @LastWrite = LastWrite, @IsFirstActive = IsFirstActive,
		@MaxID1 = MaxID1, @NeedClean1 = NeedClean1, @MaxID2 = MaxID2, @NeedClean2 = NeedClean2
    from [{name}].[State]
end

declare @maxID bigint

if (@IsFirstActive = 1)
	set @maxID = @MaxID1
else 
	set @maxID = @MaxID2

delete from [{name}].[Subscription]
where ActionOnLimitExceeding = 1 
	and (@maxID - LastCompletedID) > MaxUncompletedMessages 

update [{name}].[Subscription]
set LockTime = null, LockToken = null, [Disabled] = 1
where ActionOnLimitExceeding = 2 and [Disabled] = 0 
	and (@maxID - LastCompletedID) > MaxUncompletedMessages 

--- actions triggered by MaxIdleIntervalSeconds ---<<<
declare @candidates [{name}].[SubscriptionCandidatesToAction];

insert into @candidates (ID, NextToCompletedID, MaxIdleIntervalSeconds, ActionOnLimitExceeding)
select ID, LastCompletedID + 1 as NextToCompletedID, MaxIdleIntervalSeconds, ActionOnLimitExceeding
from [{name}].[Subscription]
where (ActionOnLimitExceeding = 1 or (ActionOnLimitExceeding = 2 and [Disabled] = 0))
	and datediff(second, LastCompletedTime, @LastWrite) > MaxIdleIntervalSeconds
	and LastCompletedID < @maxID

declare @candidatesExist bit = (select top 1 1 from @candidates);

if (@candidatesExist = 1)
begin
	-- rarely executed code
	declare @actionNeeded [{name}].[SubscriptionsToAction];

	insert into @actionNeeded (ID, ActionOnLimitExceeding)
	select C.ID, C.ActionOnLimitExceeding 
	from @candidates C
		join [{name}].[Messages1] M on C.NextToCompletedID = M.ID
	where datediff(second, M.Created, @LastWrite) > C.MaxIdleIntervalSeconds

	insert into @actionNeeded (ID, ActionOnLimitExceeding)
	select C.ID, C.ActionOnLimitExceeding 
	from @candidates C
		join [{name}].[Messages2] M on C.NextToCompletedID = M.ID
	where datediff(second, M.Created, @LastWrite) > C.MaxIdleIntervalSeconds

	declare @i int = 1
	declare @maxRowID int = SCOPE_IDENTITY()
	declare @SubIDToAction int
	declare @ActionToTake int

	while (@i <= @maxRowID)
	begin
		select @SubIDToAction = ID, @ActionToTake = ActionOnLimitExceeding
		from @actionNeeded
		where RowID = @i

		if (@ActionToTake = 1)
			delete from [{name}].[Subscription] 
			where ID = @SubIDToAction
		else if (@ActionToTake = 2)
			update [{name}].[Subscription]
			set LockTime = null, LockToken = null, [Disabled] = 1
			where ID = @SubIDToAction

		set @i += 1
	end

end
--->>>

if (@NeedClean1 = 1)
begin
    declare @SubscriptionExists1 bit

    select top 1 @SubscriptionExists1 = 1 
    from [{name}].Subscription 
    where [Disabled] = 0 and LastCompletedID < @MaxID1   
    
    if (@SubscriptionExists1 is null) 
    begin
        delete from [{name}].[Messages1] -- заменить на truncate в следующем релизе sql server

        update [{name}].[State]
        set Modified = sysutcdatetime(), MinID1 = 0, MaxID1 = 0, Num1 = 0, NeedClean1 = 0
    end
end

if (@NeedClean2 = 1)
begin
    declare @SubscriptionExists2 bit

    select top 1 @SubscriptionExists2 = 1 
    from [{name}].Subscription 
    where [Disabled] = 0 and LastCompletedID < @MaxID2  
    
    if (@SubscriptionExists2 is null) 
    begin
        delete from [{name}].[Messages2] -- заменить на truncate в следующем релизе sql server

        update [{name}].[State]
        set Modified = sysutcdatetime(), MinID2 = 0, MaxID2 = 0, Num2 = 0, NeedClean2 = 0
    end
end

END

GO


{(alter ? "ALTER" : "CREATE")} PROCEDURE [{name}].[Write] 
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

select top 1 @IsFirstActive = IsFirstActive, @MaxID1 = MaxID1, @MaxID2 = MaxID2,
    @Num1 = Num1, @Num2 = Num2, @NeedClean1 = NeedClean1, @NeedClean2 = NeedClean2, 
    @MinNum = MinNum, @TresholdNum = TresholdNum
from [{name}].[State]

if (@MaxID1 is null)
begin
    exec [{name}].[RestoreState]

    select top 1 @IsFirstActive = IsFirstActive, @MaxID1 = MaxID1, @MaxID2 = MaxID2,
        @Num1 = Num1, @Num2 = Num2, @NeedClean1 = NeedClean1, @NeedClean2 = NeedClean2, 
        @MinNum = MinNum, @TresholdNum = TresholdNum
    from [{name}].[State]
end

-- всегда оставляем последнее сообщение
if (@MinNum < 1)
    set @MinNum = 1

-- если можем очистить другую таблицу, то помечаем для очистки
if (@IsFirstActive = 1 and @Num1 >= @MinNum and @MaxID2 > 0 and @NeedClean2 = 0)
begin
    update [{name}].[State]
    set Modified = @date, NeedClean2 = 1
end
else if (@IsFirstActive = 0 and @Num2 >= @MinNum and @MaxID1 > 0 and @NeedClean1 = 0)
begin
    update [{name}].[State]
    set Modified = @date, NeedClean1 = 1
end


-- если превысили количество сообщений и другая таблица свободна, то переключаемся
if (@IsFirstActive = 1 and @Num1 >= @TresholdNum and @MaxID2 = 0)
begin
    set @IsFirstActive = 0
    set @id = @MaxID1 + 1

    update [{name}].[State]
    set Modified = @date, LastWrite = @date, MinID2 = @id, MaxID2 = @id, Num2 = 1, IsFirstActive = 0

    set @stateUpdated = 1
end
else if (@IsFirstActive = 0 and @Num2 >= @TresholdNum and @MaxID1 = 0)
begin
    set @IsFirstActive = 1
    set @id = @MaxID2 + 1

    update [{name}].[State]
    set Modified = @date, LastWrite = @date, MinID1 = @id, MaxID1 = @id, Num1 = 1, IsFirstActive = 1

    set @stateUpdated = 1
end
  

if (@IsFirstActive = 1)
begin
    if (@stateUpdated = 0)
    begin
        set @id = @MaxID1 + 1

        update [{name}].[State]
	    set Modified = @date, LastWrite = @date, MaxID1 = @id, Num1 = @Num1 + 1
    end

    insert into [{name}].Messages1 (ID, Created, Body)
    values (@id, @date, @body)
end
else
begin
    if (@stateUpdated = 0)
    begin
        set @id = @MaxID2 + 1

        update [{name}].[State]
        set Modified = @date, LastWrite = @date, MaxID2 = @id, Num2 = @Num2 + 1
    end

    insert into [{name}].Messages2 (ID, Created, Body)
    values (@id, @date, @body)
end

END

GO


{(alter ? "ALTER" : "CREATE")} PROCEDURE [{name}].[WriteMany] 
    @messageList {name}.MessageList READONLY,
    @returnIDs bit
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

declare @lastID bigint

declare @cnt int = (select count(*) from @messageList)

if (@cnt = 0)
    return;

select top 1 @IsFirstActive = IsFirstActive, @MaxID1 = MaxID1, @MaxID2 = MaxID2,
    @Num1 = Num1, @Num2 = Num2, @NeedClean1 = NeedClean1, @NeedClean2 = NeedClean2, 
    @MinNum = MinNum, @TresholdNum = TresholdNum
from [{name}].[State]

if (@MaxID1 is null)
begin
    exec [{name}].[RestoreState]

    select top 1 @IsFirstActive = IsFirstActive, @MaxID1 = MaxID1, @MaxID2 = MaxID2,
        @Num1 = Num1, @Num2 = Num2, @NeedClean1 = NeedClean1, @NeedClean2 = NeedClean2, 
        @MinNum = MinNum, @TresholdNum = TresholdNum
    from [{name}].[State]
end

-- всегда оставляем последнее сообщение
if (@MinNum < 1)
    set @MinNum = 1

-- если можем очистить другую таблицу, то помечаем для очистки
if (@IsFirstActive = 1 and @Num1 >= @MinNum and @MaxID2 > 0 and @NeedClean2 = 0)
begin
    update [{name}].[State]
    set Modified = @date, NeedClean2 = 1
end
else if (@IsFirstActive = 0 and @Num2 >= @MinNum and @MaxID1 > 0 and @NeedClean1 = 0)
begin
    update [{name}].[State]
    set Modified = @date, NeedClean1 = 1
end


-- если превысили количество сообщений и другая таблица свободна, то переключаемся
if (@IsFirstActive = 1 and @Num1 >= @TresholdNum and @MaxID2 = 0)
begin
    set @IsFirstActive = 0
    set @lastID = @MaxID1

    update [{name}].[State]
    set Modified = @date, LastWrite = @date, MinID2 = @MaxID1 + 1, MaxID2 = @MaxID1 + @cnt, Num2 = @cnt, IsFirstActive = 0

    set @stateUpdated = 1
end
else if (@IsFirstActive = 0 and @Num2 >= @TresholdNum and @MaxID1 = 0)
begin
    set @IsFirstActive = 1
    set @lastID = @MaxID2

    update [{name}].[State]
    set Modified = @date, LastWrite = @date, MinID1 = @MaxID2 + 1, MaxID1 = @MaxID2 + @cnt, Num1 = @cnt, IsFirstActive = 1

    set @stateUpdated = 1
end


if (@IsFirstActive = 1)
begin
    if (@stateUpdated = 0)
    begin
        set @lastID = @MaxID1

        update [{name}].[State]
	    set Modified = @date, LastWrite = @date, MaxID1 = @MaxID1 + @cnt, Num1 = @Num1 + @cnt
    end

    insert into [{name}].Messages1 (ID, Created, Body)
    select @lastID + ID, @date, Body
    from @messageList
    order by ID
end
else
begin
    if (@stateUpdated = 0)
    begin
        set @lastID = @MaxID2

        update [{name}].[State]
        set Modified = @date, LastWrite = @date, MaxID2 = @MaxID2 + @cnt, Num2 = @Num2 + @cnt
    end

    insert into [{name}].Messages2 (ID, Created, Body)
    select @lastID + ID, @date, Body
    from @messageList
    order by ID
end

if (@returnIDs = 1)
begin
    select @lastID + ID
    from @messageList
    order by ID
end

END

GO


{(alter ? "ALTER" : "CREATE")} PROCEDURE [{name}].[GetSubscription]
  @subscriptionID int
  WITH NATIVE_COMPILATION, SCHEMABINDING, EXECUTE AS OWNER
  AS 
  BEGIN ATOMIC 
  WITH (TRANSACTION ISOLATION LEVEL = SNAPSHOT, LANGUAGE = N'us_english')

select [ID], [Name], [LastCompletedID], [LastCompletedTime], [LockTime], [LockToken], [Disabled]
from [{name}].[Subscription]
where ID = @subscriptionID

END

GO

{(alter ? "ALTER" : "CREATE")} PROCEDURE [{name}].[FindSubscription]
  @name nvarchar(255)
  WITH NATIVE_COMPILATION, SCHEMABINDING, EXECUTE AS OWNER
  AS 
  BEGIN ATOMIC 
  WITH (TRANSACTION ISOLATION LEVEL = SNAPSHOT, LANGUAGE = N'us_english')

select [ID], [Name], [LastCompletedID], [LastCompletedTime], [LockTime], [LockToken], [Disabled]
from [{name}].[Subscription]
where [Name] = @name

END

GO

{(alter ? "ALTER" : "CREATE")} PROCEDURE [{name}].[DisableSubscription] 
  @subscriptionID int
  WITH NATIVE_COMPILATION, SCHEMABINDING, EXECUTE AS OWNER
  AS 
  BEGIN ATOMIC 
  WITH (TRANSACTION ISOLATION LEVEL = SNAPSHOT, LANGUAGE = N'us_english')

update [{name}].[Subscription]
set LockTime = null, LockToken = null, [Disabled] = 1
where ID = @subscriptionID and [Disabled] = 0

END

GO


{(alter ? "ALTER" : "CREATE")} PROCEDURE [{name}].[DeleteSubscription]
  @subscriptionID int
  WITH NATIVE_COMPILATION, SCHEMABINDING, EXECUTE AS OWNER
  AS 
  BEGIN ATOMIC 
  WITH (TRANSACTION ISOLATION LEVEL = SNAPSHOT, LANGUAGE = N'us_english')

delete from [{name}].[Subscription]
where ID = @subscriptionID

END

GO


{(alter ? "ALTER" : "CREATE")} PROCEDURE [{name}].[EnableSubscription] 
  @subscriptionID int
  WITH NATIVE_COMPILATION, SCHEMABINDING, EXECUTE AS OWNER
  AS 
  BEGIN ATOMIC 
  WITH (TRANSACTION ISOLATION LEVEL = SNAPSHOT, LANGUAGE = N'us_english')


declare @MaxID1 int
declare @MaxID2 int
declare @IsFirstActive bit

select top 1 @MaxID1 = MaxID1, @MaxID2 = MaxID2, @IsFirstActive = IsFirstActive
from [{name}].[State]

if (@MaxID1 is null)
begin
    exec [{name}].[RestoreState]

    select top 1 @MaxID1 = MaxID1, @MaxID2 = MaxID2, @IsFirstActive = IsFirstActive
    from [{name}].[State]
end


if (@IsFirstActive = 1)
    update [{name}].[Subscription]
    set LastCompletedID = @MaxID1, LastCompletedTime = sysutcdatetime(), LockTime = null, LockToken = null, [Disabled] = 0
    where ID = @subscriptionID and [Disabled] = 1
else
    update [{name}].[Subscription]
    set LastCompletedID = @MaxID2, LastCompletedTime = sysutcdatetime(), LockTime = null, LockToken = null, [Disabled] = 0
    where ID = @subscriptionID and [Disabled] = 1

END


GO


{(alter ? "ALTER" : "CREATE")} PROCEDURE [{name}].[CreateSubscription]
  @name nvarchar(255),
  @maxIdleIntervalSeconds int = null,
  @maxUncompletedMessages int = null,
  @actionOnLimitExceeding int = null,
  @subscriptionID int out
  WITH NATIVE_COMPILATION, SCHEMABINDING, EXECUTE AS OWNER
  AS 
  BEGIN ATOMIC 
  WITH (TRANSACTION ISOLATION LEVEL = SNAPSHOT, LANGUAGE = N'us_english')


declare @MaxID1 int
declare @MaxID2 int
declare @IsFirstActive bit

select top 1 @MaxID1 = MaxID1, @MaxID2 = MaxID2, @IsFirstActive = IsFirstActive
from [{name}].[State]

if (@MaxID1 is null)
begin
    exec [{name}].[RestoreState]

    select top 1 @MaxID1 = MaxID1, @MaxID2 = MaxID2, @IsFirstActive = IsFirstActive
    from [{name}].[State]
end


if (@IsFirstActive = 1)
    insert into [{name}].[Subscription] ([Name], [LastCompletedID], [LastCompletedTime], [Disabled],
		[MaxIdleIntervalSeconds], [MaxUncompletedMessages], [ActionOnLimitExceeding])
    values (@name, @MaxID1, sysutcdatetime(), 0, @maxIdleIntervalSeconds, @maxUncompletedMessages, @actionOnLimitExceeding)
else
    insert into [{name}].[Subscription] ([Name], [LastCompletedID], [LastCompletedTime], [Disabled],
		[MaxIdleIntervalSeconds], [MaxUncompletedMessages], [ActionOnLimitExceeding])
    values (@name, @MaxID2, sysutcdatetime(), 0, @maxIdleIntervalSeconds, @maxUncompletedMessages, @actionOnLimitExceeding)

set @subscriptionID = scope_identity()

END


GO


{(alter ? "ALTER" : "CREATE")} PROCEDURE [{name}].[GetSubscriptionInfo] 
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
from [{name}].[State]

if (@MaxID1 is null)
begin
    exec [{name}].[RestoreState]

    select top 1 @Modified = Modified, @IsFirstActive = IsFirstActive, @MaxID1 = MaxID1, @MaxID2 = MaxID2
    from [{name}].[State]
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
from [{name}].[Subscription]
where ID = @subscriptionID

END

GO


{(alter ? "ALTER" : "CREATE")} PROCEDURE [{name}].[GetAllSubscriptionsInfo] 
  WITH NATIVE_COMPILATION, SCHEMABINDING, EXECUTE AS OWNER
  AS 
  BEGIN ATOMIC 
  WITH (TRANSACTION ISOLATION LEVEL = SNAPSHOT, LANGUAGE = N'us_english')


declare @Modified datetime2(7)
declare @IsFirstActive bit
declare @MaxID1 bigint
declare @MaxID2 bigint

select top 1 @Modified = Modified, @IsFirstActive = IsFirstActive, @MaxID1 = MaxID1, @MaxID2 = MaxID2
from [{name}].[State]

if (@MaxID1 is null)
begin
    exec [{name}].[RestoreState]

    select top 1 @Modified = Modified, @IsFirstActive = IsFirstActive, @MaxID1 = MaxID1, @MaxID2 = MaxID2
    from [{name}].[State]
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
from [{name}].[Subscription]

END

GO


{(alter ? "ALTER" : "CREATE")} PROCEDURE [{name}].[SetSubscriptionSettings] 
  @subscriptionID int,
  @maxIdleIntervalSeconds int,
  @maxUncompletedMessages int,
  @actionOnLimitExceeding int
  WITH NATIVE_COMPILATION, SCHEMABINDING, EXECUTE AS OWNER
  AS 
  BEGIN ATOMIC 
  WITH (TRANSACTION ISOLATION LEVEL = SNAPSHOT, LANGUAGE = N'us_english')

update [{name}].[Subscription]
set [MaxIdleIntervalSeconds] = @maxIdleIntervalSeconds, 
	[MaxUncompletedMessages] = @maxUncompletedMessages, 
	[ActionOnLimitExceeding] = @actionOnLimitExceeding
where [ID] = @subscriptionID

END

GO

";
        }
    }
}
