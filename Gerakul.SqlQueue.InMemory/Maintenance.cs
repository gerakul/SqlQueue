using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Text;

namespace Gerakul.SqlQueue.InMemory
{
    public class Maintenance
    {
        private string connectionString;
        private string queueName;

        public static void UpdateFrom_1_2_0_To_1_3_0(string connectionString, string queueName)
        {
            string script = $@"

ALTER TABLE [{queueName}].[Subscription]
    ADD [MaxIdleIntervalSeconds] INT NULL,
        [MaxUncompletedMessages] INT NULL,
        [ActionOnLimitExceeding] INT NULL;


GO

ALTER PROCEDURE [{queueName}].[Clean]
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
from [{queueName}].[State]

if (@MaxID1 is null)
begin
    exec [{queueName}].[RestoreState]

    select top 1 @Modified = Modified, @IsFirstActive = IsFirstActive,
		@MaxID1 = MaxID1, @NeedClean1 = NeedClean1, @MaxID2 = MaxID2, @NeedClean2 = NeedClean2
    from [{queueName}].[State]
end

declare @maxID bigint

if (@IsFirstActive = 1)
	set @maxID = @MaxID1
else 
	set @maxID = @MaxID2

delete from [{queueName}].Subscription
where ActionOnLimitExceeding = 1 
	and ((@maxID - LastCompletedID) > MaxUncompletedMessages 
		or datediff(second, LastCompletedTime, @Modified) > MaxIdleIntervalSeconds)

update [{queueName}].Subscription
set LockTime = null, LockToken = null, [Disabled] = 1
where ActionOnLimitExceeding = 2 and [Disabled] = 0 
	and ((@maxID - LastCompletedID) > MaxUncompletedMessages 
		or datediff(second, LastCompletedTime, @Modified) > MaxIdleIntervalSeconds)

if (@NeedClean1 = 1)
begin
    declare @SubscriptionExists1 bit

    select top 1 @SubscriptionExists1 = 1 
    from [{queueName}].Subscription 
    where [Disabled] = 0 and LastCompletedID < @MaxID1   
    
    if (@SubscriptionExists1 is null) 
    begin
        delete from [{queueName}].[Messages1] -- заменить на truncate в следующем релизе sql server

        update [{queueName}].[State]
        set Modified = sysutcdatetime(), MinID1 = 0, MaxID1 = 0, Num1 = 0, NeedClean1 = 0
    end
end

if (@NeedClean2 = 1)
begin
    declare @SubscriptionExists2 bit

    select top 1 @SubscriptionExists2 = 1 
    from [{queueName}].Subscription 
    where [Disabled] = 0 and LastCompletedID < @MaxID2  
    
    if (@SubscriptionExists2 is null) 
    begin
        delete from [{queueName}].[Messages2] -- заменить на truncate в следующем релизе sql server

        update [{queueName}].[State]
        set Modified = sysutcdatetime(), MinID2 = 0, MaxID2 = 0, Num2 = 0, NeedClean2 = 0
    end
end


END

GO


ALTER PROCEDURE [{queueName}].[CreateSubscription]
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
from [{queueName}].[State]

if (@MaxID1 is null)
begin
    exec [{queueName}].[RestoreState]

    select top 1 @MaxID1 = MaxID1, @MaxID2 = MaxID2, @IsFirstActive = IsFirstActive
    from [{queueName}].[State]
end


if (@IsFirstActive = 1)
    insert into [{queueName}].[Subscription] ([Name], [LastCompletedID], [LastCompletedTime], [Disabled],
		[MaxIdleIntervalSeconds], [MaxUncompletedMessages], [ActionOnLimitExceeding])
    values (@name, @MaxID1, sysutcdatetime(), 0, @maxIdleIntervalSeconds, @maxUncompletedMessages, @actionOnLimitExceeding)
else
    insert into [{queueName}].[Subscription] ([Name], [LastCompletedID], [LastCompletedTime], [Disabled],
		[MaxIdleIntervalSeconds], [MaxUncompletedMessages], [ActionOnLimitExceeding])
    values (@name, @MaxID2, sysutcdatetime(), 0, @maxIdleIntervalSeconds, @maxUncompletedMessages, @actionOnLimitExceeding)

set @subscriptionID = scope_identity()

END


GO


CREATE PROCEDURE [{queueName}].[GetAllSubscriptionsInfo] 
  WITH NATIVE_COMPILATION, SCHEMABINDING, EXECUTE AS OWNER
  AS 
  BEGIN ATOMIC 
  WITH (TRANSACTION ISOLATION LEVEL = SNAPSHOT, LANGUAGE = N'us_english')


declare @Modified datetime2(7)
declare @IsFirstActive bit
declare @MaxID1 bigint
declare @MaxID2 bigint

select top 1 @Modified = Modified, @IsFirstActive = IsFirstActive, @MaxID1 = MaxID1, @MaxID2 = MaxID2
from [{queueName}].[State]

if (@MaxID1 is null)
begin
    exec [{queueName}].[RestoreState]

    select top 1 @Modified = Modified, @IsFirstActive = IsFirstActive, @MaxID1 = MaxID1, @MaxID2 = MaxID2
    from [{queueName}].[State]
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
from [{queueName}].[Subscription]

END


GO


CREATE PROCEDURE [{queueName}].[GetSubscriptionInfo] 
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
from [{queueName}].[State]

if (@MaxID1 is null)
begin
    exec [{queueName}].[RestoreState]

    select top 1 @Modified = Modified, @IsFirstActive = IsFirstActive, @MaxID1 = MaxID1, @MaxID2 = MaxID2
    from [{queueName}].[State]
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
from [{queueName}].[Subscription]
where ID = @subscriptionID

END


GO



CREATE PROCEDURE [{queueName}].[SetSubscriptionSettings] 
  @subscriptionID int,
  @maxIdleIntervalSeconds int,
  @maxUncompletedMessages int,
  @actionOnLimitExceeding int
  WITH NATIVE_COMPILATION, SCHEMABINDING, EXECUTE AS OWNER
  AS 
  BEGIN ATOMIC 
  WITH (TRANSACTION ISOLATION LEVEL = SNAPSHOT, LANGUAGE = N'us_english')

update [{queueName}].[Subscription]
set [MaxIdleIntervalSeconds] = @maxIdleIntervalSeconds, 
	[MaxUncompletedMessages] = @maxUncompletedMessages, 
	[ActionOnLimitExceeding] = @actionOnLimitExceeding
where [ID] = @subscriptionID

END


GO

";

            using (var conn = new SqlConnection(connectionString))
            {
                conn.Open();

                Helper.ExecuteBatches(conn, script);
            }
        }

        public Maintenance(string connectionString, string queueName)
        {
            this.connectionString = connectionString;
            this.queueName = queueName;
        }

        public void AlterMainProcedures()
        {
            using (var conn = new SqlConnection(connectionString))
            {
                conn.Open();

                Helper.ExecuteBatches(conn, QueueFactory.GetMainProceduresScript(queueName, true));
            }
        }

        public void DropAllProcedures()
        {
            using (var conn = new SqlConnection(connectionString))
            {
                conn.Open();

                Helper.ExecuteBatches(conn, QueueFactory.GetProceduresDeletionScript(queueName));
            }
        }

        public void CreateAllProcedures()
        {
            using (var conn = new SqlConnection(connectionString))
            {
                conn.Open();

                Helper.ExecuteBatches(conn, QueueFactory.GetProceduresCreationScript(queueName));
            }
        }

        public void DropAndCreateAllProcedures()
        {
            using (var conn = new SqlConnection(connectionString))
            {
                conn.Open();

                Helper.ExecuteBatches(conn, QueueFactory.GetProceduresDeletionScript(queueName));
                Helper.ExecuteBatches(conn, QueueFactory.GetProceduresCreationScript(queueName));
            }
        }

        public void FullReset()
        {
            using (var conn = new SqlConnection(connectionString))
            {
                conn.Open();

                var stage = GetStage(conn);

                if (stage != 2)
                {
                    // stage1
                    Helper.ExecuteBatches(conn, GetMntStageCreationScript());
                    Helper.ExecuteBatches(conn, QueueFactory.GetProceduresDeletionScript(queueName));
                    Helper.ExecuteBatches(conn, GetTmpTablesDeletionScript());
                    Helper.ExecuteBatches(conn, GetTmpTablesCreationScript());
                    Helper.ExecuteBatches(conn, GetDataCopyToTmpScript());
                    Helper.ExecuteBatches(conn, GetUpdateStageScript(2));
                }

                // stage2
                Helper.ExecuteBatches(conn, QueueFactory.GetObjectsDeletionScript(queueName));
                Helper.ExecuteBatches(conn, QueueFactory.GetObjectsCreationScript(queueName));
                Helper.ExecuteBatches(conn, GetDataCopyFromTmpScript());
                Helper.ExecuteBatches(conn, GetUpdateStageScript(3));

                // stage3
                Helper.ExecuteBatches(conn, GetTmpTablesDeletionScript());
                Helper.ExecuteBatches(conn, QueueFactory.GetProceduresCreationScript(queueName));
                Helper.ExecuteBatches(conn, GetMntStageDeletionScript());
            }
        }

        private int GetStage(SqlConnection conn)
        {
            var script = $@"
IF EXISTS (SELECT TOP 1 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'MntStage' AND TABLE_SCHEMA = '{queueName}')
    select ID from  [{queueName}].[MntStage];
ELSE
    select 0;
";

            SqlCommand cmd = new SqlCommand(script, conn);

            using (var r = cmd.ExecuteReader())
            {
                r.Read();
                return r.GetInt32(0);
            }
        }

        private string GetMntStageDeletionScript()
        {
            return $@"
DROP TABLE [{queueName}].[MntStage]

GO
";
        }

        private string GetMntStageCreationScript()
        {
            return $@"

IF EXISTS (SELECT TOP 1 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'MntStage' AND TABLE_SCHEMA = '{queueName}')
    DROP TABLE [{queueName}].[MntStage]

GO

CREATE TABLE [{queueName}].[MntStage](
	[ID] [int] NOT NULL,
 CONSTRAINT [PK_MntStage] PRIMARY KEY CLUSTERED 
(
	[ID] ASC
)
) 

GO

insert into [{queueName}].[MntStage] ([ID])
values (1)

";
        }

        private string GetUpdateStageScript(int stage)
        {
            return $@"
update [{queueName}].[MntStage]
set ID = {stage}
";
        }

        private string GetTmpTablesDeletionScript()
        {
            return $@"

IF EXISTS (SELECT TOP 1 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'TmpSettings' AND TABLE_SCHEMA = '{queueName}')
    DROP TABLE [{queueName}].[TmpSettings]
GO

IF EXISTS (SELECT TOP 1 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'TmpSubscription' AND TABLE_SCHEMA = '{queueName}')
    DROP TABLE [{queueName}].[TmpSubscription]
GO

IF EXISTS (SELECT TOP 1 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'TmpMessages1' AND TABLE_SCHEMA = '{queueName}')
    DROP TABLE [{queueName}].[TmpMessages1]
GO

IF EXISTS (SELECT TOP 1 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'TmpMessages2' AND TABLE_SCHEMA = '{queueName}')
    DROP TABLE [{queueName}].[TmpMessages2]
GO

";
        }

        private string GetTmpTablesCreationScript()
        {
            return $@"

CREATE TABLE [{queueName}].[TmpSettings](
	[ID] [bigint] NOT NULL,
	[MinNum] [int] NOT NULL,
	[TresholdNum] [int] NOT NULL,
 CONSTRAINT [PK_TmpSettings] PRIMARY KEY CLUSTERED 
(
	[ID] ASC
)
) 

GO


CREATE TABLE [{queueName}].[TmpSubscription](
	[ID] [int] NOT NULL,
	[Name] [nvarchar](255) NOT NULL,
	[LastCompletedID] [bigint] NOT NULL,
	[LastCompletedTime] [datetime2](7) NOT NULL,
	[LockTime] [datetime2](7) NULL,
	[LockToken] [uniqueidentifier] NULL,
	[Disabled] [bit] NOT NULL,
	[MaxIdleIntervalSeconds] [int] NULL,
	[MaxUncompletedMessages] [int] NULL,
	[ActionOnLimitExceeding] [int] NULL,
 CONSTRAINT [PK_TmpSubscription] PRIMARY KEY CLUSTERED 
(
	[ID] ASC
)
)

GO


CREATE TABLE [{queueName}].[TmpMessages1](
	[ID] [bigint] NOT NULL,
	[Created] [datetime2](7) NOT NULL,
	[Body] [varbinary](8000) NOT NULL,
 CONSTRAINT [PK_TmpMessages1] PRIMARY KEY CLUSTERED 
(
	[ID] ASC
)
)

GO


CREATE TABLE [{queueName}].[TmpMessages2](
	[ID] [bigint] NOT NULL,
	[Created] [datetime2](7) NOT NULL,
	[Body] [varbinary](8000) NOT NULL,
 CONSTRAINT [PK_TmpMessages2] PRIMARY KEY CLUSTERED 
(
	[ID] ASC
)
)

GO

";
        }

        private string GetDataCopyToTmpScript()
        {
            return $@"

insert into [{queueName}].[TmpSettings] ([ID], [MinNum], [TresholdNum])
select [ID], [MinNum], [TresholdNum]
from [{queueName}].[Settings]

insert into [{queueName}].[TmpSubscription] ([ID]
      ,[Name]
      ,[LastCompletedID]
      ,[LastCompletedTime]
      ,[LockTime]
      ,[LockToken]
      ,[Disabled]
      ,[MaxIdleIntervalSeconds]
      ,[MaxUncompletedMessages]
      ,[ActionOnLimitExceeding])
select [ID]
      ,[Name]
      ,[LastCompletedID]
      ,[LastCompletedTime]
      ,[LockTime]
      ,[LockToken]
      ,[Disabled]
      ,[MaxIdleIntervalSeconds]
      ,[MaxUncompletedMessages]
      ,[ActionOnLimitExceeding]
from [{queueName}].[Subscription]


insert into [{queueName}].[TmpMessages1] ([ID], [Created], [Body])
select [ID], [Created], [Body]
from [{queueName}].[Messages1]

insert into [{queueName}].[TmpMessages2] ([ID], [Created], [Body])
select [ID], [Created], [Body]
from [{queueName}].[Messages2]

GO

";
        }

        private string GetDataCopyFromTmpScript()
        {
            return $@"

insert into [{queueName}].[Settings] ([ID], [MinNum], [TresholdNum])
select [ID], [MinNum], [TresholdNum]
from [{queueName}].[TmpSettings]

set identity_insert [{queueName}].[Subscription] on;

insert into [{queueName}].[Subscription] ([ID]
      ,[Name]
      ,[LastCompletedID]
      ,[LastCompletedTime]
      ,[LockTime]
      ,[LockToken]
      ,[Disabled]
      ,[MaxIdleIntervalSeconds]
      ,[MaxUncompletedMessages]
      ,[ActionOnLimitExceeding])
select [ID]
      ,[Name]
      ,[LastCompletedID]
      ,[LastCompletedTime]
      ,[LockTime]
      ,[LockToken]
      ,[Disabled]
      ,[MaxIdleIntervalSeconds]
      ,[MaxUncompletedMessages]
      ,[ActionOnLimitExceeding]
from [{queueName}].[TmpSubscription]

set identity_insert [{queueName}].[Subscription] off;

insert into [{queueName}].[Messages1] ([ID], [Created], [Body])
select [ID], [Created], [Body]
from [{queueName}].[TmpMessages1]

insert into [{queueName}].[Messages2] ([ID], [Created], [Body])
select [ID], [Created], [Body]
from [{queueName}].[TmpMessages2]

GO

";
        }
    }
}
