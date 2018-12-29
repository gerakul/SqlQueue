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
    }
}
