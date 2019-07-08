using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Text;

namespace Gerakul.SqlQueue.InMemory
{
    public class Maintenance
    {
        private class Stage
        {
            public int ID;
            public bool SchemaOnlyDurability;
        }

        private string connectionString;
        private string queueName;

        public static void UpdateToLatest(string connectionString, string queueName)
        {
            var m = new Maintenance(connectionString, queueName);
            m.FullReset();
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

                Helper.ExecuteBatches(conn, QueueFactory.GetMainProceduresScript(queueName, true, false));
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

                Helper.ExecuteBatches(conn, QueueFactory.GetProceduresCreationScript(queueName, false));
            }
        }

        public void DropAndCreateAllProcedures(bool execClean = false)
        {
            using (var conn = new SqlConnection(connectionString))
            {
                conn.Open();

                Helper.ExecuteBatches(conn, QueueFactory.GetProceduresDeletionScript(queueName));
                Helper.ExecuteBatches(conn, QueueFactory.GetProceduresCreationScript(queueName, execClean));
            }
        }

        public void FullReset(bool? schemaOnlyDurability = null)
        {
            using (var conn = new SqlConnection(connectionString))
            {
                conn.Open();

                var stage = GetStage(conn);

                if (stage.ID != 2)
                {
                    // stage1
                    Helper.ExecuteBatches(conn, GetMntStageCreationScript());
                    stage = GetStage(conn);
                    Helper.ExecuteBatches(conn, QueueFactory.GetProceduresDeletionScript(queueName));
                    Helper.ExecuteBatches(conn, GetTmpTablesDeletionScript());
                    Helper.ExecuteBatches(conn, GetTmpTablesCreationScript());
                    Helper.ExecuteBatches(conn, GetDataCopyToTmpScript());
                    Helper.ExecuteBatches(conn, GetUpdateStageScript(2));
                }

                // stage2
                Helper.ExecuteBatches(conn, QueueFactory.GetObjectsDeletionScript(queueName));
                Helper.ExecuteBatches(conn, QueueFactory.GetObjectsCreationScript(queueName, schemaOnlyDurability ?? stage.SchemaOnlyDurability));
                Helper.ExecuteBatches(conn, GetDataCopyFromTmpScript());
                Helper.ExecuteBatches(conn, GetUpdateStageScript(3));

                // stage3
                Helper.ExecuteBatches(conn, GetTmpTablesDeletionScript());
                InsertGlobal(conn);
                Helper.ExecuteBatches(conn, QueueFactory.GetProceduresCreationScript(queueName, true));
                Helper.ExecuteBatches(conn, GetMntStageDeletionScript());
            }
        }

        private bool DetermineSchemaOnlyDurability(SqlConnection conn)
        {
            var script = $@"
select [durability] 
from [sys].[tables] t
	join [sys].[schemas] s on t.[schema_id] = s.[schema_id]
		and s.[name] = '{queueName}'
where t.[name] = 'Messages0'
";

            SqlCommand cmd = new SqlCommand(script, conn);
            using (var r = cmd.ExecuteReader())
            {
                if (r.Read())
                {
                    return r.GetByte(0) == 1;
                }
                else
                {
                    return false;
                }
            }
        }

        private void InsertGlobal(SqlConnection conn)
        {
            var cmd = new SqlCommand($@"
INSERT INTO [{queueName}].[Global] ([ID], [Version])
VALUES (1, @Version)
", conn);

            cmd.Parameters.AddWithValue("Version", QueueFactory.Version);
            cmd.ExecuteNonQuery();
        }

        private void UpdateGlobal(SqlConnection conn)
        {
            var cmd = new SqlCommand($@"
UPDATE [{queueName}].[Global] 
SET Version = @Version
WHERE ID = 1
", conn);

            cmd.Parameters.AddWithValue("Version", QueueFactory.Version);
            cmd.ExecuteNonQuery();
        }

        private Stage GetStage(SqlConnection conn)
        {
            var script = $@"
IF EXISTS (SELECT TOP 1 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'MntStage' AND TABLE_SCHEMA = '{queueName}')
    select ID, SchemaOnlyDurability from  [{queueName}].[MntStage];
ELSE
    select 0, cast(0 as bit);
";

            SqlCommand cmd = new SqlCommand(script, conn);

            using (var r = cmd.ExecuteReader())
            {
                r.Read();
                return new Stage
                {
                    ID = r.GetInt32(0),
                    SchemaOnlyDurability = r.GetBoolean(1)
                };
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
	[SchemaOnlyDurability] [bit] NOT NULL,
 CONSTRAINT [PK_MntStage] PRIMARY KEY CLUSTERED 
(
	[ID] ASC
)
) 

GO

declare @schemaOnlyDurability bit

select @schemaOnlyDurability = cast(iif(t.durability = 1, 1, 0) as bit)
from sys.tables t
	join sys.schemas s on t.schema_id = s.schema_id
		and s.name = '{queueName}'
where t.name = 'Messages1'

insert into [{queueName}].[MntStage] ([ID], [SchemaOnlyDurability])
values (1, @schemaOnlyDurability)

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
from [{queueName}].[Settings] WITH (SNAPSHOT)

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
from [{queueName}].[Subscription] WITH (SNAPSHOT)


insert into [{queueName}].[TmpMessages1] ([ID], [Created], [Body])
select [ID], [Created], [Body]
from [{queueName}].[Messages1] WITH (SNAPSHOT)

insert into [{queueName}].[TmpMessages2] ([ID], [Created], [Body])
select [ID], [Created], [Body]
from [{queueName}].[Messages2] WITH (SNAPSHOT)


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
