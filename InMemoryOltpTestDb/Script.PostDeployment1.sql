/*
Post-Deployment Script Template							
--------------------------------------------------------------------------------------
 This file contains SQL statements that will be appended to the build script.		
 Use SQLCMD syntax to include a file in the post-deployment script.			
 Example:      :r .\myfile.sql								
 Use SQLCMD syntax to reference a variable in the post-deployment script.		
 Example:      :setvar TableName MyTable							
               SELECT * FROM [$(TableName)]					
--------------------------------------------------------------------------------------
*/

--ALTER DATABASE CURRENT SET RECOVERY SIMPLE 
--GO

ALTER DATABASE CURRENT SET DELAYED_DURABILITY = DISABLED 
GO

INSERT INTO [Queue_Schema_Name].[Settings] ([ID], [MinNum], [TresholdNum])
VALUES (1, 10000, 100000)
