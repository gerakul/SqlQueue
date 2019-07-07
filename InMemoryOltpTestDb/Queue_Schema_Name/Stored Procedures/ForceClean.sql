

CREATE PROCEDURE [Queue_Schema_Name].[ForceClean]
AS
BEGIN
	SET NOCOUNT ON;


-- make sure the procedure has executed successfully
-- if not then execute this procedure again
-- otherwise Queue will not work

update [Queue_Schema_Name].[State]
set [ForceCleanInAction] = 1

exec [Queue_Schema_Name].[Clean]

update [Queue_Schema_Name].[State]
set [ForceCleanInAction] = 0

END