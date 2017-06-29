



CREATE PROCEDURE [Queue_Schema_Name].[DisableSubscription] 
  @subscriptionID int
  WITH NATIVE_COMPILATION, SCHEMABINDING, EXECUTE AS OWNER
  AS 
  BEGIN ATOMIC 
  WITH (TRANSACTION ISOLATION LEVEL = SNAPSHOT, LANGUAGE = N'us_english')

update [Queue_Schema_Name].[Subscription]
set LockTime = null, LockToken = null, [Disabled] = 1
where ID = @subscriptionID and [Disabled] = 0

END