



CREATE PROCEDURE [Queue_Schema_Name].[DeleteSubscription]
  @subscriptionID int
  WITH NATIVE_COMPILATION, SCHEMABINDING, EXECUTE AS OWNER
  AS 
  BEGIN ATOMIC 
  WITH (TRANSACTION ISOLATION LEVEL = SNAPSHOT, LANGUAGE = N'us_english')

delete from [Queue_Schema_Name].[Subscription]
where ID = @subscriptionID

END