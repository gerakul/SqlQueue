


CREATE PROCEDURE [Queue_Schema_Name].[SetSubscriptionSettings] 
  @subscriptionID int,
  @maxIdleIntervalSeconds int,
  @maxUncompletedMessages int,
  @actionOnLimitExceeding int
  WITH NATIVE_COMPILATION, SCHEMABINDING, EXECUTE AS OWNER
  AS 
  BEGIN ATOMIC 
  WITH (TRANSACTION ISOLATION LEVEL = SNAPSHOT, LANGUAGE = N'us_english')

update [Queue_Schema_Name].[Subscription]
set [MaxIdleIntervalSeconds] = @maxIdleIntervalSeconds, 
	[MaxUncompletedMessages] = @maxUncompletedMessages, 
	[ActionOnLimitExceeding] = @actionOnLimitExceeding
where [ID] = @subscriptionID

END