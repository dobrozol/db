CREATE BROKER PRIORITY [pp_lse_PriorityFromCommander] FOR CONVERSATION
    SET  (
            CONTRACT_NAME = [pp_lse_Contract],
            LOCAL_SERVICE_NAME = [pp_Commander],
            REMOTE_SERVICE_NAME = N'pp_Processor',
            PRIORITY_LEVEL = 3
         );

