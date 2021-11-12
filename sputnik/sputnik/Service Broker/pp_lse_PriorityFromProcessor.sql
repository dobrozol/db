CREATE BROKER PRIORITY [pp_lse_PriorityFromProcessor] FOR CONVERSATION
    SET  (
            CONTRACT_NAME = [pp_lse_Contract],
            LOCAL_SERVICE_NAME = [pp_Processor],
            REMOTE_SERVICE_NAME = N'pp_Commander',
            PRIORITY_LEVEL = 3
         );

