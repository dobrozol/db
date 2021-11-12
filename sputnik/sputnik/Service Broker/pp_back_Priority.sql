CREATE BROKER PRIORITY [pp_back_Priority] FOR CONVERSATION
    SET  (
            CONTRACT_NAME = [pp_back_Contract],
            LOCAL_SERVICE_NAME = ANY,
            REMOTE_SERVICE_NAME = ANY,
            PRIORITY_LEVEL = 8
         );

