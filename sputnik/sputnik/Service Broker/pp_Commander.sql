CREATE SERVICE [pp_Commander]
    AUTHORIZATION [dbo]
    ON QUEUE [pp].[CommanderQueue]
    ([pp_lse_Contract], [pp_back_Contract]);

