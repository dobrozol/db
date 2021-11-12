CREATE SERVICE [pp_Processor]
    AUTHORIZATION [dbo]
    ON QUEUE [pp].[ProcessorQueue]
    ([pp_lse_Contract], [pp_back_Contract]);

