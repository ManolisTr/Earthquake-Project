CREATE TABLE [dbo].[JobRuns] (
    [run_id]         INT           IDENTITY (1, 1) NOT NULL,
    [job_id]         INT           NOT NULL,
    [start_time]     DATETIME      NOT NULL,
    [end_time]       DATETIME      NULL,
    [status]         VARCHAR (50)  NOT NULL,
    [execution_time] FLOAT (53)    NULL,
    [trigger_type]   VARCHAR (50)  NULL,
    [log_file_path]  VARCHAR (500) NULL
);


GO

CREATE NONCLUSTERED INDEX [idx_jobruns_status]
    ON [dbo].[JobRuns]([status] ASC);


GO

