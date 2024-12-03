CREATE TABLE [dbo].[TaskRuns] (
    [task_run_id]    INT           IDENTITY (1, 1) NOT NULL,
    [run_id]         INT           NOT NULL,
    [task_id]        VARCHAR (255) NOT NULL,
    [start_time]     DATETIME      NOT NULL,
    [end_time]       DATETIME      NULL,
    [status]         VARCHAR (50)  NOT NULL,
    [execution_time] FLOAT (53)    NULL
);


GO

CREATE NONCLUSTERED INDEX [idx_taskruns_task_id]
    ON [dbo].[TaskRuns]([task_id] ASC);


GO

