CREATE TABLE [dbo].[Jobs] (
    [job_id]          INT           IDENTITY (1, 1) NOT NULL,
    [job_name]        VARCHAR (255) NOT NULL,
    [schedule]        VARCHAR (255) NOT NULL,
    [owner]           VARCHAR (255) NOT NULL,
    [created_at]      DATETIME      DEFAULT (getdate()) NOT NULL,
    [last_updated_at] DATETIME      NULL
);


GO

CREATE NONCLUSTERED INDEX [idx_job_name]
    ON [dbo].[Jobs]([job_name] ASC);


GO

