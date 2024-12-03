CREATE TABLE [dbo].[Alerts] (
    [alert_id]   INT           IDENTITY (1, 1) NOT NULL,
    [run_id]     INT           NOT NULL,
    [task_id]    VARCHAR (255) NULL,
    [alert_time] DATETIME      DEFAULT (getdate()) NOT NULL,
    [severity]   VARCHAR (50)  NOT NULL,
    [message]    VARCHAR (MAX) NOT NULL,
    [resolved]   BIT           DEFAULT ((0)) NOT NULL
);


GO

CREATE NONCLUSTERED INDEX [idx_alerts_severity]
    ON [dbo].[Alerts]([severity] ASC);


GO

