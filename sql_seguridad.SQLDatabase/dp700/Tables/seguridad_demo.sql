CREATE TABLE [dp700].[seguridad_demo] (
    [user_id]            INT                                                                      NULL,
    [full_name]          VARCHAR (100)                                                            NULL,
    [short_text]         VARCHAR (3) MASKED WITH (FUNCTION = 'default()')                         NULL,
    [notes]              VARCHAR (100) MASKED WITH (FUNCTION = 'default()')                       NULL,
    [email_address]      VARCHAR (100) MASKED WITH (FUNCTION = 'email()')                         NULL,
    [credit_card_number] VARCHAR (20) MASKED WITH (FUNCTION = 'partial(0, "XXXX-XXXX-XXXX-", 4)') NULL,
    [custom_id]          VARCHAR (20) MASKED WITH (FUNCTION = 'partial(2, "-MASKED-", 2)')        NULL,
    [random_code]        VARCHAR (20) MASKED WITH (FUNCTION = 'partial(0, "XXXXXX", 0)')          NULL,
    [salary]             DECIMAL (10, 2) MASKED WITH (FUNCTION = 'default()')                     NULL,
    [birth_date]         DATE MASKED WITH (FUNCTION = 'default()')                                NULL,
    [access_code]        VARBINARY (8) MASKED WITH (FUNCTION = 'default()')                       NULL,
    [random_number]      INT MASKED WITH (FUNCTION = 'random(1000, 9999)')                        NULL
);


GO

