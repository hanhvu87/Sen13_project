/* ============================
   DROP THEO THỨ TỰ PHỤ THUỘC
   ============================ */
IF OBJECT_ID('dbo.Price_M1','U')  IS NOT NULL DROP TABLE dbo.Price_M1;
IF OBJECT_ID('dbo.Price_M5','U')  IS NOT NULL DROP TABLE dbo.Price_M5;
IF OBJECT_ID('dbo.Price_M15','U') IS NOT NULL DROP TABLE dbo.Price_M15;
IF OBJECT_ID('dbo.Price_M30','U') IS NOT NULL DROP TABLE dbo.Price_M30;
IF OBJECT_ID('dbo.Price_H1','U')  IS NOT NULL DROP TABLE dbo.Price_H1;
IF OBJECT_ID('dbo.Price_H4','U')  IS NOT NULL DROP TABLE dbo.Price_H4;
IF OBJECT_ID('dbo.Price_1D','U') IS NOT NULL DROP TABLE dbo.Price_1D;
IF OBJECT_ID('dbo.Price_1W','U') IS NOT NULL DROP TABLE dbo.Price_1W;

IF OBJECT_ID('dbo.timeframe','U') IS NOT NULL DROP TABLE dbo.timeframe;
IF OBJECT_ID('dbo.symbol','U')    IS NOT NULL DROP TABLE dbo.symbol;


/* ============================
   DIMENSIONS
   ============================ */
CREATE TABLE dbo.symbol (
    symbol_id     INT IDENTITY(1,1) PRIMARY KEY,
    symbol_name   NVARCHAR(32) NOT NULL,
    Refname       NVARCHAR(32) NULL,
    [type]        NVARCHAR(20) NULL,                -- forex, crypto, stock
    active        BIT NOT NULL DEFAULT(1),
    timezone_name NVARCHAR(50) NOT NULL,            -- 'UTC','America/New_York',...
    Provider_name NVARCHAR(50) NOT NULL,            -- provider bắt buộc

    CONSTRAINT UQ_symbol_provider UNIQUE(symbol_name, Provider_name)
);

CREATE TABLE dbo.timeframe (
    timeframe_id   INT IDENTITY(1,1) PRIMARY KEY,
    timeframe_name NVARCHAR(10) NOT NULL UNIQUE     -- 'M1','M5','M15','M30','H1','H4','D1','W'
);


/* ============================
   BẢNG GIÁ THEO TIMEFRAME
   ============================ */

-- 1W
CREATE TABLE dbo.Price_1W (
    id            BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT PK_Price_1W PRIMARY KEY NONCLUSTERED,
    symbol_id     INT NOT NULL CONSTRAINT FK_Price_1W_Symbol     REFERENCES dbo.symbol(symbol_id),
    timeframe_id  INT NOT NULL CONSTRAINT FK_Price_1W_Timeframe  REFERENCES dbo.timeframe(timeframe_id),
    provider_name NVARCHAR(50) NOT NULL,
    [datetime]    DATETIME2(0) NOT NULL,
    [open]        DECIMAL(38,12) NOT NULL,
    [high]        DECIMAL(38,12) NOT NULL,
    [low]         DECIMAL(38,12) NOT NULL,
    [close]       DECIMAL(38,12) NOT NULL,
    [volume]      DECIMAL(38,12) NULL,
    CONSTRAINT UQ_Price_1W UNIQUE(symbol_id, timeframe_id, provider_name, [datetime])
);
CREATE UNIQUE CLUSTERED INDEX CIX_Price_1W ON dbo.Price_1W(symbol_id, timeframe_id, provider_name, [datetime]);

-- 1D
CREATE TABLE dbo.Price_1D (
    id            BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT PK_Price_1D PRIMARY KEY NONCLUSTERED,
    symbol_id     INT NOT NULL CONSTRAINT FK_Price_1D_Symbol     REFERENCES dbo.symbol(symbol_id),
    timeframe_id  INT NOT NULL CONSTRAINT FK_Price_1D_Timeframe  REFERENCES dbo.timeframe(timeframe_id),
    provider_name NVARCHAR(50) NOT NULL,
    [datetime]    DATETIME2(0) NOT NULL,
    [open]        DECIMAL(38,12) NOT NULL,
    [high]        DECIMAL(38,12) NOT NULL,
    [low]         DECIMAL(38,12) NOT NULL,
    [close]       DECIMAL(38,12) NOT NULL,
    [volume]      DECIMAL(38,12) NULL,
    CONSTRAINT UQ_Price_1D UNIQUE(symbol_id, timeframe_id, provider_name, [datetime])
);
CREATE UNIQUE CLUSTERED INDEX CIX_Price_1D ON dbo.Price_1D(symbol_id, timeframe_id, provider_name, [datetime]);

-- H4
CREATE TABLE dbo.Price_H4 (
    id            BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT PK_Price_H4 PRIMARY KEY NONCLUSTERED,
    symbol_id     INT NOT NULL CONSTRAINT FK_Price_H4_Symbol     REFERENCES dbo.symbol(symbol_id),
    timeframe_id  INT NOT NULL CONSTRAINT FK_Price_H4_Timeframe  REFERENCES dbo.timeframe(timeframe_id),
    provider_name NVARCHAR(50) NOT NULL,
    [datetime]    DATETIME2(0) NOT NULL,
    [open]        DECIMAL(38,12) NOT NULL,
    [high]        DECIMAL(38,12) NOT NULL,
    [low]         DECIMAL(38,12) NOT NULL,
    [close]       DECIMAL(38,12) NOT NULL,
    [volume]      DECIMAL(38,12) NULL,
    CONSTRAINT UQ_Price_H4 UNIQUE(symbol_id, timeframe_id, provider_name, [datetime])
);
CREATE UNIQUE CLUSTERED INDEX CIX_Price_H4 ON dbo.Price_H4(symbol_id, timeframe_id, provider_name, [datetime]);

-- H1
CREATE TABLE dbo.Price_H1 (
    id            BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT PK_Price_H1 PRIMARY KEY NONCLUSTERED,
    symbol_id     INT NOT NULL CONSTRAINT FK_Price_H1_Symbol     REFERENCES dbo.symbol(symbol_id),
    timeframe_id  INT NOT NULL CONSTRAINT FK_Price_H1_Timeframe  REFERENCES dbo.timeframe(timeframe_id),
    provider_name NVARCHAR(50) NOT NULL,
    [datetime]    DATETIME2(0) NOT NULL,
    [open]        DECIMAL(38,12) NOT NULL,
    [high]        DECIMAL(38,12) NOT NULL,
    [low]         DECIMAL(38,12) NOT NULL,
    [close]       DECIMAL(38,12) NOT NULL,
    [volume]      DECIMAL(38,12) NULL,
    CONSTRAINT UQ_Price_H1 UNIQUE(symbol_id, timeframe_id, provider_name, [datetime])
);
CREATE UNIQUE CLUSTERED INDEX CIX_Price_H1 ON dbo.Price_H1(symbol_id, timeframe_id, provider_name, [datetime]);

-- M30
CREATE TABLE dbo.Price_M30 (
    id            BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT PK_Price_M30 PRIMARY KEY NONCLUSTERED,
    symbol_id     INT NOT NULL CONSTRAINT FK_Price_M30_Symbol     REFERENCES dbo.symbol(symbol_id),
    timeframe_id  INT NOT NULL CONSTRAINT FK_Price_M30_Timeframe  REFERENCES dbo.timeframe(timeframe_id),
    provider_name NVARCHAR(50) NOT NULL,
    [datetime]    DATETIME2(0) NOT NULL,
    [open]        DECIMAL(38,12) NOT NULL,
    [high]        DECIMAL(38,12) NOT NULL,
    [low]         DECIMAL(38,12) NOT NULL,
    [close]       DECIMAL(38,12) NOT NULL,
    [volume]      DECIMAL(38,12) NULL,
    CONSTRAINT UQ_Price_M30 UNIQUE(symbol_id, timeframe_id, provider_name, [datetime])
);
CREATE UNIQUE CLUSTERED INDEX CIX_Price_M30 ON dbo.Price_M30(symbol_id, timeframe_id, provider_name, [datetime]);

-- M15
CREATE TABLE dbo.Price_M15 (
    id            BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT PK_Price_M15 PRIMARY KEY NONCLUSTERED,
    symbol_id     INT NOT NULL CONSTRAINT FK_Price_M15_Symbol     REFERENCES dbo.symbol(symbol_id),
    timeframe_id  INT NOT NULL CONSTRAINT FK_Price_M15_Timeframe  REFERENCES dbo.timeframe(timeframe_id),
    provider_name NVARCHAR(50) NOT NULL,
    [datetime]    DATETIME2(0) NOT NULL,
    [open]        DECIMAL(38,12) NOT NULL,
    [high]        DECIMAL(38,12) NOT NULL,
    [low]         DECIMAL(38,12) NOT NULL,
    [close]       DECIMAL(38,12) NOT NULL,
    [volume]      DECIMAL(38,12) NULL,
    CONSTRAINT UQ_Price_M15 UNIQUE(symbol_id, timeframe_id, provider_name, [datetime])
);
CREATE UNIQUE CLUSTERED INDEX CIX_Price_M15 ON dbo.Price_M15(symbol_id, timeframe_id, provider_name, [datetime]);

-- M5
CREATE TABLE dbo.Price_M5 (
    id            BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT PK_Price_M5 PRIMARY KEY NONCLUSTERED,
    symbol_id     INT NOT NULL CONSTRAINT FK_Price_M5_Symbol     REFERENCES dbo.symbol(symbol_id),
    timeframe_id  INT NOT NULL CONSTRAINT FK_Price_M5_Timeframe  REFERENCES dbo.timeframe(timeframe_id),
    provider_name NVARCHAR(50) NOT NULL,
    [datetime]    DATETIME2(0) NOT NULL,
    [open]        DECIMAL(38,12) NOT NULL,
    [high]        DECIMAL(38,12) NOT NULL,
    [low]         DECIMAL(38,12) NOT NULL,
    [close]       DECIMAL(38,12) NOT NULL,
    [volume]      DECIMAL(38,12) NULL,
    CONSTRAINT UQ_Price_M5 UNIQUE(symbol_id, timeframe_id, provider_name, [datetime])
);
CREATE UNIQUE CLUSTERED INDEX CIX_Price_M5 ON dbo.Price_M5(symbol_id, timeframe_id, provider_name, [datetime]);

-- M1
CREATE TABLE dbo.Price_M1 (
    id            BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT PK_Price_M1 PRIMARY KEY NONCLUSTERED,
    symbol_id     INT NOT NULL CONSTRAINT FK_Price_M1_Symbol     REFERENCES dbo.symbol(symbol_id),
    timeframe_id  INT NOT NULL CONSTRAINT FK_Price_M1_Timeframe  REFERENCES dbo.timeframe(timeframe_id),
    provider_name NVARCHAR(50) NOT NULL,
    [datetime]    DATETIME2(0) NOT NULL,
    [open]        DECIMAL(38,12) NOT NULL,
    [high]        DECIMAL(38,12) NOT NULL,
    [low]         DECIMAL(38,12) NOT NULL,
    [close]       DECIMAL(38,12) NOT NULL,
    [volume]      DECIMAL(38,12) NULL,
    CONSTRAINT UQ_Price_M1 UNIQUE(symbol_id, timeframe_id, provider_name, [datetime])
);
CREATE UNIQUE CLUSTERED INDEX CIX_Price_M1 ON dbo.Price_M1(symbol_id, timeframe_id, provider_name, [datetime]);
