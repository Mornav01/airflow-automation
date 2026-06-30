SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF OBJECT_ID('dbo.sp_remove_identity_columns_final_updated', 'P') IS NOT NULL
    DROP PROCEDURE [dbo].[sp_remove_identity_columns_final_updated]
GO
CREATE PROCEDURE [dbo].[sp_remove_identity_columns_final_updated]
    @SchemaName SYSNAME,
    @TableName  SYSNAME
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @FullTable NVARCHAR(300) = QUOTENAME(@SchemaName) + '.' + QUOTENAME(@TableName);
    DECLARE @sql NVARCHAR(MAX);

    PRINT 'Starting execution for table: ' + @FullTable;

    ---------------------------------------------------
    -- Drop all foreign keys referencing this table
    ---------------------------------------------------
    DECLARE @fk_sql NVARCHAR(MAX) = '';

    SELECT @fk_sql += '
    ALTER TABLE ' + QUOTENAME(OBJECT_SCHEMA_NAME(fk.parent_object_id)) + '.'
        + QUOTENAME(OBJECT_NAME(fk.parent_object_id)) +
        ' DROP CONSTRAINT ' + QUOTENAME(fk.name) + ';'
    FROM sys.foreign_keys fk
    WHERE fk.referenced_object_id = OBJECT_ID(@FullTable);

    IF @fk_sql <> ''
    BEGIN
        EXEC (@fk_sql);
        PRINT 'Dropped foreign keys referencing ' + @FullTable;
    END

    ---------------------------------------------------
    -- Temp tables
    ---------------------------------------------------
    IF OBJECT_ID('tempdb..#identity_cols') IS NOT NULL DROP TABLE #identity_cols;
    IF OBJECT_ID('tempdb..#indexes')       IS NOT NULL DROP TABLE #indexes;
    IF OBJECT_ID('tempdb..#pk')            IS NOT NULL DROP TABLE #pk;

    ---------------------------------------------------
    -- Identity columns
    ---------------------------------------------------
    SELECT
        c.name COLLATE DATABASE_DEFAULT AS column_name,
        t.name COLLATE DATABASE_DEFAULT AS data_type,
        c.max_length,
        c.precision,
        c.scale,
        c.is_nullable
    INTO #identity_cols
    FROM sys.columns c
    JOIN sys.types t ON c.user_type_id = t.user_type_id
    WHERE c.object_id = OBJECT_ID(@FullTable)
      AND c.is_identity = 1;

    IF NOT EXISTS (SELECT 1 FROM #identity_cols)
    BEGIN
        PRINT 'No identity columns found on ' + @FullTable;
    END

    ---------------------------------------------------
    -- PK
    ---------------------------------------------------
    SELECT kc.name COLLATE DATABASE_DEFAULT AS pk_name
    INTO #pk
    FROM sys.key_constraints kc
    WHERE kc.type = 'PK'
      AND kc.parent_object_id = OBJECT_ID(@FullTable);

    ---------------------------------------------------
    -- Indexes (drop_sql only)
    ---------------------------------------------------
    SELECT
        'DROP INDEX ' + QUOTENAME(i.name) + ' ON ' + @FullTable AS drop_sql,
        i.name AS idx_name
    INTO #indexes
    FROM sys.indexes i
    WHERE i.object_id = OBJECT_ID(@FullTable)
      AND i.is_primary_key = 0
      AND i.type > 0;

    ---------------------------------------------------
    -- Disable change tracking (required before dropping PK)
    ---------------------------------------------------
    IF EXISTS (
        SELECT 1 FROM sys.change_tracking_tables
        WHERE object_id = OBJECT_ID(@FullTable)
    )
    BEGIN
        SET @sql = 'ALTER TABLE ' + @FullTable + ' DISABLE CHANGE_TRACKING';
        EXEC (@sql);
        PRINT 'Disabled change tracking on ' + @FullTable;
    END

    ---------------------------------------------------
    -- Drop PK
    ---------------------------------------------------
    DECLARE @pk_name SYSNAME;
    SELECT @pk_name = pk_name FROM #pk;

    IF @pk_name IS NOT NULL
    BEGIN
        SET @sql = 'ALTER TABLE ' + @FullTable + ' DROP CONSTRAINT ' + QUOTENAME(@pk_name);
        EXEC (@sql);
        PRINT 'Dropped primary key: ' + @pk_name;
    END

    ---------------------------------------------------
    -- Drop UNIQUE constraints
    ---------------------------------------------------
    DECLARE @uq_sql NVARCHAR(MAX) = N'';

    SELECT @uq_sql += N'ALTER TABLE ' + @FullTable
                    + N' DROP CONSTRAINT ' + QUOTENAME(kc.name) + N'; '
    FROM sys.key_constraints kc
    WHERE kc.type = 'UQ'
      AND kc.parent_object_id = OBJECT_ID(@FullTable);

    IF @uq_sql <> N''
    BEGIN
        EXEC (@uq_sql);
        PRINT 'Dropped unique constraints on ' + @FullTable;
    END

    ---------------------------------------------------
    -- Drop indexes
    ---------------------------------------------------
    DECLARE @idx_drop_sql NVARCHAR(MAX) = N'';

    SELECT @idx_drop_sql += ix.drop_sql + N'; '
    FROM #indexes ix
    WHERE EXISTS (
        SELECT 1 FROM sys.indexes si
        WHERE si.name = ix.idx_name
          AND si.object_id = OBJECT_ID(@FullTable)
    );

    IF @idx_drop_sql <> N''
    BEGIN
        EXEC (@idx_drop_sql);
        PRINT 'Dropped indexes on ' + @FullTable;
    END

    ---------------------------------------------------
    -- Drop triggers (FOR XML PATH - no STRING_AGG)
    ---------------------------------------------------
    SELECT @sql = STUFF(
        (SELECT ';DROP TRIGGER ' + QUOTENAME(name COLLATE DATABASE_DEFAULT)
         FROM sys.triggers
         WHERE parent_id = OBJECT_ID(@FullTable)
         FOR XML PATH('')),
        1, 1, '');

    IF @sql IS NOT NULL AND LEN(@sql) > 0
    BEGIN
        EXEC (@sql);
        PRINT 'Dropped triggers on ' + @FullTable;
    END

    ---------------------------------------------------
    -- Remove identity columns
    ---------------------------------------------------
    DECLARE cur2 CURSOR FOR
        SELECT column_name, data_type, max_length, precision, scale, is_nullable
        FROM #identity_cols;

    DECLARE @col SYSNAME, @type SYSNAME, @len INT, @prec INT, @scale INT, @nullable BIT;

    OPEN cur2;
    FETCH NEXT FROM cur2 INTO @col, @type, @len, @prec, @scale, @nullable;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        DECLARE @newcol SYSNAME = @col + '_ndnew';
        DECLARE @datatype NVARCHAR(100) =
            CASE
                WHEN @type IN ('varchar','char','nvarchar','nchar')
                    THEN @type + '(' + CASE WHEN @len = -1 THEN 'MAX' ELSE CAST(@len AS VARCHAR) END + ')'
                WHEN @type IN ('decimal','numeric')
                    THEN @type + '(' + CAST(@prec AS VARCHAR) + ',' + CAST(@scale AS VARCHAR) + ')'
                ELSE @type
            END;

        SET @sql = 'ALTER TABLE ' + @FullTable + ' ADD ' + QUOTENAME(@newcol) + ' ' + @datatype + ' NULL';
        EXEC (@sql);

        SET @sql = 'UPDATE ' + @FullTable + ' SET ' + QUOTENAME(@newcol) + ' = ' + QUOTENAME(@col);
        EXEC (@sql);

        IF @nullable = 0
        BEGIN
            SET @sql = 'ALTER TABLE ' + @FullTable + ' ALTER COLUMN ' + QUOTENAME(@newcol) + ' ' + @datatype + ' NOT NULL';
            EXEC (@sql);
        END

        SET @sql = 'ALTER TABLE ' + @FullTable + ' DROP COLUMN ' + QUOTENAME(@col);
        EXEC (@sql);

        SET @sql = 'EXEC sp_rename ''' + @SchemaName + '.' + @TableName + '.' + @newcol + ''',''' + @col + ''',''COLUMN''';
        EXEC (@sql);

        FETCH NEXT FROM cur2 INTO @col, @type, @len, @prec, @scale, @nullable;
    END

    CLOSE cur2; DEALLOCATE cur2;

    PRINT 'SUCCESS: Identity removed, FKs dropped, PK dropped, triggers dropped, indexes dropped on ' + @FullTable;
END;
GO
