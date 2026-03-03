SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE  OR AlTER   PROCEDURE [dbo].[sp_run_diff]
(
    @table_name SYSNAME,
    @dump_db SYSNAME,   -- old db
    @diff_db SYSNAME    -- diff db
)
AS
BEGIN
    SET NOCOUNT ON;

DECLARE 
    @DiffDB SYSNAME = @diff_db,
    @OldDB  SYSNAME = DB_NAME(),
    @NewDB  SYSNAME = @dump_db,
    @Schema SYSNAME = 'dbo',
    @Table  SYSNAME,
    @SQL NVARCHAR(MAX),
    @AllCols NVARCHAR(MAX),
    @ComparableCols NVARCHAR(MAX),
    @PK SYSNAME,
    @HasPK BIT,
	@IdentityCol SYSNAME,
	@AllNewCols NVARCHAR(MAX),
    @ComparableColsBase NVARCHAR(MAX),
    @ComparableColsNew NVARCHAR(MAX),
    @ComparableColsOld NVARCHAR(MAX);

DECLARE diff_cursor CURSOR FAST_FORWARD FOR

SELECT TableName FROM (VALUES(@table_name)) AS Tables(TableName);


OPEN diff_cursor;
FETCH NEXT FROM diff_cursor INTO @Table;

WHILE @@FETCH_STATUS = 0
BEGIN
BEGIN TRY
    PRINT '🔍 Processing table: ' + @Table;

    --THROW 51000, 'INTENTIONAL FAILURE TEST', 1;


    SET @SQL = '
    SELECT TOP 1 @PK_OUT = c.name
    FROM ' + QUOTENAME(@NewDB) + '.sys.indexes i
    JOIN ' + QUOTENAME(@NewDB) + '.sys.index_columns ic 
        ON i.object_id = ic.object_id AND i.index_id = ic.index_id
    JOIN ' + QUOTENAME(@NewDB) + '.sys.columns c 
        ON ic.object_id = c.object_id AND ic.column_id = c.column_id
    JOIN ' + QUOTENAME(@NewDB) + '.sys.tables t 
        ON t.object_id = i.object_id
    JOIN ' + QUOTENAME(@NewDB) + '.sys.schemas s 
        ON s.schema_id = t.schema_id
    WHERE i.is_primary_key = 1
    AND t.name = @Table
    AND s.name = @Schema;
    ';

    EXEC sp_executesql
        @SQL,
        N'@Table SYSNAME, @Schema SYSNAME, @PK_OUT SYSNAME OUTPUT',
        @Table = @Table,
        @Schema = @Schema,
        @PK_OUT = @PK OUTPUT;

    SET @HasPK = CASE WHEN @PK IS NULL THEN 0 ELSE 1 END;

    ---------------------------------------------------
    -- 2️⃣ Column lists
    ---------------------------------------------------

	SET @SQL = '
    SELECT @IdentityCol_OUT = c.name
    FROM ' + QUOTENAME(@NewDB) + '.sys.columns c
    JOIN ' + QUOTENAME(@NewDB) + '.sys.tables t ON c.object_id = t.object_id
    JOIN ' + QUOTENAME(@NewDB) + '.sys.schemas s ON s.schema_id = t.schema_id
    WHERE t.name = @Table
    AND s.name = @Schema
    AND c.is_identity = 1;
    ';

    EXEC sp_executesql
        @SQL,
        N'@Table SYSNAME, @Schema SYSNAME, @IdentityCol_OUT SYSNAME OUTPUT',
        @Table = @Table,
        @Schema = @Schema,
        @IdentityCol_OUT = @IdentityCol OUTPUT;


 SET @SQL = '
    SELECT @Cols = STRING_AGG(''n.'' + QUOTENAME(c.name), '','')
           WITHIN GROUP (ORDER BY c.column_id)
    FROM ' + QUOTENAME(@NewDB) + '.sys.columns c
    JOIN ' + QUOTENAME(@NewDB) + '.sys.tables t ON c.object_id = t.object_id
    JOIN ' + QUOTENAME(@NewDB) + '.sys.schemas s ON s.schema_id = t.schema_id
    WHERE t.name = @Table
      AND s.name = @Schema
      --AND c.is_identity = 0
      ;';

    EXEC sp_executesql
        @SQL,
        N'@Table SYSNAME, @Schema SYSNAME, @Cols NVARCHAR(MAX) OUTPUT',
        @Table=@Table, @Schema=@Schema, @Cols=@AllNewCols OUTPUT;

	  SET @SQL = '
SELECT @Cols = STRING_AGG(QUOTENAME(x.name), '','')
FROM (
    SELECT c.name
    FROM ' + QUOTENAME(@NewDB) + '.sys.columns c
    JOIN ' + QUOTENAME(@NewDB) + '.sys.tables t ON c.object_id = t.object_id
    JOIN ' + QUOTENAME(@NewDB) + '.sys.schemas s ON s.schema_id = t.schema_id
    WHERE t.name = ''' + @Table + '''
      AND s.name = ''' + @Schema + '''
      AND c.system_type_id NOT IN (34,35,99) -- text, ntext, image
      AND c.is_computed = 0

    INTERSECT

    SELECT c.name
    FROM ' + QUOTENAME(@OldDB) + '.sys.columns c
    JOIN ' + QUOTENAME(@OldDB) + '.sys.tables t ON c.object_id = t.object_id
    JOIN ' + QUOTENAME(@OldDB) + '.sys.schemas s ON s.schema_id = t.schema_id
    WHERE t.name = ''' + @Table + '''
      AND s.name = ''' + @Schema + '''
      AND c.system_type_id NOT IN (34,35,99)
      AND c.is_computed = 0
) x';

EXEC sp_executesql
    @SQL,
    N'@Cols NVARCHAR(MAX) OUTPUT',
    @Cols = @ComparableColsBase OUTPUT;
		
	
    ---------------------------------------------------
    -- 6️⃣ Build Comparable Column Base List
    ---------------------------------------------------
    --SELECT
    --  @ComparableColsBase =
    --   STRING_AGG(
    --        CAST(
    --            'CAST(' + QUOTENAME(c.name) + ' AS NVARCHAR(MAX))'
    --            AS NVARCHAR(MAX)
    --        ),
    --        ','
    --    ) WITHIN GROUP (ORDER BY c.column_id)
    --FROM sys.columns c
    --JOIN sys.tables t ON c.object_id = t.object_id
    --JOIN sys.schemas s ON s.schema_id = t.schema_id
    --WHERE t.name = @Table
    --  AND s.name = @Schema
    --  AND c.is_computed = 0
    --  AND c.system_type_id NOT IN (34,35,99) -- image,text,ntext
    --  ;--AND c.name <> @IdentityCol;
	

--------------------------------------------------
    -- 7️⃣ Prefix Comparable Columns for n / o
    ---------------------------------------------------
	
    SELECT @ComparableColsNew =
        STRING_AGG('n.' + value, ',')
    FROM STRING_SPLIT(@ComparableColsBase, ',');

    SELECT @ComparableColsOld =
        STRING_AGG('o.' + value, ',')
    FROM STRING_SPLIT(@ComparableColsBase, ',');


---------------------------------------------------
-- Drop DIFF table if exists (fresh rebuild)
---------------------------------------------------
SET @SQL = '
DROP TABLE IF EXISTS ' + QUOTENAME(@DiffDB) + '.' + QUOTENAME(@Schema) + '.' + QUOTENAME(@Table) + ';
';
EXEC (@SQL);

	  


---------------------------------------------------
-- 3️⃣ Create diff table (structure preserved, NO IDENTITY)
---------------------------------------------------
DECLARE @CreateSQL VARCHAR(MAX);

SET @SQL = N'
USE ' + QUOTENAME(@DiffDB) + N';

IF OBJECT_ID(N''' + QUOTENAME(@Schema) + N'.' + QUOTENAME(@Table) + N''') IS NULL
BEGIN
    DECLARE @CreateSQL NVARCHAR(MAX);

    SELECT 
        @CreateSQL =
            N''CREATE TABLE ' + QUOTENAME(@DiffDB) + N'.' + QUOTENAME(@Schema) + N'.' + QUOTENAME(@Table) + N' ('' 
            + STRING_AGG(
                CAST(
                    QUOTENAME(c.name) + N'' '' +
                    t.name +
                    CASE 
                        WHEN t.name IN (''varchar'',''char'',''nvarchar'',''nchar'') 
                            THEN N''('' +
                                 CASE 
                                     WHEN c.max_length = -1 THEN N''MAX''
                                     ELSE CAST(
                                          CASE 
                                              WHEN t.name IN (''nvarchar'',''nchar'') 
                                                  THEN c.max_length / 2
                                              ELSE c.max_length
                                          END AS NVARCHAR(10))
                                 END + N'')''
                        WHEN t.name IN (''decimal'',''numeric'')
                            THEN N''('' + CAST(c.precision AS NVARCHAR(10)) 
                                   + N'','' + CAST(c.scale AS NVARCHAR(10)) + N'')''
                        ELSE N''''
                    END +
                    CASE 
                        WHEN c.is_nullable = 0 THEN N'' NOT NULL'' ELSE N'' NULL''
                    END
                AS NVARCHAR(MAX))
            , N'',''
            ) WITHIN GROUP (ORDER BY c.column_id)
            + N'')''
    FROM ' + QUOTENAME(@NewDB) + N'.sys.columns c
    JOIN ' + QUOTENAME(@NewDB) + N'.sys.types t
        ON c.user_type_id = t.user_type_id
    WHERE c.object_id = OBJECT_ID(
        N''' + QUOTENAME(@NewDB) + N'.' + QUOTENAME(@Schema) + N'.' + QUOTENAME(@Table) + N'''
    );

    EXEC sys.sp_executesql @CreateSQL;

    ALTER TABLE ' + QUOTENAME(@DiffDB) + N'.' + QUOTENAME(@Schema) + N'.' + QUOTENAME(@Table) + N'
    ADD diff_type VARCHAR(20),
        nd_extracted_date DATETIME2 DEFAULT CAST(CAST(SYSUTCDATETIME() AS DATE) AS DATETIME2);
END;';

EXEC (@SQL);



    ---------------------------------------------------
    -- 4️⃣ PK TABLE LOGIC
    ---------------------------------------------------
    IF @HasPK = 1
    BEGIN
        PRINT '🔑 Using PK-based diff on ' + @PK;
		print ('iamhere s'+ @pk )
		--print (@AllNewCols)
		--print (@ComparableColsNew)
		--print (@ComparableColsOld)
		 --  SET IDENTITY_INSERT ' + QUOTENAME(@DiffDB) + '.' + QUOTENAME(@Schema) + '.' + QUOTENAME(@Table) + ' ON;


        DECLARE @HasActiveFlag BIT = 0;

        SET @SQL = '
        SELECT @HasFlag = CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END
        FROM ' + QUOTENAME(@OldDB) + '.sys.columns c
        JOIN ' + QUOTENAME(@OldDB) + '.sys.tables t ON c.object_id = t.object_id
        JOIN ' + QUOTENAME(@OldDB) + '.sys.schemas s ON s.schema_id = t.schema_id
        WHERE t.name = @Table
        AND s.name = @Schema
        AND c.name = ''nd_ActiveFlag'';
        ';

        EXEC sp_executesql
            @SQL,
            N'@Table SYSNAME, @Schema SYSNAME, @HasFlag BIT OUTPUT',
            @Table=@Table,
            @Schema=@Schema,
            @HasFlag=@HasActiveFlag OUTPUT;

        DECLARE @nd_ActiveFlagOldDBFilter NVARCHAR(MAX) = '';

        IF @HasActiveFlag = 1
            SET @nd_ActiveFlagOldDBFilter = ' AND o.nd_ActiveFlag = ''Y'' ';


        SET @SQL = '
     

        INSERT INTO ' + QUOTENAME(@DiffDB) + '.' + QUOTENAME(@Schema) + '.' + QUOTENAME(@Table) + '
        (' + @AllNewCols + ', diff_type)
        SELECT ' + @AllNewCols + ', ''NEW_ONLY''
        FROM ' + QUOTENAME(@NewDB) + '.' + QUOTENAME(@Schema) + '.' + QUOTENAME(@Table) + ' n
        WHERE NOT EXISTS (
            SELECT 1
            FROM ' + QUOTENAME(@OldDB) + '.' + QUOTENAME(@Schema) + '.' + QUOTENAME(@Table) + ' o
            WHERE o.' + QUOTENAME(@PK) + ' = n.' + QUOTENAME(@PK) + @nd_ActiveFlagOldDBFilter  + '
        );

        INSERT INTO ' + QUOTENAME(@DiffDB) + '.' + QUOTENAME(@Schema) + '.' + QUOTENAME(@Table) + '
        (' + @AllNewCols + ', diff_type)
        SELECT ' + @AllNewCols + ', ''CHANGED''
        FROM ' + QUOTENAME(@NewDB) + '.' + QUOTENAME(@Schema) + '.' + QUOTENAME(@Table) + ' n
        JOIN ' + QUOTENAME(@OldDB) + '.' + QUOTENAME(@Schema) + '.' + QUOTENAME(@Table) + ' o
          ON o.' + QUOTENAME(@PK) + ' = n.' + QUOTENAME(@PK) + '
        WHERE HASHBYTES(''SHA2_256'', CONCAT_WS(''|'', ' + @ComparableColsNew + '))
   <> HASHBYTES(''SHA2_256'', CONCAT_WS(''|'', ' + @ComparableColsOld + '))' + @nd_ActiveFlagOldDBFilter + ';

       ;'
    EXEC (@SQL);
	 --SET IDENTITY_INSERT ' + QUOTENAME(@DiffDB) + '.' + QUOTENAME(@Schema) + '.' + QUOTENAME(@Table) + ' OFF;';
	END
	
    ELSE
    BEGIN
        PRINT '⚠️ No PK – using HASH-based diff';
---------------------------------------------------
    -- 5️⃣ Build OLD HASH TEMP TABLE (ONCE)
    ---------------------------------------------------
--    SET @SQL = '
--    IF OBJECT_ID(''tempdb..#old_hash'') IS NOT NULL DROP TABLE #old_hash;

--	CREATE TABLE #old_hash (
--    row_hash VARBINARY(32) NOT NULL
--    -- optional PK if exists
--    ' + CASE WHEN @PK IS NOT NULL THEN ', ' + QUOTENAME(@PK) + ' SQL_VARIANT' ELSE '' END + '
--	);

--    INSERT INTO #old_hash (row_hash' + CASE WHEN @PK IS NOT NULL THEN ', ' + QUOTENAME(@PK) ELSE '' END + ')
--SELECT
--    HASHBYTES(
--        ''SHA2_256'',
--        CONCAT_WS(
--            ''|'',
--            ' + @ComparableColsBase + '
--        )
--    )
--    ' + CASE WHEN @PK IS NOT NULL THEN ', ' + QUOTENAME(@PK) ELSE '' END + '
--FROM ' + QUOTENAME(@OldDB) + '.' + QUOTENAME(@Schema) + '.' + QUOTENAME(@Table) + ';

--   CREATE NONCLUSTERED INDEX IX_old_hash ON #old_hash (row_hash);;';

		--    EXEC (@SQL);

---------------------------------------------------
-- Build OLD hash table
---------------------------------------------------
		SET @SQL = '

IF OBJECT_ID(''tempdb..#old_hash'') IS NOT NULL
    DROP TABLE #old_hash;

CREATE TABLE #old_hash (
    row_hash VARBINARY(32) NOT NULL
);

INSERT INTO #old_hash (row_hash)
SELECT
    HASHBYTES(
        ''SHA2_256'',
        CONCAT_WS(
            ''|'',
            ' + @ComparableColsBase + '
        )
    )
FROM ' + QUOTENAME(@OldDB) + '.' + QUOTENAME(@Schema) + '.' + QUOTENAME(@Table) + ';

CREATE NONCLUSTERED INDEX IX_old_hash ON #old_hash (row_hash);





---------------------------------------------------
-- Insert NEW / CHANGED rows
---------------------------------------------------
INSERT INTO ' + QUOTENAME(@DiffDB) + '.' + QUOTENAME(@Schema) + '.' + QUOTENAME(@Table) + '
(' + @AllNewCols + ', diff_type)
SELECT ' + @AllNewCols + ', ''NEW_OR_CHANGED''
FROM ' + QUOTENAME(@NewDB) + '.' + QUOTENAME(@Schema) + '.' + QUOTENAME(@Table) + ' n
WHERE NOT EXISTS (
    SELECT 1
    FROM #old_hash o
    WHERE o.row_hash =
          HASHBYTES(
              ''SHA2_256'',
              CONCAT_WS(
                  ''|'',
                  ' + @ComparableColsBase + '
              )
          )
);
';

    EXEC (@SQL);
    END

    
    PRINT '✅ Completed table: ' + @Table;

END TRY
BEGIN CATCH
    DECLARE @ErrMsg NVARCHAR(4000) =
        'sp_run_diff failed for table ' + @Table + ' : ' + ERROR_MESSAGE();

    THROW 50000, @ErrMsg, 1;
END CATCH;

FETCH NEXT FROM diff_cursor INTO @Table;
END

CLOSE diff_cursor;
DEALLOCATE diff_cursor;

PRINT '🎯 ALL TABLE DIFFERENCES COMPLETED';
END

GO
