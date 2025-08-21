-- =============================================
-- Stored Procedure: bronze.load_bronze
-- Description: Loads data from CSV files into bronze layer tables
-- Author: Juan Daniel Garcia Belman
-- Version: 2.0 (Improved with parameterization)
-- =============================================


/*
===============================================================================
Stored Procedure: Load Bronze Layer - Enhanced Version (Source -> Bronze)
===============================================================================
Script Purpose:
    This parameterized stored procedure loads data into the 'bronze' schema from 
    external CSV files using a table-driven configuration approach. It performs 
    the following actions:
    - Dynamically truncates bronze tables based on configuration metadata.
    - Uses parameterized `BULK INSERT` commands to load data from CSV files.
    - Provides flexible execution options through input parameters.
    - Implements comprehensive error handling and performance monitoring.
    - Supports both CRM and ERP source systems with different file formats.

Parameters:
    @data_path NVARCHAR(500) [Optional, Default: '/data/pj/datawarehouse/data/datasets']
        - Base directory path where source CSV files are located.
        - Allows flexible deployment across different environments.
    
    @enable_logging BIT [Optional, Default: 1]
        - Controls detailed console output during execution.
        - Set to 0 for silent execution in automated processes.
        - Set to 1 for verbose logging with timing and status information.
    
    @parallel_load BIT [Optional, Default: 0] 
        - Reserved for future enhancement to enable concurrent table loading.
        - Currently not implemented but prepared for performance optimization.
    
    @validation_mode BIT [Optional, Default: 0]
        - Enables row count validation after each table load.
        - Provides data quality verification when set to 1.
        - Useful for data integrity checks and load verification.

Tables Loaded (in sequence):
    1. bronze.crm_cust_info     <- source_crm/cust_info.csv
    2. bronze.crm_prd_info      <- source_crm/prd_info.csv  
    3. bronze.crm_sales_details <- source_crm/sales_details.csv
    4. bronze.erp_loc_a101      <- source_erp/loc_a101.csv
    5. bronze.erp_cust_az12     <- source_erp/cust_az12.csv
    6. bronze.erp_px_cat_g1v2   <- source_erp/px_cat_g1v2.csv

Key Features:
    - Table-driven configuration eliminates code duplication
    - Dynamic SQL generation for flexible file handling
    - Comprehensive error logging with context information
    - Individual table timing and optional row count validation
    - Proper cursor resource management in all execution scenarios

Usage Examples:
    -- Standard execution with all defaults
    EXEC bronze.load_bronze;
    
    -- Custom data path for different environment
    EXEC bronze.load_bronze @data_path = '/custom/data/path';
    
    -- Silent execution for automated processes
    EXEC bronze.load_bronze @enable_logging = 0;
    
    -- Execution with data validation enabled
    EXEC bronze.load_bronze @validation_mode = 1;
    
    -- Full parameter specification
    EXEC bronze.load_bronze 
        @data_path = '/prod/data/warehouse',
        @enable_logging = 1,
        @validation_mode = 1;

Return Values:
    This procedure does not return explicit values but provides:
    - Console output with execution status (when @enable_logging = 1)
    - Error propagation through THROW for calling applications
    - Performance metrics and load duration information
===============================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze 
    @data_path NVARCHAR(500) = '/data/pj/datawarehouse/data/datasets',  -- Base path for data files
    @enable_logging BIT = 1,                                            -- Enable/disable detailed logging
    @parallel_load BIT = 0,                                            -- Future: Enable parallel processing
    @validation_mode BIT = 0                                           -- Enable data validation checks
AS
BEGIN
    -- Variable declarations for timing and control
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
    DECLARE @current_schema NVARCHAR(50), @current_table NVARCHAR(100), @current_source NVARCHAR(50);
    DECLARE @current_file NVARCHAR(500), @row_count INT, @has_header BIT, @field_terminator NCHAR(1);
    DECLARE @sql NVARCHAR(MAX);
    
    -- Configuration table for bronze layer loads
    DECLARE @load_config TABLE (
        load_order INT,
        schema_name NVARCHAR(50),
        table_name NVARCHAR(100),
        source_system NVARCHAR(50),
        file_name NVARCHAR(200),
        has_header BIT,
        field_terminator NCHAR(1),
        is_active BIT
    );
    
    BEGIN TRY
        SET @batch_start_time = GETDATE();
        
        -- Initialize load configuration
        -- This table-driven approach makes it easy to add/remove tables without code changes
        INSERT INTO @load_config (load_order, schema_name, table_name, source_system, file_name, has_header, field_terminator, is_active)
        VALUES 
            (1, 'bronze', 'crm_cust_info', 'source_crm', 'cust_info.csv', 1, ',', 1),
            (2, 'bronze', 'crm_prd_info', 'source_crm', 'prd_info.csv', 1, ',', 1),
            (3, 'bronze', 'crm_sales_details', 'source_crm', 'sales_details.csv', 1, ',', 1),
            (4, 'bronze', 'erp_loc_a101', 'source_erp', 'loc_a101.csv', 1, ',', 1),
            (5, 'bronze', 'erp_cust_az12', 'source_erp', 'cust_az12.csv', 1, ',', 1),
            (6, 'bronze', 'erp_px_cat_g1v2', 'source_erp', 'px_cat_g1v2.csv', 1, ',', 1);

        -- Header logging section
        IF @enable_logging = 1
        BEGIN
            PRINT '================================================';
            PRINT 'Loading Bronze Layer - Enhanced Version';
            PRINT 'Start Time: ' + CONVERT(NVARCHAR, @batch_start_time, 120);
            PRINT 'Base Data Path: ' + @data_path;
            PRINT '================================================';
        END

        -- Main processing loop - iterates through each table configuration
        DECLARE config_cursor CURSOR FOR
            SELECT schema_name, table_name, source_system, file_name, has_header, field_terminator
            FROM @load_config 
            WHERE is_active = 1 
            ORDER BY load_order;

        OPEN config_cursor;
        FETCH NEXT FROM config_cursor INTO @current_schema, @current_table, @current_source, @current_file, @has_header, @field_terminator;

        -- Process each table in the configuration
        WHILE @@FETCH_STATUS = 0
        BEGIN
            SET @start_time = GETDATE();
            
            -- Dynamic table truncation
            -- Builds and executes TRUNCATE statement dynamically
            SET @sql = 'TRUNCATE TABLE ' + @current_schema + '.' + @current_table;
            
            IF @enable_logging = 1
                PRINT '>> Truncating Table: ' + @current_schema + '.' + @current_table;
            
            EXEC sp_executesql @sql;
            
            -- Build full file path
            SET @current_file = @data_path + '/' + @current_source + '/' + @current_file;
            
            -- Dynamic bulk insert with parameterized options
            -- Constructs BULK INSERT statement based on configuration
            SET @sql = 'BULK INSERT ' + @current_schema + '.' + @current_table + 
                      ' FROM ''' + @current_file + ''' WITH (' +
                      'FIRSTROW = ' + CASE WHEN @has_header = 1 THEN '2' ELSE '1' END + ', ' +
                      'FIELDTERMINATOR = ''' + @field_terminator + ''', ' +
                      'TABLOCK, MAXERRORS = 10)';
            
            IF @enable_logging = 1
                PRINT '>> Loading Data Into: ' + @current_schema + '.' + @current_table;
            
            EXEC sp_executesql @sql;
            
            -- Get row count for validation (optional)
            IF @validation_mode = 1
            BEGIN
                SET @sql = 'SELECT @count = COUNT(*) FROM ' + @current_schema + '.' + @current_table;
                EXEC sp_executesql @sql, N'@count INT OUTPUT', @count = @row_count OUTPUT;
            END
            
            -- Performance logging section
            SET @end_time = GETDATE();
            IF @enable_logging = 1
            BEGIN
                PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
                IF @validation_mode = 1
                    PRINT '>> Rows Loaded: ' + CAST(@row_count AS NVARCHAR);
                PRINT '>> --------------------------------------------------';
            END
            
            FETCH NEXT FROM config_cursor INTO @current_schema, @current_table, @current_source, @current_file, @has_header, @field_terminator;
        END

        CLOSE config_cursor;
        DEALLOCATE config_cursor;
        
        -- Final success logging
        SET @batch_end_time = GETDATE();
        IF @enable_logging = 1
        BEGIN
            PRINT '=====================================================';
            PRINT 'Bronze Layer Loading Completed Successfully';
            PRINT 'End Time: ' + CONVERT(NVARCHAR, @batch_end_time, 120);
            PRINT 'Total Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
            PRINT '=====================================================';
        END

    END TRY 
    BEGIN CATCH
        -- Enhanced error handling section
        -- Captures and logs detailed error information
        DECLARE @error_message NVARCHAR(4000), @error_number INT, @error_state INT, @error_line INT;
        
        SELECT 
            @error_message = ERROR_MESSAGE(),
            @error_number = ERROR_NUMBER(),
            @error_state = ERROR_STATE(),
            @error_line = ERROR_LINE();
        
        -- Close cursor if still open
        IF CURSOR_STATUS('local','config_cursor') >= 0
        BEGIN
            CLOSE config_cursor;
            DEALLOCATE config_cursor;
        END
        
        -- Comprehensive error logging
        PRINT '============================================';
        PRINT 'ERROR OCCURRED DURING BRONZE LAYER LOADING';
        PRINT 'Error Number: ' + CAST(@error_number AS NVARCHAR);
        PRINT 'Error State: ' + CAST(@error_state AS NVARCHAR);
        PRINT 'Error Line: ' + CAST(@error_line AS NVARCHAR);
        PRINT 'Error Message: ' + @error_message;
        IF @current_table IS NOT NULL
            PRINT 'Failed on Table: ' + @current_schema + '.' + @current_table;
        PRINT 'Batch Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, GETDATE()) AS NVARCHAR) + ' seconds';
        PRINT '============================================';
        
        -- Re-throw error to calling application
        THROW;
        
    END CATCH
END

GO

-- =============================================
-- Usage Examples:
-- =============================================

-- Standard execution with default parameters
-- EXEC bronze.load_bronze;

-- Custom data path execution
-- EXEC bronze.load_bronze @data_path = '/custom/data/path';

-- Silent execution (no logging)
-- EXEC bronze.load_bronze @enable_logging = 0;

-- Execution with validation
-- EXEC bronze.load_bronze @validation_mode = 1;

EXEC bronze.load_bronze @validation_mode = 1;