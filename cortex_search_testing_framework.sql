-- Cortex Search Testing Framework
-- ================================
-- Comprehensive testing framework for all search scenarios
-- Use this to validate basic and advanced search capabilities

-- =============================================================================
-- TEST EXECUTION FRAMEWORK
-- =============================================================================

-- Create test results table
CREATE OR REPLACE TABLE cortex_search_test_results (
    test_id INTEGER AUTOINCREMENT,
    test_name VARCHAR(200),
    test_category VARCHAR(100),
    search_query VARCHAR(500),
    expected_behavior VARCHAR(500),
    actual_results INTEGER,
    test_status VARCHAR(50),
    execution_time_ms INTEGER,
    test_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    notes VARCHAR(1000)
);

-- Create test execution procedure
CREATE OR REPLACE PROCEDURE run_search_test(
    test_name VARCHAR,
    test_category VARCHAR,
    search_query VARCHAR,
    expected_behavior VARCHAR,
    service_name VARCHAR DEFAULT 'tpcds_comprehensive_search',
    max_results INTEGER DEFAULT 20
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    start_time TIMESTAMP_NTZ;
    end_time TIMESTAMP_NTZ;
    execution_time INTEGER;
    result_count INTEGER;
    test_status VARCHAR;
BEGIN
    -- Record start time
    start_time := CURRENT_TIMESTAMP();
    
    -- Execute search and count results
    CREATE OR REPLACE TEMPORARY TABLE temp_test_results AS
    SELECT * FROM TABLE(
        SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
            service_name,
            search_query,
            LIMIT => max_results
        )
    );
    
    SELECT COUNT(*) INTO result_count FROM temp_test_results;
    
    -- Record end time and calculate execution time
    end_time := CURRENT_TIMESTAMP();
    execution_time := DATEDIFF('milliseconds', start_time, end_time);
    
    -- Determine test status
    IF (result_count > 0) THEN
        test_status := 'PASS';
    ELSE
        test_status := 'FAIL - NO RESULTS';
    END IF;
    
    -- Insert test results
    INSERT INTO cortex_search_test_results (
        test_name, test_category, search_query, expected_behavior,
        actual_results, test_status, execution_time_ms
    ) VALUES (
        test_name, test_category, search_query, expected_behavior,
        result_count, test_status, execution_time
    );
    
    RETURN CONCAT('Test completed: ', test_status, ' (', result_count, ' results in ', execution_time, 'ms)');
END;
$$;

-- =============================================================================
-- BASIC SEARCH TESTS
-- =============================================================================

SELECT 'RUNNING BASIC SEARCH TESTS' as test_phase;

-- Test 1: Exact Matching
CALL run_search_test(
    'Exact Product Match',
    'BASIC_SEARCH',
    'running shoes',
    'Should find products with exact term "running shoes"',
    'tpcds_comprehensive_search',
    10
);

-- Test 2: Partial Matching
CALL run_search_test(
    'Partial Product Match',
    'BASIC_SEARCH', 
    'blue cotton',
    'Should find products containing "blue" or "cotton"',
    'tpcds_comprehensive_search',
    15
);

-- Test 3: Multi-column Search
CALL run_search_test(
    'Multi-Column Search',
    'BASIC_SEARCH',
    'premium brand california store',
    'Should search across product, brand, and store columns',
    'tpcds_comprehensive_search',
    20
);

-- Test 4: Brand Search
CALL run_search_test(
    'Brand-Specific Search',
    'BASIC_SEARCH',
    'electronics accessories',
    'Should find electronics brand products and accessories',
    'tpcds_comprehensive_search',
    25
);

-- Test 5: Category Search
CALL run_search_test(
    'Category-Based Search',
    'BASIC_SEARCH',
    'clothing apparel fashion',
    'Should find clothing and fashion items',
    'tpcds_comprehensive_search',
    30
);

-- Test 6: Customer Demographics Search
CALL run_search_test(
    'Customer Demographics Search',
    'BASIC_SEARCH',
    'married male customer',
    'Should find transactions from married male customers',
    'tpcds_comprehensive_search',
    20
);

-- Test 7: Temporal Search
CALL run_search_test(
    'Temporal Date Search',
    'BASIC_SEARCH',
    '2002 2001 transactions',
    'Should find transactions from specific years',
    'tpcds_comprehensive_search',
    25
);

-- Test 8: Geographic Location Search
CALL run_search_test(
    'Geographic Location Search',
    'BASIC_SEARCH',
    'Tennessee Alabama California',
    'Should find stores in specific states',
    'tpcds_comprehensive_search',
    30
);

-- Test 9: Combined Demographic and Location Search
CALL run_search_test(
    'Combined Demographics and Location',
    'BASIC_SEARCH',
    'female customer California store',
    'Should find female customers in California stores',
    'tpcds_comprehensive_search',
    25
);

-- =============================================================================
-- ADVANCED SEARCH TESTS
-- =============================================================================

SELECT 'RUNNING ADVANCED SEARCH TESTS' as test_phase;

-- Test 10: Semantic Search
CALL run_search_test(
    'Semantic Similarity',
    'ADVANCED_SEARCH',
    'comfortable workout clothing',
    'Should find semantically similar fitness apparel',
    'tpcds_product_search',
    15
);

-- Test 11: Complex Query
CALL run_search_test(
    'Complex Multi-Term Query',
    'ADVANCED_SEARCH',
    'premium luxury winter outdoor gear',
    'Should handle complex multi-term semantic search',
    'tpcds_comprehensive_search',
    25
);

-- Test 12: Location-Based Search
CALL run_search_test(
    'Geographic Search',
    'ADVANCED_SEARCH',
    'california nevada stores',
    'Should find stores in western US states',
    'tpcds_comprehensive_search',
    20
);

-- Test 13: Customer Context Search
CALL run_search_test(
    'Customer-Context Search',
    'ADVANCED_SEARCH',
    'high value customer premium',
    'Should find transactions from high-value customers',
    'tpcds_comprehensive_search',
    30
);

-- Test 14: Seasonal Search
CALL run_search_test(
    'Seasonal Product Search',
    'ADVANCED_SEARCH',
    'holiday seasonal gifts presents',
    'Should find seasonal and holiday-related products',
    'tpcds_comprehensive_search',
    40
);

-- =============================================================================
-- FILTERING AND AGGREGATION TESTS
-- =============================================================================

SELECT 'RUNNING FILTERING TESTS' as test_phase;

-- Test 15: Numeric Filtering
CREATE OR REPLACE PROCEDURE test_numeric_filtering()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    result_count INTEGER;
    test_status VARCHAR;
BEGIN
    CREATE OR REPLACE TEMPORARY TABLE temp_filter_results AS
    WITH search_results AS (
        SELECT * FROM TABLE(
            SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
                'tpcds_comprehensive_search',
                'electronics premium',
                LIMIT => 100
            )
        )
    )
    SELECT * FROM search_results
    WHERE unit_price > 50.00 AND quantity >= 2;
    
    SELECT COUNT(*) INTO result_count FROM temp_filter_results;
    
    IF (result_count > 0) THEN
        test_status := 'PASS';
    ELSE
        test_status := 'FAIL';
    END IF;
    
    INSERT INTO cortex_search_test_results (
        test_name, test_category, search_query, expected_behavior,
        actual_results, test_status, execution_time_ms, notes
    ) VALUES (
        'Numeric Filtering', 'FILTERING', 'electronics premium + price>$50',
        'Should filter by price and quantity', result_count, test_status, 0,
        'Filtered for unit_price > 50 AND quantity >= 2'
    );
    
    RETURN CONCAT('Numeric filtering test: ', test_status, ' (', result_count, ' results)');
END;
$$;

CALL test_numeric_filtering();

-- Test 16: Complex AND/OR Logic
CREATE OR REPLACE PROCEDURE test_complex_logic()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    result_count INTEGER;
    test_status VARCHAR;
BEGIN
    CREATE OR REPLACE TEMPORARY TABLE temp_logic_results AS
    WITH search_results AS (
        SELECT * FROM TABLE(
            SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
                'tpcds_comprehensive_search',
                'clothing fashion apparel',
                LIMIT => 150
            )
        )
    )
    SELECT * FROM search_results
    WHERE (total_sales > 100 AND profit > 20)
       OR (quantity >= 5 AND unit_price > 25);
    
    SELECT COUNT(*) INTO result_count FROM temp_logic_results;
    
    IF (result_count > 0) THEN
        test_status := 'PASS';
    ELSE
        test_status := 'FAIL';
    END IF;
    
    INSERT INTO cortex_search_test_results (
        test_name, test_category, search_query, expected_behavior,
        actual_results, test_status, execution_time_ms, notes
    ) VALUES (
        'Complex AND/OR Logic', 'FILTERING', 'clothing + complex conditions',
        'Should handle complex boolean logic', result_count, test_status, 0,
        'Tested: (sales>100 AND profit>20) OR (qty>=5 AND price>25)'
    );
    
    RETURN CONCAT('Complex logic test: ', test_status, ' (', result_count, ' results)');
END;
$$;

CALL test_complex_logic();

-- Test 17: Aggregation Functions
CREATE OR REPLACE PROCEDURE test_aggregations()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    distinct_categories INTEGER;
    total_transactions INTEGER;
    test_status VARCHAR;
BEGIN
    CREATE OR REPLACE TEMPORARY TABLE temp_agg_results AS
    WITH search_results AS (
        SELECT * FROM TABLE(
            SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
                'tpcds_comprehensive_search',
                'retail products sales',
                LIMIT => 200
            )
        )
    )
    SELECT 
        category_name,
        COUNT(DISTINCT item_key) as unique_products,
        COUNT(*) as total_transactions,
        SUM(total_sales) as total_revenue
    FROM search_results
    WHERE category_name IS NOT NULL
    GROUP BY category_name;
    
    SELECT COUNT(DISTINCT category_name), SUM(total_transactions)
    INTO distinct_categories, total_transactions
    FROM temp_agg_results;
    
    IF (distinct_categories > 0 AND total_transactions > 0) THEN
        test_status := 'PASS';
    ELSE
        test_status := 'FAIL';
    END IF;
    
    INSERT INTO cortex_search_test_results (
        test_name, test_category, search_query, expected_behavior,
        actual_results, test_status, execution_time_ms, notes
    ) VALUES (
        'Aggregation Functions', 'AGGREGATION', 'retail products + group by',
        'Should perform DISTINCT, COUNT, SUM operations', total_transactions, test_status, 0,
        CONCAT('Found ', distinct_categories, ' distinct categories')
    );
    
    RETURN CONCAT('Aggregation test: ', test_status, ' (', distinct_categories, ' categories, ', total_transactions, ' total transactions)');
END;
$$;

CALL test_aggregations();

-- =============================================================================
-- ENTITLEMENT TESTS
-- =============================================================================

SELECT 'RUNNING ENTITLEMENT TESTS' as test_phase;

-- Test 18: Role-Based Access
CALL run_search_test(
    'Role-Based Data Access',
    'ENTITLEMENTS',
    'customer financial data',
    'Should respect role-based access controls',
    'tpcds_entitled_search',
    15
);

-- Test 19: Data Masking
CREATE OR REPLACE PROCEDURE test_data_masking()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    masked_count INTEGER;
    total_count INTEGER;
    test_status VARCHAR;
BEGIN
    CREATE OR REPLACE TEMPORARY TABLE temp_mask_results AS
    SELECT *
    FROM TABLE(
        SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
            'tpcds_entitled_search',
            'sensitive customer information',
            LIMIT => 50
        )
    );
    
    SELECT COUNT(*) INTO total_count FROM temp_mask_results;
    SELECT COUNT(*) INTO masked_count 
    FROM temp_mask_results 
    WHERE entitled_customer_info LIKE 'CUSTOMER_%' 
       OR entitled_financial_data = 'FINANCIAL_DATA_RESTRICTED';
    
    IF (masked_count > 0) THEN
        test_status := 'PASS - Data Masking Active';
    ELSE
        test_status := 'WARNING - No Masking Detected';
    END IF;
    
    INSERT INTO cortex_search_test_results (
        test_name, test_category, search_query, expected_behavior,
        actual_results, test_status, execution_time_ms, notes
    ) VALUES (
        'Data Masking Validation', 'ENTITLEMENTS', 'sensitive customer information',
        'Should mask PII and financial data', total_count, test_status, 0,
        CONCAT('Masked records: ', masked_count, ' out of ', total_count)
    );
    
    RETURN CONCAT('Data masking test: ', test_status);
END;
$$;

CALL test_data_masking();

-- =============================================================================
-- PERFORMANCE TESTS
-- =============================================================================

SELECT 'RUNNING PERFORMANCE TESTS' as test_phase;

-- Test 20: Large Result Set Performance
CREATE OR REPLACE PROCEDURE test_performance(search_term VARCHAR, max_results INTEGER)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    start_time TIMESTAMP_NTZ;
    end_time TIMESTAMP_NTZ;
    execution_time INTEGER;
    result_count INTEGER;
    test_status VARCHAR;
BEGIN
    start_time := CURRENT_TIMESTAMP();
    
    CREATE OR REPLACE TEMPORARY TABLE temp_perf_results AS
    SELECT * FROM TABLE(
        SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
            'tpcds_comprehensive_search',
            search_term,
            LIMIT => max_results
        )
    );
    
    SELECT COUNT(*) INTO result_count FROM temp_perf_results;
    end_time := CURRENT_TIMESTAMP();
    execution_time := DATEDIFF('milliseconds', start_time, end_time);
    
    IF (execution_time < 5000 AND result_count > 0) THEN
        test_status := 'PASS';
    ELSEIF (execution_time >= 5000) THEN
        test_status := 'SLOW - Over 5 seconds';
    ELSE
        test_status := 'FAIL - No results';
    END IF;
    
    INSERT INTO cortex_search_test_results (
        test_name, test_category, search_query, expected_behavior,
        actual_results, test_status, execution_time_ms, notes
    ) VALUES (
        CONCAT('Performance Test - ', max_results, ' results'),
        'PERFORMANCE', search_term, 'Should complete within 5 seconds',
        result_count, test_status, execution_time,
        CONCAT('Target: ', max_results, ' results, Actual: ', result_count, ' in ', execution_time, 'ms')
    );
    
    RETURN CONCAT('Performance test: ', test_status, ' (', execution_time, 'ms)');
END;
$$;

-- Run performance tests with different result sizes
CALL test_performance('popular consumer products', 50);
CALL test_performance('retail sales transactions', 100);
CALL test_performance('customer purchase history', 200);

-- =============================================================================
-- TEST RESULTS ANALYSIS
-- =============================================================================

SELECT 'ANALYZING TEST RESULTS' as analysis_phase;

-- Test Summary Report
CREATE OR REPLACE VIEW test_summary_report AS
SELECT 
    test_category,
    COUNT(*) as total_tests,
    SUM(CASE WHEN test_status LIKE 'PASS%' THEN 1 ELSE 0 END) as passed_tests,
    SUM(CASE WHEN test_status LIKE 'FAIL%' THEN 1 ELSE 0 END) as failed_tests,
    SUM(CASE WHEN test_status LIKE 'WARNING%' THEN 1 ELSE 0 END) as warning_tests,
    ROUND(AVG(execution_time_ms), 2) as avg_execution_time_ms,
    ROUND(AVG(actual_results), 2) as avg_results_count,
    MIN(test_timestamp) as first_test,
    MAX(test_timestamp) as last_test
FROM cortex_search_test_results
GROUP BY test_category
ORDER BY test_category;

-- Performance Analysis
CREATE OR REPLACE VIEW performance_analysis AS
SELECT 
    test_name,
    execution_time_ms,
    actual_results,
    ROUND(actual_results / GREATEST(execution_time_ms, 1) * 1000, 2) as results_per_second,
    CASE 
        WHEN execution_time_ms < 1000 THEN 'FAST'
        WHEN execution_time_ms < 3000 THEN 'MODERATE'
        WHEN execution_time_ms < 5000 THEN 'SLOW'
        ELSE 'VERY_SLOW'
    END as performance_rating
FROM cortex_search_test_results
WHERE test_category = 'PERFORMANCE'
ORDER BY execution_time_ms;

-- Failed Tests Analysis
CREATE OR REPLACE VIEW failed_tests_analysis AS
SELECT 
    test_name,
    test_category,
    search_query,
    expected_behavior,
    test_status,
    notes,
    test_timestamp
FROM cortex_search_test_results
WHERE test_status NOT LIKE 'PASS%'
ORDER BY test_timestamp DESC;

-- Display results
SELECT 'TEST EXECUTION COMPLETE - SUMMARY REPORT' as final_status;

SELECT * FROM test_summary_report;

SELECT 'PERFORMANCE ANALYSIS' as section;
SELECT * FROM performance_analysis;

SELECT 'FAILED TESTS (if any)' as section;
SELECT * FROM failed_tests_analysis;

-- Overall test success rate
SELECT 
    'OVERALL TEST RESULTS' as section,
    COUNT(*) as total_tests_run,
    SUM(CASE WHEN test_status LIKE 'PASS%' THEN 1 ELSE 0 END) as total_passed,
    ROUND(
        SUM(CASE WHEN test_status LIKE 'PASS%' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 
        1
    ) as success_rate_pct,
    ROUND(AVG(execution_time_ms), 0) as avg_response_time_ms
FROM cortex_search_test_results;

-- Cleanup temporary tables
DROP TABLE IF EXISTS temp_test_results;
DROP TABLE IF EXISTS temp_filter_results;
DROP TABLE IF EXISTS temp_logic_results;
DROP TABLE IF EXISTS temp_agg_results;
DROP TABLE IF EXISTS temp_mask_results;
DROP TABLE IF EXISTS temp_perf_results;

SELECT 'Cortex Search Testing Framework Complete!' as final_message,
       'Check test_summary_report, performance_analysis, and failed_tests_analysis views for details' as next_steps;
