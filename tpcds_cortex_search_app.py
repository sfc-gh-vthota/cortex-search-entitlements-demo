# TPCDS Cortex Search Demo - Streamlit Application
# This app demonstrates TPCDS product and customer search via Cortex Search
# with performance monitoring and detailed analytics

import streamlit as st
import pandas as pd
import numpy as np
import time
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import json

# Snowflake connector for Streamlit in Snowflake
from snowflake.snowpark.context import get_active_session
from snowflake.core import Root  # Python API for Cortex Search

# Initialize Snowflake session
@st.cache_resource
def init_connection():
    """Initialize connection to Snowflake"""
    return get_active_session()

def convert_data_types(df):
    """Convert data types from Snowflake results for proper analysis"""
    if df.empty:
        return df
    
    # Convert numeric columns - handle both upper and lower case
    numeric_columns = ['unit_price', 'UNIT_PRICE', 'total_sales', 'TOTAL_SALES', 
                      'quantity', 'QUANTITY', 'profit', 'PROFIT', 'current_price', 'CURRENT_PRICE']
    for col in numeric_columns:
        if col in df.columns:
            try:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            except Exception as e:
                st.warning(f"Failed to convert {col} to numeric: {e}")
    
    # Convert date columns - handle both upper and lower case  
    date_columns = ['transaction_date', 'TRANSACTION_DATE']
    for col in date_columns:
        if col in df.columns:
            try:
                df[col] = pd.to_datetime(df[col], errors='coerce')
            except Exception as e:
                st.warning(f"Failed to convert {col} to datetime: {e}")
    
    # Clean any null values that might have been created (handle both cases)
    price_col = None
    if 'unit_price' in df.columns:
        price_col = 'unit_price'
    elif 'UNIT_PRICE' in df.columns:
        price_col = 'UNIT_PRICE'
    
    if price_col:
        original_length = len(df)
        df = df.dropna(subset=[price_col])
        if len(df) < original_length:
            st.info(f"Dropped {original_length - len(df)} rows with null {price_col}")
    
    return df

def search_tpcds_cortex_optimized(session, search_query="", filters=None, limit=50):
    """Optimized TPCDS Cortex Search using Python API with precise response time measurement"""
    
    # 🕐 START TIMING - Capture exact start of operation
    start_time = time.time()
    
    try:
        # 🔗 STEP 1: Initialize connection to Cortex Search service
        root = Root(session)
        database = root.databases["SAMPLE_DATA"]
        schema = database.schemas["TPCDS_SF10TCL"]
        cortex_search_service = schema.cortex_search_services["tpcds_comprehensive_search"]
        
        # 🎯 STEP 2: Prepare optimized search parameters
        # Define only essential columns for better performance
        essential_columns = [
            "item_key", "customer_key", "store_key", "date_key",
            "item_description", "product_name", "brand_name", "category_name",
            "store_name", "store_location", "customer_info",
            "unit_price", "total_sales", "quantity", "profit",
            "customer_gender", "marital_status", "transaction_date", "year"
        ]
        
        # Create filter object if filters are provided
        filter_object = None
        if filters:
            filter_conditions = []
            
            if filters.get('price_range') and filters['price_range']['min'] > 0:
                filter_conditions.append({
                    "@and": [
                        {"@gte": {"unit_price": filters['price_range']['min']}},
                        {"@lte": {"unit_price": filters['price_range']['max']}}
                    ]
                })
            
            if filters.get('year_range') and filters['year_range']:
                if len(filters['year_range']) == 1:
                    filter_conditions.append({"@eq": {"year": filters['year_range'][0]}})
                else:
                    filter_conditions.append({"@in": {"year": filters['year_range']}})
            
            if filters.get('customer_gender') and filters['customer_gender']:
                if len(filters['customer_gender']) == 1:
                    filter_conditions.append({"@eq": {"customer_gender": filters['customer_gender'][0]}})
                else:
                    filter_conditions.append({"@in": {"customer_gender": filters['customer_gender']}})
            
            if filters.get('marital_status') and filters['marital_status']:
                if len(filters['marital_status']) == 1:
                    filter_conditions.append({"@eq": {"marital_status": filters['marital_status'][0]}})
                else:
                    filter_conditions.append({"@in": {"marital_status": filters['marital_status']}})
            
            if filters.get('category_name') and filters['category_name']:
                if len(filters['category_name']) == 1:
                    filter_conditions.append({"@eq": {"category_name": filters['category_name'][0]}})
                else:
                    filter_conditions.append({"@in": {"category_name": filters['category_name']}})
            
            if filter_conditions:
                if len(filter_conditions) == 1:
                    filter_object = filter_conditions[0]
                else:
                    filter_object = {"@and": filter_conditions}
        
        # 🎯 STEP 3: Prepare scoring configuration for optimal performance
        scoring_config = {
            "reranker": "none"  # Disable reranking for faster response times
        }
        
        # 🚀 STEP 4: Execute optimized search call with performance tuning
        if search_query.strip():
            # Semantic search with filtering and no reranking
            search_response = cortex_search_service.search(
                query=search_query,
                columns=essential_columns,
                filter=filter_object,
                scoring_config=scoring_config,
                limit=limit
            )
        else:
            # Broad search for "show all" with filtering and no reranking
            search_response = cortex_search_service.search(
                query="products transactions",
                columns=essential_columns,
                filter=filter_object,
                scoring_config=scoring_config,
                limit=limit
            )
        
        # 🕐 END TIMING - Capture time immediately after API response
        end_time = time.time()
        response_time = (end_time - start_time) * 1000  # Convert to milliseconds
        
        # 📊 STEP 4: Process results efficiently
        search_results = []
        result_count = 0
        
        if hasattr(search_response, 'results') and search_response.results:
            result_count = len(search_response.results)
            
            # Efficient result processing
            for result in search_response.results:
                if isinstance(result, dict):
                    search_results.append(result)
                else:
                    # Convert to dict if needed
                    search_results.append(dict(result))
        
        # Create DataFrame and apply data type conversions
        df = pd.DataFrame(search_results)
        if not df.empty:
            df = convert_data_types(df)
            
            # Sort by unit_price for consistent ordering
            price_col = get_column_name(df, ['unit_price', 'UNIT_PRICE'])
            if price_col:
                df = df.sort_values(price_col, ascending=False).reset_index(drop=True)
        
        # 📈 Display performance metrics with optimization details
        st.success(f"⚡ **Ultra-Optimized Python API Response**: {response_time:.0f}ms | Found {result_count} TPCDS results")
        st.info(f"🎯 **Performance Optimizations Applied**: Server-side filtering + No reranking + Essential columns only")
        
        return df, response_time, result_count
        
    except Exception as e:
        # Ensure timing is captured even on error
        end_time = time.time()
        response_time = (end_time - start_time) * 1000
        
        st.error(f"❌ **Cortex Search API Error**: {str(e)}")
        st.error(f"⏱️ **Failed Request Time**: {response_time:.0f}ms")
        
        return pd.DataFrame(), response_time, 0

def get_column_name(df, possible_names):
    """Helper function to find column name regardless of case"""
    for name in possible_names:
        if name in df.columns:
            return name
    return None

def get_search_summary(df, search_info):
    """Generate comprehensive search result summary"""
    if df.empty:
        return {}
    
    # Handle different column name cases
    price_col = get_column_name(df, ['unit_price', 'UNIT_PRICE'])
    category_col = get_column_name(df, ['category_name', 'CATEGORY_NAME'])
    brand_col = get_column_name(df, ['brand_name', 'BRAND_NAME'])
    gender_col = get_column_name(df, ['customer_gender', 'CUSTOMER_GENDER'])
    location_col = get_column_name(df, ['store_location', 'STORE_LOCATION'])
    date_col = get_column_name(df, ['transaction_date', 'TRANSACTION_DATE'])
    
    summary = {
        'total_results': len(df),
        'total_value': df[price_col].sum() if price_col else 0,
        'avg_price': df[price_col].mean() if price_col else 0,
        'max_price': df[price_col].max() if price_col else 0,
        'min_price': df[price_col].min() if price_col else 0,
        'categories': df[category_col].nunique() if category_col else 0,
        'brands': df[brand_col].nunique() if brand_col else 0,
        'locations': df[location_col].nunique() if location_col else 0,
        'genders': df[gender_col].nunique() if gender_col else 0,
        'date_range': {
            'earliest': df[date_col].min() if date_col else None,
            'latest': df[date_col].max() if date_col else None
        },
        'search_info': search_info
    }
    
    return summary

def display_performance_metrics(response_time, result_count, summary):
    """Display optimized performance metrics with detailed timing analysis"""
    col1, col2, col3, col4 = st.columns(4)
    
    # Enhanced performance classification
    if response_time < 300:
        perf_status = "⚡ Excellent"
        perf_color = "🟢"
    elif response_time < 800:
        perf_status = "✅ Good" 
        perf_color = "🟡"
    elif response_time < 2000:
        perf_status = "⚠️ Acceptable"
        perf_color = "🟠"
    else:
        perf_status = "🔴 Slow"
        perf_color = "🔴"
    
    with col1:
        st.metric(
            label="⚡ Optimized API Response",
            value=f"{response_time:.0f}ms",
            delta=f"{perf_color} {perf_status}"
        )
        st.caption(f"Single Python API call with server-side filtering")
    
    with col2:
        st.metric(
            label="📊 Search Results", 
            value=f"{result_count:,}",
            delta="matches found"
        )
        st.caption("Multi-dimensional search results")
    
    with col3:
        if summary and 'total_value' in summary:
            st.metric(
                label="💰 Total Value",
                value=f"${summary['total_value']:,.2f}",
                delta=f"avg ${summary['avg_price']:,.0f}"
            )
            st.caption(f"Range: ${summary['min_price']:,.0f} - ${summary['max_price']:,.0f}")
    
    with col4:
        if summary and 'categories' in summary:
            st.metric(
                label="🏷️ Data Scope",
                value=f"{summary['categories']} categories",
                delta=f"{summary['brands']} brands"
            )
            st.caption(f"{summary['locations']} locations")
    
    # Add timing breakdown information
    st.info(f"""
    📊 **Ultra-Optimized Performance Analysis**: 
    • **API Call**: Measured from connection → search execution → response received
    • **Server-side Filtering**: Applied at Cortex Search service level  
    • **No Reranking**: Disabled reranker for maximum speed (`"reranker": "none"`)
    • **Essential Columns**: Only required fields to minimize data transfer
    • **Response Time**: Pure API response time (excludes UI rendering and visualizations)
    • **Efficiency**: {result_count} results returned in {response_time:.0f}ms = **{result_count/(response_time/1000):.1f} results/second**
    """)

def create_visualizations(df, summary):
    """Create interactive visualizations of TPCDS search data"""
    if df.empty:
        return
    
    st.subheader("📈 TPCDS Search Analytics")
    
    # Create tabs for different visualizations
    viz_tab1, viz_tab2, viz_tab3, viz_tab4 = st.tabs([
        "💰 Price Analysis", 
        "🏷️ Category & Brand", 
        "🌍 Geographic View",
        "👥 Demographics"
    ])
    
    with viz_tab1:
        # Get column names (handle different cases)
        price_col = get_column_name(df, ['unit_price', 'UNIT_PRICE'])
        item_desc_col = get_column_name(df, ['item_description', 'ITEM_DESCRIPTION'])
        product_name_col = get_column_name(df, ['product_name', 'PRODUCT_NAME'])
        
        if price_col:
            # Price distribution histogram
            fig_hist = px.histogram(
                df, 
                x=price_col, 
                title="Price Distribution",
                labels={price_col: 'Price ($)', 'count': 'Number of Items'}
            )
            fig_hist.update_layout(showlegend=False)
            st.plotly_chart(fig_hist, use_container_width=True)
            
            # Top 10 highest priced items
            st.write("**🔝 Top 10 Highest Priced Items:**")
            display_cols = [col for col in [item_desc_col, product_name_col, price_col] if col]
            if display_cols:
                top_items = df.nlargest(10, price_col)[display_cols]
                st.dataframe(top_items, use_container_width=True)
        else:
            st.warning("Price column not found - cannot display price analysis")
    
    with viz_tab2:
        # Get column names for category analysis
        price_col = get_column_name(df, ['unit_price', 'UNIT_PRICE'])
        category_col = get_column_name(df, ['category_name', 'CATEGORY_NAME'])
        brand_col = get_column_name(df, ['brand_name', 'BRAND_NAME'])
        
        col1, col2 = st.columns(2)
        
        with col1:
            if category_col:
                # Category analysis
                category_counts = df[category_col].value_counts().head(10)
                fig_category = px.bar(
                    x=category_counts.values, 
                    y=category_counts.index,
                    orientation='h',
                    title="Top 10 Categories",
                    labels={'x': 'Count', 'y': 'Category'}
                )
                st.plotly_chart(fig_category, use_container_width=True)
        
        with col2:
            if brand_col:
                # Brand analysis
                brand_counts = df[brand_col].value_counts().head(10)
                fig_brand = px.pie(
                    values=brand_counts.values, 
                    names=brand_counts.index,
                    title="Top 10 Brands"
                )
                st.plotly_chart(fig_brand, use_container_width=True)
    
    with viz_tab3:
        # Get column names for geographic analysis
        location_col = get_column_name(df, ['store_location', 'STORE_LOCATION'])
        price_col = get_column_name(df, ['unit_price', 'UNIT_PRICE'])
        
        if location_col:
            # Location analysis
            location_counts = df[location_col].value_counts().head(15)
            
            fig_geo = px.bar(
                x=location_counts.values,
                y=location_counts.index,
                orientation='h',
                title="Top 15 Store Locations",
                labels={'x': 'Transaction Count', 'y': 'Location'}
            )
            st.plotly_chart(fig_geo, use_container_width=True)
            
            if price_col:
                # Average price by location
                location_price = df.groupby(location_col)[price_col].mean().sort_values(ascending=False).head(10)
                
                fig_price_loc = px.bar(
                    x=location_price.values,
                    y=location_price.index,
                    orientation='h',
                    title="Average Price by Top 10 Locations",
                    labels={'x': 'Average Price ($)', 'y': 'Location'}
                )
                st.plotly_chart(fig_price_loc, use_container_width=True)
        else:
            st.warning("Location column not found for geographic analysis")
    
    with viz_tab4:
        # Get column names for demographic analysis
        gender_col = get_column_name(df, ['customer_gender', 'CUSTOMER_GENDER'])
        marital_col = get_column_name(df, ['marital_status', 'MARITAL_STATUS'])
        
        col1, col2 = st.columns(2)
        
        with col1:
            if gender_col:
                gender_counts = df[gender_col].value_counts()
                fig_gender = px.pie(
                    values=gender_counts.values,
                    names=gender_counts.index,
                    title="Customer Gender Distribution"
                )
                st.plotly_chart(fig_gender, use_container_width=True)
        
        with col2:
            if marital_col:
                marital_counts = df[marital_col].value_counts()
                fig_marital = px.pie(
                    values=marital_counts.values,
                    names=marital_counts.index,
                    title="Marital Status Distribution"
                )
                st.plotly_chart(fig_marital, use_container_width=True)

def main():
    """Main Streamlit application"""
    # Page configuration
    st.set_page_config(
        page_title="TPCDS Cortex Search Demo",
        page_icon="🔍",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    # Custom CSS for better styling
    st.markdown("""
        <style>
        .main-header {
            font-size: 3rem;
            color: #1f77b4;
            text-align: center;
            margin-bottom: 2rem;
        }
        .sub-header {
            font-size: 1.5rem;
            color: #ff7f0e;
            margin-top: 2rem;
            margin-bottom: 1rem;
        }
        .metric-container {
            background-color: #f0f2f6;
            padding: 1rem;
            border-radius: 0.5rem;
            margin: 0.5rem 0;
        }
        .search-container {
            background-color: #e8f4f8;
            padding: 1.5rem;
            border-radius: 0.75rem;
            margin: 1rem 0;
        }
        </style>
    """, unsafe_allow_html=True)
    
    # Main header
    st.markdown('<h1 class="main-header">🔍 TPCDS Cortex Search Demo</h1>', unsafe_allow_html=True)
    st.markdown("**Multi-dimensional product and customer search with performance monitoring**")
    st.info("🚀 **Ultra-Optimized Cortex Search Python API** - Server-side filtering + No reranking + Essential columns + Precise response timing")
    
    # Initialize connection
    session = init_connection()
    
    # Sidebar for search controls and filters
    with st.sidebar:
        st.header("🎛️ Search Controls")
        
        # Search options
        st.subheader("🔍 Search Options")
        search_query = st.text_input(
            "Semantic search query:",
            placeholder="e.g., premium electronics, comfortable clothing, California stores",
            help="Use natural language to search across products, demographics, and locations"
        )
        
        result_limit = st.slider("Max results to return:", 10, 200, 50, 10)
        
        # Filters section
        st.subheader("🔧 Filters")
        
        filters = {}
        
        # Price range filter
        st.write("**💰 Price Range**")
        price_range = st.slider(
            "Select price range ($):",
            min_value=0.0,
            max_value=1000.0,
            value=(0.0, 1000.0),
            step=10.0
        )
        filters['price_range'] = {'min': price_range[0], 'max': price_range[1]}
        
        # Year filter
        st.write("**📅 Transaction Year**")
        year_options = [2000, 2001, 2002, 2003]
        selected_years = st.multiselect(
            "Select years:",
            options=year_options,
            default=year_options
        )
        filters['year_range'] = selected_years
        
        # Demographics
        st.write("**👥 Customer Demographics**")
        gender_options = ['M', 'F']
        selected_genders = st.multiselect("Customer Gender:", options=gender_options)
        filters['customer_gender'] = selected_genders if selected_genders else None
        
        marital_status_options = ['M', 'S', 'D', 'W', 'U']
        selected_marital = st.multiselect("Marital Status:", options=marital_status_options)
        filters['marital_status'] = selected_marital if selected_marital else None
        
        # Categories
        st.write("**🛍️ Product Categories**")
        category_options = ['Electronics', 'Clothing', 'Home', 'Sports', 'Books']
        selected_categories = st.multiselect("Categories:", options=category_options)
        filters['category_name'] = selected_categories if selected_categories else None
        
        # Clear filters
        if st.button("🗑️ Clear All Filters"):
            st.rerun()
    
    # Main content area
    st.markdown('<div class="search-container">', unsafe_allow_html=True)
    
    # Search button
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        if st.button("🚀 Search TPCDS Data", type="primary", use_container_width=True):
            # Clear any previous results
            if 'search_results' in st.session_state:
                del st.session_state['search_results']
    
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Perform optimized search with detailed timing
    with st.spinner("⚡ Executing optimized TPCDS Cortex Search..."):
        df, response_time, result_count = search_tpcds_cortex_optimized(
            session, search_query, filters, result_limit
        )
        
        # Generate summary
        search_info = {
            'query': search_query if search_query else 'All products',
            'filters_applied': any(filters.values())
        }
        summary = get_search_summary(df, search_info)
    
    # Display performance metrics
    st.subheader("⚡ Performance Metrics")
    display_performance_metrics(response_time, result_count, summary)
    
    # Display results
    if not df.empty:
        # Search summary section
        st.subheader("📊 Search Results Summary")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown(f"""
            **🎯 Search Summary:**
            - **Query:** {summary['search_info']['query']}
            - **Total Results:** {summary['total_results']:,}
            - **Total Value:** ${summary['total_value']:,.2f}
            - **Average Price:** ${summary['avg_price']:,.2f}
            - **Price Range:** ${summary['min_price']:,.2f} - ${summary['max_price']:,.2f}
            """)
        
        with col2:
            st.markdown(f"""
            **📈 Data Coverage:**
            - **Categories:** {summary['categories']}
            - **Brands:** {summary['brands']}
            - **Locations:** {summary['locations']}
            - **Customer Segments:** {summary['genders']}
            - **Date Range:** {summary['date_range']['earliest']} to {summary['date_range']['latest']}
            """)
        
        # Visualizations
        create_visualizations(df, summary)
        
        # Detailed results table
        st.subheader("📋 Detailed Results")
        
        # Column selection
        available_columns = df.columns.tolist()
        key_columns = [
            'item_description', 'product_name', 'brand_name', 'category_name',
            'unit_price', 'quantity', 'customer_gender', 'marital_status', 
            'store_location', 'year'
        ]
        
        # Find actual column names
        display_columns = []
        for key_col in key_columns:
            found_col = get_column_name(df, [key_col, key_col.upper()])
            if found_col:
                display_columns.append(found_col)
        
        selected_columns = st.multiselect(
            "Select columns to display:",
            options=available_columns,
            default=display_columns[:8] if len(display_columns) > 8 else display_columns
        )
        
        if selected_columns:
            # Format display dataframe
            display_df = df[selected_columns].copy()
            
            # Format price columns
            for col in selected_columns:
                if 'price' in col.lower():
                    display_df[col] = display_df[col].apply(lambda x: f"${x:,.2f}" if pd.notnull(x) else "N/A")
                elif col.lower() in ['quantity']:
                    display_df[col] = display_df[col].apply(lambda x: f"{x:,.0f}" if pd.notnull(x) else "N/A")
            
            st.dataframe(display_df, use_container_width=True, height=400)
        
        # Export options
        col1, col2, col3 = st.columns(3)
        with col1:
            csv_data = df.to_csv(index=False)
            st.download_button(
                "📥 Download as CSV",
                csv_data,
                file_name=f"tpcds_search_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )
        
        with col2:
            json_data = df.to_json(orient='records', date_format='iso')
            st.download_button(
                "📥 Download as JSON",
                json_data,
                file_name=f"tpcds_search_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                mime="application/json"
            )
        
    else:
        # No results found
        st.warning("🚫 No results found for your search criteria.")
        st.info("""
        **Possible reasons:**
        - Search query may be too specific
        - Filters may be too restrictive
        - Search service may not be properly deployed
        
        **Try:**
        - Using broader search terms (e.g., "electronics", "clothing")
        - Removing some filters
        - Checking if the tpcds_comprehensive_search service exists
        """)
    
    # Footer
    st.markdown("---")
    st.markdown("""
    <div style='text-align: center; color: #666666; padding: 1rem;'>
        <p>🔍 <strong>TPCDS Cortex Search Demo</strong> | 
        Powered by Snowflake Cortex Search & Enhanced TPCDS Service | 
        Built with ❤️ using Streamlit</p>
        <p><em>Multi-dimensional product and customer search with performance monitoring</em></p>
    </div>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    main()
