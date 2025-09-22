# Cortex Search Entitlements Demo - Streamlit Application
# This app demonstrates user-specific transaction access via Cortex Search
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
    numeric_columns = ['AMOUNT', 'amount', 'ENTITLED_USER_COUNT', 'entitled_user_count']
    for col in numeric_columns:
        if col in df.columns:
            try:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            except Exception as e:
                st.warning(f"Failed to convert {col} to numeric: {e}")
    
    # Convert date columns - handle both upper and lower case  
    date_columns = ['TRANSACTION_DATE', 'transaction_date']
    for col in date_columns:
        if col in df.columns:
            try:
                df[col] = pd.to_datetime(df[col], errors='coerce')
            except Exception as e:
                st.warning(f"Failed to convert {col} to datetime: {e}")
    
    # Clean any null values that might have been created (handle both cases)
    amount_col = None
    if 'AMOUNT' in df.columns:
        amount_col = 'AMOUNT'
    elif 'amount' in df.columns:
        amount_col = 'amount'
    
    if amount_col:
        original_length = len(df)
        df = df.dropna(subset=[amount_col])
        if len(df) < original_length:
            st.info(f"Dropped {original_length - len(df)} rows with null {amount_col}")
    
    return df

def get_users_list(session):
    """Get list of active users from user_region_access table"""
    try:
        query = """
        SELECT DISTINCT 
            user_id,
            user_name,
            region_name,
            access_level,
            status
        FROM CORTEX_SEARCH_ENTITLEMENT_DB.DYNAMIC_DEMO.user_region_access 
        WHERE status = 'ACTIVE'
        ORDER BY user_id
        """
        result = session.sql(query).collect()
        return pd.DataFrame([row.asDict() for row in result])
    except Exception as e:
        st.error(f"Error fetching users: {str(e)}")
        return pd.DataFrame()

def search_transactions_cortex_optimized(session, user_id, search_query="", limit=50):
    """Optimized Cortex Search using Python API with precise response time measurement"""
    
    # 🕐 START TIMING - Capture exact start of operation
    start_time = time.time()
    
    try:
        # 🔗 STEP 1: Initialize connection to Cortex Search service
        root = Root(session)
        database = root.databases["CORTEX_SEARCH_ENTITLEMENT_DB"]
        schema = database.schemas["DYNAMIC_DEMO"]
        cortex_search_service = schema.cortex_search_services["financial_search_service"]
        
        # 🎯 STEP 2: Prepare optimized search parameters
        # Define only essential columns for better performance
        essential_columns = [
            "txn_id", "region_name", "description", "amount", "transaction_type", 
            "category", "merchant_name", "transaction_date", "entitled_user_ids"
        ]
        
        # Create entitlement filter (server-side filtering for optimal performance)
        entitlement_filter = {
            "@contains": {
                "entitled_user_ids": user_id
            }
        }
        
        # 🎯 STEP 3: Prepare scoring configuration for optimal performance
        scoring_config = {
            "reranker": "none"  # Disable reranking for faster response times
        }
        
        # 🚀 STEP 4: Execute optimized search call with performance tuning
        if search_query.strip():
            # Semantic search with user-specific filtering and no reranking
            search_response = cortex_search_service.search(
                query=search_query,
                columns=essential_columns,
                filter=entitlement_filter,
                scoring_config=scoring_config,
                limit=limit
            )
        else:
            # Broad search for "show all" with entitlement filtering and no reranking
            search_response = cortex_search_service.search(
                query="transaction",
                columns=essential_columns,
                filter=entitlement_filter,
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
            
            # Sort by amount for consistent ordering
            amount_col = get_column_name(df, ['AMOUNT', 'amount'])
            if amount_col:
                df = df.sort_values(amount_col, ascending=False).reset_index(drop=True)
        
        # 📈 Display performance metrics with optimization details
        st.success(f"⚡ **Ultra-Optimized Python API Response**: {response_time:.0f}ms | Found {result_count} entitled transactions")
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

def get_transaction_summary(df, user_info):
    """Generate comprehensive transaction summary"""
    if df.empty:
        return {}
    
    # Handle different column name cases (Python API might return lowercase)
    amount_col = get_column_name(df, ['AMOUNT', 'amount'])
    region_col = get_column_name(df, ['REGION_NAME', 'region_name'])
    category_col = get_column_name(df, ['CATEGORY', 'category'])
    merchant_col = get_column_name(df, ['MERCHANT_NAME', 'merchant_name'])
    txn_type_col = get_column_name(df, ['TRANSACTION_TYPE', 'transaction_type'])
    date_col = get_column_name(df, ['TRANSACTION_DATE', 'transaction_date'])
    
    summary = {
        'total_transactions': len(df),
        'total_amount': df[amount_col].sum() if amount_col else 0,
        'avg_amount': df[amount_col].mean() if amount_col else 0,
        'max_amount': df[amount_col].max() if amount_col else 0,
        'min_amount': df[amount_col].min() if amount_col else 0,
        'regions_covered': df[region_col].nunique() if region_col else 0,
        'categories': df[category_col].nunique() if category_col else 0,
        'merchants': df[merchant_col].nunique() if merchant_col else 0,
        'transaction_types': df[txn_type_col].nunique() if txn_type_col else 0,
        'date_range': {
            'earliest': df[date_col].min() if date_col else None,
            'latest': df[date_col].max() if date_col else None
        },
        'user_info': user_info
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
        # Add performance context
        st.caption(f"Single Python API call with server-side filtering")
    
    with col2:
        st.metric(
            label="📊 Entitled Results", 
            value=f"{result_count:,}",
            delta="pre-filtered by server"
        )
        st.caption("No client-side filtering needed")
    
    with col3:
        if summary and 'total_amount' in summary:
            st.metric(
                label="💰 Total Value",
                value=f"${summary['total_amount']:,.2f}",
                delta=f"avg ${summary['avg_amount']:,.0f}"
            )
            st.caption(f"Range: ${summary['min_amount']:,.0f} - ${summary['max_amount']:,.0f}")
    
    with col4:
        if summary and 'regions_covered' in summary:
            st.metric(
                label="🌍 Access Scope",
                value=f"{summary['regions_covered']} regions",
                delta=f"{summary['categories']} categories"
            )
            st.caption(f"{summary['merchants']} unique merchants")
    
    # Add timing breakdown information
    st.info(f"""
    📊 **Ultra-Optimized Performance Analysis**: 
    • **API Call**: Measured from connection → search execution → response received
    • **Server-side Filtering**: User entitlements applied at Cortex Search service level  
    • **No Reranking**: Disabled reranker for maximum speed (`"reranker": "none"`)
    • **Essential Columns**: Only required fields to minimize data transfer
    • **Response Time**: Pure API response time (excludes UI rendering and visualizations)
    • **Efficiency**: {result_count} results returned in {response_time:.0f}ms = **{result_count/(response_time/1000):.1f} results/second**
    """)

def create_visualizations(df, summary):
    """Create interactive visualizations of transaction data"""
    if df.empty:
        return
    
    st.subheader("📈 Transaction Analytics")
    
    # Create tabs for different visualizations
    viz_tab1, viz_tab2, viz_tab3, viz_tab4 = st.tabs([
        "💰 Amount Distribution", 
        "🌍 Regional Analysis", 
        "🏪 Category Breakdown",
        "📅 Timeline View"
    ])
    
    with viz_tab1:
        # Get column names (handle different cases)
        amount_col = get_column_name(df, ['AMOUNT', 'amount'])
        txn_id_col = get_column_name(df, ['TXN_ID', 'txn_id'])
        desc_col = get_column_name(df, ['DESCRIPTION', 'description'])
        merchant_col = get_column_name(df, ['MERCHANT_NAME', 'merchant_name'])
        category_col = get_column_name(df, ['CATEGORY', 'category'])
        
        if amount_col:
            # Amount distribution histogram
            fig_hist = px.histogram(
                df, 
                x=amount_col, 
                title="Transaction Amount Distribution",
                labels={amount_col: 'Amount ($)', 'count': 'Number of Transactions'}
            )
            fig_hist.update_layout(showlegend=False)
            st.plotly_chart(fig_hist, use_container_width=True)
            
            # Top 10 highest value transactions
            st.write("**🔝 Top 10 Highest Value Transactions:**")
            display_cols = [col for col in [txn_id_col, desc_col, amount_col, merchant_col, category_col] if col]
            if display_cols:
                top_transactions = df.nlargest(10, amount_col)[display_cols]
                st.dataframe(top_transactions, use_container_width=True)
        else:
            st.warning("Amount column not found - cannot display amount distribution")
    
    with viz_tab2:
        # Get column names for regional analysis
        amount_col = get_column_name(df, ['AMOUNT', 'amount'])
        region_col = get_column_name(df, ['REGION_NAME', 'region_name'])
        
        if amount_col and region_col:
            # Regional analysis
            regional_summary = df.groupby(region_col).agg({
                amount_col: ['sum', 'count', 'mean']
            }).round(2)
            regional_summary.columns = ['Total Amount', 'Transaction Count', 'Avg Amount']
            regional_summary = regional_summary.reset_index()
            
            # Regional pie chart
            fig_pie = px.pie(
                regional_summary, 
                values='Total Amount', 
                names=region_col,
                title="Transaction Value by Region"
            )
            st.plotly_chart(fig_pie, use_container_width=True)
            
            st.write("**🌍 Regional Summary:**")
            st.dataframe(regional_summary, use_container_width=True)
        else:
            st.warning("Required columns (amount, region) not found for regional analysis")
    
    with viz_tab3:
        # Get column names for category analysis
        amount_col = get_column_name(df, ['AMOUNT', 'amount'])
        category_col = get_column_name(df, ['CATEGORY', 'category'])
        
        if amount_col and category_col:
            # Category analysis
            category_summary = df.groupby(category_col).agg({
                amount_col: ['sum', 'count', 'mean']
            }).round(2)
            category_summary.columns = ['Total Amount', 'Transaction Count', 'Avg Amount']
            category_summary = category_summary.reset_index().sort_values('Total Amount', ascending=False)
            
            # Category bar chart
            fig_bar = px.bar(
                category_summary.head(10), 
                x=category_col, 
                y='Total Amount',
                title="Top 10 Categories by Total Amount",
                labels={'Total Amount': 'Total Amount ($)'}
            )
            fig_bar.update_xaxes(tickangle=45)
            st.plotly_chart(fig_bar, use_container_width=True)
            
            st.write("**🏪 Category Breakdown:**")
            st.dataframe(category_summary, use_container_width=True)
        else:
            st.warning("Required columns (amount, category) not found for category analysis")
    
    with viz_tab4:
        # Get column names for timeline analysis
        amount_col = get_column_name(df, ['AMOUNT', 'amount'])
        date_col = get_column_name(df, ['TRANSACTION_DATE', 'transaction_date'])
        txn_id_col = get_column_name(df, ['TXN_ID', 'txn_id'])
        
        if amount_col and date_col:
            # Timeline analysis
            df_timeline = df.copy()
            df_timeline[date_col] = pd.to_datetime(df_timeline[date_col])
            
            daily_summary = df_timeline.groupby(df_timeline[date_col].dt.date).agg({
                amount_col: 'sum',
                txn_id_col: 'count' if txn_id_col else amount_col + '_count'
            }).reset_index()
            daily_summary.columns = ['Date', 'Total Amount', 'Transaction Count']
            
            # Timeline chart
            fig_timeline = px.line(
                daily_summary, 
                x='Date', 
                y='Total Amount',
                title="Daily Transaction Volume Over Time",
                labels={'Total Amount': 'Daily Total ($)'}
            )
            st.plotly_chart(fig_timeline, use_container_width=True)
            
            st.write("**📅 Daily Transaction Summary:**")
            st.dataframe(daily_summary, use_container_width=True)
        else:
            st.warning("Required columns (amount, date) not found for timeline analysis")

def main():
    """Main Streamlit application"""
    # Page configuration
    st.set_page_config(
        page_title="Cortex Search Entitlements Demo",
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
    st.markdown('<h1 class="main-header">🔍 Cortex Search Entitlements Demo</h1>', unsafe_allow_html=True)
    st.markdown("**Real-time transaction access with user-based entitlements and performance monitoring**")
    st.info("🚀 **Ultra-Optimized Cortex Search Python API** - Server-side filtering + No reranking + Essential columns + Precise response timing")
    
    # Initialize connection
    session = init_connection()
    
    # Sidebar for user selection and controls
    with st.sidebar:
        st.header("🎛️ Controls")
        
        # Get users list
        with st.spinner("Loading users..."):
            users_df = get_users_list(session)
        
        if users_df.empty:
            st.error("No users found. Please check your database connection.")
            return
        
        # User selection dropdown
        st.subheader("👤 Select User")
        selected_user_id = st.selectbox(
            "Choose a user to view their accessible transactions:",
            options=users_df['USER_ID'].tolist(),
            format_func=lambda x: f"{x} ({users_df[users_df['USER_ID']==x]['USER_NAME'].iloc[0]})"
        )
        
        # Get selected user info
        selected_user_info = users_df[users_df['USER_ID'] == selected_user_id].iloc[0]
        
        # Display user info
        st.info(f"""
        **User Details:**
        - **Name:** {selected_user_info['USER_NAME']}
        - **Region:** {selected_user_info['REGION_NAME']}
        - **Access Level:** {selected_user_info['ACCESS_LEVEL']}
        - **Status:** {selected_user_info['STATUS']}
        """)
        
        # Search controls
        st.subheader("🔍 Search Options")
        search_query = st.text_input(
            "Semantic search query (optional):",
            placeholder="e.g., restaurant purchases, high value transactions",
            help="Leave empty to show all accessible transactions"
        )
        
        result_limit = st.slider("Max results to return:", 10, 200, 50, 10)
        
        # Auto-refresh option
        auto_refresh = st.checkbox("Auto-refresh every 30 seconds", value=False)
        
        if auto_refresh:
            time.sleep(30)
            st.rerun()
    
    # Main content area
    st.markdown('<div class="search-container">', unsafe_allow_html=True)
    
    # Search button
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        if st.button("🚀 Search Transactions", type="primary", use_container_width=True):
            # Clear any previous results
            if 'search_results' in st.session_state:
                del st.session_state['search_results']
    
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Perform optimized search with detailed timing
    with st.spinner("⚡ Executing optimized Cortex Search Python API..."):
        df, response_time, result_count = search_transactions_cortex_optimized(
            session, selected_user_id, search_query, result_limit
        )
        
        # Generate summary
        summary = get_transaction_summary(df, selected_user_info.to_dict())
    
    # Display performance metrics
    st.subheader("⚡ Performance Metrics")
    display_performance_metrics(response_time, result_count, summary)
    
    # Display results
    if not df.empty:
        # Transaction summary section
        st.subheader("📊 Transaction Summary")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown(f"""
            **💰 Financial Summary:**
            - **Total Transactions:** {summary['total_transactions']:,}
            - **Total Value:** ${summary['total_amount']:,.2f}
            - **Average Amount:** ${summary['avg_amount']:,.2f}
            - **Highest Transaction:** ${summary['max_amount']:,.2f}
            - **Lowest Transaction:** ${summary['min_amount']:,.2f}
            """)
        
        with col2:
            st.markdown(f"""
            **🌍 Coverage Summary:**
            - **Regions Accessible:** {summary['regions_covered']}
            - **Categories Available:** {summary['categories']}
            - **Unique Merchants:** {summary['merchants']}
            - **Transaction Types:** {summary['transaction_types']}
            - **Date Range:** {summary['date_range']['earliest']} to {summary['date_range']['latest']}
            """)
        
        # Visualizations
        create_visualizations(df, summary)
        
        # Detailed transaction table
        st.subheader("📋 Detailed Transaction Data")
        
        # Add search and filter capabilities
        with st.expander("🔧 Table Filters", expanded=False):
            # Get column names for filtering
            region_col = get_column_name(df, ['REGION_NAME', 'region_name'])
            category_col = get_column_name(df, ['CATEGORY', 'category'])
            amount_col = get_column_name(df, ['AMOUNT', 'amount'])
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                if region_col:
                    region_filter = st.multiselect(
                        "Filter by Region:",
                        options=df[region_col].unique(),
                        default=df[region_col].unique()
                    )
                else:
                    region_filter = []
                    st.info("Region column not found")
            
            with col2:
                if category_col:
                    category_filter = st.multiselect(
                        "Filter by Category:",
                        options=df[category_col].unique(),
                        default=df[category_col].unique()
                    )
                else:
                    category_filter = []
                    st.info("Category column not found")
            
            with col3:
                if amount_col:
                    # Safely handle numeric conversion for amount filtering
                    try:
                        amount_min = float(df[amount_col].min()) if not df.empty else 0.0
                        amount_max = float(df[amount_col].max()) if not df.empty else 1000.0
                        min_amount = st.number_input(
                            "Minimum Amount:",
                            min_value=amount_min,
                            max_value=amount_max,
                            value=amount_min
                        )
                    except (ValueError, TypeError):
                        # Fallback if amount conversion fails
                        min_amount = st.number_input(
                            "Minimum Amount:",
                            min_value=0.0,
                            max_value=10000.0,
                            value=0.0
                        )
                else:
                    min_amount = 0.0
                    st.info("Amount column not found")
        
        # Apply filters
        filtered_df = df.copy()
        if region_col and region_filter:
            filtered_df = filtered_df[filtered_df[region_col].isin(region_filter)]
        if category_col and category_filter:
            filtered_df = filtered_df[filtered_df[category_col].isin(category_filter)]
        if amount_col and min_amount > 0:
            filtered_df = filtered_df[filtered_df[amount_col] >= min_amount]
        
        # Display filtered results
        st.write(f"**Showing {len(filtered_df):,} of {len(df):,} transactions**")
        
        # Format the dataframe for better display
        # Get all possible columns for display
        display_columns = []
        column_mapping = {}
        
        # Define column mappings (handle both cases)
        possible_columns = {
            'Transaction ID': ['TXN_ID', 'txn_id'],
            'Description': ['DESCRIPTION', 'description'],
            'Amount': ['AMOUNT', 'amount'],
            'Type': ['TRANSACTION_TYPE', 'transaction_type'],
            'Category': ['CATEGORY', 'category'],
            'Merchant': ['MERCHANT_NAME', 'merchant_name'],
            'Region': ['REGION_NAME', 'region_name'],
            'Date': ['TRANSACTION_DATE', 'transaction_date']
        }
        
        for display_name, col_options in possible_columns.items():
            found_col = get_column_name(filtered_df, col_options)
            if found_col:
                display_columns.append(found_col)
                column_mapping[found_col] = display_name
        
        # Create display dataframe with available columns
        if display_columns:
            display_df = filtered_df[display_columns].copy()
            
            # Format amount column if it exists
            amount_col = get_column_name(display_df, ['AMOUNT', 'amount'])
            if amount_col:
                display_df[amount_col] = display_df[amount_col].apply(lambda x: f"${x:,.2f}")
            
            # Rename columns for display
            display_df = display_df.rename(columns=column_mapping)
        else:
            display_df = filtered_df
        
        st.dataframe(
            display_df,
            use_container_width=True,
            height=400
        )
        
        # Export options
        col1, col2, col3 = st.columns(3)
        with col1:
            csv_data = df.to_csv(index=False)
            st.download_button(
                "📥 Download as CSV",
                csv_data,
                file_name=f"transactions_{selected_user_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )
        
        with col2:
            json_data = df.to_json(orient='records', date_format='iso')
            st.download_button(
                "📥 Download as JSON",
                json_data,
                file_name=f"transactions_{selected_user_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                mime="application/json"
            )
        
    else:
        # No results found
        st.warning("🚫 No transactions found for this user with the current search criteria.")
        st.info("""
        **Possible reasons:**
        - User may not have access to any transactions in their region
        - Search query may be too specific
        - User may be inactive or have restricted access
        
        **Try:**
        - Removing the search query to see all accessible transactions
        - Selecting a different user
        - Checking the user's region and access level
        """)
    
    # Footer
    st.markdown("---")
    st.markdown("""
    <div style='text-align: center; color: #666666; padding: 1rem;'>
        <p>🔍 <strong>Cortex Search Entitlements Demo</strong> | 
        Powered by Snowflake Cortex Search & Dynamic Tables | 
        Built with ❤️ using Streamlit</p>
        <p><em>Real-time transaction access with user-based entitlements and performance monitoring</em></p>
    </div>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    main()
