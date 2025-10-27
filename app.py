import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine
from urllib.parse import quote_plus

# Page configuration
st.set_page_config(
    page_title="PhonePe Pulse Dashboard",
    page_icon="📱",
    layout="wide"
)

# Database connection
@st.cache_resource
def get_database_connection():
    mysql_config = {
        'host': 'localhost',
        'user': 'root',
        'password': 'sql@123',  # Change to your MySQL password
        'database': 'phonepe_pulse'
    }
    
    engine = create_engine(
        f"mysql+pymysql://{mysql_config['user']}:{quote_plus(mysql_config['password'])}@"
        f"{mysql_config['host']}/{mysql_config['database']}"
    )
    return engine

# Load data
@st.cache_data
def load_transaction_data():
    engine = get_database_connection()
    query = "SELECT * FROM aggregated_transaction"
    df = pd.read_sql(query, engine)
    return df

@st.cache_data
def load_user_data():
    engine = get_database_connection()
    query = "SELECT * FROM aggregated_user"
    df = pd.read_sql(query, engine)
    return df

# Main app
def main():
    st.title("📱 PhonePe Pulse Data Visualization Dashboard")
    st.markdown("---")
    
    # Sidebar
    st.sidebar.title("Navigation")
    page = st.sidebar.radio("Select Page", ["Overview", "Transactions", "Users"])
    
    if page == "Overview":
        show_overview()
    elif page == "Transactions":
        show_transactions()
    elif page == "Users":
        show_users()

def show_overview():
    st.header("📊 Overview")
    
    col1, col2 = st.columns(2)
    
    with col1:
        try:
            df_trans = load_transaction_data()
            total_transactions = df_trans['Transaction_count'].sum()
            total_amount = df_trans['Transaction_amount'].sum()
            
            st.metric("Total Transactions", f"{total_transactions:,}")
            st.metric("Total Transaction Amount", f"₹{total_amount/1e9:.2f}B")
        except Exception as e:
            st.error(f"Error loading transaction data: {e}")
    
    with col2:
        try:
            df_users = load_user_data()
            total_users = df_users['Transaction_count'].sum()
            
            st.metric("Total User Records", f"{len(df_users):,}")
            st.metric("Total Device Count", f"{total_users:,}")
        except Exception as e:
            st.error(f"Error loading user data: {e}")

def show_transactions():
    st.header("💳 Transaction Analysis")
    
    try:
        df = load_transaction_data()
        
        # Filters
        col1, col2 = st.columns(2)
        with col1:
            selected_year = st.selectbox("Select Year", sorted(df['Year'].unique()))
        with col2:
            selected_quarter = st.selectbox("Select Quarter", sorted(df['Quarter'].unique()))
        
        # Filter data
        filtered_df = df[(df['Year'] == selected_year) & (df['Quarter'] == selected_quarter)]
        
        # Transaction by Type
        st.subheader("Transactions by Type")
        trans_by_type = filtered_df.groupby('Transaction_type').agg({
            'Transaction_count': 'sum',
            'Transaction_amount': 'sum'
        }).reset_index()
        
        fig = px.bar(trans_by_type, 
                     x='Transaction_type', 
                     y='Transaction_amount',
                     title=f"Transaction Amount by Type (Q{selected_quarter} {selected_year})")
        st.plotly_chart(fig, use_container_width=True)
        
        # Top States
        st.subheader("Top 10 States by Transaction Amount")
        top_states = filtered_df.groupby('State')['Transaction_amount'].sum().sort_values(ascending=False).head(10)
        
        fig2 = px.bar(top_states, 
                      orientation='h',
                      title=f"Top 10 States (Q{selected_quarter} {selected_year})")
        st.plotly_chart(fig2, use_container_width=True)
        
        # Data table
        st.subheader("Transaction Data")
        st.dataframe(filtered_df, use_container_width=True)
        
    except Exception as e:
        st.error(f"Error: {e}")

def show_users():
    st.header("👥 User Analysis")
    
    try:
        df = load_user_data()
        
        # Filters
        col1, col2 = st.columns(2)
        with col1:
            selected_year = st.selectbox("Select Year", sorted(df['Year'].unique()))
        with col2:
            selected_quarter = st.selectbox("Select Quarter", sorted(df['Quarter'].unique()))
        
        # Filter data
        filtered_df = df[(df['Year'] == selected_year) & (df['Quarter'] == selected_quarter)]
        
        # Top Brands
        st.subheader("Top Mobile Brands")
        brand_data = filtered_df.groupby('Brands')['Transaction_count'].sum().sort_values(ascending=False).head(10)
        
        fig = px.pie(values=brand_data.values, 
                     names=brand_data.index,
                     title=f"Top 10 Mobile Brands (Q{selected_quarter} {selected_year})")
        st.plotly_chart(fig, use_container_width=True)
        
        # State-wise Users
        st.subheader("State-wise User Distribution")
        state_users = filtered_df.groupby('State')['Transaction_count'].sum().sort_values(ascending=False).head(10)
        
        fig2 = px.bar(state_users,
                      orientation='h',
                      title=f"Top 10 States by Users (Q{selected_quarter} {selected_year})")
        st.plotly_chart(fig2, use_container_width=True)
        
        # Data table
        st.subheader("User Data")
        st.dataframe(filtered_df, use_container_width=True)
        
    except Exception as e:
        st.error(f"Error: {e}")

if __name__ == "__main__":
    main()
    