import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import plotly.express as px
import plotly.graph_objects as go
import os

# Page configuration
st.set_page_config(
    page_title="Server Metrics Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Use GCP service account
@st.cache_resource
def get_bigquery_client():
    # When deployed to App Engine, the credentials will automatically be handled
    # Locally, use service account key file
    if os.path.exists('metrics-pipeline-key.json'):
        credentials = service_account.Credentials.from_service_account_file(
            'metrics-pipeline-key.json',
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        return bigquery.Client(credentials=credentials, project=credentials.project_id)
    else:
        return bigquery.Client()

client = get_bigquery_client()

# Application title
st.title("Server Metrics Dashboard")
st.markdown("Real-time monitoring of server performance metrics")

# Sidebar for filters
st.sidebar.header("Filters")

# Query to get all server types
server_type_query = """
SELECT DISTINCT server_type 
FROM `server-metrics-project.gold_layer.server_health_summary`
ORDER BY server_type
"""
server_types_df = client.query(server_type_query).to_dataframe()
server_types = server_types_df['server_type'].tolist()
selected_server_types = st.sidebar.multiselect("Server Type", server_types, default=server_types)

# Query to get all locations
location_query = """
SELECT DISTINCT location 
FROM `server-metrics-project.gold_layer.server_health_summary`
ORDER BY location
"""
locations_df = client.query(location_query).to_dataframe()
locations = locations_df['location'].tolist()
selected_locations = st.sidebar.multiselect("Location", locations, default=locations)

# Date selection
date_query = """
SELECT DISTINCT metric_date 
FROM `server-metrics-project.gold_layer.server_health_summary`
ORDER BY metric_date DESC
LIMIT 10
"""
dates_df = client.query(date_query).to_dataframe()
dates = dates_df['metric_date'].tolist()
if dates:
    selected_date = st.sidebar.selectbox("Date", dates, index=0)
else:
    selected_date = None
    st.warning("No data found. Make sure your pipeline has processed some data.")

# Apply filters to construct WHERE clause
where_clause = []
if selected_server_types:
    quoted_types = ['"' + str(type) + '"' for type in selected_server_types]
    where_clause.append(f"server_type IN ({', '.join(quoted_types)})")
if selected_locations:
    quoted_locs = ['"' + str(loc) + '"' for loc in selected_locations]
    where_clause.append(f"location IN ({', '.join(quoted_locs)})")
if selected_date:
    where_clause.append(f'metric_date = "{selected_date}"')


where_sql = " AND ".join(where_clause)
if where_sql:
    where_sql = "WHERE " + where_sql

# Main dashboard components
if selected_date:
    # Server Health Overview
    st.header("Server Health Overview")
    health_query = f"""
    SELECT server_name, server_type, location, server_health_score, 
           avg_daily_cpu, avg_daily_memory, high_utilization_percentage
    FROM `server-metrics-project.gold_layer.server_health_summary`
    {where_sql}
    ORDER BY server_health_score ASC
    LIMIT 20
    """
    health_df = client.query(health_query).to_dataframe()

    if not health_df.empty:
        # Create health score indicators
        col1, col2, col3 = st.columns(3)
        
        with col1:
            avg_health = health_df['server_health_score'].mean()
            st.metric("Average Health Score", f"{avg_health:.2f}")
            fig = go.Figure(go.Indicator(
                mode = "gauge+number",
                value = avg_health,
                domain = {'x': [0, 1], 'y': [0, 1]},
                title = {'text': "Avg Health"},
                gauge = {
                    'axis': {'range': [0, 100]},
                    'bar': {'color': "darkblue"},
                    'steps': [
                        {'range': [0, 50], 'color': "red"},
                        {'range': [50, 80], 'color': "orange"},
                        {'range': [80, 100], 'color': "green"}
                    ],
                }
            ))
            st.plotly_chart(fig)
        
        with col2:
            avg_cpu = health_df['avg_daily_cpu'].mean()
            st.metric("Average CPU Usage", f"{avg_cpu:.2f}%")
            fig = go.Figure(go.Indicator(
                mode = "gauge+number",
                value = avg_cpu,
                domain = {'x': [0, 1], 'y': [0, 1]},
                title = {'text': "Avg CPU Usage"},
                gauge = {
                    'axis': {'range': [0, 100]},
                    'bar': {'color': "darkblue"},
                    'steps': [
                        {'range': [0, 50], 'color': "green"},
                        {'range': [50, 80], 'color': "orange"},
                        {'range': [80, 100], 'color': "red"}
                    ],
                }
            ))
            st.plotly_chart(fig)
        
        with col3:
            avg_memory = health_df['avg_daily_memory'].mean()
            st.metric("Average Memory Usage", f"{avg_memory:.2f}%")
            fig = go.Figure(go.Indicator(
                mode = "gauge+number",
                value = avg_memory,
                domain = {'x': [0, 1], 'y': [0, 1]},
                title = {'text': "Avg Memory Usage"},
                gauge = {
                    'axis': {'range': [0, 100]},
                    'bar': {'color': "darkblue"},
                    'steps': [
                        {'range': [0, 50], 'color': "green"},
                        {'range': [50, 80], 'color': "orange"},
                        {'range': [80, 100], 'color': "red"}
                    ],
                }
            ))
            st.plotly_chart(fig)

        # Server health table
        st.subheader("Server Health Details")
        st.dataframe(health_df)
        
        # Bar chart of server health scores
        st.subheader("Server Health Scores")
        fig = px.bar(
            health_df.sort_values('server_health_score'), 
            x='server_name', 
            y='server_health_score',
            color='server_type',
            hover_data=['location', 'avg_daily_cpu', 'avg_daily_memory'],
            labels={'server_health_score': 'Health Score', 'server_name': 'Server Name'},
            height=500
        )
        st.plotly_chart(fig, use_container_width=True)

    # Hourly Performance Metrics
    st.header("Hourly Performance Analysis")
    hourly_query = f"""
    SELECT server_type, hour_of_day, 
           AVG(avg_cpu_usage) as avg_cpu, 
           AVG(avg_memory_usage) as avg_memory,
           AVG(avg_disk_io) as avg_disk_io,
           SUM(total_network_in) as total_net_in,
           SUM(total_network_out) as total_net_out
    FROM `server-metrics-project.gold_layer.hourly_server_metrics`
    WHERE metric_date = "{selected_date}"
    {' AND ' + ' AND '.join([clause for clause in where_clause if 'metric_date' not in clause]) if where_clause else ''}
    GROUP BY server_type, hour_of_day
    ORDER BY server_type, hour_of_day
    """
    hourly_df = client.query(hourly_query).to_dataframe()

    if not hourly_df.empty:
        # Line chart for CPU usage by hour
        st.subheader("CPU Usage by Hour")
        fig = px.line(
            hourly_df, 
            x='hour_of_day', 
            y='avg_cpu', 
            color='server_type',
            labels={'hour_of_day': 'Hour of Day', 'avg_cpu': 'Average CPU Usage (%)'},
            title="CPU Usage Throughout the Day"
        )
        st.plotly_chart(fig, use_container_width=True)

        # Line chart for Memory usage by hour
        st.subheader("Memory Usage by Hour")
        fig = px.line(
            hourly_df, 
            x='hour_of_day', 
            y='avg_memory', 
            color='server_type',
            labels={'hour_of_day': 'Hour of Day', 'avg_memory': 'Average Memory Usage (%)'},
            title="Memory Usage Throughout the Day"
        )
        st.plotly_chart(fig, use_container_width=True)

        # Network traffic
        st.subheader("Network Traffic by Hour")
        net_df = hourly_df.melt(
            id_vars=['server_type', 'hour_of_day'],
            value_vars=['total_net_in', 'total_net_out'],
            var_name='traffic_type', 
            value_name='traffic_volume'
        )
        fig = px.bar(
            net_df, 
            x='hour_of_day', 
            y='traffic_volume', 
            color='traffic_type',
            facet_col='server_type',
            labels={'hour_of_day': 'Hour of Day', 'traffic_volume': 'Network Traffic'},
            title="Network Traffic Throughout the Day",
            barmode='group'
        )
        st.plotly_chart(fig, use_container_width=True)

    # Location Performance
    st.header("Location Performance")
    location_query = f"""
    SELECT location, server_type, 
           AVG(avg_cpu_usage) as avg_cpu, 
           AVG(avg_memory_usage) as avg_memory,
           SUM(high_utilization_count) as high_util_count,
           AVG(high_utilization_percentage) as high_util_pct
    FROM `server-metrics-project.gold_layer.location_performance`
    {where_sql}
    GROUP BY location, server_type
    ORDER BY location, server_type
    """
    location_df = client.query(location_query).to_dataframe()

    if not location_df.empty:
        # Geo chart
        st.subheader("Performance by Location")
        fig = px.scatter_geo(
            location_df, 
            locations='location',  # This is simplified - in a real app you'd map to country codes
            size='avg_cpu',
            color='high_util_pct',
            hover_name='location',
            hover_data=['server_type', 'avg_memory', 'high_util_count'],
            title="Server Performance Across Locations",
            color_continuous_scale='Viridis'
        )
        st.plotly_chart(fig, use_container_width=True)

        # Heatmap for location performance
        pivot_df = location_df.pivot(index='location', columns='server_type', values='avg_cpu')
        st.subheader("CPU Usage Heatmap by Location and Server Type")
        fig = px.imshow(
            pivot_df,
            labels=dict(x="Server Type", y="Location", color="CPU Usage (%)"),
            x=pivot_df.columns,
            y=pivot_df.index,
            color_continuous_scale='Viridis'
        )
        st.plotly_chart(fig, use_container_width=True)
else:
    st.info("Please select a date from the sidebar to view metrics.")
