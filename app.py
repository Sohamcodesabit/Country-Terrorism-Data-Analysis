import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
import json

# Page config
st.set_page_config(
    page_title="Global Terrorism Safety Index",
    page_icon="🌍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
    <style>
    .main {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
    }
    .stMetric {
        background: rgba(255, 255, 255, 0.1);
        padding: 20px;
        border-radius: 10px;
        backdrop-filter: blur(10px);
    }
    h1, h2, h3 {
        color: white !important;
    }
    .stMarkdown {
        color: white !important;
    }
    </style>
    """, unsafe_allow_html=True)

@st.cache_data
def load_safety_data():
    """Load the safety index data from PySpark output"""
    try:
        safety_index_path = "./output_safety_index"
        
        # Find CSV files
        csv_files = list(Path(safety_index_path).rglob("*.csv"))
        data_files = [f for f in csv_files if not f.name.startswith('_') and f.stat().st_size > 0]
        
        if not data_files:
            return None, "No data files found. Please run compute_safety_index.py first."
        
        # Read the first valid CSV
        df = pd.read_csv(data_files[0])
        
        # Clean column names
        df.columns = df.columns.str.strip()
        
        return df, None
    
    except Exception as e:
        return None, f"Error loading data: {str(e)}"

@st.cache_data
def load_country_codes():
    """Load ISO country codes for mapping"""
    # Common country name mappings
    country_mapping = {
        'United States': 'USA',
        'United Kingdom': 'GBR',
        'Russia': 'RUS',
        'South Korea': 'KOR',
        'North Korea': 'PRK',
        'Vietnam': 'VNM',
        'Iran': 'IRN',
        'Syria': 'SYR',
        'Turkey': 'TUR',
        'Venezuela': 'VEN',
        'Bolivia': 'BOL',
        'Tanzania': 'TZA',
        'Democratic Republic of the Congo': 'COD',
        'Republic of the Congo': 'COG',
        'Czech Republic': 'CZE',
        'Ivory Coast': 'CIV',
        'Laos': 'LAO',
        'Moldova': 'MDA',
        'Macedonia': 'MKD',
        'Serbia': 'SRB',
        'Bosnia-Herzegovina': 'BIH',
        'West Bank and Gaza Strip': 'PSE',
        'East Timor': 'TLS'
    }
    return country_mapping

def create_world_map(df):
    """Create interactive choropleth map"""
    
    # Get country codes
    country_mapping = load_country_codes()
    
    # Try to get ISO codes
    df['iso_alpha'] = df['country'].map(country_mapping)
    
    # For countries without mapping, use the country name
    # Plotly will try to match automatically
    df['iso_alpha'] = df['iso_alpha'].fillna(df['country'])
    
    # Create the map
    fig = px.choropleth(
        df,
        locations='iso_alpha',
        locationmode='ISO-3',
        color='safetyIndex',
        hover_name='country',
        hover_data={
            'iso_alpha': False,
            'safetyIndex': ':.2f',
            'riskLevel': True,
            'totalIncidents': ':,',
            'totalKilled': ':,',
            'totalWounded': ':,',
            'casualties': ':,'
        },
        color_continuous_scale=[
            [0.0, '#FF0000'],    # Critical - Red
            [0.25, '#FFA500'],   # High - Orange
            [0.50, '#FFFF00'],   # Moderate - Yellow
            [0.75, '#90EE90'],   # Low - Light Green
            [1.0, '#008000']     # Very Low - Green
        ],
        range_color=[0, 100],
        labels={
            'safetyIndex': 'Safety Index',
            'riskLevel': 'Risk Level',
            'totalIncidents': 'Total Incidents',
            'totalKilled': 'Deaths',
            'totalWounded': 'Injuries',
            'casualties': 'Casualties'
        },
        title='Global Terrorism Safety Index by Country'
    )
    
    fig.update_geos(
        showcoastlines=True,
        coastlinecolor="White",
        showland=True,
        landcolor="rgba(51, 51, 51, 0.8)",
        showcountries=True,
        countrycolor="White",
        projection_type="natural earth"
    )
    
    fig.update_layout(
        height=600,
        margin=dict(l=0, r=0, t=50, b=0),
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font=dict(color='white'),
        title_font_size=24,
        title_font_color='white',
        geo=dict(bgcolor='rgba(0,0,0,0)')
    )
    
    return fig

def create_top_countries_chart(df, n=15):
    """Create bar chart of top safest countries"""
    top_df = df.nlargest(n, 'safetyIndex')
    
    fig = px.bar(
        top_df,
        x='safetyIndex',
        y='country',
        orientation='h',
        color='riskLevel',
        color_discrete_map={
            'Low': '#008000',
            'Moderate': '#FFFF00',
            'High': '#FFA500',
            'Critical': '#FF0000'
        },
        title=f'Top {n} Safest Countries',
        labels={'safetyIndex': 'Safety Index', 'country': 'Country'}
    )
    
    fig.update_layout(
        height=500,
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0.2)',
        font=dict(color='white'),
        title_font_size=20,
        showlegend=True,
        yaxis={'categoryorder': 'total ascending'}
    )
    
    return fig

def create_risk_distribution(df):
    """Create pie chart of risk level distribution"""
    risk_counts = df['riskLevel'].value_counts()
    
    fig = px.pie(
        values=risk_counts.values,
        names=risk_counts.index,
        title='Risk Level Distribution',
        color=risk_counts.index,
        color_discrete_map={
            'Low': '#008000',
            'Moderate': '#FFFF00',
            'High': '#FFA500',
            'Critical': '#FF0000'
        }
    )
    
    fig.update_layout(
        height=400,
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font=dict(color='white'),
        title_font_size=20
    )
    
    return fig

def create_casualties_chart(df, n=15):
    """Create chart showing casualties by country"""
    top_casualties = df.nlargest(n, 'casualties')
    
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        name='Deaths',
        x=top_casualties['country'],
        y=top_casualties['totalKilled'],
        marker_color='#FF4444'
    ))
    
    fig.add_trace(go.Bar(
        name='Injuries',
        x=top_casualties['country'],
        y=top_casualties['totalWounded'],
        marker_color='#FFA500'
    ))
    
    fig.update_layout(
        title=f'Top {n} Countries by Casualties',
        barmode='stack',
        height=400,
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0.2)',
        font=dict(color='white'),
        title_font_size=20,
        xaxis_tickangle=-45
    )
    
    return fig

# Main app
def main():
    # Header
    st.markdown("<h1 style='text-align: center; color: white;'>🌍 Global Terrorism Safety Index Dashboard</h1>", unsafe_allow_html=True)
    st.markdown("<p style='text-align: center; color: white; font-size: 1.2em;'>Interactive Analysis of Global Terrorism Data</p>", unsafe_allow_html=True)
    st.markdown("---")
    
    # Load data
    df, error = load_safety_data()
    
    if error:
        st.error(f"❌ {error}")
        st.info("💡 Please run `compute_safety_index.py` first to generate the safety index data.")
        st.code("python compute_safety_index.py", language="bash")
        return
    
    if df is None or df.empty:
        st.error("No data available.")
        return
    
    # Sidebar
    st.sidebar.title("📊 Filters & Options")
    
    # Risk level filter
    risk_levels = ['All'] + sorted(df['riskLevel'].unique().tolist())
    selected_risk = st.sidebar.selectbox("Filter by Risk Level", risk_levels)
    
    if selected_risk != 'All':
        filtered_df = df[df['riskLevel'] == selected_risk]
    else:
        filtered_df = df
    
    # Top N selector
    top_n = st.sidebar.slider("Number of countries to show in charts", 5, 30, 15)
    
    # Global Statistics
    st.markdown("## 📈 Global Statistics")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="🏳️ Total Countries",
            value=f"{len(filtered_df):,}"
        )
    
    with col2:
        st.metric(
            label="⚠️ Total Incidents",
            value=f"{filtered_df['totalIncidents'].sum():,}"
        )
    
    with col3:
        st.metric(
            label="💔 Total Casualties",
            value=f"{filtered_df['casualties'].sum():,}"
        )
    
    with col4:
        safest = filtered_df.loc[filtered_df['safetyIndex'].idxmax()]
        st.metric(
            label="🛡️ Safest Country",
            value=safest['country'],
            delta=f"Index: {safest['safetyIndex']:.1f}"
        )
    
    st.markdown("---")
    
    # Interactive World Map
    st.markdown("## 🗺️ Interactive World Map")
    st.plotly_chart(create_world_map(filtered_df), use_container_width=True)
    
    st.markdown("---")
    
    # Two column layout for charts
    col1, col2 = st.columns(2)
    
    with col1:
        st.plotly_chart(create_top_countries_chart(filtered_df, top_n), use_container_width=True)
    
    with col2:
        st.plotly_chart(create_risk_distribution(filtered_df), use_container_width=True)
    
    # Casualties chart
    st.plotly_chart(create_casualties_chart(filtered_df, top_n), use_container_width=True)
    
    st.markdown("---")
    
    # Country Search and Details
    st.markdown("## 🔍 Country Details")
    
    selected_country = st.selectbox(
        "Select a country to view detailed statistics:",
        options=sorted(filtered_df['country'].tolist())
    )
    
    if selected_country:
        country_data = filtered_df[filtered_df['country'] == selected_country].iloc[0]
        
        # Create detailed view
        col1, col2, col3 = st.columns([1, 2, 1])
        
        with col2:
            # Safety score with color
            color = country_data['colorCode']
            st.markdown(f"""
                <div style='text-align: center; padding: 30px; background: {color}; border-radius: 20px; margin: 20px 0;'>
                    <h1 style='font-size: 4em; margin: 0; color: white;'>{country_data['safetyIndex']:.1f}</h1>
                    <h3 style='margin: 10px 0; color: white;'>Safety Index</h3>
                    <h2 style='margin: 0; color: white;'>{country_data['riskLevel']} Risk</h2>
                </div>
            """, unsafe_allow_html=True)
        
        # Detailed stats
        st.markdown("### 📊 Detailed Statistics")
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total Incidents", f"{int(country_data['totalIncidents']):,}")
            st.metric("Successful Attacks", f"{int(country_data['successfulAttacks']):,}")
        
        with col2:
            st.metric("Deaths", f"{int(country_data['totalKilled']):,}")
            st.metric("Success Rate", f"{country_data['attackSuccessRate']:.1f}%")
        
        with col3:
            st.metric("Injuries", f"{int(country_data['totalWounded']):,}")
            st.metric("Suicide Attacks", f"{int(country_data['suicideAttacks']):,}")
        
        with col4:
            st.metric("Total Casualties", f"{int(country_data['casualties']):,}")
            st.metric("Property Damage", f"{int(country_data['propertyDamage']):,}")
    
    # Data table
    st.markdown("---")
    st.markdown("## 📋 Complete Data Table")
    
    # Display dataframe
    display_cols = ['country', 'safetyIndex', 'riskLevel', 'totalIncidents', 
                    'totalKilled', 'totalWounded', 'casualties', 'attackSuccessRate']
    st.dataframe(
        filtered_df[display_cols].sort_values('safetyIndex', ascending=False),
        use_container_width=True,
        height=400
    )
    
    # Download button
    csv = filtered_df.to_csv(index=False)
    st.download_button(
        label="📥 Download Data as CSV",
        data=csv,
        file_name="terrorism_safety_index.csv",
        mime="text/csv"
    )
    
    # Footer
    st.markdown("---")
    st.markdown("<p style='text-align: center; color: white;'>Data processed using PySpark | Visualization powered by Streamlit & Plotly</p>", unsafe_allow_html=True)

if __name__ == "__main__":
    main()