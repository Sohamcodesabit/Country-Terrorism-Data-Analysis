from flask import Flask, jsonify, render_template
from flask_cors import CORS
import pandas as pd
import os
from pathlib import Path

app = Flask(__name__)
CORS(app)

# Path to your safety index output
SAFETY_INDEX_PATH = "./output_safety_index"

def load_safety_data():
    """Load the safety index data from PySpark output"""
    try:
        # Find the CSV file in the output directory
        csv_files = list(Path(SAFETY_INDEX_PATH).glob("*.csv"))
        
        if not csv_files:
            # Try to find in subdirectories
            csv_files = list(Path(SAFETY_INDEX_PATH).rglob("*.csv"))
        
        if not csv_files:
            return None
        
        # Filter out _SUCCESS files and find the actual data file
        data_files = [f for f in csv_files if not f.name.startswith('_') and f.stat().st_size > 0]
        
        if not data_files:
            return None
        
        # Read the CSV
        df = pd.read_csv(data_files[0])
        return df
    
    except Exception as e:
        print(f"Error loading safety data: {e}")
        return None

@app.route('/')
def index():
    """Serve the main HTML page"""
    return render_template('index.html')

@app.route('/api/safety-data')
def get_safety_data():
    """API endpoint to get all safety data"""
    df = load_safety_data()
    
    if df is None:
        return jsonify({"error": "Could not load safety data. Make sure compute_safety_index.py has been run."}), 404
    
    # Convert to dictionary format
    data = df.to_dict('records')
    return jsonify(data)

@app.route('/api/country/<country_name>')
def get_country_data(country_name):
    """API endpoint to get data for a specific country"""
    df = load_safety_data()
    
    if df is None:
        return jsonify({"error": "Could not load safety data"}), 404
    
    # Find the country (case-insensitive)
    country_data = df[df['country'].str.lower() == country_name.lower()]
    
    if country_data.empty:
        return jsonify({"error": f"Country '{country_name}' not found"}), 404
    
    return jsonify(country_data.to_dict('records')[0])

@app.route('/api/top-countries/<int:limit>')
def get_top_countries(limit):
    """Get top N safest countries"""
    df = load_safety_data()
    
    if df is None:
        return jsonify({"error": "Could not load safety data"}), 404
    
    top_countries = df.nlargest(limit, 'safetyIndex')
    return jsonify(top_countries.to_dict('records'))

@app.route('/api/stats')
def get_global_stats():
    """Get global statistics"""
    df = load_safety_data()
    
    if df is None:
        return jsonify({"error": "Could not load safety data"}), 404
    
    stats = {
        "totalCountries": len(df),
        "totalIncidents": int(df['totalIncidents'].sum()),
        "totalCasualties": int(df['casualties'].sum()),
        "safestCountry": df.loc[df['safetyIndex'].idxmax()]['country'],
        "highestRiskCountry": df.loc[df['safetyIndex'].idxmin()]['country'],
        "averageSafetyIndex": round(df['safetyIndex'].mean(), 2)
    }
    
    return jsonify(stats)

if __name__ == '__main__':
    print("🚀 Starting Terrorism Safety Index Dashboard...")
    print("📊 Make sure you've run compute_safety_index.py first!")
    app.run(debug=True, port=5000)