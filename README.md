# 🌍 Country Terrorism Data Analysis

A comprehensive data analysis project that processes the Global Terrorism Database using PySpark and visualizes terrorism safety metrics through an interactive Streamlit dashboard.

## 📊 Project Overview

This project analyzes global terrorism data to compute a **Safety Index** for countries worldwide. It processes historical terrorism incident data, calculates risk metrics, and provides interactive visualizations to help understand global terrorism patterns and safety levels.

## ✨ Features

- **Data Processing with PySpark**: Efficient handling of large-scale terrorism datasets
- **Safety Index Calculation**: Custom metric combining incidents, casualties, and attack success rates
- **Interactive Dashboard**: Real-time filtering and visualization using Streamlit
- **Risk Classification**: Countries categorized into Critical, High, Moderate, and Low risk levels
- **Global Choropleth Map**: Visual representation of safety indices across countries
- **Detailed Country Analytics**: In-depth statistics for individual countries
- **Export Functionality**: Download processed data as CSV

## 🛠️ Technologies Used

- **Python 3.x**
- **PySpark**: Big data processing and analysis
- **Streamlit**: Interactive web dashboard
- **Plotly**: Advanced data visualizations
- **Pandas**: Data manipulation

## 📁 Project Structure

```
Country-Terrorism-Data-Analysis/
├── app.py                          # Streamlit dashboard application
├── clean_terrorism_data.py         # Data cleaning and preprocessing
├── compute_safety_index.py         # Safety index calculation engine
├── datasets/
│   └── globalterrorismdb.csv      # Raw terrorism database
├── cleaned_terrorism_data/         # Cleaned data output
└── output_safety_index/            # Computed safety indices
```

## 🚀 Getting Started

### Prerequisites

```bash
pip install pyspark pandas streamlit plotly
```

### Installation

1. Clone the repository:
```bash
git clone https://github.com/Sohamcodesabit/Country-Terrorism-Data-Analysis.git
cd Country-Terrorism-Data-Analysis
```

2. Ensure you have the Global Terrorism Database CSV file in the `datasets/` folder

### Usage

#### Step 1: Clean the Data
```bash
python clean_terrorism_data.py
```
This script:
- Removes duplicates and null values
- Standardizes numeric columns
- Trims whitespace
- Filters out irrelevant columns

#### Step 2: Compute Safety Index
```bash
python compute_safety_index.py
```
This script calculates:
- **Safety Index** (0-100 scale)
- Risk levels (Critical, High, Moderate, Low)
- Attack success rates
- Casualty statistics
- Property damage metrics

#### Step 3: Launch Dashboard
```bash
streamlit run app.py
```

## 📈 Safety Index Methodology

The Safety Index is calculated using a weighted scoring system:

| Component | Weight | Description |
|-----------|--------|-------------|
| **Incident Score** | 30% | Inverse of normalized total incidents |
| **Fatality Score** | 40% | Inverse of normalized deaths |
| **Injury Score** | 20% | Inverse of normalized injuries |
| **Success Rate Score** | 10% | Inverse of attack success rate |

**Total Range**: 0-100 (Higher = Safer)

### Risk Level Classification

- 🟢 **Low Risk**: Safety Index > 75
- 🟡 **Moderate Risk**: Safety Index 50-75
- 🟠 **High Risk**: Safety Index 25-50
- 🔴 **Critical Risk**: Safety Index < 25

## 📊 Dashboard Features

### 1. Global Statistics
- Total countries analyzed
- Total incidents recorded
- Total casualties
- Safest country identification

### 2. Interactive World Map
- Color-coded countries by safety index
- Hover details showing statistics
- ISO-3 country code mapping

### 3. Visualizations
- **Safety Index Rankings**: Horizontal bar chart of safest countries
- **Risk Distribution**: Pie chart showing risk level breakdown
- **Casualties Analysis**: Stacked bar chart of deaths and injuries

### 4. Country Details
- Individual country lookup
- Detailed statistics display
- Attack success rates
- Suicide attack metrics
- Property damage information

### 5. Data Table
- Sortable and filterable data table
- CSV export functionality

## 🎨 Dashboard Filters

- **Risk Level Filter**: View countries by risk category
- **Top N Selector**: Show top 5-50 countries or all
- **Country Search**: Detailed view for specific countries

## 🔧 Configuration

### PySpark Settings (clean_terrorism_data.py)
Update these paths according to your system:
```python
os.environ["HADOOP_HOME"] = "C:/Spark/hadoop"
os.environ["PYSPARK_PYTHON"] = "path/to/python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = "path/to/python.exe"
```

### Input/Output Paths (compute_safety_index.py)
```python
INPUT_PATH = "./cleaned_terrorism_data/*.csv"
OUTPUT_PATH = "./output_safety_index"
```

## 📊 Data Sources

This project uses the **Global Terrorism Database (GTD)**, which includes:
- Terrorist incidents from 1970 onwards
- Over 200,000+ incidents worldwide
- Detailed information on attacks, casualties, and perpetrators

## 🤝 Contributing

Contributions are welcome! Please feel free to submit pull requests or open issues for bugs and feature requests.

## 📝 License

This project is open source and available for educational purposes.

## 👤 Author

**Soham**
- GitHub: [@Sohamcodesabit](https://github.com/Sohamcodesabit)

## 🙏 Acknowledgments

- Global Terrorism Database (GTD)
- PySpark community
- Streamlit team

## 📞 Support

For questions or issues, please open an issue on GitHub.

---

**Note**: This project is for analytical and educational purposes only. The safety indices are statistical measures and should not be used as the sole factor for travel or security decisions.
