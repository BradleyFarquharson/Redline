# Redline

A data reconciliation tool for SIM-Bundle usage validation.

## Description

Redline is a tool that compares data usage reports from three sources:
- Supplier/MNO Usage Reports
- iONLINE Raw Usage Reports 
- Customer Billing Reports

The application validates that the data volumes match within configurable thresholds, highlighting discrepancies with visual flags (OK, WARN, FAIL).

## Features

- Automatic schema mapping to standardize column names
- Data reconciliation with configurable warning and failure thresholds
- Excel report generation with conditional formatting
- Simple web interface using Streamlit

## Getting Started

### Prerequisites
- Python 3.12 or higher
- Required packages listed in `requirements.txt`

### Installation

```bash
# Clone the repository
git clone https://github.com/BradleyFarquharson/Redline.git
cd Redline

# Install dependencies
pip install -r requirements.txt

# Run the application
streamlit run redline/app.py
```

### Docker
```bash
docker build -t redline .
docker run -p 8501:8501 redline
``` 