import requests
import pandas as pd
import json
from datetime import datetime

# Your EIA API key - REPLACE THIS with your actual key
API_KEY = "your_api_key_here"

def get_crude_oil_production(state="USA", start_year=2018):
    """
    Fetch crude oil production data from EIA API
    
    Parameters:
    - state: State code (e.g., 'TX' for Texas, 'USA' for total US)
    - start_year: Starting year for data retrieval
    """
    
    # EIA API endpoint for crude oil production
    base_url = "https://api.eia.gov/v2/petroleum/crd/crpdn/data/"
    
    params = {
        "api_key": API_KEY,
        "frequency": "monthly",
        "data[0]": "value",
        "facets[duoarea][]": state,
        "sort[0][column]": "period",
        "sort[0][direction]": "desc",
        "offset": 0,
        "length": 5000
    }
    
    try:
        print(f"Fetching crude oil production data for {state}...")
        response = requests.get(base_url, params=params)
        response.raise_for_status()
        
        data = response.json()
        
        if 'response' in data and 'data' in data['response']:
            df = pd.DataFrame(data['response']['data'])
            print(f"Successfully retrieved {len(df)} records")
            return df
        else:
            print("No data found in response")
            return None
            
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return None

def get_natural_gas_production(state="USA", start_year=2018):
    """
    Fetch natural gas production data from EIA API
    """
    
    base_url = "https://api.eia.gov/v2/natural-gas/prod/sum/data/"
    
    params = {
        "api_key": API_KEY,
        "frequency": "monthly",
        "data[0]": "value",
        "facets[process][]": "PGP",  # Gross withdrawals
        "facets[area][]": state,
        "sort[0][column]": "period",
        "sort[0][direction]": "desc",
        "offset": 0,
        "length": 5000
    }
    
    try:
        print(f"Fetching natural gas production data for {state}...")
        response = requests.get(base_url, params=params)
        response.raise_for_status()
        
        data = response.json()
        
        if 'response' in data and 'data' in data['response']:
            df = pd.DataFrame(data['response']['data'])
            print(f"Successfully retrieved {len(df)} records")
            return df
        else:
            print("No data found in response")
            return None
            
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return None

def save_to_csv(df, filename):
    """Save dataframe to CSV file"""
    if df is not None:
        df.to_csv(filename, index=False)
        print(f"Data saved to {filename}")
    else:
        print("No data to save")

if __name__ == "__main__":
    # Test the extraction
    print("=" * 50)
    print("EIA DATA EXTRACTION PIPELINE")
    print("=" * 50)
    
    # Extract crude oil data
    oil_df = get_crude_oil_production(state="USA")
    if oil_df is not None:
        print(f"\nOil data preview:")
        print(oil_df.head())
        save_to_csv(oil_df, "crude_oil_production.csv")
    
    print("\n" + "=" * 50)
    
    # Extract natural gas data
    gas_df = get_natural_gas_production(state="USA")
    if gas_df is not None:
        print(f"\nGas data preview:")
        print(gas_df.head())
        save_to_csv(gas_df, "natural_gas_production.csv")
    
    print("\n" + "=" * 50)
    print("Extraction complete!")
