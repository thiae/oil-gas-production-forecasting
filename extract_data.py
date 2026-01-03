import requests
import pandas as pd
from datetime import datetime

# Your EIA API key
API_KEY = "amUwiz089HSfLRFiM2PKGCknDOAbeJpqkAwdIh0f"

def get_petroleum_data():
    """
    Fetch US petroleum production data from EIA API
    This gets total US crude oil production
    """
    
    # API endpoint for petroleum production
    url = "https://api.eia.gov/v2/petroleum/crd/crpdn/data/"
    
    params = {
        "api_key": API_KEY,
        "frequency": "monthly",
        "data[0]": "value",
        "start": "2018-01",
        "sort[0][column]": "period",
        "sort[0][direction]": "desc",
        "length": 5000
    }
    
    try:
        print("Fetching US crude oil production data...")
        response = requests.get(url, params=params)
        
        print(f"Status Code: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            
            if 'response' in data and 'data' in data['response']:
                records = data['response']['data']
                df = pd.DataFrame(records)
                print(f"Successfully retrieved {len(df)} records")
                return df
            else:
                print("Response structure:", data.keys() if isinstance(data, dict) else type(data))
                return None
        else:
            print(f"Error: {response.status_code}")
            print(f"Response: {response.text[:500]}")
            return None
            
    except Exception as e:
        print(f"Error fetching data: {e}")
        return None

def get_natural_gas_data():
    """
    Fetch US natural gas production data from EIA API
    """
    
    url = "https://api.eia.gov/v2/natural-gas/prod/sum/data/"
    
    params = {
        "api_key": API_KEY,
        "frequency": "monthly",
        "data[0]": "value",
        "start": "2018-01",
        "sort[0][column]": "period",
        "sort[0][direction]": "desc",
        "length": 5000
    }
    
    try:
        print("\nFetching US natural gas production data...")
        response = requests.get(url, params=params)
        
        print(f"Status Code: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            
            if 'response' in data and 'data' in data['response']:
                records = data['response']['data']
                df = pd.DataFrame(records)
                print(f"Successfully retrieved {len(df)} records")
                return df
            else:
                print("Response structure:", data.keys() if isinstance(data, dict) else type(data))
                return None
        else:
            print(f"Error: {response.status_code}")
            print(f"Response: {response.text[:500]}")
            return None
            
    except Exception as e:
        print(f"Error fetching data: {e}")
        return None

def explore_data(df, name):
    """Quick exploration of the dataframe"""
    if df is not None and not df.empty:
        print(f"\n{'='*50}")
        print(f"{name} - Data Summary")
        print(f"{'='*50}")
        print(f"Shape: {df.shape}")
        print(f"\nColumns: {df.columns.tolist()}")
        print(f"\nFirst 5 rows:")
        print(df.head())
        print(f"\nData types:")
        print(df.dtypes)

def save_to_csv(df, filename):
    """Save dataframe to CSV file"""
    if df is not None and not df.empty:
        df.to_csv(filename, index=False)
        print(f"\n✓ Data saved to {filename}")
    else:
        print(f"\n✗ No data to save for {filename}")

if __name__ == "__main__":
    print("="*60)
    print("EIA DATA EXTRACTION PIPELINE")
    print("="*60)
    
    # Extract petroleum data
    oil_df = get_petroleum_data()
    explore_data(oil_df, "CRUDE OIL")
    save_to_csv(oil_df, "crude_oil_production.csv")
    
    # Extract natural gas data
    gas_df = get_natural_gas_data()
    explore_data(gas_df, "NATURAL GAS")
    save_to_csv(gas_df, "natural_gas_production.csv")
    
    print("\n" + "="*60)
    print("EXTRACTION COMPLETE!")
    print("="*60)