"""
Cannabis Data Processing Pipeline with Dataset Shape Report
============================================================
A comprehensive ETL pipeline for processing cannabis-related datasets across Canada.
Includes store locations, sales data, retail trade, crime statistics,
and generates a summary report of dataset shapes.
"""

import pandas as pd
import numpy as np
from dotenv import load_dotenv
import requests
import os
import time
import re
import warnings
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from pyproj import Transformer

warnings.filterwarnings('ignore')

# ============================================================================
# CONFIGURATION
# ============================================================================
load_dotenv()
class Config:
    """Central configuration for the pipeline"""
    GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
    
    SCRIPT_DIR = Path(__file__).parent if '__file__' in globals() else Path.cwd()
    BASE_DIR = SCRIPT_DIR.parent
    
    RAW_DATA_DIR = BASE_DIR / "data" / "01_raw_data"
    PROCESSED_DATA_DIR = BASE_DIR / "data" / "02_processed_data"
    FINAL_OUTPUT_DIR = BASE_DIR / "data" / "03_final_outputs"
    
    API_DELAY = 0.2
    MISSING_DATA_THRESHOLD = 0.5
    
    PROVINCE_FULL_NAMES = {
        "AB": "Alberta", "BC": "British Columbia", "MB": "Manitoba",
        "NB": "New Brunswick", "NL": "Newfoundland and Labrador",
        "NS": "Nova Scotia", "NT": "Northwest Territories", "NU": "Nunavut",
        "ON": "Ontario", "PE": "Prince Edward Island", "QC": "Quebec",
        "SK": "Saskatchewan", "YT": "Yukon"
    }
    
    CRS_WGS84 = "EPSG:4326"
    CRS_WEB_MERCATOR = "EPSG:3857"
    CRS_NAD83_UTM10N = "EPSG:26910"
    REPORT_FILE = FINAL_OUTPUT_DIR / "dataset_shape_report.txt"

# ============================================================================
# UTILITY CLASSES
# ============================================================================

class GeocodingService:
    """Handles geocoding and reverse geocoding"""
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.cache = {}
    
    def get_coordinates(self, address: str) -> Tuple[Optional[float], Optional[float], Optional[str]]:
        if not address or not isinstance(address, str):
            return None, None, None
        if address in self.cache:
            return self.cache[address]
        
        url = "https://maps.googleapis.com/maps/api/geocode/json"
        params = {"address": address, "key": self.api_key}
        try:
            response = requests.get(url, params=params)
            data = response.json()
            if data["status"] == "OK":
                result = data["results"][0]
                location = result["geometry"]["location"]
                lat, lng = location["lat"], location["lng"]
                postal_code = None
                for component in result["address_components"]:
                    if "postal_code" in component["types"]:
                        postal_code = component["long_name"]
                        break
                self.cache[address] = (lat, lng, postal_code)
                return lat, lng, postal_code
        except Exception as e:
            print(f"Error geocoding {address}: {e}")
        return None, None, None
    
    def get_postal_from_coords(self, lat: float, lng: float) -> Optional[str]:
        url = "https://maps.googleapis.com/maps/api/geocode/json"
        params = {"latlng": f"{lat},{lng}", "key": self.api_key}
        try:
            response = requests.get(url, params=params)
            data = response.json()
            if data["status"] == "OK":
                for result in data["results"]:
                    for component in result["address_components"]:
                        if "postal_code" in component["types"]:
                            return component["long_name"]
        except Exception as e:
            print(f"Error reverse geocoding ({lat}, {lng}): {e}")
        return None


class CoordinateTransformer:
    @staticmethod
    def transform_coordinates(df: pd.DataFrame, x_col: str, y_col: str, 
                              source_crs: str, target_crs: str = Config.CRS_WGS84) -> pd.DataFrame:
        transformer = Transformer.from_crs(source_crs, target_crs, always_xy=True)
        longitude, latitude = transformer.transform(df[x_col], df[y_col])
        df['latitude'] = latitude
        df['longitude'] = longitude
        return df.drop(columns=[x_col, y_col])


class DataCleaner:
    @staticmethod
    def clean_bc_address(full_address: str, city: str, province: str) -> Tuple[str, Optional[str]]:
        if not isinstance(full_address, str):
            return full_address, None
        postal_pattern = r"[A-Za-z]\d[A-Za-z]\s?\d[A-Za-z]\d"
        postal_match = re.search(postal_pattern, full_address)
        postal_code = postal_match.group(0) if postal_match else None
        remove_pattern = rf",\s*{re.escape(str(city))}\s*,?\s*{str(province)}\s*{str(postal_code) if postal_code else ''}"
        cleaned = re.sub(remove_pattern, "", full_address, flags=re.IGNORECASE)
        return cleaned.strip(), postal_code
    
    @staticmethod
    def drop_high_missing_data(df: pd.DataFrame, threshold: float = Config.MISSING_DATA_THRESHOLD) -> pd.DataFrame:
        missing_pct = df.isnull().sum() / len(df)
        cols_to_drop = missing_pct[missing_pct >= threshold].index.tolist()
        df = df.drop(columns=cols_to_drop)
        row_missing_pct = df.isnull().sum(axis=1) / df.shape[1]
        df = df[row_missing_pct < threshold]
        return df



# ============================================================================
# STORE LOCATIONS PROCESSOR
# ============================================================================

class StoreLocationsProcessor:
    """Process cannabis store location data across provinces"""
    
    def __init__(self, geocoding_service: GeocodingService, max_retries: int = 3):
        self.geocoding_service = geocoding_service
        self.raw_dir = Config.RAW_DATA_DIR / "01_store_locations"
        self.max_retries = max_retries
    
    def process_province(self, file_name: str, province_code: str, 
                         file_type: str, col_mapping: Dict[str, str]) -> pd.DataFrame:
        """Generic province data processor"""
        file_path = self.raw_dir / file_name
        print(f"--- Processing {province_code} from {file_name} ---")
        
        # Load data based on file type
        try:
            if file_type == "xls":
                df = pd.read_excel(file_path, engine="xlrd")
            elif file_type == "xlsx":
                df = pd.read_excel(file_path, engine="openpyxl")
            elif file_type == "csv":
                df = pd.read_csv(file_path)
            else:
                print(f"Unsupported file format: {file_path}")
                return None
        except ImportError as e:
            print(f"Missing dependency for reading {file_type} file: {e}")
            print("➡ Install using: pip install xlrd==1.2.0 for .xls support")
            return None
        except Exception as e:
            print(f"Error reading file {file_path}: {e}")
            return None

        # Filter by province if column exists
        if "Site Province Abbrev" in df.columns:
            df = df[df["Site Province Abbrev"] == province_code].copy()
        
        # Rename columns
        df = df.rename(columns=col_mapping)
        
        # Add province info
        df["Province"] = province_code
        df["FullProvinceName"] = Config.PROVINCE_FULL_NAMES[province_code]
        
        # Initialize geocoding columns if missing
        for col in ["Latitude", "Longitude", "PostalCode", "Address"]:
            if col not in df.columns:
                df[col] = None
        
        # Process each row for geocoding
        for idx, row in df.iterrows():
            current_addr = str(row.get("Address", "") or "")
            
            # Special BC address handling
            if province_code == "BC" and "FullAddress" in row:
                clean_addr, extracted_postal = DataCleaner.clean_bc_address(
                    row["FullAddress"], row["City"], "British Columbia"
                )
                df.at[idx, "Address"] = clean_addr
                if not row.get("PostalCode"):
                    df.at[idx, "PostalCode"] = extracted_postal
                current_addr = clean_addr
            
            city = str(row.get("City", ""))
            full_prov = row["FullProvinceName"]
            api_query = f"{current_addr}, {city}, {full_prov}, Canada"
            
            lat, lng, postal = None, None, None
            for attempt in range(1, self.max_retries + 1):
                lat, lng, postal = self.geocoding_service.get_coordinates(api_query)
                if lat is not None and lng is not None:
                    break
                print(f"⚠ Retry {attempt}/{self.max_retries} failed for: {api_query}")
                time.sleep(Config.API_DELAY * 2)
            
            if lat is None or lng is None:
                print(f"❌ Geocoding failed for: {api_query}")
            
            df.at[idx, "Latitude"] = lat
            df.at[idx, "Longitude"] = lng
            if pd.isna(df.at[idx, "PostalCode"]) and postal:
                df.at[idx, "PostalCode"] = postal
            
            # Print geocoded result
            print(f"✅ {current_addr} | Latitude: {lat} | Longitude: {lng} | PostalCode: {df.at[idx, 'PostalCode']}")
            
            time.sleep(Config.API_DELAY)
        
        # Return standardized columns
        target_cols = ["StoreName", "City", "Province", "FullProvinceName", 
                       "Address", "PostalCode", "Latitude", "Longitude"]
        return df[[c for c in target_cols if c in df.columns]]
    
    def process_all_provinces(self) -> pd.DataFrame:
        """Process all province data and combine"""
        province_configs = [
            ("Alberta.xls", "AB", "xls", {
                "Establishment Name": "StoreName", "Site City Name": "City",
                "Site Address Line 1": "Address", "Site Postal Code": "PostalCode"
            }),
            ("Alberta.xls", "BC", "xls", {
                "Establishment Name": "StoreName", "Site City Name": "City",
                "Site Address Line 1": "Address", "Site Postal Code": "PostalCode"
            }),
            ("BritishColumbia.csv", "BC", "csv", {"FullAddress": "FullAddress"}),
            ("Alberta.xls", "MB", "xls", {
                "Establishment Name": "StoreName", "Site City Name": "City",
                "Site Address Line 1": "Address", "Site Postal Code": "PostalCode"
            }),
            ("Manitoba.csv", "MB", "csv", {"FullAddress": "Address"}),
            ("NewBrunswick.csv", "NB", "csv", {"FullAddress": "Address"}),
            ("Newfoundland.csv", "NL", "csv", {"FullAddress": "Address"}),
            ("NorthwestTerritories.csv", "NT", "csv", {"FullAddress": "Address"}),
            ("NovaScotia.csv", "NS", "csv", {"FullAddress": "Address"}),
            ("Nunavut.csv", "NU", "csv", {"FullAddress": "Address"}),
            ("Ontario.csv", "ON", "csv", {
                "Store Name": "StoreName", "Municipality or First Nation": "City",
                "FullAddress": "Address"
            }),
            ("PrinceEdwardIsland.csv", "PE", "csv", {"FullAddress": "Address"}),
            ("Quebec.csv", "QC", "csv", {"FullAddress": "Address"}),
            ("Saskatchewan.csv", "SK", "csv", {
                "Operating Name": "StoreName", "Street Address": "Address"
            }),
            ("Yukon.csv", "YT", "csv", {"FullAddress": "Address"}),
        ]
        
        dfs = []
        for config in province_configs:
            try:
                df = self.process_province(*config)
                if df is not None and not df.empty:
                    dfs.append(df)
            except Exception as e:
                print(f"Error processing {config[0]}: {e}")
        
        if not dfs:
            raise ValueError(
                "No data files were successfully processed. Please check:\n"
                f"1. Files exist in: {self.raw_dir}\n"
                "2. File names match the configuration\n"
                "3. You have read permissions"
            )
        
        # Combine all dataframes
        master_df = pd.concat(dfs, ignore_index=True)
        
        # Fill missing postal codes using reverse geocoding
        missing_postal = master_df['PostalCode'].isna()
        for idx in master_df[missing_postal].index:
            lat, lng = master_df.loc[idx, ['Latitude', 'Longitude']]
            if pd.notna(lat) and pd.notna(lng):
                postal = self.geocoding_service.get_postal_from_coords(lat, lng)
                master_df.at[idx, 'PostalCode'] = postal
                print(f"🔄 Filled missing postal for {master_df.at[idx, 'Address']}: {postal}")
                time.sleep(Config.API_DELAY)
        
        return master_df



# ============================================================================
# SALES DATA PROCESSOR
# ============================================================================

class SalesDataProcessor:
    """Process cannabis sales data"""
    
    def __init__(self):
        self.raw_path = Config.RAW_DATA_DIR / "02_cannabis_sales" / "cannabis_sales.csv"
    
    def process(self) -> pd.DataFrame:
        """Process sales data"""
        print("--- Processing Sales Data ---")
        df = pd.read_csv(self.raw_path)
        
        # Select relevant columns
        columns_to_keep = [
            "REF_DATE", "GEO", "DGUID", "Type of cannabis",
            "UOM", "UOM_ID", "SCALAR_FACTOR", "SCALAR_ID", "VALUE"
        ]
        df = df[columns_to_keep].copy()
        
        print(f"Sales data shape: {df.shape}")
        return df


# ============================================================================
# RETAIL TRADE PROCESSOR
# ============================================================================

class RetailTradeProcessor:
    """Process retail trade data"""
    
    def __init__(self):
        self.raw_path = Config.RAW_DATA_DIR / "03_retail_trade" / "retail_trade.csv"
    
    def process(self) -> pd.DataFrame:
        """Process retail trade data"""
        print("--- Processing Retail Trade Data ---")
        df = pd.read_csv(self.raw_path)
        
        # Filter for cannabis retailers
        cannabis_df = df[
            df['North American Industry Classification System (NAICS)']
            .str.contains('Cannabis retailers', na=False)
        ].copy()
        
        # Clean data
        cannabis_df = DataCleaner.drop_high_missing_data(cannabis_df)
        
        # Drop rows with missing values in VALUE column
        cannabis_df = cannabis_df.dropna(subset=['VALUE'])
        
        # Optimize data types
        if 'REF_DATE' in cannabis_df.columns:
            cannabis_df['REF_DATE'] = pd.to_datetime(cannabis_df['REF_DATE'])
        
        if 'VALUE' in cannabis_df.columns:
            cannabis_df['VALUE'] = pd.to_numeric(cannabis_df['VALUE'], errors='coerce')
        
        # Convert text columns to category
        for col in ['GEO', 'Sales', 'Adjustments', 'North American Industry Classification System (NAICS)']:
            if col in cannabis_df.columns and cannabis_df[col].dtype == 'object':
                cannabis_df[col] = cannabis_df[col].astype('category')
        
        print(f"Retail trade data shape: {cannabis_df.shape}")
        return cannabis_df


# ============================================================================
# CRIME DATA PROCESSOR
# ============================================================================

class CrimeDataProcessor:
    """Process crime statistics data"""
    
    def __init__(self, geocoding_service: GeocodingService):
        self.geocoding_service = geocoding_service
        self.raw_dir = Config.RAW_DATA_DIR / "04_crime_data"
        self.city_raw_dir = Config.RAW_DATA_DIR / "05_crime_by_city_data"
    
    def process_national_data(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Process national and provincial crime data"""
        print("--- Processing National Crime Data ---")
        
        # Load crime data
        crime_df = pd.read_parquet(self.raw_dir / "crime_data.parquet")
        
        # Parse violation information
        crime_df['Violation Name'] = crime_df['Violations'].str.replace(
            r'\s*\[[^\]]+\]$', '', regex=True
        ).str.strip()
        crime_df['Violation Code'] = crime_df['Violations'].str.extract(r'\[([^\]]+)\]').squeeze()
        
        # Get unique locations for filtering
        sales_df = pd.read_csv(Config.PROCESSED_DATA_DIR / "02_cannabis_sales" / "sales_data.csv")
        unique_locations = sales_df["DGUID"].unique()
        
        # Split into province/national and city data
        province_data = crime_df[crime_df["DGUID"].isin(unique_locations)].copy()
        city_data = crime_df[~crime_df["DGUID"].isin(unique_locations)].copy()
        
        # Select relevant columns
        selected_cols = ['REF_DATE', 'GEO', 'DGUID', 'Violation Name', 
                        'Violation Code', 'Statistics', 'UOM', 'VALUE']
        
        province_data = province_data[selected_cols].dropna(subset=['VALUE'])
        city_data = city_data[selected_cols].dropna(subset=['VALUE', 'DGUID'])
        
        print(f"Province/National data shape: {province_data.shape}")
        print(f"City data shape: {city_data.shape}")
        
        return province_data, city_data
    
    def process_toronto_data(self) -> Dict[str, pd.DataFrame]:
        """Process Toronto-specific crime data"""
        print("--- Processing Toronto Crime Data ---")
        toronto_dir = self.city_raw_dir / "Toronto"
        
        # Traffic collisions
        traffic_df = pd.read_parquet(toronto_dir / "Totonto_Traffic_Collisions.parquet")
        traffic_df = traffic_df.rename(columns={
            'OBJECTID': 'Object_ID', 'OCC_DATE': 'Accident_Date',
            'LAT_WGS84': 'Latitude', 'LONG_WGS84': 'Longitude',
            'FATALITIES': 'Fatalities', 'INJURY_COLLISIONS': 'Injuries',
            'FTR_COLLISIONS': 'Fatal_And_Injury_Collisions',
            'PD_COLLISIONS': 'Property_Damage_Only_Collisions',
            'NEIGHBOURHOOD_158': 'Neighbourhood'
        })
        
        # Classify collision type
        def classify_collision(row):
            if row["Fatal_And_Injury_Collisions"] == "YES":
                return "Fatal & Injury"
            elif row["Injuries"] == "YES":
                return "Injury"
            elif row["Property_Damage_Only_Collisions"] == "YES":
                return "Property Damage Only"
            return "None"
        
        traffic_df['Collision_Type'] = traffic_df.apply(classify_collision, axis=1)
        traffic_df = traffic_df[[
            "Object_ID", "Accident_Date", "Latitude", "Longitude",
            "Fatalities", "Collision_Type", "Neighbourhood"
        ]]
        
        # Person in crisis
        crisis_df = pd.read_csv(toronto_dir / "Toronto_Persons_in_Crisis_Calls_for_Service_Attended.csv")
        crisis_df = crisis_df.rename(columns={
            'OBJECTID': 'Object_ID', 'EVENT_DATE': 'Accident_Date',
            'EVENT_TYPE': 'Event_Type', 'NEIGHBOURHOOD_158': 'Neighbourhood'
        })
        crisis_df = crisis_df[["Object_ID", "Accident_Date", "Event_Type", "Neighbourhood"]]
        
        # Calls for service
        calls_df = pd.read_csv(toronto_dir / "Toronto_Calls_for_Service_Attended.csv")
        calls_df = calls_df.rename(columns={
            'ObjectId': 'Object_ID', 'EVENT_YEAR': 'Year',
            'EVENT_COUNT': 'Event_Count', 'NEIGHBOURHOOD_158': 'Neighbourhood'
        })
        calls_df["Neighbourhood"] = calls_df["Neighbourhood"] + " (" + calls_df["HOOD_158"] + ")"
        calls_df = calls_df[["Object_ID", "Year", "Event_Count", "Neighbourhood"]]
        
        return {
            "traffic_collisions": traffic_df,
            "person_in_crisis": crisis_df,
            "calls_for_service": calls_df
        }
    
    def process_edmonton_data(self) -> pd.DataFrame:
        """Process Edmonton-specific crime data"""
        print("--- Processing Edmonton Crime Data ---")
        edmonton_dir = self.city_raw_dir / "Edmonton"
        
        # Load all years
        dfs = []
        for year in [2022, 2023, 2024, 2025]:
            df = pd.read_csv(edmonton_dir / f"Crimes_{year}.csv")
            dfs.append(df)
        
        edmonton_df = pd.concat(dfs, ignore_index=True)
        
        # Create full addresses for geocoding
        edmonton_df['Full_Address'] = edmonton_df['Intersection'] + ", Edmonton, AB, Canada"
        
        # Geocode unique addresses
        unique_addresses = edmonton_df['Full_Address'].dropna().unique()
        print(f"Geocoding {len(unique_addresses)} unique Edmonton addresses...")
        
        coordinate_map = {}
        for i, address in enumerate(unique_addresses):
            lat, lng, postal = self.geocoding_service.get_coordinates(address)
            coordinate_map[address] = (lat, lng)
            
            # Print 1 record after every 10 records
            if (i + 1) % 10 == 0:
                sample_idx = edmonton_df[edmonton_df['Full_Address'] == address].index[0]
                row = edmonton_df.loc[sample_idx]
                print(f"Sample after {i+1} addresses:")
                print(f"{row['Full_Address']} | {lng} | {lat} | {postal}\n")
            
            time.sleep(Config.API_DELAY)
        
        # Map coordinates back
        edmonton_df['coordinates'] = edmonton_df['Full_Address'].map(coordinate_map)
        edmonton_df[['latitude', 'longitude']] = pd.DataFrame(
            edmonton_df['coordinates'].tolist(), index=edmonton_df.index
        )
        edmonton_df = edmonton_df.drop(columns=['coordinates'])
        
        print(f"Edmonton data shape: {edmonton_df.shape}")
        return edmonton_df
    
    def process_vancouver_data(self) -> pd.DataFrame:
        """Process Vancouver-specific crime data"""
        print("--- Processing Vancouver Crime Data ---")
        vancouver_dir = self.city_raw_dir / "Vancouver"
        
        # Load all years
        dfs = []
        for year in range(2014, 2026):
            df = pd.read_csv(vancouver_dir / f"Crimes_{year}.csv")
            dfs.append(df)
        
        vancouver_df = pd.concat(dfs, ignore_index=True)
        
        # Create date field
        vancouver_df["Date_Reported"] = (
            vancouver_df["YEAR"].astype(str) + "-" +
            vancouver_df["MONTH"].astype(str).str.zfill(2) + "-" +
            vancouver_df["DAY"].astype(str).str.zfill(2)
        )
        vancouver_df = vancouver_df.drop(columns=['YEAR', 'MONTH', 'DAY', 'HOUR', 'MINUTE'])
        
        # Transform coordinates from NAD83 UTM Zone 10N to WGS84
        vancouver_df = CoordinateTransformer.transform_coordinates(
            vancouver_df, 'X', 'Y', Config.CRS_NAD83_UTM10N
        )
        
        print(f"Vancouver data shape: {vancouver_df.shape}")
        return vancouver_df


# ============================================================================
# DATASET SHAPE REPORT FUNCTION
# ============================================================================

def get_dataset_shape(file_path: Path):
    try:
        if file_path.suffix.lower() == ".csv":
            df = pd.read_csv(file_path)
        elif file_path.suffix.lower() == ".parquet":
            df = pd.read_parquet(file_path)
        else:
            return None
        return df.shape
    except Exception as e:
        print(f"Error reading {file_path.name}: {e}")
        return None

def generate_shape_report(output_dir: Path):
    files = [f for f in output_dir.rglob("*") if f.suffix.lower() in [".csv", ".parquet"]]
    report_lines = [
        "="*80,
        "DATASET ROWS x COLUMNS REPORT",
        "="*80,
        "",
    ]
    for file_path in files:
        shape = get_dataset_shape(file_path)
        if shape:
            rows, cols = shape
            report_lines.append(f"{file_path.relative_to(output_dir)}".ljust(50, ".") + f"{rows:>8} rows x {cols:>3} cols")
        else:
            report_lines.append(f"{file_path.relative_to(output_dir)}".ljust(50, ".") + "ERROR reading file")
    report_lines.append("")
    report_lines.append("="*80)
    report_lines.append(f"Report generated: {pd.Timestamp.now()}")
    report_lines.append("="*80)
    report = "\n".join(report_lines)
    print(report)
    
    output_dir.mkdir(parents=True, exist_ok=True)
    with open(Config.REPORT_FILE, "w") as f:
        f.write(report)
    print(f"\nReport saved to: {Config.REPORT_FILE.resolve()}")

# ============================================================================
# MAIN PIPELINE
# ============================================================================

class CannabisDataPipeline:
    def __init__(self):
        self.geocoding_service = GeocodingService(Config.GOOGLE_API_KEY)
        self.output_dir = Config.FINAL_OUTPUT_DIR
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def run(self):
        start_time = time.time()
        print("\nCANNABIS DATA PROCESSING PIPELINE\n")
        
        # 1. Process store locations
        store_processor = StoreLocationsProcessor(self.geocoding_service)
        store_locations = store_processor.process_all_provinces()
        self._save_output(store_locations, "store_locations.csv")
        
        # 2. Process sales data
        sales_processor = SalesDataProcessor()
        sales_data = sales_processor.process()
        self._save_output(sales_data, "sales_data.csv")
        
        # 3. Process retail trade
        retail_processor = RetailTradeProcessor()
        retail_data = retail_processor.process()
        self._save_output(retail_data, "retail_trade_data.csv")
        
        # 4. Process crime data
        crime_processor = CrimeDataProcessor(self.geocoding_service)
        province_crime, city_crime = crime_processor.process_national_data()
        self._save_output(province_crime, "crime_province_national.parquet")
        self._save_output(city_crime, "crime_city_level.parquet")
        
        toronto_data = crime_processor.process_toronto_data()
        self._save_output(toronto_data["traffic_collisions"], "toronto_traffic_collisions.parquet")
        self._save_output(toronto_data["person_in_crisis"], "toronto_person_in_crisis.csv")
        self._save_output(toronto_data["calls_for_service"], "toronto_calls_for_service.csv")
        
        edmonton_data = crime_processor.process_edmonton_data()
        self._save_output(edmonton_data, "edmonton_crimes.parquet")
        
        vancouver_data = crime_processor.process_vancouver_data()
        self._save_output(vancouver_data, "vancouver_crimes.csv")
        
        # 5. Generate dataset shape report
        print("\nGenerating Dataset Shape Report...")
        generate_shape_report(self.output_dir)
        
        end_time = time.time()
        print(f"\nPipeline completed in {end_time - start_time:.2f} seconds.")

    def _save_output(self, df: pd.DataFrame, filename: str):
        output_path = self.output_dir / filename
        if filename.endswith('.csv'):
            df.to_csv(output_path, index=False)
        elif filename.endswith('.parquet'):
            df.to_parquet(output_path, index=False)
        print(f"Saved: {filename} ({df.shape[0]} rows x {df.shape[1]} cols)")

# ============================================================================
# ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    pipeline = CannabisDataPipeline()
    pipeline.run()