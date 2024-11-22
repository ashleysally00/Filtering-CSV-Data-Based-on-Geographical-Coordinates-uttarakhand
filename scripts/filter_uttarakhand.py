import os
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point

# Define paths
csv_base_folder = os.path.abspath('../data')  # Path to main CSV folder
shapefile_path = os.path.abspath('../shapefiles/nyu_2451_42209/india-village-census-2001-UK.shp')  # Path to Uttarakhand shapefile
output_csv = os.path.abspath('../output/Filtered_Uttarakhand_Data.csv')  # Path to save the consolidated CSV

# Define column names based on your CSV structure
columns = [
    'id', 'year', 'month', 'day', 'time_utc',
    'lat', 'lon', 'area_km2', 'volcano', 'level',
    'reliability', 'frp_wm2', 'qf', 'hot_id'
]

# Load the Uttarakhand shapefile
try:
    uttarakhand = gpd.read_file(shapefile_path)
    print(f"Shapefile loaded successfully from {shapefile_path}.")
except FileNotFoundError:
    print(f"Error: Shapefile not found at {shapefile_path}. Please check the path and filename.")
    exit(1)
except Exception as e:
    print(f"An unexpected error occurred while loading the shapefile: {e}")
    exit(1)

# Ensure the shapefile is in WGS 84 CRS
try:
    uttarakhand = uttarakhand.to_crs(epsg=4326)
except Exception as e:
    print(f"Error in setting CRS for shapefile: {e}")
    exit(1)

# Initialize an empty list to hold filtered data
filtered_data = []

# Traverse through all subdirectories and files
for root, dirs, files in os.walk(csv_base_folder):
    for file in files:
        if file.lower().endswith('.csv'):
            file_path = os.path.join(root, file)
            try:
                # Read the CSV file with appropriate parameters
                df = pd.read_csv(
                    file_path,
                    sep='\t',               # Use tab as delimiter
                    skiprows=2,             # Skip the first two comment lines
                    names=columns,          # Assign predefined column names
                    engine='python'         # Use the Python engine for better compatibility
                )
                
                # Debug: Print the first few rows to verify correct parsing
                print(f"\nProcessing {file_path}:")
                print(f"Columns found: {df.columns.tolist()}")
                print(f"First 5 rows:\n{df.head()}")
                
                # Check if 'lat' and 'lon' are in columns
                if 'lat' in df.columns and 'lon' in df.columns:
                    # Drop rows with missing coordinates
                    df = df.dropna(subset=['lat', 'lon'])
                    
                    # Convert latitude and longitude to numeric, coercing errors
                    df['lat'] = pd.to_numeric(df['lat'], errors='coerce')
                    df['lon'] = pd.to_numeric(df['lon'], errors='coerce')
                    
                    # Drop rows where conversion failed
                    df = df.dropna(subset=['lat', 'lon'])
                    
                    # Create geometry column
                    geometry = [Point(xy) for xy in zip(df['lon'], df['lat'])]
                    gdf = gpd.GeoDataFrame(df, geometry=geometry)
                    gdf.set_crs(epsg=4326, inplace=True)
                    
                    # Spatial join to filter points within Uttarakhand
                    points_within = gpd.sjoin(gdf, uttarakhand, how='inner', predicate='within')
                    
                    # Append to the list
                    filtered_data.append(points_within)
                    
                    print(f"Processed {file_path}: {len(points_within)} points within Uttarakhand.")
                else:
                    print(f"Skipped {file_path}: 'lat' and/or 'lon' columns not found.")
            except pd.errors.EmptyDataError:
                print(f"Skipped {file_path}: File is empty.")
            except pd.errors.ParserError:
                print(f"Skipped {file_path}: Parsing error encountered.")
            except Exception as e:
                print(f"Error processing {file_path}: {e}")

# Concatenate all filtered data
if filtered_data:
    try:
        consolidated_df = pd.concat(filtered_data, ignore_index=True)
        # Drop the index_right column added by sjoin if it exists
        if 'index_right' in consolidated_df.columns:
            consolidated_df = consolidated_df.drop(columns=['index_right'])
        
        # Save to CSV
        consolidated_df.to_csv(output_csv, index=False)
        print(f"\nConsolidated filtered data saved to {output_csv}.")
    except Exception as e:
        print(f"Error during concatenation or saving CSV: {e}")
else:
    print("No data points found within Uttarakhand.")
