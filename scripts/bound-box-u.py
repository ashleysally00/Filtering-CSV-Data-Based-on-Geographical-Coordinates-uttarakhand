import os
import pandas as pd

# Define paths
csv_base_folder = os.path.abspath('../data')  # Path to main CSV folder
output_csv = os.path.abspath('../output/Filtered_Uttarakhand_Data.csv')  # Path to save the consolidated CSV

# Define column names based on your CSV structure
columns = [
    'id', 'year', 'month', 'day', 'time_utc',
    'lat', 'lon', 'area_km2', 'volcano', 'level',
    'reliability', 'frp_wm2', 'qf', 'hot_id'
]

# Define Uttarakhand's bounding box coordinates
uttarakhand_bounds = {
    'lat_min': 29.24,
    'lat_max': 31.30,  # Updated from 30.46 to 31.30
    'lon_min': 77.50,  # Updated from 78.07 to 77.50
    'lon_max': 81.80   # Slight adjustment if necessary
}


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
                    
                    # Apply bounding box filter
                    df_filtered = df[
                        (df['lat'] >= uttarakhand_bounds['lat_min']) &
                        (df['lat'] <= uttarakhand_bounds['lat_max']) &
                        (df['lon'] >= uttarakhand_bounds['lon_min']) &
                        (df['lon'] <= uttarakhand_bounds['lon_max'])
                    ]
                    
                    # Append to the list
                    filtered_data.append(df_filtered)
                    
                    print(f"Processed {file_path}: {len(df_filtered)} points within Uttarakhand.")
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
        
        # Save to CSV
        consolidated_df.to_csv(output_csv, index=False)
        print(f"\nConsolidated filtered data saved to {output_csv}.")
    except Exception as e:
        print(f"Error during concatenation or saving CSV: {e}")
else:
    print("No data points found within Uttarakhand.")
