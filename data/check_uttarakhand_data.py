import os
import pandas as pd

# Define the directory with the CSV files
base_dir = "/Users/ashleyrice/Desktop/Omdena/qgis-test/data/202403"

# Define Uttarakhand's latitude and longitude range
latitude_range = (28.43, 31.27)
longitude_range = (77.53, 81.03)

# Initialize counters and results
total_files = 0
matching_files = 0
files_with_data = []

# Walk through all the files in the directory
for root, dirs, files in os.walk(base_dir):
    for file in files:
        if file.endswith(".csv"):
            total_files += 1
            file_path = os.path.join(root, file)
            try:
                # Read the CSV file
                data = pd.read_csv(file_path)
                # Ensure latitude and longitude columns exist
                if 'lat' in data.columns and 'lon' in data.columns:
                    # Check if any rows match Uttarakhand's range
                    matches = data[(data['lat'] >= latitude_range[0]) & (data['lat'] <= latitude_range[1]) &
                                   (data['lon'] >= longitude_range[0]) & (data['lon'] <= longitude_range[1])]
                    if not matches.empty:
                        matching_files += 1
                        files_with_data.append(file_path)
            except Exception as e:
                print(f"Error reading file {file_path}: {e}")

# Print results
print(f"Total files checked: {total_files}")
print(f"Files with Uttarakhand data: {matching_files}")
print("Files with data:")
for file in files_with_data:
    print(file)
