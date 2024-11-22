import os

# Define the top-level directory
project_directory = "."
 # Change this if 'data' isn't in your current directory

# Initialize counters
total_files = 0
csv_files = 0

# Traverse directories recursively
for root, _, files in os.walk(project_directory):
    for file in files:
        total_files += 1
        if file.endswith(".csv"):
            csv_files += 1

# Print the counts
print(f"Total files: {total_files}")
print(f"Total CSV files: {csv_files}")