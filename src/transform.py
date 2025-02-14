import pandas as pd
import os

def transform_data(df):
    """
    Cleans and transforms the e-commerce dataset by:
    1. Dropping rows with missing customer_id.
    2. Converting working_date to datetime format.
    3. Removing negative or zero quantities.
    """
    # Drop rows with missing customer_id
    if 'customer_id' in df.columns:
        df = df.dropna(subset=['customer_id'])
    else:
        print("Warning: 'customer_id' column not found in the data.")
    
    # Convert the working_date to datetime format
    if 'working_date' in df.columns:
        df['working_date'] = pd.to_datetime(df['working_date'], errors='coerce')
    else:
        print("Warning: 'working_date' column not found in the data.")
    
    # Remove negative or zero quantities
    if 'qty_ordered' in df.columns:
        df = df[df['qty_ordered'] > 0]
    else:
        print("Warning: 'qty_ordered' column not found in the data.")
    
    return df

def save_transformed_data(df, output_path):
    """
    Saves the transformed DataFrame as a CSV file.
    Creates the directory if it doesn't exist.
    """
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False)
    print(f"Transformed data saved at: {output_path}")

# File paths
input_file_path = 'data/dataset.csv'  # Replace with your actual CSV file path
output_file_path = 'data/clean_data.csv'  # Updated output file path

# Read raw data
df = pd.read_csv(input_file_path, encoding='ISO-8859-1')

# Transform data
transformed_df = transform_data(df)

# Save transformed data
save_transformed_data(transformed_df, output_file_path)

# Print first 5 rows
print(transformed_df.head())
