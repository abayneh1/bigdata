import pandas as pd

def extract_data(file_path):
    # Use ISO-8859-1 encoding (Latin-1) to read the CSV file
    data = pd.read_csv(file_path, encoding='ISO-8859-1')
    return data

# Example usage
file_path = 'data/dataset.csv'  # Replace with your actual CSV file path
df = extract_data(file_path)
print(df.head())  # Print the first 5 rows of the DataFrame to verify
