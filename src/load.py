import psycopg2
import pandas as pd
from transform import transform_data  # Ensure transform is imported

# Database connection details
DATABASE_URL = "postgresql://postgres:nata2809@localhost/sales_data"  # Modify with actual credentials

# Function to clean and preprocess the data
def preprocess_data(df):
    # Clean up numeric columns (grand_total, discount_amount, price, etc.)
    for col in ['grand_total', 'discount_amount', 'price']:
        df[col] = df[col].replace({',': ''}, regex=True)  # Remove commas
        df[col] = pd.to_numeric(df[col], errors='coerce')  # Convert to numeric, replace errors with NaN

    # Handle date columns
    if 'created_at' in df.columns:
        df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')  # Convert to datetime, errors to NaT
    else:
        print("Warning: 'created_at' column not found in the data.")
    
    if 'working_date' in df.columns:
        df['working_date'] = pd.to_datetime(df['working_date'], errors='coerce')  # Convert to datetime, errors to NaT
    else:
        print("Warning: 'working_date' column not found in the data.")
    
    # Handle any problematic data, e.g., `\N` or `#REF!`
    df.replace({'#REF!': None, '\\N': None}, inplace=True)

    return df

# Function to insert data into the transactions table
def insert_data(df):
    # Connect to the PostgreSQL database
    try:
        with psycopg2.connect(DATABASE_URL) as conn:
            with conn.cursor() as cursor:
                for _, row in df.iterrows():
                    print(f"Inserting row with item_id: {row['item_id']}")  # Debugging line
                    
                    try:
                        cursor.execute("""
                            INSERT INTO transactions (
                                item_id, status, created_at, sku, price, qty_ordered, grand_total,
                                increment_id, category_name_1, sales_commission_code, discount_amount,
                                payment_method, working_date, bi_status, mv, year, month,
                                customer_since, m_y, fy, customer_id
                            )
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, (
                            row['item_id'], row['status'], row['created_at'], row['sku'], row['price'], 
                            row['qty_ordered'], row['grand_total'], row['increment_id'], row['category_name_1'],
                            row['sales_commission_code'], row['discount_amount'], row['payment_method'], 
                            row.get('working_date', None),  # Handle missing 'working_date'
                            row['bi_status'], row['mv'], row['year'], row['month'], 
                            row['customer_since'], row['m_y'], row['fy'], row['customer_id']
                        ))
                    except Exception as e:
                        print(f"Error inserting row with item_id: {row['item_id']}: {e}")
                conn.commit()
                print("Data inserted successfully into the transactions table.")
    except Exception as e:
        print(f"Error connecting to the database: {e}")

# Load the transformed data
file_path = 'data/dataset.csv'  # Path to the transformed CSV
df = pd.read_csv(file_path, encoding='ISO-8859-1')

# Preprocess and clean the data
cleaned_df = preprocess_data(df)

# Apply transformation (make sure this function returns a DataFrame with proper column names)
transformed_df = transform_data(cleaned_df)

# Insert the data into the database
insert_data(transformed_df)
