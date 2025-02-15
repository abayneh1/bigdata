import psycopg2

# Database connection details
DATABASE_URL = "postgresql://postgres:abay347@localhost/sales_data"  # Modify with actual credentials

# Function to read SQL query from the .sql file
def read_sql_file(filename):
    with open(filename, 'r') as file:
        return file.read()

# Function to create the table using raw SQL from the .sql file
def create_table_from_sql():
    create_table_sql = read_sql_file('src/cretetable.sql')
    
    # Connect to the PostgreSQL database
    with psycopg2.connect(DATABASE_URL) as conn:
        with conn.cursor() as cursor:
            cursor.execute(create_table_sql)
            conn.commit()
            print("Table created successfully using SQL from .sql file.")

# Run the function to create the table
create_table_from_sql()
