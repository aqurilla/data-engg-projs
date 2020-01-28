'''
Creating fact and dimension tables for pagila data
'''

# imports
import psycopg2

# Connect to the pagila db
conn = psycopg2.connect("host=127.0.0.1 dbname=pagila user=student password=student")

cur = conn.cursor()

# We develop a star schema for the pagila data,
# with 1 fact table representing sales and 4 
# dimension tables for date, customer, film and store


# date table
query = '''
        CREATE TABLE IF NOT EXISTS dim_date 
        (
        date_key SERIAL PRIMARY KEY,
        date DATE NOT NULL,
        year SMALLINT NOT NULL,
        quarter SMALLINT NOT NULL,
        month SMALLINT NOT NULL,
        day SMALLINT NOT NULL,
        week SMALLINT NOT NULL,
        is_weekend BOOLEAN NOT NULL
        );
        '''
cur.execute(query)

query = '''
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = 'dim_date';
        '''
cur.execute(query)
print('dim_date info: ', cur.fetchall())


# customer table
query = '''
        CREATE TABLE IF NOT EXISTS dim_customer 
        (
        customer_key SERIAL PRIMARY KEY,
        customer_id INT NOT NULL,
        first_name VARCHAR(50) NOT NULL,
        last_name VARCHAR(50) NOT NULL,
        email VARCHAR(50),
        address VARCHAR(50) NOT NULL,
        address2 VARCHAR(50),
        district VARCHAR(50) NOT NULL,
        city VARCHAR(50) NOT NULL,
        country VARCHAR(50) NOT NULL,
        postal_code VARCHAR(50),
        phone VARCHAR(50) NOT NULL,
        active INT NOT NULL,
        create_date TIMESTAMP NOT NULL,
        start_date DATE NOT NULL,
        end_date DATE NOT NULL
        );
        '''
cur.execute(query)

query = '''
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = 'dim_customer';
        '''
cur.execute(query)
print('dim_customer info: ', cur.fetchall())

# film table
query = '''
        CREATE TABLE IF NOT EXISTS dim_film
        (
        film_key SERIAL PRIMARY KEY,
        film_id INT NOT NULL,
        title VARCHAR(50) NOT NULL,
        description VARCHAR(100),
        release_year INT,
        language VARCHAR(50) NOT NULL,
        original_language VARCHAR(50),
        rental_duration INT NOT NULL,
        length INT NOT NULL,
        rating VARCHAR(50) NOT NULL,
        special_features VARCHAR(100) NOT NULL
        );
        '''
cur.execute(query)

query = '''
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = 'dim_film';
        '''
cur.execute(query)
print('dim_film info: ', cur.fetchall())


# store table
query = '''
        CREATE TABLE IF NOT EXISTS dim_store
        (
        store_key SERIAL PRIMARY KEY,
        store_id INT NOT NULL,
        address VARCHAR(50) NOT NULL,
        address2 VARCHAR(50),
        district VARCHAR(50) NOT NULL,
        city VARCHAR(50) NOT NULL,
        country VARCHAR(50) NOT NULL,
        postal_code VARCHAR(50),
        manager_first_name VARCHAR(50) NOT NULL,
        manager_last_name VARCHAR(50) NOT NULL,
        start_date DATE NOT NULL,
        end_date DATE NOT NULL
        );
        '''
cur.execute(query)

query = '''
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = 'dim_store';
        '''
cur.execute(query)
print('dim_store info: ', cur.fetchall())


# Sales fact table
query = '''
        CREATE TABLE IF NOT EXISTS fact_sales
        (
        sales_key SERIAL PRIMARY KEY,
        date_key INT REFERENCES dim_date(date_key),
        customer_key INT REFERENCES dim_customer(customer_key),
        film_key INT REFERENCES dim_film(film_key),
        store_key INT REFERENCES dim_store(store_key),
        sales_amount NUMERIC NOT NULL
        );
        '''
cur.execute(query)

query = '''
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = 'fact_sales';
        '''
cur.execute(query)
print('fact_sales info: ', cur.fetchall())