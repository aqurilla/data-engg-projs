'''
Creating fact and dimension tables for pagila data
'''

# imports
import psycopg2
import time

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
        description VARCHAR(250),
        release_year INT,
        language VARCHAR(50) NOT NULL,
        original_language VARCHAR(50),
        rental_duration INT NOT NULL,
        length INT NOT NULL,
        rating VARCHAR(50) NOT NULL,
        special_features VARCHAR(250) NOT NULL
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


'''
ETL data from 3NF to fact and dimension tables
'''

# Now that the fact and dimension tables are created,
# the next step is to extract data from the normalized db, transform
# and load into the fact and dimension tables

# date table
query = '''
        INSERT INTO dim_date (date_key, date, year, quarter, month, day, week, is_weekend)
        SELECT  DISTINCT(TO_CHAR(payment_date :: DATE, 'yyyyMMDD')::integer) AS date_key,
                date(payment_date) AS date,
                EXTRACT(year FROM payment_date) AS year,
                EXTRACT(quarter FROM payment_date) AS quarter,
                EXTRACT(month FROM payment_date) AS month,
                EXTRACT(day FROM payment_date) AS day,
                EXTRACT(week FROM payment_date) AS week,
                CASE WHEN EXTRACT(ISODOW FROM payment_date) IN (6, 7) THEN true ELSE false END AS is_weekend
        FROM payment;
        '''
cur.execute(query)

query = '''
        SELECT * FROM dim_date
        LIMIT 5;
        '''
cur.execute(query)
print('dim_date data: ', cur.fetchall())


# customer table
query = '''
        INSERT INTO dim_customer (customer_id, first_name, last_name, email, address, address2, district,
        city, country, postal_code, phone, active, create_date, start_date, end_date)
        SELECT  c.customer_id,
                c.first_name,
                c.last_name,
                c.email,
                a.address,
                a.address2,
                a.district,
                city.city,
                country.country,
                a.postal_code,
                a.phone,
                c.active,
                c.create_date,
                now() AS start_date,
                now() AS end_date
        FROM customer c
        JOIN address a ON c.address_id = a.address_id
        JOIN city ON a.city_id = city.city_id
        JOIN country ON city.country_id = country.country_id;
        '''
cur.execute(query)

query = '''
        SELECT * FROM dim_customer
        LIMIT 5;
        '''
cur.execute(query)
print('dim_customer data: ', cur.fetchall())


# store table
query = '''
        INSERT INTO dim_store (store_id, address, address2, district, city, country, postal_code,
        manager_first_name, manager_last_name, start_date, end_date)
        SELECT  s.store_id,
                a.address,
                a.address2,
                a.district,
                city.city,
                country.country,
                a.postal_code,
                st.first_name AS manager_first_name,
                st.last_name AS manager_last_name,
                now() AS start_date,
                now() AS end_date
        FROM store s
        JOIN staff st ON s.manager_staff_id = st.staff_id
        JOIN address a ON s.address_id = a.address_id
        JOIN city ON a.city_id = city.city_id
        JOIN country ON city.country_id = country.country_id;
        '''
cur.execute(query)

query = '''
        SELECT * FROM dim_store
        LIMIT 5;
        '''
cur.execute(query)
print('dim_store data: ', cur.fetchall())


# film table
query = '''
        INSERT INTO dim_film (film_id, title, description, release_year, language, original_language,
        rental_duration, length, rating, special_features)
        SELECT  f.film_id,
                f.title,
                f.description,
                f.release_year,
                l.name AS language,
                ol.name AS original_language,
                f.rental_duration,
                f.length,
                f.rating,
                f.special_features
        FROM film f
        JOIN language l ON f.language_id = l.language_id
        LEFT JOIN language ol ON f.original_language_id = ol.language_id;
        '''
cur.execute(query)

query = '''
        SELECT * FROM dim_film
        LIMIT 5;
        '''
cur.execute(query)
print('dim_film data: ', cur.fetchall())


# sales table
query = '''
        INSERT INTO fact_sales (date_key, customer_key, film_key,
        store_key, sales_amount)
        SELECT  DISTINCT(TO_CHAR(p.payment_date :: DATE, 'yyyyMMDD')::integer) AS date_key,
                p.customer_id AS customer_key,
                i.film_id AS film_key,
                i.store_id AS store_key,
                p.amount AS sales_amount
        FROM payment p
        JOIN rental r ON p.rental_id = r.rental_id
        JOIN inventory i ON r.inventory_id = i.inventory_id;
        '''
cur.execute(query)

query = '''
        SELECT COUNT(*) FROM fact_sales;
        '''
cur.execute(query)
print('fact_sales count: ', cur.fetchall())


'''
Run some quick queries to compare 3NF and star schema tables
'''

# Star schema
query = '''
        SELECT dim_film.title, dim_date.month, dim_customer.city, sum(sales_amount) as revenue
        FROM fact_sales
        JOIN dim_film on (dim_film.film_key = fact_sales.film_key)
        JOIN dim_date on (dim_date.date_key = fact_sales.date_key)
        JOIN dim_customer on (dim_customer.customer_key = fact_sales.customer_key)
        group by (dim_film.title, dim_date.month, dim_customer.city)
        order by dim_film.title, dim_date.month, dim_customer.city, revenue desc;
        '''

start = time.time()
cur.execute(query)
end = time.time()
print('Elapsed time (star schema):', end-start)


# 3NF schema
query = '''
        SELECT f.title, EXTRACT(month FROM p.payment_date) AS month, ci.city, sum(p.amount) AS revenue
        FROM payment p
        JOIN rental r ON ( p.rental_id = r.rental_id )
        JOIN inventory i ON ( r.inventory_id = i.inventory_id )
        JOIN film f ON ( i.film_id = f.film_id)
        JOIN customer c ON ( p.customer_id = c.customer_id )
        JOIN address a ON ( c.address_id = a.address_id )
        JOIN city ci ON ( a.city_id = ci.city_id )
        GROUP BY (f.title, month, ci.city)
        ORDER BY f.title, month, ci.city, revenue desc;
        '''

start = time.time()
cur.execute(query)
end = time.time()
print('Elapsed time (3NF schema):', end-start)
