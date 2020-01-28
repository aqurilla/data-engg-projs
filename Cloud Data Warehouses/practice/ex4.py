'''
OLAP Cubes
'''

# imports
import psycopg2

# Connect to the pagila db
conn = psycopg2.connect("host=127.0.0.1 dbname=pagila user=student password=student")

cur = conn.cursor()

conn.set_session(autocommit=True)

# Calculate revenue by day, rating and city
query = '''
        SELECT dates.day AS day, films.rating AS rating, customers.city AS city, SUM(f.sales_amount) AS revenue
        FROM fact_sales f
        JOIN dim_date dates ON dates.date_key = f.date_key
        JOIN dim_film films ON films.film_key = f.film_key
        JOIN dim_customer customers ON customers.customer_key = f.customer_key
        GROUP BY dates.day, films.rating, customers.city
        ORDER BY 4 DESC
        LIMIT 10;
        '''

cur.execute(query)

print('\nTotal revenue by day, rating and city:', cur.fetchall())

'''
# Slicing - reduction of dimensionality of cube by 1
'''

# evenue summary for movies with rating 'G'
query = '''
        SELECT dates.day AS day, films.rating AS rating, customers.city AS city, SUM(f.sales_amount) AS revenue
        FROM fact_sales f
        JOIN dim_date dates ON dates.date_key = f.date_key
        JOIN dim_film films ON films.film_key = f.film_key
        JOIN dim_customer customers ON customers.customer_key = f.customer_key
        WHERE films.rating = 'G'
        GROUP BY dates.day, films.rating, customers.city
        ORDER BY 4 DESC
        LIMIT 20;
        '''

cur.execute(query)

print('\nSlicing (movies with rating "G"):', cur.fetchall())

'''
Dicing - creating a subcube with same dimensionality but fewer values in 2 or more dimensions
'''

query = '''
        SELECT dates.day AS day, films.rating AS rating, customers.city AS city, SUM(f.sales_amount) AS revenue
        FROM fact_sales f
        JOIN dim_date dates ON dates.date_key = f.date_key
        JOIN dim_film films ON films.film_key = f.film_key
        JOIN dim_customer customers ON customers.customer_key = f.customer_key
        WHERE films.rating IN ('PG', 'PG-13')
        AND city IN ('Bellevue', 'Lancaster')
        AND day IN (1, 15, 30)
        GROUP BY dates.day, films.rating, customers.city
        ORDER BY 4 DESC
        LIMIT 5;
        '''

cur.execute(query)

print('\nDicing output:', cur.fetchall())

'''
Roll-up - promoting the level of aggregation
e.g. city -> country
'''

# Query that calculates revenue (sales_amount) by day, rating, and country

query = '''
        SELECT dates.day AS day, films.rating AS rating, customers.country AS country, SUM(f.sales_amount) AS revenue
        FROM fact_sales f
        JOIN dim_date dates ON dates.date_key = f.date_key
        JOIN dim_film films ON films.film_key = f.film_key
        JOIN dim_customer customers ON customers.customer_key = f.customer_key
        GROUP BY dates.day, films.rating, customers.country
        ORDER BY 4 DESC
        LIMIT 5;
        '''

cur.execute(query)

print('\nRoll-up output:', cur.fetchall())

'''
Drill-down - bringing a dimension to a lower level
e.g. city -> district
'''

# Query that calculates revenue (sales_amount) by day, rating, and district

query = '''
        SELECT dates.day AS day, films.rating AS rating, customers.district AS district, SUM(f.sales_amount) AS revenue
        FROM fact_sales f
        JOIN dim_date dates ON dates.date_key = f.date_key
        JOIN dim_film films ON films.film_key = f.film_key
        JOIN dim_customer customers ON customers.customer_key = f.customer_key
        GROUP BY dates.day, films.rating, customers.district
        ORDER BY 4 DESC
        LIMIT 5;
        '''

cur.execute(query)

print('\nDrill-down output:', cur.fetchall())

'''
Grouping Sets - one shot grouping accross various group combinations
'''
# Total revenue
query = '''
        SELECT SUM(f.sales_amount) AS revenue
        FROM fact_sales f
        LIMIT 5;
        '''

cur.execute(query)

print('\nQ1:', cur.fetchall())

# Total revenue by country
query = '''
        SELECT customers.country AS country, SUM(f.sales_amount) AS revenue
        FROM fact_sales f
        JOIN dim_customer customers ON customers.customer_key = f.customer_key
        GROUP BY customers.country
        ORDER BY revenue DESC
        LIMIT 5;
        '''

cur.execute(query)

print('\nQ2:', cur.fetchall())

# Total revenue by month
query = '''
        SELECT dates.month AS month, SUM(f.sales_amount) AS revenue
        FROM fact_sales f
        JOIN dim_customer customers ON customers.customer_key = f.customer_key
        JOIN dim_date dates ON dates.date_key = f.date_key
        GROUP BY dates.month
        ORDER BY revenue DESC
        LIMIT 5;
        '''

cur.execute(query)

print('\nQ3:', cur.fetchall())

# Total revenue by month and country
query = '''
        SELECT dates.month AS month, customers.country AS country, SUM(f.sales_amount) AS revenue
        FROM fact_sales f
        JOIN dim_customer customers ON customers.customer_key = f.customer_key
        JOIN dim_date dates ON dates.date_key = f.date_key
        GROUP BY dates.month, customers.country
        ORDER BY revenue DESC
        LIMIT 5;
        '''

cur.execute(query)

print('\nQ4:', cur.fetchall())


# All above queries together
query = '''
        SELECT dates.month AS month, customers.country AS country, SUM(f.sales_amount) AS revenue
        FROM fact_sales f
        JOIN dim_customer customers ON customers.customer_key = f.customer_key
        JOIN dim_date dates ON dates.date_key = f.date_key
        GROUP BY grouping sets ((), dates.month, customers.country, (dates.month, customers.country))
        LIMIT 20;
        '''

cur.execute(query)

print('\nGrouping Sets:', cur.fetchall())

# Using CUBE produces all combinations in 1 go
query = '''
        SELECT dates.month AS month, customers.country AS country, SUM(f.sales_amount) AS revenue
        FROM fact_sales f
        JOIN dim_customer customers ON customers.customer_key = f.customer_key
        JOIN dim_date dates ON dates.date_key = f.date_key
        GROUP BY CUBE(dates.month, customers.country)
        LIMIT 20;
        '''

cur.execute(query)

print('\nCUBE:', cur.fetchall())
