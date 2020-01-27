# imports
import psycopg2

# Create pagiladb and insert data

# PGPASSWORD=student createdb -h 127.0.0.1 -p 5432 -U student pagila
# PGPASSWORD=student psql -q -h 127.0.0.1 -p 5432 -U student -d pagila -f Data/pagila-schema.sql
# PGPASSWORD=student psql -q -h 127.0.0.1 -p 5432 -U student -d pagila -f Data/pagila-data.sql

# Connect to the pagila db


conn = psycopg2.connect("host=127.0.0.1 dbname=pagila user=student password=student")

cur = conn.cursor()

# The db is in 3NF. Explore the schema
cur.execute("SELECT COUNT(*) FROM store;")
nStores = cur.fetchone()

cur.execute("SELECT COUNT(*) FROM film;")
nFilms = cur.fetchone()

cur.execute("SELECT COUNT(*) FROM customer;")
nCustomers = cur.fetchone()

cur.execute("SELECT COUNT(*) FROM rental;")
nRentals = cur.fetchone()

cur.execute("SELECT COUNT(*) FROM payment;")
nPayments = cur.fetchone()

cur.execute("SELECT COUNT(*) FROM staff;")
nStaff = cur.fetchone()

cur.execute("SELECT COUNT(*) FROM city;")
nCity = cur.fetchone()

cur.execute("SELECT COUNT(*) FROM country;")
nCountry = cur.fetchone()

print("Number of Stores =", nStores[0])
print("Number of Films =", nFilms[0])
print("Number of Customers =", nCustomers[0])
print("Number of Rentals =", nRentals[0])
print("Number of Payments =", nPayments[0])
print("Number of Staff =", nStaff[0])
print("Number of Cities =", nCity[0])
print("Number of Countries =", nCountry[0])

# Time period
query = '''SELECT MIN(payment_date)::date, 
           MAX(payment_date)::date 
           FROM payment;'''
cur.execute(query)
print("Time period \n", cur.fetchall())

# Location
query = '''SELECT district, COUNT(address_id) 
           FROM address 
           GROUP BY district
           ORDER BY 2 DESC
           LIMIT 10;'''
cur.execute(query)
print("Locations \n", cur.fetchall())

# Analysis

# Films
query = '''SELECT film_id, title, release_year, rental_rate, rating 
           FROM film
           LIMIT 5;'''
cur.execute(query)
print("Film data sample \n", cur.fetchall())

# Payments
query = '''SELECT *
           FROM payment
           LIMIT 5;'''
cur.execute(query)
print("Payments data sample \n", cur.fetchall())

# Inventory
query = '''SELECT *
           FROM inventory
           LIMIT 5;'''
cur.execute(query)
print("Inventory data sample \n", cur.fetchall())

# Connecting the above tables: payment -> rental -> inventory -> film 
query = '''SELECT f.film_id, f.title, p.amount, p.payment_date, p.customer_id
           FROM payment p
           JOIN rental r ON p.rental_id = r.rental_id
           JOIN inventory i ON r.inventory_id = i.inventory_id
           JOIN film f ON i.film_id = f.film_id
           LIMIT 1;'''
cur.execute(query)
print("Payment film details \n", cur.fetchall())

# Revenue for each movie
query = '''SELECT f.title, SUM(p.amount)
           FROM payment p
           JOIN rental r ON p.rental_id = r.rental_id
           JOIN inventory i ON r.inventory_id = i.inventory_id
           JOIN film f ON i.film_id = f.film_id
           GROUP BY f.film_id
           ORDER BY 2 DESC
           LIMIT 10;'''
cur.execute(query)
print("Movie amount totals \n", cur.fetchall())

# Cities with most sales: payment -> customer -> address -> city
query = '''SELECT c.city, SUM(p.amount)
           FROM payment p
           JOIN customer cus ON p.customer_id = cus.customer_id
           JOIN address a ON cus.address_id = a.address_id
           JOIN city c ON a.city_id = c.city_id
           GROUP BY c.city_id
           ORDER BY 2 DESC
           LIMIT 10;'''
cur.execute(query)
print("Totals per city \n", cur.fetchall())

# Revenue of movie by customer city and by month

# Total revenue per month
query = '''SELECT EXTRACT(MONTH FROM payment_date), SUM(amount)
           FROM payment
           GROUP BY EXTRACT(MONTH FROM payment_date)
           ORDER BY 2 DESC
           LIMIT 5
           ;'''
cur.execute(query)
print("Total revenue per month \n", cur.fetchall())

# Total amount of revenue for each movie by 
# customer city and by month
query = '''SELECT f.title, c.city, EXTRACT(MONTH FROM p.payment_date) AS month, SUM(p.amount) AS revenue
           FROM payment p
           JOIN rental r ON p.rental_id = r.rental_id
           JOIN inventory i ON r.inventory_id = i.inventory_id
           JOIN film f ON i.film_id = f.film_id
           JOIN customer cus ON p.customer_id = cus.customer_id
           JOIN address a ON cus.address_id = a.address_id
           JOIN city c ON a.city_id = c.city_id
           GROUP BY f.title, c.city_id, month
           ORDER BY month, revenue DESC
           LIMIT 10;'''
cur.execute(query)
print("Totals per city \n", cur.fetchall())
