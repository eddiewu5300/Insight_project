import psycopg2

connection = psycopg2.connect(
    host='127.0.0.1', database='project', user='postgres')

cursor = connection.cursor()

cursor.execute('''INSERT INTO review(user_id, product_id, product_name, review, rating ) \
    VALUES('123', 234,'coffee','THIS coffee is sucks',3)''')
