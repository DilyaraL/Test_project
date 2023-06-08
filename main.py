import re
import requests
import psycopg2


def valid_fullname(user=None):
    if user.get('name'):
        title, first, last = user['name'].values()

        if title or first or last:
            return " ".join((title, first, last))

    return ""


def valid_email(user=None):
    if user.get('email'):
        email = user['email']

        if (email and
                re.match(r"^[a-zA-Z0-9.!#$%&'*+\/=?^_`{|}~-]+@[a-zA-Z0-9!#$&'*+\/=?^_`{|}~-]+\.[-A-Za-z]{2,}$",
                         email) is not None):
            return email

    return ""


def valid_age(user=None):
    if user.get('dob').get('age') and str(user['dob']['age']).isdigit() and 0 <= user['dob']['age'] < 150:
        return user['dob']['age']

    return None


response = requests.get("https://randomuser.me/api/?inc=name,gender,dob,email&nat=us&results=120")
users = response.json()['results']

connection_db = psycopg2.connect(dbname="postgres", user="postgres", password="123456", host="127.0.0.1", port="5432")

count = 0
with connection_db.cursor() as cursor:
    cursor.execute('''DROP TABLE IF EXISTS users;
    CREATE TABLE users 
    (id SERIAL PRIMARY KEY, 
    name VARCHAR(200) NOT NULL, 
    gender VARCHAR(10), 
    age INTEGER, 
    email VARCHAR(200) NOT NULL);''')
    connection_db.commit()

    for user in users:
        fullname = valid_fullname(user)
        gender = user.get('gender')
        age = valid_age(user)
        email = valid_email(user)

        if fullname and email:
            cursor.execute("INSERT INTO users (name, gender, age, email) VALUES (%s, %s, %s, %s)",
                           (fullname, gender, age, email))
            connection_db.commit()
            count += 1

    print(f"Внесено {count} записей в БД")