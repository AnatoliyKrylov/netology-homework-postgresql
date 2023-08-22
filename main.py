import os
import psycopg2
from dotenv import load_dotenv


load_dotenv()


def create_db(conn):
    with conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS clients(
            id SERIAL PRIMARY KEY,
            first_name VARCHAR(40) NOT NULL,
            last_name VARCHAR(40) NOT NULL,
            email VARCHAR(60) UNIQUE
        );
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS phones(
            id SERIAL PRIMARY KEY,
            phone BIGINT UNIQUE NOT NULL,
            client_id INTEGER NOT NULL REFERENCES clients(id)
        );
        """)
        conn.commit()


def add_client(conn, first_name, last_name, email, phones=None):
    with conn.cursor() as cur:
        cur.execute("""
        INSERT INTO clients(first_name, last_name, email) VALUES (%s, %s, %s);
        """, (first_name, last_name, email))
        conn.commit()
        if phones:
            cur.execute("""
            SELECT id FROM clients
             WHERE email=%s;
            """, (email,))
            client_id = cur.fetchone()[0]
            for phone in phones:
                cur.execute("""
                    INSERT INTO phones(phone, client_id) VALUES (%s, %s);
                    """, (phone, client_id))
                conn.commit()


def add_phone(conn, client_id, phone):
    with conn.cursor() as cur:
        cur.execute("""
        INSERT INTO phones(phone, client_id) VALUES (%s, %s);
        """, (phone, client_id))
        conn.commit()


def change_client(conn, client_id, first_name=None, last_name=None, email=None, phones=None):
    with conn.cursor() as cur:
        if first_name:
            cur.execute("""
            UPDATE clients SET first_name=%s WHERE id=%s;
            """, (first_name, client_id))
            conn.commit()
        if last_name:
            cur.execute("""
            UPDATE clients SET last_name=%s WHERE id=%s;
            """, (last_name, client_id))
            conn.commit()
        if email:
            cur.execute("""
            UPDATE clients SET email=%s WHERE id=%s;
            """, (email, client_id))
            conn.commit()
        if phones:
            cur.execute("""
            DELETE FROM phones
             WHERE client_id=%s;
            """, (client_id,))
            conn.commit()
            for phone in phones:
                cur.execute("""
                INSERT INTO phones(phone, client_id) VALUES (%s, %s);
                """, (phone, client_id))
                conn.commit()


def delete_phone(conn, client_id, phone):
    with conn.cursor() as cur:
        cur.execute("""
        DELETE FROM phones
         WHERE client_id=%s AND phone=%s;
        """, (client_id, phone))
        conn.commit()


def delete_client(conn, client_id):
    with conn.cursor() as cur:
        cur.execute("""
        DELETE FROM phones
         WHERE client_id=%s;
        """, (client_id,))
        conn.commit()
        cur.execute("""
        DELETE FROM clients
         WHERE id=%s;
        """, (client_id,))


def find_client(conn, first_name=None, last_name=None, email=None, phone=None):
    if first_name is None:
        first_name = '%'
    if last_name is None:
        last_name = '%'
    if email is None:
        email = '%'
    if phone is None:
        with conn.cursor() as cur:
            cur.execute("""
            SELECT first_name, last_name, email, phone FROM clients AS cl
              JOIN phones AS ph ON cl.id = ph.client_id
             WHERE first_name ILIKE %s AND last_name ILIKE %s AND email ILIKE %s; 
            """, (first_name, last_name, email))
            print(cur.fetchall())
    else:
        with conn.cursor() as cur:
            cur.execute("""
            SELECT first_name, last_name, email, phone FROM clients AS cl
              JOIN phones AS ph ON cl.id = ph.client_id
             WHERE first_name ILIKE %s AND last_name ILIKE %s AND email ILIKE %s AND phone = %s; 
            """, (first_name, last_name, email, phone))
            print(cur.fetchall())


with psycopg2.connect(database="clients_db", user="postgres", password=os.getenv("DB_PASSWORD")) as conn:
    create_db(conn)
    add_client(conn, first_name='Alex', last_name='Petrov', email='alex.petrov@mail.ru')
    add_client(conn, first_name='Vasiliy', last_name='Utkin', email='vasiliy.utkin@mail.ru', phones=['89991234567'])
    add_client(conn, first_name='Dmitriy', last_name='Gusev', email='dmitriy.gusev@mail.ru',
               phones=['89111111111', '89222222222'])
    add_phone(conn, client_id='6', phone='89444444444')
    change_client(conn, client_id=5, first_name='Vasya', email='vasya.utkin@mail.ru', phones=['89777777777'])
    change_client(conn, client_id=1, first_name='Alexander', email='alexander.petrov@mail.ru',
                  phones=['89121211212', '89343433434'])
    delete_phone(conn, client_id='6', phone='89444444444')
    delete_client(conn, client_id='1')
    find_client(conn, first_name='Vasiliy')
    find_client(conn, phone='89222222222')

conn.close()
