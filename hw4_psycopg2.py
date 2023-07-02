import psycopg2
from psycopg2.sql import SQL, Identifier
from pprint import pprint

def drop_tables(conn):
    with conn.cursor() as cur:
        cur.execute("""
        DROP TABLE IF EXISTS Phones_client;
        DROP TABLE IF EXISTS Clients
        """)

def create_db(conn):
    with conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS Clients(
            id_client SERIAL PRIMARY KEY,
            name VARCHAR(40) NOT NULL,
            surname VARCHAR(40) NOT NULL,
            email VARCHAR(40) NOT NULL
        );
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS Phones_client(
            id SERIAL PRIMARY KEY,
            id_client SERIAL NOT NULL REFERENCES Clients(id_client),
            phones VARCHAR(40) NOT NULL
        );
        """)
        conn.commit()

def add_client(conn, name, surname, email, *phones):
    with conn.cursor() as cur:
        cur.execute("""
        INSERT INTO Clients(name, surname, email) VALUES(%s, %s, %s) RETURNING id_client;
        """,(name, surname, email))
        curr_client_id = cur.fetchone()[0]

        if phones:
            for phone in phones:
                cur.execute("""
                INSERT INTO Phones_client(id_client, phones) VALUES(%s, %s);
                """,(curr_client_id, phone))          
        conn.commit()

def add_phone(conn, id_client, *phones):
    with conn.cursor() as cur:
        if phones:
            for phone in phones:
                cur.execute("""
                INSERT INTO Phones_client(id_client, phones) VALUES(%s, %s);
                """,(id_client, phone))
        conn.commit()


def change_client(conn, id_client, *phones, name=None, surname=None, email=None):
    with conn.cursor() as cur:
        cur.execute("""
        SELECT name, surname, email FROM Clients 
        WHERE id_client=%s;
        """, (id_client,))
        client_data = list(cur.fetchone())
        if name is None:
            name = client_data[0]
        if surname is None:
            surname = client_data[1]
        if email is None:
            email = client_data[2]
        if phones:
            cur.execute("""
            DELETE FROM Phones_client WHERE id_client=%s;
            """, (id_client,))
            for phone in phones:
                cur.execute("""
                INSERT INTO Phones_client(id_client, phones) VALUES(%s, %s);
                """,(id_client, phone))
        cur.execute("""
        UPDATE Clients SET name=%s, surname=%s, email=%s WHERE id_client=%s;
        """, (name, surname, email, id_client))
        conn.commit()

def delete_phone(conn, id_client, *phones):
    with conn.cursor() as cur:
        if id_client in id_list(cur):           
            for phone in phones:
                cur.execute("""
                DELETE FROM Phones_client WHERE id_client=%s AND phones=%s;
                """, (id_client, phone))
        else:
            pprint("The client does not exist. Data base without changes.")
        conn.commit()

def delete_client(conn, id_client):
    with conn.cursor() as cur:
        if id_client in id_list(cur):
            cur.execute("""
            DELETE FROM Phones_client WHERE id_client=%s;
            """, (id_client,))
            cur.execute("""
            DELETE FROM Clients WHERE id_client=%s;
            """, (id_client,))         
        else:
            pprint("The client does not exist. Data base without changes.")
        conn.commit()

def find_client(conn, **client_data):
    keys = [Identifier(i) for i in client_data.keys()]
    values = tuple([i for i in client_data.values()])
    with conn.cursor() as cur:
        if len(client_data) == 1:
            cur.execute(SQL("""
            SELECT c.id_client, c.name n, c.surname, c.email, pc.phones FROM Clients c
            LEFT JOIN Phones_client pc
            USING(id_client)
            WHERE {}=%s""").format(*keys), values)
            pprint(cur.fetchall())
        elif len(client_data) == 2:
            cur.execute(SQL("""
            SELECT c.id_client, c.name n, c.surname, c.email, pc.phones FROM Clients c
            LEFT JOIN Phones_client pc
            USING(id_client)
            WHERE {}=%s AND {}=%s""").format(*keys), values)
            pprint(cur.fetchall())
        elif len(client_data) == 3:
            cur.execute(SQL("""
            SELECT c.id_client, c.name n, c.surname, c.email, pc.phones FROM Clients c
            LEFT JOIN Phones_client pc
            USING(id_client)
            WHERE {}=%s AND {}=%s AND {}=%s""").format(*keys), values)
            pprint(cur.fetchall())
        elif len(client_data) == 4:
            cur.execute(SQL("""
            SELECT c.id_client, c.name n, c.surname, c.email, pc.phones FROM Clients c
            LEFT JOIN Phones_client pc
            USING(id_client)
            WHERE {}=%s AND {}=%s AND {}=%s AND {}=%s""").format(*keys), values)
            pprint(cur.fetchall())
        else:
            pprint("Incorrect request")


def id_list(cur):
    cur.execute("""
    SELECT id_client FROM Clients
    """)
    return str([id[0] for id in cur.fetchall()])

def state_db(conn):
    with conn.cursor() as cur:
        cur.execute("""
        SELECT c.id_client, c.name, c.surname, c.email, pc.phones FROM Clients c
        LEFT JOIN Phones_client pc
        USING(id_client);
        """)
        pprint(cur.fetchall())

with psycopg2.connect(database="clients_db", user=input("user: "), password=input("password: ")) as conn:
    drop_tables(conn)

    # 1. Тест create_db. Функция, создающая структуру БД (таблицы).
    create_db(conn)

    # 2. Тест add_client. Функция, позволяющая добавить нового клиента.
    print("Add 4 clients:")
    add_client(conn, "Ivan", "Ivanov", "iivanov@mail.ru")
    add_client(conn, "Anna", "Petrova", "Petrova_a@mail.ru", "2201", "2202")
    add_client(conn, "Ivan", "Nikolaev", "nik@mail.ru", "3301")
    add_client(conn, "Elena", "Ivanova", "Ei@mail.ru")
    state_db(conn)

    # 3. Тест add_phone. Функция, позволяющая добавить телефон для существующего клиента.
    print("\nAdd phone to id_client = 1:")
    add_phone(conn, "1", "1111")
    state_db(conn)

    # 4. Тест change_client. Функция, позволяющая изменить данные о клиенте.
    print("\nChange id_client = 2 (surname, email):")
    change_client(conn, "2", surname="Ivanova", email="Ivanova_a@mail.ru")
    state_db(conn)
    print("Change id_client = 2 (phones, name):")
    change_client(conn, "2", "2211","2212", name="Ann")
    state_db(conn)

    # 5. Тест delete_phone. Функция, позволяющая удалить телефон для существующего клиента.
    print("\nDelete phone to id_client = 2:")
    delete_phone(conn, "2", "2211")
    state_db(conn)

    # 6. Тест delete_client. Функция, позволяющая удалить существующего клиента.
    print("\nDelete client id_client = 4:")
    delete_client(conn, "4")
    state_db(conn)

    # 7. Тест find_client. Функция, позволяющая найти клиента по его данным: имени, фамилии, email или телефону.
    print("\nFind client (Ivan):")
    find_client(conn, name="Ivan")

    print("\nFind client (Ivanov):")
    find_client(conn, surname="Ivanov")

    print("\nFind client (nik@mail.ru):")
    find_client(conn, email="nik@mail.ru")

    print("\nFind client (2212):")
    find_client(conn, phones = "2212")

    print("\nFind client (Ivanov, iivanov@mail.ru):")
    find_client(conn, surname="Ivanov", email = "iivanov@mail.ru")

    print("\nFind client (Nikolay, nik@mail.ru):")
    find_client(conn, name="Nikolay", email = "nik@mail.ru")

    print("\nFind client (nik@mail.ru, 3301, Ivan):")
    find_client(conn, email="nik@mail.ru", phones="3301", name="Ivan")

    print("\nFind client (Ivanova_a@mail.ru, Ivanova, 2212, Alina):")
    find_client(conn, email="Ivanova_a@mail.ru", surname="Ivanova", phones="2212", name="Alina")

    print("\nFind client (Ivanova_a@mail.ru, Ivanova, 2212, Ann):")
    find_client(conn, email="Ivanova_a@mail.ru", surname="Ivanova", phones="2212", name="Ann")