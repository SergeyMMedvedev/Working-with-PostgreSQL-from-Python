import psycopg2
import re
from pprint import pprint


def chack_email(email):
    regex = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
    return re.fullmatch(regex, email)


def check_phone(phone):
    return phone.isdigit() and (7 <= len(phone) <= 11)


def get_not_none_keys(kwargs):
    return {
        key: val for key, val in kwargs.items()
        if val is not None
    }


def create_clients(conn):
    with conn.cursor() as cur:
        sql = '''
        CREATE TABLE IF NOT EXISTS clients(
            id SERIAL PRIMARY KEY,
            first_name VARCHAR(40) NOT NULL,
            last_name VARCHAR(40) NOT NULL,
            email  VARCHAR(40) UNIQUE NOT NULL,
            CONSTRAINT UC_full_name UNIQUE (first_name, last_name)
        );
        '''
        cur.execute(sql, conn)


def create_phones(conn):
    with conn.cursor() as cur:
        sql = '''
        CREATE TABLE IF NOT EXISTS phones(
            id SERIAL PRIMARY KEY,
            phone VARCHAR(13) CHECK (LENGTH(phone) BETWEEN 7 AND 13) NULL,
            client_id INTEGER REFERENCES clients(id) ON DELETE CASCADE,
            CONSTRAINT UC_Phone UNIQUE (phone, client_id)
        );
        '''
        cur.execute(sql, conn)


def drop_tables(conn):
    with conn.cursor() as cur:
        cur.execute('''
        DROP TABLE IF EXISTS phones;
        DROP TABLE IF EXISTS clients;
        ''')


def create_db(conn):
    create_clients(conn)
    create_phones(conn)


def insert_into_table(conn, table_name, **kwargs):
    with conn.cursor() as cur:
        sql = 'INSERT INTO %s' % table_name
        sql += '(' + ', '.join(kwargs.keys()) + ') '
        sql += 'VALUES(' + ', '.join('%s' for _ in range(len(kwargs))) + ') '
        sql += 'RETURNING id, ' + ', '.join(kwargs.keys()) + ';'
        table_name = 'clients'
        cur.execute(sql, (*kwargs.values(), ))
        line = cur.fetchone()
        return line


def add_client(conn, first_name, last_name, email, phones=None):
    if not chack_email(email):
        raise Exception('Invalid email!')
    client = insert_into_table(
        conn,
        table_name='clients',
        first_name=first_name,
        last_name=last_name,
        email=email
    )
    print('Client created:', client)
    if phones:
        for phone in phones:
            add_phone(conn, client[0], phone)


def add_phone(conn, client_id, phone):
    if not check_phone(phone):
        raise Exception('Invalid phone!')
    phone = insert_into_table(
        conn,
        table_name='phones',
        client_id=client_id,
        phone=phone
    )
    print('Phone added:', phone)


def update_table(conn, table_name, id, **kwargs):
    args = get_not_none_keys(kwargs)
    with conn.cursor() as cur:
        sql = 'UPDATE %s' % table_name
        sql += ' SET ' + ', '.join(key + '=%s' for key in args)
        sql += ' WHERE id=%s'
        cur.execute(sql, (*args.values(), id))


def change_client(
    conn, client_id, first_name=None, last_name=None, email=None, phones=None
):
    if not chack_email(email):
        raise Exception('Invalid email!')
    update_table(
        conn,
        table_name='clients',
        id=client_id,
        first_name=first_name,
        last_name=last_name,
        email=email
    )
    if phones is not None:
        change_client_phones(conn, client_id, phones)


def delete_from_table(conn, table_name, **kwargs):
    with conn.cursor() as cur:
        args = get_not_none_keys(kwargs)
        sql = 'DELETE FROM %s' % table_name
        sql += ' WHERE ' + 'AND '.join(key + '=%s' for key in args)
        cur.execute(sql, (*args.values(), ))


def change_client_phones(conn, client_id, phones):
    delete_from_table(conn, table_name='phones', client_id=client_id)
    for phone in phones:
        add_phone(conn, client_id, phone)


def delete_phone(conn, client_id, phone):
    delete_from_table(conn, table_name='phones',
                      client_id=client_id, phone=phone)


def delete_client(conn, client_id):
    delete_from_table(conn, table_name='clients', id=client_id)


def select_from_table(conn, fields='*', **kwargs):
    with conn.cursor() as cur:
        args = get_not_none_keys(kwargs)
        sql = 'SELECT %s FROM clients c' % fields
        sql += ' LEFT JOIN phones p ON p.client_id = c.id '
        if args:
            sql += ' WHERE ''' + ', '.join(key + '=%s' for key in args)
            cur.execute(sql, (*args.values(), ))
        else:
            cur.execute(sql)
        lines = cur.fetchall()
        return lines


def find_client(conn, first_name=None, last_name=None, email=None, phone=None):
    lines = select_from_table(
        conn,
        fields='first_name, last_name, email, phone',
        first_name=first_name,
        last_name=last_name,
        email=email,
        phone=phone
    )
    return lines


with psycopg2.connect(
    database='netology-db', user='postgres', password='postgres'
) as conn:
    drop_tables(conn)

    print('\n1. Функция, создающая структуру БД (таблицы):')
    create_db(conn)

    print('\n2. Функция, позволяющая добавить нового клиента:')
    add_client(conn, 'Сергей', 'Медведев', 'asd@asd.com')
    phones = ['88005553555']
    add_client(conn, 'Стас', 'Басов', 'asd@asd2.com', phones)
    phones += ['88005553556']
    add_client(conn, 'Иван', 'Иванов', 'asd@asd3.com', phones)
    pprint(select_from_table(
        conn, fields='c.id, first_name, last_name, email, phone'
    ))

    print(
        '\n3. Функция, позволяющая добавить телефон для существующего клиента:'
    )
    add_phone(conn, client_id=1, phone='88005553557')
    pprint(select_from_table(
        conn, fields='c.id, first_name, last_name, email, phone'
    ))

    print('\n4. Функция, позволяющая изменить данные о клиенте:')
    change_client(
        conn, 1,
        first_name='Sergey',
        email='qwe@qwe.com',
        phones=['12323121323', '77777777777']
    )
    pprint(select_from_table(
        conn, fields='c.id, first_name, last_name, email, phone'
    ))

    print(
        '\n5. Функция, позволяющая удалить телефон для существующего клиента:'
    )
    delete_phone(conn, 1, '77777777777')
    pprint(select_from_table(
        conn, fields='c.id, first_name, last_name, email, phone'
    ))

    print('\n6. Функция, позволяющая удалить существующего клиента:')
    delete_client(conn, 3)
    pprint(select_from_table(
        conn, fields='c.id, first_name, last_name, email, phone'
    ))

    print('\n7. Функция, позволяющая найти клиента по его данным:')
    pprint(find_client(conn, phone='12323121323'))


conn.close()
