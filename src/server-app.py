from fastapi import FastAPI
import hashlib
import mysql.connector
import uuid
import os


import db_info


class Database(object):
    def __init__(self, db_config: dict):
        self.config = db_config
        self.__connection__ = None
        self.__results__ = None
        self.connect()

    def connect(self):
        self.__connection__ = mysql.connector.connect(**self.config)
        self.__connection__.autocommit = True
        return self

    def run_sql(self, sql: str, n: int = 0):
        self.__results__ = None
        try:
            cursor = self.__connection__.cursor()
            cursor.execute(sql)
            self.__results__ = cursor.fetchall()
        except mysql.connector.OperationalError:
            if n <= 10:
                self.connect()
                self.run_sql(sql, n + 1)
            else:
                raise Exception('failed to execute sql.')
        finally:
            return self

    @property
    def db(self):
        return self.__connection__

    @property
    def data(self):
        return self.__results__

    @property
    def one_row(self):
        if self.__results__:
            return self.__results__[0]
        else:
            return None


config = {
    'host': db_info.DB_HOST,
    'port': db_info.DB_PORT,
    'user': db_info.DB_USER,
    'passwd': db_info.DB_PASSWORD,
    'database': db_info.DB_DATABASE,
    'charset': db_info.DB_CHAR_SET
}


db = Database(config)
app = FastAPI()


@app.get('/')
async def get_server_info():
    return {
        'version': 'v0.1'
    }


@app.post('/users/signup/{signup_token}')
async def signup(signup_token: str):
    username, password = signup_token.split(':')
    hashed_password = hashlib.sha512(password.encode('utf-8')).hexdigest()
    sql = f"""
        SELECT username
        FROM users
        WHERE username = '{username}'
    """
    data = db.run_sql(sql).one_row
    if data:
        return {
            'status': 'failed',
            'message': 'the user is already existed.'
        }
    sql = f"""
        INSERT INTO users
        (username, password)
        VALUES
        ('{username}', '{hashed_password}')
    """
    db.run_sql(sql)
    return {
        'status': 'ok',
        'username': username
    }


@app.post('/users/login/{login_token}')
async def login(login_token: str):
    username, password = login_token.split(':')
    hashed_password = hashlib.sha512(password.encode('utf-8')).hexdigest()
    sql = f"""
        SELECT *
        FROM users
        WHERE
            username = '{username}' AND 
            password = '{hashed_password}'
    """
    data = db.run_sql(sql).one_row
    if not data:
        return {
            'status': 'failed',
            'message': 'wrong username or password'
        }
    sql = f"""
        SELECT COUNT(id) AS `sessions_count`
        FROM tokens
        WHERE
            username = '{username}' AND
            expire > NOW() 
    """
    data = db.run_sql(sql).one_row
    if data:
        sessions_count = data[0]
        if sessions_count > 5:
            return {
                'status': 'failed',
                'message': 'too many sessions.'
            }
    token = str(uuid.uuid4()).replace('-', '')
    sql = f"""
        INSERT INTO tokens
        (username, token, expire)
        VALUES
        ('{username}', '{token}', DATE_ADD(NOW(), INTERVAL 1 DAY))
    """
    db.run_sql(sql)
    return {
        'status': 'ok',
        'username': username,
        'user_id': data[0],
        'token': token
    }


def check_token(token: str):
    sql = f"""
        SELECT *
        FROM tokens
        WHERE
            token = '{token}' AND 
            expire > NOW()
    """
    return db.run_sql(sql).one_row


@app.post('/users/change_pwd')
async def change_password(token: str, new_password: str):
    data = check_token(token)
    if not data:
        return {
            'status': 'failed',
            'message': 'invalid token.'
        }
    user_id, username, _, expire = data
    hashed_password = hashlib.sha512(new_password.encode('utf-8')).hexdigest()
    sql = f"""
        UPDATE users
        SET password = '{hashed_password}'
        WHERE
            id = {user_id}
    """
    db.run_sql(sql)
    return {
        'status': 'ok'
    }


@app.post('/users/logout')
async def logout(token: str):
    data = check_token(token)
    if not data:
        return {
            'status': 'failed',
            'message': 'invalid token.'
        }
    user_id, username, _, expire = data
    sql = f"""
        UPDATE tokens
        SET expire = DATE_SUB(NOW(), INTERVAL 999 DAY)
        WHERE username = '{username}'
    """
    db.run_sql(sql)
    return {
        'status': 'ok'
    }


@app.get('/notebooks')
async def get_notebooks(token: str):
    data = check_token(token)
    if not data:
        return {
            'status': 'failed',
            'message': 'invalid token.'
        }
    user_id, username, _, expire = data
    sql = f"""
        SELECT id, notebook, owner, rate, access_key, shared
        FROM notebooks
        WHERE
            owner = '{username}' AND 
            deleted = FALSE
    """
    data = db.run_sql(sql).data
    records = []
    for column in data:
        notebook_id, notebook_name, owner, rate, access_key, shared = column
        records.append({
            'id': notebook_id,
            'notebook': notebook_name,
            'owner': owner,
            'rate': rate,
            'access_key': access_key,
            'shared': bool(shared)
        })
    return {
        'status': 'ok',
        'username': username,
        'user_id': user_id,
        'records': records,
        'expire': str(expire)
    }


@app.post('/notebooks/{notebook_name}')
async def create_notebook(
        notebook_name: str,
        token: str,
        access_key: str = '',
        shared: bool = False):
    data = check_token(token)
    if not data:
        return {
            'status': 'failed',
            'message': 'invalid token.'
        }
    user_id, username, _, expire = data
    sql = f"""
        SELECT *
        FROM notebooks
        WHERE
            notebook = '{notebook_name}' AND 
            deleted = FALSE
    """
    data = db.run_sql(sql).one_row
    if data:
        return {
            'status': 'failed',
            'message': 'the notebook is already existed.'
        }
    sql = f"""
        INSERT INTO notebooks
        (notebook, owner, rate, access_key, shared)
        VALUES 
        ('{notebook_name}', '{username}', 0, '{access_key}', {shared})
    """
    db.run_sql(sql)
    return {
        'status': 'ok',
        'username': username,
        'user_id': user_id,
        'notebook': notebook_name,
        'expire': str(expire)
    }


@app.put('/notebooks/{notebook_name}')
async def edit_notebook(
        notebook_name: str,
        token: str,
        new_name: str = None,
        rate: float = None,
        access_key: str = None,
        shared: bool = None):
    data = check_token(token)
    if not data:
        return {
            'status': 'failed',
            'message': 'invalid token.'
        }
    user_id, username, _, expire = data
    sql = f"""
        SELECT id, notebook, rate, access_key, shared
        FROM notebooks
        WHERE
            owner = '{username}' AND 
            deleted = FALSE AND
            notebook = '{notebook_name}'
    """
    data = db.run_sql(sql).one_row
    if not data:
        return {
            'status': 'failed',
            'message': 'no such notebook.'
        }
    notebook_id, _, original_rate, original_access_key, original_shared_status = data
    sql = f"""
        SELECT id, notebook
        FROM notebooks
        WHERE
            notebook = '{new_name}' AND
            deleted = FALSE
    """
    data = db.run_sql(sql).one_row
    if data:
        return {
            'status': 'failed',
            'message': 'the notebook has already existed.'
        }
    sql = f"""
        UPDATE notebooks
        SET
            notebook = '{new_name or notebook_name}',
            rate = {rate or original_rate},
            access_key = '{access_key or original_access_key}',
            shared = {shared if shared is not None else original_shared_status}
        WHERE
            id = '{notebook_id}' AND
            owner = '{username}' AND
            deleted = FALSE
    """
    db.run_sql(sql)
    return {
        'status': 'ok',
        'username': username,
        'user_id': user_id,
        'notebook': new_name or notebook_name,
        'expire': str(expire)
    }


@app.delete('/notebooks/{notebook_name}')
async def delete_notebook(notebook_name: str, token: str, access_key: str = ''):
    data = check_token(token)
    if not data:
        return {
            'status': 'failed',
            'message': 'invalid token.'
        }
    user_id, username, _, expire = data
    data = check_notebook(notebook_name)
    if not data:
        return {
            'status': 'failed',
            'message': 'no such notebook.'
        }
    notebook_id, _, owner, shared, key = data
    if owner != username:
        return {
            'status': 'failed',
            'message': 'access denied.'
        }
    elif access_key != key:
        return {
            'status': 'failed',
            'message': 'access denied.'
        }
    sql = f"""
        UPDATE notebooks
        SET deleted = TRUE
        WHERE
            id = {notebook_id} AND
            owner='{username}'
    """
    db.run_sql(sql)
    return {
        'status': 'ok'
    }


@app.get('/notebooks/shared')
async def get_shared_notebooks(token: str):
    data = check_token(token)
    if not data:
        return {
            'status': 'failed',
            'message': 'invalid token.'
        }
    user_id, username, _, expire = data
    sql = f"""
        SELECT id, notebook, owner, rate
        FROM notebooks
        WHERE
            shared = {True} AND 
            deleted = FALSE
    """
    data = db.run_sql(sql).data
    records = []
    for column in data:
        notebook_id, notebook_name, owner, rate = column
        records.append({
            'id': notebook_id,
            'notebook': notebook_name,
            'owner': owner,
            'rate': rate,
            'shared': True
        })
    return {
        'status': 'ok',
        'username': username,
        'user_id': user_id,
        'records': records,
        'expire': str(expire)
    }


def check_notebook(notebook_name: str):
    sql = f"""
        SELECT id, notebook, owner, shared, access_key
        FROM notebooks
        WHERE
            notebook = '{notebook_name}' AND
            deleted = FALSE
    """
    return db.run_sql(sql).one_row


@app.get('/notebooks/{notebook_name}')
async def get_notes(
        notebook_name: str,
        token: str,
        access_key: str = ''):
    data = check_token(token)
    if not data:
        return {
            'status': 'failed',
            'message': 'invalid token.'
        }
    user_id, username, _, expire = data
    data = check_notebook(notebook_name)
    if not data:
        return {
            'status': 'failed',
            'message': 'no such notebook.'
        }
    notebook_id, _, owner, shared, key = data
    if owner != username and not shared:
        return {
            'status': 'failed',
            'message': 'access denied.'
        }
    elif access_key != key:
        return {
            'status': 'failed',
            'message': 'access denied.'
        }
    sql = f"""
        SELECT id, title, owner, access_key, path, tags, `read_only`
        FROM notes
        WHERE
            notebook_id = '{notebook_id}' AND
            deleted = FALSE
    """
    data = db.run_sql(sql).data
    records = []
    for column in data:
        note_id, title, owner, key, path, tags, read_only = column
        records.append({
            'id': note_id,
            'title': title,
            'owner': owner,
            'key': key,
            'path': path,
            'tags': [tag.strip() for tag in tags.split(',')],
            'read_only': bool(read_only)
        })
    return {
        'status': 'ok',
        'username': username,
        'user_id': user_id,
        'records': records,
        'expire': str(expire)
    }


@app.post('/notebooks/{notebook_name}/{note_key}')
async def upload_note(
        notebook_name: str,
        note_key: str,
        token: str,
        access_key: str = '',
        title: str = 'Untitled',
        path: str = '/',
        tags: str = '',
        read_only: bool = True):
    data = check_token(token)
    if not data:
        return {
            'status': 'failed',
            'message': 'invalid token.'
        }
    user_id, username, _, expire = data
    data = check_notebook(notebook_name)
    if not data:
        return {
            'status': 'failed',
            'message': 'no such notebook.'
        }
    notebook_id, _, owner, shared, key = data
    if owner != username and not shared:
        return {
            'status': 'failed',
            'message': 'access denied.'
        }
    elif access_key != key:
        return {
            'status': 'failed',
            'message': 'access denied.'
        }
    sql = f"""
        SELECT id
        FROM notes
        WHERE
            access_key = '{note_key}' AND 
            deleted = FALSE
    """
    data = db.run_sql(sql).one_row
    if data:
        return {
            'status': 'failed',
            'message': 'the note is already existed.'
        }
    sql = f"""
        INSERT INTO notes
        (title, owner, access_key, notebook_id, path, tags, `read_only`)
        VALUES
        ('{title}', '{username}', '{note_key}', '{notebook_id}', '{path}', '{tags}', {read_only})
    """
    db.run_sql(sql)
    return {
        'status': 'ok',
        'username': username,
        'user_id': user_id,
        'notebook': notebook_name,
        'notebook_id': notebook_id,
        'title': title,
        'note_key': note_key,
        'expire': str(expire)
    }


@app.get('/notebooks/{notebook_name}/{note_key}')
async def get_note(
        notebook_name: str,
        note_key: str,
        token: str,
        access_key: str = ''):
    data = check_token(token)
    if not data:
        return {
            'status': 'failed',
            'message': 'invalid token.'
        }
    user_id, username, _, expire = data
    data = check_notebook(notebook_name)
    if not data:
        return {
            'status': 'failed',
            'message': 'no such notebook.'
        }
    notebook_id, _, owner, shared, key = data
    if owner != username and not shared:
        return {
            'status': 'failed',
            'message': 'access denied.'
        }
    elif access_key != key:
        return {
            'status': 'failed',
            'message': 'access denied.'
        }
    sql = f"""
        SELECT id, title, owner, path, tags, `read_only`
        FROM notes
        WHERE
            notebook_id = '{notebook_id}' AND
            access_key = '{note_key}' AND
            deleted = FALSE
    """
    data = db.run_sql(sql).one_row
    if not data:
        return {
            'status': 'failed',
            'message': 'no such note.'
        }
    note_id, title, owner, path, tags, read_only = data
    return {
        'status': 'ok',
        'username': username,
        'user_id': user_id,
        'notebook': notebook_name,
        'notebook_id': notebook_id,
        'records': [
            {
                'id': note_id,
                'title': title,
                'owner': owner,
                'key': key,
                'path': path,
                'tags': [tag.strip() for tag in tags.split(',')],
                'read_only': bool(read_only)
            }
        ],
        'expire': str(expire)
    }


@app.put('/notebooks/{notebook_name}/{note_key}')
async def edit_note(
        notebook_name: str,
        note_key: str,
        token: str,
        access_key: str = '',
        new_title: str = None,
        new_notebook_name: str = None,
        path: str = None,
        tags: str = None,
        read_only: bool = None
):
    data = check_token(token)
    if not data:
        return {
            'status': 'failed',
            'message': 'invalid token.'
        }
    user_id, username, _, expire = data
    data = check_notebook(notebook_name)
    if not data:
        return {
            'status': 'failed',
            'message': 'no such notebook.'
        }
    notebook_id, _, owner, shared, key = data
    if owner != username and not shared:
        return {
            'status': 'failed',
            'message': 'access denied.'
        }
    elif access_key != key:
        return {
            'status': 'failed',
            'message': 'access denied.'
        }
    if new_notebook_name:
        sql = f"""
            SELECT id, notebook
            FROM notebooks
            WHERE
                notebook = '{new_notebook_name}' AND
                deleted = FALSE
        """
        data = db.run_sql(sql).one_row
        if not data:
            return {
                'status': 'failed',
                'message': 'no such notebook.'
            }
        new_notebook_id, _ = data
    else:
        new_notebook_id = None
    sql = f"""
        SELECT id, title, path, tags, `read_only`
        FROM notes
        WHERE
            notebook_id = '{notebook_id}' AND
            access_key = '{note_key}' AND
            deleted = FALSE
    """
    data = db.run_sql(sql).one_row
    if not data:
        return {
            'status': 'failed',
            'message': 'no such note.'
        }
    note_id, original_title, original_path, original_tags, originally_read_only = data
    sql = f"""
        UPDATE notes
        SET
            title = '{new_title or original_title}',
            notebook_id = '{new_notebook_id or notebook_id}',
            path = '{path or original_path}',
            tags = '{tags or original_tags}',
            `read_only` = {read_only or originally_read_only}
        WHERE
            id = {note_id}
    """
    db.run_sql(sql)
    return {
        'status': 'ok',
        'username': username,
        'user_id': user_id,
        'notebook': new_notebook_name or notebook_name,
        'notebook_id': new_notebook_id or notebook_id,
        'title': new_title or original_title,
        'note_key': note_key,
        'expire': str(expire)
    }


@app.delete('/notebooks/{notebook_name}/{note_key}')
async def delete_note(
        notebook_name: str,
        note_key: str,
        token: str,
        access_key: str = ''):
    data = check_token(token)
    if not data:
        return {
            'status': 'failed',
            'message': 'invalid token.'
        }
    user_id, username, _, expire = data
    data = check_notebook(notebook_name)
    if not data:
        return {
            'status': 'failed',
            'message': 'no such notebook.'
        }
    notebook_id, _, owner, shared, key = data
    if owner != username:
        return {
            'status': 'failed',
            'message': 'access denied.'
        }
    elif access_key != key:
        return {
            'status': 'failed',
            'message': 'access denied.'
        }

    sql = f"""
        SELECT id, owner
        FROM notes
        WHERE
            access_key = '{note_key}' AND 
            deleted = FALSE
    """
    data = db.run_sql(sql).one_row
    if not data:
        return {
            'status': 'failed',
            'message': 'no such note.'
        }
    note_id, _ = data
    sql = f"""
        UPDATE notes
        SET deleted = TRUE
        WHERE
            id = {note_id} AND
            owner = '{username}'
    """
    db.run_sql(sql)
    return {
        'status': 'ok'
    }


@app.post('/upload/{note_key}')
def upload_note_content(note_key: str, token: str, content: str):
    data = check_token(token)
    if not data:
        return {
            'status': 'failed',
            'message': 'invalid token.'
        }
    user_id, username, _, expire = data
    file_path = f'../notes/{note_key}.brdnote'
    if os.path.exists(file_path):
        return {
            'status': 'failed',
            'message': 'the note key conflicts.'
        }
    with open(file_path, 'w') as fp:
        fp.write(content)
    return {
        'status': 'ok'
    }


@app.get('/download/{note_key}')
def get_note_content(note_key: str, token: str):
    data = check_token(token)
    if not data:
        return {
            'status': 'failed',
            'message': 'invalid token.'
        }
    user_id, username, _, expire = data
    file_path = f'../notes/{note_key}.brdnote'
    if not os.path.exists(file_path):
        return {
            'status': 'failed',
            'message': 'no such file.'
        }
    with open(file_path, 'r') as fp:
        lines = fp.readlines()
        content = ''.join(lines)
        return {
            'status': 'ok',
            'content': content,
            'date': os.path.getmtime(file_path)
        }
