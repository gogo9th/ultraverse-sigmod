import sys
import os
import re
import getopt
import glob
import random
import pymysql
import spacy
import sqlite3


SQLITE_FILE_NAME = "backdata.sqlite3"

def help():
    global PROG_NAME
    print(PROG_NAME, '-t [1 or 2] -u [user] -p [password] -d [db] --posts [count] --users [count]')
    print('-t 1 : prepare data')
    print('-t 2 : update data')


def run_type_1(conn, post_count, user_count):
    if post_count == 0 or user_count == 0:
        help()
        sys.exit(1)

    if os.path.exists(SQLITE_FILE_NAME):
        os.remove(SQLITE_FILE_NAME)
    create_BackData(post_count)

    sqlite3_conn = sqlite3.connect(SQLITE_FILE_NAME)

    conn.ping(True)
    create_Tables(conn.cursor())

    insert_ClusteringModel(conn.cursor(), sqlite3_conn.cursor())

    insert_AllPosts(conn.cursor(), sqlite3_conn.cursor(), user_count)

    insert_Preprocessed(conn.cursor(), sqlite3_conn.cursor())

    sqlite3_conn.close()

    os.remove(SQLITE_FILE_NAME)


def run_type_2(conn):
    create_CategorizedPosts(conn.cursor())


def get_table_count(cursor, table_name):
    cursor.execute("SELECT count(*) FROM {0}".format(table_name))
    results = cursor.fetchone()
    return results[0]


def create_BackData(post_count):
    conn = sqlite3.connect(SQLITE_FILE_NAME)

    conn.execute('''CREATE TABLE "PostTable" (
	"post_id"	INTEGER NOT NULL,
	"user_id"	INTEGER NOT NULL,
	"text"	TEXT NOT NULL,
	PRIMARY KEY("post_id" AUTOINCREMENT)
);''')

    conn.execute('''CREATE TABLE "WordTable" (
	"word"	INTEGER NOT NULL,
	"post_id"	INTEGER NOT NULL,
	"count"	INTEGER NOT NULL,
	PRIMARY KEY("word","post_id"),
	FOREIGN KEY("post_id") REFERENCES "PostTable"("post_id")
);''')

    all_posts = list(glob.iglob('posts/*', recursive=True))
    all_users = list(range(1, user_count + 1))
    random.shuffle(all_users)

    idx = 0
    while True:
        i = get_table_count(conn.cursor(), "PostTable")
        if i >= post_count:
            break

        # 문제가 있는 파일은 넘어가야 되기 때문에 index 를 따로 갖음
        idx += 1

        try:
            f = open(all_posts[idx % len(all_posts)], "rt")
            text = f.read()
            f.close()

            conn.execute("INSERT INTO PostTable(user_id, text) VALUES (?, ?)",
                (all_users[i % len(all_users)], text,))
        except Exception as e:
            print(e)
            continue

    conn.commit()

    nlp = spacy.load("en_core_web_sm")

    cursor = conn.cursor()
    cursor.execute("SELECT * FROM PostTable")
    results = cursor.fetchall()
    for row in results:
        post_id = row[0]
        # user_id = row[1]
        text = row[2]
        doc = nlp(text)

        word_count_dict = dict()
        for word in doc:
            if word.pos_ in ["NOUN"]:
                w = word.text.lower()
                if w in word_count_dict:
                    word_count_dict[w] += 1
                else:
                    word_count_dict[w] = 1

        for i, j in word_count_dict.items():
            try:
                conn.execute("INSERT INTO WordTable(word, post_id, count) VALUES (?, ?, ?)",
                    (i, post_id, j,))
            except Exception as e:
                print(e)
                continue
    conn.commit()

    conn.close()


def create_Tables(cursor):
    cursor.execute("""CREATE TABLE IF NOT EXISTS `ClusteringModel` (
        `word` VARCHAR(50) NOT NULL COLLATE 'utf8_general_ci',
        `category` INT(11) NULL DEFAULT NULL,
        `score` FLOAT NULL DEFAULT NULL,
        PRIMARY KEY (`word`, `row_end`) USING BTREE
    )""")

    cursor.execute("""CREATE TABLE IF NOT EXISTS `AllPosts` (
        `post_id` INT(11) NULL DEFAULT NULL,
        `user_id` INT(11) NULL DEFAULT NULL,
        `text` TEXT NULL DEFAULT NULL
    )""")

    cursor.execute("""CREATE TABLE IF NOT EXISTS `Preprocessed` (
        `post_id` INT(11) NULL DEFAULT NULL,
        `user_id` INT(11) NULL DEFAULT NULL,
        `word` VARCHAR(50) NULL DEFAULT NULL COLLATE 'utf8_general_ci',
        `frequency` INT(11) NULL DEFAULT NULL
    )""")

    cursor.execute("""CREATE TABLE IF NOT EXISTS `CategorizedPosts` (
        `post_id` INT(11) NULL DEFAULT NULL,
        `user_id` INT(11) NULL DEFAULT NULL,
        `category` INT(11) NULL DEFAULT NULL,
        `total_score` FLOAT NULL DEFAULT NULL
    )""")


def insert_ClusteringModel(cursor, sqlite3_cursor):
    sqlite3_cursor.execute("SELECT word FROM WordTable GROUP BY word")
    for row in sqlite3_cursor.fetchall():
        word = row[0]

        try:
            cursor.execute("INSERT INTO ClusteringModel(word, category, score) VALUES (%s, %s, %s)",
                (word, random.randrange(1, 11), random.uniform(0, 1.0),))
        except Exception as e:
            print(e)
            continue


def insert_AllPosts(cursor, sqlite3_cursor, user_count):
    sqlite3_cursor.execute("SELECT * FROM PostTable")
    for row in sqlite3_cursor.fetchall():
        post_id = row[0]
        user_id = row[1]
        text = row[2]

        try:
            cursor.execute("INSERT INTO AllPosts(post_id, user_id, text) VALUES (%s, %s, %s)",
                (post_id, user_id, text,))
        except Exception as e:
            print(e)
            continue


def insert_Preprocessed(cursor, sqlite3_cursor):
    sqlite3_cursor.execute("""SELECT WordTable.post_id, PostTable.user_id, word, count
        FROM WordTable INNER JOIN PostTable
            ON WordTable.post_id = PostTable.post_id""")
    for row in sqlite3_cursor.fetchall():
        post_id = row[0]
        user_id = row[1]
        word = row[2]
        count = row[3]

        try:
            cursor.execute("INSERT INTO Preprocessed(post_id, user_id, word, frequency) VALUES (%s, %s, %s, %s)",
                (post_id, user_id, word, count,))
        except Exception as e:
            print(e)
            continue


def create_CategorizedPosts(cursor):
    cursor.execute("SELECT * FROM AllPosts")
    for row in cursor.fetchall():
        post_id = row[0]
        user_id = row[1]

        try:
            cursor.execute("""INSERT INTO CategorizedPosts
                SELECT post_id, user_id, category, sum(score * frequency) AS total_score
                    FROM Preprocessed JOIN ClusteringModel
                    WHERE Preprocessed.post_id = %s
                        AND Preprocessed.user_id = %s
                        AND Preprocessed.word = ClusteringModel.word
                    GROUP BY category ORDER BY total_score DESC LIMIT 1""", (post_id, user_id,))
        except Exception as e:
            print(e)
            continue


if __name__ == "__main__":
    global PROG_NAME
    PROG_NAME = sys.argv[0]

    runtype = 0
    username = ""
    password = ""
    database = ""
    post_count = 0
    user_count = 0

    try:
        opts, etc_args = getopt.getopt(sys.argv[1:], "ht:u:p:d:", ["help","posts=","users="])
    except Exception as e:
        print(e)
        help()
        sys.exit(1)

    for opt, arg in opts:
        if opt in ("-h", "--help"):
            help()
            sys.exit(0)
        elif opt in ("-t"):
            runtype = int(arg)
        elif opt in ("-u"):
            username = arg
        elif opt in ("-p"):
            password = arg
        elif opt in ("-d"):
            database = arg
        elif opt in ("--posts"):
            post_count = int(arg)
        elif opt in ("--users"):
            user_count = int(arg)

    if len(username) == 0 or len(password) == 0 or len(database) == 0:
        help()
        sys.exit(1)

    conn = pymysql.connect(host='localhost', user=username, password=password,
                        db=database, charset='utf8', autocommit=True)

    if runtype == 1:
        run_type_1(conn, post_count, user_count)
    elif runtype == 2:
        run_type_2(conn)
    else:
        help()
        sys.exit(1)

    conn.commit()

    conn.close()

    sys.exit(0)
