import sqlite3


class mapscraping(object):

    def __init__(self):
        self.sql_path = "map.db"
        self.conn = sqlite3.connect(self.sql_path)
        self.c = self.conn.cursor()

        self.c.execute('''CREATE TABLE IF NOT EXISTS user_map
                            (user_id TEXT PRIMARY KEY NOT NULL,
                            user_works TEXT,
                            user_groups TEXT,
                            user_notes TEXT,
                            user_book_do TEXT,
                            user_book_wish TEXT,
                            user_book_collect TEXT,
                            user_movie_do TEXT,
                            user_movie_wish TEXT,
                            user_movie_collect TEXT,
                            user_music_do TEXT,
                            user_music_wish TEXT,
                            user_music_collect TEXT,
                            user_reviews TEXT)''')
        # todo: need to improve
        # self.c.execute('''CREATE TABLE IF NOT EXISTS subject_map
        #                     (subject_id TEXT PRIMARY NOT NULL,
        #                     subject_profile TEXT)''')
        self.dic = [
            "user_groups",
            "user_works",
            "user_notes",
            "user_book_do",
            "user_book_wish",
            "user_book_collect",
            "user_movie_do",
            "user_movie_wish",
            "user_movie_collect",
            "user_music_do",
            "user_music_wish",
            "user_music_collect",
            "user_reviews"
        ]

    def scan(self, sql_profile, sql_index):
        p_conn = sqlite3.connect(sql_profile)
        i_conn = sqlite3.connect(sql_index)
        pc = p_conn.cursor()
        ic = i_conn.cursor()
        ic.execute("select count(*) from user_index")
        amount = ic.fetchall()[0][0]
        print(amount, 'user_id detected')
        ic.execute("select * from user_index")
        while 1:
            try:
                tmp = ic.fetchone()[0]
            except Exception:
                break
            if not tmp:
                break
            self.c.execute("insert or ignore into user_map (user_id) values (?)", (tmp,))
        self.conn.commit()
        print("user map created in", self.sql_path)
        record = {}
        for table_name in self.dic:
            print("scanning:", table_name)
            li = 0
            count = 0
            ic.execute("select * from user_index")
            while 1:
                try:
                    tmp = ic.fetchone()[0]
                except Exception:
                    break
                if not tmp:
                    break
                sql = "select count(*) from "+ table_name +" where user_id = ?"
                pc.execute(sql,(tmp,))
                count += 1
                detected = pc.fetchall()[0][0]
                if detected > 0:
                    li += 1
                print(table_name, 'detected(', count, '/', amount, ')', sep=' ')
                sql = "update user_map set " + table_name + " = ? where user_id = ?"
                self.c.execute(sql, (detected,tmp))
            print(li, 'confirmed in', table_name)
            record[table_name] = li
            self.conn.commit()
        print(record)


if __name__ == "__main__":
    m = mapscraping()
    m.scan("douban_profile_0630.db","douban_user_index_0611.db")




