# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

import sqlite3
from douban.user_settings import PROFILE_DB_PATH,MAP_NEW_PATH


class DoubanPipeline(object):

    def __init__(self):
        self.conn = sqlite3.connect(PROFILE_DB_PATH)
        self.c = self.conn.cursor()

        self.c.execute('''CREATE TABLE IF NOT EXISTS user_index
                    (_id TEXT PRIMARY KEY NOT NULL)''')  # user_index
        self.c.execute('''CREATE TABLE IF NOT EXISTS user_relationship
                    (user_id TEXT NOT NULL,
                    user_follower TEXT NOT NULL)''')  # user_relationship
        self.c.execute('''CREATE TABLE IF NOT EXISTS user_profile
                    (user_id TEXT PRIMARY KEY NOT NULL,
                    user_link TEXT,
                    user_contacts TEXT,
                    user_rev_contacts TEXT)''')  # user_profile
        self.c.execute('''CREATE TABLE IF NOT EXISTS user_groups
                    (user_id TEXT NOT NULL,
                    group_id TEXT NOT NULL)''')  # user_groups
        self.c.execute('''CREATE TABLE IF NOT EXISTS user_works
                    (user_id TEXT NOT NULL,
                    subject_id TEXT NOT NULL)''')  # user_works
        self.c.execute('''CREATE TABLE IF NOT EXISTS user_notes
                    (user_id TEXT NOT NULL,
                    note_id TEXT NOT NULL)''')  # user_notes
        self.c.execute('''CREATE TABLE IF NOT EXISTS user_book_do
                    (user_id TEXT NOT NULL,
                    subject_id TEXT NOT NULL)''')  # user_book_do
        self.c.execute('''CREATE TABLE IF NOT EXISTS user_book_wish
                    (user_id TEXT NOT NULL,
                    subject_id TEXT NOT NULL)''')  # user_book_wish
        self.c.execute('''CREATE TABLE IF NOT EXISTS user_book_collect
                    (user_id TEXT NOT NULL,
                    subject_id TEXT NOT NULL)''')  # user_book_collect
        self.c.execute('''CREATE TABLE IF NOT EXISTS user_movie_do
                    (user_id TEXT NOT NULL,
                    subject_id TEXT NOT NULL)''')  # user_movie_do
        self.c.execute('''CREATE TABLE IF NOT EXISTS user_movie_wish
                    (user_id TEXT NOT NULL,
                    subject_id TEXT NOT NULL)''')  # user_movie_wish
        self.c.execute('''CREATE TABLE IF NOT EXISTS user_movie_collect
                    (user_id TEXT NOT NULL,
                    subject_id TEXT NOT NULL)''')  # user_movie_collect
        self.c.execute('''CREATE TABLE IF NOT EXISTS user_music_do
                    (user_id TEXT NOT NULL,
                    subject_id TEXT NOT NULL)''')  # user_music_do
        self.c.execute('''CREATE TABLE IF NOT EXISTS user_music_wish
                    (user_id TEXT NOT NULL,
                    subject_id TEXT NOT NULL)''')  # user_music_wish
        self.c.execute('''CREATE TABLE IF NOT EXISTS user_music_collect
                    (user_id TEXT NOT NULL,
                    subject_id TEXT NOT NULL)''')  # user_music_collect
        self.c.execute('''CREATE TABLE IF NOT EXISTS note_profile
                    (note_id TEXT PRIMARY KEY NOT NULL,
                    note_name TEXT,
                    note TEXT)''')  # note_profile
        self.c.execute('''CREATE TABLE IF NOT EXISTS user_reviews
                    (user_id TEXT NOT NULL,
                    review_id TEXT NOT NULL)''')  # user_reviews
        self.c.execute('''CREATE TABLE IF NOT EXISTS user_board
                    (user_id TEXT NOT NULL,
                    c_id TEXT PRIMARY KEY NOT NULL,
                    commenter_id TEXT,
                    c_time TEXT,
                    comment TEXT)''')  # user_board
        self.c.execute('''CREATE TABLE IF NOT EXISTS subject_profile
                    (subject_id TEXT PRIMARY KEY NOT NULL,
                    subject_type TEXT NOT NULL,
                    subject_name TEXT,
                    subject_info TEXT,
                    subject_rating TEXT,
                    subject_rating_people TEXT,
                    subject_s5_rating_per TEXT,
                    subject_s4_rating_per TEXT,
                    subject_s3_rating_per TEXT,
                    subject_s2_rating_per TEXT,
                    subject_s1_rating_per TEXT)''')  # subject_profile
        self.c.execute('''CREATE TABLE IF NOT EXISTS subject_comments
                    (subject_id TEXT NOT NULL,
                    c_id TEXT PRIMARY KEY NOT NULL,
                    commenter_id  TEXT,
                    commenter_rating TEXT,
                    c_time TEXT,
                    comment TEXT,
                    c_vote TEXT)''')  # subject_comments
        self.c.execute('''CREATE TABLE IF NOT EXISTS subject_reviews
                    (subject_id TEXT NOT NULL,
                    review_id TEXT NOT NULL)''')  # subject_reviews
        self.c.execute('''CREATE TABLE IF NOT EXISTS review_profile
                    (review_id TEXT PRIMARY KEY NOT NULL,
                    review_spoil TEXT,
                    review_rating TEXT,
                    review_time TEXT,
                    review TEXT)''')  # review_profile
        self.c.execute('''CREATE TABLE IF NOT EXISTS review_comments
                    (review_id TEXT NOT NULL,
                    c_id TEXT PRIMARY KEY NOT NULL,
                    commenter_id TEXT,
                    ref_cid TEXT,
                    c_time TEXT,
                    comment TEXT)''')  # review_comments

        self.mapconn = sqlite3.connect(MAP_NEW_PATH)
        self.mc = self.mapconn.cursor()

        pass
         
    def process_item(self, item, spider):

        if 'map_user_id' in item.keys():
            _id = item['map_user_id']
            _j = item['map_j']
            sql = "update user_map set " + item['map_i'] + " = ? where user_id = ?"
            self.mc.execute(sql, (_j, _id))

        if 'user_id' in item.keys():
            _id = item['user_id']
            self.c.execute(
                "insert or ignore into user_index (_id) values (?)", (_id,))

        if 'user_follower' in item.keys():
            _id = item['user_follower']
            self.c.execute(
                "insert or ignore into user_index (_id) values (?)", (_id,))

        if '_i' in item.keys():

            if item['_i'] == 'user_relationship':

                user_id = item['user_id']
                user_follower = item['user_follower']
                self.c.execute("insert or ignore into user_relationship (user_id, user_follower) values (?, ?)",
                               (user_id, user_follower))

            elif item['_i'] == 'user_profile':

                if item['user_link'] is not None:
                    user_id = item['user_id']
                    user_link = item['user_link']
                    self.c.execute("insert or ignore into user_profile (user_id, user_link) values (?, ?)",
                                   (user_id, user_link))
                else:
                    user_id = item['user_id']
                    sql = "update user_profile set " + item['_j'] + " = ? where user_id = ?"
                    self.c.execute(sql, (item[item['_j']], user_id))

            elif item['_i'] == 'user_groups':

                user_id = item['user_id']
                group_id = item['group_id']
                self.c.execute("insert or ignore into user_groups (user_id, group_id) values (?, ?)",
                               (user_id, group_id))

            elif item['_i'] == 'user_works':

                user_id = item['user_id']
                subject_id = item['subject_id']
                self.c.execute("insert or ignore into user_works (user_id, subject_id) values (?, ?)",
                               (user_id, subject_id))

            elif item['_i'] == 'user_notes':

                user_id = item['user_id']
                note_id = item['note_id']
                self.c.execute("insert or ignore into user_notes (user_id, note_id) values (?, ?)",
                               (user_id, note_id))

            elif item['_i'] == 'user_book_do':

                user_id = item['user_id']
                subject_id = item['subject_id']
                self.c.execute("insert or ignore into user_book_do (user_id, subject_id) values (?, ?)",
                               (user_id, subject_id))

            elif item['_i'] == 'user_book_wish':

                user_id = item['user_id']
                subject_id = item['subject_id']
                self.c.execute("insert or ignore into user_book_wish (user_id, subject_id) values (?, ?)",
                               (user_id, subject_id))

            elif item['_i'] == 'user_book_collect':

                user_id = item['user_id']
                subject_id = item['subject_id']
                self.c.execute("insert or ignore into user_book_collect (user_id, subject_id) values (?, ?)",
                               (user_id, subject_id))

            elif item['_i'] == 'user_movie_do':

                user_id = item['user_id']
                subject_id = item['subject_id']
                self.c.execute("insert or ignore into user_movie_do (user_id, subject_id) values (?, ?)",
                               (user_id, subject_id))

            elif item['_i'] == 'user_movie_wish':

                user_id = item['user_id']
                subject_id = item['subject_id']
                self.c.execute("insert or ignore into user_movie_wish (user_id, subject_id) values (?, ?)",
                               (user_id, subject_id))

            elif item['_i'] == 'user_movie_collect':

                user_id = item['user_id']
                subject_id = item['subject_id']
                self.c.execute("insert or ignore into user_movie_collect (user_id, subject_id) values (?, ?)",
                               (user_id, subject_id))

            elif item['_i'] == 'user_music_do':

                user_id = item['user_id']
                subject_id = item['subject_id']
                self.c.execute("insert or ignore into user_music_do (user_id, subject_id) values (?, ?)",
                               (user_id, subject_id))

            elif item['_i'] == 'user_music_wish':

                user_id = item['user_id']
                subject_id = item['subject_id']
                self.c.execute("insert or ignore into user_music_wish (user_id, subject_id) values (?, ?)",
                               (user_id, subject_id))

            elif item['_i'] == 'user_music_collect':

                user_id = item['user_id']
                subject_id = item['subject_id']
                self.c.execute("insert or ignore into user_music_collect (user_id, subject_id) values (?, ?)",
                               (user_id, subject_id))

            elif item['_i'] == 'note_profile':

                note_id = item['note_id']
                note_name = item['note_name']
                note = item['note']
                self.c.execute("insert or ignore into note_profile (note_id, note_name, note) values (?, ?, ?)",
                               (note_id, note_name, note))

            elif item['_i'] == 'user_reviews':

                user_id = item['user_id']
                review_id = item['review_id']
                self.c.execute("insert or ignore into user_reviews (user_id, review_id) values (?, ?)",
                               (user_id, review_id))

            elif item['_i'] == 'user_board':

                user_id = item['user_id']
                c_id = item['c_id']
                commenter_id = item['commenter_id']
                c_time = item['c_time']
                comment = item['comment']
                self.c.execute('''insert or ignore into user_board (user_id, c_id, commenter_id, c_time, comment) values (?, ?, ?, ?, ?)''',
                               (user_id, c_id, commenter_id, c_time, comment))

            elif item['_i'] == 'subject_profile':

                subject_id = item['subject_id']
                subject_type = item['subject_type']
                subject_name = item['subject_name']
                subject_info = item['subject_info']
                subject_rating = item['subject_rating']
                subject_rating_people = item['subject_rating_people']
                subject_s5_rating_per = item['subject_s5_rating_per']
                subject_s4_rating_per = item['subject_s4_rating_per']
                subject_s3_rating_per = item['subject_s3_rating_per']
                subject_s2_rating_per = item['subject_s2_rating_per']
                subject_s1_rating_per = item['subject_s1_rating_per']
                self.c.execute("insert or ignore into subject_profile (subject_id, subject_type, subject_name, subject_info, subject_rating, subject_rating_people, subject_s5_rating_per, subject_s4_rating_per, subject_s3_rating_per, subject_s2_rating_per, subject_s1_rating_per) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                               (subject_id, subject_type, subject_name, subject_info, subject_rating, subject_rating_people, subject_s5_rating_per, subject_s4_rating_per, subject_s3_rating_per, subject_s2_rating_per, subject_s1_rating_per))

            elif item['_i'] == 'subject_comments':

                subject_id = item['subject_id']
                c_id = item['c_id']
                commenter_id = item['commenter_id']
                commenter_rating = item['commenter_rating']
                c_time = item['c_time']
                comment = item['comment']
                c_vote = item['c_vote']
                self.c.execute("insert or ignore into subject_comments (subject_id, c_id, commenter_id, commenter_rating, c_time, comment, c_vote) values (?, ?, ?, ?, ?, ?, ?)",
                               (subject_id, c_id, commenter_id, commenter_rating, c_time, comment, c_vote))

            elif item['_i'] == 'subject_reviews':

                subject_id = item['subject_id']
                review_id = item['review_id']
                self.c.execute("insert or ignore into subject_reviews (subject_id, review_id) values (?, ?)",
                               (subject_id, review_id))

            elif item['_i'] == 'review_profile':

                review_id = item['review_id']
                review_spoil = item['review_spoil']
                review_rating = item['review_rating']
                review_time = item['review_time']
                review = item['review']
                self.c.execute("insert or ignore into review_profile (review_id, review_spoil, review_rating, review_time, review) values (?, ?, ?, ?, ?)",
                               (review_id, review_spoil, review_rating, review_time, review))

            elif item['_i'] == 'review_comments':

                review_id = item['review_id']
                c_id= item['c_id']
                commenter_id = item['commenter_id']
                ref_cid = item['ref_cid']
                c_time = item['c_time']
                comment = item['comment']
                self.c.execute("insert or ignore into review_comments (review_id, c_id, commenter_id, ref_cid, c_time, comment) values (?, ?, ?, ?, ?, ?)",
                               (review_id, c_id, commenter_id, ref_cid, c_time, comment))
            else:
                pass

        self.conn.commit()
        self.mapconn.commit()
        return item

    def close_spider(self,spider):
        self.conn.close()
        self.mapconn.close()
