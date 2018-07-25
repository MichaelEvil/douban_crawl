# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class DoubanItem(scrapy.Item):

    _i = scrapy.Field()
    _j = scrapy.Field()

    user_id = scrapy.Field()
    user_follower = scrapy.Field()
    user_link = scrapy.Field()
    user_contacts = scrapy.Field()
    user_rev_contacts = scrapy.Field()

    group_id = scrapy.Field()

    subject_id = scrapy.Field()

    subject_type = scrapy.Field()
    subject_name = scrapy.Field()
    subject_info = scrapy.Field()
    subject_rating = scrapy.Field()
    subject_rating_people = scrapy.Field()
    subject_s5_rating_per = scrapy.Field()
    subject_s4_rating_per = scrapy.Field()
    subject_s3_rating_per = scrapy.Field()
    subject_s2_rating_per = scrapy.Field()
    subject_s1_rating_per = scrapy.Field()

    note_id = scrapy.Field()
    note_name = scrapy.Field()
    note = scrapy.Field()

    c_id = scrapy.Field()
    commenter_id = scrapy.Field()
    c_time = scrapy.Field()
    ref_cid = scrapy.Field()
    comment = scrapy.Field()
    commenter_rating = scrapy.Field()
    c_vote = scrapy.Field()

    review_id = scrapy.Field()
    review_name = scrapy.Field()
    review_spoil = scrapy.Field()
    review_rating = scrapy.Field()
    review_time = scrapy.Field()
    review = scrapy.Field()

    pass

class DoubanMapItem(scrapy.Item):

    map_user_id = scrapy.Field()
    map_i = scrapy.Field()
    map_j = scrapy.Field()

    pass
