# -*- coding: utf-8 -*-
import scrapy
import sqlite3
from douban.spiders.doubanuser import DoubanuserSpider
from lxml import etree
from douban.items import DoubanItem, DoubanMapItem
import re
from copy import deepcopy
from douban.user_settings import MAP_PATH


class DoubandetailSpider(DoubanuserSpider):
    name = 'doubandetail'
    allowed_domains = ['douban.com']
    start_urls = []
    path = {

        'user_contacts': '//div[@class="info"]/h1/text()',
        'user_rev_contacts': '//div[@class="info"]/h1/text()',
        'user_groups': '//dd/a/@href',
        'user_works': '//ul[@class="work-list"]//h2/a/@href',
        'user_notes': '//div[@class="article"]//div[@class="rr"]/a[@class="j a_unfolder_n"]/@href',
        'user_reviews': '//div[@class="review-list chart "]//h2/a/@href',

        'user_book_do': '//ul[@class="interest-list"]//h2/a/@href',
        'user_book_wish': '//ul[@class="interest-list"]//h2/a/@href',
        'user_book_collect': '//ul[@class="interest-list"]//h2/a/@href',
        'user_movie_do': '//div[@class="grid-view"]//li[@class="title"]/a/@href',
        'user_movie_wish': '//div[@class="grid-view"]//li[@class="title"]/a/@href',
        'user_movie_collect': '//div[@class="grid-view"]//li[@class="title"]/a/@href',
        'user_music_do': '//div[@class="grid-view"]//li[@class="title"]/a/@href',
        'user_music_wish': '//div[@class="grid-view"]//li[@class="title"]/a/@href',
        'user_music_collect': '//div[@class="grid-view"]//li[@class="title"]/a/@href'
    }

    h = DoubanuserSpider.headers
    h_book = deepcopy(h)
    h_movie = deepcopy(h)
    h_music = deepcopy(h)
    h_book['Host'] = 'book.douban.com'
    h_movie['Host'] = 'movie.douban.com'
    h_music['Host'] = 'music.douban.com'

    # debug
    def start_requests(self):
        yield  scrapy.Request(url="https://www.douban.com/", headers=self.h, callback = self.queue_requests)

    def queue_requests(self, response):
        '''
        con1 = sqlite3.connect('douban_user_index.db')
        c1 = con1.cursor()
        c1.execute('SELECT distinct _id FROM user_index')


        while 1:
            try:
                idt = c1.fetchone()
                user_id = idt[0]
            except Exception:
                break

            if user_id is None:
                return False

            # user_id = 'guixiaobo'  # debug: user_id = 'guixiaobo'

            item = DoubanItem()
            item['_i'] = 'user_profile'
            link = "https://www.douban.com/people/" + user_id + '/'
            item['user_id'] = user_id
            item['user_link'] = link

            # yield item
            '''
        mapconn = sqlite3.connect(MAP_PATH)
        mc = mapconn.cursor()


            # contacts
            # yield scrapy.Request(url=link + 'contacts', headers=self.h,
            #                      callback=lambda response, getid=user_id, get='user_contacts': self.parse_simple(
            #                          response, getid, get), meta={'cookiejar': response.meta['cookiejar']})
            #
            # # rev_contacts
            # yield scrapy.Request(url=link + 'rev_contacts', headers=self.h,
            #                      callback=lambda response, getid=user_id, get='user_rev_contacts': self.parse_simple(
            #                          response, getid, get), meta={'cookiejar': response.meta['cookiejar']})
            #

        mc.execute("select user_id from user_map where user_book_do = 0")
        # mc.execute("select user_id from user_map")
        while 1:
            try:
                user_id = mc.fetchone()[0]
            except Exception:
                break
            if not user_id:
                break
            link = "https://www.douban.com/people/" + user_id + '/'
        # if True:
        #     print("debug....")
        #     # TODO:debug -single
        #     user_id = "guixiaobo"
        #     link = "https://www.douban.com/people/" + user_id + '/'

            # groups
            # yield scrapy.Request(url=link + 'groups', headers=self.h,
            #                      callback=lambda response, getid=user_id, get_i='user_groups',
            #                                      get_j='group_id': self.parse_complex(response, getid, get_i, get_j))
            #
            # works
            # yield scrapy.Request(url=link + 'works', headers=self.h,
            #                      callback=lambda response, getid=user_id, get_i='user_works',
            #                                      get_j='subject_id': self.parse_complex(response, getid, get_i, get_j))
            #
            # # notes
            # yield scrapy.Request(url=link + 'notes', headers=self.h,
            #                      callback=lambda response, getid=user_id, get_i='user_notes',
            #                                      get_j='note_id': self.parse_complex(response, getid, get_i, get_j))
            #
            # # reviews
            # yield scrapy.Request(url=link + 'reviews', headers=self.h,
            #                      callback=lambda response, getid=user_id, get_i='user_reviews',
            #                                      get_j='review_id': self.parse_complex(response, getid, get_i, get_j))
            #
            # # board
            # yield scrapy.Request(url=link + 'board', headers=self.h,
            #                      callback=lambda response, getid=user_id: self.parse_board(response, getid))
            #
            # book do
            yield scrapy.Request(url='https://book.douban.com/people/' + user_id + '/do', headers=self.h_book,
                                 callback=lambda response, getid=user_id, get_i='user_book_do',
                                                 get_j='subject_id': self.parse_complex(response, getid, get_i, get_j))

            # book wish
            # yield scrapy.Request(url='https://book.douban.com/people/' + user_id + '/wish', headers=self.h_book,
            #                      callback=lambda response, getid=user_id, get_i='user_book_wish',
            #                                      get_j='subject_id': self.parse_complex(response, getid, get_i, get_j))
            #
            # # book collect
            # yield scrapy.Request(url='https://book.douban.com/people/' + user_id + '/collect', headers=self.h_book,
            #                      callback=lambda response, getid=user_id, get_i='user_book_collect',
            #                                      get_j='subject_id': self.parse_complex(response, getid, get_i, get_j))
            #
            # # movie do
            # yield scrapy.Request(url='https://movie.douban.com/people/' + user_id + '/do', headers=self.h_movie,
            #                      callback=lambda response, getid=user_id, get_i='user_movie_do',
            #                                      get_j='subject_id': self.parse_complex(response, getid, get_i, get_j))
            #
            # # movie wish
            # yield scrapy.Request(url='https://movie.douban.com/people/' + user_id + '/wish', headers=self.h_movie,
            #                      callback=lambda response, getid=user_id, get_i='user_movie_wish',
            #                                      get_j='subject_id': self.parse_complex(response, getid, get_i, get_j))
            #
            # # movie collect
            # yield scrapy.Request(url='https://movie.douban.com/people/' + user_id + '/collect', headers=self.h_movie,
            #                      callback=lambda response, getid=user_id, get_i='user_movie_collect',
            #                                      get_j='subject_id': self.parse_complex(response, getid, get_i, get_j))
            #
            # # music do
            # yield scrapy.Request(url='https://music.douban.com/people/' + user_id + '/do', headers=self.h_music,
            #                      callback=lambda response, getid=user_id, get_i='user_music_do',
            #                                      get_j='subject_id': self.parse_complex(response, getid, get_i, get_j))
            #
            # # music wish
            # yield scrapy.Request(url='https://music.douban.com/people/' + user_id + '/wish', headers=self.h_music,
            #                      callback=lambda response, getid=user_id, get_i='user_music_wish',
            #                                      get_j='subject_id': self.parse_complex(response, getid, get_i, get_j))
            #
            # # music collect
            # yield scrapy.Request(url='https://music.douban.com/people/' + user_id + '/collect', headers=self.h_music,
            #                      callback=lambda response, getid=user_id, get_i='user_music_collect',
            #                                      get_j='subject_id': self.parse_complex(response, getid, get_i, get_j))

    def parse_simple(self, response, getid, get):

        item = DoubanItem()
        item['_i'] = 'user_profile'
        item['_j'] = get
        item['user_id'] = getid
        item['user_link'] = None

        p = response.body.decode('UTF-8')
        a = etree.HTML(p).xpath(self.path[get])
        b = re.split(r'[()]\s*', a[0])
        item[get] = b[-2]
        return item

    # TODO: Need to change a lot
    def parse_complex(self, response, getid, get_i, get_j):

        item = DoubanItem()
        mitem = DoubanMapItem()
        item['_i'] = get_i
        mitem['map_i'] = get_i
        item['user_id'] = getid
        mitem['map_user_id'] = getid

        p = response.body.decode('UTF-8')
        a = etree.HTML(p).xpath(self.path[get_i])
        t = []
        for i in a:
            test = re.search(r'douban\.com/', i)
            if test:
                b = i.split('/')
                t.append(b[-2])
        try:
            num = etree.HTML(p).xpath('//span[@class="subject-num"]/text()')[0]
            num = re.findall("\d+", num)[-1]
            num = int(num)
        except IndexError:
            num = len(t)
        if num > 0:
            mitem['map_j'] = num
        else:
            mitem['map_j'] = -1
        yield mitem
        for i in t:
            item[get_j] = i
            yield item
            if get_j == 'subject_id':
                if get_i.split('_')[1] == 'book' or get_i.split('_')[1] == 'works':
                    yield scrapy.Request(url='https://book.douban.com/subject/' + i + '/', headers=self.h_book,
                                         callback=lambda response, getid=i: self.parse_sub(getid, response))
                    yield scrapy.Request(url='https://book.douban.com/subject/' + i + '/comments/', headers=self.h_book,
                                         callback=lambda response, getid=i: self.parse_sub_com(getid, response))
                elif get_i.split('_')[1] == 'movie':
                    yield scrapy.Request(url='https://movie.douban.com/subject/' + i + '/', headers=self.h_movie,
                                         callback=lambda response, getid=i: self.parse_sub(getid, response))
                    yield scrapy.Request(url='https://movie.douban.com/subject/' + i + '/comments/',
                                         headers=self.h_movie,
                                         callback=lambda response, getid=i: self.parse_sub_com(getid, response))
                elif get_i.split('_')[1] == 'music':
                    yield scrapy.Request(url='https://music.douban.com/subject/' + i + '/', headers=self.h_music,
                                         callback=lambda response, getid=i: self.parse_sub(getid, response))
                    yield scrapy.Request(url='https://music.douban.com/subject/' + i + '/comments/',
                                         headers=self.h_music,
                                         callback=lambda response, getid=i: self.parse_sub_com(getid, response))
                else:
                    pass
            elif get_j == 'review_id':
                yield scrapy.Request(url='https://book.douban.com/review/' + i + '/', headers=self.h_book,
                                     callback=lambda response, getid=i, ty="rev": self.parse_rev(getid, ty, response))
            elif get_j == 'note_id':
                yield scrapy.Request(url='https://www.douban.com/note/' + i + '/', headers=self.h_book,
                                     callback=lambda response, getid=i: self.parse_note(getid, response))

        try:
            link = etree.HTML(p).xpath('//link[@rel="next"]/@href')[0]
            u = response.url
            if link.startswith("https://"):
                pass
            elif link.startswith("?"):
                if "?start=" in u:
                    place = u.find("?start=")
                    tmp = u[:place]
                    link = tmp + link
                else:
                    link = u + link
            else:
                place = u.find("douban.com")
                link = u[:place] + "douban.com" + link
        except IndexError:
            link = None
        if link:
            yield scrapy.Request(url=link, headers=self.h, callback=lambda response, getid=getid, get_i=get_i,
                                                                           get_j=get_j: self.parse_complex(response,
                                                                                                           getid,
                                                                                                           get_i,
                                                                                                           get_j))

    @staticmethod
    def parse_board(response, getid):

        item = DoubanItem()
        item['_i'] = 'user_board'
        item['user_id'] = getid

        cid = []
        user = []
        time = []
        com = []
        tmp = []
        p = response.body.decode('UTF-8')
        a = etree.HTML(p).xpath('//li[@class="mbtrdot comment-item"]')
        for i in a:
            cid.append(i.attrib['data-cid'])
            time.append(i.find('span[@class="pl"]').text)
            user.append(i.find('a').attrib['href'][30:-1])

        a = etree.HTML(p).xpath('//li[@class="mbtrdot comment-item"]/text()')
        for i in a:
            if i != '\n    ':
                tmp.append(i)
        for i in tmp:
            c = i.replace('\n', '')
            c = c.replace(' ', '')
            c = c.replace('\xa0', '')
            com.append(c[1:])

        i = 0
        board = []
        while True:
            try:
                tmp = [cid[i], user[i], time[i], com[i]]
                board.append(tmp)
                i = i + 1
            except Exception:
                break
        for i in board:
            item['c_id'] = i[0]
            item['commenter_id'] = i[1]
            item['c_time'] = i[2]
            item['comment'] = i[3]
            yield item

    @staticmethod
    def parse_sub(ty, response):

        item = DoubanItem()
        item['_i'] = 'subject_profile'
        item['subject_id'] = re.split(r'[/?]\s*', response.url)[4]
        item['subject_type'] = ty

        p = response.body.decode('UTF-8')
        item['subject_name'] = etree.HTML(p).xpath('//div[@id="wrapper"]//h1/span/text()')[0]
        subject_info = []
        tmp = etree.HTML(p).xpath('//div[@id="info"]//text()')
        for i in tmp:
            if '\n' not in i:
                subject_info.append(i)
        item['subject_info'] = ' '.join(subject_info)
        try:
            item['subject_rating'] = etree.HTML(p).xpath('//strong/text()')[0]
        except Exception:
            item['subject_rating'] = None
        try:
            item['subject_rating_people'] = etree.HTML(p).xpath('//span[@property="v:votes"]/text()')[0]
        except Exception:
            item['subject_rating_people'] = None
        try:
            tmp = etree.HTML(p).xpath('//span[@class="rating_per"]/text()')
            item['subject_s5_rating_per'] = tmp[0]
            item['subject_s4_rating_per'] = tmp[1]
            item['subject_s3_rating_per'] = tmp[2]
            item['subject_s2_rating_per'] = tmp[3]
            item['subject_s1_rating_per'] = tmp[4]
        except Exception:
            item['subject_s5_rating_per'] = None
            item['subject_s4_rating_per'] = None
            item['subject_s3_rating_per'] = None
            item['subject_s2_rating_per'] = None
            item['subject_s1_rating_per'] = None
        return item

    @staticmethod
    def parse_sub_com(getid, response):
        item = DoubanItem()
        item['_i'] = 'subject_comments'
        item['subject_id'] = getid

        p = response.body.decode('UTF-8')
        i = etree.HTML(p).xpath('//li[@class="comment-item"]')

        for j in i:
            l = j.find('div//span[@class="comment-info"]/a').attrib['href']
            item['commenter_id'] = re.split(r'[/?]\s*', l)[4]
            item['c_id'] = j.attrib['data-cid']
            q = j.findall(
                'div//span[@class="comment-info"]/span')
            if len(q) > 1:
                item['c_time'] = q[1].text
            else:
                item['c_time'] = q[0].text
            try:
                l = j.find('div//span[@class="comment-info"]/span').attrib['class']
                item['commenter_rating'] = re.findall("\d+", l)[0][0]
            except Exception:
                item['commenter_rating'] = None
            item['comment'] = j.find('div/p[@class="comment-content"]').text
            item['c_vote'] = j.find('div//span[@class="vote-count"]').text
            yield item

    @staticmethod
    def parse_rev(getid, ty, response):
        if ty == "rev":
            item = DoubanItem()
            item['_i'] = 'review_profile'
            item['review_id'] = getid

            p = response.body.decode('UTF-8')
            item['review_name'] = etree.HTML(p).xpath('//div[@class="article"]/h1/span/text()')[0]
            tmp = etree.HTML(p).xpath(
                '//div[@class="main-bd"]/p[@class="main-title-tip"]/text()')
            if tmp:
                item['review_spoil'] = tmp[0]
            else:
                item['review_spoil'] = None
            l = etree.HTML(p).xpath('//header[@class="main-hd"]/span/@class')[0]
            item['review_rating'] = re.findall("\d+", l)[0][0]
            item['review_time'] = etree.HTML(p).xpath('//header[@class="main-hd"]/span[@class="main-meta"]/text()')[0]
            string = ""
            m = etree.HTML(p).xpath('//div[@id="link-report"]//p/text()')
            if not m:
                m = etree.HTML(p).xpath('//div[@id="link-report"]/div/text()')
            for n in m:
                string = string + n
            item['review'] = string
            yield item

        item = DoubanItem()
        item['_i'] = 'review_comments'
        item['review_id'] = getid

        p = response.body.decode('UTF-8')
        i = etree.HTML(p).xpath('//div[@class="comment-item"]')
        for j in i:
            string = ""
            item['c_id'] = j.attrib['data-cid']
            item['commenter_id'] = re.split(r'[/?]\s*', j.attrib['data-user_url'])[4]
            item['ref_cid'] = j.attrib['data-ref_cid']
            item['c_time'] = j.find('div//div[@class="header"]/span').text
            m = j.findall('div//p[@class="comment-text"]')
            if m:
                for n in m:
                    string = string + n.text
            else:
                string = ""
            item['comment'] = string
            yield item
    
    @staticmethod
    def parse_note(getid, response):
        item = DoubanItem()
        item['_i'] = 'note_profile'
        item['note_id'] = getid

        p = response.body.decode('UTF-8')
        note_name = etree.HTML(p).xpath('//div[@class="note-container"]//h1/text()')
        if note_name:
            note_name = note_name[0]
        else:
            note_name = None
        note = etree.HTML(p).xpath('//div[@class="note"][@id="link-report"]//p/text()')
        if note:
            note = note[0]
        else:
            note = None
        item["note_name"] = note_name
        item["note"] = note
        yield item
    
    @staticmethod
    def parse_rev_com(getid, response):
        item = DoubanItem()
        item['_i'] = 'review_comments'
        item['review_id'] = getid

        p = response.body.decode('UTF-8')
        i = etree.HTML(p).xpath('//div[@class="comment-item"]')
        for j in i:
            string = ""
            item['c_id'] = j.attrib['data-cid']
            item['commenter_id'] = re.split(r'[/?]\s*', j.attrib['data-user_url'])[4]
            item['ref_cid'] = j.attrib['data-ref_cid']
            item['c_time'] = j.find('div//div[@class="header"]/span').text
            m = j.findall('div//p[@class="comment-text"]')
            for n in m:
                string = string + n.text
            item['comment'] = string
            yield item
        