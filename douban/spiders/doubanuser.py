# -*- coding: utf-8 -*-
import sys
import scrapy
import urllib.request
from douban.items import DoubanItem
from lxml import etree
import re
from scrapy.http.cookies import CookieJar
import logging

# specially using in windows
# from PIL import Image
# import matplotlib.pyplot as plt


class DoubanuserSpider(scrapy.Spider):
    cookie_jar = CookieJar()

    name = 'doubanuser'

    # 'https://www.douban.com/people/'+ 'A-Za-z0-9' + '/' as initial urls
    start_urls = [
        'https://www.douban.com/people/guixiaobo/']

    used_urls = []

    allowed_domains = ['douban.com']

    headers = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
               # 'Accept-Encoding': 'gzip, deflate, br',
               'Accept-Language': 'zh-CN,zh;q=0.9',
               'Cache-Control': 'max-age=0',
               'Connection': 'keep-alive',
               'DNT': '1',
               'Host': 'www.douban.com',
               'Upgrade-Insecure-Requests': '1',
               'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36',
               'referer': 'https://www.douban.com'
               }

    formdata = {'source': 'None',
                'redir': 'https://www.douban.com',
                # todo:need to input an account
                'form_email': '',
                'form_password': '',
                 'login': '登录'}

    def start_requests(self):
        yield scrapy.Request(url='https://www.douban.com/login',
                             headers=self.headers,
                             callback=self.parse_login,
                             meta={'cookiejar': self.cookie_jar})

    def parse_login(self, response):
        page = response.body.decode('utf-8')
        captcha = etree.HTML(page).xpath('//img[@id="captcha_image"]/@src')
        ck = etree.HTML(page).xpath('//input[@name="ck"]/@value')
        new_form = self.formdata

        if ck:
            new_form.setdefault('ck', ck)
            self.log('validation:ck------detected:' + ck)

        if captcha:
            print(captcha)
            name = "captcha.jpg"
            conn = urllib.request.urlopen(captcha[0])
            f = open(name, 'wb')
            f.write(conn.read())
            f.close()
            print('Pic Saved!')
            self.log('validation:captcha------detected:' + captcha[0])

            # img = Image.open(name)
            # plt.figure("captcha")
            # plt.imshow(img)
            # plt.show()

            print('wait for captcha input:')
            vali = ""
            while vali is "":
                vali = input()
            print('captcha input complete')
            str1 = 'https://www.douban.com/misc/captcha\?id='
            str2 = '&'
            str3 = 'amp;'
            str4 = 'size=s'
            tmp = re.sub(str1, "", captcha[0])
            tmp = re.sub(str2, "", tmp)
            tmp = re.sub(str3, "", tmp)
            solution = re.sub(str4, "", tmp)

            new_form.setdefault('captcha-solution', vali)
            new_form.setdefault('captcha-id', solution)

            return scrapy.FormRequest(url='https://www.douban.com/login',
                                      headers=self.headers,
                                      formdata=new_form,
                                      callback=self.check_login,
                                      meta={'cookiejar': response.meta['cookiejar']})

        else:
            self.log('validation:no validation')
            return scrapy.FormRequest(url='https://www.douban.com/login',
                                      headers=self.headers,
                                      formdata=self.formdata,
                                      callback=self.check_login,
                                      meta={'cookiejar': response.meta['cookiejar']})

    def check_login(self, response):

        page = response.body.decode('utf-8')
        nav = etree.HTML(page).xpath('//li[@class="nav-user-account"]')
        if nav:
            self.log('login:succeeded!')
        else:
            self.log('login:failed!')
            return self.start_requests()

        self.cookie_jar.extract_cookies(response, response.request)

        with open('cookies_user.txt', 'w') as f:
            for cookie in self.cookie_jar:
                f.write(str(cookie) + '\n')

        return self.queue_requests(response)

    def queue_requests(self, response):

        for i in self.start_urls:
            self.used_urls.append(i)
            a = i + 'rev_contacts'
            b = i + 'contacts'
            yield scrapy.Request(url=a, headers=self.headers, callback=self.parse,
                                 meta={'cookiejar': response.meta['cookiejar']})
            yield scrapy.Request(url=b, headers=self.headers, callback=self.parse,
                                 meta={'cookiejar': response.meta['cookiejar']})

    def parse(self, response):

        page = response.body.decode('UTF-8')
        item = DoubanItem()
        item['_i'] = 'user_relationship'
        info = re.split(r'[/?]\s*', response.url)

        if info[5] == 'rev_contacts':
            item['user_id'] = info[4]
        else:
            item['user_follower'] = info[4]

        raw_urls = etree.HTML(page).xpath('//a/@href')
        next_page = etree.HTML(page).xpath('//link[@rel="next"]/@href')
        fin_urls = []
        for i in raw_urls:
            userlink = re.search(r'^https://www\.douban\.com/people/[a-zA-Z0-9_]+/$', i)
            if userlink:
                if i not in fin_urls:
                    fin_urls.append(i)

        for i in next_page:
            j = 'https://www.douban.com' + i
            loginfo = j + ": depth minus 1"
            self.log(message=loginfo, level=logging.INFO)
            yield scrapy.Request(url=j, headers=self.headers, callback=self.parse,
                                 meta={'cookiejar': response.meta['cookiejar'],
                                       'depth':response.meta['depth']-1})

        for i in fin_urls:
            self.start_urls.append(i)
            if info[5] == 'rev_contacts':
                item['user_follower'] = i[30:-1]
            else:
                item['user_id'] = i[30:-1]

            yield item

        while True:
            try:
                i = self.start_urls.pop()
            except Exception:
                return False
            if i not in self.used_urls:
                self.used_urls.append(i)
                a = i + 'rev_contacts'
                b = i + 'contacts'
                yield scrapy.Request(url=a, headers=self.headers, callback=self.parse,
                                     meta={'cookiejar': response.meta['cookiejar']})
                yield scrapy.Request(url=b, headers=self.headers, callback=self.parse,
                                     meta={'cookiejar': response.meta['cookiejar']})
