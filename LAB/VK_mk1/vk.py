import asyncio
import json
import time
from config import *
import aiohttp
from datetime import datetime
import requests
from pymongo import MongoClient
from pandas import ExcelWriter
import pandas
import subprocess


class VK:
    def __init__(self, domain, count=10, t_p=60 * 60 * 24 * 2, proxy=None, token=None, ids=None, owner_id=None):
        # Настройка
        self.ids = ids
        self.TOKEN = token
        self.DOMAIN = domain  # Адрес сообщества
        self.COUNT = count  # Количсетво постов за 1 запрос (100)
        self.delay = DELAY  # секунда
        self.array_id = {}
        self.lastRequestTime = 0
        self.time_period = t_p
        self.proxy = proxy
        self.url = f'https://api.vk.com/method/wall.get?domain={self.DOMAIN}&count=1&v=5.131&access_token={self.TOKEN}'
        self.owner_id = owner_id
        self.trigger_words = TRIGGER_WORDS
        self.danger_arr = {}
        #self.client = MongoClient('localhost', 27017)
        #self.db = self.client['vk']
        #self.users = self.db['user']
        self.mongo = MongoDB()


    # Запросы к API серверу
    async def requests_func(self, method, url_params):
        url = f'https://api.vk.com/method/{method}?v=5.131&access_token={self.TOKEN}&{url_params}'
        while True:
            await asyncio.sleep(0.01)
            if self.lastRequestTime + self.delay < time.time():
                print(self.ids, method, time.strftime('%H:%M:%S'))
                self.lastRequestTime = time.time()

                async with aiohttp.ClientSession() as session:
                    async with session.post(url, proxy=self.proxy) as response:
                        data = await response.text()
                        return data
                        
    # Основная функция которая управляет действиями
    async def get_online(self):
        global temp_urls
        print(f'Поток {self.ids} запущен!')
        while True:
            await asyncio.sleep(0.01)

            url = "wall.get", f"domain={self.DOMAIN}&count={self.COUNT}"
            #try:
            req_posts = await self.requests_func(*url)
            try:
                posts = json.loads(req_posts)['response']['items']
            except:
                print(req_posts)
            for post in posts:
                # за последние 2 дня
                if post['date'] > time.time() - self.time_period:
                    # проверка поста
                    await self.func_post(post)

                    # количество комментариев больше 0
                    if int(post['comments']['count']) > 0:
                        await self.func_comment(post)
            #except:
                #print(req_posts)

    # Сбор постов
    async def func_post(self, post):
        # сбор информации о фото и видео
        await asyncio.sleep(0.01)
        photo = {}
        video = {}
        if 'attachments' in post.keys():
            for i in range(len(post['attachments'])):
                if 'video' in post['attachments'][i].keys():
                    video |= {len(video): post['attachments'][i]['video']['image'][-1]['url']}
                    temp_urls.append(post['attachments'][i]['video']['image'][-1]['url'])

                elif 'photo' in post['attachments'][i].keys():
                    photo |= {len(photo): post['attachments'][i]['photo']['sizes'][-1]['url']}
                    temp_urls.append(post['attachments'][i]['photo']['sizes'][-1]['url'])
        else:
            photo = {}
            video = {}
        
        # сбор информации в целом о посте
        temp_dict = {
            'date': datetime.utcfromtimestamp(int(post['date'])).strftime('%Y-%m-%d %H:%M:%S'),
            'user_id': post['owner_id'],
            'text': str(post['text'].replace("'", "").replace("\n\n", "\n")),
            'photo': photo,
            'video': video,
            'count_comments': int(post['comments']['count']),
            'comments': {0: {'from_id': '0', 'first_name': '0', 'last_name': '0','date': '0', 'text': '0', 'video': '0', 'photo': '0', 'danger': '0'}},
        }

        self.array_id[post['id']] = temp_dict
        
        await self.post_check()
        
    # Сбор комментариев
    async def func_comment(self, post):
        # __init__
        await asyncio.sleep(0.01)
        arr_comments = {}
        arr_comments['comments'] = {}

        for offset in range(0, self.array_id[post['id']]['count_comments']+100, 100):
            url = ["wall.getComments", f"owner_id=-{self.owner_id}&post_id={post['id']}&count={100}&offset={offset}&extended=1"]
            #try:
            comments_full = json.loads(await self.requests_func(*url))

            if 'response' in comments_full.keys():
                comments = comments_full['response']['items']
                profiles = comments_full['response']['profiles']

                for comment in comments:

                    # сбор информации о фото и видео
                    photo = {}
                    video = {}
                    if 'attachments' in comment.keys():
                        for k in range(len(comment['attachments'])):
                            if 'video' in comment['attachments'][k].keys():
                                video |= {len(video): comment['attachments'][k]['video']['image'][-1]['url']}
                                temp_urls.append(comment['attachments'][k]['video']['image'][-1]['url'])
                            elif 'photo' in comment['attachments'][k].keys():
                                photo |= {len(photo): comment['attachments'][k]['photo']['sizes'][-1]['url']}
                                temp_urls.append(comment['attachments'][k]['photo']['sizes'][-1]['url'])
                    else:
                        photo = {}
                        video = {}
                    
                    for temp in range(len(profiles)):
                        first_name = None
                        last_name = None
                        
                        if comment['from_id'] == profiles[temp]['id']:
                            first_name = profiles[temp]['first_name']
                            last_name = profiles[temp]['last_name']
                            
                            break
                            
                    # сбор информации в целом о комментарии
                    date = datetime.utcfromtimestamp(int(comment['date'])).strftime('%Y-%m-%d %H:%M:%S')
                    arr_comments['comments'] |= {comment['id']: {'from_id': str(comment['from_id']),
                                                                    'first_name': str(first_name),
                                                                    'last_name': str(last_name),
                                                                    'date': str(date),
                                                                    'text': str(comment['text']),
                                                                    'video': video,
                                                                    'photo': photo,
                                                                    'danger': str(0)}}

                    self.array_id[post['id']]['comments'][comment['id']] = arr_comments['comments'][comment['id']]

                    # Проверка tread
                    if comment['thread']['count'] > 0:

                        for offset in range(0, comment['thread']['count'] + 100, 100):
                            url = ["wall.getComments", f"owner_id=-{self.owner_id}&post_id={post['id']}&comment_id={comment['id']}&count={100}&offset={offset}&extended=1"]
                            #try:
                            comments_tread_full = json.loads(await self.requests_func(*url))

                            if 'response' in comments_tread_full.keys():
                                comments_tread = comments_tread_full['response']['items']
                                profiles_tread = comments_tread_full['response']['profiles']

                                for comment_tread in comments_tread:
                                    
                                    for t in range(len(profiles_tread)):
                                        first_name_tread = None
                                        last_name_tread = None
                                        
                                        if comment_tread['from_id'] == profiles_tread[t]['id']:
                                            first_name_tread = profiles_tread[t]['first_name']
                                            last_name_tread = profiles_tread[t]['last_name']
                                            
                                            break

                                    # сбор информации о фото и видео
                                    photo = {}
                                    video = {}

                                    if 'attachments' in comment_tread.keys():

                                        for k in range(len(comment_tread['attachments'])):
                                            if 'video' in comment_tread['attachments'][k].keys():
                                                video |= {len(video): comment_tread['attachments'][k]['video']['image'][-1]['url']}
                                                temp_urls.append(comment_tread['attachments'][k]['video']['image'][-1]['url'])
                                            elif 'photo' in comment_tread['attachments'][k].keys():
                                                photo |= {len(photo): comment_tread['attachments'][k]['photo']['sizes'][-1]['url']}
                                                temp_urls.append(comment_tread['attachments'][k]['photo']['sizes'][-1]['url'])
                                    else:
                                        photo = {}
                                        video = {}

                                    # сбор информации в целом о комментарии
                                    date = datetime.utcfromtimestamp(int(comment_tread['date'])).strftime('%Y-%m-%d %H:%M:%S')
                                    arr_comments['comments'] |= {comment_tread['id']: {'from_id': str(comment_tread['from_id']),
                                                                                'first_name': str(first_name_tread),
                                                                                'last_name': str(last_name_tread),
                                                                                'date': str(date),
                                                                                'text': str(comment_tread['text']),
                                                                                'video': video,
                                                                                'photo': photo,
                                                                                'danger': str(0)}}

                                    self.array_id[post['id']]['comments'][comment_tread['id']] = arr_comments['comments'][comment_tread['id']]
                            #except:
                                #print(comments_tread)
            else:
                print(f"\nПост {post['id']} удален\n")
            #except:
                #print(comments)
                
        await self.comment_check()
        
    # Проверка постов и комментариев
    async def post_check(self):
        await asyncio.sleep(0.01)
        # проверка текста в посте на триггеры
        for post_id in self.array_id.keys():
            if post_id not in self.danger_arr.keys():
                for words in self.trigger_words:
                    if words in self.array_id[post_id]['text'].lower():
                        
                        mongo_array = self.array_id[post_id]
                        mongo_array['link'] = f'https://vk.com/{self.DOMAIN}?w=wall-{self.owner_id}_{post_id}'
                        
                        await self.mongo.insert(mongo_array)
                        self.danger_arr[post_id] = 1
                        break

    async def comment_check(self):
        await asyncio.sleep(0.01)
        # проверка текста в комментарии на триггеры
        for post_id in self.array_id.keys():
            for comment_id in self.array_id[post_id]['comments']:
                if comment_id not in self.danger_arr.keys():
                    for arr in self.trigger_words:
                        if arr in self.array_id[post_id]['comments'][comment_id]['text'].lower():
                            
                            mongo_array = self.array_id[post_id]['comments'][comment_id]
                            mongo_array['link'] = f'https://vk.com/{self.DOMAIN}?w=wall-{self.owner_id}_{post_id}'
                            mongo_array['group'] = self.DOMAIN

                            await self.mongo.insert(mongo_array)
                            
                            self.danger_arr[comment_id] = 1
                            break


class MongoDB:
    def __init__(self):
        self.client = MongoClient('localhost', 27017)
        self.db = self.client['vk']
        self.users = self.db['user'] 

    async def func(self):
        await asyncio.sleep(0.01)
        return self.users.find()

    async def insert(self, data):
        await asyncio.sleep(0.01)
        try:
            self.users.insert_one(data)
        except:
            print(data)

if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(main())
