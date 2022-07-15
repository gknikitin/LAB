from cProfile import label
from turtle import goto
import requests
import json
import os
import time

from auth_data import token

def check_list_and_comment(group_name,user_ids):
    l = []
    with open(f"{group_name}/exist_posts_{group_name}.txt") as f:
        l = f.read().splitlines()
    for i in l:
        url = f"https://api.vk.com/method/wall.getComments?owner_id={user_ids}&post_id={i}&access_token={token}&v=5.103"
        req = requests.get(url)
        src = req.json()
        if os.path.exists(f"{group_name}"):
            print
        else:
            os.mkdir(group_name)
        with open(f"{group_name}/comment{i}.json", "w", encoding="utf-8") as file:
            json.dump(src, file, indent=4, ensure_ascii=False)
        time.sleep(1)
        
        all_commits = []
        commits = src["response"]["items"]
        for all_commit in commits:
            all_commit = all_commit["text"]
            all_commits.append(all_commit)

        """Проверка, если файла не существует, значит это первый
        парсинг группы(отправляем все новые посты). Иначе начинаем
        проверку и отправляем только новые посты."""
        with open(f"{group_name}/text_comment{i}_{group_name}.txt", "w", encoding='utf-8') as file:
                for item in commits:
                    file.write(str(item) + "\n")
    #url = f"https://api.vk.com/method/wall.getComments?owner_id={user_ids}&post_id=3361&access_token={token}&v=5.103"
   

def main():
    group_name = input("Введите домен пользователя (буквы): ")
    user_ids = input("Введите id пользователя (цифры): ")
    check_list_and_comment(group_name,user_ids)





if __name__ == '__main__':
    main()