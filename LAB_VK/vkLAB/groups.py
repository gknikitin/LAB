import requests
import json
import os
import time

from auth_data import token

def get_Profile_Info(group_name,user_ids):
    url = f"https://api.vk.com/method/groups.get?user_id={user_ids}&fields=photo_50&access_token={token}&extended=1&v=5.103"
    req = requests.get(url)
    src = req.json()

    if os.path.exists(f"{group_name}"):
        print(f"Директория с именем {group_name} уже существует!")
    else:
        os.mkdir(group_name)
    
    with open(f"{group_name}/groups_user.json", "w", encoding="utf-8") as file:
        json.dump(src, file, indent=4, ensure_ascii=False)

    all_commits = []
    commits = src["response"]["items"]
    for all_commit in commits:
        all_commit = all_commit["name"]
        all_commits.append(all_commit)

    """Проверка, если файла не существует, значит это первый
    парсинг группы(отправляем все новые посты). Иначе начинаем
    проверку и отправляем только новые посты."""
    with open(f"{group_name}/text_{group_name}.txt", "w", encoding='utf-8') as file:
            for item in commits:
                file.write(str(item) + "\n")
    
def main():
    group_name = input("Введите домен пользователя (буквы): ")
    user_ids = input("Введите id пользователя (цифры): ")
    get_Profile_Info(group_name,user_ids)


if __name__ == '__main__':
    main()