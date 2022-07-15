import requests
import json
import os

from auth_data import token


def get_Profile_Info(group_name,user_ids):
    url = f"https://api.vk.com/method/users.get?user_ids={user_ids}&fields=activities,about,books,bdate,career,common_count,connections,contacts,city,country,domain,education,exports,followers_count,friend_status,home_town,sex,site,schools,screen_name,status,verified,games,interests,is_favorite,is_hidden_from_feed,last_seen,maiden_name,military,movies,music,nickname,occupation,online,personal,quotes,relation,relatives,timezone,tv,universities&access_token={token}&v=5.103"
    req = requests.get(url)
    src = req.json()

    if os.path.exists(f"{group_name}"):
        print(f"Директория с именем {group_name} уже существует!")
    else:
        os.mkdir(group_name)
    
    with open(f"{group_name}/info_user.json", "w", encoding="utf-8") as file:
        json.dump(src, file, indent=4, ensure_ascii=False)


def main():
    group_name = input("Введите домен пользователя (буквы): ")
    user_ids = input("Введите id пользователя (цифры): ")
    get_Profile_Info(group_name,user_ids)


if __name__ == '__main__':
    main()