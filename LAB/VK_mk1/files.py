from config import *
import requests
import sys


class sBot:
    def __init__(self, TOKEN):
        self.token = TOKEN
        self.chat_id = USER_ID

    def send_file(self, files=None):
        url = f'https://api.telegram.org/bot{self.token}/sendDocument'
        requests.post(url=url, data={"chat_id": self.chat_id}, files=files)


def files_main():
    with open(f"{file_name}.xlsx", mode='rb') as filexlsx:
        bot.send_file(files={"document": filexlsx})


if __name__ == '__main__':
    file_name = sys.argv[1]
    bot = sBot(token_tg)
    files_main()
