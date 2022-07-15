import json
import os
import io


def get_text(group_name):

    word = input(u'Введите слово: ')
    b = 0
    l = []
    with open(f"{group_name}/exist_posts_{group_name}.txt") as f:
        l = f.read().splitlines()
    for i in l:
        with io.open(f"{group_name}/text_comment{i}_{group_name}.txt", encoding='utf-8') as file:
            
            for line in file:
                if word in line:
                    b += 1
                    print(b)
        if b != 0:
            with open(f"{group_name}/Rezult_comment_{group_name}.txt", "w", encoding='utf-8') as file_end:
                file_end.write("не подходит")
        else:
            with open(f"{group_name}/Rezult_comment_{group_name}.txt", "w", encoding='utf-8') as file_end:
                file_end.write("ничего криминального")

def main():
    group_name = input("Введите название группы: ")
    get_text(group_name)

if __name__ == '__main__':
    main()