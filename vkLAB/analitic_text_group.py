import json
import os
import io


def get_text(group_name):
    word = input(u'Введите слово: ')
    
    with io.open(f"{group_name}/text_{group_name}.txt", encoding='utf-8') as file:
        b = 0
        for line in file:
            if word in line:
                b += 1
    if b == 1:
        with open(f"{group_name}/Rezult_{group_name}.txt", "w", encoding='utf-8') as file_end:
            file_end.write("не подходит")
    else:
        with open(f"{group_name}/Rezult_{group_name}.txt", "w", encoding='utf-8') as file_end:
            file_end.write("ничего криминального")

def main():
    group_name = input("Введите название группы: ")
    get_text(group_name)

if __name__ == '__main__':
    main()