from bs4 import BeautifulSoup
import ftplib
import requests
import csv
import time
import mysql.connector
from mysql.connector import Error


""" Рабочие переменные """
links_result, create_csv, string_csv, delimiter = [], [], '', ','
gender, age, experience, education, language, salary = [], [], [], [], [], []
vacancy = {'Избранное': 'https://hh.ru/employer/resumefolders?page=', 'Общее': 'https://hh.ru/resumes/pomoschnik-rukovoditelya?page='}

""" Начало работы """
print('*** Выберите желаемую вакансию для парсинга ***')
vacancy_parser = input('1, 2 : ').lower()
print(f"*** Введите кол-во страниц для парсинга вакансии '{vacancy_parser}' ***")
number_of_pages = int(input('Кол-во страниц: '))

""" Переменные для SQL """
table_name = {'Избранное': 'hh_employer', 'Общее': 'hh_all'}
sql = "INSERT INTO " + table_name[vacancy_parser] + "(link, gender, age, experience, education, language, salary) VALUES (%s, %s, %s, %s, %s, %s, %s)"
hh_parser = """
CREATE TABLE """ + table_name[vacancy_parser] + """ (
  id INT PRIMARY KEY AUTO_INCREMENT, 
  link VARCHAR(150), 
  gender VARCHAR(10), 
  age INT, 
  experience INT,
  education VARCHAR(20),
  language VARCHAR(20),
  salary VARCHAR(10)
)
"""

""" Парсинг ссылок """
def parse_links():
	for i in range(number_of_pages):
		URL = vacancy[vacancy_parser] + str(i)
		HEADERS = {
			'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.152 YaBrowser/21.2.3.100 Yowser/2.5 Safari/537.36'
			}

		response = requests.get(URL, headers = HEADERS)
		soup = BeautifulSoup(response.content, 'html.parser')
		items = soup.findAll('div', class_='resume-search-item')
		comps = []
	
		for item in items:
			comps.append({
				'link': item.find('a', class_ = 'resume-search-item__name').get('href')
			})

			for comp in comps:
				print('https://hh.ru', comp['link'], sep='')
				links_result.append('https://hh.ru' + comp['link'])

""" Таймер """
def timer():
	time_count = [3, 2]
	for j in range(time_count[1], 0, -1):
		time.sleep(1)
	print(f"*** {len(links_result)} ссылок для парсинга анкет успешно получены ***")
	for k in range(time_count[1], 0, -1):
		time.sleep(1)
	print('*** Парсинг анкет начнется через ***')
	for i in range(time_count[0], 0, -1):
		print(i)
		time.sleep(1)
	print('*** Парсинг анкет запущен ***')


parse_links()
timer()

""" Парсинг анкет """
def parse_resume():
	for link_resume in range(len(links_result)):
		URL_RES = links_result[link_resume]
		HEADERS_RES = {
			'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.152 YaBrowser/21.2.3.100 Yowser/2.5 Safari/537.36'
			}

		response_res = requests.get(URL_RES, headers = HEADERS_RES)
		soup_res = BeautifulSoup(response_res.content, 'html.parser')
		items_res = soup_res.findAll('div', class_='resume-applicant')
		comps_res = []
	
		for item in items_res:
			comps_res.append({
				'gender_age': item.find('div', class_ = 'resume-header-title').get_text(strip = True),
				'exp': item.find('span', class_ = 'resume-block__title-text resume-block__title-text_sub').get_text(strip = True),
				'salary': item.find('h2', class_ = 'bloko-header-2').get_text(strip = True),
				'edu_lang': soup_res.get_text(strip = True)
			})

			for comp in comps_res:
				if 'Мужчина' in comp['gender_age']:
					gender.append('Мужской')
					age.append(age_formating(comp['gender_age']))
					experience.append(exp_formating(comp['exp']))
					education.append(education_formating(comp['edu_lang']))
					language.append(language_formating(comp['edu_lang']))
					salary.append(salary_formating(comp['salary']))
					string_csv = 'Мужской,' + str(age_formating(comp['gender_age'])) + delimiter + str(exp_formating(comp['exp'])) + delimiter + education_formating(comp['edu_lang']) + delimiter + language_formating(comp['edu_lang']) + delimiter + str(salary_formating(comp['salary']))
					create_csv.append(string_csv)
					print('Пол: Мужской -> Возраст:', age_formating(comp['gender_age']), '-> Стаж:', exp_formating(comp['exp']), 
						'-> Образование:', education_formating(comp['edu_lang']), '-> Язык:', language_formating(comp['edu_lang']), '-> Зарплата:', salary_formating(comp['salary']))
				else:
					gender.append('Женский')
					age.append(age_formating(comp['gender_age']))
					experience.append(exp_formating(comp['exp']))
					education.append(education_formating(comp['edu_lang']))
					language.append(language_formating(comp['edu_lang']))
					salary.append(salary_formating(comp['salary']))
					string_csv = 'Женский,' + str(age_formating(comp['gender_age'])) + delimiter + str(exp_formating(comp['exp'])) + delimiter + education_formating(comp['edu_lang']) + delimiter + language_formating(comp['edu_lang']) + delimiter + str(salary_formating(comp['salary']))
					create_csv.append(string_csv)
					print('Пол: Женский -> Возраст:', age_formating(comp['gender_age']), '-> Стаж:', exp_formating(comp['exp']), 
						'-> Образование:', education_formating(comp['edu_lang']), '-> Язык:', language_formating(comp['edu_lang']), '-> Зарплата:', salary_formating(comp['salary']))
	print(f"*** {len(gender)} анкет было успешно получено ***")

""" Работа с текстом стажа """
def exp_formating(exp):
	text = exp.replace(' ', '')
	text = text[10:]
	years, months, perehodnaya, result_exp = 0, 0, '', 0
	if len(text) <= 9:
		for i in range(len(text)):
			if text[i].isdigit():
				perehodnaya += text[i]
			else:
				if len(perehodnaya) > 0:
					months = int(perehodnaya)
					perehodnaya = ''
				else:
					break
	else:
		for j in range(len(text)):
			if 'л' in text[j] or 'г' in text[j] or 'y' in text[j]:
				years = int(perehodnaya) * 12
				perehodnaya = ''
			else:
				if text[j].isdigit():
					perehodnaya += text[j]
					if 'м' in text[j] or 'm' in text[j]:
						break
		if len(perehodnaya) > 0:
			months = int(perehodnaya)
	result_exp = years + months
	if result_exp > 0:
		return result_exp
	else:
		return 'null'

""" Работа с текстом возраста """
def age_formating(age):
	age_parsing = age
	let_rus, godi_rus, year_en = age_parsing.find('лет'), age_parsing.find('год'), age_parsing.find('year')
	if let_rus != -1:
		age_parsing = age_parsing[(let_rus - 2):let_rus]
	elif godi_rus != -1:
		age_parsing = age_parsing[(godi_rus - 2):godi_rus]
	else:
		age_parsing = age_parsing[(year_en - 2):year_en]
	if age_parsing.isdigit():
		return age_parsing
	else:
		return 'null'

""" Работа с текстом зарплаты """
def salary_formating(salary_new):
	salary = salary_new.replace('\u2009', '').lower()
	currency, currency_str = ['usd', 'eur', 'rub', 'руб'], 0
	for i in range(len(currency)):
		currency_str = salary.find(currency[i])
		if currency_str != -1:
			salary = salary[:currency_str]
			break
	if i == 0:
		salary = int(salary) * 74
		salary = str(salary)
	elif i == 1:
		salary = int(salary) * 88
		salary = str(salary)
	if len(salary) < 7:
		return salary
	else:
		return 'null'

""" Работа с текстом языка """
def language_formating(set_language):
	lang = set_language
	if 'английский' in lang.lower():
		return 'Английский'
	else:
		return 'Русский'

""" Работа с текстом образования """
def education_formating(set_education):
	edu = set_education
	if 'университет' in edu.lower() or 'институт' in edu.lower() or 'university' in edu.lower():
		return 'Университет'
	elif 'колледж' in edu.lower() or 'пту' in edu.lower() or 'техникум' in edu.lower() or 'училище' in edu.lower():
		return 'Колледж'
	else:
		return 'Школа'

""" Функция для отправки результатов в базу данных """
def create_connection(host_name, user_name, user_password, db_name, table, insert):
    connection = None
    try:
        connection = mysql.connector.connect(
            host = host_name,
            user = user_name,
            password = user_password,
            database = db_name
        )
        print("*** Вы успешно подключились к базе данных ***")
    except Error as e:
        print(f"Ошибка: '{e}'")
    print(f"*** Создать таблицу '{table_name[vacancy_parser]}'? ***")
    create_table_question = input('Напишите yes/no: ')
    if 'yes' in create_table_question or 'y' in create_table_question:
        cursor = connection.cursor()
        try:
            cursor.execute(table)
            connection.commit()
            print("*** Таблица успешно создана ***")
        except Error as e:
            print(f"Ошибка: '{e}'")
    print(f"*** Внести данные в таблицу '{table_name[vacancy_parser]}'? ***")
    insert_question = input('Напишите yes/no: ')
    if 'yes' in insert_question or 'y' in insert_question:
        cursor = connection.cursor()
        for i in range(len(gender)):
            values_pars = [(links_result[i], gender[i], age[i], experience[i], education[i], language[i], salary[i])]
            cursor.executemany(insert, values_pars)
            connection.commit()
            print(f"Строка '{(i + 1)}' - занесена")
        print(f"*** Все данные были успешно добавлены в таблицу '{table_name[vacancy_parser]}' ***")
    else:
        print('*** Работа с базой данных завершена ***')

""" Функция записи CSV файла """
def csv_writer():
    path = table_name[vacancy_parser] + '.csv'
    with open(path, "w", newline='') as csv_file:
        writer = csv.writer(csv_file, delimiter = ',')
        columns = 'gender,age,experience,education,language,salary'.split(delimiter)
        writer.writerow(columns)
        for line in range(len(create_csv)):
            data = create_csv[line].split(delimiter)
            writer.writerow(data)

parse_resume()
print('*** Подключиться к базе данных? ***')
connect_question = input('Напишите yes/no: ')
if 'yes' in connect_question or 'y' in connect_question:
	create_connection('37.140.192.51', 'u1330875_admin', input('Введите пароль от базы данных: '), 'u1330875_default', hh_parser, sql)

print(f"*** Создать CSV файл с результатами? ***")
create_csv_question = input('Напишите yes/no: ')
if 'yes' in create_csv_question or 'y' in create_csv_question:
	csv_writer()
	print(f"*** Файл '{table_name[vacancy_parser]}.csv' успешно создан ")