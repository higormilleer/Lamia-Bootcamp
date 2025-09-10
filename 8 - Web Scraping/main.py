from bs4 import BeautifulSoup
import requests 
import time

#abrir o arquivo html apenas para leitura e apos, e impresso o q esta escrito no arquivo html
with open("home.html","r") as html_file:
content = html_file.read()
print(content)

#imprimirá a mesma coisa que a anterior, mas o lxml e mais eficiente para documentos maiores
soup = BeautifulSoup(content,"lxml")
print(soup.prettify())

#print de todos o h5 
tags = soup.find_all("h5")
print(tags)

#Imprime o nome dos cursos 
courses_html_tags = soup.find_all("h5")
for course in courses_html_tags:
    print(course.text)

#Encontra todos os elementos <div> com a classe "card" na página HTML
course_cards = soup.find_all("div", class_="card")
for course in course_cards:
     #Extrai o nome do curso, que está dentro da tag <h5>
    course_name = course.h5.text
    #Faz a mesma coisa que o anterior, mas divide o texto para pegar o ultimo elemento(neste caso o preco)
    course_price = course.a.text.split()[-1]

    print(f"{course_name} costs {course_price}")

#--------------------------------------------------------------------------------------------------#

def find_jobs():
    html_text = requests.get("https://g1.globo.com/").text #Vai no site
    soup = BeautifulSoup(html_text, "lxml") #Junta o HTML com a biblioteca BeutifulSouop
    #jobs = soup.find_all("div", class_="feed-post-body") #procura todos os div q tiver a class com este nome
    #print(jobs)

    jobs = soup.find_all("div", class_="glb-grid")
    for job in jobs: 
        print('Put some skill that you are not familiar with')
        unfamiliar_skill = input('>')
        print(f'Filtering out {unfamiliar_skill}')
        # Tenta encontrar o parágrafo e o título conforme os setores especificados, onde o recplace e utilizado para deixar apenas o texto da materia e o strip que remove espaços extras no início e no final do texto 
        job_date = job.find("span", class_="feed-post-datetime").text.strip()
        if 'few' in job_date:
            company_name = job.find("div", class_="feed-post-header with-post-chapeu").text.replace("","")
            notice = job.find("div", class_="feed-post-body-title gui-color-primary gui-color-hover").text.replace("","")
            #procura o link da materia
            links = job.h2.a["href"]
            if unfamiliar_skill not in notice:
                with open("posts/{index}.txt', 'w'") as f:
                    f.write(f"Company Name: {company_name.strip()}\n - Notice: {notice.strip()}\n - Publication Date: {job_date}\n - More Info: {links}\n")
               
                print(f'file saved {index}')

if __name__  == "__main__":
    while True:
        find_jobs()
        time_wait = 10
        print(f"Waiting{time_wait} seconds...")
        time.sleep(time_wait*60) 
    
