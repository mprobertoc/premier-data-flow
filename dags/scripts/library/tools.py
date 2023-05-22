import pandas as pd
from pandas.io.json import json_normalize
import string
import numpy as np
import json
import http.client
import time
import os
from scrapy.selector import Selector
import requests
from datetime import date, timedelta
from fuzzywuzzy import fuzz
import boto3


def identifier():

    """lee el archivo que contiene los equipos oficiales de la liga (teams.csv) 
    y el que entrega la informacion de cuales son los siguientes partidos de la semana (next_fixtures.csv).
    
    la libreria fuzzywuzzy realiza una comparacion de las cadenas de texto de los equipos del fixture (next_fixtures.csv) con la de los equipos en la API (teams.csv),
    para que el API logre interpretar la referencia de equipo con diferencias de escritura, ejemplo: Wolves - Wolverhampton Wanderers.
   
    se asignan los valores correspondientes de ids a cada equipo y se genera el archivo para solicitar datos (next_fixtures_with_ids.csv)
    """

    api_data = pd.read_csv('/opt/airflow/dags/data/operations/teams.csv')
    web_data = pd.read_csv('/opt/airflow/dags/data/operations/next_fixtures.csv')

    api_teams = api_data["team_name"].to_list()
    web_home_teams = web_data["team_home"].to_list()
    web_away_teams = web_data["team_away"].to_list()


    target_home_teams = list()
    target_positions_home = list()
    #comparacion
    for api_team in api_teams:
        for web_home_team in web_home_teams:
            comparator = fuzz.partial_ratio(api_team, web_home_team)
            if comparator >= 83:
                target_home_teams.append(api_team)
                target_positions_home.append(web_home_teams.index(web_home_team))

               
    target_away_teams = list()
    target_positions_away = list()
    for api_team in api_teams:
        for web_away_team in web_away_teams:
            comparator = fuzz.partial_ratio(api_team, web_away_team)
            if comparator >= 83:
                target_away_teams.append(api_team)
                target_positions_away.append(web_away_teams.index(web_away_team))
    

    home_update = api_data[api_data['team_name'].isin(target_home_teams)] 
    home_update["position"] = target_positions_home
    
    away_update = api_data[api_data['team_name'].isin(target_away_teams)]
    away_update["position"] = target_positions_away 

    home_ordered = home_update[["team_id", "team_name", "position"]].sort_values("position")
    away_ordered = away_update[["team_id", "team_name", "position"]].sort_values("position")

    merged = pd.merge(home_ordered, away_ordered, on="position",suffixes=('_home', '_away') )

    fixture_file = merged.to_csv('/opt/airflow/dags/data/operations/next_fixtures_with_ids.csv')
                
    return  fixture_file 

def fixture_file():
    
    """la funcion toma de la url las fechas de lox proximos partidos, limpia los datos y los divide en pedazos que seran 
    utilizados para crear un dafaframe con los nombres de columna preestablecidos, existe la posibilidad de que la cantidad de datos 
    obtenidos no sean suficientes o excedan el criterio de su creacion por lo que se tienen dos opciones en un try-except para cada caso.
    Luego de tener el dataframe se filtra y se dejan solo los partidos que se jueguen 'mañana', 'pospuestos', y los siguientes que son
    delimitados hasta el proximo domingo mas cercano, luego se guarda en un archivo .csv llamado next_fixtures"""
    
 




    req = requests.get("https://onefootball.com/en/competition/premier-league-9/fixtures")

    resp = req.content

    sel = Selector( text = resp )

    xdata = sel.xpath('//div[contains(@class, "simple-match-card__content")]//*/text()')

    lista_data = xdata.extract()

    striped = []

    for i in lista_data:

        k = i.translate({ord(c): None for c in string.whitespace})
        striped.append(k)


    hits = (i for i,value in enumerate(striped) if value == 'Postponed')

    for i in hits:
        striped.insert((i+1), '')

    chunks = np.array_split(striped, (len(striped))/6)


    try:

        web_data = pd.DataFrame(chunks, columns=['team_home', 'score_home', 'team_away', 'score_away','schedule', 'status'])

    except ValueError:
            
        web_data = pd.DataFrame(chunks, columns=['team_home', 'score_home', 'team_away', 'score_away','schedule', 'status', 'fix_column'])



    filtered = web_data[web_data['schedule'].str.contains('Tomorrow|Postponed|/\d\d/')]

    today = date.today()

    # Obtener el número del día de la semana de hoy
    today_weekday = today.weekday()


    # Obtener la fecha del próximo domingo
    if today_weekday == 6:  # Si hoy es domingo

        next_sunday = today + timedelta(days=7)
    else:
        next_sunday = today + timedelta(days=6 - today_weekday)
    

    # Convertir la fecha del próximo domingo a formato de cadena
    next_sunday_str = next_sunday.strftime('%d/%m/%Y')
    today_str = today.strftime('%d/%m/%Y')

    tomorrow = date.today() + timedelta(days=1)
    tomorrow_str = tomorrow.strftime('%d/%m/%Y')

    # Convertir 'Tomorrow' en la fecha de mañana
    filtered['schedule'] = filtered['schedule'].apply(lambda x: tomorrow_str if x == 'Tomorrow' else x)


    # Filtrar el dataframe
    filtered_to_sunday = filtered.loc[(filtered['schedule'] > today_str) & (filtered['schedule'] <= next_sunday_str)]


    return filtered_to_sunday.to_csv('/opt/airflow/dags/data/operations/next_fixtures.csv')

    # return file_filtered

class data_access:

    def __init__(self, host:str, key:str, aws_key:str, aws_id:str, region:str, bucket:str, season, league:int, folder_name):

        #credenciales API
        self.host = host
        self.key = key

        #Archivo de datos
        self.folder_name = folder_name

        #Temporada y liga
        self.season = season
        self.league = league

        #Credenciales AWS
        self.aws_key = aws_key
        self.aws_id = aws_id
        self.region = region
        self.bucket = bucket


    
    
 
    def teams_json(self): 
           
            """ Conecta con la API para extraer el nombre de los equipos que 
            conforman una liga de clubs de futbol en formato JSON  """



            headers = {'x-rapidapi-host': self.host,'x-rapidapi-key': self.key}
    
            conn = http.client.HTTPSConnection("v3.football.api-sports.io")
        
            #url
            url="/teams?season={}&league={}".format(self.season, self.league)

            
            
            #request
            conn.request("GET", url, headers=headers)
            
            #response
            res = conn.getresponse()
            data = res.read().decode("utf-8")
        
            #create file per team
            jsonData = json.dumps(data)
            jsonFile = open("/opt/airflow/dags/data/operations/teams.json", "w")
            jsonFile.write(jsonData)
            jsonFile.close()

    def teams_csv(self):

        """ La función teams_csv() abre el archivo JSON que contiene información de equipos de una liga de Futbol, 
        extrae el identificador(id) y nombre de los equipos, normaliza los datos y los exporta a un archivo CSV. """



        with open("/opt/airflow/dags/data/operations/teams.json", 'r') as f:


                    json_load_str = json.load(f)
                    json_load_dict = json.loads(json_load_str)
                    response_json = json_load_dict["response"]
                    normalize_data = pd.json_normalize(response_json, max_level=4,  sep='_') 

                    teams_csv = normalize_data[["team_id", "team_name"]]

                    teams_csv.to_csv("/opt/airflow/dags/data/operations/teams.csv")
                    
    def teams_stats_json(self):
            

            """ obtiene la estadística de los equipos seleccionados en formato 
            JSON a partir de los ids de los equipos. 

            Para cada equipo, se genera un archivo JSON que contiene su estadística dentro del directorio local, 
            para ello, primero lee un archivo CSV que contiene los ids de los proximos partidos ('next_fixtures_with_ids.csv'),
            crea el directorio necesario para almacenar los archivos JSON, establece los encabezados para la solicitud HTTP para cada equipo, 
            procesa la respuesta, crea un archivo JSON por cada equipo y lo guarda en el directorio correspondiente.
            
            La función incluye una pausa de 8 segundos entre cada 
            solicitud HTTP para no sobrepasar el limite de requests."""

            #ES FACTIBLE O DEBE CREARSE EL DIRECTORIO CON OTRA TAREA?

            read_file = pd.read_csv('/opt/airflow/dags/data/operations/next_fixtures_with_ids.csv') 

            row_ids = read_file.loc[:, ['team_id_home','team_id_away']].values.flatten().tolist()

            row_teams = read_file.loc[:, ['team_name_home','team_name_away']].values.flatten().tolist()

            

            directory = '{}'.format(self.folder_name)
            parent_directory = '/opt/airflow/dags/data/resources'
            path_1 = os.path.join(parent_directory, directory)
            os.mkdir(path_1)


            api_directory = 'json_files'
            api_parent_directory = '/opt/airflow/dags/data/resources/{}'.format(directory)
            path_2 = os.path.join(api_parent_directory, api_directory)
            os.mkdir(path_2)


            headers = {'x-rapidapi-host': self.host,'x-rapidapi-key': self.key}


            for id, team in zip(row_ids, row_teams):
                
                
                conn = http.client.HTTPSConnection("v3.football.api-sports.io")
                
                
                
                #url
                url="/teams/statistics?season={}&team={}&league={}".format(self.season,id, self.league)

            
                
                #request
                conn.request("GET", url, headers=headers)
                
                #response
                res = conn.getresponse()
                data = res.read().decode("utf-8")
            
                #create file per team
                jsonData = json.dumps(data)
                jsonFile = open("{}/{}/{}.json".format(api_parent_directory, api_directory, team), "w")
                jsonFile.write(jsonData)
                jsonFile.close()

                time.sleep(8)
           
    def teams_stats_csv(self):

            """ Esta función lee los archivos JSON creados por la función teams_stats_json(), 
            los combina y normaliza la data para guardarla en un archivo CSV por cada fixture (2 equipos).
            
            Primero, la función crea un directorio para guardar los archivos CSV. Luego, itera sobre 
            los equipos emparejados que se encuentran en el archivo next_fixtures_with_ids.csv.

            Para cada emparejamiento, la función crea un sub directorio y un archivo CSV. La función carga 
            los archivos JSON para cada equipo, normaliza la data, la combina en un DataFrame y filtra por los equipos emparejados.

            """

             

            read_file = pd.read_csv('/opt/airflow/dags/data/operations/next_fixtures_with_ids.csv') 


            row_teams = read_file.loc[:, ['team_name_home','team_name_away']].values.flatten().tolist()


            paired_teams = list(map(lambda x: [row_teams[x], row_teams[x+1]], range(0, len(row_teams), 2))) 


            csv_directory = 'csv_files'
            parent_directory = '/opt/airflow/dags/data/resources/{}/'.format(self.folder_name)
            path = os.path.join(parent_directory, csv_directory)
            os.mkdir(path)

           


            for par in paired_teams:#
           

                match_directory = '{}_{}'.format(par[0], par[1])
                api_parent_directory = '/opt/airflow/dags/data/resources/{}/csv_files/'.format(self.folder_name)
                path_2 = os.path.join(api_parent_directory, match_directory)
                os.mkdir(path_2)

                
                match_directory = 'teams'
                api_parent_directory = '/opt/airflow/dags/data/resources/{}/csv_files/{}_{}/'.format(self.folder_name, par[0], par[1])
                path_2 = os.path.join(api_parent_directory, match_directory)
                os.mkdir(path_2)



            response_json_list = list()

            for team in row_teams:
                
            #list all teams 
        
                with open('/opt/airflow/dags/data/resources/{}/json_files/{}.json'.format(self.folder_name, team), 'r') as f:
                
                        json_load_str = json.load(f)
                        json_load_dict = json.loads(json_load_str)

                        response_json = json_load_dict["response"]
                        response_json_list.append(response_json)

                #normalize and concatenate

            concat_data_norm = pd.DataFrame()

    


          
            for i in response_json_list:

                normalize_data = pd.json_normalize(i, max_level=4,  sep='_') 
                concat_data_norm = pd.concat([concat_data_norm, normalize_data], ignore_index=True)

            
            team_files = []

            for team_pair in paired_teams:

                teams_filter = concat_data_norm[concat_data_norm['team_name'].isin(team_pair)]
                file_name = f"{team_pair[0]}_{team_pair[1]}_teams.csv"
                dir_path = f"/opt/airflow/dags/data/resources/{self.folder_name}/csv_files/{team_pair[0]}_{team_pair[1]}/teams"
                os.makedirs(dir_path, exist_ok=True)
                file_path = f"{dir_path}/{file_name}"
                teams_filter.to_csv(file_path, index=False)
                team_files.append(file_path)
            
            return team_files

    def players_stats_json(self):

        """lee el archivo "next_fixtures_with_ids" de donde se obtienen las listas de equipos(nombres) y las de sus ids (identificadores).

        Se crea un diccionario que relaciona ambas listas. 
        
        Luego, utiliza el API para obtener información de 
        jugadores de cada equipo en el diccionario y guardarla en archivos JSON por equipo en su carpeta correspondiente.

        incluye un tiempo de espera de 8 segundos entre cada llamada a la API para no sobrepasar el limite de requests."""
        
        
        read_file = pd.read_csv('/opt/airflow/dags/data/operations/next_fixtures_with_ids.csv') 
   

        row_ids = read_file.loc[:, ['team_id_home','team_id_away']].values.flatten().tolist()

        row_teams = read_file.loc[:, ['team_name_home','team_name_away']].values.flatten().tolist()

        dictionary = dict(zip(row_teams, row_ids))
    

        headers = {'x-rapidapi-host': self.host,'x-rapidapi-key': self.key}


            
        conn = http.client.HTTPSConnection(self.host)
        
            
        for team, id in dictionary.items():

            
            url="/players?league={}&season={}&team={}".format(self.league, self.season, id)
            
            
     
            conn.request("GET", url, headers=headers)
            
  
            res = conn.getresponse()
            data = res.read().decode("utf-8")
     
            jsonData = json.dumps(data)
            jsonFile = open('/opt/airflow/dags/data/resources/{}/json_files/{}_players.json'.format(self.folder_name, team), "w")
            jsonFile.write(jsonData)
            jsonFile.close()
            time.sleep(8)

    def players_stats_csv(self):


        """lee el archivo CSV ('next_fixtures_with_ids.csv') que contiene información sobre los siguientes partidos,
        luego crea una lista de parejas de equipos (fixture) y crea una carpeta para cada par de equipos. 
        
        La función luego carga datos de un archivo JSON para cada equipo y los normaliza para crear 
        dos marcos de datos (players y stats) que los une finalmente en un solo dataframe. 
        
        Luego, para cada par de equipos que conforman el partido, se crea un archivo csv con los datos mencionados
       
        Finalmente, la función devuelve una lista con las rutas de archivo para crear cada archivo CSV."""



        read_file = pd.read_csv('/opt/airflow/dags/data/operations/next_fixtures_with_ids.csv') 


        row_teams = read_file.loc[:, ['team_name_home','team_name_away']].values.flatten().tolist()


        paired_teams = list(map(lambda x: [row_teams[x], row_teams[x+1]], range(0, len(row_teams), 2)))

        for par in paired_teams:
             
            match_directory = 'players'
            api_parent_directory = '/opt/airflow/dags/data/resources/{}/csv_files/{}_{}/'.format(self.folder_name, par[0], par[1])
            path_2 = os.path.join(api_parent_directory, match_directory)
            os.mkdir(path_2)



   
        response_json_list = list()
        players = pd.DataFrame()
        stats = pd.DataFrame()


        for team in row_teams:

            

            with open('/opt/airflow/dags/data/resources/{}/json_files/{}_players.json'.format(self.folder_name, team), 'r') as f:
            
                json_load_str = json.load(f)
                json_load_dict = json.loads(json_load_str)
                response_json = json_load_dict["response"]
                response_json_list.append(response_json)

                
        
        for index in range(0, len(response_json_list)):

            for i in response_json_list[index]:

                normalize_players= pd.json_normalize(i["player"], max_level=4, sep='_')
                nomalize_stats = pd.json_normalize(i["statistics"][0], max_level=4, sep='_')



                players = pd.concat([players,  normalize_players], ignore_index=True)
                stats = pd.concat([stats,  nomalize_stats], ignore_index=True)

        players_stats = pd.merge(players, stats, left_index=True, right_index=True)
       

        player_files = []

        for team_pair in paired_teams:



            print(f"Creating file for teams: {team_pair}")
            teams_filter = players_stats[players_stats['team_name'].isin(team_pair)]
            file_name = f"{team_pair[0]}_{team_pair[1]}_players.csv"
            dir_path = f"/opt/airflow/dags/data/resources/{self.folder_name}/csv_files/{team_pair[0]}_{team_pair[1]}/players"
            os.makedirs(dir_path, exist_ok=True)
            file_path = f"{dir_path}/{file_name}"
            teams_filter.to_csv(file_path, index=False)
            player_files.append(file_path)
        
        return player_files
        
    def to_aws_bucket(self):

        """busca el archivo de la ruta especificada y lo sube al bucket premier-league-stats, con sus subcarpetas y archivos json y csv. """

        path = f"/opt/airflow/dags/data/resources/{self.folder_name}"

        session = boto3.Session(
            aws_access_key_id=self.aws_id,
            aws_secret_access_key=self.aws_key,
            region_name=self.region)



        s3 = session.resource('s3')
        bucket = s3.Bucket(f'{self.bucket}')

        bucket.put_object(Key=f"{self.folder_name}/")
    
        for subdir, dirs, files in os.walk(path):
            for file in files:
                full_path = os.path.join(subdir, file)
                with open(full_path, 'rb') as data:
                    key = os.path.relpath(full_path, path)
                    key = f"{self.folder_name}/{key}"
                    bucket.put_object(Key=key, Body=data)
    
def sensor():

    """ corrobora que la estructura de la pagina sea la correcta al momento de solicitar los datos en el servidor """

    req = requests.get("https://onefootball.com/en/competition/premier-league-9/fixtures")

    resp = req.content

    sel = Selector( text = resp )

    xdata = sel.xpath('//div[contains(@class, "simple-match-card__content")]//*/text()')


    lista_data = xdata.extract()

    striped = []

    for i in lista_data:

        k = i.translate({ord(c): None for c in string.whitespace})
        striped.append(k)


    hits = (i for i,value in enumerate(striped) if value == 'Postponed')

    for i in hits:
        striped.insert((i+1), '')

    chunks = np.array_split(striped, (len(striped))/6)

    try:

        df = pd.DataFrame(chunks, columns=['team_home', 'score_home', 'team_away', 'score_away','schedule', 'status'])

    except:

        df = pd.DataFrame(chunks, columns=['team_home', 'score_home', 'team_away', 'score_away','schedule', 'status', 'fix_column'])




    df_filtered = df[df['schedule'].str.contains('Tomorrow|Postponed|/\d\d/')]


    schedules = df_filtered["schedule"].values
    #check characters
    checker = []

    for schedule in schedules:
        if ('/' in schedule[2]) or ('Postponed' in schedule) or ('Tomorrow' in schedule):
            checker.append(True)

    checker_checker = []

    for i in checker:
        checker_checker.append(True)


    if (checker == checker_checker):

        return True  

    else:
        return False 

def season_number():
    
    """ extrae de la url el numero de temporada actual de futbol, 
    si la pagina no responde a la solicitud se generara un nombre generico que indique que no hay datos, 
    """

    try:
        req = requests.get("https://www.eurosport.com/football/premier-league/calendar-results.shtml")

        resp = req.content

        sel = Selector( text = resp )

        xdata = sel.xpath('//*[@id="content"]/div/div[2]/div/div[1]/h1/text()')

        splited = xdata.extract()[0].split()

        season = splited[-1][0:4]

        return season
    
    except:
       
        return "no_season_data_"


