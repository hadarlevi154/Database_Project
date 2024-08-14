from __future__ import print_function
import mysql.connector
import pymysql
import requests
import json

cnx = pymysql.connect(  host='localhost',
                        port=3305,
                        user='DbMysql35',
                        password='DbMysql35',
                        database='DbMysql35'
                      )

cursor = cnx.cursor()


###################### create tables ######################

## Info for the API ##

querystring = {"page_size":"50"}
headers = {
    'x-rapidapi-host': "data-imdb1.p.rapidapi.com",
    'x-rapidapi-key': "3e4bed8bd9mshd7585f537edbe30p166b99jsn800d1097fd15"
    }



def find_movies_ids_from_2013_till_now():

    # now we'll get the ids of movies ##

    ids = []
    for i in range(2010, 2022):

        url = "https://data-imdb1.p.rapidapi.com/movie/byYear/%d/" % (i)
        response = requests.request("GET", url, headers=headers, params=querystring)
        json_data = json.loads(response.text)
        print(json_data)

        for j in range(50): ## we want 50 movies per year
            ids.append(json_data["results"][j]["imdb_id"])

    return ids
def find_actors__ids(ids_of_movies):
    ids = ids_of_movies
    actors_id = []

    for id in ids:
        url = "https://data-imdb1.p.rapidapi.com/movie/id/%s/cast/" % (id)
        response = requests.request("GET", url, headers=headers)
        json_data = json.loads(response.text)

        if len(json_data["results"]) == 0:
            continue

        number_of_ppl = 8  ## we don't need more than that per movie
        if len(json_data["results"]["roles"]) < 8:
            number_of_ppl = len(json_data["results"]["roles"])

        for j in range(number_of_ppl):

            if (json_data["results"]["roles"][j]["role"]) != "Writer" and json_data["results"]["roles"][j]["role"] != "Director":
                actors_id.append(json_data["results"]["roles"][j]["actor"]["imdb_id"])
    return actors_id
def find_directors__ids(ids_of_movies):
    ids = ids_of_movies
    directors_id = []

    for id in ids:
        print(id)
        url = "https://data-imdb1.p.rapidapi.com/movie/id/%s/cast/" % (id)
        response = requests.request("GET", url, headers=headers)
        json_data = json.loads(response.text)
        ##print(json_data["results"]["roles"][0]["actor"]["imdb_id"])

        if len(json_data["results"]) == 0:
            continue

        for j in range(3): ## Director should be in the first one, but in case not
            if json_data["results"]["roles"][j]["role"] == "Director":
                directors_id.append(json_data["results"]["roles"][0]["actor"]["imdb_id"])
    return directors_id
def create_movie_data_table(ids_of_movies):


    cursor.execute("CREATE TABLE IF NOT EXISTS movie_data (movie_id VARCHAR(255) PRIMARY KEY, title VARCHAR(255), genre VARCHAR(255), year VARCHAR(255), length VARCHAR(255), rating VARCHAR(255))")

    ids = ids_of_movies

    for id in ids:
        print(id)
        url = "https://data-imdb1.p.rapidapi.com/movie/id/%s/" % (id)
        response = requests.request("GET", url, headers=headers)
        json_data = json.loads(response.text)
        sql = "INSERT INTO movie_data (movie_id, title, genre, year, length, rating) VALUES (%s, %s, %s, %s, %s, %s)"

        if json_data["results"]["rating"] > 6: ## We only want good movies, with rating more than 6.
            val = (id,
                    json_data["results"]["title"],
                    json_data["results"]["gen"][0]["genre"],
                    json_data["results"]["year"],
                    json_data["results"]["movie_length"],
                    json_data["results"]["rating"])
            try:
                cursor.execute(sql, val)
                cnx.commit()
            except:
                cnx.rollback()

    try:
        # SQL Statement to create an index
        sqlCreateIndex = """CREATE INDEX rating_ind ON movie_data(rating);"""
        cursor.execute(sqlCreateIndex)
    except:
        print("Index rating is already exists.")
def create_movie_description_table(ids_of_movies):
    ## Notice we have here a full text index
    cursor.execute("CREATE TABLE IF NOT EXISTS movie_description (movie_id VARCHAR(255) PRIMARY KEY, description TEXT, FULLTEXT idx (description), FOREIGN KEY (movie_id) REFERENCES movie_data(movie_id))")

    ids = ids_of_movies

    for id in ids:
        url = "https://data-imdb1.p.rapidapi.com/movie/id/%s/" % (id)
        response = requests.request("GET", url, headers=headers)
        json_data = json.loads(response.text)
        sql = "INSERT INTO movie_description (movie_id, description) VALUES (%s, %s)"

        if json_data["results"]["rating"] > 6:  ## We only want good movies, with rating more than 6.
            val = (id,
                   json_data["results"]["description"])
            try:
                cursor.execute(sql, val)
                cnx.commit()
            except:
                cnx.rollback()
def create_awards_table_and_movies_awards_table(ids_of_movies):

    cursor.execute("CREATE TABLE IF NOT EXISTS awards (award_id VARCHAR(255) PRIMARY KEY, event_name VARCHAR(255), award VARCHAR(255), nomineeORwinner VARCHAR(255))")
    cursor.execute("CREATE TABLE IF NOT EXISTS movies_awards (movie_id VARCHAR(255), award_id VARCHAR(255), PRIMARY KEY(movie_id,award_id))")

    ids = ids_of_movies
    all_movies = []
    index = 0

    # Will contain award_id and info about the award
    awards_dict = {}
    # Will contain award_id and movie_id
    movie_awards = {}

    for id in ids:
            url = "https://data-imdb1.p.rapidapi.com/movie/id/%s/awards/" % (id)
            response = requests.request("GET", url, headers=headers, params=querystring)
            json_data = json.loads(response.text)
            if json_data["count"] == 0: ### There are no awards for this movie
                continue

            bad_word = "worst" ### We don't want the awards for "worst movie" etc.


            sql_for_awards = "INSERT INTO awards (award_id, event_name, award, nomineeORwinner) VALUES (%s, %s, %s, %s)"
            sql_for_movies_awards = "INSERT INTO movies_awards (movie_id, award_id) VALUES (%s, %s)"



            for j in range(len(json_data["results"])):

                if bad_word in json_data["results"][j]["award"].lower():
                    continue

                all_movies.append((id,json_data["results"][j]["event_name"], json_data["results"][j]["award"], json_data["results"][j]["type"]))
                for item in all_movies:
                    if item[1:] in awards_dict:
                        movie_awards[item[0]] = awards_dict[item[1:]]
                    else:
                        awards_dict[item[1:]] = index
                        movie_awards[item[0]] = awards_dict[item[1:]]
                        index += 1

                val_for_awards = (awards_dict[(json_data["results"][j]["event_name"], json_data["results"][j]["award"], json_data["results"][j]["type"])],
                       json_data["results"][j]["event_name"],
                       json_data["results"][j]["award"],
                       json_data["results"][j]["type"])
                x = str(id)
                val_for_movies_awards = (id, movie_awards[x])


                try:
                    cursor.execute(sql_for_awards, val_for_awards)
                    cnx.commit()
                except:
                    cnx.rollback()

                try:
                    cursor.execute(sql_for_movies_awards, val_for_movies_awards)
                    cnx.commit()
                except:
                    cnx.rollback()


            while json_data["next"] is not None:
                url = json_data["next"]
                response = requests.request("GET", url, headers=headers, params=querystring)
                json_data = json.loads(response.text)

                sql_for_awards = "INSERT INTO awards (award_id, event_name, award, nomineeORwinner) VALUES (%s, %s, %s, %s)"
                sql_for_movies_awards = "INSERT INTO movies_awards (movie_id, award_id) VALUES (%s, %s)"

                for j in range(len(json_data["results"])):
                    print(j)
                    if bad_word in json_data["results"][j]["award"].lower():
                        continue

                    all_movies.append((id, json_data["results"][j]["event_name"], json_data["results"][j]["award"],json_data["results"][j]["type"]))
                    print(all_movies)
                    for item in all_movies:
                        if item[1:] in awards_dict:
                            movie_awards[item[0]] = awards_dict[item[1:]]
                        else:
                            awards_dict[item[1:]] = index
                            movie_awards[item[0]] = awards_dict[item[1:]]
                            index += 1

                    val_for_awards = (awards_dict[(json_data["results"][j]["event_name"],
                                                   json_data["results"][j]["award"], json_data["results"][j]["type"])],
                                      json_data["results"][j]["event_name"],
                                      json_data["results"][j]["award"],
                                      json_data["results"][j]["type"])
                    x = str(id)
                    val_for_movies_awards = (id, movie_awards[x])


                    try:
                        cursor.execute(sql_for_awards, val_for_awards)
                        cnx.commit()
                    except:
                        cnx.rollback()

                    try:
                        cursor.execute(sql_for_movies_awards, val_for_movies_awards)
                        cnx.commit()
                    except:
                        cnx.rollback()


    try:
        # SQL Statement to create an index
        sqlCreateIndex = """CREATE INDEX event_ind ON awards(event_name);"""
        cursor.execute(sqlCreateIndex)
        sqlCreateIndex2 = ("""CREATE INDEX nominee_or_winner_ind
        ON awards (nomineeORwinner)""")
        cursor.execute(sqlCreateIndex2)
    except:
        print("Index rating is already exists.")
def create_cast_table(ids_of_movies):

    cursor.execute("CREATE TABLE IF NOT EXISTS cast (person_id VARCHAR(255), movie_id VARCHAR(255), role VARCHAR(255), PRIMARY KEY(person_id, movie_id), FOREIGN KEY (movie_id) REFERENCES movie_data(movie_id), FOREIGN KEY (person_id) REFERENCES person_data(person_id))")

    ids = ids_of_movies

    for id in ids:
        url = "https://data-imdb1.p.rapidapi.com/movie/id/%s/cast/" % (id)
        response = requests.request("GET", url, headers=headers)
        json_data = json.loads(response.text)

        sql = "INSERT INTO cast (person_id, movie_id, role) VALUES (%s, %s, %s)"

        if len(json_data["results"]) == 0:
            continue

        number_of_ppl = 8 ## we don't need more than that per movie
        if len(json_data["results"]["roles"]) < 8:
            number_of_ppl = len(json_data["results"]["roles"])

        for j in range(number_of_ppl):

            if json_data["results"]["roles"][j]["role"] == "Writer":
                continue

            val = (
                   json_data["results"]["roles"][j]["actor"]["imdb_id"],
                   id,
                   json_data["results"]["roles"][j]["role"])
            try:
                cursor.execute(sql, val)
                cnx.commit()
            except:
                cnx.rollback()


    try:
        # SQL Statement to create an index
        sqlCreateIndex = ("""CREATE INDEX role_index
        ON cast (role)""")
        cursor.execute(sqlCreateIndex)
    except:
        print("Index rating is already exists.")
def create_director_data_table(directors_id):

    cursor.execute("CREATE TABLE IF NOT EXISTS director_data (director_id VARCHAR(255) PRIMARY KEY, full_name VARCHAR(255), FOREIGN KEY (director_id) REFERENCES person_data(person_id))")

    ids = directors_id

    for id in ids:
        url = "https://data-imdb1.p.rapidapi.com/actor/id/%s/" % (id)
        response = requests.request("GET", url, headers=headers)
        json_data = json.loads(response.text)

        sql = "INSERT INTO director_data (director_id, full_name) VALUES (%s, %s)"

        val = (id,
               json_data["results"]["name"])
        try:
            cursor.execute(sql, val)
            cnx.commit()
        except:
            cnx.rollback()
def create_actor_data_table(actors_ids):

    cursor.execute("CREATE TABLE IF NOT EXISTS actor_data (actor_id VARCHAR(255) PRIMARY KEY, full_name VARCHAR(255), age VARCHAR(255), FOREIGN KEY (actor_id) REFERENCES person_data(person_id))")

    ids = actors_ids

    for id in ids:

        url = "https://data-imdb1.p.rapidapi.com/actor/id/%s/" % (id)
        response = requests.request("GET", url, headers=headers, params=querystring)
        json_data = json.loads(response.text)

        age = "Null"
        sql = "INSERT INTO actor_data (actor_id, full_name, age) VALUES (%s, %s, %s)"
        if json_data["results"]["birth_date"] is not None:
            if len(json_data["results"]["birth_date"]) <= 1:
                age = "Null"
            else:
                age = 2022 - int(json_data["results"]["birth_date"][0:4])

        val = (id,
               json_data["results"]["name"],
               str(age))
        try:
            cursor.execute(sql, val)
            cnx.commit()
        except:
            cnx.rollback()

    try:
        # SQL Statement to create an index
        sqlCreateIndex = """CREATE INDEX actor_ind ON actor_data(age);"""
        cursor.execute(sqlCreateIndex)
    except:
        print("Index rating is already exists.")
def create_person_data_table(actor_ids, director_ids):

    person_ids = actor_ids + director_ids

    cursor.execute("CREATE TABLE IF NOT EXISTS person_data (person_id VARCHAR(255) PRIMARY KEY)")

    for id in person_ids:

        sql = "INSERT INTO person_data (person_id) VALUES (%s)"

        val = (id)
        try:
            cursor.execute(sql, val)
            cnx.commit()
        except:
            cnx.rollback()



def main():
    ### Variables for the different creation of tables.

    #ids_of_movies = find_movies_ids_from_2013_till_now()
    #director_ids = find_directors__ids(ids_of_movies)
    #actor_ids = find_actors__ids(ids_of_movies)

    ### Creation of all tables

    #create_movie_data_table(ids_of_movies)
    #create_person_data_table(director_ids, actor_ids)
    #create_actor_data_table(actor_ids)
    #create_director_data_table(director_ids)
    #create_cast_table(ids_of_movies)
    #create_movie_description_table(ids_of_movies)
    #create_awards_table_and_movies_awards_table(ids_of_movies)

    cursor.close()
    cnx.close()


#main()
