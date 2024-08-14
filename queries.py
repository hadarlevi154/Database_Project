from __future__ import print_function
import re
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


####### query for best movies in genre with high rating #######
def find_best_movie_with_high_rating_and_according_to_genre():

    query_of_genre_rating = ("""SELECT  DbMysql35.movie_data.genre, COUNT(*) AS number_of_movies
FROM DbMysql35.movie_data
WHERE DbMysql35.movie_data.year >=2017
AND DbMysql35.movie_data.rating>=7
GROUP BY DbMysql35.movie_data.genre
ORDER BY COUNT(*) DESC
LIMIT 3
""")

    try:
        cursor.execute(query_of_genre_rating)
        cnx.commit()
    except:
        cnx.rollback()

    rows = cursor.fetchall()
    for row in rows:
        print(row)

    # cursor.close()
    # cnx.close()
######## query to find best movies from spec genre #######
def find_best_movies_with_spec_genre():

    genres_example = ['Adventure', 'Family', 'Fantasy', 'Crime', 'Drama', 'Comedy', 'Thriller']

    ####### input from user of genre #######
    success = 0
    while success == 0:
        print("This is a list of possible genres:")
        print(genres_example)
        genre = input("Please choose a genre from that list to your film. Use a capital letter.")
        if genre not in genres_example:
            print("Not a valid genre. Please try again!")
        else:
            success = 1


    query_of_best_movie_from_spec_gen=(""" SELECT DbMysql35.movie_data.title
FROM DbMysql35.movie_data as movie_data
WHERE movie_data.rating >=7.0
      AND movie_data.genre = %s
      AND movie_data.movie_id IN(
           SELECT DbMysql35.movies_awards.movie_id
			FROM DbMysql35.movies_awards
            INNER JOIN DbMysql35.awards ON DbMysql35.movies_awards.award_id = DbMysql35.awards.award_id
            WHERE DbMysql35.awards.nomineeORwinner = "Winner"
            GROUP BY DbMysql35.movies_awards.movie_id
            HAVING COUNT(*)>=3)
ORDER BY  movie_data.title
LIMIT 0,10""")

    cursor.execute(query_of_best_movie_from_spec_gen, genre)
    cnx.commit()
    rows = cursor.fetchall()
    for row in rows:
        print(row)

    # cursor.close()
    # cnx.close()
####### query to find avg length of movies from the same genre #######
def find_avg_length_of_movies():

    query_of_length = ("""SELECT DbMysql35.movie_data.genre, AVG(DbMysql35.movie_data.length) as Average_movie_length_in_minutes
    FROM DbMysql35.movie_data AS movie_data
    WHERE movie_data.rating >= 7
    AND movie_data.movie_id IN(
    SELECT DbMysql35.movies_awards.movie_id as movie_id
    FROM DbMysql35.movies_awards as movies_awards
    INNER JOIN DbMysql35.awards ON movies_awards.award_id = DbMysql35.awards.award_id
    GROUP BY movie_id
    HAVING COUNT(*)>=3
    )
    GROUP BY movie_data.genre
    ORDER BY  movie_data.genre""")


    cursor.execute(query_of_length)
    cnx.commit()


    rows = cursor.fetchall()
    for row in rows:
        print(row)

####### query to find best director #######
def find_best_director():
    query_of_best_director = ("""SELECT DbMysql35.director_data.full_name
    FROM DbMysql35.director_data
    INNER JOIN DbMysql35.cast ON DbMysql35.director_data.director_id=DbMysql35.cast.person_id
    INNER JOIN DbMysql35.movie_data ON DbMysql35.cast.movie_id = DbMysql35.movie_data.movie_id
    WHERE DbMysql35.cast.role = "Director" AND
    DbMysql35.cast.person_id IN (
    SELECT DbMysql35.cast.person_id AS id
    FROM DbMysql35.cast
    GROUP BY id
    HAVING COUNT(*)>=5)

    GROUP BY DbMysql35.director_data.director_id
    ORDER BY avg(movie_data.rating)
    LIMIT 5""")

    cursor.execute(query_of_best_director)
    cnx.commit()
    rows = cursor.fetchall()
    for row in rows:
        print(row)

####### query to find age range for actors #######
def find_actors_according_to_age():

    success = 0
    while success ==0:
        min_age = float(input("Please choose a number from 1 to 120 for minimum age:"))
        max_age = float(input("Please choose a number from 1 to 120 for maximum age:"))

        if min_age < 1 or min_age > 120:
            print("Min age is not valid")
            continue
        if max_age < 1 or max_age > 120:
            print("Max age is not valid")
            continue
        else:
            success = 1

    query_of_age = ("""SELECT DbMysql35.actor_data.full_name
FROM DbMysql35.actor_data AS actor_data
WHERE actor_data.age BETWEEN %s AND %s
AND actor_data.actor_id IN (
	SELECT DbMysql35.cast.person_id
	FROM DbMysql35.cast 
	INNER JOIN DbMysql35.movies_awards ON DbMysql35.cast.movie_id =  DbMysql35.movies_awards.movie_id
	INNER JOIN DbMysql35.awards ON DbMysql35.movies_awards.award_id = DbMysql35.awards.award_id
	WHERE DbMysql35.awards.nomineeORwinner = "Winner"
	GROUP BY DbMysql35.cast.person_id
	HAVING COUNT(*)>=3
	ORDER BY COUNT(*) DESC)
LIMIT 0,20""")



    cursor.execute(query_of_age, (min_age, max_age))
    cnx.commit()


    rows = cursor.fetchall()
    for row in rows:
        print(row)

####### query of best movies and actors by event #######
def find_best_movies_by_spec_event():

    ### input from user of event ###
    success = 0
    while success == 0:
        list_of_events = ["Golden Globes, USA", "BAFTA Awards", "Academy Awards, USA", "Kids' Choice Awards, USA"]
        print(list_of_events)
        event = input("Which event from the above is your dream goal? Choose from the list above")
        if event not in list_of_events:
            print("Not a valid event. Please try again! Use a capital letter in every word")
        else:
            success = 1

    query_of_best_by_event = ("""SELECT DbMysql35.movie_data.title AS movie_name
    FROM DbMysql35.movie_data as movie_data
    WHERE movie_data.movie_id IN (
    SELECT DbMysql35.movies_awards.movie_id
    FROM DbMysql35.movies_awards as movies_awards
    INNER JOIN DbMysql35.awards ON movies_awards.award_id = DbMysql35.awards.award_id
    WHERE DbMysql35.awards.event_name = %s
    AND DbMysql35.awards.nomineeORwinner ="Winner"
    GROUP BY movies_awards.movie_id
    ORDER BY COUNT(*) DESC)
    LIMIT 0,5""")


    cursor.execute(query_of_best_by_event, event)
    cnx.commit()

    rows = cursor.fetchall()
    for row in rows:
        print(row)

####### full text query for searching keywords in description #######
def find_key_words_in_descr():

    print("Hello my friend!")
    print("")
    success = 0
    while success==0:
        try:
            number_of_key_words = int(input("How many key words would you like to give? Notice that we only accept one word each time."))
            print("")
            print("You chose the number:", number_of_key_words)
            print("")
            success = 1
        except ValueError:
            print("This is not a valid number, please try again!")

    count = 0
    key_words = []
    while count < number_of_key_words:
        key_word = input("please enter a single key word:")
        if re.match(r'\A[\w-]+\Z', key_word):
            count+=1
            key_words.append(key_word)
            print("This is word number:", count,".", "There are", number_of_key_words-count, "words left.")
            print("")
        else:
            print("Sorry, this is not a valid single word. Please try again and remember- we only accept a single word!")

    print("The key words you chose are:", key_words)
    print("")

    for i in range(len(key_words)):
        query_for_search_in_descr = ("""SELECT movie_data.title, movie_desc.description 
FROM DbMysql35.movie_description AS movie_desc
INNER JOIN DbMysql35.movie_data AS movie_data ON movie_desc.movie_id = movie_data.movie_id
WHERE MATCH(description) AGAINST(%s)
ORDER BY movie_data.title""")


        cursor.execute(query_for_search_in_descr,key_words[i])
        print("The word", "'", key_words[i], "'", "is at the following movies:")
        cnx.commit()

        rows = cursor.fetchall()
        for row in rows:
            print(row)

def main():
    print("*********")
    #find_best_movie_with_high_rating_and_according_to_genre()
    # print("*********")
    #find_best_movies_with_spec_genre()
    # print("*********")
    #find_avg_length_of_movies()
    # print("*********")
    #find_best_director()
    # print("*********")
    #find_actors_according_to_age()
    # print("*********")
    #find_best_movies_by_spec_event()
    # print("*********")
    #find_key_words_in_descr()
    # print("*********")

main()

cursor.close()
cnx.close()