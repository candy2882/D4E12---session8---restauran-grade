import pymysql
from pymongo import MongoClient

mongo_client = MongoClient()
movie_db = mongo_client.get_database('mysql_ex')
restaurants_collection = movie_db.get_collection('restaurants')

mysql_client = pymysql.connect(
    host = 'localhost',
    user = 'root',
    password = 'Minhthy_2882',
    cursorclass = pymysql.cursors.DictCursor
)


cursor = mysql_client.cursor()

# cursor.execute('CREATE DATABASE d4e12')
cursor.execute(
    '''
    CREATE TABLE IF NOT EXISTS d4e12.restaurant(
        _id VARCHAR(255) PRIMARY KEY,
        building VARCHAR(255),
        zipcode VARCHAR(255),
        street VARCHAR(255),
        borough VARCHAR(45),
        cuisine VARCHAR(255)
    )
    '''
)

cursor.execute(
    '''
    CREATE TABLE IF NOT EXISTS d4e12.grade(
        id INT AUTO_INCREMENT,
        restaurant_id VARCHAR(255) ,
        grade VARCHAR(45),
        score VARCHAR(45),
        PRIMARY KEY(id)
    )
    '''
)

# for restaurant in restaurants_collection.find():
#     _id = str(restaurant['_id'])
#     address_dic = restaurant['address']
#     building = address_dic['building']  
#     street = address_dic['street']
#     zipcode = address_dic['zipcode']
#     cursor.execute(
#     f'''
#     INSERT INTO d4e12.restaurant(_id, building, zipcode, street, borough, cuisine)
#     VALUES (
#         "{_id}",
#         "{building}",
#         "{zipcode}",
#         "{street}",
#         "{restaurant['borough']}",
#         "{restaurant['cuisine']}"
#     )
#     '''
# )


for restaurant in restaurants_collection.find():
    for value in restaurant['grades']:
        grade = value['grade']
        score = value['score']
        restaurant_id = restaurant['restaurant_id']
        cursor.execute(
            f'''
            INSERT INTO d4e12.grade(restaurant_id, grade, score)
            VALUES (
                '{restaurant_id}',
                '{grade}',
                '{score}'
            )
            '''
        )



mysql_client.commit()