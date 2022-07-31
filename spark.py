from select import select
from unicodedata import name
import findspark
findspark.init()
from pyspark.sql.functions import col

from config import df_loader


# 1. Вывести количество фильмов в каждой категории, отсортировать по убыванию.

def first_query():
    category = df_loader('category')
    film_category = df_loader('film_category')

    number_of_films_by_category = category.join(film_category, category.category_id == film_category.category_id, "inner") \
        .select("name", "film_id") \
        .groupBy("name").count() \
        .orderBy(col("count").desc()) \
        .show(truncate=False)


# 2. Вывести 10 актеров, чьи фильмы большего всего арендовали, отсортировать по убыванию.

def second_query():
    rental = df_loader('rental')
    inventory = df_loader('inventory')
    film = df_loader('film')
    film_actor = df_loader('film_actor')
    actor = df_loader('actor')

    top_ten_actors = rental.join(inventory, rental.inventory_id == inventory.inventory_id, "inner") \
        .join(film, inventory.film_id == film.film_id, "inner") \
        .join(film_actor, film.film_id == film_actor.film_id, "inner") \
        .join(actor, film_actor.actor_id == actor.actor_id, "inner") \
        .select(actor.first_name, actor.last_name, actor.actor_id) \
        .groupBy(actor.actor_id).count() \
        .orderBy(col("count").desc()) \
        .show(10,truncate=False)

# 3. Вывести категорию фильмов, на которую потратили больше всего денег.

def third_query():
    rental = df_loader('rental')
    inventory = df_loader('inventory')
    payment = df_loader('payment')
    payment.printSchema()
    film = df_loader('film')
    film_category = df_loader('film_category')
    category = df_loader('category')

    most_profit_category = rental.join(inventory, rental.inventory_id == inventory.inventory_id, "inner") \
        .join(payment, rental.rental_id == payment.rental_id, "inner") \
        .join(film, inventory.film_id == film.film_id, "inner") \
        .join(film_category, film.film_id == film_category.film_id, "inner") \
        .join(category, film_category.category_id == category.category_id, "inner") \
        .select(category.name, payment.amount) \
        .groupBy(category.name).agg({"amount": "sum"}) \
        .orderBy(col("sum(amount)").desc()) \
        .show(1, truncate=False)


# 4. Вывести названия фильмов, которых нет в inventory. Написать запрос без использования оператора IN.

def fourth_query():
    film = df_loader('film')
    inventory = df_loader('inventory')
    # Creating list of values 
    unique_in_inventory = inventory.select("film_id").distinct().rdd.flatMap(lambda x: x).collect()
    films_not_in_inventory = film.filter(~film.film_id \
        .isin(unique_in_inventory)) \
        .select("film_id", "title") \
        .show(truncate=False)


# 5. Вывести топ 3 актеров, которые больше всего появлялись в фильмах в категории “Children”. 
# Если у нескольких актеров одинаковое кол-во фильмов, вывести всех

def fifth_query():
    film_category = df_loader('film_category')
    category = df_loader('category')
    film_actor = df_loader('film_actor')
    actor = df_loader('actor')
    top_3_actors_in_children = film_category.join(category, film_category.category_id == category.category_id, "inner") \
        .join(film_actor, film_category.film_id == film_actor.film_id, "inner") \
        .join(actor, film_actor.actor_id == actor.actor_id, "inner") \
        .select(category.name, film_actor.actor_id, actor.first_name, actor.last_name) \
        .groupBy(film_actor.actor_id, category.name).agg({"name": "count"}) \
        .orderBy(col("count(name)").desc()) \
        
    top_3_actors_in_children.filter(top_3_actors_in_children.name.isin("Children")) \
        .show(3, truncate=False) \


# 6. Вывести города с количеством активных и неактивных клиентов (активный — customer.active = 1). 
# Отсортировать по количеству неактивных клиентов по убыванию.

def sixth_query():
    rental = df_loader('rental')
    customer = df_loader('customer')
    address = df_loader('address')
    city = df_loader('city')
    active_non_active_by_city = rental.join(customer, rental.customer_id == customer.customer_id, "inner") \
        .join(address, customer.address_id == address.address_id, "inner") \
        .join(city, address.city_id == city.city_id, "inner") \
        .groupBy(city.city, customer.active) \
        .count() \
        .orderBy(col('active'), col("count").desc()) \
        .show(truncate=False) \


# 7. Вывести категорию фильмов, у которой самое большое кол-во часов суммарной аренды в городах 
#  (customer.address_id в этом city), и которые начинаются на букву “a”. То же самое сделать для городов в которых есть символ “-”. 
#  Написать все в одном запросе.

def seventh_query():
    rental = df_loader('rental')
    customer = df_loader('customer')
    address = df_loader('address')
    city = df_loader('city')
    inventory = df_loader('inventory')
    film = df_loader('film')
    film_category = df_loader('film_category')
    category = df_loader('category')
    top_1_category_in_special_cities = rental.join(customer, rental.customer_id == customer.customer_id, "inner") \
        .join(address, customer.address_id == address.address_id, "inner") \
        .join(city, address.city_id == city.city_id, "inner") \
        .join(inventory, rental.inventory_id == inventory.inventory_id, "inner") \
        .join(film, inventory.film_id == film.film_id, "inner") \
        .join(film_category, film.film_id == film_category.film_id, "inner") \
        .join(category, film_category.category_id == category.category_id, "inner") 
    top_1_category_in_special_cities.filter((col('city').like("a%")) & (col('city').like("%-%"))) \
        .groupBy(customer.address_id, category.name) \
        .agg({'rental_duration': 'sum'}) \
        .select(category.name, 'sum(rental_duration)') \
        .orderBy(col("sum(rental_duration)").desc()) \
        .show(1, truncate=False)


if __name__ == '__main__':
    first_query()
    second_query()
    third_query()
    fourth_query()
    fifth_query()
    sixth_query()
    seventh_query()