-- 1. Вывести количество фильмов в каждой категории, отсортировать по убыванию.

SELECT c.name, COUNT(c.category_id) as count
FROM category c 
    INNER JOIN film_category f
    ON c.category_id = f.category_id
GROUP BY c.name
ORDER BY count DESC
;



-- 2. Вывести 10 актеров, чьи фильмы большего всего арендовали, отсортировать по убыванию.

SELECT actor.first_name, actor.last_name, COUNT(rental.rental_id) AS rentals_count
FROM rental
    INNER JOIN inventory ON rental.inventory_id = inventory.inventory_id
    INNER JOIN film ON inventory.film_id = film.film_id
    INNER JOIN film_actor ON film.film_id = film_actor.film_id
    INNER JOIN actor ON film_actor.actor_id = actor.actor_id
GROUP BY actor.actor_id
ORDER BY rentals_count DESC
LIMIT 10
;



-- 3. Вывести категорию фильмов, на которую потратили больше всего денег.

SELECT category.name, SUM(payment.amount)
FROM rental
  INNER JOIN inventory ON rental.inventory_id = inventory.inventory_id
  INNER JOIN payment ON rental.rental_id = payment.rental_id
  INNER JOIN film ON inventory.film_id = film.film_id
  INNER JOIN film_category ON film.film_id = film_category.film_id
  INNER JOIN category ON film_category.category_id = category.category_id
GROUP BY category.name
ORDER BY SUM(payment.amount) DESC
LIMIT 1
;


-- 4. Вывести названия фильмов, которых нет в inventory. Написать запрос без использования оператора IN.

SELECT film_id, title
FROM film
WHERE film_id <> ALL (
	SELECT distinct film_id
	FROM inventory
	ORDER BY film_id
)
ORDER BY film_id
;


-- 5.Вывести топ 3 актеров, которые больше всего появлялись в фильмах в категории “Children”. Если у нескольких актеров одинаковое кол-во фильмов, вывести всех

SELECT foo.name, foo.first_name, foo.last_name, count(foo.name) as appearence_time
FROM (
	SELECT c.name, fa.actor_id, a.first_name, a.last_name
	FROM film_category f
		INNER JOIN category c ON f.category_id = c.category_id
		INNER JOIN film_actor fa ON f.film_id = fa.film_id
		INNER JOIN actor a ON fa.actor_id = a.actor_id
	GROUP BY fa.actor_id, c.name , f.film_id, a.first_name, a.last_name
	ORDER BY fa.actor_id
	) as foo
GROUP BY foo.actor_id,foo.name, foo.first_name, foo.last_name
HAVING name IN ('Children')
ORDER BY appearence_time DESC
LIMIT 3
;


-- 6. Вывести города с количеством активных и неактивных клиентов (активный — customer.active = 1). 
-- Отсортировать по количеству неактивных клиентов по убыванию.

SELECT city, customer.active, COUNT(rental.customer_id)
FROM rental
	INNER JOIN customer ON rental.customer_id = customer.customer_id
	LEFT JOIN address ON customer.address_id = address.address_id
	INNER JOIN city ON address.city_id = city.city_id
GROUP BY city, customer.active
ORDER BY customer.active, count DESC
;


-- 7. Вывести категорию фильмов, у которой самое большое кол-во часов суммарной аренды в городах 
-- (customer.address_id в этом city), и которые начинаются на букву “a”. То же самое сделать для городов в которых есть символ “-”. 
-- Написать все в одном запросе.

SELECT name, hours_total
FROM (
	SELECT sum(rental_duratiON) as hours_total, name, city
	FROM rental
		INNER JOIN customer ON rental.customer_id = customer.customer_id
		INNER JOIN address ON customer.address_id =  address.address_id
		INNER JOIN city ON address.city_id = city.city_id
		INNER JOIN inventory ON rental.inventory_id = inventory.inventory_id
		INNER JOIN film ON inventory.film_id = film.film_id
		INNER JOIN film_category ON film.film_id = film_category.film_id
		INNER JOIN category ON film_category.category_id = category.category_id
		where city ilike ('a%') and city like ('%-%')
	GROUP BY customer.address_id, name, city
	) as foo
GROUP BY name, hours_total
ORDER BY hours_total DESC
LIMIT 1
;

