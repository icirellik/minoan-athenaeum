-- Capital cities
SELECT cities.name AS name, country, population
FROM countries, cities
WHERE capital = cities.name
