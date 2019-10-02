-- Cities listed next to cities that are smaller
SELECT a.name, b.name AS smaller_city
FROM cities AS a, cities AS b
WHERE a.population > b.population
