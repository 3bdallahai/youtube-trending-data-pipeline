SELECT title, views, likes, CAST(likes As double)/ NULLIF(views,0)  as likes_to_views_precentage
FROM youtube_trending
WHERE views >0 AND
year = 2026 AND
month = 04 AND
day =12
ORDER BY likes_to_views_precentage DESC;