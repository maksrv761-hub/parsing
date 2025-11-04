TRUNCATE TABLE wine.wine_quality_agg;
INSERT INTO wine.wine_quality_agg (type, quality, cnt, avg_alcohol, avg_sulphates, avg_residual_sugar)
SELECT type, quality, COUNT(*), AVG(alcohol), AVG(sulphates), AVG(residual_sugar)
FROM wine.wine_quality_raw
GROUP BY type, quality
ORDER BY type, quality;