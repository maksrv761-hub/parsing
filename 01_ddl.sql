CREATE SCHEMA IF NOT EXISTS wine;
CREATE TABLE IF NOT EXISTS wine.wine_quality_raw (
  id SERIAL PRIMARY KEY,
  type TEXT NOT NULL,
  fixed_acidity NUMERIC,
  volatile_acidity NUMERIC,
  citric_acid NUMERIC,
  residual_sugar NUMERIC,
  chlorides NUMERIC,
  free_sulfur_dioxide NUMERIC,
  total_sulfur_dioxide NUMERIC,
  density NUMERIC,
  pH NUMERIC,
  sulphates NUMERIC,
  alcohol NUMERIC,
  quality INT
);
CREATE TABLE IF NOT EXISTS wine.wine_quality_agg (
  type TEXT,
  quality INT,
  cnt BIGINT,
  avg_alcohol NUMERIC,
  avg_sulphates NUMERIC,
  avg_residual_sugar NUMERIC
);