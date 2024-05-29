
CREATE TABLE OpenFoodFacts(
    id SERIAL,
    completeness REAL, 
    energy_kcal_100g REAL,
    energy_100g REAL, 
    fat_100g REAL, 
    saturated_fat_100g REAL, 
    carbohydrates_100g REAL,
    sugars_100g REAL, 
    proteins_100g REAL, 
    salt_100g REAL, 
    sodium_100g REAL,
    PRIMARY KEY (id)
);

CREATE TABLE Predictions(
    id integer,
    prediction integer
);

COPY OpenFoodFacts (id, completeness, energy_kcal_100g, energy_100g, fat_100g, saturated_fat_100g, carbohydrates_100g, sugars_100g, proteins_100g, salt_100g, sodium_100g)
FROM '/var/lib/postgresql/csvs/openfoodfacts_filtered.csv'
DELIMITER ','
CSV HEADER;