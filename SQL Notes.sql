SQL Notes

-- Records where the starting age of ISCED0 is greater than ISCED1
-- This makes little sense, as it is the equivalent of joining nursery after year 1
SELECT id, st125q01na, st126q01ta FROM responses WHERE st125q01na > st126q01ta;

-- Records where the starting age of ISCED0 is less than ISCED1
-- As in the normal way that time works
SELECT id, st125q01na, st126q01ta FROM responses WHERE  st125q01na < st126q01ta;


SELECT id, st125q01na, st126q01ta FROM responses;

SELECT belong, ST034Q01TA, ST034Q02TA, ST034Q03TA, ST034Q04TA, ST034Q05TA, ST034Q06TA FROM responses;

SELECT misced, fisced, hisced FROM responses;

SELECT  FROM responses;

SELECT id, st125q01na, st126q01ta, st034q01ta, st034q02ta, st034q03ta, st034q04ta, st034q05ta, st034q06ta FROM responses;

-- Warehouse Table MAnagement

DROP TABLE submission_times;
CREATE TABLE submission_times (
    id SERIAL PRIMARY KEY,
    original_id int,
    created_at timestamp
);
TRUNCATE submission_times;

DROP TABLE learning_hours;
CREATE TABLE learning_hours (
	id SERIAL PRIMARY KEY,
	original_id int,
	country_code varchar(3),
	class_periods int,
	average_mins int
);
TRUNCATE learning_hours;

DROP TABLE early_education_and_belonging;
CREATE TABLE early_education_and_belonging (
    id SERIAL PRIMARY KEY,
    original_id int,
    country_code varchar(3),
    durecec int,
    belong float
);
TRUNCATE early_education_and_belonging;

TRUNCATE submission_times;
TRUNCATE early_education_and_belonging;
TRUNCATE learning_hours;

SELECT country_code, AVG(class_periods * average_mins)
FROM learning_hours
WHERE country_code IN ('FRA', 'GBR', 'THA')
GROUP BY country_code ORDER BY country_code;

SELECT country_code, AVG(durecec) AS avg_durecec, AVG(belong) AS avg_belong, count(country_code)
FROM early_education_and_belonging
GROUP BY country_code
ORDER BY country_code;