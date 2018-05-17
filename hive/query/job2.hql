DROP TABLE IF EXISTS review;
CREATE TABLE review (id int, productid string, userid string, profilename string, hfn int, hfd int, score int, data bigint, summary string, body string) row format delimited fields terminated by ',';

LOAD DATA LOCAL INPATH '~/Reviews.csv' OVERWRITE INTO TABLE review;

DROP TABLE IF EXISTS anno_prodotto_score;
CREATE TABLE anno_prodotto_score (anno int, productid string, score int) row format delimited fields terminated by ',' COLLECTION ITEMS TERMINATED BY ' '; 
INSERT INTO anno_prodotto_score 
SELECT year(from_unixtime(data)) AS anno, productid, score 
FROM review;

DROP TABLE IF EXISTS result;
CREATE TABLE result (productid string, anno int, avg_score double) row format delimited fields terminated by ',';
INSERT OVERWRITE TABLE result 
SELECT productid, anno, round(AVG(score), 2) as avg_score 
FROM anno_prodotto_score
WHERE anno>=2003 and anno<=2012
GROUP BY productid, anno 
ORDER BY productid DESC, anno ASC;

SELECT productid, collect_set(concat_ws(" ",cast(anno as string), ":",  cast(avg_score as string)))
FROM result  
GROUP BY productid;
