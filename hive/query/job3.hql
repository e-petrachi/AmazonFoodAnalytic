DROP TABLE IF EXISTS review;
CREATE TABLE review (id int, productid string, userid string, profilename string, hfn int, hfd int, score int, data bigint, summary string, body string) row format delimited fields terminated by ',';

LOAD DATA LOCAL INPATH '~/Reviews.csv' OVERWRITE INTO TABLE review;

DROP TABLE IF EXISTS prodotto_utente;
CREATE TABLE prodotto_utente (productid string, userid string) row format delimited fields terminated by ',' COLLECTION ITEMS TERMINATED BY ' '; 
INSERT INTO prodotto_utente 
SELECT DISTINCT productid, userid
FROM review;

DROP TABLE IF EXISTS prodotto_utente_2;
CREATE TABLE prodotto_utente_2 (productid1 string, productid2 string, userid string) row format delimited fields terminated by ',' COLLECTION ITEMS TERMINATED BY ' '; 
INSERT INTO prodotto_utente_2 
SELECT p1.productid, p2.productid, p1.userid
FROM prodotto_utente p1 inner join prodotto_utente p2 on p1.userid=p2.userid and p1.productid<>p2.productid and p1.productid<p2.productid;

SELECT productid1, productid2, COUNT(*)
FROM prodotto_utente_2
GROUP BY productid1, productid2;
