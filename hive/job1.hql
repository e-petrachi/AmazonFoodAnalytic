DROP TABLE IF EXISTS review;
CREATE TABLE review (id int, productid string, userid string, profilename string, hfn int, hfd int, score int, data bigint, summary string, body string) row format delimited fields terminated by ',';

LOAD DATA LOCAL INPATH '/home/dibbidouble/Workspace/uni/bigdata/firstproject/amazon-fine-food-reviews/Reviews.csv' OVERWRITE INTO TABLE review;

DROP TABLE IF EXISTS anno_summary;
CREATE TABLE anno_summary (anno int, summary string) row format delimited fields terminated by ',' COLLECTION ITEMS TERMINATED BY ' '; 
INSERT INTO anno_summary 
SELECT year(from_unixtime(data)) AS anno, lower(trim(regexp_replace(summary, '[\'_|$#<>\\^=\\[\\]\\*/\\\\,;,.\\-:()?!\"]', " "))) 
FROM review;

DROP TABLE IF EXISTS result;
CREATE TABLE result (anno int, word string, occ int) row format delimited fields terminated by ',';
INSERT OVERWRITE TABLE result 
SELECT anno, word, COUNT(*) as count 
FROM anno_summary LATERAL VIEW explode(split(summary, ' +')) lTable as word 
WHERE anno>=1999 
GROUP BY anno,word 
ORDER BY count DESC, word ASC;

DROP TABLE IF EXISTS result_rank;
CREATE TABLE result_rank as (
    SELECT anno, word, occ, row_number() over (partition by anno order by occ desc) as rnk
    FROM result
);

INSERT OVERWRITE LOCAL DIRECTORY '/home/dibbidouble/Workspace/uni/bigdata/firstproject/Hive/job1h_result'
SELECT anno, collect_set(concat_ws(" ",word, cast(occ as string))) 
FROM result_rank 
WHERE rnk<=10 
GROUP BY anno;
