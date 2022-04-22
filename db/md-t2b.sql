-- Without joining the dictionary
SELECT e, tag, count(e) as ef
FROM doc
WHERE docid=21381293
group by e, tag 
order by ef desc
limit 10;
