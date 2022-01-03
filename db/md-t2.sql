-- Do we get the same results?
select text, tag, count(*) as ef
from doc
where docid=21381293
group by text, tag 
order by ef desc
limit 10;
