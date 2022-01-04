-- Ten most frequent entities
select e, count(*) as ef
from doc
where field='body'
group by e
order by ef desc
limit 10;
