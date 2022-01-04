-- Ten most frequent entities
select text, count(*) as ef
from 'msmarco_doc_00.parquet'
where field=2
group by text
order by ef desc
limit 10;
