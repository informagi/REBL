-- Do we get the same results?
select text, tag, count(*) as ef
from 'msmarco_doc_00.parquet' 
where identifier='msmarco_doc_00_21381293'
group by text, tag 
order by ef desc
limit 10;
