select text, tag, count(*) as ef
from 'doc-ok.parquet' 
group by text, tag 
order by ef desc
limit 10;
