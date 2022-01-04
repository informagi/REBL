-- Ten most frequent entities

-- building the index does not pay off for a single query
-- create index ex on edict(eid);

select edict.e, count(doc.e) as ef
from doc, edict
where field='body' AND doc.e = eid
group by doc.e, edict.e
order by ef desc
limit 10;
