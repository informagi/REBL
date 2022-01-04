-- Ten most frequent entities
select edict.e, edict.ef
from edict
order by ef desc
limit 10;
