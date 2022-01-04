-- Do we get the same results?
SELECT edict.e, doc.tag, count(doc.e) as ef
FROM doc, edict
WHERE 
      docid=21381293
  AND doc.e = edict.eid
group by doc.e, edict.e, doc.tag 
order by ef desc
limit 10;
