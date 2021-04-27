SELECT SUM (n.n_nationkey) 
FROM nation AS n, region AS r 
WHERE (r.r_regionkey = n.n_regionkey) AND (n.n_name = 'UNITED STATES')
