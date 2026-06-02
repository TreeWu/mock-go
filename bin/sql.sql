DELETE FROM edge
WHERE source NOT IN (SELECT id FROM vertex)
   OR target NOT IN (SELECT id FROM vertex);