INSERT INTO scientificname (scientificName, authorship)
SELECT acceptedname, scientificnameauthorship FROM biodiv.taxon WHERE
id IN (SELECT nptaxonid FROM biodiv.commontaxa) OR
id IN (SELECT taxonid FROM biodiv.speciesannex) OR
id IN (SELECT taxonid FROM biodiv.media) OR
id IN (SELECT taxonid FROM biodiv.identifiablespecies WHERE
id IN (SELECT identifiablespeciesid FROM biodiv.occurence))
-- Uncomment the next line for fast mode
--LIMIT 1000
;