INSERT INTO scientificname ("deprecatedTaxonId", "scientificName", "authorship")
SELECT id, acceptedname, scientificnameauthorship FROM biodiv.taxon WHERE
id IN (SELECT nptaxonid FROM biodiv.commontaxa) OR
id IN (SELECT taxonid FROM biodiv.speciesannex) OR
id IN (SELECT taxonid FROM biodiv.media) OR
id IN (SELECT taxonid FROM biodiv.identifiablespecies WHERE
id IN (SELECT identifiablespeciesid FROM biodiv.occurence))
LIMIT {{ limit }};
