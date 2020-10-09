-- That table contains taxonomic data from GBIF (obtained after matches on the content of scientificname table)
-- ! Content of this table should stay totally GBIF-populated (so it can be dropped and recreated at any time by running the scripts again)
CREATE table rank (
    "id" serial PRIMARY KEY,
    "name" character varying(50) UNIQUE
);

CREATE TABLE taxonomy (
    "id" serial PRIMARY KEY,  -- internal (to the DB) ID
    "gbifId" integer NOT NULL UNIQUE, -- ID at GBIF
    "scientificName" character varying(255), -- as returned by GBIF
    "rankId" integer references rank(id),
    "acceptedId" integer REFERENCES taxonomy(id), -- internal (to the DB) pointer
    "parentId" integer REFERENCES taxonomy(id),  -- internal (to the DB) pointer
    "exotic_be" boolean -- as returned by GBIF (info from GRIIS Belgium checklist)
);

CREATE TYPE gbifmatchtype AS ENUM ('EXACT', 'FUZZY', 'HIGHERRANK', 'NONE');
-- table contains scientificnames in use in the database, a link to "taxonomy" and metadata about the taxonomic match at GBIF
CREATE TABLE scientificname (
    "id" serial PRIMARY KEY,
    "taxonomyId" integer REFERENCES taxonomy(id), -- Can be null if no match
    "deprecatedTaxonId" integer REFERENCES taxon(id),
    "scientificName" character varying(255) NOT NULL, -- as appear in the old "taxon" table
    "authorship" character varying(255), -- as appear in the old "taxon" table or in annexes,
    -- !! the following field are attributes of the match process !!
    "lastMatched" timestamp with time zone, -- when was a GBIF match last attempted?
    "matchConfidence" smallint,
    "matchType" gbifmatchtype,
    CONSTRAINT scn_auth UNIQUE("scientificName", "authorship")
);
CREATE UNIQUE INDEX scn_auth_not_null ON scientificname("scientificName", ("authorship" IS NULL))
WHERE "authorship" IS NULL;


CREATE TABLE annexscientificname (
    "id" serial PRIMARY KEY,
    "scientificnameId" integer REFERENCES scientificname(id), -- Can be null if no match
    "scientificNameOriginal" character varying(1023) NOT NULL, -- as appear in the original annexes
    "scientificName" character varying(255), -- corrected names (typos, ...)
    "authorship" character varying(255), -- corrected names (typos, ...)
    "remarks" character varying (1023), -- remarks about correction
    "annexCode" character varying(255) REFERENCES annex(annexcode)
);

CREATE TABLE vernacularnamesource (
    "id" serial PRIMARY KEY,
    "datasetKey" character varying(50) UNIQUE, -- alphanumeric GBIF datasetKey (UUID)
    "datasetTitle" character varying (1023) UNIQUE -- as appears in GBIF
);

CREATE TABLE vernacularname (
    "id" serial PRIMARY KEY,
    "taxonomyId" integer REFERENCES taxonomy(id) NOT NULL, -- Can be null if no match
    "language" character varying(2) NOT NULL, -- Follows ISO 639-1 standard
    "name" character varying(255) NOT NULL,
    "source" integer REFERENCES vernacularnamesource(id)
)