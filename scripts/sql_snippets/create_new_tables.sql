-- That table contains taxonomic data from GBIF (obtained after matches on the content of scientificname table)
-- ! Content of this table should stay totally GBIF-populated (so it can be dropped and recreated at any time by running the scripts again)
CREATE TABLE taxonomy (
    "id" serial PRIMARY KEY,  -- internal (to the DB) ID
    "gbifId" integer NOT NULL UNIQUE, -- ID at GBIF
    "scientificName" character varying(255), -- as returned by GBIF
    "kingdom" character varying(50),
    "parentId" integer REFERENCES taxonomy(id)  -- internal (to the DB) pointer
);

CREATE TYPE gbifmatchtype AS ENUM ('EXACT', 'FUZZY', 'HIGHERRANK', 'NONE');
-- table contains scientificnames in use in the database, a link to "taxonomy" and metadata about the taxonomic match at GBIF
CREATE TABLE scientificname (
    "id" serial PRIMARY KEY,
    "taxonomyId" integer REFERENCES taxonomy(id), -- Can be null if no match
    "deprecatedTaxonId" integer REFERENCES taxon(id),
    "scientificName" character varying(255) NOT NULL, -- as appear in the old "taxon" table
    "authorship" character varying(255), -- as appear in the old "taxon" table,
    -- !! the following field are attributes of the match process !!
    "lastMatched" timestamp with time zone, -- when was a GBIF match last attempted?
    "matchConfidence" smallint,
    "matchType" gbifmatchtype
);

CREATE TABLE vernacularname (
    "id" serial PRIMARY KEY,
    "taxonomyId" integer REFERENCES taxonomy(id) NOT NULL, -- Can be null if no match
    "language" character varying(2) NOT NULL, -- Follows ISO 639-1 standard
    "name" character varying(255) NOT NULL,
    "source" character varying(255)
)