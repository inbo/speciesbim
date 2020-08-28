# speciesbim
Development of the postgreSQL species database of Brussels Environment

## Changelog / development journal

- Add functionality of running existing code in a demo mode to showcase the work done up to now on a small but significant subset of taxa
- Create a table called scientificnameannex and write functionality to populate with taxa in `official_annexes.csv`
- Add text file, `official_annexes.csv`, in `data/raw` containing all taxa mentioned in official annexes and ordinances
- Added field `acceptedId` to `taxonomy` table. If a taxon is a synonym in GBIF Backbone, the correspondent accepted 
taxon is added and its `id` is used as value of `acceptedId`. Exotic status of synonyms is also inherited by the 
correspondent accepted taxa.
- Added exotic status to `taxonomy` table (field `exotic_be`) describing whether a species is exotic in Belgium.
    Information is inferred from the GBIF checklist [Global Register of Introduced and Invasive Species - Belgium](https://www.gbif.org/dataset/6d9e952f-948c-4483-9807-575348147c7e).
    The exotic status is propagatd to all children of a taxon, if present.  
- Added vernacular names in `vernacularname` with all vernacular names. Adding only a subset of languages is possible
- Improved the taxonomy table so rank info is also included (in a separate "rank" table)
- Dropped the kingdom column in `taxonomy`: it's a cleaner design to infer it by browsing the tree (see below)

The following examples use a recursive CTE to get parents information for a given taxon:

### Example 1: for every entry in "taxonomy", show detailed info about higher levels:

```
WITH RECURSIVE parents AS
(
  SELECT
    id              AS id,
    0               AS number_of_ancestors,
    ARRAY [id]      AS ancestry,
    NULL :: INTEGER AS "parentId",
    id              AS start_of_ancestry
  FROM biodiv.taxonomy
  WHERE
    "parentId" IS NULL
  UNION
  SELECT
    child.id                                    AS id,
    p.number_of_ancestors + 1                   AS ancestry_size,
    array_append(p.ancestry, child.id)          AS ancestry,
    child."parentId"                                AS parentId,
    coalesce(p.start_of_ancestry, child."parentId") AS start_of_ancestry
  FROM biodiv.taxonomy child
    INNER JOIN parents p ON p.id = child."parentId"
)
SELECT
  p.id,
  p.number_of_ancestors,
  p.ancestry,
  p."parentId",
  p.start_of_ancestry,
  t."scientificName",
  t."rankId",
  r.name AS "rank",
  kingdom."scientificName" AS kingdom
FROM parents AS p, biodiv.taxonomy AS t, biodiv.rank AS r, biodiv.taxonomy AS kingdom
WHERE p.id = t.id AND t."rankId" = r.id AND kingdom.id = p.start_of_ancestry
--AND t.id=32;
```

Truncated result: 

| id | number\_of\_ancestors | ancestry | parentId | start\_of\_ancestry | scientificName | rankId | rank | kingdom |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| 1 | 0 | {1} | NULL | 1 | Fungi | 7 | KINGDOM | Fungi |
| 30 | 0 | {30} | NULL | 30 | Animalia | 7 | KINGDOM | Animalia |
| 2 | 1 | {1,2} | 1 | 1 | Ascomycota | 6 | PHYLUM | Fungi |
| 14 | 1 | {1,14} | 1 | 1 | Basidiomycota | 6 | PHYLUM | Fungi |
| 31 | 1 | {30,31} | 30 | 30 | Arthropoda | 6 | PHYLUM | Animalia |
| 3 | 2 | {1,2,3} | 2 | 1 | Leotiomycetes | 5 | CLASS | Fungi |
| 8 | 2 | {1,2,8} | 2 | 1 | Dothideomycetes | 5 | CLASS | Fungi |
| 15 | 2 | {1,14,15} | 14 | 1 | Pucciniomycetes | 5 | CLASS | Fungi |
| 22 | 2 | {1,14,22} | 14 | 1 | Agaricomycetes | 5 | CLASS | Fungi |
| 32 | 2 | {30,31,32} | 31 | 30 | Insecta | 5 | CLASS | Animalia |
...

### Example 2: simplified version: for a given taxon, show all fields from taxonomy + kingdom name (similar results to the previous simpler table structure)

```
WITH RECURSIVE parents AS
(
  SELECT
    id              AS id,
    0               AS number_of_ancestors,
    ARRAY [id]      AS ancestry,
    NULL :: INTEGER AS "parentId",
    id              AS start_of_ancestry
  FROM biodiv.taxonomy
  WHERE
    "parentId" IS NULL
  UNION
  SELECT
    child.id                                    AS id,
    p.number_of_ancestors + 1                   AS ancestry_size,
    array_append(p.ancestry, child.id)          AS ancestry,
    child."parentId"                                AS parentId,
    coalesce(p.start_of_ancestry, child."parentId") AS start_of_ancestry
  FROM biodiv.taxonomy child
    INNER JOIN parents p ON p.id = child."parentId"
)
SELECT
  t.*,
  r.name AS "rank",
  kingdom."scientificName" AS kingdom
FROM parents AS p, biodiv.taxonomy AS t, biodiv.rank AS r, biodiv.taxonomy AS kingdom
WHERE p.id = t.id AND t."rankId" = r.id AND kingdom.id = p.start_of_ancestry
AND t.id=32;
```

Truncated result:

| id | gbifId | scientificName | rankId | parentId | rank | kingdom |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| 32 | 216 | Insecta | 5 | 31 | CLASS | Animalia |
