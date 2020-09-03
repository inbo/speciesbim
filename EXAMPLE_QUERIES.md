# Example 1: for every entry in "taxonomy", show detailed info about higher levels:

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

# Example 2: simplified version: for a given taxon, show all fields from taxonomy + kingdom name (similar results to the previous simpler table structure)

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

# Example 3: Get 2 vernacularnames for a given taxon, priority to names from the Belgian Species List

```
WITH vernacularnames_with_priority AS (
    SELECT
           "taxonomyId",
           "language",
           "name",
           "source",
           (CASE
               WHEN "source" LIKE 'Belgian Species List' THEN TRUE
               ELSE FALSE END
            ) "high_priority_source"
    FROM biodiv.vernacularname
    )


SELECT * FROM vernacularnames_with_priority
WHERE "taxonomyId" = 8 -- Marsh frog;
AND language LIKE 'fr'
ORDER by high_priority_source DESC -- High priority source first
LIMIT 2;
```

| taxonomyId | language | name | source | high\_priority\_source |
| :--- | :--- | :--- | :--- | :--- |
| 8 | fr | grenouille rieuse | Belgian Species List | true |
| 8 | fr | Grenouille rieuse | EUNIS Biodiversity Database | false |
