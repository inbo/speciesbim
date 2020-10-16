# speciesbim
Development of the postgreSQL species database of Brussels Environment

## Changelog / development journal

- Implement the match of `annexscientificname` table to the `scientificname` table. Add taxa if not present
- Add field `isScientificName` to `annexscientificname`  table
- Rename `scientificnameannex` as `annexscientificname` and field `scientificNameOriginal` as `scientificNameInAnnex`
- Store details (`datasetKey` and `datasetTitle`) about the source (dataset) of vernacular names in a new table (`verncaularnamesource`)
- Add functionality of running existing code in a demo mode to showcase the work done up to now on a small but significant subset of taxa
- Create a table called `scientificnameannex` and write functionality to populate with taxa in `official_annexes.csv`
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

