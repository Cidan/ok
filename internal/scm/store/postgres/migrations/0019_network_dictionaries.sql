-- A dictionary's ID is its network's (docs/hosted.md, "Cells, as built"): a frame in the network's
-- packs names it, so a network that moves to another cell keeps its dictionaries' IDs, which that
-- cell's other networks may hold too. New IDs still come from one sequence, which a move advances
-- past those it brings.
ALTER TABLE dictionary DROP CONSTRAINT dictionary_pkey;
ALTER TABLE dictionary ADD PRIMARY KEY (network_id, id);
DROP INDEX dictionary_repository;  -- (network_id, id), which the key is now
