-- A network's packs by number: what its chunk index names them by. A reader asks for the numbers
-- its chunks name (NumberedPacks), not for every pack the network holds — a pack a publish.
CREATE UNIQUE INDEX pack_num ON pack (network_id, num);
