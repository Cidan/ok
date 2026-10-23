-- Pack numbers from blocks a replica reserves of its network's counter (packNum), not the counter
-- each publish wrote: a number is a pack's alone, refused to another — as a block reserved before
-- the network moved away and back would give it again.
CREATE UNIQUE INDEX pack_num ON pack (network_id, num);
