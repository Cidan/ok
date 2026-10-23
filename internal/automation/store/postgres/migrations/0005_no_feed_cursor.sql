-- Automation follows each repository's log (follow_cursor); its place in the SCM's feed across every
-- repository goes, and so do the database's chunks of logs, which are the bucket's (store/logs.go).
DROP TABLE feed_cursor;
DROP TABLE log_chunk;
