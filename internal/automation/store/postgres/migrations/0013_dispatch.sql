-- Dispatch over the bus (docs/hosted.md, "Hosted automation, as built"): every queued run is a job
-- on its pool's queue, enqueued before the transaction that queued it commits, and the bus's lease
-- on that job is the run's lease while it runs. dispatch_job is the ID of the job that dispatches
-- the run now — a delivery of another is a stale copy, dropped — and lease_handle the bus's handle
-- of its delivery while the run runs, which heartbeats extend and an outcome acknowledges.
ALTER TABLE run ADD COLUMN dispatch_job text;
ALTER TABLE run ADD COLUMN lease_handle bytea;
