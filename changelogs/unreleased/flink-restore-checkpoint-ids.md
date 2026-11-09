type: patch

### Fixed
- **Restoring an older Flink CDC position into Iceberg no longer drops rows** - the restored job numbered its checkpoints from the old position, and Flink CDC's Iceberg committer skipped every commit whose number it had already seen, so what the job wrote until it passed the old numbers was lost. A restore of an older position, or from a checkpoint after a crash (when the job may have committed later ones), now numbers past every checkpoint the pipeline reached (ADR 0014).
- **A Flink CDC job to Elasticsearch that can't read its own state stops and says why** - Flink CDC 3.7's Elasticsearch writer runs out of memory reading the requests it held when the position was saved, and restarted forever; BeziSync now cancels it and says to Start over, copying the tables again (documents are written by id, so nothing is lost).
