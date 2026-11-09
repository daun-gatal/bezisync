type: patch

### Fixed
- **Restoring an older Flink CDC position into Iceberg no longer drops rows** - the restored job numbered its checkpoints from the old position, and Flink CDC's Iceberg committer skipped every commit whose number it had already seen, so what the job wrote until it passed the old numbers was lost. A restore of an older position, or from a checkpoint after a crash (when the job may have committed later ones), now numbers past every checkpoint the pipeline reached (ADR 0014).
