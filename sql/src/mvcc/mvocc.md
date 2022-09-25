## For a Tx to be serializable

- Read stability: if a Tx T has read V1 of a record, then at the end of
  the Tx, V1 is still the version that is visible to T, not some other
  committed version.
- Phantom avoidance: Tx's scan should also not return new version
  (inserted/deleted)


### Read
only version's valid time overlaps with the. Different versions of a
record have non overlapping time => at most one version is visible to a
read time

## Pseudo hash index:

Hash by field (name), the value of the hash map is a linked list of
version

## Phases
### Preparing phase

Preparation phase. During this phase the transaction determines whether 
it can commit or is forced to abort. If it has to abort, it switches its
state to Aborted and continues to the next phase. If it is ready to commit,
it writes all its new versions and information about deleted versions to
a redo log record and waits for the log record to reach non-volatile storage. The
transaction then switches its state to Committed.
### Validation phase

## Lower isolation level implementation:
- Repeatable read: For validation, only validate reads, no need to
  validate phantom
- Read committed: on visibility test, use current_ts as the read_ts. No
  need to validate read/phantom
- Snapshot isolation: always use tx's begin_ts to read 
- Read-only Tx: either read committed or snapshot isolation

