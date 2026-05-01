# Retention Lease Deep Dive - Cross-Cluster Replication

## Overview

Retention leases are a critical mechanism in OpenSearch that prevents the deletion of historical operations needed for replication. This document explains how CCR uses retention leases to preserve translog operations and soft-deleted documents during bootstrap and continuous replication.

---

## Why Retention Leases Are Needed

### The Problem

In normal OpenSearch operation, old data is cleaned up to save space:

1. **Soft Deletes**: When documents are deleted or updated, they're marked as "soft deleted" in Lucene segments. These soft deletes get merged away during segment merges.

2. **Translog Truncation**: The translog (transaction log) contains recent operations. Once operations are safely persisted to Lucene and all replicas have acknowledged them, the translog can be truncated.

Without retention leases, by the time the follower cluster tries to replicate operations, they might already be deleted!

### The Solution: Retention Leases

A retention lease is a **guarantee** that:
- Operations starting from a specific sequence number will NOT be deleted
- Soft deletes will NOT be merged away if they're needed
- Translog will NOT be truncated if it contains required operations

Think of it as a "bookmark" that says: "I need everything from this point forward - don't delete it!"

---

## Key Concepts

### Sequence Numbers (SeqNo)

Every operation in OpenSearch gets a monotonically increasing sequence number:
- Creates, updates, deletes all get sequence numbers
- Sequence numbers are per-shard
- They enable tracking which operations have been replicated

### RetainingSequenceNumber

This is the sequence number specified in a retention lease. It means:
> "Preserve all operations with seqNo >= retainingSequenceNumber"

### RETAIN_ALL

A special constant (typically -1) that means:
> "Preserve ALL operations from the beginning of the index"

This is used during bootstrap when we don't yet know what sequence number to start from.

---

## Retention Lease Lifecycle in CCR

### Phase 1: Bootstrap (Initial Setup)

**Location**: `RemoteClusterRestoreLeaderService.kt:114-118`

```kotlin
// During bootstrap at the leader cluster
val indexCommitRef = leaderIndexShard.acquireSafeIndexCommit()

// Add retention lease with RETAIN_ALL
var fromSeqNo = RetentionLeaseActions.RETAIN_ALL
retentionLeaseHelper.addRetentionLease(
    request.leaderShardId, 
    fromSeqNo,  // RETAIN_ALL = -1 
    request.followerShardId,
    timeout
)
```

**What Happens**:

1. **Acquire History Retention Lock**: Before creating the lease, acquire a retention lock to prevent any cleanup
   ```kotlin
   val retentionLock = leaderIndexShard.acquireHistoryRetentionLock()
   ```

2. **Acquire Safe Index Commit**: Get a consistent snapshot of the index
   ```kotlin
   val indexCommitRef = leaderIndexShard.acquireSafeIndexCommit()
   ```

3. **Add Retention Lease with RETAIN_ALL**: Tell the leader shard to preserve everything
   - Uses `RetentionLeaseActions.RETAIN_ALL` (typically -1)
   - This ensures no operations are deleted during bootstrap
   - The lease is identified by: `replication:<follower-cluster-name>:<follower-shard-id>`

4. **Release Retention Lock**: Once the lease is in place, release the lock
   ```kotlin
   retentionLock.close()
   ```

**Why RETAIN_ALL?**
- During bootstrap, we copy the current state via snapshot/restore
- We don't yet know what sequence number the follower will start at after restore
- So we preserve ALL operations to be safe

---

### Phase 2: Continuous Replication Starts

**Location**: `ShardReplicationTask.kt:202-210`

After bootstrap completes, shard replication tasks start:

```kotlin
// Initial renewal when shard task starts
retentionLeaseHelper.renewRetentionLease(
    leaderShardId, 
    indexShard.lastSyncedGlobalCheckpoint + 1,  // Specific seqNo!
    followerShardId
)
```

**What Happens**:

1. **Get Follower's Current Position**: Check `indexShard.lastSyncedGlobalCheckpoint`
   - This is the last operation the follower has successfully replicated
   - It's the "high water mark" of replication

2. **Renew with Specific SeqNo**: Update the retention lease
   - Old value: `RETAIN_ALL` (-1)
   - New value: `lastSyncedGlobalCheckpoint + 1`
   - Means: "I need operations starting from this specific sequence number"

3. **Why +1?**
   ```kotlin
   //Retention leases preserve the operations including and starting from 
   //the retainingSequenceNumber we specify when we take the lease.
   //hence renew retention lease with lastSyncedGlobalCheckpoint + 1
   ```
   - The checkpoint represents the last operation we HAVE
   - We need operations AFTER that (hence +1)
   - The lease is inclusive of the retainingSequenceNumber

**Example**:
- Follower has replicated up to seqNo 1000
- Renewal sets retainingSequenceNumber = 1001
- Leader preserves operations 1001, 1002, 1003, ...

---

### Phase 3: Ongoing Renewal

**Location**: `ShardReplicationTask.kt:290-313`

During continuous replication, the lease is periodically renewed:

```kotlin
// After successfully writing changes to follower
retentionLeaseHelper.renewRetentionLease(
    leaderShardId, 
    indexShard.lastSyncedGlobalCheckpoint + 1, 
    followerShardId
)
lastLeaseRenewalMillis = System.currentTimeMillis()
```

**When Does Renewal Happen?**
- After each batch of changes is successfully written to the follower shard
- Inside the replication loop (while `isActive`)
- Not on a fixed schedule, but after progress is made

**Why Continuous Renewal?**

As the follower makes progress:
- Follower replicates operations up to seqNo 1500
- Lease is renewed with retainingSequenceNumber = 1501
- Leader can now cleanup operations < 1501 (they're safe to delete)
- This prevents unbounded growth of translog and soft deletes

**Error Handling**:

```kotlin
catch (ex: Exception) {
    when (ex) {
        is RetentionLeaseNotFoundException -> {
            // Critical: Can't continue without lease
            throw ex
        }
        is RetentionLeaseInvalidRetainingSeqNoException -> {
            // Trying to renew with seqNo lower than current
            // Only fail if this keeps happening
            if (System.currentTimeMillis() - lastLeaseRenewalMillis > 
                replicationSettings.leaseRenewalMaxFailureDuration.millis) {
                throw ex
            }
        }
        else -> {
            // Log but don't fail
        }
    }
}
```

---

### Phase 4: Cleanup (Stop Replication)

**Location**: `IndexReplicationTask.kt:896`, `RemoteClusterRetentionLeaseHelper.kt:164-178`

When replication stops:

```kotlin
retentionLeaseHelper.attemptRetentionLeaseRemoval(leaderShardId, followerShardId)
```

**What Happens**:
1. Sends `RetentionLeaseActions.RemoveRequest` to leader
2. Leader removes the lease from the shard
3. Operations are now eligible for cleanup (based on other retention policies)
4. Failures are logged but don't block stop operation

**Important**: Removal is best-effort. If it fails, the lease stays active but replication still stops.

---

## How Retention Leases Prevent Deletion

### 1. Translog Truncation Prevention

**Location**: `ReplicationTranslogDeletionPolicy.kt:91-143`

CCR provides a custom translog deletion policy:

```kotlin
override fun minTranslogGenRequired(
    readers: List<TranslogReader>, 
    writer: TranslogWriter
): Long {
    val minByRetentionLeases = getMinTranslogGenByRetentionLease(
        readers, writer, retentionLeasesSupplier
    )
    // ... combine with size, age, and file count policies
}
```

**How It Works**:

1. **Get Minimum Retaining SeqNo** from all retention leases:
   ```kotlin
   val minimumRetainingSequenceNumber = retentionLeasesSupplier.get()
       .leases()
       .stream()
       .mapToLong(RetentionLease::retainingSequenceNumber)
       .min()
       .orElse(Long.MAX_VALUE)
   ```

2. **Find Translog Generation** containing that seqNo:
   ```kotlin
   for (reader in readers.reversed()) {
       if (reader.minSeqNo <= minimumRetainingSequenceNumber &&
           reader.maxSeqNo >= minimumRetainingSequenceNumber) {
           minGen = minGen.coerceAtMost(reader.generation)
       }
   }
   ```

3. **Result**: Translog generations containing operations needed by any retention lease are preserved

**Example**:
- Retention lease has retainingSequenceNumber = 1500
- Translog generation 10 contains seqNo 1200-1600
- Generation 10 and all newer generations are kept
- Generations 1-9 can be deleted (if no other policy prevents it)

### 2. Soft Delete Preservation

Soft deletes in Lucene are preserved based on retention leases:

**OpenSearch Settings** (enabled by CCR):

```kotlin
// In setupAndStartRestore()
val settingsBuilder = Settings.builder()
    .put(REPLICATION_INDEX_TRANSLOG_PRUNING_ENABLED_SETTING.key, true)
```

When `REPLICATION_INDEX_TRANSLOG_PRUNING_ENABLED_SETTING` is true:
- The custom `ReplicationTranslogDeletionPolicy` is activated
- Soft deletes are kept if they're within retention lease range
- Segment merges avoid merging away needed soft deletes

---

## Settings That Control Retention

### Leader Cluster Settings

**Set during bootstrap** (`IndexReplicationTask.kt:setupAndStartRestore()`):

1. **Enable Retention Lease-Based Pruning**:
   ```kotlin
   REPLICATION_INDEX_TRANSLOG_PRUNING_ENABLED_SETTING = true
   ```
   - Enables custom translog deletion policy
   - Respects retention leases when deciding what to delete

2. **Translog Generation Size**:
   ```kotlin
   INDEX_TRANSLOG_GENERATION_THRESHOLD_SIZE_SETTING = 32 MB
   ```
   - Smaller generations = more granular cleanup
   - Easier to find specific sequence number ranges
   - Avoids searching huge translog files

### Follower Cluster Settings

**Index Settings**:
- `index.soft_deletes.enabled`: Must be true (CCR requires soft deletes)
- `index.soft_deletes.retention_lease.period`: How long to keep expired leases (default: 12h)

**Cluster Settings**:
- `indices.memory.shard_inactive_time`: How long to wait before closing inactive shards
- Affects when retention leases are renewed if shard is inactive

---

## Retention Lease ID Format

**Location**: `RemoteClusterRetentionLeaseHelper.kt:48-54`

```kotlin
fun retentionLeaseIdForShard(
    followerClusterName: String, 
    followerShardId: ShardId
): String {
    val retentionLeaseSource = "$RETENTION_LEASE_PREFIX$followerClusterName"
    return "$retentionLeaseSource:$followerShardId"
}
```

**Format**: `replication:<follower-cluster-name>:<follower-cluster-uuid>:[index-name][shard-id]`

**Example**: `replication:follower-cluster:abc123xyz:[follower-01][0]`

**Why Include Cluster UUID?**
- Handles cluster name reuse
- Prevents conflicts if a cluster is recreated with same name
- Old retention leases can be detected and cleaned up

---

## Edge Cases and Error Scenarios

### 1. Retention Lease Already Exists

**Location**: `RemoteClusterRetentionLeaseHelper.kt:200-222`

```kotlin
catch (e: RetentionLeaseAlreadyExistsException) {
    log.info("Found a stale retention lease $retentionLeaseId on leader.")
    if (canRetry) {
        canRetry = false
        attemptRetentionLeaseRemoval(leaderShardId, followerShardId, timeout)
        log.info("Cleared stale retention lease. Retrying...")
    } else {
        throw e
    }
}
```

**Cause**: Previous replication didn't clean up properly
**Solution**: Remove stale lease and retry

### 2. Invalid Retaining SeqNo

**Location**: `ShardReplicationTask.kt:302-310`

```kotlin
is RetentionLeaseInvalidRetainingSeqNoException -> {
    if (System.currentTimeMillis() - lastLeaseRenewalMillis > 
        replicationSettings.leaseRenewalMaxFailureDuration.millis) {
        throw ex  // Fail if stuck too long
    } else {
        log.error("Retention lease renewal failed. Ignoring.")
    }
}
```

**Cause**: Trying to renew with seqNo < current lease seqNo
**Solution**: Temporary failures are logged; only fail if persistent

### 3. Retention Lease Not Found

```kotlin
is RetentionLeaseNotFoundException -> {
    // Check if old lease format exists
    addNewRetentionLeaseIfOldExists(leaderShardId, followerShardId, seqNo)
}
```

**Cause**: 
- Lease was removed
- Leader shard relocated
- Upgrade scenario (old lease format)

**Solution**: Create new lease or fail if truly missing

### 4. Operations Already Deleted

If retention lease is added TOO LATE:
- Some operations might already be deleted from translog
- Soft deletes might already be merged away
- **Result**: Replication cannot catch up from that point
- **Solution**: Full re-bootstrap required (stop and restart replication)

---

## Relationship to Soft Deletes

### What Are Soft Deletes?

In Lucene/OpenSearch:
- Deletes and updates mark old documents as "deleted"
- Deleted documents still occupy space in segments
- During segment merges, deleted documents can be purged

### How Retention Leases Preserve Soft Deletes

1. **Retention Lease Sets Lower Bound**:
   - Lease says: "Need operations from seqNo X onward"
   
2. **Merge Policy Checks Leases**:
   - Before merging segments, check minimum retaining seqNo
   - Documents with seqNo >= minimum are preserved
   - Even if they're marked deleted

3. **CCR Can Read Deleted Documents**:
   - During replication, follower needs to know about deletes
   - Soft deletes allow reading "this document was deleted at seqNo Y"
   - Without soft deletes, we'd lose delete operations

### Example Timeline

```
Time 0: Doc1 created at seqNo 100
Time 1: Doc1 updated at seqNo 200 (old version soft-deleted)
Time 2: Retention lease added with retainingSeqNo = 150
Time 3: Segment merge happens
        - Segment merger sees retention lease
        - Preserves soft-deleted version (seqNo 100-200 range)
        - Follower can still see the update operation
```

---

## Monitoring and Debugging

### Check Retention Leases on Leader

```bash
GET /leader-01/_stats/shards?level=shards

# Look for retention_leases in the response:
{
  "retention_leases": {
    "primary_term": 1,
    "version": 5,
    "leases": [
      {
        "id": "replication:follower-cluster:uuid:[follower-01][0]",
        "retaining_seq_no": 1500,
        "timestamp": 1234567890,
        "source": "replication:follower-cluster:uuid"
      }
    ]
  }
}
```

### Check Translog on Leader

```bash
GET /leader-01/_stats/translog

# Response shows:
{
  "translog": {
    "operations": 5000,
    "size_in_bytes": 10485760,
    "uncommitted_operations": 100,
    "earliest_last_modified_age": 3600000  # Age of oldest translog
  }
}
```

### Common Issues

1. **Translog Growing Unbounded**
   - Symptom: Leader disk fills up with translog files
   - Cause: Retention lease not being renewed (follower stuck)
   - Solution: Check follower replication progress, consider stopping and restarting

2. **Replication Failing with "Operations Not Available"**
   - Symptom: `OpenSearchException: Operations not available`
   - Cause: Operations deleted before lease was added, or lease expired
   - Solution: Stop and restart replication (triggers full bootstrap)

3. **Old Retention Leases Not Cleaned Up**
   - Symptom: Many leases in `_stats` from old replications
   - Cause: Stop replication API didn't clean up properly
   - Solution: Manually remove via retention lease API (OpenSearch 2.0+)

---

## Performance Implications

### Storage Overhead

**Translog**:
- Retention leases prevent translog truncation
- With active replication: Typically a few MB to GB
- With stuck replication: Can grow to tens/hundreds of GB

**Soft Deletes**:
- Preserved until retention lease advances
- Typically small overhead (metadata only)
- Large impact if many updates/deletes occur

### Recommendations

1. **Monitor Replication Lag**: 
   - Keep lag under a few seconds
   - Prevents retention lease from holding too much history

2. **Set Appropriate Translog Size**:
   - CCR sets `INDEX_TRANSLOG_GENERATION_THRESHOLD_SIZE = 32 MB`
   - Smaller = more files but better cleanup granularity
   - Larger = fewer files but coarser cleanup

3. **Configure Lease Retention Period**:
   ```json
   {
     "index.soft_deletes.retention_lease.period": "12h"
   }
   ```
   - Expired leases are kept this long before removal
   - Allows temporary follower outages
   - Too short = replication breaks during outages
   - Too long = unnecessary overhead

---

## Key Takeaways

1. **Retention Leases Are Essential**: Without them, CCR cannot work reliably

2. **Two-Phase Lifecycle**:
   - Bootstrap: Use `RETAIN_ALL` to preserve everything
   - Continuous: Renew with specific seqNo as replication progresses

3. **Prevents Two Types of Deletion**:
   - Translog truncation (via custom deletion policy)
   - Soft delete merging (via merge policy checks)

4. **Automatic Management**: CCR handles lease lifecycle automatically
   - Add during bootstrap
   - Renew during replication
   - Remove on stop

5. **Monitor Translog Growth**: Main indicator of retention lease issues

6. **Failures Are Recoverable**: Full re-bootstrap is always an option

---

## Code References

| Component | File | Purpose |
|-----------|------|---------|
| Add lease (bootstrap) | `RemoteClusterRestoreLeaderService.kt:114-118` | Initial lease with RETAIN_ALL |
| Renew lease (start) | `ShardReplicationTask.kt:202-210` | Initial renewal with specific seqNo |
| Renew lease (ongoing) | `ShardReplicationTask.kt:290-313` | Continuous renewal as replication progresses |
| Remove lease (stop) | `IndexReplicationTask.kt:896` | Cleanup on stop |
| Retention lease helper | `RemoteClusterRetentionLeaseHelper.kt` | All lease operations |
| Translog deletion policy | `ReplicationTranslogDeletionPolicy.kt` | Prevents translog truncation |
| Enable on leader | `IndexReplicationTask.kt:setupAndStartRestore()` | Configure leader settings |

---

## Further Reading

- OpenSearch Retention Leases: https://opensearch.org/docs/latest/
- Lucene Soft Deletes: https://lucene.apache.org/
- CCR RFC: `docs/RFC.md` (section on retention leases)
- Integration Tests: `src/test/kotlin/` (search for "retention")
