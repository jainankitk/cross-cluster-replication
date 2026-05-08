# Checkpoints vs Sequence Numbers in OpenSearch

## Quick Answer

**Sequence Number (SeqNo)**: The unique identifier assigned to EACH individual operation (create, update, delete).

**Checkpoint**: A marker indicating "all operations up to THIS sequence number have been successfully processed."

Think of it this way:
- **SeqNo** = Individual page numbers in a book (1, 2, 3, 4...)
- **Checkpoint** = Bookmark saying "I've read up to page X"

---

## Detailed Explanation

### Sequence Numbers (SeqNo)

Every write operation in OpenSearch gets a unique, monotonically increasing sequence number **per shard**:

```
Operation 1: Create doc "user-1"     → seqNo = 0
Operation 2: Update doc "user-1"     → seqNo = 1
Operation 3: Delete doc "user-2"     → seqNo = 2
Operation 4: Create doc "user-3"     → seqNo = 3
... and so on
```

**Key Properties**:
- **Unique per shard**: Each shard has its own sequence number space starting from 0
- **Monotonically increasing**: Never decreases, always goes up
- **Assigned on primary**: The primary shard assigns the seqNo
- **Immutable**: Once assigned, never changes
- **Gaps possible**: If operations fail or are skipped, gaps can exist

**In Code**:
```kotlin
// Each operation has its own seqNo
operation.seqNo()  // Returns the unique seqNo for this operation

// Example from GetChangesRequest
val fromSeqNo: Long = 1000
val toSeqNo: Long = 1100
// This means "get me operations with seqNo between 1000 and 1100"
```

---

### Checkpoints

A checkpoint is a **milestone marker** indicating that all operations up to a certain sequence number have been successfully processed. There are two types:

#### 1. Local Checkpoint

**Definition**: The highest sequence number for which **all prior operations** have been successfully processed on this shard copy.

```
Operations processed: 0, 1, 2, 3, 5, 6, 7
                                 ↑ Missing seqNo 4!

Local Checkpoint = 3  (cannot advance past the gap)
```

**Why it matters**: 
- Indicates durable, contiguous progress
- Cannot advance if there's a gap in the sequence
- Used to determine what can be safely deleted

**In Code**:
```kotlin
// From ShardReplicationTask.kt:199
val indexShard = followerIndexService.getShard(followerShardId.id)
val localCheckpoint = indexShard.localCheckpoint

// Example: localCheckpoint = 4999
// Means: All operations 0-4999 have been processed
```

#### 2. Global Checkpoint (or lastSyncedGlobalCheckpoint)

**Definition**: The highest sequence number for which **all operations** have been successfully processed on **all active copies** (primary + all replicas) of the shard.

```
Primary shard:    localCheckpoint = 1000
Replica 1:        localCheckpoint = 998
Replica 2:        localCheckpoint = 999

Global Checkpoint = 998  (minimum across all copies)
```

**Why it matters**:
- Indicates data that is fully replicated and durable
- Safe to use for recovery and synchronization
- Used by CCR to determine what to replicate

**In Code**:
```kotlin
// From ShardReplicationTask.kt:204
retentionLeaseHelper.renewRetentionLease(
    leaderShardId, 
    indexShard.lastSyncedGlobalCheckpoint + 1,  // ← Global checkpoint
    followerShardId
)
```

---

## Visual Comparison

### Sequence Numbers: Individual Operations

```
Timeline of operations on a shard:

seqNo:  0    1    2    3    4    5    6    7    8    9
        │    │    │    │    │    │    │    │    │    │
Op:    [C]  [U]  [D]  [C]  [U]  [U]  [C]  [D]  [U]  [C]
       │    │    │    │    │    │    │    │    │    │
Doc:   A    A    B    C    A    C    D    D    B    E

C = Create, U = Update, D = Delete

Each operation has its own unique seqNo
```

### Checkpoints: Progress Markers

```
State of a shard:

seqNo:      0    1    2    3    4    5    6    7    8    9
            │    │    │    │    │    │    │    │    │    │
Status:    [✓]  [✓]  [✓]  [✓]  [✓]  [✓]  [?]  [?]  [?]  [?]
            └────┴────┴────┴────┴────┘
                                    │
                    Local Checkpoint = 5
                    
"All operations from 0 to 5 have been successfully processed"

Operations 6-9 might be:
- In flight (being processed)
- Not yet received
- Failed and being retried
```

---

## Checkpoints in Cross-Cluster Replication

### Follower Shard Tracking

**Location**: `ShardReplicationChangesTracker.kt:38-39`

```kotlin
// Track progress using follower's local checkpoint
private val observedSeqNoAtLeader = AtomicLong(indexShard.localCheckpoint)
private val seqNoAlreadyRequested = AtomicLong(indexShard.localCheckpoint)
```

**What's Happening**:

1. **Initial State**: Follower starts with `localCheckpoint = 4999`
   - Means: "I have operations 0-4999"
   - Need: Operations starting from 5000

2. **Request Batch**: `requestBatchToFetch()` returns `(5000, 5100)`
   ```kotlin
   val fromSeq = seqNoAlreadyRequested.getAndAdd(currentBatchSize) + 1
   // fromSeq = 4999 + 1 = 5000
   // toSeq = 5000 + 100 - 1 = 5099
   ```

3. **Fetch Changes**: `getChanges(fromSeqNo = 5000, toSeqNo = 5100)`
   - Request operations in this seqNo range
   - Leader returns operations + its `lastSyncedGlobalCheckpoint`

4. **Apply Changes**: TranslogSequencer applies operations in order
   - Follower's `localCheckpoint` advances as operations complete

5. **Update Tracking**: `updateBatchFetched()`
   ```kotlin
   observedSeqNoAtLeader.updateAndGet { 
       if (seqNoAtLeader > value) seqNoAtLeader else value 
   }
   ```

### GetChanges API

**Location**: `GetChangesRequest.kt` & `GetChangesResponse.kt`

**Request**:
```kotlin
class GetChangesRequest(
    val shardId: ShardId,
    val fromSeqNo: Long,      // Start of range
    val toSeqNo: Long         // End of range
)
```

**Response**:
```kotlin
class GetChangesResponse(
    val changes: List<Translog.Operation>,  // Actual operations
    val fromSeqNo: Long,                     // Starting seqNo of batch
    val maxSeqNoOfUpdatesOrDeletes: Long,    // Highest seqNo in batch
    val lastSyncedGlobalCheckpoint: Long     // Leader's current checkpoint
)
```

**Example**:
```kotlin
// Follower requests:
GetChangesRequest(
    shardId = leader-01[0],
    fromSeqNo = 5000,
    toSeqNo = 5100
)

// Leader responds:
GetChangesResponse(
    changes = [Op(seqNo=5000), Op(seqNo=5001), ..., Op(seqNo=5050)],
    fromSeqNo = 5000,
    maxSeqNoOfUpdatesOrDeletes = 5050,
    lastSyncedGlobalCheckpoint = 5150  // ← Leader's current position!
)
```

**Key Insight**: 
- Request asks for operations by seqNo range
- Response includes leader's global checkpoint
- Follower now knows: "Leader is at 5150, I'm at 5050, I'm 100 operations behind"

---

## Why Global Checkpoint for Retention Leases?

**Location**: `ShardReplicationTask.kt:203-204`

```kotlin
retentionLeaseHelper.renewRetentionLease(
    leaderShardId, 
    indexShard.lastSyncedGlobalCheckpoint + 1,  // Why global, not local?
    followerShardId
)
```

### Why Not Local Checkpoint?

**Local checkpoint** only guarantees processing on this follower shard copy. If the follower shard has replicas, they might be behind:

```
Follower Primary:   localCheckpoint = 5000
Follower Replica 1: localCheckpoint = 4998
Follower Replica 2: localCheckpoint = 4999
```

If the follower primary fails and replica 1 is promoted, it would need operations from 4998, not 5000!

### Why Global Checkpoint + 1?

**Global checkpoint** ensures ALL copies of the follower shard have the data:

```kotlin
// Global checkpoint = 4999 means:
// - Primary has operations 0-4999
// - ALL replicas have operations 0-4999
// - All copies are in sync up to 4999

// Retention lease = 4999 + 1 = 5000 means:
// "Preserve operations starting from 5000 onward"
// Everything before 5000 is safely replicated
```

**The +1 is critical**:
- Checkpoint 4999 = "Have through 4999"
- Retention lease 5000 = "Need from 5000 onward"
- Inclusive retention: preserves seqNo >= 5000

---

## Practical Example: Following Replication

### Initial State

```
LEADER SHARD:
  Current seqNo: 5600
  Global Checkpoint: 5600 (all caught up)
  
FOLLOWER SHARD:
  Local Checkpoint: 4999
  Global Checkpoint: 4999
  
What happens next?
```

### Step 1: Start Replication

```kotlin
// Follower initializes
logInfo("Shard replication started: 
         localCheckpoint=${indexShard.localCheckpoint},        // 4999
         globalCheckpoint=${indexShard.lastSyncedGlobalCheckpoint}")  // 4999

// Add retention lease to leader
retentionLeaseHelper.renewRetentionLease(
    leaderShardId, 
    5000,  // globalCheckpoint (4999) + 1
    followerShardId
)
```

**Leader state**:
```
Retention Lease: retainingSeqNo = 5000
Message: "Preserve operations from 5000 onward"
```

### Step 2: Request Batch

```kotlin
// Follower's change tracker
observedSeqNoAtLeader = 4999      // What we know leader has
seqNoAlreadyRequested = 4999      // What we've asked for

// Request next batch
val batch = requestBatchToFetch()  // Returns (5000, 5100)

// Track that we've requested up to 5100
seqNoAlreadyRequested = 5100
```

### Step 3: Fetch Changes

```kotlin
val changesResponse = getChanges(fromSeqNo = 5000, toSeqNo = 5100)

// Response contains:
changes = [
    Op(seqNo=5000, type=INDEX, doc=...),
    Op(seqNo=5001, type=UPDATE, doc=...),
    ...
    Op(seqNo=5099, type=DELETE, doc=...)
]
lastSyncedGlobalCheckpoint = 5600  // Leader's position!
```

**Follower learns**: "Leader is at 5600, I just got 5000-5099, need to catch up to 5600"

### Step 4: Apply Changes

```kotlin
// TranslogSequencer applies operations in order
// As each operation completes:

// After applying seqNo 5000:
indexShard.localCheckpoint = 5000  // (assuming no gaps)

// After applying seqNo 5001:
indexShard.localCheckpoint = 5001

// ... continues ...

// After applying all operations up to 5099:
indexShard.localCheckpoint = 5099
```

### Step 5: Update Tracking

```kotlin
changeTracker.updateBatchFetched(
    success = true,
    fromSeqNoRequested = 5000,
    toSeqNoRequested = 5100,
    toSeqNoReceived = 5099,        // Got up to here
    seqNoAtLeader = 5600           // Leader's position
)

// Update observed leader position
observedSeqNoAtLeader = 5600

// Now we know:
// - We have: 0-5099
// - Leader has: 0-5600
// - Still need: 5100-5600
```

### Step 6: Renew Retention Lease

```kotlin
// After writing changes successfully
retentionLeaseHelper.renewRetentionLease(
    leaderShardId, 
    indexShard.lastSyncedGlobalCheckpoint + 1,  // 5099 + 1 = 5100
    followerShardId
)
```

**Leader state updated**:
```
Retention Lease: retainingSeqNo = 5100 (was 5000)
Message: "Preserve operations from 5100 onward"
Leader can now cleanup operations 5000-5099 (already replicated)
```

### Step 7: Next Iteration

```kotlin
// Request next batch
val nextBatch = requestBatchToFetch()  // Returns (5100, 5200)

// Fetch, apply, update...
// Process repeats until caught up
```

---

## Sequence Number Gaps and Checkpoints

### Scenario: Missing Operation

```
Operations received:
seqNo:  5000  5001  5002  [MISSING 5003]  5004  5005

Processing state:
        ✓     ✓     ✓           ?          ✓     ✓

Local Checkpoint = 5002  (cannot advance past gap!)
```

**Why checkpoint stops at gap**:
- Checkpoint means "all prior operations processed"
- Missing 5003 means we DON'T have all prior operations to 5004
- Checkpoint remains at 5002 until 5003 arrives

**In CCR**:
```kotlin
// TranslogSequencer handles this
private val unAppliedChanges = ConcurrentHashMap<Long, GetChangesResponse>()

// Operations 5004-5005 are buffered
unAppliedChanges[5004] = response1
unAppliedChanges[5005] = response2

// Wait for 5003
var highWatermark = 5002
while (unAppliedChanges.containsKey(highWatermark + 1)) {
    val response = unAppliedChanges.remove(highWatermark + 1)
    // Apply operations from response
    highWatermark++
}
// Once 5003 arrives, can apply 5003, 5004, 5005 in order
```

---

## Checkpoint Synchronization Across Replicas

### Local vs Global in Detail

```
SHARD with 1 Primary + 2 Replicas:

Time T1: Write operation seqNo=100 to primary
  Primary:    localCheckpoint = 100  ✓
  Replica 1:  localCheckpoint = 99   ⏳ (replicating)
  Replica 2:  localCheckpoint = 99   ⏳ (replicating)
  
  Global Checkpoint = 99  (minimum of all)

Time T2: Operation 100 replicated to Replica 1
  Primary:    localCheckpoint = 100  ✓
  Replica 1:  localCheckpoint = 100  ✓
  Replica 2:  localCheckpoint = 99   ⏳ (still replicating)
  
  Global Checkpoint = 99  (still waiting for Replica 2)

Time T3: Operation 100 replicated to Replica 2
  Primary:    localCheckpoint = 100  ✓
  Replica 1:  localCheckpoint = 100  ✓
  Replica 2:  localCheckpoint = 100  ✓
  
  Global Checkpoint = 100  (all copies have it!)
```

**lastSyncedGlobalCheckpoint**:
- The most recent global checkpoint that was synced to disk
- Slightly behind global checkpoint (which may be in memory)
- Most conservative measure of durable, replicated data

---

## Key Differences Summary

| Aspect | Sequence Number | Checkpoint |
|--------|----------------|-----------|
| **What** | Identifier for individual operation | Progress marker for batch of operations |
| **Scope** | Per operation | Per shard state |
| **Type** | Exact value | Threshold value |
| **Meaning** | "This is operation X" | "All operations up to X are done" |
| **Changes** | Never (immutable) | Frequently (as work progresses) |
| **Gaps** | Can have gaps | Cannot advance over gaps |
| **In Requests** | Specify range (from/to seqNo) | Not directly sent |
| **In Responses** | Each operation has one | Leader's current position |
| **Retention Lease** | Based on checkpoint | Specified as seqNo threshold |
| **Code Access** | `operation.seqNo()` | `indexShard.localCheckpoint` |

---

## Common Patterns in CCR Code

### Pattern 1: Determine What to Fetch

```kotlin
// Use follower's checkpoint to know where to start
val startSeqNo = indexShard.localCheckpoint + 1  // Need operations AFTER checkpoint
val endSeqNo = startSeqNo + batchSize - 1

val request = GetChangesRequest(leaderShardId, startSeqNo, endSeqNo)
```

### Pattern 2: Track Progress

```kotlin
// Use leader's checkpoint to know how far behind we are
val response = getChanges(fromSeqNo, toSeqNo)
val leaderPosition = response.lastSyncedGlobalCheckpoint
val followerPosition = indexShard.localCheckpoint

val lag = leaderPosition - followerPosition  // Operations behind
```

### Pattern 3: Safe Deletion

```kotlin
// Use checkpoint for retention lease renewal
val safeToDeleteUpTo = indexShard.lastSyncedGlobalCheckpoint
val preserveFrom = safeToDeleteUpTo + 1

retentionLeaseHelper.renewRetentionLease(leaderShardId, preserveFrom, followerShardId)
// Leader can now delete operations with seqNo < preserveFrom
```

### Pattern 4: Log Progress

```kotlin
// From ShardReplicationTask.kt:199
logInfo("Shard replication started: 
        leaderShard=$leaderShardId, 
        followerShard=$followerShardId, 
        localCheckpoint=${indexShard.localCheckpoint},              // Where we are
        globalCheckpoint=${indexShard.lastSyncedGlobalCheckpoint}") // Safe position
```

---

## Debugging Tips

### Check Replication Lag

```bash
# On follower cluster
GET /follower-index/_stats/shards?level=shards

# Look for:
"seq_no": {
  "max_seq_no": 5600,           # Highest seqNo seen
  "local_checkpoint": 5550,     # What's been processed
  "global_checkpoint": 5550     # What's on all replicas
}
```

### Check Leader Position

```bash
# On leader cluster  
GET /leader-index/_stats/shards?level=shards

# Look for:
"seq_no": {
  "max_seq_no": 5650,           # Highest seqNo assigned
  "local_checkpoint": 5650,
  "global_checkpoint": 5650
}

# Calculate lag:
# Leader global_checkpoint (5650) - Follower global_checkpoint (5550) = 100 ops behind
```

### Check Retention Lease Position

```bash
# On leader cluster
GET /leader-index/_stats/shards?level=shards

# Look for:
"retention_leases": {
  "leases": [
    {
      "id": "replication:follower-cluster:...",
      "retaining_seq_no": 5551,    # Preserve from here onward
      ...
    }
  ]
}

# This should be: follower global_checkpoint + 1
```

---

## Key Takeaways

1. **SeqNo = Identity**: Each operation gets a unique sequence number
2. **Checkpoint = Progress**: Marks how far we've successfully processed
3. **Global Checkpoint = Safety**: What's safe across all replicas
4. **Use Global for Retention**: Ensures no replica is left behind
5. **Checkpoint + 1 = Next Needed**: Natural boundary for "what to preserve"
6. **Gaps Block Checkpoints**: Missing operations prevent checkpoint advancement
7. **Leader's Checkpoint = Target**: Tells follower how much to catch up

Understanding the distinction is crucial for:
- Determining what operations to fetch
- Knowing when data is safely replicated
- Setting appropriate retention lease boundaries
- Debugging replication lag issues
- Understanding why checkpoints don't advance

---

## Further Reading

- OpenSearch Sequence Numbers: Core concept documentation
- Lucene Sequence Numbers: How they're implemented in Lucene
- Global Checkpoint Sync: How OpenSearch synchronizes checkpoints
- CCR Replication Flow: `docs/RFC.md` section on replication mechanics
- Code: `ShardReplicationChangesTracker.kt` - See how tracking works in practice
