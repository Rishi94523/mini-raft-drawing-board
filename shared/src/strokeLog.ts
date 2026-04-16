import { LogEntry } from "./types.js";

/**
 * Append-only, index-ordered stroke log for a single Raft replica.
 *
 * Entries are keyed by their `index` field.  The log is a sparse-safe array
 * but in normal operation every index from 0 to lastIndex is always present.
 *
 * Member 2 owns this class.
 */
export class StrokeLog {
  private entries: LogEntry[] = [];

  // ─── read ──────────────────────────────────────────────────────────────────

  get length(): number {
    return this.entries.length;
  }

  get lastIndex(): number {
    return this.entries.length - 1;
  }

  get lastTerm(): number {
    if (this.entries.length === 0) return -1;
    return this.entries[this.entries.length - 1].term;
  }

  /**
   * Returns the entry at position `index`, or `undefined` if absent.
   */
  getAt(index: number): LogEntry | undefined {
    if (index < 0 || index >= this.entries.length) return undefined;
    return this.entries[index];
  }

  /**
   * Returns all entries whose `index` >= `fromIndex`.
   * Used by `handleSyncLog` so a restarted follower can catch up.
   */
  getFrom(fromIndex: number): LogEntry[] {
    if (fromIndex < 0) return [...this.entries];
    return this.entries.filter((e) => e.index >= fromIndex);
  }

  /**
   * Returns all entries up to and including `upTo` (the commit index).
   */
  getCommitted(upTo: number): LogEntry[] {
    return this.entries.filter((e) => e.index <= upTo);
  }

  /**
   * Returns all entries as a shallow copy (debugging / GET /log).
   */
  all(): LogEntry[] {
    return [...this.entries];
  }

  // ─── write ─────────────────────────────────────────────────────────────────

  /**
   * Appends a single entry.
   *
   * Rules:
   * - The entry's `index` must equal `lastIndex + 1` (strict monotonic).
   * - Throws if the index is out of order so callers surface bugs early.
   *
   * Callers should call `truncateTo` first if a conflict was detected.
   */
  append(entry: LogEntry): void {
    const expected = this.entries.length; // next expected index
    if (entry.index !== expected) {
      throw new Error(
        `StrokeLog.append: expected index ${expected}, got ${entry.index}`
      );
    }
    this.entries.push(entry);
  }

  /**
   * Removes all entries with `index > keepUpTo`.
   *
   * After this call `lastIndex === keepUpTo` (assuming keepUpTo was valid).
   * Used to resolve log conflicts on a follower when the leader sends entries
   * that diverge from what the follower already has.
   */
  truncateTo(keepUpTo: number): void {
    this.entries = this.entries.filter((e) => e.index <= keepUpTo);
  }

  /**
   * Idempotent helper used during append-entries processing.
   *
   * Returns true  → entry already present and identical (skip).
   * Returns false → entry conflicts or is simply absent (append/overwrite).
   */
  hasMatchingEntry(entry: LogEntry): boolean {
    const existing = this.getAt(entry.index);
    if (!existing) return false;
    return existing.term === entry.term && existing.stroke.id === entry.stroke.id;
  }
}
