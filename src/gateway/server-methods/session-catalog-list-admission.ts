import { AsyncLocalStorage } from "node:async_hooks";

type QueuedProviderList = {
  start: () => void;
};

class SessionCatalogListBusyError extends Error {
  readonly code = "catalog_busy";

  constructor(maxConcurrent: number, maxQueued: number) {
    super(`session catalog is busy (${maxConcurrent} active, ${maxQueued} queued); retry shortly`);
    this.name = "SessionCatalogListBusyError";
  }
}

export class SessionCatalogListAdmission {
  private active = 0;
  private readonly queue: QueuedProviderList[] = [];

  constructor(
    private readonly maxConcurrent: number,
    private readonly maxQueued: number,
  ) {
    if (!Number.isInteger(maxConcurrent) || maxConcurrent < 1) {
      throw new Error("maxConcurrent must be a positive integer");
    }
    if (!Number.isInteger(maxQueued) || maxQueued < 0) {
      throw new Error("maxQueued must be a non-negative integer");
    }
  }

  run<T>(task: () => Promise<T>, signal?: AbortSignal): Promise<T> {
    if (signal?.aborted) {
      // oxlint-disable-next-line typescript/prefer-promise-reject-errors -- AbortSignal preserves its exact reason, including non-Error values.
      return Promise.reject(signal.reason);
    }
    if (this.active < this.maxConcurrent) {
      return this.start(task);
    }
    if (this.queue.length >= this.maxQueued) {
      return Promise.reject(new SessionCatalogListBusyError(this.maxConcurrent, this.maxQueued));
    }
    // A released slot runs the next caller's plugin and root scope, never the
    // preceding provider's context inherited by the queue drain.
    const runInAsyncContext = AsyncLocalStorage.snapshot();
    return new Promise<T>((resolve, reject) => {
      const queued: QueuedProviderList = {
        start: () => {
          signal?.removeEventListener("abort", onAbort);
          void runInAsyncContext(() => this.start(task)).then(resolve, reject);
        },
      };
      const onAbort = () => {
        const index = this.queue.indexOf(queued);
        if (index < 0) {
          return;
        }
        this.queue.splice(index, 1);
        signal?.removeEventListener("abort", onAbort);
        // oxlint-disable-next-line typescript/prefer-promise-reject-errors -- AbortSignal preserves its exact reason, including non-Error values.
        reject(signal?.reason);
      };
      // Only waiting work can retire here; started providers own their physical completion.
      this.queue.push(queued);
      signal?.addEventListener("abort", onAbort, { once: true });
      if (signal?.aborted) {
        onAbort();
      }
    });
  }

  private async start<T>(task: () => Promise<T>): Promise<T> {
    this.active += 1;
    try {
      return await task();
    } finally {
      // Release before draining so every settlement, including rejection, hands
      // exactly one slot to the oldest waiter instead of leaking capacity.
      this.active -= 1;
      this.drain();
    }
  }

  private drain(): void {
    while (this.active < this.maxConcurrent) {
      const next = this.queue.shift();
      if (!next) {
        return;
      }
      next.start();
    }
  }
}
