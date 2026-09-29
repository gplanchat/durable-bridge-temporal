<?php

declare(strict_types=1);

namespace Gplanchat\Bridge\Temporal\Store;

/**
 * Who polls one task queue, as the server saw it.
 *
 * A stopped worker stays in the server's list for minutes, so its absence shows in `lastPolledAt`
 * getting old, not in the list emptying: {@see polledSince()} is the question to ask. A live worker
 * polls again about once a minute, the length of a long poll, so allow two:
 * `$queue->polledSince(new \DateTimeImmutable('-2 minutes'))`.
 */
final readonly class TaskQueuePollers
{
    public function __construct(
        public TaskQueueKind $kind,
        public string $taskQueue,
        public int $pollers,
        public ?\DateTimeImmutable $lastPolledAt,
        /** Why the server could not be asked; the rest is then unknown. */
        public ?string $error = null,
    ) {}

    /** Whether a worker is known to have polled at or after `$since`: never when the server did not answer. */
    public function polledSince(\DateTimeImmutable $since): bool
    {
        return null !== $this->lastPolledAt && $this->lastPolledAt >= $since;
    }
}
