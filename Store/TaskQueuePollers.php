<?php

declare(strict_types=1);

namespace Gplanchat\Bridge\Temporal\Store;

/**
 * Who polls one task queue, as the server saw it.
 *
 * A stopped worker stays in the server's list for a while, so its absence shows in `lastPolledAt`
 * getting old, not in the list emptying: {@see polledSince()} is the question to ask. A live worker
 * polls again at least once a minute, the length of a long poll.
 */
final readonly class TaskQueuePollers
{
    public const WORKFLOW = 'workflow';
    public const ACTIVITY = 'activity';
    public const NEXUS = 'nexus';

    /** @param self::WORKFLOW|self::ACTIVITY|self::NEXUS $type */
    public function __construct(
        public string $type,
        public string $taskQueue,
        public int $pollers,
        public ?\DateTimeImmutable $lastPolledAt,
        /** Why the server could not be asked; the rest is then unknown. */
        public ?string $error = null,
    ) {}

    /** Whether a worker polled at or after `$since`; an unanswered probe is not an absent worker. */
    public function polledSince(\DateTimeImmutable $since): bool
    {
        return null !== $this->error || (null !== $this->lastPolledAt && $this->lastPolledAt >= $since);
    }
}
