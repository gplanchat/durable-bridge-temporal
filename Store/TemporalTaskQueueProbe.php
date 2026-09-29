<?php

declare(strict_types=1);

namespace Gplanchat\Bridge\Temporal\Store;

use Gplanchat\Bridge\Temporal\Grpc\TemporalGrpcTimeouts;
use Gplanchat\Bridge\Temporal\TemporalConnection;
use Gplanchat\Bridge\Temporal\WorkflowServiceClientInterface;
use Temporal\Api\Enums\V1\TaskQueueType;
use Temporal\Api\Taskqueue\V1\PollerInfo;
use Temporal\Api\Taskqueue\V1\TaskQueue;
use Temporal\Api\Workflowservice\V1\DescribeTaskQueueRequest;

/**
 * Which of the connection's task queues a worker polls.
 *
 * Without a worker on a queue, an execution stops at the first task of that kind and nothing
 * fails: the operator learns it from the customer. The host names the types it runs, because the
 * connection cannot tell: its Nexus queue follows the workflow queue whether anyone serves Nexus
 * or not.
 */
final class TemporalTaskQueueProbe
{
    public function __construct(
        private readonly WorkflowServiceClientInterface $client,
        private readonly TemporalConnection $connection,
    ) {}

    /**
     * @param list<TaskQueuePollers::WORKFLOW|TaskQueuePollers::ACTIVITY|TaskQueuePollers::NEXUS> $types
     *
     * @return list<TaskQueuePollers> one per type, in the order asked
     */
    public function describe(array $types = [TaskQueuePollers::WORKFLOW, TaskQueuePollers::ACTIVITY]): array
    {
        return array_map($this->describeOne(...), $types);
    }

    /**
     * @param TaskQueuePollers::WORKFLOW|TaskQueuePollers::ACTIVITY|TaskQueuePollers::NEXUS $type
     */
    private function describeOne(string $type): TaskQueuePollers
    {
        [$queue, $queueType] = match ($type) {
            TaskQueuePollers::WORKFLOW => [$this->connection->workflowTaskQueue->name(), TaskQueueType::TASK_QUEUE_TYPE_WORKFLOW],
            TaskQueuePollers::ACTIVITY => [$this->connection->activityTaskQueue->name(), TaskQueueType::TASK_QUEUE_TYPE_ACTIVITY],
            TaskQueuePollers::NEXUS => [$this->connection->nexusTaskQueue->name(), TaskQueueType::TASK_QUEUE_TYPE_NEXUS],
        };

        $request = new DescribeTaskQueueRequest();
        $request->setNamespace($this->connection->namespace->name());
        $request->setTaskQueue(new TaskQueue(['name' => $queue]));
        $request->setTaskQueueType($queueType);

        try {
            $response = $this->client->DescribeTaskQueue($request, [], ['timeout' => TemporalGrpcTimeouts::SHORT_US]);
        } catch (\Throwable $failure) {
            return new TaskQueuePollers($type, $queue, 0, null, $failure->getMessage());
        }

        $polls = [];
        /** @var PollerInfo $poller */
        foreach ($response->getPollers() as $poller) {
            $polls[] = (int) ($poller->getLastAccessTime()?->getSeconds() ?? 0);
        }
        $latest = max([0, ...$polls]);

        return new TaskQueuePollers($type, $queue, \count($polls), 0 === $latest ? null : new \DateTimeImmutable('@' . $latest));
    }
}
