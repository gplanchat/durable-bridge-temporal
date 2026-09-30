<?php

declare(strict_types=1);

namespace Gplanchat\Bridge\Temporal\Codec;

use Google\Protobuf\Any;
use Google\Protobuf\DescriptorPool;
use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\Message;
use Gplanchat\Bridge\Temporal\AbstractWorkflowServiceClient;
use Gplanchat\Bridge\Temporal\Grpc\TemporalGrpcTimeouts;
use Gplanchat\Bridge\Temporal\WorkflowServiceClientInterface;
use Temporal\Api\Common\V1\Payload;
use Temporal\Api\Enums\V1\ActivityTaskFailedCause;
use Temporal\Api\Enums\V1\WorkflowTaskFailedCause;
use Temporal\Api\Failure\V1\ApplicationFailureInfo;
use Temporal\Api\Failure\V1\Failure;
use Temporal\Api\Workflowservice\V1\PollActivityTaskQueueRequest;
use Temporal\Api\Workflowservice\V1\PollWorkflowTaskQueueRequest;
use Temporal\Api\Workflowservice\V1\RespondActivityTaskFailedRequest;
use Temporal\Api\Workflowservice\V1\RespondWorkflowTaskFailedRequest;

/**
 * Applies a {@see PayloadCodecInterface} where every RPC passes (DUR055): the payloads of a request
 * are encoded on the way out, those of a response decoded on the way in.
 *
 * The walk reads the generated descriptors, so a message a later API version adds is covered with
 * no list to keep. Search attributes are skipped by message type: the server must index them.
 * An `Any` is unpacked, walked and packed again: the update protocol carries its requests and
 * results that way. This is where Temporal's Go SDK puts its gRPC codec interceptor, and what it
 * skips.
 *
 * The request is copied, which costs its size once more; the response is decoded in place, so the
 * inner client must hand out a response of its own, not a shared one.
 */
final class PayloadCodecWorkflowServiceClient extends AbstractWorkflowServiceClient
{
    private const SEARCH_ATTRIBUTES = 'temporal.api.common.v1.SearchAttributes';
    private const GRPC_NOT_FOUND = 5;

    public function __construct(
        private readonly WorkflowServiceClientInterface $inner,
        private readonly PayloadCodecInterface $codec,
    ) {}

    /**
     * @template T of Message
     *
     * @param class-string<T>      $responseClass
     * @param array<string, mixed> $metadata
     * @param array<string, mixed> $options
     *
     * @return T
     */
    protected function call(string $rpc, Message $request, string $responseClass, array $metadata, array $options): Message
    {
        // A copy: a caller that sends the same request again must not find it encoded twice.
        $copy = new ($request::class)();
        $copy->mergeFromString($request->serializeToString());
        $this->walk($copy, $this->codec->encode(...));

        $response = $this->inner->{$rpc}($copy, $metadata, $options);
        \assert($response instanceof $responseClass);

        try {
            $this->walk($response, $this->codec->decode(...));
        } catch (\Throwable $e) {
            if (!$this->failTask($copy, $response, $e)) {
                throw $e;
            }

            // An empty poll, as after a long poll that found nothing: every worker loop polls again.
            return new $responseClass();
        }

        return $response;
    }

    /**
     * A polled task that cannot be decoded is answered as failed (#775), here because this is the
     * one place that holds both the task token and the error. Left to throw, it stopped the worker,
     * and the next worker to receive the task stopped too.
     *
     * The failure carries the error's class and message, never its stack trace: the trace quotes
     * arguments, and those of a decrypt call are the key or the plaintext this server must not see.
     *
     * @return bool false when the call is not a task poll, whose caller gets the error instead
     */
    private function failTask(Message $request, Message $response, \Throwable $error): bool
    {
        if ($request instanceof PollWorkflowTaskQueueRequest) {
            $failed = new RespondWorkflowTaskFailedRequest(['cause' => WorkflowTaskFailedCause::WORKFLOW_TASK_FAILED_CAUSE_WORKFLOW_WORKER_UNHANDLED_FAILURE]);
            $rpc = 'RespondWorkflowTaskFailed';
        } elseif ($request instanceof PollActivityTaskQueueRequest) {
            // A server older than this field ignores it, as proto3 does with any unknown field.
            $failed = new RespondActivityTaskFailedRequest(['cause' => ActivityTaskFailedCause::ACTIVITY_TASK_FAILED_CAUSE_ACTIVITY_WORKER_UNHANDLED_FAILURE]);
            $rpc = 'RespondActivityTaskFailed';
        } else {
            return false;
        }
        \assert(method_exists($response, 'getTaskToken'));

        $failure = new Failure();
        $failure->setMessage(sprintf('Payload decode failed: %s', $error->getMessage()));
        $failure->setSource('DurablePayloadCodec');
        $failure->setApplicationFailureInfo(new ApplicationFailureInfo(['type' => $error::class]));

        $failed->setNamespace($request->getNamespace());
        $failed->setIdentity($request->getIdentity());
        $failed->setTaskToken($response->getTaskToken());
        $failed->setFailure($failure);

        try {
            $this->inner->{$rpc}($failed, [], ['timeout' => TemporalGrpcTimeouts::SHORT_US]);
        } catch (\RuntimeException $e) {
            // The task already closed or timed out: nothing is left to answer.
            if (self::GRPC_NOT_FOUND !== $e->getCode()) {
                throw $e;
            }
        }

        return true;
    }

    /**
     * @param \Closure(Payload): Payload $transform
     */
    private function walk(Message $message, \Closure $transform): void
    {
        if ($message instanceof Any) {
            // An unknown type throws here rather than leaving its payloads untouched.
            $packed = $message->unpack();
            $this->walk($packed, $transform);
            $message->pack($packed);

            return;
        }
        $descriptor = DescriptorPool::getGeneratedPool()->getDescriptorByClassName($message::class);
        if (self::SEARCH_ATTRIBUTES === $descriptor->getFullName()) {
            return;
        }

        for ($index = 0; $index < $descriptor->getFieldCount(); ++$index) {
            $field = $descriptor->getField($index);
            if (GPBType::MESSAGE !== $field->getType()) {
                continue;
            }
            $name = str_replace('_', '', ucwords($field->getName(), '_'));
            $value = $message->{'get' . $name}();
            if ($value instanceof Payload) {
                $message->{'set' . $name}($transform($value));
            } elseif ($value instanceof Message) {
                $this->walk($value, $transform);
            } elseif (is_iterable($value)) {
                // A repeated field or a map: both are replaced item by item, keys kept.
                foreach ($value as $key => $item) {
                    if ($item instanceof Payload) {
                        $value[$key] = $transform($item);
                    } elseif ($item instanceof Message) {
                        $this->walk($item, $transform);
                    }
                }
            }
        }
    }
}
