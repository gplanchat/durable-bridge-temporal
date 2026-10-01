<?php

declare(strict_types=1);

namespace Gplanchat\Bridge\Temporal\Codec;

/**
 * A payload read outside a task poll could not be decoded; the codec's own error is the previous
 * exception. The workflow worker catches this type when it reads a later history page (#824), and
 * only this one: a transport error on the same call still propagates. The workflow task runner
 * also throws it when the history does not read, a started memo whose execution id is invalid
 * included (#890); the {@see \JsonException} is then the previous exception.
 */
final class PayloadDecodeFailure extends \RuntimeException {}
