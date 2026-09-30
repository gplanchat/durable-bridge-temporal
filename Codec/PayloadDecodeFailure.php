<?php

declare(strict_types=1);

namespace Gplanchat\Bridge\Temporal\Codec;

/**
 * A payload read outside a task poll could not be decoded; the codec's own error is the previous
 * exception. The workflow worker catches this type when it reads a later history page (#824), and
 * only this one: a transport error on the same call still propagates.
 */
final class PayloadDecodeFailure extends \RuntimeException {}
