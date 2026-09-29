<?php

declare(strict_types=1);

namespace Gplanchat\Bridge\Temporal\Store;

/** The kinds of task a worker polls for, one queue each on the connection. */
enum TaskQueueKind: string
{
    case Workflow = 'workflow';
    case Activity = 'activity';
    case Nexus = 'nexus';
}
