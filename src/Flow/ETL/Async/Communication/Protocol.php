<?php

declare(strict_types=1);

namespace Flow\ETL\Async\Communication;

final class Protocol
{
    public const CLIENT_IDENTIFY = 'identify';

    public const SERVER_PIPES = 'pipes';

    public const CLIENT_PROCESSED = 'processed';

    public const CLIENT_FETCH = 'fetch';

    public const SERVER_PROCESS = 'process';
}