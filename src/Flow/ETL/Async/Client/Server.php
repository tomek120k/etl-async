<?php declare(strict_types=1);

namespace Flow\ETL\Async\Client;

use Flow\ETL\Async\Communication\Message;

interface Server
{
    public const DEFAULT_PORT = 6651;

    public function send(Message $message) : void;
}
