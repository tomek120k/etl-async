<?php

namespace Flow\ETL\Async\Worker;

use Flow\ETL\Async\Communication\Message;

interface Server
{
    public const DEFAULT_PORT = 6651;

    public function send(Message $message) : void;
}