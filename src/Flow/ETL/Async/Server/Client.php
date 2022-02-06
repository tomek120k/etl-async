<?php

declare(strict_types=1);

namespace Flow\ETL\Async\Server;

use Flow\ETL\Async\Communication\Message;

interface Client
{
    public function send(Message $message) : void;

    public function disconnect() : void;
}