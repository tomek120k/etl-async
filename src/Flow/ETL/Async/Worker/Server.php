<?php

namespace Flow\ETL\Async\Worker;

use Flow\ETL\Async\Communication\Message;

interface Server
{
    public function send(Message $message) : void;
}