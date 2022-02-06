<?php

declare(strict_types=1);

namespace Flow\ETL\Async\Server;

interface Server
{
    public function initialize(ServerProtocol $handler) : void;

    public function start() : void;

    public function stop() : void;
}