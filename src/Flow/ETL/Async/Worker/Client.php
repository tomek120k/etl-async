<?php

namespace Flow\ETL\Async\Worker;

interface Client
{
    public function connect(string $id, string $host, int $port, ClientProtocol $protocol): void;
}