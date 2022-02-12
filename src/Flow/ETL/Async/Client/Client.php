<?php declare(strict_types=1);

namespace Flow\ETL\Async\Client;

interface Client
{
    public function connect(string $id, string $host, int $port, ClientProtocol $protocol) : void;
}
