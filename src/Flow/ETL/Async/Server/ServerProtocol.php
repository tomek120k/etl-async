<?php

declare(strict_types=1);

namespace Flow\ETL\Async\Server;

use Flow\ETL\Async\Communication\Message;
use Flow\ETL\Async\Communication\Protocol;
use Flow\ETL\Async\Worker\Pool;
use Flow\ETL\Pipeline\Pipes;

final class ServerProtocol
{
    private Pool $workers;

    private \Generator $rows;

    private Pipes $pipes;

    /**
     * @var ?callable(Rows $rows) : void
     */
    private $callback;

    /**
     * MessageHandler constructor.
     * @param Pool $workers
     * @param \Generator $rows
     * @param Pipes $pipes
     * @param ?callable(Rows $rows) : void $callback
     */
    public function __construct(Pool $workers, \Generator $rows, Pipes $pipes, ?callable $callback = null)
    {
        $this->rows = $rows;
        $this->pipes = $pipes;
        $this->workers = $workers;
        $this->callback = $callback;
    }

    public function handle(Message $message, Client $client, Server $server) : void
    {
        switch ($message->type()) {
            case Protocol::CLIENT_IDENTIFY:

                $client->send(Message::serverPipes($this->pipes));
                break;
            case Protocol::CLIENT_FETCH:

                if ($this->rows->valid()) {
                    $client->send(Message::rows($this->rows->current()));
                    $this->rows->next();
                } else {
                    $server->stop();
                }

                break;
            case Protocol::CLIENT_PROCESSED:
                if ($this->callback !== null) {
                    ($this->callback)($message->payload()['rows']);
                }

                if ($this->rows->valid() && $server) {
                    $client->send(Message::rows($this->rows->current()));
                    $this->rows->next();
                } else {
                    $server->stop();
                }

                break;
        }
    }
}