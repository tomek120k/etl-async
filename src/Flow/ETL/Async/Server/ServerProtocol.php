<?php

declare(strict_types=1);

namespace Flow\ETL\Async\Server;

use Flow\ETL\Async\Communication\Message;
use Flow\ETL\Async\Communication\Protocol;
use Flow\ETL\Async\Client\Pool;
use Flow\ETL\Pipeline\Pipes;
use Flow\ETL\Rows;

final class ServerProtocol
{
    private Pool $workers;

    /**
     * @var \Generator<int, Rows, mixed, void>
     */
    private \Generator $rows;

    private Pipes $pipes;

    /**
     * @var ?callable(Rows) : void
     */
    private $callback;

    /**
     * MessageHandler constructor.
     *
     * @param Pool $workers
     * @param \Generator<int, Rows, mixed, void> $rows
     * @param Pipes $pipes
     * @param null|callable(Rows $rows) : void $callback
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
                /** @phpstan-ignore-next-line  */
                if ($this->workers->has((string) $message->payload()['id'])) {
                    $client->send(Message::pipes($this->pipes));
                } else {
                    $client->disconnect();
                }

                break;
            case Protocol::CLIENT_FETCH:

                if ($this->rows->valid()) {
                    $client->send(Message::process($this->rows->current()));
                    $this->rows->next();
                } else {
                    $server->stop();
                }

                break;
            case Protocol::CLIENT_PROCESSED:
                if ($this->callback !== null) {
                    /**
                     * @psalm-suppress MixedArgument
                     */
                    ($this->callback)($message->payload()['rows']);
                }

                if ($this->rows->valid()) {
                    $client->send(Message::process($this->rows->current()));
                    $this->rows->next();
                } else {
                    $server->stop();
                }

                break;
        }
    }
}
