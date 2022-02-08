<?php

declare(strict_types=1);

namespace Flow\ETL\Async\Worker;

use Flow\ETL\Async\Communication\Message;
use Flow\ETL\Async\Communication\Protocol;

final class ClientProtocol
{
    private Processor $processor;

    public function __construct(Processor $processor)
    {
        $this->processor = $processor;
    }

    public function handle(Message $message, Server $server) : void
    {
        switch ($message->type()) {
            case Protocol::SERVER_PIPES:
                /**
                 * @psalm-suppress MixedArgument
                 * @phpstan-ignore-next-line
                 */
                $this->processor->setPipes($message->payload()['pipes']);

                $server->send(Message::fetch());

                break;
            case Protocol::SERVER_PROCESS:
                /**
                 * @psalm-suppress MixedArgument
                 * @phpstan-ignore-next-line
                 */
                $rows = $this->processor->process($message->payload()['rows']);

                $server->send(Message::processed($rows));

                break;
        }
    }

    public function identify(string $id, Server $server) : void
    {
        $server->send(Message::identify($id));
    }
}
