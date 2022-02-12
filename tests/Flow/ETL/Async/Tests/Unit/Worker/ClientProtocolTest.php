<?php

declare(strict_types=1);

namespace Flow\ETL\Async\Tests\Unit\Worker;

use Flow\ETL\Async\Communication\Message;
use Flow\ETL\Async\Client\ClientProtocol;
use Flow\ETL\Async\Client\Processor;
use Flow\ETL\Async\Client\Server;
use Flow\ETL\Pipeline\Pipes;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

final class ClientProtocolTest extends TestCase
{
    public function test_fetch_after_getting_pipes() : void
    {
        $protocol = new ClientProtocol(new Processor('worker', new NullLogger()));

        $server = $this->createMock(Server::class);

        $server->expects($this->once())
            ->method('send')
            ->with(Message::fetch());

        $protocol->handle(
            Message::pipes(Pipes::empty()),
            $server
        );
    }

    public function test_send_back_processed_rows_to_server() : void
    {
        $protocol = new ClientProtocol(new Processor('worker', new NullLogger()));

        $server = $this->createMock(Server::class);

        $server->expects($this->once())
            ->method('send')
            ->with(Message::processed($rows = new Rows(Row::create(new Row\Entry\IntegerEntry('id', 1)))));

        $protocol->handle(
            Message::process($rows),
            $server
        );
    }
}
