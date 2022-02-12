<?php

declare(strict_types=1);

namespace Flow\ETL\Async\Tests\Unit\Server;

use Flow\ETL\Async\Communication\Message;
use Flow\ETL\Async\Server\Client;
use Flow\ETL\Async\Server\Server;
use Flow\ETL\Async\Server\ServerProtocol;
use Flow\ETL\Async\Client\Pool;
use Flow\ETL\Extractor\ProcessExtractor;
use Flow\ETL\Pipeline\Pipes;
use Flow\ETL\Rows;
use PHPUnit\Framework\TestCase;

final class ServerProtocolTest extends TestCase
{
    public function test_sending_pipes_after_successful_identification() : void
    {
        $serverProtocol = new ServerProtocol(
            $pool = Pool::generate(1),
            (new ProcessExtractor(new Rows()))->extract(),
            $pipes = Pipes::empty()
        );

        $client = $this->createMock(Client::class);
        $server = $this->createMock(Server::class);

        $client->expects($this->once())
            ->method('send')
            ->with(Message::pipes($pipes));

        $serverProtocol->handle(
            Message::identify(\current($pool->ids())->toString()),
            $client,
            $server
        );
    }

    public function test_disconnecting_client_after_unsuccessful_identification() : void
    {
        $serverProtocol = new ServerProtocol(
            $pool = Pool::generate(1),
            (new ProcessExtractor(new Rows()))->extract(),
            $pipes = Pipes::empty()
        );

        $client = $this->createMock(Client::class);
        $server = $this->createMock(Server::class);

        $client->expects($this->once())
            ->method('disconnect');

        $serverProtocol->handle(
            Message::identify('invalid-id'),
            $client,
            $server
        );
    }

    public function test_send_rows_after_fetch_when_there_are_still_some_rows_left_otherwise_stop_server() : void
    {
        $serverProtocol = new ServerProtocol(
            $pool = Pool::generate(1),
            (new ProcessExtractor($rows = new Rows()))->extract(),
            $pipes = Pipes::empty()
        );

        $client = $this->createMock(Client::class);
        $server = $this->createMock(Server::class);

        $client->expects($this->once())
            ->method('send')
            ->with(Message::process($rows));

        $server->expects($this->once())
            ->method('stop');

        $serverProtocol->handle(
            Message::fetch(),
            $client,
            $server
        );
        $serverProtocol->handle(
            Message::fetch(),
            $client,
            $server
        );
    }

    public function test_pass_rows_to_callback_and_send_new_rows_after_processed_when_there_are_still_some_rows_left_otherwise_stop_server() : void
    {
        $callbackExecuted = false;
        $serverProtocol = new ServerProtocol(
            $pool = Pool::generate(1),
            (new ProcessExtractor($rows = new Rows()))->extract(),
            $pipes = Pipes::empty(),
            function (Rows $rows) use (&$callbackExecuted) : void {
                $callbackExecuted = $rows instanceof Rows;
            }
        );

        $client = $this->createMock(Client::class);
        $server = $this->createMock(Server::class);

        $client->expects($this->once())
            ->method('send')
            ->with(Message::process($rows));

        $server->expects($this->once())
            ->method('stop');

        $serverProtocol->handle(
            Message::processed($processedRows = new Rows()),
            $client,
            $server
        );
        $serverProtocol->handle(
            Message::fetch(),
            $client,
            $server
        );

        $this->assertTrue($callbackExecuted);
    }
}
