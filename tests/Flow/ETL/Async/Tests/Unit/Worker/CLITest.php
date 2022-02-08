<?php

declare(strict_types=1);

namespace Flow\ETL\Async\Tests\Unit\Worker;

use Flow\ETL\Async\Worker\CLI;
use Flow\ETL\Async\Worker\Client;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

final class CLITest extends TestCase
{
    public function test_running_cli_without_worker_id_option() : void
    {
        $cli = new CLI(new NullLogger(), $this->createMock(Client::class));

        $this->assertSame(
            1,
            $cli->run(new CLI\Input(['bin/worker', '--host=127.0.0.1', '--port=6651']))
        );
    }

    public function test_running_cli_with_all_required_options() : void
    {
        $cli = new CLI(new NullLogger(), $this->createMock(Client::class));

        $this->assertSame(
            0,
            $cli->run(new CLI\Input(['bin/worker', '--host=127.0.0.1', '--port=6651', '--id="worker_id"']))
        );
    }
}
