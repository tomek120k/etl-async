<?php

declare(strict_types=1);

namespace Flow\ETL\Async;

use Flow\ETL\Async\Server\Server;
use Flow\ETL\Async\Server\ServerProtocol;
use Flow\ETL\Async\Worker\Launcher;
use Flow\ETL\Async\Worker\Pool;
use Flow\ETL\ErrorHandler;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Pipeline;
use Flow\ETL\Pipeline\Pipe;
use Flow\ETL\Pipeline\Pipes;

final class LocalPipeline implements Pipeline
{
    private Server $server;

    private Launcher $launcher;

    private int $totalWorkers;

    /**
     * @phpstan-ignore-next-line
     */
    private ErrorHandler $errorHandler;

    private Pipes $pipes;

    public function __construct(Server $server, Launcher $launcher, int $workers)
    {
        if ($workers < 1) {
            throw new InvalidArgumentException("Number of workers can't be lower than 1, given: {$workers}");
        }

        $this->server = $server;
        $this->launcher = $launcher;
        $this->totalWorkers = $workers;
        $this->pipes = Pipes::empty();
        $this->errorHandler = new ErrorHandler\IgnoreError();
    }

    public function clean() : Pipeline
    {
        return new Pipeline\SynchronousPipeline();
    }

    public function add(Pipe $pipe) : void
    {
        $this->pipes->add($pipe);
    }

    public function process(\Generator $generator, callable $callback = null) : void
    {
        $pool = Pool::generate($this->totalWorkers);

        $this->server->initialize(new ServerProtocol($pool, $generator, $this->pipes, $callback));

        $this->launcher->launch($pool);

        $this->server->start();
    }

    public function onError(ErrorHandler $errorHandler) : void
    {
        $this->errorHandler = $errorHandler;
    }
}
