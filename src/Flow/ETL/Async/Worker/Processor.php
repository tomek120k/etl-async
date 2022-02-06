<?php

declare(strict_types=1);

namespace Flow\ETL\Async\Worker;

use Aeon\Calendar\Stopwatch;
use Flow\ETL\ETL;
use Flow\ETL\Extractor;
use Flow\ETL\Loader;
use Flow\ETL\Pipeline\Pipes;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;
use Psr\Log\LoggerInterface;
use Psr\Log\LogLevel;

final class Processor
{
    private string $workerId;
    private LoggerInterface $logger;
    private Pipes $pipes;

    public function __construct(string $workerId, LoggerInterface $logger)
    {
        $this->logger = $logger;
        $this->pipes = Pipes::empty();
        $this->workerId = $workerId;
    }

    public function setPipes(Pipes $pipes) : void
    {
        $this->pipes = $pipes;
    }

    public function process(Rows $rows) : Rows
    {
        $stopwatch = new Stopwatch();
        $stopwatch->start();
        $this->logger->log(LogLevel::DEBUG, '[worker] processing', [
            'rows' => $rows->count(),
            'id' => $this->workerId
        ]);

        $etl = ETL::extract(
            new class($rows) implements Extractor {
                private Rows $rows;

                public function __construct(Rows $rows)
                {
                    $this->rows = $rows;
                }

                public function extract() : \Generator
                {
                    yield $this->rows;
                }
            }
        );

        foreach ($this->pipes->all() as $pipe) {
            if ($pipe instanceof Transformer) {
                $etl = $etl->transform($pipe);
            } elseif ($pipe instanceof Loader) {
                $etl = $etl->load($pipe);
            }
        }

        $etl->load(
            $loader = new class implements Loader {
                public Rows $rows;

                public function __construct()
                {
                    $this->rows = new Rows();
                }

                public function load(Rows $rows) : void
                {
                    $this->rows = $rows;
                }

                public function __serialize(): array
                {
                    return [];
                }

                public function __unserialize(array $data): void
                {
                }
            })
            ->run();

        $stopwatch->stop();
        $this->logger->debug('[worker] processed', [
            'rows' => $rows->count(),
            'id' => $this->workerId,
            'time_s' => $stopwatch->totalElapsedTime()->inSecondsPrecise()
        ]);

        return $loader->rows;
    }
}