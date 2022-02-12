<?php

declare(strict_types=1);

namespace Flow\ETL\Async\Client;

use Aeon\Calendar\Stopwatch;
use Flow\ETL\ETL;
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
            'id' => $this->workerId,
        ]);

        $etl = ETL::process($rows);

        foreach ($this->pipes->all() as $pipe) {
            if ($pipe instanceof Transformer) {
                $etl = $etl->transform($pipe);
            } elseif ($pipe instanceof Loader) {
                $etl = $etl->load($pipe);
            }
        }

        $transformerRows = $etl->fetch();

        $stopwatch->stop();
        $this->logger->debug('[worker] processed', [
            'id' => $this->workerId,
            'rows' => $rows->count(),
            'transformer_rows' => $transformerRows->count(),
            'time_sec' => $stopwatch->totalElapsedTime()->inSecondsPrecise(),
        ]);

        return $transformerRows;
    }
}
