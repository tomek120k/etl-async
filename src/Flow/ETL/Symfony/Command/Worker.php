<?php

declare(strict_types=1);

namespace Flow\ETL\Symfony\Command;

use Aeon\Calendar\Stopwatch;
use Clue\React\NDJson\Decoder;
use Clue\React\NDJson\Encoder;
use Flow\ETL\ETL;
use Flow\ETL\Extractor;
use Flow\ETL\Loader;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;
use Psr\Log\LoggerInterface;
use Psr\Log\LogLevel;
use React\EventLoop\StreamSelectLoop;
use React\Socket\ConnectionInterface;
use React\Socket\TcpConnector;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;

final class Worker extends Command
{
    protected static $defaultName = 'run';

    private LoggerInterface $logger;

    /**
     * @var array<Loader|Transformer>
     */
    private array $pipeline;

    public function __construct(LoggerInterface $logger)
    {
        parent::__construct();
        $this->logger = $logger;
        $this->pipeline = [];
    }

    protected function configure() : void
    {
        $this->setDescription('Outputs "Hello World"')
            ->addOption('id', null, InputOption::VALUE_REQUIRED, 'Worker identifier')
            ->addOption('host', null, InputOption::VALUE_OPTIONAL, 'Coordinator host', '127.0.0.1')
            ->addOption('port', null, InputOption::VALUE_OPTIONAL, 'Coordinator port', 6651)
            ->addOption('message-size', null, InputOption::VALUE_OPTIONAL, 'Maximum message size', 134_217_728) // 128mb
;
    }

    protected function execute(InputInterface $input, OutputInterface $output) : int
    {
        $stopwatch = new Stopwatch();
        $stopwatch->start();

        $id = $input->getOption('id');
        $host = $input->getOption('host');
        $port = (int) $input->getOption('port');
        $messageSize = (int) $input->getOption('message-size');

        $loop = new StreamSelectLoop();

        $connector = new TcpConnector($loop);

        $connector->connect("{$host}:{$port}")->then(
            function (ConnectionInterface $connection) use ($id, $messageSize) : void {
                    $in = new Decoder($connection, true, 512, 0, $messageSize);
                    $out = new Encoder($connection);

                    $this->logger->log(LogLevel::DEBUG, '[client] connected to server ', ['address' => $connection->getLocalAddress()]);

                    $in->on('data', function (array $message) use ($connection, $out, $messageSize, $id) : void {
                        $this->logger->log(LogLevel::DEBUG, '[client] received from server ', [
                            'address' => $connection->getLocalAddress(),
                            'message' => $message['type'],
                        ]);

                        switch ($message['type']) {
                            case 'pipeline':
                                try {
                                    $this->pipeline = \unserialize($message['payload']['pipeline']);

                                    $out->write([
                                        'type' => 'fetch',
                                        'payload' => [],
                                    ]);
                                } catch (\Error $e) {
                                    $this->logger->error($e->getMessage(), ['on', $message['type'], 'trace' => $e->getTraceAsString()]);
                                }

                                break;

                            case 'rows':
                                try {
                                    $rows = $this->process($id, \unserialize($message['payload']['rows']));

                                    $encodedRows = \serialize($rows);
                                    $rowsSize = \strlen($encodedRows);

                                    if ($rowsSize > $messageSize - 100) {
                                        $this->logger->error("Rows exceeded maximum message size: {$messageSize}, given: {$rowsSize}");

                                        foreach ($rows->chunks(2) as $rowsChunk) {
                                            $out->write([
                                                'type' => 'processed',
                                                'payload' => [
                                                    'rows' => \serialize($rowsChunk),
                                                ],
                                            ]);
                                        }
                                    } else {
                                        $out->write([
                                            'type' => 'processed',
                                            'payload' => [
                                                'rows' => $encodedRows,
                                            ],
                                        ]);
                                    }
                                } catch (\Error $e) {
                                    $this->logger->error($e->getMessage(), ['on', $message['type'], 'trace' => $e->getTraceAsString()]);
                                }

                                break;
                        }
                    });

                    $out->on('error', function (\Throwable $e) : void {
                        $this->logger->error('[client] something went wrong', ['exception' => $e]);
                    });

                    $out->write(['type' => 'identification', 'payload' => ['id' => $id]]);
                },
            function (\Exception $error) : void {
                    $this->logger->log(LogLevel::ERROR, '[client] connection closed');
                }
        );

        $loop->run();

        $stopwatch->stop();

        $this->logger->log(LogLevel::DEBUG, 'Worker work is done', ['time_s' => $stopwatch->totalElapsedTime()->inSecondsPrecise()]);

        return 0;
    }

    private function process($id, Rows $rows) : Rows
    {
        $stopwatch = new Stopwatch();
        $stopwatch->start();
        $this->logger->log(LogLevel::DEBUG, '[worker] processing', ['rows' => $rows->count(), 'id' => $id]);

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

        foreach ($this->pipeline as $element) {
            if ($element instanceof Transformer) {
                $etl = $etl->transform($element);
            } else {
                $etl = $etl->load($element);
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
                }
        )
            ->run();

        $stopwatch->stop();
        $this->logger->log(LogLevel::DEBUG, '[worker] processed', ['rows' => $rows->count(), 'id' => $id, 'time_s' => $stopwatch->totalElapsedTime()->inSecondsPrecise()]);

        return $loader->rows;
    }
}
