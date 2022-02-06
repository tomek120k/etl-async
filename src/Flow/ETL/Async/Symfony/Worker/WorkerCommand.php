<?php

declare(strict_types=1);

namespace Flow\ETL\Async\Symfony\Worker;

use Aeon\Calendar\Stopwatch;
use Flow\ETL\Async\Communication\Message;
use Flow\ETL\Async\Communication\Protocol;
use Flow\ETL\ETL;
use Flow\ETL\Extractor;
use Flow\ETL\Loader;
use Flow\ETL\Pipeline\Pipes;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;
use Flow\Serializer\Serializer;
use Psr\Log\LoggerInterface;
use Psr\Log\LogLevel;
use React\EventLoop\StreamSelectLoop;
use React\Socket\ConnectionInterface;
use React\Socket\TcpConnector;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;

final class WorkerCommand extends Command
{
    protected static $defaultName = 'run';

    private LoggerInterface $logger;

    private Serializer $serializer;

    private Pipes $pipes;

    public function __construct(LoggerInterface $logger, Serializer $serializer)
    {
        parent::__construct();
        $this->logger = $logger;
        $this->pipes = Pipes::empty();
        $this->serializer = $serializer;
    }

    protected function configure() : void
    {
        $this->setDescription('Outputs "Hello World"')
            ->addOption('id', null, InputOption::VALUE_REQUIRED, 'Worker identifier')
            ->addOption('host', null, InputOption::VALUE_OPTIONAL, 'Coordinator host', '127.0.0.1')
            ->addOption('port', null, InputOption::VALUE_OPTIONAL, 'Coordinator port', 6651)
        ;
    }

    protected function execute(InputInterface $input, OutputInterface $output) : int
    {
        $stopwatch = new Stopwatch();
        $stopwatch->start();

        $id = $input->getOption('id');
        $host = $input->getOption('host');
        $port = (int) $input->getOption('port');

        $this->logger->debug('[worker] starting worker', [
            'id' => $id,
            'host' => $host,
            'port' => $port
        ]);

        $loop = new StreamSelectLoop();

        $connector = new TcpConnector($loop);

        $connector->connect("{$host}:{$port}")->then(
            function (ConnectionInterface $connection) use ($id, $loop) : void {

                    $this->logger->log(LogLevel::DEBUG, '[client] connected to server ', ['address' => $connection->getLocalAddress()]);

                    $connection->on('data', function (string $data) use ($id, $connection) : void {
                        /** @var Message $message */
                        $message = $this->serializer->unserialize($data);

                        $this->logger->debug('[client] received from server', [
                            'address' => $connection->getLocalAddress(),
                            'message' => [
                                'type' => $message->type(),
                                'size' => \strlen($data)
                            ]
                        ]);

                        switch ($message->type()) {
                            case Protocol::SERVER_PIPES:
                                $this->pipes = $message->payload()['pipes'];

                                $connection->write(
                                    $this->serializer->serialize(Message::fetch())
                                );

                                break;
                            case Protocol::SERVER_PROCESS:
                                try {
                                    $rows = $this->process($id, $message->payload()['rows']);

                                    $connection->write(
                                        $this->serializer->serialize(Message::processed($rows))
                                    );
                                } catch (\Throwable $e) {
                                    $this->logger->error('[client] processing error: ' . $e->getMessage(), [
                                        'message' => $message->type(),
                                        'trace' => $e->getTraceAsString()
                                    ]);
                                }
                                break;
                        }
                    });

                    $connection->on('error', function (\Throwable $e) : void {
                        $this->logger->error('[client] something went wrong', ['exception' => $e]);
                    });

                    $connection->on('close', function () use ($loop) : void {
                        $this->logger->debug('[client] server closes connection', []);
                        $loop->stop();
                    });

                    $connection->write($this->serializer->serialize(Message::clientIdentify($id)));
                },
            function (\Throwable $error) : void {
                $this->logger->error('[client] connection closed due to internal error', [
                    'exception' => $error
                ]);
            }
        );

        $loop->run();

        $stopwatch->stop();

        $this->logger->log(LogLevel::DEBUG, '[worker] work is done', ['time_s' => $stopwatch->totalElapsedTime()->inSecondsPrecise()]);

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
                }
            )
            ->run();

        $stopwatch->stop();
        $this->logger->log(LogLevel::DEBUG, '[worker] processed', ['rows' => $rows->count(), 'id' => $id, 'time_s' => $stopwatch->totalElapsedTime()->inSecondsPrecise()]);

        return $loader->rows;
    }
}
