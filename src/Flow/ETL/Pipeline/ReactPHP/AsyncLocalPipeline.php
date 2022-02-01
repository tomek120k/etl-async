<?php

declare(strict_types=1);

namespace Flow\ETL\Pipeline\ReactPHP;

use Aeon\Calendar\Stopwatch;
use Clue\React\NDJson\Decoder;
use Clue\React\NDJson\Encoder;
use Flow\ETL\ErrorHandler;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Loader;
use Flow\ETL\Pipeline;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;
use Psr\Log\LoggerInterface;
use Psr\Log\LogLevel;
use Psr\Log\NullLogger;
use Ramsey\Uuid\Uuid;
use React\ChildProcess\Process;
use React\EventLoop\LoopInterface;
use React\EventLoop\StreamSelectLoop;
use React\Socket\ConnectionInterface;
use React\Socket\ServerInterface;
use React\Socket\TcpServer;

/**
 * @internal
 */
final class AsyncLocalPipeline implements Pipeline
{
    /**
     * @var array<Loader|Transformer>
     */
    private array $elements;

    private ErrorHandler $errorHandler;

    private int $port;

    /**
     * @var array<string, Process>
     */
    private array $pool;

    /**
     * @var array<string, ConnectionInterface>
     */
    private array $connections;

    private int $maxWorkers;

    private string $workerPath;

    /**
     * @var array<Rows>
     */
    private array $queue;

    private LoggerInterface $logger;

    private int $maximumMessageSize;

    public function __construct(string $workerPath, int $port, int $maxWorkers, LoggerInterface $logger = null, $maximumMessageSize = 134_217_728)
    {
        if (!\file_exists($workerPath)) {
            throw new InvalidArgumentException("Worker not found under given path:: {$workerPath}");
        }

        if ($maxWorkers < 1) {
            throw new InvalidArgumentException("Number of workers can't be lower than 1, given: {$maxWorkers}");
        }

        $this->elements = [];
        $this->pool = [];
        $this->connections = [];
        $this->queue = [];
        $this->errorHandler = new ErrorHandler\ThrowError();
        $this->port = $port;
        $this->workerPath = $workerPath;
        $this->logger = $logger ?? new NullLogger();
        $this->maxWorkers = $maxWorkers;
        $this->maximumMessageSize = $maximumMessageSize;
    }

    public function clean() : Pipeline
    {
        return new Pipeline\SynchronousPipeline();
    }

    public function registerTransformer(Transformer $transformer) : void
    {
        $this->elements[] = $transformer;
    }

    public function registerLoader(Loader $loader) : void
    {
        $this->elements[] = $loader;
    }

    public function process(\Generator $generator, callable $callback = null) : void
    {
        $stopwatch = new Stopwatch();
        $stopwatch->start();
        $loop = new StreamSelectLoop();

        $server = new TcpServer("127.0.0.1:{$this->port}", $loop);

        $server->on('connection', function (ConnectionInterface $connection) use ($callback, $generator, $loop, $server) : void {
            $address = $connection->getRemoteAddress();
            $out = new Encoder($connection);
            $in = new Decoder($connection, true, 512, 0, $this->maximumMessageSize);

            $this->logger->log(LogLevel::DEBUG, '[server] client connected', ['address' => $connection->getRemoteAddress()]);

            $in->on('data', function (array $message) use ($connection, $callback, $generator, $out, $loop, $server) : void {
                $this->logger->log(LogLevel::DEBUG, '[server] message received', ['address' => $connection->getRemoteAddress(), 'data' => $message['type']]);

                switch ($message['type']) {
                    case 'identification':
                        $this->connections[$connection->getRemoteAddress()] = $message['payload']['id'];

                        $out->write(
                            [
                                'type' => 'pipeline',
                                'payload' => [
                                    'pipeline' => \serialize($this->elements),
                                ],
                            ]
                        );

                        break;
                    case 'processed':
                        if ($callback !== null) {
                            $callback(\unserialize($message['payload']['rows']));
                        }

                        $this->sendToProcessing($generator, $loop, $server, $out);

                        break;
                    case 'fetch':
                        $this->sendToProcessing($generator, $loop, $server, $out);

                        break;
                }
            });
        });

        for ($i = 0; $i < $this->maxWorkers; $i++) {
            $this->startNewWorker($loop);
        }

        $loop->addPeriodicTimer(30, function () use ($generator, $loop, $server) : void {
            if ($generator->valid() === false) {
                $loop->stop();
                $server->close();
            }
        });

        $loop->run();

        $stopwatch->stop();

        $this->logger->log(LogLevel::DEBUG, 'Server work is done', ['time_s' => $stopwatch->totalElapsedTime()->inSecondsPrecise()]);
    }

    public function onError(ErrorHandler $errorHandler) : void
    {
        $this->errorHandler = $errorHandler;
    }

    private function startNewWorker(LoopInterface $loop) : void
    {
        if (\count($this->pool) < $this->maxWorkers) {
            $id = Uuid::uuid4()->toString();

            $path = \realpath($this->workerPath) . " --id=\"{$id}\" --host=\"127.0.0.1\" --port=\"{$this->port}\" --message-size=\"{$this->maximumMessageSize}\"";

            $this->logger->log(
                LogLevel::DEBUG,
                '[coordinator] starting worker',
                ['path' => $path]
            );

            $process = new Process($path);
            $process->start($loop);

            $this->pool[$id] = $process;
        }
    }

    /**
     * @param \Generator $generator
     * @param int $index
     */
    private function take(\Generator $generator, LoopInterface $loop, ServerInterface $server) : ?Rows
    {
        if ($generator->valid()) {
            /** @var Rows $rows */
            $rows = $generator->current();

            $generator->next();

            return $rows;
        }
        $server->close();
        $loop->stop();

        return null;
    }

    /**
     * @param \Generator $generator
     * @param StreamSelectLoop $loop
     * @param TcpServer $server
     * @param Encoder $out
     */
    private function sendToProcessing(\Generator $generator, StreamSelectLoop $loop, TcpServer $server, Encoder $out) : void
    {
        $rows = $this->take($generator, $loop, $server);

        if ($rows) {
            $out->write(
                [
                    'type' => 'rows',
                    'payload' => [
                        'rows' => \serialize($rows),
                    ],
                ]
            );
        }
    }
}
