<?php

declare(strict_types=1);

namespace Flow\ETL\Async\Worker;

use Flow\ETL\Async\Worker\CLI\Input;
use Psr\Log\LoggerInterface;

final class CLI
{
    private LoggerInterface $logger;

    private Client $client;

    public function __construct(LoggerInterface $logger, Client $client)
    {
        $this->logger = $logger;
        $this->client = $client;
    }

    public function run(Input $input) : int
    {
        $port = (int) $input->optionValue('port', Server::DEFAULT_PORT);
        $host = $input->optionValue('host', '127.0.0.1');
        $id = $input->optionValue('id');

        if ($id === null) {
            $this->logger->error("[worker] missing --id option", [
                'argv' => $input->argv()
            ]);

            return 1;
        }

        try {
            $this->client->connect($id, $host, $port, new ClientProtocol(new Processor($id, $this->logger)));
        } catch (\Throwable $e) {
            $this->logger->error("[worker] something went wrong", [
                'exception' => $e
            ]);

            return 1;
        }

        return 0;
    }
}