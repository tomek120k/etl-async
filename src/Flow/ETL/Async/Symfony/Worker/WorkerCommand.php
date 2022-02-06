<?php

declare(strict_types=1);

namespace Flow\ETL\Async\Symfony\Worker;

use Flow\ETL\Async\Worker\Client;
use Flow\ETL\Async\Worker\ClientProtocol;
use Flow\ETL\Async\Worker\Processor;
use Psr\Log\LoggerInterface;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;

final class WorkerCommand extends Command
{
    protected static $defaultName = 'run';

    private LoggerInterface $logger;

    private Client $client;

    public function __construct(LoggerInterface $logger, Client $client)
    {
        parent::__construct();
        $this->logger = $logger;
        $this->client = $client;
    }

    protected function configure() : void
    {
        $this->setDescription('Outputs "Hello World"')
            ->addOption('id', null, InputOption::VALUE_REQUIRED, 'Worker identifier')
            ->addOption('host', null, InputOption::VALUE_REQUIRED, 'Server host')
            ->addOption('port', null, InputOption::VALUE_REQUIRED, 'Server port')
        ;
    }

    protected function execute(InputInterface $input, OutputInterface $output) : int
    {
        $id = $input->getOption('id');
        $host = $input->getOption('host');
        $port = (int) $input->getOption('port');

        $this->client->connect($id, $host, $port, new ClientProtocol(new Processor($id, $this->logger)));

        return 0;
    }
}
