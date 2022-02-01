# ETL - Asynchronous Pipeline [EXPERIMENTAL]   

This repository provides experimental implementation of [asynchronous processing](https://github.com/flow-php/etl/discussions/129) for Flow, PHP ETL.

Current implementation is based on following external libraries: 

* ReactPHP
    - Event Loop 
    - Socket Server/Client 
    - Child Process
* Symfony 
    - Console
    
The end goal is to provide interfaces for the following components

- Socket Server 
- Socket Client 
- Child Process 

That would become a technical implementation of the following elements: 

- Worker
- Coordinator
- Planner 
- Communication Protocol
- Serializer

Symfony Console at this stage is used just to parse CLI Application input options (3-4 options max) 
It's so simple task that eventually Symfony dependency should be dropped in favour internal CLI Worker Application.   

More advanced configuration of Worker should be possible through environment variables or custom implementations taking
advantage of predefined entry points. 

After stabilizing abstraction layer for all components, different async adapters 

- [ReactPHP](https://reactphp.org/)
- [AMP](https://amphp.org/) 

Should be extracted to standalone repositories 

- etl-async-react
- etl-async-amp

Supporting as many as possible versions of those libraries in order to reduce issues with dependencies in 
final projects. 
React and AMP are chosen due to their popularity, in order to not introduce another dependency into a project
that is already using one or the other library. 


Working example: 

```php
<?php

use Flow\ETL\ETL;
use Flow\ETL\Extractor;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry\ArrayEntry;
use Flow\ETL\Rows;
use Flow\ETL\Transformer\ArrayUnpackTransformer;
use Flow\ETL\Transformer\Cast\CastToInteger;
use Flow\ETL\Transformer\CastTransformer;
use Flow\ETL\Transformer\RemoveEntriesTransformer;
use Flow\ETL\Transformer\Rename\EntryRename;
use Flow\ETL\Transformer\RenameEntriesTransformer;
use Flow\ETL\Transformer\StringConcatTransformer;
use Monolog\Handler\StreamHandler;
use Monolog\Logger;

return new class implements Extractor {
    public function extract(): \Generator
    {
        $rows = [];
        for ($i = 0; $i <= 10_000_000; $i++) {
            $rows[] = Row::create(
                new ArrayEntry(
                    'row', ['id' => $i, 'name' => 'Name', 'last name' => 'Last Name', 'phone' => '123 123 123']
                ),
            );

            if (\count($rows) >= 1000) {
                echo "extracted " . $i . "\n ";
                yield new Rows(...$rows);

                $rows = [];
            }
        }

        if (\count($rows) >= 0) {
            yield new Rows(...$rows);
        }
    }
};


$logger = new Logger('server');
$logger->pushHandler(new StreamHandler(__DIR__ . '/var/logs/server.log', Logger::DEBUG));
$logger->pushHandler(new StreamHandler(__DIR__ . '/var/logs/server_error.log', Logger::ERROR, false));

ETL::extract($extractor, new AsyncLocalPipeline(__DIR__ . "/bin/worker", $port = 6651, $workers = 10, $logger))
    ->transform(new ArrayUnpackTransformer('row'))
    ->transform(new RemoveEntriesTransformer('row'))
    ->transform(new CastTransformer(new CastToInteger(['id'])))
    ->transform(new StringConcatTransformer(['name', 'last name'], ' ', '_name'))
    ->transform(new RemoveEntriesTransformer('name', 'last name'))
    ->transform(new RenameEntriesTransformer(new EntryRename('_name', 'name')))
    ->run();
```