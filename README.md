# ETL - Asynchronous Pipeline

This repository provides an abstraction for [asynchronous processing](https://github.com/flow-php/etl/discussions/129) for Flow, PHP ETL.

At this point Asynchronouse Pipelines does not require any custom third party PHP Extensions. 

## Adapters

- [flow-php/etl-async-adapter-reactphp](https://github.com/flow-php/etl-async-adapter-reactphp)

## Processing Modes

The goal is to make following processing modes available.

* Single Process - [[flow-php/etl](https://github.com/flow-php/etl)]
* Multi Process - [[flow-php/etl](https://github.com/flow-php/etl), [flow-php/etl-async](https://github.com/flow-php/etl-async)]
* Local Cluster - [[flow-php/etl](https://github.com/flow-php/etl), [flow-php/etl-async](https://github.com/flow-php/etl-async), ...]
* Remote Cluster - [[flow-php/etl](https://github.com/flow-php/etl), [flow-php/etl-async](https://github.com/flow-php/etl-async), ...]

![Processing Modes](docs/processing_modes.png)
