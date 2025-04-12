# artemis-light

A stripped down, modernised fork of the Artemis library.

At its core, Artemis is architected as an event processing pipeline. The library is made up of three main components:

1. [Collectors](types::Collector): *Collectors* take in external events (such as pending txs, new blocks, marketplace orders, etc. ) and turn them into an internal *event* representation.

2. [Strategies](types::Strategy): *Strategies* contain the core logic.  required for each opportunity. They take in *events* as inputs, and compute whether any opportunities are available (for example, a strategy might listen to a stream of borrow and supply logs see if there are any delinquent borrowers that can be liquidated). *Strategies* produce *actions*.

3. [Executors](types::Executor): *Executors* process *actions*, and are responsible for executing them in different domains (for example, submitting txs, posting off-chain orders, etc.).

These components are tied together by the [Engine](engine::Engine), which is responsible for orchestrating the flow of data between them.
