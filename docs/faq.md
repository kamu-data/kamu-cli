# Frequently Asked Questions <!-- omit in toc -->

- [How does `kamu` compare to Spark / Flink (or other enterprise data processing tech)?](#how-does-kamu-compare-to-spark--flink-or-other-enterprise-data-processing-tech)
- [Can I create derivative datasets with Pandas (or other data procesing library)?](#can-i-create-derivative-datasets-with-pandas-or-other-data-procesing-library)

## How does `kamu` compare to Spark / Flink (or other enterprise data processing tech)?

Spark and Flink to `kamu` is what `diff` is for `git` - an implementation detail of a particular flavor of stream processing. `kamu` builds on top of those engines to provide higher level properties like preserving history and reproducibility of data, making data verifiable, and enabeling collaboration.


## Can I create derivative datasets with Pandas (or other data procesing library)?

`kamu`'s goal is to be as inclusive of different ways to process data as possible, but also uphold certain key properties of data like low latency and reproducibility. 

Pandas and most other libraries do **batch processing**, so they are not suited for processing data that flows continuously. Using them would be inefficient, non-reproducible, and error prone, as batch paradigm does not handle late and out-of-order data, and other types of temporal problems we see in data constantly (see our [learning materials](learning_materials.md) on the topic of streaming vs. batch). 

Also Pandas and similar libraries are imperative and their code is hard to analyze - this complicates our goal of achieving fine-grain provenance. So we're currently biased towards processing engines that use Streaming SQL, but will be glad to discuss extending support to other frameworks. 