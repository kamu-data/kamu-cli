<div align="center">

<img alt="kamu" src="docs/readme_files/kamu_logo.png" width=270/>

<p><strong><i>World's first decentralized real-time data warehouse, on your laptop</i></strong></p>

[Documentation] | [Tutorials] | [Examples] | [FAQ] | [Chat] | [Website]

[![build](https://github.com/kamu-data/kamu-cli/workflows/build/badge.svg)](https://github.com/kamu-data/kamu-cli/actions)
[![Release](https://github.com/kamu-data/kamu-cli/workflows/release/badge.svg)](https://github.com/kamu-data/kamu-cli/actions)

</p>
</div>


## About

`kamu` is an easy to use command-line tool for managing, transforming, and collaborating on structured data. 

In short it can be described as:
* Git for data (think collaboration, not diffs)
* Decentralized data warehouse
* A peer-to-peer stream processing data pipeline
* A supply chain for structured data
* Blockchain for data
* Or even Kubernetes for data :)

> Watch [this introductory video](https://www.youtube.com/watch?v=oUTiWW6W78A&list=PLV91cS45lwVG20Hicztbv7hsjN6x69MJk) to see it in action.

Following the **"data as code"** philosophy it lets you build arbitrarily complex data pipelines. First, you define where to get data from and how to ingest it, and the tool will **keep datasets always up-to-date**. You can then **create derivative datasets** by joining or transforming other data - you write an **SQL query** and the tool will take care of updating derivative data as input datasets get new data. 

All datasets can be easily shared with others, creating a decentralized peer-to-peer data pipeline, where all data by design is **reproducible**, **verifiable** and maintains a complete **provenance** trail. In the spirit of Open-Source Software it lets you collaborate with people you don't necessarily know, but still be certain that data is trustworthy.

`kamu` is a reference implementation of [Open Data Fabric](https://github.com/kamu-data/open-data-fabric) - a **Web 3.0 protocol** for providing timely, high-quality, and verifiable data for data science, smart contracts, web and applications.

<div align="center">
<img src="./docs/readme_files/dataset_graph.png" alt="Open Data Fabric">
</div>


## Features

**For Data Publishers:**
- Easily **share your data** with the world **without moving it** anywhere
- Retain full **ownership and control** of your data
- Close the feedback loop and **see who and how uses your data**
- Provide **real-time**, **verifiable and reproducible** data that follows the best data management practices
  ![Pull Data](docs/readme_files/pull-multi.gif)

**For Data Professionals:**
- **Ingest any existing dataset** from the web
- Always **stay up-to-date** by pulling latest updates from the data sources with just one command
- Use **stable data references** to make your data projects fully reproducible
- **Collaborate** on cleaning and improving data of existing datasets
- Create derivative datasets by transforming, enriching, and summarizing data others have published
- **Write query once and run it forever** - our pipelines require nearly zero maintenance
- Built-in support for **GIS data**
- **Share** your results with others in a fully reproducible and reusable form

**For Data Consumers:**
- **Download** a dataset from a shared repository
- **Verify** that all data comes from trusted sources using 100% accurate **lineage**
- **Audit** the chain of transformations this data went through
- **Validate** that downloaded was not tampered with a single command
- **Trust** your data by knowing where every single bit of information came from with our **fine grain provenance**


**For Data Exploration:**
- Explore data and run **ad-hoc SQL queries** (backed by the power of Apache Spark)
- Launch a **Jupyter notebook** with one command
- Join, filter, and shape your data using SQL
- Visualize the result using your favorite library
  ![SQL Shell](docs/first_steps_files/sql.gif)
  ![Jupyter](docs/first_steps_files/notebook-005.png)


---

<div align="center">
  
[Documentation] | [Tutorials] | [Examples] | [FAQ] | [Chat] | [Website] | [Contributing] | [License]
</div>

[Tutorials]: docs/learning_materials.md
[Examples]: docs/examples/index.md
[Documentation]: docs/index.md
[FAQ]: docs/faq.md
[Chat]: https://discord.gg/aSpVjWwu
[Contributing]: docs/contributing.md
[License]: LICENSE.txt
[Website]: https://www.kamu.dev