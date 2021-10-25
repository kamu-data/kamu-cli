<div align="center">

<img alt="kamu" src="docs/readme_files/kamu_logo.png" width=270/>

<p><strong><i>World's first decentralized real-time data warehouse, on your laptop</i></strong></p>

[Docs] | [Tutorials] | [Examples] | [FAQ] | [Chat] | [Website]

[![build](https://github.com/kamu-data/kamu-cli/workflows/build/badge.svg)](https://github.com/kamu-data/kamu-cli/actions)
[![Release](https://github.com/kamu-data/kamu-cli/workflows/release/badge.svg)](https://github.com/kamu-data/kamu-cli/actions)
[![Chat](https://shields.io/discord/898726370199359498)](https://discord.gg/nU6TXRQNXC)

</p>
</div>

## Get Started

* Watch [this introductory video](https://www.youtube.com/watch?v=oUTiWW6W78A&list=PLV91cS45lwVG20Hicztbv7hsjN6x69MJk) to see `kamu` in action.
* Learn how to use `kamu` with this [self-guided demo](/images/demo/README.md) without needing to install anything.
* Then follow the "Getting Started" section of our [documentation] to install the tool and try a bunch of examples.

## About

`kamu` is an easy to use command-line tool for managing, transforming, and collaborating on structured data. 

In short it can be described as:
* Git for data (think collaboration, not diffs)
* Decentralized data warehouse
* A peer-to-peer stream processing data pipeline
* A supply chain for structured data
* Blockchain for data
* Or even Kubernetes for data :)

Following the **"data as code"** philosophy it lets you build arbitrarily complex data pipelines. First, you define where to get data from and how to ingest it, and the tool will **keep datasets always up-to-date**. You can then **create derivative datasets** by joining or transforming other data - you write an **SQL query** and the tool will take care of updating derivative data as input datasets get new data. 

All datasets can be easily shared with others, creating a decentralized peer-to-peer data pipeline, where all data by design is **reproducible**, **verifiable** and maintains a complete **provenance** trail. In the spirit of Open-Source Software it lets you collaborate with people you don't necessarily know, but still be certain that data is trustworthy.

`kamu` is a reference implementation of [Open Data Fabric](https://github.com/kamu-data/open-data-fabric) - a **Web 3.0 protocol** for providing timely, high-quality, and verifiable data for data science, smart contracts, web and applications.

<div align="center">
<img src="./docs/readme_files/dataset_graph.png" alt="Open Data Fabric">
</div>


## Use Cases

In general, `kamu` is a great fit for cases where data is exchanged between several independent parties, and for (low to moderate frequency & volume) mission-critical data where high degree of trustworthiness and protection from malicious actors is required.

Examples:

<details>
<summary><b>Open Data</b></summary>
To share data outside of your organization today you have limited options:

- You can publish it on some open data portal and lose ownership and control of your data
- You can deploy and operate some open-source data portal, but you probably have neither time or money to do so
- You can self-host it as a CSV file on some simple HTTP/FTP server, but then you are making it extremely hard for others to discover your data

For most data publishers, publishing data is not a core part of their business, so `kamu` aims to make publishing as easy as possible. It invisibly guides publishers towards best data management practices (preserving history, making data reproducible and verifiable) and lets consumers access data instantly, in a ready to use form.
</details>

<details>
<summary><b>Science & Research</b></summary>
One of the driving forces behind `kamu`'s design was the [ongoing reproducibility crisis](https://www.nature.com/articles/533452a) in science. We believe that to a large extent it's caused by our poor data management practices.

After incidents like [The Surgisphere scandal](https://www.the-scientist.com/features/the-surgisphere-scandal-what-went-wrong--67955) the sentiment in research is changing from assuming that all research is done in good faith, to considering any research unreliable until its proven so.

With `kamu`:

- You can make your data projects fully reproducible using built-in stable references mechanism
- Your data analysis results can be reproduced and verified by others in minutes
- All the data prep work (that often accounts for [80% of time of a data scientist](https://www.forbes.com/sites/gilpress/2016/03/23/data-preparation-most-time-consuming-least-enjoyable-data-science-task-survey-says/?sh=348d5f876f63)) can be shared and reused by others
- Your datasets will continue to function long after you're done with your project, so the work done years ago will continue to produce valuable insights with minimal maintenance on your part
</details>

<details>
<summary><b>Data-driven Journalism</b></summary>
Data-driven journalism is on the rise and has proven to be extremely effective. In the world of misinformation and extremely polarized opinions data provides us an achoring point to discuss complex problems and analyze cause and effect. Data itself is non-partisan and has no secret agenda, and arguments around different interpretations of data are infinitely more productive than ones based on gut feelings.

Unfortunately, too often data has issues that undermine its trustworthiness. And even if the data is correct, it's very easy to pose a question about its sources that will take too long to answer - the data will be dismissed, and the gut feelings will step in.

This is why `kamu`'s goal is to make data **verifiably trustworthy** and make answering **provenance** questions a matter of seconds. Only when data cannot be easily dismissed we will start to pay proper attention to it.
</details>

<details>
<summary><b>Business core data</b></summary>
`kamu` aims to be the most reliable data management solution that provides recent data while maintaining highest degree of accountability and tamper-proof provenance, without you having to put all data in some central database. We're developing it with financial and pharmaceutical use cases in mind, where audit and compliance could be fully automated through our system.

Note that we currently focus on mission-critical data and `kamu` is not well suited for IoT or other high-frequency and high-volume cases.
</details>

<details>
<summary><b>Personal analytics</b></summary>
Being data geeks, we use `kamu` for data-driven decision making even in our personal lives. Actually, our largest data pipelines so far were created for personal finance - to collect and harmonize data from multiple bank accounts, convert currencies, and analyze stocks trading data. We also scrape a lot of websites to make smarter purchasing decisions. `kamu` lets us keep all this data up-to-date with absolute minimal effort.
</details>

## Features

`kamu` connects **publishers** and **consumers** of data through a decentralized network and lets people **collaborate** on extracting insight from data. It offers many perks for everyone who participates in this first-of-a-kind data supply chain:

<details>
<summary><b>For Data Publishers</b></summary>

- Easily **share your data** with the world **without moving it** anywhere
- Retain full **ownership and control** of your data
- Close the feedback loop and **see who and how uses your data**
- Provide **real-time**, **verifiable and reproducible** data that follows the best data management practices
  ![Pull Data](docs/readme_files/pull-multi.gif)

</details>

<details>
<summary><b>For Data Scientists</b></summary>

- **Ingest any existing dataset** from the web
- Always **stay up-to-date** by pulling latest updates from the data sources with just one command
- Use **stable data references** to make your data projects fully reproducible
- **Collaborate** on cleaning and improving data of existing datasets
- Create derivative datasets by transforming, enriching, and summarizing data others have published
- **Write query once and run it forever** - our pipelines require nearly zero maintenance
- Built-in support for **GIS data**
- **Share** your results with others in a fully reproducible and reusable form

</details>

<details>
<summary><b>For Data Consumers</b></summary>

- **Download** a dataset from a shared repository
- **Verify** that all data comes from trusted sources using 100% accurate **lineage**
- **Audit** the chain of transformations this data went through
- **Validate** that downloaded was not tampered with a single command
- **Trust** your data by knowing where every single bit of information came from with our **fine grain provenance**

</details>

<details>
<summary><b>For Data Exploration</b></summary>

- Explore data and run **ad-hoc SQL queries** (backed by the power of Apache Spark)
- Launch a **Jupyter notebook** with one command
- Join, filter, and shape your data using SQL
- Visualize the result using your favorite library
  ![SQL Shell](docs/first_steps_files/sql.gif)
  ![Jupyter](docs/first_steps_files/notebook-005.png)

</details>


## Community

Stop by and say "Hi" in our [Discord Server](https://discord.gg/nU6TXRQNXC) - we're always happy to chat about data.

If you like what we're doing - support us by starring the repo.

If you'd like to contribute [start here](docs/contributing.md).

---

<div align="center">
  
[Docs] | [Tutorials] | [Examples] | [FAQ] | [Chat] | [Website] | [Contributing] | [License]
</div>

[Tutorials]: docs/learning_materials.md
[Examples]: docs/examples/index.md
[Docs]: docs/index.md
[Documentation]: docs/index.md
[FAQ]: docs/faq.md
[Chat]: https://discord.gg/nU6TXRQNXC
[Contributing]: docs/contributing.md
[License]: LICENSE.txt
[Website]: https://www.kamu.dev