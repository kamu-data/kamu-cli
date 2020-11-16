# Exploring Data

- [Exploring Data](#exploring-data)
  - [SQL Console](#sql-console)
  - [Jupyter Notebooks](#jupyter-notebooks)

While `kamu` focuses primarily on the problem of data management, you often may want to do some basic data exploration before exporting data for further use in your data science projects. Just like `gitk` in `git` we decided to provide a few simple exploration tools for you to assess the state of data without leaving the comfort of one tool.

## SQL Console
`kamu` provides a simple way to run ad-hoc queries and explore data using SQL language.

![kamu sql](./first_steps_files/sql.gif)

Following comand will drop you into the SQL shell:
```bash
$ kamu sql
```

SQL console currently uses [Apache Spark](https://spark.apache.org/) engine.

All datasets in your workspace should be available to you as tables:

```bash
kamu> show tables;
```

You can use `describe` to inspect the dataset's schema:

```bash
kamu> describe `us.cityofnewyork.data.zipcode-boundaries`;
```

> Note the extra back ticks needed to treat the dataset ID containing dots as a table name.

For brevity you can create aliases as:

```bash
kamu> create temp view zipcodes as (select * from `us.cityofnewyork.data.zipcode-boundaries`);
```

And of course you can run queries against any dataset:

```bash
0: kamu> select po_name, sum(population) from zipcodes group by po_name;
```

Use `Ctrl+D` to exit the SQL shell.

SQL is a widely supported language, so `kamu` can be used in conjuction with many other tools that support it, such as Tableau and Power BI. Use following command to expose `kamu` data through the JDBC server:

```bash
$ kamu sql server
```

The `kamu sql` is a very powerful command that you can use both interactively or for scripting. We encourage you to explore more of its options through `kamu sql --help`.


## Jupyter Notebooks
`kamu` also connects the power of Apache Spark with the [Jupyter Notebook](https://jupyter.org/) server. You can get started by running:

```bash
$ kamu notebook
```

> Note: You can use `-e ENV_VAR` option to pass additional environment variable into the notebook server. This can be very useful for different access and security tokens needed by different visualization APIs.

Executing this should open your default browser with a Jupyter running in it.

From here create a `PySpark` notebook. We start all notebooks by loading `kamu` extension:

```
%load_ext kamu
```

After this the `import_dataset` command becomes available and we can load the dataset and alias it by doing:

```
%import_dataset us.cityofnewyork.data.zipcode-boundaries --alias zipcodes
```

![kamu notebook 001](./first_steps_files/notebook-001.png)

This will take a few seconds as in the background it creates Apache Spark session, and it is Spark that loads the dataset into what it calls a "dataframe".

You can then start using the `zipcodes` dataframe in the exact same way you would in an interactive `spark-shell`.

There few very important things to understand here:
- Spark and Jupyter are running in separate processes
- The commands you execute in the notebook are executed "remotely" and the results are transferred back
- This means that it doesn't really matter if your data is located on your machine or somewhere else - the notebook will work the same

The dataframe is automatically exposed in the SQL engine too, and you can run SQL queries using `%%sql` annotation:

![kamu notebook 002](./first_steps_files/notebook-002.png)

Thanks to the [sparkmagic](https://github.com/jupyter-incubator/sparkmagic) library you also get some simple instant visualizations for results of your queries.

![kamu notebook 003](./first_steps_files/notebook-003.png)

After you are done joining, filtering, and shaping the data you can choose to get it out of the Spark into the Jupyter notebook kernel using `%%sql -o alias` command

![kamu notebook 004](./first_steps_files/notebook-004.png)

Now that you have the data in Jupyter - you can use any of your favorite tools and libraries to further process it or visualize it.
