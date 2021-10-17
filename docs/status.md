# Status <!-- omit in toc -->

- [Project Status Disclaimer](#project-status-disclaimer)
- [Feature Coverage](#feature-coverage)
- [Stability](#stability)

## Project Status Disclaimer
`kamu` is an **alpha quality** software. Our main goal currently is to demonstrate the potential of the [Open Data Fabric](https://github.com/kamu-data/open-data-fabric) protocol and its transformative properties to the community and the industry and validate our ideas.

Naturally, we don't recommend using `kamu` for any critical tasks - it's definitely **not prod-ready**. We are, however absolutely delighted to use `kamu` for our personal data analytics needs and small projects, and hoping you will enjoy it too.

If you do - simply make sure to **maintain your source data separately** and don't rely on `kamu` for data storage. This way any time a new version comes out that breaks some compatibility you can simply delete your kamu workspace and re-create it from scratch in a matter of seconds.

Also, please **be patient with current performance** and resource usage. We fully realize that waiting 15s to process a few KiB of CSV isn't great. Stream processing technologies is a relatively new area, and the data processing engines `kamu` uses (e.g. Apache Spark and Flink) are tailored to run in large clusters, not on a laptop. They take a lot of resources to just boot up, so the start-stop-continue nature of `kamu`'s transformations is at odds with their design. We are hoping that the industry will recognize our use-case and expect to see a better support for it in future. We are committed to improving the performance significantly in the near future.


## Feature Coverage

| Feature                            |  ODF  | kamu  |
| ---------------------------------- | :---: | :---: |
| Root datasets                      |   ✔️   |   ✔️   |
| Ingest merge strategies            |   ✔️   |   ✔️   |
| Derivative datasets                |   ✔️   |   ✔️   |
| Validation - Metadata integrity    |   ✔️   |   ✔️   |
| Validation - Data integrity        |   ❌   |   ❌   |
| Validation - Transformation replay |   ✔️   |   ✔️   |
| Source evolution                   |   ❌   |   ❌   |
| Schema evolution                   |   ❌   |   ❌   |
| Query migrations                   |   ❌   |   ❌   |
| Engine versioning                  |   ❌   |   ❌   |
| Engine migrations                  |   ❌   |   ❌   |



## Stability

| Component              | Stability |
| ---------------------- | :-------: |
| Dataset on-disk layout | Unstable  |
| CLI interface          | Unstable  |
| Engine interface       | Unstable  |


---

See also:
- [Supported engines](transform.md#supported-engines)