# Directory Structure

- [Directory Structure](#directory-structure)
  - [Workspace](#workspace)
  - [Metadata Repository](#metadata-repository)
  - [Data Volume](#data-volume)

## Workspace

Workspace is a directory that is set up to keep track of certain datasets. It consists of:

- `.kamu` - Metadata repository
- `.kamu.local` - Local data volume

Workspace is usually a part of a `git` (or any other version control) repository. VCS stores everything needed to reconstruct exactly the same state of data on any other machine, but doesn't store data itself.

```
├── .git
├── .kamu
│   └── <see metadata repository>
│── .kamu.local
│   └── <see volume>
└── ...
```

## Metadata Repository

Metadata repository consists of:

- `datasets` - definitions of all datasets tracked by the workspace
- `volumes` - definitions of all volumes that are used for externally storing the dataset data
- `kamu.yaml` - local workspace configuration

```
├── datasets
│   ├── com.example.deriv.yaml
│   └── com.example.root.yaml
├── volumes
│   └── public.s3.example.com.yaml
├── .gitignore
└── kamu.yaml
```

## Data Volume

Volumes store actual data records locally within the workspace or externally (e.g. S3, GCS).

- `datasets` - definitions of the datasets present in the volume
- `data` - actual data records
- `checkpoints` - information necessary to resume the data processing
- `cache` (local volume only) - used for caching results between ingestion stages to speed up iterations

```
├── datasets
│   ├── com.example.deriv.yaml
│   └── com.example.root.yaml
├── data
│   ├── com.example.deriv
│   │   └── *.parquet
│   └── com.example.root
│       └── *.parquet
├── checkpoints
│   ├── com.example.root
│   │   ├── foo_1
│   │   │   └── <ingest polling checkpoint files>
│   │   └── bar_n
│   │       └── ...
│   └── com.example.deriv
│       └── primary
│           └── <spark streaming checkpoint files>
└── kamu.yaml
```
