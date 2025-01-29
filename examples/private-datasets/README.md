# private-datasets

In this example, combinations of datasets are provided, for convenient testing of the Private Datasets functionality.

### Datasets for lineage testing

**Legend:**

Dataset naming scheme: `$ACCOUNT/$LEVEL $VISIBILITY [$INDEX]`, where:
- `$ACCOUNT`: account
- `$LEVEL`: level in the dataset hierarchy
- `$VISIBILITY`: visibility, can be `priv` or `pub`
- `[$INDEX]`: optional dataset index


```mermaid
stateDiagram-v2
    direction LR

    alice/1pub1 --> alice/2pub
    alice/1priv1 --> alice/2pub
    alice/1pub2 --> alice/2priv
    alice/1priv2 --> alice/2priv

    alice/2pub --> alice/3pub
    alice/2priv --> alice/3pub

    alice/3pub --> alice/4pub
    alice/3pub --> alice/4priv

    alice/4pub --> alice/5pub1
    alice/4pub --> alice/5priv1
    alice/4priv --> alice/5pub2
    alice/4priv --> alice/5priv2
```

How `alice` sees the `alice/3pub` dataset lineage:

![alice_3pub_lineage.png](assets/alice_3pub_lineage.png)

How `bob` sees the `alice/3pub` dataset lineage:

![bob_3pub_lineage.png](assets/bob_3pub_lineage.png)

### Dataset with a private dependency

In case we have an inaccessible dataset of another user, we cannot update our derived dataset:

![bob_6pub1_lineage.png](assets/bob_6pub1_lineage.png)
![bob_6pub1_update.png](assets/bob_6pub1_update.png)
