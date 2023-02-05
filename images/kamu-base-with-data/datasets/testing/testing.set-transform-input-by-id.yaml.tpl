version: 1
kind: DatasetSnapshot
content:
  name: testing.set-transform-input-by-id
  kind: derivative
  metadata:
    - kind: setTransform
      inputs:
        - id: "did:odf:<substitiute>"
          name: foo
      transform:
        kind: sql
        engine: flink
        query: "select event_time from foo"
