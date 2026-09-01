# CBZXL 4 reference implementation

This directory preserves the final Python implementation and its tests as a
behavioral reference for the Rust rebuild. It is not copied into the version 5
container and is not part of the production runtime.

Run its tests from this directory with the old Python requirements installed:

```sh
python -m unittest discover -s tests -v
```

The authoritative application, migration tooling, and deployment files are at
the repository root.

