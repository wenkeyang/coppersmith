# Metadata tool

The metadata tool is a tool to generate JSON/PSV metadata from feature job jars. The 
following are instructions on it. We intend to improve this process but the instructions
below work

- Step 1 - Build the assembly for the feature job. In the job's directory:
```bash
sbt assembly
```
and note the location of the output. Also note the package in which the job's features are specified.

- Step 2 - Build the metadata tool. In coppersmith's directory:
```bash
sbt 'project tools' package
```
noting the location of the jar.

Now, running

```bash
java -cp <assembly-jar>:<tools-jar> commbank.coppersmith.tools.MetadataMain --json <feature-package>
```

Note that the `--json` option can be replaced by `--psv` for Hydro PSV output.
