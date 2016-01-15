# Metadata tool

The metadata tool is a tool to generate Lua metadata from feature job jars. The 
following are instructions on it. We intend to improve this process but the instructions
below work

- Step 1 - Build the assembly for the feature job. In the job's directory:
```bash
sbt assembly
```
and note the location of the output. Also note the package in which the job's features are specified.

- Step 2 - Build the metadata tool. In coppersmith's directory:
```bash
sbt 'project tools2' package
```
noting the location of the jar.

Now, running

```bash
java -jar <assembly-jar>:<tools-jar> commbank.coppersmith.MetadataMain <feature-package>
```
