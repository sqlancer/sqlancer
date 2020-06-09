# Development


## Options

SQLancer uses [JCommander](https://jcommander.org/) for handling options. The `MainOptions` class contains options that are expected to be supported by all DBMS-testing implementations. Furthermore, each `*Provider` class provides a method to return an additional set of supported options.

An option can include lowercase alphanumeric characters, and hyphens. The format of the options is checked by a unit test.
