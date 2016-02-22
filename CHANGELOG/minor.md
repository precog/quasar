Convert the REPL over to our new architecture, with a handful of improvements.
- When launching the REPL, "-c" or "--config" should now be used on the command line to specify the location of the configuration file.
- Better feedback about errors when launching the REPL.
- A new setting, `format`, which allows JSON or CSV output to be selected. The default is still the nicely aligned table.

It's now possible to evaluate "queries" that do not refer to any data source (such as `1 + 2`), even if multiple mounts are present.
