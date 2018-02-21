There is some strangeness with the build and behavior because we are including v3 / Raptor JARs in the build that don't expect to be used outside of Altus. I'm documenting them here so that people are aware and don't report them as bugs. These are OK and harmless, as best I can determine.

* When launching from jar, it will create a directory labeled `file:/`
* On startup, DAL will throw some exceptions
* DAL will complain about `_lookuphost` being undefined
* It will also pause for about 90 seconds on startup

All of these things are odd, but we're shoe-horning here and everything still works so just ignore and carry on.
