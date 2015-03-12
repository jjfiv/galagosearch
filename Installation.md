# Introduction #

Installing Galago

# Details #

## 1. Dependencies ##

**Java 6 or newer**

Probably already installed on your machine,
but if not:
> - http://www.java.com/

**Maven 2.0 or greater**

Available for most operating systems. This package can be obtained from:
> - http://maven.apache.org/

## 2. Obtaining the source code ##

For the most recent version, please use the svn command from:
> - http://code.google.com/p/galagosearch/source/checkout


## 3. Compiling ##

Assuming you have installed mvn and checked out the most recent version of galago. Within a terminal, go to the galago directory and run the commands:

```
> mvn package
```

You will see a lot of output.
Eventually you should see something similar to:

```


[INFO] ------------------------------------------------------------------------
[INFO] Reactor Summary:
[INFO] ------------------------------------------------------------------------
[INFO] galagosearch .......................................... SUCCESS [1:32.327s]
[INFO] galagosearch-tupleflow-typebuilder .................... SUCCESS [2:47.822s]
[INFO] galagosearch-tupleflow ................................ SUCCESS [10.986s]
[INFO] galagosearch-core ..................................... SUCCESS [2:03.878s]
[INFO] ------------------------------------------------------------------------
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESSFUL
[INFO] ------------------------------------------------------------------------
[INFO] Total time: 6 minutes 35 seconds
[INFO] Finished at: Thu Feb 24 11:15:12 EST 2011
[INFO] Final Memory: 44M/123M
[INFO] ------------------------------------------------------------------------
```

This output indicates success.

## 4. Running the Galago application ##

You can now set permissions and run the Galago application:

```
> chmod +x ./galagosearch-core/target/appassembler/bin/galago
> ./galagosearch-core/target/appassembler/bin/galago
```

You should see the output:

```
Popular commands:
   build-fast
   search
   batch-search

All commands:
   batch-search
   build
   build-fast
   build-parallel
   build-topdocs
   doc
   dump-connection
   dump-corpus
   dump-index
   dump-keys
   dump-keyvalue
   dump-lengths
   dump-names
   eval
   make-corpus
   merge-index
   ngram
   ngram-se
   pagerank
   parameter-sweep
   search
```